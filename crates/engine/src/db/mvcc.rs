//! MVCC version buffer & snapshot isolation (F3, `research.md` §MVCC).
//!
//! Every commit allocates a monotonically increasing commit epoch (under the
//! commit lock, so epochs follow WAL order). While at least one snapshot
//! reader is active, each install records the superseded row state:
//! `chains[(table, pk)]` holds `(until_epoch, row)` newest-first, and
//! `committed[(table, pk)]` holds the live row's epoch.
//!
//! A snapshot pinned at read epoch R sees, per key, the current row when its
//! commit epoch is < R, else the row of the chain entry with the smallest
//! `until` still >= R (that entry's row was valid up to `until`). Absent keys
//! (never-inserted, or created after R) resolve to `None`; keys deleted after
//! R resolve to their pre-delete row, so scans sweep chains for missing keys.
//!
//! Recording is skipped entirely when no snapshot is active (zero overhead
//! for plain OLTP, and the common case stays exactly the old code path).
//! History never survives restarts (in-memory only; post-open rows read as
//! epoch 0) and multi-row commits install row-by-row, so a concurrent
//! snapshot may observe a commit partially — per-row time travel, not atomic
//! commit visibility. Both are documented v1 limits.
//!
//! Garbage collection: an entry with `until < oldest_active_R` is never
//! consulted by any live reader (walks stop at the first `until < R`), so
//! pruning drops those entries and committed epochs `< oldest`. With no
//! active readers everything drains.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use super::{ActiveTxn, Database, Output, Session};
use crate::error::{Error, Result};
use crate::table::Table;
use crate::types::Datum;

/// Superseded states for one key, newest first: `(until_epoch, row)` where
/// `row` was the visible state for epochs `< until_epoch` (`None` = absent).
type Chain = Vec<(u64, Option<Vec<Datum>>)>;

pub(crate) struct VersionState {
    /// Latest committed epoch per live key.
    committed: HashMap<(String, Vec<u8>), u64>,
    /// Superseded states per key, newest first.
    chains: HashMap<(String, Vec<u8>), Chain>,
    /// Active snapshot readers: snapshot id -> pinned read epoch.
    snapshots: HashMap<u64, u64>,
    next_snapshot_id: AtomicU64,
}

impl VersionState {
    pub(crate) fn new() -> Self {
        VersionState {
            committed: HashMap::new(),
            chains: HashMap::new(),
            snapshots: HashMap::new(),
            next_snapshot_id: AtomicU64::new(1),
        }
    }

    /// Drop history no live reader can consult (see module docs).
    fn gc_locked(&mut self) {
        match self.snapshots.values().copied().min() {
            None => {
                self.chains.clear();
                self.committed.clear();
            }
            Some(oldest) => {
                self.chains.retain(|_, chain| {
                    chain.retain(|(until, _)| *until >= oldest);
                    !chain.is_empty()
                });
                self.committed.retain(|_, e| *e >= oldest);
            }
        }
    }
}

/// Snapshot pinned by `START TRANSACTION WITH CONSISTENT SNAPSHOT`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SnapshotPin {
    pub(crate) id: u64,
    pub(crate) read_epoch: u64,
}

impl Database {
    /// Next commit epoch (called under the commit lock; follows WAL order).
    pub(crate) fn alloc_commit_epoch(&self) -> u64 {
        self.commit_epoch.fetch_add(1, Ordering::SeqCst)
    }

    /// Record the superseded state of one install at `epoch`. Skips silently
    /// when no snapshot reader is active.
    pub(crate) fn record_install(
        &self,
        table: &Arc<Table>,
        key: &[u8],
        new_enc: Option<&[u8]>,
        epoch: u64,
    ) -> Result<()> {
        let mut vs = self.versions.write().unwrap();
        if vs.snapshots.is_empty() {
            return Ok(());
        }
        let prev_raw = table.tree().get(key);
        match (&prev_raw, new_enc) {
            (None, None) => return Ok(()), // deleting an absent key: no state
            (Some(a), Some(b)) if a == b => {
                // Identical overwrite: keep the older epoch (row unchanged).
                return Ok(());
            }
            _ => {}
        }
        let prev = match prev_raw {
            Some(raw) => Some(table.decode_stored(&raw)?),
            None => None,
        };
        let tkey = (table.def.name.clone(), key.to_vec());
        vs.chains.entry(tkey.clone()).or_default().insert(0, (epoch, prev));
        match new_enc {
            Some(_) => {
                vs.committed.insert(tkey, epoch);
            }
            None => {
                vs.committed.remove(&tkey);
            }
        }
        Ok(())
    }

    /// Resolve `current` (the tree state) to what `session`'s snapshot sees.
    /// Passes through untouched when the session holds no snapshot.
    /// Boundary: commit C is visible at read epoch R iff C < R (the pin
    /// observes the allocation counter, so the next commit lands exactly on
    /// R and must stay invisible).
    pub(crate) fn snapshot_lookup(
        &self,
        session: &Session,
        table: &Arc<Table>,
        key: &[u8],
        current: Option<Vec<Datum>>,
    ) -> Result<Option<Vec<Datum>>> {
        let Some(snap) = &session.snapshot else {
            return Ok(current);
        };
        let vs = self.versions.read().unwrap();
        let tkey = (table.def.name.clone(), key.to_vec());
        if vs.committed.get(&tkey).copied().unwrap_or(0) < snap.read_epoch {
            return Ok(current);
        }
        if let Some(chain) = vs.chains.get(&tkey) {
            let mut ans: Option<Vec<Datum>> = None;
            let mut found = false;
            for (until, row) in chain.iter() {
                if *until >= snap.read_epoch {
                    ans = row.clone();
                    found = true;
                } else {
                    break;
                }
            }
            if found {
                return Ok(ans);
            }
        }
        // Defensive: history pruned beyond recognition; current is the only
        // state left (the prune rule keeps every entry a live reader needs,
        // so this is unreachable in practice).
        Ok(current)
    }

    /// Keys whose history shows presence at the snapshot epoch but which are
    /// absent from the current scan (deleted after pinning). `present` holds
    /// the scanned keys of `table`.
    pub(crate) fn snapshot_scan_extra(
        &self,
        session: &Session,
        table: &Arc<Table>,
        present: &HashSet<Vec<u8>>,
    ) -> Result<Vec<(Vec<u8>, Vec<Datum>)>> {
        let Some(snap) = &session.snapshot else {
            return Ok(Vec::new());
        };
        let vs = self.versions.read().unwrap();
        let mut out = Vec::new();
        for ((t, k), chain) in vs.chains.iter() {
            if t != &table.def.name || present.contains(k) {
                continue;
            }
            let mut ans: Option<Vec<Datum>> = None;
            let mut found = false;
            for (until, row) in chain.iter() {
                if *until >= snap.read_epoch {
                    ans = row.clone();
                    found = true;
                } else {
                    break;
                }
            }
            if found {
                if let Some(row) = ans {
                    out.push((k.clone(), row));
                }
            }
        }
        Ok(out)
    }

    /// `START TRANSACTION WITH CONSISTENT SNAPSHOT`: begin a txn pinned at
    /// the current commit epoch and register it for GC protection.
    pub(crate) fn snapshot_begin(&self, session: &mut Session) -> Result<Output> {
        if session.txn.is_some() {
            return Err(Error::TxnConflict("transaction already active".into()));
        }
        let read_epoch = self.commit_epoch.load(Ordering::SeqCst);
        let id = {
            let mut vs = self.versions.write().unwrap();
            let id = vs.next_snapshot_id.fetch_add(1, Ordering::Relaxed);
            vs.snapshots.insert(id, read_epoch);
            id
        };
        let txn_id = self.next_txn.fetch_add(1, Ordering::Relaxed);
        session.txn = Some(ActiveTxn {
            id: txn_id,
            staged: HashMap::new(),
        });
        session.snapshot = Some(SnapshotPin { id, read_epoch });
        Ok(Output::ok("BEGIN"))
    }

    /// Release a session's snapshot pin and prune newly-unreachable history.
    pub(crate) fn snapshot_end(&self, session: &mut Session) {
        if let Some(snap) = session.snapshot.take() {
            let mut vs = self.versions.write().unwrap();
            vs.snapshots.remove(&snap.id);
            vs.gc_locked();
        }
    }

    /// Prune history unreachable by live readers (called on checkpoint).
    pub(crate) fn gc_versions(&self) {
        self.versions.write().unwrap().gc_locked();
    }

    /// Forget all history for one table (called on DROP TABLE).
    pub(crate) fn purge_table_versions(&self, table_key: &str) {
        let mut vs = self.versions.write().unwrap();
        vs.chains.retain(|(t, _), _| t != table_key);
        vs.committed.retain(|(t, _), _| t != table_key);
    }

    /// Forget all history for a database prefix (called on DROP DATABASE).
    pub(crate) fn purge_db_versions(&self, prefix: &str) {
        let mut vs = self.versions.write().unwrap();
        vs.chains.retain(|(t, _), _| !t.starts_with(prefix));
        vs.committed.retain(|(t, _), _| !t.starts_with(prefix));
    }

    /// (chain keys, total entries, active snapshots) — test observability.
    #[cfg(test)]
    pub(crate) fn version_stats(&self) -> (usize, usize, usize) {
        let vs = self.versions.read().unwrap();
        let entries = vs.chains.values().map(|c| c.len()).sum();
        (vs.chains.len(), entries, vs.snapshots.len())
    }
}
