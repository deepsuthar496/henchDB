//! Database facade: sessions, transactions, SQL execution, recovery, and
//! checkpointing.
//!
//! Concurrency model (v0.1):
//! - Reads hit the B+ trees directly via optimistic lock coupling — no
//!   global read lock, no latch on the catalog (tables are `Arc`-cloned).
//! - Writes are staged per transaction in a session-local write set. COMMIT
//!   takes the single commit lock, validates (duplicate-key checks), writes
//!   the WAL batch durably (group-commit seam), then installs into the
//!   trees. This gives snapshot-of-committed-state reads and instant aborts
//!   (drop the staging buffer — no undo log), matching the RCC direction in
//!   the research doc in simplified form.
//! - DDL is autocommit and serialized through the same commit lock.
//!
//! Roadmap replacements are annotated throughout (see agents.md): snapshot
//! MVCC version buffer replacing the commit lock, per-core WAL shards, and
//! pointer-swizzled trees.
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use crate::catalog;
use crate::error::{Error, Result};
use crate::metrics::{Metrics, StmtKind};
use crate::page::{BufferPool, MAX_VALUE_LEN};
use crate::sql::{parse_sql, Expr, Privilege, Statement};
use crate::table::Table;
use crate::types::{decode_key, encode_key, Datum};
use crate::wal::{Record, Wal};
use mvcc::SnapshotPin;

use dml::parse_simple_literal;
use plan::AccessPath;

pub(crate) mod batch;
pub(crate) mod cost;
pub(crate) mod ddl;
pub(crate) mod diag;
pub(crate) mod dml;
pub(crate) mod explain;
pub(crate) mod fk;
pub(crate) mod join;
pub(crate) mod memo;
pub(crate) mod mvcc;
pub(crate) mod plan;
pub mod privilege;
pub mod check;
pub(crate) mod query;

pub use check::CheckReport;
pub(crate) mod replica;
pub(crate) mod subquery;
pub(crate) mod sysviews;
#[cfg(test)]
mod tests;

#[derive(Debug, Clone, PartialEq)]
pub struct Output {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Datum>>,
    pub message: String,
}

impl Output {
    pub(super) fn ok(msg: impl Into<String>) -> Output {
        Output {
            columns: vec![],
            rows: vec![],
            message: msg.into(),
        }
    }
}

/// A client session: at most one active transaction and active database context.
use std::time::Duration;
use crate::sql::IsolationLevel;

pub struct Session {
    pub(crate) txn: Option<ActiveTxn>,
    pub current_db: String,
    /// Authenticated username (empty before login; defaults to `root` for
    /// local sessions). Backs the `user()` system function.
    pub user: String,
    pub max_execution_time: Option<Duration>,
    pub max_result_rows: Option<usize>,
    pub max_result_bytes: Option<usize>,
    pub isolation_level: IsolationLevel,
    /// Pinned MVCC snapshot (`START TRANSACTION WITH CONSISTENT SNAPSHOT`).
    pub(crate) snapshot: Option<SnapshotPin>,
    /// Subquery evaluation state (derived-table materializations, correlated
    /// row frames, uncorrelated fold cache). Per-statement scope: cleared
    /// at the top of `execute`, managed by `db/subquery.rs`.
    pub(crate) subq: subquery::SubqueryState,
}

impl Session {
    /// True while an explicit transaction is open (drives PG ReadyForQuery).
    pub fn in_transaction(&self) -> bool {
        self.txn.is_some()
    }
}

impl Default for Session {
    fn default() -> Self {
        Session {
            txn: None,
            current_db: "default".to_string(),
            user: "root".to_string(),
            max_execution_time: None,
            max_result_rows: None,
            max_result_bytes: None,
            isolation_level: IsolationLevel::RepeatableRead,
            snapshot: None,
            subq: subquery::SubqueryState::default(),
        }
    }
}

pub(crate) struct ActiveTxn {
    id: u64,
    /// Staged write set: (table, encoded pk) -> write. `row: None` = delete.
    staged: HashMap<(String, Vec<u8>), StagedWrite>,
}

#[derive(Clone)]
pub(crate) struct StagedWrite {
    row: Option<Vec<Datum>>,
    is_insert: bool,
}

pub struct Database {
    pub(crate) databases: RwLock<HashSet<String>>,
    pub(crate) tables: RwLock<HashMap<String, Arc<Table>>>,
    pub(crate) wal: Wal,
    pub(crate) dir: PathBuf,
    /// Off-page overflow pool for wide rows (Priority 2). Shared by all
    /// tables; the `pages.bin` file persists next to WAL/snapshot so
    /// snapshot locators stay valid across restarts.
    pool: Arc<BufferPool>,
    /// Phase A of commit: validate + append (short critical section, no fsync).
    pub(crate) commit_lock: Mutex<()>,
    /// Phase C: installs happen strictly in WAL-offset order so in-memory
    /// state always matches replayed state. Guarded by `install_cv`.
    pub(crate) install: Mutex<u64>,
    install_cv: std::sync::Condvar,
    /// Keys of commits appended but not yet installed (duplicate-key guard
    /// for concurrent inserts while the commit lock is released for sync).
    in_flight: Mutex<HashSet<(String, Vec<u8>)>>,
    next_txn: AtomicU64,
    epoch: Arc<crate::epoch::EpochManager>,
    /// Monotonic commit epoch for MVCC (allocated under the commit lock, so
    /// epochs follow WAL order). Starts at 1; post-open rows read as epoch 0.
    commit_epoch: AtomicU64,
    /// Latest fully installed commit epoch visible to new snapshot readers.
    pub(crate) visible_epoch: AtomicU64,
    /// MVCC version buffer + snapshot registry (F3). Empty in plain OLTP.
    versions: RwLock<mvcc::VersionState>,
    /// Atomic telemetry: query/latency/WAL/connection counters + the
    /// process registry behind `SHOW PROCESSLIST` (see `metrics.rs`).
    /// Plain field — interior mutability via atomics, no lock on `&self`.
    metrics: Metrics,
    /// Replica read-only mode: rejects all WAL-appending statements with
    /// `Error::ReadOnlyReplica`. Set once at startup (`--replica-of`).
    read_only: AtomicBool,
    /// Upstream primary this replica follows (`host:port`, empty when none).
    /// Server-side replication client sets it; promotion clears it. Read by
    /// `SHOW STATUS LIKE 'Replica_%'`.
    pub(crate) replica_upstream: Mutex<String>,
    /// WAL archive directory for PITR (Priority 10): when set, every
    /// checkpoint copies the discarded durable WAL prefix into an immutable
    /// `HDBA` segment before truncating. Set once at startup
    /// (`--wal-archive-dir`); `None` disables archiving. Owned by
    /// `archive.rs` (`set_archive_dir` lives there per the file ceiling).
    pub(crate) archive_dir: Mutex<Option<PathBuf>>,
    /// RBAC principals + grants + staged passwords (see `privilege.rs`).
    pub(crate) privs: RwLock<privilege::PrivilegeStore>,
    /// Configurable maximum snapshot age in milliseconds (0 = unlimited).
    pub(crate) max_snapshot_age_ms: AtomicU64,
}

impl Database {
    pub fn set_max_snapshot_age(&self, dur: Option<Duration>) {
        self.max_snapshot_age_ms
            .store(dur.map(|d| d.as_millis() as u64).unwrap_or(0), Ordering::Relaxed);
    }

    pub fn max_snapshot_age(&self) -> Option<Duration> {
        let ms = self.max_snapshot_age_ms.load(Ordering::Relaxed);
        if ms == 0 {
            None
        } else {
            Some(Duration::from_millis(ms))
        }
    }
}

/// Default overflow-pool size: 8 frames x 256 KiB = 2 MiB resident. Small
/// on purpose — datasets larger than RAM are the point of the pool.
pub const DEFAULT_POOL_FRAMES: usize = 8;
impl Database {
    // ------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------

    /// Open (or create) a database directory: load the snapshot, then redo
    /// committed WAL records.
    pub fn open(dir: &Path) -> Result<Database> {
        fs::create_dir_all(dir)?;
        let epoch = crate::epoch::EpochManager::new();
        let pool = Arc::new(BufferPool::open(&dir.join("pages.bin"), DEFAULT_POOL_FRAMES, epoch.clone())?);
        let wal = Wal::open(&dir.join("wal.log"))?;
        let mut databases: HashSet<String> = HashSet::new();
        databases.insert("default".to_string());
        let mut tables: HashMap<String, Arc<Table>> = HashMap::new();

        let snap = dir.join("snapshot.bin");
        if snap.exists() {
            let mut f = File::open(&snap)?;
            let (snap_dbs, snap_tables) = catalog::decode_snapshot(&mut f)?;
            for db_name in snap_dbs {
                databases.insert(db_name);
            }
            for (def, rows) in snap_tables {
                if let Some((db_prefix, _)) = def.name.split_once('.') {
                    databases.insert(db_prefix.to_string());
                }
                let table = Arc::new(Table::new(def));
                table.set_pool(pool.clone());
                table.set_epoch_manager(epoch.clone());
                for row in rows {
                    match row.key {
                        Some(key) => table.restore_kv(&key, &row.value)?,
                        None => table.restore_raw(&row.value)?,
                    }
                }
                tables.insert(table.def.name.clone(), table);
            }
        }

        // Redo: buffer records per txn, install only on Commit.
        let mut pending: HashMap<u64, Vec<Record>> = HashMap::new();
        for rec in wal.read_all()? {
            match rec {
                Record::Commit { txn, .. } => {
                    if let Some(batch) = pending.remove(&txn) {
                        apply_records(&mut databases, &mut tables, &pool, &epoch, batch)?;
                    }
                }
                other => {
                    pending.entry(txn_of(&other)).or_default().push(other);
                }
            }
        }
        // Uncommitted tails are discarded: instant abort semantics.

        // Rebuild AUTO_INCREMENT counters from durable state so the sequence
        // never regresses across restarts (discarded tails stay unconsumed).
        for table in tables.values() {
            table.refresh_auto_inc()?;
        }
        // Create-before-auto-index tables gain their FK indexes now.
        Self::fk_ensure_all_auto_indexes(&tables)?;

        let install_frontier = wal.next_offset();
        Ok(Database {
            databases: RwLock::new(databases),
            tables: RwLock::new(tables),
            wal,
            dir: dir.to_path_buf(),
            pool,
            commit_lock: Mutex::new(()),
            install: Mutex::new(install_frontier),
            install_cv: std::sync::Condvar::new(),
            in_flight: Mutex::new(HashSet::new()),
            next_txn: AtomicU64::new(1),
            epoch,
            commit_epoch: AtomicU64::new(1),
            visible_epoch: AtomicU64::new(0),
            versions: RwLock::new(mvcc::VersionState::new()),
            metrics: Metrics::new(),
            read_only: AtomicBool::new(false),
            replica_upstream: Mutex::new(String::new()),
            archive_dir: Mutex::new(None),
            privs: RwLock::new(privilege::PrivilegeStore::default()),
            max_snapshot_age_ms: AtomicU64::new(0),
        })
    }

    pub(crate) fn acquire_commit_lock(&self) -> std::sync::MutexGuard<'_, ()> {
        if let Ok(guard) = self.commit_lock.try_lock() {
            return guard;
        }
        let t0 = std::time::Instant::now();
        let guard = self.commit_lock.lock().unwrap();
        self.metrics.record_lock_wait(t0.elapsed().as_micros() as u64);
        guard
    }

    /// Archive directory, if enabled.
    pub fn archive_dir(&self) -> Option<PathBuf> {
        self.archive_dir.lock().unwrap().clone()
    }

    pub fn new_session(&self) -> Session {
        Session::default()
    }

    pub fn epoch(&self) -> &Arc<crate::epoch::EpochManager> {
        &self.epoch
    }

    /// (sync_data calls, total bytes synced) from the WAL syncer.
    pub fn sync_stats(&self) -> (u64, u64) {
        self.wal.sync_stats()
    }


    /// Overflow-pool counters (hits, faults, evictions, residency).
    pub fn pool_stats(&self) -> crate::page::PoolStats {
        self.pool.stats()
    }

    /// Online physical backup into `writer` (HDBB archive). Checkpoints
    /// first, then streams under the commit and install locks; see
    /// `backup.rs` for the format and consistency contract.
    pub fn dump<W: std::io::Write>(&self, writer: &mut W) -> Result<crate::backup::BackupStats> {
        self.checkpoint()?;
        self.dump_live(writer).map(|(stats, _)| stats)
    }

    /// Flush a durable snapshot and truncate the WAL.
    pub fn checkpoint(&self) -> Result<()> {
        let _guard = self.acquire_commit_lock();
        self.pool.sync_data()?;

        let dbs_guard = self.databases.read().unwrap();
        let mut db_list: Vec<String> = dbs_guard.iter().cloned().collect();
        db_list.sort();
        drop(dbs_guard);

        let tables = self.tables.read().unwrap();
        let mut data = Vec::new();
        for table in tables.values() {
            let rows = table.tree().scan_all();
            data.push((table.table_def(), rows));
        }
        drop(tables);
        crate::failpoint!("before_snapshot_temp_write");
        let tmp = self.dir.join("snapshot.bin.tmp");
        {
            let f = File::create(&tmp)?;
            let mut bw = std::io::BufWriter::with_capacity(128 * 1024, f);
            catalog::encode_snapshot(&mut bw, &db_list, &data)?;
            bw.flush()?;
            crate::failpoint!("after_snapshot_temp_write");
            crate::failpoint!("before_snapshot_fsync");
            bw.into_inner().map_err(|e| Error::Io(e.to_string()))?.sync_data()?;
            crate::failpoint!("after_snapshot_fsync");
        }
        crate::failpoint!("before_snapshot_rename");
        fs::rename(&tmp, self.dir.join("snapshot.bin"))?;
        crate::failpoint!("after_snapshot_rename");
        // PITR: persist the about-to-be-truncated durable prefix first (see
        // `archive.rs`; an archive failure aborts before the truncate, and
        // open() redoes the intact WAL idempotently).
        self.archive_checkpoint_prefix()?;
        crate::failpoint!("before_wal_reset");
        self.wal.reset()?;
        crate::failpoint!("after_wal_reset");
        // Re-base the install frontier: offsets restart after the truncate.
        let frontier = self.wal.next_offset();
        *self.install.lock().unwrap() = frontier;
        // History unreachable by live readers drains here (everything, when
        // no snapshot is active).
        self.gc_versions();
        Ok(())
    }

    // ------------------------------------------------------------------
    // Execution
    // ------------------------------------------------------------------

    pub fn execute(&self, session: &mut Session, sql: &str) -> Result<Output> {
        // Replica governance first: rejected writes never reach the
        // executor, the WAL, or the metrics counters.
        if self.read_only.load(Ordering::Relaxed) && replica::is_write_statement(sql.trim()) {
            return Err(Error::ReadOnlyReplica);
        }
        // Fresh subquery scope per statement (derived materializations,
        // correlation frames, and fold caches never leak across queries).
        session.subq.eph.clear();
        session.subq.outer.clear();
        session.subq.fold.clear();
        let _guard = self.epoch.pin();
        let t0 = std::time::Instant::now();
        let trimmed = sql.trim();
        let res = self.execute_routed(session, sql, trimmed);
        // Telemetry (hot path: atomics only). Classification from the first
        // keyword covers fast-path and parsed statements uniformly; errors
        // count like MySQL (attempted statements are still queries).
        let kind = StmtKind::classify(trimmed);
        self.metrics
            .record_query(kind, t0.elapsed().as_micros() as u64);
        res
    }

    fn execute_routed(&self, session: &mut Session, _sql: &str, trimmed: &str) -> Result<Output> {
        if trimmed.eq_ignore_ascii_case("begin") || trimmed.eq_ignore_ascii_case("begin;") {
            return self.execute_stmt(session, Statement::Begin { isolation: None, read_only: false });
        }
        if trimmed.eq_ignore_ascii_case("commit") || trimmed.eq_ignore_ascii_case("commit;") {
            return self.execute_stmt(session, Statement::Commit);
        }
        if trimmed.eq_ignore_ascii_case("rollback") || trimmed.eq_ignore_ascii_case("rollback;") {
            return self.execute_stmt(session, Statement::Rollback);
        }
        if let Some(out) = self.try_fast_point_select(session, trimmed)? {
            return Ok(out);
        }
        if let Some(out) = self.try_fast_point_update(session, trimmed)? {
            return Ok(out);
        }
        self.prepare_statement_snapshot(session, trimmed)?;
        let res = self.execute_inner(session, trimmed);
        self.cleanup_statement_snapshot(session);
        res
    }

    fn prepare_statement_snapshot(&self, session: &mut Session, trimmed: &str) -> Result<()> {
        if let Some(snap) = &session.snapshot {
            if let Some(max_age) = self.max_snapshot_age() {
                if snap.created_at.elapsed() > max_age {
                    return Err(Error::ExecutionError(format!(
                        "snapshot exceeded maximum configured age limit ({:?})",
                        max_age
                    )));
                }
            }
        }
        if trimmed.is_empty() {
            return Ok(());
        }
        let first_word = trimmed.split_whitespace().next().unwrap_or("");
        let is_read = first_word.eq_ignore_ascii_case("select")
            || first_word.eq_ignore_ascii_case("explain")
            || first_word.eq_ignore_ascii_case("with");
        if session.in_transaction() {
            if session.isolation_level == IsolationLevel::RepeatableRead {
                if session.snapshot.is_none() && is_read {
                    self.snapshot_pin_active(session);
                }
            } else if is_read {
                self.snapshot_pin_statement(session);
            }
        } else if is_read {
            self.snapshot_pin_statement(session);
        }
        Ok(())
    }

    fn cleanup_statement_snapshot(&self, session: &mut Session) {
        self.snapshot_end_statement(session);
    }

    fn execute_inner(&self, session: &mut Session, trimmed: &str) -> Result<Output> {
        let stmt = parse_sql(trimmed)?;
        self.execute_stmt(session, stmt)
    }

    fn try_fast_point_select(&self, session: &Session, sql: &str) -> Result<Option<Output>> {
        if session.txn.is_some() || session.snapshot.is_some() {
            return Ok(None);
        }
        let s = sql.strip_suffix(';').unwrap_or(sql).trim();
        if s.len() < 15 {
            return Ok(None);
        }
        if !s.get(..7).map_or(false, |p| p.eq_ignore_ascii_case("SELECT ")) {
            return Ok(None);
        }
        let rest = s[7..].trim_start();
        let from_pos = match rest.to_ascii_lowercase().find(" from ") {
            Some(i) => i,
            None => return Ok(None),
        };
        let col_clause = rest[..from_pos].trim();
        let rest = rest[from_pos + 6..].trim_start();

        let where_pos = match rest.to_ascii_lowercase().find(" where ") {
            Some(i) => i,
            None => return Ok(None),
        };
        let table_name = rest[..where_pos].trim();
        let where_clause = rest[where_pos + 7..].trim();

        if table_name.contains(|c: char| c.is_whitespace()) || col_clause.contains(',') {
            return Ok(None);
        }

        let (pk_col, pk_val_str) = match where_clause.split_once('=') {
            Some((c, v)) => (c.trim(), v.trim()),
            None => return Ok(None),
        };
        if pk_col.contains(|c: char| c.is_whitespace()) || pk_val_str.contains(|c: char| c.is_whitespace()) {
            return Ok(None);
        }

        let pk_val = match parse_simple_literal(pk_val_str) {
            Some(d) => d,
            None => return Ok(None),
        };
        // NULL never matches (and has no key encoding): slow path returns
        // empty correctly.
        if matches!(pk_val, Datum::Null) {
            return Ok(None);
        }

        let table_arc = match self.table(session, table_name) {
            Ok(t) => t,
            Err(_) => return Ok(None),
        };
        // Fast-path reads are gated like parsed SELECTs (deny, don't skip).
        if let Err(e) = privilege::check_table(self, session, table_name, Privilege::Select) {
            return Err(e);
        }
        let schema = table_arc.schema();

        if schema.columns[schema.pk_idx].name != pk_col {
            return Ok(None);
        }

        let col_idx = match schema.index_of(col_clause) {
            Some(i) => i,
            None => return Ok(None),
        };

        let key = encode_key(&pk_val)?;
        let raw = match table_arc.tree().get(&key) {
            Some(r) => r,
            None => {
                return Ok(Some(Output {
                    columns: vec![col_clause.to_string()],
                    rows: vec![],
                    message: "OK".into(),
                }))
            }
        };
        let row = table_arc.decode_stored(&raw)?;
        let val = row[col_idx].clone();
        Ok(Some(Output {
            columns: vec![col_clause.to_string()],
            rows: vec![vec![val]],
            message: "OK".into(),
        }))
    }

    fn execute_stmt(&self, session: &mut Session, stmt: Statement) -> Result<Output> {
        privilege::enforce(self, session, &stmt)?;
        match stmt {
            Statement::CreateUser { .. }
            | Statement::DropUser { .. }
            | Statement::AlterUser { .. }
            | Statement::Grant { .. }
            | Statement::Revoke { .. }
            | Statement::ShowGrants { .. } => privilege::exec_user_mgmt(self, session, stmt),
            Statement::Begin { isolation, read_only: _ } => {
                if session.txn.is_some() {
                    return Err(Error::TxnConflict("transaction already active".into()));
                }
                if let Some(lvl) = isolation {
                    session.isolation_level = lvl;
                }
                let id = self.next_txn.fetch_add(1, Ordering::Relaxed);
                session.txn = Some(ActiveTxn {
                    id,
                    staged: HashMap::new(),
                });
                self.metrics.txn_begin();
                Ok(Output::ok("BEGIN"))
            }
            Statement::Commit => {
                let txn = session.txn.take().ok_or(Error::TxnNotActive)?;
                self.metrics.txn_end();
                self.commit_txn(txn.id, txn.staged)?;
                self.snapshot_end(session);
                Ok(Output::ok("COMMIT"))
            }
            Statement::Rollback => {
                session.txn.take().ok_or(Error::TxnNotActive)?;
                self.metrics.txn_end();
                self.snapshot_end(session);
                Ok(Output::ok("ROLLBACK"))
            }
            Statement::StartTransaction { snapshot, isolation, read_only } => {
                if let Some(lvl) = isolation {
                    session.isolation_level = lvl;
                }
                if snapshot {
                    self.snapshot_begin(session)
                } else {
                    return self.execute_stmt(session, Statement::Begin { isolation, read_only });
                }
            }
            Statement::SetTransaction { isolation, global } => {
                if !global {
                    session.isolation_level = isolation;
                }
                Ok(Output::ok("isolation level set"))
            }
            Statement::ShowTables => {
                let prefix = format!("{}.", session.current_db);
                let tables = self.tables.read().unwrap();
                let mut names: Vec<String> = Vec::new();
                for key in tables.keys() {
                    if let Some(rest) = key.strip_prefix(&prefix) {
                        names.push(rest.to_string());
                    } else if session.current_db == "default" && !key.contains('.') {
                        names.push(key.clone());
                    }
                }
                names.sort();
                Ok(Output {
                    columns: vec!["table".into()],
                    rows: names.into_iter().map(|n| vec![Datum::Text(n)]).collect(),
                    message: "OK".into(),
                })
            }
            Statement::Checkpoint => {
                self.checkpoint()?;
                Ok(Output::ok("checkpoint complete"))
            }
            Statement::Promote => {
                // Allowed through the read-only gate (PROMOTE is not a
                // write verb): on replicas it fences + flips to primary
                // and the feeder detaches; on primaries it errors.
                // Any authenticated user may promote (per-user privileges
                // are the SEC8 follow-up, same as COM_SHUTDOWN).
                self.promote()?;
                Ok(Output::ok(format!(
                    "promoted to primary (generation {})",
                    self.wal_generation()
                )))
            }
            Statement::Backup { path } => {
                let file = std::fs::File::create(&path)?;
                let mut bw = std::io::BufWriter::with_capacity(128 * 1024, file);
                let stats = self.dump(&mut bw)?;
                drop(bw);
                Ok(Output::ok(format!(
                    "backup complete: {} databases, {} tables, {} rows, {} bytes",
                    stats.databases, stats.tables, stats.rows, stats.bytes_written
                )))
            }
            Statement::SetVariable { name, value } => {
                if name.eq_ignore_ascii_case("transaction_isolation")
                    || name.eq_ignore_ascii_case("tx_isolation")
                {
                    match &value {
                        Datum::Text(s) => {
                            let up = s.to_ascii_uppercase().replace('_', "-");
                            if up.contains("READ-COMMITTED") || up.contains("READ COMMITTED") {
                                session.isolation_level = IsolationLevel::ReadCommitted;
                            } else if up.contains("REPEATABLE-READ") || up.contains("REPEATABLE READ") {
                                session.isolation_level = IsolationLevel::RepeatableRead;
                            } else if up.contains("SERIALIZABLE") {
                                session.isolation_level = IsolationLevel::Serializable;
                            }
                        }
                        _ => {}
                    }
                }
                if name.eq_ignore_ascii_case("max_execution_time") {
                    match value {
                        Datum::Int(ms) if ms > 0 => {
                            session.max_execution_time = Some(Duration::from_millis(ms as u64));
                        }
                        Datum::Int(_) => {
                            session.max_execution_time = None;
                        }
                        _ => {
                            return Err(Error::ParseError(
                                "max_execution_time must be an integer (milliseconds)".into(),
                            ))
                        }
                    }
                }
                if name.eq_ignore_ascii_case("max_result_rows") || name.eq_ignore_ascii_case("max_rows") {
                    match value {
                        Datum::Int(r) if r > 0 => session.max_result_rows = Some(r as usize),
                        Datum::Int(_) => session.max_result_rows = None,
                        _ => return Err(Error::ParseError("max_result_rows must be an integer".into())),
                    }
                }
                if name.eq_ignore_ascii_case("max_result_bytes") {
                    match value {
                        Datum::Int(b) if b > 0 => session.max_result_bytes = Some(b as usize),
                        Datum::Int(_) => session.max_result_bytes = None,
                        _ => return Err(Error::ParseError("max_result_bytes must be an integer".into())),
                    }
                }
                if name.eq_ignore_ascii_case("max_snapshot_age") || name.eq_ignore_ascii_case("max_snapshot_age_secs") {
                    match value {
                        Datum::Int(s) if s > 0 => self.set_max_snapshot_age(Some(Duration::from_secs(s as u64))),
                        Datum::Int(_) => self.set_max_snapshot_age(None),
                        _ => return Err(Error::ParseError("max_snapshot_age must be an integer (seconds)".into())),
                    }
                }
                Ok(Output::ok("variable set"))
            }
            Statement::CreateDatabase { name, if_not_exists } => {
                self.exec_create_database(&name, if_not_exists)
            }
            Statement::DropDatabase { name, if_exists } => {
                self.exec_drop_database(&name, if_exists)
            }
            Statement::UseDatabase { name } => {
                self.exec_use_database(session, &name)
            }
            Statement::ShowDatabases => {
                let dbs = self.databases.read().unwrap();
                let mut names: Vec<String> = dbs.iter().cloned().collect();
                names.sort();
                Ok(Output {
                    columns: vec!["Database".into()],
                    rows: names.into_iter().map(|n| vec![Datum::Text(n)]).collect(),
                    message: "OK".into(),
                })
            }
            Statement::ShowStatus { like } => Ok(self.show_status(like.as_deref())),
            Statement::ShowEngineStatus => Ok(self.show_engine_status()),
            Statement::ShowProcesslist => Ok(self.show_processlist()),
            Statement::AnalyzeTable { table } => self.exec_analyze(session, &table),
            Statement::CheckTable { table } => {
                let report = self.check_table(session, &table)?;
                let msg_text = report
                    .error_msg
                    .unwrap_or_else(|| format!("OK (hash: 0x{:08X})", report.hash));
                Ok(Output {
                    columns: vec![
                        "Table".into(),
                        "Op".into(),
                        "Msg_type".into(),
                        "Msg_text".into(),
                    ],
                    rows: vec![vec![
                        Datum::Text(report.table),
                        Datum::Text(report.op),
                        Datum::Text(report.status),
                        Datum::Text(msg_text),
                    ]],
                    message: "OK".into(),
                })
            }
            Statement::Explain { analyze, statement } => {
                self.exec_explain(session, analyze, &statement)
            }
            Statement::ExplainMemo { statement } => {
                self.exec_explain_memo(session, &statement)
            }
            Statement::CreateTable { name, columns, foreign_keys } => {
                self.exec_create_table(session, name, columns, foreign_keys)
            }
            Statement::DropTable { name } => self.exec_drop_table(session, &name),
            Statement::Insert { table, rows } => self.exec_insert(session, &table, rows),
            Statement::Select {
                items,
                from,
                joins,
                selection,
                order_by,
                limit,
                group_by,
            } => self.exec_select(session, items, &from, joins, selection, order_by, limit, group_by),
            Statement::Update {
                table,
                assignments,
                selection,
            } => self.exec_update(session, &table, assignments, selection),
            Statement::Delete { table, selection } => {
                self.exec_delete(session, &table, selection)
            }
            Statement::CreateIndex { name, table, column } => {
                self.exec_create_index(session, name, table, column)
            }
            Statement::DropIndex { name, table } => {
                self.exec_drop_index(session, name, table)
            }
        }
    }

    pub(crate) fn resolve_table_key(&self, session: &Session, table_name: &str) -> String {
        if table_name.contains('.') {
            table_name.to_string()
        } else {
            let qual = format!("{}.{table_name}", session.current_db);
            let guard = self.tables.read().unwrap();
            if guard.contains_key(&qual) {
                qual
            } else if guard.contains_key(table_name) {
                table_name.to_string()
            } else {
                qual
            }
        }
    }

    pub(crate) fn table(&self, session: &Session, name: &str) -> Result<Arc<Table>> {
        let key = self.resolve_table_key(session, name);
        self.tables
            .read()
            .unwrap()
            .get(&key)
            .cloned()
            .ok_or_else(|| Error::TableNotFound(name.to_string()))
    }

    // -- DDL (autocommit): see ddl.rs ------------------------------------

    // ------------------------------------------------------------------
    // Visibility + commit plumbing
    // ------------------------------------------------------------------

    /// Append `records` (a single self-contained txn: records + Commit) and
    /// run it through the install sequencer so WAL offsets stay contiguous
    /// from the sequencer's point of view. Used by DDL, whose "install" is
    /// the catalog update that already happened.
    fn wal_commit(&self, records: Vec<Record>) -> Result<()> {
        let (start, end, n) = {
            let _guard = self.acquire_commit_lock();
            // DDL builds its Commit without a timestamp; stamp it here so
            // every durable commit carries PITR time (ordering follows the
            // commit lock).
            let now = crate::wal::unix_now();
            let stamped: Vec<Record> = records
                .into_iter()
                .map(|r| match r {
                    Record::Commit { txn, ts: None } => Record::Commit { txn, ts: Some(now) },
                    other => other,
                })
                .collect();
            let n = stamped.len();
            let (start, end) = self.wal.append_records(&stamped)?;
            (start, end, n)
        };
        self.metrics
            .record_wal(n, end.saturating_sub(start));
        self.wal.wait_durable(end)?;
        let mut frontier = self.install.lock().unwrap();
        while *frontier != start {
            frontier = self.install_cv.wait(frontier).unwrap();
        }
        *frontier = end;
        drop(frontier);
        self.install_cv.notify_all();
        Ok(())
    }

    /// Commit the staged set directly (autocommit) or merge into the active
    /// transaction's write set.
    pub(super) fn commit_staged_or_stage(
        &self,
        session: &mut Session,
        _table: &str,
        staged: HashMap<(String, Vec<u8>), StagedWrite>,
    ) -> Result<()> {
        match &mut session.txn {
            Some(txn) => {
                txn.staged.extend(staged);
                Ok(())
            }
            None => {
                let txn_id = self.next_txn.fetch_add(1, Ordering::Relaxed);
                self.commit_txn(txn_id, staged)
            }
        }
    }

    /// Commit in three phases:
    ///   A. validate + append to WAL (short lock, no fsync — concurrent
    ///      commits keep appending while others are syncing)
    ///   B. wait for durability (the WAL syncer batches our fsync with every
    ///      other concurrently pending commit — group commit)
    ///   C. install into the trees strictly in WAL-offset order, so
    ///      in-memory state always matches replayed state.
    fn commit_txn(
        &self,
        txn_id: u64,
        staged: HashMap<(String, Vec<u8>), StagedWrite>,
    ) -> Result<()> {
        if staged.is_empty() {
            return Ok(());
        }

        let _committer = self.wal.enter_commit();

        let mut tables: HashMap<String, Arc<Table>> = HashMap::new();
        for (t, _k) in staged.keys() {
            if !tables.contains_key(t) {
                let table_arc = self.tables.read().unwrap().get(t).cloned().ok_or_else(|| Error::TableNotFound(t.clone()))?;
                tables.insert(t.clone(), table_arc);
            }
        }

        let mut records = Vec::with_capacity(staged.len() + 1);
        let mut encoded_rows = Vec::with_capacity(staged.len());
        for ((table, key), w) in &staged {
            match &w.row {
                Some(row) => {
                    let enc = Table::encode_row(row);
                    if enc.len() > MAX_VALUE_LEN {
                        return Err(Error::NotSupported("row too large".into()));
                    }
                    records.push(Record::Put {
                        txn: txn_id,
                        table: table.clone(),
                        key: key.clone(),
                        row: enc.clone(),
                    });
                    encoded_rows.push(Some(enc));
                }
                None => {
                    records.push(Record::Delete {
                        txn: txn_id,
                        table: table.clone(),
                        key: key.clone(),
                    });
                    encoded_rows.push(None);
                }
            }
        }
        records.push(Record::Commit { txn: txn_id, ts: Some(crate::wal::unix_now()) });

        let has_inserts = staged.values().any(|w| w.is_insert);

        crate::failpoint!("before_wal_reserve");
        // Phase A: validation + append under the short commit_lock. The
        // MVCC commit epoch is allocated here so epochs follow WAL order.
        let (start, end, commit_epoch) = {
            let _guard = self.acquire_commit_lock();
            crate::failpoint!("after_wal_reserve");
            // Duplicate-key check covers both installed state and commits
            // that are appended-but-not-yet-installed (in flight).
            if has_inserts {
                let in_flight = self.in_flight.lock().unwrap();
                for ((table, key), w) in &staged {
                    if w.is_insert {
                        let t = &tables[table];
                        if t.tree().get(key).is_some()
                            || in_flight.contains(&(table.clone(), key.clone()))
                        {
                            let pk = decode_key(key)?;
                            return Err(Error::DuplicateKey(pk.to_string()));
                        }
                    }
                }
            }
            let commit_epoch = self.alloc_commit_epoch();
            crate::failpoint!("before_wal_write");
            let offsets = self.wal.append_records(&records)?;
            if has_inserts {
                let mut in_flight = self.in_flight.lock().unwrap();
                for ((table, key), w) in &staged {
                    if w.is_insert {
                        in_flight.insert((table.clone(), key.clone()));
                    }
                }
            }
            (offsets.0, offsets.1, commit_epoch)
        };
        self.metrics
            .record_wal(records.len(), end.saturating_sub(start));

        // Phase B: group commit — one fsync by the syncer covers us plus
        // every other commit appended while the fsync was running.
        crate::failpoint!("before_wal_sync");
        self.wal.wait_durable(end)?;
        crate::failpoint!("after_wal_sync");

        // Phase C: install in WAL order.
        crate::failpoint!("before_install");
        {
            let mut frontier = self.install.lock().unwrap();
            while *frontier != start {
                frontier = self.install_cv.wait(frontier).unwrap();
            }
            self.record_commit_batch(&tables, &staged, &encoded_rows, commit_epoch)?;
            let mut installed_rows = 0;
            for (((table, key), _), enc_opt) in staged.iter().zip(encoded_rows.into_iter()) {
                let t = &tables[table];
                match enc_opt {
                    Some(enc) => t.apply_raw(key, &enc)?,
                    None => t.remove_raw(key),
                }
                installed_rows += 1;
                if installed_rows == 1 && staged.len() > 1 {
                    crate::failpoint!("during_multirow_install");
                }
            }
            self.visible_epoch.store(commit_epoch, Ordering::SeqCst);
            *frontier = end;
            drop(frontier);
            self.install_cv.notify_all();
            if has_inserts {
                let mut in_flight = self.in_flight.lock().unwrap();
                for (table, key) in staged.keys() {
                    in_flight.remove(&(table.clone(), key.clone()));
                }
            }
        }
        Ok(())
    }

    /// Point read honoring read-your-own-writes through the staged overlay.
    pub(super) fn visible_row(
        &self,
        session: &Session,
        table: &Arc<Table>,
        key: &[u8],
    ) -> Result<Option<Vec<Datum>>> {
        if let Some(txn) = &session.txn {
            if let Some(w) = txn.staged.get(&(table.def.name.clone(), key.to_vec())) {
                return Ok(w.row.clone());
            }
        }
        let raw = table.tree().get(key);
        let current = match raw {
            Some(buf) => Some(table.decode_stored(&buf)?),
            None => None,
        };
        // Snapshot readers time-travel; everyone else sees current state.
        self.snapshot_lookup(session, table, key, current)
    }

    /// All rows of a table visible to the session: committed tree state,
    /// filtered, with the transaction's staged writes overlaid.
    /// `pub(super)` so the batch executor (`db/batch.rs`) can source
    /// unfiltered rows (passing `None`) and apply its own vectorized
    /// filter + pushdown while sharing access paths, snapshots, overlays,
    /// and row order exactly.
    pub(super) fn visible_rows(
        &self,
        session: &Session,
        table: &Arc<Table>,
        selection: Option<&Expr>,
    ) -> Result<Vec<Vec<Datum>>> {
        let deadline = session.max_execution_time.map(|t| std::time::Instant::now() + t);
        if let Some(dl) = deadline {
            if std::time::Instant::now() > dl {
                return Err(Error::QueryTimeout);
            }
        }
        let schema = table.schema();
        let overlay: Option<Vec<(Vec<u8>, &StagedWrite)>> = session.txn.as_ref().map(|t| {
            t.staged
                .iter()
                .filter(|((tbl, _), _)| tbl == &table.def.name)
                .map(|((_, k), w)| (k.clone(), w))
                .collect()
        });

        let mut rows: Vec<(Vec<u8>, Vec<Datum>)> = Vec::new();
        // Point/secondary paths resolve per key through `visible_row`
        // (snapshot-aware); range/full scans below read committed rows and
        // are substituted afterwards. IN-list order is preserved (no
        // re-sorting) on the multi-point paths.
        //
        // The access path is cost-based (`db/cost.rs`): the heuristic
        // candidate is kept for PK routes and downgraded to a full scan
        // when random secondary dereferencing costs more than scanning.
        let choice = crate::db::cost::choose_access_path(table, selection)?;
        let ordered_points = matches!(
            choice.path,
            AccessPath::PkIn(_) | AccessPath::SecIn { .. }
        );
        match choice.path {
            AccessPath::Point(lit) => {
                let key = encode_key(&lit)?;
                if let Some(r) = self.visible_row(session, table, &key)? {
                    rows.push((key, r));
                }
            }
            AccessPath::PkIn(lits) => {
                // Multi-point seek in IN-list order; the full predicate
                // re-filter below keeps rows not matching other clauses out.
                for lit in lits {
                    let key = encode_key(&lit)?;
                    if let Some(r) = self.visible_row(session, table, &key)? {
                        rows.push((key, r));
                    }
                }
            }
            AccessPath::Range { lo, hi } => {
                let scanned = table.scan_range(
                    lo.as_ref().map(|(d, i)| (d, *i)),
                    hi.as_ref().map(|(d, i)| (d, *i)),
                )?;
                for (_, r) in scanned {
                    let key = encode_key(&r[schema.pk_idx])?;
                    rows.push((key, r));
                }
            }
            AccessPath::SecondaryIndex { col_idx, lo, hi } => {
                if let Some(pks) = table.scan_secondary(
                    col_idx,
                    lo.as_ref().map(|(d, i)| (d, *i)),
                    hi.as_ref().map(|(d, i)| (d, *i)),
                )? {
                    for pk in pks {
                        let key = encode_key(&pk)?;
                        if let Some(r) = self.visible_row(session, table, &key)? {
                            rows.push((key, r));
                        }
                    }
                }
            }
            AccessPath::SecIn { col_idx, values } => {
                // Secondary multi-point seek: one point probe per value,
                // then read-your-own-writes per primary key. The `seen` set
                // is belt-and-braces against index anomalies.
                let mut seen: HashSet<Vec<u8>> = HashSet::new();
                for v in values {
                    if let Some(pks) = table.scan_secondary(
                        col_idx,
                        Some((&v, true)),
                        Some((&v, true)),
                    )? {
                        for pk in pks {
                            let key = encode_key(&pk)?;
                            if seen.insert(key.clone()) {
                                if let Some(r) = self.visible_row(session, table, &key)? {
                                    rows.push((key, r));
                                }
                            }
                        }
                    }
                }
            }
            AccessPath::FullScan => {
                let mut check_counter = 0usize;
                for (k, raw) in table.tree().scan_all() {
                    check_counter += 1;
                    if check_counter % 256 == 0 {
                        if let Some(dl) = deadline {
                            if std::time::Instant::now() > dl {
                                return Err(Error::QueryTimeout);
                            }
                        }
                    }
                    rows.push((k, table.decode_stored(&raw)?));
                }
            }
        }

        // Snapshot substitution for scan paths (point paths already went
        // through `visible_row`): rows created after the pin vanish, rows
        // deleted after it are restored from history.
        if session.snapshot.is_some() {
            let mut present: HashSet<Vec<u8>> = HashSet::new();
            let mut kept: Vec<(Vec<u8>, Vec<Datum>)> = Vec::with_capacity(rows.len());
            for (k, r) in rows {
                if let Some(hist) = self.snapshot_lookup(session, table, &k, Some(r))? {
                    present.insert(k.clone());
                    kept.push((k, hist));
                }
            }
            rows = kept;
            let extra = self.snapshot_scan_extra(session, table, &present)?;
            if !extra.is_empty() {
                rows.extend(extra);
                if !ordered_points {
                    rows.sort_by(|a, b| a.0.cmp(&b.0));
                }
            }
        }

        // Overlay staged writes (update-in-place by encoded key; deletes
        // remove; inserts land in key order).
        if let Some(ov) = overlay {
            for (key, w) in ov {
                match &w.row {
                    Some(row) => {
                        if let Some(slot) = rows.iter_mut().find(|(k, _)| *k == key) {
                            slot.1 = row.clone();
                        } else {
                            rows.push((key.clone(), row.clone()));
                            rows.sort_by(|a, b| a.0.cmp(&b.0));
                        }
                    }
                    None => rows.retain(|(k, _)| *k != key),
                }
            }
        }

        // Filter with the full predicate (index predicates re-evaluated —
        // correct and simple; the executor fast path avoids a re-scan).
        // Resolution falls back outward across correlation frames, so
        // subquery row environments evaluate here as well as in the
        // dedicated fold paths (empty frames = plain scope behavior).
        if let Some(sel) = selection {
            let mut out = Vec::new();
            for (_, r) in rows {
                // Unknown columns filter the row out (legacy behavior);
                // every other evaluation error propagates.
                match crate::sql::eval_with(sel, &mut |name| {
                    match schema.index_of(name) {
                        Some(idx) => Ok(r[idx].clone()),
                        None => {
                            for frame in session.subq.outer.iter().rev() {
                                match Self::resolve_scope(&frame.tables, name) {
                                    Ok(pos) => return Ok(frame.row[pos].clone()),
                                    Err(Error::ColumnNotFound(_)) | Err(Error::TableNotFound(_)) => {
                                        continue
                                    }
                                    Err(e) => return Err(e),
                                }
                            }
                            Err(Error::ColumnNotFound(name.into()))
                        }
                    }
                }) {
                    Ok(true) => out.push(r),
                    Ok(false) => {}
                    Err(Error::ColumnNotFound(_)) => {}
                    Err(e) => return Err(e),
                }
            }
            Ok(out)
        } else {
            Ok(rows.into_iter().map(|(_, r)| r).collect())
        }
    }
}

// ---------------------------------------------------------------------------
// Recovery helpers
// ---------------------------------------------------------------------------


fn txn_of(rec: &Record) -> u64 {
    match rec {
        Record::Put { txn, .. }
        | Record::Delete { txn, .. }
        | Record::CreateTable { txn, .. }
        | Record::DropTable { txn, .. }
        | Record::CreateIndex { txn, .. }
        | Record::DropIndex { txn, .. }
        | Record::CreateDatabase { txn, .. }
        | Record::DropDatabase { txn, .. }
        | Record::Commit { txn, .. } => *txn,
    }
}

fn apply_records(
    databases: &mut HashSet<String>,
    tables: &mut HashMap<String, Arc<Table>>,
    pool: &Arc<BufferPool>,
    epoch: &Arc<crate::epoch::EpochManager>,
    batch: Vec<Record>,
) -> Result<()> {
    for rec in batch {
        match rec {
            Record::CreateDatabase { name, .. } => {
                databases.insert(name);
            }
            Record::DropDatabase { name, .. } => {
                databases.remove(&name);
                let prefix = format!("{name}.");
                tables.retain(|k, _| !k.starts_with(&prefix));
            }
            Record::CreateTable { def, .. } => {
                if !tables.contains_key(&def.name) {
                    let t = Arc::new(Table::new(def));
                    t.set_pool(pool.clone());
                    t.set_epoch_manager(epoch.clone());
                    tables.insert(t.def.name.clone(), t);
                }
            }
            Record::DropTable { name, .. } => {
                tables.remove(&name);
            }
            Record::Put { table, key, row, .. } => {
                if let Some(t) = tables.get(&table) {
                    t.apply_raw(&key, &row)?;
                }
            }
            Record::Delete { table, key, .. } => {
                if let Some(t) = tables.get(&table) {
                    t.remove_raw(&key);
                }
            }
            Record::CreateIndex { table, name, column, .. } => {
                if let Some(t) = tables.get(&table) {
                    let _ = t.add_index(name, column);
                }
            }
            Record::DropIndex { table, name, .. } => {
                if let Some(t) = tables.get(&table) {
                    let _ = t.drop_index(&name);
                }
            }
            Record::Commit { .. } => {}
        }
    }
    Ok(())
}

