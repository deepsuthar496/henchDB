//! Read-only replica support: write gating plus streamed WAL/snapshot
//! application.
//!
//! A replica never commits locally. It applies the primary's committed WAL
//! batches (`apply_replica_batch`) and whole snapshots
//! (`apply_replica_snapshot`) to its live catalog, and serves reads through
//! the normal executor. All mutating statements are rejected up front in
//! `Database::execute` with `Error::ReadOnlyReplica` (MySQL 1290).
//!
//! Consistency notes (v1):
//! - A WAL batch applies record-by-record under the catalog locks, so a
//!   concurrent reader can observe a multi-record transaction partially.
//!   Batches converge to the primary's committed state; per-record installs
//!   are individually atomic via the OLC trees.
//! - Snapshot-apply replaces the table set wholesale; queries racing the
//!   swap see either side, never torn rows (tables swap as whole `Arc`s).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::Database;
use crate::error::{Error, Result};
use crate::table::{Table, TableDef};
use crate::wal::Record;

impl Database {
    /// Promote a read-only replica to an authoritative read-write primary.
    ///
    /// Quiesce protocol (Priority 12):
    /// 1. Fail fast when already primary (`InvalidOperation`).
    /// 2. `checkpoint()` makes every applied record durable in the
    ///    snapshot (the feeder's `pending` map only ever holds
    ///    *uncommitted* records, which crash semantics discard anyway)
    ///    and resets the log: generation + 1 (`wal.gen`), offset back to
    ///    the header. Old primaries and divergent peers can never silently
    ///    stream into this history (generation fencing; the replica
    ///    handshake snapshots on any mismatch, and the replica refuses
    ///    snapshots from older generations).
    /// 3. The `read_only` gate lifts last, so no local commit can interleave
    ///    with the fence. The server feeder thread observes the flip and
    ///    detaches without applying further upstream batches.
    /// The checkpoint doubles as the promotion milestone record (durable
    /// point + new generation are both in files afterwards).
    pub fn promote(&self) -> Result<()> {
        use std::sync::atomic::Ordering;
        if !self.read_only.load(Ordering::Acquire) {
            return Err(Error::InvalidOperation("already primary".into()));
        }
        self.checkpoint()?;
        *self.replica_upstream.lock().unwrap() = String::new();
        self.read_only.store(false, Ordering::Release);
        self.metrics.set_repl_status("PROMOTED");
        Ok(())
    }

    /// Offline variant for `server promote --dir` (stopped node): the
    /// read-only flag is in-memory only, so a freshly opened directory
    /// cannot prove it was a replica — fence unconditionally instead of
    /// failing. Never errors on role, only on I/O. (The checkpoint inside
    /// already resets the log: generation + 1, offset to header.)
    pub fn promote_offline(&self) -> Result<()> {
        use std::sync::atomic::Ordering;
        self.checkpoint()?;
        *self.replica_upstream.lock().unwrap() = String::new();
        self.read_only.store(false, Ordering::Release);
        Ok(())
    }

    /// Upstream primary this replica follows (`host:port`, empty when
    /// none). Set by the server replication client at startup; cleared by
    /// promotion. Surfaced as `Replica_Upstream_Host` in `SHOW STATUS`.
    pub fn set_replica_upstream(&self, upstream: &str) {
        *self.replica_upstream.lock().unwrap() = upstream.to_string();
    }

    pub fn replica_upstream(&self) -> String {
        self.replica_upstream.lock().unwrap().clone()
    }

    /// "Primary" when writable, "Replica" when gated (for status rows).
    pub fn replica_role(&self) -> &'static str {
        use std::sync::atomic::Ordering;
        if self.read_only.load(Ordering::Relaxed) {
            "Replica"
        } else {
            "Primary"
        }
    }
}

/// First-keyword gate for replica mode. Session-local reads and transaction
/// framing stay allowed (an empty staged txn commits to nothing); anything
/// that would append to the WAL or mutate catalog/files is rejected.
pub(crate) fn is_write_statement(sql: &str) -> bool {
    let verb: String = sql
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .trim_end_matches([';', '('])
        .to_ascii_uppercase();
    matches!(
        verb.as_str(),
        "INSERT"
            | "UPDATE"
            | "DELETE"
            | "CREATE"
            | "DROP"
            | "ALTER"
            | "GRANT"
            | "REVOKE"
            | "TRUNCATE"
            | "BACKUP"
            | "CHECKPOINT"
            | "ANALYZE"
            | "ARCHIVE"
    )
}

impl Database {
    /// Enable/disable replica read-only mode (set once at startup before
    /// serving; the replica thread flips nothing at runtime).
    pub fn set_read_only(&self, ro: bool) {
        self.read_only
            .store(ro, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn is_read_only(&self) -> bool {
        self.read_only.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Primary log head (next append offset) for the replication feeder.
    pub fn wal_head(&self) -> u64 {
        self.wal.next_offset()
    }

    /// Durable prefix end: replication streams only up to here.
    pub fn wal_durable(&self) -> u64 {
        self.wal.durable_offset()
    }

    /// Current log generation (bumped per checkpoint reset).
    pub fn wal_generation(&self) -> u64 {
        self.wal.generation()
    }

    /// Wait for durable progress past `offset` (feeder wake-up).
    pub fn wait_wal_durable(&self, offset: u64, timeout: std::time::Duration) -> u64 {
        self.wal.wait_durable_change(offset, timeout)
    }

    /// Read raw log bytes for one replication chunk.
    pub fn read_wal_range(&self, from: u64, max_len: usize) -> Result<(Vec<u8>, u64)> {
        self.wal.read_range(from, max_len)
    }

    /// Apply one committed transaction's records from the primary's WAL.
    /// Idempotent for redos: Puts overwrite, Deletes remove, DDL guards on
    /// existence like the open-time replay path.
    /// Persists the batch to local WAL so offline promotion recovers all streamed data.
    pub fn apply_replica_batch(&self, batch: Vec<Record>) -> Result<()> {
        crate::failpoint!("during_replica_apply");
        let has_commit = batch.iter().any(|r| matches!(r, Record::Commit { .. }));
        let mut wal_batch = batch.clone();
        if !has_commit {
            if let Some(last) = batch.last() {
                let txn = match last {
                    Record::CreateDatabase { txn, .. }
                    | Record::DropDatabase { txn, .. }
                    | Record::CreateTable { txn, .. }
                    | Record::DropTable { txn, .. }
                    | Record::Put { txn, .. }
                    | Record::Delete { txn, .. }
                    | Record::CreateIndex { txn, .. }
                    | Record::DropIndex { txn, .. }
                    | Record::Commit { txn, .. } => *txn,
                };
                wal_batch.push(Record::Commit { txn, ts: None });
            }
        }
        {
            let mut dbs = self.databases.write().unwrap();
            let mut tables = self.tables.write().unwrap();
            for rec in batch {
                match rec {
                    Record::CreateDatabase { name, .. } => {
                        dbs.insert(name);
                    }
                    Record::DropDatabase { name, .. } => {
                        dbs.remove(&name);
                        let prefix = format!("{name}.");
                        tables.retain(|k, _| !k.starts_with(&prefix));
                    }
                    Record::CreateTable { def, .. } => {
                        if !tables.contains_key(&def.name) {
                            let t = Arc::new(Table::new(def));
                            t.set_pool(self.pool.clone());
                            t.set_epoch_manager(self.epoch.clone());
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
        }
        if !wal_batch.is_empty() {
            self.wal_commit(wal_batch)?;
        }
        Ok(())
    }

    /// Replace the whole catalog with a primary snapshot (databases, table
    /// defs + rows, pool image). Persists `snapshot.bin` via checkpoint so a
    /// replica restart resumes from files matching `end_offset`.
    pub fn apply_replica_snapshot(
        &self,
        databases: Vec<String>,
        tables: Vec<(TableDef, Vec<(Vec<u8>, Vec<u8>)>)>,
        pages: Vec<u8>,
    ) -> Result<()> {
        // 1. Stage the new table set off to the side (no locks held while
        //    decoding rows into trees).
        let mut staged: HashMap<String, Arc<Table>> = HashMap::new();
        let mut staged_dbs: HashSet<String> = HashSet::new();
        for db_name in databases {
            staged_dbs.insert(db_name);
        }
        if staged_dbs.is_empty() {
            staged_dbs.insert("default".to_string());
        }
        for (def, rows) in tables {
            if let Some((db_prefix, _)) = def.name.split_once('.') {
                staged_dbs.insert(db_prefix.to_string());
            }
            let table = Arc::new(Table::new(def));
            table.set_pool(self.pool.clone());
            table.set_epoch_manager(self.epoch.clone());
            for (key, val) in &rows {
                table.restore_kv(key, val)?;
            }
            staged.insert(table.def.name.clone(), table);
        }
        for table in staged.values() {
            table.refresh_auto_inc()?;
        }
        Self::fk_ensure_all_auto_indexes(&staged)?;
        // 2. Swap the pool image first (locators in the staged rows point
        //    at it), then the catalog, then drop MVCC history whose epochs
        //    are meaningless against the new table set.
        std::fs::write(self.dir.join("pages.bin"), &pages)?;
        self.pool.invalidate()?;
        {
            let mut dbs = self.databases.write().unwrap();
            let mut tbls = self.tables.write().unwrap();
            *dbs = staged_dbs;
            *tbls = staged;
        }
        self.versions.write().unwrap().clear();
        // 3. Persist snapshot.bin from the live state so restarts resume
        //    from files (local WAL stays empty; offsets live server-side).
        self.checkpoint()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;

    #[test]
    fn write_gate_classifies() {
        for s in [
            "INSERT INTO t VALUES (1)",
            "update t set a=1",
            "DELETE FROM t",
            "CREATE TABLE t (a INT PRIMARY KEY)",
            "drop table t",
            "ALTER TABLE t ADD COLUMN b INT",
            "TRUNCATE t",
            "BACKUP DATABASE TO 'x'",
            "CHECKPOINT",
            "ANALYZE TABLE t",
        ] {
            assert!(is_write_statement(s), "should gate: {s}");
        }
        for s in [
            "SELECT * FROM t",
            "SHOW STATUS",
            "SHOW PROCESSLIST",
            "EXPLAIN SELECT 1",
            "BEGIN",
            "COMMIT",
            "ROLLBACK",
            "USE foo",
            "SET max_execution_time = 1",
        ] {
            assert!(!is_write_statement(s), "should allow: {s}");
        }
    }

    #[test]
    fn replica_rejects_writes_end_to_end() {
        let dir = std::env::temp_dir().join(format!("hdbreadonly_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let db = Database::open(&dir).unwrap();
        let mut s = db.new_session();
        db.execute(&mut s, "CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
        db.execute(&mut s, "INSERT INTO t VALUES (1, 1)").unwrap();
        db.set_read_only(true);
        assert!(db.is_read_only());
        for s in [
            "INSERT INTO t VALUES (2, 2)",
            "UPDATE t SET v = 9 WHERE id = 1",
            "DELETE FROM t",
            "CREATE TABLE u (id INT PRIMARY KEY)",
            "DROP TABLE t",
            "BACKUP DATABASE TO '/tmp/x'",
            "CHECKPOINT",
            "ANALYZE TABLE t",
        ] {
            assert_eq!(
                db.execute(&mut db.new_session(), s),
                Err(Error::ReadOnlyReplica),
                "{s}"
            );
        }
        // Reads still work, including the pre-existing row.
        let out = db.execute(&mut db.new_session(), "SELECT * FROM t").unwrap();
        assert_eq!(out.rows.len(), 1);
        let out = db.execute(&mut db.new_session(), "SHOW STATUS").unwrap();
        assert!(!out.rows.is_empty());
        db.set_read_only(false);
        db.execute(&mut db.new_session(), "INSERT INTO t VALUES (2, 2)").unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn replica_batch_applies_idempotently() {
        let dir = std::env::temp_dir().join(format!("hdbreplbatch_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let db = Database::open(&dir).unwrap();
        let def = crate::table::TableDef {
            name: "default.t".into(),
            schema: crate::table::Schema {
                columns: vec![crate::table::ColumnDef {
                    name: "id".into(),
                    ctype: crate::types::ColumnType::Int,
                    nullable: false,
                    auto_increment: false,
                    default_value: None,
                }],
                pk_idx: 0,
            },
            indexes: vec![],
            foreign_keys: vec![],
            stats: None,
        };
        let key = crate::types::encode_key(&crate::types::Datum::Int(1)).unwrap();
        let batch = vec![
            Record::CreateTable { txn: 7, def },
            Record::Put {
                txn: 7,
                table: "default.t".into(),
                key: key.clone(),
                row: crate::table::Table::encode_row(&[crate::types::Datum::Int(1)]),
            },
            Record::Commit { txn: 7, ts: None },
        ];
        db.apply_replica_batch(batch.clone()).unwrap();
        // Redo is idempotent.
        db.apply_replica_batch(batch).unwrap();
        let mut s = db.new_session();
        let out = db.execute(&mut s, "SELECT * FROM t").unwrap();
        assert_eq!(out.rows.len(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn status_value(db: &Database, name: &str) -> Option<String> {
        let out = db.execute(&mut db.new_session(), "SHOW STATUS").unwrap();
        out.rows.iter().find_map(|r| match &r[..] {
            [crate::types::Datum::Text(n), crate::types::Datum::Text(v)] if n == name => {
                Some(v.clone())
            }
            _ => None,
        })
    }

    #[test]
    fn promote_sql_flips_replica_to_primary() {
        let dir = std::env::temp_dir().join(format!("hdbpromote_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let db = Database::open(&dir).unwrap();
        let mut s = db.new_session();
        db.execute(&mut s, "CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
        db.execute(&mut s, "INSERT INTO t VALUES (1, 1)").unwrap();
        db.set_read_only(true);
        db.set_replica_upstream("127.0.0.1:9999");
        let gen_before = db.wal_generation();
        assert_eq!(status_value(&db, "Replica_Role").as_deref(), Some("Replica"));
        assert_eq!(
            status_value(&db, "Replica_Upstream_Host").as_deref(),
            Some("127.0.0.1:9999")
        );
        // PROMOTE passes the read-only gate (not a write verb).
        let out = db.execute(&mut db.new_session(), "PROMOTE").unwrap();
        assert!(out.message.contains("promoted to primary"), "{}", out.message);
        assert!(!db.is_read_only());
        assert_eq!(db.wal_generation(), gen_before + 1);
        assert_eq!(status_value(&db, "Replica_Role").as_deref(), Some("Primary"));
        assert_eq!(status_value(&db, "Replica_Upstream_Host").as_deref(), Some(""));
        // Writes work immediately and persist across reopen.
        db.execute(&mut db.new_session(), "INSERT INTO t VALUES (2, 2)").unwrap();
        drop(db);
        let db2 = Database::open(&dir).unwrap();
        assert!(!db2.is_read_only());
        let out = db2.execute(&mut db2.new_session(), "SELECT COUNT(*) FROM t").unwrap();
        assert_eq!(out.rows[0][0], crate::types::Datum::Int(2));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn promote_on_primary_is_invalid_operation() {
        let dir = std::env::temp_dir().join(format!("hdbpromote2_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let db = Database::open(&dir).unwrap();
        let gen_before = db.wal_generation();
        assert_eq!(
            db.execute(&mut db.new_session(), "PROMOTE"),
            Err(Error::InvalidOperation("already primary".into()))
        );
        // Failed promotion fences nothing.
        assert_eq!(db.wal_generation(), gen_before);
        // Offline promotion never fails on role (fresh opens cannot prove
        // replica history) but still fences + ensures read-write.
        db.promote_offline().unwrap();
        assert_eq!(db.wal_generation(), gen_before + 1);
        assert!(!db.is_read_only());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
