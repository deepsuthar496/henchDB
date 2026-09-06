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
use crate::error::Result;
use crate::table::{Table, TableDef};
use crate::wal::Record;

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
            | "TRUNCATE"
            | "BACKUP"
            | "CHECKPOINT"
            | "ANALYZE"
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
    pub fn apply_replica_batch(&self, batch: Vec<Record>) -> Result<()> {
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
}
