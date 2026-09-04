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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use crate::catalog;
use crate::error::{Error, Result};
use crate::page::{BufferPool, MAX_VALUE_LEN};
use crate::sql::{eval_expr, parse_sql, Expr, Statement};
use crate::table::{Schema, Table, TableDef};
use crate::types::{decode_key, encode_key, ColumnType, Datum};
use crate::wal::{Record, Wal};

use plan::{access_path, AccessPath};

pub(crate) mod plan;
pub(crate) mod query;

#[cfg(test)]
mod tests;

#[derive(Debug, Clone)]
pub struct Output {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Datum>>,
    pub message: String,
}

impl Output {
    fn ok(msg: impl Into<String>) -> Output {
        Output {
            columns: vec![],
            rows: vec![],
            message: msg.into(),
        }
    }
}

/// A client session: at most one active transaction.
#[derive(Default)]
pub struct Session {
    txn: Option<ActiveTxn>,
}

struct ActiveTxn {
    id: u64,
    /// Staged write set: (table, encoded pk) -> write. `row: None` = delete.
    staged: HashMap<(String, Vec<u8>), StagedWrite>,
}

#[derive(Clone)]
struct StagedWrite {
    row: Option<Vec<Datum>>,
    is_insert: bool,
}

pub struct Database {
    tables: RwLock<HashMap<String, Arc<Table>>>,
    wal: Wal,
    dir: PathBuf,
    /// Off-page overflow pool for wide rows (Priority 2). Shared by all
    /// tables; the `pages.bin` file persists next to WAL/snapshot so
    /// snapshot locators stay valid across restarts.
    pool: Arc<BufferPool>,
    /// Phase A of commit: validate + append (short critical section, no fsync).
    commit_lock: Mutex<()>,
    /// Phase C: installs happen strictly in WAL-offset order so in-memory
    /// state always matches replayed state. Guarded by `install_cv`.
    install: Mutex<u64>,
    install_cv: std::sync::Condvar,
    /// Keys of commits appended but not yet installed (duplicate-key guard
    /// for concurrent inserts while the commit lock is released for sync).
    in_flight: Mutex<HashSet<(String, Vec<u8>)>>,
    next_txn: AtomicU64,
    epoch: Arc<crate::epoch::EpochManager>,
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
        let mut tables: HashMap<String, Arc<Table>> = HashMap::new();

        let snap = dir.join("snapshot.bin");
        if snap.exists() {
            let mut f = File::open(&snap)?;
            for (def, rows) in catalog::decode_snapshot(&mut f)? {
                let table = Arc::new(Table::new(def));
                table.set_pool(pool.clone());
                for row in rows {
                    match row.key {
                        // v2: explicit key + stored value (may be a locator).
                        Some(key) => table.restore_kv(&key, &row.value)?,
                        // v1: inline values only, key re-derived.
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
                Record::Commit { txn } => {
                    if let Some(batch) = pending.remove(&txn) {
                        apply_records(&mut tables, &pool, batch)?;
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

        let install_frontier = wal.next_offset();
        Ok(Database {
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
        })
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

    /// Flush a durable snapshot and truncate the WAL.
    pub fn checkpoint(&self) -> Result<()> {
        let _guard = self.commit_lock.lock().unwrap();
        // The snapshot references overflow pages, so the pool must be durable
        // first; WAL replay (full rows) remains the backstop regardless.
        self.pool.sync_data()?;
        let tables = self.tables.read().unwrap();
        let mut data = Vec::new();
        for table in tables.values() {
            let rows = table.tree().scan_all();
            data.push((table.table_def(), rows));
        }
        drop(tables);
        let tmp = self.dir.join("snapshot.bin.tmp");
        {
            let f = File::create(&tmp)?;
            let mut bw = std::io::BufWriter::with_capacity(128 * 1024, f);
            catalog::encode_snapshot(&mut bw, &data)?;
            bw.flush()?;
            bw.into_inner().map_err(|e| Error::Io(e.to_string()))?.sync_data()?;
        }
        fs::rename(&tmp, self.dir.join("snapshot.bin"))?;
        self.wal.reset()?;
        // Re-base the install frontier: offsets restart after the truncate.
        let frontier = self.wal.next_offset();
        *self.install.lock().unwrap() = frontier;
        Ok(())
    }

    // ------------------------------------------------------------------
    // Execution
    // ------------------------------------------------------------------

    pub fn execute(&self, session: &mut Session, sql: &str) -> Result<Output> {
        let _guard = self.epoch.pin();
        let trimmed = sql.trim();
        if let Some(out) = self.try_fast_point_update(session, trimmed)? {
            return Ok(out);
        }
        let stmt = parse_sql(trimmed)?;
        self.execute_stmt(session, stmt)
    }

    fn execute_stmt(&self, session: &mut Session, stmt: Statement) -> Result<Output> {
        match stmt {
            Statement::Begin => {
                if session.txn.is_some() {
                    return Err(Error::TxnConflict("transaction already active".into()));
                }
                let id = self.next_txn.fetch_add(1, Ordering::Relaxed);
                session.txn = Some(ActiveTxn {
                    id,
                    staged: HashMap::new(),
                });
                Ok(Output::ok("BEGIN"))
            }
            Statement::Commit => {
                let txn = session.txn.take().ok_or(Error::TxnNotActive)?;
                self.commit_txn(txn.id, txn.staged)?;
                Ok(Output::ok("COMMIT"))
            }
            Statement::Rollback => {
                session.txn.take().ok_or(Error::TxnNotActive)?;
                Ok(Output::ok("ROLLBACK"))
            }
            Statement::ShowTables => {
                let tables = self.tables.read().unwrap();
                let mut names: Vec<String> = tables.keys().cloned().collect();
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
            Statement::CreateTable { name, columns } => self.exec_create_table(name, columns),
            Statement::DropTable { name } => self.exec_drop_table(&name),
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
                self.exec_create_index(name, table, column)
            }
            Statement::DropIndex { name, table } => {
                self.exec_drop_index(name, table)
            }
        }
    }

    fn table(&self, name: &str) -> Result<Arc<Table>> {
        self.tables
            .read()
            .unwrap()
            .get(name)
            .cloned()
            .ok_or_else(|| Error::TableNotFound(name.to_string()))
    }

    // -- DDL (autocommit) ------------------------------------------------

    fn exec_create_table(&self, name: String, columns: Vec<crate::sql::ColumnSpec>) -> Result<Output> {
        let mut pk_count = 0usize;
        let mut pk_idx = None;
        let mut auto_inc_count = 0usize;
        let mut defs = Vec::with_capacity(columns.len());
        for (i, c) in columns.into_iter().enumerate() {
            if c.primary_key {
                pk_count += 1;
                pk_idx = Some(i);
            }
            if c.auto_increment {
                auto_inc_count += 1;
            }
            let ctype = ColumnType::parse(&c.ctype)?;
            if c.auto_increment
                && !matches!(ctype, ColumnType::Int | ColumnType::BigInt)
            {
                return Err(Error::InvalidSchema(
                    "AUTO_INCREMENT requires an INT or BIGINT column".into(),
                ));
            }
            defs.push(crate::table::ColumnDef {
                name: c.name,
                ctype,
                nullable: !c.not_null,
                auto_increment: c.auto_increment,
            });
        }
        if pk_count > 1 {
            return Err(Error::MultiplePrimaryKeys);
        }
        let pk_idx = pk_idx.ok_or(Error::MissingPrimaryKey)?;
        if auto_inc_count > 1 {
            return Err(Error::InvalidSchema(
                "only one AUTO_INCREMENT column is supported".into(),
            ));
        }
        if auto_inc_count == 1 && !defs[pk_idx].auto_increment {
            return Err(Error::InvalidSchema(
                "AUTO_INCREMENT must be the primary key".into(),
            ));
        }
        let def = TableDef {
            name: name.clone(),
            schema: Schema {
                columns: defs,
                pk_idx,
            },
            indexes: Vec::new(),
        };
        let table = Arc::new(Table::new(def.clone()));
        {
            let mut guard = self.tables.write().unwrap();
            if guard.contains_key(&name) {
                return Err(Error::TableExists(name));
            }
            table.set_pool(self.pool.clone());
            guard.insert(name.clone(), table);
        }
        let txn = self.next_txn.fetch_add(1, Ordering::Relaxed);
        // DDL is a single-record transaction: the Commit marker is what
        // recovery keys on, so it must be part of the durable batch.
        self.wal_commit(vec![
            Record::CreateTable { txn, def },
            Record::Commit { txn },
        ])?;
        Ok(Output::ok(format!("table '{name}' created")))
    }

    fn exec_drop_table(&self, name: &str) -> Result<Output> {
        {
            let mut guard = self.tables.write().unwrap();
            guard.remove(name).ok_or_else(|| Error::TableNotFound(name.to_string()))?;
        }
        let txn = self.next_txn.fetch_add(1, Ordering::Relaxed);
        self.wal_commit(vec![
            Record::DropTable {
                txn,
                name: name.to_string(),
            },
            Record::Commit { txn },
        ])?;
        Ok(Output::ok(format!("table '{name}' dropped")))
    }

    fn exec_create_index(
        &self,
        name: String,
        table_name: String,
        column: String,
    ) -> Result<Output> {
        let table = self.table(&table_name)?;
        table.add_index(name.clone(), column.clone())?;
        let txn = self.next_txn.fetch_add(1, Ordering::Relaxed);
        self.wal_commit(vec![
            Record::CreateIndex {
                txn,
                table: table_name.clone(),
                name: name.clone(),
                column,
            },
            Record::Commit { txn },
        ])?;
        Ok(Output::ok(format!("index '{name}' created on '{table_name}'")))
    }

    fn exec_drop_index(&self, name: String, table_name: String) -> Result<Output> {
        let table = self.table(&table_name)?;
        table.drop_index(&name)?;
        let txn = self.next_txn.fetch_add(1, Ordering::Relaxed);
        self.wal_commit(vec![
            Record::DropIndex {
                txn,
                table: table_name.clone(),
                name: name.clone(),
            },
            Record::Commit { txn },
        ])?;
        Ok(Output::ok(format!("index '{name}' dropped from '{table_name}'")))
    }

    // -- DML ---------------------------------------------------------------

    fn exec_insert(
        &self,
        session: &mut Session,
        table: &str,
        rows: Vec<Vec<Expr>>,
    ) -> Result<Output> {
        let table_arc = self.table(table)?;
        let mut staged: HashMap<(String, Vec<u8>), StagedWrite> = HashMap::new();
        for row_exprs in rows {
            let mut row = Vec::with_capacity(row_exprs.len());
            for e in row_exprs {
                match e {
                    Expr::Literal(d) => row.push(d),
                    other => {
                        return Err(Error::NotSupported(format!(
                            "INSERT values must be literals, got {other:?}"
                        )))
                    }
                }
            }
            // Fill AUTO_INCREMENT (NULL trigger) before validation, so NULL
            // never reaches the NOT NULL check on the key column.
            table_arc.assign_auto_inc(&mut row)?;
            let row = table_arc.validate_row(row)?;
            let key = encode_key(&row[table_arc.schema().pk_idx])?;
            if staged.contains_key(&(table.to_string(), key.clone()))
                || self.visible_row(session, &table_arc, &key)?.is_some()
            {
                let pk = decode_key(&key)?;
                return Err(Error::DuplicateKey(pk.to_string()));
            }
            staged.insert(
                (table.to_string(), key),
                StagedWrite {
                    row: Some(row),
                    is_insert: true,
                },
            );
        }
        let n = staged.len();
        self.commit_staged_or_stage(session, table, staged)?;
        Ok(Output::ok(format!("{n} row(s) inserted")))
    }

    fn try_fast_point_update(&self, session: &mut Session, sql: &str) -> Result<Option<Output>> {
        if session.txn.is_some() {
            return Ok(None);
        }
        let s = sql.strip_suffix(';').unwrap_or(sql).trim();
        if s.len() < 10 {
            return Ok(None);
        }
        let (upd_prefix, rest) = s.split_at(7);
        if !upd_prefix.eq_ignore_ascii_case("UPDATE ") {
            return Ok(None);
        }
        let rest = rest.trim_start();
        let table_end = match rest.find(|c: char| c.is_whitespace()) {
            Some(i) => i,
            None => return Ok(None),
        };
        let table = &rest[..table_end];
        let rest = rest[table_end..].trim_start();

        if rest.len() < 4 {
            return Ok(None);
        }
        let (set_prefix, rest) = rest.split_at(4);
        if !set_prefix.eq_ignore_ascii_case("SET ") {
            return Ok(None);
        }
        let rest = rest.trim_start();

        let where_pos = match rest.to_ascii_lowercase().find(" where ") {
            Some(i) => i,
            None => return Ok(None),
        };
        let set_clause = rest[..where_pos].trim();
        let where_clause = rest[where_pos + 7..].trim();

        let (col, val_str) = match set_clause.split_once('=') {
            Some((c, v)) => (c.trim(), v.trim()),
            None => return Ok(None),
        };
        if val_str.contains(',') {
            return Ok(None);
        }

        let (pk_col, pk_val_str) = match where_clause.split_once('=') {
            Some((c, v)) => (c.trim(), v.trim()),
            None => return Ok(None),
        };
        if pk_val_str.contains(|c: char| c.is_whitespace()) {
            return Ok(None);
        }

        let val = match parse_simple_literal(val_str) {
            Some(d) => d,
            None => return Ok(None),
        };
        let pk_val = match parse_simple_literal(pk_val_str) {
            Some(d) => d,
            None => return Ok(None),
        };

        let table_arc = match self.table(table) {
            Ok(t) => t,
            Err(_) => return Ok(None),
        };
        let schema = table_arc.schema();

        if schema.columns[schema.pk_idx].name != pk_col {
            return Ok(None);
        }

        let col_idx = match schema.index_of(col) {
            Some(i) => i,
            None => return Ok(None),
        };

        let key = encode_key(&pk_val)?;
        let raw = match table_arc.tree().get(&key) {
            Some(r) => r,
            None => return Ok(Some(Output::ok("0 row(s) updated"))),
        };
        let mut row = table_arc.decode_stored(&raw)?;
        if row[col_idx] == val {
            return Ok(Some(Output::ok("0 row(s) updated")));
        }
        row[col_idx] = val;
        let row = table_arc.validate_row(row)?;
        let enc = Table::encode_row(&row);
        self.commit_single_update(table, &table_arc, key, enc)?;
        Ok(Some(Output::ok("1 row(s) updated")))
    }

    fn commit_single_update(
        &self,
        table_name: &str,
        table: &Arc<Table>,
        key: Vec<u8>,
        enc: Vec<u8>,
    ) -> Result<()> {
        if enc.len() > MAX_VALUE_LEN {
            return Err(Error::NotSupported("row too large".into()));
        }
        let txn_id = self.next_txn.fetch_add(1, Ordering::Relaxed);
        let _committer = self.wal.enter_commit();

        let records = [
            Record::Put {
                txn: txn_id,
                table: table_name.to_string(),
                key: key.clone(),
                row: enc.clone(),
            },
            Record::Commit { txn: txn_id },
        ];

        let (start, end) = {
            let _guard = self.commit_lock.lock().unwrap();
            self.wal.append_records(&records)?
        };

        self.wal.wait_durable(end)?;

        {
            let mut frontier = self.install.lock().unwrap();
            while *frontier != start {
                frontier = self.install_cv.wait(frontier).unwrap();
            }
            table.apply_raw(&key, &enc)?;
            *frontier = end;
            drop(frontier);
            self.install_cv.notify_all();
        }
        Ok(())
    }

    fn exec_update(
        &self,
        session: &mut Session,
        table: &str,
        assignments: Vec<(String, Expr)>,
        selection: Option<Expr>,
    ) -> Result<Output> {
        let table_arc = self.table(table)?;
        let schema = table_arc.schema();
        let mut set_idx = Vec::with_capacity(assignments.len());
        for (col, expr) in &assignments {
            let idx = schema.index_of(col).ok_or_else(|| Error::ColumnNotFound(col.clone()))?;
            match expr {
                Expr::Literal(_) => {}
                other => {
                    return Err(Error::NotSupported(format!(
                        "SET values must be literals, got {other:?}"
                    )))
                }
            }
            set_idx.push((idx, expr.clone()));
        }

        // Fast path for point update on PK when autocommit
        if session.txn.is_none() {
            if let Ok(AccessPath::Point(lit)) = access_path(&table_arc, selection.as_ref()) {
                let key = encode_key(&lit)?;
                if let Some(raw) = table_arc.tree().get(&key) {
                    let mut row = table_arc.decode_stored(&raw)?;
                    let mut changed = false;
                    for (idx, expr) in &set_idx {
                        if let Expr::Literal(d) = expr {
                            if &row[*idx] != d {
                                row[*idx] = d.clone();
                                changed = true;
                            }
                        }
                    }
                    if !changed {
                        return Ok(Output::ok("0 row(s) updated"));
                    }
                    let row = table_arc.validate_row(row)?;
                    let enc = Table::encode_row(&row);
                    self.commit_single_update(table, &table_arc, key, enc)?;
                    return Ok(Output::ok("1 row(s) updated"));
                } else {
                    return Ok(Output::ok("0 row(s) updated"));
                }
            }
        }

        let rows = self.visible_rows(session, &table_arc, selection.as_ref())?;
        let mut staged: HashMap<(String, Vec<u8>), StagedWrite> = HashMap::new();
        for row in rows {
            let mut new_row = row.clone();
            let mut changed = false;
            for (idx, expr) in &set_idx {
                if let Expr::Literal(d) = expr {
                    if &new_row[*idx] != d {
                        new_row[*idx] = d.clone();
                        changed = true;
                    }
                }
            }
            if !changed {
                continue;
            }
            let new_row = table_arc.validate_row(new_row)?;
            let key = encode_key(&new_row[schema.pk_idx])?;
            staged.insert(
                (table.to_string(), key),
                StagedWrite {
                    row: Some(new_row),
                    is_insert: false,
                },
            );
        }
        let n = staged.len();
        self.commit_staged_or_stage(session, table, staged)?;
        Ok(Output::ok(format!("{n} row(s) updated")))
    }

    fn exec_delete(
        &self,
        session: &mut Session,
        table: &str,
        selection: Option<Expr>,
    ) -> Result<Output> {
        let table_arc = self.table(table)?;
        let rows = self.visible_rows(session, &table_arc, selection.as_ref())?;
        let mut staged: HashMap<(String, Vec<u8>), StagedWrite> = HashMap::new();
        for row in rows {
            let key = encode_key(&row[table_arc.schema().pk_idx])?;
            staged.insert(
                (table.to_string(), key),
                StagedWrite {
                    row: None,
                    is_insert: false,
                },
            );
        }
        let n = staged.len();
        self.commit_staged_or_stage(session, table, staged)?;
        Ok(Output::ok(format!("{n} row(s) deleted")))
    }

    // ------------------------------------------------------------------
    // Visibility + commit plumbing
    // ------------------------------------------------------------------

    /// Append `records` (a single self-contained txn: records + Commit) and
    /// run it through the install sequencer so WAL offsets stay contiguous
    /// from the sequencer's point of view. Used by DDL, whose "install" is
    /// the catalog update that already happened.
    fn wal_commit(&self, records: Vec<Record>) -> Result<()> {
        let (start, end) = {
            let _guard = self.commit_lock.lock().unwrap();
            self.wal.append_records(&records)?
        };
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
    fn commit_staged_or_stage(
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
                tables.insert(t.clone(), self.table(t)?);
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
        records.push(Record::Commit { txn: txn_id });

        let has_inserts = staged.values().any(|w| w.is_insert);

        // Phase A: validation + append under the short commit_lock.
        let (start, end) = {
            let _guard = self.commit_lock.lock().unwrap();
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
            let offsets = self.wal.append_records(&records)?;
            if has_inserts {
                let mut in_flight = self.in_flight.lock().unwrap();
                for ((table, key), w) in &staged {
                    if w.is_insert {
                        in_flight.insert((table.clone(), key.clone()));
                    }
                }
            }
            offsets
        };

        // Phase B: group commit — one fsync by the syncer covers us plus
        // every other commit appended while the fsync was running.
        self.wal.wait_durable(end)?;

        // Phase C: install in WAL order.
        {
            let mut frontier = self.install.lock().unwrap();
            while *frontier != start {
                frontier = self.install_cv.wait(frontier).unwrap();
            }
            for (((table, key), _), enc_opt) in staged.iter().zip(encoded_rows.into_iter()) {
                let t = &tables[table];
                match enc_opt {
                    Some(enc) => t.apply_raw(key, &enc)?,
                    None => t.remove_raw(key),
                }
            }
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
    fn visible_row(
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
        match raw {
            Some(buf) => Ok(Some(table.decode_stored(&buf)?)),
            None => Ok(None),
        }
    }

    /// All rows of a table visible to the session: committed tree state,
    /// filtered, with the transaction's staged writes overlaid.
    fn visible_rows(
        &self,
        session: &Session,
        table: &Arc<Table>,
        selection: Option<&Expr>,
    ) -> Result<Vec<Vec<Datum>>> {
        let schema = table.schema();
        let overlay: Option<Vec<(Vec<u8>, &StagedWrite)>> = session.txn.as_ref().map(|t| {
            t.staged
                .iter()
                .filter(|((tbl, _), _)| tbl == &table.def.name)
                .map(|((_, k), w)| (k.clone(), w))
                .collect()
        });

        let mut rows: Vec<(Vec<u8>, Vec<Datum>)> = Vec::new();
        match access_path(table, selection)? {
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
                for (k, raw) in table.tree().scan_all() {
                    rows.push((k, table.decode_stored(&raw)?));
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
        if let Some(sel) = selection {
            rows.into_iter()
                .filter(|(_, r)| eval_expr(sel, schema, r).unwrap_or(false))
                .map(|(_, r)| Ok(r))
                .collect()
        } else {
            Ok(rows.into_iter().map(|(_, r)| r).collect())
        }
    }
}

// ---------------------------------------------------------------------------
// Recovery helpers
// ---------------------------------------------------------------------------

fn parse_simple_literal(s: &str) -> Option<Datum> {
    if s.eq_ignore_ascii_case("null") {
        return Some(Datum::Null);
    }
    if s.eq_ignore_ascii_case("true") {
        return Some(Datum::Bool(true));
    }
    if s.eq_ignore_ascii_case("false") {
        return Some(Datum::Bool(false));
    }
    if let Ok(n) = s.parse::<i64>() {
        return Some(Datum::Int(n));
    }
    if let Ok(f) = s.parse::<f64>() {
        return Some(Datum::Float(f));
    }
    if (s.starts_with('\'') && s.ends_with('\'') && s.len() >= 2)
        || (s.starts_with('"') && s.ends_with('"') && s.len() >= 2)
    {
        return Some(Datum::Text(s[1..s.len() - 1].to_string()));
    }
    None
}

fn txn_of(rec: &Record) -> u64 {    match rec {
        Record::Put { txn, .. }
        | Record::Delete { txn, .. }
        | Record::CreateTable { txn, .. }
        | Record::DropTable { txn, .. }
        | Record::CreateIndex { txn, .. }
        | Record::DropIndex { txn, .. }
        | Record::Commit { txn } => *txn,
    }
}

fn apply_records(tables: &mut HashMap<String, Arc<Table>>, pool: &Arc<BufferPool>, batch: Vec<Record>) -> Result<()> {
    for rec in batch {
        match rec {
            Record::CreateTable { def, .. } => {
                if !tables.contains_key(&def.name) {
                    let t = Arc::new(Table::new(def));
                    t.set_pool(pool.clone());
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
