//! Table layer: schema + row encoding on top of the B+ tree.
//!
//! The tree maps order-preserving encoded primary keys to encoded rows. All
//! layout decisions (row codec, key codec) are versioned so the WAL/snapshot
//! formats can evolve without breaking recovery.

use std::sync::{Arc, Mutex, RwLock};

use crate::btree::BTree;
use crate::error::{Error, Result};
use crate::page::{BufferPool, Locator};
use crate::types::{
    decode_key, decode_sec_index_key, encode_key, encode_sec_index_key,
    encode_sec_key_prefix, ColumnType, Datum,
};

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub ctype: ColumnType,
    pub nullable: bool,
    pub auto_increment: bool,
}

#[derive(Debug, Clone)]
pub struct Schema {
    pub columns: Vec<ColumnDef>,
    /// Index into `columns` of the single-column primary key.
    pub pk_idx: usize,
}

impl Schema {
    pub fn index_of(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    pub fn column_names(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.name.clone()).collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexDef {
    pub name: String,
    pub column: String,
}

#[derive(Debug, Clone)]
pub struct TableDef {
    pub name: String,
    pub schema: Schema,
    pub indexes: Vec<IndexDef>,
}

impl TableDef {
    pub fn new(name: impl Into<String>, schema: Schema) -> Self {
        TableDef {
            name: name.into(),
            schema,
            indexes: Vec::new(),
        }
    }
}

pub struct SecondaryIndex {
    pub def: IndexDef,
    pub col_idx: usize,
    pub tree: BTree,
}

pub struct Table {
    pub def: TableDef,
    tree: BTree,
    indexes: RwLock<Vec<SecondaryIndex>>,
    /// Off-page overflow store for wide rows (Priority 2). `None` keeps
    /// Table usable standalone (tests); `Database` always attaches one, and
    /// wide rows stay inline when detached.
    pool: RwLock<Option<Arc<BufferPool>>>,
    /// AUTO_INCREMENT sequence state, if the schema declares one. Gaps from
    /// rolled-back or deleted values are kept (MySQL semantics); the counter
    /// is rebuilt as max(pk)+1 on open so it never regresses across restarts.
    auto_inc: Mutex<Option<AutoIncState>>,
}

#[derive(Debug, Clone)]
struct AutoIncState {
    col_idx: usize,
    next: u64,
}

/// Encoded rows at or below this size stay in the B+ tree; larger rows spill
/// to the page pool and the tree holds a 14-byte locator instead.
pub const MAX_INLINE_ROW: usize = 1024;

impl Table {
    pub fn new(def: TableDef) -> Self {
        let mut sec_indexes = Vec::new();
        for idx in &def.indexes {
            if let Some(col_idx) = def.schema.index_of(&idx.column) {
                sec_indexes.push(SecondaryIndex {
                    def: idx.clone(),
                    col_idx,
                    tree: BTree::new(),
                });
            }
        }
        let auto_inc = Table::auto_inc_for(&def);
        Table {
            def,
            tree: BTree::new(),
            indexes: RwLock::new(sec_indexes),
            pool: RwLock::new(None),
            auto_inc: Mutex::new(auto_inc),
        }
    }

    fn auto_inc_for(def: &TableDef) -> Option<AutoIncState> {
        def.schema
            .columns
            .iter()
            .position(|c| c.auto_increment)
            .map(|col_idx| AutoIncState { col_idx, next: 1 })
    }

    /// Rebuild the sequence counter as max(pk)+1 over stored rows. Called by
    /// `Database::open` after snapshot load + WAL replay; with no rows the
    /// counter rests at 1. Only integer PKs can be auto-increment (enforced
    /// at CREATE), so undecodable keys are skipped, never fatal.
    pub fn refresh_auto_inc(&self) -> Result<()> {
        if self.auto_inc.lock().unwrap().is_none() {
            return Ok(());
        }
        let mut max: Option<u64> = None;
        for (k, _) in self.tree.scan_all() {
            if let Ok(Datum::Int(v)) = decode_key(&k) {
                if v >= 0 {
                    max = Some(max.map_or(v as u64, |m: u64| m.max(v as u64)));
                }
            }
        }
        // Hold the lock only for the writeback, not the scan.
        if let Some(st) = self.auto_inc.lock().unwrap().as_mut() {
            st.next = max.map_or(1, |m| m.saturating_add(1));
        }
        Ok(())
    }

    /// Fill an AUTO_INCREMENT column: NULL triggers the next sequence value
    /// (MySQL-style); an explicit integer bumps the counter past itself.
    /// Runs before row validation, so NULL never reaches the NOT NULL check.
    pub fn assign_auto_inc(&self, row: &mut Vec<Datum>) -> Result<()> {
        let mut guard = self.auto_inc.lock().unwrap();
        let Some(st) = guard.as_mut() else {
            return Ok(());
        };
        if st.col_idx >= row.len() {
            return Err(Error::Corrupted("auto-inc column out of range".into()));
        }
        match &row[st.col_idx] {
            Datum::Null => {
                let n = st.next;
                if n > i64::MAX as u64 {
                    return Err(Error::NotSupported("auto-increment exhausted".into()));
                }
                st.next = n + 1;
                row[st.col_idx] = Datum::Int(n as i64);
            }
            Datum::Int(v) => {
                if *v >= 0 && (*v as u64) >= st.next {
                    st.next = (*v as u64).saturating_add(1);
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Attach the database's overflow page pool (called by `Database::open`
    /// for every table, including ones created later by DDL).
    pub fn set_pool(&self, pool: Arc<BufferPool>) {
        *self.pool.write().unwrap() = Some(pool);
    }

    pub fn schema(&self) -> &Schema {
        &self.def.schema
    }

    pub fn row_count(&self) -> usize {
        self.tree.len()
    }

    // -- row codec ------------------------------------------------------

    pub fn encode_row(row: &[Datum]) -> Vec<u8> {
        let mut out = Vec::with_capacity(32);
        for d in row {
            d.encode(&mut out);
        }
        out
    }

    pub fn decode_row(&self, buf: &[u8]) -> Result<Vec<Datum>> {
        let mut off = 0usize;
        let mut out = Vec::with_capacity(self.def.schema.columns.len());
        for _ in 0..self.def.schema.columns.len() {
            out.push(Datum::decode(buf, &mut off)?);
        }
        if off != buf.len() {
            return Err(Error::Corrupted("row trailing bytes".into()));
        }
        Ok(out)
    }

    // -- overflow paging --------------------------------------------------
    //
    // Wide rows live in the page pool; the tree holds locators. Every read
    // resolves through here, every write stores through `store_value`, every
    // delete frees through `free_value`. Locator bytes are opaque to the
    // tree (OLC traversal never interprets values), so eviction/faults only
    // touch the pool's own locks.

    /// Resolve a stored tree value to full row bytes (inline passthrough or
    /// pool load). A locator without an attached pool is corruption, never a
    /// panic.
    pub fn resolve_value<'a>(&self, raw: &'a [u8]) -> Result<std::borrow::Cow<'a, [u8]>> {
        if !Locator::is_locator(raw) {
            return Ok(std::borrow::Cow::Borrowed(raw));
        }
        let pool = self.pool.read().unwrap().clone();
        let pool = pool.ok_or_else(|| Error::Corrupted("overflow value without pool".into()))?;
        let loc = Locator::decode(raw)?;
        Ok(std::borrow::Cow::Owned(pool.load(loc)?))
    }

    /// Decode a stored tree value straight to a row.
    pub fn decode_stored(&self, raw: &[u8]) -> Result<Vec<Datum>> {
        if !Locator::is_locator(raw) {
            return self.decode_row(raw);
        }
        let full = self.resolve_value(raw)?;
        self.decode_row(&full)
    }

    /// Make full row bytes storable: page wide rows into the pool and return
    /// the bytes to install in the tree. Never frees: the caller frees the
    /// replaced tree value (returned by the tree op), so every locator is
    /// freed exactly once by the writer that displaced it.
    pub fn alloc_value(&self, full: &[u8]) -> Result<Vec<u8>> {
        if full.len() <= MAX_INLINE_ROW {
            return Ok(full.to_vec());
        }
        let pool = self.pool.read().unwrap().clone();
        let Some(pool) = pool else {
            return Ok(full.to_vec());
        };
        Ok(pool.store(full)?.encode())
    }

    /// Release the locator in a stored value, if any. Never fails a commit.
    pub fn free_value(&self, raw: &[u8]) {
        if !Locator::is_locator(raw) {
            return;
        }
        if let Some(pool) = self.pool.read().unwrap().clone() {
            if let Ok(loc) = Locator::decode(raw) {
                pool.free(loc);
            }
        }
    }

    // -- key helpers ------------------------------------------------------

    fn key_of(&self, row: &[Datum]) -> Result<Vec<u8>> {
        encode_key(&row[self.def.schema.pk_idx])
    }

    /// Validate/coerce a candidate row against the schema; returns an owned
    /// normalized copy.
    pub fn validate_row(&self, row: Vec<Datum>) -> Result<Vec<Datum>> {
        let schema = &self.def.schema;
        if row.len() != schema.columns.len() {
            return Err(Error::ColumnCountMismatch {
                expected: schema.columns.len(),
                got: row.len(),
            });
        }
        let mut norm = Vec::with_capacity(row.len());
        for (col, d) in schema.columns.iter().zip(row.into_iter()) {
            if d == Datum::Null {
                if !col.nullable {
                    return Err(Error::NotNullViolation(col.name.clone()));
                }
                norm.push(Datum::Null);
                continue;
            }
            if !col.ctype.accepts(&d) {
                return Err(Error::TypeMismatch {
                    expected: col.ctype.name().to_string(),
                    got: d.type_name().to_string(),
                });
            }
            norm.push(d);
        }
        Ok(norm)
    }

    // -- data operations ----------------------------------------------------

    /// Insert a validated row. Fails on duplicate primary key.
    pub fn insert_row(&self, row: Vec<Datum>) -> Result<()> {
        let key = self.key_of(&row)?;
        if self.tree.get(&key).is_some() {
            let pk = decode_key(&key)?;
            return Err(Error::DuplicateKey(pk.to_string()));
        }
        let storable = self.alloc_value(&Self::encode_row(&row))?;
        if !self.tree.insert(&key, &storable) {
            // Lost a race with a concurrent inserter: release the orphan.
            self.free_value(&storable);
            let pk = decode_key(&key)?;
            return Err(Error::DuplicateKey(pk.to_string()));
        }
        let idx_guard = self.indexes.read().unwrap();
        if !idx_guard.is_empty() {
            let pk_val = &row[self.def.schema.pk_idx];
            for idx in idx_guard.iter() {
                let sec_val = &row[idx.col_idx];
                let sec_k = encode_sec_index_key(sec_val, pk_val)?;
                idx.tree.insert(&sec_k, &[]);
            }
        }
        Ok(())
    }

    /// Insert or replace a row, returning the previous row if any.
    pub fn upsert_row(&self, row: Vec<Datum>) -> Result<Option<Vec<Datum>>> {
        let key = self.key_of(&row)?;
        let storable = self.alloc_value(&Self::encode_row(&row))?;
        let prev = self.tree.upsert(&key, &storable);
        if let Some(ref buf) = prev {
            self.free_value(buf);
        }
        let prev = prev.map(|b| self.decode_stored(&b)).transpose()?;
        let idx_guard = self.indexes.read().unwrap();
        if !idx_guard.is_empty() {
            let pk_val = &row[self.def.schema.pk_idx];
            if let Some(ref old_row) = prev {
                for idx in idx_guard.iter() {
                    let old_val = &old_row[idx.col_idx];
                    let new_val = &row[idx.col_idx];
                    if old_val != new_val {
                        let old_k = encode_sec_index_key(old_val, pk_val)?;
                        idx.tree.remove(&old_k);
                        let new_k = encode_sec_index_key(new_val, pk_val)?;
                        idx.tree.insert(&new_k, &[]);
                    }
                }
            } else {
                for idx in idx_guard.iter() {
                    let sec_val = &row[idx.col_idx];
                    let sec_k = encode_sec_index_key(sec_val, pk_val)?;
                    idx.tree.insert(&sec_k, &[]);
                }
            }
        }
        Ok(prev)
    }

    pub fn delete_row(&self, key: &Datum) -> Result<Option<Vec<Datum>>> {
        let key_bytes = encode_key(key)?;
        match self.tree.remove(&key_bytes) {
            Some(buf) => {
                self.free_value(&buf);
                let row = self.decode_stored(&buf)?;
                let idx_guard = self.indexes.read().unwrap();
                if !idx_guard.is_empty() {
                    for idx in idx_guard.iter() {
                        let sec_val = &row[idx.col_idx];
                        let sec_k = encode_sec_index_key(sec_val, key)?;
                        idx.tree.remove(&sec_k);
                    }
                }
                Ok(Some(row))
            }
            None => Ok(None),
        }
    }

    pub fn get_row(&self, key: &Datum) -> Result<Option<Vec<Datum>>> {
        let key = encode_key(key)?;
        match self.tree.get(&key) {
            Some(buf) => Ok(Some(self.decode_stored(&buf)?)),
            None => Ok(None),
        }
    }

    /// Point lookup returning the raw stored value (inline row or locator).
    /// Wide-row callers resolve via [`Table::resolve_value`].
    pub fn get_raw(&self, key: &Datum) -> Result<Option<Vec<u8>>> {
        Ok(self.tree.get(&encode_key(key)?))
    }

    /// All rows in primary-key order.
    pub fn scan(&self) -> Result<Vec<(Datum, Vec<Datum>)>> {
        self.tree
            .scan_all()
            .into_iter()
            .map(|(k, v)| Ok((decode_key(&k)?, self.decode_stored(&v)?)))
            .collect()
    }

    /// Primary-key range scan with explicit bound inclusivity.
    pub fn scan_range(
        &self,
        lo: Option<(&Datum, bool)>,
        hi: Option<(&Datum, bool)>,
    ) -> Result<Vec<(Datum, Vec<Datum>)>> {
        let lo_key = lo.as_ref().map(|(d, _)| encode_key(d)).transpose()?;
        let hi_key = hi.as_ref().map(|(d, _)| encode_key(d)).transpose()?;
        self.tree
            .range(
                lo_key.as_deref(),
                lo.as_ref().map(|(_, incl)| *incl).unwrap_or(true),
                hi_key.as_deref(),
                hi.as_ref().map(|(_, incl)| *incl).unwrap_or(true),
            )
            .into_iter()
            .map(|(k, v)| Ok((decode_key(&k)?, self.decode_stored(&v)?)))
            .collect()
    }

    // -- secondary index operations -----------------------------------------

    pub fn add_index(&self, name: String, column: String) -> Result<()> {
        let col_idx = self
            .def
            .schema
            .index_of(&column)
            .ok_or_else(|| Error::ColumnNotFound(column.clone()))?;
        if col_idx == self.def.schema.pk_idx {
            return Err(Error::InvalidSchema(
                "cannot create secondary index on primary key".into(),
            ));
        }
        let mut guard = self.indexes.write().unwrap();
        if guard.iter().any(|idx| idx.def.name == name) {
            return Err(Error::IndexExists(name));
        }
        let tree = BTree::new();
        let pk_idx = self.def.schema.pk_idx;
        for (_, row) in self.scan()? {
            let sec_val = &row[col_idx];
            let pk_val = &row[pk_idx];
            let sec_k = encode_sec_index_key(sec_val, pk_val)?;
            tree.insert(&sec_k, &[]);
        }
        let def = IndexDef { name, column };
        guard.push(SecondaryIndex {
            def,
            col_idx,
            tree,
        });
        Ok(())
    }

    pub fn drop_index(&self, name: &str) -> Result<()> {
        let mut guard = self.indexes.write().unwrap();
        let pos = guard
            .iter()
            .position(|idx| idx.def.name == name)
            .ok_or_else(|| Error::IndexNotFound(name.to_string()))?;
        guard.remove(pos);
        Ok(())
    }

    pub fn secondary_indexes(&self) -> Vec<IndexDef> {
        self.indexes.read().unwrap().iter().map(|i| i.def.clone()).collect()
    }

    pub fn table_def(&self) -> TableDef {
        let mut def = self.def.clone();
        def.indexes = self.secondary_indexes();
        def
    }

    /// Primary keys matching a secondary index range or equality query.
    pub fn scan_secondary(
        &self,
        col_idx: usize,
        lo: Option<(&Datum, bool)>,
        hi: Option<(&Datum, bool)>,
    ) -> Result<Option<Vec<Datum>>> {
        let guard = self.indexes.read().unwrap();
        let Some(idx) = guard.iter().find(|i| i.col_idx == col_idx) else {
            return Ok(None);
        };
        let lo_key = lo
            .as_ref()
            .map(|(d, _)| encode_sec_key_prefix(d))
            .transpose()?;

        let scanned = idx.tree.range(
            lo_key.as_deref(),
            true,
            None,
            true,
        );
        let mut pks = Vec::with_capacity(scanned.len());
        for (k, _) in scanned {
            let (sec, pk) = decode_sec_index_key(&k)?;
            if let Some((lo_val, incl)) = lo {
                if incl {
                    if &sec < lo_val {
                        continue;
                    }
                } else if &sec <= lo_val {
                    continue;
                }
            }
            if let Some((hi_val, incl)) = hi {
                if incl {
                    if &sec > hi_val {
                        break;
                    }
                } else if &sec >= hi_val {
                    break;
                }
            }
            pks.push(pk);
        }
        Ok(Some(pks))
    }

    // -- persistence helpers ------------------------------------------------

    /// Re-insert an inline encoded row from a v1 snapshot (pre-paging data
    /// has no locators). The key is re-derived from the decoded row.
    pub fn restore_raw(&self, raw: &[u8]) -> Result<()> {
        let row = self.decode_row(raw)?;
        let key = self.key_of(&row)?;
        self.tree.upsert(&key, raw);
        let idx_guard = self.indexes.read().unwrap();
        if !idx_guard.is_empty() {
            let pk_val = &row[self.def.schema.pk_idx];
            for idx in idx_guard.iter() {
                let sec_val = &row[idx.col_idx];
                let sec_k = encode_sec_index_key(sec_val, pk_val)?;
                idx.tree.upsert(&sec_k, &[]);
            }
        }
        Ok(())
    }

    /// Restore one v2 snapshot entry: explicit key + stored value (inline row
    /// or overflow locator; the pool file persists alongside the snapshot so
    /// locators stay valid). Corrupt locators fail the open, never panic.
    pub fn restore_kv(&self, key: &[u8], val: &[u8]) -> Result<()> {
        let row = self.decode_stored(val)?;
        let expect = self.key_of(&row)?;
        if expect != key {
            return Err(Error::Corrupted("snapshot key/row mismatch".into()));
        }
        self.tree.upsert(key, val);
        let idx_guard = self.indexes.read().unwrap();
        if !idx_guard.is_empty() {
            let pk = decode_key(key)?;
            for idx in idx_guard.iter() {
                let sec_val = &row[idx.col_idx];
                let sec_k = encode_sec_index_key(sec_val, &pk)?;
                idx.tree.upsert(&sec_k, &[]);
            }
        }
        Ok(())
    }

    /// Apply a WAL redo record (key + full encoded row), replacing any
    /// existing version. Recovery is idempotent redo, so upsert semantics
    /// are correct; the displaced value's locator is freed exactly once.
    /// Wide rows are size-checked before WAL append, so paging here only
    /// fails on real I/O errors.
    pub fn apply_raw(&self, key: &[u8], full: &[u8]) -> Result<()> {
        let storable = self.alloc_value(full)?;
        let prev = self.tree.upsert(key, &storable);
        if let Some(ref buf) = prev {
            self.free_value(buf);
        }
        let idx_guard = self.indexes.read().unwrap();
        if !idx_guard.is_empty() {
            if let (Ok(pk), Ok(new_row)) = (decode_key(key), self.decode_row(full)) {
                if let Some(prev_buf) = prev {
                    if let Ok(old_row) = self.decode_stored(&prev_buf) {
                        for idx in idx_guard.iter() {
                            let old_val = &old_row[idx.col_idx];
                            let new_val = &new_row[idx.col_idx];
                            if old_val != new_val {
                                if let Ok(old_k) = encode_sec_index_key(old_val, &pk) {
                                    idx.tree.remove(&old_k);
                                }
                                if let Ok(new_k) = encode_sec_index_key(new_val, &pk) {
                                    idx.tree.upsert(&new_k, &[]);
                                }
                            }
                        }
                        return Ok(());
                    }
                }
                for idx in idx_guard.iter() {
                    let sec_val = &new_row[idx.col_idx];
                    if let Ok(sec_k) = encode_sec_index_key(sec_val, &pk) {
                        idx.tree.upsert(&sec_k, &[]);
                    }
                }
            }
        }
        Ok(())
    }

    pub fn remove_raw(&self, key: &[u8]) {
        if let Some(buf) = self.tree.remove(key) {
            self.free_value(&buf);
            let idx_guard = self.indexes.read().unwrap();
            if !idx_guard.is_empty() {
                if let (Ok(pk), Ok(row)) = (decode_key(key), self.decode_stored(&buf)) {
                    for idx in idx_guard.iter() {
                        let sec_val = &row[idx.col_idx];
                        if let Ok(sec_k) = encode_sec_index_key(sec_val, &pk) {
                            idx.tree.remove(&sec_k);
                        }
                    }
                }
            }
        }
    }

    pub fn tree(&self) -> &BTree {
        &self.tree
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::epoch::EpochManager;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TBL_POOL_SEQ: AtomicU64 = AtomicU64::new(0);

    fn make_paged_table(frames: usize) -> (Table, Arc<BufferPool>) {
        let schema = Schema {
            columns: vec![
                ColumnDef { name: "id".into(), ctype: ColumnType::Int, nullable: false, auto_increment: false },
                ColumnDef { name: "body".into(), ctype: ColumnType::Text, nullable: true, auto_increment: false },
            ],
            pk_idx: 0,
        };
        let table = Table::new(TableDef::new("docs", schema));
        let id = TBL_POOL_SEQ.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("hench_tblpool_{}_{}.bin", std::process::id(), id));
        let _ = std::fs::remove_file(&path);
        let pool = Arc::new(BufferPool::open(&path, frames, EpochManager::new()).unwrap());
        table.set_pool(pool.clone());
        (table, pool)
    }

    fn make_test_table() -> Table {
        let schema = Schema {
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    ctype: ColumnType::Int,
                    nullable: false,
                    auto_increment: false,
                },
                ColumnDef {
                    name: "age".into(),
                    ctype: ColumnType::Int,
                    nullable: false,
                    auto_increment: false,
                },
                ColumnDef {
                    name: "name".into(),
                    ctype: ColumnType::Text,
                    nullable: true,
                    auto_increment: false,
                },
            ],
            pk_idx: 0,
        };
        Table::new(TableDef::new("users", schema))
    }

    #[test]
    fn table_secondary_index_lifecycle() {
        let table = make_test_table();

        // Insert initial rows
        table.insert_row(vec![Datum::Int(1), Datum::Int(25), Datum::Text("Alice".into())]).unwrap();
        table.insert_row(vec![Datum::Int(2), Datum::Int(30), Datum::Text("Bob".into())]).unwrap();
        table.insert_row(vec![Datum::Int(3), Datum::Int(25), Datum::Text("Charlie".into())]).unwrap();

        // Create secondary index on `age`
        table.add_index("idx_age".into(), "age".into()).unwrap();
        assert_eq!(table.secondary_indexes().len(), 1);

        // Point scan on secondary index: age == 25 should return pks 1 and 3
        let pks = table.scan_secondary(1, Some((&Datum::Int(25), true)), Some((&Datum::Int(25), true))).unwrap().unwrap();
        assert_eq!(pks, vec![Datum::Int(1), Datum::Int(3)]);

        // Range scan: age >= 25 and age < 30 should return pks 1 and 3
        let pks = table.scan_secondary(1, Some((&Datum::Int(25), true)), Some((&Datum::Int(30), false))).unwrap().unwrap();
        assert_eq!(pks, vec![Datum::Int(1), Datum::Int(3)]);

        // Range scan: age >= 25 and age <= 30 should return 1, 3, 2
        let pks = table.scan_secondary(1, Some((&Datum::Int(25), true)), Some((&Datum::Int(30), true))).unwrap().unwrap();
        assert_eq!(pks, vec![Datum::Int(1), Datum::Int(3), Datum::Int(2)]);

        // Insert another row: age 30
        table.insert_row(vec![Datum::Int(4), Datum::Int(30), Datum::Text("David".into())]).unwrap();
        let pks = table.scan_secondary(1, Some((&Datum::Int(30), true)), Some((&Datum::Int(30), true))).unwrap().unwrap();
        assert_eq!(pks, vec![Datum::Int(2), Datum::Int(4)]);

        // Update row 1: change age from 25 to 30
        table.upsert_row(vec![Datum::Int(1), Datum::Int(30), Datum::Text("Alice".into())]).unwrap();
        let pks_25 = table.scan_secondary(1, Some((&Datum::Int(25), true)), Some((&Datum::Int(25), true))).unwrap().unwrap();
        assert_eq!(pks_25, vec![Datum::Int(3)]);
        let pks_30 = table.scan_secondary(1, Some((&Datum::Int(30), true)), Some((&Datum::Int(30), true))).unwrap().unwrap();
        assert_eq!(pks_30, vec![Datum::Int(1), Datum::Int(2), Datum::Int(4)]);

        // Delete row 3
        table.delete_row(&Datum::Int(3)).unwrap();
        let pks_25 = table.scan_secondary(1, Some((&Datum::Int(25), true)), Some((&Datum::Int(25), true))).unwrap().unwrap();
        assert!(pks_25.is_empty());

        // Drop index
        table.drop_index("idx_age").unwrap();
        assert_eq!(table.secondary_indexes().len(), 0);
        assert!(table.scan_secondary(1, None, None).unwrap().is_none());
    }

    #[test]
    fn paged_rows_crud_with_eviction() {
        // 2 frames: chained + multi-page data must evict and fault back.
        let (table, pool) = make_paged_table(2);
        let wide = |n: usize, c: char| Datum::Text(c.to_string().repeat(n));

        // Inline + single-page + chained (>256 KiB) rows.
        table.insert_row(vec![Datum::Int(1), wide(10, 'a')]).unwrap();
        table.insert_row(vec![Datum::Int(2), wide(5000, 'b')]).unwrap();
        table.insert_row(vec![Datum::Int(3), wide(300 * 1024, 'c')]).unwrap();
        assert_eq!(table.row_count(), 3);

        // Raw storage reflects the split: inline vs locator.
        let raw1 = table.get_raw(&Datum::Int(1)).unwrap().unwrap();
        let raw3 = table.get_raw(&Datum::Int(3)).unwrap().unwrap();
        assert!(!crate::page::Locator::is_locator(&raw1));
        assert!(crate::page::Locator::is_locator(&raw3));

        // Reads fault evicted pages back transparently.
        assert_eq!(table.get_row(&Datum::Int(1)).unwrap().unwrap()[1], wide(10, 'a'));
        assert_eq!(table.get_row(&Datum::Int(2)).unwrap().unwrap()[1], wide(5000, 'b'));
        assert_eq!(table.get_row(&Datum::Int(3)).unwrap().unwrap()[1], wide(300 * 1024, 'c'));

        // Five 200 KiB rows: each needs its own page (two never share one),
        // so a 2-frame pool must evict and fault.
        for i in 10..15 {
            table.insert_row(vec![Datum::Int(i), wide(200 * 1024, (b'e' + i as u8) as char)]).unwrap();
        }
        let st = pool.stats();
        assert!(st.evictions > 0, "expected evictions, got {st:?}");
        for i in 10..15 {
            assert_eq!(
                table.get_row(&Datum::Int(i)).unwrap().unwrap()[1],
                wide(200 * 1024, (b'e' + i as u8) as char)
            );
        }
        // Re-reads after eviction fault pages back from disk.
        assert!(pool.stats().faults > 0);
        assert_eq!(table.get_row(&Datum::Int(3)).unwrap().unwrap()[1], wide(300 * 1024, 'c'));

        // Full scan + secondary index over mixed storage.
        table.add_index("idx_body".into(), "body".into()).unwrap();
        assert_eq!(table.scan().unwrap().len(), 8);

        // Update a wide row (frees the old chain) and delete another.
        table.upsert_row(vec![Datum::Int(2), wide(6000, 'd')]).unwrap();
        assert_eq!(table.get_row(&Datum::Int(2)).unwrap().unwrap()[1], wide(6000, 'd'));
        table.delete_row(&Datum::Int(1)).unwrap();
        assert!(table.get_row(&Datum::Int(1)).unwrap().is_none());
        assert_eq!(table.get_row(&Datum::Int(3)).unwrap().unwrap()[1], wide(300 * 1024, 'c'));

        // Duplicate detection still works on paged rows.
        assert!(table.insert_row(vec![Datum::Int(2), wide(5, 'x')]).is_err());
    }
}

