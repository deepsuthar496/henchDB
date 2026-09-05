//! Foreign key enforcement: child-side existence checks plus parent-side
//! referential actions (RESTRICT / CASCADE / SET NULL).
//!
//! Checks run at statement level over the staged write set (`staged`) plus
//! accumulated cascade actions (`pending`) plus the session overlay, so
//! multi-row statements and explicit transactions see their own writes.
//! Parent lookups prefer PK point seeks and secondary-index seeks and fall
//! back to filtered scans. `staged`/`pending` here only ever hold puts for
//! the child-check paths (inserts and updates); deletes appear only inside
//! cascade processing, which never calls the existence check.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::{Database, Session, StagedWrite};
use crate::error::{Error, Result};
use crate::sql::ForeignKeySpec;
use crate::table::{ForeignKeyDef, Table};
use crate::types::{encode_key, Datum};

/// Evaluator-consistent equality for FK matching: NULL never matches, and
/// Int/Float + DateTime/Text coerce exactly like `eval_with`.
fn coerce_eq(a: &Datum, b: &Datum) -> bool {
    if matches!(a, Datum::Null) || matches!(b, Datum::Null) {
        return false;
    }
    let (x, y) = crate::sql::eval::coerce_pair(a.clone(), b.clone());
    x == y
}

/// Lookup candidates for one FK value: the value itself plus its coerced
/// mirrors (Int<->Float, DateTime<->Text), matching `coerce_pair`.
fn fk_candidates(val: &Datum) -> Vec<Datum> {
    let mut out = vec![val.clone()];
    match val {
        Datum::Int(v) => out.push(Datum::Float(*v as f64)),
        Datum::Float(v) => {
            if v.fract() == 0.0 && *v >= i64::MIN as f64 && *v <= i64::MAX as f64 {
                out.push(Datum::Int(*v as i64));
            }
        }
        Datum::DateTime(m) => out.push(Datum::Text(crate::types::format_datetime_micros(*m))),
        Datum::Text(s) => {
            if let Some(m) = crate::types::parse_datetime_str(s) {
                out.push(Datum::DateTime(m));
            }
        }
        _ => {}
    }
    out
}

impl Database {
    /// `(child_key, child_table, fk)` for every FK referencing `parent_key`.
    fn fk_children_of(&self, parent_key: &str) -> Vec<(String, Arc<Table>, ForeignKeyDef)> {
        let guard = self.tables.read().unwrap();
        let mut out = Vec::new();
        for (tkey, tbl) in guard.iter() {
            for fk in &tbl.def.foreign_keys {
                if fk.ref_table == parent_key {
                    out.push((tkey.clone(), tbl.clone(), fk.clone()));
                }
            }
        }
        out
    }

    /// True when any table holds an FK referencing `table_key`. Used to gate
    /// the UPDATE fast paths (which bypass statement-level checks).
    pub(crate) fn fk_is_referenced(&self, table_key: &str) -> bool {
        let guard = self.tables.read().unwrap();
        guard
            .values()
            .any(|t| t.def.foreign_keys.iter().any(|fk| fk.ref_table == table_key))
    }

    /// One row as the FK checker sees it: cascade actions, then statement
    /// staged writes, then the session overlay (committed + txn-staged).
    fn fk_row(
        &self,
        session: &Session,
        table: &Arc<Table>,
        key: &[u8],
        staged: &HashMap<(String, Vec<u8>), StagedWrite>,
        pending: &HashMap<(String, Vec<u8>), StagedWrite>,
    ) -> Result<Option<Vec<Datum>>> {
        let tkey = (table.def.name.clone(), key.to_vec());
        if let Some(w) = pending.get(&tkey).or_else(|| staged.get(&tkey)) {
            return Ok(w.row.clone());
        }
        self.visible_row(session, table, key)
    }

    /// True when a statement-staged or cascade put carries a matching value
    /// in `col`. (Committed + session-txn state is covered by the read
    /// paths; staged deletes cannot occur on the existence-check paths.)
    fn fk_staged_hit(
        table_key: &str,
        col: usize,
        val: &Datum,
        staged: &HashMap<(String, Vec<u8>), StagedWrite>,
        pending: &HashMap<(String, Vec<u8>), StagedWrite>,
    ) -> bool {
        staged
            .iter()
            .chain(pending.iter())
            .filter(|((t, _), _)| t == table_key)
            .filter_map(|(_, w)| w.row.as_ref())
            .any(|row| coerce_eq(&row[col], val))
    }

    /// Parent-side existence for one FK value: statement writes first, then
    /// PK point seeks, secondary-index seeks, or a filtered scan.
    fn fk_parent_exists(
        &self,
        session: &Session,
        parent: &Arc<Table>,
        parent_col: usize,
        val: &Datum,
        staged: &HashMap<(String, Vec<u8>), StagedWrite>,
        pending: &HashMap<(String, Vec<u8>), StagedWrite>,
    ) -> Result<bool> {
        if Self::fk_staged_hit(&parent.def.name, parent_col, val, staged, pending) {
            return Ok(true);
        }
        let schema = parent.schema();
        if parent_col == schema.pk_idx {
            for cand in fk_candidates(val) {
                let key = encode_key(&cand)?;
                if self.fk_row(session, parent, &key, staged, pending)?.is_some() {
                    return Ok(true);
                }
            }
            return Ok(false);
        }
        let indexed = parent
            .secondary_indexes()
            .iter()
            .any(|d| schema.index_of(&d.column) == Some(parent_col));
        if indexed {
            for cand in fk_candidates(val) {
                if let Some(pks) =
                    parent.scan_secondary(parent_col, Some((&cand, true)), Some((&cand, true)))?
                {
                    for pk in pks {
                        let key = encode_key(&pk)?;
                        if let Some(row) = self.fk_row(session, parent, &key, staged, pending)? {
                            if coerce_eq(&row[parent_col], val) {
                                return Ok(true);
                            }
                        }
                    }
                }
            }
            return Ok(false);
        }
        for row in self.visible_rows(session, parent, None)? {
            if coerce_eq(&row[parent_col], val) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Validate one child row against every FK on its table. NULL FK values
    /// reference nothing and always pass.
    pub(crate) fn fk_check_child_row(
        &self,
        session: &Session,
        child: &Arc<Table>,
        row: &[Datum],
        staged: &HashMap<(String, Vec<u8>), StagedWrite>,
        pending: &HashMap<(String, Vec<u8>), StagedWrite>,
    ) -> Result<()> {
        for fk in &child.def.foreign_keys {
            let col = child
                .def
                .schema
                .index_of(&fk.column)
                .ok_or_else(|| Error::ColumnNotFound(fk.column.clone()))?;
            let val = &row[col];
            if matches!(val, Datum::Null) {
                continue;
            }
            let parent = self
                .tables
                .read()
                .unwrap()
                .get(&fk.ref_table)
                .cloned()
                .ok_or_else(|| Error::TableNotFound(fk.ref_table.clone()))?;
            let pcol = parent
                .def
                .schema
                .index_of(&fk.ref_column)
                .ok_or_else(|| Error::ColumnNotFound(fk.ref_column.clone()))?;
            if !self.fk_parent_exists(session, &parent, pcol, val, staged, pending)? {
                return Err(Error::ForeignKeyViolation(format!(
                    "value {val} in '{}.{}' has no match in '{}.{}'",
                    child.def.name, fk.column, parent.def.name, fk.ref_column
                )));
            }
        }
        Ok(())
    }

    /// Rows of `child` whose FK column references `val`: statement writes
    /// first (puts match, deletes suppress), then a secondary seek or a
    /// filtered scan over session-visible rows.
    fn fk_find_child_refs(
        &self,
        session: &Session,
        child: &Arc<Table>,
        col: usize,
        val: &Datum,
        staged: &HashMap<(String, Vec<u8>), StagedWrite>,
        pending: &HashMap<(String, Vec<u8>), StagedWrite>,
    ) -> Result<Vec<(Vec<u8>, Vec<Datum>)>> {
        // Overlay for this child: pending wins over staged.
        let mut overlay: HashMap<&Vec<u8>, &Option<Vec<Datum>>> = HashMap::new();
        for ((t, k), w) in staged.iter() {
            if t == &child.def.name {
                overlay.insert(k, &w.row);
            }
        }
        for ((t, k), w) in pending.iter() {
            if t == &child.def.name {
                overlay.insert(k, &w.row);
            }
        }
        let mut out = Vec::new();
        for (k, row_opt) in overlay.iter() {
            if let Some(row) = row_opt {
                if coerce_eq(&row[col], val) {
                    out.push(((*k).clone(), row.clone()));
                }
            }
        }
        let schema = child.schema();
        let indexed = child
            .secondary_indexes()
            .iter()
            .any(|d| schema.index_of(&d.column) == Some(col));
        if indexed {
            for cand in fk_candidates(val) {
                if let Some(pks) =
                    child.scan_secondary(col, Some((&cand, true)), Some((&cand, true)))?
                {
                    for pk in pks {
                        let key = encode_key(&pk)?;
                        if overlay.contains_key(&key) {
                            continue;
                        }
                        if let Some(row) = self.fk_row(session, child, &key, staged, pending)? {
                            if coerce_eq(&row[col], val) {
                                out.push((key, row));
                            }
                        }
                    }
                }
            }
            return Ok(out);
        }
        for row in self.visible_rows(session, child, None)? {
            let key = encode_key(&row[schema.pk_idx])?;
            if overlay.contains_key(&key) {
                continue;
            }
            if coerce_eq(&row[col], val) {
                out.push((key, row));
            }
        }
        Ok(out)
    }

    /// Parent-side referential action for one deleted parent key. Matching
    /// child rows RESTRICT (error), CASCADE (deleted, recursively), or
    /// SET NULL (nulled). Actions accumulate in `pending`; `visited` keeps
    /// cascades terminating on cyclic schemas.
    pub(crate) fn fk_on_parent_delete(
        &self,
        session: &Session,
        parent_key: &str,
        parent_pk: &Datum,
        staged: &HashMap<(String, Vec<u8>), StagedWrite>,
        pending: &mut HashMap<(String, Vec<u8>), StagedWrite>,
        visited: &mut HashSet<(String, Vec<u8>)>,
    ) -> Result<()> {
        for (child_key, child, fk) in self.fk_children_of(parent_key) {
            let col = child
                .def
                .schema
                .index_of(&fk.column)
                .ok_or_else(|| Error::ColumnNotFound(fk.column.clone()))?;
            let refs = self.fk_find_child_refs(session, &child, col, parent_pk, staged, pending)?;
            if refs.is_empty() {
                continue;
            }
            match fk.on_delete {
                crate::table::FkAction::Restrict => {
                    return Err(Error::ForeignKeyViolation(format!(
                        "cannot delete from '{parent_key}': {} row(s) in '{child_key}' reference it (constraint '{}')",
                        refs.len(),
                        fk.name
                    )));
                }
                crate::table::FkAction::Cascade => {
                    for (k, row) in refs {
                        if !visited.insert((child_key.clone(), k.clone())) {
                            continue;
                        }
                        pending.insert(
                            (child_key.clone(), k.clone()),
                            StagedWrite {
                                row: None,
                                is_insert: false,
                            },
                        );
                        let child_pk = row[child.def.schema.pk_idx].clone();
                        self.fk_on_parent_delete(
                            session,
                            &child_key,
                            &child_pk,
                            staged,
                            pending,
                            visited,
                        )?;
                    }
                }
                crate::table::FkAction::SetNull => {
                    if !child.def.schema.columns[col].nullable {
                        return Err(Error::ForeignKeyViolation(format!(
                            "cannot SET NULL on '{child_key}.{}': column is NOT NULL (constraint '{}')",
                            fk.column, fk.name
                        )));
                    }
                    for (k, row) in refs {
                        if !visited.insert((child_key.clone(), k.clone())) {
                            continue;
                        }
                        let mut new_row = row.clone();
                        new_row[col] = Datum::Null;
                        let new_row = child.validate_row(new_row)?;
                        pending.insert(
                            (child_key.clone(), k.clone()),
                            StagedWrite {
                                row: Some(new_row),
                                is_insert: false,
                            },
                        );
                    }
                }
            }
        }
        Ok(())
    }
}

impl Database {
    // -- DDL-time + statement-level wiring (called from db/mod.rs) -----------
    //
    // These live here (not in mod.rs) to hold the 1,500-line ceiling.
    // `TableDef` import covers the schema-only def passed to the builder.

    /// Validate FK specs for a new table and build storage defs. Ref tables
    /// resolve to catalog keys now (self-reference allowed).
    pub(crate) fn fk_build_defs(
        &self,
        session: &Session,
        key: &str,
        name: &str,
        def: &crate::table::TableDef,
        specs: Vec<ForeignKeySpec>,
    ) -> Result<Vec<ForeignKeyDef>> {
        let mut fks = Vec::with_capacity(specs.len());
        for fk in specs {
            if def.schema.index_of(&fk.column).is_none() {
                return Err(Error::ColumnNotFound(fk.column.clone()));
            }
            let ref_key = if fk.ref_table.contains('.') {
                fk.ref_table.clone()
            } else {
                format!("{}.{}", session.current_db, fk.ref_table)
            };
            // Self-reference (by bare or qualified name) validates against
            // the new schema itself.
            let self_ref = ref_key == key || fk.ref_table == name;
            if !self_ref {
                let guard = self.tables.read().unwrap();
                let parent = guard
                    .get(&ref_key)
                    .ok_or_else(|| Error::TableNotFound(fk.ref_table.clone()))?;
                if parent.def.schema.index_of(&fk.ref_column).is_none() {
                    return Err(Error::ColumnNotFound(fk.ref_column.clone()));
                }
            } else if def.schema.index_of(&fk.ref_column).is_none() {
                return Err(Error::ColumnNotFound(fk.ref_column.clone()));
            }
            fks.push(ForeignKeyDef {
                name: fk
                    .name
                    .unwrap_or_else(|| format!("fk_{}_{}_{}", name, fk.column, fk.ref_table)),
                column: fk.column,
                ref_table: if self_ref { key.to_string() } else { ref_key },
                ref_column: fk.ref_column,
                on_delete: fk.on_delete,
            });
        }
        Ok(fks)
    }

    /// MySQL behavior: every FK column gets a secondary index unless one
    /// already covers it (PK columns need none — PK seeks are indexed).
    /// Runs at CREATE time and once at open for pre-existing tables.
    pub(crate) fn fk_ensure_auto_indexes(table: &Arc<Table>) -> Result<()> {
        for fk in &table.def.foreign_keys {
            if table.def.schema.index_of(&fk.column) == Some(table.def.schema.pk_idx) {
                continue;
            }
            let covered = table
                .secondary_indexes()
                .iter()
                .any(|d| d.column == fk.column);
            if covered {
                continue;
            }
            let mut auto = format!("fk_{}", fk.column);
            let mut n = 2u32;
            while table.secondary_indexes().iter().any(|d| d.name == auto) {
                auto = format!("fk_{}_{}", fk.column, n);
                n += 1;
            }
            table.add_index(auto, fk.column.clone())?;
        }
        Ok(())
    }

    /// Open-time migration for tables created before auto-indexing.
    pub(crate) fn fk_ensure_all_auto_indexes(
        tables: &HashMap<String, Arc<Table>>,
    ) -> Result<()> {
        for t in tables.values() {
            Self::fk_ensure_auto_indexes(t)?;
        }
        Ok(())
    }

    /// Schema-level RESTRICT: a referenced parent cannot be dropped.
    pub(crate) fn fk_check_drop(&self, key: &str, name: &str) -> Result<()> {
        let guard = self.tables.read().unwrap();
        let mut blockers = Vec::new();
        for (t, tbl) in guard.iter() {
            for fk in &tbl.def.foreign_keys {
                if fk.ref_table == key {
                    blockers.push(format!("'{t}' (constraint '{}')", fk.name));
                }
            }
        }
        if !blockers.is_empty() {
            return Err(Error::ForeignKeyViolation(format!(
                "cannot drop table '{name}': referenced by {}",
                blockers.join(", ")
            )));
        }
        Ok(())
    }

    /// An FK column must keep index coverage: dropping its last index is
    /// rejected (correctness would survive via scan fallback, but MySQL
    /// forbids it and seeks stay fast).
    pub(crate) fn fk_check_drop_index(&self, table: &Arc<Table>, index_name: &str) -> Result<()> {
        let idxs = table.secondary_indexes();
        let Some(def) = idxs.iter().find(|d| d.name == index_name) else {
            return Ok(());
        };
        for fk in &table.def.foreign_keys {
            if fk.column == def.column
                && !idxs.iter().any(|d| d.name != index_name && d.column == fk.column)
            {
                return Err(Error::ForeignKeyViolation(format!(
                    "cannot drop index '{index_name}': foreign key '{}' needs an index on '{}.{}'",
                    fk.name, table.def.name, fk.column
                )));
            }
        }
        Ok(())
    }

    /// Child-side validation for a freshly staged INSERT set.
    pub(crate) fn fk_check_insert_rows(
        &self,
        session: &Session,
        staged: &HashMap<(String, Vec<u8>), StagedWrite>,
    ) -> Result<()> {
        let pending: HashMap<(String, Vec<u8>), StagedWrite> = HashMap::new();
        for ((t, _), w) in staged {
            if let Some(row) = &w.row {
                let tbl = self
                    .tables
                    .read()
                    .unwrap()
                    .get(t)
                    .cloned()
                    .ok_or_else(|| Error::TableNotFound(t.clone()))?;
                if !tbl.def.foreign_keys.is_empty() {
                    self.fk_check_child_row(session, &tbl, row, staged, &pending)?;
                }
            }
        }
        Ok(())
    }

    /// Child-side re-validation (FK columns changed) plus parent-PK-change
    /// propagation for a staged UPDATE set. `pairs` is (new key, old row).
    /// Cascade/SET NULL actions extend `staged` in place.
    pub(crate) fn fk_check_updated(
        &self,
        session: &Session,
        table_key: &str,
        table: &Arc<Table>,
        pairs: &[(Vec<u8>, Vec<Datum>)],
        staged: &mut HashMap<(String, Vec<u8>), StagedWrite>,
    ) -> Result<()> {
        let pk_idx = table.schema().pk_idx;
        let mut pending: HashMap<(String, Vec<u8>), StagedWrite> = HashMap::new();
        let mut visited: HashSet<(String, Vec<u8>)> = HashSet::new();
        for (new_key, old_row) in pairs {
            let new_row = staged[&(table_key.to_string(), new_key.clone())]
                .row
                .as_ref()
                .expect("staged update row");
            if !table.def.foreign_keys.is_empty() {
                let changed = table.def.foreign_keys.iter().any(|fk| {
                    table
                        .def
                        .schema
                        .index_of(&fk.column)
                        .map(|i| old_row[i] != new_row[i])
                        .unwrap_or(false)
                });
                if changed {
                    self.fk_check_child_row(session, table, new_row, staged, &pending)?;
                }
            }
            if old_row[pk_idx] != new_row[pk_idx] {
                self.fk_on_parent_delete(
                    session,
                    table_key,
                    &old_row[pk_idx],
                    staged,
                    &mut pending,
                    &mut visited,
                )?;
            }
        }
        staged.extend(pending);
        Ok(())
    }

    /// Parent-side actions for a staged DELETE set. Returns the directly
    /// matched row count (cascade additions excluded from the message).
    pub(crate) fn fk_check_deleted(
        &self,
        session: &Session,
        table: &Arc<Table>,
        rows: &[Vec<Datum>],
        staged: &mut HashMap<(String, Vec<u8>), StagedWrite>,
    ) -> Result<usize> {
        let n = staged.len();
        if self.fk_is_referenced(&table.def.name) {
            let pk_idx = table.schema().pk_idx;
            let old_by_key: HashMap<Vec<u8>, &Vec<Datum>> = rows
                .iter()
                .map(|r| (encode_key(&r[pk_idx]).expect("row key"), r))
                .collect();
            let mut pending: HashMap<(String, Vec<u8>), StagedWrite> = HashMap::new();
            let mut visited: HashSet<(String, Vec<u8>)> = HashSet::new();
            for ((t, k), w) in staged.iter() {
                if w.row.is_none() {
                    if let Some(old_row) = old_by_key.get(k) {
                        self.fk_on_parent_delete(
                            session,
                            t,
                            &old_row[pk_idx],
                            staged,
                            &mut pending,
                            &mut visited,
                        )?;
                    }
                }
            }
            staged.extend(pending);
        }
        Ok(n)
    }
}
