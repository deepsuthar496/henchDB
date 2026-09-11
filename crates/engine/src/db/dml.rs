//! Data manipulation language execution (INSERT, UPDATE, DELETE)
//! and fast-path point updates.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::plan::{access_path, AccessPath};
use super::privilege;
use super::subquery;
use super::{Database, Output, Session, StagedWrite};
use crate::error::{Error, Result};
use crate::page::MAX_VALUE_LEN;
use crate::sql::{Expr, Privilege};
use crate::table::Table;
use crate::types::{decode_key, encode_key, Datum};
use crate::wal::Record;

impl Database {
    pub(super) fn exec_insert(
        &self,
        session: &mut Session,
        table: &str,
        rows: Vec<Vec<Expr>>,
    ) -> Result<Output> {
        let table_arc = self.table(session, table)?;
        let table_key = self.resolve_table_key(session, table);
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
                (table_key.clone(), key),
                StagedWrite {
                    row: Some(row),
                    is_insert: true,
                },
            );
        }
        let n = staged.len();
        // FK child check: every inserted row must reference an existing
        // parent key (statement-staged parents included).
        if !table_arc.def.foreign_keys.is_empty() {
            self.fk_check_insert_rows(session, &staged)?;
        }
        self.commit_staged_or_stage(session, table, staged)?;
        Ok(Output::ok(format!("{n} row(s) inserted")))
    }

    pub(super) fn try_fast_point_update(&self, session: &mut Session, sql: &str) -> Result<Option<Output>> {
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
        // NULL matches nothing (update affects zero rows).
        if matches!(pk_val, Datum::Null) {
            return Ok(Some(Output::ok("0 row(s) updated")));
        }

        let table_arc = match self.table(session, table) {
            Ok(t) => t,
            Err(_) => return Ok(None),
        };
        // Fast-path writes are gated like parsed UPDATEs (deny, don't skip).
        if let Err(e) = privilege::check_table(self, session, table, Privilege::Update) {
            return Err(e);
        }
        let key_name = self.resolve_table_key(session, table);
        let schema = table_arc.schema();

        // FK-involved tables take the slow path (statement-level checks).
        if !table_arc.def.foreign_keys.is_empty() || self.fk_is_referenced(&key_name) {
            return Ok(None);
        }

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
        self.commit_single_update(&key_name, &table_arc, key, enc)?;
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
            Record::Commit { txn: txn_id, ts: Some(crate::wal::unix_now()) },
        ];

        let (start, end, commit_epoch) = {
            let _guard = self.acquire_commit_lock();
            let commit_epoch = self.alloc_commit_epoch();
            let offsets = self.wal.append_records(&records)?;
            (offsets.0, offsets.1, commit_epoch)
        };
        self.metrics
            .record_wal(records.len(), end.saturating_sub(start));

        self.wal.wait_durable(end)?;

        {
            let mut frontier = self.install.lock().unwrap();
            while *frontier != start {
                frontier = self.install_cv.wait(frontier).unwrap();
            }
            self.record_install(table, &key, Some(&enc), commit_epoch)?;
            table.apply_raw(&key, &enc)?;
            if let Some(ref observer) = *self.commit_observer.read().unwrap() {
                let decoded_row = table.decode_stored(&enc).ok();
                observer(commit_epoch, &[(table_name.to_string(), key.clone(), decoded_row)]);
            }
            self.visible_epoch.store(commit_epoch, std::sync::atomic::Ordering::SeqCst);
            if let Ok(mut vs) = self.versions.write() {
                vs.gc_locked();
            }
            *frontier = end;
            drop(frontier);
            self.install_cv.notify_all();
        }
        Ok(())
    }

    pub(super) fn exec_update(
        &self,
        session: &mut Session,
        table: &str,
        assignments: Vec<(String, Expr)>,
        selection: Option<Expr>,
    ) -> Result<Output> {
        let table_arc = self.table(session, table)?;
        let table_key = self.resolve_table_key(session, table);
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

        // Fast path for point update on PK when autocommit (skipped for
        // FK-involved tables: those need statement-level checks below).
        // Subquery predicates skip it too: the point probe cannot enforce
        // them, and the generic path below folds per row.
        let fk_involved =
            !table_arc.def.foreign_keys.is_empty() || self.fk_is_referenced(&table_key);
        let has_sub = selection.as_ref().is_some_and(subquery::has_subquery);
        if session.txn.is_none() && !fk_involved && !has_sub {
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
                    self.commit_single_update(&table_key, &table_arc, key, enc)?;
                    return Ok(Output::ok("1 row(s) updated"));
                } else {
                    return Ok(Output::ok("0 row(s) updated"));
                }
            }
        }

        let (plain_sel, sub_sel) = match selection.as_ref() {
            Some(s) if subquery::has_subquery(s) => {
                let (p, q) = subquery::split_subquery_parts(s);
                (p, q)
            }
            _ => (selection.clone(), None),
        };
        let mut rows = self.visible_rows(session, &table_arc, plain_sel.as_ref())?;
        if let Some(q) = sub_sel.as_ref() {
            let scope = std::slice::from_ref(&table_arc);
            rows = subquery::filter_with_subqueries(self, session, scope, rows, q)?;
        }
        let mut staged: HashMap<(String, Vec<u8>), StagedWrite> = HashMap::new();
        // (new key, old row) per updated row for FK checks.
        let mut pairs: Vec<(Vec<u8>, Vec<Datum>)> = Vec::new();
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
                (table_key.clone(), key.clone()),
                StagedWrite {
                    row: Some(new_row),
                    is_insert: false,
                },
            );
            // Pair new key with the old row (keys match unless SET touched
            // the PK; PK changes compare old vs new PK datums directly).
            pairs.push((key, row));
        }
        // FK: re-validate rows whose FK columns changed; propagate
        // parent-PK changes to referencing children (cascade actions extend
        // the staged set in place).
        if !pairs.is_empty()
            && (!table_arc.def.foreign_keys.is_empty() || self.fk_is_referenced(&table_key))
        {
            self.fk_check_updated(session, &table_key, &table_arc, &pairs, &mut staged)?;
        }
        let n = staged.len();
        self.commit_staged_or_stage(session, table, staged)?;
        Ok(Output::ok(format!("{n} row(s) updated")))
    }

    pub(super) fn exec_delete(
        &self,
        session: &mut Session,
        table: &str,
        selection: Option<Expr>,
    ) -> Result<Output> {
        let table_arc = self.table(session, table)?;
        let table_key = self.resolve_table_key(session, table);
        let (plain_sel, sub_sel) = match selection.as_ref() {
            Some(s) if subquery::has_subquery(s) => {
                let (p, q) = subquery::split_subquery_parts(s);
                (p, q)
            }
            _ => (selection.clone(), None),
        };
        let mut rows = self.visible_rows(session, &table_arc, plain_sel.as_ref())?;
        if let Some(q) = sub_sel.as_ref() {
            let scope = std::slice::from_ref(&table_arc);
            rows = subquery::filter_with_subqueries(self, session, scope, rows, q)?;
        }
        let mut staged: HashMap<(String, Vec<u8>), StagedWrite> = HashMap::new();
        for row in &rows {
            let key = encode_key(&row[table_arc.schema().pk_idx])?;
            staged.insert(
                (table_key.clone(), key),
                StagedWrite {
                    row: None,
                    is_insert: false,
                },
            );
        }
        // FK parent actions: RESTRICT rejects, CASCADE/SET NULL extend the
        // staged set (transitively, cycle-safe). The reported count stays
        // the directly matched rows.
        let n = self.fk_check_deleted(session, &table_arc, &rows, &mut staged)?;
        self.commit_staged_or_stage(session, table, staged)?;
        Ok(Output::ok(format!("{n} row(s) deleted")))
    }


}

pub(super) fn parse_simple_literal(s: &str) -> Option<Datum> {
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
