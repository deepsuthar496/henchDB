//! Storage correctness diagnostics, `CHECK TABLE`, and consistency hashing (§6).
//!
//! Validates:
//! - B+ tree primary key ordering (strict monotonicity, no duplicate keys)
//! - Row decode integrity and NOT NULL schema constraints
//! - Secondary index bidirectional consistency (index points to live row, live row in index)
//! - Foreign key referential integrity (child non-null FK points to existing parent PK)
//! - Deterministic CRC32 logical hash per table and per database

use std::collections::HashSet;

use super::{Database, Session};
use crate::error::Result;
use crate::types::{decode_key, decode_sec_index_key, encode_key, encode_sec_index_key, Datum};
use crate::wal::crc32;

/// Diagnostic check report for one table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckReport {
    pub table: String,
    pub op: String,
    pub status: String,
    pub hash: u32,
    pub error_msg: Option<String>,
}

impl CheckReport {
    pub fn is_ok(&self) -> bool {
        self.error_msg.is_none()
    }
}

impl Database {
    /// Perform comprehensive diagnostic integrity checks on a table and compute
    /// its deterministic logical CRC32 hash.
    pub fn check_table(&self, session: &Session, table_name: &str) -> Result<CheckReport> {
        let table = self.table(session, table_name)?;
        let qual_name = self.resolve_table_key(session, table_name);

        let mut errors: Vec<String> = Vec::new();

        // 1. Scan all rows in primary key order
        let all_raw = table.tree().scan_all();
        let mut prev_key: Option<Vec<u8>> = None;
        let mut decoded_rows: Vec<(Datum, Vec<Datum>)> = Vec::with_capacity(all_raw.len());

        for (k, v) in &all_raw {
            // Strict monotonicity: k must be strictly greater than prev_key
            if let Some(ref prev) = prev_key {
                if k <= prev {
                    errors.push(format!(
                        "B+ tree ordering violation: key {:?} is not strictly greater than previous key {:?}",
                        k, prev
                    ));
                }
            }
            prev_key = Some(k.clone());

            // Primary key decode
            let pk = match decode_key(k) {
                Ok(pk) => pk,
                Err(e) => {
                    errors.push(format!("Corrupt primary key {:?}: {}", k, e));
                    continue;
                }
            };

            // Stored row decode
            let row = match table.decode_stored(v) {
                Ok(row) => row,
                Err(e) => {
                    errors.push(format!("Corrupt row for PK {:?}: {}", pk, e));
                    continue;
                }
            };

            // Schema constraint verification
            let schema = table.schema();
            if row.len() != schema.columns.len() {
                errors.push(format!(
                    "Row column count mismatch for PK {:?}: expected {}, got {}",
                    pk,
                    schema.columns.len(),
                    row.len()
                ));
            } else {
                for (col_idx, col_def) in schema.columns.iter().enumerate() {
                    if !col_def.nullable && matches!(row[col_idx], Datum::Null) {
                        errors.push(format!(
                            "NOT NULL constraint violated for column '{}' at PK {:?}",
                            col_def.name, pk
                        ));
                    }
                }
            }

            decoded_rows.push((pk, row));
        }

        // 2. Secondary index bidirectional consistency verification
        let sec_indices = table.scan_secondary_index_entries();
        for (idx_name, _col_name, col_idx, sec_entries) in sec_indices {
            let mut sec_set: HashSet<Vec<u8>> = HashSet::with_capacity(sec_entries.len());

            for sec_k in sec_entries {
                sec_set.insert(sec_k.clone());
                match decode_sec_index_key(&sec_k) {
                    Ok((sec_val, pk_val)) => {
                        match encode_key(&pk_val) {
                            Ok(pk_bytes) => {
                                match table.tree().get(&pk_bytes) {
                                    Some(val_bytes) => {
                                        if let Ok(row) = table.decode_stored(&val_bytes) {
                                            if col_idx < row.len() && row[col_idx] != sec_val {
                                                errors.push(format!(
                                                    "Secondary index '{}' value mismatch: index has {:?}, row has {:?}",
                                                    idx_name, sec_val, row[col_idx]
                                                ));
                                            }
                                        }
                                    }
                                    None => {
                                        errors.push(format!(
                                            "Secondary index '{}' points to non-existent PK {:?}",
                                            idx_name, pk_val
                                        ));
                                    }
                                }
                            }
                            Err(e) => {
                                errors.push(format!(
                                    "Secondary index '{}' has invalid PK in key: {}",
                                    idx_name, e
                                ));
                            }
                        }
                    }
                    Err(e) => {
                        errors.push(format!(
                            "Secondary index '{}' has undecodable key: {}",
                            idx_name, e
                        ));
                    }
                }
            }

            // Reverse check: every row must be present in the secondary index
            let pk_idx = table.def.schema.pk_idx;
            for (pk, row) in &decoded_rows {
                if col_idx < row.len() {
                    let sec_val = &row[col_idx];
                    let pk_val = &row[pk_idx];
                    if let Ok(expect_sec_k) = encode_sec_index_key(sec_val, pk_val) {
                        if !sec_set.contains(&expect_sec_k) {
                            errors.push(format!(
                                "Row PK {:?} missing from secondary index '{}'",
                                pk, idx_name
                            ));
                        }
                    }
                }
            }
        }

        // 3. Foreign key referential integrity verification
        if !table.def.foreign_keys.is_empty() {
            let tables_guard = self.tables.read().unwrap();
            for fk in &table.def.foreign_keys {
                let parent_table_opt = if fk.ref_table.contains('.') {
                    tables_guard.get(&fk.ref_table)
                } else {
                    let qual = format!("{}.{}", session.current_db, fk.ref_table);
                    tables_guard.get(&qual).or_else(|| tables_guard.get(&fk.ref_table))
                };

                match parent_table_opt {
                    Some(parent_table) => {
                        let child_col_idx = table
                            .def
                            .schema
                            .columns
                            .iter()
                            .position(|c| c.name == fk.column);

                        if let Some(col_idx) = child_col_idx {
                            for (pk, row) in &decoded_rows {
                                if col_idx < row.len() {
                                    let fk_val = &row[col_idx];
                                    if !matches!(fk_val, Datum::Null) {
                                        if let Ok(parent_pk_bytes) = encode_key(fk_val) {
                                            if parent_table.tree().get(&parent_pk_bytes).is_none() {
                                                errors.push(format!(
                                                    "Foreign key constraint '{}' violated on PK {:?}: referenced row {:?} not found in '{}'",
                                                    fk.name,
                                                    pk,
                                                    fk_val,
                                                    fk.ref_table
                                                ));
                                            }
                                        }
                                    }
                                }
                            }
                        } else {
                            errors.push(format!(
                                "Foreign key references non-existent column '{}' in this table",
                                fk.column
                            ));
                        }
                    }
                    None => {
                        errors.push(format!(
                            "Foreign key references missing parent table '{}'",
                            fk.ref_table
                        ));
                    }
                }
            }
        }

        // 4. Deterministic logical CRC32 hash calculation
        let mut hasher_buf = Vec::with_capacity(all_raw.len() * 32);
        for (k, v) in &all_raw {
            hasher_buf.extend_from_slice(&(k.len() as u32).to_le_bytes());
            hasher_buf.extend_from_slice(k);
            hasher_buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
            hasher_buf.extend_from_slice(v);
        }
        let hash = crc32(&hasher_buf);

        if errors.is_empty() {
            Ok(CheckReport {
                table: qual_name,
                op: "check".into(),
                status: "status".into(),
                hash,
                error_msg: None,
            })
        } else {
            Ok(CheckReport {
                table: qual_name,
                op: "check".into(),
                status: "error".into(),
                hash,
                error_msg: Some(errors.join("; ")),
            })
        }
    }

    /// Compute the deterministic logical CRC32 hash of a table.
    pub fn table_logical_hash(&self, session: &Session, table_name: &str) -> Result<u32> {
        let report = self.check_table(session, table_name)?;
        Ok(report.hash)
    }

    /// Compute the combined deterministic logical CRC32 hash across all tables
    /// in the current database.
    pub fn database_logical_hash(&self, session: &Session) -> Result<u32> {
        let prefix = format!("{}.", session.current_db);
        let tables_guard = self.tables.read().unwrap();
        let mut names: Vec<String> = Vec::new();
        for key in tables_guard.keys() {
            if let Some(rest) = key.strip_prefix(&prefix) {
                names.push(rest.to_string());
            } else if session.current_db == "default" && !key.contains('.') {
                names.push(key.clone());
            }
        }
        drop(tables_guard);
        names.sort();

        let mut combined_buf = Vec::new();
        for name in &names {
            let h = self.table_logical_hash(session, name)?;
            combined_buf.extend_from_slice(&(name.len() as u32).to_le_bytes());
            combined_buf.extend_from_slice(name.as_bytes());
            combined_buf.extend_from_slice(&h.to_le_bytes());
        }
        Ok(crc32(&combined_buf))
    }

    /// Perform comprehensive diagnostic integrity checks on all tables in the current
    /// database and return individual table reports plus a summary report (§48).
    pub fn check_database(&self, session: &Session) -> Result<Vec<CheckReport>> {
        let prefix = format!("{}.", session.current_db);
        let tables_guard = self.tables.read().unwrap();
        let mut names: Vec<String> = Vec::new();
        for key in tables_guard.keys() {
            if let Some(rest) = key.strip_prefix(&prefix) {
                names.push(rest.to_string());
            } else if session.current_db == "default" && !key.contains('.') {
                names.push(key.clone());
            }
        }
        drop(tables_guard);
        names.sort();

        let mut reports = Vec::with_capacity(names.len() + 1);
        let mut combined_buf = Vec::new();
        let mut any_error = false;

        for name in &names {
            let report = self.check_table(session, name)?;
            if !report.is_ok() {
                any_error = true;
            }
            combined_buf.extend_from_slice(&(name.len() as u32).to_le_bytes());
            combined_buf.extend_from_slice(name.as_bytes());
            combined_buf.extend_from_slice(&report.hash.to_le_bytes());
            reports.push(report);
        }

        let db_hash = crc32(&combined_buf);
        reports.push(CheckReport {
            table: format!("{}.*", session.current_db),
            op: "check_database".into(),
            status: if any_error { "error".into() } else { "status".into() },
            hash: db_hash,
            error_msg: if any_error {
                Some("one or more tables failed integrity checks".into())
            } else {
                None
            },
        });

        Ok(reports)
    }
}
