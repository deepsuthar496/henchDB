//! Database and schema DDL: databases, tables, and secondary indexes.
//!
//! All DDL is autocommit and serialized through the commit lock via
//! `Database::wal_commit` (defined in the parent module). Methods are
//! `pub(super)` so only `db/mod.rs`'s statement dispatcher calls them;
//! zero-copy and error behavior are unchanged from when they lived inline.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::{Database, Output, Session};
use crate::error::{Error, Result};
use crate::table::{Schema, Table, TableDef};
use crate::types::ColumnType;
use crate::wal::Record;

impl Database {
    // -- Database DDL ---------------------------------------------------

    pub(super) fn exec_create_database(&self, name: &str, if_not_exists: bool) -> Result<Output> {
        let mut dbs = self.databases.write().unwrap();
        if dbs.contains(name) {
            if if_not_exists {
                return Ok(Output::ok(format!("database '{name}' exists")));
            }
            return Err(Error::DatabaseExists(name.to_string()));
        }
        dbs.insert(name.to_string());
        drop(dbs);

        let txn = self.next_txn.fetch_add(1, Ordering::Relaxed);
        self.wal_commit(vec![
            Record::CreateDatabase {
                txn,
                name: name.to_string(),
            },
            Record::Commit { txn },
        ])?;
        Ok(Output::ok(format!("database '{name}' created")))
    }

    pub(super) fn exec_drop_database(&self, name: &str, if_exists: bool) -> Result<Output> {
        let mut dbs = self.databases.write().unwrap();
        if !dbs.contains(name) {
            if if_exists {
                return Ok(Output::ok(format!("database '{name}' does not exist")));
            }
            return Err(Error::DatabaseNotFound(name.to_string()));
        }
        dbs.remove(name);
        drop(dbs);

        let prefix = format!("{name}.");
        {
            let mut tables = self.tables.write().unwrap();
            tables.retain(|k, _| !k.starts_with(&prefix));
        }
        self.purge_db_versions(&prefix);

        let txn = self.next_txn.fetch_add(1, Ordering::Relaxed);
        self.wal_commit(vec![
            Record::DropDatabase {
                txn,
                name: name.to_string(),
            },
            Record::Commit { txn },
        ])?;
        Ok(Output::ok(format!("database '{name}' dropped")))
    }

    pub(super) fn exec_use_database(&self, session: &mut Session, name: &str) -> Result<Output> {
        let dbs = self.databases.read().unwrap();
        if !dbs.contains(name) {
            return Err(Error::DatabaseNotFound(name.to_string()));
        }
        session.current_db = name.to_string();
        Ok(Output::ok("Database changed"))
    }

    // -- DDL (autocommit) ------------------------------------------------

    pub(super) fn exec_create_table(
        &self,
        session: &Session,
        name: String,
        columns: Vec<crate::sql::ColumnSpec>,
        foreign_keys: Vec<crate::sql::ForeignKeySpec>,
    ) -> Result<Output> {
        let key = if name.contains('.') { name.clone() } else { format!("{}.{name}", session.current_db) };
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
                default_value: c.default_value,
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
        // Validate FKs (parent must exist unless self-ref), then auto-index
        // every FK column MySQL-style before the table becomes visible.
        let bare = TableDef {
            name: key.clone(),
            schema: Schema {
                columns: defs,
                pk_idx,
            },
            indexes: Vec::new(),
            foreign_keys: Vec::new(),
        };
        let fks = self.fk_build_defs(session, &key, &name, &bare, foreign_keys)?;
        let mut def = bare;
        def.foreign_keys = fks;
        let table = Arc::new(Table::new(def.clone()));
        Self::fk_ensure_auto_indexes(&table)?;
        def.indexes = table.secondary_indexes();
        {
            let mut guard = self.tables.write().unwrap();
            if guard.contains_key(&key) {
                return Err(Error::TableExists(name));
            }
            table.set_pool(self.pool.clone());
            table.set_epoch_manager(self.epoch.clone());
            guard.insert(key, table);
        }
        let txn = self.next_txn.fetch_add(1, Ordering::Relaxed);
        self.wal_commit(vec![
            Record::CreateTable { txn, def },
            Record::Commit { txn },
        ])?;
        Ok(Output::ok(format!("table '{name}' created")))
    }

    pub(super) fn exec_drop_table(&self, session: &Session, name: &str) -> Result<Output> {
        let key = self.resolve_table_key(session, name);
        // A referenced parent cannot be dropped (schema-level RESTRICT).
        self.fk_check_drop(&key, name)?;
        {
            let mut guard = self.tables.write().unwrap();
            guard.remove(&key).ok_or_else(|| Error::TableNotFound(name.to_string()))?;
        }
        self.purge_table_versions(&key);
        let txn = self.next_txn.fetch_add(1, Ordering::Relaxed);
        self.wal_commit(vec![
            Record::DropTable {
                txn,
                name: key,
            },
            Record::Commit { txn },
        ])?;
        Ok(Output::ok(format!("table '{name}' dropped")))
    }

    pub(super) fn exec_create_index(
        &self,
        session: &Session,
        name: String,
        table_name: String,
        column: String,
    ) -> Result<Output> {
        let table = self.table(session, &table_name)?;
        let key = self.resolve_table_key(session, &table_name);
        table.add_index(name.clone(), column.clone())?;
        let txn = self.next_txn.fetch_add(1, Ordering::Relaxed);
        self.wal_commit(vec![
            Record::CreateIndex {
                txn,
                table: key,
                name: name.clone(),
                column,
            },
            Record::Commit { txn },
        ])?;
        Ok(Output::ok(format!("index '{name}' created on '{table_name}'")))
    }

    pub(super) fn exec_drop_index(&self, session: &Session, name: String, table_name: String) -> Result<Output> {
        let table = self.table(session, &table_name)?;
        let key = self.resolve_table_key(session, &table_name);
        // FK columns keep their last covering index.
        self.fk_check_drop_index(&table, &name)?;
        table.drop_index(&name)?;
        let txn = self.next_txn.fetch_add(1, Ordering::Relaxed);
        self.wal_commit(vec![
            Record::DropIndex {
                txn,
                table: key,
                name: name.clone(),
            },
            Record::Commit { txn },
        ])?;
        Ok(Output::ok(format!("index '{name}' dropped from '{table_name}'")))
    }
}
