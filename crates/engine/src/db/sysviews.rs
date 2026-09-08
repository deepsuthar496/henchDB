//! Virtual system catalogs: `pg_catalog.*` and `information_schema.*`.
//!
//! Real PostgreSQL clients, GUIs, and ORMs (Prisma, DBeaver, SQLAlchemy,
//! `pgcli`, DataGrip, Metabase) open every session with introspection
//! queries (`SELECT version()`, `SELECT * FROM pg_catalog.pg_class WHERE
//! ...`). This module synthesizes those views on the fly from the live
//! table registry — no storage, no WAL, no persistence — as ephemeral
//! row-id tables, so the normal executor (WHERE, ORDER BY, LIMIT, JOIN,
//! GROUP BY, EXPLAIN, prepared statements) works over them untouched.
//!
//! Read-only by construction: the interception points are the SELECT-side
//! resolvers (`resolve_table_ref`, `inner_scope` in `subquery.rs`), which
//! return a throwaway ephemeral table without touching the session
//! materialization map. DML/DDL paths resolve through `Database::table`
//! directly and still fail with `TableNotFound`, so system views can
//! never be written.
//!
//! v1 boundaries (clean errors, documented): fixed PG-18-shaped column
//! sets (a subset of real `pg_catalog`); OIDs are deterministic per open
//! (`16384 + sorted index`, like PG user objects) but not stable across
//! restarts; system functions evaluate in the projection list only.

use std::sync::Arc;

use super::{subquery, Database, Output, Session};
use crate::error::{Error, Result};
use crate::sql::{SelectItem, SelectStmt};
use crate::table::{ColumnDef, Schema, Table};
use crate::types::{ColumnType, Datum};
use crate::{PRODUCT_NAME, VERSION};

/// First user-object OID, matching PostgreSQL convention.
const BASE_OID: i64 = 16384;

/// Split `schema.view` (case-insensitive, exactly two parts) into its
/// canonical lowercase pair. Anything else is not a system view.
fn split_sysview(name: &str) -> Option<(&str, &str, String, String)> {
    let mut parts = name.split('.');
    let (schema, view) = match (parts.next(), parts.next(), parts.next()) {
        (Some(s), Some(v), None) => (s, v),
        _ => return None,
    };
    let sl = schema.to_ascii_lowercase();
    let vl = view.to_ascii_lowercase();
    Some((schema, view, sl, vl))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SysView {
    PgNamespace,
    PgClass,
    PgType,
    PgAttribute,
    PgDatabase,
    Schemata,
    Tables,
    Columns,
}

fn sysview_kind(schema: &str, view: &str) -> Option<SysView> {
    match (schema, view) {
        ("pg_catalog", "pg_namespace") => Some(SysView::PgNamespace),
        ("pg_catalog", "pg_class") => Some(SysView::PgClass),
        ("pg_catalog", "pg_type") => Some(SysView::PgType),
        ("pg_catalog", "pg_attribute") => Some(SysView::PgAttribute),
        ("pg_catalog", "pg_database") => Some(SysView::PgDatabase),
        ("information_schema", "schemata") => Some(SysView::Schemata),
        ("information_schema", "tables") => Some(SysView::Tables),
        ("information_schema", "columns") => Some(SysView::Columns),
        _ => None,
    }
}

/// Resolve a FROM/JOIN source against the system catalogs: `Ok(Some)`
/// synthesizes the view, `Ok(None)` means "not a system view, resolve
/// normally". Unknown relations inside `pg_catalog` / `information_schema`
/// fail with `TableNotFound`, like PostgreSQL's "relation does not exist".
pub(crate) fn sysview_source(
    db: &Database,
    session: &Session,
    name: &str,
) -> Result<Option<Arc<Table>>> {
    let Some((_, _, sl, vl)) = split_sysview(name) else {
        return Ok(None);
    };
    if sl != "pg_catalog" && sl != "information_schema" {
        return Ok(None);
    }
    let kind = sysview_kind(&sl, &vl).ok_or_else(|| Error::TableNotFound(name.to_string()))?;
    let (schema, rows) = build_view(db, session, kind);
    let table = Arc::new(Table::new_ephemeral(format!("{sl}.{vl}"), schema));
    for (i, row) in rows.iter().enumerate() {
        table.append_ephemeral(i as u64, row)?;
    }
    Ok(Some(table))
}

/// Zero-argument system functions (`SELECT version()`). `None` = not a
/// system function; the executor rejects unknown names cleanly.
pub(crate) fn eval_sysfunc(session: &Session, name: &str) -> Option<Datum> {
    match name.to_ascii_lowercase().as_str() {
        "current_schema" => Some(Datum::Text("public".into())),
        "current_database" => Some(Datum::Text(session.current_db.clone())),
        "version" => Some(Datum::Text(format!(
            "PostgreSQL 18.0 ({PRODUCT_NAME} {VERSION})"
        ))),
        "user" => Some(Datum::Text(session.user.clone())),
        _ => None,
    }
}

/// PostgreSQL type OID for one engine column type.
fn pg_oid_for(ctype: ColumnType) -> i64 {
    match ctype {
        ColumnType::Bool => 16,
        ColumnType::BigInt => 20,
        ColumnType::Int => 23,
        ColumnType::Text | ColumnType::VarChar => 25,
        ColumnType::Float => 700,
        ColumnType::Double => 701,
        ColumnType::DateTime => 1114,
        ColumnType::Timestamp => 1184,
    }
}

fn col(name: &str, ctype: ColumnType) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        ctype,
        nullable: true,
        auto_increment: false,
        default_value: None,
    }
}

/// Live user tables as `(db, name, table)`, sorted for deterministic OIDs.
fn live_tables(db: &Database) -> Vec<(String, String, Arc<Table>)> {
    let guard = db.tables.read().unwrap();
    let mut out: Vec<(String, String, Arc<Table>)> = Vec::with_capacity(guard.len());
    for (key, t) in guard.iter() {
        if t.is_ephemeral() {
            continue;
        }
        let (d, n) = match key.split_once('.') {
            Some((d, n)) => (d.to_string(), n.to_string()),
            None => ("default".to_string(), key.clone()),
        };
        out.push((d, n, t.clone()));
    }
    drop(guard);
    out.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
    out
}

fn live_databases(db: &Database) -> Vec<String> {
    let guard = db.databases.read().unwrap();
    let mut out: Vec<String> = guard.iter().cloned().collect();
    drop(guard);
    out.sort();
    out
}

fn build_view(db: &Database, session: &Session, kind: SysView) -> (Schema, Vec<Vec<Datum>>) {
    match kind {
        SysView::PgNamespace => {
            let schema = Schema {
                columns: vec![
                    col("oid", ColumnType::Int),
                    col("nspname", ColumnType::Text),
                    col("nspowner", ColumnType::Int),
                ],
                pk_idx: 0,
            };
            let rows = vec![
                vec![Datum::Int(11), Datum::Text("public".into()), Datum::Int(10)],
                vec![Datum::Int(22), Datum::Text("pg_catalog".into()), Datum::Int(10)],
                vec![
                    Datum::Int(33),
                    Datum::Text("information_schema".into()),
                    Datum::Int(10),
                ],
            ];
            (schema, rows)
        }
        SysView::PgClass => {
            let schema = Schema {
                columns: vec![
                    col("oid", ColumnType::Int),
                    col("relname", ColumnType::Text),
                    col("relnamespace", ColumnType::Int),
                    col("relkind", ColumnType::Text),
                    col("reltuples", ColumnType::Int),
                ],
                pk_idx: 0,
            };
            let mut rows = Vec::new();
            for (i, (_, name, t)) in live_tables(db).iter().enumerate() {
                rows.push(vec![
                    Datum::Int(BASE_OID + i as i64),
                    Datum::Text(name.clone()),
                    Datum::Int(11),
                    Datum::Text("r".into()),
                    Datum::Int(t.tree().entry_count() as i64),
                ]);
            }
            (schema, rows)
        }
        SysView::PgType => {
            let schema = Schema {
                columns: vec![
                    col("oid", ColumnType::Int),
                    col("typname", ColumnType::Text),
                    col("typnamespace", ColumnType::Int),
                ],
                pk_idx: 0,
            };
            let rows = [
                (16, "bool"),
                (20, "int8"),
                (23, "int4"),
                (25, "text"),
                (700, "float4"),
                (701, "float8"),
                (1114, "timestamp"),
                (1184, "timestamptz"),
            ]
            .into_iter()
            .map(|(oid, name)| {
                vec![
                    Datum::Int(oid),
                    Datum::Text(name.into()),
                    Datum::Int(11),
                ]
            })
            .collect();
            (schema, rows)
        }
        SysView::PgAttribute => {
            let schema = Schema {
                columns: vec![
                    col("attrelid", ColumnType::Int),
                    col("attname", ColumnType::Text),
                    col("atttypid", ColumnType::Int),
                    col("attnum", ColumnType::Int),
                    col("attnotnull", ColumnType::Bool),
                ],
                pk_idx: 0,
            };
            let mut rows = Vec::new();
            for (i, (_, _, t)) in live_tables(db).iter().enumerate() {
                let relid = BASE_OID + i as i64;
                for (n, c) in t.schema().columns.iter().enumerate() {
                    rows.push(vec![
                        Datum::Int(relid),
                        Datum::Text(c.name.clone()),
                        Datum::Int(pg_oid_for(c.ctype)),
                        Datum::Int(n as i64 + 1),
                        Datum::Bool(!c.nullable),
                    ]);
                }
            }
            (schema, rows)
        }
        SysView::PgDatabase => {
            let schema = Schema {
                columns: vec![col("oid", ColumnType::Int), col("datname", ColumnType::Text)],
                pk_idx: 0,
            };
            let rows = live_databases(db)
                .into_iter()
                .enumerate()
                .map(|(i, name)| vec![Datum::Int(BASE_OID + i as i64), Datum::Text(name)])
                .collect();
            (schema, rows)
        }
        SysView::Schemata => {
            let schema = Schema {
                columns: vec![col("schema_name", ColumnType::Text)],
                pk_idx: 0,
            };
            let rows = ["public", "pg_catalog", "information_schema"]
                .into_iter()
                .map(|n| vec![Datum::Text(n.into())])
                .collect();
            (schema, rows)
        }
        SysView::Tables => {
            let schema = Schema {
                columns: vec![
                    col("table_catalog", ColumnType::Text),
                    col("table_schema", ColumnType::Text),
                    col("table_name", ColumnType::Text),
                    col("table_type", ColumnType::Text),
                ],
                pk_idx: 2,
            };
            let mut rows = Vec::new();
            for (d, name, _) in live_tables(db) {
                // The current database is the query's catalog; other
                // databases' tables are still listed under their own name
                // so GUI tree views stay complete.
                let catalog = if d == session.current_db {
                    session.current_db.clone()
                } else {
                    d.clone()
                };
                rows.push(vec![
                    Datum::Text(catalog),
                    Datum::Text("public".into()),
                    Datum::Text(name),
                    Datum::Text("BASE TABLE".into()),
                ]);
            }
            (schema, rows)
        }
        SysView::Columns => {
            let schema = Schema {
                columns: vec![
                    col("table_catalog", ColumnType::Text),
                    col("table_schema", ColumnType::Text),
                    col("table_name", ColumnType::Text),
                    col("column_name", ColumnType::Text),
                    col("ordinal_position", ColumnType::Int),
                    col("data_type", ColumnType::Text),
                    col("is_nullable", ColumnType::Text),
                ],
                pk_idx: 3,
            };
            let mut rows = Vec::new();
            for (d, name, t) in live_tables(db) {
                let catalog = if d == session.current_db {
                    session.current_db.clone()
                } else {
                    d.clone()
                };
                for (n, c) in t.schema().columns.iter().enumerate() {
                    rows.push(vec![
                        Datum::Text(catalog.clone()),
                        Datum::Text("public".into()),
                        Datum::Text(name.clone()),
                        Datum::Text(c.name.clone()),
                        Datum::Int(n as i64 + 1),
                        Datum::Text(c.ctype.name().to_ascii_lowercase()),
                        Datum::Text(if c.nullable { "YES".into() } else { "NO".into() }),
                    ]);
                }
            }
            (schema, rows)
        }
    }
}

// ---------------------------------------------------------------------------
// FROM-less SELECT serving (lives here, not in `query.rs`, per the 1,500-line
// file ceiling: the single-row evaluator + describer for `TableRef::Empty`).
// ---------------------------------------------------------------------------

/// FROM-less SELECT: evaluate one row of literals, system functions, and
/// uncorrelated scalar subqueries (correlated ones fail cleanly as unknown
/// columns, like in global aggregates).
pub(super) fn exec_select_nofrom(
    db: &Database,
    session: &mut Session,
    items: Vec<SelectItem>,
    limit: Option<usize>,
) -> Result<Output> {
    let mut out_columns = Vec::with_capacity(items.len());
    let mut row = Vec::with_capacity(items.len());
    for item in &items {
        match item {
            SelectItem::Literal(d) => {
                out_columns.push(d.to_string());
                row.push(d.clone());
            }
            SelectItem::SysFunc { name, alias } => {
                let d = eval_sysfunc(session, name).ok_or_else(|| {
                    Error::NotSupported(format!("unknown function '{name}'"))
                })?;
                out_columns.push(alias.clone().unwrap_or_else(|| name.to_ascii_lowercase()));
                row.push(d);
            }
            SelectItem::Subquery { query, alias } => {
                let d = subquery::eval_scalar_uncorrelated(db, session, query)?;
                let nm =
                    alias.clone().unwrap_or_else(|| subquery::projection_scalar_name(query, None));
                out_columns.push(nm);
                row.push(d);
            }
            other => {
                return Err(Error::NotSupported(format!("{other:?} requires FROM")));
            }
        }
    }
    let rows = if limit == Some(0) { vec![] } else { vec![row] };
    Ok(Output {
        columns: out_columns,
        rows,
        message: "OK".into(),
    })
}

/// Describe a FROM-less projection: literals, system functions, and scalar
/// subqueries only (anything addressing a table needs FROM).
pub(super) fn describe_nofrom(
    db: &Database,
    session: &Session,
    tmp: &mut Session,
    items: &[SelectItem],
) -> Result<Vec<(String, ColumnType)>> {
    let mut cols = Vec::with_capacity(items.len());
    for item in items {
        match item {
            SelectItem::Literal(d) => cols.push((
                d.to_string(),
                match d {
                    Datum::Int(_) => ColumnType::BigInt,
                    Datum::Float(_) => ColumnType::Double,
                    Datum::Text(_) => ColumnType::Text,
                    Datum::Bool(_) => ColumnType::Bool,
                    Datum::DateTime(_) => ColumnType::DateTime,
                    Datum::Null => ColumnType::Text,
                },
            )),
            SelectItem::SysFunc { name, alias } => {
                if eval_sysfunc(session, name).is_none() {
                    return Err(Error::NotSupported(format!("unknown function '{name}'")));
                }
                cols.push((
                    alias.clone().unwrap_or_else(|| name.to_ascii_lowercase()),
                    ColumnType::Text,
                ));
            }
            SelectItem::Subquery { query, alias } => {
                cols.push(describe_subquery_item(db, session, tmp, query, alias)?)
            }
            other => {
                return Err(Error::NotSupported(format!("{other:?} requires FROM")));
            }
        }
    }
    Ok(cols)
}

/// Describe one scalar-subquery projection item (shared by table and
/// FROM-less describe paths).
pub(super) fn describe_subquery_item(
    db: &Database,
    session: &Session,
    tmp: &mut Session,
    query: &SelectStmt,
    alias: &Option<String>,
) -> Result<(String, ColumnType)> {
    let inner = db.describe_select(session, tmp, &query.items, &query.from, &query.joins)?;
    if inner.len() != 1 {
        return Err(Error::InvalidQuery(
            "Subquery must return only one column".into(),
        ));
    }
    let name = alias.clone().unwrap_or_else(|| inner[0].0.clone());
    Ok((name, inner[0].1))
}
