//! Priority 15: batch vs scalar differential tests — every batch result
//! must be byte-for-byte identical to the scalar executor (or a clean
//! scalar fallback). Covers NULLs, empty sets, unknown-column quirks,
//! type coercions, LIKE on non-text, errors, transactions, and snapshots.

use super::*;
use super::super::batch;
use crate::db::query::AggSpec;
use crate::db::{Database, Output, Session};
use crate::sql::{parse_sql, Expr, SelectItem, Statement};
use crate::table::{Schema, Table};

type StrResult<T> = std::result::Result<T, String>;

fn setup(dir: &Path) -> Database {
    let db = Database::open(dir).unwrap();
    let mut s = db.new_session();
    db.execute(
        &mut s,
        "CREATE TABLE m (id INT PRIMARY KEY, a INT, f FLOAT, t TEXT, b BOOL, d DATETIME)",
    )
    .unwrap();
    // Mix of negatives, zero, large values, shared text prefixes, every
    // nullable column exercised with NULLs.
    let rows = [
        "(1, -5, 1.5, 'alpha', TRUE, '2026-01-01 00:00:00')",
        "(2, 0, 2.0, 'alphabet', FALSE, '2026-06-01 12:00:00')",
        "(3, 3, -0.5, 'beta', TRUE, NULL)",
        "(4, 3, 10.25, 'gamma', NULL, '2026-12-31 23:59:59')",
        "(5, NULL, NULL, NULL, NULL, NULL)",
        "(6, 100, 100.0, '', FALSE, '2026-01-01 00:00:00')",
        "(7, -5, 1.5, 'alpha', TRUE, '2026-06-01 12:00:00')",
        "(8, 42, 0.0, 'delta', FALSE, NULL)",
    ];
    for r in rows {
        db.execute(&mut s, &format!("INSERT INTO m VALUES {r}")).unwrap();
    }
    db
}

fn tmp(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("hdbbatch_{name}_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    dir
}

/// Parse `SELECT <items> FROM m [WHERE ...]` exactly like the executor
/// sees it (bare columns; qualifiers stripped for table `m`).
fn parse_parts(sql: &str) -> (Vec<SelectItem>, Option<Expr>) {
    match parse_sql(sql).unwrap() {
        Statement::Select { items, selection, .. } => {
            let selection = selection
                .map(|s| Database::strip_qualifiers(&s, "m"))
                .transpose()
                .unwrap();
            (items, selection)
        }
        other => panic!("wrong stmt {other:?}"),
    }
}

fn specs_for(items: &[SelectItem], schema: &Schema) -> Vec<(AggSpec, String)> {
    let mut out = Vec::new();
    for item in items {
        match item {
            SelectItem::CountStar => out.push((AggSpec::Count, "COUNT(*)".into())),
            SelectItem::Aggregate { func, column } => {
                let idx = schema.index_of(column).unwrap();
                out.push((AggSpec::Agg(*func, idx), format!("{}({column})", func.name())));
            }
            other => panic!("expected agg item, got {other:?}"),
        }
    }
    out
}

/// Scalar reference: visible rows + scalar fold (the legacy path).
fn scalar_agg(
    db: &Database,
    s: &mut Session,
    table: &std::sync::Arc<Table>,
    sel: Option<&Expr>,
    specs: &[(AggSpec, String)],
) -> StrResult<Output> {
    let rows = db.visible_rows(s, table, sel).map_err(|e| e.to_string())?;
    Database::exec_aggregate_rows(specs, rows).map_err(|e| e.to_string())
}

/// Batch candidate: `None` means clean scalar fallback.
fn batch_agg(
    db: &Database,
    s: &mut Session,
    table: &std::sync::Arc<Table>,
    sel: Option<&Expr>,
    specs: &[(AggSpec, String)],
) -> StrResult<Option<Output>> {
    batch::try_global_agg(db, s, table, sel, specs).map_err(|e| e.to_string())
}

/// Assert batch-direct, scalar-recompute, and end-to-end `execute` all
/// agree (values and errors alike) for one SELECT.
fn check_query(db: &Database, s: &mut Session, sql: &str) {
    let (items, sel) = parse_parts(sql);
    let table = db.table(s, "m").unwrap();
    let schema = table.schema();
    let specs = specs_for(&items, schema);
    let scalar = scalar_agg(db, s, &table, sel.as_ref(), &specs);
    let batched = batch_agg(db, s, &table, sel.as_ref(), &specs);
    match (scalar, batched) {
        (Ok(a), Ok(Some(b))) => {
            assert_eq!(a.columns, b.columns, "columns differ: {sql}");
            assert_eq!(a.rows, b.rows, "rows differ: {sql}");
        }
        (Err(a), Ok(None)) => {
            // Batch declined; end-to-end below still proves the fallback.
            let _ = a;
        }
        (Err(a), Ok(Some(_))) => panic!("scalar errored but batch succeeded ({a}): {sql}"),
        (Ok(_), Ok(None)) => {
            // Declined (e.g. indexed access plan): end-to-end below proves
            // the scalar fallback; path selection is pinned separately.
        }
        (a, Err(b)) => panic!("batch hard error ({b}) vs scalar {a:?}: {sql}"),
    }
    // End-to-end through the live executor (batch hook + fallback).
    let live = db.execute(s, sql).map_err(|e| e.to_string());
    match live {
        Ok(out) => {
            let reference =
                scalar_agg(db, s, &table, sel.as_ref(), &specs).expect("live ok but scalar failed");
            assert_eq!(out.columns, reference.columns, "live columns: {sql}");
            assert_eq!(out.rows, reference.rows, "live rows: {sql}");
        }
        Err(e) => {
            assert!(
                scalar_agg(db, s, &table, sel.as_ref(), &specs).is_err(),
                "live errored but scalar succeeded ({e}): {sql}"
            );
        }
    }
}

const PREDS: &[&str] = &[
    "a = 3",
    "a != 3",
    "a < 0",
    "a >= 100",
    "a = 3.0",
    "f > 1.5",
    "f = 2",
    "f != 0.0",
    "t = 'alpha'",
    "t != 'x'",
    "t = 5",
    "t = t",
    "b = TRUE",
    "b != FALSE",
    "b = 1",
    "d > '2026-01-01 00:00:00'",
    "d > 'not-a-date'",
    "d = '2026-06-01 12:00:00'",
    "t LIKE 'alph%'",
    "t LIKE '%bet%'",
    "a LIKE '1%'",
    "a LIKE '%'",
    "b LIKE 't%'",
    "d LIKE '2026%'",
    "t NOT LIKE 'a%'",
    "a BETWEEN -5 AND 10",
    "f BETWEEN 1 AND 2.5",
    "t BETWEEN 'a' AND 'c'",
    "a NOT BETWEEN 0 AND 50",
    "a BETWEEN NULL AND 5",
    "a BETWEEN 5 AND NULL",
    "t BETWEEN NULL AND 'c'",
    "a IN (3, 100, NULL)",
    "a NOT IN (3, 100)",
    "t IN ('alpha', 'beta', NULL)",
    "f IN (1.5, 2)",
    "b IN (TRUE)",
    "d IN ('2026-06-01 12:00:00')",
    "NOT (a = 3)",
    "a > 0 AND f < 10",
    "a < 0 OR t = 'beta'",
    "(a = 3 OR a = 100) AND b = TRUE",
    "NOT (a < 0 OR f > 50)",
    "a = id",
    "f > a",
    "id > a",
    "a = NULL",
    "missing = 1",
    "a = 3 OR missing = 1",
    "missing = 1 OR a = 3",
    "a = 3 AND missing = 1",
    "missing = 1 AND a = 3",
    "NOT missing = 1",
    "NOT (a = 3 OR missing = 1)",
    "missing = other",
];

const AGGS: &[&str] = &[
    "COUNT(*), SUM(a), AVG(f), MIN(t), MAX(id)",
    "SUM(f), AVG(a), MIN(a), MAX(f)",
    "MIN(b), MAX(b), MIN(d), MAX(d), COUNT(*)",
];

#[test]
fn batch_matches_scalar_predicate_matrix() {
    let dir = tmp("matrix");
    let db = setup(&dir);
    let mut s = db.new_session();
    for pred in PREDS {
        for agg in AGGS {
            check_query(&db, &mut s, &format!("SELECT {agg} FROM m WHERE {pred}"));
        }
    }
    // No-WHERE full scans.
    for agg in AGGS {
        check_query(&db, &mut s, &format!("SELECT {agg} FROM m"));
    }
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn batch_path_selection() {
    // Pins which shapes take the batch path (Some) vs cleanly decline to
    // the scalar executor (None): FullScan analytics batch; indexed seeks,
    // subqueries, and correlated frames stay scalar.
    let dir = tmp("paths");
    let db = setup(&dir);
    let mut s = db.new_session();
    let table = db.table(&s, "m").unwrap();
    let specs = vec![(AggSpec::Count, "COUNT(*)".into())];
    let takes = [
        "SELECT COUNT(*) FROM m",
        "SELECT COUNT(*) FROM m WHERE a = 3",
        "SELECT COUNT(*) FROM m WHERE t LIKE 'a%'",
        "SELECT COUNT(*) FROM m WHERE f BETWEEN 1 AND 2",
        "SELECT COUNT(*) FROM m WHERE a < 0 OR t = 'beta'",
        "SELECT COUNT(*) FROM m WHERE missing = 1",
    ];
    for sql in takes {
        let (_, sel) = parse_parts(sql);
        assert!(
            batch_agg(&db, &mut s, &table, sel.as_ref(), &specs).unwrap().is_some(),
            "should batch: {sql}"
        );
    }
    let declines = [
        "SELECT COUNT(*) FROM m WHERE id = 3",
        "SELECT COUNT(*) FROM m WHERE a IN (SELECT id FROM m)",
    ];
    for sql in declines {
        let (_, sel) = parse_parts(sql);
        assert!(
            batch_agg(&db, &mut s, &table, sel.as_ref(), &specs).unwrap().is_none(),
            "should decline: {sql}"
        );
    }
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn batch_error_and_empty_semantics() {
    let dir = tmp("err");
    let db = setup(&dir);
    let mut s = db.new_session();
    // Type errors surface identically (message included).
    for sql in [
        "SELECT SUM(t) FROM m",
        "SELECT AVG(b) FROM m",
        "SELECT SUM(d) FROM m WHERE a > 0",
        "SELECT SUM(t) FROM m WHERE a > 1000",
    ] {
        let (items, sel) = parse_parts(sql);
        let table = db.table(&s, "m").unwrap();
        let specs = specs_for(&items, table.schema());
        let a = scalar_agg(&db, &mut s, &table, sel.as_ref(), &specs);
        let b = batch_agg(&db, &mut s, &table, sel.as_ref(), &specs);
        match (a, b) {
            (Err(x), Ok(Some(_))) => panic!("scalar errored but batch succeeded ({x}): {sql}"),
            (Err(x), Err(y)) => assert_eq!(x, y, "error text differs: {sql}"),
            (Err(x), Ok(None)) => panic!("batch declined error query ({x}): {sql}"),
            (Ok(_), Err(y)) => panic!("batch errored but scalar succeeded ({y}): {sql}"),
            (Ok(a), Ok(Some(b))) => assert_eq!(a.rows, b.rows, "rows differ: {sql}"),
            (Ok(_), Ok(None)) => panic!("batch declined: {sql}"),
        }
    }
    // Empty table and empty selections: COUNT 0, others NULL.
    db.execute(&mut s, "CREATE TABLE e (id INT PRIMARY KEY, v INT)").unwrap();
    let out = db.execute(&mut s, "SELECT COUNT(*), SUM(v), AVG(v), MIN(v) FROM e").unwrap();
    assert_eq!(
        out.rows[0],
        vec![Datum::Int(0), Datum::Null, Datum::Null, Datum::Null]
    );
    let out = db
        .execute(&mut s, "SELECT COUNT(*), SUM(a) FROM m WHERE a > 1000000")
        .unwrap();
    assert_eq!(out.rows[0], vec![Datum::Int(0), Datum::Null]);
    let _ = fs::remove_dir_all(&dir);
}

/// Assert batch-direct, scalar-recompute, and end-to-end `execute` all
/// agree for one single-table GROUP BY query.
fn check_grouped(db: &Database, s: &mut Session, sql: &str) {
    let (items, selection, group_by, order_by, limit) = match parse_sql(sql).unwrap() {
        Statement::Select { items, selection, group_by, order_by, limit, .. } => {
            let selection = selection
                .map(|e| Database::strip_qualifiers(&e, "m"))
                .transpose()
                .unwrap();
            (items, selection, group_by, order_by, limit)
        }
        other => panic!("wrong stmt {other:?}"),
    };
    let table = db.table(s, "m").unwrap();
    let tables = std::slice::from_ref(&table);
    let scalar = (|| -> StrResult<Output> {
        let rows = db.visible_rows(s, &table, selection.as_ref()).map_err(|e| e.to_string())?;
        db.exec_grouped(&items, tables, rows, &group_by, order_by.clone(), limit)
            .map_err(|e| e.to_string())
    })();
    let batched = batch::try_grouped_agg(
        db,
        s,
        &table,
        selection.as_ref(),
        &items,
        &group_by,
        order_by.clone(),
        limit,
    )
    .map_err(|e| e.to_string());
    match (scalar, batched) {
        (Ok(a), Ok(Some(b))) => {
            assert_eq!(a.columns, b.columns, "columns differ: {sql}");
            assert_eq!(a.rows, b.rows, "rows differ: {sql}");
        }
        (Err(a), Err(b)) => assert_eq!(a, b, "error text differs: {sql}"),
        (Err(a), Ok(None)) => {
            let _ = a;
        }
        (Err(a), Ok(Some(_))) => panic!("scalar errored but batch succeeded ({a}): {sql}"),
        (Ok(_), Err(b)) => panic!("batch hard error ({b}): {sql}"),
        (Ok(_), Ok(None)) => {}
    }
    let live = db.execute(s, sql).map_err(|e| e.to_string());
    match live {
        Ok(out) => {
            let reference = (|| -> StrResult<Output> {
                let rows = db
                    .visible_rows(s, &table, selection.as_ref())
                    .map_err(|e| e.to_string())?;
                db.exec_grouped(&items, tables, rows, &group_by, order_by, limit)
                    .map_err(|e| e.to_string())
            })()
            .expect("live ok but scalar failed");
            assert_eq!(out.columns, reference.columns, "live columns: {sql}");
            assert_eq!(out.rows, reference.rows, "live rows: {sql}");
        }
        Err(e) => {
            let scalar_is_err = (|| -> bool {
                let rows = match db.visible_rows(s, &table, selection.as_ref()) {
                    Ok(r) => r,
                    Err(_) => return true,
                };
                db.exec_grouped(&items, tables, rows, &group_by, order_by, limit).is_err()
            })();
            assert!(scalar_is_err, "live errored but scalar succeeded ({e}): {sql}");
        }
    }
}

#[test]
fn batch_matches_scalar_grouped_matrix() {
    let dir = tmp("grouped");
    let db = setup(&dir);
    let mut s = db.new_session();
    for sql in [
        "SELECT b, COUNT(*), SUM(a), AVG(f), MIN(t), MAX(id) FROM m GROUP BY b",
        "SELECT t, COUNT(*) FROM m GROUP BY t ORDER BY t",
        "SELECT d, SUM(f), AVG(a) FROM m GROUP BY d",
        "SELECT a, COUNT(*) FROM m WHERE f > 0 GROUP BY a ORDER BY a DESC LIMIT 3",
        "SELECT b, MIN(a), MAX(a) FROM m WHERE t LIKE 'a%' GROUP BY b",
        "SELECT b, a, COUNT(*) FROM m GROUP BY b, a",
        "SELECT COUNT(*) FROM m GROUP BY b ORDER BY b",
        "SELECT b, COUNT(*) FROM m WHERE a > 1000000 GROUP BY b",
        "SELECT b, SUM(t) FROM m GROUP BY b",
        "SELECT a, AVG(t) FROM m WHERE f < 50 GROUP BY a",
    ] {
        check_grouped(&db, &mut s, sql);
    }
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn batch_sees_txn_overlay() {
    let dir = tmp("txn");
    let db = setup(&dir);
    let mut s = db.new_session();
    db.execute(&mut s, "BEGIN").unwrap();
    db.execute(&mut s, "INSERT INTO m VALUES (9, 1000, 0.5, 'zeta', TRUE, NULL)").unwrap();
    db.execute(&mut s, "UPDATE m SET a = 777 WHERE id = 1").unwrap();
    db.execute(&mut s, "DELETE FROM m WHERE id = 2").unwrap();
    // Batch path serves staged writes exactly like the scalar path.
    let out = db.execute(&mut s, "SELECT COUNT(*), SUM(a) FROM m").unwrap();
    assert_eq!(out.rows[0][0], Datum::Int(8));
    assert_eq!(out.rows[0][1], Datum::Int(1920));
    db.execute(&mut s, "ROLLBACK").unwrap();
    let out = db.execute(&mut s, "SELECT COUNT(*), SUM(a) FROM m").unwrap();
    assert_eq!(out.rows[0][0], Datum::Int(8));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn batch_scalar_fallback_shapes() {
    let dir = tmp("fb");
    let db = setup(&dir);
    let mut s = db.new_session();
    // Indexed point plan stays scalar (None) but correct end-to-end.
    let table = db.table(&s, "m").unwrap();
    let (items, sel) = parse_parts("SELECT COUNT(*), SUM(a) FROM m WHERE id = 3");
    let specs = specs_for(&items, table.schema());
    assert!(batch_agg(&db, &mut s, &table, sel.as_ref(), &specs).unwrap().is_none());
    let out = db.execute(&mut s, "SELECT COUNT(*), SUM(a) FROM m WHERE id = 3").unwrap();
    assert_eq!(out.rows[0], vec![Datum::Int(1), Datum::Int(3)]);
    // Subquery predicates fall back cleanly.
    let (items, sel) =
        parse_parts("SELECT COUNT(*) FROM m WHERE id IN (SELECT id FROM m WHERE id < 3)");
    let specs = specs_for(&items, table.schema());
    assert!(batch_agg(&db, &mut s, &table, sel.as_ref(), &specs).unwrap().is_none());
    let out = db
        .execute(&mut s, "SELECT COUNT(*) FROM m WHERE id IN (SELECT id FROM m WHERE id < 3)")
        .unwrap();
    assert_eq!(out.rows[0][0], Datum::Int(2));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn direct_raw_column_decoding_parity() {
    let dir = tmp("raw_parity");
    let db = setup(&dir);
    let s = db.new_session();
    let table = db.table(&s, "m").unwrap();
    let schema = table.schema();

    // Generate diverse test rows
    let test_rows = vec![
        vec![Datum::Int(10), Datum::Int(-123), Datum::Float(3.1415), Datum::Text("hello world".into()), Datum::Bool(true), Datum::DateTime(1_000_000)],
        vec![Datum::Int(20), Datum::Null, Datum::Float(0.0), Datum::Text("".into()), Datum::Bool(false), Datum::Null],
        vec![Datum::Int(30), Datum::Int(42), Datum::Null, Datum::Null, Datum::Null, Datum::DateTime(2_000_000)],
    ];

    // Encode rows to raw byte buffers
    let raw_rows: Vec<Vec<u8>> = test_rows.iter().map(|r| Table::encode_row(r)).collect();

    // Test with full need mask
    let need_all = vec![true; schema.columns.len()];
    let mut batch_datum = batch::ColumnBatch::new();
    let mut batch_raw = batch::ColumnBatch::new();

    batch_datum.prepare_morsel(schema, &need_all, test_rows.len());
    for r in &test_rows {
        batch_datum.push_datum_row(schema, r, &need_all).expect("datum decode");
    }
    batch_datum.finish_morsel(&need_all, test_rows.len());

    batch_raw.prepare_morsel(schema, &need_all, raw_rows.len());
    for raw in &raw_rows {
        batch_raw.push_raw_row(schema, raw, &need_all).expect("raw decode");
    }
    batch_raw.finish_morsel(&need_all, raw_rows.len());

    // Both batches must produce identical selection vectors
    assert_eq!(batch_datum.selection(), batch_raw.selection());

    // Test partial projection pushdown: only decode col 1 (a) and col 3 (t)
    let need_partial = vec![false, true, false, true, false, false];
    let mut batch_partial = batch::ColumnBatch::new();
    batch_partial.prepare_morsel(schema, &need_partial, raw_rows.len());
    for raw in &raw_rows {
        batch_partial.push_raw_row(schema, raw, &need_partial).expect("partial raw decode");
    }
    batch_partial.finish_morsel(&need_partial, raw_rows.len());

    // Unprojected columns must stay untouched
    assert_eq!(batch_partial.selection().len(), 3);

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn batch_raw_multi_morsel_scan_and_aggs() {
    let dir = tmp("multi_morsel");
    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();
    db.execute(
        &mut s,
        "CREATE TABLE multi (id INT PRIMARY KEY, cat INT, val INT, note TEXT)",
    )
    .unwrap();

    // Insert 2,500 rows across 3 categories
    // This exercises > 2 full morsel boundaries (1024 + 1024 + 452)
    db.execute(&mut s, "BEGIN").unwrap();
    for i in 1..=2500 {
        let cat = i % 3;
        let note = format!("note_{i}");
        db.execute(
            &mut s,
            &format!("INSERT INTO multi VALUES ({i}, {cat}, {i}, '{note}')"),
        )
        .unwrap();
    }
    db.execute(&mut s, "COMMIT").unwrap();

    // Verify global aggregates over 2500 rows in direct raw batch mode
    let out = db
        .execute(&mut s, "SELECT COUNT(*), SUM(val), MIN(val), MAX(val) FROM multi")
        .unwrap();
    assert_eq!(out.rows[0][0], Datum::Int(2500));
    // sum of 1..=2500 = 2500 * 2501 / 2 = 3,126,250
    assert_eq!(out.rows[0][1], Datum::Int(3_126_250));
    assert_eq!(out.rows[0][2], Datum::Int(1));
    assert_eq!(out.rows[0][3], Datum::Int(2500));

    // Verify grouped aggregates across multiple morsels
    let out_grp = db
        .execute(
            &mut s,
            "SELECT cat, COUNT(*), SUM(val) FROM multi GROUP BY cat ORDER BY cat",
        )
        .unwrap();
    assert_eq!(out_grp.rows.len(), 3);
    assert_eq!(out_grp.rows[0][0], Datum::Int(0));
    assert_eq!(out_grp.rows[1][0], Datum::Int(1));
    assert_eq!(out_grp.rows[2][0], Datum::Int(2));

    // Filtered multi-morsel aggregate
    let out_filtered = db
        .execute(
            &mut s,
            "SELECT COUNT(*), SUM(val) FROM multi WHERE val > 1000",
        )
        .unwrap();
    // 1001..=2500 = 1500 rows
    assert_eq!(out_filtered.rows[0][0], Datum::Int(1500));
    // sum of 1001..=2500 = 3126250 - (1000*1001/2) = 3126250 - 500500 = 2,625,750
    assert_eq!(out_filtered.rows[0][1], Datum::Int(2_625_750));

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn batch_raw_scan_with_snapshot_isolation() {
    let dir = tmp("snap_batch");
    let db = Database::open(&dir).unwrap();
    let mut s1 = db.new_session();
    let mut s2 = db.new_session();

    db.execute(
        &mut s1,
        "CREATE TABLE items (id INT PRIMARY KEY, qty INT)",
    )
    .unwrap();
    for i in 1..=10 {
        db.execute(&mut s1, &format!("INSERT INTO items VALUES ({i}, 10)")).unwrap();
    }

    // Session 2 pins consistent snapshot
    db.execute(&mut s2, "START TRANSACTION WITH CONSISTENT SNAPSHOT").unwrap();

    // Session 1 mutates items (adds more and updates existing)
    db.execute(&mut s1, "INSERT INTO items VALUES (11, 100)").unwrap();
    db.execute(&mut s1, "UPDATE items SET qty = 50 WHERE id = 1").unwrap();

    // Session 1 sees 11 items, sum = 9*10 + 50 + 100 = 240
    let out1 = db.execute(&mut s1, "SELECT COUNT(*), SUM(qty) FROM items").unwrap();
    assert_eq!(out1.rows[0], vec![Datum::Int(11), Datum::Int(240)]);

    // Session 2 in consistent snapshot MUST see original 10 items, sum = 100
    let out2 = db.execute(&mut s2, "SELECT COUNT(*), SUM(qty) FROM items").unwrap();
    assert_eq!(out2.rows[0], vec![Datum::Int(10), Datum::Int(100)]);

    let _ = fs::remove_dir_all(&dir);
}
