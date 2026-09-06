//! Optimizer suite: ANALYZE, selectivity-driven access paths, filtered
//! join ordering with pushdown, EXPLAIN / EXPLAIN ANALYZE, and stats
//! persistence across restarts.

use super::*;

fn opt_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("hdbopt_{tag}_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    dir
}

fn load_big_small(dir: &std::path::Path) -> Database {
    let db = Database::open(dir).unwrap();
    let mut s = db.new_session();
    db.execute(&mut s, "CREATE TABLE big (id INT PRIMARY KEY, g INT)").unwrap();
    for i in (0..500).step_by(50) {
        let vals: Vec<String> = (i..(i + 50)).map(|k| format!("({k}, {})", k % 50)).collect();
        db.execute(&mut s, &format!("INSERT INTO big VALUES {}", vals.join(", "))).unwrap();
    }
    db.execute(&mut s, "CREATE TABLE small (id INT PRIMARY KEY, g INT)").unwrap();
    db.execute(&mut s, "INSERT INTO small VALUES (1, 7), (2, 7), (3, 8), (4, 9), (5, 10)").unwrap();
    db.execute(&mut s, "CREATE INDEX idx_big_g ON big (g)").unwrap();
    db.execute(&mut s, "ANALYZE TABLE big").unwrap();
    db.execute(&mut s, "ANALYZE TABLE small").unwrap();
    db
}

fn explain_row(out: &Output) -> &[Datum] {
    assert_eq!(
        out.columns,
        ["table", "access_path", "type", "key", "rows", "filtered", "cost"]
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>()
    );
    assert_eq!(out.rows.len(), 1);
    &out.rows[0]
}

#[test]
fn analyze_output_shape_and_stats() {
    let dir = opt_dir("shape");
    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();
    db.execute(&mut s, "CREATE TABLE t (id INT PRIMARY KEY, g INT)").unwrap();
    let out = db.execute(&mut s, "ANALYZE TABLE t").unwrap();
    assert_eq!(
        out.columns,
        ["Table", "Op", "Msg_type", "Msg_text"].into_iter().map(String::from).collect::<Vec<_>>()
    );
    assert_eq!(out.rows.len(), 1);
    assert_eq!(out.rows[0][1], Datum::Text("analyze".into()));
    assert_eq!(out.rows[0][2], Datum::Text("status".into()));
    assert_eq!(out.rows[0][3], Datum::Text("OK".into()));
    // Empty table: EXPLAIN estimates zero rows, full scan.
    let out = db.execute(&mut s, "EXPLAIN SELECT * FROM t").unwrap();
    let row = explain_row(&out);
    assert_eq!(row[1], Datum::Text("FULL SCAN".into()));
    assert_eq!(row[4], Datum::Int(0));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn cost_model_picks_seek_or_scan() {
    let dir = opt_dir("cost");
    let db = load_big_small(&dir);
    let mut s = db.new_session();
    // g = 7 keeps 10 of 500 rows (2%): random SEC SEEK still loses to a
    // 500-row scan under RANDOM_PAGE_COST = 4 — FULL SCAN is correct here.
    let out = db.execute(&mut s, "EXPLAIN SELECT * FROM big WHERE g = 7").unwrap();
    let row = explain_row(&out);
    assert_eq!(row[1], Datum::Text("FULL SCAN".into()));
    // PK point seeks always win: bounded single descent.
    let out = db.execute(&mut s, "EXPLAIN SELECT * FROM big WHERE id = 42").unwrap();
    let row = explain_row(&out);
    assert_eq!(row[1], Datum::Text("PK POINT".into()));
    assert_eq!(row[2], Datum::Text("const".into()));
    assert_eq!(row[3], Datum::Text("id".into()));
    // Results identical on every path (executor re-filters).
    let out = db.execute(&mut s, "SELECT id FROM big WHERE g = 7 ORDER BY id").unwrap();
    assert_eq!(out.rows.len(), 10);
    assert_eq!(out.rows[0][0], Datum::Int(7));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn selective_secondary_seek_wins_when_truly_selective() {
    let dir = opt_dir("seek");
    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();
    db.execute(&mut s, "CREATE TABLE t (id INT PRIMARY KEY, g INT)").unwrap();
    for i in (0..2000).step_by(100) {
        let vals: Vec<String> = (i..(i + 100)).map(|k| format!("({k}, {k})")).collect();
        db.execute(&mut s, &format!("INSERT INTO t VALUES {}", vals.join(", "))).unwrap();
    }
    db.execute(&mut s, "CREATE INDEX idx_g ON t (g)").unwrap();
    db.execute(&mut s, "ANALYZE TABLE t").unwrap();
    // g = 5 keeps 1 of 2000 rows (0.05%): SEC SEEK (1 + 4.02) beats a
    // 2000-row scan (40).
    let out = db.execute(&mut s, "EXPLAIN SELECT * FROM t WHERE g = 5").unwrap();
    let row = explain_row(&out);
    assert_eq!(row[1], Datum::Text("SEC SEEK".into()));
    assert_eq!(row[2], Datum::Text("ref".into()));
    assert_eq!(row[3], Datum::Text("g".into()));
    let out = db.execute(&mut s, "SELECT id FROM t WHERE g = 5").unwrap();
    assert_eq!(out.rows, vec![vec![Datum::Int(5)]]);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn join_ordering_uses_filtered_sizes_with_pushdown() {
    let dir = opt_dir("join");
    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();
    db.execute(&mut s, "CREATE TABLE big (id INT PRIMARY KEY, g INT)").unwrap();
    for i in (0..300).step_by(50) {
        let vals: Vec<String> = (i..(i + 50)).map(|k| format!("({k}, {k})")).collect();
        db.execute(&mut s, &format!("INSERT INTO big VALUES {}", vals.join(", "))).unwrap();
    }
    db.execute(&mut s, "CREATE TABLE mid (id INT PRIMARY KEY, g INT)").unwrap();
    for i in 0..100 {
        db.execute(&mut s, &format!("INSERT INTO mid VALUES ({i}, {i})")).unwrap();
    }
    db.execute(&mut s, "CREATE TABLE tiny (id INT PRIMARY KEY, g INT)").unwrap();
    db.execute(&mut s, "INSERT INTO tiny VALUES (1, 5), (2, 6)").unwrap();
    for t in ["big", "mid", "tiny"] {
        db.execute(&mut s, &format!("ANALYZE TABLE {t}")).unwrap();
    }
    // tiny.id < 2 keeps 1 of 2 rows: tiny must jump ahead of mid.
    // (Star joins on big.g so both orders are valid and the estimate wins.)
    let out = db.execute(
        &mut s,
        "EXPLAIN SELECT * FROM big JOIN mid ON big.g = mid.g JOIN tiny ON big.g = tiny.g WHERE tiny.id < 2",
    ).unwrap();
    assert_eq!(out.rows.len(), 3);
    let order: Vec<String> = out.rows.iter().map(|r| r[0].to_string()).collect();
    assert_eq!(order, vec!["big".to_string(), "tiny".to_string(), "mid".to_string()]);
    // tiny's row shows the pushed filter; mid is unfiltered.
    assert_eq!(out.rows[1][1], Datum::Text("FILTERED SCAN".into()));
    assert_eq!(out.rows[2][1], Datum::Text("FULL SCAN".into()));
    // Results exact (pushdown skips only doomed rows).
    let out = db.execute(
        &mut s,
        "SELECT big.id FROM big JOIN mid ON big.g = mid.g JOIN tiny ON big.g = tiny.g WHERE tiny.id < 2 ORDER BY big.id",
    ).unwrap();
    assert_eq!(out.rows, vec![vec![Datum::Int(5)]]);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn left_join_right_predicates_not_pushed() {
    let dir = opt_dir("left");
    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();
    db.execute(&mut s, "CREATE TABLE a (id INT PRIMARY KEY, g INT)").unwrap();
    db.execute(&mut s, "INSERT INTO a VALUES (1, 1), (2, 2)").unwrap();
    db.execute(&mut s, "CREATE TABLE b (id INT PRIMARY KEY, g INT)").unwrap();
    db.execute(&mut s, "INSERT INTO b VALUES (1, 1)").unwrap();
    db.execute(&mut s, "ANALYZE TABLE a").unwrap();
    db.execute(&mut s, "ANALYZE TABLE b").unwrap();
    // Post-join WHERE on the NULL-supplying side filters padded rows out.
    let out = db.execute(
        &mut s,
        "SELECT a.id FROM a LEFT JOIN b ON a.g = b.g WHERE b.id = 1 ORDER BY a.id",
    ).unwrap();
    assert_eq!(out.rows, vec![vec![Datum::Int(1)]]);
    // EXPLAIN shows no pushed filter on the right side.
    let out = db.execute(
        &mut s,
        "EXPLAIN SELECT a.id FROM a LEFT JOIN b ON a.g = b.g WHERE b.id = 1",
    ).unwrap();
    assert_eq!(out.rows.len(), 2);
    assert_eq!(out.rows[1][1], Datum::Text("FULL SCAN".into()));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn explain_analyze_reports_actuals() {
    let dir = opt_dir("analyzex");
    let db = load_big_small(&dir);
    let mut s = db.new_session();
    let out = db.execute(&mut s, "EXPLAIN ANALYZE SELECT * FROM big WHERE id < 10").unwrap();
    assert_eq!(
        out.columns,
        ["table", "access_path", "type", "key", "rows_est", "rows_act", "cost", "time_ms"]
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>()
    );
    assert_eq!(out.rows.len(), 1);
    assert_eq!(out.rows[0][1], Datum::Text("PK RANGE".into()));
    assert_eq!(out.rows[0][5], Datum::Int(10));
    let ms: f64 = out.rows[0][7].to_string().parse().unwrap();
    assert!(ms >= 0.0);

    // Join ANALYZE: per-input actuals in exec order + real execution.
    let out = db.execute(
        &mut s,
        "EXPLAIN ANALYZE SELECT * FROM big JOIN small ON big.g = small.g WHERE small.id < 3",
    ).unwrap();
    assert_eq!(out.rows.len(), 2);
    assert_eq!(out.rows[0][0], Datum::Text("big".into()));
    assert_eq!(out.rows[1][0], Datum::Text("small".into()));
    // small pushed to 2 rows (ids 1,2); big unfiltered at 500.
    assert_eq!(out.rows[1][5], Datum::Int(2));
    assert_eq!(out.rows[0][5], Datum::Int(500));
    // DESCRIBE synonym matches EXPLAIN shape.
    let out = db.execute(&mut s, "DESCRIBE SELECT * FROM big WHERE id = 1").unwrap();
    assert_eq!(out.columns.len(), 7);
    assert_eq!(out.rows[0][1], Datum::Text("PK POINT".into()));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn stats_survive_checkpoint_and_restart() {
    let dir = opt_dir("persist");
    let est_after_reopen: i64;
    {
        let db = Database::open(&dir).unwrap();
        let mut s = db.new_session();
        db.execute(&mut s, "CREATE TABLE t (id INT PRIMARY KEY, g INT)").unwrap();
        for i in (0..100).step_by(20) {
            let vals: Vec<String> = (i..(i + 20)).map(|k| format!("({k}, {k})")).collect();
            db.execute(&mut s, &format!("INSERT INTO t VALUES {}", vals.join(", "))).unwrap();
        }
        db.execute(&mut s, "ANALYZE TABLE t").unwrap();
        db.execute(&mut s, "CHECKPOINT").unwrap();
    }
    {
        let db = Database::open(&dir).unwrap();
        let mut s = db.new_session();
        // Delete 90 rows WITHOUT re-analyzing: a persisted stats image
        // still estimates ~100 rows (stale by design, proof of restore).
        db.execute(&mut s, "DELETE FROM t WHERE id < 90").unwrap();
        let out = db.execute(&mut s, "SELECT COUNT(*) FROM t").unwrap();
        assert_eq!(out.rows[0][0], Datum::Int(10));
        let out = db.execute(&mut s, "EXPLAIN SELECT * FROM t").unwrap();
        est_after_reopen = match out.rows[0][4] {
            Datum::Int(n) => n,
            ref other => panic!("rows not int: {other:?}"),
        };
        assert_eq!(est_after_reopen, 100, "stats must survive restart");
    }
    // Fresh ANALYZE picks up the new reality.
    {
        let db = Database::open(&dir).unwrap();
        let mut s = db.new_session();
        db.execute(&mut s, "ANALYZE TABLE t").unwrap();
        let out = db.execute(&mut s, "EXPLAIN SELECT * FROM t").unwrap();
        assert_eq!(out.rows[0][4], Datum::Int(10));
    }
    let _ = fs::remove_dir_all(&dir);
}
