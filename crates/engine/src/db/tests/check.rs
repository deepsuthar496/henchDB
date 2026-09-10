//! Storage diagnostics, `CHECK TABLE`, and resource limit test suite.

use super::*;
use std::time::Duration;

#[test]
fn check_table_validates_clean_table_and_computes_deterministic_hash() {
    let dir = std::env::temp_dir().join(format!("hdbcheck_clean_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = setup(&dir);
    let mut s = db.new_session();

    db.execute(&mut s, "INSERT INTO t VALUES (1, 'alice', 95.0)").unwrap();
    db.execute(&mut s, "INSERT INTO t VALUES (2, 'bob', 88.5)").unwrap();
    db.execute(&mut s, "INSERT INTO t VALUES (3, 'carol', 72.0)").unwrap();
    db.execute(&mut s, "CREATE INDEX idx_name ON t (name)").unwrap();

    let report = db.check_table(&s, "t").unwrap();
    assert_eq!(report.status, "status");
    assert!(report.error_msg.is_none());
    assert!(report.hash > 0);

    // Test SQL command execution
    let out = db.execute(&mut s, "CHECK TABLE t;").unwrap();
    assert_eq!(out.columns, vec!["Table", "Op", "Msg_type", "Msg_text"]);
    assert_eq!(out.rows.len(), 1);
    assert_eq!(out.rows[0][0], Datum::Text("default.t".into()));
    assert_eq!(out.rows[0][1], Datum::Text("check".into()));
    assert_eq!(out.rows[0][2], Datum::Text("status".into()));
    let msg = match &out.rows[0][3] {
        Datum::Text(t) => t.clone(),
        _ => panic!("expected text"),
    };
    assert!(msg.starts_with("OK (hash: 0x"));

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn check_table_detects_broken_foreign_key() {
    let dir = std::env::temp_dir().join(format!("hdbcheck_fk_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();

    db.execute(&mut s, "CREATE TABLE parent (id INT PRIMARY KEY, name TEXT)").unwrap();
    db.execute(
        &mut s,
        "CREATE TABLE child (id INT PRIMARY KEY, pid INT, FOREIGN KEY (pid) REFERENCES parent (id))",
    )
    .unwrap();

    db.execute(&mut s, "INSERT INTO parent VALUES (10, 'parent1')").unwrap();
    db.execute(&mut s, "INSERT INTO child VALUES (1, 10)").unwrap();

    // Clean check
    let report = db.check_table(&s, "child").unwrap();
    assert_eq!(report.status, "status");

    // Manually delete the parent row by deleting raw from the parent's tree (simulating corruption or bypass)
    let parent_t = db.table(&s, "parent").unwrap();
    let pk_bytes = crate::types::encode_key(&Datum::Int(10)).unwrap();
    parent_t.remove_raw(&pk_bytes);

    // Now check_table must detect the broken foreign key!
    let report = db.check_table(&s, "child").unwrap();
    assert_eq!(report.status, "error");
    let err = report.error_msg.expect("expected error message");
    assert!(err.contains("Foreign key constraint"));
    assert!(err.contains("not found in 'default.parent'"));

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn database_logical_hash_identical_across_identical_datasets() {
    let dir1 = std::env::temp_dir().join(format!("hdbhash_1_{}", std::process::id()));
    let dir2 = std::env::temp_dir().join(format!("hdbhash_2_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir1);
    let _ = fs::remove_dir_all(&dir2);

    let db1 = Database::open(&dir1).unwrap();
    let mut s1 = db1.new_session();
    db1.execute(&mut s1, "CREATE TABLE a (id INT PRIMARY KEY, val TEXT)").unwrap();
    db1.execute(&mut s1, "CREATE TABLE b (id INT PRIMARY KEY, score FLOAT)").unwrap();
    db1.execute(&mut s1, "INSERT INTO a VALUES (1, 'x'), (2, 'y'), (3, 'z')").unwrap();
    db1.execute(&mut s1, "INSERT INTO b VALUES (10, 1.1), (20, 2.2)").unwrap();

    let db2 = Database::open(&dir2).unwrap();
    let mut s2 = db2.new_session();
    db2.execute(&mut s2, "CREATE TABLE b (id INT PRIMARY KEY, score FLOAT)").unwrap();
    db2.execute(&mut s2, "CREATE TABLE a (id INT PRIMARY KEY, val TEXT)").unwrap();
    // Insert into a in reverse order: logical sorted hash must match regardless of insertion order
    db2.execute(&mut s2, "INSERT INTO a VALUES (3, 'z'), (1, 'x'), (2, 'y')").unwrap();
    db2.execute(&mut s2, "INSERT INTO b VALUES (20, 2.2), (10, 1.1)").unwrap();

    let h1 = db1.database_logical_hash(&s1).unwrap();
    let h2 = db2.database_logical_hash(&s2).unwrap();
    assert_eq!(h1, h2, "logical database hash must be identical across identical datasets");

    let _ = fs::remove_dir_all(&dir1);
    let _ = fs::remove_dir_all(&dir2);
}

#[test]
fn resource_governance_max_result_rows_enforced() {
    let dir = std::env::temp_dir().join(format!("hdbres_rows_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = setup(&dir);
    let mut s = db.new_session();

    for i in 1..=10 {
        db.execute(&mut s, &format!("INSERT INTO t VALUES ({i}, 'item{i}', 1.0)")).unwrap();
    }

    // Normal query works
    let out = db.execute(&mut s, "SELECT * FROM t").unwrap();
    assert_eq!(out.rows.len(), 10);

    // Set max_result_rows = 5 via SQL
    db.execute(&mut s, "SET max_result_rows = 5").unwrap();
    let res = db.execute(&mut s, "SELECT * FROM t");
    assert!(res.is_err());
    let err = res.unwrap_err().to_string();
    assert!(err.contains("max_result_rows limit (5)"));

    // Reset limit
    db.execute(&mut s, "SET max_result_rows = 0").unwrap();
    let out = db.execute(&mut s, "SELECT * FROM t").unwrap();
    assert_eq!(out.rows.len(), 10);

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn resource_governance_max_result_bytes_enforced() {
    let dir = std::env::temp_dir().join(format!("hdbres_bytes_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = setup(&dir);
    let mut s = db.new_session();

    db.execute(&mut s, "INSERT INTO t VALUES (1, 'short', 1.0)").unwrap();
    db.execute(&mut s, "INSERT INTO t VALUES (2, 'a very long text string that takes many bytes to store', 2.0)").unwrap();

    // Set max_result_bytes = 40
    db.execute(&mut s, "SET max_result_bytes = 40").unwrap();
    let res = db.execute(&mut s, "SELECT * FROM t");
    assert!(res.is_err());
    let err = res.unwrap_err().to_string();
    assert!(err.contains("max_result_bytes limit"));

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn mvcc_max_snapshot_age_expiration() {
    let dir = std::env::temp_dir().join(format!("hdbmvcc_age_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = setup(&dir);

    // Set max snapshot age to 20ms
    db.set_max_snapshot_age(Some(Duration::from_millis(20)));

    let mut s1 = db.new_session();
    db.execute(&mut s1, "INSERT INTO t VALUES (1, 'initial', 10.0)").unwrap();

    // Start transaction with consistent snapshot in session 2
    let mut s2 = db.new_session();
    db.execute(&mut s2, "START TRANSACTION WITH CONSISTENT SNAPSHOT").unwrap();

    // Mutate in session 1
    db.execute(&mut s1, "UPDATE t SET score = 99.0 WHERE id = 1").unwrap();

    // Sleep past max_snapshot_age
    std::thread::sleep(Duration::from_millis(35));

    // s2 query must be rejected due to snapshot expiration!
    let res = db.execute(&mut s2, "SELECT * FROM t WHERE id = 1");
    assert!(res.is_err(), "query should fail due to snapshot age expiration");
    let err = res.unwrap_err().to_string();
    assert!(err.contains("snapshot exceeded maximum configured age limit"));

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn resource_governance_max_intermediate_rows_enforced() {
    let dir = std::env::temp_dir().join(format!("hdbres_inter_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = setup(&dir);
    let mut s = db.new_session();

    for i in 1..=10 {
        db.execute(&mut s, &format!("INSERT INTO t VALUES ({i}, 'item{i}', 1.0)")).unwrap();
    }
    db.execute(&mut s, "CREATE TABLE u (uid INT PRIMARY KEY, tag TEXT)").unwrap();
    for i in 1..=10 {
        db.execute(&mut s, &format!("INSERT INTO u VALUES ({i}, 'tag{i}')")).unwrap();
    }

    // 1. Join intermediate limit
    db.execute(&mut s, "SET max_intermediate_rows = 5").unwrap();
    let res = db.execute(&mut s, "SELECT * FROM t JOIN u ON t.id = u.uid");
    assert!(res.is_err());
    let err = res.unwrap_err().to_string();
    assert!(err.contains("max_intermediate_rows limit (5) during join"));

    // 2. Aggregation / group by intermediate limit
    db.execute(&mut s, "SET max_intermediate_rows = 4").unwrap();
    let res = db.execute(&mut s, "SELECT id, COUNT(*) FROM t GROUP BY id");
    assert!(res.is_err());
    let err = res.unwrap_err().to_string();
    assert!(err.contains("max_intermediate_rows limit (4) during aggregation"));

    // 3. Sort intermediate limit
    db.execute(&mut s, "SET max_intermediate_rows = 6").unwrap();
    let res = db.execute(&mut s, "SELECT * FROM t ORDER BY score DESC");
    assert!(res.is_err());
    let err = res.unwrap_err().to_string();
    assert!(err.contains("max_intermediate_rows limit (6) during sort"));

    // 4. Subquery IN intermediate limit
    db.execute(&mut s, "SET max_intermediate_rows = 5").unwrap();
    let res = db.execute(&mut s, "SELECT * FROM t WHERE id IN (SELECT uid FROM u)");
    assert!(res.is_err());
    let err = res.unwrap_err().to_string();
    assert!(err.contains("max_intermediate_rows limit (5) during subquery IN evaluation"));

    // 5. Derived table intermediate limit
    db.execute(&mut s, "SET max_intermediate_rows = 5").unwrap();
    let res = db.execute(&mut s, "SELECT * FROM (SELECT id FROM t) AS d");
    assert!(res.is_err());
    let err = res.unwrap_err().to_string();
    assert!(err.contains("max_intermediate_rows limit (5) during subquery materialization"));

    // Reset limit: all queries succeed cleanly!
    db.execute(&mut s, "SET max_intermediate_rows = 0").unwrap();
    let out = db.execute(&mut s, "SELECT * FROM t JOIN u ON t.id = u.uid").unwrap();
    assert_eq!(out.rows.len(), 10);

    let _ = fs::remove_dir_all(&dir);
}

