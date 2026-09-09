use std::fs;
use std::path::Path;

use super::*;
use crate::sql::IsolationLevel;
use crate::types::Datum;

fn mvcc_test_setup(dir: &Path) -> Database {
    let db = Database::open(dir).unwrap();
    let mut s = db.new_session();
    db.execute(&mut s, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT, num INT)")
        .unwrap();
    db.execute(&mut s, "CREATE INDEX idx_num ON t(num)").unwrap();
    db.execute(&mut s, "INSERT INTO t VALUES (1, 'one', 10), (2, 'two', 20), (3, 'three', 30)")
        .unwrap();
    db
}

fn query_val(db: &Database, s: &mut Session, id: i64) -> Option<String> {
    let out = db
        .execute(s, &format!("SELECT v FROM t WHERE id = {id}"))
        .unwrap();
    out.rows.first().map(|r| match &r[0] {
        Datum::Text(v) => v.clone(),
        other => panic!("unexpected {other:?}"),
    })
}

#[test]
fn mvcc_default_begin_repeatable_read() {
    let dir = std::env::temp_dir().join(format!("hdb_mrr_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = mvcc_test_setup(&dir);
    let mut reader = db.new_session();
    let mut writer = db.new_session();

    // Standard BEGIN (default RepeatableRead) pins snapshot upon first read.
    db.execute(&mut reader, "BEGIN").unwrap();
    assert_eq!(query_val(&db, &mut reader, 1).as_deref(), Some("one"));

    // Writer updates row and inserts new row concurrently.
    db.execute(&mut writer, "UPDATE t SET v = 'updated_one' WHERE id = 1").unwrap();
    db.execute(&mut writer, "INSERT INTO t VALUES (4, 'four', 40)").unwrap();

    // Reader MUST still observe its original snapshot.
    assert_eq!(query_val(&db, &mut reader, 1).as_deref(), Some("one"));
    assert_eq!(query_val(&db, &mut reader, 4), None);

    let cnt = db.execute(&mut reader, "SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(cnt.rows[0][0], Datum::Int(3));

    // Fresh autocommit session sees new state.
    assert_eq!(query_val(&db, &mut writer, 1).as_deref(), Some("updated_one"));
    assert_eq!(query_val(&db, &mut writer, 4).as_deref(), Some("four"));

    // Ending the transaction releases the snapshot.
    db.execute(&mut reader, "COMMIT").unwrap();
    assert_eq!(query_val(&db, &mut reader, 1).as_deref(), Some("updated_one"));
    assert_eq!(query_val(&db, &mut reader, 4).as_deref(), Some("four"));

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn mvcc_atomic_multi_row_commit_visibility() {
    let dir = std::env::temp_dir().join(format!("hdb_mam_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = mvcc_test_setup(&dir);
    let mut reader = db.new_session();
    let mut writer = db.new_session();

    // Reader pins snapshot
    db.execute(&mut reader, "START TRANSACTION WITH CONSISTENT SNAPSHOT").unwrap();
    assert_eq!(query_val(&db, &mut reader, 1).as_deref(), Some("one"));

    // Multi-row transaction updates rows 1, 2, and 3 simultaneously
    db.execute(&mut writer, "BEGIN").unwrap();
    db.execute(&mut writer, "UPDATE t SET v = 'a1' WHERE id = 1").unwrap();
    db.execute(&mut writer, "UPDATE t SET v = 'b2' WHERE id = 2").unwrap();
    db.execute(&mut writer, "UPDATE t SET v = 'c3' WHERE id = 3").unwrap();
    db.execute(&mut writer, "COMMIT").unwrap();

    // Reader must observe all previous versions atomically, never partial
    assert_eq!(query_val(&db, &mut reader, 1).as_deref(), Some("one"));
    assert_eq!(query_val(&db, &mut reader, 2).as_deref(), Some("two"));
    assert_eq!(query_val(&db, &mut reader, 3).as_deref(), Some("three"));

    db.execute(&mut reader, "COMMIT").unwrap();
    // After commit, new state is visible
    assert_eq!(query_val(&db, &mut reader, 1).as_deref(), Some("a1"));
    assert_eq!(query_val(&db, &mut reader, 2).as_deref(), Some("b2"));
    assert_eq!(query_val(&db, &mut reader, 3).as_deref(), Some("c3"));

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn mvcc_read_committed_isolation_mode() {
    let dir = std::env::temp_dir().join(format!("hdb_mrc_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = mvcc_test_setup(&dir);
    let mut s1 = db.new_session();
    let mut s2 = db.new_session();

    // Set ReadCommitted
    db.execute(&mut s1, "SET TRANSACTION ISOLATION LEVEL READ COMMITTED").unwrap();
    assert_eq!(s1.isolation_level, IsolationLevel::ReadCommitted);

    db.execute(&mut s1, "BEGIN").unwrap();
    assert_eq!(query_val(&db, &mut s1, 1).as_deref(), Some("one"));

    // Writer modifies row 1 and commits
    db.execute(&mut s2, "UPDATE t SET v = 'fresh_one' WHERE id = 1").unwrap();

    // In ReadCommitted, each statement takes a fresh statement snapshot,
    // so s1 sees the newly committed value within the same transaction!
    assert_eq!(query_val(&db, &mut s1, 1).as_deref(), Some("fresh_one"));

    db.execute(&mut s1, "COMMIT").unwrap();

    // Now switch back to RepeatableRead
    db.execute(&mut s1, "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ").unwrap();
    assert_eq!(s1.isolation_level, IsolationLevel::RepeatableRead);

    db.execute(&mut s1, "BEGIN").unwrap();
    assert_eq!(query_val(&db, &mut s1, 1).as_deref(), Some("fresh_one"));

    // Writer modifies row 1 again
    db.execute(&mut s2, "UPDATE t SET v = 'newer_one' WHERE id = 1").unwrap();

    // In RepeatableRead, s1 MUST NOT see the newer value!
    assert_eq!(query_val(&db, &mut s1, 1).as_deref(), Some("fresh_one"));

    db.execute(&mut s1, "COMMIT").unwrap();
    assert_eq!(query_val(&db, &mut s1, 1).as_deref(), Some("newer_one"));

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn mvcc_secondary_index_and_point_time_travel() {
    let dir = std::env::temp_dir().join(format!("hdb_msi_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = mvcc_test_setup(&dir);
    let mut reader = db.new_session();
    let mut writer = db.new_session();

    // Reader pins snapshot
    db.execute(&mut reader, "START TRANSACTION WITH CONSISTENT SNAPSHOT").unwrap();

    // Writer updates both primary and secondary column, and deletes a row
    db.execute(&mut writer, "UPDATE t SET v = 'one_mod', num = 99 WHERE id = 1").unwrap();
    db.execute(&mut writer, "DELETE FROM t WHERE id = 2").unwrap();

    // 1. Secondary index seek for num = 10 (which was id=1 before update)
    let out = db.execute(&mut reader, "SELECT v FROM t WHERE num = 10").unwrap();
    assert_eq!(out.rows.len(), 1);
    assert_eq!(out.rows[0][0], Datum::Text("one".into()));

    // 2. Secondary index seek for num = 20 (which was deleted)
    let out = db.execute(&mut reader, "SELECT v FROM t WHERE num = 20").unwrap();
    assert_eq!(out.rows.len(), 1);
    assert_eq!(out.rows[0][0], Datum::Text("two".into()));

    // 3. Point lookup on deleted row
    assert_eq!(query_val(&db, &mut reader, 2).as_deref(), Some("two"));

    // Writer confirms changes are active in current state
    assert_eq!(query_val(&db, &mut writer, 2), None);
    let out = db.execute(&mut writer, "SELECT v FROM t WHERE num = 99").unwrap();
    assert_eq!(out.rows.len(), 1);
    assert_eq!(out.rows[0][0], Datum::Text("one_mod".into()));

    db.execute(&mut reader, "ROLLBACK").unwrap();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn mvcc_vectorized_batch_aggregate_snapshot_isolation() {
    let dir = std::env::temp_dir().join(format!("hdb_mba_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();
    db.execute(&mut s, "CREATE TABLE items (id INT PRIMARY KEY, qty INT, price FLOAT)").unwrap();

    // Insert 100 items
    for i in 1..=100 {
        db.execute(&mut s, &format!("INSERT INTO items VALUES ({i}, 10, 2.5)")).unwrap();
    }

    let mut reader = db.new_session();
    let mut writer = db.new_session();

    // Reader pins consistent snapshot
    db.execute(&mut reader, "START TRANSACTION WITH CONSISTENT SNAPSHOT").unwrap();

    // Verify baseline aggregate via ColumnBatch vectorized pushdown
    let out = db.execute(&mut reader, "SELECT COUNT(*), SUM(qty) FROM items").unwrap();
    assert_eq!(out.rows[0][0], Datum::Int(100));
    assert_eq!(out.rows[0][1], Datum::Int(1000));

    // Writer modifies, deletes, and adds items concurrently
    for i in 1..=20 {
        db.execute(&mut writer, &format!("UPDATE items SET qty = 100 WHERE id = {i}")).unwrap();
    }
    for i in 81..=100 {
        db.execute(&mut writer, &format!("DELETE FROM items WHERE id = {i}")).unwrap();
    }
    for i in 101..=120 {
        db.execute(&mut writer, &format!("INSERT INTO items VALUES ({i}, 50, 1.0)")).unwrap();
    }

    // Writer sees new aggregates
    let out_w = db.execute(&mut writer, "SELECT COUNT(*), SUM(qty) FROM items").unwrap();
    // 100 - 20 (deleted) + 20 (new) = 100 rows
    // Sum: 20*100 + 60*10 + 20*50 = 2000 + 600 + 1000 = 3600
    assert_eq!(out_w.rows[0][0], Datum::Int(100));
    assert_eq!(out_w.rows[0][1], Datum::Int(3600));

    // Reader MUST strictly see original aggregates: 100 rows, 1000 sum!
    let out_r = db.execute(&mut reader, "SELECT COUNT(*), SUM(qty) FROM items").unwrap();
    assert_eq!(out_r.rows[0][0], Datum::Int(100));
    assert_eq!(out_r.rows[0][1], Datum::Int(1000));

    db.execute(&mut reader, "COMMIT").unwrap();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn mvcc_set_variable_and_session_isolation() {
    let dir = std::env::temp_dir().join(format!("hdb_msv_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();

    db.execute(&mut s, "SET transaction_isolation = 'READ-COMMITTED'").unwrap();
    assert_eq!(s.isolation_level, IsolationLevel::ReadCommitted);

    db.execute(&mut s, "SET @@tx_isolation = 'REPEATABLE-READ'").unwrap();
    assert_eq!(s.isolation_level, IsolationLevel::RepeatableRead);

    db.execute(&mut s, "SET tx_isolation = 'SERIALIZABLE'").unwrap();
    assert_eq!(s.isolation_level, IsolationLevel::Serializable);

    let _ = fs::remove_dir_all(&dir);
}
