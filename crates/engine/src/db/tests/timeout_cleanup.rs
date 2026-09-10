//! Comprehensive Statement Timeout Interruption & Resource Cleanup Verification (Assessment §11).
//!
//! Verifies that when a statement exceeds `max_execution_time` / `statement_timeout`,
//! execution aborts immediately with `Err(Error::QueryTimeout)`, and ALL runtime
//! resources are cleanly released:
//! - Memory reservations (`QueryMemoryTracker.current_bytes() == 0`)
//! - Temporary subquery structures (`SubqueryState.eph.is_empty()`)
//! - Active EBR guards (`epoch.stats().active_guards == 0`)
//! - Transaction locks and session state.

use std::fs;
use std::time::Duration;

use crate::db::Database;
use crate::error::Error;

fn populate_test_db(dir: &std::path::Path) -> Database {
    let _ = fs::remove_dir_all(dir);
    let db = Database::open(dir).unwrap();
    let mut s = db.new_session();

    db.execute(&mut s, "CREATE TABLE t1 (id INT PRIMARY KEY, v TEXT, num INT)").unwrap();
    db.execute(&mut s, "CREATE TABLE t2 (id INT PRIMARY KEY, v TEXT, ref_id INT)").unwrap();

    for i in 0..1_000 {
        let text_val = format!("val_{}_{}", i % 50, "x".repeat(32));
        db.execute(&mut s, &format!("INSERT INTO t1 VALUES ({i}, '{text_val}', {i})")).unwrap();
        db.execute(&mut s, &format!("INSERT INTO t2 VALUES ({i}, '{text_val}', {})", i % 100)).unwrap();
    }
    db
}

#[test]
fn timeout_cleanup_large_scan() {
    let dir = std::env::temp_dir().join(format!("hdbtm_scan_{}", std::process::id()));
    let db = populate_test_db(&dir);
    let mut s = db.new_session();

    // Trigger timeout during large full scan
    db.execute(&mut s, "SET statement_timeout = 1").unwrap();
    std::thread::sleep(Duration::from_millis(2));

    let res = db.execute(&mut s, "SELECT * FROM t1 WHERE v LIKE '%val_10%'");
    assert_eq!(res, Err(Error::QueryTimeout));

    // Verify resource cleanup
    assert_eq!(s.mem_tracker.current_bytes(), 0, "Memory tracker must release reservations");
    assert!(s.subq.eph.is_empty(), "Temporary subquery tables must be cleared");
    assert_eq!(db.epoch.stats().active_guards, 0, "EBR guards must not remain held");

    // Session can immediately run subsequent queries
    db.execute(&mut s, "SET statement_timeout = 0").unwrap();
    let count_res = db.execute(&mut s, "SELECT COUNT(*) FROM t1").unwrap();
    assert_eq!(count_res.rows.len(), 1);

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn timeout_cleanup_hash_join() {
    let dir = std::env::temp_dir().join(format!("hdbtm_join_{}", std::process::id()));
    let db = populate_test_db(&dir);
    let mut s = db.new_session();

    db.execute(&mut s, "SET statement_timeout = 1").unwrap();
    std::thread::sleep(Duration::from_millis(2));

    let res = db.execute(
        &mut s,
        "SELECT * FROM t1 JOIN t2 ON t1.id = t2.ref_id WHERE t1.v LIKE '%x%'",
    );
    assert_eq!(res, Err(Error::QueryTimeout));

    // Operator memory reservations must be released via RAII
    assert_eq!(s.mem_tracker.current_bytes(), 0, "Join reservation must be dropped");
    assert_eq!(db.epoch.stats().active_guards, 0, "EBR guards released");

    db.execute(&mut s, "SET statement_timeout = 0").unwrap();
    assert!(db.execute(&mut s, "SELECT 1").is_ok());

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn timeout_cleanup_sort_and_aggregation() {
    let dir = std::env::temp_dir().join(format!("hdbtm_sort_{}", std::process::id()));
    let db = populate_test_db(&dir);
    let mut s = db.new_session();

    // 1. Sort timeout
    db.execute(&mut s, "SET statement_timeout = 1").unwrap();
    std::thread::sleep(Duration::from_millis(2));
    let res = db.execute(&mut s, "SELECT * FROM t1 ORDER BY v DESC");
    assert_eq!(res, Err(Error::QueryTimeout));
    assert_eq!(s.mem_tracker.current_bytes(), 0, "Sort reservation dropped");

    // 2. Grouped aggregation timeout
    std::thread::sleep(Duration::from_millis(2));
    let res2 = db.execute(&mut s, "SELECT v, COUNT(*), SUM(num) FROM t1 GROUP BY v");
    assert_eq!(res2, Err(Error::QueryTimeout));
    assert_eq!(s.mem_tracker.current_bytes(), 0, "Aggregation reservation dropped");

    db.execute(&mut s, "SET statement_timeout = 0").unwrap();
    assert!(db.execute(&mut s, "SELECT 1").is_ok());

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn timeout_cleanup_subqueries_and_in_lists() {
    let dir = std::env::temp_dir().join(format!("hdbtm_subq_{}", std::process::id()));
    let db = populate_test_db(&dir);
    let mut s = db.new_session();

    // 1. Subquery IN list
    db.execute(&mut s, "SET statement_timeout = 1").unwrap();
    std::thread::sleep(Duration::from_millis(2));
    let res = db.execute(
        &mut s,
        "SELECT * FROM t1 WHERE id IN (SELECT ref_id FROM t2 WHERE v LIKE '%val%')",
    );
    assert_eq!(res, Err(Error::QueryTimeout));
    assert_eq!(s.mem_tracker.current_bytes(), 0, "Subquery IN reservation dropped");
    assert!(s.subq.eph.is_empty(), "Derived tables cleaned");

    // 2. Derived table in FROM clause
    std::thread::sleep(Duration::from_millis(2));
    let res2 = db.execute(
        &mut s,
        "SELECT * FROM (SELECT id, v FROM t1 WHERE num > 10) AS sub WHERE v LIKE '%val%'",
    );
    assert_eq!(res2, Err(Error::QueryTimeout));
    assert_eq!(s.mem_tracker.current_bytes(), 0);
    assert!(s.subq.eph.is_empty());

    db.execute(&mut s, "SET statement_timeout = 0").unwrap();
    assert!(db.execute(&mut s, "SELECT 1").is_ok());

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn timeout_cleanup_during_transaction_preserves_rollback() {
    let dir = std::env::temp_dir().join(format!("hdbtm_txn_{}", std::process::id()));
    let db = populate_test_db(&dir);
    let mut s = db.new_session();

    db.execute(&mut s, "BEGIN").unwrap();
    db.execute(&mut s, "INSERT INTO t1 VALUES (99999, 'uncommitted', 100)").unwrap();

    // Trigger timeout on an expensive statement within the transaction
    db.execute(&mut s, "SET statement_timeout = 1").unwrap();
    std::thread::sleep(Duration::from_millis(2));
    let res = db.execute(&mut s, "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id WHERE t1.v LIKE '%x%'");
    assert_eq!(res, Err(Error::QueryTimeout));

    // Transaction state and locks must remain in a rollback-able state
    assert_eq!(s.mem_tracker.current_bytes(), 0);
    assert_eq!(db.epoch.stats().active_guards, 0);

    // Rollback cleans up staged writes cleanly
    db.execute(&mut s, "SET statement_timeout = 0").unwrap();
    db.execute(&mut s, "ROLLBACK").unwrap();
    assert!(s.txn.is_none());

    // Row 99999 must not exist
    let check = db.execute(&mut s, "SELECT * FROM t1 WHERE id = 99999").unwrap();
    assert_eq!(check.rows.len(), 0);

    // Subsequent transaction executes cleanly
    db.execute(&mut s, "BEGIN").unwrap();
    db.execute(&mut s, "INSERT INTO t1 VALUES (99999, 'committed', 200)").unwrap();
    db.execute(&mut s, "COMMIT").unwrap();

    let check2 = db.execute(&mut s, "SELECT * FROM t1 WHERE id = 99999").unwrap();
    assert_eq!(check2.rows.len(), 1);

    let _ = fs::remove_dir_all(&dir);
}
