//! Automated Systematic Crash Matrix Test Suite (Assessment §3).
//!
//! Validates atomic recovery across all durability failpoints, transaction classes
//! (single-row, multi-row, insert, update, delete, secondary index, foreign keys),
//! verifying strict atomicity, absence of partial writes, `CHECK DATABASE` integrity,
//! and subsequent read/write functionality.

use std::fs;
use std::panic::AssertUnwindSafe;

use crate::db::{Database, Datum};
use crate::failpoint::{self, FailAction, FailMode};

fn run_crash_matrix_scenario(failpoint_name: &'static str) {
    let test_id = format!("hdbcrash_matrix_{}_{}", failpoint_name, std::process::id());
    let dir = std::env::temp_dir().join(test_id);
    let _ = fs::remove_dir_all(&dir);

    // 1. Initialize database with PK, secondary index, and Foreign Key
    {
        let db = Database::open(&dir).unwrap();
        let mut s = db.new_session();

        db.execute(&mut s, "CREATE TABLE parent (id INT PRIMARY KEY, name TEXT)").unwrap();
        db.execute(
            &mut s,
            "CREATE TABLE child (id INT PRIMARY KEY, parent_id INT, val TEXT, FOREIGN KEY (parent_id) REFERENCES parent(id))",
        ).unwrap();
        db.execute(&mut s, "CREATE INDEX idx_child_val ON child (val)").unwrap();

        // Baseline committed transaction
        db.execute(&mut s, "BEGIN").unwrap();
        db.execute(&mut s, "INSERT INTO parent VALUES (1, 'p1'), (2, 'p2')").unwrap();
        db.execute(&mut s, "INSERT INTO child VALUES (10, 1, 'c10'), (20, 2, 'c20')").unwrap();
        db.execute(&mut s, "COMMIT").unwrap();

        // Checkpoint to flush baseline snapshot
        db.checkpoint().unwrap();

        // Baseline integrity verification
        let reports = db.check_database(&s).unwrap();
        for r in &reports {
            assert!(r.is_ok(), "Baseline CHECK DATABASE failed: {:?}", r.error_msg);
        }
    }

    // 2. Arm the failpoint for the next transaction
    failpoint::set(failpoint_name, FailMode::Once, FailAction::Panic("simulated crash matrix fault"));

    // 3. Execute multi-row, multi-table mixed transaction (Insert, Update, Delete with FK & Index)
    let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let db = Database::open(&dir).unwrap();
        let mut s = db.new_session();

        db.execute(&mut s, "BEGIN").unwrap();
        db.execute(&mut s, "INSERT INTO parent VALUES (3, 'p3')").unwrap();
        db.execute(&mut s, "INSERT INTO child VALUES (30, 3, 'c30')").unwrap();
        db.execute(&mut s, "UPDATE parent SET name = 'p1_updated' WHERE id = 1").unwrap();
        db.execute(&mut s, "DELETE FROM child WHERE id = 10").unwrap();
        let _ = db.execute(&mut s, "COMMIT");
    }));

    // 4. Disarm failpoint and simulate restart & recovery
    failpoint::clear();

    let db = Database::open(&dir).expect("Database must recover cleanly after crash");
    let mut s = db.new_session();

    // 5. Verification: CHECK DATABASE must pass with 0 errors
    let reports = db.check_database(&s).unwrap();
    for r in &reports {
        assert!(
            r.is_ok(),
            "Integrity violated after crash at {}: {:?}",
            failpoint_name,
            r.error_msg
        );
    }

    // 6. Strict atomicity verification: either ALL or NONE of txn changes exist
    let parent_rows = db.execute(&mut s, "SELECT id, name FROM parent ORDER BY id").unwrap().rows;
    let child_rows = db.execute(&mut s, "SELECT id, parent_id, val FROM child ORDER BY id").unwrap().rows;

    let has_p3 = parent_rows.iter().any(|r| r[0] == Datum::Int(3));
    let has_c30 = child_rows.iter().any(|r| r[0] == Datum::Int(30));
    let p1_is_updated = parent_rows.iter().any(|r| r[0] == Datum::Int(1) && r[1] == Datum::Text("p1_updated".into()));
    let has_c10 = child_rows.iter().any(|r| r[0] == Datum::Int(10));

    if has_p3 {
        // Transaction committed durably before/during the crash
        assert!(has_c30, "Atomicity violated: parent 3 committed but child 30 missing");
        assert!(p1_is_updated, "Atomicity violated: parent 3 committed but parent 1 not updated");
        assert!(!has_c10, "Atomicity violated: parent 3 committed but child 10 not deleted");
    } else {
        // Transaction did not reach durable commit; must be 100% rolled back
        assert!(!has_c30, "Atomicity violated: parent 3 rolled back but child 30 exists");
        assert!(!p1_is_updated, "Atomicity violated: parent 3 rolled back but parent 1 updated");
        assert!(has_c10, "Atomicity violated: parent 3 rolled back but child 10 deleted");
    }

    // 7. Live operational verification: database accepts new writes and transactions
    db.execute(&mut s, "BEGIN").unwrap();
    db.execute(&mut s, "INSERT INTO parent VALUES (99, 'p99')").unwrap();
    db.execute(&mut s, "COMMIT").unwrap();

    let check_after = db.check_database(&s).unwrap();
    for r in &check_after {
        assert!(r.is_ok());
    }

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn crash_matrix_failpoint_before_wal_write() {
    run_crash_matrix_scenario("before_wal_write");
}

#[test]
fn crash_matrix_failpoint_before_wal_sync() {
    run_crash_matrix_scenario("before_wal_sync");
}

#[test]
fn crash_matrix_failpoint_after_wal_sync() {
    run_crash_matrix_scenario("after_wal_sync");
}

#[test]
fn crash_matrix_failpoint_before_install() {
    run_crash_matrix_scenario("before_install");
}

#[test]
fn crash_matrix_failpoint_during_multirow_install() {
    run_crash_matrix_scenario("during_multirow_install");
}

#[test]
fn crash_matrix_failpoint_before_snapshot_rename() {
    run_crash_matrix_scenario("before_snapshot_rename");
}

#[test]
fn crash_matrix_failpoint_before_wal_reset() {
    run_crash_matrix_scenario("before_wal_reset");
}
