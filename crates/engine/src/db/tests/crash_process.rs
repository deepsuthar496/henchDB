//! Real Subprocess Crash and Recovery Testing Harness (Assessment §2).
//!
//! Replaces in-process panic simulation with actual OS process termination
//! (`std::process::abort()`). The database subprocess is abruptly killed mid-transaction
//! across all durability failpoints, followed by out-of-process recovery, `CHECK DATABASE`
//! integrity audits, and live write verification.

use std::fs;
use std::process::Command;

use crate::db::{Database, Datum};
use crate::failpoint::{self, FailAction, FailMode};

/// Subprocess worker entrypoint: executed in a child process when
/// `HENCHDB_PROCESS_CRASH_WORKER=1` is set in the environment.
#[test]
fn run_worker() {
    if std::env::var("HENCHDB_PROCESS_CRASH_WORKER").is_err() {
        return; // normal test runner skips this helper
    }

    let dir = std::env::var("HENCHDB_PROCESS_CRASH_DIR").expect("HENCHDB_PROCESS_CRASH_DIR required");
    let failpoint_name = std::env::var("HENCHDB_PROCESS_CRASH_FAILPOINT").expect("HENCHDB_PROCESS_CRASH_FAILPOINT required");

    // Arm the failpoint to trigger a hard OS abort
    failpoint::set_global(&failpoint_name, FailMode::Once, FailAction::Abort);

    let db = Database::open(std::path::Path::new(&dir)).expect("child opens db");
    let mut s = db.new_session();

    // Execute multi-row, multi-table mixed transaction with FK and secondary index
    db.execute(&mut s, "BEGIN").unwrap();
    db.execute(&mut s, "INSERT INTO parent VALUES (3, 'p3')").unwrap();
    db.execute(&mut s, "INSERT INTO child VALUES (30, 3, 'c30')").unwrap();
    db.execute(&mut s, "UPDATE parent SET name = 'p1_updated' WHERE id = 1").unwrap();
    db.execute(&mut s, "DELETE FROM child WHERE id = 10").unwrap();

    // Commit triggers the WAL failpoints and aborts the OS process
    let _ = db.execute(&mut s, "COMMIT");

    // Checkpoint triggers the snapshot and WAL reset failpoints and aborts the OS process
    let _ = db.checkpoint();

    // If abort was not triggered (failpoint did not fire), exit non-zero
    std::process::exit(0);
}

fn run_real_process_crash(failpoint_name: &'static str) {
    let test_id = format!("hdbproc_crash_{}_{}", failpoint_name, std::process::id());
    let dir = std::env::temp_dir().join(test_id);
    let _ = fs::remove_dir_all(&dir);

    // 1. Parent process initializes baseline database state
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

        // Checkpoint baseline to ensure snapshot on disk
        db.checkpoint().unwrap();

        let reports = db.check_database(&s).unwrap();
        for r in &reports {
            assert!(r.is_ok(), "Baseline CHECK DATABASE failed: {:?}", r.error_msg);
        }
    }

    // 2. Spawn child process to execute the transaction and crash via abort
    let exe = std::env::current_exe().expect("current executable path");
    let status = Command::new(exe)
        .arg("--")
        .arg("db::tests::crash_process::run_worker")
        .arg("--exact")
        .arg("--nocapture")
        .env("HENCHDB_PROCESS_CRASH_WORKER", "1")
        .env("HENCHDB_PROCESS_CRASH_DIR", dir.to_str().unwrap())
        .env("HENCHDB_PROCESS_CRASH_FAILPOINT", failpoint_name)
        .status()
        .expect("spawn crash worker subprocess");

    // Assert that the child process actually crashed (aborted)
    assert!(
        !status.success(),
        "Worker subprocess must crash abnormally at failpoint '{}', but exited with {:?}",
        failpoint_name,
        status
    );

    // 3. Parent process recovers the database from disk post-crash
    let db = Database::open(&dir).expect("Database must recover cleanly after real process crash");
    let mut s = db.new_session();

    // 4. Verification: CHECK DATABASE must pass with 0 errors
    let reports = db.check_database(&s).unwrap();
    for r in &reports {
        assert!(
            r.is_ok(),
            "Structural integrity violated after real process crash at {}: {:?}",
            failpoint_name,
            r.error_msg
        );
    }

    // 5. Strict atomicity verification: either ALL or NONE of txn changes exist
    let parent_rows = db.execute(&mut s, "SELECT id, name FROM parent ORDER BY id").unwrap().rows;
    let child_rows = db.execute(&mut s, "SELECT id, parent_id, val FROM child ORDER BY id").unwrap().rows;

    let has_p3 = parent_rows.iter().any(|r| r[0] == Datum::Int(3));
    let has_c30 = child_rows.iter().any(|r| r[0] == Datum::Int(30));
    let p1_updated = parent_rows.iter().any(|r| r[0] == Datum::Int(1) && r[1] == Datum::Text("p1_updated".into()));
    let c10_deleted = !child_rows.iter().any(|r| r[0] == Datum::Int(10));

    let all_applied = has_p3 && has_c30 && p1_updated && c10_deleted;
    let none_applied = !has_p3 && !has_c30 && !p1_updated && !c10_deleted;

    assert!(
        all_applied || none_applied,
        "Atomicity broken after real process crash at {}: partial transaction state detected! parent={:?}, child={:?}",
        failpoint_name, parent_rows, child_rows
    );

    // 6. Verify subsequent live writes succeed on recovered database
    db.execute(&mut s, "INSERT INTO parent VALUES (4, 'p4')").unwrap();
    db.execute(&mut s, "INSERT INTO child VALUES (40, 4, 'c40')").unwrap();
    let final_check = db.check_database(&s).unwrap();
    for r in &final_check {
        assert!(r.is_ok(), "Post-recovery live writes corrupted database: {:?}", r.error_msg);
    }

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn real_process_crash_before_wal_write() {
    run_real_process_crash("before_wal_write");
}

#[test]
fn real_process_crash_before_wal_sync() {
    run_real_process_crash("before_wal_sync");
}

#[test]
fn real_process_crash_after_wal_sync() {
    run_real_process_crash("after_wal_sync");
}

#[test]
fn real_process_crash_before_install() {
    run_real_process_crash("before_install");
}

#[test]
fn real_process_crash_during_multirow_install() {
    run_real_process_crash("during_multirow_install");
}

#[test]
fn real_process_crash_before_snapshot_rename() {
    run_real_process_crash("before_snapshot_rename");
}

#[test]
fn real_process_crash_before_wal_reset() {
    run_real_process_crash("before_wal_reset");
}
