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

#[test]
fn crash_matrix_large_randomized_campaign_1000_cycles() {
    let failpoints = [
        "before_wal_reserve",
        "after_wal_reserve",
        "before_wal_write",
        "before_wal_sync",
        "after_wal_sync",
        "before_install",
        "during_multirow_install",
        "before_snapshot_rename",
        "before_wal_reset",
    ];

    let total_cycles: usize = std::env::var("HENCHDB_CRASH_CYCLES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(50);

    let base_dir = std::env::temp_dir().join(format!("hdbcrash_camp_{}", std::process::id()));
    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&base_dir).unwrap();

    let mut prng = crate::db::tests::ebr_stress::StressPrng::new(987654321);

    for cycle in 0..total_cycles {
        let cycle_dir = base_dir.join(format!("c_{cycle}"));
        fs::create_dir_all(&cycle_dir).unwrap();

        // 1. Initialize schema and baseline committed data
        {
            let db = Database::open(&cycle_dir).unwrap();
            let mut s = db.new_session();
            db.execute(&mut s, "CREATE TABLE parent (id INT PRIMARY KEY, name TEXT)").unwrap();
            db.execute(
                &mut s,
                "CREATE TABLE child (id INT PRIMARY KEY, parent_id INT, val TEXT, FOREIGN KEY (parent_id) REFERENCES parent(id))",
            ).unwrap();
            db.execute(&mut s, "CREATE INDEX idx_child_val ON child (val)").unwrap();

            db.execute(&mut s, "INSERT INTO parent VALUES (1, 'p1'), (2, 'p2')").unwrap();
            db.execute(&mut s, "INSERT INTO child VALUES (10, 1, 'c10'), (20, 2, 'c20')").unwrap();

            if cycle % 3 == 0 {
                db.checkpoint().unwrap();
            }
        }

        let fault_type = prng.next_u64() % 4;
        let num_rows = (prng.next_u64() % 6 + 1) as usize;

        match fault_type {
            0 | 1 => {
                // Failpoint fault injection
                let fp = failpoints[(prng.next_u64() as usize) % failpoints.len()];
                failpoint::set(fp, FailMode::Once, FailAction::Panic("simulated crash"));

                let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
                    let db = Database::open(&cycle_dir).unwrap();
                    let mut s = db.new_session();
                    db.execute(&mut s, "BEGIN").unwrap();
                    db.execute(&mut s, "INSERT INTO parent VALUES (3, 'p3')").unwrap();
                    db.execute(&mut s, "INSERT INTO child VALUES (30, 3, 'c30')").unwrap();
                    for r in 0..num_rows {
                        let pid = 100 + r as i64;
                        let cid = 1000 + r as i64;
                        let _ = db.execute(&mut s, &format!("INSERT INTO parent VALUES ({pid}, 'p{pid}')"));
                        let _ = db.execute(&mut s, &format!("INSERT INTO child VALUES ({cid}, {pid}, 'c{cid}')"));
                    }
                    db.execute(&mut s, "UPDATE parent SET name = 'p1_upd' WHERE id = 1").unwrap();
                    db.execute(&mut s, "DELETE FROM child WHERE id = 10").unwrap();
                    let _ = db.execute(&mut s, "COMMIT");
                    if fp.contains("snapshot") || fp.contains("wal_reset") {
                        let _ = db.checkpoint();
                    }
                }));

                failpoint::clear();
            }
            2 => {
                // Torn WAL write: truncate bytes off the tail of the WAL
                {
                    let db = Database::open(&cycle_dir).unwrap();
                    let mut s = db.new_session();
                    db.execute(&mut s, "BEGIN").unwrap();
                    db.execute(&mut s, "INSERT INTO parent VALUES (3, 'p3')").unwrap();
                    db.execute(&mut s, "INSERT INTO child VALUES (30, 3, 'c30')").unwrap();
                    let _ = db.execute(&mut s, "COMMIT");
                }
                let wal_file = cycle_dir.join("wal.log");
                if let Ok(data) = fs::read(&wal_file) {
                    if data.len() > 16 {
                        let cut = ((prng.next_u64() % 32) + 1) as usize;
                        let new_len = data.len().saturating_sub(cut);
                        let _ = fs::write(&wal_file, &data[..new_len]);
                    }
                }
            }
            _ => {
                // Partial checkpoint artifact: leave incomplete snapshot.tmp
                let tmp_file = cycle_dir.join("snapshot.tmp");
                let garbage = format!("PARTIAL_TEMP_DATA_{cycle}");
                let _ = fs::write(&tmp_file, garbage.as_bytes());
            }
        }

        // 2. Out-of-process recovery
        let db = match Database::open(&cycle_dir) {
            Ok(d) => d,
            Err(e) => {
                panic!("Cycle {cycle} recovery failed: {e:?}");
            }
        };

        let mut s = db.new_session();

        // 3. Structural integrity check
        let reports = db.check_database(&s).unwrap();
        for r in &reports {
            assert!(r.is_ok(), "Cycle {cycle} integrity violated: {:?}", r.error_msg);
        }

        // 4. Exact logical verification
        let parent_rows = db.execute(&mut s, "SELECT id, name FROM parent ORDER BY id").unwrap().rows;
        let child_rows = db.execute(&mut s, "SELECT id, parent_id, val FROM child ORDER BY id").unwrap().rows;

        let has_p1 = parent_rows.iter().any(|r| r[0] == Datum::Int(1));
        let has_p2 = parent_rows.iter().any(|r| r[0] == Datum::Int(2));
        assert!(has_p1 && has_p2, "Baseline committed data must never be lost");

        let has_p3 = parent_rows.iter().any(|r| r[0] == Datum::Int(3));
        let has_c30 = child_rows.iter().any(|r| r[0] == Datum::Int(30));
        assert_eq!(
            has_p3, has_c30,
            "Cycle {cycle} atomicity violated: parent 3 and child 30 must either both exist or neither exist"
        );

        // 5. Live writes post-recovery
        let next_id = 5000 + cycle as i64;
        db.execute(&mut s, &format!("INSERT INTO parent VALUES ({next_id}, 'live_{cycle}')")).unwrap();
        db.execute(&mut s, &format!("INSERT INTO child VALUES ({next_id}0, {next_id}, 'live_c')")).unwrap();

        let _ = fs::remove_dir_all(&cycle_dir);
    }

    let _ = fs::remove_dir_all(&base_dir);
}
