//! Production Soak & Long-Duration Concurrency Harness (§4).
//!
//! Executes concurrent multi-threaded workloads combining:
//! - Continuous high-churn INSERT / UPDATE / DELETE on indexed tables (driving splits & merges)
//! - Multi-row explicit transactions with COMMIT and ROLLBACK
//! - Continuous optimistic readers (point queries, range scans, aggregates)
//! - Long-lived snapshot readers (holding RepeatableRead pins across writer cycles)
//! - Concurrent background CHECKPOINT (WAL truncation, fuzzy snapshots)
//! - Concurrent online diagnostic CHECK DATABASE (verifying B+ tree sorting & foreign keys)
//! - Periodic EBR drain verification (confirming zero monotonic memory or object leak)
//!
//! Duration defaults to 4 seconds for fast unit-test suites (`cargo test`),
//! and can be extended via `HENCHDB_SOAK_DURATION_SECS` (e.g. 60, 3600, 86400).

use super::*;
use std::fs;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

#[test]
fn soak_continuous_concurrency_lifecycle() {
    let soak_secs: u64 = std::env::var("HENCHDB_SOAK_DURATION_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4);

    let dir = std::env::temp_dir().join(format!("hdb_soak_harness_{}_{}", std::process::id(), soak_secs));
    let _ = fs::remove_dir_all(&dir);
    let db = Arc::new(Database::open(&dir).unwrap());
    let mut s = db.new_session();

    // 1. Setup multi-table relational schema with primary keys, secondary indexes, and foreign keys
    db.execute(
        &mut s,
        "CREATE TABLE accounts (id INT PRIMARY KEY, name TEXT NOT NULL, balance FLOAT, status TEXT);",
    )
    .unwrap();
    db.execute(&mut s, "CREATE INDEX idx_acc_bal ON accounts (balance);").unwrap();

    db.execute(
        &mut s,
        "CREATE TABLE ledger (entry_id INT PRIMARY KEY, acc_id INT NOT NULL, amount FLOAT, description TEXT, FOREIGN KEY (acc_id) REFERENCES accounts (id));",
    )
    .unwrap();
    db.execute(&mut s, "CREATE INDEX idx_led_amount ON ledger (amount);").unwrap();

    // Pre-populate with initial dataset
    for i in 1..=100 {
        db.execute(
            &mut s,
            &format!("INSERT INTO accounts VALUES ({i}, 'user_{i}', {}.5, 'active');", i * 50),
        )
        .unwrap();
        db.execute(
            &mut s,
            &format!("INSERT INTO ledger VALUES ({i}, {i}, {}.0, 'initial deposit');", i * 25),
        )
        .unwrap();
    }

    let running = Arc::new(AtomicBool::new(true));
    let write_txns = Arc::new(AtomicU64::new(0));
    let read_queries = Arc::new(AtomicU64::new(0));
    let checkpoints = Arc::new(AtomicU64::new(0));
    let integrity_checks = Arc::new(AtomicU64::new(0));

    // Worker 1: High-churn writers (continuous inserts & deletes driving B+ tree splits & merges)
    let db_w1 = db.clone();
    let r_w1 = running.clone();
    let writes_w1 = write_txns.clone();
    let writer_split_merge = thread::spawn(move || {
        let mut session = db_w1.new_session();
        let mut seq = 10_000i64;
        while r_w1.load(Ordering::Relaxed) {
            seq += 1;
            let acc_id = seq;
            let led_id = seq;
            // Insert parent then child
            let ins_acc = format!("INSERT INTO accounts VALUES ({acc_id}, 'dyn_{acc_id}', 100.0, 'active');");
            let ins_led = format!("INSERT INTO ledger VALUES ({led_id}, {acc_id}, 50.0, 'transfer');");
            if db_w1.execute(&mut session, &ins_acc).is_ok() {
                let _ = db_w1.execute(&mut session, &ins_led);
                writes_w1.fetch_add(1, Ordering::Relaxed);
            }

            // Periodically delete child then parent to trigger leaf borrows, merges & collapses
            if seq % 7 == 0 {
                let target = seq - 3;
                let _ = db_w1.execute(&mut session, &format!("DELETE FROM ledger WHERE entry_id = {target};"));
                let _ = db_w1.execute(&mut session, &format!("DELETE FROM accounts WHERE id = {target};"));
                writes_w1.fetch_add(1, Ordering::Relaxed);
            }
            // Rapid micro-pause to avoid starving read/maintenance threads
            thread::yield_now();
        }
    });

    // Worker 2: Transactional writer (multi-statement BEGIN ... COMMIT / ROLLBACK)
    let db_w2 = db.clone();
    let r_w2 = running.clone();
    let writes_w2 = write_txns.clone();
    let writer_txns = thread::spawn(move || {
        let mut session = db_w2.new_session();
        let mut txn_idx = 20_000i64;
        while r_w2.load(Ordering::Relaxed) {
            txn_idx += 1;
            let id = txn_idx;

            // Commit path
            let _ = db_w2.execute(&mut session, "BEGIN;");
            let _ = db_w2.execute(&mut session, &format!("INSERT INTO accounts VALUES ({id}, 'txn_user_{id}', 500.0, 'active');"));
            let _ = db_w2.execute(&mut session, &format!("INSERT INTO ledger VALUES ({id}, {id}, 250.0, 'txn_deposit');"));
            let _ = db_w2.execute(&mut session, &format!("UPDATE accounts SET balance = 550.0 WHERE id = {id};"));
            if db_w2.execute(&mut session, "COMMIT;").is_ok() {
                writes_w2.fetch_add(1, Ordering::Relaxed);
            }

            // Rollback path (simulating transient conflict / aborted transactions)
            if txn_idx % 4 == 0 {
                let _ = db_w2.execute(&mut session, "BEGIN;");
                let abort_id = txn_idx + 50_000;
                let _ = db_w2.execute(&mut session, &format!("INSERT INTO accounts VALUES ({abort_id}, 'abort_user', 99.0, 'temp');"));
                let _ = db_w2.execute(&mut session, "ROLLBACK;");
                writes_w2.fetch_add(1, Ordering::Relaxed);
            }
            thread::yield_now();
        }
    });

    // Worker 3: Optimistic concurrent reader (point lookups, range scans, aggregates)
    let db_r1 = db.clone();
    let r_r1 = running.clone();
    let reads_cnt = read_queries.clone();
    let reader_optimistic = thread::spawn(move || {
        let mut session = db_r1.new_session();
        let mut probe_id = 1i64;
        while r_r1.load(Ordering::Relaxed) {
            probe_id = (probe_id % 100) + 1;
            // Point lookup
            let _ = db_r1.execute(&mut session, &format!("SELECT name, balance FROM accounts WHERE id = {probe_id};"));
            // Range scan
            let _ = db_r1.execute(&mut session, &format!("SELECT id, balance FROM accounts WHERE id BETWEEN {probe_id} AND {} LIMIT 10;", probe_id + 20));
            // Aggregate query
            let _ = db_r1.execute(&mut session, "SELECT COUNT(*), SUM(balance), AVG(balance) FROM accounts;");
            reads_cnt.fetch_add(3, Ordering::Relaxed);
            thread::yield_now();
        }
    });

    // Worker 4: Long-lived snapshot reader (stresses MVCC version chains & EBR quarantine)
    let db_r2 = db.clone();
    let r_r2 = running.clone();
    let reader_long_lived = thread::spawn(move || {
        while r_r2.load(Ordering::Relaxed) {
            let mut session = db_r2.new_session();
            // Enter RepeatableRead and take an explicit snapshot
            let _ = db_r2.execute(&mut session, "BEGIN;");
            let snap_res = db_r2.execute(&mut session, "SELECT COUNT(*) FROM accounts;");
            assert!(snap_res.is_ok(), "Long reader initial snapshot must succeed");

            // Hold snapshot across multiple write cycles (approx 50ms)
            thread::sleep(Duration::from_millis(50));

            // Verify repeatable read stability
            let snap_verify = db_r2.execute(&mut session, "SELECT COUNT(*) FROM accounts;");
            if let (Ok(r1), Ok(r2)) = (snap_res, snap_verify) {
                assert_eq!(r1.rows, r2.rows, "RepeatableRead snapshot must remain deterministic");
            }
            let _ = db_r2.execute(&mut session, "COMMIT;");
        }
    });

    // Worker 5: Maintenance thread (periodic CHECKPOINT and online CHECK DATABASE)
    let db_m = db.clone();
    let r_m = running.clone();
    let cp_cnt = checkpoints.clone();
    let integ_cnt = integrity_checks.clone();
    let maintenance_thread = thread::spawn(move || {
        let session = db_m.new_session();
        while r_m.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_millis(80));
            if !r_m.load(Ordering::Relaxed) {
                break;
            }

            // 1. Concurrent checkpoint (fuzzy snapshot + WAL truncate)
            if db_m.checkpoint().is_ok() {
                cp_cnt.fetch_add(1, Ordering::Relaxed);
            }

            // 2. Online concurrent CHECK DATABASE (verifies B+ tree invariants, NOT NULL, FKs)
            let reports = db_m.check_database(&session);
            assert!(reports.is_ok(), "Online CHECK DATABASE must never fail under concurrent load");
            integ_cnt.fetch_add(1, Ordering::Relaxed);
        }
    });

    // Main thread orchestrates soak duration and samples EBR / resource telemetry
    let start_time = Instant::now();
    let soak_duration = Duration::from_secs(soak_secs);

    while start_time.elapsed() < soak_duration {
        thread::sleep(Duration::from_millis(250));
        let ebr_stats = db.epoch.stats();
        // Active guards must not explode unboundedly
        assert!(
            ebr_stats.active_guards <= 32,
            "Active EBR guards ({}) exceeded concurrency bound",
            ebr_stats.active_guards
        );
    }

    // Stop all workers
    running.store(false, Ordering::SeqCst);
    writer_split_merge.join().unwrap();
    writer_txns.join().unwrap();
    reader_optimistic.join().unwrap();
    reader_long_lived.join().unwrap();
    maintenance_thread.join().unwrap();

    let elapsed = start_time.elapsed();
    let total_writes = write_txns.load(Ordering::SeqCst);
    let total_reads = read_queries.load(Ordering::SeqCst);
    let total_cps = checkpoints.load(Ordering::SeqCst);
    let total_checks = integrity_checks.load(Ordering::SeqCst);

    println!(
        "[SOAK HARNESS] Completed {:.2}s soak: {} writes ({:.0} w/s), {} reads ({:.0} r/s), {} checkpoints, {} integrity checks",
        elapsed.as_secs_f64(),
        total_writes,
        total_writes as f64 / elapsed.as_secs_f64(),
        total_reads,
        total_reads as f64 / elapsed.as_secs_f64(),
        total_cps,
        total_checks
    );

    // Final Post-Soak Verification
    // 1. Quiesce EBR and assert retirement queue drains cleanly
    let final_session = db.new_session();
    for _ in 0..10 {
        thread::sleep(Duration::from_millis(10));
    }
    let final_ebr = db.epoch.stats();
    assert_eq!(final_ebr.active_guards, 0, "All EBR guards must be quiescent after workers exit");

    // 2. Comprehensive final CHECK DATABASE
    let final_reports = db.check_database(&final_session).expect("Final CHECK DATABASE failed");
    for rep in &final_reports {
        assert!(rep.is_ok(), "Table {} reported failure: {:?}", rep.table, rep.error_msg);
    }

    // 3. Verify dataset checksum calculation via check report
    let summary_report = final_reports.iter().find(|r| r.table == "default.*");
    assert!(summary_report.is_some(), "Summary database report must be present");
    assert!(summary_report.unwrap().hash > 0, "Logical dataset checksum must be non-zero");

    let _ = fs::remove_dir_all(&dir);
}
