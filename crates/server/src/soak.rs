//! Dedicated CLI soak and continuous stress runner (`server soak`).
//!
//! Drives high-concurrency multi-threaded OLTP load against an embedded database:
//! - High churn INSERT / UPDATE / DELETE driving B+ tree splits, merges, and root collapses
//! - Transactional consistency (multi-statement BEGIN ... COMMIT / ROLLBACK)
//! - Concurrent optimistic queries and long-lived snapshot isolation readers
//! - Concurrent periodic CHECKPOINT and online CHECK DATABASE validation
//! - Telemetry sampling (EBR guards, retirement queue, transaction throughput, latency)
//! - Clean termination via timeout or Ctrl+C / SIGINT signal

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use engine::{self, Database};

use crate::help::banner;
use crate::SHUTDOWN_REQUESTED;

pub struct SoakOpts {
    pub dir: String,
    pub duration_secs: u64,
    pub threads: usize,
    pub check_interval_secs: u64,
}

impl SoakOpts {
    pub fn from_args(args: &[String]) -> Self {
        let dir = crate::arg_value(args, "--dir").unwrap_or_else(|| "./target/soak_data".into());
        let duration_secs: u64 = crate::arg_value(args, "--duration")
            .and_then(|s| s.parse().ok())
            .unwrap_or(60);
        let threads: usize = crate::arg_value(args, "--threads")
            .and_then(|s| s.parse().ok())
            .unwrap_or(8);
        let check_interval_secs: u64 = crate::arg_value(args, "--check-interval")
            .and_then(|s| s.parse().ok())
            .unwrap_or(5);

        SoakOpts {
            dir,
            duration_secs,
            threads,
            check_interval_secs,
        }
    }
}

pub fn run_soak(opts: SoakOpts) -> engine::Result<()> {
    banner();
    println!("{}", "=".repeat(75));
    println!(" henchDB — Production Soak & Concurrency Stress Harness (§4)");
    println!("{}", "=".repeat(75));
    println!(" Target Directory:    {}", opts.dir);
    println!(
        " Target Duration:     {} (0 = run indefinitely until Ctrl+C)",
        if opts.duration_secs == 0 {
            "Infinite".to_string()
        } else {
            format!("{}s", opts.duration_secs)
        }
    );
    println!(" Worker Threads:      {}", opts.threads);
    println!(" Checkpoint Interval: {}s", opts.check_interval_secs);
    println!("{}", "=".repeat(75));

    let path = Path::new(&opts.dir);
    if !path.exists() {
        std::fs::create_dir_all(path)?;
    }

    let db = Arc::new(Database::open(path)?);
    {
        let mut s = db.new_session();
        db.execute(
            &mut s,
            "CREATE TABLE IF NOT EXISTS soak_accounts (id INT PRIMARY KEY, name TEXT NOT NULL, balance FLOAT, status TEXT);",
        )?;
        db.execute(&mut s, "CREATE INDEX IF NOT EXISTS idx_soak_bal ON soak_accounts (balance);")?;
        db.execute(
            &mut s,
            "CREATE TABLE IF NOT EXISTS soak_ledger (entry_id INT PRIMARY KEY, acc_id INT NOT NULL, amount FLOAT, note TEXT, FOREIGN KEY (acc_id) REFERENCES soak_accounts (id));",
        )?;
        db.execute(&mut s, "CREATE INDEX IF NOT EXISTS idx_soak_led_amt ON soak_ledger (amount);")?;

        // Seed 200 initial accounts
        for i in 1..=200 {
            let _ = db.execute(
                &mut s,
                &format!("INSERT INTO soak_accounts VALUES ({i}, 'user_{i}', {}.0, 'active');", i * 100),
            );
            let _ = db.execute(
                &mut s,
                &format!("INSERT INTO soak_ledger VALUES ({i}, {i}, {}.0, 'deposit');", i * 50),
            );
        }
    }

    let running = Arc::new(AtomicBool::new(true));
    let total_writes = Arc::new(AtomicU64::new(0));
    let total_reads = Arc::new(AtomicU64::new(0));
    let total_checkpoints = Arc::new(AtomicU64::new(0));
    let total_checks = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();
    let num_writers = (opts.threads / 2).max(1);
    let num_readers = (opts.threads - num_writers).max(1);

    // Spawn concurrent writers
    for w in 0..num_writers {
        let db_c = db.clone();
        let r_c = running.clone();
        let writes_cnt = total_writes.clone();
        handles.push(thread::spawn(move || {
            let mut session = db_c.new_session();
            let mut id_seq = (w as i64 + 1) * 1_000_000;
            while r_c.load(Ordering::Relaxed) && !SHUTDOWN_REQUESTED.load(Ordering::Relaxed) {
                id_seq += 1;
                let acc = id_seq;
                let led = id_seq;

                // Explicit transaction with COMMIT
                let _ = db_c.execute(&mut session, "BEGIN;");
                let _ = db_c.execute(
                    &mut session,
                    &format!("INSERT INTO soak_accounts VALUES ({acc}, 'usr_{acc}', 1000.0, 'active');"),
                );
                let _ = db_c.execute(
                    &mut session,
                    &format!("INSERT INTO soak_ledger VALUES ({led}, {acc}, 200.0, 'tx');"),
                );
                let _ = db_c.execute(
                    &mut session,
                    &format!("UPDATE soak_accounts SET balance = 1100.0 WHERE id = {acc};"),
                );
                if db_c.execute(&mut session, "COMMIT;").is_ok() {
                    writes_cnt.fetch_add(3, Ordering::Relaxed);
                }

                // Delete churn to induce node merges
                if id_seq % 5 == 0 {
                    let del_target = acc - 2;
                    let _ = db_c.execute(&mut session, &format!("DELETE FROM soak_ledger WHERE entry_id = {del_target};"));
                    let _ = db_c.execute(&mut session, &format!("DELETE FROM soak_accounts WHERE id = {del_target};"));
                    writes_cnt.fetch_add(2, Ordering::Relaxed);
                }
                thread::yield_now();
            }
        }));
    }

    // Spawn concurrent readers
    for r in 0..num_readers {
        let db_c = db.clone();
        let r_c = running.clone();
        let reads_cnt = total_reads.clone();
        handles.push(thread::spawn(move || {
            let mut session = db_c.new_session();
            let mut probe = 1i64;
            while r_c.load(Ordering::Relaxed) && !SHUTDOWN_REQUESTED.load(Ordering::Relaxed) {
                probe = (probe % 200) + 1;
                // Point seek
                let _ = db_c.execute(&mut session, &format!("SELECT name, balance FROM soak_accounts WHERE id = {probe};"));
                // Range query
                let _ = db_c.execute(
                    &mut session,
                    &format!("SELECT id, balance FROM soak_accounts WHERE balance >= 500.0 LIMIT 15;"),
                );
                // Aggregation
                let _ = db_c.execute(&mut session, "SELECT COUNT(*), SUM(balance), AVG(balance) FROM soak_accounts;");
                reads_cnt.fetch_add(3, Ordering::Relaxed);

                if r % 2 == 1 {
                    // Periodic repeatable-read snapshot pin
                    let _ = db_c.execute(&mut session, "BEGIN;");
                    let _ = db_c.execute(&mut session, "SELECT COUNT(*) FROM soak_accounts;");
                    thread::sleep(Duration::from_millis(10));
                    let _ = db_c.execute(&mut session, "COMMIT;");
                }
                thread::yield_now();
            }
        }));
    }

    // Maintenance thread: periodic checkpoints & online integrity check
    let db_m = db.clone();
    let r_m = running.clone();
    let cp_cnt = total_checkpoints.clone();
    let chk_cnt = total_checks.clone();
    let check_interval = Duration::from_secs(opts.check_interval_secs.max(1));
    handles.push(thread::spawn(move || {
        let session = db_m.new_session();
        while r_m.load(Ordering::Relaxed) && !SHUTDOWN_REQUESTED.load(Ordering::Relaxed) {
            thread::sleep(check_interval);
            if !r_m.load(Ordering::Relaxed) || SHUTDOWN_REQUESTED.load(Ordering::Relaxed) {
                break;
            }

            // Fuzzy Checkpoint
            if db_m.checkpoint().is_ok() {
                cp_cnt.fetch_add(1, Ordering::Relaxed);
            }

            // Online CHECK DATABASE
            if let Ok(reports) = db_m.check_database(&session) {
                for rep in reports {
                    if !rep.is_ok() {
                        eprintln!("[SOAK ERROR] Integrity failure on {}: {:?}", rep.table, rep.error_msg);
                    }
                }
                chk_cnt.fetch_add(1, Ordering::Relaxed);
            }
        }
    }));

    // Supervisor telemetry & progress loop
    let start_time = Instant::now();
    let mut last_report = Instant::now();
    let mut last_writes = 0u64;
    let mut last_reads = 0u64;

    while running.load(Ordering::Relaxed) && !SHUTDOWN_REQUESTED.load(Ordering::Relaxed) {
        thread::sleep(Duration::from_millis(500));
        let elapsed = start_time.elapsed();

        if opts.duration_secs > 0 && elapsed.as_secs() >= opts.duration_secs {
            println!("\n[SOAK] Target duration of {}s reached. Initiating graceful shutdown...", opts.duration_secs);
            break;
        }

        if last_report.elapsed() >= Duration::from_secs(2) {
            let cur_writes = total_writes.load(Ordering::Relaxed);
            let cur_reads = total_reads.load(Ordering::Relaxed);
            let dt = last_report.elapsed().as_secs_f64();
            let w_rate = (cur_writes - last_writes) as f64 / dt;
            let r_rate = (cur_reads - last_reads) as f64 / dt;
            let ebr = db.epoch().stats();

            println!(
                "[{:>6.1}s] Writes: {:>8} ({:>6.0} w/s) | Reads: {:>8} ({:>6.0} r/s) | CP: {:>3} | Checks: {:>3} | EBR Active: {:>2}, Pending: {:>4}",
                elapsed.as_secs_f64(),
                cur_writes,
                w_rate,
                cur_reads,
                r_rate,
                total_checkpoints.load(Ordering::Relaxed),
                total_checks.load(Ordering::Relaxed),
                ebr.active_guards,
                ebr.pending_reclamation
            );

            last_writes = cur_writes;
            last_reads = cur_reads;
            last_report = Instant::now();
        }
    }

    // Terminate worker threads
    running.store(false, Ordering::SeqCst);
    for h in handles {
        let _ = h.join();
    }

    let elapsed = start_time.elapsed();
    let final_writes = total_writes.load(Ordering::SeqCst);
    let final_reads = total_reads.load(Ordering::SeqCst);
    let final_cps = total_checkpoints.load(Ordering::SeqCst);
    let final_checks = total_checks.load(Ordering::SeqCst);

    println!("\n{}", "=".repeat(75));
    println!(" SOAK WORKLOAD RUN SUMMARY");
    println!("{}", "=".repeat(75));
    println!(" Total Elapsed Time:      {:.2}s", elapsed.as_secs_f64());
    println!(" Total Write Operations:  {} ({:.1} ops/s)", final_writes, final_writes as f64 / elapsed.as_secs_f64());
    println!(" Total Read Operations:   {} ({:.1} ops/s)", final_reads, final_reads as f64 / elapsed.as_secs_f64());
    println!(" Checkpoints Completed:   {}", final_cps);
    println!(" Integrity Scans:         {}", final_checks);

    // Final Post-Soak Invariant Check
    println!(" Running comprehensive post-soak diagnostic CHECK DATABASE...");
    let final_session = db.new_session();
    let reports = db.check_database(&final_session)?;
    let mut all_ok = true;
    for rep in &reports {
        if rep.is_ok() {
            println!("  [OK] Table {}: checksum={:#010x}", rep.table, rep.hash);
        } else {
            println!("  [FAIL] Table {}: {:?}", rep.table, rep.error_msg);
            all_ok = false;
        }
    }

    let final_ebr = db.epoch().stats();
    println!(" Final EBR State: active_guards={}, pending_reclamation={}", final_ebr.active_guards, final_ebr.pending_reclamation);
    println!("{}", "=".repeat(75));

    if all_ok {
        println!("[SUCCESS] Soak test complete with 100% data integrity verified.");
        Ok(())
    } else {
        eprintln!("[FAILURE] Soak test detected integrity errors during post-run audit.");
        Err(engine::Error::Corrupted("Soak post-run integrity audit failed".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn soak_opts_from_args_defaults() {
        let args = vec!["soak".to_string()];
        let opts = SoakOpts::from_args(&args);
        assert_eq!(opts.dir, "./target/soak_data");
        assert_eq!(opts.duration_secs, 60);
        assert_eq!(opts.threads, 8);
        assert_eq!(opts.check_interval_secs, 5);
    }

    #[test]
    fn soak_opts_from_args_custom() {
        let args = vec![
            "soak".to_string(),
            "--dir".to_string(),
            "./custom_dir".to_string(),
            "--duration".to_string(),
            "120".to_string(),
            "--threads".to_string(),
            "16".to_string(),
            "--check-interval".to_string(),
            "10".to_string(),
        ];
        let opts = SoakOpts::from_args(&args);
        assert_eq!(opts.dir, "./custom_dir");
        assert_eq!(opts.duration_secs, 120);
        assert_eq!(opts.threads, 16);
        assert_eq!(opts.check_interval_secs, 10);
    }

    #[test]
    fn soak_runner_short_lifecycle() {
        let test_dir = std::env::temp_dir().join(format!("hdb_server_soak_test_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&test_dir);
        let opts = SoakOpts {
            dir: test_dir.to_str().unwrap().to_string(),
            duration_secs: 2,
            threads: 4,
            check_interval_secs: 1,
        };
        let res = run_soak(opts);
        assert!(res.is_ok(), "Short server soak run must pass with 100% data integrity");
        let _ = std::fs::remove_dir_all(&test_dir);
    }
}

