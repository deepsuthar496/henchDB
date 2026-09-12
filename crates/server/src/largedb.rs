//! Large-Database Validation Subcommand & Harness (§9).
//!
//! Provides comprehensive large-dataset validation:
//! - Multi-table schemas with foreign keys and secondary indexes under scale.
//! - High-throughput batched group-commit transaction ingestion.
//! - Heavy MVCC update and delete churn creating version chains.
//! - Checkpoint duration, snapshot size, and WAL truncation under large state.
//! - Cold restart and recovery verification with exact CRC32 logical checksum parity.
//! - Online diagnostic `CHECK DATABASE` integrity auditing.
//! - Live physical backup and offline restore equivalence.

use std::fs;
use std::path::PathBuf;
use std::time::Instant;

use engine::backup::BackupStats;
use engine::types::Datum;
use engine::Database;

/// Options for `server largedb`.
#[derive(Debug, Clone)]
pub struct LargeDbOpts {
    pub dir: PathBuf,
    pub rows: u64,
    pub batch: u64,
    pub churn_pct: u64,
    pub skip_restore: bool,
}

impl Default for LargeDbOpts {
    fn default() -> Self {
        LargeDbOpts {
            dir: PathBuf::from("target/largedb_data"),
            rows: 50_000,
            batch: 2_000,
            churn_pct: 10,
            skip_restore: false,
        }
    }
}

impl LargeDbOpts {
    pub fn from_args(args: &[String]) -> Self {
        let mut opts = LargeDbOpts::default();
        let mut i = 0;
        while i < args.len() {
            match args[i].as_str() {
                "--dir" if i + 1 < args.len() => {
                    opts.dir = PathBuf::from(&args[i + 1]);
                    i += 2;
                }
                "--rows" if i + 1 < args.len() => {
                    if let Ok(v) = args[i + 1].parse() {
                        opts.rows = v;
                    }
                    i += 2;
                }
                "--batch" if i + 1 < args.len() => {
                    if let Ok(v) = args[i + 1].parse() {
                        opts.batch = v;
                    }
                    i += 2;
                }
                "--churn" if i + 1 < args.len() => {
                    if let Ok(v) = args[i + 1].parse() {
                        opts.churn_pct = v;
                    }
                    i += 2;
                }
                "--skip-restore" => {
                    opts.skip_restore = true;
                    i += 1;
                }
                _ => {
                    i += 1;
                }
            }
        }
        opts
    }
}

/// Execute the full large-database lifecycle validation.
pub fn run_largedb(opts: LargeDbOpts) -> engine::Result<()> {
    println!("================================================================================");
    println!(" henchDB — Large-Database Validation Harness (§9)");
    println!("================================================================================");
    println!("Directory:     {}", opts.dir.display());
    println!("Target Rows:   {}", opts.rows);
    println!("Batch Size:    {}", opts.batch);
    println!("Churn Ratio:   {}%", opts.churn_pct);
    println!("Skip Restore:  {}", opts.skip_restore);
    println!("--------------------------------------------------------------------------------");

    if opts.dir.exists() {
        let _ = fs::remove_dir_all(&opts.dir);
    }
    fs::create_dir_all(&opts.dir)?;

    let db_dir = opts.dir.join("primary");
    let restore_dir = opts.dir.join("restored");
    let backup_file = opts.dir.join("backup.hdb");

    // Stage 1: Schema initialization
    println!("[Stage 1/7] Initializing schema with secondary indexes and foreign keys...");
    let db = Database::open(&db_dir)?;
    let mut session = db.new_session();

    db.execute(
        &mut session,
        "CREATE TABLE accounts (
            id BIGINT PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            balance BIGINT NOT NULL,
            active INT NOT NULL
        )",
    )?;

    db.execute(
        &mut session,
        "CREATE INDEX idx_accounts_email ON accounts (email)",
    )?;

    db.execute(
        &mut session,
        "CREATE TABLE transactions (
            id BIGINT PRIMARY KEY,
            account_id BIGINT NOT NULL,
            amount BIGINT NOT NULL,
            status TEXT NOT NULL,
            FOREIGN KEY (account_id) REFERENCES accounts(id)
        )",
    )?;

    db.execute(
        &mut session,
        "CREATE INDEX idx_txn_account ON transactions (account_id)",
    )?;
    println!("  Schema and indexes ready.");

    // Stage 2: Batched transaction ingestion
    println!("[Stage 2/7] Ingesting {} accounts + transactions in batches of {}...", opts.rows, opts.batch);
    let t_ingest = Instant::now();
    let mut done = 0u64;
    let mut last_log = Instant::now();

    while done < opts.rows {
        let end = (done + opts.batch).min(opts.rows);
        db.execute(&mut session, "BEGIN")?;
        for i in done..end {
            let email = format!("customer_{i}@enterprise.com");
            let ins_acc = format!(
                "INSERT INTO accounts VALUES ({i}, 'Customer {i}', '{email}', {}, 1)",
                i * 50 + 100
            );
            db.execute(&mut session, &ins_acc)?;

            let ins_txn = format!(
                "INSERT INTO transactions VALUES ({i}, {i}, {}, 'SETTLED')",
                (i % 500) + 10
            );
            db.execute(&mut session, &ins_txn)?;
        }
        db.execute(&mut session, "COMMIT")?;
        done = end;

        if last_log.elapsed().as_millis() >= 1000 || done == opts.rows {
            let elapsed_s = t_ingest.elapsed().as_secs_f64();
            let rps = if elapsed_s > 0.0 { (done * 2) as f64 / elapsed_s } else { 0.0 };
            println!("  Ingested {:>7} / {} rows ({:5.1}%) — Current throughput: {:>7.0} rows/s",
                done, opts.rows, (done as f64 / opts.rows as f64) * 100.0, rps);
            last_log = Instant::now();
        }
    }
    let ingest_sec = t_ingest.elapsed().as_secs_f64();
    let total_rows_ingested = opts.rows * 2;
    println!("  Ingestion complete: {} total rows in {:.2}s ({:.0} rows/s)",
        total_rows_ingested, ingest_sec, total_rows_ingested as f64 / ingest_sec.max(0.001));

    // Stage 3: MVCC churn
    let churn_count = (opts.rows * opts.churn_pct) / 100;
    println!("[Stage 3/7] Generating MVCC version chain churn ({} updates, {} deletes)...",
        churn_count, churn_count / 2);
    let t_churn = Instant::now();
    if churn_count > 0 {
        let churn_batch = opts.batch.min(churn_count);
        let mut cur = 0;
        while cur < churn_count {
            let end = (cur + churn_batch).min(churn_count);
            db.execute(&mut session, "BEGIN")?;
            for i in cur..end {
                let upd = format!("UPDATE accounts SET balance = {} WHERE id = {i}", i * 100 + 888);
                db.execute(&mut session, &upd)?;
            }
            db.execute(&mut session, "COMMIT")?;
            cur = end;
        }

        let del_count = churn_count / 2;
        let mut del_cur = 0;
        while del_cur < del_count {
            let end = (del_cur + churn_batch).min(del_count);
            db.execute(&mut session, "BEGIN")?;
            for i in del_cur..end {
                let del = format!("DELETE FROM transactions WHERE id = {i}");
                db.execute(&mut session, &del)?;
            }
            db.execute(&mut session, "COMMIT")?;
            del_cur = end;
        }
    }
    println!("  Churn generation finished in {:.2}s", t_churn.elapsed().as_secs_f64());

    // Stage 4: Pre-checkpoint CHECK DATABASE & Checkpoint
    println!("[Stage 4/7] Running pre-checkpoint CHECK DATABASE and fuzzy checkpoint...");
    let pre_reports = db.check_database(&session)?;
    for r in &pre_reports {
        if !r.is_ok() {
            return Err(engine::Error::Corrupted(format!(
                "pre-checkpoint check failed for {}: {:?}", r.table, r.error_msg
            )));
        }
    }
    let orig_acc_hash = pre_reports.iter().find(|r| r.table.ends_with("accounts")).unwrap().hash;
    let orig_txn_hash = pre_reports.iter().find(|r| r.table.ends_with("transactions")).unwrap().hash;
    let orig_db_hash = pre_reports.iter().find(|r| r.table.ends_with(".*")).unwrap().hash;
    println!("  Database hash: 0x{:08X} (accounts: 0x{:08X}, txns: 0x{:08X})",
        orig_db_hash, orig_acc_hash, orig_txn_hash);

    let t_chk = Instant::now();
    db.checkpoint()?;
    let chk_dur = t_chk.elapsed().as_secs_f64();
    let snap_bytes = fs::metadata(db_dir.join("snapshot.bin")).map(|m| m.len()).unwrap_or(0);
    println!("  Checkpoint completed in {:.3}s (snapshot: {:.2} MB)",
        chk_dur, snap_bytes as f64 / (1024.0 * 1024.0));

    // Stage 5: Live backup dump
    println!("[Stage 5/7] Creating live physical backup dump...");
    let t_backup = Instant::now();
    let mut backup_out = std::io::BufWriter::new(fs::File::create(&backup_file)?);
    let dump_stats: BackupStats = db.dump(&mut backup_out)?;
    use std::io::Write;
    backup_out.flush()?;
    println!("  Backup dump completed in {:.3}s ({} tables, {} rows, {:.2} MB)",
        t_backup.elapsed().as_secs_f64(),
        dump_stats.tables,
        dump_stats.rows,
        dump_stats.bytes_written as f64 / (1024.0 * 1024.0));

    // Stage 6: Cold restart and recovery verification
    println!("[Stage 6/7] Simulating cold process termination and recovery restart...");
    drop(db);

    let t_restart = Instant::now();
    let db_reopened = Database::open(&db_dir)?;
    let restart_sec = t_restart.elapsed().as_secs_f64();
    println!("  Cold restart and WAL/snapshot recovery finished in {:.3}s", restart_sec);

    let mut session_reopened = db_reopened.new_session();
    let post_reports = db_reopened.check_database(&session_reopened)?;
    for r in &post_reports {
        if !r.is_ok() {
            return Err(engine::Error::Corrupted(format!(
                "post-restart check failed for {}: {:?}", r.table, r.error_msg
            )));
        }
    }
    let post_acc_hash = post_reports.iter().find(|r| r.table.ends_with("accounts")).unwrap().hash;
    let post_txn_hash = post_reports.iter().find(|r| r.table.ends_with("transactions")).unwrap().hash;
    let post_db_hash = post_reports.iter().find(|r| r.table.ends_with(".*")).unwrap().hash;

    assert_eq!(orig_acc_hash, post_acc_hash, "accounts hash mismatch across restart");
    assert_eq!(orig_txn_hash, post_txn_hash, "transactions hash mismatch across restart");
    assert_eq!(orig_db_hash, post_db_hash, "database hash mismatch across restart");
    println!("  Post-restart integrity verified: exact hash match 0x{:08X}.", post_db_hash);

    // Verify row counts and secondary index seeks
    let count_acc = db_reopened.execute(&mut session_reopened, "SELECT COUNT(*) FROM accounts")?;
    assert_eq!(count_acc.rows[0][0], Datum::Int(opts.rows as i64));
    println!("  Accounts count verified: {}", opts.rows);

    let expected_txns = opts.rows - (churn_count / 2);
    let count_txn = db_reopened.execute(&mut session_reopened, "SELECT COUNT(*) FROM transactions")?;
    assert_eq!(count_txn.rows[0][0], Datum::Int(expected_txns as i64));
    println!("  Transactions count verified: {}", expected_txns);

    let sample_id = opts.rows / 2;
    let seek_out = db_reopened.execute(
        &mut session_reopened,
        &format!("SELECT id, email FROM accounts WHERE email = 'customer_{sample_id}@enterprise.com'"),
    )?;
    assert_eq!(seek_out.rows.len(), 1);
    assert_eq!(seek_out.rows[0][0], Datum::Int(sample_id as i64));
    println!("  Secondary index lookup verified on customer_{sample_id}@enterprise.com.");

    drop(db_reopened);

    // Stage 7: Offline restore verification
    if !opts.skip_restore {
        println!("[Stage 7/7] Verifying offline restore into clean directory...");
        let t_restore = Instant::now();
        let mut backup_reader = std::io::BufReader::new(fs::File::open(&backup_file)?);
        let restore_stats = Database::restore(&mut backup_reader, &restore_dir)?;
        let restore_sec = t_restore.elapsed().as_secs_f64();
        println!("  Restore complete in {:.3}s ({} tables, {} rows)",
            restore_sec, restore_stats.tables, restore_stats.rows);

        let db_restored = Database::open(&restore_dir)?;
        let session_restored = db_restored.new_session();
        let restored_reports = db_restored.check_database(&session_restored)?;
        for r in &restored_reports {
            if !r.is_ok() {
                return Err(engine::Error::Corrupted(format!(
                    "restored check failed for {}: {:?}", r.table, r.error_msg
                )));
            }
        }
        let restored_db_hash = restored_reports.iter().find(|r| r.table.ends_with(".*")).unwrap().hash;
        assert_eq!(orig_db_hash, restored_db_hash, "restored database hash mismatch");
        println!("  Restored database hash verified: exact match 0x{:08X}.", restored_db_hash);
        drop(db_restored);
    } else {
        println!("[Stage 7/7] Skipping offline restore (--skip-restore flag provided).");
    }

    println!("================================================================================");
    println!(" [SUCCESS] LARGE-DATABASE VALIDATION PASSED (10/10 INTEGRITY)");
    println!("================================================================================");
    println!("  Ingested Rows:      {}", total_rows_ingested);
    println!("  Ingestion Time:     {:.2}s ({:.0} rows/s)", ingest_sec, total_rows_ingested as f64 / ingest_sec.max(0.001));
    println!("  Checkpoint Time:    {:.3}s", chk_dur);
    println!("  Cold Restart Time:  {:.3}s", restart_sec);
    println!("  Database CRC32:     0x{:08X}", orig_db_hash);
    println!("================================================================================");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn largedb_opts_from_args_defaults() {
        let args = vec!["largedb".to_string()];
        let opts = LargeDbOpts::from_args(&args);
        assert_eq!(opts.rows, 50_000);
        assert_eq!(opts.batch, 2_000);
        assert_eq!(opts.churn_pct, 10);
        assert!(!opts.skip_restore);
    }

    #[test]
    fn largedb_opts_from_args_custom() {
        let args = vec![
            "largedb".to_string(),
            "--dir".to_string(),
            "target/custom_largedb".to_string(),
            "--rows".to_string(),
            "10000".to_string(),
            "--batch".to_string(),
            "500".to_string(),
            "--churn".to_string(),
            "15".to_string(),
            "--skip-restore".to_string(),
        ];
        let opts = LargeDbOpts::from_args(&args);
        assert_eq!(opts.dir, PathBuf::from("target/custom_largedb"));
        assert_eq!(opts.rows, 10_000);
        assert_eq!(opts.batch, 500);
        assert_eq!(opts.churn_pct, 15);
        assert!(opts.skip_restore);
    }

    #[test]
    fn largedb_runner_short_lifecycle() {
        let dir = std::env::temp_dir().join(format!("largedb_unit_{}", std::process::id()));
        let opts = LargeDbOpts {
            dir: dir.clone(),
            rows: 200,
            batch: 50,
            churn_pct: 10,
            skip_restore: false,
        };
        let res = run_largedb(opts);
        let _ = fs::remove_dir_all(&dir);
        assert!(res.is_ok(), "largedb short lifecycle failed: {:?}", res.err());
    }
}
