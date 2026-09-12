//! Large-database validation test suite (§9).
//!
//! Validates:
//! - Multi-table schemas with foreign keys and secondary indexes under scale.
//! - Batched group-commit transaction ingestion.
//! - Update and delete churn generating MVCC version buffers.
//! - Checkpoint snapshot generation and WAL truncation.
//! - Cold restart and recovery verification with identical CRC32 logical checksums.
//! - Online `CHECK DATABASE` integrity verification.
//! - Backup dump and offline restore equivalence.

use std::fs;
use std::time::Instant;

use super::*;
use crate::backup::BackupStats;
use crate::types::Datum;

#[test]
fn largedb_lifecycle_and_recovery() {
    let base_dir = std::env::temp_dir().join(format!("hdblargedb_test_{}", std::process::id()));
    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&base_dir).unwrap();

    let db_dir = base_dir.join("primary_data");
    let restore_dir = base_dir.join("restored_data");
    let backup_file = base_dir.join("backup.hdb");

    let rows: u64 = std::env::var("HENCHDB_LARGEDB_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2_000);
    let batch_size: u64 = 400;

    println!("Starting largedb validation with {rows} rows (batch={batch_size})...");

    // 1. Initialize database and schema
    let db = Database::open(&db_dir).expect("open primary db");
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
    )
    .expect("create accounts");

    db.execute(
        &mut session,
        "CREATE INDEX idx_accounts_email ON accounts (email)",
    )
    .expect("create index accounts email");

    db.execute(
        &mut session,
        "CREATE TABLE transactions (
            id BIGINT PRIMARY KEY,
            account_id BIGINT NOT NULL,
            amount BIGINT NOT NULL,
            status TEXT NOT NULL,
            FOREIGN KEY (account_id) REFERENCES accounts(id)
        )",
    )
    .expect("create transactions");

    db.execute(
        &mut session,
        "CREATE INDEX idx_txn_account ON transactions (account_id)",
    )
    .expect("create index txn account");

    // 2. Batched ingestion
    let t_ingest = Instant::now();
    let mut done = 0u64;
    while done < rows {
        let end = (done + batch_size).min(rows);
        db.execute(&mut session, "BEGIN").unwrap();
        for i in done..end {
            let email = format!("user_{i}@example.com");
            let ins_acc = format!(
                "INSERT INTO accounts VALUES ({i}, 'User {i}', '{email}', {}, 1)",
                i * 100 + 50
            );
            db.execute(&mut session, &ins_acc).unwrap();

            let ins_txn = format!(
                "INSERT INTO transactions VALUES ({i}, {i}, {}, 'COMPLETED')",
                (i % 100) + 1
            );
            db.execute(&mut session, &ins_txn).unwrap();
        }
        db.execute(&mut session, "COMMIT").unwrap();
        done = end;
    }
    let ingest_dur = t_ingest.elapsed();
    println!("Ingested {rows} accounts + transactions in {ingest_dur:?}");

    // 3. Churn & MVCC history: update 20% of accounts, delete 10% of transactions
    let churn_count = rows / 5;
    if churn_count > 0 {
        db.execute(&mut session, "BEGIN").unwrap();
        for i in 0..churn_count {
            let upd = format!("UPDATE accounts SET balance = {} WHERE id = {i}", i * 200 + 999);
            db.execute(&mut session, &upd).unwrap();
        }
        db.execute(&mut session, "COMMIT").unwrap();

        let del_count = rows / 10;
        db.execute(&mut session, "BEGIN").unwrap();
        for i in 0..del_count {
            let del = format!("DELETE FROM transactions WHERE id = {i}");
            db.execute(&mut session, &del).unwrap();
        }
        db.execute(&mut session, "COMMIT").unwrap();
    }

    // 4. Pre-checkpoint CHECK DATABASE
    let pre_reports = db.check_database(&session).expect("pre-checkpoint check");
    assert_eq!(pre_reports.len(), 3);
    for r in &pre_reports {
        assert!(r.is_ok(), "Table {} check failed: {:?}", r.table, r.error_msg);
        assert_ne!(r.hash, 0, "Logical hash for {} should be non-zero", r.table);
    }
    let orig_acc_hash = pre_reports.iter().find(|r| r.table.ends_with("accounts")).unwrap().hash;
    let orig_txn_hash = pre_reports.iter().find(|r| r.table.ends_with("transactions")).unwrap().hash;
    let orig_db_hash = pre_reports.iter().find(|r| r.table.ends_with(".*")).unwrap().hash;

    // 5. Checkpoint
    let t_chk = Instant::now();
    db.checkpoint().expect("checkpoint");
    let chk_dur = t_chk.elapsed();
    println!("Checkpoint completed in {chk_dur:?}");
    assert!(db_dir.join("snapshot.bin").exists());

    // 6. Live backup dump
    let mut backup_out = std::io::BufWriter::new(
        fs::File::create(&backup_file).expect("create backup file"),
    );
    let dump_stats: BackupStats = db.dump(&mut backup_out).expect("dump live backup");
    use std::io::Write;
    backup_out.flush().unwrap();
    println!("Backup dump created: {} tables, {} rows", dump_stats.tables, dump_stats.rows);

    // 7. Cold restart & recovery
    drop(db);

    let t_restart = Instant::now();
    let db_reopened = Database::open(&db_dir).expect("reopen primary db");
    let restart_dur = t_restart.elapsed();
    println!("Reopened primary in {restart_dur:?}");

    let mut session_reopened = db_reopened.new_session();
    let post_reports = db_reopened
        .check_database(&session_reopened)
        .expect("post-recovery check");
    assert_eq!(post_reports.len(), 3);
    for r in &post_reports {
        assert!(r.is_ok(), "Post-restart table {} check failed: {:?}", r.table, r.error_msg);
    }

    let post_acc_hash = post_reports.iter().find(|r| r.table.ends_with("accounts")).unwrap().hash;
    let post_txn_hash = post_reports.iter().find(|r| r.table.ends_with("transactions")).unwrap().hash;
    let post_db_hash = post_reports.iter().find(|r| r.table.ends_with(".*")).unwrap().hash;
    assert_eq!(orig_acc_hash, post_acc_hash, "accounts hash mismatch across restart");
    assert_eq!(orig_txn_hash, post_txn_hash, "transactions hash mismatch across restart");
    assert_eq!(orig_db_hash, post_db_hash, "database hash mismatch across restart");

    // Query checks
    let count_acc = db_reopened
        .execute(&mut session_reopened, "SELECT COUNT(*) FROM accounts")
        .unwrap();
    assert_eq!(count_acc.rows[0][0], Datum::Int(rows as i64));

    let count_txn = db_reopened
        .execute(&mut session_reopened, "SELECT COUNT(*) FROM transactions")
        .unwrap();
    assert_eq!(count_txn.rows[0][0], Datum::Int((rows - (rows / 10)) as i64));

    // Secondary index lookup check
    let email_seek = db_reopened
        .execute(
            &mut session_reopened,
            "SELECT id, balance FROM accounts WHERE email = 'user_42@example.com'",
        )
        .unwrap();
    assert_eq!(email_seek.rows.len(), 1);
    assert_eq!(email_seek.rows[0][0], Datum::Int(42));

    drop(db_reopened);

    // 8. Offline restore verification
    let mut backup_reader = std::io::BufReader::new(
        fs::File::open(&backup_file).expect("open backup file"),
    );
    let restore_stats = Database::restore(&mut backup_reader, &restore_dir).expect("restore");
    assert_eq!(restore_stats.tables, 2);

    let db_restored = Database::open(&restore_dir).expect("open restored db");
    let session_restored = db_restored.new_session();
    let restored_reports = db_restored
        .check_database(&session_restored)
        .expect("restored check");
    assert_eq!(restored_reports.len(), 3);
    for r in &restored_reports {
        assert!(r.is_ok(), "Restored table {} check failed: {:?}", r.table, r.error_msg);
    }
    let restored_acc_hash = restored_reports.iter().find(|r| r.table.ends_with("accounts")).unwrap().hash;
    let restored_txn_hash = restored_reports.iter().find(|r| r.table.ends_with("transactions")).unwrap().hash;
    let restored_db_hash = restored_reports.iter().find(|r| r.table.ends_with(".*")).unwrap().hash;
    assert_eq!(orig_acc_hash, restored_acc_hash, "accounts hash mismatch across restore");
    assert_eq!(orig_txn_hash, restored_txn_hash, "transactions hash mismatch across restore");
    assert_eq!(orig_db_hash, restored_db_hash, "database hash mismatch across restore");

    drop(db_restored);

    let _ = fs::remove_dir_all(&base_dir);
    println!("Large-database lifecycle validation passed cleanly!");
}
