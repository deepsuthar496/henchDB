//! Concurrent storage integrity testing suite (§16).
//!
//! Verifies that `CHECK DATABASE` and `check_table` maintain absolute consistency
//! and detect zero violations while running concurrently with:
//! - INSERT, UPDATE, DELETE mutations
//! - B+Tree node splits and merges
//! - Checkpoint creation and WAL truncation
//! - MVCC snapshots and concurrent point/range reads
//! - Concurrent DDL (CREATE TABLE, DROP TABLE)
//!
//! Invariants verified:
//! - Strict monotonic primary key ordering
//! - Secondary index bidirectional consistency (forward: index -> row, reverse: row -> index)
//! - Foreign key referential integrity (child -> existing parent PK)
//! - Zero memory leaks or dangling tree references
//! - Clean serialization with concurrent DDL under `commit_lock`

use super::*;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

#[test]
fn concurrent_check_database_under_high_write_split_merge_load() {
    let dir = std::env::temp_dir().join(format!("hdb_storage_integ_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = Arc::new(Database::open(&dir).unwrap());
    let mut s = db.new_session();

    // 1. Setup relational schema with primary keys, secondary index, and foreign key
    db.execute(
        &mut s,
        "CREATE TABLE customers (id INT PRIMARY KEY, name TEXT NOT NULL, score FLOAT);",
    )
    .unwrap();
    db.execute(&mut s, "CREATE INDEX idx_cust_score ON customers (score);")
        .unwrap();

    db.execute(
        &mut s,
        "CREATE TABLE orders (id INT PRIMARY KEY, cust_id INT NOT NULL, amount FLOAT, FOREIGN KEY (cust_id) REFERENCES customers (id));",
    )
    .unwrap();
    db.execute(&mut s, "CREATE INDEX idx_ord_amount ON orders (amount);")
        .unwrap();

    // Seed initial rows
    for i in 1..=50 {
        db.execute(
            &mut s,
            &format!("INSERT INTO customers VALUES ({i}, 'customer_{i}', {}.5);", i * 10),
        )
        .unwrap();
        db.execute(
            &mut s,
            &format!("INSERT INTO orders VALUES ({i}, {i}, {}.0);", i * 20),
        )
        .unwrap();
    }

    let running = Arc::new(AtomicBool::new(true));
    let check_count = Arc::new(AtomicU64::new(0));

    // Thread 1: High churn inserts & deletes causing splits and merges
    let db1 = db.clone();
    let r1 = running.clone();
    let writer_split_merge = thread::spawn(move || {
        let mut s1 = db1.new_session();
        let mut id = 1000i64;
        while r1.load(Ordering::Relaxed) {
            id += 1;
            // Insert parent then child
            let cust_sql = format!("INSERT INTO customers VALUES ({id}, 'dyn_cust_{id}', {}.0);", id % 100);
            let ord_sql = format!("INSERT INTO orders VALUES ({id}, {id}, {}.0);", id % 500);
            if db1.execute(&mut s1, &cust_sql).is_ok() {
                let _ = db1.execute(&mut s1, &ord_sql);
            }

            // Periodically delete child then parent to trigger node borrows and merges
            if id % 3 == 0 {
                let del_id = id - 2;
                let _ = db1.execute(&mut s1, &format!("DELETE FROM orders WHERE id = {del_id};"));
                let _ = db1.execute(&mut s1, &format!("DELETE FROM customers WHERE id = {del_id};"));
            }
            thread::yield_now();
        }
    });

    // Thread 2: In-place & non-in-place updates modifying secondary index values
    let db2 = db.clone();
    let r2 = running.clone();
    let writer_updates = thread::spawn(move || {
        let mut s2 = db2.new_session();
        let mut step = 0;
        while r2.load(Ordering::Relaxed) {
            step += 1;
            let target_id = (step % 40) + 1; // within seeded 1..50
            let new_score = (step * 7 % 1000) as f64;
            let new_amount = (step * 13 % 2000) as f64;
            let _ = db2.execute(
                &mut s2,
                &format!("UPDATE customers SET score = {new_score} WHERE id = {target_id};"),
            );
            let _ = db2.execute(
                &mut s2,
                &format!("UPDATE orders SET amount = {new_amount} WHERE id = {target_id};"),
            );
            thread::yield_now();
        }
    });

    // Thread 3: MVCC snapshot readers executing concurrent range queries
    let db3 = db.clone();
    let r3 = running.clone();
    let reader_mvcc = thread::spawn(move || {
        let mut s3 = db3.new_session();
        while r3.load(Ordering::Relaxed) {
            // RepeatableRead snapshot: verify point-in-time consistency
            let _ = db3.execute(&mut s3, "BEGIN;");
            let out = db3.execute(
                &mut s3,
                "SELECT c.id, c.name, o.amount FROM customers c JOIN orders o ON c.id = o.cust_id WHERE c.id <= 30;",
            );
            if let Ok(res) = out {
                assert!(res.rows.len() <= 30);
            }
            let _ = db3.execute(&mut s3, "COMMIT;");
            thread::yield_now();
        }
    });

    // Thread 4: Background checkpoints persisting snapshots & truncating WAL
    let db4 = db.clone();
    let r4 = running.clone();
    let checkpointer = thread::spawn(move || {
        while r4.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_millis(25));
            let _ = db4.checkpoint();
        }
    });

    // Thread 5: Main thread continuously running CHECK DATABASE
    let mut check_session = db.new_session();
    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline {
        // Direct method call
        let reports = db.check_database(&check_session).unwrap();
        assert!(reports.len() >= 3); // customers, orders, and database summary
        for report in &reports {
            assert!(
                report.is_ok(),
                "CHECK DATABASE reported error during concurrent load: table={}, error={:?}",
                report.table,
                report.error_msg
            );
            assert!(report.hash > 0, "Logical hash must be non-zero");
        }

        // SQL command execution
        let sql_out = db.execute(&mut check_session, "CHECK DATABASE;").unwrap();
        assert!(sql_out.rows.len() >= 3);
        for row in &sql_out.rows {
            assert_eq!(
                row[2],
                Datum::Text("status".into()),
                "SQL CHECK DATABASE row error: table={:?}, op={:?}, msg={:?}",
                row[0], row[1], row[3]
            );
        }

        check_count.fetch_add(1, Ordering::Relaxed);
    }

    running.store(false, Ordering::Relaxed);
    let _ = writer_split_merge.join();
    let _ = writer_updates.join();
    let _ = reader_mvcc.join();
    let _ = checkpointer.join();

    // Final quiescent check: must pass with 0 errors
    let final_reports = db.check_database(&check_session).unwrap();
    for report in &final_reports {
        assert!(report.is_ok(), "Final CHECK DATABASE failed: {:?}", report.error_msg);
    }

    assert!(check_count.load(Ordering::Relaxed) >= 5, "Must complete multiple concurrent check runs");
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn concurrent_storage_integrity_with_ddl() {
    let dir = std::env::temp_dir().join(format!("hdb_storage_ddl_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = Arc::new(Database::open(&dir).unwrap());
    let mut s = db.new_session();

    db.execute(&mut s, "CREATE TABLE persistent (id INT PRIMARY KEY, val TEXT);").unwrap();
    db.execute(&mut s, "INSERT INTO persistent VALUES (1, 'base_val');").unwrap();

    let running = Arc::new(AtomicBool::new(true));

    // Writer thread mutating persistent table
    let db_w = db.clone();
    let r_w = running.clone();
    let writer_handle = thread::spawn(move || {
        let mut sw = db_w.new_session();
        let mut k = 100i64;
        while r_w.load(Ordering::Relaxed) {
            k += 1;
            let _ = db_w.execute(&mut sw, &format!("INSERT INTO persistent VALUES ({k}, 'v_{k}');"));
            thread::yield_now();
        }
    });

    // DDL thread rapidly creating and dropping ephemeral tables
    let db_ddl = db.clone();
    let r_ddl = running.clone();
    let ddl_handle = thread::spawn(move || {
        let mut s_ddl = db_ddl.new_session();
        let mut ddl_id = 0;
        while r_ddl.load(Ordering::Relaxed) {
            ddl_id += 1;
            let tbl = format!("temp_ddl_{ddl_id}");
            let _ = db_ddl.execute(
                &mut s_ddl,
                &format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, tag TEXT);"),
            );
            let _ = db_ddl.execute(
                &mut s_ddl,
                &format!("INSERT INTO {tbl} VALUES (1, 'payload');"),
            );
            let _ = db_ddl.execute(&mut s_ddl, &format!("DROP TABLE {tbl};"));
            thread::yield_now();
        }
    });

    // Observer thread continuously running CHECK DATABASE concurrently with DDL
    let s_obs = db.new_session();
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut ddl_checks = 0;
    while Instant::now() < deadline {
        let reports = db.check_database(&s_obs).unwrap();
        // At any point, whatever tables are registered in catalog must pass 100% cleanly
        for report in &reports {
            assert!(
                report.is_ok(),
                "CHECK DATABASE failed during concurrent DDL: table={}, err={:?}",
                report.table,
                report.error_msg
            );
        }
        ddl_checks += 1;
        thread::yield_now();
    }

    running.store(false, Ordering::Relaxed);
    let _ = writer_handle.join();
    let _ = ddl_handle.join();

    assert!(ddl_checks >= 5, "Completed concurrent DDL checks");
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_sweep_orphan_tmp_files_on_open() {
    let dir = std::env::temp_dir().join(format!("hdb_tmp_sweep_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();

    // Create orphaned temporary files simulating crashed checkpoints, dumps, or page flushes
    let snap_bin_tmp = dir.join("snapshot.bin.tmp");
    let snap_tmp = dir.join("snapshot.tmp");
    let pages_tmp = dir.join("pages.bin.tmp");
    let custom_tmp = dir.join(".wal_00000000_00000001.tmp.9999");
    fs::write(&snap_bin_tmp, b"orphan snapshot.bin.tmp 8B content").unwrap();
    fs::write(&snap_tmp, b"orphan snapshot.tmp content").unwrap();
    fs::write(&pages_tmp, b"orphan pages.bin.tmp content").unwrap();
    fs::write(&custom_tmp, b"orphan .tmp. content").unwrap();

    assert!(snap_bin_tmp.exists());
    assert!(snap_tmp.exists());
    assert!(pages_tmp.exists());
    assert!(custom_tmp.exists());

    // Opening database must sweep all orphan .tmp files
    let db = Database::open(&dir).unwrap();

    assert!(!snap_bin_tmp.exists(), "snapshot.bin.tmp must be swept on open");
    assert!(!snap_tmp.exists(), "snapshot.tmp must be swept on open");
    assert!(!pages_tmp.exists(), "pages.bin.tmp must be swept on open");
    assert!(!custom_tmp.exists(), "orphan .tmp. files must be swept on open");

    // Operations succeed normally
    let mut s = db.new_session();
    db.execute(&mut s, "CREATE TABLE t (id INT PRIMARY KEY);").unwrap();
    db.execute(&mut s, "INSERT INTO t VALUES (1);").unwrap();
    let out = db.execute(&mut s, "SELECT COUNT(*) FROM t;").unwrap();
    assert_eq!(out.rows[0][0], Datum::Int(1));

    drop(db);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_server_dir_lock_prevents_concurrent_open() {
    let dir = std::env::temp_dir().join(format!("hdb_dir_lock_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();

    let db1 = Database::open(&dir).unwrap();
    assert!(dir.join("server.lock").exists());

    #[cfg(windows)]
    {
        // Second process / handle attempting to open the same directory concurrently
        let res = Database::open(&dir);
        match res {
            Err(Error::InvalidOperation(msg)) => {
                assert!(
                    msg.contains("directory is locked by a live server process"),
                    "unexpected error message: {msg}"
                );
            }
            Err(e) => panic!("expected Error::InvalidOperation with lock rejection, got: {e:?}"),
            Ok(_) => panic!("expected lock rejection error, got Ok"),
        }
    }

    // Dropping the first database releases the OS kernel lock on server.lock
    drop(db1);

    // Now opening the directory succeeds
    let db2 = Database::open(&dir).expect("should succeed after first database is dropped");
    let mut s = db2.new_session();
    db2.execute(&mut s, "CREATE TABLE t (id INT PRIMARY KEY);").unwrap();
    let out = db2.execute(&mut s, "SELECT COUNT(*) FROM t;").unwrap();
    assert_eq!(out.rows[0][0], Datum::Int(0));

    drop(db2);
    let _ = fs::remove_dir_all(&dir);
}
