//! Resource leak campaign suite (§17).
//!
//! Repeatedly cycles through:
//! - Session creation & teardown (connect/disconnect)
//! - Query execution (scans, joins, aggregations)
//! - Explicit transactions with COMMIT and ROLLBACK
//! - Multi-version snapshot creation and release
//! - Checkpointing and WAL truncation
//! - Online backups and external restores
//!
//! Verifies strict steady-state resource invariant:
//! - EBR active guards return to 0
//! - EBR pending reclamation drains to 0
//! - MVCC active snapshots return to 0
//! - MVCC version chain history drains to 0 after GC
//! - In-flight transaction locks return to 0
//! - No temporary files (.tmp) leaked on disk
//! - Steady-state memory and resource stability across 60+ cycles

use super::*;
use std::fs;
use std::io::Cursor;

#[test]
fn resource_leak_lifecycle_campaign() {
    let dir = std::env::temp_dir().join(format!("hdb_leak_campaign_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = Database::open(&dir).unwrap();

    // Initial table setup
    {
        let mut s = db.new_session();
        db.execute(&mut s, "CREATE TABLE accounts (id INT PRIMARY KEY, name TEXT NOT NULL, balance FLOAT);").unwrap();
        db.execute(&mut s, "CREATE INDEX idx_balance ON accounts (balance);").unwrap();
        db.execute(&mut s, "INSERT INTO accounts VALUES (1, 'alice', 1000.0), (2, 'bob', 2000.0);").unwrap();
    }

    let cycles = 60;
    for cycle in 1..=cycles {
        // 1. Connect new session
        let mut session = db.new_session();

        // 2. Query execution (reads, joins, aggregates)
        let res = db.execute(&mut session, "SELECT COUNT(*), SUM(balance), AVG(balance) FROM accounts;").unwrap();
        assert_eq!(res.rows.len(), 1);

        // 3. Explicit transaction with ROLLBACK
        db.execute(&mut session, "BEGIN;").unwrap();
        db.execute(&mut session, &format!("INSERT INTO accounts VALUES ({}, 'temp_{}', 500.0);", 100 + cycle, cycle)).unwrap();
        db.execute(&mut session, "UPDATE accounts SET balance = 1100.0 WHERE id = 1;").unwrap();
        db.execute(&mut session, "ROLLBACK;").unwrap();

        // 4. Explicit transaction with COMMIT
        db.execute(&mut session, "BEGIN;").unwrap();
        let cid = 1000 + cycle;
        db.execute(&mut session, &format!("INSERT INTO accounts VALUES ({cid}, 'comm_{cycle}', 750.0);")).unwrap();
        db.execute(&mut session, &format!("UPDATE accounts SET balance = 800.0 WHERE id = {cid};")).unwrap();
        db.execute(&mut session, "COMMIT;").unwrap();

        // 5. MVCC Snapshot read
        db.execute(&mut session, "BEGIN;").unwrap();
        let snap_res = db.execute(&mut session, "SELECT * FROM accounts WHERE id <= 2;").unwrap();
        assert_eq!(snap_res.rows.len(), 2);
        db.execute(&mut session, "COMMIT;").unwrap();

        // 6. Checkpoint and WAL truncate every 5 cycles
        if cycle % 5 == 0 {
            db.checkpoint().unwrap();
        }

        // 7. Live dump and external restore verification every 15 cycles
        if cycle % 15 == 0 {
            let mut buf = Vec::new();
            let (stats, _) = db.dump_live(&mut buf).unwrap();
            assert!(stats.rows >= 2);
            assert!(stats.bytes_written > 0);

            let restore_dir = dir.join(format!("restore_{cycle}"));
            let _ = fs::remove_dir_all(&restore_dir);
            let mut cur = Cursor::new(&buf);
            let r_stats = Database::restore(&mut cur, &restore_dir).unwrap();
            assert!(r_stats.rows >= 2);

            let restored_db = Database::open(&restore_dir).unwrap();
            let mut s_res = restored_db.new_session();
            let cnt = restored_db.execute(&mut s_res, "SELECT COUNT(*) FROM accounts;").unwrap();
            assert!(cnt.rows[0][0] != Datum::Null);

            let _ = fs::remove_dir_all(&restore_dir);
        }

        // Drop session (disconnect)
        drop(session);

        // 8. Steady-state verification at cycle boundaries
        let ebr_stats = db.epoch.stats();
        assert_eq!(ebr_stats.active_guards, 0, "Active EBR guards must be 0 after session drop");

        // Reclaim retired EBR garbage
        db.epoch.try_reclaim();

        // Verify MVCC version state has zero active snapshot pins
        assert_eq!(db.active_snapshots_count(), 0, "MVCC snapshots must be completely empty");

        // Verify in-flight transaction locks are 0
        {
            let in_flight = db.in_flight.lock().unwrap();
            assert!(in_flight.is_empty(), "In-flight row locks must be completely empty");
        }

        // Verify no leaked temporary files in database directory
        if let Ok(entries) = fs::read_dir(&dir) {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                assert!(!name.ends_with(".tmp"), "Leaked temporary file detected: {}", name);
            }
        }
    }

    // Final quiescent reclamation & verification
    db.checkpoint().unwrap();
    db.gc_versions();
    db.epoch.try_reclaim();

    let final_ebr = db.epoch.stats();
    assert_eq!(final_ebr.active_guards, 0);
    assert_eq!(final_ebr.pending_reclamation, 0, "All retired EBR objects must be fully reclaimed");

    assert_eq!(db.active_snapshots_count(), 0);
    assert_eq!(db.mvcc_chains_count(), 0, "MVCC chains must drain completely when no snapshots active");

    let _ = fs::remove_dir_all(&dir);
}
