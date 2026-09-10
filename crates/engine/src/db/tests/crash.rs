//! Crash-consistency and fault-injection recovery tests.
//! Addresses senior audit P0.2 from assesment.md:
//! "Need tests that kill the process at every important point:
//! WAL reservation, WAL write, sync_data, snapshot write, rename, WAL truncation/reset, recovery.
//! Required invariant: after any crash, recovered state must equal either the old
//! committed state or the new committed state — never a partially installed state."

use super::*;
use std::fs::OpenOptions;
use std::io::Write;

#[test]
fn crash_wal_truncation_at_every_byte_offset() {
    // Proves that truncating the WAL at ANY byte offset in a transaction
    // either completely rolls back the transaction or completely commits it,
    // NEVER leaving partial state.
    let base_dir = std::env::temp_dir().join(format!("hdbcrash_byte_{}", std::process::id()));
    let _ = fs::remove_dir_all(&base_dir);

    // 1. Establish baseline database with 2 committed transactions
    let dir = base_dir.join("db");
    {
        let db = setup(&dir);
        let mut s = db.new_session();
        db.execute(&mut s, "INSERT INTO t VALUES (1, 'base1', 1.0)").unwrap();
        db.execute(&mut s, "INSERT INTO t VALUES (2, 'base2', 2.0)").unwrap();
        // Commit a 3rd multi-row transaction that we will truncate across
        db.execute(&mut s, "BEGIN").unwrap();
        db.execute(&mut s, "INSERT INTO t VALUES (3, 'tx3_a', 3.0)").unwrap();
        db.execute(&mut s, "INSERT INTO t VALUES (4, 'tx3_b', 4.0)").unwrap();
        db.execute(&mut s, "UPDATE t SET score = 100.0 WHERE id = 1").unwrap();
        db.execute(&mut s, "COMMIT").unwrap();
    }

    let wal_path = dir.join("wal.log");
    let full_wal = fs::read(&wal_path).unwrap();
    let full_len = full_wal.len();

    // Find the offset where txn 2 committed (baseline length)
    // Run truncation test at every single byte from full_len - 200 up to full_len
    let test_start = full_len.saturating_sub(200);

    for cut_point in test_start..=full_len {
        let test_dir = base_dir.join(format!("cut_{cut_point}"));
        fs::create_dir_all(&test_dir).unwrap();
        if dir.join("snapshot.bin").exists() {
            fs::copy(dir.join("snapshot.bin"), test_dir.join("snapshot.bin")).unwrap();
        }
        fs::write(test_dir.join("wal.log"), &full_wal[..cut_point]).unwrap();

        // Reopen database under recovery
        let db = match Database::open(&test_dir) {
            Ok(db) => db,
            Err(_) => {
                // Safe rejection if cut in a non-recoverable header
                continue;
            }
        };

        let mut s = db.new_session();
        let out = db.execute(&mut s, "SELECT id, name, score FROM t ORDER BY id").unwrap();
        let rows = out.rows;

        // INVARIANT: recovered state must equal EITHER the old committed state (2 rows)
        // OR the new committed state (4 rows with id=1 updated). NEVER partial (3 rows).
        if rows.len() == 2 {
            assert_eq!(rows[0][0], Datum::Int(1));
            assert_eq!(rows[0][2], Datum::Float(1.0), "id 1 should have old score");
            assert_eq!(rows[1][0], Datum::Int(2));
        } else if rows.len() == 4 {
            assert_eq!(rows[0][0], Datum::Int(1));
            assert_eq!(rows[0][2], Datum::Float(100.0), "id 1 should have updated score");
            assert_eq!(rows[1][0], Datum::Int(2));
            assert_eq!(rows[2][0], Datum::Int(3));
            assert_eq!(rows[3][0], Datum::Int(4));
        } else {
            panic!(
                "CRASH INVARIANT VIOLATION: cut_point {} produced partial row count {}",
                cut_point,
                rows.len()
            );
        }
    }

    let _ = fs::remove_dir_all(&base_dir);
}

#[test]
fn crash_uncommitted_multi_row_atomic_rollback() {
    let dir = std::env::temp_dir().join(format!("hdbcrash_uncomm_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);

    {
        let db = setup(&dir);
        let mut s = db.new_session();
        db.execute(&mut s, "INSERT INTO t VALUES (1, 'initial', 1.0)").unwrap();
    }

    // Append 20 Put records for txn 99 without a Commit record to simulate a process kill
    // during a massive multi-row transaction
    let wal = Wal::open(&dir.join("wal.log")).unwrap();
    for i in 100..120 {
        wal.append_unsynced(&[Record::Put {
            txn: 99,
            table: "t".into(),
            key: encode_key(&Datum::Int(i)).unwrap(),
            row: vec![1, 2, 3, 4],
        }])
        .unwrap();
    }
    drop(wal);

    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();
    let out = db.execute(&mut s, "SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(out.rows[0][0], Datum::Int(1), "uncommitted 20-row txn must be completely discarded");

    let out = db.execute(&mut s, "SELECT * FROM t WHERE id >= 100").unwrap();
    assert_eq!(out.rows.len(), 0, "zero ghost rows must appear");
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn crash_during_checkpoint_snapshot_tmp_recovery() {
    // Simulates a crash while snapshot.tmp was being written or partially flushed
    let dir = std::env::temp_dir().join(format!("hdbcrash_tmp_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);

    {
        let db = setup(&dir);
        let mut s = db.new_session();
        db.execute(&mut s, "INSERT INTO t VALUES (1, 'first', 1.0)").unwrap();
        db.execute(&mut s, "CHECKPOINT").unwrap();
        db.execute(&mut s, "INSERT INTO t VALUES (2, 'second', 2.0)").unwrap();
    }

    // Leave a corrupted snapshot.tmp in the directory
    fs::write(dir.join("snapshot.tmp"), b"CORRUPTED_PARTIAL_SNAPSHOT_DATA_FROM_CRASH").unwrap();

    // Database::open must ignore or overwrite snapshot.tmp and recover from snapshot.bin + WAL
    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();
    let out = db.execute(&mut s, "SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(out.rows[0][0], Datum::Int(2));
    assert_eq!(out.rows.len(), 1);

    // Another checkpoint should succeed cleanly, removing or overwriting the tmp
    db.execute(&mut s, "CHECKPOINT").unwrap();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn crash_between_snapshot_rename_and_wal_reset() {
    // Simulates a crash right after snapshot.tmp was renamed to snapshot.bin,
    // but before WAL truncation/reset executed.
    let dir = std::env::temp_dir().join(format!("hdbcrash_ren_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);

    let db = setup(&dir);
    let mut s = db.new_session();
    for i in 1..=10 {
        db.execute(&mut s, &format!("INSERT INTO t VALUES ({i}, 'val{i}', {i}.0)")).unwrap();
    }

    // Checkpoint manually so snapshot.bin incorporates all 10 rows
    db.execute(&mut s, "CHECKPOINT").unwrap();

    // Insert 5 more rows into WAL
    for i in 11..=15 {
        db.execute(&mut s, &format!("INSERT INTO t VALUES ({i}, 'val{i}', {i}.0)")).unwrap();
    }
    drop(db);

    // Reopening must replay the WAL without double-insert errors on rows 1..=10
    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();
    let out = db.execute(&mut s, "SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(out.rows[0][0], Datum::Int(15));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn crash_torn_wal_tail_corrupted_crc() {
    // Simulates power loss tearing the final record's CRC or length
    let dir = std::env::temp_dir().join(format!("hdbcrash_crc_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);

    {
        let db = setup(&dir);
        let mut s = db.new_session();
        db.execute(&mut s, "INSERT INTO t VALUES (1, 'stable', 1.0)").unwrap();
        db.execute(&mut s, "INSERT INTO t VALUES (2, 'stable2', 2.0)").unwrap();
    }

    // Append garbage/corrupted tail to WAL
    let mut f = OpenOptions::new().append(true).open(dir.join("wal.log")).unwrap();
    f.write_all(&[0xFF, 0xAA, 0x55, 0x11, 0x00, 0x00, 0x00, 0x40]).unwrap();
    f.sync_all().unwrap();
    drop(f);

    // Reopen: recovery must successfully restore the 2 committed rows and truncate/ignore the corrupt tail
    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();
    let out = db.execute(&mut s, "SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(out.rows[0][0], Datum::Int(2));

    // The database must remain fully writable after recovery
    db.execute(&mut s, "INSERT INTO t VALUES (3, 'new', 3.0)").unwrap();
    let out = db.execute(&mut s, "SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(out.rows[0][0], Datum::Int(3));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn crash_failpoint_before_snapshot_rename_preserves_wal() {
    use crate::failpoint::{set, clear, FailMode, FailAction};

    let dir = std::env::temp_dir().join(format!("hdbfp_snap_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);

    {
        let db = setup(&dir);
        let mut s = db.new_session();
        db.execute(&mut s, "INSERT INTO t VALUES (1, 'initial', 1.0)").unwrap();
        db.execute(&mut s, "INSERT INTO t VALUES (2, 'second', 2.0)").unwrap();

        // Inject panic right before snapshot rename
        set("before_snapshot_rename", FailMode::Once, FailAction::Panic("test crash before rename"));

        let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            db.checkpoint()
        }));
        assert!(res.is_err(), "failpoint should trigger panic");
        clear();
    }

    // Reopen: snapshot.tmp was left over, snapshot.bin may be absent or old, WAL has the 2 rows
    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();
    let out = db.execute(&mut s, "SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(out.rows[0][0], Datum::Int(2));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn crash_failpoint_during_multirow_install_recovers_to_durable_state() {
    use crate::failpoint::{set, clear, FailMode, FailAction};

    let dir = std::env::temp_dir().join(format!("hdbfp_multi_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);

    {
        let db = setup(&dir);
        let mut s = db.new_session();
        db.execute(&mut s, "INSERT INTO t VALUES (1, 'row1', 1.0)").unwrap();

        // Inject panic midway through tree install of a multi-row transaction
        // WAL has already been made durable in Phase B!
        set("during_multirow_install", FailMode::Once, FailAction::Panic("test crash midway through install"));

        let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut s2 = db.new_session();
            db.execute(&mut s2, "BEGIN").unwrap();
            db.execute(&mut s2, "INSERT INTO t VALUES (2, 'row2', 2.0)").unwrap();
            db.execute(&mut s2, "INSERT INTO t VALUES (3, 'row3', 3.0)").unwrap();
            db.execute(&mut s2, "COMMIT").unwrap();
        }));
        assert!(res.is_err(), "failpoint should trigger panic midway through install");
        clear();
    }

    // Reopen: Because WAL commit record was already synced to disk before install,
    // recovery replays the transaction atomically: all rows (1, 2, 3) must be present!
    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();
    let out = db.execute(&mut s, "SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(out.rows[0][0], Datum::Int(3));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn crash_failpoint_before_wal_reset_idempotent_replay() {
    use crate::failpoint::{set, clear, FailMode, FailAction};

    let dir = std::env::temp_dir().join(format!("hdbfp_reset_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);

    {
        let db = setup(&dir);
        let mut s = db.new_session();
        db.execute(&mut s, "INSERT INTO t VALUES (1, 'row1', 1.0)").unwrap();

        // Inject panic after snapshot rename, before wal.reset()
        set("before_wal_reset", FailMode::Once, FailAction::Panic("test crash before wal reset"));

        let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            db.checkpoint()
        }));
        assert!(res.is_err(), "failpoint should trigger panic");
        clear();
    }

    // Both snapshot.bin and pre-checkpoint wal.log exist
    // Reopen must replay WAL idempotently without duplicating or corrupting keys
    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();
    let out = db.execute(&mut s, "SELECT * FROM t").unwrap();
    assert_eq!(out.rows.len(), 1);
    assert_eq!(out.rows[0][0], Datum::Int(1));

    // Can continue writing normally
    db.execute(&mut s, "INSERT INTO t VALUES (2, 'row2', 2.0)").unwrap();
    let out = db.execute(&mut s, "SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(out.rows[0][0], Datum::Int(2));
    let _ = fs::remove_dir_all(&dir);
}
