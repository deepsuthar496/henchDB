//! MVCC System-Level Property & Reference Differential Test Suite (Assessment §4).
//!
//! Maintains an independent reference oracle tracking key -> version history,
//! verifying RepeatableRead and ReadCommitted snapshot isolation, rollback atomicity,
//! and version chain stability across GC and checkpoints.

use std::collections::BTreeMap;
use std::fs;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::db::{Database, Datum};

/// Independent reference oracle for MVCC property verification (§4.1).
#[derive(Default)]
struct MvccReferenceOracle {
    /// key -> list of (epoch, value), where None signifies deletion.
    committed: BTreeMap<i64, Vec<(u64, Option<String>)>>,
    current_epoch: u64,
}

impl MvccReferenceOracle {
    fn commit_batch(&mut self, writes: &[(i64, Option<String>)]) -> u64 {
        self.current_epoch += 1;
        let e = self.current_epoch;
        for (k, val) in writes {
            self.committed
                .entry(*k)
                .or_default()
                .push((e, val.clone()));
        }
        e
    }

    fn commit_batch_at(&mut self, epoch: u64, writes: &[(i64, Option<String>)]) {
        self.current_epoch = epoch;
        for (k, val) in writes {
            self.committed
                .entry(*k)
                .or_default()
                .push((epoch, val.clone()));
        }
    }

    fn read_visible(&self, key: i64, visible_epoch: u64) -> Option<String> {
        let history = self.committed.get(&key)?;
        // Find latest version committed at or before visible_epoch
        for (e, val) in history.iter().rev() {
            if *e <= visible_epoch {
                return val.clone();
            }
        }
        None
    }
}

#[test]
fn mvcc_property_differential_reference_oracle() {
    let dir = std::env::temp_dir().join(format!("hdbmvcc_prop_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = Database::open(&dir).unwrap();
    let mut oracle = MvccReferenceOracle::default();

    let mut admin = db.new_session();
    db.execute(&mut admin, "CREATE TABLE accounts (id INT PRIMARY KEY, val TEXT)").unwrap();

    // 1. Initial population (Batch 1)
    let b1 = vec![
        (1, Some("v1".into())),
        (2, Some("v2".into())),
        (3, Some("v3".into())),
    ];
    let e1 = oracle.commit_batch(&b1);
    db.execute(&mut admin, "BEGIN").unwrap();
    db.execute(&mut admin, "INSERT INTO accounts VALUES (1, 'v1'), (2, 'v2'), (3, 'v3')").unwrap();
    db.execute(&mut admin, "COMMIT").unwrap();

    // Snapshot S1 pinned at epoch e1
    let mut s1 = db.new_session();
    db.execute(&mut s1, "BEGIN").unwrap();
    // Pin snapshot by reading
    let _ = db.execute(&mut s1, "SELECT * FROM accounts WHERE id = 1").unwrap();

    // 2. Updates and Deletions (Batch 2)
    let b2 = vec![
        (1, Some("v1_mod".into())),
        (2, None), // deleted
        (4, Some("v4_new".into())),
    ];
    let e2 = oracle.commit_batch(&b2);
    db.execute(&mut admin, "BEGIN").unwrap();
    db.execute(&mut admin, "UPDATE accounts SET val = 'v1_mod' WHERE id = 1").unwrap();
    db.execute(&mut admin, "DELETE FROM accounts WHERE id = 2").unwrap();
    db.execute(&mut admin, "INSERT INTO accounts VALUES (4, 'v4_new')").unwrap();
    db.execute(&mut admin, "COMMIT").unwrap();

    // 3. Rollback test (Batch 3 that must NOT modify oracle or live state)
    db.execute(&mut admin, "BEGIN").unwrap();
    db.execute(&mut admin, "UPDATE accounts SET val = 'dirty' WHERE id = 1").unwrap();
    db.execute(&mut admin, "DELETE FROM accounts WHERE id = 3").unwrap();
    db.execute(&mut admin, "ROLLBACK").unwrap();

    // 4. Verify Snapshot S1 sees historical state matching Oracle at e1
    for k in 1..=4 {
        let expected = oracle.read_visible(k, e1);
        let res = db.execute(&mut s1, &format!("SELECT val FROM accounts WHERE id = {k}")).unwrap();
        if let Some(exp_val) = expected {
            assert_eq!(res.rows.len(), 1, "Key {k} must exist in S1 snapshot");
            assert_eq!(res.rows[0][0], Datum::Text(exp_val));
        } else {
            assert_eq!(res.rows.len(), 0, "Key {k} must not exist in S1 snapshot");
        }
    }

    // 5. Verify Fresh Snapshot S2 sees latest state matching Oracle at e2
    let mut s2 = db.new_session();
    for k in 1..=4 {
        let expected = oracle.read_visible(k, e2);
        let res = db.execute(&mut s2, &format!("SELECT val FROM accounts WHERE id = {k}")).unwrap();
        if let Some(exp_val) = expected {
            assert_eq!(res.rows.len(), 1, "Key {k} must exist in S2 snapshot");
            assert_eq!(res.rows[0][0], Datum::Text(exp_val));
        } else {
            assert_eq!(res.rows.len(), 0, "Key {k} must not exist in S2 snapshot");
        }
    }

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn mvcc_property_long_lived_snapshot_across_gc_and_checkpoint() {
    let dir = std::env::temp_dir().join(format!("hdbmvcc_gc_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = Database::open(&dir).unwrap();

    let mut admin = db.new_session();
    db.execute(&mut admin, "CREATE TABLE items (id INT PRIMARY KEY, score INT)").unwrap();
    db.execute(&mut admin, "INSERT INTO items VALUES (1, 100), (2, 200)").unwrap();

    // Long-lived reader starts RepeatableRead transaction
    let mut reader = db.new_session();
    db.execute(&mut reader, "BEGIN").unwrap();
    let out0 = db.execute(&mut reader, "SELECT score FROM items WHERE id = 1").unwrap();
    assert_eq!(out0.rows[0][0], Datum::Int(100));

    // Writer performs multiple updates, vacuum passes, and checkpoint
    for i in 1..=50 {
        db.execute(&mut admin, &format!("UPDATE items SET score = {} WHERE id = 1", 100 + i)).unwrap();
        if i % 10 == 0 {
            db.gc_versions();
        }
    }

    // Trigger full checkpoint
    db.checkpoint().unwrap();

    // Invariant: Long-lived reader MUST STILL read original score 100 despite GC and checkpoint
    let out_hist = db.execute(&mut reader, "SELECT score FROM items WHERE id = 1").unwrap();
    assert_eq!(
        out_hist.rows[0][0],
        Datum::Int(100),
        "Long-lived snapshot isolation broken across GC and checkpoint"
    );

    // Commit reader
    db.execute(&mut reader, "COMMIT").unwrap();

    // Fresh read observes latest updated score 150
    let out_fresh = db.execute(&mut reader, "SELECT score FROM items WHERE id = 1").unwrap();
    assert_eq!(out_fresh.rows[0][0], Datum::Int(150));

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn mvcc_concurrent_randomized_differential_oracle_stress() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    let seed: u64 = std::env::var("HENCHDB_STRESS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0xCAFE_BABE_9999_1111);

    let dir = std::env::temp_dir().join(format!("hdbmvcc_conc_prop_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);

    let db = Arc::new(Database::open(&dir).unwrap());
    let oracle = Arc::new(std::sync::RwLock::new(MvccReferenceOracle::default()));
    let stop = Arc::new(AtomicBool::new(false));
    let read_ops = Arc::new(AtomicUsize::new(0));

    {
        let mut admin = db.new_session();
        db.execute(&mut admin, "CREATE TABLE kv (id INT PRIMARY KEY, val TEXT)").unwrap();
        // Seed initial items
        let initial_writes = vec![
            (1, Some("v1".into())),
            (2, Some("v2".into())),
            (3, Some("v3".into())),
            (4, Some("v4".into())),
        ];
        db.execute(&mut admin, "BEGIN").unwrap();
        db.execute(&mut admin, "INSERT INTO kv VALUES (1, 'v1'), (2, 'v2'), (3, 'v3'), (4, 'v4')").unwrap();
        db.execute(&mut admin, "COMMIT").unwrap();
        let init_epoch = db.visible_epoch.load(Ordering::SeqCst);
        oracle.write().unwrap().commit_batch_at(init_epoch, &initial_writes);
    }

    // Spawn 2 concurrent readers verifying snapshot consistency against oracle
    let mut readers = Vec::new();
    for r_idx in 0..2 {
        let db_c = db.clone();
        let oracle_c = oracle.clone();
        let stop_c = stop.clone();
        let read_ops_c = read_ops.clone();

        readers.push(std::thread::spawn(move || {
            let mut prng = crate::db::tests::ebr_stress::StressPrng::new(seed.wrapping_add(r_idx as u64 + 100));
            while !stop_c.load(Ordering::Relaxed) {
                let mut s = db_c.new_session();
                let use_consistent_snap = prng.next_u64() % 2 == 0;
                let snap_epoch = if use_consistent_snap {
                    let _o_snap = oracle_c.read().unwrap();
                    db_c.execute(&mut s, "START TRANSACTION WITH CONSISTENT SNAPSHOT").unwrap();
                    s.snapshot.as_ref().map(|p| p.read_epoch).unwrap_or(0)
                } else {
                    let _o_snap = oracle_c.read().unwrap();
                    db_c.execute(&mut s, "BEGIN").unwrap();
                    // First read pins snapshot epoch under RepeatableRead
                    let _ = db_c.execute(&mut s, "SELECT * FROM kv WHERE id = 1").unwrap();
                    s.snapshot.as_ref().map(|p| p.read_epoch).unwrap_or(0)
                };

                // Read random keys across the snapshot
                for _ in 0..5 {
                    let k = prng.gen_range(1, 8) as i64;
                    let oracle_val = oracle_c.read().unwrap().read_visible(k, snap_epoch);
                    let res = db_c.execute(&mut s, &format!("SELECT val FROM kv WHERE id = {k}")).unwrap();

                    if let Some(expected) = oracle_val {
                        assert_eq!(res.rows.len(), 1, "Key {k} must exist at snap epoch {snap_epoch}");
                        assert_eq!(
                            res.rows[0][0],
                            Datum::Text(expected.clone()),
                            "MVCC differential violation: key {k} read {:?} but expected {:?}",
                            res.rows[0][0],
                            expected
                        );
                    } else {
                        assert_eq!(res.rows.len(), 0, "Key {k} must not exist at snap epoch {snap_epoch}");
                    }
                    read_ops_c.fetch_add(1, Ordering::Relaxed);
                }

                db_c.execute(&mut s, "COMMIT").unwrap();
            }
        }));
    }

    // Main writer thread executing randomized commits and rollbacks
    let mut prng = crate::db::tests::ebr_stress::StressPrng::new(seed);
    let n_batches = std::env::var("HENCHDB_MVCC_STRESS_OPS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(50);
    for b in 0..n_batches {
        let mut admin = db.new_session();
        let should_rollback = (b % 5) == 0;
        let k = prng.gen_range(1, 8) as i64;
        let op = prng.next_u64() % 2;

        let check = db.execute(&mut admin, &format!("SELECT id FROM kv WHERE id = {k}")).unwrap();
        let exists = !check.rows.is_empty();

        let (sql, writes) = if !exists {
            let val = format!("val_{b}_{k}");
            (format!("INSERT INTO kv VALUES ({k}, '{val}')"), vec![(k, Some(val))])
        } else if op == 0 {
            let val = format!("val_upd_{b}_{k}");
            (format!("UPDATE kv SET val = '{val}' WHERE id = {k}"), vec![(k, Some(val))])
        } else {
            (format!("DELETE FROM kv WHERE id = {k}"), vec![(k, None)])
        };

        db.execute(&mut admin, "BEGIN").unwrap();
        db.execute(&mut admin, &sql).unwrap();

        if should_rollback {
            db.execute(&mut admin, "ROLLBACK").unwrap();
        } else {
            let mut o = oracle.write().unwrap();
            let _ = db.execute(&mut admin, "COMMIT").unwrap();
            let epoch = db.visible_epoch.load(Ordering::SeqCst);
            o.commit_batch_at(epoch, &writes);
        }

        if b % 15 == 0 {
            db.gc_versions();
        }
    }

    stop.store(true, Ordering::Relaxed);
    for r in readers {
        r.join().unwrap();
    }

    assert!(read_ops.load(Ordering::Relaxed) > 50);

    // Final checkpoint, reopen from disk, and verify post-restart recovery matches oracle
    let final_epoch = db.visible_epoch.load(Ordering::SeqCst);
    db.checkpoint().unwrap();
    drop(db);

    let recovered_db = Database::open(&dir).expect("reopen recovered database");
    let mut s = recovered_db.new_session();
    for k in 1..8 {
        let exp = oracle.read().unwrap().read_visible(k, final_epoch);
        let res = recovered_db.execute(&mut s, &format!("SELECT val FROM kv WHERE id = {k}")).unwrap();
        if let Some(v) = exp {
            assert_eq!(res.rows.len(), 1, "Key {k} must exist post-recovery");
            assert_eq!(res.rows[0][0], Datum::Text(v));
        } else {
            assert_eq!(res.rows.len(), 0, "Key {k} must not exist post-recovery");
        }
    }

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn mvcc_oracle_race_focused_stress_suite() {
    let dir = std::env::temp_dir().join(format!("hdbmvcc_race_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = Arc::new(Database::open(&dir).unwrap());
    let oracle = Arc::new(std::sync::RwLock::new(MvccReferenceOracle::default()));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let read_queries = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    {
        let mut admin = db.new_session();
        db.execute(&mut admin, "CREATE TABLE accounts (id INT PRIMARY KEY, val TEXT)").unwrap();
        let init = vec![
            (1, Some("a1".into())),
            (2, Some("a2".into())),
            (3, Some("a3".into())),
            (4, Some("a4".into())),
        ];
        db.execute(&mut admin, "BEGIN").unwrap();
        db.execute(&mut admin, "INSERT INTO accounts VALUES (1, 'a1'), (2, 'a2'), (3, 'a3'), (4, 'a4')").unwrap();
        db.execute(&mut admin, "COMMIT").unwrap();
        let e = db.visible_epoch.load(Ordering::SeqCst);
        oracle.write().unwrap().commit_batch_at(e, &init);
    }

    // Spawn 3 concurrent reader threads continually querying snapshots
    let mut readers = Vec::new();
    for r_idx in 0..3 {
        let db_c = db.clone();
        let oracle_c = oracle.clone();
        let stop_c = stop.clone();
        let read_q = read_queries.clone();

        readers.push(std::thread::spawn(move || {
            let mut prng = crate::db::tests::ebr_stress::StressPrng::new(9999 + r_idx as u64);
            while !stop_c.load(Ordering::Relaxed) {
                let mut s = db_c.new_session();
                let is_consistent = prng.next_u64() % 2 == 0;
                let snap_epoch = {
                    let _o_lock = oracle_c.read().unwrap();
                    if is_consistent {
                        db_c.execute(&mut s, "START TRANSACTION WITH CONSISTENT SNAPSHOT").unwrap();
                    } else {
                        db_c.execute(&mut s, "BEGIN").unwrap();
                        let _ = db_c.execute(&mut s, "SELECT * FROM accounts WHERE id = 1").unwrap();
                    }
                    s.snapshot.as_ref().map(|p| p.read_epoch).unwrap_or(0)
                };

                for _ in 0..4 {
                    let k = prng.gen_range(1, 8) as i64;
                    let exp = oracle_c.read().unwrap().read_visible(k, snap_epoch);
                    let res = db_c.execute(&mut s, &format!("SELECT val FROM accounts WHERE id = {k}")).unwrap();
                    if let Some(expected_val) = exp {
                        assert_eq!(res.rows.len(), 1, "Key {k} must exist at snap epoch {snap_epoch}");
                        assert_eq!(res.rows[0][0], Datum::Text(expected_val));
                    } else {
                        assert_eq!(res.rows.len(), 0, "Key {k} must not exist at snap epoch {snap_epoch}");
                    }
                    read_q.fetch_add(1, Ordering::Relaxed);
                }
                db_c.execute(&mut s, "COMMIT").unwrap();
            }
        }));
    }

    // Spawn background maintenance thread periodically triggering GC and fuzzy checkpoints
    let db_maint = db.clone();
    let stop_maint = stop.clone();
    let maint_handle = std::thread::spawn(move || {
        while !stop_maint.load(Ordering::Relaxed) {
            std::thread::sleep(std::time::Duration::from_millis(10));
            db_maint.gc_versions();
            let _ = db_maint.checkpoint();
        }
    });

    // Writer executes randomized multi-row updates, inserts, deletes, and rollbacks
    let mut prng = crate::db::tests::ebr_stress::StressPrng::new(55555);
    let n_ops = std::env::var("HENCHDB_MVCC_STRESS_OPS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(60);
    for b in 0..n_ops {
        let mut admin = db.new_session();
        let should_rollback = (b % 4) == 0;
        let k = prng.gen_range(1, 8) as i64;

        let check = db.execute(&mut admin, &format!("SELECT id FROM accounts WHERE id = {k}")).unwrap();
        let exists = !check.rows.is_empty();

        let (sql, writes) = if !exists {
            let val = format!("acc_{b}_{k}");
            (format!("INSERT INTO accounts VALUES ({k}, '{val}')"), vec![(k, Some(val))])
        } else if b % 2 == 0 {
            let val = format!("acc_mod_{b}_{k}");
            (format!("UPDATE accounts SET val = '{val}' WHERE id = {k}"), vec![(k, Some(val))])
        } else {
            (format!("DELETE FROM accounts WHERE id = {k}"), vec![(k, None)])
        };

        db.execute(&mut admin, "BEGIN").unwrap();
        db.execute(&mut admin, &sql).unwrap();

        if should_rollback {
            db.execute(&mut admin, "ROLLBACK").unwrap();
        } else {
            let mut o = oracle.write().unwrap();
            db.execute(&mut admin, "COMMIT").unwrap();
            let epoch = db.visible_epoch.load(Ordering::SeqCst);
            o.commit_batch_at(epoch, &writes);
        }
    }

    stop.store(true, Ordering::Relaxed);
    for r in readers {
        r.join().unwrap();
    }
    maint_handle.join().unwrap();

    assert!(read_queries.load(Ordering::Relaxed) > 50);

    // Final checkpoint, drop, and restart recovery
    let final_epoch = db.visible_epoch.load(Ordering::SeqCst);
    db.checkpoint().unwrap();
    drop(db);

    let recovered = Database::open(&dir).expect("reopen database after race stress");
    let mut rec_s = recovered.new_session();
    let check_reports = recovered.check_database(&rec_s).unwrap();
    for rep in &check_reports {
        assert!(rep.is_ok(), "CHECK DATABASE failed post-restart: {:?}", rep.error_msg);
    }

    // Verify all keys match oracle state
    for k in 1..8 {
        let exp = oracle.read().unwrap().read_visible(k, final_epoch);
        let res = recovered.execute(&mut rec_s, &format!("SELECT val FROM accounts WHERE id = {k}")).unwrap();
        if let Some(expected_val) = exp {
            assert_eq!(res.rows.len(), 1, "Key {k} must exist post-recovery");
            assert_eq!(res.rows[0][0], Datum::Text(expected_val));
        } else {
            assert_eq!(res.rows.len(), 0, "Key {k} must not exist post-recovery");
        }
    }

    let _ = fs::remove_dir_all(&dir);
}

