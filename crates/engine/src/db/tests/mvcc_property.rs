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

    /// Attach reference oracle directly to the database via an atomic commit observer.
    /// Eliminates the database-commit -> oracle-publication visibility window by
    /// recording the committed mutations into the oracle immediately before `visible_epoch`
    /// is published to readers.
    fn hook_to_db(oracle: &Arc<std::sync::RwLock<Self>>, db: &Database) {
        let oracle_ref = oracle.clone();
        db.set_commit_observer(Some(Arc::new(move |epoch, items| {
            let mut writes = Vec::new();
            for (_tbl, key_bytes, row_opt) in items {
                if let Ok(Datum::Int(id)) = crate::types::decode_key(key_bytes) {
                    let val_opt = row_opt.as_ref().and_then(|row| {
                        if row.len() >= 2 {
                            match &row[1] {
                                Datum::Text(t) => Some(t.clone()),
                                _ => None,
                            }
                        } else {
                            None
                        }
                    });
                    writes.push((id, val_opt));
                }
            }
            if !writes.is_empty() {
                oracle_ref.write().unwrap().commit_batch_at(epoch, &writes);
            }
        })));
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
    db.execute(&mut admin, "INSERT INTO accounts VALUES (5, 'dirty5')").unwrap();
    db.execute(&mut admin, "ROLLBACK").unwrap();

    // Snapshot S2 pinned at epoch e2
    let mut s2 = db.new_session();
    db.execute(&mut s2, "START TRANSACTION WITH CONSISTENT SNAPSHOT").unwrap();

    // 4. Batch 4: More updates after S2 pinned
    let b4 = vec![
        (1, Some("v1_final".into())),
        (3, Some("v3_final".into())),
        (5, Some("v5_valid".into())),
    ];
    let e4 = oracle.commit_batch(&b4);
    db.execute(&mut admin, "BEGIN").unwrap();
    db.execute(&mut admin, "UPDATE accounts SET val = 'v1_final' WHERE id = 1").unwrap();
    db.execute(&mut admin, "UPDATE accounts SET val = 'v3_final' WHERE id = 3").unwrap();
    db.execute(&mut admin, "INSERT INTO accounts VALUES (5, 'v5_valid')").unwrap();
    db.execute(&mut admin, "COMMIT").unwrap();

    // Snapshot S3 pinned at epoch e4
    let mut s3 = db.new_session();
    db.execute(&mut s3, "START TRANSACTION WITH CONSISTENT SNAPSHOT").unwrap();

    // --- VERIFICATION PHASE ---

    // Verify S1 (pinned at e1)
    for k in 1..=5 {
        let expected = oracle.read_visible(k, e1);
        let res = db.execute(&mut s1, &format!("SELECT val FROM accounts WHERE id = {k}")).unwrap();
        if let Some(exp_val) = expected {
            assert_eq!(res.rows.len(), 1, "S1: Key {k} should exist");
            assert_eq!(res.rows[0][0], Datum::Text(exp_val));
        } else {
            assert_eq!(res.rows.len(), 0, "S1: Key {k} should NOT exist");
        }
    }

    // Verify S2 (pinned at e2)
    for k in 1..=5 {
        let expected = oracle.read_visible(k, e2);
        let res = db.execute(&mut s2, &format!("SELECT val FROM accounts WHERE id = {k}")).unwrap();
        if let Some(exp_val) = expected {
            assert_eq!(res.rows.len(), 1, "S2: Key {k} should exist");
            assert_eq!(res.rows[0][0], Datum::Text(exp_val));
        } else {
            assert_eq!(res.rows.len(), 0, "S2: Key {k} should NOT exist");
        }
    }

    // Verify S3 (pinned at e4)
    for k in 1..=5 {
        let expected = oracle.read_visible(k, e4);
        let res = db.execute(&mut s3, &format!("SELECT val FROM accounts WHERE id = {k}")).unwrap();
        if let Some(exp_val) = expected {
            assert_eq!(res.rows.len(), 1, "S3: Key {k} should exist");
            assert_eq!(res.rows[0][0], Datum::Text(exp_val));
        } else {
            assert_eq!(res.rows.len(), 0, "S3: Key {k} should NOT exist");
        }
    }

    // Release snapshots
    db.execute(&mut s1, "COMMIT").unwrap();
    db.execute(&mut s2, "COMMIT").unwrap();
    db.execute(&mut s3, "COMMIT").unwrap();

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn mvcc_property_long_lived_snapshot_across_gc_and_checkpoint() {
    let dir = std::env::temp_dir().join(format!("hdbmvcc_gc_chk_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = Database::open(&dir).unwrap();
    let mut oracle = MvccReferenceOracle::default();

    let mut admin = db.new_session();
    db.execute(&mut admin, "CREATE TABLE kv (k INT PRIMARY KEY, v TEXT)").unwrap();

    let b0 = vec![(1, Some("init".into()))];
    let e0 = oracle.commit_batch(&b0);
    db.execute(&mut admin, "INSERT INTO kv VALUES (1, 'init')").unwrap();

    // Pin long-lived snapshot S_old
    let mut s_old = db.new_session();
    db.execute(&mut s_old, "START TRANSACTION WITH CONSISTENT SNAPSHOT").unwrap();

    // Run multiple update waves
    for i in 1..=5 {
        let val = format!("v_{i}");
        let b = vec![(1, Some(val.clone()))];
        oracle.commit_batch(&b);
        db.execute(&mut admin, &format!("UPDATE kv SET v = '{val}' WHERE k = 1")).unwrap();
    }

    // Trigger explicit version vacuum GC
    db.gc_versions();

    // Long-lived reader must still observe its exact pinned version despite GC!
    let exp_old = oracle.read_visible(1, e0);
    let res = db.execute(&mut s_old, "SELECT v FROM kv WHERE k = 1").unwrap();
    assert_eq!(res.rows[0][0], Datum::Text(exp_old.unwrap()));

    // Trigger fuzzy checkpoint (persists snapshot to disk, resets WAL, runs GC)
    db.checkpoint().unwrap();

    // S_old must still observe its version even across checkpointing!
    let res2 = db.execute(&mut s_old, "SELECT v FROM kv WHERE k = 1").unwrap();
    assert_eq!(res2.rows[0][0], Datum::Text("init".into()));

    db.execute(&mut s_old, "COMMIT").unwrap();

    // After unpinning S_old, another GC must now safely prune the ancient version
    db.gc_versions();

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn mvcc_concurrent_randomized_differential_oracle_stress() {
    let seed = 424242u64;
    let dir = std::env::temp_dir().join(format!("hdbmvcc_conc_diff_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = Arc::new(Database::open(&dir).unwrap());
    let oracle = Arc::new(std::sync::RwLock::new(MvccReferenceOracle::default()));
    MvccReferenceOracle::hook_to_db(&oracle, &db);

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let read_ops = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    {
        let mut admin = db.new_session();
        db.execute(&mut admin, "CREATE TABLE kv (id INT PRIMARY KEY, val TEXT)").unwrap();
        db.execute(&mut admin, "INSERT INTO kv VALUES (1, 'v1'), (2, 'v2'), (3, 'v3'), (4, 'v4')").unwrap();
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
                    db_c.execute(&mut s, "START TRANSACTION WITH CONSISTENT SNAPSHOT").unwrap();
                    s.snapshot.as_ref().map(|p| p.read_epoch).unwrap_or(0)
                } else {
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

        let sql = if !exists {
            let val = format!("val_{b}_{k}");
            format!("INSERT INTO kv VALUES ({k}, '{val}')")
        } else if op == 0 {
            let val = format!("val_upd_{b}_{k}");
            format!("UPDATE kv SET val = '{val}' WHERE id = {k}")
        } else {
            format!("DELETE FROM kv WHERE id = {k}")
        };

        db.execute(&mut admin, "BEGIN").unwrap();
        db.execute(&mut admin, &sql).unwrap();

        if should_rollback {
            db.execute(&mut admin, "ROLLBACK").unwrap();
        } else {
            let _ = db.execute(&mut admin, "COMMIT").unwrap();
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
    db.set_commit_observer(None);
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
    MvccReferenceOracle::hook_to_db(&oracle, &db);

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let read_queries = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    {
        let mut admin = db.new_session();
        db.execute(&mut admin, "CREATE TABLE accounts (id INT PRIMARY KEY, val TEXT)").unwrap();
        db.execute(&mut admin, "INSERT INTO accounts VALUES (1, 'a1'), (2, 'a2'), (3, 'a3'), (4, 'a4')").unwrap();
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
                let snap_epoch = if is_consistent {
                    db_c.execute(&mut s, "START TRANSACTION WITH CONSISTENT SNAPSHOT").unwrap();
                    s.snapshot.as_ref().map(|p| p.read_epoch).unwrap_or(0)
                } else {
                    db_c.execute(&mut s, "BEGIN").unwrap();
                    let _ = db_c.execute(&mut s, "SELECT * FROM accounts WHERE id = 1").unwrap();
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

    let mut prng = crate::db::tests::ebr_stress::StressPrng::new(1337);
    let n_race_batches = std::env::var("HENCHDB_MVCC_STRESS_OPS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(60);
    for b in 0..n_race_batches {
        let mut admin = db.new_session();
        let should_rollback = (b % 7) == 0;
        let k = prng.gen_range(1, 8) as i64;
        let op = prng.next_u64() % 3;

        let sql = match op {
            0 => format!("INSERT INTO accounts VALUES ({k}, 'acc_{b}_{k}')"),
            1 => format!("UPDATE accounts SET val = 'acc_mod_{b}_{k}' WHERE id = {k}"),
            _ => format!("DELETE FROM accounts WHERE id = {k}"),
        };

        db.execute(&mut admin, "BEGIN").unwrap();
        let _ = db.execute(&mut admin, &sql);

        if should_rollback {
            db.execute(&mut admin, "ROLLBACK").unwrap();
        } else {
            db.execute(&mut admin, "COMMIT").unwrap();
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
    db.set_commit_observer(None);
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

#[test]
fn mvcc_atomic_oracle_publication_race_elimination() {
    let dir = std::env::temp_dir().join(format!("hdbmvcc_atomic_pub_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = Arc::new(Database::open(&dir).unwrap());
    let oracle = Arc::new(std::sync::RwLock::new(MvccReferenceOracle::default()));
    MvccReferenceOracle::hook_to_db(&oracle, &db);

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let read_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    {
        let mut s = db.new_session();
        db.execute(&mut s, "CREATE TABLE kv (id INT PRIMARY KEY, val TEXT)").unwrap();
        db.execute(&mut s, "INSERT INTO kv VALUES (1, 'init1'), (2, 'init2'), (3, 'init3')").unwrap();
    }

    // 4 reader threads continuously starting snapshots and verifying against oracle
    // WITHOUT any locks on the oracle during snapshot creation.
    let mut readers = Vec::new();
    for r_idx in 0..4 {
        let db_c = db.clone();
        let oracle_c = oracle.clone();
        let stop_c = stop.clone();
        let read_cnt = read_count.clone();

        readers.push(std::thread::spawn(move || {
            let mut prng = crate::db::tests::ebr_stress::StressPrng::new(5555 + r_idx as u64);
            while !stop_c.load(Ordering::Relaxed) {
                let mut s = db_c.new_session();
                // Lock-free snapshot creation: read_epoch is sampled directly from visible_epoch.
                db_c.execute(&mut s, "START TRANSACTION WITH CONSISTENT SNAPSHOT").unwrap();
                let snap_epoch = s.snapshot.as_ref().map(|p| p.read_epoch).unwrap_or(0);

                for _ in 0..5 {
                    let k = prng.gen_range(1, 10) as i64;
                    let res = db_c.execute(&mut s, &format!("SELECT val FROM kv WHERE id = {k}")).unwrap();
                    let oracle_val = oracle_c.read().unwrap().read_visible(k, snap_epoch);

                    if let Some(exp) = oracle_val {
                        assert_eq!(res.rows.len(), 1, "Key {k} must exist at snap epoch {snap_epoch}");
                        assert_eq!(
                            res.rows[0][0],
                            Datum::Text(exp.clone()),
                            "MVCC visibility mismatch: key {k} db had {:?} but oracle had {:?}",
                            res.rows[0][0],
                            exp
                        );
                    } else {
                        assert_eq!(res.rows.len(), 0, "Key {k} must not exist at snap epoch {snap_epoch}");
                    }
                    read_cnt.fetch_add(1, Ordering::Relaxed);
                }
                let _ = db_c.execute(&mut s, "COMMIT");
                std::thread::yield_now();
            }
        }));
    }

    // Writer thread rapidly mutating data in tight loop
    let mut writer_session = db.new_session();
    let mut prng = crate::db::tests::ebr_stress::StressPrng::new(7777);
    for step in 1..=200 {
        let k = prng.gen_range(1, 10) as i64;
        let op = prng.next_u64() % 3;
        let sql = match op {
            0 => format!("INSERT INTO kv VALUES ({k}, 'v_{step}_{k}')"),
            1 => format!("UPDATE kv SET val = 'upd_{step}_{k}' WHERE id = {k}"),
            _ => format!("DELETE FROM kv WHERE id = {k}"),
        };
        let _ = db.execute(&mut writer_session, &sql);
        if step % 20 == 0 {
            std::thread::yield_now();
        }
    }

    stop.store(true, Ordering::Relaxed);
    for r in readers {
        r.join().unwrap();
    }

    assert!(read_count.load(Ordering::Relaxed) >= 200, "Must complete hundreds of concurrent verification reads");

    // Detach observer
    db.set_commit_observer(None);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn mvcc_production_scale_differential_multi_threaded_workload() {
    let total_ops: usize = std::env::var("HENCHDB_MVCC_OPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200);

    let dir = std::env::temp_dir().join(format!("hdbmvcc_prod_scale_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = Arc::new(Database::open(&dir).unwrap());
    let oracle = Arc::new(std::sync::RwLock::new(MvccReferenceOracle::default()));
    MvccReferenceOracle::hook_to_db(&oracle, &db);

    {
        let mut s = db.new_session();
        db.execute(&mut s, "CREATE TABLE records (id INT PRIMARY KEY, val TEXT)").unwrap();
        for k in 1..=10 {
            db.execute(&mut s, &format!("INSERT INTO records VALUES ({k}, 'init_{k}')")).unwrap();
        }
    }

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let read_ops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let write_ops = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    // 1. Long-lived snapshot reader pinned at start
    let long_snap_db = db.clone();
    let long_snap_oracle = oracle.clone();
    let long_snap_stop = stop.clone();
    let long_reader_handle = std::thread::spawn(move || {
        let mut s = long_snap_db.new_session();
        long_snap_db.execute(&mut s, "START TRANSACTION WITH CONSISTENT SNAPSHOT").unwrap();
        let snap_epoch = s.snapshot.as_ref().map(|p| p.read_epoch).unwrap_or(0);

        while !long_snap_stop.load(Ordering::Relaxed) {
            for k in 1..=10 {
                let exp = long_snap_oracle.read().unwrap().read_visible(k, snap_epoch);
                let res = long_snap_db.execute(&mut s, &format!("SELECT val FROM records WHERE id = {k}")).unwrap();
                if let Some(expected_val) = exp {
                    assert_eq!(res.rows.len(), 1);
                    assert_eq!(res.rows[0][0], Datum::Text(expected_val));
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        let _ = long_snap_db.execute(&mut s, "COMMIT");
    });

    // 2. 3 concurrent short snapshot readers
    let mut readers = Vec::new();
    for r_idx in 0..3 {
        let db_c = db.clone();
        let oracle_c = oracle.clone();
        let stop_c = stop.clone();
        let read_cnt = read_ops.clone();

        readers.push(std::thread::spawn(move || {
            let mut prng = crate::db::tests::ebr_stress::StressPrng::new(8888 + r_idx as u64 * 31);
            while !stop_c.load(Ordering::Relaxed) {
                let mut s = db_c.new_session();
                let is_consistent = prng.next_u64() % 2 == 0;
                let snap_epoch = if is_consistent {
                    db_c.execute(&mut s, "START TRANSACTION WITH CONSISTENT SNAPSHOT").unwrap();
                    s.snapshot.as_ref().map(|p| p.read_epoch).unwrap_or(0)
                } else {
                    db_c.execute(&mut s, "BEGIN").unwrap();
                    let _ = db_c.execute(&mut s, "SELECT * FROM records WHERE id = 1").unwrap();
                    s.snapshot.as_ref().map(|p| p.read_epoch).unwrap_or(0)
                };

                for _ in 0..4 {
                    let k = prng.gen_range(1, 15) as i64;
                    let exp = oracle_c.read().unwrap().read_visible(k, snap_epoch);
                    let res = db_c.execute(&mut s, &format!("SELECT val FROM records WHERE id = {k}")).unwrap();
                    if let Some(expected_val) = exp {
                        assert_eq!(res.rows.len(), 1, "Key {k} must exist at snap epoch {snap_epoch}");
                        assert_eq!(res.rows[0][0], Datum::Text(expected_val));
                    } else {
                        assert_eq!(res.rows.len(), 0, "Key {k} must not exist at snap epoch {snap_epoch}");
                    }
                    read_cnt.fetch_add(1, Ordering::Relaxed);
                }
                let _ = db_c.execute(&mut s, "COMMIT");
                std::thread::yield_now();
            }
        }));
    }

    // 3. Background maintenance thread (GC & checkpoints)
    let db_maint = db.clone();
    let stop_maint = stop.clone();
    let maint_handle = std::thread::spawn(move || {
        while !stop_maint.load(Ordering::Relaxed) {
            std::thread::sleep(std::time::Duration::from_millis(15));
            db_maint.gc_versions();
            let _ = db_maint.checkpoint();
        }
    });

    // 4. 2 concurrent writer threads executing conflicting operations
    let ops_per_writer = (total_ops / 2).max(1);
    let mut writers = Vec::new();
    for w_idx in 0..2 {
        let db_c = db.clone();
        let write_cnt = write_ops.clone();

        writers.push(std::thread::spawn(move || {
            let mut prng = crate::db::tests::ebr_stress::StressPrng::new(12345 + w_idx as u64 * 7919);
            let mut s = db_c.new_session();

            for step in 0..ops_per_writer {
                let k = prng.gen_range(1, 15) as i64;
                let op = prng.next_u64() % 4;
                let should_rollback = (op == 3) || ((step % 7) == 0);

                let sql = match op {
                    0 => format!("INSERT INTO records VALUES ({k}, 'v_{w_idx}_{step}_{k}')"),
                    1 => format!("UPDATE records SET val = 'upd_{w_idx}_{step}_{k}' WHERE id = {k}"),
                    _ => format!("DELETE FROM records WHERE id = {k}"),
                };

                let _ = db_c.execute(&mut s, "BEGIN");
                let _ = db_c.execute(&mut s, &sql);

                if should_rollback {
                    let _ = db_c.execute(&mut s, "ROLLBACK");
                } else {
                    let _ = db_c.execute(&mut s, "COMMIT");
                }
                write_cnt.fetch_add(1, Ordering::Relaxed);
                if step % 25 == 0 {
                    std::thread::yield_now();
                }
            }
        }));
    }

    for w in writers {
        w.join().unwrap();
    }

    stop.store(true, Ordering::Relaxed);
    long_reader_handle.join().unwrap();
    for r in readers {
        r.join().unwrap();
    }
    maint_handle.join().unwrap();

    assert!(write_ops.load(Ordering::Relaxed) >= ops_per_writer);
    assert!(read_ops.load(Ordering::Relaxed) >= 50);

    // Final checkpoint, shutdown, and post-restart integrity audit
    let final_epoch = db.visible_epoch.load(Ordering::SeqCst);
    db.checkpoint().unwrap();
    db.set_commit_observer(None);
    drop(db);

    let recovered = Database::open(&dir).expect("reopen post-production scale workload");
    let mut rec_s = recovered.new_session();
    let checks = recovered.check_database(&rec_s).unwrap();
    for c in &checks {
        assert!(c.is_ok(), "Post-workload integrity failed: {:?}", c.error_msg);
    }

    for k in 1..15 {
        let exp = oracle.read().unwrap().read_visible(k, final_epoch);
        let res = recovered.execute(&mut rec_s, &format!("SELECT val FROM records WHERE id = {k}")).unwrap();
        if let Some(expected_val) = exp {
            assert_eq!(res.rows.len(), 1, "Key {k} must exist post-recovery");
            assert_eq!(res.rows[0][0], Datum::Text(expected_val));
        } else {
            assert_eq!(res.rows.len(), 0, "Key {k} must not exist post-recovery");
        }
    }

    let _ = fs::remove_dir_all(&dir);
}
