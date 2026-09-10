//! MVCC System-Level Property & Reference Differential Test Suite (Assessment §4).
//!
//! Maintains an independent reference oracle tracking key -> version history,
//! verifying RepeatableRead and ReadCommitted snapshot isolation, rollback atomicity,
//! and version chain stability across GC and checkpoints.

use std::collections::BTreeMap;
use std::fs;

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
