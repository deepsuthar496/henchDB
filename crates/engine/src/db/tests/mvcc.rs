use std::fs;
use std::path::Path;

use super::*;
use crate::sql::IsolationLevel;
use crate::types::Datum;

fn mvcc_test_setup(dir: &Path) -> Database {
    let db = Database::open(dir).unwrap();
    let mut s = db.new_session();
    db.execute(&mut s, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT, num INT)")
        .unwrap();
    db.execute(&mut s, "CREATE INDEX idx_num ON t(num)").unwrap();
    db.execute(&mut s, "INSERT INTO t VALUES (1, 'one', 10), (2, 'two', 20), (3, 'three', 30)")
        .unwrap();
    db
}

fn query_val(db: &Database, s: &mut Session, id: i64) -> Option<String> {
    let out = db
        .execute(s, &format!("SELECT v FROM t WHERE id = {id}"))
        .unwrap();
    out.rows.first().map(|r| match &r[0] {
        Datum::Text(v) => v.clone(),
        other => panic!("unexpected {other:?}"),
    })
}

#[test]
fn mvcc_default_begin_repeatable_read() {
    let dir = std::env::temp_dir().join(format!("hdb_mrr_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = mvcc_test_setup(&dir);
    let mut reader = db.new_session();
    let mut writer = db.new_session();

    // Standard BEGIN (default RepeatableRead) pins snapshot upon first read.
    db.execute(&mut reader, "BEGIN").unwrap();
    assert_eq!(query_val(&db, &mut reader, 1).as_deref(), Some("one"));

    // Writer updates row and inserts new row concurrently.
    db.execute(&mut writer, "UPDATE t SET v = 'updated_one' WHERE id = 1").unwrap();
    db.execute(&mut writer, "INSERT INTO t VALUES (4, 'four', 40)").unwrap();

    // Reader MUST still observe its original snapshot.
    assert_eq!(query_val(&db, &mut reader, 1).as_deref(), Some("one"));
    assert_eq!(query_val(&db, &mut reader, 4), None);

    let cnt = db.execute(&mut reader, "SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(cnt.rows[0][0], Datum::Int(3));

    // Fresh autocommit session sees new state.
    assert_eq!(query_val(&db, &mut writer, 1).as_deref(), Some("updated_one"));
    assert_eq!(query_val(&db, &mut writer, 4).as_deref(), Some("four"));

    // Ending the transaction releases the snapshot.
    db.execute(&mut reader, "COMMIT").unwrap();
    assert_eq!(query_val(&db, &mut reader, 1).as_deref(), Some("updated_one"));
    assert_eq!(query_val(&db, &mut reader, 4).as_deref(), Some("four"));

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn mvcc_atomic_multi_row_commit_visibility() {
    let dir = std::env::temp_dir().join(format!("hdb_mam_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = mvcc_test_setup(&dir);
    let mut reader = db.new_session();
    let mut writer = db.new_session();

    // Reader pins snapshot
    db.execute(&mut reader, "START TRANSACTION WITH CONSISTENT SNAPSHOT").unwrap();
    assert_eq!(query_val(&db, &mut reader, 1).as_deref(), Some("one"));

    // Multi-row transaction updates rows 1, 2, and 3 simultaneously
    db.execute(&mut writer, "BEGIN").unwrap();
    db.execute(&mut writer, "UPDATE t SET v = 'a1' WHERE id = 1").unwrap();
    db.execute(&mut writer, "UPDATE t SET v = 'b2' WHERE id = 2").unwrap();
    db.execute(&mut writer, "UPDATE t SET v = 'c3' WHERE id = 3").unwrap();
    db.execute(&mut writer, "COMMIT").unwrap();

    // Reader must observe all previous versions atomically, never partial
    assert_eq!(query_val(&db, &mut reader, 1).as_deref(), Some("one"));
    assert_eq!(query_val(&db, &mut reader, 2).as_deref(), Some("two"));
    assert_eq!(query_val(&db, &mut reader, 3).as_deref(), Some("three"));

    db.execute(&mut reader, "COMMIT").unwrap();
    // After commit, new state is visible
    assert_eq!(query_val(&db, &mut reader, 1).as_deref(), Some("a1"));
    assert_eq!(query_val(&db, &mut reader, 2).as_deref(), Some("b2"));
    assert_eq!(query_val(&db, &mut reader, 3).as_deref(), Some("c3"));

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn mvcc_read_committed_isolation_mode() {
    let dir = std::env::temp_dir().join(format!("hdb_mrc_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = mvcc_test_setup(&dir);
    let mut s1 = db.new_session();
    let mut s2 = db.new_session();

    // Set ReadCommitted
    db.execute(&mut s1, "SET TRANSACTION ISOLATION LEVEL READ COMMITTED").unwrap();
    assert_eq!(s1.isolation_level, IsolationLevel::ReadCommitted);

    db.execute(&mut s1, "BEGIN").unwrap();
    assert_eq!(query_val(&db, &mut s1, 1).as_deref(), Some("one"));

    // Writer modifies row 1 and commits
    db.execute(&mut s2, "UPDATE t SET v = 'fresh_one' WHERE id = 1").unwrap();

    // In ReadCommitted, each statement takes a fresh statement snapshot,
    // so s1 sees the newly committed value within the same transaction!
    assert_eq!(query_val(&db, &mut s1, 1).as_deref(), Some("fresh_one"));

    db.execute(&mut s1, "COMMIT").unwrap();

    // Now switch back to RepeatableRead
    db.execute(&mut s1, "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ").unwrap();
    assert_eq!(s1.isolation_level, IsolationLevel::RepeatableRead);

    db.execute(&mut s1, "BEGIN").unwrap();
    assert_eq!(query_val(&db, &mut s1, 1).as_deref(), Some("fresh_one"));

    // Writer modifies row 1 again
    db.execute(&mut s2, "UPDATE t SET v = 'newer_one' WHERE id = 1").unwrap();

    // In RepeatableRead, s1 MUST NOT see the newer value!
    assert_eq!(query_val(&db, &mut s1, 1).as_deref(), Some("fresh_one"));

    db.execute(&mut s1, "COMMIT").unwrap();
    assert_eq!(query_val(&db, &mut s1, 1).as_deref(), Some("newer_one"));

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn mvcc_secondary_index_and_point_time_travel() {
    let dir = std::env::temp_dir().join(format!("hdb_msi_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = mvcc_test_setup(&dir);
    let mut reader = db.new_session();
    let mut writer = db.new_session();

    // Reader pins snapshot
    db.execute(&mut reader, "START TRANSACTION WITH CONSISTENT SNAPSHOT").unwrap();

    // Writer updates both primary and secondary column, and deletes a row
    db.execute(&mut writer, "UPDATE t SET v = 'one_mod', num = 99 WHERE id = 1").unwrap();
    db.execute(&mut writer, "DELETE FROM t WHERE id = 2").unwrap();

    // 1. Secondary index seek for num = 10 (which was id=1 before update)
    let out = db.execute(&mut reader, "SELECT v FROM t WHERE num = 10").unwrap();
    assert_eq!(out.rows.len(), 1);
    assert_eq!(out.rows[0][0], Datum::Text("one".into()));

    // 2. Secondary index seek for num = 20 (which was deleted)
    let out = db.execute(&mut reader, "SELECT v FROM t WHERE num = 20").unwrap();
    assert_eq!(out.rows.len(), 1);
    assert_eq!(out.rows[0][0], Datum::Text("two".into()));

    // 3. Point lookup on deleted row
    assert_eq!(query_val(&db, &mut reader, 2).as_deref(), Some("two"));

    // Writer confirms changes are active in current state
    assert_eq!(query_val(&db, &mut writer, 2), None);
    let out = db.execute(&mut writer, "SELECT v FROM t WHERE num = 99").unwrap();
    assert_eq!(out.rows.len(), 1);
    assert_eq!(out.rows[0][0], Datum::Text("one_mod".into()));

    db.execute(&mut reader, "ROLLBACK").unwrap();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn mvcc_vectorized_batch_aggregate_snapshot_isolation() {
    let dir = std::env::temp_dir().join(format!("hdb_mba_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();
    db.execute(&mut s, "CREATE TABLE items (id INT PRIMARY KEY, qty INT, price FLOAT)").unwrap();

    // Insert 100 items
    for i in 1..=100 {
        db.execute(&mut s, &format!("INSERT INTO items VALUES ({i}, 10, 2.5)")).unwrap();
    }

    let mut reader = db.new_session();
    let mut writer = db.new_session();

    // Reader pins consistent snapshot
    db.execute(&mut reader, "START TRANSACTION WITH CONSISTENT SNAPSHOT").unwrap();

    // Verify baseline aggregate via ColumnBatch vectorized pushdown
    let out = db.execute(&mut reader, "SELECT COUNT(*), SUM(qty) FROM items").unwrap();
    assert_eq!(out.rows[0][0], Datum::Int(100));
    assert_eq!(out.rows[0][1], Datum::Int(1000));

    // Writer modifies, deletes, and adds items concurrently
    for i in 1..=20 {
        db.execute(&mut writer, &format!("UPDATE items SET qty = 100 WHERE id = {i}")).unwrap();
    }
    for i in 81..=100 {
        db.execute(&mut writer, &format!("DELETE FROM items WHERE id = {i}")).unwrap();
    }
    for i in 101..=120 {
        db.execute(&mut writer, &format!("INSERT INTO items VALUES ({i}, 50, 1.0)")).unwrap();
    }

    // Writer sees new aggregates
    let out_w = db.execute(&mut writer, "SELECT COUNT(*), SUM(qty) FROM items").unwrap();
    // 100 - 20 (deleted) + 20 (new) = 100 rows
    // Sum: 20*100 + 60*10 + 20*50 = 2000 + 600 + 1000 = 3600
    assert_eq!(out_w.rows[0][0], Datum::Int(100));
    assert_eq!(out_w.rows[0][1], Datum::Int(3600));

    // Reader MUST strictly see original aggregates: 100 rows, 1000 sum!
    let out_r = db.execute(&mut reader, "SELECT COUNT(*), SUM(qty) FROM items").unwrap();
    assert_eq!(out_r.rows[0][0], Datum::Int(100));
    assert_eq!(out_r.rows[0][1], Datum::Int(1000));

    db.execute(&mut reader, "COMMIT").unwrap();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn mvcc_set_variable_and_session_isolation() {
    let dir = std::env::temp_dir().join(format!("hdb_msv_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();

    db.execute(&mut s, "SET transaction_isolation = 'READ-COMMITTED'").unwrap();
    assert_eq!(s.isolation_level, IsolationLevel::ReadCommitted);

    db.execute(&mut s, "SET @@tx_isolation = 'REPEATABLE-READ'").unwrap();
    assert_eq!(s.isolation_level, IsolationLevel::RepeatableRead);

    db.execute(&mut s, "SET tx_isolation = 'SERIALIZABLE'").unwrap();
    assert_eq!(s.isolation_level, IsolationLevel::Serializable);

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn mvcc_randomized_differential_testing() {
    let dir = std::env::temp_dir().join(format!("hdb_mvcc_diff_{}_{}", std::process::id(), std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
    let _ = fs::remove_dir_all(&dir);
    let db = Database::open(&dir).unwrap();

    let mut init_s = db.new_session();
    db.execute(&mut init_s, "CREATE TABLE kv (id INT PRIMARY KEY, val TEXT NOT NULL)").unwrap();

    // Independent reference model
    #[derive(Clone, Debug)]
    struct RefSession {
        in_txn: bool,
        iso: IsolationLevel,
        snapshot: Option<HashMap<i64, String>>,
        staged: HashMap<i64, Option<String>>,
    }

    struct RefModel {
        committed: HashMap<i64, String>,
        sessions: Vec<RefSession>,
    }

    let mut model = RefModel {
        committed: HashMap::new(),
        sessions: vec![
            RefSession { in_txn: false, iso: IsolationLevel::RepeatableRead, snapshot: None, staged: HashMap::new() },
            RefSession { in_txn: false, iso: IsolationLevel::RepeatableRead, snapshot: None, staged: HashMap::new() },
            RefSession { in_txn: false, iso: IsolationLevel::RepeatableRead, snapshot: None, staged: HashMap::new() },
        ],
    };

    let mut db_sessions = vec![db.new_session(), db.new_session(), db.new_session()];

    struct Prng(u64);
    impl Prng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn range(&mut self, min: u64, max: u64) -> u64 {
            min + (self.next() % (max - min + 1))
        }
    }

    let mut rng = Prng(0xDEADBEEF_CAFE1234);

    for step in 0..1000 {
        let sid = (rng.range(0, 2)) as usize;
        let op_type = rng.range(0, 6);

        match op_type {
            0 => {
                // BEGIN
                if !model.sessions[sid].in_txn {
                    let iso = if rng.range(0, 1) == 0 {
                        IsolationLevel::RepeatableRead
                    } else {
                        IsolationLevel::ReadCommitted
                    };
                    model.sessions[sid].in_txn = true;
                    model.sessions[sid].iso = iso;
                    model.sessions[sid].snapshot = None;
                    model.sessions[sid].staged.clear();

                    let sql = match iso {
                        IsolationLevel::RepeatableRead => "START TRANSACTION ISOLATION LEVEL REPEATABLE READ",
                        IsolationLevel::ReadCommitted => "START TRANSACTION ISOLATION LEVEL READ COMMITTED",
                        IsolationLevel::Serializable => "START TRANSACTION",
                    };
                    db.execute(&mut db_sessions[sid], sql).unwrap();
                }
            }
            1 => {
                // INSERT or UPDATE
                let key = rng.range(1, 15) as i64;
                let val = format!("v_{}_{}", key, step);

                let ref_s = &mut model.sessions[sid];
                if ref_s.in_txn {
                    ref_s.staged.insert(key, Some(val.clone()));
                } else {
                    model.committed.insert(key, val.clone());
                }

                // Check in DB
                let check = db.execute(&mut db_sessions[sid], &format!("SELECT val FROM kv WHERE id = {key}")).unwrap();
                if check.rows.is_empty() {
                    let _ = db.execute(&mut db_sessions[sid], &format!("INSERT INTO kv VALUES ({key}, '{val}')"));
                } else {
                    let _ = db.execute(&mut db_sessions[sid], &format!("UPDATE kv SET val = '{val}' WHERE id = {key}"));
                }
            }
            2 => {
                // DELETE
                let key = rng.range(1, 15) as i64;
                let ref_s = &mut model.sessions[sid];

                let is_present = if ref_s.in_txn {
                    if let Some(staged_val) = ref_s.staged.get(&key) {
                        staged_val.is_some()
                    } else if ref_s.iso == IsolationLevel::RepeatableRead {
                        if ref_s.snapshot.is_none() {
                            ref_s.snapshot = Some(model.committed.clone());
                        }
                        ref_s.snapshot.as_ref().unwrap().contains_key(&key)
                    } else {
                        model.committed.contains_key(&key)
                    }
                } else {
                    model.committed.contains_key(&key)
                };

                if is_present {
                    if ref_s.in_txn {
                        ref_s.staged.insert(key, None);
                    } else {
                        model.committed.remove(&key);
                    }
                }
                let _ = db.execute(&mut db_sessions[sid], &format!("DELETE FROM kv WHERE id = {key}"));
            }
            3 => {
                // Point SELECT
                let key = rng.range(1, 15) as i64;
                let ref_s = &mut model.sessions[sid];

                let expected = if ref_s.in_txn {
                    if let Some(staged_val) = ref_s.staged.get(&key) {
                        staged_val.clone()
                    } else if ref_s.iso == IsolationLevel::RepeatableRead {
                        if ref_s.snapshot.is_none() {
                            ref_s.snapshot = Some(model.committed.clone());
                        }
                        ref_s.snapshot.as_ref().unwrap().get(&key).cloned()
                    } else {
                        model.committed.get(&key).cloned()
                    }
                } else {
                    model.committed.get(&key).cloned()
                };

                let out = db.execute(&mut db_sessions[sid], &format!("SELECT val FROM kv WHERE id = {key}")).unwrap();
                let actual = if out.rows.is_empty() {
                    None
                } else {
                    match &out.rows[0][0] {
                        Datum::Text(s) => Some(s.clone()),
                        _ => None,
                    }
                };

                assert_eq!(actual, expected, "Mismatch at step {step} for key {key}");
            }
            4 => {
                // Range SELECT
                let lo = rng.range(1, 8) as i64;
                let hi = rng.range(8, 15) as i64;
                let ref_s = &mut model.sessions[sid];

                let expected_map: HashMap<i64, String> = if ref_s.in_txn {
                    let base = if ref_s.iso == IsolationLevel::RepeatableRead {
                        if ref_s.snapshot.is_none() {
                            ref_s.snapshot = Some(model.committed.clone());
                        }
                        ref_s.snapshot.as_ref().unwrap().clone()
                    } else {
                        model.committed.clone()
                    };
                    let mut res = base;
                    for (&k, opt_v) in &ref_s.staged {
                        match opt_v {
                            Some(v) => { res.insert(k, v.clone()); }
                            None => { res.remove(&k); }
                        }
                    }
                    res
                } else {
                    model.committed.clone()
                };

                let mut expected_rows: Vec<(i64, String)> = expected_map
                    .into_iter()
                    .filter(|(k, _)| *k >= lo && *k <= hi)
                    .collect();
                expected_rows.sort_by_key(|r| r.0);

                let out = db.execute(
                    &mut db_sessions[sid],
                    &format!("SELECT id, val FROM kv WHERE id BETWEEN {lo} AND {hi} ORDER BY id"),
                ).unwrap();

                let mut actual_rows: Vec<(i64, String)> = Vec::new();
                for r in &out.rows {
                    if let (Datum::Int(k), Datum::Text(v)) = (&r[0], &r[1]) {
                        actual_rows.push((*k, v.clone()));
                    }
                }

                assert_eq!(actual_rows, expected_rows, "Range mismatch at step {step} for [{lo}..{hi}]");
            }
            5 => {
                // COMMIT
                if model.sessions[sid].in_txn {
                    for (&k, opt_v) in &model.sessions[sid].staged {
                        match opt_v {
                            Some(v) => { model.committed.insert(k, v.clone()); }
                            None => { model.committed.remove(&k); }
                        }
                    }
                    model.sessions[sid].in_txn = false;
                    model.sessions[sid].staged.clear();
                    model.sessions[sid].snapshot = None;
                    db.execute(&mut db_sessions[sid], "COMMIT").unwrap();
                }
            }
            6 => {
                // ROLLBACK
                if model.sessions[sid].in_txn {
                    model.sessions[sid].in_txn = false;
                    model.sessions[sid].staged.clear();
                    model.sessions[sid].snapshot = None;
                    db.execute(&mut db_sessions[sid], "ROLLBACK").unwrap();
                }
            }
            _ => unreachable!(),
        }
    }

    let _ = fs::remove_dir_all(&dir);
}
