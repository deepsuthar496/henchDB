//! Transaction tests: explicit txns, recovery, concurrency, MVCC snapshots.

use super::*;

#[test]
fn explicit_txn_commit_and_rollback() {
    let dir = std::env::temp_dir().join(format!("hdbtxn_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = setup(&dir);
    let mut s = db.new_session();
    db.execute(&mut s, "INSERT INTO t VALUES (1, 'ann', 1.0)").unwrap();

    // Rollback discards staged writes.
    db.execute(&mut s, "BEGIN").unwrap();
    db.execute(&mut s, "INSERT INTO t VALUES (2, 'bob', 2.0)").unwrap();
    let out = db.execute(&mut s, "SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(out.rows[0][0], Datum::Int(2), "own writes visible in txn");
    db.execute(&mut s, "ROLLBACK").unwrap();
    let out = db.execute(&mut s, "SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(out.rows[0][0], Datum::Int(1));

    // Commit persists staged writes.
    db.execute(&mut s, "BEGIN").unwrap();
    db.execute(&mut s, "INSERT INTO t VALUES (2, 'bob', 2.0)").unwrap();
    db.execute(&mut s, "UPDATE t SET name = 'ann2' WHERE id = 1").unwrap();
    db.execute(&mut s, "COMMIT").unwrap();
    let out = db.execute(&mut s, "SELECT name FROM t WHERE id = 1").unwrap();
    assert_eq!(out.rows[0][0], Datum::Text("ann2".into()));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn recovery_after_crash() {
    let dir = std::env::temp_dir().join(format!("hdbrec_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    {
        let db = setup(&dir);
        let mut s = db.new_session();
        db.execute(&mut s, "INSERT INTO t VALUES (1, 'ann', 1.0), (2, 'bob', 2.0)")
            .unwrap();
        db.execute(&mut s, "UPDATE t SET score = 3.0 WHERE id = 1").unwrap();
        db.execute(&mut s, "DELETE FROM t WHERE id = 2").unwrap();
        // Drop without checkpoint: recovery must replay everything.
    }
    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();
    let out = db.execute(&mut s, "SELECT id, name, score FROM t").unwrap();
    assert_eq!(out.rows.len(), 1);
    assert_eq!(out.rows[0][0], Datum::Int(1));
    assert_eq!(out.rows[0][1], Datum::Text("ann".into()));
    assert_eq!(out.rows[0][2], Datum::Float(3.0));

    // Checkpoint, mutate again, recover from snapshot + WAL.
    db.execute(&mut s, "CHECKPOINT").unwrap();
    db.execute(&mut s, "INSERT INTO t VALUES (5, 'eve', 5.0)").unwrap();
    drop(db);
    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();
    let out = db.execute(&mut s, "SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(out.rows[0][0], Datum::Int(2));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn uncommitted_tail_is_discarded() {
    // Simulates a crash between the last Put and its Commit record by
    // building a WAL manually, then opening the database over it.
    let dir = std::env::temp_dir().join(format!("hdgtail_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    let db = setup(&dir);
    let mut s = db.new_session();
    db.execute(&mut s, "INSERT INTO t VALUES (1, 'a', 1.0)").unwrap();
    drop(db);

    // Append a dangling Put without a Commit.
    let wal = Wal::open(&dir.join("wal.log")).unwrap();
    wal.append_unsynced(&[Record::Put {
        txn: 99,
        table: "t".into(),
        key: encode_key(&Datum::Int(2)).unwrap(),
        row: vec![],
    }])
    .unwrap();
    drop(wal);

    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();
    let out = db.execute(&mut s, "SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(out.rows[0][0], Datum::Int(1), "dangling txn must not apply");
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn concurrent_commits_all_persist() {
    let dir = std::env::temp_dir().join(format!("hdbconc_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = Arc::new(setup(&dir));
    let mut handles = vec![];
    for w in 0..4u64 {
        let db = db.clone();
        handles.push(std::thread::spawn(move || {
            let mut s = db.new_session();
            for i in 0..100u64 {
                let id = (w * 100 + i) as i64;
                db.execute(&mut s, &format!("INSERT INTO t VALUES ({id}, 'w{w}', {i}.0)"))
                    .unwrap();
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    let mut s = db.new_session();
    let out = db.execute(&mut s, "SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(out.rows[0][0], Datum::Int(400));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn overflow_uncommitted_tail_discarded() {
    // A committed overflow row replays; a dangling WAL tail does not.
    let dir = std::env::temp_dir().join(format!("hdbpgtail_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    {
        let db = Database::open(&dir).unwrap();
        let mut s = db.new_session();
        db.execute(&mut s, "CREATE TABLE docs (id INT PRIMARY KEY, body TEXT)").unwrap();
        let wide = "w".repeat(8000);
        db.execute(&mut s, &format!("INSERT INTO docs VALUES (1, '{wide}')")).unwrap();
    }
    // Dangling Put without Commit (wide row, never installed).
    let wal = Wal::open(&dir.join("wal.log")).unwrap();
    wal.append_unsynced(&[Record::Put {
        txn: 99,
        table: "docs".into(),
        key: encode_key(&Datum::Int(2)).unwrap(),
        row: Table::encode_row(&[Datum::Int(2), Datum::Text("x".repeat(8000))]),
    }])
    .unwrap();
    drop(wal);
    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();
    let out = db.execute(&mut s, "SELECT COUNT(*) FROM docs").unwrap();
    assert_eq!(out.rows[0][0], Datum::Int(1));
    let out = db.execute(&mut s, "SELECT body FROM docs WHERE id = 1").unwrap();
    assert_eq!(out.rows[0][0], Datum::Text("w".repeat(8000)));
    let _ = fs::remove_dir_all(&dir);
}

// -- MVCC snapshots (F3) -----------------------------------------------------

fn mvcc_setup(dir: &Path) -> Database {
    let db = Database::open(dir).unwrap();
    let mut s = db.new_session();
    db.execute(&mut s, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        .unwrap();
    db.execute(&mut s, "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .unwrap();
    db
}

fn mvcc_val(db: &Database, s: &mut Session, id: i64) -> Option<String> {
    let out = db
        .execute(s, &format!("SELECT v FROM t WHERE id = {id}"))
        .unwrap();
    out.rows.first().map(|r| match &r[0] {
        Datum::Text(v) => v.clone(),
        other => panic!("unexpected {other:?}"),
    })
}

#[test]
fn snapshot_repeatable_read() {
    let dir = std::env::temp_dir().join(format!("hdbmv1_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = mvcc_setup(&dir);
    let mut a = db.new_session();
    let mut b = db.new_session();
    db.execute(&mut a, "START TRANSACTION WITH CONSISTENT SNAPSHOT")
        .unwrap();
    assert_eq!(mvcc_val(&db, &mut a, 1).as_deref(), Some("a"));
    // Concurrent update commits after the pin.
    db.execute(&mut b, "UPDATE t SET v = 'b2' WHERE id = 1").unwrap();
    db.execute(&mut b, "INSERT INTO t VALUES (4, 'd')").unwrap();
    // Long reader still sees the pinned state; fresh sessions see new.
    assert_eq!(mvcc_val(&db, &mut a, 1).as_deref(), Some("a"));
    assert_eq!(mvcc_val(&db, &mut a, 4), None);
    assert_eq!(mvcc_val(&db, &mut b, 1).as_deref(), Some("b2"));
    assert_eq!(mvcc_val(&db, &mut b, 4).as_deref(), Some("d"));
    // Repeatable: re-reads within the snapshot never change.
    assert_eq!(mvcc_val(&db, &mut a, 1).as_deref(), Some("a"));
    db.execute(&mut a, "COMMIT").unwrap();
    assert_eq!(mvcc_val(&db, &mut a, 1).as_deref(), Some("b2"));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn snapshot_sees_deleted_rows() {
    let dir = std::env::temp_dir().join(format!("hdbmv2_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = mvcc_setup(&dir);
    let mut a = db.new_session();
    let mut b = db.new_session();
    db.execute(&mut a, "START TRANSACTION WITH CONSISTENT SNAPSHOT")
        .unwrap();
    db.execute(&mut b, "DELETE FROM t WHERE id = 2").unwrap();
    // Deleted-after-pin still visible to the snapshot (point + scan).
    assert_eq!(mvcc_val(&db, &mut a, 2).as_deref(), Some("b"));
    let out = db.execute(&mut a, "SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(out.rows[0][0], Datum::Int(3));
    let out = db.execute(&mut b, "SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(out.rows[0][0], Datum::Int(2));
    db.execute(&mut a, "ROLLBACK").unwrap();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn snapshot_rollback_isolation() {
    let dir = std::env::temp_dir().join(format!("hdbmv3_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = mvcc_setup(&dir);
    let mut a = db.new_session();
    let mut b = db.new_session();
    // Uncommitted writes never leak, pinned or not.
    db.execute(&mut b, "BEGIN").unwrap();
    db.execute(&mut b, "INSERT INTO t VALUES (9, 'uncommitted')").unwrap();
    db.execute(&mut a, "START TRANSACTION WITH CONSISTENT SNAPSHOT")
        .unwrap();
    assert_eq!(mvcc_val(&db, &mut a, 9), None);
    assert_eq!(mvcc_val(&db, &mut b, 9).as_deref(), Some("uncommitted"));
    db.execute(&mut b, "ROLLBACK").unwrap();
    assert_eq!(mvcc_val(&db, &mut a, 9), None);
    db.execute(&mut a, "COMMIT").unwrap();
    let out = db.execute(&mut b, "SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(out.rows[0][0], Datum::Int(3));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn snapshot_version_pruning() {
    let dir = std::env::temp_dir().join(format!("hdbmv4_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = mvcc_setup(&dir);
    let mut a = db.new_session();
    let mut b = db.new_session();
    db.execute(&mut a, "START TRANSACTION WITH CONSISTENT SNAPSHOT")
        .unwrap();
    for i in 0..5 {
        db.execute(&mut b, &format!("UPDATE t SET v = 'v{i}' WHERE id = 1"))
            .unwrap();
    }
    let (chains, entries, active) = db.version_stats();
    assert_eq!(active, 1);
    assert!(chains >= 1 && entries >= 5, "chains={chains} entries={entries}");
    // Releasing the only reader drains everything.
    db.execute(&mut a, "COMMIT").unwrap();
    assert_eq!(db.version_stats(), (0, 0, 0));
    // Plain OLTP records nothing at all.
    db.execute(&mut b, "UPDATE t SET v = 'final' WHERE id = 1")
        .unwrap();
    assert_eq!(db.version_stats(), (0, 0, 0));
    assert_eq!(mvcc_val(&db, &mut b, 1).as_deref(), Some("final"));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn snapshot_concurrent_writer_stress() {
    use std::sync::{Arc, Barrier};
    let dir = std::env::temp_dir().join(format!("hdbmv5_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = Arc::new(mvcc_setup(&dir));
    // Pin before the writer starts.
    let mut r = db.new_session();
    db.execute(&mut r, "START TRANSACTION WITH CONSISTENT SNAPSHOT")
        .unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let (dbw, barrierw) = (db.clone(), barrier.clone());
    let writer = std::thread::spawn(move || {
        let mut w = dbw.new_session();
        barrierw.wait();
        for i in 0..200 {
            dbw.execute(&mut w, &format!("UPDATE t SET v = 'w{i}' WHERE id = 1"))
                .unwrap();
        }
    });
    barrier.wait();
    // The pinned reader observes one stable value throughout the storm
    // (first read pins nothing new — every read re-resolves at read_epoch).
    let first = mvcc_val(&db, &mut r, 1);
    assert_eq!(first.as_deref(), Some("a"));
    for _ in 0..50 {
        assert_eq!(mvcc_val(&db, &mut r, 1), first);
    }
    writer.join().unwrap();
    assert_eq!(mvcc_val(&db, &mut r, 1).as_deref(), Some("a"));
    db.execute(&mut r, "COMMIT").unwrap();
    // After release the newest value is visible.
    let mut f = db.new_session();
    assert_eq!(mvcc_val(&db, &mut f, 1).as_deref(), Some("w199"));
    let _ = fs::remove_dir_all(&dir);
}
