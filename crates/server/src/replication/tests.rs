//! End-to-end replication tests: in-process primary + replica over
//! localhost TCP (real sockets, real WAL, real checkpoint paths).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use engine::Database;

use super::primary;
use super::replica::{self, ReplicaOpts};

fn leak_flag() -> &'static AtomicBool {
    Box::leak(Box::new(AtomicBool::new(false)))
}

fn primary_db(dir: &std::path::Path) -> Arc<Database> {
    let _ = std::fs::remove_dir_all(dir);
    let db = Database::open(dir).unwrap();
    // auth.bin with empty-password root (dev bootstrap), matching the
    // server's own startup path.
    crate::auth::UserStore::load_or_bootstrap(&dir.join("auth.bin")).unwrap();
    Arc::new(db)
}

fn load_rows(db: &Database, table: &str, base: u64, n: u64) {
    let mut s = db.new_session();
    // Multi-row batches keep the 1,000-row load to a handful of commits.
    for chunk in (base..base + n).collect::<Vec<_>>().chunks(100) {
        let vals: Vec<String> = chunk.iter().map(|i| format!("({i}, {})", i * 3)).collect();
        db.execute(&mut s, &format!("INSERT INTO {table} VALUES {}", vals.join(", ")))
            .unwrap();
    }
}

fn wait_for(msg: &str, timeout: Duration, mut cond: impl FnMut() -> bool) {
    let t0 = Instant::now();
    while !cond() {
        assert!(
            t0.elapsed() < timeout,
            "timed out waiting for replica: {msg}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn count(db: &Database, table: &str) -> i64 {
    // Missing table (pre-snapshot replica) reads as 0 so wait loops keep
    // polling instead of panicking.
    let mut s = db.new_session();
    let Ok(out) = db.execute(&mut s, &format!("SELECT COUNT(*) FROM {table}")) else {
        return 0;
    };
    match out.rows[0][0] {
        engine::Datum::Int(n) => n,
        ref other => panic!("count not int: {other:?}"),
    }
}

#[test]
fn error_codes_for_read_only() {
    assert_eq!(
        crate::wire::packet::mysql_error_for(&engine::Error::ReadOnlyReplica),
        (1290, "HY000")
    );
    assert_eq!(
        crate::wire::pg::codec::sqlstate(&engine::Error::ReadOnlyReplica),
        "25006"
    );
    assert!(engine::Error::ReadOnlyReplica
        .to_string()
        .contains("--read-only"));
}

#[test]
fn error_codes_for_subqueries() {
    assert_eq!(
        crate::wire::packet::mysql_error_for(&engine::Error::InvalidQuery("x".into())),
        (1241, "21000")
    );
    assert_eq!(
        crate::wire::packet::mysql_error_for(&engine::Error::ExecutionError("x".into())),
        (1242, "21000")
    );
    assert_eq!(
        crate::wire::pg::codec::sqlstate(&engine::Error::InvalidQuery("x".into())),
        "21000"
    );
    assert_eq!(
        crate::wire::pg::codec::sqlstate(&engine::Error::ExecutionError("x".into())),
        "21000"
    );
}

#[test]
fn primary_replica_streaming_e2e() {
    let base = std::env::temp_dir();
    let pdir = base.join(format!("hdbrpl_p_{}", std::process::id()));
    let rdir = base.join(format!("hdbrpl_r_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&pdir);
    let _ = std::fs::remove_dir_all(&rdir);

    // -- Primary with pre-existing data (replica must snapshot first). --
    let pdb = primary_db(&pdir);
    {
        let mut s = pdb.new_session();
        db_execute(&pdb, &mut s, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    }
    load_rows(&pdb, "t", 0, 200);

    // -- Replication listener on an ephemeral port. --
    let (pl, port) = primary::bind_repl(0).expect("bind");
    let draining_p = Arc::new(AtomicBool::new(false));
    let global_p = leak_flag();
    let pdb2 = pdb.clone();
    let draining_p2 = draining_p.clone();
    let auth_path = pdir.join("auth.bin");
    let repl_thread = std::thread::spawn(move || {
        primary::serve_primary(pdb2, pl, auth_path, draining_p2, global_p)
    });

    // -- Replica (fresh dir → snapshot bootstrap, read-only serving). --
    let rdb = Arc::new(Database::open(&rdir).unwrap());
    rdb.set_read_only(true);
    let draining_r = Arc::new(AtomicBool::new(false));
    let global_r = leak_flag();
    let rdb2 = rdb.clone();
    let draining_r2 = draining_r.clone();
    let replica_thread = std::thread::spawn(move || {
        replica::run_replica(
            rdb2,
            ReplicaOpts {
                primary: format!("127.0.0.1:{port}"),
                user: "root".into(),
                password: "".into(),
                dir: rdir.clone(),
            },
            draining_r2,
            global_r,
        )
    });

    // -- Live writes stream after the snapshot. --
    load_rows(&pdb, "t", 200, 800);
    wait_for("replica catch-up to 1000", Duration::from_secs(30), || {
        count(&rdb, "t") == 1000
    });

    // Identical results, spot-checked across the key space.
    {
        let mut ps = pdb.new_session();
        let mut rs = rdb.new_session();
        for probe in [0u64, 7, 199, 200, 555, 999] {
            let p = pdb
                .execute(&mut ps, &format!("SELECT v FROM t WHERE id = {probe}"))
                .unwrap();
            let r = rdb
                .execute(&mut rs, &format!("SELECT v FROM t WHERE id = {probe}"))
                .unwrap();
            assert_eq!(p.rows, r.rows, "mismatch at id {probe}");
        }
    }

    // Replica telemetry is live (checked after post-snapshot streaming
    // below, so `applied` has advanced past the snapshot head).
    {
        let mut rs = rdb.new_session();
        let out = rdb.execute(&mut rs, "SHOW STATUS LIKE 'Rpl_%'").unwrap();
        let val = |name: &str| {
            out.rows
                .iter()
                .find(|r| r[0] == engine::Datum::Text(name.into()))
                .unwrap_or_else(|| panic!("missing {name}"))
                .get(1)
                .cloned()
                .unwrap()
                .to_string()
        };
        assert_eq!(val("Rpl_replica_status"), "STREAMING");
        let text = rdb.prometheus_text();
        assert!(text.contains("replication_applied_offset"));
        assert!(text.contains("replication_lag_bytes"));
        assert!(text.contains("replication_connected_replicas"));
    }
    // Primary sees its subscriber.
    wait_for("primary replica count", Duration::from_secs(10), || {
        pdb.metrics().snapshot().repl_connected == 1
    });

    // Writes rejected with 1290/ReadOnly on the replica.
    for stmt in [
        "INSERT INTO t VALUES (1001, 1)",
        "UPDATE t SET v = 0 WHERE id = 1",
        "DELETE FROM t WHERE id = 1",
        "CREATE TABLE u (id INT PRIMARY KEY)",
        "DROP TABLE t",
        "BACKUP DATABASE TO '/tmp/nope'",
    ] {
        assert_eq!(
            rdb.execute(&mut rdb.new_session(), stmt),
            Err(engine::Error::ReadOnlyReplica),
            "{stmt}"
        );
    }
    // ...while the primary keeps accepting them.
    {
        let mut ps = pdb.new_session();
        pdb.execute(&mut ps, "INSERT INTO t VALUES (1001, 3003)").unwrap();
    }
    wait_for("replica catch-up to 1001", Duration::from_secs(30), || {
        count(&rdb, "t") == 1001
    });
    // Post-snapshot streaming advanced the applied offset past the fresh
    // log head, and heartbeats drive lag back to zero once caught up.
    wait_for("applied offset advanced", Duration::from_secs(15), || {
        let mut rs = rdb.new_session();
        let out = rdb.execute(&mut rs, "SHOW STATUS LIKE 'Rpl_replica_applied_offset'").unwrap();
        out.rows[0][1].to_string().parse::<u64>().unwrap() > 8
    });
    wait_for("lag drained", Duration::from_secs(15), || {
        let mut rs = rdb.new_session();
        let out = rdb.execute(&mut rs, "SHOW STATUS LIKE 'Rpl_replica_lag_bytes'").unwrap();
        out.rows[0][1].to_string() == "0"
    });

    draining_r.store(true, Ordering::Relaxed);
    draining_p.store(true, Ordering::Relaxed);
    replica_thread.join().unwrap();
    repl_thread.join().unwrap();
    let _ = std::fs::remove_dir_all(&pdir);
    let _ = std::fs::remove_dir_all(&base.join(format!("hdbrpl_r_{}", std::process::id())));
}

#[test]
fn replica_reconnects_and_resumes_from_offset() {
    let base = std::env::temp_dir();
    let pid = std::process::id();
    let pdir = base.join(format!("hdbrc_p_{pid}"));
    let rdir = base.join(format!("hdbrc_r_{pid}"));
    let _ = std::fs::remove_dir_all(&pdir);
    let _ = std::fs::remove_dir_all(&rdir);

    let pdb = primary_db(&pdir);
    {
        let mut s = pdb.new_session();
        db_execute(&pdb, &mut s, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    }
    load_rows(&pdb, "t", 0, 500);

    // Replica starts BEFORE any primary listener: backoff-connect first.
    let rdb = Arc::new(Database::open(&rdir).unwrap());
    rdb.set_read_only(true);
    // Bind the port first (closed listener = connection refused) so the
    // address is fixed; the real listener comes up late.
    let probe = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);
    let draining_r = Arc::new(AtomicBool::new(false));
    let global_r = leak_flag();
    let rdb2 = rdb.clone();
    let draining_r2 = draining_r.clone();
    let rdir2 = rdir.clone();
    let replica_thread = std::thread::spawn(move || {
        replica::run_replica(
            rdb2,
            ReplicaOpts {
                primary: format!("127.0.0.1:{port}"),
                user: "root".into(),
                password: "".into(),
                dir: rdir2,
            },
            draining_r2,
            global_r,
        )
    });
    // Nothing listening yet: replica must still be empty, not crashed.
    std::thread::sleep(Duration::from_millis(1500));
    assert_eq!(count(&rdb, "t"), 0);

    // Primary listener (phase 1) + drain of the first 500 rows.
    let (pl1, _) = std::net::TcpListener::bind(("127.0.0.1", port))
        .map(|l| (l, port))
        .expect("rebind");
    let draining_p1 = Arc::new(AtomicBool::new(false));
    let global_p = leak_flag();
    let pdb2 = pdb.clone();
    let draining_p12 = draining_p1.clone();
    let auth_path = pdir.join("auth.bin");
    let t1 = std::thread::spawn(move || {
        primary::serve_primary(pdb2, pl1, auth_path, draining_p12, global_p)
    });
    wait_for("initial catch-up to 500", Duration::from_secs(30), || {
        count(&rdb, "t") == 500
    });
    let snap_path = rdir.join("snapshot.bin");
    wait_for("snapshot.bin exists", Duration::from_secs(10), || {
        snap_path.exists()
    });
    let snap_mtime = std::fs::metadata(&snap_path)
        .unwrap()
        .modified()
        .unwrap();

    // Kill the listener mid-stream (accepted feeder sockets die with the
    // replica's reconnect, not the listener — the point is the replica
    // survives a dead primary and resumes without re-snapshotting).
    draining_p1.store(true, Ordering::Relaxed);
    t1.join().unwrap();
    // Writes land on the primary while nobody listens.
    load_rows(&pdb, "t", 500, 500);
    std::thread::sleep(Duration::from_millis(1500));
    // Replica cannot advance without the primary (no crash, no phantom).
    assert_eq!(count(&rdb, "t"), 500);

    // Listener (phase 2) on the same port, same primary database.
    let (pl2, _) = std::net::TcpListener::bind(("127.0.0.1", port))
        .map(|l| (l, port))
        .expect("rebind");
    let draining_p2 = Arc::new(AtomicBool::new(false));
    let pdb3 = pdb.clone();
    let draining_p22 = draining_p2.clone();
    let auth_path = pdir.join("auth.bin");
    let t2 = std::thread::spawn(move || {
        primary::serve_primary(pdb3, pl2, auth_path, draining_p22, global_p)
    });
    wait_for("resume catch-up to 1000", Duration::from_secs(30), || {
        count(&rdb, "t") == 1000
    });
    // No re-snapshot happened: same generation/offset resume means the
    // snapshot files were never rewritten.
    let snap_mtime2 = std::fs::metadata(rdir.join("snapshot.bin"))
        .unwrap()
        .modified()
        .unwrap();
    assert_eq!(
        snap_mtime, snap_mtime2,
        "replica re-snapshotted instead of resuming"
    );

    draining_r.store(true, Ordering::Relaxed);
    draining_p2.store(true, Ordering::Relaxed);
    replica_thread.join().unwrap();
    t2.join().unwrap();
    let _ = std::fs::remove_dir_all(&pdir);
    let _ = std::fs::remove_dir_all(&rdir);
}

fn db_execute(db: &Database, s: &mut engine::Session, sql: &str) {
    db.execute(s, sql).unwrap();
}
