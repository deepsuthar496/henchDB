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
fn error_codes_for_promote() {
    assert_eq!(
        crate::wire::packet::mysql_error_for(&engine::Error::InvalidOperation("x".into())),
        (1317, "HY000")
    );
    assert_eq!(
        crate::wire::pg::codec::sqlstate(&engine::Error::InvalidOperation("x".into())),
        "55000"
    );
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

    // Wait for the replica to finish bootstrapping the initial 200 rows via snapshot
    wait_for("replica initial snapshot 200", Duration::from_secs(30), || {
        count(&rdb, "t") == 200
    });

    // -- Live writes stream after the snapshot. --
    load_rows(&pdb, "t", 200, 800);
    // Trajectory samples so a timeout explains itself (frozen vs slow).
    {
        let t0 = Instant::now();
        let mut samples = Vec::new();
        while count(&rdb, "t") != 1000 {
            std::thread::sleep(Duration::from_millis(250));
            if samples.len() < 120 {
                samples.push((
                    t0.elapsed().as_secs(),
                    count(&pdb, "t"),
                    pdb.wal_durable(),
                    rdb.metrics().snapshot().repl_applied,
                ));
            }
            assert!(
                t0.elapsed() < Duration::from_secs(60),
                "timed out waiting for replica: catch-up to 1000, trajectory (t, p_count, p_durable, r_applied) {samples:?}"
            );
        }
    }

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

// ---------------------------------------------------------------------------
// Priority 12: promotion & failover.
// ---------------------------------------------------------------------------

fn spawn_primary(
    db: &Arc<Database>,
    auth_path: std::path::PathBuf,
) -> (std::thread::JoinHandle<()>, u16, Arc<AtomicBool>) {
    let (pl, port) = primary::bind_repl(0).expect("bind repl");
    let draining = Arc::new(AtomicBool::new(false));
    let global = leak_flag();
    let db2 = db.clone();
    let draining2 = draining.clone();
    let handle = std::thread::spawn(move || {
        primary::serve_primary(db2, pl, auth_path, draining2, global)
    });
    (handle, port, draining)
}

fn spawn_replica(
    db: &Arc<Database>,
    primary: &str,
    dir: &std::path::Path,
) -> (std::thread::JoinHandle<()>, Arc<AtomicBool>) {
    let draining = Arc::new(AtomicBool::new(false));
    let global = leak_flag();
    let db2 = db.clone();
    let draining2 = draining.clone();
    let opts = ReplicaOpts {
        primary: primary.to_string(),
        user: "root".into(),
        password: "".into(),
        dir: dir.to_path_buf(),
    };
    let handle = std::thread::spawn(move || {
        replica::run_replica(db2, opts, draining2, global)
    });
    (handle, draining)
}

fn status_val(db: &Database, name: &str) -> String {
    let out = db
        .execute(&mut db.new_session(), &format!("SHOW STATUS LIKE '{name}'"))
        .unwrap();
    assert_eq!(out.rows.len(), 1, "missing status {name}");
    out.rows[0][1].to_string()
}

/// Generation sampled after it stops moving (a snapshot-apply's trailing
/// checkpoint can still be in flight when row counts first converge).
fn stable_generation(db: &Database) -> u64 {
    let t0 = Instant::now();
    let mut last = db.wal_generation();
    loop {
        assert!(t0.elapsed() < Duration::from_secs(15), "generation never settled");
        std::thread::sleep(Duration::from_millis(100));
        let cur = db.wal_generation();
        if cur > 0 && cur == last {
            return cur;
        }
        last = cur;
    }
}

#[test]
fn promote_offline_flips_replica_to_read_write() {
    let base = std::env::temp_dir();
    let pid = std::process::id();
    let pdir = base.join(format!("hdbpo_p_{pid}"));
    let rdir = base.join(format!("hdbpo_r_{pid}"));
    let _ = std::fs::remove_dir_all(&pdir);
    let _ = std::fs::remove_dir_all(&rdir);

    let pdb = primary_db(&pdir);
    {
        let mut s = pdb.new_session();
        db_execute(&pdb, &mut s, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    }
    load_rows(&pdb, "t", 0, 300);
    let (pt, pport, pdrain) = spawn_primary(&pdb, pdir.join("auth.bin"));

    let rdb = Arc::new(Database::open(&rdir).unwrap());
    rdb.set_read_only(true);
    let (rt, rdrain) = spawn_replica(&rdb, &format!("127.0.0.1:{pport}"), &rdir);
    wait_for("replica sync 300", Duration::from_secs(30), || count(&rdb, "t") == 300);

    // Stop the replica world (feeder + handle) before offline promotion;
    // joining first quiesces any trailing apply checkpoint, so the
    // generation sampled below is stable.
    rdrain.store(true, Ordering::Relaxed);
    rt.join().unwrap();
    let gen_before = rdb.wal_generation();
    drop(rdb);

    // Offline promote on the stopped directory.
    let db = Database::open(&rdir).unwrap();
    db.promote_offline().unwrap();
    assert_eq!(db.wal_generation(), gen_before + 1);
    assert!(!db.is_read_only());
    db.execute(&mut db.new_session(), "INSERT INTO t VALUES (300, 900)").unwrap();
    drop(db);

    // Reopen: read-write persisted, all rows present.
    let db2 = Database::open(&rdir).unwrap();
    assert!(!db2.is_read_only());
    assert_eq!(count(&db2, "t"), 301);
    drop(db2);

    pdrain.store(true, Ordering::Relaxed);
    pt.join().unwrap();
    let _ = std::fs::remove_dir_all(&pdir);
    let _ = std::fs::remove_dir_all(&rdir);
}

#[test]
fn promote_live_runtime_stops_feeder_and_enables_writes() {
    let base = std::env::temp_dir();
    let pid = std::process::id();
    let pdir = base.join(format!("hdbpl_p_{pid}"));
    let rdir = base.join(format!("hdbpl_r_{pid}"));
    let _ = std::fs::remove_dir_all(&pdir);
    let _ = std::fs::remove_dir_all(&rdir);

    let pdb = primary_db(&pdir);
    {
        let mut s = pdb.new_session();
        db_execute(&pdb, &mut s, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    }
    load_rows(&pdb, "t", 0, 200);
    let (pt, pport, pdrain) = spawn_primary(&pdb, pdir.join("auth.bin"));

    let rdb = Arc::new(Database::open(&rdir).unwrap());
    rdb.set_read_only(true);
    rdb.set_replica_upstream(&format!("127.0.0.1:{pport}"));
    let (rt, _) = spawn_replica(&rdb, &format!("127.0.0.1:{pport}"), &rdir);
    wait_for("replica sync 200", Duration::from_secs(30), || {
        count(&rdb, "t") == 200 && rdb.metrics().snapshot().repl_applied > 0
    });
    let gen_before = stable_generation(&rdb);
    assert_eq!(status_val(&rdb, "Replica_Role"), "Replica");

    // Live promotion through the SQL surface (parser + gate + fence).
    let out = rdb.execute(&mut rdb.new_session(), "PROMOTE").unwrap();
    assert!(out.message.contains("promoted to primary"), "{}", out.message);
    // The feeder thread observes the flip and detaches on its own.
    rt.join().unwrap();
    assert!(!rdb.is_read_only());
    assert_eq!(rdb.wal_generation(), gen_before + 1);
    assert_eq!(status_val(&rdb, "Replica_Role"), "Primary");
    assert_eq!(status_val(&rdb, "Rpl_replica_status"), "PROMOTED");
    // The fence is sealed on disk with the promotion marker.
    let seal = std::fs::read_to_string(rdir.join("repl.offset")).unwrap();
    assert!(
        seal.contains("promoted") && seal.starts_with(&format!("{} ", gen_before + 1)),
        "bad seal: {seal}"
    );
    // Immediate local writes, then more streamed history stays put.
    rdb.execute(&mut rdb.new_session(), "INSERT INTO t VALUES (200, 600)").unwrap();
    assert_eq!(count(&rdb, "t"), 201);

    pdrain.store(true, Ordering::Relaxed);
    pt.join().unwrap();
    let _ = std::fs::remove_dir_all(&pdir);
    let _ = std::fs::remove_dir_all(&rdir);
}

#[test]
fn cascading_replication_after_promotion() {
    let base = std::env::temp_dir();
    let pid = std::process::id();
    let pdir = base.join(format!("hdbcas_p_{pid}"));
    let r1dir = base.join(format!("hdbcas_r1_{pid}"));
    let r2dir = base.join(format!("hdbcas_r2_{pid}"));
    for d in [&pdir, &r1dir, &r2dir] {
        let _ = std::fs::remove_dir_all(d);
    }

    // P streams to R1 and R2.
    let pdb = primary_db(&pdir);
    {
        let mut s = pdb.new_session();
        db_execute(&pdb, &mut s, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    }
    load_rows(&pdb, "t", 0, 200);
    let (pt, pport, pdrain) = spawn_primary(&pdb, pdir.join("auth.bin"));
    let paddr = format!("127.0.0.1:{pport}");

    let r1db = Arc::new(Database::open(&r1dir).unwrap());
    r1db.set_read_only(true);
    let (rt1, _) = spawn_replica(&r1db, &paddr, &r1dir);
    let r2db = Arc::new(Database::open(&r2dir).unwrap());
    r2db.set_read_only(true);
    let (rt2, rdrain2) = spawn_replica(&r2db, &paddr, &r2dir);
    wait_for("r1 sync 200", Duration::from_secs(30), || count(&r1db, "t") == 200);
    wait_for("r2 sync 200", Duration::from_secs(30), || count(&r2db, "t") == 200);

    // P dies. R1 promotes and keeps serving (it needs an auth store for
    // its own subscribers; snapshots never carried one).
    pdrain.store(true, Ordering::Relaxed);
    pt.join().unwrap();
    crate::auth::UserStore::load_or_bootstrap(&r1dir.join("auth.bin")).unwrap();
    r1db.execute(&mut r1db.new_session(), "PROMOTE").unwrap();
    rt1.join().unwrap();
    assert!(!r1db.is_read_only());
    // Post-promotion writes on the new primary.
    load_rows(&r1db, "t", 200, 50);
    assert_eq!(count(&r1db, "t"), 250);
    let (r1t, r1port, r1drain) = spawn_primary(&r1db, r1dir.join("auth.bin"));

    // R2 repoints to R1 (new generation, new upstream): snapshot bootstrap
    // across the promotion boundary, then plain chunk streaming.
    rdrain2.store(true, Ordering::Relaxed);
    rt2.join().unwrap();
    let (rt2b, rdrain2b) =
        spawn_replica(&r2db, &format!("127.0.0.1:{r1port}"), &r2dir);
    wait_for("r2 cascade to 250", Duration::from_secs(30), || count(&r2db, "t") == 250);
    // Spot-check a post-promotion row came over intact.
    let out = r2db
        .execute(&mut r2db.new_session(), "SELECT v FROM t WHERE id = 240")
        .unwrap();
    assert_eq!(out.rows[0][0], engine::Datum::Int(720));
    // The promoter was never rolled back by its own subscriber.
    assert_eq!(count(&r1db, "t"), 250);

    rdrain2b.store(true, Ordering::Relaxed);
    r1drain.store(true, Ordering::Relaxed);
    rt2b.join().unwrap();
    r1t.join().unwrap();
    for d in [&pdir, &r1dir, &r2dir] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[test]
fn stale_primary_snapshot_refused_after_promotion() {
    let base = std::env::temp_dir();
    let pid = std::process::id();
    let pdir = base.join(format!("hdbfen_p_{pid}"));
    let rdir = base.join(format!("hdbfen_r_{pid}"));
    let _ = std::fs::remove_dir_all(&pdir);
    let _ = std::fs::remove_dir_all(&rdir);

    let pdb = primary_db(&pdir);
    {
        let mut s = pdb.new_session();
        db_execute(&pdb, &mut s, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    }
    load_rows(&pdb, "t", 0, 100);
    let (pt, pport, pdrain) = spawn_primary(&pdb, pdir.join("auth.bin"));
    let paddr = format!("127.0.0.1:{pport}");

    let rdb = Arc::new(Database::open(&rdir).unwrap());
    rdb.set_read_only(true);
    let (rt, _) = spawn_replica(&rdb, &paddr, &rdir);
    wait_for("replica sync 100", Duration::from_secs(30), || {
        count(&rdb, "t") == 100 && rdb.metrics().snapshot().repl_applied > 0
    });

    // Promote (feeder detaches + seals), then write past the old history.
    rdb.execute(&mut rdb.new_session(), "PROMOTE").unwrap();
    rt.join().unwrap();
    load_rows(&rdb, "t", 100, 10);
    assert_eq!(count(&rdb, "t"), 110);
    let seal = std::fs::read_to_string(rdir.join("repl.offset")).unwrap();

    // Reconnect the feeder to the SAME stale primary: it must refuse
    // (never connect-usefully), leaving all 110 rows intact.
    let (rt2, rdrain2) = spawn_replica(&rdb, &paddr, &rdir);
    wait_for("refusal observed", Duration::from_secs(20), || {
        status_val(&rdb, "Rpl_replica_status") == "DISCONNECTED"
    });
    std::thread::sleep(Duration::from_secs(3));
    assert_eq!(count(&rdb, "t"), 110, "stale primary overwrote promoted state");
    // The seal is untouched by refusals.
    assert_eq!(std::fs::read_to_string(rdir.join("repl.offset")).unwrap(), seal);

    rdrain2.store(true, Ordering::Relaxed);
    rt2.join().unwrap();
    pdrain.store(true, Ordering::Relaxed);
    pt.join().unwrap();
    let _ = std::fs::remove_dir_all(&pdir);
    let _ = std::fs::remove_dir_all(&rdir);
}

#[test]
fn replica_crash_and_restart_catches_up() {
    let base = std::env::temp_dir();
    let pid = std::process::id();
    let pdir = base.join(format!("hdbrcr_p_{pid}"));
    let rdir = base.join(format!("hdbrcr_r_{pid}"));
    let _ = std::fs::remove_dir_all(&pdir);
    let _ = std::fs::remove_dir_all(&rdir);

    let pdb = primary_db(&pdir);
    {
        let mut s = pdb.new_session();
        db_execute(&pdb, &mut s, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    }
    load_rows(&pdb, "t", 0, 100);
    let (pt, pport, pdrain) = spawn_primary(&pdb, pdir.join("auth.bin"));
    let paddr = format!("127.0.0.1:{pport}");

    // Phase 1: Replica syncs initial 100 rows
    let rdb = Arc::new(Database::open(&rdir).unwrap());
    rdb.set_read_only(true);
    let (rt, rdrain) = spawn_replica(&rdb, &paddr, &rdir);
    wait_for("replica sync 100", Duration::from_secs(30), || {
        count(&rdb, "t") == 100
    });

    // Phase 2: Simulate replica crash (stop thread, drop DB instance)
    rdrain.store(true, Ordering::Relaxed);
    rt.join().unwrap();
    drop(rdb);

    // Phase 3: Primary receives 100 more writes while replica is offline
    load_rows(&pdb, "t", 100, 100);
    assert_eq!(count(&pdb, "t"), 200);

    // Phase 4: Replica reopens from the same data directory, catches up
    let rdb2 = Arc::new(Database::open(&rdir).unwrap());
    rdb2.set_read_only(true);
    let (rt2, rdrain2) = spawn_replica(&rdb2, &paddr, &rdir);
    wait_for("replica catch-up after restart to 200", Duration::from_secs(30), || {
        count(&rdb2, "t") == 200
    });

    // Verify all rows match primary bit-for-bit
    {
        let mut ps = pdb.new_session();
        let mut rs = rdb2.new_session();
        for probe in [0u64, 50, 99, 100, 150, 199] {
            let p = pdb.execute(&mut ps, &format!("SELECT v FROM t WHERE id = {probe}")).unwrap();
            let r = rdb2.execute(&mut rs, &format!("SELECT v FROM t WHERE id = {probe}")).unwrap();
            assert_eq!(p.rows, r.rows, "mismatch at probe id {probe}");
        }
    }

    rdrain2.store(true, Ordering::Relaxed);
    rt2.join().unwrap();
    pdrain.store(true, Ordering::Relaxed);
    pt.join().unwrap();
    let _ = std::fs::remove_dir_all(&pdir);
    let _ = std::fs::remove_dir_all(&rdir);
}

#[test]
fn corrupt_wal_chunk_rejected_safely() {
    use super::protocol::{write_frame, Frame};
    use std::io::Write;
    use std::net::TcpListener;

    let base = std::env::temp_dir();
    let pid = std::process::id();
    let rdir = base.join(format!("hdbcwal_r_{pid}"));
    let _ = std::fs::remove_dir_all(&rdir);

    // Create a mock server that sends a malformed/corrupted WAL chunk
    let mock_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let mock_port = mock_listener.local_addr().unwrap().port();

    let rdb = Arc::new(Database::open(&rdir).unwrap());
    rdb.set_read_only(true);
    let (rt, rdrain) = spawn_replica(&rdb, &format!("127.0.0.1:{mock_port}"), &rdir);

    // Mock primary accepts connection and performs handshake
    let mock_thread = std::thread::spawn(move || {
        if let Ok((mut stream, _)) = mock_listener.accept() {
            // Read Handshake
            let _ = super::protocol::read_frame(&mut stream);
            // Send HandshakeAck
            let _ = write_frame(&mut stream, &Frame::HandshakeAck {
                ok: true,
                message: "streaming".into(),
                wal_version: engine::wal::WAL_FORMAT_VERSION,
                durable_offset: 100,
            });
            // Read StartReplication
            let _ = super::protocol::read_frame(&mut stream);
            // Send a corrupt WAL chunk: invalid CRC payload
            let corrupt_data = vec![0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04];
            let _ = write_frame(&mut stream, &Frame::WalChunk {
                offset: 8,
                data: corrupt_data,
            });
            let _ = stream.flush();
        }
    });

    // The replica must detect the bad WAL chunk and disconnect gracefully without panicking
    wait_for("replica disconnected on corrupt chunk", Duration::from_secs(15), || {
        status_val(&rdb, "Rpl_replica_status") == "DISCONNECTED"
    });

    mock_thread.join().unwrap();
    rdrain.store(true, Ordering::Relaxed);
    rt.join().unwrap();
    let _ = std::fs::remove_dir_all(&rdir);
}

#[test]
fn replication_network_duplicate_frames_safely_ignored() {
    use super::protocol::{write_frame, Frame};
    use std::io::Write;
    use std::net::TcpListener;

    let base = std::env::temp_dir();
    let pid = std::process::id();
    let rdir = base.join(format!("hdbdup_r_{pid}"));
    let _ = std::fs::remove_dir_all(&rdir);

    let mock_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let mock_port = mock_listener.local_addr().unwrap().port();

    let rdb = Arc::new(Database::open(&rdir).unwrap());
    rdb.set_read_only(true);
    let (rt, rdrain) = spawn_replica(&rdb, &format!("127.0.0.1:{mock_port}"), &rdir);

    let mock_thread = std::thread::spawn(move || {
        if let Ok((mut stream, _)) = mock_listener.accept() {
            let _ = super::protocol::read_frame(&mut stream);
            let _ = write_frame(&mut stream, &Frame::HandshakeAck {
                ok: true,
                message: "streaming".into(),
                wal_version: engine::wal::WAL_FORMAT_VERSION,
                durable_offset: 200,
            });
            let _ = super::protocol::read_frame(&mut stream);

            // Send a valid snapshot or heartbeat frame first
            let _ = write_frame(&mut stream, &Frame::Heartbeat { durable_offset: 200 });
            // Send duplicate/older WAL offset chunks (network reordering/duplication)
            let _ = write_frame(&mut stream, &Frame::WalChunk {
                offset: 0,
                data: vec![],
            });
            let _ = write_frame(&mut stream, &Frame::WalChunk {
                offset: 0,
                data: vec![],
            });
            let _ = stream.flush();
            std::thread::sleep(Duration::from_millis(50));
        }
    });

    std::thread::sleep(Duration::from_millis(200));
    // Verify replica remains operational and clean
    assert!(rdb.is_read_only());

    mock_thread.join().unwrap();
    rdrain.store(true, Ordering::Relaxed);
    rt.join().unwrap();
    let _ = std::fs::remove_dir_all(&rdir);
}

#[test]
fn primary_crash_mid_stream_and_replica_reconnect_resumes() {
    let base = std::env::temp_dir();
    let pid = std::process::id();
    let pdir = base.join(format!("hdbpcm_p_{pid}"));
    let rdir = base.join(format!("hdbpcm_r_{pid}"));
    let _ = std::fs::remove_dir_all(&pdir);
    let _ = std::fs::remove_dir_all(&rdir);

    let pdb = primary_db(&pdir);
    {
        let mut s = pdb.new_session();
        db_execute(&pdb, &mut s, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    }
    load_rows(&pdb, "t", 0, 50);

    let (pl, port) = primary::bind_repl(0).expect("bind repl");
    let draining_p = Arc::new(AtomicBool::new(false));
    let global_p = leak_flag();
    let pdb2 = pdb.clone();
    let draining_p2 = draining_p.clone();
    let auth_path = pdir.join("auth.bin");
    let pt = std::thread::spawn(move || {
        primary::serve_primary(pdb2, pl, auth_path, draining_p2, global_p)
    });

    let rdb = Arc::new(Database::open(&rdir).unwrap());
    rdb.set_read_only(true);
    let (rt, rdrain) = spawn_replica(&rdb, &format!("127.0.0.1:{port}"), &rdir);

    wait_for("replica sync initial 50", Duration::from_secs(20), || {
        count(&rdb, "t") == 50
    });

    // 1. Primary crashes mid-stream (kill listener & thread abruptly)
    draining_p.store(true, Ordering::Relaxed);
    pt.join().unwrap();

    // 2. Replica enters DISCONNECTED state without crashing or corrupting local data
    wait_for("replica disconnect after primary crash", Duration::from_secs(15), || {
        status_val(&rdb, "Rpl_replica_status") == "DISCONNECTED"
    });
    assert_eq!(count(&rdb, "t"), 50);

    // 3. Primary restarts on same port, writes 50 more rows
    load_rows(&pdb, "t", 50, 50);
    assert_eq!(count(&pdb, "t"), 100);

    let (pl2, _) = std::net::TcpListener::bind(("127.0.0.1", port))
        .map(|l| (l, port))
        .expect("rebind primary port");
    let draining_p_new = Arc::new(AtomicBool::new(false));
    let global_p_new = leak_flag();
    let pdb3 = pdb.clone();
    let draining_p_new2 = draining_p_new.clone();
    let auth_path2 = pdir.join("auth.bin");
    let pt2 = std::thread::spawn(move || {
        primary::serve_primary(pdb3, pl2, auth_path2, draining_p_new2, global_p_new)
    });

    // 4. Replica automatically reconnects, resumes streaming, reaches 100 rows
    wait_for("replica reconnect and reach 100", Duration::from_secs(25), || {
        count(&rdb, "t") == 100
    });

    draining_p_new.store(true, Ordering::Relaxed);
    pt2.join().unwrap();
    rdrain.store(true, Ordering::Relaxed);
    rt.join().unwrap();
    let _ = std::fs::remove_dir_all(&pdir);
    let _ = std::fs::remove_dir_all(&rdir);
}

#[test]
fn replica_persists_to_local_wal_and_offline_promotion_preserves_data() {
    let pdir = std::env::temp_dir().join(format!("hdbrepl_prom_p_{}", std::process::id()));
    let rdir = std::env::temp_dir().join(format!("hdbrepl_prom_r_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&pdir);
    let _ = std::fs::remove_dir_all(&rdir);

    let pdb = primary_db(&pdir);
    let mut ps = pdb.new_session();
    pdb.execute(&mut ps, "CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    load_rows(&pdb, "t", 0, 30);

    let (pl, port) = primary::bind_repl(0).expect("bind repl");
    let draining_p = Arc::new(AtomicBool::new(false));
    let global_p = leak_flag();
    let pdb2 = pdb.clone();
    let draining_p2 = draining_p.clone();
    let auth_path = pdir.join("auth.bin");
    let pt = std::thread::spawn(move || {
        primary::serve_primary(pdb2, pl, auth_path, draining_p2, global_p)
    });

    let rdb = Arc::new(Database::open(&rdir).unwrap());
    rdb.set_read_only(true);
    let (rt, rdrain) = spawn_replica(&rdb, &format!("127.0.0.1:{port}"), &rdir);

    wait_for("replica catches up to 30", Duration::from_secs(15), || {
        count(&rdb, "t") == 30
    });

    // Stream 20 more rows via replication
    load_rows(&pdb, "t", 30, 20);
    wait_for("replica streams 20 more rows", Duration::from_secs(15), || {
        count(&rdb, "t") == 50
    });

    // Stop replica thread
    rdrain.store(true, Ordering::Relaxed);
    rt.join().unwrap();
    draining_p.store(true, Ordering::Relaxed);
    pt.join().unwrap();

    drop(rdb);
    drop(pdb);

    // Promote offline: open rdir directly without running server
    let promoted_db = Database::open(&rdir).unwrap();
    assert_eq!(count(&promoted_db, "t"), 50);
    promoted_db.promote_offline().unwrap();

    // Now write to promoted node as primary
    let mut s = promoted_db.new_session();
    promoted_db.execute(&mut s, "INSERT INTO t VALUES (999, 999)").unwrap();
    assert_eq!(count(&promoted_db, "t"), 51);

    drop(promoted_db);

    // Reopen promoted directory to verify durability across restart
    let reopened = Database::open(&rdir).unwrap();
    assert_eq!(count(&reopened, "t"), 51);

    let _ = std::fs::remove_dir_all(&pdir);
    let _ = std::fs::remove_dir_all(&rdir);
}

