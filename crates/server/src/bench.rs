//! In-process micro-benchmarks and client benchmark routines.

use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use engine::{self, Database};

use crate::arg_value;
use crate::help::banner;
use crate::mock_innodb;

/// In-process group-commit probe: N threads doing single-row autocommit
/// INSERTs (durable), no client/network. Prints throughput + syncer batches.
pub fn bench_gc(dir: &Path, threads: usize, per_thread: u64) -> engine::Result<()> {
    banner();
    if dir.exists() {
        std::fs::remove_dir_all(dir)?;
    }
    let db = Arc::new(Database::open(dir)?);
    {
        let mut s = db.new_session();
        db.execute(&mut s, "CREATE TABLE gc (id INT PRIMARY KEY, v INT)")?;
    }
    let t0 = Instant::now();
    let mut handles = vec![];
    for w in 0..threads {
        let db = db.clone();
        handles.push(std::thread::spawn(move || {
            let mut s = db.new_session();
            for i in 0..per_thread {
                let id = (w as u64) * per_thread + i;
                db.execute(&mut s, &format!("INSERT INTO gc VALUES ({}, {})", id, id))
                    .unwrap();
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    let dt = t0.elapsed();
    let total = threads as u64 * per_thread;
    let (syncs, bytes) = db.sync_stats();
    println!(
        "gc-bench: {} threads, {} durable commits in {:.3}s = {:.0} commits/s",
        threads,
        total,
        dt.as_secs_f64(),
        total as f64 / dt.as_secs_f64()
    );
    if syncs > 0 {
        println!("  syncer: {} syncs, avg batch {:.1} commits/sync", syncs, bytes as f64 / syncs as f64 / 40.0);
    }
    Ok(())
}

/// Architectural comparison: henchDB's OLC B+ tree vs the mock InnoDB-style
/// data path (buffer-hash translation + pessimistic latches + global mutexes
/// + doublewrite), in-process, identical workload.
pub fn bench_mock(threads: usize, n_keys: u64, per_thread: u64) -> engine::Result<()> {
    banner();
    let mut rng_state = 0x9E3779B97F4A7C15u64;
    let mut _next_key = move || {
        rng_state ^= rng_state << 13;
        rng_state ^= rng_state >> 7;
        rng_state ^= rng_state << 17;
        rng_state % n_keys
    };

    // -- point select --
    let mock = Arc::new(mock_innodb::MockInnoDB::new(n_keys));
    let tree = Arc::new(engine::btree::BTree::new());
    for k in 0..n_keys {
        let kb = k.to_be_bytes();
        tree.insert(&kb, &kb);
    }

    let run_mt = |make: &dyn Fn() -> Arc<dyn Fn(u64) + Send + Sync>| -> f64 {
        let t0 = Instant::now();
        let mut handles = vec![];
        for _ in 0..threads {
            let f = make();
            handles.push(std::thread::spawn(move || {
                let mut st = 0x9E3779B97F4A7C15u64 ^ std::process::id() as u64;
                for _ in 0..per_thread {
                    st ^= st << 13;
                    st ^= st >> 7;
                    st ^= st << 17;
                    f(st % n_keys);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        t0.elapsed().as_secs_f64()
    };

    let total = (threads as u64 * per_thread) as f64;

    let mock_point = run_mt(&|| {
        let m = mock.clone();
        Arc::new(move |k| {
            let _ = m.point_select(k);
        })
    });
    let hench_point = run_mt(&|| {
        let t = tree.clone();
        Arc::new(move |k| {
            let kb = k.to_be_bytes();
            let _ = t.get(&kb);
        })
    });

    let mock_upd = run_mt(&|| {
        let m = mock.clone();
        Arc::new(move |k| m.update_index(k))
    });
    let hench_upd = run_mt(&|| {
        let t = tree.clone();
        let db_tree = t.clone();
        Arc::new(move |k| {
            let kb = k.to_be_bytes();
            let _ = db_tree.upsert(&kb, &kb);
        })
    });

    println!("=== architectural mock comparison ({} threads, {} keys) ===", threads, n_keys);
    println!(
        "{:>34} {:>14} {:>14} {:>8}",
        "workload", "mock InnoDB", "henchDB OLC", "ratio"
    );
    println!(
        "{:>34} {:>14.0} {:>14.0} {:>7.2}x",
        "point_select (ops/s)",
        total / mock_point,
        total / hench_point,
        mock_point / hench_point
    );
    println!(
        "{:>34} {:>14.0} {:>14.0} {:>7.2}x",
        "update_index (ops/s)",
        total / mock_upd,
        total / hench_upd,
        mock_upd / hench_upd
    );
    let (translations, lsn) = mock.stats();
    println!(
        "mock counters: {} buffer-pool translations, {} redo bytes",
        translations, lsn
    );
    Ok(())
}

/// Compiled-client benchmark for henchDB: minimal length-prefixed TCP client,
/// matching the overhead class of the mysql.exe CLI used on the MySQL side.
pub fn client_bench(args: &[String]) -> engine::Result<()> {
    use std::io::{Read, Write};
    let host = arg_value(args, "--host").unwrap_or_else(|| "127.0.0.1".into());
    let port: u16 = arg_value(args, "--port").and_then(|p| p.parse().ok()).unwrap_or(3308);
    let threads: usize = arg_value(args, "--threads").and_then(|t| t.parse().ok()).unwrap_or(1);
    let ops: u64 = arg_value(args, "--ops").and_then(|o| o.parse().ok()).unwrap_or(50_000);
    let mode = arg_value(args, "--mode").unwrap_or_else(|| "point".into());
    let rows: u64 = arg_value(args, "--rows").and_then(|r| r.parse().ok()).unwrap_or(50_000);

    let mut handles = vec![];
    let t0 = Instant::now();
    for w in 0..threads {
        let host = host.clone();
        let mode = mode.clone();
        handles.push(std::thread::spawn(move || -> std::io::Result<()> {
            let mut sock = std::net::TcpStream::connect((host.as_str(), port))?;
            sock.set_nodelay(true)?;
            let mut buf = Vec::with_capacity(256);
            let mut req = Vec::with_capacity(128);
            let mut st = 0x9E3779B97F4A7C15u64 ^ (w as u64 + 1) * 0x1000193;
            for _ in 0..ops {
                st ^= st << 13;
                st ^= st >> 7;
                st ^= st << 17;
                let k = st % rows;
                let one = |sock: &mut std::net::TcpStream,
                                sql: String,
                                buf: &mut Vec<u8>,
                                req: &mut Vec<u8>|
                 -> std::io::Result<()> {
                    let b = sql.as_bytes();
                    req.clear();
                    req.extend_from_slice(&(b.len() as u32).to_be_bytes());
                    req.extend_from_slice(b);
                    sock.write_all(req)?;
                    let mut hdr = [0u8; 4];
                    sock.read_exact(&mut hdr)?;
                    let n = u32::from_be_bytes(hdr) as usize;
                    buf.resize(n, 0);
                    sock.read_exact(buf)?;
                    if buf.starts_with(b"ERR") {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            String::from_utf8_lossy(buf).into_owned(),
                        ));
                    }
                    Ok(())
                };
                if mode == "txn" {
                    // 10 point selects + 1 durable update per transaction,
                    // matching the MySQL-side script.
                    one(&mut sock, "BEGIN".into(), &mut buf, &mut req)?;
                    for _ in 0..10 {
                        let k2 = {
                            st ^= st << 13;
                            st ^= st >> 7;
                            st ^= st << 17;
                            st % rows
                        };
                        one(&mut sock, format!("SELECT v FROM bench WHERE id = {}", k2), &mut buf, &mut req)?;
                    }
                    one(&mut sock, format!("UPDATE bench SET v = {} WHERE id = {}", k, k), &mut buf, &mut req)?;
                    one(&mut sock, "COMMIT".into(), &mut buf, &mut req)?;
                } else if mode == "update" {
                    one(&mut sock, format!("UPDATE bench SET v = {} WHERE id = {}", k, k), &mut buf, &mut req)?;
                } else {
                    one(&mut sock, format!("SELECT v FROM bench WHERE id = {}", k), &mut buf, &mut req)?;
                }
            }
            Ok(())
        }));
    }
    for h in handles {
        h.join().unwrap()?;
    }
    let dt = t0.elapsed();
    println!(
        "henchDB clientbench ({} compiled client(s)): {} {} ops in {:.3}s = {:.0} ops/s",
        threads,
        threads as u64 * ops,
        mode,
        dt.as_secs_f64(),
        (threads as u64 * ops) as f64 / dt.as_secs_f64()
    );
    Ok(())
}
