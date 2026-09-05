//! Server binary for the database engine.
//!
//! Modes:
//!   server                 interactive SQL shell (embedded database in ./data)
//!   server serve           TCP server (length-prefixed text protocol)
//!   server bench           in-process OLTP micro-benchmark
//!
//! The v0.1 server is thread-per-connection over blocking TCP — portable and
//! correct. The research doc's pinned thread-per-core runtime with io_uring
//! is the Linux roadmap item (see agents.md); session handling is already
//! isolated per connection so the runtime can be swapped without touching
//! the engine.

use std::io::{BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use engine::{Database, Datum, Output, PRODUCT_NAME, PRODUCT_TAGLINE, VERSION};

mod auth;
mod mock_innodb;
mod wire;

/// Global shutdown flag: set by SIGINT/SIGTERM handlers and COM_SHUTDOWN.
/// The accept loop polls it; connection threads observe it per command.
pub static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

/// Runtime policy for `serve` (SEC1 connection governance).
#[derive(Debug, Clone)]
struct ServerOpts {
    port: u16,
    max_connections: usize,
    idle_timeout: Option<Duration>,
    allow_legacy: bool,
    /// PEM certificate chain for TLS (SEC2). Both must be set to enable.
    tls_cert: Option<String>,
    tls_key: Option<String>,
    /// PostgreSQL wire port (PG1, 0 = disabled).
    pg_port: u16,
    allow_pg: bool,
}

impl ServerOpts {
    fn from_args(args: &[String]) -> Self {
        let port: u16 = arg_value(args, "--port").and_then(|p| p.parse().ok()).unwrap_or(3307);
        let max_connections: usize = arg_value(args, "--max-connections")
            .and_then(|m| m.parse().ok())
            .unwrap_or(200);
        let idle_timeout: Option<Duration> = match arg_value(args, "--idle-timeout") {
            Some(s) => s.parse::<u64>().ok().map(Duration::from_secs),
            None => Some(Duration::from_secs(28_800)), // MySQL wait_timeout default
        };
        let allow_legacy = !args.iter().any(|a| a == "--no-legacy");
        let allow_pg = !args.iter().any(|a| a == "--no-pg");
        ServerOpts {
            port,
            max_connections,
            idle_timeout,
            allow_legacy,
            tls_cert: arg_value(args, "--tls-cert"),
            tls_key: arg_value(args, "--tls-key"),
            pg_port: arg_value(args, "--pg-port")
                .and_then(|p| p.parse().ok())
                .unwrap_or(5432),
            allow_pg,
        }
    }
}

/// Live-connection registry so shutdown can wake blocked readers.
#[derive(Clone, Default)]
struct ConnRegistry {
    inner: Arc<Mutex<RegistryInner>>,
}

#[derive(Default)]
struct RegistryInner {
    next: u64,
    socks: std::collections::HashMap<u64, TcpStream>,
}

impl ConnRegistry {
    fn add(&self, sock: &TcpStream) -> u64 {
        let mut g = self.inner.lock().unwrap();
        g.next += 1;
        let id = g.next;
        if let Ok(clone) = sock.try_clone() {
            g.socks.insert(id, clone);
        }
        id
    }
    fn remove(&self, id: u64) {
        self.inner.lock().unwrap().socks.remove(&id);
    }
    fn shutdown_all(&self) {
        let g = self.inner.lock().unwrap();
        for s in g.socks.values() {
            let _ = s.shutdown(std::net::Shutdown::Both);
        }
    }
    fn len(&self) -> usize {
        self.inner.lock().unwrap().socks.len()
    }
}

/// One counted connection slot; released back to the pool on drop.
struct ConnGuard {
    active: Arc<Mutex<usize>>,
}

impl Drop for ConnGuard {
    fn drop(&mut self) {
        let mut g = self.active.lock().unwrap();
        *g = g.saturating_sub(1);
    }
}

// ---------------------------------------------------------------------------
// OS signal handling (zero-dependency FFI, cfg-gated per platform)
// ---------------------------------------------------------------------------

/// Install SIGINT/SIGTERM handlers that only set the global shutdown flag.
///
/// SAFETY: the C handlers below perform no allocation, locking, or I/O —
/// they store `true` to a static `AtomicBool` with `Relaxed` ordering, which
/// is async-signal-safe on every supported platform. Registration itself runs
/// once on the main thread before serving. No Rust state is touched.
#[cfg(unix)]
fn install_signal_handlers() {
    extern "C" fn on_signal(_sig: std::os::raw::c_int) {
        SHUTDOWN_REQUESTED.store(true, Ordering::Relaxed);
    }
    extern "C" {
        fn signal(sig: std::os::raw::c_int, handler: extern "C" fn(std::os::raw::c_int)) -> usize;
    }
    const SIGINT: std::os::raw::c_int = 2;
    const SIGTERM: std::os::raw::c_int = 15;
    // SAFETY: see function-level argument; `signal` with BSD semantics keeps
    // the handler installed (Linux glibc, macOS/*BSD libc).
    unsafe {
        // SIG_ERR is (void*)-1; any other return means installed.
        if signal(SIGINT, on_signal) == usize::MAX {
            eprintln!("warning: could not arm SIGINT handler");
        }
        if signal(SIGTERM, on_signal) == usize::MAX {
            eprintln!("warning: could not arm SIGTERM handler");
        }
    }
}

/// Windows console/shutdown handler: same single-flag contract as unix.
///
/// SAFETY: identical argument — the callback only stores to the static flag
/// and returns TRUE (handled). `SetConsoleCtrlHandler` is process-wide and
/// installed once from the main thread.
#[cfg(windows)]
fn install_signal_handlers() {
    extern "system" fn ctrl_handler(_ctrl: u32) -> i32 {
        SHUTDOWN_REQUESTED.store(true, Ordering::Relaxed);
        1 // TRUE: handled, do not chain
    }
    extern "system" {
        fn SetConsoleCtrlHandler(handler: Option<extern "system" fn(u32) -> i32>, add: i32) -> i32;
    }
    // SAFETY: see function-level argument.
    unsafe {
        if SetConsoleCtrlHandler(Some(ctrl_handler), 1) == 0 {
            eprintln!("warning: could not arm console-control handler");
        }
    }
}

#[cfg(not(any(unix, windows)))]
fn install_signal_handlers() {
    // No portable trap here; COM_SHUTDOWN still stops the server gracefully.
}

/// Set (or reset) a user's password in `<dir>/auth.bin`.
fn passwd_cmd(dir: &Path, args: &[String]) -> engine::Result<()> {
    let user = arg_value(args, "--user").ok_or_else(|| {
        engine::Error::Io("passwd: --user is required".into())
    })?;
    let plugin = match arg_value(args, "--plugin").as_deref() {
        None | Some("sha2") | Some("caching_sha2_password") => auth::PLUGIN_CACHING_SHA2,
        Some("native") | Some("mysql_native_password") => auth::PLUGIN_NATIVE,
        Some(other) => {
            return Err(engine::Error::Io(format!("passwd: unknown plugin '{other}' (sha2|native)")));
        }
    };
    // Prefer --password; otherwise read one line from stdin (piping avoids
    // shell history; there is no TTY echo control in std).
    let password = match arg_value(args, "--password") {
        Some(p) => p,
        None => {
            eprint!("password for '{user}': ");
            use std::io::BufRead;
            let mut line = String::new();
            std::io::stdin()
                .lock()
                .read_line(&mut line)
                .map_err(|e| engine::Error::Io(format!("passwd: stdin: {e}")))?;
            line.trim_end_matches(['\r', '\n']).to_string()
        }
    };
    let path = dir.join("auth.bin");
    std::fs::create_dir_all(dir)?;
    let (mut store, _) =
        auth::UserStore::load_or_bootstrap(&path).map_err(engine::Error::Io)?;
    store.set_password(&user, password.as_bytes(), plugin).map_err(engine::Error::Io)?;
    println!("password set for '{user}' ({plugin})");
    Ok(())
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let dir = arg_value(&args, "--dir").unwrap_or_else(|| "data".to_string());
    let result = match args.first().map(String::as_str) {
        Some("serve") => {
            let opts = ServerOpts::from_args(&args);
            serve(Path::new(&dir), opts)
        }
        Some("passwd") => passwd_cmd(Path::new(&dir), &args),
        Some("gcbench") => {
            let threads: usize = arg_value(&args, "--threads")
                .and_then(|t| t.parse().ok())
                .unwrap_or(8);
            bench_gc(Path::new(&dir), threads, 2_000)
        }
        Some("clientbench") => client_bench(&args),
        Some("benchmock") => {
            let threads: usize = arg_value(&args, "--threads")
                .and_then(|t| t.parse().ok())
                .unwrap_or(4);
            bench_mock(threads, 100_000, 500_000)
        }
        Some("bench") => {
            let rows: u64 = arg_value(&args, "--rows")
                .and_then(|r| r.parse().ok())
                .unwrap_or(50_000);
            bench(Path::new(&dir), rows)
        }
        Some(other) if !other.starts_with('-') => {
            eprintln!("unknown command '{other}'");
            eprintln!("usage: server [serve|passwd|bench] [--dir data] [--port 3307] [--rows 50000]");
            eprintln!("  serve --max-connections 200 --idle-timeout 28800 [--no-legacy] [--tls-cert cert.pem --tls-key key.pem] [--pg-port 5432|--no-pg]");
            eprintln!("  passwd --user root --password <pw> [--plugin sha2|native]  (omit --password to read stdin)");
            std::process::exit(2);
        }
        _ => shell(Path::new(&dir)),
    };
    if let Err(e) = result {
        eprintln!("fatal: {e}");
        std::process::exit(1);
    }
}

/// In-process group-commit probe: N threads doing single-row autocommit
/// INSERTs (durable), no client/network. Prints throughput + syncer batches.
fn bench_gc(dir: &Path, threads: usize, per_thread: u64) -> engine::Result<()> {
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
fn bench_mock(threads: usize, n_keys: u64, per_thread: u64) -> engine::Result<()> {
    use std::sync::Arc;

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
fn client_bench(args: &[String]) -> engine::Result<()> {
    use std::io::{Read, Write};
    let host = arg_value(args, "--host").unwrap_or_else(|| "127.0.0.1".into());
    let port: u16 = arg_value(&args, "--port").and_then(|p| p.parse().ok()).unwrap_or(3308);
    let threads: usize = arg_value(&args, "--threads").and_then(|t| t.parse().ok()).unwrap_or(1);
    let ops: u64 = arg_value(&args, "--ops").and_then(|o| o.parse().ok()).unwrap_or(50_000);
    let mode = arg_value(&args, "--mode").unwrap_or_else(|| "point".into());
    let rows: u64 = arg_value(&args, "--rows").and_then(|r| r.parse().ok()).unwrap_or(50_000);

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

fn arg_value(args: &[String], flag: &str) -> Option<String> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

fn banner() {
    println!("{PRODUCT_NAME} v{VERSION} — {PRODUCT_TAGLINE}");
}

// ---------------------------------------------------------------------------
// Interactive shell
// ---------------------------------------------------------------------------

fn shell(dir: &Path) -> engine::Result<()> {
    banner();
    let db = Database::open(dir)?;
    let mut session = db.new_session();
    println!("embedded mode. type SQL, or .help / .quit");
    let stdin = std::io::stdin();
    let mut line = String::new();
    loop {
        print!("db> ");
        std::io::stdout().flush()?;
        line.clear();
        if stdin.read_line(&mut line)? == 0 {
            break; // EOF
        }
        let trimmed = line.trim();
        match trimmed {
            "" => continue,
            ".quit" | ".exit" => break,
            ".help" => {
                println!("SQL: CREATE/INSERT/SELECT/UPDATE/DELETE/BEGIN/COMMIT/ROLLBACK");
                println!("     SHOW TABLES | CHECKPOINT | DROP TABLE t");
                println!("meta: .help .quit");
                continue;
            }
            _ => {}
        }
        match db.execute(&mut session, trimmed) {
            Ok(out) => print_output(&out),
            Err(e) => println!("error: {e}"),
        }
    }
    println!("checkpointing...");
    db.checkpoint()?;
    Ok(())
}

fn print_output(out: &Output) {
    if !out.message.is_empty() && out.rows.is_empty() {
        println!("{}", out.message);
        return;
    }
    let widths: Vec<usize> = out
        .columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            out.rows
                .iter()
                .map(|r| r[i].to_string().len())
                .max()
                .unwrap_or(0)
                .max(c.len())
        })
        .collect();
    let sep = |w: &[usize]| {
        let s: String = w.iter().map(|x| "-".repeat(x + 2) + "+").collect();
        format!("+{s}")
    };
    let row_str = |vals: &[String], w: &[usize]| {
        vals.iter()
            .zip(w)
            .map(|(v, x)| format!(" {v:>x$} "))
            .collect::<Vec<_>>()
            .join("|")
    };
    let headers: Vec<String> = out.columns.clone();
    println!("{}", sep(&widths));
    println!("|{}|", row_str(&headers, &widths));
    println!("{}", sep(&widths));
    for r in &out.rows {
        let vals: Vec<String> = r.iter().map(|d| d.to_string()).collect();
        println!("|{}|", row_str(&vals, &widths));
    }
    println!("{}", sep(&widths));
    println!("{} row(s)", out.rows.len());
}

// ---------------------------------------------------------------------------
// TCP server
// ---------------------------------------------------------------------------

/// Length-prefixed text protocol (legacy, used by clientbench/benches):
///   request:  [u32 BE len][utf8 sql]
///   response: [u32 BE len][utf8 payload]
/// Payload: "ERR <msg>" or "OK\n<tab-separated columns>\n<tab-separated rows>"
/// with NULL encoded as the two-character sequence \N.
///
/// The same port also speaks the MySQL client wire protocol
/// (HandshakeV10 + COM_QUERY/COM_PING/COM_QUIT/COM_INIT_DB plus binary
/// prepared statements COM_STMT_PREPARE/EXECUTE/CLOSE/RESET, see wire.rs)
/// with automatic sniffing: legacy clients send bytes immediately while
/// MySQL clients wait for the server handshake.
fn serve(dir: &Path, opts: ServerOpts) -> engine::Result<()> {
    banner();
    std::fs::create_dir_all(dir)?;
    // Fail closed when the auth store cannot bootstrap.
    let auth_path = dir.join("auth.bin");
    auth::UserStore::load_or_bootstrap(&auth_path).map_err(engine::Error::Io)?;
    let db = Arc::new(Database::open(dir)?);
    install_signal_handlers();
    // SEC2: TLS is all-or-nothing at startup — one side without the other
    // is a misconfiguration, and unreadable files must never silently
    // downgrade to plaintext.
    let tls = match (&opts.tls_cert, &opts.tls_key) {
        (Some(cert), Some(key)) => Some(
            wire::tls::load_tls_config(Path::new(cert), Path::new(key))
                .map_err(|e| engine::Error::Io(e.to_string()))?,
        ),
        (None, None) => None,
        _ => {
            eprintln!("tls error: --tls-cert and --tls-key must be given together");
            std::process::exit(2);
        }
    };
    let listener = TcpListener::bind(("0.0.0.0", opts.port))?;
    listener.set_nonblocking(true)?;
    println!("listening on 0.0.0.0:{} (dir: {})", opts.port, dir.display());
    println!("protocols: mysql wire (HandshakeV10 + COM_QUERY + prepared statements, authenticated) + legacy framed text (auto-detect)");
    println!("tls: {}",
        if tls.is_some() { "enabled (CLIENT_SSL advertised)" } else { "disabled (plaintext only)" });
    println!("limits: max_connections={} idle_timeout={} legacy={}",
        opts.max_connections,
        opts.idle_timeout.map(|d| format!("{}s", d.as_secs())).unwrap_or_else(|| "off".into()),
        if opts.allow_legacy { "on" } else { "off" });
    println!("connect: mysql -h 127.0.0.1 -P {} -u root", opts.port);
    let active = Arc::new(Mutex::new(0usize));
    let registry = ConnRegistry::default();
    // One process-wide drain flag shared by every connection: COM_SHUTDOWN
    // sets it from any wire thread; signals set the static below, which the
    // accept loop merges in.
    let draining = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::new();
    // PG1: dedicated PostgreSQL listener (own port, no sniffing). Tries the
    // configured port, then +1..+8 (a local Postgres often owns 5432);
    // giving up only disables PG, never the MySQL side. `--no-pg` or
    // `--pg-port 0` disables it outright.
    if opts.allow_pg && opts.pg_port != 0 {
        let mut bound = None;
        for port in opts.pg_port..opts.pg_port.saturating_add(9) {
            match TcpListener::bind(("0.0.0.0", port)) {
                Ok(l) => {
                    bound = Some((l, port));
                    break;
                }
                Err(_) => continue,
            }
        }
        match bound {
            Some((pg_listener, pg_port)) => {
                if pg_port != opts.pg_port {
                    eprintln!("pg: port {} busy, listening on {pg_port} instead", opts.pg_port);
                }
                println!("pg listening on 0.0.0.0:{pg_port} (protocol 3.0 simple query)");
                println!("connect: psql -h 127.0.0.1 -p {pg_port} -U root");
                let _ = pg_listener.set_nonblocking(true);
                let pg = std::thread::spawn({
                    let db = db.clone();
                    let opts = opts.clone();
                    let tls = tls.clone();
                    let auth_path = auth_path.clone();
                    let active = active.clone();
                    let registry = registry.clone();
                    let draining = draining.clone();
                    move || {
                        while !SHUTDOWN_REQUESTED.load(Ordering::Relaxed)
                            && !draining.load(Ordering::Relaxed)
                        {
                            match pg_listener.accept() {
                                Ok((stream, _)) => {
                                    if let Err(e) = stream.set_nonblocking(false) {
                                        eprintln!("pg accept error: {e}");
                                        continue;
                                    }
                                    let admitted = {
                                        let mut n = active.lock().unwrap();
                                        if *n >= opts.max_connections.max(1) {
                                            false
                                        } else {
                                            *n += 1;
                                            true
                                        }
                                    };
                                    let guard = ConnGuard { active: active.clone() };
                                    let reg_id = registry.add(&stream);
                                    let db = db.clone();
                                    let opts = opts.clone();
                                    let tls = tls.clone();
                                    let auth_path = auth_path.clone();
                                    let registry = registry.clone();
                                    let draining = draining.clone();
                                    std::thread::spawn(move || {
                                        let _guard = guard;
                                        let ctx = wire::ConnCtx {
                                            auth_path,
                                            idle_timeout: opts.idle_timeout,
                                            shutdown: draining,
                                            admitted,
                                            tls,
                                        };
                                        let r = wire::pg::handle_pg_connection(db, stream, &ctx);
                                        registry.remove(reg_id);
                                        if let Err(e) = r {
                                            eprintln!("pg connection error: {e}");
                                        }
                                    });
                                }
                                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                    std::thread::sleep(Duration::from_millis(100));
                                }
                                Err(e) => eprintln!("pg accept error: {e}"),
                            }
                        }
                    }
                });
                handles.push(pg);
            }
            None => eprintln!("pg: ports {}-{} all busy, postgresql wire disabled", opts.pg_port, opts.pg_port.saturating_add(8)),
        }
    }
    // Nonblocking accept so SIGINT/SIGTERM and COM_SHUTDOWN are honored
    // within ~100ms even with zero traffic.
    while !SHUTDOWN_REQUESTED.load(Ordering::Relaxed) && !draining.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((stream, _)) => {
                // The listener is nonblocking (shutdown polling); accepted
                // sockets inherit that on some platforms, so force blocking
                // I/O back immediately — timeouts are managed per connection.
                if let Err(e) = stream.set_nonblocking(false) {
                    eprintln!("accept error: {e}");
                    continue;
                }
                // Counted slot: a full pool still serves handshake + ERR 1040.
                let admitted = {
                    let mut n = active.lock().unwrap();
                    if *n >= opts.max_connections.max(1) {
                        false
                    } else {
                        *n += 1;
                        true
                    }
                };
                let guard = ConnGuard { active: active.clone() };
                let reg_id = registry.add(&stream);
                let db = db.clone();
                let opts = opts.clone();
                let tls = tls.clone();
                let auth_path = auth_path.clone();
                let registry = registry.clone();
                let draining = draining.clone();
                handles.push(std::thread::spawn(move || {
                    let _guard = guard; // slot released when the thread ends
                    let ctx = wire::ConnCtx {
                        auth_path,
                        idle_timeout: opts.idle_timeout,
                        shutdown: draining,
                        admitted,
                        tls,
                    };
                    let r = handle_auto(db, stream, &opts, &ctx);
                    registry.remove(reg_id);
                    if let Err(e) = r {
                        eprintln!("connection error: {e}");
                    }
                }));
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(e) => eprintln!("accept error: {e}"),
        }
    }
    // Graceful drain: no new connections, wake blocked readers, let
    // in-flight queries finish, then checkpoint everything they committed.
    println!("shutting down ({} live connections)...", registry.len());
    drop(listener);
    registry.shutdown_all();
    for h in handles {
        let _ = h.join();
    }
    println!("checkpointing...");
    db.checkpoint()?;
    println!("clean shutdown");
    Ok(())
}

/// Sniff the protocol: legacy framed clients push bytes immediately;
/// MySQL clients wait silently for the server handshake.
fn handle_auto(
    db: Arc<Database>,
    stream: TcpStream,
    opts: &ServerOpts,
    ctx: &wire::ConnCtx,
) -> std::io::Result<()> {
    use std::time::Duration;
    stream.set_read_timeout(Some(Duration::from_millis(200)))?;
    let mut probe = [0u8; 1];
    match stream.peek(&mut probe) {
        Ok(n) if n > 0 => {
            let _ = stream.set_read_timeout(None);
            if !opts.allow_legacy {
                eprintln!("legacy protocol disabled: closing {}", stream.peer_addr()?);
                return Ok(());
            }
            if !ctx.admitted {
                // Counted out: one framed ERR, then close.
                let mut w = stream.try_clone()?;
                let err = b"ERR too many connections";
                let mut resp = Vec::with_capacity(4 + err.len());
                resp.extend_from_slice(&(err.len() as u32).to_be_bytes());
                resp.extend_from_slice(err);
                w.write_all(&resp)?;
                w.flush()?;
                return Ok(());
            }
            handle_connection(db, stream, opts.idle_timeout)
        }
        Ok(_) => Ok(()), // client closed immediately
        Err(e)
            if e.kind() == std::io::ErrorKind::WouldBlock
                || e.kind() == std::io::ErrorKind::TimedOut =>
        {
            let _ = stream.set_read_timeout(None);
            wire::handle_mysql_connection(db, stream, ctx)
        }
        Err(e) => Err(e),
    }
}

fn handle_connection(
    db: Arc<Database>,
    stream: TcpStream,
    idle_timeout: Option<Duration>,
) -> std::io::Result<()> {
    let peer = stream.peer_addr()?;
    println!("connected: {peer}");
    stream.set_nodelay(true)?;
    let mut writer = stream.try_clone()?;
    writer.set_nodelay(true)?;
    let mut reader = BufReader::new(stream);
    if let Some(d) = idle_timeout {
        let _ = reader.get_mut().set_read_timeout(Some(d));
    }
    let mut session = db.new_session();
    let mut buf = Vec::with_capacity(256);
    let mut resp = Vec::with_capacity(256);
    loop {
        // Read frame: 4-byte BE length + payload.
        let mut hdr = [0u8; 4];
        match reader.read_exact(&mut hdr) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::TimedOut => break, // idle
            Err(_) => break, // client closed
        }
        let len = u32::from_be_bytes(hdr) as usize;
        if len > 16 * 1024 * 1024 {
            break; // protocol guard
        }
        buf.resize(len, 0);
        reader.read_exact(&mut buf)?;
        let sql = match std::str::from_utf8(&buf) {
            Ok(s) => s,
            Err(_) => {
                let err = b"ERR invalid utf-8";
                resp.clear();
                resp.extend_from_slice(&(err.len() as u32).to_be_bytes());
                resp.extend_from_slice(err);
                writer.write_all(&resp)?;
                continue;
            }
        };

        let payload = match db.execute(&mut session, sql.trim()) {
            Ok(out) => format_output(&out),
            Err(e) => format!("ERR {e}"),
        };
        let bytes = payload.as_bytes();
        resp.clear();
        resp.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
        resp.extend_from_slice(bytes);
        writer.write_all(&resp)?;
        writer.flush()?;
    }
    println!("disconnected: {peer}");
    Ok(())
}

fn format_output(out: &Output) -> String {
    if out.rows.is_empty() {
        return format!("OK {}", out.message);
    }
    let mut s = String::from("OK\n");
    s.push_str(&out.columns.join("\t"));
    s.push('\n');
    for r in &out.rows {
        let vals: Vec<String> = r
            .iter()
            .map(|d| match d {
                Datum::Null => "\\N".to_string(),
                other => other.to_string(),
            })
            .collect();
        s.push_str(&vals.join("\t"));
        s.push('\n');
    }
    s
}

// ---------------------------------------------------------------------------
// Micro-benchmark
// ---------------------------------------------------------------------------

fn bench(dir: &Path, rows: u64) -> engine::Result<()> {
    banner();
    // Fresh bench directory so numbers are reproducible.
    if dir.exists() {
        std::fs::remove_dir_all(dir)?;
    }
    let db = Database::open(dir)?;
    let mut s = db.new_session();
    db.execute(&mut s, "CREATE TABLE bench (id INT PRIMARY KEY, v BIGINT, t TEXT NOT NULL)")?;

    let batch = 1_000;
    let t0 = Instant::now();
    let mut done = 0u64;
    while done < rows {
        let mut s = db.new_session();
        db.execute(&mut s, "BEGIN")?;
        let end = (done + batch).min(rows);
        for i in done..end {
            db.execute(
                &mut s,
                &format!("INSERT INTO bench VALUES ({i}, {}, 'row-{i}')", i * 7),
            )?;
        }
        db.execute(&mut s, "COMMIT")?;
        done = end;
    }
    let ins_secs = t0.elapsed().as_secs_f64();
    println!(
        "insert: {rows} rows in {:.3}s = {:.0} rows/s (WAL-synced, batch={batch})",
        ins_secs,
        rows as f64 / ins_secs
    );

    // Point lookups through the full SQL pipeline (parse -> plan -> OLC tree).
    let probes = rows.min(200_000);
    let t1 = Instant::now();
    let mut hits = 0u64;
    for i in 0..probes {
        let mut s = db.new_session();
        let out = db.execute(&mut s, &format!("SELECT v FROM bench WHERE id = {}", i * 7 % rows))?;
        hits += out.rows.len() as u64;
    }
    let sel_secs = t1.elapsed().as_secs_f64();
    println!(
        "point select: {probes} queries in {:.3}s = {:.0} q/s ({hits} rows returned)",
        sel_secs,
        probes as f64 / sel_secs
    );

    // Range scan throughput.
    let t2 = Instant::now();
    let ranges = 1_000u64;
    let mut scanned = 0u64;
    for i in 0..ranges {
        let lo = i * (rows / ranges);
        let hi = lo + rows / ranges / 10;
        let out = db.execute(
            &mut s,
            &format!("SELECT COUNT(*) FROM bench WHERE id >= {lo} AND id < {hi}"),
        )?;
        if let Some(row) = out.rows.first() {
            if let Some(Datum::Int(n)) = row.first() {
                scanned += *n as u64;
            }
        }
    }
    let scan_secs = t2.elapsed().as_secs_f64();
    println!(
        "range scan: {ranges} ranges, {scanned} rows in {:.3}s = {:.0} rows/s",
        scan_secs,
        scanned as f64 / scan_secs
    );

    let t3 = Instant::now();
    db.checkpoint()?;
    println!("checkpoint: {:.3}s", t3.elapsed().as_secs_f64());
    Ok(())
}
