//! Tests for the pool/poller runtime (Priority 11): broker handoffs,
//! idle-connection scaling, exact over-capacity error packets, and drain.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use engine::Database;

use super::{Broker, Conn, ConnState, Origin, Shared};
use crate::ConnRegistry;

static TEST_SEQ: AtomicU64 = AtomicU64::new(0);

fn leak_flag() -> &'static AtomicBool {
    Box::leak(Box::new(AtomicBool::new(false)))
}

fn test_db(tag: &str) -> (Arc<Database>, std::path::PathBuf) {
    let n = TEST_SEQ.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "hdbpool_{tag}_{}_{n}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let db = Database::open(&dir).unwrap();
    crate::auth::UserStore::load_or_bootstrap(&dir.join("auth.bin")).unwrap();
    let mut s = db.new_session();
    db.execute(&mut s, "CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute(&mut s, "INSERT INTO t VALUES (1, 10), (2, 20)").unwrap();
    (Arc::new(db), dir)
}

/// Live runtime: broker + pool + poller over real loopback sockets.
struct Harness {
    broker: Arc<Broker>,
    pool: super::pool::Pool,
    poller: Option<std::thread::JoinHandle<()>>,
    registry: ConnRegistry,
    active: Arc<Mutex<usize>>,
    draining: Arc<AtomicBool>,
    db: Arc<Database>,
    dir: std::path::PathBuf,
}

impl Harness {
    fn start(tag: &str, threads: usize, idle_timeout: Option<Duration>) -> Self {
        let (db, dir) = test_db(tag);
        let broker = Arc::new(Broker::new());
        let registry = ConnRegistry::default();
        let active = Arc::new(Mutex::new(0usize));
        let draining = Arc::new(AtomicBool::new(false));
        let shared = Arc::new(Shared {
            db: db.clone(),
            auth_path: dir.join("auth.bin"),
            idle_timeout,
            shutdown: draining.clone(),
            global: leak_flag(),
            tls: None,
            allow_legacy: true,
        });
        let pool = super::pool::Pool::start(threads, broker.clone(), shared.clone(), registry.clone());
        let poller = {
            let broker = broker.clone();
            let shared = shared.clone();
            let registry = registry.clone();
            let tx = pool.sender().expect("pool running");
            std::thread::Builder::new()
                .name("test-poller".into())
                .spawn(move || super::poller::run_poller(broker, tx, shared, registry))
                .expect("spawn poller")
        };
        Harness { broker, pool, poller: Some(poller), registry, active, draining, db, dir }
    }

    /// Register one accepted socket and queue its handshake job.
    fn admit(&self, stream: TcpStream, origin: Origin, admitted: bool) {
        // Over-capacity conns still hold a slot until establish rejects.
        *self.active.lock().unwrap() += 1;
        let _ = stream.set_nonblocking(true);
        let reg_id = self.registry.add(&stream);
        let guard = super::ConnGuard { active: self.active.clone() };
        let state = match origin {
            Origin::Pg => ConnState::NewPg(stream),
            Origin::Main => ConnState::NewMain(stream),
        };
        let id = self.broker.register_marked(Conn::new(origin, state, admitted, reg_id, guard, self.db.clone()));
        let tx = self.pool.sender().expect("pool running");
        assert!(super::submit(&tx, id), "queue full in test");
    }

    fn active_count(&self) -> usize {
        *self.active.lock().unwrap()
    }

    /// Drain: stop poller, wake workers, join everything.
    fn shutdown(self) {
        self.draining.store(true, Ordering::Relaxed);
        if let Some(p) = self.poller {
            let _ = p.join();
        }
        self.registry.shutdown_all();
        self.pool.join();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Legacy framed client over an already-connected socket.
struct LegacyClient {
    sock: TcpStream,
}

impl LegacyClient {
    fn new(sock: TcpStream) -> Self {
        sock.set_read_timeout(Some(Duration::from_secs(15))).unwrap();
        LegacyClient { sock }
    }

    fn query(&mut self, sql: &str) -> String {
        let b = sql.as_bytes();
        self.sock.write_all(&(b.len() as u32).to_be_bytes()).unwrap();
        self.sock.write_all(b).unwrap();
        self.sock.flush().unwrap();
        let mut hdr = [0u8; 4];
        self.sock.read_exact(&mut hdr).unwrap();
        let len = u32::from_be_bytes(hdr) as usize;
        assert!(len < 16 * 1024 * 1024, "frame too large: {len}");
        let mut buf = vec![0u8; len];
        self.sock.read_exact(&mut buf).unwrap();
        String::from_utf8(buf).unwrap()
    }
}

/// Connect a client + register the server side; returns the client socket.
fn loopback_pair(h: &Harness, origin: Origin, admitted: bool) -> TcpStream {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let client = TcpStream::connect(("127.0.0.1", port)).unwrap();
    let (server, _) = listener.accept().unwrap();
    h.admit(server, origin, admitted);
    client
}

fn wait_for(msg: &str, timeout: Duration, mut cond: impl FnMut() -> bool) {
    let t0 = Instant::now();
    while !cond() {
        assert!(t0.elapsed() < timeout, "timed out: {msg}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn broker_checkout_checkin_roundtrip() {
    let (db, dir) = test_db("broker");
    let broker = Broker::new();
    let registry = ConnRegistry::default();
    let active = Arc::new(Mutex::new(0usize));
    *active.lock().unwrap() += 1;
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let _client = TcpStream::connect(("127.0.0.1", port)).unwrap();
    let (server, _) = listener.accept().unwrap();
    let reg_id = registry.add(&server);
    let guard = super::ConnGuard { active: active.clone() };
    let id = broker.register_marked(Conn::new(
        Origin::Main,
        ConnState::NewMain(server),
        true,
        reg_id,
        guard,
        db,
    ));
    // A pre-marked conn suppresses duplicate submits; clearing re-arms it.
    assert!(!broker.try_mark_queued(id));
    broker.unmark(id);
    assert!(broker.try_mark_queued(id));
    broker.unmark(id);
    assert!(broker.checkout(id).is_some());
    // Duplicate checkout no-ops (checked-out conns leave the table).
    assert!(broker.checkout(id).is_none());
    // Reap sees nothing for a fresh conn without timeout.
    assert!(broker.reap(Some(Duration::from_secs(60)), false).is_empty());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn pool_serves_many_idle_connections_without_starving_workers() {
    // 4 workers, 500 mostly-idle legacy connections: queries on a fresh
    // connection must stay fast (idle sockets hold no worker thread).
    const IDLE: usize = 500;
    let h = Harness::start("manyidle", 4, None);
    let mut clients = Vec::with_capacity(IDLE);
    for _ in 0..IDLE {
        let sock = loopback_pair(&h, Origin::Main, true);
        let mut c = LegacyClient::new(sock);
        let resp = c.query("SELECT COUNT(*) FROM t");
        assert!(resp.contains('2'), "unexpected: {resp}");
        clients.push(c);
    }
    // All established: the broker parks them between commands.
    wait_for("park all", Duration::from_secs(20), || h.broker.len() == IDLE);
    // A new connection's query must still be fast with 500 idle parked.
    let t0 = Instant::now();
    let sock = loopback_pair(&h, Origin::Main, true);
    let mut probe = LegacyClient::new(sock);
    let resp = probe.query("SELECT v FROM t WHERE id = 2");
    assert!(resp.contains("20"), "unexpected: {resp}");
    assert!(
        t0.elapsed() < Duration::from_secs(10),
        "workers starved by idle conns: {:?}",
        t0.elapsed()
    );
    // Every idle connection still answers.
    for c in clients.iter_mut().take(20) {
        assert!(c.query("SELECT COUNT(*) FROM t").contains('2'));
    }
    // Closing every client reaps every slot (peek-EOF, no leak).
    drop(probe);
    for c in clients.drain(..) {
        drop(c);
    }
    wait_for("reap all", Duration::from_secs(20), || h.broker.len() == 0);
    wait_for("slots released", Duration::from_secs(20), || h.active_count() == 0);
    h.shutdown();
}

#[test]
fn mysql_rejected_with_exact_1040_when_full() {
    let h = Harness::start("m1040", 2, None);
    let mut sock = loopback_pair(&h, Origin::Main, false);
    sock.set_read_timeout(Some(Duration::from_secs(15))).unwrap();
    // Read the server handshake (3-byte LE len + seq, then payload).
    let mut hdr = [0u8; 4];
    sock.read_exact(&mut hdr).unwrap();
    let hlen = (hdr[0] as usize) | ((hdr[1] as usize) << 8) | ((hdr[2] as usize) << 16);
    let mut hbuf = vec![0u8; hlen];
    sock.read_exact(&mut hbuf).unwrap();
    assert!(!hbuf.is_empty(), "empty handshake");
    // Minimal 1-byte handshake response (never valid auth, but admission
    // is checked first, so 1040 must come back regardless).
    sock.write_all(&[1, 0, 0, 1, 0]).unwrap();
    sock.flush().unwrap();
    sock.read_exact(&mut hdr).unwrap();
    let rlen = (hdr[0] as usize) | ((hdr[1] as usize) << 8) | ((hdr[2] as usize) << 16);
    let mut rbuf = vec![0u8; rlen];
    sock.read_exact(&mut rbuf).unwrap();
    assert_eq!(rbuf[0], 0xFF, "expected ERR packet");
    let code = u16::from_le_bytes([rbuf[1], rbuf[2]]);
    assert_eq!(code, 1040, "expected ER_CON_COUNT_ERROR");
    let text = String::from_utf8_lossy(&rbuf);
    assert!(text.contains("Too many connections"), "unexpected: {text}");
    h.shutdown();
}

#[test]
fn pg_rejected_with_exact_53300_when_full() {
    use crate::wire::pg::codec::*;
    let h = Harness::start("p53300", 2, None);
    let mut sock = loopback_pair(&h, Origin::Pg, false);
    sock.set_read_timeout(Some(Duration::from_secs(15))).unwrap();
    // Minimal StartupMessage: protocol 3.0, user + database.
    let mut body = Vec::new();
    body.extend_from_slice(b"user\0root\0database\0default\0\0");
    let mut pkt = Vec::new();
    pkt.extend_from_slice(&((body.len() + 8) as u32).to_be_bytes());
    pkt.extend_from_slice(&196608u32.to_be_bytes());
    pkt.extend_from_slice(&body);
    sock.write_all(&pkt).unwrap();
    sock.flush().unwrap();
    // First server message must be ErrorResponse with 53300.
    let mut t = [0u8; 1];
    sock.read_exact(&mut t).unwrap();
    assert_eq!(t[0], MSG_ERROR, "expected ErrorResponse, got {:02X}", t[0]);
    let mut hlen = [0u8; 4];
    sock.read_exact(&mut hlen).unwrap();
    let len = u32::from_be_bytes(hlen) as usize - 4;
    let mut payload = vec![0u8; len];
    sock.read_exact(&mut payload).unwrap();
    let text = String::from_utf8_lossy(&payload);
    assert!(text.contains("53300"), "unexpected: {text}");
    assert!(text.contains("too many"), "unexpected: {text}");
    h.shutdown();
}

#[test]
fn drain_closes_parked_connections_and_joins() {
    let mut h = Harness::start("drain", 2, None);
    let mut clients = Vec::new();
    for _ in 0..10 {
        let sock = loopback_pair(&h, Origin::Main, true);
        let mut c = LegacyClient::new(sock);
        let resp = c.query("SELECT COUNT(*) FROM t");
        assert!(resp.contains('2'), "unexpected: {resp:?}");
        clients.push(c);
    }
    wait_for("park all", Duration::from_secs(20), || h.broker.len() == 10);
    // Draining closes parked sockets: clients observe EOF...
    h.draining.store(true, Ordering::Relaxed);
    // ...and the poller joins with an empty table.
    if let Some(p) = h.poller.take() {
        let _ = p.join();
    }
    // NOTE: pool join is covered by `shutdown` below; here we assert the
    // poller reaps everything it owns.
    wait_for("drained", Duration::from_secs(20), || h.broker.len() == 0);
    h.shutdown();
}

#[test]
fn pool_thread_count_parses() {
    assert!(super::pool::default_threads() >= 4);
    assert!(super::pool::parse_threads(&[]) >= 4);
    let bad = vec!["x".to_string(), "--threads".to_string(), "zzz".to_string()];
    assert_eq!(super::pool::parse_threads(&bad), super::pool::default_threads());
    let zero = vec!["s".to_string(), "--threads".to_string(), "0".to_string()];
    assert_eq!(super::pool::parse_threads(&zero), super::pool::default_threads());
    let eight = vec!["s".to_string(), "--threads".to_string(), "8".to_string()];
    assert_eq!(super::pool::parse_threads(&eight), 8);
}
