//! Bounded worker pool: connections are owned only while being worked.
//!
//! A worker checks a connection out of the broker, takes its socket
//! blocking, and either runs the establish step (handshake/auth, bounded by
//! the pre-auth timeout) or protocol steps (one packet/message/frame each,
//! draining already-buffered pipelines up to `MAX_STEPS_PER_CHECKOUT`).
//! Afterwards the connection is checked back in (parked non-blocking, no
//! thread attached) or closed. Idle connections therefore never occupy a
//! worker; only handshakes and real command processing do.

use std::sync::atomic::Ordering;
use std::sync::{mpsc::SyncSender, Arc};
use std::time::Duration;

use super::{Broker, Conn, ConnState, Disposition, Origin, Shared, JOB_QUEUE_BOUND, MAX_STEPS_PER_CHECKOUT};
use crate::wire;

pub struct Pool {
    /// `None` after shutdown (dropping the sender lets workers exit once
    /// the queue drains).
    tx: Option<SyncSender<super::Job>>,
    handles: Vec<std::thread::JoinHandle<()>>,
}

impl Pool {
    pub fn start(
        threads: usize,
        broker: Arc<Broker>,
        shared: Arc<Shared>,
        registry: crate::ConnRegistry,
    ) -> Self {
        let (tx, rx) = std::sync::mpsc::sync_channel::<super::Job>(JOB_QUEUE_BOUND);
        let rx = Arc::new(std::sync::Mutex::new(rx));
        let mut handles = Vec::with_capacity(threads);
        for i in 0..threads {
            let rx = rx.clone();
            let broker = broker.clone();
            let shared = shared.clone();
            let registry = registry.clone();
            handles.push(
                std::thread::Builder::new()
                    .name(format!("worker-{i}"))
                    .spawn(move || worker_loop(rx, broker, shared, registry))
                    .expect("spawn worker"),
            );
        }
        Pool { tx: Some(tx), handles }
    }

    /// Sender handle for the poller and acceptors (same bounded queue).
    pub fn sender(&self) -> Option<SyncSender<super::Job>> {
        self.tx.clone()
    }

    pub fn join(mut self) {
        self.tx.take(); // close the queue: workers exit after draining it
        for h in self.handles.drain(..) {
            let _ = h.join();
        }
    }
}

fn worker_loop(
    rx: Arc<std::sync::Mutex<std::sync::mpsc::Receiver<super::Job>>>,
    broker: Arc<Broker>,
    shared: Arc<Shared>,
    registry: crate::ConnRegistry,
) {
    loop {
        let id = {
            let rx = rx.lock().unwrap();
            match rx.recv() {
                Ok(super::Job::Conn(id)) => id,
                Err(_) => break, // queue closed: shut down
            }
        };
        let Some(conn) = broker.checkout(id) else {
            continue; // duplicate job or reaped meanwhile: no-op
        };
        if shared.shutdown.load(Ordering::Relaxed) || shared.global.load(Ordering::Relaxed) {
            close_conn(&registry, conn);
            continue;
        }
        // Workers speak blocking I/O with timeouts (all protocol code
        // assumes it); a dead socket fails the step and closes below.
        let _ = conn.set_blocking(true);
        let ctx = shared.conn_ctx();
        let is_new = matches!(conn.state, ConnState::NewMain(_) | ConnState::NewPg(_));
        let (conn, disposition) = if is_new {
            let (conn, disp) = establish(&shared, &ctx, &registry, conn);
            if matches!(disp, Disposition::Idle)
                && conn
                    .as_ref()
                    .is_some_and(|c| c.has_pending_input().unwrap_or(false))
            {
                run_steps(&shared, &ctx, conn.unwrap())
            } else {
                (conn, disp)
            }
        } else {
            run_steps(&shared, &ctx, conn)
        };
        match (conn, disposition) {
            (Some(conn), Disposition::Idle) => {
                if let Some(late) = broker.checkin(conn) {
                    // Poller is gone: close instead of parking.
                    close_conn(&registry, late);
                }
            }
            (Some(conn), Disposition::Closed) => close_conn(&registry, conn),
            // Establish already cleaned up (socket dropped, slot released).
            (None, _) => {}
        }
    }
}

/// First job for a fresh socket: sniff (main port) then run the blocking
/// handshake + authentication. Bounded by the pre-auth timeout inside the
/// establish functions; afterwards the connection parks like any other.
/// Returns `None` when the socket is already gone (registry cleaned here).
fn establish(
    shared: &Arc<Shared>,
    ctx: &wire::ConnCtx,
    registry: &crate::ConnRegistry,
    conn: Conn,
) -> (Option<Conn>, Disposition) {
    // Destructure: the handshake consumes the raw socket by value, so the
    // shell cannot be rebuilt on failure (the socket is dropped = closed).
    let super::conn::ConnParts {
        id,
        origin,
        state,
        admitted,
        reg_id,
        guard,
        db,
    } = conn.into_parts();
    let stream = match state {
        ConnState::NewMain(s) | ConnState::NewPg(s) => s,
        _ => {
            registry.remove(reg_id);
            return (None, Disposition::Closed);
        }
    };
    let _ = stream.set_read_timeout(Some(Duration::from_millis(200)));
    // Sniff the main port (legacy clients push bytes immediately, MySQL
    // clients wait silently for the handshake); the PG port needs no
    // sniffing. Mirrors the old `handle_auto` decision exactly.
    let mysql = if origin == Origin::Pg {
        true
    } else {
        let mut probe = [0u8; 1];
        match stream.peek(&mut probe) {
            Ok(n) if n > 0 => false, // pushed bytes: legacy framed text
            Ok(_) => {
                registry.remove(reg_id);
                return (None, Disposition::Closed); // peer closed immediately
            }
            Err(e)
                if e.kind() == std::io::ErrorKind::TimedOut
                    || e.kind() == std::io::ErrorKind::WouldBlock =>
            {
                true // silent client: MySQL wire, waits for our handshake
            }
            Err(_) => {
                registry.remove(reg_id);
                return (None, Disposition::Closed);
            }
        }
    };
    let _ = stream.set_read_timeout(None);
    if !mysql && !shared.allow_legacy {
        eprintln!("legacy protocol disabled: closing connection #{id}");
        registry.remove(reg_id);
        return (None, Disposition::Closed);
    }
    let rebuild = |state: ConnState| {
        Some(Conn::rebuild(id, origin, state, admitted, reg_id, guard, db.clone()))
    };
    if origin == Origin::Pg || mysql {
        let next = if origin == Origin::Pg {
            wire::pg::pg_establish(db.clone(), stream, ctx, admitted)
                .map(|o| o.map(ConnState::Pg))
        } else {
            wire::mysql_establish(db.clone(), stream, ctx, admitted)
                .map(|o| o.map(ConnState::Mysql))
        };
        match next {
            // Established: park until the poller sees input. Quiet closes
            // (`None`) and errors both drop the socket.
            Ok(Some(state)) => (rebuild(state), Disposition::Idle),
            Ok(None) | Err(_) => {
                registry.remove(reg_id);
                (None, Disposition::Closed)
            }
        }
    } else {
        match crate::legacy_establish(db.clone(), stream, shared.idle_timeout, admitted)
            .map(|o| o.map(ConnState::Legacy))
        {
            Ok(Some(state)) => (rebuild(state), Disposition::Idle),
            Ok(None) | Err(_) => {
                registry.remove(reg_id);
                (None, Disposition::Closed)
            }
        }
    }
}

/// Run protocol steps until the connection parks, closes, or runs out of
/// already-buffered input (pipelined frames drain without a poller trip).
fn run_steps(
    shared: &Arc<Shared>,
    ctx: &wire::ConnCtx,
    mut conn: Conn,
) -> (Option<Conn>, Disposition) {
    let db = shared.db.clone();
    for _ in 0..MAX_STEPS_PER_CHECKOUT {
        let step = match &mut conn.state {
            ConnState::Mysql(m) => wire::mysql_step(&db, m, ctx),
            ConnState::Pg(p) => wire::pg::pg_step(&db, p, ctx),
            ConnState::Legacy(l) => {
                let v0 = db.privilege_version();
                let r = crate::legacy_step(&db, l);
                crate::auth::persist_if_changed(&db, &shared.auth_path, v0);
                r
            }
            ConnState::NewMain(_) | ConnState::NewPg(_) => {
                return (Some(conn), Disposition::Closed);
            }
        };
        match step {
            Err(_) | Ok(Disposition::Closed) => return (Some(conn), Disposition::Closed),
            Ok(Disposition::Idle) => {
                if !conn.has_buffered_input() {
                    return (Some(conn), Disposition::Idle);
                }
            }
        }
        if shared.shutdown.load(Ordering::Relaxed) || shared.global.load(Ordering::Relaxed) {
            return (Some(conn), Disposition::Closed);
        }
    }
    (Some(conn), Disposition::Idle)
}

/// Close path: wake blockers, drop registry entry; `guard` drops here and
/// releases the counted slot.
fn close_conn(registry: &crate::ConnRegistry, conn: Conn) {
    eprintln!("pool: closed {} connection #{}", conn.describe(), conn.id);
    conn.shutdown_sock();
    registry.remove(conn.reg_id);
}

/// Default pool size: twice the available parallelism (one thread can
/// always make progress while another blocks in a long query).
pub fn default_threads() -> usize {
    std::thread::available_parallelism()
        .map(|n| (n.get() * 2).max(4))
        .unwrap_or(8)
}

/// Parse `--threads N` (falls back to the default on garbage).
pub fn parse_threads(args: &[String]) -> usize {
    crate::arg_value(args, "--threads")
        .and_then(|t| t.parse::<usize>().ok())
        .filter(|n| (1..=4096).contains(n))
        .unwrap_or_else(default_threads)
}
