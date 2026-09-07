//! Bounded worker pool + polled connection multiplexing (Priority 11).
//!
//! The old model spent one OS thread per connection (a quiet client held a
//! thread blocked in `read()` for up to the idle timeout). Now:
//! - one **poller** thread parks every idle connection with a non-blocking
//!   socket and detects activity with `peek` (no bytes consumed, no framing
//!   disturbed; for TLS only ciphertext visibility — a hint, not parsing);
//! - a bounded **pool** of workers (`--threads`, default 2x parallelism)
//!   owns a connection only while establishing or stepping it; between
//!   commands the connection goes back to the poller, so 10k quiet clients
//!   cost file descriptors, not threads or stacks;
//! - protocol code is untouched semantically: each frontend exposes an
//!   `establish` (blocking handshake, timeout-bounded) plus a single
//!   `step` (one packet / message / frame); session state rides in the
//!   `Conn` across handoffs, and already-buffered pipelines drain in the
//!   same checkout without a poller round trip.
//!
//! Shutdown: `draining` closes parked connections, the job queue is dropped
//! so workers exit after in-flight steps, then everything joins before the
//! final checkpoint. Blocked worker reads still wake via socket shutdown
//! (the `ConnRegistry` mechanism, unchanged).

pub mod conn;
pub mod poller;
pub mod pool;
#[cfg(test)]
mod tests;

pub(crate) use conn::{Conn, ConnGuard, ConnState, Disposition, LegacySession, Origin};

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::{mpsc::SyncSender, Arc, Mutex};
use std::time::{Duration, Instant};

use engine::Database;

use crate::wire::ConnCtx;

/// Work item for the pool: take over one connection.
#[derive(Debug)]
pub enum Job {
    Conn(u64),
}

/// Everything a worker needs, shared across pool + poller + acceptors.
pub struct Shared {
    pub db: Arc<Database>,
    pub auth_path: PathBuf,
    pub idle_timeout: Option<Duration>,
    /// Draining (COM_SHUTDOWN / signal): finish steps, then close.
    pub shutdown: Arc<AtomicBool>,
    pub global: &'static AtomicBool,
    pub tls: Option<Arc<rustls::ServerConfig>>,
    pub allow_legacy: bool,
}

impl Shared {
    /// Per-connection policy for the wire frontends (admission is decided
    /// by the acceptor and stored on the `Conn`, not in this shared ctx).
    pub fn conn_ctx(&self) -> ConnCtx {
        ConnCtx {
            auth_path: self.auth_path.clone(),
            idle_timeout: self.idle_timeout,
            shutdown: self.shutdown.clone(),
            tls: self.tls.clone(),
        }
    }
}

/// Central connection table. Presence in the table means poller-owned
/// (parked); absence means a worker owns it or it is gone. This makes every
/// handoff race-free: double-queued jobs find nothing on the second
/// checkout, and the poller can never reap a connection a worker holds.
pub struct Broker {
    inner: Mutex<BrokerInner>,
}

struct BrokerInner {
    next: u64,
    conns: HashMap<u64, Conn>,
    /// False once the poller stopped: late checkins close immediately so
    /// no parked connection outlives the sweep loop.
    accepting: bool,
}

impl Broker {
    pub fn new() -> Self {
        Broker {
            inner: Mutex::new(BrokerInner { next: 0, conns: HashMap::new(), accepting: true }),
        }
    }

    /// Register and pre-mark queued (admit paths submit a handshake job
    /// for the new connection immediately).
    pub fn register_marked(&self, mut conn: Conn) -> u64 {
        conn.queued = true;
        let mut g = self.inner.lock().unwrap();
        g.next += 1;
        let id = g.next;
        conn.id = id;
        conn.last_active = Instant::now();
        g.conns.insert(id, conn);
        id
    }

    /// Mark a parked, unqueued connection as having a job in flight.
    /// `false` when already queued or gone (caller skips the submit).
    #[cfg(test)]
    pub fn try_mark_queued(&self, id: u64) -> bool {
        let mut g = self.inner.lock().unwrap();
        match g.conns.get_mut(&id) {
            Some(c) if !c.queued => {
                c.queued = true;
                true
            }
            _ => false,
        }
    }

    /// Clear a mark (queue-full submit path: the connection stays parked
    /// and a later sweep retries).
    pub fn unmark(&self, id: u64) {
        let mut g = self.inner.lock().unwrap();
        if let Some(c) = g.conns.get_mut(&id) {
            c.queued = false;
        }
    }

    /// Take ownership for processing; `None` when already gone (a duplicate
    /// queued job simply no-ops).
    pub fn checkout(&self, id: u64) -> Option<Conn> {
        self.inner.lock().unwrap().conns.remove(&id)
    }

    /// Park after processing. When the poller is gone the connection is
    /// closed instead of parked (the caller drops the returned conn).
    pub fn checkin(&self, mut conn: Conn) -> Option<Conn> {
        conn.last_active = Instant::now();
        conn.queued = false;
        // Park non-blocking; a failure means the socket is dead anyway.
        let _ = conn.set_blocking(false);
        let mut g = self.inner.lock().unwrap();
        if !g.accepting {
            return Some(conn);
        }
        g.conns.insert(conn.id, conn);
        None
    }

    /// Remove unconditionally (close path).
    pub fn remove(&self, id: u64) -> Option<Conn> {
        self.inner.lock().unwrap().conns.remove(&id)
    }

    pub fn set_accepting(&self, accepting: bool) {
        self.inner.lock().unwrap().accepting = accepting;
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().conns.len()
    }

    /// What the poller found on one connection. Probe failures take three
    /// consecutive bad sweeps to reap (a single failed peek says nothing
    /// about the connection's health under socket pressure); successes
    /// reset the count.
    pub fn snapshot(&self) -> Vec<(u64, PeekAction)> {
        let mut g = self.inner.lock().unwrap();
        let mut out = Vec::new();
        for (id, conn) in g.conns.iter_mut() {
            if conn.queued {
                continue;
            }
            match conn.has_pending_input() {
                Ok(true) => {
                    conn.probe_fails = 0;
                    conn.queued = true;
                    out.push((*id, PeekAction::Data));
                }
                Ok(false) => {
                    conn.probe_fails = 0;
                }
                Err(_) => {
                    conn.probe_fails = conn.probe_fails.saturating_add(1);
                    if conn.probe_fails >= 3 {
                        out.push((*id, PeekAction::Close));
                    }
                }
            }
        }
        out
    }

    /// Idle reap + drain sweep: ids of parked connections quiet past the
    /// deadline, or everything when `close_all` (drain).
    pub fn reap(&self, idle_timeout: Option<Duration>, close_all: bool) -> Vec<u64> {
        let d = match (idle_timeout, close_all) {
            (_, true) => None,
            (Some(d), false) => Some(d),
            (None, false) => return Vec::new(),
        };
        let now = Instant::now();
        let g = self.inner.lock().unwrap();
        g.conns
            .iter()
            .filter(|(_, c)| close_all || d.is_some_and(|d| now.duration_since(c.last_active) > d))
            .map(|(id, _)| *id)
            .collect()
    }
}

/// Poller observation for one parked connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeekAction {
    Data,
    Close,
}

/// Queue depth for ready-connection jobs (poller never blocks on submit).
pub const JOB_QUEUE_BOUND: usize = 4096;

/// Max protocol steps per worker checkout: already-buffered pipelines drain
/// without a poller round trip, but a chatty connection still yields.
pub const MAX_STEPS_PER_CHECKOUT: usize = 1024;

/// Submit helper: `false` when the queue is full (caller closes/reparks).
pub fn submit(tx: &SyncSender<Job>, id: u64) -> bool {
    tx.try_send(Job::Conn(id)).is_ok()
}
