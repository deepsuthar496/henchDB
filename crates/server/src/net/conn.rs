//! Managed connections for the pool/poller runtime (Priority 11).
//!
//! A `Conn` owns everything a client session needs across worker handoffs:
//! the transport plus the protocol session state (which lives in the wire
//! frontends). The poller parks `Idle` connections with non-blocking
//! sockets and detects activity with `peek` (never consuming bytes, so no
//! framing state is disturbed); workers take them blocking and run exactly
//! one protocol step per checkout, draining already-buffered pipelines
//! before checking the connection back in.

use std::io::BufReader;
use std::net::{Shutdown, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use engine::Database;

use crate::wire::{MysqlSession, ProcGuard};

/// One counted connection slot; released back to the pool on drop.
/// (Moved here from main.rs; the broker holds it for the conn's lifetime.)
pub(crate) struct ConnGuard {
    pub(crate) active: Arc<Mutex<usize>>,
}

impl Drop for ConnGuard {
    fn drop(&mut self) {
        let mut g = self.active.lock().unwrap();
        *g = g.saturating_sub(1);
    }
}

/// Which listener accepted the socket (decides the establish path).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Origin {
    /// Main port: sniff legacy-framed vs MySQL wire on first bytes.
    Main,
    /// PG port: PostgreSQL 3.0 directly.
    Pg,
}

/// Legacy framed-text session (`[u32 BE len][utf8 sql]`), previously owned
/// by the per-connection thread in main.rs.
pub(crate) struct LegacySession {
    pub(crate) reader: BufReader<TcpStream>,
    pub(crate) writer: TcpStream,
    pub(crate) session: engine::Session,
    pub(crate) proc: ProcGuard,
    pub(crate) buf: Vec<u8>,
    pub(crate) resp: Vec<u8>,
}

/// Protocol state held by a connection. `New*` variants still need the
/// (blocking, timeout-bounded) establish step; the rest are parked/stepped.
pub(crate) enum ConnState {
    NewMain(TcpStream),
    NewPg(TcpStream),
    Mysql(MysqlSession),
    Pg(crate::wire::pg::PgSession),
    Legacy(LegacySession),
}

/// Post-step disposition decided by the wire frontends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Disposition {
    /// Park in the poller until new input arrives.
    Idle,
    /// Close the socket and release the slot.
    Closed,
}

/// A broker-owned client connection.
pub(crate) struct Conn {
    pub(crate) id: u64,
    pub(crate) origin: Origin,
    pub(crate) state: ConnState,
    pub(crate) admitted: bool,
    pub(crate) last_active: Instant,
    /// A pool job is already queued for this connection (suppresses
    /// duplicate submissions: without it two workers could take the same
    /// connection and the loser would block forever on an empty read).
    pub(crate) queued: bool,
    /// Consecutive failed readiness probes (a single failed probe never
    /// reaps: transient resource errors must not kill a possibly-live
    /// connection; only persistent failure does).
    pub(crate) probe_fails: u8,
    pub(crate) reg_id: u64,
    /// Counted slot; dropping the conn releases it.
    pub(crate) guard: ConnGuard,
    pub(crate) db: Arc<Database>,
}

/// Destructured connection shell: the establish step consumes the raw
/// socket by value, so the shell travels separately and is rebuilt only on
/// success (failures drop the socket = close).
/// Destructured connection shell: the establish step consumes the raw
/// socket by value, so the shell travels separately and is rebuilt only on
/// success (failures drop the socket = close).
pub(crate) struct ConnParts {
    pub(crate) id: u64,
    pub(crate) origin: Origin,
    pub(crate) state: ConnState,
    pub(crate) admitted: bool,
    pub(crate) reg_id: u64,
    pub(crate) guard: ConnGuard,
    pub(crate) db: Arc<Database>,
}

impl Conn {
    pub(crate) fn new(
        origin: Origin,
        state: ConnState,
        admitted: bool,
        reg_id: u64,
        guard: ConnGuard,
        db: Arc<Database>,
    ) -> Self {
        Conn { id: 0, origin, state, admitted, last_active: Instant::now(), queued: false, probe_fails: 0, reg_id, guard, db }
    }

    pub(crate) fn into_parts(self) -> ConnParts {
        ConnParts {
            id: self.id,
            origin: self.origin,
            state: self.state,
            admitted: self.admitted,
            reg_id: self.reg_id,
            guard: self.guard,
            db: self.db,
        }
    }

    pub(crate) fn rebuild(
        id: u64,
        origin: Origin,
        state: ConnState,
        admitted: bool,
        reg_id: u64,
        guard: ConnGuard,
        db: Arc<Database>,
    ) -> Self {
        Conn { id, origin, state, admitted, last_active: Instant::now(), queued: false, probe_fails: 0, reg_id, guard, db }
    }

    /// Toggle blocking mode on the underlying socket(s), whatever the
    /// current state. Poller parks non-blocking; workers take blocking.
    pub(crate) fn set_blocking(&self, blocking: bool) -> std::io::Result<()> {
        let nb = !blocking;
        match &self.state {
            ConnState::NewMain(s) | ConnState::NewPg(s) => s.set_nonblocking(nb),
            ConnState::Mysql(m) => m.stream().set_blocking(blocking),
            ConnState::Pg(p) => p.stream().set_blocking(blocking),
            ConnState::Legacy(l) => {
                l.reader.get_ref().set_nonblocking(nb)?;
                l.writer.set_nonblocking(nb)
            }
        }
    }

    /// Activity probe for the poller: true when at least one byte is
    /// waiting (peek never consumes, so framing is undisturbed). For TLS
    /// this observes ciphertext — a readiness hint only; the worker's
    /// blocking read still bounds partial records by the idle timeout.
    /// `Ok(0)` (orderly FIN) and hard errors (reset/refused) report
    /// failure so the poller reaps the dead connection. TRANSIENT resource
    /// errors (out of buffer space under socket pressure) report Ok(false):
    /// a failed readiness probe must never kill a connection that may
    /// still be alive — the worker's real I/O is the arbiter, and the next
    /// sweep retries the probe.
    pub(crate) fn has_pending_input(&self) -> std::io::Result<bool> {
        let mut one = [0u8; 1];
        let sock: &TcpStream = match &self.state {
            ConnState::NewMain(s) | ConnState::NewPg(s) => s,
            ConnState::Mysql(m) => m.stream().inner_tcp(),
            ConnState::Pg(p) => p.stream().inner_tcp(),
            ConnState::Legacy(l) => l.reader.get_ref(),
        };
        match sock.peek(&mut one) {
            Ok(0) => Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "peer closed",
            )),
            Ok(_) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => Ok(false),
            Err(e) if is_transient_probe_error(&e) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Wake any thread blocked in I/O on this connection (drain path).
    pub(crate) fn shutdown_sock(&self) {
        let _ = match &self.state {
            ConnState::NewMain(s) | ConnState::NewPg(s) => s.shutdown(Shutdown::Both),
            ConnState::Mysql(m) => m.stream().inner_tcp().shutdown(Shutdown::Both),
            ConnState::Pg(p) => p.stream().inner_tcp().shutdown(Shutdown::Both),
            ConnState::Legacy(l) => l.reader.get_ref().shutdown(Shutdown::Both),
        };
    }

    /// True when `reader` already holds a full or partial pipelined frame
    /// beyond what the last step consumed (worker keeps stepping without a
    /// poller round trip).
    pub(crate) fn has_buffered_input(&self) -> bool {
        match &self.state {
            ConnState::Mysql(m) => !m.buffered().is_empty(),
            ConnState::Pg(p) => !p.buffered().is_empty(),
            ConnState::Legacy(l) => !l.reader.buffer().is_empty(),
            _ => false,
        }
    }

    pub(crate) fn describe(&self) -> &'static str {
        match &self.state {
            ConnState::NewMain(_) | ConnState::NewPg(_) => "handshake",
            ConnState::Mysql(_) => "mysql",
            ConnState::Pg(_) => "pg",
            ConnState::Legacy(_) => "legacy",
        }
    }
}

/// True for transient socket-resource errors that must not reap a parked
/// connection (retry on the next sweep instead). Readiness probes compete
/// with hundreds of sibling sockets; failing one probe says nothing about
/// the connection's health.
fn is_transient_probe_error(e: &std::io::Error) -> bool {
    match e.raw_os_error() {
        // WSAENOBUFS on Windows, ENOBUFS elsewhere: kernel out of buffer
        // space (or ephemeral-port pressure) — retry on the next sweep.
        #[cfg(windows)]
        Some(10055) => true,
        #[cfg(not(windows))]
        Some(105) => true,
        _ => false,
    }
}
