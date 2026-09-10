//! Primary side of physical WAL streaming: a dedicated listener accepts
//! replica connections, authenticates them against `auth.bin`, and feeds
//! durable WAL bytes strictly in sequence (plus heartbeats and whole
//! snapshots when a replica is fresh or diverged).
//!
//! Offset model: the primary's log restarts at the 8-byte header on every
//! checkpoint, so a replica position is always `(generation, offset)`.
//! Any generation mismatch (or an explicit fresh start) serves a snapshot
//! first; the snapshot's end offset is the fresh log head, so the replica
//! continues with plain WAL chunks afterwards. Redo on both sides is
//! idempotent, so duplicate delivery after a failover is safe.

use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use engine::Database;

use super::protocol::*;
use crate::auth;

/// First valid log offset (right after the 8-byte WAL header).
pub const LOG_START: u64 = 8;
/// Max bytes per WAL chunk frame.
pub const CHUNK_BYTES: usize = 64 * 1024;
/// Max bytes per snapshot chunk frame.
pub const SNAP_CHUNK_BYTES: usize = 256 * 1024;
/// Heartbeat period on an idle stream; dead sockets surface on write.
pub const HEARTBEAT_PERIOD: Duration = Duration::from_secs(5);
/// First-frame deadline (Slowloris bound on unauthenticated sockets).
pub const HANDSHAKE_DEADLINE: Duration = Duration::from_secs(30);

/// Cleartext-password check against the `auth.bin` verifier (replication
/// runs over trusted networks in v1; the password never touches disk or
/// logs on either side).
fn verify_repl_password(
    path: &std::path::Path,
    user: &str,
    password: &str,
) -> bool {
    let Ok(store) = crate::auth::UserStore::load(path) else {
        return false;
    };
    let Some(v) = store.users.get(user) else {
        return false;
    };
    if v.hash.is_empty() {
        return password.is_empty();
    }
    if v.plugin == auth::PLUGIN_CACHING_SHA2 && v.hash.len() == 32 {
        return auth::sha256(password.as_bytes()) == v.hash.as_slice();
    }
    if v.plugin == auth::PLUGIN_NATIVE && v.hash.len() == 20 {
        let stage1 = auth::sha1(password.as_bytes());
        return auth::sha1(&stage1) == v.hash.as_slice();
    }
    false
}

/// Blocking accept loop; returns on either shutdown flag. Each replica gets
/// its own feeder thread; the connected count feeds `SHOW STATUS`.
pub fn serve_primary(
    db: Arc<Database>,
    listener: TcpListener,
    auth_path: PathBuf,
    draining: Arc<AtomicBool>,
    global: &'static AtomicBool,
) {
    let _ = listener.set_nonblocking(true);
    loop {
        if draining.load(Ordering::Relaxed) || global.load(Ordering::Relaxed) {
            break;
        }
        match listener.accept() {
            Ok((stream, peer)) => {
                let _ = stream.set_nonblocking(false);
                let db = db.clone();
                let auth_path = auth_path.clone();
                let draining = draining.clone();
                std::thread::spawn(move || {
                    db.metrics().repl_connected_inc();
                    let r = handle_replica(&db, stream, &auth_path, &draining, global);
                    db.metrics().repl_connected_dec();
                    if let Err(e) = r {
                        eprintln!("replica {peer} disconnected: {e}");
                    }
                });
            }
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(e) => eprintln!("replication accept error: {e}"),
        }
    }
}

/// Bind the replication listener with the usual +1..+8 fallback.
/// Returns the listener and the ACTUAL bound port (differs when `port` is
/// 0 or a fallback was taken).
#[allow(dead_code)]
pub fn bind_repl(port: u16) -> Option<(TcpListener, u16)> {
    bind_repl_on("0.0.0.0", port)
}

pub fn bind_repl_on(host: &str, port: u16) -> Option<(TcpListener, u16)> {
    for p in port..port.saturating_add(9) {
        match TcpListener::bind((host, p)) {
            Ok(l) => {
                let actual = l.local_addr().map(|a| a.port()).unwrap_or(p);
                return Some((l, actual));
            }
            Err(_) => continue,
        }
    }
    eprintln!(
        "replication: ports {}-{} all busy on {}, primary streaming disabled",
        port,
        port.saturating_add(8),
        host
    );
    None
}

fn handle_replica(
    db: &Arc<Database>,
    mut stream: TcpStream,
    auth_path: &std::path::Path,
    draining: &Arc<AtomicBool>,
    global: &'static AtomicBool,
) -> Result<(), CodecError> {
    set_timeouts(&stream, HANDSHAKE_DEADLINE)?;
    let peer = stream
        .peer_addr()
        .map(|p| p.to_string())
        .unwrap_or_else(|_| "?".into());
    // -- Handshake + auth (fail closed, same indistinguishability as the
    // SQL frontends: unknown users and wrong passwords look identical). --
    let user = match read_frame(&mut stream)? {
        Some(Frame::Handshake { version, user, password }) => {
            if version != PROTOCOL_VERSION {
                write_frame(
                    &mut stream,
                    &Frame::HandshakeAck {
                        ok: false,
                        message: format!("unsupported replication protocol {version}"),
                        wal_version: 0,
                        durable_offset: 0,
                    },
                )?;
                return Err(CodecError("protocol version mismatch".into()));
            }
            if !verify_repl_password(auth_path, &user, &password) {
                write_frame(
                    &mut stream,
                    &Frame::HandshakeAck {
                        ok: false,
                        message: "access denied".into(),
                        wal_version: 0,
                        durable_offset: 0,
                    },
                )?;
                eprintln!("replication access denied for '{user}' from {peer}");
                return Err(CodecError("access denied".into()));
            }
            user
        }
        Some(other) => {
            return Err(CodecError(format!("expected handshake, got {other:?}")));
        }
        None => return Err(CodecError("handshake timeout".into())),
    };
    println!("replica '{user}' subscribed from {peer}");
    write_frame(
        &mut stream,
        &Frame::HandshakeAck {
            ok: true,
            message: "streaming".into(),
            wal_version: engine::wal::WAL_FORMAT_VERSION,
            durable_offset: db.wal_durable(),
        },
    )?;
    // Steady state: per-connection (generation, next-offset) cursor, set by
    // StartReplication and advanced by every chunk/snapshot served. The
    // socket polls every 500ms (streaming latency bound); heartbeats go out
    // every HEARTBEAT_PERIOD on idle, and shutdown is honored promptly.
    let mut cursor: Option<(u64, u64)> = None;
    let mut last_hb = std::time::Instant::now();
    set_timeouts(&stream, Duration::from_millis(500))?;
    loop {
        if draining.load(Ordering::Relaxed) || global.load(Ordering::Relaxed) {
            return Ok(());
        }
        // Opportunistically push available WAL before blocking on input.
        if let Some((gen, next)) = cursor {
            if gen != db.wal_generation() {
                // Checkpoint (or any reset) moved the log: re-bootstrap.
                send_snapshot(db, &mut stream)?;
                cursor = Some((db.wal_generation(), db.wal_durable()));
                continue;
            }
            let durable = db.wal_durable();
            if next > durable {
                // Stale cursor past the durable prefix: re-bootstrap rather
                // than guessing (same-generation overrun cannot happen —
                // resets always bump the generation — but fail safe anyway).
                send_snapshot(db, &mut stream)?;
                cursor = Some((db.wal_generation(), db.wal_durable()));
                continue;
            }
            if next < durable {
                let max = (durable - next).min(CHUNK_BYTES as u64) as usize;
                match db.read_wal_range(next, max) {
                    Ok((data, end)) if !data.is_empty() => {
                        write_frame(&mut stream, &Frame::WalChunk { offset: next, data })?;
                        cursor = Some((gen, end));
                        continue; // drain the backlog without blocking
                    }
                    Ok(_) => {}
                    Err(e) => {
                        // Raced a truncate: re-bootstrap rather than guessing.
                        eprintln!("replica {peer} range read failed ({e}); re-snapshotting");
                        send_snapshot(db, &mut stream)?;
                        cursor = Some((db.wal_generation(), db.wal_durable()));
                        continue;
                    }
                }
            }
        }
        // Block for the replica's next request, or heartbeat on idle.
        match read_frame(&mut stream)? {
            Some(Frame::StartReplication { generation, from_offset }) => {
                if generation == NO_GENERATION
                    || generation != db.wal_generation()
                    || from_offset < LOG_START
                    || from_offset > db.wal_durable()
                {
                    send_snapshot(db, &mut stream)?;
                    cursor = Some((db.wal_generation(), db.wal_durable()));
                } else {
                    cursor = Some((generation, from_offset));
                }
            }
            Some(Frame::HeartbeatAck) => {}
            Some(other) => {
                return Err(CodecError(format!("unexpected frame {other:?}")));
            }
            None => {
                // Poll tick: heartbeat only each period (write failure =
                // dead socket), then keep waiting for requests.
                if cursor.is_some() && last_hb.elapsed() >= HEARTBEAT_PERIOD {
                    write_frame(
                        &mut stream,
                        &Frame::Heartbeat { durable_offset: db.wal_durable() },
                    )?;
                    last_hb = std::time::Instant::now();
                }
            }
        }
    }
}

/// Stream a consistent HDBB image WITHOUT checkpointing: the image is a
/// point-in-time read under the commit/install locks and the replica
/// continues from the mid-log head with idempotent redo, so no truncate is
/// needed. Deliberately generation-stable: serving snapshots must not bump
/// the log (which would re-bootstrap every other connected replica and
/// erode promotion fencing). Writers stall briefly under the dump locks;
/// replicas racing the image converge via idempotent replay.
fn send_snapshot(db: &Arc<Database>, stream: &mut TcpStream) -> Result<(), CodecError> {
    let mut image = Vec::new();
    let (_, end_offset) = db.dump_live(&mut image).map_err(|e| CodecError(e.to_string()))?;
    let generation = db.wal_generation();
    write_frame(
        stream,
        &Frame::SnapshotBegin {
            total_bytes: image.len() as u64,
            end_offset,
            generation,
        },
    )?;
    for chunk in image.chunks(SNAP_CHUNK_BYTES) {
        write_frame(stream, &Frame::SnapshotChunk { data: chunk.to_vec() })?;
    }
    write_frame(stream, &Frame::SnapshotEnd)?;
    println!(
        "replication snapshot sent ({} bytes, continues at offset {end_offset})",
        image.len()
    );
    Ok(())
}
