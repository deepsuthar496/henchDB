//! Replica side: connect to the primary, bootstrap via snapshot when
//! needed, then apply the committed WAL stream to the live database while
//! serving read-only queries.
//!
//! Position model: `(generation, applied_offset)` in primary log space.
//! Only snapshot boundaries are persisted (`<dir>/repl.offset`); WAL
//! progress lives in memory (replay after a restart is idempotent, so
//! re-streaming the post-snapshot prefix is safe). If the persisted files
//! cannot back the persisted offset (snapshot missing), the replica
//! re-bootstraps from scratch. Any transport or codec error drops the
//! connection and retries with exponential backoff (500ms → 5s).

use std::collections::HashMap;
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use engine::{wal::Wal, Database};

use super::primary::LOG_START;
use super::protocol::*;

const OFFSET_FILE: &str = "repl.offset";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const FRAME_TIMEOUT: Duration = Duration::from_secs(30);
/// Short reads so shutdown is honored promptly between frames.
const POLL_TIMEOUT: Duration = Duration::from_millis(500);

pub struct ReplicaOpts {
    pub primary: String,
    pub user: String,
    pub password: String,
    pub dir: PathBuf,
}

/// Persisted resume position: generation + offset the local snapshot files
/// are valid through, plus the upstream they came from and a promotion
/// marker. Missing/corrupt file (or snapshot files) = fresh.
/// Fencing rules:
/// - The upstream anchors generation comparisons: counters from different
///   primaries live in unrelated namespaces, so a repointed upstream is
///   trusted once, then recorded.
/// - A `promoted` marker seals a local promotion: reconnecting to the SAME
///   upstream afterwards never even connects (it must fail, not roll state
///   back); repointing elsewhere clears the marker on the next snapshot.
///   Deleting the file forces a fresh bootstrap (the explicit override).
fn read_resume(dir: &std::path::Path) -> (u64, u64, Option<String>, bool) {
    let text = std::fs::read_to_string(dir.join(OFFSET_FILE)).unwrap_or_default();
    let mut it = text.split_whitespace();
    let gen: u64 = it.next().and_then(|s| s.parse().ok()).unwrap_or(NO_GENERATION);
    let off: u64 = it.next().and_then(|s| s.parse().ok()).unwrap_or(LOG_START);
    // Trailing tokens: `[host] [promoted]` (`promoted` alone leaves no host).
    let rest: Vec<&str> = it.collect();
    let (host, promoted) = match &rest[..] {
        [] => (None, false),
        ["promoted"] => (None, true),
        [h] => (Some(h.to_string()), false),
        [h, "promoted"] => (Some(h.to_string()), true),
        _ => (None, false),
    };
    if gen == NO_GENERATION || off < LOG_START || !dir.join("snapshot.bin").exists() {
        return (NO_GENERATION, LOG_START, None, false);
    }
    (gen, off, host, promoted)
}

fn write_resume(
    dir: &std::path::Path,
    generation: u64,
    offset: u64,
    upstream: &str,
    promoted: bool,
) {
    let mark = if promoted { " promoted" } else { "" };
    let _ = std::fs::write(
        dir.join(OFFSET_FILE),
        format!("{generation} {offset} {upstream}{mark}\n"),
    );
}

/// Blocking replica loop; returns on either shutdown flag.
pub fn run_replica(
    db: Arc<Database>,
    opts: ReplicaOpts,
    draining: Arc<AtomicBool>,
    global: &'static AtomicBool,
) {
    let (mut generation, mut applied, mut resume_host, mut promoted) = read_resume(&opts.dir);
    eprintln!("replica: resuming at generation {generation} offset {applied}");
    // Fencing baseline: a local checkpoint since startup (snapshot apply,
    // promotion) rewinds the log and bumps the generation. A read-write
    // flip past this baseline means the node fenced itself while we were
    // following — seal and detach (live promotion).
    let started_read_only = db.is_read_only();
    let start_gen = db.wal_generation();
    let mut backoff = Duration::from_millis(500);
    loop {
        if draining.load(Ordering::Relaxed) || global.load(Ordering::Relaxed) {
            db.metrics().set_repl_status("DISCONNECTED");
            return;
        }
        // Promoted seal: this node fenced itself past `opts.primary`.
        // Reconnecting to the SAME upstream must fail (never roll local
        // state back); repointing elsewhere proceeds and clears the seal
        // on the next applied snapshot.
        if promoted && resume_host.as_deref() == Some(opts.primary.as_str()) {
            eprintln!(
                "replica: refusing to rejoin stale upstream {} after local promotion (delete {} to force a fresh bootstrap)",
                opts.primary,
                opts.dir.join(OFFSET_FILE).display()
            );
            // Refusing: the feeder is disconnected (the node itself stays
            // a healthy primary — see Replica_Role).
            db.metrics().set_repl_status("DISCONNECTED");
            std::thread::sleep(backoff);
            backoff = (backoff * 2).min(Duration::from_secs(5));
            continue;
        }
        // Live promotion (or a manual read-only lift past a local fence):
        // seal the new generation and detach without applying further
        // upstream batches — the fence already made everything applied
        // durable, and `pending` only ever holds uncommitted records,
        // which abort semantics discard. A feeder STARTED on a primary
        // (no fence since startup) keeps legacy follow behavior — that is
        // also how a forced fresh bootstrap (seal file deleted) rejoins.
        if started_read_only && !db.is_read_only() && db.wal_generation() != start_gen {
            write_resume(&opts.dir, db.wal_generation(), LOG_START, &opts.primary, true);
            eprintln!("replica: promoted to primary, detaching from {}", opts.primary);
            return;
        }
        db.metrics().set_repl_status("CONNECTING");
        match stream_once(
            &db,
            &opts,
            &mut generation,
            &mut applied,
            &mut resume_host,
            &mut promoted,
            &draining,
            global,
        ) {
            Ok(()) => {
                // Clean server-side close (primary draining): reconnect.
                backoff = Duration::from_millis(500);
                eprintln!("replica: primary closed the stream; reconnecting");
            }
            Err(e) => {
                eprintln!("replica: {e}; reconnecting in {}ms", backoff.as_millis());
                db.metrics().set_repl_status("DISCONNECTED");
                std::thread::sleep(backoff);
                backoff = (backoff * 2).min(Duration::from_secs(5));
            }
        }
    }
}

/// One connected session: handshake, bootstrap, stream until error.
fn stream_once(
    db: &Arc<Database>,
    opts: &ReplicaOpts,
    generation: &mut u64,
    applied: &mut u64,
    resume_host: &mut Option<String>,
    promoted: &mut bool,
    draining: &Arc<AtomicBool>,
    global: &'static AtomicBool,
) -> Result<(), CodecError> {
    let mut stream = TcpStream::connect_timeout(
        &opts.primary.parse().map_err(|_| CodecError(format!("bad --replica-of '{}'", opts.primary)))?,
        CONNECT_TIMEOUT,
    )
    .map_err(|e| CodecError(format!("connect {}: {e}", opts.primary)))?;
    set_timeouts(&stream, FRAME_TIMEOUT)?;
    write_frame(
        &mut stream,
        &Frame::Handshake {
            version: PROTOCOL_VERSION,
            user: opts.user.clone(),
            password: opts.password.clone(),
        },
    )?;
    let (wal_version, mut primary_durable) = match read_frame(&mut stream)? {
        Some(Frame::HandshakeAck { ok: true, wal_version, durable_offset, .. }) => {
            (wal_version, durable_offset)
        }
        Some(Frame::HandshakeAck { ok: false, message, .. }) => {
            return Err(CodecError(format!("primary refused: {message}")));
        }
        Some(other) => return Err(CodecError(format!("expected handshake ack, got {other:?}"))),
        None => return Err(CodecError("handshake ack timeout".into())),
    };
    // Column-codec generations predate the WAL version counter's later
    // bumps (v4 only extends Commit payloads): only versions below 3 use
    // the legacy column layout. Comparing against WAL_FORMAT_VERSION here
    // would mis-decode v3 streams once the log moves to v4+.
    let legacy_cols = wal_version < 3;
    db.metrics().set_repl_status("STREAMING");
    write_frame(
        &mut stream,
        &Frame::StartReplication { generation: *generation, from_offset: *applied },
    )?;
    set_timeouts(&stream, POLL_TIMEOUT)?;
    let mut pending: HashMap<u64, Vec<engine::wal::Record>> = HashMap::new();
    // Snapshot assembly buffer (only one snapshot at a time per stream).
    let mut snap: Option<SnapshotAsm> = None;
    loop {
        if draining.load(Ordering::Relaxed) || global.load(Ordering::Relaxed) {
            return Ok(());
        }
        // Same promotion check inside a connected session (the outer loop
        // only re-checks between reconnects).
        if !db.is_read_only() {
            return Ok(());
        }
        match read_frame(&mut stream)? {
            None => continue, // poll tick: re-check shutdown
            Some(Frame::WalChunk { offset, data }) => {
                snap = None;
                if offset != *applied {
                    // Missed or duplicate bytes (failover race): re-request
                    // our position; the primary replays or re-snapshots.
                    write_frame(
                        &mut stream,
                        &Frame::StartReplication {
                            generation: *generation,
                            from_offset: *applied,
                        },
                    )?;
                    continue;
                }
                let (records, consumed) = Wal::decode_wal_range(&data, legacy_cols)
                    .map_err(|e| CodecError(format!("wal decode: {e}")))?;
                if consumed != data.len() {
                    return Err(CodecError("torn WAL chunk from primary".into()));
                }
                for rec in records {
                    let txn = txn_of(&rec);
                    if matches!(rec, engine::wal::Record::Commit { .. }) {
                        if let Some(batch) = pending.remove(&txn) {
                            db.apply_replica_batch(batch)
                                .map_err(|e| CodecError(format!("apply: {e}")))?;
                        }
                    } else {
                        pending.entry(txn).or_default().push(rec);
                    }
                }
                *applied = offset + data.len() as u64;
                db.metrics().set_repl_applied(*applied);
                db.metrics()
                    .set_repl_lag(primary_durable.saturating_sub(*applied));
            }
            Some(Frame::Heartbeat { durable_offset }) => {
                primary_durable = durable_offset;
                db.metrics()
                    .set_repl_lag(primary_durable.saturating_sub(*applied));
                write_frame(&mut stream, &Frame::HeartbeatAck)?;
            }
            Some(Frame::SnapshotRequired) => {
                write_frame(
                    &mut stream,
                    &Frame::StartReplication {
                        generation: *generation,
                        from_offset: *applied,
                    },
                )?;
            }
            Some(Frame::SnapshotBegin { total_bytes, end_offset, generation: g }) => {
                if total_bytes > MAX_FRAME as u64 {
                    return Err(CodecError("snapshot too large".into()));
                }
                snap = Some(SnapshotAsm {
                    buf: Vec::with_capacity(total_bytes.min(64 << 20) as usize),
                    total_bytes,
                    end_offset,
                    generation: g,
                });
            }
            Some(Frame::SnapshotChunk { data }) => {
                let s = snap
                    .as_mut()
                    .ok_or_else(|| CodecError("chunk outside snapshot".into()))?;
                if s.buf.len() as u64 + data.len() as u64 > s.total_bytes {
                    return Err(CodecError("snapshot overrun".into()));
                }
                s.buf.extend_from_slice(&data);
            }
            Some(Frame::SnapshotEnd) => {
                let s = snap
                    .take()
                    .ok_or_else(|| CodecError("snapshot end outside snapshot".into()))?;
                if s.buf.len() as u64 != s.total_bytes {
                    return Err(CodecError("snapshot short read".into()));
                }
                // Generation fencing, anchored to the upstream host (see
                // `read_resume`): counters from different primaries are
                // unrelated, so a repointed upstream is trusted once, while
                // the SAME upstream going backwards means stale (e.g. the
                // old primary after this node promoted past it) — refuse to
                // overwrite local state.
                let repointed = resume_host.as_deref() != Some(opts.primary.as_str());
                if !repointed && *generation != NO_GENERATION && s.generation < *generation {
                    return Err(CodecError(format!(
                        "refusing snapshot from older generation {} (local {})",
                        s.generation, *generation
                    )));
                }
                apply_snapshot(
                    db,
                    &opts.dir,
                    &s.buf,
                    s.end_offset,
                    s.generation,
                    &opts.primary,
                )?;
                *resume_host = Some(opts.primary.clone());
                // A trusted snapshot from a live upstream clears any stale
                // promotion seal (explicit repoint = new history adopted).
                *promoted = false;
                *generation = s.generation;
                *applied = s.end_offset;
                pending.clear();
                db.metrics().set_repl_applied(*applied);
                db.metrics().set_repl_lag(primary_durable.saturating_sub(*applied));
            }
            Some(Frame::StartReplication { .. })
            | Some(Frame::Handshake { .. })
            | Some(Frame::HandshakeAck { .. })
            | Some(Frame::HeartbeatAck) => {
                return Err(CodecError("unexpected upstream frame".into()));
            }
        }
    }
}

struct SnapshotAsm {
    buf: Vec<u8>,
    total_bytes: u64,
    end_offset: u64,
    generation: u64,
}

fn apply_snapshot(
    db: &Arc<Database>,
    dir: &std::path::Path,
    image: &[u8],
    end_offset: u64,
    generation: u64,
    upstream: &str,
) -> Result<(), CodecError> {
    let mut slice: &[u8] = image;
    let (decoded, _) =
        engine::backup::decode_archive(&mut slice).map_err(|e| CodecError(format!("snapshot: {e}")))?;
    // `decode_archive` leaves no trailing bytes on success for images we
    // produced; tolerate none (fail closed on truncation).
    if !slice.is_empty() {
        return Err(CodecError("snapshot trailing bytes".into()));
    }
    db.apply_replica_snapshot(decoded.databases, decoded.tables, decoded.pages)
        .map_err(|e| CodecError(format!("snapshot apply: {e}")))?;
    write_resume(dir, generation, end_offset, upstream, false);
    db.metrics().set_repl_applied(end_offset);
    eprintln!("replica: snapshot applied (generation {generation}, offset {end_offset})");
    Ok(())
}

/// Transaction id of a streamed record (mirrors the engine replay path).
fn txn_of(rec: &engine::wal::Record) -> u64 {
    match rec {
        engine::wal::Record::Put { txn, .. }
        | engine::wal::Record::Delete { txn, .. }
        | engine::wal::Record::CreateTable { txn, .. }
        | engine::wal::Record::DropTable { txn, .. }
        | engine::wal::Record::CreateIndex { txn, .. }
        | engine::wal::Record::DropIndex { txn, .. }
        | engine::wal::Record::CreateDatabase { txn, .. }
        | engine::wal::Record::DropDatabase { txn, .. }
        | engine::wal::Record::Commit { txn, .. } => *txn,
    }
}
