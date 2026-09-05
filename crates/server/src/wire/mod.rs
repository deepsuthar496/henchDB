//! MySQL client/server wire protocol (text + binary prepared statements,
//! dependency-free).
//!
//! Implements enough of the MySQL 4.1+ protocol for stock drivers, ORMs,
//! GUIs and CLIs to connect with zero client changes:
//!   - HandshakeV10 + verified authentication (`caching_sha2_password` by
//!     default, `mysql_native_password` accepted; see `auth.rs`)
//!   - COM_QUERY (text), COM_PING, COM_QUIT, COM_INIT_DB, COM_RESET_CONNECTION,
//!     COM_SHUTDOWN (checkpoint + graceful stop)
//!   - COM_STMT_PREPARE / COM_STMT_EXECUTE / COM_STMT_CLOSE / COM_STMT_RESET
//!     with binary result sets (server-side cursors/COM_STMT_FETCH excluded)
//!   - Text-protocol result sets (column count + ColumnDefinition41 +
//!     EOF + text rows + EOF), OK and ERR packets
//!   - Canned responses for common introspection queries drivers send on
//!     connect (`SELECT @@version`, `SHOW VARIABLES`, bare `SELECT 1`,
//!     `SET ...`, `information_schema` probes) so connections survive setup
//!
//! Prepared statements bind textually: `?` markers (outside quotes) are
//! replaced by escaped literals and run through the normal executor, so
//! parameter semantics always match COM_QUERY. Binary parameters decode per
//! the wire spec (ints, floats, strings, dates-as-text, null bitmap); result
//! rows stream in binary format with per-column types.
//!
//! Framing: `[3-byte LE len][1-byte seq][payload]`, max 16MiB per packet.
//! Multi-packet continuations (len == 0xFFFFFF) are handled on both paths.
//! Linux-only fast paths stay out of here; this is portable std only.

use std::collections::HashMap;
use std::io::{BufReader, Write};
use std::net::{IpAddr, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use engine::Database;

use crate::auth::{self, UserStore};

pub mod canned;
pub mod constants;
pub mod handshake;
pub mod packet;
pub mod stmt;
pub mod tls;

#[cfg(test)]
mod tests;

pub use canned::canned_output;
pub use constants::SERVER_CAPS;

use canned::{normalize_dialect, split_statements};
use constants::*;
use handshake::{fresh_scramble, handshake_payload, parse_handshake_response, parse_ssl_request};
use packet::{err_payload, ok_payload, read_packet, write_err, write_err_msg, write_packet, eof_payload};
use tls::ConnStream;
use stmt::{
    column_def_payload, datum_literal, decode_execute_params, find_placeholders,
    neutralize_placeholders, prepare_ok_payload, schema_col_type, substitute, write_output,
    Prepared,
};

/// Run text statements through the executor, streaming each result in text
/// or binary form. Shared by COM_QUERY and COM_STMT_EXECUTE so parameter
/// semantics always match text queries.
fn execute_statements<W: std::io::Write>(
    db: &Arc<Database>,
    session: &mut engine::Session,
    stmts: &[String],
    writer: &mut W,
    seq: &mut u8,
    deprecate_eof: bool,
    binary: bool,
) -> std::io::Result<()> {
    for stmt in stmts {
        let stmt = normalize_dialect(stmt.trim());
        if stmt.is_empty() {
            continue;
        }
        match db.execute(session, stmt) {
            Ok(out) => write_output(writer, seq, &out, deprecate_eof, binary)?,
            Err(e) => {
                if let Some(canned) = canned_output(stmt) {
                    write_output(writer, seq, &canned, deprecate_eof, binary)?;
                } else {
                    write_err(writer, seq, &e)?;
                    break;
                }
            }
        }
    }
    Ok(())
}

/// Connection rate limiter for IP flood protection (DoS protection).
pub struct RateLimiter {
    per_ip: Mutex<HashMap<IpAddr, (u32, Instant)>>,
    max_per_sec: u32,
}

impl RateLimiter {
    pub fn new(max_per_sec: u32) -> Self {
        RateLimiter {
            per_ip: Mutex::new(HashMap::new()),
            max_per_sec,
        }
    }

    pub fn check(&self, ip: IpAddr) -> bool {
        let now = Instant::now();
        let mut guard = self.per_ip.lock().unwrap();
        if guard.len() > 1000 {
            guard.retain(|_, (_, start)| now.duration_since(*start) < Duration::from_secs(5));
        }
        let entry = guard.entry(ip).or_insert((0, now));
        if now.duration_since(entry.1) >= Duration::from_secs(1) {
            entry.0 = 1;
            entry.1 = now;
            true
        } else if entry.0 < self.max_per_sec {
            entry.0 += 1;
            true
        } else {
            false
        }
    }
}

/// Per-connection server policy, built by `serve` in main.rs.
pub struct ConnCtx {
    /// Path to `auth.bin` (reloaded per connection, so `passwd` applies live).
    pub auth_path: PathBuf,
    /// Idle read timeout; `None` disables (not recommended when exposed).
    pub idle_timeout: Option<Duration>,
    /// Set by COM_SHUTDOWN or the signal handler; the accept loop polls it.
    pub shutdown: Arc<AtomicBool>,
    /// False when the server is at `max_connections`: handshake, then ERR.
    pub admitted: bool,
    /// TLS config when `--tls-cert`/`--tls-key` were given. `None` means
    /// plaintext only (SSLRequest fails closed with an error).
    pub tls: Option<Arc<rustls::ServerConfig>>,
    /// Rate limiter for DoS / IP flood protection.
    pub rate_limiter: Option<Arc<RateLimiter>>,
}

/// MySQL access-denied error (1045/28000), same shape for unknown users and
/// wrong passwords (no user enumeration). Passwords are never logged.
fn access_denied(user: &str, peer: &str, using_password: bool) -> Vec<u8> {
    err_payload(
        1045,
        "28000",
        &format!(
            "Access denied for user '{user}'@'{peer}' (using password: {})",
            if using_password { "YES" } else { "NO" }
        ),
    )
}

/// Serve one MySQL-protocol connection until QUIT, error, or close.
pub fn handle_mysql_connection(
    db: Arc<Database>,
    stream: TcpStream,
    ctx: &ConnCtx,
) -> std::io::Result<()> {
    let peer = stream.peer_addr().unwrap_or_else(|_| "unknown:0".parse().unwrap());
    let peer_host = peer.ip().to_string();
    let mut stream = stream;
    stream.set_nodelay(true)?;
    let _ = stream.set_read_timeout(None);
    let mut session = db.new_session();
    let mut stmts: HashMap<u32, Prepared> = HashMap::new();
    let mut next_stmt_id: u32 = 1;

    let mut sseq: u8 = 0;
    if let Some(limiter) = &ctx.rate_limiter {
        if !limiter.check(peer.ip()) {
            eprintln!("connection rate limit exceeded for {peer}");
            write_packet(&mut stream, &err_payload(1040, "HY000", "Connection rate limit exceeded"), &mut sseq)?;
            stream.flush()?;
            return Ok(());
        }
    }

    // -- Handshake (fresh 20-byte scramble per connection: replay across
    // sessions is impossible even though the scramble travels in clear). --
    let conn_id = std::process::id() ^ (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0));
    let scramble = fresh_scramble();
    // CLIENT_SSL is advertised only when a certificate is configured.
    let caps = SERVER_CAPS | if ctx.tls.is_some() { CAP_SSL } else { 0 };
    write_packet(&mut stream, &handshake_payload(conn_id, &scramble, AUTH_PLUGIN, caps), &mut sseq)?;
    stream.flush()?;
    // Bound the pre-auth phase (Slowloris): the client must answer quickly.
    // Idle policy applies after authentication instead.
    let _ = stream.set_read_timeout(Some(Duration::from_secs(30)));
    let (cseq0, first) = {
        let mut pre = BufReader::new(&mut stream);
        match read_packet(&mut pre, 16 * 1024 * 1024) {
            Ok(v) => v,
            Err(_) => return Ok(()),
        }
    };
    let _ = stream.set_read_timeout(None);
    // TLS upgrade: the client answered with an SSLRequest instead of a
    // handshake response. Without a configured certificate this fails
    // closed (ERR, then close) rather than downgrading to plaintext.
    let conn = if parse_ssl_request(&first).is_some() {
        match &ctx.tls {
            Some(cfg) => match tls::accept_tls(cfg, stream) {
                Ok(t) => ConnStream::Tls(Box::new(t)),
                Err(e) => {
                    eprintln!("tls handshake failed for {peer}: {e}");
                    return Ok(());
                }
            },
            None => {
                let _ = write_packet(
                    &mut stream,
                    &err_payload(1047, "HY000", "SSL requested but the server has no TLS certificate configured"),
                    &mut sseq,
                );
                let _ = stream.flush();
                return Ok(());
            }
        }
    } else {
        ConnStream::Plain(stream)
    };
    let mut reader = BufReader::new(conn);
    // Over TLS the handshake response arrives as the next packet; over
    // plaintext we already hold it.
    let (cseq, resp) = if matches!(reader.get_ref(), ConnStream::Tls(_)) {
        match read_packet(&mut reader, 16 * 1024 * 1024) {
            Ok(v) => v,
            Err(_) => return Ok(()),
        }
    } else {
        (cseq0, first)
    };
    let hs = parse_handshake_response(&resp);
    // Effective caps are the intersection: the client must not use features
    // the server did not advertise (notably DEPRECATE_EOF, which we do not
    // offer, so EOF packets stay mandatory). Masking matters: stock clients
    // set DEPRECATE_EOF unconditionally and would otherwise wait forever
    // for a missing EOF after column definitions.
    let deprecate_eof = hs
        .as_ref()
        .map(|h| h.caps & SERVER_CAPS & CAP_DEPRECATE_EOF != 0)
        .unwrap_or(false);
    sseq = cseq.wrapping_add(1);
    // -- Admission + authentication, before any OK. --
    if !ctx.admitted {
        eprintln!("connection refused (max_connections): {peer}");
        write_packet(reader.get_mut(), &err_payload(1040, "HY000", "Too many connections"), &mut sseq)?;
        reader.get_mut().flush()?;
        return Ok(());
    }
    let authed_user = match hs {
        Some(hs) if !hs.username.is_empty() => {
            // The client may answer with a different plugin than offered
            // (--default-auth). When it names a plugin we support, run the
            // AuthSwitch exchange (0xFE + plugin + scramble, raw reply);
            // unknown plugins fail closed.
            let plugin = if hs.plugin.is_empty() { AUTH_PLUGIN.to_string() } else { hs.plugin.clone() };
            let mut proof = hs.auth.clone();
            if plugin != AUTH_PLUGIN
                && (plugin == auth::PLUGIN_NATIVE || plugin == auth::PLUGIN_CACHING_SHA2)
            {
                let mut sw = vec![0xFE];
                sw.extend_from_slice(plugin.as_bytes());
                sw.push(0);
                sw.extend_from_slice(&scramble);
                sw.push(0);
                write_packet(reader.get_mut(), &sw, &mut sseq)?;
                reader.get_mut().flush()?;
                let (cseq2, sw_resp) = match read_packet(&mut reader, 16 * 1024 * 1024) {
                    Ok(v) => v,
                    Err(_) => return Ok(()),
                };
                sseq = cseq2.wrapping_add(1);
                proof = sw_resp;
            }
            // Fail closed when the auth store is unreadable.
            let store = match UserStore::load(&ctx.auth_path) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("auth store unavailable for {peer}: {e}");
                    let p = access_denied(&hs.username, &peer_host, !hs.auth.is_empty());
                    write_packet(reader.get_mut(), &p, &mut sseq)?;
                    reader.get_mut().flush()?;
                    return Ok(());
                }
            };
            // Unknown users fail exactly like wrong passwords.
            let using_password = !proof.is_empty() && proof != [0];
            let ok = store
                .users
                .get(&hs.username)
                .map(|v| auth::verify(v, &plugin, &scramble, &proof))
                .unwrap_or(false);
            if !ok {
                eprintln!("access denied for '{}' from {peer}", hs.username);
                let p = access_denied(&hs.username, &peer_host, using_password);
                write_packet(reader.get_mut(), &p, &mut sseq)?;
                reader.get_mut().flush()?;
                return Ok(());
            }
            if let Some(db_name) = &hs.db {
                if !db_name.is_empty() {
                    let _ = db.execute(&mut session, &format!("USE `{db_name}`"));
                }
            }
            hs.username
        }
        _ => {
            let p = access_denied("", &peer_host, false);
            write_packet(reader.get_mut(), &p, &mut sseq)?;
            reader.get_mut().flush()?;
            return Ok(());
        }
    };
    write_packet(reader.get_mut(), &ok_payload(0, ""), &mut sseq)?;
    reader.get_mut().flush()?;
    println!("mysql connected: {peer} as '{authed_user}'");
    // Idle timeout from here on (handshake already completed); a quiet
    // connection is reaped instead of held forever.
    if let Some(d) = ctx.idle_timeout {
        let _ = reader.get_mut().set_read_timeout(Some(d));
    } else {
        let _ = reader.get_mut().set_read_timeout(None);
    }

    // -- Command phase --
    loop {
        if ctx.shutdown.load(Ordering::Relaxed) {
            break; // draining: listener is closing, finish promptly
        }
        let (cseq, payload) = match read_packet(&mut reader, 16 * 1024 * 1024) {
            Ok(v) => v,
            Err(e) if e.kind() == std::io::ErrorKind::TimedOut => break, // idle timeout
            Err(_) => break, // client closed
        };
        if payload.is_empty() {
            continue;
        }
        let mut out_seq = cseq.wrapping_add(1);
        match payload[0] {
            COM_QUIT => break,
            COM_SHUTDOWN => {
                // Any authenticated user may request shutdown in v1
                // (per-user privileges are the SEC8 follow-up).
                println!("shutdown requested by '{authed_user}' from {peer}");
                match db.checkpoint() {
                    Ok(()) => {
                        write_packet(reader.get_mut(), &ok_payload(0, ""), &mut out_seq)?;
                        reader.get_mut().flush()?;
                    }
                    Err(e) => write_err(reader.get_mut(), &mut out_seq, &e)?,
                }
                ctx.shutdown.store(true, Ordering::Relaxed);
                break;
            }
            COM_INIT_DB => {
                let db_name = String::from_utf8_lossy(&payload[1..])
                    .trim_matches('\0')
                    .trim()
                    .to_string();
                if db_name.is_empty() {
                    write_err_msg(reader.get_mut(), &mut out_seq, 1049, "Unknown database ''")?;
                } else {
                    match db.execute(&mut session, &format!("USE `{db_name}`")) {
                        Ok(_) => {
                            write_packet(reader.get_mut(), &ok_payload(0, ""), &mut out_seq)?;
                            reader.get_mut().flush()?;
                        }
                        Err(e) => write_err(reader.get_mut(), &mut out_seq, &e)?,
                    }
                }
            }
            COM_PING => {
                write_packet(reader.get_mut(), &ok_payload(0, ""), &mut out_seq)?;
                reader.get_mut().flush()?;
            }
            COM_RESET_CONNECTION => {
                session = db.new_session();
                write_packet(reader.get_mut(), &ok_payload(0, ""), &mut out_seq)?;
                reader.get_mut().flush()?;
            }
            COM_QUERY => {
                let sql = String::from_utf8_lossy(&payload[1..]).into_owned();
                let sql = sql.trim();
                if sql.is_empty() {
                    write_err_msg(reader.get_mut(), &mut out_seq, 1065, "empty query")?;
                    continue;
                }
                // `mysqladmin shutdown` issues SHUTDOWN as text (COM_QUERY),
                // not COM_SHUTDOWN: same graceful path, authenticated only
                // (per-user privileges are the SEC8 follow-up).
                if sql.eq_ignore_ascii_case("shutdown") {
                    println!("shutdown requested by '{authed_user}' from {peer}");
                    match db.checkpoint() {
                        Ok(()) => {
                            write_packet(reader.get_mut(), &ok_payload(0, ""), &mut out_seq)?;
                            reader.get_mut().flush()?;
                        }
                        Err(e) => write_err(reader.get_mut(), &mut out_seq, &e)?,
                    }
                    ctx.shutdown.store(true, Ordering::Relaxed);
                    break;
                }
                // Multi-statement text (client `-e "A; B"` with
                // MULTI_STATEMENTS): execute in order, stream one
                // resultset/OK per statement.
                let batch = split_statements(sql);
                let batch = if batch.is_empty() { vec![sql.to_string()] } else { batch };
                execute_statements(&db, &mut session, &batch, reader.get_mut(), &mut out_seq, deprecate_eof, false)?;
            }
            COM_STMT_PREPARE => {
                let sql = String::from_utf8_lossy(&payload[1..]).into_owned();
                let sql = sql.trim().to_string();
                if sql.is_empty() {
                    write_err_msg(reader.get_mut(), &mut out_seq, 1065, "empty statement")?;
                    continue;
                }
                if split_statements(&sql).len() > 1 {
                    write_err_msg(reader.get_mut(), &mut out_seq, 1064, "multi-statement prepare not supported")?;
                    continue;
                }
                let offsets = find_placeholders(&sql);
                if offsets.len() > MAX_PARAMS_PER_STMT {
                    write_err_msg(reader.get_mut(), &mut out_seq, 1064, "too many parameters")?;
                    continue;
                }
                let neutral = neutralize_placeholders(&sql, &offsets);
                match db.describe(&session, &neutral) {
                    Ok(cols) => {
                        if stmts.len() >= MAX_PREPARED_PER_CONN {
                            write_err_msg(reader.get_mut(), &mut out_seq, 1047, "too many prepared statements")?;
                            continue;
                        }
                        let id = next_stmt_id;
                        next_stmt_id = next_stmt_id.wrapping_add(1).max(1);
                        let num_params = offsets.len();
                        stmts.insert(id, Prepared::new(sql, offsets));
                        write_packet(reader.get_mut(), &prepare_ok_payload(id, cols.len(), num_params), &mut out_seq)?;
                        if num_params > 0 {
                            for _ in 0..num_params {
                                write_packet(reader.get_mut(), &column_def_payload("?", TYPE_VAR_STRING), &mut out_seq)?;
                            }
                            if !deprecate_eof {
                                write_packet(reader.get_mut(), &eof_payload(), &mut out_seq)?;
                            }
                        }
                        for (name, ctype) in &cols {
                            write_packet(reader.get_mut(), &column_def_payload(name, schema_col_type(*ctype)), &mut out_seq)?;
                        }
                        if !cols.is_empty() && !deprecate_eof {
                            write_packet(reader.get_mut(), &eof_payload(), &mut out_seq)?;
                        }
                        reader.get_mut().flush()?;
                    }
                    Err(e) => write_err(reader.get_mut(), &mut out_seq, &e)?,
                }
            }
            COM_STMT_EXECUTE => {
                if payload.len() < 10 {
                    write_err_msg(reader.get_mut(), &mut out_seq, 1064, "malformed EXECUTE packet")?;
                    continue;
                }
                let id = u32::from_le_bytes([payload[1], payload[2], payload[3], payload[4]]);
                let Some(ps) = stmts.get_mut(&id) else {
                    write_err_msg(reader.get_mut(), &mut out_seq, 1243, "unknown prepared statement")?;
                    continue;
                };
                if ps.long_overflow {
                    write_err_msg(reader.get_mut(), &mut out_seq, 1105, "statement long data too large")?;
                    continue;
                }
                let num_params = ps.num_params();
                match decode_execute_params(&payload[10..], num_params, &ps.param_types, &ps.long_data) {
                    Ok((values, types)) => {
                        let lits: Vec<String> = values.iter().map(datum_literal).collect();
                        match substitute(&ps.sql, &ps.offsets, &lits) {
                            Ok(final_sql) => {
                                ps.param_types = Some(types);
                                ps.reset_long_data();
                                let batch = split_statements(&final_sql);
                                let batch = if batch.is_empty() { vec![final_sql] } else { batch };
                                execute_statements(&db, &mut session, &batch, reader.get_mut(), &mut out_seq, deprecate_eof, true)?;
                            }
                            Err(msg) => write_err_msg(reader.get_mut(), &mut out_seq, 1064, &msg)?,
                        }
                    }
                    Err(msg) => write_err_msg(reader.get_mut(), &mut out_seq, 1064, &msg)?,
                }
            }
            COM_STMT_SEND_LONG_DATA => {
                // [0x18][stmt_id u32][param_idx u16][data...]: no response.
                if payload.len() >= 7 {
                    let id = u32::from_le_bytes([payload[1], payload[2], payload[3], payload[4]]);
                    let idx = u16::from_le_bytes([payload[5], payload[6]]) as usize;
                    if let Some(ps) = stmts.get_mut(&id) {
                        if idx < ps.long_data.len() {
                            if ps.long_data[idx].len() + payload[7..].len() > MAX_LONG_DATA_PER_PARAM {
                                ps.long_overflow = true;
                            } else {
                                ps.long_data[idx].extend_from_slice(&payload[7..]);
                            }
                        }
                    }
                }
            }
            COM_STMT_CLOSE => {
                if payload.len() >= 5 {
                    let id = u32::from_le_bytes([payload[1], payload[2], payload[3], payload[4]]);
                    stmts.remove(&id);
                }
                // No response packet.
            }
            COM_STMT_RESET => {
                if payload.len() >= 5 {
                    let id = u32::from_le_bytes([payload[1], payload[2], payload[3], payload[4]]);
                    match stmts.get_mut(&id) {
                        Some(ps) => {
                            ps.reset_long_data();
                            write_packet(reader.get_mut(), &ok_payload(0, ""), &mut out_seq)?;
                            reader.get_mut().flush()?;
                        }
                        None => write_err_msg(reader.get_mut(), &mut out_seq, 1243, "unknown prepared statement")?,
                    }
                } else {
                    write_err_msg(reader.get_mut(), &mut out_seq, 1064, "malformed RESET packet")?;
                }
            }
            COM_STMT_FETCH => {
                write_err_msg(reader.get_mut(), &mut out_seq, 1047, "server-side cursors not supported")?;
            }
            other => {
                let msg = format!("unsupported command 0x{other:02X}");
                write_err_msg(reader.get_mut(), &mut out_seq, 1047, &msg)?;
            }
        }
    }
    println!("mysql disconnected: {peer}");
    Ok(())
}
