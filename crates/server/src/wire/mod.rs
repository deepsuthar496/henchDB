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
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use engine::Database;

use crate::auth::{self, UserStore};

pub mod canned;
pub mod constants;
pub mod handshake;
pub mod packet;
pub mod pg;
pub mod stmt;
pub mod tls;

#[cfg(test)]
mod tests;
#[cfg(test)]
mod fuzz;

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

/// RAII processlist entry: registers the connection on creation,
/// unregisters on drop (all exits covered). Shared by the MySQL, PG, and
/// legacy frontends.
pub(crate) struct ProcGuard {
    db: Arc<Database>,
    id: u64,
}

impl ProcGuard {
    pub(crate) fn register(db: &Arc<Database>, user: &str, host: &str) -> Self {
        let id = db.register_process(user, host);
        ProcGuard { db: db.clone(), id }
    }

    pub(crate) fn id(&self) -> u64 {
        self.id
    }
}

impl Drop for ProcGuard {
    fn drop(&mut self) {
        self.db.unregister_process(self.id);
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
    /// TLS config when `--tls-cert`/`--tls-key` were given. `None` means
    /// plaintext only (SSLRequest fails closed with an error).
    pub tls: Option<Arc<rustls::ServerConfig>>,
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

/// MySQL session state carried across pool worker handoffs (Priority 11).
/// The `reader` (with its already-buffered pipeline bytes), the engine
/// session, prepared statements, and the processlist guard all live here so
/// a connection parks and resumes without losing protocol position.
pub(crate) struct MysqlSession {
    reader: BufReader<ConnStream>,
    session: engine::Session,
    stmts: HashMap<u32, Prepared>,
    next_stmt_id: u32,
    deprecate_eof: bool,
    authed_user: String,
    peer: String,
    proc: ProcGuard,
}

impl MysqlSession {
    /// Underlying transport (poller peeks / toggles blocking through this).
    pub(crate) fn stream(&self) -> &ConnStream {
        self.reader.get_ref()
    }

    /// Already-buffered bytes beyond the last consumed packet (lets the
    /// worker drain pipelined commands without a poller round trip).
    pub(crate) fn buffered(&self) -> &[u8] {
        self.reader.buffer()
    }
}

/// Blocking handshake + authentication for one MySQL-protocol connection.
/// Runs once on a pool worker (bounded by the 30s pre-auth timeout), then
/// the session parks in the poller until real commands arrive.
/// `Ok(None)` = quiet close (peer went away mid-handshake); the pool drops
/// the connection either way.
pub(crate) fn mysql_establish(
    db: Arc<Database>,
    stream: TcpStream,
    ctx: &ConnCtx,
    admitted: bool,
) -> std::io::Result<Option<MysqlSession>> {
    let peer = stream.peer_addr().unwrap_or_else(|_| "unknown:0".parse().unwrap());
    let peer_host = peer.ip().to_string();
    let mut stream = stream;
    stream.set_nodelay(true)?;
    let _ = stream.set_read_timeout(None);
    let mut session = db.new_session();
    let stmts: HashMap<u32, Prepared> = HashMap::new();
    let next_stmt_id: u32 = 1;

    // -- Handshake (fresh 20-byte scramble per connection: replay across
    // sessions is impossible even though the scramble travels in clear). --
    let conn_id = std::process::id() ^ (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0));
    let scramble = fresh_scramble();
    let mut sseq: u8 = 0;
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
            Err(_) => return Ok(None),
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
                    return Ok(None);
                }
            },
            None => {
                let _ = write_packet(
                    &mut stream,
                    &err_payload(1047, "HY000", "SSL requested but the server has no TLS certificate configured"),
                    &mut sseq,
                );
                let _ = stream.flush();
                return Ok(None);
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
            Err(_) => return Ok(None),
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
    if !admitted {
        eprintln!("connection refused (max_connections): {peer}");
        write_packet(reader.get_mut(), &err_payload(1040, "HY000", "Too many connections"), &mut sseq)?;
        reader.get_mut().flush()?;
        return Ok(None);
    }
    let authed_user = match hs {
        Some(hs) if !hs.username.is_empty() => {
            // Fail closed when the auth store is unreadable.
            let store = match UserStore::load(&ctx.auth_path) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("auth store unavailable for {peer}: {e}");
                    let p = access_denied(&hs.username, &peer_host, !hs.auth.is_empty());
                    write_packet(reader.get_mut(), &p, &mut sseq)?;
                    reader.get_mut().flush()?;
                    return Ok(None);
                }
            };
            // Unknown users fail exactly like wrong passwords (no user enumeration).
            let Some(user_def) = store.users.get(&hs.username) else {
                eprintln!("access denied for '{}' from {peer}", hs.username);
                let p = access_denied(&hs.username, &peer_host, !hs.auth.is_empty() && hs.auth != [0]);
                write_packet(reader.get_mut(), &p, &mut sseq)?;
                reader.get_mut().flush()?;
                return Ok(None);
            };

            let client_plugin = if hs.plugin.is_empty() { AUTH_PLUGIN } else { hs.plugin.as_str() };
            let target_plugin = &user_def.plugin;
            let mut proof = hs.auth.clone();
            let mut did_auth_switch = false;

            // When the client offered credentials under a different plugin than
            // the account requires, or the client sent empty proof anticipating a
            // switch (and the account has a password), prompt the client
            // via AuthSwitchRequest (0xFE + plugin + scramble) to submit proof
            // for the account's required plugin.
            if !user_def.hash.is_empty()
                && (client_plugin != target_plugin || proof.is_empty())
                && (target_plugin == auth::PLUGIN_NATIVE || target_plugin == auth::PLUGIN_CACHING_SHA2)
            {
                let mut sw = vec![0xFE];
                sw.extend_from_slice(target_plugin.as_bytes());
                sw.push(0);
                sw.extend_from_slice(&scramble);
                sw.push(0);
                write_packet(reader.get_mut(), &sw, &mut sseq)?;
                reader.get_mut().flush()?;
                let (cseq2, sw_resp) = match read_packet(&mut reader, 16 * 1024 * 1024) {
                    Ok(v) => v,
                    Err(_) => return Ok(None),
                };
                sseq = cseq2.wrapping_add(1);
                proof = sw_resp;
                did_auth_switch = true;
            }

            let using_password = (!hs.auth.is_empty() && hs.auth != [0]) || (!proof.is_empty() && proof != [0]);
            let ok = auth::verify(user_def, target_plugin, &scramble, &proof);
            if !ok {
                eprintln!("access denied for '{}' from {peer}", hs.username);
                let p = access_denied(&hs.username, &peer_host, using_password);
                write_packet(reader.get_mut(), &p, &mut sseq)?;
                reader.get_mut().flush()?;
                return Ok(None);
            }
            // For caching_sha2_password negotiated via AuthSwitch, the client expects
            // a fast_auth_success indicator (0x03) before the OK packet.
            if did_auth_switch && target_plugin == auth::PLUGIN_CACHING_SHA2 {
                write_packet(reader.get_mut(), &[0x03], &mut sseq)?;
                reader.get_mut().flush()?;
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
            return Ok(None);
        }
    };
    write_packet(reader.get_mut(), &ok_payload(0, ""), &mut sseq)?;
    reader.get_mut().flush()?;
    println!("mysql connected: {peer} as '{authed_user}'");
    session.user = authed_user.clone();
    // Processlist entry for SHOW PROCESSLIST / Threads_connected.
    let proc = ProcGuard::register(&db, &authed_user, &peer_host);
    // Idle timeout from here on (handshake already completed); a quiet
    // connection is reaped instead of held forever.
    if let Some(d) = ctx.idle_timeout {
        let _ = reader.get_mut().set_read_timeout(Some(d));
    } else {
        let _ = reader.get_mut().set_read_timeout(None);
    }
    Ok(Some(MysqlSession {
        reader,
        session,
        stmts,
        next_stmt_id,
        deprecate_eof,
        authed_user,
        peer: peer.to_string(),
        proc,
    }))
}

/// One command-phase step: read a single packet, dispatch it, and report
/// whether the connection parks (`Idle`) or closes (`Closed`). Reads block
/// up to the idle timeout exactly like the old per-connection thread, so a
/// quiet client closes here while an active one keeps its worker.
pub(crate) fn mysql_step(
    db: &Arc<Database>,
    m: &mut MysqlSession,
    ctx: &ConnCtx,
) -> std::io::Result<crate::net::Disposition> {
    use crate::net::Disposition::{Closed, Idle};
    // Disjoint field borrows: the reader, session, and statement map move
    // independently while `m` stays whole for the copy fields below.
    let reader = &mut m.reader;
    let session = &mut m.session;
    let stmts = &mut m.stmts;
    let next_stmt_id = &mut m.next_stmt_id;
    let proc = &m.proc;
    let deprecate_eof = m.deprecate_eof;
    if ctx.shutdown.load(Ordering::Relaxed) {
        return Ok(Closed); // draining: listener is closing, finish promptly
    }
    let (cseq, payload) = match read_packet(reader, 16 * 1024 * 1024) {
        Ok(v) => v,
        Err(e) if e.kind() == std::io::ErrorKind::TimedOut => return Ok(Closed), // idle timeout
        Err(_) => return Ok(Closed), // client closed
    };
        if payload.is_empty() {
            return Ok(Idle);
        }
        let mut out_seq = cseq.wrapping_add(1);
        match payload[0] {
            COM_QUIT => return Ok(Closed),
            COM_SHUTDOWN => {
                // Shutdown needs admin rights (root or global ALL); the
                // engine session carries the authenticated username.
                if !db.is_admin(&session.user) {
                    write_err(
                        reader.get_mut(),
                        &mut out_seq,
                        &engine::Error::AccessDenied {
                            user: session.user.clone(),
                            command: "SHUTDOWN".into(),
                            object: "*.*".into(),
                        },
                    )?;
                    return Ok(Idle);
                }
                println!("shutdown requested by '{}' from {}", m.authed_user, m.peer);
                match db.checkpoint() {
                    Ok(()) => {
                        write_packet(reader.get_mut(), &ok_payload(0, ""), &mut out_seq)?;
                        reader.get_mut().flush()?;
                    }
                    Err(e) => write_err(reader.get_mut(), &mut out_seq, &e)?,
                }
                ctx.shutdown.store(true, Ordering::Relaxed);
                return Ok(Closed);
            }
            COM_INIT_DB => {
                let db_name = String::from_utf8_lossy(&payload[1..])
                    .trim_matches('\0')
                    .trim()
                    .to_string();
                if db_name.is_empty() {
                    write_err_msg(reader.get_mut(), &mut out_seq, 1049, "Unknown database ''")?;
                } else {
                    match db.execute(session, &format!("USE `{db_name}`")) {
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
                // Session state resets, but the connection stays
                // authenticated as the login user (resetting to root would
                // silently escalate privileges).
                let user = std::mem::take(&mut session.user);
                *session = db.new_session();
                session.user = user;
                write_packet(reader.get_mut(), &ok_payload(0, ""), &mut out_seq)?;
                reader.get_mut().flush()?;
            }
            COM_QUERY => {
                let sql = String::from_utf8_lossy(&payload[1..]).into_owned();
                let sql = sql.trim();
                if sql.is_empty() {
                    write_err_msg(reader.get_mut(), &mut out_seq, 1065, "empty query")?;
                    return Ok(Idle);
                }
                // `mysqladmin shutdown` issues SHUTDOWN as text (COM_QUERY),
                // not COM_SHUTDOWN: same graceful path, admin rights required.
                if sql.eq_ignore_ascii_case("shutdown") {
                    if !db.is_admin(&session.user) {
                        write_err(
                            reader.get_mut(),
                            &mut out_seq,
                            &engine::Error::AccessDenied {
                                user: session.user.clone(),
                                command: "SHUTDOWN".into(),
                                object: "*.*".into(),
                            },
                        )?;
                        return Ok(Idle);
                    }
                    println!("shutdown requested by '{}' from {}", m.authed_user, m.peer);
                    match db.checkpoint() {
                        Ok(()) => {
                            write_packet(reader.get_mut(), &ok_payload(0, ""), &mut out_seq)?;
                            reader.get_mut().flush()?;
                        }
                        Err(e) => write_err(reader.get_mut(), &mut out_seq, &e)?,
                    }
                    ctx.shutdown.store(true, Ordering::Relaxed);
                    return Ok(Closed);
                }
                // Multi-statement text (client `-e "A; B"` with
                // MULTI_STATEMENTS): execute in order, stream one
                // resultset/OK per statement.
                let batch = split_statements(sql);
                let batch = if batch.is_empty() { vec![sql.to_string()] } else { batch };
                db.note_command(proc.id(), &session.current_db, "Query", batch.first().map(String::as_str).unwrap_or(sql));
                let v0 = db.privilege_version();
                execute_statements(&db, session, &batch, reader.get_mut(), &mut out_seq, deprecate_eof, false)?;
                auth::persist_if_changed(&db, &ctx.auth_path, v0);
                db.note_idle(proc.id());
            }
            COM_STMT_PREPARE => {
                let sql = String::from_utf8_lossy(&payload[1..]).into_owned();
                let sql = sql.trim().to_string();
                if sql.is_empty() {
                    write_err_msg(reader.get_mut(), &mut out_seq, 1065, "empty statement")?;
                    return Ok(Idle);
                }
                if split_statements(&sql).len() > 1 {
                    write_err_msg(reader.get_mut(), &mut out_seq, 1064, "multi-statement prepare not supported")?;
                    return Ok(Idle);
                }
                let offsets = find_placeholders(&sql);
                if offsets.len() > MAX_PARAMS_PER_STMT {
                    write_err_msg(reader.get_mut(), &mut out_seq, 1064, "too many parameters")?;
                    return Ok(Idle);
                }
                let neutral = neutralize_placeholders(&sql, &offsets);
                match db.describe(&session, &neutral) {
                    Ok(cols) => {
                        if stmts.len() >= MAX_PREPARED_PER_CONN {
                            write_err_msg(reader.get_mut(), &mut out_seq, 1047, "too many prepared statements")?;
                            return Ok(Idle);
                        }
                        let id = *next_stmt_id;
                        *next_stmt_id = next_stmt_id.wrapping_add(1).max(1);
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
                    return Ok(Idle);
                }
                let id = u32::from_le_bytes([payload[1], payload[2], payload[3], payload[4]]);
                let Some(ps) = stmts.get_mut(&id) else {
                    write_err_msg(reader.get_mut(), &mut out_seq, 1243, "unknown prepared statement")?;
                    return Ok(Idle);
                };
                if ps.long_overflow {
                    write_err_msg(reader.get_mut(), &mut out_seq, 1105, "statement long data too large")?;
                    return Ok(Idle);
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
                                db.note_command(proc.id(), &session.current_db, "Execute", batch.first().map(String::as_str).unwrap_or(""));
                                let v0 = db.privilege_version();
                                execute_statements(&db, session, &batch, reader.get_mut(), &mut out_seq, deprecate_eof, true)?;
                                auth::persist_if_changed(&db, &ctx.auth_path, v0);
                                db.note_idle(proc.id());
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
        Ok(Idle)
    }
