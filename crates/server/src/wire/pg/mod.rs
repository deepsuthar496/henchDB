//! PostgreSQL wire protocol 3.0 frontend (simple query protocol).
//!
//! Native PG clients (`psql`, DBeaver in simple mode, `pg8000`) connect on
//! `--pg-port` and run the standard startup handshake: optional SSLRequest,
//! StartupMessage, cleartext-password auth against `auth.bin`, then
//! ParameterStatus / BackendKeyData / ReadyForQuery. Queries arrive as
//! `'Q'` simple-protocol messages and execute through the same engine
//! executor as the MySQL wire (text format rows).
//!
//! Not yet supported (clean `0A000` errors, connection stays alive): the
//! extended protocol (Parse/Bind/Describe/Execute/Sync), COPY, cursors,
//! LISTEN/NOTIFY, and SASL/SCRAM auth. Passwords travel cleartext inside TLS
//! when `--tls-cert`/`--tls-key` are configured; without TLS they cross the
//! socket as-is (same exposure class as the legacy framed protocol).

pub mod codec;
pub mod copy;
pub mod exec;
#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::io::{BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use engine::Database;

use super::canned::{normalize_dialect, split_statements};
use super::tls::{self, ConnStream};
use super::ConnCtx;
use crate::auth::{self, UserStore};

use codec::*;

/// Read one length-prefixed startup packet (no type byte): `[u32 BE len]`
/// followed by `len - 4` payload bytes.
fn read_startup_body<R: Read>(reader: &mut BufReader<R>) -> std::io::Result<Vec<u8>> {
    let mut h = [0u8; 4];
    reader.read_exact(&mut h)?;
    let len = u32::from_be_bytes(h) as usize;
    if len < 8 || len > 16 * 1024 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "bad pg startup length",
        ));
    }
    let mut body = vec![0u8; len - 4];
    reader.read_exact(&mut body)?;
    Ok(body)
}

/// Read one typed frontend message: `[type][u32 BE len][payload]`.
fn read_message<R: Read>(reader: &mut BufReader<R>) -> std::io::Result<(u8, Vec<u8>)> {
    let mut t = [0u8; 1];
    reader.read_exact(&mut t)?;
    let mut h = [0u8; 4];
    reader.read_exact(&mut h)?;
    let len = u32::from_be_bytes(h) as usize;
    if len < 4 || len > 16 * 1024 * 1024 + 4 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "bad pg message length",
        ));
    }
    let mut payload = vec![0u8; len - 4];
    reader.read_exact(&mut payload)?;
    Ok((t[0], payload))
}

fn access_denied(user: &str) -> Vec<u8> {
    error_response(
        "28P01",
        &format!("password authentication failed for user \"{user}\""),
    )
}

/// Verify a cleartext password against the stored verifier.
fn verify_password(v: &auth::Verifier, password: &[u8]) -> bool {
    if v.hash.is_empty() {
        return password.is_empty();
    }
    if v.plugin == auth::PLUGIN_CACHING_SHA2 && v.hash.len() == 32 {
        let expect = <[u8; 32]>::try_from(v.hash.as_slice()).unwrap_or([0u8; 32]);
        return auth::sha256(password) == expect;
    }
    if v.plugin == auth::PLUGIN_NATIVE && v.hash.len() == 20 {
        let expect = <[u8; 20]>::try_from(v.hash.as_slice()).unwrap_or([0u8; 20]);
        return auth::sha1(&auth::sha1(password)) == expect;
    }
    false
}

/// Serve one PostgreSQL-protocol connection until Terminate, error, or close.
pub fn handle_pg_connection(
    db: Arc<Database>,
    stream: TcpStream,
    ctx: &ConnCtx,
) -> std::io::Result<()> {
    let peer = stream.peer_addr().unwrap_or_else(|_| "unknown:0".parse().unwrap());
    let mut stream = stream;
    let _ = stream.set_read_timeout(Some(Duration::from_secs(30)));

    // -- Phase 1: first packet on the raw socket (SSLRequest or startup). --
    let first = {
        let mut pre = BufReader::new(&mut stream);
        match read_startup_body(&mut pre) {
            Ok(v) => v,
            Err(_) => return Ok(()),
        }
    };
    // -- Phase 2: optional TLS upgrade, then a uniform ConnStream. --
    let mut need_startup = false;
    let mut reader: BufReader<ConnStream> = if first.len() == 4 {
        let code = u32::from_be_bytes([first[0], first[1], first[2], first[3]]);
        if code == SSL_REQUEST_CODE {
            match &ctx.tls {
                Some(cfg) => {
                    stream.write_all(b"S")?;
                    stream.flush()?;
                    match tls::accept_tls(cfg, stream) {
                        Ok(t) => {
                            need_startup = true;
                            BufReader::new(ConnStream::Tls(Box::new(t)))
                        }
                        Err(e) => {
                            eprintln!("pg tls handshake failed for {peer}: {e}");
                            return Ok(());
                        }
                    }
                }
                None => {
                    stream.write_all(b"N")?;
                    stream.flush()?;
                    need_startup = true;
                    BufReader::new(ConnStream::Plain(stream))
                }
            }
        } else if code == GSS_REQUEST_CODE {
            stream.write_all(b"N")?;
            stream.flush()?;
            need_startup = true;
            BufReader::new(ConnStream::Plain(stream))
        } else {
            let _ = stream.write_all(&error_response("08006", "unsupported protocol version"));
            return Ok(());
        }
    } else {
        BufReader::new(ConnStream::Plain(stream))
    };
    let body = if need_startup {
        match read_startup_body(&mut reader) {
            Ok(v) => v,
            Err(_) => return Ok(()),
        }
    } else {
        first
    };
    let params = match parse_startup(&body) {
        Some(p) => p,
        None => {
            let _ = reader.get_mut().write_all(&error_response("08006", "invalid startup packet"));
            return Ok(());
        }
    };
    if params.user.is_empty() {
        let _ = reader.get_mut().write_all(&access_denied(""));
        return Ok(());
    }
    pg_session(db, &mut reader, ctx, &peer, &params)
}

/// Post-startup session over an established (plaintext or TLS) stream.
fn pg_session(
    db: Arc<Database>,
    reader: &mut BufReader<ConnStream>,
    ctx: &ConnCtx,
    peer: &std::net::SocketAddr,
    params: &StartupParams,
) -> std::io::Result<()> {
    let mut session = db.new_session();

    if !ctx.admitted {
        eprintln!("pg connection refused (max_connections): {peer}");
        let w = reader.get_mut();
        w.write_all(&error_response("53300", "too many connections"))?;
        w.flush()?;
        return Ok(());
    }
    // Authenticate against auth.bin (fail closed on store errors).
    let store = match UserStore::load(&ctx.auth_path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("pg auth store unavailable for {peer}: {e}");
            let w = reader.get_mut();
            w.write_all(&access_denied(&params.user))?;
            w.flush()?;
            return Ok(());
        }
    };
    let authed_user = match store.users.get(&params.user) {
        None => {
            eprintln!("pg access denied for '{}' from {peer}", params.user);
            let w = reader.get_mut();
            w.write_all(&access_denied(&params.user))?;
            w.flush()?;
            return Ok(());
        }
        Some(v) if v.hash.is_empty() => {
            reader.get_mut().write_all(&auth_message(AUTH_OK))?;
            params.user.clone()
        }
        Some(v) => {
            {
                let w = reader.get_mut();
                w.write_all(&auth_message(AUTH_CLEARTEXT))?;
                w.flush()?;
            }
            let (t, payload) = match read_message(reader) {
                Ok(m) => m,
                Err(_) => return Ok(()),
            };
            if t != MSG_PASSWORD {
                let w = reader.get_mut();
                w.write_all(&access_denied(&params.user))?;
                w.flush()?;
                return Ok(());
            }
            let password = payload.strip_suffix(&[0]).unwrap_or(&payload);
            if !verify_password(v, password) {
                eprintln!("pg access denied for '{}' from {peer}", params.user);
                let w = reader.get_mut();
                w.write_all(&access_denied(&params.user))?;
                w.flush()?;
                return Ok(());
            }
            reader.get_mut().write_all(&auth_message(AUTH_OK))?;
            params.user.clone()
        }
    };
    if !params.database.is_empty() {
        let _ = db.execute(&mut session, &format!("USE `{}`", params.database));
    }
    // Initialization burst.
    let mut init = Vec::new();
    let version = format!("18.0 ({} {})", engine::PRODUCT_NAME, env!("CARGO_PKG_VERSION"));
    for (k, v) in [
        ("server_version", version.as_str()),
        ("client_encoding", "UTF8"),
        ("server_encoding", "UTF8"),
        ("standard_conforming_strings", "on"),
        ("TimeZone", "UTC"),
        ("integer_datetimes", "on"),
    ] {
        init.extend_from_slice(&parameter_status(k, v));
    }
    let pid = std::process::id() as i32;
    let secret = {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut h = DefaultHasher::new();
        peer.hash(&mut h);
        std::time::SystemTime::now().hash(&mut h);
        h.finish() as i32
    };
    init.extend_from_slice(&backend_key_data(pid, secret));
    init.extend_from_slice(&ready_for_query(b'I'));
    let w = reader.get_mut();
    w.write_all(&init)?;
    w.flush()?;
    println!("pg connected: {peer} as '{authed_user}'");

    // Idle timeout from here on (handshake already completed).
    let _ = reader.get_mut().set_read_timeout(ctx.idle_timeout);
    // Extended-protocol state (PG2): statements, portals, error barrier.
    let mut pg = exec::PgConn::default();
    // -- Simple query loop. --
    loop {
        if ctx.shutdown.load(std::sync::atomic::Ordering::Relaxed) {
            break;
        }
        let (t, payload) = match read_message(reader) {
            Ok(v) => v,
            Err(e) if e.kind() == std::io::ErrorKind::TimedOut => break,
            Err(_) => break,
        };
        // After an extended-flow error, skip everything until Sync (Flush
        // still flushes, Terminate still closes).
        if pg.failed && !matches!(t, MSG_SYNC | MSG_FLUSH | MSG_TERMINATE) {
            continue;
        }
        match t {
            MSG_TERMINATE => break,
            MSG_QUERY => {
                // Simple Query resets extended error state and the unnamed
                // statement/portal (spec behavior).
                pg.failed = false;
                pg.stmts.remove("");
                pg.portals.remove("");
                let sql = read_cstring(&payload).unwrap_or_default();
                if copy::is_copy_from_stdin(&sql) {
                    run_copy_in(&db, &mut session, &sql, reader, ctx)?;
                } else {
                    run_simple(&db, &mut session, &sql, reader)?;
                }
            }
            MSG_PARSE => {
                let w = reader.get_mut();
                match parse_parse_msg(&payload) {
                    Some(m) => match pg.on_parse(&m) {
                        Ok(resp) => {
                            w.write_all(&resp)?;
                            w.flush()?;
                        }
                        Err((code, msg)) => {
                            pg.failed = true;
                            w.write_all(&error_response(&code, &msg))?;
                            w.flush()?;
                        }
                    },
                    None => {
                        pg.failed = true;
                        w.write_all(&error_response("08P01", "malformed Parse message"))?;
                        w.flush()?;
                    }
                }
            }
            MSG_BIND => {
                let w = reader.get_mut();
                match parse_bind_msg(&payload) {
                    Some(m) => match pg.on_bind(&m) {
                        Ok(resp) => {
                            w.write_all(&resp)?;
                            w.flush()?;
                        }
                        Err((code, msg)) => {
                            pg.failed = true;
                            w.write_all(&error_response(&code, &msg))?;
                            w.flush()?;
                        }
                    },
                    None => {
                        pg.failed = true;
                        w.write_all(&error_response("08P01", "malformed Bind message"))?;
                        w.flush()?;
                    }
                }
            }
            MSG_DESCRIBE => {
                let w = reader.get_mut();
                match parse_describe_msg(&payload) {
                    Some(m) => match pg.on_describe(&db, &session, &m) {
                        Ok(resp) => {
                            w.write_all(&resp)?;
                            w.flush()?;
                        }
                        Err((code, msg)) => {
                            pg.failed = true;
                            w.write_all(&error_response(&code, &msg))?;
                            w.flush()?;
                        }
                    },
                    None => {
                        pg.failed = true;
                        w.write_all(&error_response("08P01", "malformed Describe message"))?;
                        w.flush()?;
                    }
                }
            }
            MSG_EXECUTE => {
                let w = reader.get_mut();
                match parse_execute_msg(&payload) {
                    Some(m) => match pg.on_execute(&db, &mut session, &m) {
                        Ok(resp) => {
                            w.write_all(&resp)?;
                            w.flush()?;
                        }
                        Err((code, msg)) => {
                            pg.failed = true;
                            w.write_all(&error_response(&code, &msg))?;
                            w.flush()?;
                        }
                    },
                    None => {
                        pg.failed = true;
                        w.write_all(&error_response("08P01", "malformed Execute message"))?;
                        w.flush()?;
                    }
                }
            }
            MSG_CLOSE => match parse_close_msg(&payload) {
                Some(m) => match pg.on_close(&m) {
                    Ok(resp) => {
                        let w = reader.get_mut();
                        w.write_all(&resp)?;
                        w.flush()?;
                    }
                    Err((code, msg)) => {
                        pg.failed = true;
                        let w = reader.get_mut();
                        w.write_all(&error_response(&code, &msg))?;
                        w.flush()?;
                    }
                },
                None => {
                    pg.failed = true;
                    let w = reader.get_mut();
                    w.write_all(&error_response("08P01", "malformed Close message"))?;
                    w.flush()?;
                }
            },
            MSG_SYNC => {
                pg.failed = false;
                let w = reader.get_mut();
                w.write_all(&ready_for_query(status(&session)))?;
                w.flush()?;
            }
            MSG_FLUSH => {
                reader.get_mut().flush()?;
            }
            // Stray COPY messages outside a copy (e.g. a client's CopyFail
            // racing our own abort): ignore to stay message-aligned.
            MSG_COPY_DATA | MSG_COPY_DONE | MSG_COPY_FAIL => {}
            _ => {
                // Anything else (COPY, replication, SASL) is a documented
                // follow-up; fail per-message without killing the connection.
                let w = reader.get_mut();
                w.write_all(&error_response(
                    "0A000",
                    "message type not supported; use simple or extended query protocol",
                ))?;
                w.write_all(&ready_for_query(status(&session)))?;
                w.flush()?;
            }
        }
    }
    println!("pg disconnected: {peer}");
    Ok(())
}

fn status(session: &engine::Session) -> u8 {
    if session.in_transaction() {
        b'T'
    } else {
        b'I'
    }
}

/// Run one simple-protocol query string (possibly multi-statement).
fn run_simple(
    db: &Arc<Database>,
    session: &mut engine::Session,
    sql: &str,
    reader: &mut BufReader<ConnStream>,
) -> std::io::Result<()> {
    let batch = split_statements(sql);
    let mut out = Vec::new();
    let mut any = false;
    for stmt in &batch {
        let text = normalize_dialect(stmt.trim());
        if text.is_empty() {
            continue;
        }
        any = true;
        match db.execute(session, text) {
            Ok(result) => {
                if result.columns.is_empty() {
                    out.extend_from_slice(&command_complete(&command_tag(
                        &result.message,
                        result.rows.len(),
                        false,
                    )));
                } else {
                    // Column types come from prepare-time describe; fall back
                    // to TEXT when describe rejects a runnable statement.
                    let types: HashMap<String, engine::types::ColumnType> = db
                        .describe(session, text)
                        .map(|cols| cols.into_iter().collect())
                        .unwrap_or_default();
                    let desc: Vec<(String, engine::types::ColumnType)> = result
                        .columns
                        .iter()
                        .map(|c| {
                            let bare = c.rsplit('.').next().unwrap_or(c);
                            let ct = types
                                .get(c)
                                .or_else(|| types.get(bare))
                                .copied()
                                .unwrap_or(engine::types::ColumnType::Text);
                            (c.clone(), ct)
                        })
                        .collect();
                    out.extend_from_slice(&row_description(&desc));
                    for row in &result.rows {
                        let cells: Vec<Option<Vec<u8>>> =
                            row.iter().map(datum_text).collect();
                        out.extend_from_slice(&data_row(&cells));
                    }
                    out.extend_from_slice(&command_complete(&command_tag(
                        &result.message,
                        result.rows.len(),
                        true,
                    )));
                }
            }
            Err(e) => {
                out.extend_from_slice(&error_response(sqlstate(&e), &e.to_string()));
                break; // subsequent statements of this Q are skipped
            }
        }
    }
    if !any {
        out.extend_from_slice(&empty_query_response());
    }
    out.extend_from_slice(&ready_for_query(status(session)));
    let w = reader.get_mut();
    w.write_all(&out)?;
    w.flush()?;
    Ok(())
}

/// Discard in-flight CopyData until the client's CopyDone/CopyFail (or
/// disconnect). Returns the terminator message type when clean.
fn drain_copy_in(reader: &mut BufReader<ConnStream>) -> Option<u8> {
    loop {
        match read_message(reader) {
            Ok((MSG_COPY_DATA, _)) => continue,
            Ok((t, _)) => return Some(t),
            Err(_) => return None,
        }
    }
}

/// Run one `COPY ... FROM STDIN` statement: CopyInResponse, then the
/// CopyData/CopyDone/CopyFail ingestion loop.
fn run_copy_in(
    db: &Arc<Database>,
    session: &mut engine::Session,
    sql: &str,
    reader: &mut BufReader<ConnStream>,
    ctx: &ConnCtx,
) -> std::io::Result<()> {
    let fail = |w: &mut ConnStream, code: &str, msg: &str, txn: bool| -> std::io::Result<()> {
        w.write_all(&error_response(code, msg))?;
        w.write_all(&ready_for_query(if txn { b'T' } else { b'I' }))?;
        w.flush()?;
        Ok(())
    };
    // COPY must travel alone: trailing statements would be eaten as data.
    let parts: Vec<String> = split_statements(sql)
        .into_iter()
        .map(|s| normalize_dialect(s.trim()).to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if parts.len() != 1 {
        let w = reader.get_mut();
        return fail(w, "42601", "COPY must be the only statement in the query", session.in_transaction());
    }
    let spec = match copy::parse_copy(&parts[0]) {
        Ok(s) => s,
        Err(m) => {
            let w = reader.get_mut();
            return fail(w, "42601", &m, session.in_transaction());
        }
    };
    let (mut runner, resp) = match copy::CopyRunner::begin(db, session, spec) {
        Ok(v) => v,
        Err((code, msg)) => {
            let w = reader.get_mut();
            return fail(w, &code, &msg, session.in_transaction());
        }
    };
    {
        let w = reader.get_mut();
        w.write_all(&resp)?;
        w.flush()?;
    }
    loop {
        if ctx.shutdown.load(std::sync::atomic::Ordering::Relaxed) {
            runner.rollback(db, session);
            break;
        }
        match read_message(reader) {
            Ok((MSG_COPY_DATA, chunk)) => {
                if let Err((code, msg)) = runner.feed(&chunk) {
                    // The client already sent more data + CopyDone behind
                    // this chunk: drain to the terminator so both sides
                    // agree on the message boundary before ErrorResponse.
                    drain_copy_in(reader);
                    runner.rollback(db, session);
                    let txn = session.in_transaction();
                    let w = reader.get_mut();
                    fail(w, &code, &format!("COPY failed: {msg}"), txn)?;
                    return Ok(());
                }
            }
            Ok((MSG_COPY_DONE, _)) => {
                match runner.finish(db, session) {
                    Ok(bytes) => {
                        let w = reader.get_mut();
                        w.write_all(&bytes)?;
                        w.flush()?;
                    }
                    Err((code, msg)) => {
                        // finish() already rolled back the implicit txn.
                        let txn = session.in_transaction();
                        let w = reader.get_mut();
                        fail(w, &code, &format!("COPY failed: {msg}"), txn)?;
                    }
                }
                return Ok(());
            }
            Ok((MSG_COPY_FAIL, payload)) => {
                let msg = read_cstring(&payload).unwrap_or_default();
                let out = runner.abort(db, session, &msg);
                let w = reader.get_mut();
                w.write_all(&out)?;
                w.flush()?;
                return Ok(());
            }
            Ok(_) => {
                runner.rollback(db, session);
                let txn = session.in_transaction();
                let w = reader.get_mut();
                fail(w, "08P04", "unexpected message during COPY, aborting", txn)?;
                return Ok(());
            }
            Err(_) => {
                // Dead connection (or idle timeout): roll back silently.
                runner.rollback(db, session);
                break;
            }
        }
    }
    Ok(())
}
