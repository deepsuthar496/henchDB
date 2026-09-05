//! Extended query protocol (PG2): Parse/Bind/Describe/Execute/Close/Sync.
//!
//! PostgreSQL `$1, $2, ...` placeholders convert to the engine's `?` markers
//! and reuse the MySQL-side substitution pipeline (`stmt.rs`), so parameter
//! semantics always match. Bound values decode to `Datum` (text or binary
//! per format code + type OID); results emit in the requested per-column
//! format. Named statements/portals live in the connection state; the
//! unnamed ones (`""`) overwrite on reuse and die on the next simple `Q`.
//! After any extended-flow error the connection skips everything until
//! `Sync` (except `Flush`/`Terminate`), per the protocol spec.

use std::collections::HashMap;

use engine::types::{parse_datetime_str, ColumnType};
use engine::{Database, Datum};

use super::super::stmt::{datum_literal, find_placeholders, neutralize_placeholders, substitute};
use super::codec::*;

/// Microseconds between the Unix epoch and the PostgreSQL epoch (2000-01-01).
const PG_EPOCH_MICROS: i64 = 946_684_800_000_000;

/// A parsed statement: engine-ready SQL plus declared parameter OIDs.
pub struct PgStmt {
    sql: String,
    offsets: Vec<usize>,
    oids: Vec<u32>,
    empty: bool,
}

/// Buffered rows for a partially-consumed portal execution.
pub struct Pending {
    cols: Vec<(String, ColumnType)>,
    rows: Vec<Vec<Datum>>,
    pos: usize,
    formats: Vec<i16>,
    total: usize,
}

/// A bound portal: statement reference, decoded params, requested formats.
pub struct PgPortal {
    stmt: String,
    params: Vec<Option<Datum>>,
    result_formats: Vec<i16>,
    described: bool,
    pending: Option<Pending>,
}

/// Per-connection extended-protocol state.
#[derive(Default)]
pub struct PgConn {
    pub stmts: HashMap<String, PgStmt>,
    pub portals: HashMap<String, PgPortal>,
    pub failed: bool,
}

/// Convert `$1, $2, ...` (outside quotes/comments) to `?` markers. Returns
/// the rewritten SQL and the highest placeholder index. Gaps or `$0` are
/// protocol errors.
pub fn pg_to_markers(sql: &str) -> Result<(String, usize), String> {
    let b = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut used = Vec::new();
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'\'' | b'"' | b'`' => {
                let q = b[i];
                out.push(q as char);
                i += 1;
                while i < b.len() {
                    out.push(b[i] as char);
                    if b[i] == q {
                        if i + 1 < b.len() && b[i + 1] == q {
                            out.push(q as char);
                            i += 2;
                        } else {
                            i += 1;
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
            }
            b'-' if i + 1 < b.len() && b[i + 1] == b'-' => {
                while i < b.len() && b[i] != b'\n' {
                    out.push(b[i] as char);
                    i += 1;
                }
            }
            b'/' if i + 1 < b.len() && b[i + 1] == b'*' => {
                while i < b.len() {
                    out.push(b[i] as char);
                    if b[i] == b'*' && i + 1 < b.len() && b[i + 1] == b'/' {
                        out.push('/');
                        i += 2;
                        break;
                    }
                    i += 1;
                }
            }
            b'$' => {
                let mut j = i + 1;
                while j < b.len() && b[j].is_ascii_digit() {
                    j += 1;
                }
                if j == i + 1 {
                    return Err("trailing $ in query".to_string());
                }
                let n: usize = sql[i + 1..j].parse().map_err(|_| "bad placeholder".to_string())?;
                if n == 0 {
                    return Err("placeholder $0 out of range".to_string());
                }
                used.push(n);
                out.push('?');
                i = j;
            }
            _ => {
                out.push(b[i] as char);
                i += 1;
            }
        }
    }
    // Non-ASCII bytes pass through byte-wise; placeholder digits are ASCII
    // so indices stay aligned (offsets are byte indices either way).
    let max = used.iter().copied().max().unwrap_or(0);
    for k in 1..=max {
        if !used.contains(&k) {
            return Err(format!("missing placeholder ${k}"));
        }
    }
    Ok((out, max))
}

/// Decode one text parameter by OID (0 = infer).
fn decode_text(oid: u32, text: &str) -> Result<Datum, (String, String)> {
    let bad = |m: String| (String::from("22P02"), m);
    match oid {
        0 => {
            if let Ok(n) = text.parse::<i64>() {
                return Ok(Datum::Int(n));
            }
            if let Ok(f) = text.parse::<f64>() {
                return Ok(Datum::Float(f));
            }
            match text.to_ascii_lowercase().as_str() {
                "true" | "t" | "yes" | "on" => return Ok(Datum::Bool(true)),
                "false" | "f" | "no" | "off" => return Ok(Datum::Bool(false)),
                _ => {}
            }
            Ok(Datum::Text(text.to_string()))
        }
        21 => text.parse::<i16>().map(|v| Datum::Int(v as i64)).map_err(|_| bad(format!("invalid int2: {text}"))),
        20 | 23 => text.parse::<i64>().map(Datum::Int).map_err(|_| bad(format!("invalid integer: {text}"))),
        700 | 701 => text.parse::<f64>().map(Datum::Float).map_err(|_| bad(format!("invalid float: {text}"))),
        16 => match text.to_ascii_lowercase().as_str() {
            "true" | "t" | "yes" | "on" | "1" => Ok(Datum::Bool(true)),
            "false" | "f" | "no" | "off" | "0" => Ok(Datum::Bool(false)),
            _ => Err(bad(format!("invalid boolean: {text}"))),
        },
        25 | 1043 | 1042 => Ok(Datum::Text(text.to_string())),
        1114 | 1184 => parse_datetime_str(text)
            .map(Datum::DateTime)
            .ok_or_else(|| (String::from("22008"), format!("invalid timestamp: {text}"))),
        _ => Err((String::from("42804"), format!("unsupported parameter type OID {oid}"))),
    }
}

/// Decode one binary parameter by OID.
fn decode_binary(oid: u32, bytes: &[u8]) -> Result<Datum, (String, String)> {
    let bad_len = || (String::from("22P03"), format!("bad binary length for OID {oid}"));
    let need = |n: usize| {
        if bytes.len() == n {
            Ok(())
        } else {
            Err(bad_len())
        }
    };
    match oid {
        21 => {
            need(2)?;
            Ok(Datum::Int(i16::from_be_bytes([bytes[0], bytes[1]]) as i64))
        }
        23 => {
            need(4)?;
            Ok(Datum::Int(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as i64))
        }
        20 => {
            need(8)?;
            let mut b = [0u8; 8];
            b.copy_from_slice(bytes);
            Ok(Datum::Int(i64::from_be_bytes(b)))
        }
        700 => {
            need(4)?;
            let mut b = [0u8; 4];
            b.copy_from_slice(bytes);
            Ok(Datum::Float(f32::from_be_bytes(b) as f64))
        }
        701 => {
            need(8)?;
            let mut b = [0u8; 8];
            b.copy_from_slice(bytes);
            Ok(Datum::Float(f64::from_be_bytes(b)))
        }
        16 => {
            need(1)?;
            Ok(Datum::Bool(bytes[0] != 0))
        }
        25 | 1043 | 1042 => std::str::from_utf8(bytes)
            .map(|s| Datum::Text(s.to_string()))
            .map_err(|_| (String::from("22P02"), "invalid utf8 in text parameter".to_string())),
        1114 | 1184 => {
            need(8)?;
            let mut b = [0u8; 8];
            b.copy_from_slice(bytes);
            Ok(Datum::DateTime(i64::from_be_bytes(b) + PG_EPOCH_MICROS))
        }
        1082 => {
            need(4)?;
            let days = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as i64;
            Ok(Datum::DateTime(days * 86_400_000_000 + PG_EPOCH_MICROS))
        }
        _ => Err((String::from("42804"), format!("unsupported binary parameter type OID {oid}"))),
    }
}

/// Decode one bound parameter to a Datum (`None` = SQL NULL).
fn decode_param(oid: u32, p: &BoundParam) -> Result<Option<Datum>, (String, String)> {
    match &p.value {
        None => Ok(None),
        Some(bytes) => {
            let d = if p.format == 1 {
                decode_binary(oid, bytes)?
            } else if p.format == 0 {
                let text = std::str::from_utf8(bytes).map_err(|_| {
                    (String::from("22P02"), "invalid utf8 in text parameter".to_string())
                })?;
                decode_text(oid, text)?
            } else {
                return Err((String::from("08P01"), format!("bad parameter format {}", p.format)));
            };
            Ok(Some(d))
        }
    }
}

/// Encode one datum cell in binary format for the column type.
fn encode_binary_cell(d: &Datum, ctype: ColumnType) -> Result<Option<Vec<u8>>, String> {
    match d {
        Datum::Null => Ok(None),
        Datum::Int(v) => match ctype {
            ColumnType::Int => {
                i32::try_from(*v)
                    .map(|n| Some(n.to_be_bytes().to_vec()))
                    .map_err(|_| "integer out of int4 range".to_string())
            }
            _ => Ok(Some(v.to_be_bytes().to_vec())),
        },
        Datum::Float(v) => match ctype {
            ColumnType::Float => Ok(Some((*v as f32).to_be_bytes().to_vec())),
            _ => Ok(Some(v.to_be_bytes().to_vec())),
        },
        Datum::Bool(b) => Ok(Some(vec![u8::from(*b)])),
        Datum::Text(s) => Ok(Some(s.as_bytes().to_vec())),
        Datum::DateTime(m) => Ok(Some((m - PG_EPOCH_MICROS).to_be_bytes().to_vec())),
    }
}

/// Expand Bind's result-format codes against the column count (0 codes or a
/// single code broadcasts; otherwise counts must match).
fn expand_formats(codes: &[i16], ncols: usize) -> Result<Vec<i16>, String> {
    if codes.is_empty() || (codes.len() == 1) {
        let f = codes.first().copied().unwrap_or(0);
        if f != 0 && f != 1 {
            return Err(format!("bad result format {f}"));
        }
        return Ok(vec![f; ncols]);
    }
    if codes.len() != ncols {
        return Err(format!(
            "result formats {} != columns {ncols}",
            codes.len()
        ));
    }
    for f in codes {
        if *f != 0 && *f != 1 {
            return Err(format!("bad result format {f}"));
        }
    }
    Ok(codes.to_vec())
}

impl PgConn {
    /// Handle Parse: convert placeholders, syntax-check, store, ParseComplete.
    pub fn on_parse(&mut self, msg: &ParsedParse) -> Result<Vec<u8>, (String, String)> {
        let err = |m: String| (String::from("42601"), m);
        if msg.query.trim().is_empty() {
            self.stmts.insert(
                msg.name.clone(),
                PgStmt { sql: String::new(), offsets: Vec::new(), oids: Vec::new(), empty: true },
            );
            return Ok(parse_complete());
        }
        let (sql, max) = pg_to_markers(&msg.query).map_err(err)?;
        let offsets = find_placeholders(&sql);
        if offsets.len() != max {
            return Err(err("placeholder/parameter mismatch".to_string()));
        }
        // Syntax-check now so Parse (not Execute) reports bad SQL.
        // Markers neutralize to NULL, which always parses cleanly.
        let neutral = neutralize_placeholders(&sql, &offsets);
        engine::sql::parse_sql(&neutral).map_err(|e| (String::from("42601"), e.to_string()))?;
        let mut oids = msg.param_oids.clone();
        oids.resize(max, 0);
        self.stmts.insert(
            msg.name.clone(),
            PgStmt { sql, offsets, oids, empty: false },
        );
        Ok(parse_complete())
    }

    /// Handle Bind: decode params, store the portal, BindComplete.
    pub fn on_bind(&mut self, msg: &ParsedBind) -> Result<Vec<u8>, (String, String)> {
        let stmt = self.stmts.get(&msg.statement).ok_or_else(|| {
            (String::from("26000"), format!("prepared statement \"{}\" does not exist", msg.statement))
        })?;
        if msg.params.len() != stmt.offsets.len() {
            return Err((
                String::from("08P01"),
                format!(
                    "bind supplies {} parameters, prepared statement requires {}",
                    msg.params.len(),
                    stmt.offsets.len()
                ),
            ));
        }
        let mut params = Vec::with_capacity(msg.params.len());
        for (i, p) in msg.params.iter().enumerate() {
            params.push(decode_param(stmt.oids[i], p)?);
        }
        self.portals.insert(
            msg.portal.clone(),
            PgPortal {
                stmt: msg.statement.clone(),
                params,
                result_formats: msg.result_formats.clone(),
                described: false,
                pending: None,
            },
        );
        Ok(bind_complete())
    }

    /// Column types for a statement via prepare-time describe (TEXT fallback
    /// when describe rejects a runnable statement, mirroring simpleQuery).
    fn stmt_columns(
        db: &Database,
        session: &engine::Session,
        stmt: &PgStmt,
    ) -> Vec<(String, ColumnType)> {
        let neutral = neutralize_placeholders(&stmt.sql, &stmt.offsets);
        // Describe needs real column names: run describe on the neutralized
        // template, then align by position with a fresh parse is overkill —
        // instead describe the substituted NULL form (types don't depend on
        // values) and fall back to TEXT per column on any failure.
        let probe_lits: Vec<String> = stmt.offsets.iter().map(|_| "NULL".to_string()).collect();
        let probe = substitute(&stmt.sql, &stmt.offsets, &probe_lits).unwrap_or(neutral);
        db.describe(session, &probe)
            .map(|cols| {
                cols.into_iter()
                    .map(|(n, t)| {
                        let bare = n.rsplit('.').next().unwrap_or(&n).to_string();
                        (bare, t)
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Handle Describe (statement or portal).
    pub fn on_describe(
        &mut self,
        db: &Database,
        session: &engine::Session,
        msg: &ParsedDescribe,
    ) -> Result<Vec<u8>, (String, String)> {
        if msg.kind == b'S' {
            let stmt = self.stmts.get(&msg.name).ok_or_else(|| {
                (String::from("26000"), format!("prepared statement \"{}\" does not exist", msg.name))
            })?;
            let mut out = parameter_description(&stmt.oids);
            if stmt.empty {
                out.extend_from_slice(&no_data());
                return Ok(out);
            }
            let cols = Self::stmt_columns(db, session, stmt);
            if cols.is_empty() {
                out.extend_from_slice(&no_data());
            } else {
                out.extend_from_slice(&row_description(&cols));
            }
            // Later Executes on portals of this statement may skip RowDescription.
            for portal in self.portals.values_mut() {
                if portal.stmt == msg.name {
                    portal.described = true;
                }
            }
            Ok(out)
        } else {
            let portal = self.portals.get(&msg.name).ok_or_else(|| {
                (String::from("34000"), format!("portal \"{}\" does not exist", msg.name))
            })?;
            let stmt = self.stmts.get(&portal.stmt).ok_or_else(|| {
                (String::from("26000"), format!("prepared statement \"{}\" does not exist", portal.stmt))
            })?;
            let cols = Self::stmt_columns(db, session, stmt);
            if cols.is_empty() {
                Ok(no_data())
            } else {
                Ok(row_description(&cols))
            }
        }
    }

    /// Handle Execute: run (or resume) the portal, emitting rows + completion.
    pub fn on_execute(
        &mut self,
        db: &Database,
        session: &mut engine::Session,
        msg: &ParsedExecute,
    ) -> Result<Vec<u8>, (String, String)> {
        let emap = |m: String| (String::from("08P01"), m);
        let ebin = |m: String| (String::from("22003"), m);
        // Snapshot portal state under short borrows (never held across
        // engine calls or a second map borrow).
        let stmt_name = {
            let portal = self.portals.get(&msg.portal).ok_or_else(|| {
                (String::from("34000"), format!("portal \"{}\" does not exist", msg.portal))
            })?;
            portal.stmt.clone()
        };
        let need_run = {
            let portal = self.portals.get(&msg.portal).ok_or_else(|| {
                (String::from("34000"), format!("portal \"{}\" does not exist", msg.portal))
            })?;
            portal.pending.is_none()
        };
        // First Execute of this run: substitute, run, buffer.
        if need_run {
            let stmt = self.stmts.get(&stmt_name).ok_or_else(|| {
                (String::from("26000"), format!("prepared statement \"{stmt_name}\" does not exist"))
            })?;
            if stmt.empty {
                return Ok(empty_query_response());
            }
            let params: Vec<Option<Datum>> = {
                let portal = self.portals.get(&msg.portal).ok_or_else(|| {
                    (String::from("34000"), format!("portal \"{}\" does not exist", msg.portal))
                })?;
                portal.params.clone()
            };
            let lits: Vec<String> = params.iter().map(|p| match p {
                Some(d) => datum_literal(d),
                None => "NULL".to_string(),
            }).collect();
            let formats = {
                let portal = self.portals.get(&msg.portal).ok_or_else(|| {
                    (String::from("34000"), format!("portal \"{}\" does not exist", msg.portal))
                })?;
                portal.result_formats.clone()
            };
            let final_sql =
                substitute(&stmt.sql, &stmt.offsets, &lits).map_err(|m| (String::from("08P01"), m))?;
            let result = db.execute(session, &final_sql).map_err(|e| (sqlstate(&e).to_string(), e.to_string()))?;
            if result.columns.is_empty() {
                return Ok(command_complete(&command_tag(&result.message, result.rows.len(), false)));
            }
            let cols = Self::stmt_columns(db, session, stmt);
            let cols: Vec<(String, ColumnType)> = if cols.len() == result.columns.len() {
                result.columns.iter().cloned().zip(cols.into_iter().map(|(_, t)| t)).collect()
            } else {
                result.columns.iter().map(|c| (c.clone(), ColumnType::Text)).collect()
            };
            let formats = expand_formats(&formats, cols.len()).map_err(emap)?;
            let total = result.rows.len();
            let portal = self.portals.get_mut(&msg.portal).ok_or_else(|| {
                (String::from("34000"), format!("portal \"{}\" does not exist", msg.portal))
            })?;
            portal.pending = Some(Pending { cols, rows: result.rows, pos: 0, formats, total });
        }
        // Emit rows from the buffered run.
        let mut out = Vec::new();
        let done = {
            let portal = self.portals.get_mut(&msg.portal).ok_or_else(|| {
                (String::from("34000"), format!("portal \"{}\" does not exist", msg.portal))
            })?;
            let pending = portal.pending.as_mut().ok_or_else(|| {
                (String::from("XX000"), "portal has no pending rows".to_string())
            })?;
            if !portal.described {
                // Client skipped Describe: RowDescription must precede DataRows.
                let mut p = Vec::new();
                p.extend_from_slice(&(pending.cols.len() as u16).to_be_bytes());
                for ((name, ctype), fmt) in pending.cols.iter().zip(pending.formats.iter()) {
                    let (oid, size) = pg_type(*ctype);
                    p.extend_from_slice(name.as_bytes());
                    p.push(0);
                    p.extend_from_slice(&0u32.to_be_bytes());
                    p.extend_from_slice(&0u16.to_be_bytes());
                    p.extend_from_slice(&oid.to_be_bytes());
                    p.extend_from_slice(&size.to_be_bytes());
                    p.extend_from_slice(&(-1i32).to_be_bytes());
                    p.extend_from_slice(&fmt.to_be_bytes());
                }
                let mut framed = vec![MSG_ROW_DESC];
                framed.extend_from_slice(&((p.len() + 4) as u32).to_be_bytes());
                framed.extend_from_slice(&p);
                out.extend_from_slice(&framed);
                portal.described = true;
            }
            let limit = if msg.max_rows == 0 { usize::MAX } else { msg.max_rows as usize };
            let mut sent = 0usize;
            while pending.pos < pending.rows.len() && sent < limit {
                let row = &pending.rows[pending.pos];
                if pending.formats.iter().all(|f| *f == 0) {
                    let cells: Vec<Option<Vec<u8>>> = row.iter().map(datum_text).collect();
                    out.extend_from_slice(&data_row(&cells));
                } else {
                    let mut cells: Vec<Option<Vec<u8>>> = Vec::with_capacity(row.len());
                    for ((d, ctype), fmt) in row.iter().zip(pending.cols.iter().map(|(_, t)| t)).zip(pending.formats.iter()) {
                        if *fmt == 0 {
                            cells.push(datum_text(d));
                        } else {
                            cells.push(encode_binary_cell(d, *ctype).map_err(ebin)?);
                        }
                    }
                    out.extend_from_slice(&data_row(&cells));
                }
                pending.pos += 1;
                sent += 1;
            }
            let exhausted = pending.pos >= pending.rows.len();
            let total = pending.total;
            (exhausted, total)
        };
        if !done.0 {
            out.extend_from_slice(&portal_suspended());
        } else {
            // Exhausted: drop the buffer so the next Execute re-runs fresh.
            if let Some(portal) = self.portals.get_mut(&msg.portal) {
                portal.pending = None;
            }
            out.extend_from_slice(&command_complete(&format!("SELECT {}", done.1)));
        }
        Ok(out)
    }

    /// Handle Close: drop a statement or portal, CloseComplete.
    pub fn on_close(&mut self, msg: &ParsedClose) -> Result<Vec<u8>, (String, String)> {
        if msg.kind == b'S' {
            self.stmts.remove(&msg.name);
            // Closing a statement closes its portals (spec behavior).
            self.portals.retain(|_, p| p.stmt != msg.name);
        } else if msg.kind == b'P' {
            self.portals.remove(&msg.name);
        } else {
            return Err((String::from("08P01"), "close target must be S or P".to_string()));
        }
        Ok(close_complete())
    }
}
