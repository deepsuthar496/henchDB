//! Binary prepared statements (COM_STMT_*) handling, parameter decoding,
//! substitution, and binary result set serialization.

use std::net::TcpStream;
use std::io::Write;

use engine::types::ColumnType;
use engine::{Datum, Output};

use super::constants::*;
use super::packet::{
    enc_lenenc_int, enc_lenenc_str3, eof_payload, err_payload, ok_payload, read_le,
    read_lenenc_bytes, write_packet,
};

// ---------------------------------------------------------------------------
// Result sets
// ---------------------------------------------------------------------------

/// Per-column wire types for a result set, with numeric promotion: any
/// Float in a column makes it DOUBLE, else any Int/Bool makes it LONGLONG,
/// else VAR_STRING. Deterministic across text and binary encodings.
pub fn result_column_types(out: &Output) -> Vec<u8> {
    out.columns
        .iter()
        .enumerate()
        .map(|(i, _)| {
            let mut has_float = false;
            let mut has_int = false;
            for r in &out.rows {
                match &r[i] {
                    Datum::Null => {}
                    Datum::Float(_) => {
                        has_float = true;
                        break;
                    }
                    Datum::Int(_) | Datum::Bool(_) => has_int = true,
                    Datum::DateTime(_) => return TYPE_DATETIME,
                    Datum::Text(_) => return TYPE_VAR_STRING,
                }
            }
            if has_float {
                TYPE_DOUBLE
            } else if has_int {
                TYPE_LONGLONG
            } else {
                TYPE_VAR_STRING
            }
        })
        .collect()
}

pub fn schema_col_type(t: ColumnType) -> u8 {
    match t {
        ColumnType::Int | ColumnType::BigInt => TYPE_LONGLONG,
        ColumnType::Float | ColumnType::Double => TYPE_DOUBLE,
        ColumnType::Text | ColumnType::VarChar => TYPE_VAR_STRING,
        ColumnType::Bool => TYPE_TINY,
        ColumnType::DateTime => TYPE_DATETIME,
        ColumnType::Timestamp => TYPE_TIMESTAMP,
    }
}

pub fn column_def_payload(name: &str, col_type: u8) -> Vec<u8> {
    let mut p = Vec::new();
    enc_lenenc_str3(&mut p, "def");
    enc_lenenc_str3(&mut p, "");
    enc_lenenc_str3(&mut p, "");
    enc_lenenc_str3(&mut p, "");
    enc_lenenc_str3(&mut p, name);
    enc_lenenc_str3(&mut p, "");
    enc_lenenc_int(&mut p, 0x0C);
    p.extend_from_slice(&33u16.to_le_bytes()); // charset utf8_general_ci
    p.extend_from_slice(&1024u32.to_le_bytes()); // column length
    p.push(col_type);
    p.extend_from_slice(&0u16.to_le_bytes()); // flags
    p.push(0); // decimals
    p.extend_from_slice(&[0u8; 2]); // filler
    p
}

pub fn row_payload(row: &[Datum]) -> Vec<u8> {
    let mut p = Vec::new();
    for d in row {
        match d {
            Datum::Null => p.push(0xFB),
            other => enc_lenenc_str3(&mut p, &other.to_string()),
        }
    }
    p
}

/// Encode one row in binary-protocol format: `0x00` header, null bitmap
/// with column `i` at bit `i+2`, then values per column type. Fails cleanly
/// on value/type mismatch (caller sends ERR instead of a torn result set).
pub fn binary_row_payload(row: &[Datum], types: &[u8]) -> Result<Vec<u8>, String> {
    if row.len() != types.len() {
        return Err("row/column count mismatch".into());
    }
    let n = row.len();
    let mut p = vec![0u8; 1 + (n + 9) / 8];
    for (i, (d, t)) in row.iter().zip(types.iter()).enumerate() {
        if matches!(d, Datum::Null) {
            p[1 + (i + 2) / 8] |= 1 << ((i + 2) % 8);
            continue;
        }
        match (d, *t) {
            (Datum::Bool(b), TYPE_TINY) => p.push(*b as u8),
            (Datum::Bool(b), TYPE_LONGLONG) => p.extend_from_slice(&(*b as i64).to_le_bytes()),
            (Datum::Bool(b), TYPE_DOUBLE) => p.extend_from_slice(&(*b as u8 as f64).to_le_bytes()),
            (Datum::Int(v), TYPE_LONGLONG) => p.extend_from_slice(&v.to_le_bytes()),
            (Datum::Int(v), TYPE_DOUBLE) => p.extend_from_slice(&(*v as f64).to_le_bytes()),
            (Datum::Float(v), TYPE_DOUBLE) => p.extend_from_slice(&v.to_le_bytes()),
            (Datum::Float(v), TYPE_LONGLONG) => {
                if v.fract() != 0.0 || *v > i64::MAX as f64 || *v < i64::MIN as f64 {
                    return Err(format!("column {i}: fractional float in integer column"));
                }
                p.extend_from_slice(&(*v as i64).to_le_bytes());
            }
            (Datum::DateTime(v), TYPE_DATETIME) | (Datum::DateTime(v), TYPE_TIMESTAMP) => {
                p.extend_from_slice(&encode_binary_datetime(*v));
            }
            (Datum::Text(s), TYPE_DATETIME) | (Datum::Text(s), TYPE_TIMESTAMP) => {
                let micros = engine::types::parse_datetime_str(s).unwrap_or(0);
                p.extend_from_slice(&encode_binary_datetime(micros));
            }
            (Datum::Text(s), TYPE_VAR_STRING) => enc_lenenc_str3(&mut p, s),
            (other, TYPE_VAR_STRING) => enc_lenenc_str3(&mut p, &other.to_string()),
            (Datum::Text(_), _) => return Err(format!("column {i}: text in numeric column")),
            _ => return Err(format!("column {i}: value/type mismatch")),
        }
    }
    Ok(p)
}

pub fn parse_affected(message: &str) -> u64 {
    message
        .split_whitespace()
        .next()
        .and_then(|t| t.parse::<u64>().ok())
        .unwrap_or(0)
}

pub fn write_output(
    writer: &mut TcpStream,
    seq: &mut u8,
    out: &Output,
    deprecate_eof: bool,
    binary: bool,
) -> std::io::Result<()> {
    if out.columns.is_empty() {
        let p = ok_payload(parse_affected(&out.message), &out.message);
        write_packet(writer, &p, seq)?;
        writer.flush()?;
        return Ok(());
    }
    // Binary rows are pre-encoded before the header so a value/type
    // mismatch becomes a clean ERR instead of a torn result set.
    let bin_rows: Option<Result<Vec<Vec<u8>>, String>> = if binary {
        let types = result_column_types(out);
        Some(
            out.rows
                .iter()
                .map(|r| binary_row_payload(r, &types))
                .collect(),
        )
    } else {
        None
    };
    // Column count.
    let mut c = Vec::new();
    enc_lenenc_int(&mut c, out.columns.len() as u64);
    let types = result_column_types(out);
    // Definitions.
    let mut defs = Vec::with_capacity(out.columns.len());
    for (name, t) in out.columns.iter().zip(types.iter()) {
        defs.push(column_def_payload(name, *t));
    }
    if binary {
        let rows = match bin_rows.unwrap() {
            Ok(r) => r,
            Err(msg) => {
                write_packet(writer, &err_payload(1047, "HY000", &msg), seq)?;
                writer.flush()?;
                return Ok(());
            }
        };
        write_packet(writer, &c, seq)?;
        for d in &defs {
            write_packet(writer, d, seq)?;
        }
        if !deprecate_eof {
            write_packet(writer, &eof_payload(), seq)?;
        }
        for r in &rows {
            write_packet(writer, r, seq)?;
        }
        if deprecate_eof {
            write_packet(writer, &ok_payload(0, ""), seq)?;
        } else {
            write_packet(writer, &eof_payload(), seq)?;
        }
        writer.flush()?;
        return Ok(());
    }
    write_packet(writer, &c, seq)?;
    for d in &defs {
        write_packet(writer, d, seq)?;
    }
    if !deprecate_eof {
        write_packet(writer, &eof_payload(), seq)?;
    }
    for row in &out.rows {
        write_packet(writer, &row_payload(row), seq)?;
    }
    if deprecate_eof {
        let p = ok_payload(0, "");
        // OK-as-EOF: server must set 0xFE semantics via caps; minimal OK works
        // for clients that skip the trailing EOF.
        write_packet(writer, &p, seq)?;
    } else {
        write_packet(writer, &eof_payload(), seq)?;
    }
    writer.flush()?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Parameters & Substitution
// ---------------------------------------------------------------------------

/// Byte offsets of `?` markers outside quotes, backticks and `--` comments.
/// The engine lexer rejects `?` outright, so textual substitution cannot
/// collide with engine syntax.
pub fn find_placeholders(sql: &str) -> Vec<usize> {
    let b = sql.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'\'' | b'"' | b'`' => {
                let q = b[i];
                i += 1;
                while i < b.len() {
                    if b[i] == q {
                        if i + 1 < b.len() && b[i + 1] == q {
                            i += 2; // doubled quote = escaped quote
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
                    i += 1;
                }
            }
            b'?' => {
                out.push(i);
                i += 1;
            }
            _ => i += 1,
        }
    }
    out
}

/// Replace markers with literal text (same count required).
pub fn substitute(sql: &str, offsets: &[usize], lits: &[String]) -> Result<String, String> {
    if offsets.len() != lits.len() {
        return Err(format!(
            "parameter count mismatch: {} markers, {} values",
            offsets.len(),
            lits.len()
        ));
    }
    let mut out = String::with_capacity(sql.len() + lits.iter().map(|l| l.len()).sum::<usize>());
    let mut prev = 0;
    for (off, lit) in offsets.iter().zip(lits.iter()) {
        out.push_str(&sql[prev..*off]);
        out.push_str(lit);
        prev = off + 1;
    }
    out.push_str(&sql[prev..]);
    Ok(out)
}

/// `?` markers read as NULL for prepare-time describing (parses cleanly).
pub fn neutralize_placeholders(sql: &str, offsets: &[usize]) -> String {
    substitute(sql, offsets, &vec!["NULL".to_string(); offsets.len()])
        .unwrap_or_else(|_| sql.to_string())
}

pub fn render_float(v: f64) -> String {
    let s = format!("{v}");
    if s.bytes().all(|b| b.is_ascii_digit() || b == b'.' || b == b'-' || b == b'+') {
        s
    } else {
        // Exponent/inf/nan forms would not lex; fixed notation always does
        // (inf/nan become parse errors downstream — clean ERR, no corruption).
        format!("{v:.17}")
    }
}

/// Render a bound value as engine SQL. The lexer has no backslash escapes,
/// so doubling `'` is the complete string escaping.
pub fn datum_literal(d: &Datum) -> String {
    match d {
        Datum::Null => "NULL".to_string(),
        Datum::Int(n) => n.to_string(),
        Datum::Float(v) => render_float(*v),
        Datum::Bool(b) => {
            if *b {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }
        }
        Datum::Text(s) => format!("'{}'", s.replace('\'', "''")),
        Datum::DateTime(v) => format!("'{}'", engine::types::format_datetime_micros(*v)),
    }
}

fn encode_binary_datetime(micros: i64) -> Vec<u8> {
    if micros == 0 {
        return vec![0];
    }
    let s = engine::types::format_datetime_micros(micros);
    if let Some((y, m, d, h, min, sec, micro)) = parse_datetime_components(&s) {
        if h == 0 && min == 0 && sec == 0 && micro == 0 {
            let mut buf = vec![4u8];
            buf.extend_from_slice(&(y as u16).to_le_bytes());
            buf.push(m as u8);
            buf.push(d as u8);
            buf
        } else if micro == 0 {
            let mut buf = vec![7u8];
            buf.extend_from_slice(&(y as u16).to_le_bytes());
            buf.push(m as u8);
            buf.push(d as u8);
            buf.push(h as u8);
            buf.push(min as u8);
            buf.push(sec as u8);
            buf
        } else {
            let mut buf = vec![11u8];
            buf.extend_from_slice(&(y as u16).to_le_bytes());
            buf.push(m as u8);
            buf.push(d as u8);
            buf.push(h as u8);
            buf.push(min as u8);
            buf.push(sec as u8);
            buf.extend_from_slice(&(micro as u32).to_le_bytes());
            buf
        }
    } else {
        vec![0]
    }
}

fn parse_datetime_components(s: &str) -> Option<(u32, u32, u32, u32, u32, u32, u32)> {
    let parts: Vec<&str> = s.split_whitespace().collect();
    if parts.is_empty() { return None; }
    let date: Vec<&str> = parts[0].split('-').collect();
    if date.len() != 3 { return None; }
    let y: u32 = date[0].parse().ok()?;
    let m: u32 = date[1].parse().ok()?;
    let d: u32 = date[2].parse().ok()?;
    let (h, min, sec, micro) = if parts.len() == 2 {
        let time: Vec<&str> = parts[1].split(':').collect();
        if time.len() != 3 { return None; }
        let h: u32 = time[0].parse().ok()?;
        let min: u32 = time[1].parse().ok()?;
        let sec_parts: Vec<&str> = time[2].split('.').collect();
        let s: u32 = sec_parts[0].parse().ok()?;
        let ms: u32 = if sec_parts.len() == 2 { sec_parts[1].parse().ok().unwrap_or(0) } else { 0 };
        (h, min, s, ms)
    } else {
        (0, 0, 0, 0)
    };
    Some((y, m, d, h, min, sec, micro))
}

pub fn render_mysql_date(parts: &[u64], with_time: bool, micros: Option<u64>) -> String {
    let date = format!("{:04}-{:02}-{:02}", parts[0], parts.get(1).unwrap_or(&0), parts.get(2).unwrap_or(&0));
    if !with_time {
        return date;
    }
    let t = format!(
        "{:02}:{:02}:{:02}",
        parts.get(3).unwrap_or(&0),
        parts.get(4).unwrap_or(&0),
        parts.get(5).unwrap_or(&0)
    );
    match micros {
        Some(m) => format!("{date} {t}.{m:06}"),
        None => format!("{date} {t}"),
    }
}

/// Decode one binary-protocol parameter value. `extra` is accumulated
/// COM_STMT_SEND_LONG_DATA for this index (prepended for string types).
pub fn decode_param_value(
    typ: u8,
    unsigned: bool,
    buf: &[u8],
    pos: &mut usize,
    extra: &[u8],
) -> Result<Datum, String> {
    let fail = |what: &str| Err(format!("param type 0x{typ:02X}: {what}"));
    match typ {
        TYPE_NULL => Ok(Datum::Null),
        TYPE_TINY | TYPE_SHORT | TYPE_LONG | TYPE_INT24 | TYPE_LONGLONG => {
            if !extra.is_empty() {
                return fail("long data on numeric param");
            }
            let n = match typ {
                TYPE_TINY => 1,
                TYPE_SHORT => 2,
                TYPE_LONG | TYPE_INT24 => 4,
                _ => 8,
            };
            let raw = read_le(buf, pos, n).ok_or_else(|| "truncated EXECUTE packet".to_string())?;
            if unsigned {
                // Mask to the transmitted width before widening.
                let mask = if n == 8 { u64::MAX } else { (1u64 << (8 * n)) - 1 };
                let v = raw & mask;
                if v > i64::MAX as u64 {
                    Ok(Datum::Text(v.to_string()))
                } else {
                    Ok(Datum::Int(v as i64))
                }
            } else {
                // Sign-extend from the transmitted width.
                let shift = 64 - 8 * n;
                Ok(Datum::Int(((raw << shift) as i64) >> shift))
            }
        }
        TYPE_FLOAT => {
            if !extra.is_empty() {
                return fail("long data on numeric param");
            }
            let bits = read_le(buf, pos, 4).ok_or_else(|| "truncated EXECUTE packet".to_string())? as u32;
            Ok(Datum::Float(f32::from_le_bytes(bits.to_le_bytes()) as f64))
        }
        TYPE_DOUBLE => {
            if !extra.is_empty() {
                return fail("long data on numeric param");
            }
            let bits = read_le(buf, pos, 8).ok_or_else(|| "truncated EXECUTE packet".to_string())?;
            Ok(Datum::Float(f64::from_le_bytes(bits.to_le_bytes())))
        }
        TYPE_DECIMAL | TYPE_NEWDECIMAL => {
            // DECIMAL/NEWDECIMAL arrive as text.
            let b = read_lenenc_bytes(buf, pos).ok_or_else(|| "truncated EXECUTE packet".to_string())?;
            let mut full = extra.to_vec();
            full.extend_from_slice(&b);
            let s = String::from_utf8_lossy(&full).into_owned();
            if let Ok(n) = s.parse::<i64>() {
                Ok(Datum::Int(n))
            } else if let Ok(f) = s.parse::<f64>() {
                Ok(Datum::Float(f))
            } else {
                Ok(Datum::Text(s))
            }
        }
        TYPE_VARCHAR | TYPE_VAR_STRING | TYPE_STRING | TYPE_ENUM | TYPE_SET | TYPE_TINY_BLOB | TYPE_MEDIUM_BLOB | TYPE_LONG_BLOB | TYPE_BLOB => {
            let b = read_lenenc_bytes(buf, pos).ok_or_else(|| "truncated EXECUTE packet".to_string())?;
            let mut full = extra.to_vec();
            full.extend_from_slice(&b);
            Ok(Datum::Text(String::from_utf8_lossy(&full).into_owned()))
        }
        TYPE_DATE | TYPE_TIME | TYPE_DATETIME | TYPE_TIMESTAMP => {
            // DATE/DATETIME/TIMESTAMP: len byte + u16 year + u8* fields.
            if !extra.is_empty() {
                return fail("long data on date param");
            }
            if *pos >= buf.len() {
                return Err("truncated EXECUTE packet".to_string());
            }
            let len = buf[*pos] as usize;
            *pos += 1;
            if len == 0 {
                return Ok(Datum::Null);
            }
            if len != 4 && len != 7 && len != 11 {
                return fail("bad date length");
            }
            let mut parts = [0u64; 6];
            parts[0] = read_le(buf, pos, 2).ok_or_else(|| "truncated EXECUTE packet".to_string())?;
            for p in parts.iter_mut().skip(1).take((len - 2).min(5)) {
                *p = read_le(buf, pos, 1).ok_or_else(|| "truncated EXECUTE packet".to_string())?;
            }
            if len == 11 {
                let micros = read_le(buf, pos, 4).ok_or_else(|| "truncated EXECUTE packet".to_string())?;
                return Ok(Datum::Text(render_mysql_date(&parts, typ != TYPE_DATE, Some(micros))));
            }
            Ok(Datum::Text(render_mysql_date(&parts, typ != TYPE_DATE, None)))
        }
        TYPE_YEAR => {
            if !extra.is_empty() {
                return fail("long data on numeric param");
            }
            let y = read_le(buf, pos, 2).ok_or_else(|| "truncated EXECUTE packet".to_string())?;
            Ok(Datum::Int(y as i64))
        }
        TYPE_BIT => {
            let b = read_lenenc_bytes(buf, pos).ok_or_else(|| "truncated EXECUTE packet".to_string())?;
            let mut full = extra.to_vec();
            full.extend_from_slice(&b);
            if full.len() <= 8 {
                let mut v = 0u64;
                for (k, byte) in full.iter().enumerate() {
                    v |= (*byte as u64) << (8 * k);
                }
                Ok(Datum::Int(v as i64))
            } else {
                Ok(Datum::Text(String::from_utf8_lossy(&full).into_owned()))
            }
        }
        TYPE_GEOMETRY => {
            let b = read_lenenc_bytes(buf, pos).ok_or_else(|| "truncated EXECUTE packet".to_string())?;
            let mut full = extra.to_vec();
            full.extend_from_slice(&b);
            Ok(Datum::Text(String::from_utf8_lossy(&full).into_owned()))
        }
        _ => Err(format!("unsupported param type 0x{typ:02X}")),
    }
}

/// Decode the parameter section of COM_STMT_EXECUTE (everything after the
/// 10-byte header). Returns bound values plus the type list to cache.
pub fn decode_execute_params(
    buf: &[u8],
    num_params: usize,
    cached: &Option<Vec<(u8, bool)>>,
    long_data: &[Vec<u8>],
) -> Result<(Vec<Datum>, Vec<(u8, bool)>), String> {
    if num_params == 0 {
        if !buf.is_empty() {
            return Err("unexpected EXECUTE tail with zero params".into());
        }
        return Ok((Vec::new(), Vec::new()));
    }
    let nbytes = (num_params + 7) / 8;
    if buf.len() < nbytes + 1 {
        return Err("truncated EXECUTE packet".into());
    }
    let (bitmap, rest) = buf.split_at(nbytes);
    let (flag, mut rest) = (rest[0], &rest[1..]);
    let types: Vec<(u8, bool)> = if flag == 1 {
        if rest.len() < 2 * num_params {
            return Err("truncated EXECUTE param types".into());
        }
        let mut t = Vec::with_capacity(num_params);
        for i in 0..num_params {
            t.push((rest[2 * i], rest[2 * i + 1] & 0x80 != 0));
        }
        rest = &rest[2 * num_params..];
        t
    } else {
        cached.clone().ok_or_else(|| "EXECUTE without param types".to_string())?
    };
    if types.len() != num_params {
        return Err("param type count mismatch".into());
    }
    let mut pos = 0;
    let mut out = Vec::with_capacity(num_params);
    for i in 0..num_params {
        if bitmap[i / 8] & (1 << (i % 8)) != 0 {
            out.push(Datum::Null);
            continue;
        }
        let extra = long_data.get(i).map(|v| v.as_slice()).unwrap_or(&[]);
        out.push(decode_param_value(types[i].0, types[i].1, rest, &mut pos, extra)?);
    }
    if pos != rest.len() {
        return Err("trailing bytes in EXECUTE packet".into());
    }
    Ok((out, types))
}

/// One server-side prepared statement (per connection).
pub struct Prepared {
    pub sql: String,
    pub offsets: Vec<usize>,
    pub param_types: Option<Vec<(u8, bool)>>,
    pub long_data: Vec<Vec<u8>>,
    pub long_overflow: bool,
}

impl Prepared {
    pub fn new(sql: String, offsets: Vec<usize>) -> Self {
        let n = offsets.len();
        Prepared {
            sql,
            offsets,
            param_types: None,
            long_data: vec![Vec::new(); n],
            long_overflow: false,
        }
    }
    pub fn num_params(&self) -> usize {
        self.offsets.len()
    }
    pub fn reset_long_data(&mut self) {
        for v in &mut self.long_data {
            v.clear();
        }
        self.long_overflow = false;
    }
}

/// COM_STMT_PREPARE response: [0x00][stmt_id][num_cols u16][num_params u16]
/// [0x00 filler][warnings u16].
pub fn prepare_ok_payload(stmt_id: u32, num_cols: usize, num_params: usize) -> Vec<u8> {
    let mut p = Vec::with_capacity(12);
    p.push(0x00);
    p.extend_from_slice(&stmt_id.to_le_bytes());
    p.extend_from_slice(&(num_cols.min(0xFFFF) as u16).to_le_bytes());
    p.extend_from_slice(&(num_params.min(0xFFFF) as u16).to_le_bytes());
    p.push(0x00);
    p.extend_from_slice(&0u16.to_le_bytes());
    p
}
