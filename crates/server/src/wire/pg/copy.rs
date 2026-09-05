//! `COPY ... FROM STDIN` streaming bulk ingestion (PG COPY protocol).
//!
//! Flow: the simple-Query handler detects a COPY statement, resolves the
//! target schema via prepare-time describe, replies `CopyInResponse`, then
//! consumes `CopyData` chunks until `CopyDone` (commit) or `CopyFail`
//! (abort). Rows parse incrementally — text lines split on `\n`, CSV keeps
//! quote state across chunk boundaries — and buffer as SQL literal tuples;
//! at completion they insert in bounded multi-row statements inside one
//! implicit transaction (atomic when autocommit; staged when the client
//! already holds a transaction, so `CopyFail` before completion writes
//! nothing in both modes).

use engine::types::{parse_datetime_str, ColumnType};
use engine::{Database, Datum};

use super::super::stmt::datum_literal;
use super::codec::*;

/// Rows per INSERT statement at completion (bounds statement size).
const INSERT_CHUNK_ROWS: usize = 2000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyFormat {
    Text,
    Csv,
}

/// Parsed `COPY table [(cols)] FROM STDIN [options]`.
#[derive(Debug)]
pub struct CopySpec {
    pub table: String,
    pub columns: Vec<String>,
    pub format: CopyFormat,
    pub delimiter: u8,
    pub null: Vec<u8>,
    pub header: bool,
}

/// True when `sql` looks like a COPY ... FROM STDIN statement (cheap token
/// pre-check; `parse_copy` validates fully).
pub fn is_copy_from_stdin(sql: &str) -> bool {
    let toks: Vec<&str> = sql.split_whitespace().collect();
    if toks.len() < 4 || !toks[0].eq_ignore_ascii_case("COPY") {
        return false;
    }
    toks.windows(2).any(|w| w[0].eq_ignore_ascii_case("FROM") && w[1].eq_ignore_ascii_case("STDIN"))
}

/// Split preamble into tokens: words, single-quoted strings ('' escape),
/// double-quoted identifiers ("" escape), and single-char symbols.
fn tokenize_preamble(s: &str) -> Vec<String> {
    let b = s.as_bytes();
    let mut toks = Vec::new();
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b' ' | b'\t' | b'\n' | b'\r' | b';' => i += 1,
            b'\'' => {
                let mut cur = String::from("'");
                i += 1;
                while i < b.len() {
                    cur.push(b[i] as char);
                    if b[i] == b'\'' {
                        if i + 1 < b.len() && b[i + 1] == b'\'' {
                            cur.push('\'');
                            i += 2;
                        } else {
                            i += 1;
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
                toks.push(cur);
            }
            b'"' => {
                let mut cur = String::new();
                i += 1;
                while i < b.len() {
                    if b[i] == b'"' {
                        if i + 1 < b.len() && b[i + 1] == b'"' {
                            cur.push('"');
                            i += 2;
                        } else {
                            i += 1;
                            break;
                        }
                    } else {
                        cur.push(b[i] as char);
                        i += 1;
                    }
                }
                toks.push(cur);
            }
            b'(' | b')' | b',' => {
                toks.push((b[i] as char).to_string());
                i += 1;
            }
            _ => {
                let mut cur = String::new();
                while i < b.len() && !matches!(b[i], b' ' | b'\t' | b'\n' | b'\r' | b';' | b'(' | b')' | b',') {
                    cur.push(b[i] as char);
                    i += 1;
                }
                toks.push(cur);
            }
        }
    }
    toks
}

/// Unquote a single-quoted preamble literal (`'it''s'` -> `it\'s` raw).
fn unquote(tok: &str) -> Result<String, String> {
    if tok.len() >= 2 && tok.starts_with('\'') && tok.ends_with('\'') {
        Ok(tok[1..tok.len() - 1].replace("''", "'"))
    } else {
        Err(format!("expected quoted string, got {tok}"))
    }
}

/// Parse `COPY ... FROM STDIN ...` into a spec (errors are 42601 text).
pub fn parse_copy(sql: &str) -> Result<CopySpec, String> {
    let toks = tokenize_preamble(sql);
    let mut p = 0usize;
    let eat = |p: &mut usize, kw: &str, toks: &[String]| -> bool {
        if toks.get(*p).map(|t| t.eq_ignore_ascii_case(kw)).unwrap_or(false) {
            *p += 1;
            true
        } else {
            false
        }
    };
    if !eat(&mut p, "COPY", &toks) {
        return Err("not a COPY statement".to_string());
    }
    eat(&mut p, "ONLY", &toks);
    let table = toks.get(p).cloned().ok_or("COPY missing table".to_string())?;
    if table.eq_ignore_ascii_case("FROM") {
        return Err("COPY missing table".to_string());
    }
    p += 1;
    let mut columns = Vec::new();
    if toks.get(p).map(|t| t.as_str()) == Some("(") {
        p += 1;
        loop {
            let col = toks.get(p).cloned().ok_or("COPY unterminated column list".to_string())?;
            if col == ")" {
                p += 1;
                break;
            }
            if col == "," {
                p += 1;
                continue;
            }
            columns.push(col);
            p += 1;
        }
    }
    if !eat(&mut p, "FROM", &toks) || !eat(&mut p, "STDIN", &toks) {
        return Err("only COPY ... FROM STDIN is supported".to_string());
    }
    let mut spec = CopySpec {
        table,
        columns,
        format: CopyFormat::Text,
        delimiter: b'\t',
        null: b"\\N".to_vec(),
        header: false,
    };
    // Optional WITH (...) or legacy bare options (WITH prefix allowed).
    eat(&mut p, "WITH", &toks);
    if toks.get(p).map(|t| t.as_str()) == Some("(") {
        p += 1;
        loop {
            let key = toks.get(p).cloned().ok_or("COPY unterminated WITH list".to_string())?;
            if key == ")" {
                break;
            }
            if key == "," {
                p += 1;
                continue;
            }
            p += 1;
            apply_option(&mut spec, &key, &toks, &mut p)?;
        }
    } else {
        while p < toks.len() {
            let key = toks[p].clone();
            p += 1;
            if key == "," {
                continue;
            }
            apply_option(&mut spec, &key, &toks, &mut p)?;
        }
    }
    if spec.format == CopyFormat::Csv {
        if spec.delimiter == b'\t' {
            spec.delimiter = b',';
        }
        if spec.null == b"\\N".to_vec() {
            spec.null = Vec::new();
        }
    }
    Ok(spec)
}

/// Apply one option keyword (value consumed from `toks` when required).
fn apply_option(spec: &mut CopySpec, key: &str, toks: &[String], p: &mut usize) -> Result<(), String> {
    match key.to_ascii_uppercase().as_str() {
        "FORMAT" => {
            let v = toks.get(*p).cloned().ok_or("FORMAT needs a value".to_string())?;
            *p += 1;
            match v.to_ascii_lowercase().as_str() {
                "text" => spec.format = CopyFormat::Text,
                "csv" => spec.format = CopyFormat::Csv,
                "binary" => return Err("COPY BINARY is not supported".to_string()),
                _ => return Err(format!("unknown COPY format {v}")),
            }
        }
        "CSV" => spec.format = CopyFormat::Csv,
        "TEXT" => spec.format = CopyFormat::Text,
        "BINARY" => return Err("COPY BINARY is not supported".to_string()),
        "DELIMITER" => {
            let v = toks.get(*p).cloned().ok_or("DELIMITER needs a value".to_string())?;
            *p += 1;
            let d = unquote(&v)?;
            if d.len() != 1 {
                return Err("DELIMITER must be one character".to_string());
            }
            spec.delimiter = d.as_bytes()[0];
        }
        "NULL" => {
            let v = toks.get(*p).cloned().ok_or("NULL needs a value".to_string())?;
            *p += 1;
            spec.null = unquote(&v)?.into_bytes();
        }
        "HEADER" => {
            // Bare HEADER means true; HEADER false/match disable.
            if let Some(next) = toks.get(*p) {
                if next.eq_ignore_ascii_case("false") || next.eq_ignore_ascii_case("match") {
                    *p += 1;
                } else if next.eq_ignore_ascii_case("true") {
                    *p += 1;
                    spec.header = true;
                } else {
                    spec.header = true;
                }
            } else {
                spec.header = true;
            }
        }
        _ => return Err(format!("unsupported COPY option {key}")),
    }
    Ok(())
}

/// Decode one text-format escape starting after the backslash. Returns the
/// decoded byte(s) and characters consumed.
fn text_escape(rest: &[u8]) -> Result<(Vec<u8>, usize), String> {
    if rest.is_empty() {
        return Err("trailing backslash".to_string());
    }
    match rest[0] {
        b'b' => Ok((vec![8], 1)),
        b'f' => Ok((vec![12], 1)),
        b'n' => Ok((vec![b'\n'], 1)),
        b'r' => Ok((vec![b'\r'], 1)),
        b't' => Ok((vec![b'\t'], 1)),
        b'v' => Ok((vec![11], 1)),
        b'\\' => Ok((vec![b'\\'], 1)),
        b'x' => {
            if rest.len() < 3 {
                return Err("bad \\x escape".to_string());
            }
            let hex = std::str::from_utf8(&rest[1..3]).map_err(|_| "bad \\x escape".to_string())?;
            let v = u8::from_str_radix(hex, 16).map_err(|_| "bad \\x escape".to_string())?;
            Ok((vec![v], 3))
        }
        b'0'..=b'7' => {
            let mut n = 0usize;
            let mut val = 0u8;
            while n < 3 && n < rest.len() && rest[n].is_ascii_digit() && rest[n] < b'8' {
                val = val * 8 + (rest[n] - b'0');
                n += 1;
            }
            Ok((vec![val], n))
        }
        c => Err(format!("invalid escape \\{}", c as char)),
    }
}

/// Split one text-format line into RAW fields (backslashes preserved).
/// The delimiter only splits when unescaped; a backslash quotes the next
/// byte whatever it is, so chunk and escape processing stay total.
fn split_text_line(line: &[u8], delim: u8) -> Vec<Vec<u8>> {
    let mut fields: Vec<Vec<u8>> = vec![Vec::new()];
    let mut i = 0;
    while i < line.len() {
        if line[i] == b'\\' && i + 1 < line.len() {
            fields.last_mut().expect("field").push(b'\\');
            fields.last_mut().expect("field").push(line[i + 1]);
            i += 2;
        } else if line[i] == delim {
            fields.push(Vec::new());
            i += 1;
        } else {
            fields.last_mut().expect("field").push(line[i]);
            i += 1;
        }
    }
    fields
}

/// Unescape one raw text field (escapes verified complete by the splitter).
fn unescape_text(raw: &[u8]) -> Result<Vec<u8>, String> {
    let mut out = Vec::with_capacity(raw.len());
    let mut i = 0;
    while i < raw.len() {
        if raw[i] == b'\\' {
            let (bytes, used) = text_escape(&raw[i + 1..])?;
            out.extend_from_slice(&bytes);
            i += 1 + used;
        } else {
            out.push(raw[i]);
            i += 1;
        }
    }
    Ok(out)
}

/// One CSV field: raw bytes plus whether it was quoted (quoted empty is an
/// empty string, unquoted empty is the NULL marker).
#[derive(Debug, Clone)]
pub struct CsvField {
    pub bytes: Vec<u8>,
    pub quoted: bool,
}

/// Incremental CSV tokenizer: quote state survives across `feed` calls so
/// embedded newlines inside quoted fields work over chunk boundaries.
#[derive(Debug, Default)]
pub struct CsvTokenizer {
    in_quotes: bool,
    field: Vec<u8>,
    field_quoted: bool,
    field_started: bool,
    row: Vec<CsvField>,
}

impl CsvTokenizer {
    /// Feed bytes; complete rows accumulate in `out`.
    pub fn feed(&mut self, chunk: &[u8], delim: u8, out: &mut Vec<Vec<CsvField>>) -> Result<(), String> {
        let mut i = 0;
        while i < chunk.len() {
            let c = chunk[i];
            if self.in_quotes {
                if c == b'"' {
                    if i + 1 < chunk.len() && chunk[i + 1] == b'"' {
                        self.field.push(b'"');
                        i += 2;
                    } else {
                        self.in_quotes = false;
                        i += 1;
                    }
                } else {
                    self.field.push(c);
                    i += 1;
                }
                continue;
            }
            match c {
                b'"' if !self.field_started => {
                    self.in_quotes = true;
                    self.field_quoted = true;
                    self.field_started = true;
                    i += 1;
                }
                b'\r' => {
                    // Accept \r, \r\n, \n as row terminators.
                    if i + 1 < chunk.len() && chunk[i + 1] == b'\n' {
                        i += 1;
                    }
                    self.end_field();
                    out.push(std::mem::take(&mut self.row));
                    i += 1;
                }
                b'\n' => {
                    self.end_field();
                    out.push(std::mem::take(&mut self.row));
                    i += 1;
                }
                c if c == delim => {
                    self.end_field();
                    i += 1;
                }
                _ => {
                    self.field.push(c);
                    self.field_started = true;
                    i += 1;
                }
            }
        }
        Ok(())
    }

    fn end_field(&mut self) {
        self.row.push(CsvField {
            bytes: std::mem::take(&mut self.field),
            quoted: self.field_quoted,
        });
        self.field_quoted = false;
        self.field_started = false;
    }

    /// Terminate input at CopyDone: an open quote is corrupt, but a pending
    /// field/row simply ends (a final line needs no trailing newline).
    pub fn end_input(&mut self, out: &mut Vec<Vec<CsvField>>) -> Result<(), String> {
        if self.in_quotes {
            return Err("unterminated quoted field at end of COPY".to_string());
        }
        if self.field_started || !self.row.is_empty() {
            self.end_field();
            out.push(std::mem::take(&mut self.row));
        }
        Ok(())
    }

    /// True when mid-row or mid-quote state is pending (unterminated input).
    pub fn pending(&self) -> bool {
        self.in_quotes || self.field_started || !self.row.is_empty()
    }
}

/// Coerce one raw field to a Datum for the target column type. In text
/// mode the NULL marker matches pre-unescape (a lone `\N` never reaches
/// escape processing); CSV fields never unescape.
fn coerce_field(
    raw: &[u8],
    quoted: bool,
    text_mode: bool,
    ctype: ColumnType,
    null: &[u8],
) -> Result<Datum, String> {
    let unescaped: Vec<u8>;
    let raw = if text_mode {
        if raw == null {
            return Ok(Datum::Null);
        }
        unescaped = unescape_text(raw)?;
        unescaped.as_slice()
    } else {
        if !quoted && raw == null {
            return Ok(Datum::Null);
        }
        raw
    };
    let bad = |what: &str| format!("invalid {what}: '{}'", String::from_utf8_lossy(raw));
    match ctype {
        ColumnType::Int | ColumnType::BigInt => std::str::from_utf8(raw)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .map(Datum::Int)
            .ok_or_else(|| bad("integer")),
        ColumnType::Float | ColumnType::Double => std::str::from_utf8(raw)
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            .map(Datum::Float)
            .ok_or_else(|| bad("float")),
        ColumnType::Text | ColumnType::VarChar => std::str::from_utf8(raw)
            .map(|s| Datum::Text(s.to_string()))
            .map_err(|_| bad("text")),
        ColumnType::Bool => match raw.to_ascii_lowercase().as_slice() {
            b"true" | b"t" | b"yes" | b"on" | b"1" => Ok(Datum::Bool(true)),
            b"false" | b"f" | b"no" | b"off" | b"0" => Ok(Datum::Bool(false)),
            _ => Err(bad("boolean")),
        },
        ColumnType::DateTime | ColumnType::Timestamp => std::str::from_utf8(raw)
            .ok()
            .and_then(parse_datetime_str)
            .map(Datum::DateTime)
            .ok_or_else(|| bad("timestamp")),
    }
}

/// Active COPY ingestion: parsed spec, resolved schema, buffered row tuples.
pub struct CopyRunner {
    spec: CopySpec,
    /// (schema position, column type) per COPY column, in COPY order.
    targets: Vec<(usize, ColumnType)>,
    /// Full-width literal tuples buffered for the completion commit.
    tuples: Vec<String>,
    /// Text-mode line accumulation across CopyData chunks.
    text_buf: Vec<u8>,
    csv: CsvTokenizer,
    implicit_txn: bool,
    row_count: u64,
    header_skipped: bool,
}

impl CopyRunner {
    /// Resolve the spec against the engine schema and open an implicit
    /// transaction when the session holds none.
    pub fn begin(
        db: &Database,
        session: &mut engine::Session,
        spec: CopySpec,
    ) -> Result<(Self, Vec<u8>), (String, String)> {
        let emap = |e: engine::Error| (sqlstate(&e).to_string(), e.to_string());
        let cols = db.describe(session, &format!("SELECT * FROM {}", spec.table)).map_err(emap)?;
        if cols.is_empty() {
            return Err((String::from("42P01"), format!("table \"{}\" does not exist", spec.table)));
        }
        // Map COPY columns (empty = all, schema order) to positions.
        let wanted: Vec<&str> = if spec.columns.is_empty() {
            cols.iter().map(|(n, _)| n.as_str()).collect()
        } else {
            spec.columns.iter().map(|s| s.as_str()).collect()
        };
        let mut targets = Vec::with_capacity(wanted.len());
        for name in &wanted {
            let bare = name.trim_matches('"').replace("\"\"", "\"");
            let pos = cols
                .iter()
                .position(|(n, _)| n == name || n == &bare)
                .ok_or_else(|| (String::from("42703"), format!("column \"{name}\" does not exist")))?;
            targets.push((pos, cols[pos].1));
        }
        let implicit_txn = !session.in_transaction();
        if implicit_txn {
            db.execute(session, "BEGIN").map_err(emap)?;
        }
        let ncols = targets.len() as u16;
        let mut resp = vec![MSG_COPY_IN];
        resp.extend_from_slice(&((4 + 1 + 2 + 2 * ncols as usize) as u32).to_be_bytes());
        resp.push(0); // overall text format
        resp.extend_from_slice(&ncols.to_be_bytes());
        for _ in 0..ncols {
            resp.extend_from_slice(&0u16.to_be_bytes());
        }
        Ok((
            CopyRunner {
                spec,
                targets,
                tuples: Vec::new(),
                text_buf: Vec::new(),
                csv: CsvTokenizer::default(),
                implicit_txn,
                row_count: 0,
                header_skipped: false,
            },
            resp,
        ))
    }

    /// Feed one CopyData chunk: parse rows, buffer literal tuples.
    pub fn feed(&mut self, chunk: &[u8]) -> Result<(), (String, String)> {
        let bad = |m: String| (String::from("22P04"), m);
        if self.spec.format == CopyFormat::Csv {
            let mut rows: Vec<Vec<CsvField>> = Vec::new();
            self.csv.feed(chunk, self.spec.delimiter, &mut rows).map_err(bad)?;
            for fields in rows {
                let no = self.row_count + 1;
                self.push_row_csv(fields, no).map_err(|m| (String::from("22P02"), m))?;
            }
            return Ok(());
        }
        // Text mode: lines split safely on \n (raw newlines are escaped).
        self.text_buf.extend_from_slice(chunk);
        let mut start = 0usize;
        let mut lines: Vec<Vec<u8>> = Vec::new();
        for (i, b) in self.text_buf.iter().enumerate() {
            if *b == b'\n' {
                let mut line = self.text_buf[start..i].to_vec();
                if line.last() == Some(&b'\r') {
                    line.pop();
                }
                lines.push(line);
                start = i + 1;
            }
        }
        self.text_buf.drain(..start);
        for line in lines {
            // A lone `\.` line ends text input client-side; tolerate it if
            // a client forwards it anyway.
            if line == b"\\." {
                continue;
            }
            let fields = split_text_line(&line, self.spec.delimiter);
            let no = self.row_count + 1;
            self.push_row_text(fields, no).map_err(|m| (String::from("22P02"), m))?;
        }
        Ok(())
    }

    fn push_row_text(&mut self, fields: Vec<Vec<u8>>, row_no: u64) -> Result<(), String> {
        if self.spec.header && !self.header_skipped {
            self.header_skipped = true;
            return Ok(());
        }
        if fields.len() != self.targets.len() {
            return Err(format!(
                "row {row_no}: expected {} fields, got {}",
                self.targets.len(),
                fields.len()
            ));
        }
        let mut datums = Vec::with_capacity(fields.len());
        for ((_, ctype), raw) in self.targets.iter().zip(fields.iter()) {
            datums.push(
                coerce_field(raw, false, true, *ctype, &self.spec.null)
                    .map_err(|m| format!("row {row_no}: {m}"))?,
            );
        }
        self.push_datums(datums);
        Ok(())
    }

    fn push_row_csv(&mut self, fields: Vec<CsvField>, row_no: u64) -> Result<(), String> {
        if self.spec.header && !self.header_skipped {
            self.header_skipped = true;
            return Ok(());
        }
        if fields.len() != self.targets.len() {
            return Err(format!(
                "row {row_no}: expected {} fields, got {}",
                self.targets.len(),
                fields.len()
            ));
        }
        let mut datums = Vec::with_capacity(fields.len());
        for ((_, ctype), f) in self.targets.iter().zip(fields.iter()) {
            datums.push(
                coerce_field(&f.bytes, f.quoted, false, *ctype, &self.spec.null)
                    .map_err(|m| format!("row {row_no}: {m}"))?,
            );
        }
        self.push_datums(datums);
        Ok(())
    }

    fn push_datums(&mut self, datums: Vec<Datum>) {
        // Full-width row in schema order; unlisted columns become NULL and
        // let the commit path apply defaults / NOT NULL checks.
        let width = self.targets.iter().map(|(p, _)| *p).max().unwrap_or(0) + 1;
        let mut row: Vec<Datum> = vec![Datum::Null; width];
        for ((pos, _), d) in self.targets.iter().zip(datums.into_iter()) {
            row[*pos] = d;
        }
        let lits: Vec<String> = row.iter().map(datum_literal).collect();
        self.tuples.push(format!("({})", lits.join(",")));
        self.row_count += 1;
    }

    /// Finish: insert buffered tuples in bounded statements, commit an
    /// implicit transaction, and report the wire-ready completion bytes.
    /// Any failure rolls back the implicit transaction first.
    pub fn finish(
        mut self,
        db: &Database,
        session: &mut engine::Session,
    ) -> Result<Vec<u8>, (String, String)> {
        let r = self.finish_inner(db, session);
        if r.is_err() {
            self.rollback(db, session);
        }
        r
    }

    fn finish_inner(
        &mut self,
        db: &Database,
        session: &mut engine::Session,
    ) -> Result<Vec<u8>, (String, String)> {
        // A final line needs no trailing newline; an open quote is corrupt.
        if self.spec.format == CopyFormat::Csv {
            let mut rows: Vec<Vec<CsvField>> = Vec::new();
            self.csv.end_input(&mut rows).map_err(|m| (String::from("22P04"), m))?;
            debug_assert!(!self.csv.pending());
            for fields in rows {
                let no = self.row_count + 1;
                self.push_row_csv(fields, no).map_err(|m| (String::from("22P02"), m))?;
            }
        } else if !self.text_buf.is_empty() {
            let mut line = std::mem::take(&mut self.text_buf);
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            if line != b"\\.".as_slice() {
                let fields = split_text_line(&line, self.spec.delimiter);
                let no = self.row_count + 1;
                self.push_row_text(fields, no).map_err(|m| (String::from("22P02"), m))?;
            }
        }
        let emap = |e: engine::Error| (sqlstate(&e).to_string(), e.to_string());
        // Header-only input: zero rows, still a successful COPY.
        for chunk in self.tuples.chunks(INSERT_CHUNK_ROWS) {
            let sql = format!("INSERT INTO {} VALUES {}", self.spec.table, chunk.join(","));
            db.execute(session, &sql).map_err(emap)?;
        }
        if self.implicit_txn {
            db.execute(session, "COMMIT").map_err(emap)?;
        }
        let mut out = command_complete(&format!("COPY {}", self.row_count));
        out.extend_from_slice(&ready_for_query(if session.in_transaction() { b'T' } else { b'I' }));
        Ok(out)
    }

    /// Abort: roll back an implicit transaction and report the failure.
    pub fn abort(
        self,
        db: &Database,
        session: &mut engine::Session,
        client_msg: &str,
    ) -> Vec<u8> {
        self.rollback(db, session);
        let mut out = error_response("57014", &format!("COPY aborted by client: {client_msg}"));
        out.extend_from_slice(&ready_for_query(if session.in_transaction() { b'T' } else { b'I' }));
        out
    }

    /// Roll back an implicit transaction (quietly; the connection may be dead).
    pub fn rollback(&self, db: &Database, session: &mut engine::Session) {
        if self.implicit_txn {
            let _ = db.execute(session, "ROLLBACK");
        }
    }
}
