//! PostgreSQL wire protocol 3.0 message codecs (pure functions, no I/O).
//!
//! Frontend messages carry a type byte, except the startup exchange (length
//! first, no type). Backend messages are `[type][u32 BE len][payload]` where
//! len includes itself but not the type byte. All integers are big-endian,
//! strings are NUL-terminated.

use engine::types::ColumnType;

// Startup exchange.
pub const PG_PROTOCOL_VERSION: u32 = 0x0003_0000;
pub const SSL_REQUEST_CODE: u32 = 80877103;
pub const GSS_REQUEST_CODE: u32 = 80877104;

// Backend message types.
pub const MSG_AUTH: u8 = b'R';
pub const MSG_PARAMETER_STATUS: u8 = b'S';
pub const MSG_BACKEND_KEY: u8 = b'K';
pub const MSG_READY: u8 = b'Z';
pub const MSG_ROW_DESC: u8 = b'T';
pub const MSG_DATA_ROW: u8 = b'D';
pub const MSG_COMMAND_COMPLETE: u8 = b'C';
pub const MSG_ERROR: u8 = b'E';
pub const MSG_EMPTY_QUERY: u8 = b'I';

// Frontend message types we handle.
pub const MSG_QUERY: u8 = b'Q';
pub const MSG_TERMINATE: u8 = b'X';
pub const MSG_PASSWORD: u8 = b'p';

// Authentication codes.
pub const AUTH_OK: i32 = 0;
pub const AUTH_CLEARTEXT: i32 = 3;

// Type OIDs for text-format RowDescription.
pub const OID_BOOL: u32 = 16;
pub const OID_INT: u32 = 23;
pub const OID_BIGINT: u32 = 20;
pub const OID_FLOAT: u32 = 700;
pub const OID_DOUBLE: u32 = 701;
pub const OID_TEXT: u32 = 25;
pub const OID_TIMESTAMP: u32 = 1114;
pub const OID_TIMESTAMPTZ: u32 = 1184;

/// Startup parameters (`user`, `database`, ...). Unknown keys are ignored.
#[derive(Debug, Default)]
pub struct StartupParams {
    pub user: String,
    pub database: String,
    pub application_name: String,
}

/// Parse a StartupMessage body (version + NUL-separated key/value pairs).
/// Returns `None` on wrong version or malformed bytes.
pub fn parse_startup(body: &[u8]) -> Option<StartupParams> {
    if body.len() < 4 {
        return None;
    }
    let version = u32::from_be_bytes([body[0], body[1], body[2], body[3]]);
    if version != PG_PROTOCOL_VERSION {
        return None;
    }
    let mut out = StartupParams::default();
    let mut parts = body[4..].split(|b| *b == 0);
    loop {
        let key = parts.next()?;
        if key.is_empty() {
            break; // terminating NUL
        }
        let val = parts.next()?;
        let (k, v) = (
            String::from_utf8_lossy(key).into_owned(),
            String::from_utf8_lossy(val).into_owned(),
        );
        match k.as_str() {
            "user" => out.user = v,
            "database" => out.database = v,
            "application_name" => out.application_name = v,
            _ => {}
        }
    }
    Some(out)
}

/// Encode a StartupMessage body for tests/clients.
#[cfg(test)]
pub fn encode_startup(user: &str, database: &str) -> Vec<u8> {
    let mut v = Vec::new();
    v.extend_from_slice(&PG_PROTOCOL_VERSION.to_be_bytes());
    for (k, val) in [("user", user), ("database", database), ("client_encoding", "UTF8")] {
        v.extend_from_slice(k.as_bytes());
        v.push(0);
        v.extend_from_slice(val.as_bytes());
        v.push(0);
    }
    v.push(0);
    v
}

fn frame(msg_type: u8, payload: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(5 + payload.len());
    v.push(msg_type);
    v.extend_from_slice(&((payload.len() + 4) as u32).to_be_bytes());
    v.extend_from_slice(payload);
    v
}

fn nul_term(s: &str, out: &mut Vec<u8>) {
    out.extend_from_slice(s.as_bytes());
    out.push(0);
}

/// Authentication response: `AuthenticationOk` or cleartext request.
pub fn auth_message(code: i32) -> Vec<u8> {
    let mut p = Vec::with_capacity(4);
    p.extend_from_slice(&code.to_be_bytes());
    frame(MSG_AUTH, &p)
}

/// ParameterStatus message.
pub fn parameter_status(name: &str, value: &str) -> Vec<u8> {
    let mut p = Vec::new();
    nul_term(name, &mut p);
    nul_term(value, &mut p);
    frame(MSG_PARAMETER_STATUS, &p)
}

/// BackendKeyData message.
pub fn backend_key_data(pid: i32, secret: i32) -> Vec<u8> {
    let mut p = Vec::with_capacity(8);
    p.extend_from_slice(&pid.to_be_bytes());
    p.extend_from_slice(&secret.to_be_bytes());
    frame(MSG_BACKEND_KEY, &p)
}

/// ReadyForQuery message (`I` idle, `T` in transaction, `E` failed).
pub fn ready_for_query(status: u8) -> Vec<u8> {
    frame(MSG_READY, &[status])
}

/// Map an engine error to a PostgreSQL SQLSTATE code.
pub fn sqlstate(e: &engine::Error) -> &'static str {
    match e {
        engine::Error::TableNotFound(_) => "42P01",
        engine::Error::TableExists(_) => "42P07",
        engine::Error::DuplicateKey(_) => "23505",
        engine::Error::ColumnNotFound(_) => "42703",
        engine::Error::ColumnCountMismatch { .. } => "42601",
        engine::Error::TypeMismatch { .. } => "42804",
        engine::Error::NotNullViolation(_) => "23502",
        engine::Error::ForeignKeyViolation(_) => "23503",
        engine::Error::ParseError(_) => "42601",
        engine::Error::NotSupported(_) => "0A000",
        engine::Error::TxnConflict(_) => "40001",
        engine::Error::TxnNotActive => "25000",
        engine::Error::QueryTimeout => "57014",
        engine::Error::IndexExists(_) => "42P07",
        engine::Error::IndexNotFound(_) => "42704",
        engine::Error::InvalidSchema(_) => "42601",
        engine::Error::DatabaseNotFound(_) => "3D000",
        engine::Error::DatabaseExists(_) => "42P04",
        _ => "XX000",
    }
}

/// ErrorResponse with severity/code/message fields.
pub fn error_response(code: &str, message: &str) -> Vec<u8> {
    let mut p = Vec::new();
    p.push(b'S');
    nul_term("ERROR", &mut p);
    p.push(b'V');
    nul_term("ERROR", &mut p);
    p.push(b'C');
    nul_term(code, &mut p);
    p.push(b'M');
    nul_term(message, &mut p);
    p.push(0);
    frame(MSG_ERROR, &p)
}

/// Map an engine column type to (OID, size) for RowDescription.
pub fn pg_type(ctype: ColumnType) -> (u32, i16) {
    match ctype {
        ColumnType::Int => (OID_INT, 4),
        ColumnType::BigInt => (OID_BIGINT, 8),
        ColumnType::Float => (OID_FLOAT, 4),
        ColumnType::Double => (OID_DOUBLE, 8),
        ColumnType::Text | ColumnType::VarChar => (OID_TEXT, -1),
        ColumnType::Bool => (OID_BOOL, 1),
        ColumnType::DateTime => (OID_TIMESTAMP, 8),
        ColumnType::Timestamp => (OID_TIMESTAMPTZ, 8),
    }
}

/// RowDescription for text-format columns.
pub fn row_description(cols: &[(String, ColumnType)]) -> Vec<u8> {
    let mut p = Vec::with_capacity(32 + cols.len() * 32);
    p.extend_from_slice(&(cols.len() as u16).to_be_bytes());
    for (name, ctype) in cols {
        let (oid, size) = pg_type(*ctype);
        nul_term(name, &mut p);
        p.extend_from_slice(&0u32.to_be_bytes()); // table OID
        p.extend_from_slice(&0u16.to_be_bytes()); // column attr
        p.extend_from_slice(&oid.to_be_bytes());
        p.extend_from_slice(&size.to_be_bytes());
        p.extend_from_slice(&(-1i32).to_be_bytes()); // typmod
        p.extend_from_slice(&0u16.to_be_bytes()); // text format
    }
    frame(MSG_ROW_DESC, &p)
}

/// DataRow in text format (`None` datum encodes as NULL / -1 length).
pub fn data_row(cells: &[Option<Vec<u8>>]) -> Vec<u8> {
    let mut p = Vec::with_capacity(16 + cells.len() * 16);
    p.extend_from_slice(&(cells.len() as u16).to_be_bytes());
    for cell in cells {
        match cell {
            Some(bytes) => {
                p.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
                p.extend_from_slice(bytes);
            }
            None => p.extend_from_slice(&(-1i32).to_be_bytes()),
        }
    }
    frame(MSG_DATA_ROW, &p)
}

/// CommandComplete with a NUL-terminated tag.
pub fn command_complete(tag: &str) -> Vec<u8> {
    let mut p = Vec::new();
    nul_term(tag, &mut p);
    frame(MSG_COMMAND_COMPLETE, &p)
}

/// EmptyQueryResponse for blank queries.
pub fn empty_query_response() -> Vec<u8> {
    frame(MSG_EMPTY_QUERY, &[])
}

/// Render one datum in text format (`None` for NULL).
pub fn datum_text(d: &engine::Datum) -> Option<Vec<u8>> {
    match d {
        engine::Datum::Null => None,
        other => Some(other.to_string().into_bytes()),
    }
}

/// Command tag for an executed statement: SELECT/INSERT/UPDATE/DELETE carry
/// row counts; everything else echoes a canonical verb.
pub fn command_tag(message: &str, rows: usize, has_columns: bool) -> String {
    if has_columns {
        return format!("SELECT {rows}");
    }
    let mut words = message.split_whitespace();
    if let (Some(n), Some(_rows), Some(verb)) = (words.next(), words.next(), words.next()) {
        if let Ok(count) = n.parse::<usize>() {
            match verb {
                "inserted" => return format!("INSERT 0 {count}"),
                "updated" => return format!("UPDATE {count}"),
                "deleted" => return format!("DELETE {count}"),
                _ => {}
            }
        }
    }
    match message {
        "BEGIN" => "BEGIN".into(),
        "COMMIT" => "COMMIT".into(),
        "ROLLBACK" => "ROLLBACK".into(),
        "OK" => "OK".into(),
        m if m.ends_with("created\"") || m.ends_with("created") => {
            if m.contains("table") {
                "CREATE TABLE".into()
            } else if m.contains("index") {
                "CREATE INDEX".into()
            } else {
                "CREATE".into()
            }
        }
        m if m.contains("dropped") => "DROP".into(),
        m if m.contains("CHECKPOINT") || m.contains("checkpoint") => "CHECKPOINT".into(),
        m => m.split_whitespace().next().unwrap_or("OK").to_uppercase(),
    }
}

/// Read one NUL-terminated string from the front of `buf`.
pub fn read_cstring(buf: &[u8]) -> Option<String> {
    let end = buf.iter().position(|b| *b == 0)?;
    Some(String::from_utf8_lossy(&buf[..end]).into_owned())
}
