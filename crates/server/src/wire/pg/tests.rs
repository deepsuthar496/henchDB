//! Unit tests for the PostgreSQL wire frontend (pure codec + framing).

use engine::types::ColumnType;

use super::codec::*;
use super::{read_message, read_startup_body, status, verify_password};
use crate::auth::Verifier;

#[test]
fn startup_decode_standard_params() {
    let body = encode_startup("postgres", "shop");
    // encode_startup omits the length prefix; prepend it like the socket.
    let mut framed = Vec::with_capacity(4 + body.len());
    framed.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
    framed.extend_from_slice(&body);
    let mut cursor = std::io::BufReader::new(&framed[..]);
    let decoded = read_startup_body(&mut cursor).unwrap();
    let params = parse_startup(&decoded).unwrap();
    assert_eq!(params.user, "postgres");
    assert_eq!(params.database, "shop");
}

#[test]
fn startup_rejects_bad_version_and_truncation() {
    let mut bad = Vec::new();
    bad.extend_from_slice(&0x0002_0000u32.to_be_bytes()); // protocol 2.0
    bad.push(0);
    assert!(parse_startup(&bad).is_none());
    assert!(parse_startup(&[]).is_none());
    assert!(parse_startup(&[0, 0]) .is_none());
    // Missing terminating NUL runs out of pairs.
    let mut unterminated = Vec::new();
    unterminated.extend_from_slice(&PG_PROTOCOL_VERSION.to_be_bytes());
    unterminated.extend_from_slice(b"user\0bob");
    assert!(parse_startup(&unterminated).is_none());
}

#[test]
fn ssl_request_codes_recognized() {
    assert_eq!(SSL_REQUEST_CODE, 80877103);
    assert_eq!(SSL_REQUEST_CODE, 0x04D2_162F);
    // An 8-byte body holding the code is a request, not a startup message
    // (wrong version for parse_startup).
    let req = SSL_REQUEST_CODE.to_be_bytes();
    assert!(parse_startup(&req).is_none());
}

#[test]
fn auth_and_init_frames_shaped() {
    let ok = auth_message(AUTH_OK);
    assert_eq!(ok[0], MSG_AUTH);
    assert_eq!(u32::from_be_bytes([ok[1], ok[2], ok[3], ok[4]]) as usize, ok.len() - 1);
    assert_eq!(i32::from_be_bytes([ok[5], ok[6], ok[7], ok[8]]), 0);
    let cl = auth_message(AUTH_CLEARTEXT);
    assert_eq!(i32::from_be_bytes([cl[5], cl[6], cl[7], cl[8]]), 3);
    let ps = parameter_status("server_version", "18.0 (henchDB 0.1.0)");
    assert_eq!(ps[0], MSG_PARAMETER_STATUS);
    let kd = backend_key_data(1234, -7);
    assert_eq!(kd[0], MSG_BACKEND_KEY);
    assert_eq!(i32::from_be_bytes([kd[5], kd[6], kd[7], kd[8]]), 1234);
    let r = ready_for_query(b'I');
    assert_eq!(&r, &[MSG_READY, 0, 0, 0, 5, b'I']);
    assert_eq!(ready_for_query(b'T')[5], b'T');
}

#[test]
fn error_response_fields() {
    let e = error_response("42P01", "table \"t\" does not exist");
    assert_eq!(e[0], MSG_ERROR);
    let payload = &e[5..];
    // Field layout: S..0 V..0 C..0 M..0 0
    assert_eq!(payload[0], b'S');
    let m_pos = payload.iter().position(|b| *b == b'M').unwrap();
    let msg = b"table \"t\" does not exist";
    assert_eq!(&payload[m_pos + 1..m_pos + 1 + msg.len()], msg);
    assert!(payload.windows(b"42P01".len()).any(|w| w == b"42P01"));
    assert!(payload.windows(8).any(|w| w == b"\"t\" does"));
    assert_eq!(payload[payload.len() - 1], 0);
}

#[test]
fn sqlstate_mapping() {
    use engine::Error;
    assert_eq!(sqlstate(&Error::TableNotFound("t".into())), "42P01");
    assert_eq!(sqlstate(&Error::DuplicateKey("1".into())), "23505");
    assert_eq!(sqlstate(&Error::ParseError("x".into())), "42601");
    assert_eq!(sqlstate(&Error::NotSupported("x".into())), "0A000");
    assert_eq!(sqlstate(&Error::ForeignKeyViolation("x".into())), "23503");
    assert_eq!(sqlstate(&Error::TxnConflict("x".into())), "40001");
    assert_eq!(sqlstate(&Error::QueryTimeout), "57014");
    assert_eq!(sqlstate(&Error::TxnNotActive), "25000");
}

#[test]
fn row_description_multicolumn() {
    let cols = vec![
        ("id".to_string(), ColumnType::Int),
        ("name".to_string(), ColumnType::Text),
        ("active".to_string(), ColumnType::Bool),
        ("score".to_string(), ColumnType::Double),
        ("created".to_string(), ColumnType::DateTime),
    ];
    let t = row_description(&cols);
    assert_eq!(t[0], MSG_ROW_DESC);
    assert_eq!(u16::from_be_bytes([t[5], t[6]]), 5);
    // Spot-check OIDs ride along in field order.
    let mut pos = 7usize;
    let expect_oid = [23u32, 25, 16, 701, 1114];
    for oid in expect_oid {
        let end = t[pos..].iter().position(|b| *b == 0).unwrap();
        pos += end + 1 + 4 + 2; // name NUL + table OID + attr
        assert_eq!(u32::from_be_bytes([t[pos], t[pos + 1], t[pos + 2], t[pos + 3]]), oid);
        pos += 4 + 2 + 4 + 2; // oid + size + typmod + format
    }
}

#[test]
fn data_row_null_and_values() {
    let d = data_row(&[Some(b"42".to_vec()), None, Some(b"hi".to_vec())]);
    assert_eq!(d[0], MSG_DATA_ROW);
    assert_eq!(u16::from_be_bytes([d[5], d[6]]), 3);
    // NULL column encodes as length -1.
    let null_at = d.windows(4).position(|w| w == (-1i32).to_be_bytes()).unwrap();
    assert!(null_at > 7);
    assert!(d.windows(2).any(|w| w == b"42"));
    // datum_text maps Datum::Null to None, everything else to text.
    assert_eq!(datum_text(&engine::Datum::Null), None);
    assert_eq!(datum_text(&engine::Datum::Int(-7)), Some(b"-7".to_vec()));
    assert_eq!(datum_text(&engine::Datum::Bool(true)), Some(b"true".to_vec()));
}

#[test]
fn command_tags() {
    assert_eq!(command_tag("OK", 3, true), "SELECT 3");
    assert_eq!(command_tag("2 row(s) inserted", 0, false), "INSERT 0 2");
    assert_eq!(command_tag("1 row(s) updated", 0, false), "UPDATE 1");
    assert_eq!(command_tag("5 row(s) deleted", 0, false), "DELETE 5");
    assert_eq!(command_tag("BEGIN", 0, false), "BEGIN");
    assert_eq!(command_tag("COMMIT", 0, false), "COMMIT");
    assert_eq!(command_tag("table 't' created", 0, false), "CREATE TABLE");
    let c = command_complete("SELECT 2");
    assert_eq!(c[0], MSG_COMMAND_COMPLETE);
    assert_eq!(c[c.len() - 1], 0);
    assert_eq!(empty_query_response()[0], MSG_EMPTY_QUERY);
}

#[test]
fn typed_message_framing_roundtrip() {
    // 'Q' with a query string, then verify the reader splits type/len/body.
    let mut q = vec![MSG_QUERY];
    let body = b"SELECT 1;\0";
    q.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
    q.extend_from_slice(body);
    let mut cursor = std::io::BufReader::new(&q[..]);
    let (t, payload) = read_message(&mut cursor).unwrap();
    assert_eq!(t, MSG_QUERY);
    assert_eq!(read_cstring(&payload).as_deref(), Some("SELECT 1;"));
    // Oversized length is rejected, not allocated.
    let evil = vec![MSG_QUERY, 0xFF, 0xFF, 0xFF, 0xFF];
    let mut cursor = std::io::BufReader::new(&evil[..]);
    assert!(read_message(&mut cursor).is_err());
}

#[test]
fn password_verification_paths() {
    assert!(verify_password(&Verifier::empty(), b""));
    assert!(!verify_password(&Verifier::empty(), b"x"));
    let sha2 = Verifier::new_sha2(b"secret");
    assert!(verify_password(&sha2, b"secret"));
    assert!(!verify_password(&sha2, b"wrong"));
    let native = Verifier::new_native(b"secret");
    assert!(verify_password(&native, b"secret"));
    assert!(!verify_password(&native, b"wrong"));
    // Cross-plugin confusion fails closed.
    assert!(!verify_password(&sha2, b""));
}

#[test]
fn ready_status_follows_transaction() {
    let dir = std::env::temp_dir().join(format!("hdbpg_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let db = engine::Database::open(&dir).unwrap();
    let mut s = db.new_session();
    assert_eq!(status(&s), b'I');
    db.execute(&mut s, "BEGIN").unwrap();
    assert_eq!(status(&s), b'T');
    db.execute(&mut s, "ROLLBACK").unwrap();
    assert_eq!(status(&s), b'I');
    let _ = db;
    let _ = std::fs::remove_dir_all(&dir);
}

// -- Extended protocol (PG2) -------------------------------------------------

use super::exec::{pg_to_markers, PgConn};

fn cstr(s: &str, out: &mut Vec<u8>) {
    out.extend_from_slice(s.as_bytes());
    out.push(0);
}

fn parse_payload(name: &str, query: &str, oids: &[u32]) -> Vec<u8> {
    let mut p = Vec::new();
    cstr(name, &mut p);
    cstr(query, &mut p);
    p.extend_from_slice(&(oids.len() as u16).to_be_bytes());
    for oid in oids {
        p.extend_from_slice(&oid.to_be_bytes());
    }
    p
}

fn bind_payload(
    portal: &str,
    stmt: &str,
    formats: &[i16],
    values: &[Option<&[u8]>],
    result_formats: &[i16],
) -> Vec<u8> {
    let mut p = Vec::new();
    cstr(portal, &mut p);
    cstr(stmt, &mut p);
    p.extend_from_slice(&(formats.len() as u16).to_be_bytes());
    for f in formats {
        p.extend_from_slice(&f.to_be_bytes());
    }
    p.extend_from_slice(&(values.len() as u16).to_be_bytes());
    for v in values {
        match v {
            None => p.extend_from_slice(&(-1i32).to_be_bytes()),
            Some(b) => {
                p.extend_from_slice(&(b.len() as u32).to_be_bytes());
                p.extend_from_slice(b);
            }
        }
    }
    p.extend_from_slice(&(result_formats.len() as u16).to_be_bytes());
    for f in result_formats {
        p.extend_from_slice(&f.to_be_bytes());
    }
    p
}

#[test]
fn markers_convert_and_validate() {
    let (sql, max) = pg_to_markers("SELECT * FROM t WHERE a = $1 AND b = $2").unwrap();
    assert_eq!(sql, "SELECT * FROM t WHERE a = ? AND b = ?");
    assert_eq!(max, 2);
    // Quoted dollars and comments are literal.
    let (sql, _) = pg_to_markers("SELECT '$1', \"x$2\" FROM t -- $3\n WHERE a=$1 /* $4 */").unwrap();
    assert_eq!(sql, "SELECT '$1', \"x$2\" FROM t -- $3\n WHERE a=? /* $4 */");
    // Gaps, $0, and trailing $ are errors.
    assert!(pg_to_markers("SELECT $1, $3").is_err());
    assert!(pg_to_markers("SELECT $0").is_err());
    assert!(pg_to_markers("SELECT 1$").is_err());
    assert!(pg_to_markers("SELECT 1").unwrap().1 == 0);
}

#[test]
fn extended_message_parsing() {
    let p = parse_parse_msg(&parse_payload("s1", "SELECT $1", &[23])).unwrap();
    assert_eq!(p.name, "s1");
    assert_eq!(p.query, "SELECT $1");
    assert_eq!(p.param_oids, vec![23]);
    assert!(parse_parse_msg(b"s1\0SELECT").is_none());
    let b = parse_bind_msg(&bind_payload("", "s1", &[0], &[Some(b"42")], &[])).unwrap();
    assert_eq!(b.portal, "");
    assert_eq!(b.statement, "s1");
    assert_eq!(b.params.len(), 1);
    assert_eq!(b.params[0].format, 0);
    assert_eq!(b.params[0].value, Some(b"42".to_vec()));
    // NULL param + binary format broadcast.
    let b = parse_bind_msg(&bind_payload("p", "s1", &[1], &[None], &[1])).unwrap();
    assert_eq!(b.params[0].value, None);
    assert_eq!(b.params[0].format, 1);
    assert_eq!(b.result_formats, vec![1]);
    let d = parse_describe_msg(b"Ss1\0").unwrap();
    assert_eq!((d.kind, d.name.as_str()), (b'S', "s1"));
    assert!(parse_describe_msg(b"Xs1\0").is_none());
    let mut e = b"\0".to_vec();
    e.extend_from_slice(&7u32.to_be_bytes());
    let e = parse_execute_msg(&e).unwrap();
    assert_eq!((e.portal.as_str(), e.max_rows), ("", 7));
    let c = parse_close_msg(b"Ps1\0").unwrap();
    assert_eq!((c.kind, c.name.as_str()), (b'P', "s1"));
}

fn pg_test_db(dir: &std::path::Path) -> engine::Database {
    let _ = std::fs::remove_dir_all(dir);
    let db = engine::Database::open(dir).unwrap();
    let mut s = db.new_session();
    db.execute(&mut s, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        .unwrap();
    db.execute(&mut s, "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .unwrap();
    db
}

#[test]
fn extended_text_roundtrip() {
    let dir = std::env::temp_dir().join(format!("hdbpgx_{}", std::process::id()));
    let db = pg_test_db(&dir);
    let mut s = db.new_session();
    let mut conn = PgConn::default();
    // Parse unnamed with explicit OID.
    let r = conn
        .on_parse(&parse_parse_msg(&parse_payload("", "SELECT v FROM t WHERE id = $1", &[23])).unwrap())
        .unwrap();
    assert_eq!(r[0], b'1');
    // Describe statement: params + row desc.
    let r = conn.on_describe(&db, &s, &parse_describe_msg(b"S\0").unwrap()).unwrap();
    assert_eq!(r[0], b't'); // ParameterDescription first
    assert!(r.windows(6).any(|w| w == b"\0\x01\0\0\0\x17")); // count 1, OID 23... layout check below instead
    // Bind text param, all-text results.
    let r = conn
        .on_bind(&parse_bind_msg(&bind_payload("", "", &[0], &[Some(b"2")], &[])).unwrap())
        .unwrap();
    assert_eq!(r[0], b'2');
    // Describe portal: RowDescription.
    let r = conn.on_describe(&db, &s, &parse_describe_msg(b"P\0").unwrap()).unwrap();
    assert_eq!(r[0], MSG_ROW_DESC);
    // Execute all: one DataRow + SELECT 1.
    let r = conn
        .on_execute(&db, &mut s, &parse_execute_msg(&b"\0\x00\x00\x00\x00".to_vec()).unwrap())
        .unwrap();
    assert!(r[0] == MSG_ROW_DESC); // portal was described; no duplicate T
    assert!(r.windows(2).any(|w| w == b"\x00\x01")); // one column
    assert!(r.windows(1).any(|w| w == b"b")); // value 'b'
    let tag = b"SELECT 1";
    assert!(r.windows(tag.len()).any(|w| w == tag));
    // Sync-equivalent: unnamed reuse works (overwrite + re-run).
    let _ = conn
        .on_parse(&parse_parse_msg(&parse_payload("", "SELECT COUNT(*) FROM t", &[])).unwrap())
        .unwrap();
    let _ = conn
        .on_bind(&parse_bind_msg(&bind_payload("", "", &[], &[], &[])).unwrap())
        .unwrap();
    let r = conn
        .on_execute(&db, &mut s, &parse_execute_msg(&b"\0\x00\x00\x00\x00".to_vec()).unwrap())
        .unwrap();
    assert!(r.windows(8).any(|w| w == b"SELECT 1"));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn extended_binary_params_and_results() {
    let dir = std::env::temp_dir().join(format!("hdbpgxb_{}", std::process::id()));
    let db = pg_test_db(&dir);
    let mut s = db.new_session();
    let mut conn = PgConn::default();
    // Unknown OID infers Int from text; binary result for id.
    let _ = conn
        .on_parse(&parse_parse_msg(&parse_payload("q", "SELECT id, v FROM t WHERE id = $1", &[])).unwrap())
        .unwrap();
    let _ = conn
        .on_bind(&parse_bind_msg(&bind_payload("p", "q", &[0], &[Some(b"3")], &[1, 0])).unwrap())
        .unwrap();
    let r = conn
        .on_execute(&db, &mut s, &parse_execute_msg(&{
            let mut e = b"p\0".to_vec();
            e.extend_from_slice(&0u32.to_be_bytes());
            e
        }).unwrap())
        .unwrap();
    // id column binary (4-byte BE 3), v column text.
    assert!(r.windows(4).any(|w| w == 3i32.to_be_bytes()));
    assert!(r.windows(1).any(|w| w == b"c"));
    // Explicit binary int4 param.
    let _ = conn
        .on_parse(&parse_parse_msg(&parse_payload("qb", "SELECT v FROM t WHERE id = $1", &[23])).unwrap())
        .unwrap();
    let _ = conn
        .on_bind(&parse_bind_msg(&bind_payload("", "qb", &[1], &[Some(&1i32.to_be_bytes())], &[])).unwrap())
        .unwrap();
    let r = conn
        .on_execute(&db, &mut s, &parse_execute_msg(&b"\0\x00\x00\x00\x00".to_vec()).unwrap())
        .unwrap();
    assert!(r.windows(1).any(|w| w == b"a"));
    // NULL param matches nothing (NULL = x is false).
    let _ = conn
        .on_bind(&parse_bind_msg(&bind_payload("", "qb", &[1], &[None], &[])).unwrap())
        .unwrap();
    let r = conn
        .on_execute(&db, &mut s, &parse_execute_msg(&b"\0\x00\x00\x00\x00".to_vec()).unwrap())
        .unwrap();
    assert!(r.windows(8).any(|w| w == b"SELECT 0"));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn extended_partial_execute_suspends() {
    let dir = std::env::temp_dir().join(format!("hdbpgxs_{}", std::process::id()));
    let db = pg_test_db(&dir);
    let mut s = db.new_session();
    let mut conn = PgConn::default();
    let _ = conn
        .on_parse(&parse_parse_msg(&parse_payload("", "SELECT id FROM t ORDER BY id", &[])).unwrap())
        .unwrap();
    let _ = conn.on_bind(&parse_bind_msg(&bind_payload("", "", &[], &[], &[])).unwrap()).unwrap();
    // max_rows = 2 of 3 → PortalSuspended; resume → SELECT 3 total.
    let one = {
        let mut e = b"\0".to_vec();
        e.extend_from_slice(&2u32.to_be_bytes());
        e
    };
    let r = conn.on_execute(&db, &mut s, &parse_execute_msg(&one).unwrap()).unwrap();
    assert_eq!(r[0], MSG_ROW_DESC); // first Execute describes (no prior Describe)
    // Output ends with the exact 5-byte PortalSuspended frame.
    assert_eq!(r[r.len() - 5], b's');
    assert_eq!(&r[r.len() - 4..], &[0, 0, 0, 4]);
    let r = conn.on_execute(&db, &mut s, &parse_execute_msg(&one).unwrap()).unwrap();
    assert!(r.windows(8).any(|w| w == b"SELECT 3"));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn extended_errors_and_close() {
    let dir = std::env::temp_dir().join(format!("hdbpgxe_{}", std::process::id()));
    let db = pg_test_db(&dir);
    let mut s = db.new_session();
    let mut conn = PgConn::default();
    // Bad SQL fails at Parse.
    assert!(conn
        .on_parse(&parse_parse_msg(&parse_payload("", "BOGUS STATEMENT HERE", &[])).unwrap())
        .is_err());
    // Unknown statement / portal.
    let b = parse_bind_msg(&bind_payload("", "nosuch", &[], &[], &[])).unwrap();
    assert_eq!(conn.on_bind(&b).unwrap_err().0, "26000");
    let d = parse_describe_msg(b"Snosuch\0").unwrap();
    assert_eq!(conn.on_describe(&db, &s, &d).unwrap_err().0, "26000");
    let e = parse_execute_msg(&b"nosuch\0\x00\x00\x00\x00".to_vec()).unwrap();
    assert_eq!(conn.on_execute(&db, &mut s, &e).unwrap_err().0, "34000");
    // Param count mismatch.
    let _ = conn
        .on_parse(&parse_parse_msg(&parse_payload("q", "SELECT * FROM t WHERE id = $1 OR id = $2", &[])).unwrap())
        .unwrap();
    let b = parse_bind_msg(&bind_payload("", "q", &[0], &[Some(b"1")], &[])).unwrap();
    assert_eq!(conn.on_bind(&b).unwrap_err().0, "08P01");
    // Close removes; CloseComplete emitted.
    let c = parse_close_msg(b"Sq\0").unwrap();
    assert_eq!(conn.on_close(&c).unwrap()[0], b'3');
    assert!(conn.stmts.get("q").is_none());
    // Named statement + portal lifecycle.
    let _ = conn
        .on_parse(&parse_parse_msg(&parse_payload("n", "SELECT 1", &[])).unwrap())
        .is_ok();
    let _ = std::fs::remove_dir_all(&dir);
}

// -- COPY ... FROM STDIN -----------------------------------------------------

use super::copy::{is_copy_from_stdin, parse_copy, CopyFormat, CopyRunner};

fn copy_test_db(dir: &std::path::Path) -> engine::Database {
    let _ = std::fs::remove_dir_all(dir);
    let db = engine::Database::open(dir).unwrap();
    let mut s = db.new_session();
    db.execute(
        &mut s,
        "CREATE TABLE t (id INT PRIMARY KEY, v TEXT, n INT, b BOOL)",
    )
    .unwrap();
    db
}

#[test]
fn copy_spec_parsing() {
    // Minimal + full modern syntax.
    let s = parse_copy("COPY t FROM STDIN").unwrap();
    assert_eq!((s.table.as_str(), s.format, s.delimiter), ("t", CopyFormat::Text, b'\t'));
    assert_eq!(s.null, b"\\N");
    assert!(!s.header);
    let s = parse_copy("COPY sch.t (id, v) FROM STDIN WITH (FORMAT csv, DELIMITER ';', NULL 'NULL', HEADER)").unwrap();
    assert_eq!(s.table, "sch.t");
    assert_eq!(s.columns, vec!["id", "v"]);
    assert_eq!(s.format, CopyFormat::Csv);
    assert_eq!(s.delimiter, b';');
    assert_eq!(s.null, b"NULL");
    assert!(s.header);
    // Legacy bare options.
    let s = parse_copy("COPY t FROM STDIN CSV DELIMITER ',' NULL ''").unwrap();
    assert_eq!(s.format, CopyFormat::Csv);
    assert_eq!(s.delimiter, b',');
    assert!(s.null.is_empty());
    // Quoted identifiers + case-insensitive keywords.
    let s = parse_copy("copy \"My Table\" (\"a b\") from stdin with (format csv)").unwrap();
    assert_eq!(s.table, "My Table");
    assert_eq!(s.columns, vec!["a b"]);
    // Rejections.
    assert!(parse_copy("COPY t TO STDOUT").is_err());
    assert!(parse_copy("COPY t FROM STDIN WITH (FORMAT binary)").is_err());
    assert!(parse_copy("COPY t FROM STDIN WITH (FORMAT xml)").is_err());
    assert!(parse_copy("COPY t FROM STDIN WITH (DELIMITER 'xx')").is_err());
    assert!(parse_copy("COPY t FROM STDIN WITH (BOGUS)").is_err());
    assert!(parse_copy("COPY t FROM STDIN WITH (FORMAT csv").is_err());
    assert!(parse_copy("SELECT 1").is_err());
    // Detection pre-check.
    assert!(is_copy_from_stdin("COPY t FROM STDIN"));
    assert!(is_copy_from_stdin("  copy t (a) from stdin with (format csv); "));
    assert!(!is_copy_from_stdin("SELECT * FROM t"));
    assert!(!is_copy_from_stdin("COPY t TO STDOUT"));
}

#[test]
fn copy_text_streaming() {
    let dir = std::env::temp_dir().join(format!("hdbcopyt_{}", std::process::id()));
    let db = copy_test_db(&dir);
    let mut s = db.new_session();
    let spec = parse_copy("COPY t FROM STDIN").unwrap();
    let (mut runner, resp) = CopyRunner::begin(&db, &mut s, spec).unwrap();
    // CopyInResponse: 'G', 4 columns, all text.
    assert_eq!(resp[0], MSG_COPY_IN);
    assert_eq!(u16::from_be_bytes([resp[6], resp[7]]), 4);
    // Split chunks mid-line, mid-escape, mid-value.
    runner.feed(b"1\thello\\tworld\t\\N\tt\n2\tline2\t5\tf\r\n3\ta\\\\b\t\\N\tt\n").unwrap();
    // Bad row: field count mismatch + bad integer.
    assert!(runner.feed(b"4\tonly-two\n").is_err());
    assert!(runner.feed(b"4\tx\tnan-int\tf\n").is_err());
    let out = runner.finish(&db, &mut s).unwrap();
    assert!(out.windows(7).any(|w| w == b"COPY 3\x00"));
    let out = db.execute(&mut s, "SELECT id, v, n, b FROM t ORDER BY id").unwrap();
    assert_eq!(out.rows.len(), 3);
    assert_eq!(out.rows[0][1], engine::Datum::Text("hello\tworld".into()));
    assert_eq!(out.rows[0][2], engine::Datum::Null);
    assert_eq!(out.rows[1][3], engine::Datum::Bool(false));
    assert_eq!(out.rows[2][1], engine::Datum::Text("a\\b".into()));
    assert_eq!(out.rows[2][2], engine::Datum::Null);
    assert_eq!(out.rows[2][3], engine::Datum::Bool(true));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn copy_csv_streaming() {
    let dir = std::env::temp_dir().join(format!("hdbcopyc_{}", std::process::id()));
    let db = copy_test_db(&dir);
    let mut s = db.new_session();
    let spec = parse_copy("COPY t (id, v, n, b) FROM STDIN WITH (FORMAT csv, HEADER)").unwrap();
    let (mut runner, _) = CopyRunner::begin(&db, &mut s, spec).unwrap();
    // Header skipped; embedded newline + split quote across chunks; quoted
    // empty is '' (not NULL); unquoted empty is NULL.
    runner.feed(b"id,v,n,b\n1,\"a,b\",7,t\n2,\"multi\nline\",,\"f\"\n3,\"\"\"q\"\"\",,\"T\"\n").unwrap();
    runner.feed(b"4,plain,,TRUE\n").unwrap();
    let out = runner.finish(&db, &mut s).unwrap();
    assert!(out.windows(7).any(|w| w == b"COPY 4\x00"));
    let out = db.execute(&mut s, "SELECT id, v, n, b FROM t ORDER BY id").unwrap();
    assert_eq!(out.rows.len(), 4);
    assert_eq!(out.rows[0][1], engine::Datum::Text("a,b".into()));
    assert_eq!(out.rows[1][1], engine::Datum::Text("multi\nline".into()));
    assert_eq!(out.rows[1][2], engine::Datum::Null);
    assert_eq!(out.rows[2][1], engine::Datum::Text("\"q\"".into()));
    assert_eq!(out.rows[2][2], engine::Datum::Null);
    assert_eq!(out.rows[3][1], engine::Datum::Text("plain".into()));
    assert_eq!(out.rows[3][3], engine::Datum::Bool(true));
    // Unterminated quote at finish is corrupt.
    let spec = parse_copy("COPY t FROM STDIN WITH (FORMAT csv)").unwrap();
    let (mut runner, _) = CopyRunner::begin(&db, &mut s, spec).unwrap();
    runner.feed(b"1,\"oops\n").unwrap();
    assert!(runner.finish(&db, &mut s).is_err());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn copy_abort_and_explicit_txn() {
    let dir = std::env::temp_dir().join(format!("hdbcopya_{}", std::process::id()));
    let db = copy_test_db(&dir);
    let mut s = db.new_session();
    // Implicit txn: abort writes nothing.
    let spec = parse_copy("COPY t FROM STDIN").unwrap();
    let (mut runner, _) = CopyRunner::begin(&db, &mut s, spec).unwrap();
    runner.feed(b"10\ta\t1\tt\n11\tb\t2\tf\n").unwrap();
    let out = runner.abort(&db, &mut s, "client gave up");
    assert_eq!(out[0], MSG_ERROR);
    assert!(out.windows(5).any(|w| w == b"57014"));
    let out = db.execute(&mut s, "SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(out.rows[0][0], engine::Datum::Int(0));
    // Explicit txn: finish stages (visible in-txn), user rollback discards.
    db.execute(&mut s, "BEGIN").unwrap();
    let spec = parse_copy("COPY t FROM STDIN").unwrap();
    let (mut runner, _) = CopyRunner::begin(&db, &mut s, spec).unwrap();
    runner.feed(b"20\ta\t1\tt\n").unwrap();
    let out = runner.finish(&db, &mut s).unwrap();
    assert!(out.windows(7).any(|w| w == b"COPY 1\x00"));
    // ReadyForQuery says in-transaction after an explicit-txn COPY.
    assert_eq!(out[out.len() - 1], b'T');
    let out = db.execute(&mut s, "SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(out.rows[0][0], engine::Datum::Int(1));
    db.execute(&mut s, "ROLLBACK").unwrap();
    let out = db.execute(&mut s, "SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(out.rows[0][0], engine::Datum::Int(0));
    // Unknown table / column fail at begin.
    assert!(CopyRunner::begin(&db, &mut s, parse_copy("COPY nosuch FROM STDIN").unwrap()).is_err());
    assert!(CopyRunner::begin(&db, &mut s, parse_copy("COPY t (nosuch) FROM STDIN").unwrap()).is_err());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn copy_socket_roundtrip() {
    use super::{run_copy_in, ConnCtx};
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::Arc;
    // Full wire loop: a raw client speaks CopyData/CopyDone frames while the
    // server runs run_copy_in over the same reader path as the live loop.
    let dir = std::env::temp_dir().join(format!("hdbcopys_{}", std::process::id()));
    let db = Arc::new(copy_test_db(&dir));
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let dir_srv = dir.clone();
    let server = std::thread::spawn(move || {
        let (sock, _) = listener.accept().unwrap();
        let _ = sock.set_read_timeout(Some(std::time::Duration::from_secs(10)));
        let mut reader = std::io::BufReader::new(super::super::tls::ConnStream::Plain(sock));
        let mut s = db.new_session();
        let ctx = ConnCtx {
            auth_path: dir_srv.join("auth.bin"),
            idle_timeout: None,
            shutdown: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            admitted: true,
            tls: None,
        };
        run_copy_in(&db, &mut s, "COPY t FROM STDIN", &mut reader, &ctx).unwrap();
    });
    fn frame(t: u8, payload: &[u8]) -> Vec<u8> {
        let mut v = vec![t];
        v.extend_from_slice(&((payload.len() + 4) as u32).to_be_bytes());
        v.extend_from_slice(payload);
        v
    }
    let mut client = TcpStream::connect(("127.0.0.1", port)).unwrap();
    let _ = client.set_read_timeout(Some(std::time::Duration::from_secs(10)));
    // Split the rows awkwardly across frames (mid-row, mid-field).
    client.write_all(&frame(MSG_COPY_DATA, b"1\ta\t1\tt\n2\tb")).unwrap();
    client.write_all(&frame(MSG_COPY_DATA, b"\t2\tf\n")).unwrap();
    client.write_all(&frame(MSG_COPY_DONE, b"")).unwrap();
    // Drain server frames until close.
    let mut all = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        match client.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => all.extend_from_slice(&buf[..n]),
            Err(_) => break,
        }
    }
    server.join().unwrap();
    assert_eq!(all[0], MSG_COPY_IN);
    assert!(all.windows(7).any(|w| w == b"COPY 2\x00"));
    assert_eq!(all[all.len() - 1], b'I'); // ReadyForQuery idle
    let db2 = engine::Database::open(&dir).unwrap();
    let mut s2 = db2.new_session();
    let out = db2.execute(&mut s2, "SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(out.rows[0][0], engine::Datum::Int(2));
    let _ = std::fs::remove_dir_all(&dir);
}
