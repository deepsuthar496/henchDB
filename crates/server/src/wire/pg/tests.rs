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
    // status() reads engine sessions; fresh sessions are idle.
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
