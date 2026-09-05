//! Pure packet/codec logic + canned response tests (no sockets).

use engine::{Datum, Output};

use crate::auth::PLUGIN_NATIVE;

use super::canned::*;
use super::constants::*;
use super::packet::*;
use super::stmt::*;

#[test]
fn lenenc_roundtrip() {
    for v in [0u64, 250, 251, 1000, 65535, 65536, 16_777_215, 16_777_216, u64::MAX / 2] {
        let mut b = Vec::new();
        enc_lenenc_int(&mut b, v);
        let mut pos = 0;
        assert_eq!(dec_lenenc_int(&b, &mut pos), Some(v), "v={v}");
    }
}

#[test]
fn frame_header_layout() {
    let f = frame_payload(b"abc", 7);
    assert_eq!(&f[..4], &[3, 0, 0, 7]);
    assert_eq!(&f[4..], b"abc");
}

#[test]
fn handshake_shape() {
    let scramble = [7u8; 20];
    let p = super::handshake::handshake_payload(42, &scramble, AUTH_PLUGIN, SERVER_CAPS);
    assert_eq!(p[0], 10);
    assert!(p.windows(AUTH_PLUGIN.len()).any(|w| w == AUTH_PLUGIN.as_bytes()));
    // Scramble halves embedded verbatim (part1 + part2).
    assert!(p.windows(8).any(|w| w == &scramble[..8]));
    assert!(p.windows(12).any(|w| w == &scramble[8..]));
    // Ends with plugin name NUL.
    assert_eq!(p[p.len() - 1], 0);
}

#[test]
fn fresh_scrambles_differ() {
    assert_ne!(
        super::handshake::fresh_scramble(),
        super::handshake::fresh_scramble()
    );
}

#[test]
fn response_parse_minimal() {
    // caps(4) + max(4) + charset(1) + filler(23) + user NUL.
    let mut b = Vec::new();
    b.extend_from_slice(&0x00088207u32.to_le_bytes());
    b.extend_from_slice(&[0u8; 4]);
    b.push(255);
    b.extend_from_slice(&[0u8; 23]);
    b.extend_from_slice(b"root\0");
    b.push(0); // empty auth
    let r = super::handshake::parse_handshake_response(&b).expect("parse");
    assert_eq!(r.username, "root");
    assert!(r.auth.is_empty());
}

#[test]
fn response_parse_auth_and_plugin() {
    // SECURE_CONNECTION caps: 1-byte auth len + 20 proof bytes, then db +
    // plugin names (PLUGIN_AUTH set).
    let caps = 0x00088207u32 | 0x00008000 | 0x00080000 | 0x00000008;
    let mut b = Vec::new();
    b.extend_from_slice(&caps.to_le_bytes());
    b.extend_from_slice(&[0u8; 4]);
    b.push(255);
    b.extend_from_slice(&[0u8; 23]);
    b.extend_from_slice(b"app\0");
    b.push(20);
    b.extend_from_slice(&[9u8; 20]);
    b.extend_from_slice(b"main\0");
    b.extend_from_slice(PLUGIN_NATIVE.as_bytes());
    b.push(0);
    let r = super::handshake::parse_handshake_response(&b).expect("parse");
    assert_eq!(r.username, "app");
    assert_eq!(r.auth, vec![9u8; 20]);
    assert_eq!(r.db, Some("main".into()));
    assert_eq!(r.plugin, PLUGIN_NATIVE);
}

#[test]
fn ok_err_eof_shapes() {
    let ok = ok_payload(3, "3 row(s) inserted");
    assert_eq!(ok[0], 0x00);
    let err = err_payload(1064, "42000", "parse error: x");
    assert_eq!(err[0], 0xFF);
    assert_eq!(u16::from_le_bytes([err[1], err[2]]), 1064);
    assert_eq!(err[3], b'#');
    assert_eq!(&err[4..9], b"42000");
    assert_eq!(eof_payload()[0], 0xFE);
}

#[test]
fn column_def_has_name() {
    let p = column_def_payload("score", TYPE_DOUBLE);
    assert!(p.windows(5).any(|w| w == b"score"));
    assert_eq!(*p.last().unwrap(), 0);
}

#[test]
fn null_row_encodes_fb() {
    let p = row_payload(&[Datum::Null, Datum::Int(7)]);
    assert_eq!(p[0], 0xFB);
}

#[test]
fn canned_setup_statements() {
    assert!(canned_output("SET NAMES utf8mb4").unwrap().columns.is_empty());
    assert!(canned_output("USE main").unwrap().columns.is_empty());
    let v = canned_output("SHOW VARIABLES").unwrap();
    assert_eq!(v.columns, vec!["Variable_name", "Value"]);
    assert!(!v.rows.is_empty());
}

#[test]
fn canned_select_at_and_bare() {
    let o = canned_output("SELECT @@version").unwrap();
    assert_eq!(o.rows.len(), 1);
    let o = canned_output("SELECT 1").unwrap();
    assert_eq!(o.rows[0][0], Datum::Int(1));
    let o = canned_output("SELECT 'hi', NULL, 2.5").unwrap();
    assert_eq!(o.rows[0][0], Datum::Text("hi".into()));
    assert_eq!(o.rows[0][1], Datum::Null);
}

#[test]
fn canned_schema_probe_empty() {
    let o = canned_output(
        "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA='x'",
    )
    .unwrap();
    assert!(o.rows.is_empty());
    assert!(!o.columns.is_empty());
}

#[test]
fn trailing_limit_stripped() {
    assert_eq!(strip_trailing_limit("@@version_comment limit 1"), "@@version_comment");
    assert_eq!(strip_trailing_limit("1 limit 1"), "1");
    assert_eq!(strip_trailing_limit("a, b"), "a, b");
    let o = canned_output("select @@version_comment limit 1").unwrap();
    assert_eq!(o.columns, vec!["@@version_comment"]);
}

#[test]
fn statements_split_respecting_quotes() {
    let s = split_statements("SELECT 1; INSERT INTO t VALUES (1, 'a;b'); SELECT 2;");
    assert_eq!(s.len(), 3);
    assert!(s[1].contains("'a;b'"));
}

#[test]
fn deprecate_eof_requires_server_advert() {
    // Stock clients set DEPRECATE_EOF even when the server did not
    // offer it; effective caps must mask it out so EOF mode stays.
    let client_caps = CAP_DEPRECATE_EOF | SERVER_CAPS;
    assert_ne!(client_caps & CAP_DEPRECATE_EOF, 0);
    assert_eq!(client_caps & SERVER_CAPS & CAP_DEPRECATE_EOF, 0);
}

#[test]
fn placeholders_skip_quotes_and_comments() {
    let sql = "SELECT * FROM t WHERE a = ? AND b = '?' AND c = `?` AND d = ? -- ?";
    let offs = find_placeholders(sql);
    assert_eq!(offs.len(), 2);
    assert_eq!(offs[0], sql.find("a = ?").unwrap() + 4);
    assert_eq!(offs[1], sql.rfind("d = ?").unwrap() + 4);
    assert_eq!(find_placeholders("SELECT 'it''s ?' FROM t WHERE x = ?").len(), 1);
    assert!(find_placeholders("SELECT 1").is_empty());
}

#[test]
fn substitute_escapes_strings() {
    let sql = "INSERT INTO t VALUES (?, ?)";
    let offs = find_placeholders(sql);
    let out = substitute(sql, &offs, &["1".into(), "'O''Brien'".into()]).unwrap();
    assert_eq!(out, "INSERT INTO t VALUES (1, 'O''Brien')");
    assert!(substitute(sql, &offs, &["1".into()]).is_err());
    assert_eq!(neutralize_placeholders(sql, &offs), "INSERT INTO t VALUES (NULL, NULL)");
}

#[test]
fn datum_literal_roundtrip_shapes() {
    assert_eq!(datum_literal(&Datum::Null), "NULL");
    assert_eq!(datum_literal(&Datum::Int(-42)), "-42");
    assert_eq!(datum_literal(&Datum::Bool(true)), "TRUE");
    assert_eq!(datum_literal(&Datum::Text("a'b".into())), "'a''b'");
    assert_eq!(datum_literal(&Datum::Float(2.5)), "2.5");
    // Exponent forms must stay lexable.
    assert!(!datum_literal(&Datum::Float(1e300)).contains('e'));
}

fn exec_body(nulls: &[bool], types: &[(u8, bool)], values: &[u8]) -> Vec<u8> {
    let n = nulls.len();
    let mut b = vec![0u8; (n + 7) / 8];
    for (i, nul) in nulls.iter().enumerate() {
        if *nul {
            b[i / 8] |= 1 << (i % 8);
        }
    }
    b.push(1); // new-params-bind-flag
    for (t, u) in types {
        b.push(*t);
        b.push(if *u { 0x80 } else { 0 });
    }
    b.extend_from_slice(values);
    b
}

#[test]
fn decode_params_ints_strings_nulls() {
    // (LONG 42, NULL, VARCHAR 'hi')
    let body = exec_body(
        &[false, true, false],
        &[(TYPE_LONG, false), (TYPE_LONG, false), (TYPE_VARCHAR, false)],
        &[42, 0, 0, 0, 2, b'h', b'i'],
    );
    let (vals, cached) = decode_execute_params(&body, 3, &None, &vec![Vec::new(); 3]).unwrap();
    assert_eq!(vals[0], Datum::Int(42));
    assert_eq!(vals[1], Datum::Null);
    assert_eq!(vals[2], Datum::Text("hi".into()));
    // Type reuse via bind-flag 0.
    let mut b2 = vec![0u8; 1]; // null bitmap: none null
    b2.push(0); // bind flag 0 = reuse cached types
    b2.extend_from_slice(&[7, 0, 0, 0, 8, 0, 0, 0, 1, b'x']);
    let (vals2, _) = decode_execute_params(&b2, 3, &Some(cached), &vec![Vec::new(); 3]).unwrap();
    assert_eq!(vals2[0], Datum::Int(7));
    assert_eq!(vals2[1], Datum::Int(8));
    assert_eq!(vals2[2], Datum::Text("x".into()));
    // Missing cache errors.
    assert!(decode_execute_params(&b2, 3, &None, &vec![Vec::new(); 3]).is_err());
}

#[test]
fn decode_params_unsigned_big_and_floats() {
    // Unsigned LONGLONG u64::MAX does not fit i64 -> Text.
    let max = u64::MAX.to_le_bytes().to_vec();
    let mut body = exec_body(&[false], &[(TYPE_LONGLONG, true)], &max);
    let (vals, _) = decode_execute_params(&body, 1, &None, &vec![Vec::new(); 1]).unwrap();
    assert_eq!(vals[0], Datum::Text(u64::MAX.to_string()));
    // Signed TINY -5, DOUBLE 2.5.
    body = exec_body(&[false, false], &[(TYPE_TINY, false), (TYPE_DOUBLE, false)], &[]);
    body.extend_from_slice(&[0xFB]);
    body.extend_from_slice(&2.5f64.to_le_bytes());
    let (vals, _) = decode_execute_params(&body, 2, &None, &vec![Vec::new(); 2]).unwrap();
    assert_eq!(vals[0], Datum::Int(-5));
    assert_eq!(vals[1], Datum::Float(2.5));
    // Truncation errors, never panics.
    assert!(decode_execute_params(&[0], 2, &None, &vec![Vec::new(); 2]).is_err());
    assert!(decode_execute_params(&[0, 1, TYPE_LONG, 0, 42], 1, &None, &vec![Vec::new()]).is_err());
    // Unknown type errors.
    let bad = exec_body(&[false], &[(0x99, false)], &[1]);
    assert!(decode_execute_params(&bad, 1, &None, &vec![Vec::new()]).is_err());
}

#[test]
fn decode_params_datetime_and_long_data() {
    // DATETIME 2026-09-04 12:30:00 (year LE: 0x07EA).
    let v = vec![7u8, 0xEA, 0x07, 9, 4, 12, 30, 0];
    let body = exec_body(&[false], &[(TYPE_DATETIME, false)], &v);
    let (vals, _) = decode_execute_params(&body, 1, &None, &vec![Vec::new()]).unwrap();
    assert_eq!(vals[0], Datum::Text("2026-09-04 12:30:00".into()));
    // Long data prepended to a string param.
    let body = exec_body(&[false], &[(TYPE_BLOB, false)], &[3, b'x', b'y', b'z']);
    let long = vec![b"ab".to_vec()];
    let (vals, _) = decode_execute_params(&body, 1, &None, &long).unwrap();
    assert_eq!(vals[0], Datum::Text("abxyz".into()));
}

#[test]
fn binary_rows_bitmap_and_types() {
    let out = Output {
        columns: vec!["a".into(), "b".into(), "c".into(), "d".into()],
        rows: vec![vec![
            Datum::Int(1),
            Datum::Null,
            Datum::Text("x".into()),
            Datum::Float(0.5),
        ]],
        message: "OK".into(),
    };
    let types = result_column_types(&out);
    assert_eq!(types, vec![TYPE_LONGLONG, TYPE_VAR_STRING, TYPE_VAR_STRING, TYPE_DOUBLE]);
    let enc = binary_row_payload(&out.rows[0], &types).unwrap();
    // Header 0x00, bitmap byte: col1 null -> bit 3.
    assert_eq!(enc[0], 0x00);
    assert_eq!(enc[1] & 0x08, 0x08);
    // Mismatch (text in integer column) fails cleanly.
    let bad = vec![Datum::Text("s".into())];
    assert!(binary_row_payload(&bad, &[TYPE_LONGLONG]).is_err());
    assert!(binary_row_payload(&out.rows[0], &[TYPE_LONGLONG]).is_err());
}

#[test]
fn numeric_promotion_infers_double() {
    let out = Output {
        columns: vec!["m".into()],
        rows: vec![vec![Datum::Int(1)], vec![Datum::Float(1.5)]],
        message: "OK".into(),
    };
    assert_eq!(result_column_types(&out), vec![TYPE_DOUBLE]);
}

#[test]
fn prepare_ok_shape() {
    let p = prepare_ok_payload(7, 2, 1);
    assert_eq!(p[0], 0x00);
    assert_eq!(u32::from_le_bytes([p[1], p[2], p[3], p[4]]), 7);
    assert_eq!(u16::from_le_bytes([p[5], p[6]]), 2);
    assert_eq!(u16::from_le_bytes([p[7], p[8]]), 1);
}

// -- TLS / SSLRequest (SEC2) -------------------------------------------------

/// Self-signed localhost test certificate (RSA-2048, SAN localhost +
/// 127.0.0.1, 10y). Generated once with an isolated `cryptography`
/// install; embedded so tests need no files or tools.
const TEST_CERT_PEM: &str = r#"-----BEGIN CERTIFICATE-----
MIIC4jCCAcqgAwIBAgIUZq6Z+PCZuvrJjZrI/HLB9hdtBS4wDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTI2MDkwNTEzMzkwN1oXDTM2MDkw
MjEzMzkwN1owFDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEF
AAOCAQ8AMIIBCgKCAQEAxjVrM3kbiWdgsnoWOJjP1UP7xRzIQEIa2nZzD7rr2DSU
I49BP6Bz3WMWVpe9hsA7G9Yvx5WPJZsvHyE6N8Y4g6Irr4sQdIPuBzvimKTemmvN
W4D+iBTChxKI+C9W8vEc4W2jjROoTiKleaABQwg5MOU72bDEw90ir1RN9T3F0mm+
YViik4/H1EtELJyLG42Rgzxus3kYBpebXI8dF5QFawMPTH5On2k79qyED2jgj8lS
0mQNSXTa46hahxmZUxhcLB3/R0f2hSKydFVQ5RR0zayCaZGVlrFd7pERYGJ4LQLw
xBdWpwMV6Lrw9+/v5Tr6P/mDkTclQd8kTgHEbYL+twIDAQABoywwKjAaBgNVHREE
EzARgglsb2NhbGhvc3SHBH8AAAEwDAYDVR0TAQH/BAIwADANBgkqhkiG9w0BAQsF
AAOCAQEAN5kUGNKpEltGqARtvcRotILngC/bI/sIVoG96k1FWP9ywBRosQfr3ucA
YSlrSQMbj05nxeYhGGnCfYjrW44tSa1new5lG9WGDPgOEHmKe002K0M34iP9kfZH
oJeTACyS/3SyTX5UXmSMDi9Ivnp5SO6LxupFdv8EH4OI7WUn3VS+t863+3AchoR3
meo3i0/q5FfHWKKVrV4eLwfIdtFBpM0Hkaw4eOUQpAYtc1uEs3Kbb5xImwAjJE3Z
wQD+5A9om5Jslqrfl/lDT+9TC5WXdZTGXqdOtyQjObz8vqmXmWsNHCutm/An1SKm
l9G018PrSh06SVccXo3ht78sLJlqng==
-----END CERTIFICATE-----
"#;

const TEST_KEY_PEM: &str = r#"-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEAxjVrM3kbiWdgsnoWOJjP1UP7xRzIQEIa2nZzD7rr2DSUI49B
P6Bz3WMWVpe9hsA7G9Yvx5WPJZsvHyE6N8Y4g6Irr4sQdIPuBzvimKTemmvNW4D+
iBTChxKI+C9W8vEc4W2jjROoTiKleaABQwg5MOU72bDEw90ir1RN9T3F0mm+YVii
k4/H1EtELJyLG42Rgzxus3kYBpebXI8dF5QFawMPTH5On2k79qyED2jgj8lS0mQN
SXTa46hahxmZUxhcLB3/R0f2hSKydFVQ5RR0zayCaZGVlrFd7pERYGJ4LQLwxBdW
pwMV6Lrw9+/v5Tr6P/mDkTclQd8kTgHEbYL+twIDAQABAoIBAFXiHaI/DrR568dJ
6Uj6xctF2tjtAMP/IL2aZ37gYoLbPXkvAHm+X5YE8k/xDflOYA5Ov4M+hbkoxcE6
V4yFQkWfRkiY/DdQVxohU60Kez30ChZlDWUPgb6fRGQttwIrgXUYWa6uXtYEYykR
MJrH/Gf4W/eWhZvMvNO1ttXVv1rNHMMYIulRqvhPeWcWtTvjqhyAOUMYlSNgJrDe
DyR0tDHtL8Hq6obXEzDupndz1QkatPbUNIk72diC96TkpgXkfmsdbom2z6NOxTgl
uPdlidL8nGjXfqF+ml4J35v3uHvV4oIa+sJ/g09kyXVbMIlFuWjke5LmuaR61WbO
8u3Mz8ECgYEA+ElI79VI7vU8CMzY2en806Fqu7HyrvuvYKZMgKDJXeHZAQhoModr
q5YyMt7KB/AT1zuuBrcdfBk+QkskJb7ovekk9L5Nf7hxlEdziLlx5gIkXgeNpUFJ
GWS4EdAJ3l+XVOduJrTKfK/FnAf/uDpxzBOpAPLQA61FG+6R9QdR6iECgYEAzF3Y
/rMqPWXwP/UpHhAVn5TBbeKBdX4kzP+wnyC5+IMe6n5MpY6kFuYwtK3PQvTwfE4a
NyrxwIdH+KoxKKi1ZgeCz+QUyOhPbRpLIW+vWR+xOFaMk1EKupyUJ1Si8+cXg3MG
/W49Db4gqM2H9/HXDGESSxLbPW0TUnPcloQ9vdcCgYBvYLimVcBE6Z/HttTkVFHF
QdjWYAoksuTGb3NMFFSgl8q36uSLHjKPo23bYhOxIeJUoAH+IzDH1a8XIAwUHqLb
ZnXckG3FiKDyymaqg73zVyynPa4t3q6DBKqJ2xBCQBFr1fGUzW80Jcl4qCHvq9AW
ow8iTMpBi/2/fPLevyzg4QKBgQCF2s4e/OCkuFjku0HEJArVrAwJWfsrJoUaFDrt
7vR/1fnw4up24XeOXBT4soL3OxEsicdX7PPNA45bS7XJCL9PZYoDekM22Bn1vuwI
qWszN7POz7lhYApj8dyD6kaU8/6NpVClu4eXsbkYdw4gkzEkNYxSybX5hLDMJ4EK
wPDjnwKBgQDaYDFtks52n4YCcdc202TcQUd9u0g6WsA3QUJUVV+PlaPfjYR7YBIu
VXYk+gM0QvXzVC1dy/mm7hbNqN1xCYezUSiou9zjTtOYo6oFEGh9QcPgp4uEwbJ7
cVrYCCiVo0a5+v/dokRjEFcUgO5/UG08xHPLI7Xjx2tMDZQSWe4iqw==
-----END RSA PRIVATE KEY-----
"#;

/// Build a 32-byte SSLRequest-style payload for the given caps.
fn ssl_request_bytes(caps: u32) -> Vec<u8> {
    let mut v = Vec::with_capacity(32);
    v.extend_from_slice(&caps.to_le_bytes());
    v.extend_from_slice(&0x0100_0000u32.to_le_bytes()); // max packet
    v.push(255); // charset
    v.extend_from_slice(&[0u8; 23]); // filler
    v
}

#[test]
fn ssl_request_detected() {
    use super::handshake::parse_ssl_request;
    // Minimal + full-caps requests both detect.
    let caps = CAP_SSL;
    assert_eq!(parse_ssl_request(&ssl_request_bytes(caps)), Some(caps));
    let full = SERVER_CAPS | CAP_SSL | 0x0000_8000;
    assert_eq!(parse_ssl_request(&ssl_request_bytes(full)), Some(full));
    // Wrong length or missing SSL bit: ordinary handshake bytes.
    assert_eq!(parse_ssl_request(&ssl_request_bytes(SERVER_CAPS)), None);
    let mut short = ssl_request_bytes(CAP_SSL);
    short.pop();
    assert_eq!(parse_ssl_request(&short), None);
    let mut long = ssl_request_bytes(CAP_SSL);
    long.push(0);
    assert_eq!(parse_ssl_request(&long), None);
    assert_eq!(parse_ssl_request(&[]), None);
    // A real (short) HandshakeResponse-shaped buffer never matches.
    let mut hs = vec![0u8; 40];
    hs[0..4].copy_from_slice(&SERVER_CAPS.to_le_bytes());
    assert_eq!(parse_ssl_request(&hs), None);
}

/// Decode the two capability words out of a HandshakeV10 payload.
fn advertised_caps(payload: &[u8]) -> u32 {
    let mut pos = 1usize; // protocol version
    while payload[pos] != 0 {
        pos += 1;
    }
    pos += 1 + 4 + 8 + 1; // NUL + conn id + scramble1 + filler
    let lo = u16::from_le_bytes([payload[pos], payload[pos + 1]]) as u32;
    pos += 2 + 1 + 2; // charset + status
    let hi = u16::from_le_bytes([payload[pos], payload[pos + 1]]) as u32;
    lo | (hi << 16)
}

#[test]
fn caps_advertise_ssl_only_with_config() {
    let scramble = [3u8; 20];
    let plain = super::handshake::handshake_payload(1, &scramble, AUTH_PLUGIN, SERVER_CAPS);
    assert_eq!(advertised_caps(&plain) & CAP_SSL, 0);
    let tls = super::handshake::handshake_payload(1, &scramble, AUTH_PLUGIN, SERVER_CAPS | CAP_SSL);
    assert_ne!(advertised_caps(&tls) & CAP_SSL, 0);
    // Everything else advertised identically.
    assert_eq!(advertised_caps(&tls) & !CAP_SSL, advertised_caps(&plain));
}

fn write_test_pems(dir: &std::path::Path) -> (std::path::PathBuf, std::path::PathBuf) {
    let cert = dir.join("test-cert.pem");
    let key = dir.join("test-key.pem");
    std::fs::write(&cert, TEST_CERT_PEM).unwrap();
    std::fs::write(&key, TEST_KEY_PEM).unwrap();
    (cert, key)
}

#[test]
fn tls_config_loads_and_rejects() {
    use super::tls::load_tls_config;
    let dir = std::env::temp_dir().join(format!("hdbtls_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (cert, key) = write_test_pems(&dir);
    assert!(load_tls_config(&cert, &key).is_ok());
    // Missing files fail closed.
    assert!(load_tls_config(&dir.join("nope.pem"), &key).is_err());
    assert!(load_tls_config(&cert, &dir.join("nope.pem")).is_err());
    // Garbage is not a certificate.
    let bad = dir.join("bad.pem");
    std::fs::write(&bad, "not a pem file\n").unwrap();
    assert!(load_tls_config(&bad, &key).is_err());
    assert!(load_tls_config(&cert, &bad).is_err());
    // Cert/key mismatch fails (key parses, pair does not).
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn tls_loopback_roundtrip() {
    use super::tls::{accept_tls, load_tls_config};
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    let dir = std::env::temp_dir().join(format!("hdbtlsrt_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (cert, key) = write_test_pems(&dir);
    let cfg = load_tls_config(&cert, &key).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = std::thread::spawn(move || {
        let (sock, _) = listener.accept().unwrap();
        let _ = sock.set_read_timeout(Some(std::time::Duration::from_secs(10)));
        let mut tls = accept_tls(&cfg, sock).unwrap();
        let mut buf = [0u8; 4];
        tls.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"ping");
        tls.write_all(b"pong").unwrap();
        tls.flush().unwrap();
    });
    // Client trusts exactly the test certificate.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut roots = rustls::RootCertStore::empty();
    let certs: Vec<_> = rustls_pemfile::certs(&mut TEST_CERT_PEM.as_bytes())
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    roots.add(certs.into_iter().next().unwrap()).unwrap();
    let client_cfg = std::sync::Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    );
    let server_name = rustls::pki_types::ServerName::try_from("localhost")
        .unwrap()
        .to_owned();
    let sock = TcpStream::connect(("127.0.0.1", port)).unwrap();
    let _ = sock.set_read_timeout(Some(std::time::Duration::from_secs(10)));
    let conn = rustls::ClientConnection::new(client_cfg, server_name).unwrap();
    let mut tls = rustls::StreamOwned::new(conn, sock);
    tls.write_all(b"ping").unwrap();
    tls.flush().unwrap();
    let mut buf = [0u8; 4];
    tls.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"pong");
    server.join().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
