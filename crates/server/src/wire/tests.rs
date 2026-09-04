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
    let p = super::handshake::handshake_payload(42, &scramble, AUTH_PLUGIN);
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
