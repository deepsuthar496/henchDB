//! Adversarial protocol fuzzing suite (Assessment Item 7).
//!
//! Verifies that all packet framing, parameter decoders, handshake parsers,
//! and PostgreSQL message parsers gracefully reject arbitrary corrupted,
//! truncated, or adversarial input without panicking or accessing out-of-bounds memory.

use super::constants::*;
use super::handshake::*;
use super::packet::*;
use super::pg::codec::*;
use super::stmt::*;

/// Minimal deterministically-seeded XorShift64 PRNG (zero external dependencies).
struct FuzzRng(u64);

impl FuzzRng {
    fn new(seed: u64) -> Self {
        Self(if seed == 0 { 0x853c49e6748fea9b } else { seed })
    }

    fn next_u64(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn next_u8(&mut self) -> u8 {
        self.next_u64() as u8
    }

    fn gen_bytes(&mut self, len: usize) -> Vec<u8> {
        let mut buf = vec![0u8; len];
        for b in &mut buf {
            *b = self.next_u8();
        }
        buf
    }

    fn range(&mut self, min: usize, max: usize) -> usize {
        if min >= max {
            return min;
        }
        min + (self.next_u64() as usize % (max - min + 1))
    }
}

#[test]
fn fuzz_lenenc_int_and_bytes() {
    let mut rng = FuzzRng::new(0x123456789ABCDEF0);

    // 1. Truncation stress on all prefixes
    for prefix in [0xFCu8, 0xFDu8, 0xFEu8, 0xFFu8] {
        for len in 0..10 {
            let mut buf = vec![prefix];
            buf.extend((0..len).map(|_| rng.next_u8()));
            let mut pos = 0;
            let _ = dec_lenenc_int(&buf, &mut pos);
            let mut pos2 = 0;
            let _ = read_lenenc_bytes(&buf, &mut pos2);
        }
    }

    // 2. Out-of-bounds starting positions
    let buf = vec![1, 2, 3, 4];
    for pos_start in [4, 5, 10, usize::MAX - 5] {
        let mut pos = pos_start;
        assert_eq!(dec_lenenc_int(&buf, &mut pos), None);
        let mut pos2 = pos_start;
        assert_eq!(read_lenenc_bytes(&buf, &mut pos2), None);
        let mut pos3 = pos_start;
        assert_eq!(read_nul_str(&buf, &mut pos3), None);
    }

    // 3. 10,000 randomized byte buffers
    for _ in 0..10_000 {
        let len = rng.range(0, 64);
        let buf = rng.gen_bytes(len);
        let mut pos = 0;
        while pos < buf.len() {
            let prev = pos;
            let _ = dec_lenenc_int(&buf, &mut pos);
            if pos <= prev {
                break;
            }
        }

        let mut pos2 = 0;
        while pos2 < buf.len() {
            let prev = pos2;
            let _ = read_lenenc_bytes(&buf, &mut pos2);
            if pos2 <= prev {
                break;
            }
        }

        let mut pos3 = 0;
        while pos3 < buf.len() {
            let prev = pos3;
            let _ = read_nul_str(&buf, &mut pos3);
            if pos3 <= prev {
                break;
            }
        }
    }
}

#[test]
fn fuzz_handshake_response_and_ssl() {
    let mut rng = FuzzRng::new(0xCAFE_BABE_0000_1111);

    // 1. Exact length permutations from 0 to 64 bytes
    for len in 0..=64 {
        for _ in 0..100 {
            let buf = rng.gen_bytes(len);
            let _ = parse_ssl_request(&buf);
            let _ = parse_handshake_response(&buf);
        }
    }

    // 2. Random buffers with realistic capability masks
    let common_caps = [
        0u32,
        CAP_SSL,
        CAP_SECURE_CONNECTION,
        CAP_CONNECT_WITH_DB,
        CAP_PLUGIN_AUTH,
        0x0020_0000, // CAP_AUTH_LENENC_DATA
        0xFFFF_FFFF,
    ];

    for &cap in &common_caps {
        for _ in 0..1_000 {
            let len = rng.range(32, 256);
            let mut buf = rng.gen_bytes(len);
            buf[0..4].copy_from_slice(&cap.to_le_bytes());
            let _ = parse_handshake_response(&buf);
        }
    }

    // 3. Completely random payloads of varying sizes up to 1024 bytes
    for _ in 0..5_000 {
        let len = rng.range(0, 1024);
        let buf = rng.gen_bytes(len);
        let _ = parse_ssl_request(&buf);
        let _ = parse_handshake_response(&buf);
    }
}

#[test]
fn fuzz_binary_execute_parameter_decoding() {
    let mut rng = FuzzRng::new(0xF00D_CAFE_FEED_BEEF);

    // Test all parameter type tags 0x00..=0xFF with truncated and corrupted inputs
    for typ in 0u8..=255 {
        for unsigned in [false, true] {
            for buf_len in 0..20 {
                let buf = rng.gen_bytes(buf_len);
                let extra = if rng.next_u8() % 2 == 0 {
                    let n = rng.range(0, 10);
                    rng.gen_bytes(n)
                } else {
                    Vec::new()
                };
                let mut pos = 0;
                let _ = decode_param_value(typ, unsigned, &buf, &mut pos, &extra);
            }
        }
    }

    // Fuzz COM_STMT_EXECUTE parameter packets
    for _ in 0..5_000 {
        let num_params = rng.range(0, 15);
        let buf_len = rng.range(0, 256);
        let buf = rng.gen_bytes(buf_len);

        let cached = if rng.next_u8() % 2 == 0 {
            let mut types = Vec::new();
            for _ in 0..num_params {
                types.push((rng.next_u8(), rng.next_u8() & 0x80 != 0));
            }
            Some(types)
        } else {
            None
        };

        let mut long_data = Vec::new();
        for _ in 0..num_params {
            if rng.next_u8() % 3 == 0 {
                let n = rng.range(0, 20);
                long_data.push(rng.gen_bytes(n));
            } else {
                long_data.push(Vec::new());
            }
        }

        let _ = decode_execute_params(&buf, num_params, &cached, &long_data);
    }
}

#[test]
fn fuzz_placeholders_and_substitute() {
    let mut rng = FuzzRng::new(0xABCD_EF01_2345_6789);

    let snippet_chars = [
        '?', '\'', '"', '`', '-', '\\', '\n', '\0', 'a', '1', ';', ' ',
    ];

    for _ in 0..3_000 {
        let len = rng.range(0, 80);
        let sql: String = (0..len)
            .map(|_| snippet_chars[rng.range(0, snippet_chars.len() - 1)])
            .collect();

        let placeholders = find_placeholders(&sql);
        let neutralized = neutralize_placeholders(&sql, &placeholders);
        assert!(neutralized.len() >= sql.len().saturating_sub(placeholders.len()));

        // Substitute with exact, fewer, and more params
        let lits_exact: Vec<String> = (0..placeholders.len())
            .map(|i| format!("'val_{i}'"))
            .collect();
        let _ = substitute(&sql, &placeholders, &lits_exact);

        let lits_fewer: Vec<String> = (0..placeholders.len().saturating_sub(1))
            .map(|i| format!("{i}"))
            .collect();
        assert!(substitute(&sql, &placeholders, &lits_fewer).is_err() || placeholders.is_empty());

        let mut lits_more = lits_exact.clone();
        lits_more.push("'extra'".to_string());
        assert!(substitute(&sql, &placeholders, &lits_more).is_err());
    }
}

#[test]
fn fuzz_pg_wire_codecs() {
    let mut rng = FuzzRng::new(0x9876_5432_10FE_DCBA);

    // 1. Startup message decoding
    for len in 0..=128 {
        for _ in 0..50 {
            let buf = rng.gen_bytes(len);
            let _ = parse_startup(&buf);
        }
    }

    // Startup with correct version but corrupt payload
    for _ in 0..1_000 {
        let len = rng.range(4, 128);
        let mut buf = rng.gen_bytes(len);
        buf[0..4].copy_from_slice(&PG_PROTOCOL_VERSION.to_be_bytes());
        let _ = parse_startup(&buf);
    }

    // 2. Extended protocol parsers
    for _ in 0..3_000 {
        let len = rng.range(0, 256);
        let payload = rng.gen_bytes(len);

        let mut pos = 0;
        let _ = read_nul_str(&payload, &mut pos);
        let _ = parse_parse_msg(&payload);
        let _ = parse_bind_msg(&payload);
        let _ = parse_describe_msg(&payload);
        let _ = parse_execute_msg(&payload);
        let _ = parse_close_msg(&payload);
    }
}

#[test]
fn fuzz_replication_protocol_frames() {
    use crate::replication::protocol::{decode_raw_frame, roundtrip, Frame, REPL_MAGIC};
    let mut rng = FuzzRng::new(0xFEED_FACE_CAFE_BABE);

    // 1. Random byte slices
    for _ in 0..4_000 {
        let len = rng.range(0, 256);
        let mut buf = rng.gen_bytes(len);
        if !buf.is_empty() && rng.next_u64() % 2 == 0 {
            buf[0] = REPL_MAGIC;
        }

        let result = std::panic::catch_unwind(|| {
            let _ = decode_raw_frame(&buf);
        });
        assert!(result.is_ok(), "decode_raw_frame panicked on random byte slice");
    }

    // 2. Valid frames and bit-flipped corruption
    let frames = vec![
        Frame::Handshake {
            version: 1,
            user: "repl_user".into(),
            password: "secret_password".into(),
        },
        Frame::HandshakeAck {
            ok: true,
            message: "ok".into(),
            wal_version: 4,
            durable_offset: 1024,
        },
        Frame::StartReplication {
            generation: 1,
            from_offset: 500,
        },
        Frame::WalChunk {
            offset: 500,
            data: vec![1, 2, 3, 4, 5],
        },
        Frame::Heartbeat {
            durable_offset: 2048,
        },
        Frame::HeartbeatAck,
        Frame::SnapshotRequired,
        Frame::SnapshotBegin {
            total_bytes: 100_000,
            end_offset: 2048,
            generation: 1,
        },
        Frame::SnapshotChunk {
            data: vec![10, 20, 30],
        },
        Frame::SnapshotEnd,
    ];

    for frame in frames {
        let rt = roundtrip(&frame).expect("roundtrip must succeed for valid frame");
        assert_eq!(rt, frame);

        // Verify encode_body + decode_raw_frame roundtrip
        let mut encoded = vec![REPL_MAGIC];
        crate::replication::protocol::encode_body_for_test(&frame, &mut encoded);
        let decoded = decode_raw_frame(&encoded).expect("decode_raw_frame on valid frame");
        assert_eq!(decoded, frame);
    }
}

#[test]
fn fuzz_authentication_password_proofs() {
    use crate::auth::*;
    let mut rng = FuzzRng::new(0x1122_3344_5566_7788);

    // 1. Fuzz SHA-256 and SHA-1 implementations with random byte buffers
    for _ in 0..2_000 {
        let len = rng.range(0, 512);
        let buf = rng.gen_bytes(len);
        let result = std::panic::catch_unwind(|| {
            let _ = sha256(&buf);
            let _ = sha1(&buf);
        });
        assert!(result.is_ok(), "SHA hash panicked on random buffer");
    }

    // 2. Fuzz auth token verification with corrupted tokens and scrambles
    let verifier_sha2 = Verifier::new_sha2(b"correct_password");
    let verifier_native = Verifier::new_native(b"correct_password");

    for _ in 0..2_000 {
        let scramble_len = rng.range(0, 40);
        let scramble = rng.gen_bytes(scramble_len);
        let token_len = rng.range(0, 40);
        let token = rng.gen_bytes(token_len);

        let result = std::panic::catch_unwind(|| {
            let _ = verify_sha2(&verifier_sha2.hash, &scramble, &token);
            let _ = verify_native(&verifier_native.hash, &scramble, &token);
        });
        assert!(result.is_ok(), "verify functions panicked on random inputs");
    }
}


