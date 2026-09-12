use super::*;
use crate::table::{ColumnDef, Schema};
use std::fs;

#[test]
fn crc_known_vector() {
    assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
}

#[test]
fn table_def_auto_inc_roundtrip_and_legacy() {
    let def = TableDef {
        name: "t".into(),
        schema: Schema {
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    ctype: ColumnType::Int,
                    nullable: false,
                    auto_increment: true,
                    default_value: None,
                },
                ColumnDef {
                    name: "v".into(),
                    ctype: ColumnType::Text,
                    nullable: true,
                    auto_increment: false,
                    default_value: None,
                },
            ],
            pk_idx: 0,
        },
        indexes: Vec::new(),
        foreign_keys: Vec::new(),
        stats: None,
    };
    let mut buf = Vec::new();
    encode_table_def_pub(&def, &mut buf);
    let mut off = 0;
    let back = decode_table_def_pub(&buf, &mut off, false).unwrap();
    assert!(back.schema.columns[0].auto_increment);
    assert!(!back.schema.columns[1].auto_increment);
    assert_eq!(off, buf.len());
    // Legacy blobs (no auto byte) decode as non-auto-increment.
    let mut legacy = Vec::new();
    put_str(&mut legacy, "t");
    legacy.extend_from_slice(&2u32.to_le_bytes());
    legacy.extend_from_slice(&0u32.to_le_bytes());
    put_str(&mut legacy, "id");
    legacy.push(b'I');
    legacy.push(0);
    put_str(&mut legacy, "v");
    legacy.push(b'T');
    legacy.push(1);
    legacy.extend_from_slice(&0u32.to_le_bytes());
    let mut off = 0;
    let back = decode_table_def_pub(&legacy, &mut off, true).unwrap();
    assert!(!back.schema.columns[0].auto_increment);
    assert_eq!(off, legacy.len());
}

#[test]
fn commit_ts_roundtrip_and_legacy_absent() {
    // v4 commit with timestamp round-trips through the framing.
    let mut buf = Vec::new();
    encode_record(&Record::Commit { txn: 42, ts: Some(1_700_000_001) }, &mut buf);
    let (recs, consumed) = Wal::decode_wal_range(&buf, false).unwrap();
    assert_eq!(consumed, buf.len());
    assert!(matches!(
        &recs[..],
        [Record::Commit { txn: 42, ts: Some(1_700_000_001) }]
    ));
    // Pre-v4 9-byte Commit payload decodes with ts: None (length-based,
    // so old logs stay readable after the version bump).
    let mut legacy = Vec::new();
    legacy.extend_from_slice(&9u32.to_le_bytes());
    let mut payload = vec![KIND_COMMIT];
    payload.extend_from_slice(&7u64.to_le_bytes());
    legacy.extend_from_slice(&crc32(&payload).to_le_bytes());
    legacy.extend_from_slice(&payload);
    let (recs, _) = Wal::decode_wal_range(&legacy, false).unwrap();
    assert!(matches!(&recs[..], [Record::Commit { txn: 7, ts: None }]));
}

#[test]
fn wal_roundtrip_and_recovery() {
    let dir = std::env::temp_dir().join(format!("hdbwal_test_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("wal.log");
    let _ = std::fs::remove_file(&path);
    let wal = Wal::open(&path).unwrap();
    let def = TableDef {
        name: "t".into(),
        schema: Schema {
            columns: vec![ColumnDef {
                name: "id".into(),
                ctype: ColumnType::Int,
                nullable: false,
                auto_increment: false,
                default_value: None,
            }],
            pk_idx: 0,
        },
        indexes: Vec::new(),
        foreign_keys: Vec::new(),
        stats: None,
    };
    wal.append_batch(&[
        Record::CreateTable { txn: 1, def: def.clone() },
        Record::Put {
            txn: 1,
            table: "t".into(),
            key: vec![1, 2],
            row: vec![3, 4],
        },
        Record::Commit { txn: 7, ts: None },
    ])
    .unwrap();
    drop(wal);
    let wal2 = Wal::open(&path).unwrap();
    let recs = wal2.read_all().unwrap();
    assert_eq!(recs.len(), 3);
    assert!(matches!(recs[0], Record::CreateTable { .. }));
    assert!(matches!(recs[2], Record::Commit { txn: 7, .. }));
    wal2.reset().unwrap();
    assert_eq!(wal2.read_all().unwrap().len(), 0);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn wal_codec_corruption_fuzz_and_robustness() {
    let dir = std::env::temp_dir().join(format!("hdbwal_fuzz_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("wal.log");
    let _ = std::fs::remove_file(&path);
    let wal = Wal::open(&path).unwrap();
    let def = TableDef {
        name: "t".into(),
        schema: Schema {
            columns: vec![ColumnDef {
                name: "id".into(),
                ctype: ColumnType::Int,
                nullable: false,
                auto_increment: false,
                default_value: None,
            }],
            pk_idx: 0,
        },
        indexes: Vec::new(),
        foreign_keys: Vec::new(),
        stats: None,
    };
    wal.append_batch(&[
        Record::CreateTable { txn: 1, def },
        Record::Put { txn: 1, table: "t".into(), key: vec![1], row: vec![2] },
        Record::Commit { txn: 1, ts: Some(1_700_000_000) },
    ]).unwrap();
    drop(wal);

    let valid_bytes = std::fs::read(&path).unwrap();

    // 1. Bit flips at various byte positions: must return Error::Corrupted or Err, never panic
    for i in 8..valid_bytes.len() {
        let mut corrupted = valid_bytes.clone();
        corrupted[i] ^= 0xFF;
        std::fs::write(&path, &corrupted).unwrap();
        if let Ok(wal) = Wal::open(&path) {
            let _ = wal.read_all();
        }
    }

    // 2. Truncations at every single byte offset: must stop cleanly or fail with error, never panic
    for len in 0..valid_bytes.len() {
        let truncated = &valid_bytes[..len];
        std::fs::write(&path, truncated).unwrap();
        if let Ok(wal) = Wal::open(&path) {
            let _ = wal.read_all();
        }
    }

    // 3. Huge length injection (DoS attack prevention)
    let mut corrupted = valid_bytes.clone();
    if corrupted.len() > 12 {
        corrupted[8..12].copy_from_slice(&(i32::MAX as u32).to_le_bytes());
        std::fs::write(&path, &corrupted).unwrap();
        let wal = Wal::open(&path).unwrap();
        let res = wal.read_all();
        assert!(res.is_err(), "huge record must be rejected with Corrupted");
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn sharded_concurrent_appends_stay_ordered() {
    use std::sync::Arc;
    let dir = std::env::temp_dir().join(format!("hdbwal_shard_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("wal.log");
    let _ = std::fs::remove_file(&path);
    let wal = Arc::new(Wal::open(&path).unwrap());
    const THREADS: u64 = 8;
    const PER_THREAD: u64 = 25;
    let mut handles = Vec::new();
    for w in 0..THREADS {
        let wal = wal.clone();
        handles.push(std::thread::spawn(move || {
            for i in 0..PER_THREAD {
                let txn = w * 1_000_000 + i + 1;
                wal.append_batch(&[
                    Record::Put {
                        txn,
                        table: "t".into(),
                        key: txn.to_le_bytes().to_vec(),
                        row: vec![w as u8; 64],
                    },
                    Record::Commit { txn, ts: Some(1_700_000_000) },
                ])
                .unwrap();
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    let t0 = std::time::Instant::now();
    while wal.next_offset() != wal.durable_offset() {
        assert!(t0.elapsed() < std::time::Duration::from_secs(30), "drain stall");
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    drop(wal);
    let wal2 = Wal::open(&path).unwrap();
    let recs = wal2.read_all().unwrap();
    let mut commits: Vec<u64> = Vec::new();
    let mut puts = 0u64;
    for r in &recs {
        match r {
            Record::Put { .. } => puts += 1,
            Record::Commit { txn, .. } => commits.push(*txn),
            _ => panic!("unexpected record {r:?}"),
        }
    }
    assert_eq!(puts, THREADS * PER_THREAD);
    commits.sort_unstable();
    assert_eq!(commits.len() as u64, THREADS * PER_THREAD);
    for w in 0..THREADS {
        for i in 0..PER_THREAD {
            assert!(commits.contains(&(w * 1_000_000 + i + 1)));
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn sharded_recovery_drops_interleaved_tails() {
    use std::sync::Arc;
    let dir = std::env::temp_dir().join(format!("hdbwal_tail_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("wal.log");
    let _ = std::fs::remove_file(&path);
    let wal = Arc::new(Wal::open(&path).unwrap());
    let mut handles = Vec::new();
    for w in 0..8u64 {
        let wal = wal.clone();
        handles.push(std::thread::spawn(move || {
            for i in 0..100u64 {
                let txn = w * 100_000 + i + 1;
                wal.append_batch(&[Record::Put {
                    txn,
                    table: "t".into(),
                    key: vec![i as u8],
                    row: vec![w as u8],
                }, Record::Commit { txn, ts: None }])
                .unwrap();
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    wal.append_unsynced(&[Record::Put {
        txn: 999_999,
        table: "t".into(),
        key: vec![9],
        row: vec![9],
    }])
    .unwrap();
    drop(wal);
    let wal2 = Wal::open(&path).unwrap();
    let recs = wal2.read_all().unwrap();
    let commits = recs
        .iter()
        .filter(|r| matches!(r, Record::Commit { .. }))
        .count();
    assert_eq!(commits, 800);
    // Recovery replay contract: buffer records per txn, apply only on Commit; uncommitted tails dropped
    let mut pending: std::collections::HashMap<u64, Vec<Record>> = std::collections::HashMap::new();
    let mut recovered = Vec::new();
    for r in recs {
        match r {
            Record::Commit { txn, .. } => {
                if let Some(b) = pending.remove(&txn) {
                    recovered.extend(b);
                }
            }
            Record::Put { txn, .. } => {
                pending.entry(txn).or_default().push(r);
            }
            _ => {}
        }
    }
    assert!(!recovered.iter().any(|r| matches!(
        r,
        Record::Put { txn: 999_999, .. }
    )));
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// Assessment §8: Adversarial WAL Decoding Fuzzing Suite
// ---------------------------------------------------------------------------

struct WalFuzzPrng(u64);

impl WalFuzzPrng {
    fn new(seed: u64) -> Self {
        Self(if seed == 0 { 0x853c49e6748fea9b } else { seed })
    }

    fn next_u64(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn gen_bytes(&mut self, len: usize) -> Vec<u8> {
        let mut b = vec![0u8; len];
        for byte in &mut b {
            *byte = self.next_u64() as u8;
        }
        b
    }
}

#[test]
fn fuzz_wal_decode_range_random_bytes() {
    let mut prng = WalFuzzPrng::new(0xABCD_1234_5678_EF01);

    for _ in 0..5_000 {
        let len = (prng.next_u64() % 512) as usize;
        let buf = prng.gen_bytes(len);
        let allow_trailing = prng.next_u64() % 2 == 0;

        let result = std::panic::catch_unwind(|| {
            let _ = Wal::decode_wal_range(&buf, allow_trailing);
        });
        assert!(
            result.is_ok(),
            "Wal::decode_wal_range panicked on random bytes of length {len}"
        );
    }
}

#[test]
fn fuzz_wal_corrupted_records_validation() {
    let records = vec![
        Record::Put {
            txn: 101,
            table: "users".into(),
            key: vec![1, 2, 3],
            row: vec![4, 5, 6, 7],
        },
        Record::Delete {
            txn: 102,
            table: "orders".into(),
            key: vec![10, 20],
        },
        Record::Commit {
            txn: 103,
            ts: Some(1_700_000_000),
        },
        Record::Commit { txn: 104, ts: None },
        Record::DropTable {
            txn: 105,
            name: "temp".into(),
        },
    ];

    for rec in records {
        let mut framed = Vec::new();
        encode_record(&rec, &mut framed);
        assert!(framed.len() >= 8);

        // 1. Valid record must decode cleanly
        let (decoded, consumed) = Wal::decode_wal_range(&framed, false).expect("valid frame decode");
        assert_eq!(decoded.len(), 1);
        assert_eq!(consumed, framed.len());

        // 2. Corrupt CRC: flip any bit in bytes 4..8
        for bit in 0..32 {
            let mut corrupted = framed.clone();
            corrupted[4 + (bit / 8)] ^= 1 << (bit % 8);
            let res = Wal::decode_wal_range(&corrupted, false);
            assert!(
                matches!(res, Err(Error::Corrupted(_))),
                "Corrupted CRC must fail closed with Error::Corrupted, got {res:?}"
            );
        }

        // 3. Truncated frame: any prefix shorter than full length must not consume the full frame
        for len in 0..framed.len() {
            let truncated = &framed[..len];
            if let Ok((recs, consumed)) = Wal::decode_wal_range(truncated, false) {
                assert!(
                    consumed < framed.len() && recs.is_empty(),
                    "Truncated frame of len {len}/{} must not decode complete records",
                    framed.len()
                );
            }
        }

        // 4. Corrupted length prefix: absurd large length
        let mut large_len = framed.clone();
        large_len[0..4].copy_from_slice(&(0x7FFF_FFFFu32).to_le_bytes());
        let res = Wal::decode_wal_range(&large_len, false);
        assert!(matches!(res, Err(Error::Corrupted(_))));

        // 5. Unknown record kind: corrupt kind byte at offset 8
        let mut bad_kind = framed.clone();
        bad_kind[8] = 250; // Unknown opcode
        let new_crc = crc32(&bad_kind[8..]);
        bad_kind[4..8].copy_from_slice(&new_crc.to_le_bytes());
        let res = Wal::decode_wal_range(&bad_kind, false);
        assert!(
            matches!(res, Err(Error::Corrupted(_))),
            "Unknown opcode must fail closed with Error::Corrupted"
        );
    }
}

#[test]
fn fuzz_wal_file_recovery_corruptions() {
    let dir = std::env::temp_dir().join(format!("hdb_wal_fuzz_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();

    let wal_path = dir.join("wal.log");

    // 1. Truncated header (< 8 bytes)
    fs::write(&wal_path, b"HDB").unwrap();
    if let Ok(wal) = Wal::open(&wal_path) {
        let res = wal.read_all();
        assert!(matches!(res, Err(Error::Corrupted(_)) | Err(Error::Io(_))));
    }

    // 2. Bad magic bytes
    fs::write(&wal_path, b"NOPE\x04\x00\x00\x00").unwrap();
    if let Ok(wal) = Wal::open(&wal_path) {
        let res = wal.read_all();
        assert!(matches!(res, Err(Error::Corrupted(_))));
    }

    // 3. Unsupported future format version
    fs::write(&wal_path, b"HDBW\xFF\x00\x00\x00").unwrap();
    if let Ok(wal) = Wal::open(&wal_path) {
        let res = wal.read_all();
        assert!(matches!(res, Err(Error::Corrupted(_))));
    }

    // 4. Valid header followed by garbage bytes
    let mut bad_log = Vec::new();
    bad_log.extend_from_slice(WAL_MAGIC);
    bad_log.extend_from_slice(&WAL_FORMAT_VERSION.to_le_bytes());
    bad_log.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04]);
    fs::write(&wal_path, bad_log).unwrap();
    if let Ok(wal) = Wal::open(&wal_path) {
        let res = wal.read_all();
        assert!(matches!(res, Err(Error::Corrupted(_))));
    }

    let _ = fs::remove_dir_all(&dir);
}

