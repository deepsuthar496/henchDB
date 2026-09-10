//! Point-in-time recovery (PITR): base backup + WAL archive roll-forward.
//!
//! Recovery reuses crash-recovery semantics exactly: instead of applying
//! records directly, the selected transaction byte ranges are copied
//! verbatim into a fresh `wal.log` and [`Database::open`] redoes them, so
//! replayed state cannot diverge from normal recovery.
//!
//! Target semantics:
//! - `--target-time T`: replays commits with timestamp `<= T` (unix
//!   seconds) and stops at the first commit exceeding `T`. Commits without
//!   a timestamp (pre-v4 logs, hand-built batches) carry no time to compare
//!   and are always replayed.
//! - `--target-txn K`: replays every transaction strictly before the first
//!   commit with id `K` and halts exactly before it (exclusive, like the
//!   name says). Transaction ids restart at 1 after a server restart
//!   (`next_txn` is in-memory), so cross-restart targets stop at the first
//!   matching id in stream order.
//! - Uncommitted records buffered at the halt point are discarded (instant
//!   abort, same as crash recovery).
//! - After replay the directory is sealed with a fresh WAL whose generation
//!   sidecar is one past the highest archived generation, so a resumed
//!   server (or its replicas) can never alias old offsets.

use std::collections::HashMap;
use std::io::{BufReader, Write};
use std::path::Path;

use crate::archive::{list_segments, read_segment_payload, split_frames, SegmentRef};
use crate::db::Database;
use crate::error::{Error, Result};
use crate::wal::{Record, Wal, WAL_FORMAT_VERSION};

/// Roll-forward bound: stop replaying at a commit timestamp or id.
#[derive(Debug, Clone, Default)]
pub struct PitrTarget {
    /// Inclusive upper bound (unix seconds); `None` = no time bound.
    pub time: Option<u64>,
    /// Exclusive transaction id; `None` = no id bound.
    pub txn: Option<u64>,
}

/// Summary of a PITR run.
#[derive(Debug, Clone)]
pub struct PitrStats {
    pub segments_scanned: usize,
    pub segments_replayed: usize,
    pub txns_replayed: u64,
    pub bytes_replayed: u64,
    /// Commit timestamp that exceeded the time target (if any).
    pub stopped_at_time: Option<u64>,
    /// Transaction id that matched the txn target (if any).
    pub stopped_at_txn: Option<u64>,
}

/// Parse `--target-time`: `YYYY-MM-DD HH:MM:SS` or `YYYY-MM-DD` (midnight),
/// interpreted as UTC. std-only civil-to-epoch conversion.
pub fn parse_target_time(s: &str) -> Result<u64> {
    let s = s.trim();
    let bad = || Error::InvalidQuery(format!("bad --target-time '{s}' (want YYYY-MM-DD [HH:MM:SS])"));
    let (date, clock) = match s.split_once(' ') {
        Some((d, c)) => (d, Some(c)),
        None => (s, None),
    };
    let dparts: Vec<&str> = date.split('-').collect();
    if dparts.len() != 3 {
        return Err(bad());
    }
    let y: i64 = dparts[0].parse().map_err(|_| bad())?;
    let m: u32 = dparts[1].parse().map_err(|_| bad())?;
    let d: u32 = dparts[2].parse().map_err(|_| bad())?;
    if !(1970..=2100).contains(&y) || !(1..=12).contains(&m) || d < 1 {
        return Err(bad());
    }
    let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    let dim = match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if leap {
                29
            } else {
                28
            }
        }
    };
    if d > dim {
        return Err(bad());
    }
    let (hh, mm, ss) = match clock {
        None => (0, 0, 0),
        Some(c) => {
            let p: Vec<&str> = c.split(':').collect();
            if p.len() != 3 {
                return Err(bad());
            }
            let hh: u32 = p[0].parse().map_err(|_| bad())?;
            let mm: u32 = p[1].parse().map_err(|_| bad())?;
            let ss: u32 = p[2].parse().map_err(|_| bad())?;
            if hh > 23 || mm > 59 || ss > 59 {
                return Err(bad());
            }
            (hh, mm, ss)
        }
    };
    Ok(days_from_civil(y, m, d) as u64 * 86_400 + hh as u64 * 3600 + mm as u64 * 60 + ss as u64)
}

/// Days since 1970-01-01 (Hinnant's civil algorithm; valid 1970-2100).
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m as i64 + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Restore `backup` into `target_dir`, then roll archive segments forward
/// up to `target`. The target directory must be empty or absent (same
/// discipline as plain `restore`; callers wipe it with `--force` first).
pub fn restore_pitr(
    backup: &Path,
    target_dir: &Path,
    archive_dir: &Path,
    target: &PitrTarget,
) -> Result<PitrStats> {
    // 1. Base backup: validated + materialized + verified by open().
    {
        let f = std::fs::File::open(backup)
            .map_err(|e| Error::Io(format!("pitr: cannot open backup: {e}")))?;
        let mut r = BufReader::with_capacity(128 * 1024, f);
        Database::restore(&mut r, target_dir)?;
    }
    // 2. Verified, continuity-checked segment list (replay order).
    if !archive_dir.exists() {
        return Err(Error::Io(format!(
            "pitr: archive dir '{}' does not exist",
            archive_dir.display()
        )));
    }
    let segs: Vec<SegmentRef> = list_segments(archive_dir)?;
    for s in &segs {
        if s.meta.wal_version < 3 || s.meta.wal_version > WAL_FORMAT_VERSION {
            return Err(Error::Corrupted(format!(
                "pitr: unsupported WAL version {} in {}",
                s.meta.wal_version,
                s.path.display()
            )));
        }
    }
    // 3. Select whole transactions up to the target; keep verbatim bytes.
    let mut included: Vec<u8> = Vec::new();
    let mut pending: HashMap<u64, Vec<Vec<u8>>> = HashMap::new();
    let mut stats = PitrStats {
        segments_scanned: segs.len(),
        segments_replayed: 0,
        txns_replayed: 0,
        bytes_replayed: 0,
        stopped_at_time: None,
        stopped_at_txn: None,
    };
    let mut halted = false;
    let mut max_gen = 0u64;
    for seg in &segs {
        if halted {
            break;
        }
        max_gen = max_gen.max(seg.meta.generation);
        let payload = read_segment_payload(&seg.path, &seg.meta)?;
        let frames = split_frames(&payload)?;
        let mut seg_complete = true;
        for frame in frames {
            let (recs, _) = Wal::decode_wal_range(frame, false)?;
            let [rec] = recs.as_slice() else {
                return Err(Error::Corrupted("pitr: frame holds != 1 record".into()));
            };
            match rec {
                Record::Commit { txn, ts } => {
                    if target.txn == Some(*txn) {
                        pending.remove(txn);
                        stats.stopped_at_txn = Some(*txn);
                        halted = true;
                        seg_complete = false;
                        break;
                    }
                    if let (Some(limit), Some(ts)) = (target.time, ts) {
                        if *ts > limit {
                            pending.remove(txn);
                            stats.stopped_at_time = Some(*ts);
                            halted = true;
                            seg_complete = false;
                            break;
                        }
                    }
                    if let Some(buf) = pending.remove(txn) {
                        for raw in buf {
                            stats.bytes_replayed += raw.len() as u64;
                            included.extend_from_slice(&raw);
                        }
                    }
                    stats.bytes_replayed += frame.len() as u64;
                    included.extend_from_slice(frame);
                    stats.txns_replayed += 1;
                    crate::failpoint!("during_restore_replay");
                }
                other => {
                    pending.entry(txn_of(other)).or_default().push(frame.to_vec());
                }
            }
        }
        if seg_complete {
            stats.segments_replayed += 1;
        }
    }
    // Uncommitted tails at the halt point are discarded (instant abort).
    // 4. Seal a fresh live WAL with the selected bytes and a next-generation
    // sidecar, then let `open()` redo them with crash-recovery semantics.
    {
        let log_path = target_dir.join("wal.log");
        let mut f = std::fs::File::create(&log_path)?;
        f.write_all(crate::wal::WAL_MAGIC)?;
        f.write_all(&WAL_FORMAT_VERSION.to_le_bytes())?;
        f.write_all(&included)?;
        f.sync_data()?;
    }
    if !segs.is_empty() {
        crate::wal::write_generation(&target_dir.join("wal.log"), max_gen + 1)?;
    }
    drop(Database::open(target_dir)?);
    Ok(stats)
}

fn txn_of(rec: &Record) -> u64 {
    match rec {
        Record::Put { txn, .. }
        | Record::Delete { txn, .. }
        | Record::CreateTable { txn, .. }
        | Record::DropTable { txn, .. }
        | Record::CreateIndex { txn, .. }
        | Record::DropIndex { txn, .. }
        | Record::CreateDatabase { txn, .. }
        | Record::DropDatabase { txn, .. }
        | Record::Commit { txn, .. } => *txn,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::unix_now;

    #[test]
    fn target_time_parses() {
        assert_eq!(parse_target_time("1970-01-01 00:00:00").unwrap(), 0);
        assert_eq!(parse_target_time("1970-01-02").unwrap(), 86_400);
        assert_eq!(
            parse_target_time("2026-09-07 12:00:00").unwrap(),
            parse_target_time("2026-09-07").unwrap() + 43_200
        );
        assert!(parse_target_time("2026-13-01").is_err());
        assert!(parse_target_time("2026-02-30").is_err());
        assert!(parse_target_time("2025-02-29").is_err()); // not a leap year
        assert!(parse_target_time("2024-02-29").is_ok()); // leap year
        assert!(parse_target_time("not a date").is_err());
        assert!(parse_target_time("2026-09-07 25:00:00").is_err());
        assert!(parse_target_time("1969-12-31").is_err());
    }

    fn fresh_dirs(tag: &str) -> (std::path::PathBuf, std::path::PathBuf, std::path::PathBuf, std::path::PathBuf, std::path::PathBuf) {
        let base = std::env::temp_dir().join(format!("hdbpitr_{tag}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        let src = base.join("src");
        let adir = base.join("arch");
        let dest = base.join("dest");
        let backup = base.join("base.hdbb");
        (base, src, adir, dest, backup)
    }

    fn count(db: &Database, table: &str) -> i64 {
        let mut s = db.new_session();
        let out = db.execute(&mut s, &format!("SELECT COUNT(*) FROM {table}")).unwrap();
        match out.rows[0][0] {
            crate::types::Datum::Int(n) => n,
            _ => panic!("count not int"),
        }
    }

    fn table_exists(db: &Database, table: &str) -> bool {
        let mut s = db.new_session();
        db.execute(&mut s, &format!("SELECT COUNT(*) FROM {table}")).is_ok()
    }

    #[test]
    fn pitr_to_timestamp_skips_later_drop() {
        let (base, src, adir, dest, backup) = fresh_dirs("ts");
        let db = Database::open(&src).unwrap();
        db.set_archive_dir(&adir).unwrap();
        let mut s = db.new_session();
        db.execute(&mut s, "CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
        // 1000 rows, 100 per statement (10 txns).
        for batch in 0..10 {
            let mut vals = Vec::new();
            for i in 0..100 {
                let id = batch * 100 + i;
                vals.push(format!("({id}, {id})"));
            }
            db.execute(&mut s, &format!("INSERT INTO t VALUES {}", vals.join(","))).unwrap();
        }
        // Base backup (archives segment 1: create + 1000 rows).
        {
            let f = std::fs::File::create(&backup).unwrap();
            let mut w = std::io::BufWriter::with_capacity(128 * 1024, f);
            db.dump(&mut w).unwrap();
            w.flush().unwrap();
        }
        // 500 more rows, then checkpoint (segment 2).
        for batch in 0..5 {
            let mut vals = Vec::new();
            for i in 0..100 {
                let id = 1000 + batch * 100 + i;
                vals.push(format!("({id}, {id})"));
            }
            db.execute(&mut s, &format!("INSERT INTO t VALUES {}", vals.join(","))).unwrap();
        }
        db.checkpoint().unwrap();
        // 500 more rows at T1, then checkpoint (segment 3).
        for batch in 0..5 {
            let mut vals = Vec::new();
            for i in 0..100 {
                let id = 1500 + batch * 100 + i;
                vals.push(format!("({id}, {id})"));
            }
            db.execute(&mut s, &format!("INSERT INTO t VALUES {}", vals.join(","))).unwrap();
        }
        let t1 = unix_now();
        db.checkpoint().unwrap();
        // Wait for the clock to advance, then the disaster (segment 4).
        while unix_now() <= t1 {
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        db.execute(&mut s, "DROP TABLE t").unwrap();
        db.checkpoint().unwrap();
        drop(db);

        // Restore to T1: all 2000 rows back, the drop omitted.
        let stats = restore_pitr(
            &backup,
            &dest,
            &adir,
            &PitrTarget { time: Some(t1), txn: None },
        )
        .unwrap();
        assert_eq!(stats.segments_scanned, 4);
        assert!(table_exists(&Database::open(&dest).unwrap(), "t"));
        let db2 = Database::open(&dest).unwrap();
        assert_eq!(count(&db2, "t"), 2000);
        drop(db2);

        // Full restore (no target): the archived drop replays, table gone.
        let dest2 = dest.with_extension("full");
        let stats = restore_pitr(&backup, &dest2, &adir, &PitrTarget::default()).unwrap();
        assert_eq!(stats.txns_replayed > 0, true);
        assert!(stats.stopped_at_time.is_none() && stats.stopped_at_txn.is_none());
        let db3 = Database::open(&dest2).unwrap();
        assert!(!table_exists(&db3, "t"));
        drop(db3);
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn pitr_to_txn_halts_before_target() {
        let (base, src, adir, dest, backup) = fresh_dirs("txn");
        let db = Database::open(&src).unwrap();
        db.set_archive_dir(&adir).unwrap();
        let mut s = db.new_session();
        // Fresh dir: txn ids are 1 (create), 2/3/4 (inserts).
        db.execute(&mut s, "CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
        {
            let f = std::fs::File::create(&backup).unwrap();
            let mut w = std::io::BufWriter::with_capacity(128 * 1024, f);
            db.dump(&mut w).unwrap();
            w.flush().unwrap();
        }
        db.execute(&mut s, "INSERT INTO t VALUES (10)").unwrap();
        db.execute(&mut s, "INSERT INTO t VALUES (20)").unwrap();
        db.execute(&mut s, "INSERT INTO t VALUES (30)").unwrap();
        db.checkpoint().unwrap();
        drop(db);

        // Halt exactly before txn 4: rows from txns 2 and 3 only.
        let stats = restore_pitr(
            &backup,
            &dest,
            &adir,
            &PitrTarget { time: None, txn: Some(4) },
        )
        .unwrap();
        assert_eq!(stats.stopped_at_txn, Some(4));
        let db2 = Database::open(&dest).unwrap();
        assert_eq!(count(&db2, "t"), 2);
        let mut s2 = db2.new_session();
        let out = db2.execute(&mut s2, "SELECT id FROM t").unwrap();
        let mut ids: Vec<i64> = out
            .rows
            .iter()
            .map(|r| match r[0] {
                crate::types::Datum::Int(n) => n,
                _ => panic!(),
            })
            .collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![10, 20]);
        drop(db2);
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn pitr_empty_archive_is_plain_restore() {
        let (base, src, adir, dest, backup) = fresh_dirs("empty");
        let db = Database::open(&src).unwrap();
        let mut s = db.new_session();
        db.execute(&mut s, "CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
        db.execute(&mut s, "INSERT INTO t VALUES (1)").unwrap();
        {
            let f = std::fs::File::create(&backup).unwrap();
            let mut w = std::io::BufWriter::with_capacity(128 * 1024, f);
            db.dump(&mut w).unwrap();
            w.flush().unwrap();
        }
        drop(db);
        std::fs::create_dir_all(&adir).unwrap(); // exists, but no segments
        let stats = restore_pitr(&backup, &dest, &adir, &PitrTarget::default()).unwrap();
        assert_eq!((stats.segments_scanned, stats.txns_replayed), (0, 0));
        let db2 = Database::open(&dest).unwrap();
        assert_eq!(count(&db2, "t"), 1);
        drop(db2);
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn pitr_faults_corrupt_archive_and_gaps() {
        let (base, src, adir, dest, backup) = fresh_dirs("faults");
        let db = Database::open(&src).unwrap();
        db.set_archive_dir(&adir).unwrap();
        let mut s = db.new_session();
        db.execute(&mut s, "CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
        {
            let f = std::fs::File::create(&backup).unwrap();
            let mut w = std::io::BufWriter::with_capacity(128 * 1024, f);
            db.dump(&mut w).unwrap();
            w.flush().unwrap();
        }
        db.execute(&mut s, "INSERT INTO t VALUES (10)").unwrap();
        db.checkpoint().unwrap();
        db.execute(&mut s, "INSERT INTO t VALUES (20)").unwrap();
        db.checkpoint().unwrap();
        drop(db);

        // 1. Missing archive directory fails with Error::Io
        let missing_adir = base.join("nonexistent_archive");
        let res_missing = restore_pitr(&backup, &dest, &missing_adir, &PitrTarget::default());
        assert!(matches!(res_missing, Err(Error::Io(_))));

        // 2. Corrupt segment CRC in archive directory fails closed with Error::Corrupted
        let segments = crate::archive::list_segments(&adir).unwrap();
        assert!(segments.len() >= 2);
        let first_seg = &segments[0].path;
        let mut bytes = std::fs::read(first_seg).unwrap();
        bytes[10] ^= 0xFF; // Corrupt header
        std::fs::write(first_seg, bytes).unwrap();

        let res_corrupt = restore_pitr(&backup, &dest, &adir, &PitrTarget::default());
        assert!(matches!(res_corrupt, Err(Error::Corrupted(_))));

        // 3. Corrupt base backup fails closed with Error::Corrupted
        let mut backup_bytes = std::fs::read(&backup).unwrap();
        backup_bytes[2] ^= 0xFF; // Corrupt magic
        std::fs::write(&backup, backup_bytes).unwrap();
        let dest_corrupt = base.join("dest_corrupt");
        let res_bad_backup = restore_pitr(&backup, &dest_corrupt, &adir, &PitrTarget::default());
        assert!(matches!(res_bad_backup, Err(Error::Corrupted(_))));

        let _ = std::fs::remove_dir_all(&base);
    }
}

