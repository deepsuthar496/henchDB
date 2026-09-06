//! Immutable WAL archive segments (`HDBA`) for point-in-time recovery.
//!
//! When [`Database::checkpoint`](crate::db::Database::checkpoint) truncates
//! the live log, the discarded durable prefix is first copied verbatim into
//! a numbered segment file inside the configured archive directory, so no
//! committed record is ever lost to a checkpoint. PITR (`pitr.rs`) replays
//! these segments over a base backup.
//!
//! Segment file layout (all integers little-endian, WAL-family order):
//! ```text
//! [4]  magic b"HDBA"
//! [4]  archive version (1)
//! [4]  WAL format version the payload was copied from (3 or 4)
//! [8]  log generation (matches the `wal.gen` sidecar epoch)
//! [8]  segment index (globally sequential, starts at 1)
//! [8]  start_offset (absolute WAL offset of the first payload byte)
//! [8]  end_offset (absolute WAL offset one past the last payload byte)
//! [8]  start_ts (minimum commit timestamp in the payload, 0 when none)
//! [8]  end_ts (maximum commit timestamp in the payload, 0 when none)
//! [4]  header CRC32 (over the 56 bytes from archive version to end_ts)
//! [N]  payload: raw framed WAL bytes, byte-identical to the live log
//! [4]  payload CRC32
//! ```
//! Filenames are `wal_{generation:08x}_{index:08x}.hdbw`, which sort in
//! replay order. Writes go to a `.{name}.tmp.{pid}` sibling, are fsync'd,
//! then atomically renamed; a crash between rename and WAL truncate is
//! idempotent because the next checkpoint derives the same index and reuses
//! the identical file instead of writing a duplicate.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::wal::{crc32, unix_now, Record, Wal, WAL_FORMAT_VERSION};

/// `Database` archiving surface (lives here, not in `db/mod.rs`, per the
/// 1,500-line file ceiling).
impl crate::db::Database {
    /// Enable continuous WAL archiving into `dir` (creates it when absent).
    /// The next checkpoint starts emitting `HDBA` segments there.
    pub fn set_archive_dir(&self, dir: &Path) -> Result<()> {
        std::fs::create_dir_all(dir)?;
        *self.archive_dir.lock().unwrap() = Some(dir.to_path_buf());
        Ok(())
    }

    /// Copy the about-to-be-truncated durable WAL prefix into one segment.
    /// Called by `checkpoint()` under the commit lock, before `wal.reset()`.
    pub(crate) fn archive_checkpoint_prefix(&self) -> Result<()> {
        let Some(adir) = self.archive_dir.lock().unwrap().clone() else {
            return Ok(()); // archiving disabled
        };
        archive_live_prefix(&self.wal, &adir, WAL_FORMAT_VERSION)?;
        Ok(())
    }
}

pub const ARCHIVE_MAGIC: &[u8; 4] = b"HDBA";
pub const ARCHIVE_VERSION: u32 = 1;
/// Fixed header length: magic(4) + version(4) + wal_version(4) +
/// generation(8) + index(8) + start(8) + end(8) + start_ts(8) + end_ts(8) +
/// header_crc(4).
pub const HEADER_LEN: usize = 64;

// Allocation caps (fail closed, never panic on corrupt input).
const MAX_SEGMENTS: usize = 1 << 20;
const MAX_PAYLOAD_LEN: u64 = 1 << 32;
const MAX_FILENAME_LEN: usize = 128;

/// Verified header of one archive segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentMeta {
    pub wal_version: u32,
    pub generation: u64,
    pub index: u64,
    pub start_offset: u64,
    pub end_offset: u64,
    pub start_ts: u64,
    pub end_ts: u64,
}

impl SegmentMeta {
    pub fn payload_len(&self) -> u64 {
        self.end_offset.saturating_sub(self.start_offset)
    }
}

/// A listed segment: verified header plus its file path (payloads are read
/// on demand so replay never holds the whole archive in memory).
#[derive(Debug, Clone)]
pub struct SegmentRef {
    pub meta: SegmentMeta,
    pub path: PathBuf,
}

/// Archive file name for `(generation, index)`; sorts in replay order.
pub fn segment_filename(generation: u64, index: u64) -> String {
    format!("wal_{generation:08x}_{index:08x}.hdbw")
}

/// Parse an archive file name back into `(generation, index)`.
pub fn parse_segment_filename(name: &str) -> Option<(u64, u64)> {
    if name.len() > MAX_FILENAME_LEN {
        return None;
    }
    let rest = name.strip_prefix("wal_")?.strip_suffix(".hdbw")?;
    let (gen, idx) = rest.split_once('_')?;
    if gen.len() != 8 || idx.len() != 8 {
        return None;
    }
    Some((u64::from_str_radix(gen, 16).ok()?, u64::from_str_radix(idx, 16).ok()?))
}

pub fn encode_header(meta: &SegmentMeta) -> [u8; HEADER_LEN] {
    let mut h = [0u8; HEADER_LEN];
    h[0..4].copy_from_slice(ARCHIVE_MAGIC);
    h[4..8].copy_from_slice(&ARCHIVE_VERSION.to_le_bytes());
    h[8..12].copy_from_slice(&meta.wal_version.to_le_bytes());
    h[12..20].copy_from_slice(&meta.generation.to_le_bytes());
    h[20..28].copy_from_slice(&meta.index.to_le_bytes());
    h[28..36].copy_from_slice(&meta.start_offset.to_le_bytes());
    h[36..44].copy_from_slice(&meta.end_offset.to_le_bytes());
    h[44..52].copy_from_slice(&meta.start_ts.to_le_bytes());
    h[52..60].copy_from_slice(&meta.end_ts.to_le_bytes());
    let hcrc = crc32(&h[4..60]).to_le_bytes();
    h[60..64].copy_from_slice(&hcrc);
    h
}

pub fn decode_header(bytes: &[u8]) -> Result<SegmentMeta> {
    if bytes.len() < HEADER_LEN {
        return Err(Error::Corrupted("archive: truncated header".into()));
    }
    if &bytes[0..4] != ARCHIVE_MAGIC {
        return Err(Error::Corrupted("archive: bad magic".into()));
    }
    let version = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
    if version != ARCHIVE_VERSION {
        return Err(Error::Corrupted(format!("archive version {version}")));
    }
    if crc32(&bytes[4..60]) != u32::from_le_bytes(bytes[60..64].try_into().unwrap()) {
        return Err(Error::Corrupted("archive: header CRC mismatch".into()));
    }
    let meta = SegmentMeta {
        wal_version: u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
        generation: u64::from_le_bytes(bytes[12..20].try_into().unwrap()),
        index: u64::from_le_bytes(bytes[20..28].try_into().unwrap()),
        start_offset: u64::from_le_bytes(bytes[28..36].try_into().unwrap()),
        end_offset: u64::from_le_bytes(bytes[36..44].try_into().unwrap()),
        start_ts: u64::from_le_bytes(bytes[44..52].try_into().unwrap()),
        end_ts: u64::from_le_bytes(bytes[52..60].try_into().unwrap()),
    };
    if meta.end_offset < meta.start_offset {
        return Err(Error::Corrupted("archive: inverted offsets".into()));
    }
    if meta.payload_len() > MAX_PAYLOAD_LEN {
        return Err(Error::Corrupted("archive: payload too large".into()));
    }
    if meta.index == 0 {
        return Err(Error::Corrupted("archive: index starts at 1".into()));
    }
    Ok(meta)
}

/// Split a raw WAL byte range into its framed `[len][crc][payload]` record
/// slices (bounds-checked; torn tails and absurd lengths fail closed).
/// Lets PITR slice verbatim bytes per transaction without re-encoding.
pub fn split_frames(payload: &[u8]) -> Result<Vec<&[u8]>> {
    let mut out = Vec::new();
    let mut off = 0usize;
    while off < payload.len() {
        if payload.len() - off < 8 {
            return Err(Error::Corrupted("archive: torn record header".into()));
        }
        let len =
            u32::from_le_bytes(payload[off..off + 4].try_into().unwrap()) as usize;
        if len > 1 << 30 {
            return Err(Error::Corrupted("archive: record too large".into()));
        }
        if payload.len() - off - 8 < len {
            return Err(Error::Corrupted("archive: torn record payload".into()));
        }
        // Verify the frame CRC now so corrupt segments fail before replay.
        let body = &payload[off + 8..off + 8 + len];
        let expect =
            u32::from_le_bytes(payload[off + 4..off + 8].try_into().unwrap());
        if crc32(body) != expect {
            return Err(Error::Corrupted("archive: record CRC mismatch".into()));
        }
        out.push(&payload[off..off + 8 + len]);
        off += 8 + len;
    }
    Ok(out)
}

/// Minimum/maximum commit timestamps in a payload (`0, 0` when no commit
/// carries a timestamp). Commits without timestamps are ignored for the
/// range (PITR replays them unconditionally).
pub fn commit_ts_range(payload: &[u8]) -> Result<(u64, u64)> {
    // v3 and v4 payloads share the table-def codec; only Commit framing
    // differs, and that is length-based, so one pass covers both.
    let (records, _) = Wal::decode_wal_range(payload, false)?;
    let mut min = u64::MAX;
    let mut max = 0u64;
    for rec in &records {
        if let Record::Commit { ts: Some(ts), .. } = rec {
            min = min.min(*ts);
            max = max.max(*ts);
        }
    }
    if max == 0 && min == u64::MAX {
        return Ok((0, 0));
    }
    Ok((min, max))
}

/// Write `payload` as segment `(generation, index)` into `dir`, crash-safe
/// (tmp + fsync + rename). When the final file already exists with an
/// identical header it is reused (checkpoint retry after a crash between
/// rename and WAL truncate); a same-named file with different content fails
/// closed.
pub fn write_segment(
    dir: &Path,
    meta: &SegmentMeta,
    payload: &[u8],
) -> Result<PathBuf> {
    if payload.len() as u64 != meta.payload_len() {
        return Err(Error::Corrupted("archive: payload/offset mismatch".into()));
    }
    let name = segment_filename(meta.generation, meta.index);
    let final_path = dir.join(&name);
    if final_path.exists() {
        let existing = std::fs::read(&final_path)?;
        if existing.len() >= HEADER_LEN {
            if let Ok(old) = decode_header(&existing[..HEADER_LEN]) {
                if old == *meta
                    && existing.len() as u64 == HEADER_LEN as u64 + meta.payload_len() + 4
                {
                    return Ok(final_path);
                }
            }
        }
        return Err(Error::Corrupted(format!(
            "archive: conflicting segment file {name}"
        )));
    }
    let tmp_path = dir.join(format!(".{name}.tmp.{}", std::process::id()));
    {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;
        f.write_all(&encode_header(meta))?;
        f.write_all(payload)?;
        f.write_all(&crc32(payload).to_le_bytes())?;
        f.sync_data()?;
    }
    std::fs::rename(&tmp_path, &final_path)?;
    // Directory fsync so the rename itself is durable.
    if let Ok(d) = std::fs::File::open(dir) {
        let _ = d.sync_data();
    }
    Ok(final_path)
}

/// Read and verify one segment's payload (header must match `meta`).
pub fn read_segment_payload(path: &Path, meta: &SegmentMeta) -> Result<Vec<u8>> {
    let len = std::fs::metadata(path)?.len();
    let expect = HEADER_LEN as u64 + meta.payload_len() + 4;
    if len != expect {
        return Err(Error::Corrupted(format!(
            "archive: size mismatch for {}",
            path.display()
        )));
    }
    let bytes = std::fs::read(path)?;
    let head = decode_header(&bytes)?;
    if head != *meta {
        return Err(Error::Corrupted("archive: header changed on disk".into()));
    }
    let payload = &bytes[HEADER_LEN..HEADER_LEN + meta.payload_len() as usize];
    let crc_off = HEADER_LEN + meta.payload_len() as usize;
    let expect_crc = u32::from_le_bytes(bytes[crc_off..crc_off + 4].try_into().unwrap());
    if crc32(payload) != expect_crc {
        return Err(Error::Corrupted("archive: payload CRC mismatch".into()));
    }
    Ok(payload.to_vec())
}

/// List every verified segment in `dir`, sorted by (generation, index),
/// with continuity enforced: indices must be contiguous; within a
/// generation each segment must start where the previous ended; a new
/// generation must restart at WAL offset 8 (post-truncate header).
/// A missing or corrupt segment fails closed — replaying over a gap would
/// silently lose transactions.
pub fn list_segments(dir: &Path) -> Result<Vec<SegmentRef>> {
    let mut out: Vec<SegmentRef> = Vec::new();
    let entries = match std::fs::read_dir(dir) {
        Err(_) => return Ok(Vec::new()), // absent archive dir = no segments
        Ok(e) => e,
    };
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        let Some((generation, index)) = parse_segment_filename(&name) else {
            continue; // tmp files, strays: not ours
        };
        let bytes = std::fs::read(entry.path())?;
        if bytes.len() < HEADER_LEN + 4 {
            return Err(Error::Corrupted(format!("archive: truncated {name}")));
        }
        let meta = decode_header(&bytes[..HEADER_LEN]).map_err(|e| {
            Error::Corrupted(format!("archive: {name}: {e}"))
        })?;
        if meta.generation != generation || meta.index != index {
            return Err(Error::Corrupted(format!(
                "archive: {name} header/name mismatch"
            )));
        }
        let payload_len = meta.payload_len() as usize;
        if bytes.len() != HEADER_LEN + payload_len + 4 {
            return Err(Error::Corrupted(format!("archive: {name} size mismatch")));
        }
        let payload = &bytes[HEADER_LEN..HEADER_LEN + payload_len];
        let got = u32::from_le_bytes(
            bytes[HEADER_LEN + payload_len..HEADER_LEN + payload_len + 4]
                .try_into()
                .unwrap(),
        );
        if crc32(payload) != got {
            return Err(Error::Corrupted(format!(
                "archive: {name} payload CRC mismatch"
            )));
        }
        if out.len() >= MAX_SEGMENTS {
            return Err(Error::Corrupted("archive: too many segments".into()));
        }
        out.push(SegmentRef { meta, path: entry.path() });
    }
    out.sort_by(|a, b| {
        (a.meta.generation, a.meta.index).cmp(&(b.meta.generation, b.meta.index))
    });
    for w in out.windows(2) {
        let (prev, next) = (&w[0].meta, &w[1].meta);
        if next.index != prev.index + 1 {
            return Err(Error::Corrupted(format!(
                "archive: missing segment between index {} and {}",
                prev.index, next.index
            )));
        }
        if next.generation == prev.generation {
            if next.start_offset != prev.end_offset {
                return Err(Error::Corrupted(format!(
                    "archive: offset gap in generation {}",
                    next.generation
                )));
            }
        } else {
            if next.generation <= prev.generation {
                return Err(Error::Corrupted("archive: generation went backwards".into()));
            }
            // Generations may skip (empty checkpoints write no segment),
            // but a new generation always restarts at the WAL header end.
            if next.start_offset != 8 {
                return Err(Error::Corrupted(format!(
                    "archive: generation {} must start at offset 8",
                    next.generation
                )));
            }
        }
    }
    Ok(out)
}

/// Archiver resume state for one checkpoint: the next segment index
/// (global max + 1, starting at 1) and the WAL offset to copy from (the
/// end of the newest segment of `generation`, or 8 when none exists).
pub fn scan_state(dir: &Path, generation: u64) -> Result<(u64, u64)> {
    let segs = list_segments(dir)?;
    let mut next_index = 1u64;
    let mut resume = 8u64;
    for s in &segs {
        next_index = next_index.max(s.meta.index + 1);
        if s.meta.generation == generation {
            resume = resume.max(s.meta.end_offset);
        }
    }
    Ok((next_index, resume))
}

/// Copy the durable WAL prefix `[resume, durable)` into one segment file.
/// Called by `checkpoint()` under the commit lock, before `wal.reset()`.
/// Returns the written meta, or `None` when there is nothing new to
/// archive (or archiving is disabled — the caller checks the dir).
/// `wal_version` stamps the payload's log format for the replay decoder.
pub(crate) fn archive_live_prefix(
    wal: &Wal,
    dir: &Path,
    wal_version: u32,
) -> Result<Option<SegmentMeta>> {
    std::fs::create_dir_all(dir)?;
    let generation = wal.generation();
    let (next_index, resume) = scan_state(dir, generation)?;
    let durable = wal.durable_offset();
    if resume > durable {
        return Err(Error::Corrupted("archive: resume past durable".into()));
    }
    if resume == durable {
        return Ok(None); // nothing committed since the last archive
    }
    // Copy the durable prefix in chunks (complete framed records only).
    let mut payload = Vec::with_capacity((durable - resume).min(8 << 20) as usize);
    let mut from = resume;
    while from < durable {
        let (chunk, end) = wal.read_range(from, 1 << 20)?;
        if end <= from {
            return Err(Error::Corrupted("archive: WAL shrank mid-archive".into()));
        }
        payload.extend_from_slice(&chunk);
        from = end;
    }
    let (start_ts, mut end_ts) = commit_ts_range(&payload)?;
    if end_ts == 0 {
        // No stamped commits (DDL-only or legacy tail): bound the segment
        // with the archive time so header ranges stay meaningful.
        end_ts = unix_now();
    }
    let meta = SegmentMeta {
        wal_version,
        generation,
        index: next_index,
        start_offset: resume,
        end_offset: durable,
        start_ts,
        end_ts,
    };
    write_segment(dir, &meta, &payload)?;
    Ok(Some(meta))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::{ColumnDef, Schema, TableDef};
    use crate::types::ColumnType;
    use crate::wal::WAL_FORMAT_VERSION;

    fn test_dir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!(
            "hdbarch_{tag}_{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    /// Build a framed WAL payload via a scratch log (exercises the real
    /// record codec, incl. v4 commit timestamps).
    fn sample_payload(dir: &Path, ts: Option<u64>) -> Vec<u8> {
        let log = dir.join("scratch.log");
        let _ = std::fs::remove_file(&log);
        let wal = Wal::open(&log).unwrap();
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
            Record::Commit { txn: 1, ts },
        ])
        .unwrap();
        drop(wal);
        let bytes = std::fs::read(&log).unwrap();
        bytes[8..].to_vec()
    }

    #[test]
    fn filename_roundtrip_and_rejects() {
        assert_eq!(segment_filename(0, 1), "wal_00000000_00000001.hdbw");
        assert_eq!(
            parse_segment_filename("wal_00000000_00000001.hdbw"),
            Some((0, 1))
        );
        assert_eq!(
            parse_segment_filename("wal_ffffffff_0000000a.hdbw"),
            Some((0xffff_ffff, 10))
        );
        assert_eq!(parse_segment_filename("wal_1.hdbw"), None);
        assert_eq!(parse_segment_filename("wal_00000000_00000001.tmp"), None);
        assert_eq!(parse_segment_filename("other.hdbw"), None);
    }

    #[test]
    fn header_roundtrip() {
        let meta = SegmentMeta {
            wal_version: WAL_FORMAT_VERSION,
            generation: 3,
            index: 7,
            start_offset: 8,
            end_offset: 12345,
            start_ts: 100,
            end_ts: 200,
        };
        let back = decode_header(&encode_header(&meta)).unwrap();
        assert_eq!(back, meta);
    }

    #[test]
    fn header_rejects_corruption() {
        let meta = SegmentMeta {
            wal_version: WAL_FORMAT_VERSION,
            generation: 0,
            index: 1,
            start_offset: 8,
            end_offset: 100,
            start_ts: 0,
            end_ts: 0,
        };
        let good = encode_header(&meta);
        // Every single-byte flip in the header must fail closed.
        for i in 0..HEADER_LEN {
            let mut bad = good;
            bad[i] ^= 0xFF;
            assert!(decode_header(&bad).is_err(), "flip at {i} accepted");
        }
        // Truncations fail closed.
        for len in [0, 4, 63] {
            assert!(decode_header(&good[..len]).is_err());
        }
        // Bad magic / version rejected.
        let mut bad = good;
        bad[0..4].copy_from_slice(b"XXXX");
        assert!(decode_header(&bad).is_err());
        let mut bad = good;
        bad[4..8].copy_from_slice(&99u32.to_le_bytes());
        assert!(decode_header(&bad).is_err());
    }

    #[test]
    fn write_list_read_roundtrip_with_chain() {
        let dir = test_dir("chain");
        let payload = sample_payload(&dir, Some(500));
        let m1 = SegmentMeta {
            wal_version: WAL_FORMAT_VERSION,
            generation: 0,
            index: 1,
            start_offset: 8,
            end_offset: 8 + payload.len() as u64,
            start_ts: 500,
            end_ts: 500,
        };
        write_segment(&dir, &m1, &payload).unwrap();
        // Idempotent rewrite (crash-retry path) reuses the file.
        write_segment(&dir, &m1, &payload).unwrap();
        // Conflicting same-named content fails closed.
        let mut m1b = m1.clone();
        m1b.end_ts = 501;
        assert!(write_segment(&dir, &m1b, &payload).is_err());
        // Second generation restarts at offset 8.
        let m2 = SegmentMeta {
            wal_version: WAL_FORMAT_VERSION,
            generation: 1,
            index: 2,
            start_offset: 8,
            end_offset: 8 + payload.len() as u64,
            start_ts: 600,
            end_ts: 600,
        };
        write_segment(&dir, &m2, &payload).unwrap();
        let segs = list_segments(&dir).unwrap();
        assert_eq!(segs.len(), 2);
        assert_eq!(segs[0].meta, m1);
        assert_eq!(segs[1].meta, m2);
        let back = read_segment_payload(&segs[0].path, &segs[0].meta).unwrap();
        assert_eq!(back, payload);
        let (next, resume) = scan_state(&dir, 1).unwrap();
        assert_eq!((next, resume), (3, 8 + payload.len() as u64));
        let (next, resume) = scan_state(&dir, 99).unwrap();
        assert_eq!((next, resume), (3, 8));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn list_rejects_gaps_and_corruption() {
        let dir = test_dir("gaps");
        let payload = sample_payload(&dir, Some(500));
        let seg = |index: u64, start: u64| SegmentMeta {
            wal_version: WAL_FORMAT_VERSION,
            generation: 0,
            index,
            start_offset: start,
            end_offset: start + payload.len() as u64,
            start_ts: 500,
            end_ts: 500,
        };
        // Index gap (1, 3): fails closed.
        write_segment(&dir, &seg(1, 8), &payload).unwrap();
        write_segment(&dir, &seg(3, 8 + payload.len() as u64), &payload).unwrap();
        assert!(list_segments(&dir).is_err());
        std::fs::remove_file(&dir.join(segment_filename(0, 3))).unwrap();
        // Offset gap within a generation: fails closed.
        write_segment(&dir, &seg(2, 9999), &payload).unwrap();
        assert!(list_segments(&dir).is_err());
        std::fs::remove_file(&dir.join(segment_filename(0, 2))).unwrap();
        // Flipped payload byte: fails closed.
        write_segment(&dir, &seg(2, 8 + payload.len() as u64), &payload).unwrap();
        let p2 = dir.join(segment_filename(0, 2));
        let mut bytes = std::fs::read(&p2).unwrap();
        bytes[HEADER_LEN] ^= 0xFF;
        std::fs::write(&p2, &bytes).unwrap();
        assert!(list_segments(&dir).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn ts_range_and_frames() {
        let dir = test_dir("ts");
        let payload = sample_payload(&dir, Some(777));
        assert_eq!(commit_ts_range(&payload).unwrap(), (777, 777));
        let plain = sample_payload(&dir, None);
        assert_eq!(commit_ts_range(&plain).unwrap(), (0, 0));
        // Frames cover the payload exactly and verify CRCs.
        let frames = split_frames(&payload).unwrap();
        assert_eq!(frames.len(), 3);
        let total: usize = frames.iter().map(|f| f.len()).sum();
        assert_eq!(total, payload.len());
        let mut bad = payload.clone();
        bad[10] ^= 0xFF;
        assert!(split_frames(&bad).is_err());
        assert!(split_frames(&payload[..payload.len() - 1]).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
