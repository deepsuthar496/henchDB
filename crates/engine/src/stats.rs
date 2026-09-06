//! Table statistics for the cost-based optimizer (`ANALYZE TABLE`).
//!
//! `TableStats` is collected by scanning committed rows: per-column null
//! counts, distinct counts, min/max, and most-common values (MCVs) for
//! skew. Estimates derived here feed `db/cost.rs` (selectivity + access
//! path choice). Stats ride the `TableDef` codec as a tolerant trailing
//! section, so old snapshots/WALs decode to `stats: None`.
//!
//! Staleness contract: stats are a point-in-time snapshot. DML after
//! `ANALYZE` does not invalidate them (no auto-recalculation); re-run
//! `ANALYZE TABLE` to refresh. Estimates degrade gracefully, never
//! incorrectly — the executor always re-filters rows.

use std::collections::{HashMap, HashSet};

use crate::db::{Database, Output, Session};
use crate::error::{Error, Result};
use crate::table::Schema;
use crate::types::Datum;

/// Values kept for the most-common-values list.
pub const MCV_CAP: usize = 8;

/// Hashable datum identity for distinct counting. NaN sorts by bit pattern
/// (each NaN value is its own group); NULL is its own group and is also
/// counted in `null_count`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Frozen {
    Null,
    Int(i64),
    Float(u64),
    Text(String),
    Bool(bool),
    DateTime(i64),
}

impl Frozen {
    fn of(d: &Datum) -> Frozen {
        match d {
            Datum::Null => Frozen::Null,
            Datum::Int(v) => Frozen::Int(*v),
            Datum::Float(v) => Frozen::Float(v.to_bits()),
            Datum::Text(v) => Frozen::Text(v.clone()),
            Datum::Bool(v) => Frozen::Bool(*v),
            Datum::DateTime(v) => Frozen::DateTime(*v),
        }
    }

    fn datum(&self) -> Datum {
        match self {
            Frozen::Null => Datum::Null,
            Frozen::Int(v) => Datum::Int(*v),
            Frozen::Float(b) => Datum::Float(f64::from_bits(*b)),
            Frozen::Text(v) => Datum::Text(v.clone()),
            Frozen::Bool(v) => Datum::Bool(*v),
            Frozen::DateTime(v) => Datum::DateTime(*v),
        }
    }
}

/// Per-column statistics from one `ANALYZE TABLE` pass.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnStats {
    /// Rows where this column was NULL.
    pub null_count: usize,
    /// Unique non-null values observed.
    pub distinct_count: usize,
    /// Minimum non-null value in datum order (None when empty/all NULL).
    pub min: Option<Datum>,
    /// Maximum non-null value in datum order.
    pub max: Option<Datum>,
    /// Most common values, most frequent first (capped at `MCV_CAP`).
    pub mcv: Vec<(Datum, usize)>,
    /// Total values observed (== table row count).
    pub total: usize,
}

/// Per-table statistics from one `ANALYZE TABLE` pass.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TableStats {
    pub row_count: usize,
    pub columns: HashMap<String, ColumnStats>,
    /// Unix seconds when the pass ran.
    pub analyzed_at: u64,
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Collect statistics over decoded committed rows (`rows[i]` aligns with
/// `schema.columns[i]`).
pub fn analyze_rows(schema: &Schema, rows: &[Vec<Datum>]) -> TableStats {
    let mut columns = HashMap::new();
    for (idx, col) in schema.columns.iter().enumerate() {
        let mut null_count = 0usize;
        let mut distinct: HashSet<Frozen> = HashSet::new();
        let mut freq: HashMap<Frozen, usize> = HashMap::new();
        let mut min: Option<Datum> = None;
        let mut max: Option<Datum> = None;
        for row in rows {
            let v = row.get(idx).unwrap_or(&Datum::Null);
            if matches!(v, Datum::Null) {
                null_count += 1;
                continue;
            }
            let f = Frozen::of(v);
            distinct.insert(f.clone());
            *freq.entry(f).or_insert(0) += 1;
            if min.as_ref().map_or(true, |m| v < m) {
                min = Some(v.clone());
            }
            if max.as_ref().map_or(true, |m| v > m) {
                max = Some(v.clone());
            }
        }
        let mut mcv: Vec<(Datum, usize)> = freq
            .into_iter()
            .map(|(f, c)| (f.datum(), c))
            .collect();
        mcv.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        mcv.truncate(MCV_CAP);
        columns.insert(
            col.name.clone(),
            ColumnStats {
                null_count,
                distinct_count: distinct.len(),
                min,
                max,
                mcv,
                total: rows.len(),
            },
        );
    }
    TableStats {
        row_count: rows.len(),
        columns,
        analyzed_at: now_secs(),
    }
}

// ---------------------------------------------------------------------------
// Codec (tolerant trailing section of the TableDef codec in wal.rs)
// ---------------------------------------------------------------------------

fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn put_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn put_str(out: &mut Vec<u8>, s: &str) {
    put_u32(out, s.len() as u32);
    out.extend_from_slice(s.as_bytes());
}

fn take_u32(buf: &[u8], off: &mut usize) -> Result<u32> {
    let end = off.checked_add(4).ok_or_else(|| Error::Corrupted("stats: EOF".into()))?;
    if end > buf.len() {
        return Err(Error::Corrupted("stats: EOF".into()));
    }
    *off = end;
    Ok(u32::from_le_bytes(buf[end - 4..end].try_into().map_err(|_| Error::Corrupted("stats: int".into()))?))
}

fn take_u64(buf: &[u8], off: &mut usize) -> Result<u64> {
    let end = off.checked_add(8).ok_or_else(|| Error::Corrupted("stats: EOF".into()))?;
    if end > buf.len() {
        return Err(Error::Corrupted("stats: EOF".into()));
    }
    *off = end;
    Ok(u64::from_le_bytes(buf[end - 8..end].try_into().map_err(|_| Error::Corrupted("stats: int".into()))?))
}

fn take_str(buf: &[u8], off: &mut usize) -> Result<String> {
    let len = take_u32(buf, off)? as usize;
    if len > 1024 {
        return Err(Error::Corrupted("stats: name too long".into()));
    }
    let end = off.checked_add(len).ok_or_else(|| Error::Corrupted("stats: EOF".into()))?;
    if end > buf.len() {
        return Err(Error::Corrupted("stats: EOF".into()));
    }
    let s = String::from_utf8(buf[*off..end].to_vec()).map_err(|_| Error::Corrupted("stats: utf8".into()))?;
    *off = end;
    Ok(s)
}

pub(crate) fn encode_stats(s: &TableStats, out: &mut Vec<u8>) {
    put_u64(out, s.row_count as u64);
    put_u64(out, s.analyzed_at);
    put_u32(out, s.columns.len() as u32);
    let mut names: Vec<&String> = s.columns.keys().collect();
    names.sort();
    for name in names {
        let c = &s.columns[name];
        put_str(out, name);
        put_u64(out, c.null_count as u64);
        put_u64(out, c.distinct_count as u64);
        put_u64(out, c.total as u64);
        match &c.min {
            Some(d) => {
                out.push(1);
                d.encode(out);
            }
            None => out.push(0),
        }
        match &c.max {
            Some(d) => {
                out.push(1);
                d.encode(out);
            }
            None => out.push(0),
        }
        put_u32(out, c.mcv.len() as u32);
        for (d, n) in &c.mcv {
            d.encode(out);
            put_u64(out, *n as u64);
        }
    }
}

pub(crate) fn decode_stats(buf: &[u8], off: &mut usize) -> Result<TableStats> {    let row_count = take_u64(buf, off)? as usize;
    if row_count > 1_000_000_000 {
        return Err(Error::Corrupted("stats: row count too large".into()));
    }
    let analyzed_at = take_u64(buf, off)?;
    let ncols = take_u32(buf, off)? as usize;
    if ncols > 10_000 {
        return Err(Error::Corrupted("stats: too many columns".into()));
    }
    let mut columns = HashMap::with_capacity(ncols.min(64));
    for _ in 0..ncols {
        let name = take_str(buf, off)?;
        let null_count = take_u64(buf, off)? as usize;
        let distinct_count = take_u64(buf, off)? as usize;
        let total = take_u64(buf, off)? as usize;
        let min = match *buf.get(*off).ok_or_else(|| Error::Corrupted("stats: EOF".into()))? {
            0 => {
                *off += 1;
                None
            }
            _ => {
                *off += 1;
                Some(Datum::decode(buf, off)?)
            }
        };
        let max = match *buf.get(*off).ok_or_else(|| Error::Corrupted("stats: EOF".into()))? {
            0 => {
                *off += 1;
                None
            }
            _ => {
                *off += 1;
                Some(Datum::decode(buf, off)?)
            }
        };
        let nmcv = take_u32(buf, off)? as usize;
        if nmcv > MCV_CAP * 128 {
            return Err(Error::Corrupted("stats: MCV list too large".into()));
        }
        let mut mcv = Vec::with_capacity(nmcv.min(16));
        for _ in 0..nmcv {
            let d = Datum::decode(buf, off)?;
            let n = take_u64(buf, off)? as usize;
            mcv.push((d, n));
        }
        columns.insert(
            name,
            ColumnStats {
                null_count,
                distinct_count,
                min,
                max,
                mcv,
                total,
            },
        );
    }
    Ok(TableStats {
        row_count,
        columns,
        analyzed_at,
    })
}

impl Database {
    /// `ANALYZE TABLE <tbl>`: scan committed rows, store per-column stats,
    /// and checkpoint so the snapshot carries them across restarts.
    /// Returns a MySQL-style `(Table, Op, Msg_type, Msg_text)` row.
    pub(crate) fn exec_analyze(&self, session: &Session, table: &str) -> Result<Output> {
        let table_arc = self.table(session, table)?;
        let key = self.resolve_table_key(session, table);
        let rows: Vec<Vec<Datum>> = table_arc
            .scan()?
            .into_iter()
            .map(|(_, row)| row)
            .collect();
        let stats = analyze_rows(table_arc.schema(), &rows);
        table_arc.set_stats(stats);
        // Durable by construction: table_def() attaches stats to the
        // snapshot image written here.
        self.checkpoint()?;
        Ok(Output {
            columns: vec!["Table".into(), "Op".into(), "Msg_type".into(), "Msg_text".into()],
            rows: vec![vec![
                Datum::Text(key),
                Datum::Text("analyze".into()),
                Datum::Text("status".into()),
                Datum::Text("OK".into()),
            ]],
            message: "OK".into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::ColumnDef;
    use crate::types::ColumnType;

    fn schema() -> Schema {
        Schema {
            columns: vec![
                ColumnDef { name: "id".into(), ctype: ColumnType::Int, nullable: false, auto_increment: false, default_value: None },
                ColumnDef { name: "grp".into(), ctype: ColumnType::Int, nullable: true, auto_increment: false, default_value: None },
                ColumnDef { name: "name".into(), ctype: ColumnType::Text, nullable: true, auto_increment: false, default_value: None },
            ],
            pk_idx: 0,
        }
    }

    fn row(id: i64, grp: Option<i64>, name: Option<&str>) -> Vec<Datum> {
        vec![
            Datum::Int(id),
            grp.map(Datum::Int).unwrap_or(Datum::Null),
            name.map(|s| Datum::Text(s.into())).unwrap_or(Datum::Null),
        ]
    }

    #[test]
    fn analyze_empty_table() {
        let s = analyze_rows(&schema(), &[]);
        assert_eq!(s.row_count, 0);
        let id = &s.columns["id"];
        assert_eq!(id.distinct_count, 0);
        assert_eq!(id.min, None);
        assert_eq!(id.max, None);
        assert!(id.mcv.is_empty());
    }

    #[test]
    fn analyze_uniform_distribution() {
        let rows: Vec<Vec<Datum>> = (0..100).map(|i| row(i, Some(i % 10), None)).collect();
        let s = analyze_rows(&schema(), &rows);
        assert_eq!(s.row_count, 100);
        let id = &s.columns["id"];
        assert_eq!(id.distinct_count, 100);
        assert_eq!(id.null_count, 0);
        assert_eq!(id.min, Some(Datum::Int(0)));
        assert_eq!(id.max, Some(Datum::Int(99)));
        let grp = &s.columns["grp"];
        assert_eq!(grp.distinct_count, 10);
        // Uniform: every value appears 10 times; MCV covers all 10 (cap 8? no — 10 values, cap 8).
        assert_eq!(grp.mcv.len(), MCV_CAP);
        assert!(grp.mcv.iter().all(|(_, c)| *c == 10));
        let name = &s.columns["name"];
        assert_eq!(name.null_count, 100);
        assert_eq!(name.distinct_count, 0);
    }

    #[test]
    fn analyze_skewed_distribution_mcv() {
        // 90 x grp=1, 10 x grp=2..11.
        let mut rows: Vec<Vec<Datum>> = (0..90).map(|i| row(i, Some(1), Some("hot"))).collect();
        for i in 90..100 {
            rows.push(row(i, Some(i), Some("cold")));
        }
        let s = analyze_rows(&schema(), &rows);
        let grp = &s.columns["grp"];
        assert_eq!(grp.distinct_count, 11);
        assert_eq!(grp.mcv[0], (Datum::Int(1), 90));
        let name = &s.columns["name"];
        assert_eq!(name.mcv[0], (Datum::Text("hot".into()), 90));
    }

    #[test]
    fn stats_codec_roundtrip_and_truncation() {
        let rows: Vec<Vec<Datum>> = (0..20).map(|i| row(i, Some(i % 4), Some("x"))).collect();
        let s = analyze_rows(&schema(), &rows);
        let mut buf = Vec::new();
        encode_stats(&s, &mut buf);
        let mut off = 0;
        let back = decode_stats(&buf, &mut off).unwrap();
        assert_eq!(back, s);
        assert_eq!(off, buf.len());
        // Truncations fail closed, never panic.
        for len in 0..buf.len() {
            let mut off = 0;
            let _ = decode_stats(&buf[..len], &mut off);
        }
        // Bit flips fail closed, never panic.
        for i in 0..buf.len() {
            let mut bad = buf.clone();
            bad[i] ^= 0xFF;
            let mut off = 0;
            let _ = decode_stats(&bad, &mut off);
        }
    }
}
