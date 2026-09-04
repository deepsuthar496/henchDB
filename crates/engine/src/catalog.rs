//! Catalog: table metadata registry + snapshot (checkpoint) file codec.
//!
//! The snapshot is written at checkpoint time; the WAL is truncated right
//! after. Recovery = load snapshot, then redo committed WAL records.

use std::collections::BTreeMap;
use std::io::{Read, Write};

use crate::error::{Error, Result};
use crate::table::TableDef;

pub const SNAPSHOT_MAGIC: &[u8; 4] = b"HDBS";
/// v2 stores explicit (key, value) row pairs so overflow locators survive
/// checkpoints (v1 stored values only and re-derived keys, which locators
/// cannot provide). v3 adds the per-column AUTO_INCREMENT byte (F7).
/// Older snapshots still decode (missing fields take safe defaults).
pub const SNAPSHOT_FORMAT_VERSION: u32 = 3;

/// One snapshot row: explicit key plus stored value (inline row or locator).
#[derive(Debug, Clone)]
pub struct SnapRow {
    pub key: Option<Vec<u8>>, // None for v1 rows
    pub value: Vec<u8>,
}

#[derive(Debug, Default)]
pub struct Catalog {
    /// BTreeMap so SHOW TABLES is deterministic.
    pub tables: BTreeMap<String, TableDef>,
}

impl Catalog {
    pub fn get(&self, name: &str) -> Option<&TableDef> {
        self.tables.get(name)
    }

    pub fn create(&mut self, def: TableDef) -> Result<()> {
        if self.tables.contains_key(&def.name) {
            return Err(Error::TableExists(def.name));
        }
        self.tables.insert(def.name.clone(), def);
        Ok(())
    }

    pub fn drop(&mut self, name: &str) -> Result<TableDef> {
        self.tables
            .remove(name)
            .ok_or_else(|| Error::TableNotFound(name.to_string()))
    }

    pub fn list(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }
}

/// Encode the full data directory snapshot (all tables + rows).
/// Layout: magic, version, table_count, then per table:
///   table_def, row_count(u64), rows:
///     v1: [u32 len][value bytes]*
///     v2: [u32 key_len][key][u32 val_len][value]*
pub fn encode_snapshot<W: Write>(w: &mut W, tables: &[(TableDef, Vec<(Vec<u8>, Vec<u8>)>)]) -> Result<()> {
    w.write_all(SNAPSHOT_MAGIC)?;
    w.write_all(&SNAPSHOT_FORMAT_VERSION.to_le_bytes())?;
    w.write_all(&(tables.len() as u32).to_le_bytes())?;
    for (def, rows) in tables {
        let mut buf = Vec::new();
        crate::wal::encode_table_def_pub(def, &mut buf);
        w.write_all(&(buf.len() as u32).to_le_bytes())?;
        w.write_all(&buf)?;
        w.write_all(&(rows.len() as u64).to_le_bytes())?;
        for (key, val) in rows {
            if key.len() > 1024 * 1024 {
                return Err(Error::Corrupted("snapshot key too large".into()));
            }
            w.write_all(&(key.len() as u32).to_le_bytes())?;
            w.write_all(key)?;
            w.write_all(&(val.len() as u32).to_le_bytes())?;
            w.write_all(val)?;
        }
    }
    w.flush()?;
    Ok(())
}

pub fn decode_snapshot<R: Read>(r: &mut R) -> Result<Vec<(TableDef, Vec<SnapRow>)>> {
    let mut magic = [0u8; 4];
    r.read_exact(&mut magic)?;
    if &magic != SNAPSHOT_MAGIC {
        return Err(Error::Corrupted("bad snapshot magic".into()));
    }
    let mut b4 = [0u8; 4];
    r.read_exact(&mut b4)?;
    let version = u32::from_le_bytes(b4);
    if version != 1 && version != 2 && version != SNAPSHOT_FORMAT_VERSION {
        return Err(Error::Corrupted("snapshot version".into()));
    }
    // Pre-v3 table defs carry no AUTO_INCREMENT byte per column.
    r.read_exact(&mut b4)?;
    let ntables = u32::from_le_bytes(b4) as usize;
    if ntables > 100_000 {
        return Err(Error::Corrupted("snapshot table count too large".into()));
    }
    let mut out = Vec::with_capacity(ntables.min(1024));
    for _ in 0..ntables {
        r.read_exact(&mut b4)?;
        let def_len = u32::from_le_bytes(b4) as usize;
        if def_len > 16 * 1024 * 1024 {
            return Err(Error::Corrupted("snapshot table def too large".into()));
        }
        let mut def_buf = vec![0u8; def_len];
        r.read_exact(&mut def_buf)?;
        let mut off = 0usize;
        let def = crate::wal::decode_table_def_pub(&def_buf, &mut off, version <= 2)?;
        let mut b8 = [0u8; 8];
        r.read_exact(&mut b8)?;
        let nrows = u64::from_le_bytes(b8) as usize;
        let mut rows = Vec::with_capacity(nrows.min(1024));
        for _ in 0..nrows {
            if version == 1 {
                r.read_exact(&mut b4)?;
                let len = u32::from_le_bytes(b4) as usize;
                if len > 64 * 1024 * 1024 {
                    return Err(Error::Corrupted("snapshot row too large".into()));
                }
                let mut row = vec![0u8; len];
                r.read_exact(&mut row)?;
                rows.push(SnapRow { key: None, value: row });
            } else {
                r.read_exact(&mut b4)?;
                let klen = u32::from_le_bytes(b4) as usize;
                if klen > 1024 * 1024 {
                    return Err(Error::Corrupted("snapshot key too large".into()));
                }
                let mut key = vec![0u8; klen];
                r.read_exact(&mut key)?;
                r.read_exact(&mut b4)?;
                let vlen = u32::from_le_bytes(b4) as usize;
                if vlen > 64 * 1024 * 1024 {
                    return Err(Error::Corrupted("snapshot row too large".into()));
                }
                let mut val = vec![0u8; vlen];
                r.read_exact(&mut val)?;
                rows.push(SnapRow { key: Some(key), value: val });
            }
        }
        out.push((def, rows));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::{ColumnDef, Schema};
    use crate::types::ColumnType;

    #[test]
    fn snapshot_roundtrip() {
        let def = TableDef {
            name: "t".into(),
            schema: Schema {
                columns: vec![
                    ColumnDef { name: "id".into(), ctype: ColumnType::Int, nullable: false, auto_increment: false },
                    ColumnDef { name: "v".into(), ctype: ColumnType::Text, nullable: true, auto_increment: false },
                ],
                pk_idx: 0,
            },
            indexes: Vec::new(),
        };
        let tables = vec![(def, vec![(vec![9u8], vec![1, 9, 3, 0, 0, 0, 0, 0, 0, 1, 2])])];
        let mut buf = Vec::new();
        encode_snapshot(&mut buf, &tables).unwrap();
        let decoded = decode_snapshot(&mut buf.as_slice()).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].0.name, "t");
        assert_eq!(decoded[0].1.len(), 1);
        assert_eq!(decoded[0].1[0].key, Some(vec![9u8]));
    }

    #[test]
    fn snapshot_v1_still_decodes() {
        // Hand-built v1 image (values only): migration path must accept it.
        let mut buf = Vec::new();
        buf.extend_from_slice(b"HDBS");
        buf.extend_from_slice(&1u32.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes()); // zero tables
        let decoded = decode_snapshot(&mut buf.as_slice()).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn snapshot_codec_corruption_robustness() {
        let def = TableDef {
            name: "t".into(),
            schema: Schema {
                columns: vec![
                    ColumnDef { name: "id".into(), ctype: ColumnType::Int, nullable: false, auto_increment: false },
                    ColumnDef { name: "v".into(), ctype: ColumnType::Text, nullable: true, auto_increment: false },
                ],
                pk_idx: 0,
            },
            indexes: Vec::new(),
        };
        let tables = vec![(def, vec![(vec![9u8], vec![1, 9, 3, 0, 0, 0, 0, 0, 0, 1, 2])])];
        let mut buf = Vec::new();
        encode_snapshot(&mut buf, &tables).unwrap();

        // 1. Bit flip fuzzing: must return Error::Corrupted or Err, never panic
        for i in 0..buf.len() {
            let mut corrupted = buf.clone();
            corrupted[i] ^= 0xAA;
            let _ = decode_snapshot(&mut corrupted.as_slice());
        }

        // 2. Truncation fuzzing: must return Err on unexpected EOF, never panic
        for len in 0..buf.len() {
            let _ = decode_snapshot(&mut &buf[..len]);
        }
    }
}
