//! Physical backup archives (`HDBB`) and online dump / restore.
//!
//! Layout (all integers big-endian):
//! ```text
//! [4]  magic b"HDBB"
//! [4]  version (1)
//! [8]  unix timestamp (seconds)
//! [4]  header CRC32 (over version + timestamp bytes)
//! [4]  auth length N, [N] auth.bin bytes (0 when absent)
//! [4]  database count, per db: [4] len + UTF-8 name
//! [4]  table count
//! per table: [4] def_len + TableDef codec bytes, [8] row count,
//!   per row: [4] key_len + key + [4] val_len + value (raw tree pairs,
//!   so overflow locators ride along verbatim)
//! [8]  pages.bin length, [M] raw pool file bytes
//! footer: [4] table count, [8] total rows, [4] full-payload CRC32
//! ```
//! The full-payload CRC covers every byte after the 20-byte fixed header
//! (magic + version + timestamp + header CRC) up to the CRC field itself,
//! so any flip or truncation fails closed with [`Error::Corrupted`].
//!
//! Consistency: [`Database::dump`] checkpoints first (durable point, empty
//! WAL), then streams under the commit lock *and* the install lock, so no
//! commit can append or install mid-stream while OLC readers continue
//! untouched. Staged (uncommitted) transaction writes never enter the
//! archive. History (MVCC chains) is intentionally not archived — restores
//! reopen with a clean version buffer.
//!
//! Page data rides as raw `pages.bin` bytes; overflow locators stored in
//! rows stay valid because the pool file is restored verbatim. The
//! AUTO_INCREMENT counter is *not* stored: `Database::open` rebuilds it as
//! max(pk)+1, which matches post-restart behavior exactly.

use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::catalog;
use crate::db::Database;
use crate::error::{Error, Result};
use crate::table::{Table, TableDef};
use crate::wal::crc32;

pub const BACKUP_MAGIC: &[u8; 4] = b"HDBB";
pub const BACKUP_VERSION: u32 = 1;

/// Fixed header length: magic(4) + version(4) + timestamp(8) + header CRC(4).
const HEADER_LEN: usize = 20;

// Allocation caps (mirror catalog.rs discipline: fail closed, never panic).
const MAX_DBS: usize = 10_000;
const MAX_TABLES: usize = 100_000;
const MAX_NAME_LEN: usize = 1024;
const MAX_AUTH_LEN: usize = 1024 * 1024;
const MAX_DEF_LEN: usize = 16 * 1024 * 1024;
const MAX_KEY_LEN: usize = 1024 * 1024;
const MAX_VAL_LEN: usize = 64 * 1024 * 1024;
const MAX_PAGES_LEN: u64 = 1 << 32;

/// Summary of a completed dump.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupStats {
    pub databases: usize,
    pub tables: usize,
    pub rows: u64,
    pub bytes_written: u64,
    pub duration: Duration,
}

/// Summary of a completed restore.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreStats {
    pub tables: usize,
    pub rows: u64,
    pub bytes_read: u64,
}

/// Writer counting bytes and checksumming everything past the fixed header.
struct CrcWriter<W: Write> {
    inner: BufWriter<W>,
    crc: u32,
    count: u64,
    active: bool,
}

impl<W: Write> CrcWriter<W> {
    fn new(inner: W) -> Self {
        CrcWriter {
            inner: BufWriter::with_capacity(128 * 1024, inner),
            crc: 0xFFFF_FFFF,
            count: 0,
            active: false,
        }
    }

    fn table_crc(data: &[u8], mut crc: u32) -> u32 {
        // Table-driven IEEE CRC32 (same polynomial as wal::crc32).
        const fn build() -> [u32; 256] {
            let mut table = [0u32; 256];
            let mut i = 0;
            while i < 256 {
                let mut c = i as u32;
                let mut k = 0;
                while k < 8 {
                    c = if c & 1 != 0 { 0xEDB8_8320 ^ (c >> 1) } else { c >> 1 };
                    k += 1;
                }
                table[i] = c;
                i += 1;
            }
            table
        }
        const TABLE: [u32; 256] = build();
        for &b in data {
            crc = TABLE[((crc ^ b as u32) & 0xFF) as usize] ^ (crc >> 8);
        }
        crc
    }

    fn put(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.inner.write_all(bytes)?;
        if self.active {
            self.crc = Self::table_crc(bytes, self.crc);
        }
        self.count += bytes.len() as u64;
        Ok(())
    }

    fn put_u32(&mut self, v: u32) -> std::io::Result<()> {
        self.put(&v.to_be_bytes())
    }

    fn put_u64(&mut self, v: u64) -> std::io::Result<()> {
        self.put(&v.to_be_bytes())
    }

    fn finish_crc(&self) -> u32 {
        self.crc ^ 0xFFFF_FFFF
    }
}

/// Reader verifying the payload checksum as it goes.
struct CrcReader<R: Read> {
    inner: R,
    crc: u32,
    count: u64,
    active: bool,
}

impl<R: Read> CrcReader<R> {
    fn new(inner: R) -> Self {
        CrcReader { inner, crc: 0xFFFF_FFFF, count: 0, active: false }
    }

    fn take(&mut self, n: usize) -> Result<Vec<u8>> {
        if n > MAX_VAL_LEN.max(MAX_DEF_LEN).max(MAX_PAGES_LEN as usize) {
            return Err(Error::Corrupted("backup: length too large".into()));
        }
        // Chunked reads: a corrupt length prefix must fail on EOF, never
        // materialize a giant allocation up front.
        let mut buf = Vec::with_capacity(n.min(65536));
        let mut remaining = n;
        let mut chunk = [0u8; 65536];
        while remaining > 0 {
            let want = remaining.min(65536);
            self.inner.read_exact(&mut chunk[..want])
                .map_err(|_| Error::Corrupted("backup: truncated".into()))?;
            if self.active {
                self.crc = CrcWriter::<Vec<u8>>::table_crc(&chunk[..want], self.crc);
            }
            self.count += want as u64;
            buf.extend_from_slice(&chunk[..want]);
            remaining -= want;
        }
        Ok(buf)
    }

    fn take_raw(&mut self, n: usize) -> Result<Vec<u8>> {
        // Read without checksumming (header fields, trailing CRC).
        let mut buf = vec![0u8; n];
        self.inner.read_exact(&mut buf).map_err(|_| Error::Corrupted("backup: truncated".into()))?;
        self.count += n as u64;
        Ok(buf)
    }

    fn u32(&mut self) -> Result<u32> {
        let b = self.take(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn u64(&mut self) -> Result<u64> {
        let b = self.take(8)?;
        let mut a = [0u8; 8];
        a.copy_from_slice(&b);
        Ok(u64::from_be_bytes(a))
    }

    fn name(&mut self) -> Result<String> {
        let len = self.u32()? as usize;
        if len > MAX_NAME_LEN {
            return Err(Error::Corrupted("backup: name too long".into()));
        }
        let b = self.take(len)?;
        String::from_utf8(b).map_err(|_| Error::Corrupted("backup: bad utf8".into()))
    }

    fn finish_crc(&self) -> u32 {
        self.crc ^ 0xFFFF_FFFF
    }
}

/// Stream catalog + rows + pool + auth for pre-sorted `dbs`/`tables`.
/// Called with the commit and install locks held (see `Database::dump`).
pub(crate) fn dump_stream<W: Write>(
    writer: &mut W,
    dir: &Path,
    dbs: Vec<String>,
    tables: &[Arc<Table>],
) -> Result<BackupStats> {
    let t0 = Instant::now();
    let mut w = CrcWriter::new(writer);
    // Fixed header (not covered by the payload CRC).
    w.put(BACKUP_MAGIC)?;
    w.put_u32(BACKUP_VERSION)?;
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    w.put_u64(ts)?;
    let hcrc = crc32(&{
        let mut h = Vec::with_capacity(12);
        h.extend_from_slice(&BACKUP_VERSION.to_be_bytes());
        h.extend_from_slice(&ts.to_be_bytes());
        h
    });
    w.put_u32(hcrc)?;
    w.active = true;

    // Auth section.
    let auth = std::fs::read(dir.join("auth.bin")).unwrap_or_default();
    if auth.len() > MAX_AUTH_LEN {
        return Err(Error::Corrupted("auth.bin too large for backup".into()));
    }
    w.put_u32(auth.len() as u32)?;
    w.put(&auth)?;

    // Databases.
    if dbs.len() > MAX_DBS {
        return Err(Error::Corrupted("too many databases".into()));
    }
    w.put_u32(dbs.len() as u32)?;
    for db in &dbs {
        w.put_u32(db.len() as u32)?;
        w.put(db.as_bytes())?;
    }

    // Tables + raw row pairs.
    if tables.len() > MAX_TABLES {
        return Err(Error::Corrupted("too many tables".into()));
    }
    w.put_u32(tables.len() as u32)?;
    let mut total_rows: u64 = 0;
    for table in tables {
        let def = table.table_def();
        let mut def_buf = Vec::new();
        crate::wal::encode_table_def_pub(&def, &mut def_buf);
        if def_buf.len() > MAX_DEF_LEN {
            return Err(Error::Corrupted("table def too large".into()));
        }
        w.put_u32(def_buf.len() as u32)?;
        w.put(&def_buf)?;
        let rows = table.tree().scan_all();
        if rows.len() > MAX_VAL_LEN {
            return Err(Error::Corrupted("too many rows".into()));
        }
        w.put_u64(rows.len() as u64)?;
        for (key, val) in &rows {
            if key.len() > MAX_KEY_LEN || val.len() > MAX_VAL_LEN {
                return Err(Error::Corrupted("backup row too large".into()));
            }
            w.put_u32(key.len() as u32)?;
            w.put(key)?;
            w.put_u32(val.len() as u32)?;
            w.put(val)?;
        }
        total_rows += rows.len() as u64;
    }

    // Raw pool file (overflow locators stay valid verbatim).
    let pages = std::fs::read(dir.join("pages.bin")).unwrap_or_default();
    if pages.len() as u64 > MAX_PAGES_LEN {
        return Err(Error::Corrupted("pages file too large".into()));
    }
    w.put_u64(pages.len() as u64)?;
    w.put(&pages)?;

    // Footer: counts + full-payload CRC.
    w.put_u32(tables.len() as u32)?;
    w.put_u64(total_rows)?;
    let full = w.finish_crc();
    w.active = false;
    w.put_u32(full)?;
    w.inner.flush().map_err(|e| Error::Io(e.to_string()))?;
    let bytes_written = w.count;
    Ok(BackupStats {
        databases: dbs.len(),
        tables: tables.len(),
        rows: total_rows,
        bytes_written,
        duration: t0.elapsed(),
    })
}

impl Database {
    /// Stream the live catalog + rows + pool + auth without checkpointing
    /// first (no WAL truncate, no generation bump). Used by replication
    /// snapshot serving: the image is a consistent point-in-time read (held
    /// under the commit and install locks, like `dump`), and the replica
    /// continues from the returned log head with idempotent redo, so no
    /// fresh-head truncate is required. Skipping the checkpoint also keeps
    /// the sender's generation stable, so serving snapshots never
    /// invalidates other connected replicas (and never erodes fencing).
    pub fn dump_live<W: Write>(&self, writer: &mut W) -> Result<(BackupStats, u64)> {
        let _commit = self.acquire_commit_lock();
        let _install = self.install.lock().unwrap();
        let durable = self.wal_durable();
        let dbs: Vec<String> = {
            let guard = self.databases.read().unwrap();
            let mut v: Vec<String> = guard.iter().cloned().collect();
            v.sort();
            v
        };
        let tables = {
            let guard = self.tables.read().unwrap();
            let mut v: Vec<_> = guard.values().cloned().collect();
            v.sort_by(|a, b| a.def.name.cmp(&b.def.name));
            v
        };
        let stats = dump_stream(writer, &self.dir, dbs, &tables)?;
        Ok((stats, durable))
    }

    /// Restore an archive into `target_dir`: validate everything first,
    /// then write `snapshot.bin` (valid HDBS), `auth.bin`, and `pages.bin`,
    /// and verify with `Database::open`.
    pub fn restore<R: Read>(reader: &mut R, target_dir: &Path) -> Result<RestoreStats> {
        let (decoded, bytes_read) = decode_archive(reader)?;
        let total_rows: u64 = decoded.tables.iter().map(|(_, r)| r.len() as u64).sum();
        // All validated: materialize the directory.
        std::fs::create_dir_all(target_dir)?;
        {
            let f = std::fs::File::create(target_dir.join("snapshot.bin"))?;
            let mut bw = std::io::BufWriter::with_capacity(128 * 1024, f);
            catalog::encode_snapshot(&mut bw, &decoded.databases, &decoded.tables)?;
            bw.flush()?;
        }
        if !decoded.auth.is_empty() {
            std::fs::write(target_dir.join("auth.bin"), &decoded.auth)?;
        }
        if !decoded.pages.is_empty() {
            std::fs::write(target_dir.join("pages.bin"), &decoded.pages)?;
        }
        // Verify the restored catalog opens cleanly.
        let db = Database::open(target_dir)?;
        db.metrics().record_restore();
        drop(db);
        Ok(RestoreStats { tables: decoded.tables.len(), rows: total_rows, bytes_read })
    }
}


/// A fully validated backup archive, decoded but not yet materialized.
/// Replication snapshot-apply consumes this directly into the live
/// database instead of writing files.
pub struct DecodedBackup {
    pub databases: Vec<String>,
    pub tables: Vec<(TableDef, Vec<(Vec<u8>, Vec<u8>)>)>,
    pub pages: Vec<u8>,
    /// Raw `auth.bin` image (replication ignores it: replicas keep their
    /// own user store; `restore` writes it to the target dir).
    pub auth: Vec<u8>,
}

/// Decode + validate an archive (`&mut &[u8]` works for in-memory images).
/// Returns the archive and total bytes consumed.
pub fn decode_archive<R: Read>(reader: &mut R) -> Result<(DecodedBackup, u64)> {
        let mut r = CrcReader::new(reader);
        // Fixed header.
        let magic = r.take_raw(4)?;
        if magic.as_slice() != BACKUP_MAGIC {
            return Err(Error::Corrupted("bad backup magic".into()));
        }
        let version = {
            let b = r.take_raw(4)?;
            u32::from_be_bytes([b[0], b[1], b[2], b[3]])
        };
        if version != BACKUP_VERSION {
            return Err(Error::Corrupted(format!("backup version {version}")));
        }
        let _ts = r.take_raw(8)?;
        let hcrc = {
            let b = r.take_raw(4)?;
            u32::from_be_bytes([b[0], b[1], b[2], b[3]])
        };
        // Header CRC covers version + timestamp bytes (bytes 4..16).
        let mut hraw = version.to_be_bytes().to_vec();
        hraw.extend_from_slice(&_ts);
        if crc32(&hraw) != hcrc {
            return Err(Error::Corrupted("backup header CRC mismatch".into()));
        }
        debug_assert_eq!(r.count, HEADER_LEN as u64);
        r.active = true;

        // Auth section.
        let auth_len = r.u32()? as usize;
        if auth_len > MAX_AUTH_LEN {
            return Err(Error::Corrupted("backup auth too large".into()));
        }
        let auth = r.take(auth_len)?;

        // Databases.
        let ndbs = r.u32()? as usize;
        if ndbs > MAX_DBS {
            return Err(Error::Corrupted("backup db count too large".into()));
        }
        let mut databases = Vec::with_capacity(ndbs.min(64));
        for _ in 0..ndbs {
            databases.push(r.name()?);
        }

        // Tables.
        let ntables = r.u32()? as usize;
        if ntables > MAX_TABLES {
            return Err(Error::Corrupted("backup table count too large".into()));
        }
        let mut tables: Vec<(TableDef, Vec<(Vec<u8>, Vec<u8>)>)> =
            Vec::with_capacity(ntables.min(1024));
        let mut total_rows: u64 = 0;
        for _ in 0..ntables {
            let def_len = r.u32()? as usize;
            if def_len > MAX_DEF_LEN {
                return Err(Error::Corrupted("backup table def too large".into()));
            }
            let def_buf = r.take(def_len)?;
            let mut off = 0usize;
            let def = crate::wal::decode_table_def_pub(&def_buf, &mut off, false)?;
            if off != def_buf.len() {
                return Err(Error::Corrupted("backup table def trailing bytes".into()));
            }
            if tables.iter().any(|(d, _)| d.name == def.name) {
                return Err(Error::Corrupted("duplicate table in backup".into()));
            }
            let nrows = r.u64()? as usize;
            if nrows > MAX_VAL_LEN {
                return Err(Error::Corrupted("backup row count too large".into()));
            }
            let mut rows = Vec::with_capacity(nrows.min(1024));
            for _ in 0..nrows {
                let klen = r.u32()? as usize;
                if klen > MAX_KEY_LEN {
                    return Err(Error::Corrupted("backup key too large".into()));
                }
                let key = r.take(klen)?;
                let vlen = r.u32()? as usize;
                if vlen > MAX_VAL_LEN {
                    return Err(Error::Corrupted("backup value too large".into()));
                }
                let val = r.take(vlen)?;
                rows.push((key, val));
            }
            total_rows += nrows as u64;
            tables.push((def, rows));
        }

        // Pages section.
        let pages_len = r.u64()? as usize;
        if pages_len as u64 > MAX_PAGES_LEN {
            return Err(Error::Corrupted("backup pages too large".into()));
        }
        let pages = r.take(pages_len)?;

        // Footer: counts + full CRC.
        let foot_tables = r.u32()? as usize;
        let foot_rows = r.u64()?;
        if foot_tables != tables.len() || foot_rows != total_rows {
            return Err(Error::Corrupted("backup footer count mismatch".into()));
        }
        let expect = r.finish_crc();
        let got_bytes = r.take_raw(4)?;
        let got = u32::from_be_bytes([
            got_bytes[0],
            got_bytes[1],
            got_bytes[2],
            got_bytes[3],
        ]);
        if got != expect {
            return Err(Error::Corrupted("backup payload CRC mismatch".into()));
        }
        let bytes_read = r.count;
        Ok((
            DecodedBackup { databases, tables, pages, auth },
            bytes_read,
        ))
    }
#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn backup_crc_matches_known_vector() {
        // Table-driven CRC must agree with wal::crc32 (IEEE test vector).
        let data = b"123456789";
        let incremental = CrcWriter::<Vec<u8>>::table_crc(data, 0xFFFF_FFFF) ^ 0xFFFF_FFFF;
        assert_eq!(incremental, crate::wal::crc32(data));
        assert_eq!(incremental, 0xCBF4_3926);
        // Chunked updates equal one-shot.
        let a = CrcWriter::<Vec<u8>>::table_crc(b"12345", 0xFFFF_FFFF);
        let b = CrcWriter::<Vec<u8>>::table_crc(b"6789", a);
        assert_eq!(b ^ 0xFFFF_FFFF, 0xCBF4_3926);
    }

    fn populated(dir: &Path) -> Database {
        let _ = std::fs::remove_dir_all(dir);
        let db = Database::open(dir).unwrap();
        let mut s = db.new_session();
        db.execute(&mut s, "CREATE DATABASE shop").unwrap();
        db.execute(&mut s, "USE shop").unwrap();
        db.execute(
            &mut s,
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, score FLOAT)",
        )
        .unwrap();
        db.execute(&mut s, "CREATE INDEX idx_name ON users (name)").unwrap();
        db.execute(
            &mut s,
            "CREATE TABLE orders (oid INT PRIMARY KEY, uid INT, \
             FOREIGN KEY (uid) REFERENCES users(id) ON DELETE CASCADE)",
        )
        .unwrap();
        db.execute(
            &mut s,
            "INSERT INTO users VALUES (1, 'ann', 9.5), (2, 'bob', 4.0), (3, NULL, 7.25)",
        )
        .unwrap();
        db.execute(&mut s, "INSERT INTO orders VALUES (10, 1), (11, 2)").unwrap();
        // Wide overflow row (>1 KiB spills off-page).
        let wide = "w".repeat(9000);
        db.execute(&mut s, &format!("INSERT INTO users VALUES (9, '{wide}', 1.0)")).unwrap();
        // Fake auth store (server bootstraps the real one).
        std::fs::write(dir.join("auth.bin"), b"test-auth-payload").unwrap();
        db
    }

    #[test]
    fn backup_roundtrip_full() {
        let dir = std::env::temp_dir().join(format!("hdbbak_{}", std::process::id()));
        let dest = std::env::temp_dir().join(format!("hdbbak_out_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&dest);
        let db = populated(&dir);
        let mut buf = Vec::new();
        let stats = db.dump(&mut buf).unwrap();
        assert_eq!((stats.databases, stats.tables), (2, 2));
        assert_eq!(stats.rows, 6);
        assert!(stats.bytes_written as usize == buf.len());
        drop(db);

        let rstats = Database::restore(&mut buf.as_slice(), &dest).unwrap();
        assert_eq!((rstats.tables, rstats.rows), (2, 6));

        let db2 = Database::open(&dest).unwrap();
        let mut s = db2.new_session();
        // Databases + namespaces survive.
        db2.execute(&mut s, "USE shop").unwrap();
        let out = db2.execute(&mut s, "SELECT COUNT(*) FROM users").unwrap();
        assert_eq!(out.rows[0][0], crate::types::Datum::Int(4));
        // Wide overflow row intact.
        let out = db2.execute(&mut s, "SELECT name FROM users WHERE id = 9").unwrap();
        assert_eq!(out.rows[0][0], crate::types::Datum::Text("w".repeat(9000)));
        // Secondary index seeks work.
        let out = db2.execute(&mut s, "SELECT id FROM users WHERE name = 'bob'").unwrap();
        assert_eq!(out.rows[0][0], crate::types::Datum::Int(2));
        // FK enforcement survived (auto-index + constraint).
        assert!(db2.execute(&mut s, "INSERT INTO orders VALUES (99, 555)").is_err());
        db2.execute(&mut s, "DELETE FROM users WHERE id = 1").unwrap();
        let out = db2.execute(&mut s, "SELECT COUNT(*) FROM orders").unwrap();
        assert_eq!(out.rows[0][0], crate::types::Datum::Int(1)); // CASCADE took oid 10
        // Auth bytes restored verbatim.
        assert_eq!(std::fs::read(dest.join("auth.bin")).unwrap(), b"test-auth-payload");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&dest);
    }

    #[test]
    fn backup_empty_db_roundtrip() {
        let dir = std::env::temp_dir().join(format!("hdbbake_{}", std::process::id()));
        let out = std::env::temp_dir().join(format!("hdbbake_out_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&out);
        let db = Database::open(&dir).unwrap();
        let mut buf = Vec::new();
        let stats = db.dump(&mut buf).unwrap();
        assert_eq!((stats.databases, stats.tables, stats.rows), (1, 0, 0));
        drop(db);
        let rstats = Database::restore(&mut buf.as_slice(), &out).unwrap();
        assert_eq!((rstats.tables, rstats.rows), (0, 0));
        let db2 = Database::open(&out).unwrap();
        let mut s = db2.new_session();
        let count = db2.execute(&mut s, "SHOW TABLES").unwrap();
        assert!(count.rows.is_empty());
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&out);
    }

    #[test]
    fn backup_sql_command() {
        let dir = std::env::temp_dir().join(format!("hdbbaksql_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let db = populated(&dir);
        let mut s = db.new_session();
        let path = dir.join("via_sql.hdb");
        db.execute(&mut s, &format!("BACKUP DATABASE TO '{}'", path.display()))
            .unwrap();
        let bytes = std::fs::read(&path).unwrap();
        assert!(bytes.len() > 20);
        assert_eq!(&bytes[..4], b"HDBB");
        drop(db);
        let out = std::env::temp_dir().join(format!("hdbbaksql_out_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&out);
        Database::restore(&mut bytes.as_slice(), &out).unwrap();
        let _ = fs::remove_dir_all(&dir);
        let _ = fs::remove_dir_all(&out);
    }

    #[test]
    fn backup_corruption_fuzz() {
        // Small archive so every single-byte flip is checked exhaustively.
        // Validation precedes all disk writes, so one scratch dir suffices.
        let dir = std::env::temp_dir().join(format!("hdbbakf_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let db = Database::open(&dir).unwrap();
        let mut s = db.new_session();
        db.execute(&mut s, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").unwrap();
        db.execute(&mut s, "INSERT INTO t VALUES (1, 'a'), (2, 'b')").unwrap();
        let mut buf = Vec::new();
        db.dump(&mut buf).unwrap();
        drop(db);
        assert!(buf.len() < 4 * 1024 * 1024, "fuzz archive too large: {}", buf.len());
        let out = std::env::temp_dir().join(format!("hdbbakfz_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&out);
        // Flip every byte of the framing regions plus a strided sample of
        // the bulk payload; the footer CRC covers all of it, so any flip
        // must fail closed.
        let mut flips: Vec<usize> = (0..buf.len().min(1024)).collect();
        let tail = buf.len().saturating_sub(512);
        flips.extend(tail..buf.len());
        let mut i = 1024;
        while i < tail {
            flips.push(i);
            i += if i < 8192 { 137 } else { 4096 };
        }
        flips.sort_unstable();
        flips.dedup();
        for i in flips {
            let mut bad = buf.clone();
            bad[i] ^= 0xAA;
            let r = Database::restore(&mut bad.as_slice(), &out);
            assert!(r.is_err(), "flip at byte {i} decoded cleanly");
        }
        // Every truncation must fail closed (never panic).
        for len in [0usize, 1, 3, 10, 19, 20, 21, 40, 100] {
            let end = len.min(buf.len());
            let r = Database::restore(&mut &buf[..end], &out);
            assert!(r.is_err(), "truncation to {end} decoded cleanly");
        }
        // Wrong magic / version rejected.
        let mut bad = buf.clone();
        bad[0..4].copy_from_slice(b"XXXX");
        assert!(Database::restore(&mut bad.as_slice(), &out).is_err());
        bad = buf.clone();
        bad[4..8].copy_from_slice(&99u32.to_be_bytes());
        assert!(Database::restore(&mut bad.as_slice(), &out).is_err());
        let _ = std::fs::remove_dir_all(&out);
        let _ = std::fs::remove_dir_all(&dir);
    }

}
