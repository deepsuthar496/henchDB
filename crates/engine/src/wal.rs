//! Write-Ahead Log (WAL) with CRC-checked records and group-commit friendly
//! batch appends.
//!
//! File layout:
//!   header: magic "HDBW" + u32 format version (little-endian)
//!   records: [u32 payload_len][u32 crc32(payload)][payload]*
//!
//! A transaction is a sequence of Put/Delete records followed by a Commit
//! record; recovery ignores trailing records without a matching Commit, which
//! makes crash recovery an idempotent redo of committed transactions.
//!
//! v0.1 uses a single serialized log file with `sync_data` per commit batch
//! (correct, portable). The research doc's per-core distributed WAL with
//! io_uring group commit is the roadmap item; the `append_batch` seam below
//! is where it plugs in.

use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::error::{Error, Result};
use crate::table::TableDef;
use crate::types::{ColumnType, Datum};
use std::io::Seek;

pub const WAL_MAGIC: &[u8; 4] = b"HDBW";
/// v2 adds the per-column AUTO_INCREMENT byte to table defs (F7). v3 adds
/// default column values and datetime/timestamp coltypes.
pub const WAL_FORMAT_VERSION: u32 = 3;

const KIND_PUT: u8 = 1;
const KIND_DELETE: u8 = 2;
const KIND_COMMIT: u8 = 3;
const KIND_CREATE_TABLE: u8 = 4;
const KIND_DROP_TABLE: u8 = 5;
const KIND_CREATE_INDEX: u8 = 6;
const KIND_DROP_INDEX: u8 = 7;
const KIND_CREATE_DB: u8 = 8;
const KIND_DROP_DB: u8 = 9;

/// Every record carries its transaction id so recovery can buffer uncommitted
/// work and only redo transactions whose Commit marker reached the log.
#[derive(Debug, Clone)]
pub enum Record {
    Put {
        txn: u64,
        table: String,
        key: Vec<u8>,
        row: Vec<u8>,
    },
    Delete {
        txn: u64,
        table: String,
        key: Vec<u8>,
    },
    Commit {
        txn: u64,
    },
    CreateTable {
        txn: u64,
        def: TableDef,
    },
    DropTable {
        txn: u64,
        name: String,
    },
    CreateIndex {
        txn: u64,
        table: String,
        name: String,
        column: String,
    },
    DropIndex {
        txn: u64,
        table: String,
        name: String,
    },
    CreateDatabase {
        txn: u64,
        name: String,
    },
    DropDatabase {
        txn: u64,
        name: String,
    },
}

/// Shared between `Wal` and the background syncer thread.
///
/// Group commit: `append_records` only writes bytes into the OS page cache
/// (fast, short file-mutex critical section) and advances `written`. A single
/// dedicated syncer thread batches all concurrently pending commits into one
/// `sync_data` call and advances `durable`; committing threads wait for
/// `durable >= my_end`. This is what makes N concurrent durable commits cost
/// roughly one fsync instead of N.
struct WalShared {
    file: Mutex<File>,
    sync_file: Mutex<File>,
    /// End offset of all bytes handed to the OS (monotone under file mutex).
    written: std::sync::atomic::AtomicU64,
    /// End offset known to be durably on disk (monotone; see syncer).
    durable: std::sync::atomic::AtomicU64,
    /// Number of concurrent threads currently waiting on durability.
    waiters: std::sync::atomic::AtomicUsize,
    /// Number of transactions currently in the commit pipeline.
    committing: std::sync::atomic::AtomicUsize,
    /// Signalled on append and on durability progress.
    work: std::sync::Condvar,
    /// Stop flag paired with `work` for the syncer's wait-for-work loop.
    state: Mutex<bool>,
    /// Diagnostics: number of sync_data calls and records covered.
    syncs: std::sync::atomic::AtomicU64,
    /// Bytes covered by those syncs (batch-size proxy).
    synced_bytes: std::sync::atomic::AtomicU64,
}

pub struct CommitterGuard<'a>(&'a std::sync::atomic::AtomicUsize);

impl Drop for CommitterGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

pub struct Wal {
    shared: Arc<WalShared>,
    path: PathBuf,
    syncer: Option<std::thread::JoinHandle<()>>,
}

impl Wal {
    /// Open (creating if absent) the WAL at `path` and start the syncer.
    pub fn open(path: &Path) -> Result<Wal> {
        let exists = path.exists();
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)?;
        if !exists || file.metadata()?.len() == 0 {
            file.seek(std::io::SeekFrom::Start(0))?;
            file.write_all(WAL_MAGIC)?;
            file.write_all(&WAL_FORMAT_VERSION.to_le_bytes())?;
            file.sync_data()?;
        }
        let len = file.metadata()?.len();
        let sync_file = file.try_clone()?;
        let shared = Arc::new(WalShared {
            file: Mutex::new(file),
            sync_file: Mutex::new(sync_file),
            written: std::sync::atomic::AtomicU64::new(len),
            durable: std::sync::atomic::AtomicU64::new(len),
            waiters: std::sync::atomic::AtomicUsize::new(0),
            committing: std::sync::atomic::AtomicUsize::new(0),
            work: std::sync::Condvar::new(),
            state: Mutex::new(false),
            syncs: std::sync::atomic::AtomicU64::new(0),
            synced_bytes: std::sync::atomic::AtomicU64::new(0),
        });
        let worker_shared = shared.clone();
        let syncer = std::thread::Builder::new()
            .name("wal-syncer".into())
            .spawn(move || syncer_loop(worker_shared))?;
        Ok(Wal {
            shared,
            path: path.to_path_buf(),
            syncer: Some(syncer),
        })
    }

    /// Mark that a transaction has entered the commit pipeline.
    pub fn enter_commit(&self) -> CommitterGuard<'_> {
        self.shared.committing.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        CommitterGuard(&self.shared.committing)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Next absolute byte offset a new record batch will start at. Used by
    /// the commit sequencer as the initial install frontier.
    /// (sync_data calls, total bytes synced) — average bytes per sync is the
    /// observed group-commit batch size.
    pub fn sync_stats(&self) -> (u64, u64) {
        (
            self.shared.syncs.load(std::sync::atomic::Ordering::Relaxed),
            self.shared.synced_bytes.load(std::sync::atomic::Ordering::Relaxed),
        )
    }

    pub fn next_offset(&self) -> u64 {
        self.shared
            .written
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Append records without waiting for durability; returns (start, end)
    /// file offsets. The commit sequencer orders installs by these offsets.
    pub fn append_records(&self, records: &[Record]) -> Result<(u64, u64)> {
        let mut buf = Vec::with_capacity(128);
        for rec in records {
            encode_record(rec, &mut buf);
        }
        let len = buf.len() as u64;
        let mut file = self.shared.file.lock().unwrap();
        file.write_all(&buf)?;
        // Atomic bump inside the file lock keeps `written` order == byte order.
        let start = self
            .shared
            .written
            .fetch_add(len, std::sync::atomic::Ordering::AcqRel);
        drop(file);
        self.shared.work.notify_all();
        Ok((start, start + len))
    }

    /// Block until all bytes up to `end` are durably on disk. Concurrent
    /// waiters are batched into the syncer's fsyncs (group commit).
    pub fn wait_durable(&self, end: u64) -> Result<()> {
        let shared = &*self.shared;
        if shared.durable.load(std::sync::atomic::Ordering::Acquire) >= end {
            return Ok(());
        }
        shared.waiters.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        struct WaiterGuard<'a>(&'a std::sync::atomic::AtomicUsize);
        impl Drop for WaiterGuard<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            }
        }
        let _guard = WaiterGuard(&shared.waiters);

        let mut stop = shared.state.lock().unwrap();
        while shared.durable.load(std::sync::atomic::Ordering::Acquire) < end {
            if *stop {
                return Err(Error::Io("wal syncer stopped".into()));
            }
            stop = shared.work.wait(stop).unwrap();
        }
        Ok(())
    }

    /// Append a batch and make it durable (single-shot synchronous path used
    /// by DDL; still batched with any concurrently pending commits).
    pub fn append_batch(&self, records: &[Record]) -> Result<()> {
        let _guard = self.enter_commit();
        let (_, end) = self.append_records(records)?;
        self.wait_durable(end)
    }

    /// Append records without waiting (recovery-test helper: builds a
    /// deliberately dangling transaction tail).
    pub fn append_unsynced(&self, records: &[Record]) -> Result<()> {
        self.append_records(records).map(|_| ())
    }
}

/// How long the syncer collects concurrent appends before issuing one
/// fsync.
const GROUP_COMMIT_WINDOW: std::time::Duration = std::time::Duration::from_micros(100);

/// One fsync covers every commit appended since the previous iteration.
fn syncer_loop(shared: Arc<WalShared>) {
    use std::sync::atomic::Ordering;
    loop {
        let mut stop = shared.state.lock().unwrap();
        while shared.written.load(Ordering::Acquire) == shared.durable.load(Ordering::Acquire) {
            if *stop {
                return;
            }
            stop = shared.work.wait(stop).unwrap();
        }
        if *stop {
            return;
        }
        drop(stop);

        // If multiple committers are in flight, collect them in the group-commit
        // window using spin_loop. If only 1 is committing, flush immediately.
        if shared.committing.load(Ordering::Acquire) > 1 || shared.waiters.load(Ordering::Acquire) > 1 {
            let deadline = std::time::Instant::now() + GROUP_COMMIT_WINDOW;
            while std::time::Instant::now() < deadline {
                std::hint::spin_loop();
            }
        }

        let target = shared.written.load(Ordering::Acquire);
        {
            let sync_file = shared.sync_file.lock().unwrap();
            if sync_file.sync_data().is_err() {
                return; // disk gone: leave `durable` behind so waiters error out
            }
        }
        // Clamp to `written`: a checkpoint reset may have truncated the log
        // while this iteration was in flight.
        let cap = shared.written.load(Ordering::Acquire);
        shared
            .durable
            .fetch_max(target.min(cap), Ordering::AcqRel);
        shared.syncs.fetch_add(1, Ordering::Relaxed);
        shared.synced_bytes.fetch_add(cap, Ordering::Relaxed);
        shared.work.notify_all();
    }
}

impl Drop for Wal {
    fn drop(&mut self) {
        {
            let mut stop = self.shared.state.lock().unwrap();
            *stop = true;
        }
        self.shared.work.notify_all();
        if let Some(h) = self.syncer.take() {
            let _ = h.join();
        }
    }
}

impl Wal {
    /// Truncate the log after a successful checkpoint (snapshot) and rewrite
    /// the header.
    pub fn reset(&self) -> Result<()> {
        // Windows quirk: set_len is not permitted through an append-mode
        // handle, so truncate via a fresh write handle instead. Serialized
        // against appends/syncs through the shared file mutex; callers must
        // ensure no commits are in flight (checkpoint runs under the commit
        // lock in an idle window).
        const HEADER_LEN: u64 = 8; // magic(4) + version(4)
        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        file.write_all(WAL_MAGIC)?;
        file.write_all(&WAL_FORMAT_VERSION.to_le_bytes())?;
        file.sync_data()?;
        drop(file);

        let new_append = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&self.path)?;
        let new_sync = new_append.try_clone()?;

        {
            let mut f = self.shared.file.lock().unwrap();
            let mut sf = self.shared.sync_file.lock().unwrap();
            *f = new_append;
            *sf = new_sync;
            self.shared
                .written
                .store(HEADER_LEN, std::sync::atomic::Ordering::Release);
            self.shared
                .durable
                .store(HEADER_LEN, std::sync::atomic::Ordering::Release);
        }
        Ok(())
    }

    /// Read and verify every record in the log. Returns `Err(Corrupted)` on a
    /// torn/invalid tail; callers should replay the valid prefix returned by
    /// `read_all_prefix` semantics here (records before the error are lost —
    /// the full prefix version is `scan_records`).
    pub fn read_all(&self) -> Result<Vec<Record>> {
        let file = self.shared.file.lock().unwrap();
        let mut clone = file.try_clone()?;
        drop(file);
        clone.seek(std::io::SeekFrom::Start(0))?;
        let mut reader = BufReader::new(clone);
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;
        if &magic != WAL_MAGIC {
            return Err(Error::Corrupted("bad WAL magic".into()));
        }
        let mut vb = [0u8; 4];
        reader.read_exact(&mut vb)?;
        let version = u32::from_le_bytes(vb);
        if version != 1 && version != 2 && version != WAL_FORMAT_VERSION {
            return Err(Error::Corrupted(format!("WAL version {version}")));
        }
        let legacy_cols = version < 3;
        let mut out = Vec::new();
        loop {
            let mut hdr = [0u8; 8];
            match reader.read_exact(&mut hdr) {
                Ok(()) => {}
                Err(_) => break, // clean EOF or torn header: stop
            }
            let len = u32::from_le_bytes(hdr[0..4].try_into().unwrap()) as usize;
            let crc = u32::from_le_bytes(hdr[4..8].try_into().unwrap());
            if len > 1 << 30 {
                return Err(Error::Corrupted("WAL record too large".into()));
            }
            let mut payload = vec![0u8; len];
            if reader.read_exact(&mut payload).is_err() {
                break; // torn tail
            }
            if crc32(&payload) != crc {
                return Err(Error::Corrupted("WAL crc mismatch".into()));
            }
            let mut off = 0usize;
            out.push(decode_record(&payload, &mut off, legacy_cols)?);
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// Record codec
// ---------------------------------------------------------------------------

fn encode_record(rec: &Record, out: &mut Vec<u8>) {
    let start = out.len();
    out.extend_from_slice(&[0u8; 8]); // Reserve 4 bytes len + 4 bytes crc
    let payload_start = out.len();
    match rec {
        Record::Put { txn, table, key, row } => {
            out.push(KIND_PUT);
            out.extend_from_slice(&txn.to_le_bytes());
            put_str(out, table);
            put_bytes(out, key);
            put_bytes(out, row);
        }
        Record::Delete { txn, table, key } => {
            out.push(KIND_DELETE);
            out.extend_from_slice(&txn.to_le_bytes());
            put_str(out, table);
            put_bytes(out, key);
        }
        Record::Commit { txn } => {
            out.push(KIND_COMMIT);
            out.extend_from_slice(&txn.to_le_bytes());
        }
        Record::CreateTable { txn, def } => {
            out.push(KIND_CREATE_TABLE);
            out.extend_from_slice(&txn.to_le_bytes());
            encode_table_def(def, out);
        }
        Record::DropTable { txn, name } => {
            out.push(KIND_DROP_TABLE);
            out.extend_from_slice(&txn.to_le_bytes());
            put_str(out, name);
        }
        Record::CreateIndex { txn, table, name, column } => {
            out.push(KIND_CREATE_INDEX);
            out.extend_from_slice(&txn.to_le_bytes());
            put_str(out, table);
            put_str(out, name);
            put_str(out, column);
        }
        Record::DropIndex { txn, table, name } => {
            out.push(KIND_DROP_INDEX);
            out.extend_from_slice(&txn.to_le_bytes());
            put_str(out, table);
            put_str(out, name);
        }
        Record::CreateDatabase { txn, name } => {
            out.push(KIND_CREATE_DB);
            out.extend_from_slice(&txn.to_le_bytes());
            put_str(out, name);
        }
        Record::DropDatabase { txn, name } => {
            out.push(KIND_DROP_DB);
            out.extend_from_slice(&txn.to_le_bytes());
            put_str(out, name);
        }
    }
    let payload_len = (out.len() - payload_start) as u32;
    let crc = crc32(&out[payload_start..]);
    out[start..start + 4].copy_from_slice(&payload_len.to_le_bytes());
    out[start + 4..start + 8].copy_from_slice(&crc.to_le_bytes());
}

fn decode_record(buf: &[u8], off: &mut usize, legacy_cols: bool) -> Result<Record> {
    let kind = *buf
        .get(*off)
        .ok_or_else(|| Error::Corrupted("record: EOF".into()))?;
    *off += 1;
    let txn = {
        if *off + 8 > buf.len() {
            return Err(Error::Corrupted("record: truncated txn id".into()));
        }
        let t = u64::from_le_bytes(buf[*off..*off + 8].try_into().unwrap());
        *off += 8;
        t
    };
    Ok(match kind {
        KIND_PUT => {
            let table = take_str(buf, off)?;
            let key = take_bytes(buf, off)?;
            let row = take_bytes(buf, off)?;
            Record::Put { txn, table, key, row }
        }
        KIND_DELETE => {
            let table = take_str(buf, off)?;
            let key = take_bytes(buf, off)?;
            Record::Delete { txn, table, key }
        }
        KIND_COMMIT => Record::Commit { txn },
        KIND_CREATE_TABLE => Record::CreateTable {
            txn,
            def: decode_table_def(buf, off, legacy_cols)?,
        },
        KIND_DROP_TABLE => Record::DropTable {
            txn,
            name: take_str(buf, off)?,
        },
        KIND_CREATE_INDEX => {
            let table = take_str(buf, off)?;
            let name = take_str(buf, off)?;
            let column = take_str(buf, off)?;
            Record::CreateIndex {
                txn,
                table,
                name,
                column,
            }
        }
        KIND_DROP_INDEX => {
            let table = take_str(buf, off)?;
            let name = take_str(buf, off)?;
            Record::DropIndex { txn, table, name }
        }
        KIND_CREATE_DB => Record::CreateDatabase {
            txn,
            name: take_str(buf, off)?,
        },
        KIND_DROP_DB => Record::DropDatabase {
            txn,
            name: take_str(buf, off)?,
        },
        t => return Err(Error::Corrupted(format!("unknown record kind {t}"))),
    })
}

fn put_str(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as u32).to_le_bytes());
    out.extend_from_slice(s.as_bytes());
}

fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    out.extend_from_slice(&(b.len() as u32).to_le_bytes());
    out.extend_from_slice(b);
}

fn take_bytes(buf: &[u8], off: &mut usize) -> Result<Vec<u8>> {
    if *off + 4 > buf.len() {
        return Err(Error::Corrupted("bytes: truncated".into()));
    }
    let n = u32::from_le_bytes(buf[*off..*off + 4].try_into().unwrap()) as usize;
    *off += 4;
    if *off + n > buf.len() {
        return Err(Error::Corrupted("bytes: overruns record".into()));
    }
    let s = buf[*off..*off + n].to_vec();
    *off += n;
    Ok(s)
}


fn take_u32(buf: &[u8], off: &mut usize) -> Result<u32> {
    if *off + 4 > buf.len() {
        return Err(Error::Corrupted("u32: truncated".into()));
    }
    let v = u32::from_le_bytes(buf[*off..*off + 4].try_into().unwrap());
    *off += 4;
    Ok(v)
}

fn take_str(buf: &[u8], off: &mut usize) -> Result<String> {
    let b = take_bytes(buf, off)?;
    String::from_utf8(b).map_err(|_| Error::Corrupted("utf8 string".into()))
}

fn encode_table_def(def: &TableDef, out: &mut Vec<u8>) {
    encode_table_def_pub(def, out)
}

/// Catalog snapshot codec reuses the table-def codec; exposed as pub(crate).
pub(crate) fn encode_table_def_pub(def: &TableDef, out: &mut Vec<u8>) {
    put_str(out, &def.name);
    out.extend_from_slice(&(def.schema.columns.len() as u32).to_le_bytes());
    out.extend_from_slice(&(def.schema.pk_idx as u32).to_le_bytes());
    for col in &def.schema.columns {
        put_str(out, &col.name);
        out.push(col.ctype.name().as_bytes()[0]);
        out.push(col.nullable as u8);
        out.push(col.auto_increment as u8);
        match &col.default_value {
            Some(d) => {
                out.push(1);
                d.encode(out);
            }
            None => out.push(0),
        }
    }
    out.extend_from_slice(&(def.indexes.len() as u32).to_le_bytes());
    for idx in &def.indexes {
        put_str(out, &idx.name);
        put_str(out, &idx.column);
    }
    // Trailing FK section (same tolerant pattern as indexes above): older
    // images simply end here and decode to zero FKs; older readers stop
    // before these bytes (def blobs are length-prefixed). No version bump.
    out.extend_from_slice(&(def.foreign_keys.len() as u32).to_le_bytes());
    for fk in &def.foreign_keys {
        put_str(out, &fk.name);
        put_str(out, &fk.column);
        put_str(out, &fk.ref_table);
        put_str(out, &fk.ref_column);
        out.push(match fk.on_delete {
            crate::table::FkAction::Restrict => 0,
            crate::table::FkAction::Cascade => 1,
            crate::table::FkAction::SetNull => 2,
        });
    }
}

fn decode_table_def(buf: &[u8], off: &mut usize, legacy_cols: bool) -> Result<TableDef> {
    decode_table_def_pub(buf, off, legacy_cols)
}

pub(crate) fn decode_table_def_pub(buf: &[u8], off: &mut usize, legacy_cols: bool) -> Result<TableDef> {
    let name = take_str(buf, off)?;
    let ncols = take_u32(buf, off)? as usize;
    if ncols > 10_000 {
        return Err(Error::Corrupted("too many columns in table def".into()));
    }
    let pk_idx = take_u32(buf, off)? as usize;
    let mut columns = Vec::with_capacity(ncols.min(64));
    for _ in 0..ncols {
        let cname = take_str(buf, off)?;
        let tbyte = *buf
            .get(*off)
            .ok_or_else(|| Error::Corrupted("coldef: EOF".into()))?;
        *off += 1;
        let nullable = *buf
            .get(*off)
            .ok_or_else(|| Error::Corrupted("coldef: EOF".into()))?
            != 0;
        *off += 1;
        // Pre-v2 WAL / pre-v3 snapshot defs have no auto-increment byte.
        let auto_increment = if legacy_cols {
            false
        } else {
            let b = *buf
                .get(*off)
                .ok_or_else(|| Error::Corrupted("coldef: EOF".into()))?;
            *off += 1;
            b != 0
        };
        let ctype = match tbyte {
            b'I' => ColumnType::Int,
            b'B' => ColumnType::BigInt,
            b'F' => ColumnType::Float,
            b'D' => ColumnType::Double,
            b'T' => ColumnType::Text,
            b'V' => ColumnType::VarChar,
            b'L' => ColumnType::Bool,
            b'E' | b'M' => ColumnType::DateTime,
            b'P' | b'S' => ColumnType::Timestamp,
            b => return Err(Error::Corrupted(format!("unknown col type {b}"))),
        };
        let default_value = if legacy_cols {
            None
        } else if let Some(&has_def) = buf.get(*off) {
            *off += 1;
            if has_def != 0 {
                Some(Datum::decode(buf, off)?)
            } else {
                None
            }
        } else {
            None
        };
        columns.push(crate::table::ColumnDef {
            name: cname,
            ctype,
            nullable,
            auto_increment,
            default_value,
        });
    }
    let mut indexes = Vec::new();
    if *off < buf.len() {
        let nidx = take_u32(buf, off)? as usize;
        if nidx > 10_000 {
            return Err(Error::Corrupted("too many indexes in table def".into()));
        }
        for _ in 0..nidx {
            let iname = take_str(buf, off)?;
            let icol = take_str(buf, off)?;
            indexes.push(crate::table::IndexDef {
                name: iname,
                column: icol,
            });
        }
    }
    let mut foreign_keys = Vec::new();
    if *off < buf.len() {
        let nfk = take_u32(buf, off)? as usize;
        if nfk > 10_000 {
            return Err(Error::Corrupted("too many FKs in table def".into()));
        }
        for _ in 0..nfk {
            let name = take_str(buf, off)?;
            let column = take_str(buf, off)?;
            let ref_table = take_str(buf, off)?;
            let ref_column = take_str(buf, off)?;
            let on_delete = take_fk_action(buf, off)?;
            foreign_keys.push(crate::table::ForeignKeyDef {
                name,
                column,
                ref_table,
                ref_column,
                on_delete,
            });
        }
    }
    Ok(TableDef {
        name,
        schema: crate::table::Schema { columns, pk_idx },
        indexes,
        foreign_keys,
    })
}

fn take_fk_action(buf: &[u8], off: &mut usize) -> Result<crate::table::FkAction> {
    let b = *buf
        .get(*off)
        .ok_or_else(|| Error::Corrupted("fk: EOF".into()))?;
    *off += 1;
    match b {
        0 => Ok(crate::table::FkAction::Restrict),
        1 => Ok(crate::table::FkAction::Cascade),
        2 => Ok(crate::table::FkAction::SetNull),
        v => Err(Error::Corrupted(format!("unknown FK action {v}"))),
    }
}

/// CRC-32 (IEEE, reflected, table-driven). Small, dependency-free, and fast
/// enough for v0.1; swap for a hardware-CRC or xxh3 later.
pub fn crc32(data: &[u8]) -> u32 {
    fn poly_table() -> &'static [u32; 256] {
        use std::sync::OnceLock;
        static TABLE: OnceLock<[u32; 256]> = OnceLock::new();
        TABLE.get_or_init(|| {
            let mut table = [0u32; 256];
            for (i, e) in table.iter_mut().enumerate() {
                let mut c = i as u32;
                for _ in 0..8 {
                    c = if c & 1 != 0 { 0xEDB8_8320 ^ (c >> 1) } else { c >> 1 };
                }
                *e = c;
            }
            table
        })
    }
    let table = poly_table();
    let mut crc = 0xFFFF_FFFFu32;
    for &b in data {
        crc = table[((crc ^ b as u32) & 0xFF) as usize] ^ (crc >> 8);
    }
    crc ^ 0xFFFF_FFFF
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::{ColumnDef, Schema};

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
                    ColumnDef { name: "id".into(), ctype: ColumnType::Int, nullable: false, auto_increment: true, default_value: None },
                    ColumnDef { name: "v".into(), ctype: ColumnType::Text, nullable: true, auto_increment: false, default_value: None },
                ],
                pk_idx: 0,
            },
            indexes: Vec::new(),
            foreign_keys: Vec::new(),
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
        };
        wal.append_batch(&[
            Record::CreateTable { txn: 1, def: def.clone() },
            Record::Put {
                txn: 1,
                table: "t".into(),
                key: vec![1, 2],
                row: vec![3, 4],
            },
            Record::Commit { txn: 7 },
        ])
        .unwrap();
        drop(wal);
        let wal2 = Wal::open(&path).unwrap();
        let recs = wal2.read_all().unwrap();
        assert_eq!(recs.len(), 3);
        assert!(matches!(recs[0], Record::CreateTable { .. }));
        assert!(matches!(recs[2], Record::Commit { txn: 7 }));
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
        };
        wal.append_batch(&[
            Record::CreateTable { txn: 1, def },
            Record::Put { txn: 1, table: "t".into(), key: vec![1], row: vec![2] },
            Record::Commit { txn: 1 },
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
}
