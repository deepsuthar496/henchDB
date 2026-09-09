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
//! v0.1 uses a single log file with per-core staging shards (Priority 17):
//! commits reserve offsets atomically, stage bytes into shard FIFOs without
//! touching the file lock, and one background syncer drains all shards in
//! offset order into one write + `sync_data` per round (correct, portable).
//! The research doc's lock-free commit pipeline and io_uring group commit
//! stay roadmap items; the syncer's single ordered write is their seam
//! (see `wal/shard.rs`).

use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::error::{Error, Result};
use crate::table::TableDef;
use crate::types::{ColumnType, Datum};
use std::io::Seek;

mod shard;

pub const WAL_MAGIC: &[u8; 4] = b"HDBW";
/// v2 adds the per-column AUTO_INCREMENT byte to table defs (F7). v3 adds
/// default column values and datetime/timestamp coltypes. v4 appends an
/// optional trailing commit timestamp (u64 unix seconds) to Commit payloads
/// (PITR); v1-v3 commits (9-byte payloads) decode with `ts: None`.
pub const WAL_FORMAT_VERSION: u32 = 4;

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
        /// Wall-clock commit time (unix seconds), stamped by the commit
        /// path. `None` for pre-v4 log records and hand-built test batches;
        /// PITR treats those as always-included (no time to compare).
        ts: Option<u64>,
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

/// Shared between `Wal`, the per-shard staging appends, and the background
/// syncer thread.
///
/// Group commit with sharded staging (Priority 17): `append_records`
/// encodes outside any lock, reserves global offsets with one atomic
/// `fetch_add` on `written`, and stages bytes into a core-local shard FIFO
/// (short shard-mutex critical section — no file lock, no syscall). The
/// single syncer drains all shards in monotone offset order into one
/// contiguous file write per round and advances `durable` with one
/// `sync_data`; committing threads wait for `durable >= my_end`. File
/// bytes, framing, and offset order are identical to the unsharded log.
struct WalShared {
    file: Mutex<File>,
    sync_file: Mutex<File>,
    /// Reservation frontier: one-past the last RESERVED offset (monotone
    /// via atomic fetch_add). Reservation order == file order.
    written: std::sync::atomic::AtomicU64,
    /// End offset physically present in the file (page cache). The syncer
    /// advances it after each flush batch, before syncing; readers clamp
    /// to it so staged-but-unflushed bytes are never read.
    file_written: std::sync::atomic::AtomicU64,
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
    /// Cumulative microseconds spent inside sync_data (fsync latency).
    sync_us: std::sync::atomic::AtomicU64,
    /// Log generation for replication: bumped on every checkpoint reset
    /// (offsets restart at the header, so replicas key their position by
    /// (generation, offset)). Persisted in a `wal.gen` sidecar next to the
    /// log so primary restarts don't alias a new history onto old offsets.
    generation: std::sync::atomic::AtomicU64,
    /// Per-core staging shards (see `shard.rs`).
    shards: shard::ShardPool,
    /// Sequencer lock: held across offset reservation + staged push so push
    /// order always equals reservation order (per-shard FIFOs stay sorted
    /// even for lock-free `append_batch` callers). Tiny critical section —
    /// fetch_add plus one queue push, no syscalls, no encoding — so it
    /// never approaches the old file-mutex contention.
    stage_lock: Mutex<()>,
    /// Serializes the syncer's drain+write+sync rounds against `reset()`'s
    /// drain+truncate+swap. Appends never take it (shard locks only).
    /// Lock order everywhere: flush -> shard.
    flush_lock: Mutex<()>,
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

/// Sidecar file holding the log generation (see `WalShared::generation`).
/// Missing/unparsable sidecar means generation 0 (pre-replication logs).
fn generation_path(log_path: &Path) -> PathBuf {
    let mut s = log_path.as_os_str().to_owned();
    s.push(".gen");
    PathBuf::from(s)
}

fn read_generation(log_path: &Path) -> u64 {
    let Ok(bytes) = std::fs::read(generation_path(log_path)) else {
        return 0;
    };
    if bytes.len() != 8 {
        return 0;
    }
    u64::from_le_bytes(bytes[..8].try_into().unwrap_or([0u8; 8]))
}

/// Crash-safe write of the `wal.gen` sidecar (pub(crate) so PITR can seal
/// a rolled-forward directory with the next generation).
pub(crate) fn write_generation(log_path: &Path, generation: u64) -> Result<()> {
    let tmp = generation_path(log_path);
    // Write-then-sync the single word; a torn sidecar reads back as 0,
    // which only ever forces a (safe) replica re-snapshot.
    let mut f = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&tmp)?;
    f.write_all(&generation.to_le_bytes())?;
    f.sync_data()?;
    Ok(())
}

/// Wall-clock time (unix seconds) for commit stamping. Second precision
/// matches the `--target-time` CLI granularity; ordering within a second
/// still follows WAL offset order.
pub fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
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
        let generation = read_generation(path);
        let shared = Arc::new(WalShared {
            file: Mutex::new(file),
            sync_file: Mutex::new(sync_file),
            written: std::sync::atomic::AtomicU64::new(len),
            file_written: std::sync::atomic::AtomicU64::new(len),
            durable: std::sync::atomic::AtomicU64::new(len),
            waiters: std::sync::atomic::AtomicUsize::new(0),
            committing: std::sync::atomic::AtomicUsize::new(0),
            work: std::sync::Condvar::new(),
            state: Mutex::new(false),
            syncs: std::sync::atomic::AtomicU64::new(0),
            synced_bytes: std::sync::atomic::AtomicU64::new(0),
            sync_us: std::sync::atomic::AtomicU64::new(0),
            generation: std::sync::atomic::AtomicU64::new(generation),
            shards: shard::ShardPool::new(),
            stage_lock: Mutex::new(()),
            flush_lock: Mutex::new(()),
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

    /// Next absolute byte offset a new record batch will start at
    /// (reservation frontier). Used by the commit sequencer as the install
    /// frontier and by checkpoint to re-base after truncation.
    /// (sync_data calls, total bytes synced) — average bytes per sync is the
    /// observed group-commit batch size.
    pub fn sync_stats(&self) -> (u64, u64) {
        (
            self.shared.syncs.load(std::sync::atomic::Ordering::Relaxed),
            self.shared.synced_bytes.load(std::sync::atomic::Ordering::Relaxed),
        )
    }

    /// Cumulative microseconds spent inside `sync_data` (fsync latency).
    pub fn fsync_us(&self) -> u64 {
        self.shared.sync_us.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Current log generation (replication epochs; see field docs).
    pub fn generation(&self) -> u64 {
        self.shared.generation.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Wait (up to `timeout`) for the durable prefix to advance past
    /// `offset`. Returns the current durable offset — the replication
    /// feeder's wake-up call (spurious wakeups just re-poll).
    pub fn wait_durable_change(
        &self,
        offset: u64,
        timeout: std::time::Duration,
    ) -> u64 {
        use std::sync::atomic::Ordering;
        let shared = &*self.shared;
        if shared.durable.load(Ordering::Acquire) > offset {
            return shared.durable.load(Ordering::Acquire);
        }
        let guard = shared.state.lock().unwrap();
        let _ = shared.work.wait_timeout(guard, timeout);
        shared.durable.load(Ordering::Acquire)
    }

    pub fn next_offset(&self) -> u64 {
        self.shared
            .written
            .load(std::sync::atomic::Ordering::Acquire)
    }
    /// End offset known durably on disk. Replication streams only the
    /// durable prefix, so every streamed byte is a complete framed record.
    pub fn durable_offset(&self) -> u64 {
        self.shared
            .durable
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Read up to `max_len` raw log bytes starting at absolute `from`.
    /// Returns `(bytes, end)` with `end = from + bytes.len()`. The caller
    /// must keep `end <= durable_offset()` so torn tails are impossible;
    /// out-of-range `from` is an error (replica must re-bootstrap).
    pub fn read_range(&self, from: u64, max_len: usize) -> Result<(Vec<u8>, u64)> {
        const HEADER_LEN: u64 = 8;
        if from < HEADER_LEN {
            return Err(Error::Corrupted("wal range below header".into()));
        }
        let file = self.shared.file.lock().unwrap();
        let mut clone = file.try_clone()?;
        drop(file);
        clone.seek(std::io::SeekFrom::Start(from))?;
        // Clamp to bytes physically in the file (never read staged-but-
        // unflushed reservations or past EOF into a short buffer that
        // decode would misread as torn).
        let written = self.next_offset();
        if from > written {
            return Err(Error::Corrupted("wal range beyond written".into()));
        }
        let filed = self
            .shared
            .file_written
            .load(std::sync::atomic::Ordering::Acquire);
        let avail = filed.saturating_sub(from).min(max_len as u64) as usize;
        let mut buf = vec![0u8; avail];
        let mut filled = 0usize;
        while filled < avail {
            match clone.read(&mut buf[filled..]) {
                Ok(0) => break, // raced a truncate: return the prefix
                Ok(n) => filled += n,
                Err(e) => return Err(Error::Io(e.to_string())),
            }
        }
        buf.truncate(filled);
        Ok((buf, from + filled as u64))
    }

    /// Decode framed `[len][crc][payload]` records from a raw byte slice
    /// (replica side). Returns the records plus bytes consumed; stops
    /// cleanly at a torn tail so the caller can buffer the remainder.
    /// CRC failures are hard errors (fail closed, never apply partial).
    pub fn decode_wal_range(data: &[u8], legacy_cols: bool) -> Result<(Vec<Record>, usize)> {
        let mut out = Vec::new();
        let mut off = 0usize;
        while off < data.len() {
            if data.len() - off < 8 {
                break; // torn header: wait for more bytes
            }
            let len =
                u32::from_le_bytes(data[off..off + 4].try_into().unwrap()) as usize;
            let crc = u32::from_le_bytes(data[off + 4..off + 8].try_into().unwrap());
            if len > 1 << 30 {
                return Err(Error::Corrupted("WAL record too large".into()));
            }
            if data.len() - off - 8 < len {
                break; // torn payload: wait for more bytes
            }
            let payload = &data[off + 8..off + 8 + len];
            if crc32(payload) != crc {
                return Err(Error::Corrupted("WAL crc mismatch".into()));
            }
            let mut poff = 0usize;
            out.push(decode_record(payload, &mut poff, legacy_cols)?);
            off += 8 + len;
        }
        Ok((out, off))
    }

    /// Append records without waiting for durability; returns (start, end)
    /// file offsets. The commit sequencer orders installs by these offsets.
    /// Encoding happens outside any lock; offsets are reserved atomically
    /// (reservation order == file order) and bytes stage into the calling
    /// thread's shard FIFO for the syncer to drain in order — no file
    /// lock and no syscall inside the caller's critical section.
    pub fn append_records(&self, records: &[Record]) -> Result<(u64, u64)> {
        let mut buf = Vec::with_capacity(128);
        for rec in records {
            encode_record(rec, &mut buf);
        }
        let len = buf.len() as u64;
        if len == 0 {
            let w = self
                .shared
                .written
                .load(std::sync::atomic::Ordering::Acquire);
            return Ok((w, w));
        }
        // Reserve + stage atomically w.r.t. other appenders (sequencer
        // lock): global reservation order == push order, so every shard
        // FIFO stays sorted and the syncer always fronts the frontier.
        // Encode already happened outside; this section is fetch_add plus
        // one queue push — no syscalls, no file lock.
        let start = {
            let _seq = self.shared.stage_lock.lock().unwrap();
            let start = self
                .shared
                .written
                .fetch_add(len, std::sync::atomic::Ordering::AcqRel);
            let idx = self.shared.shards.pick();
            self.shared.shards.shard(idx).push(start, buf);
            start
        };
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

/// Max bytes coalesced into one file write per syncer round (bounds a
/// single round's latency while keeping big batches to one write+fsync).
const DRAIN_BATCH_CAP: u64 = 4 << 20;

/// Pop staged segments in global offset order starting at `frontier`,
/// up to `cap` bytes. Returns the segments (shard index + start + bytes)
/// and the new frontier. Stops at the first gap: under commit-lock
/// serialization every reservation is staged immediately, so a gap only
/// means a transient lock-free interleave (wait for the next append
/// notification) — never skip ahead, never write out of order.
/// Caller must hold `flush_lock`.
fn drain_available(
    shared: &WalShared,
    cap: u64,
    batch: &mut Vec<(usize, u64, Vec<u8>)>,
) -> u64 {
    use std::sync::atomic::Ordering;
    batch.clear();
    let mut frontier = shared.durable.load(Ordering::Acquire);
    let reserved = shared.written.load(Ordering::Acquire);
    let mut bytes = 0u64;
    while frontier < reserved && bytes < cap {
        let mut found: Option<(usize, Vec<u8>)> = None;
        for i in 0..shared.shards.len() {
            if let Some(data) = shared.shards.shard(i).pop_at(frontier) {
                found = Some((i, data));
                break;
            }
        }
        let Some((idx, data)) = found else {
            debug_assert!(
                false,
                "wal drain gap at offset {frontier} (reserved {reserved})"
            );
            break;
        };
        frontier += data.len() as u64;
        bytes += data.len() as u64;
        batch.push((idx, frontier - data.len() as u64, data));
    }
    frontier
}

/// Write one coalesced batch to the file (the syncer is the sole writer;
/// reset() is excluded by `flush_lock`), advance `file_written`, sync, and
/// publish `durable`. On write error the popped segments are re-queued in
/// order for a later round; on sync error the bytes stay filed but
/// undurable — both mirror the legacy failure contract. Caller holds
/// `flush_lock`.
fn write_and_sync(
    shared: &WalShared,
    batch: &mut Vec<(usize, u64, Vec<u8>)>,
    frontier: u64,
    coalesce: &mut Vec<u8>,
) -> Result<()> {
    use std::sync::atomic::Ordering;
    coalesce.clear();
    for (_, _, data) in batch.iter() {
        coalesce.extend_from_slice(data);
    }
    // Portable flush backend: one ordered page-cache write. This exact
    // call is the seam where a Linux io_uring (IOPOLL) submit-and-wait
    // backend plugs in behind #[cfg(target_os = "linux")] (see
    // `wal/shard.rs` for why it ships as a seam, not an implementation).
    let write_ok = {
        use std::io::Write;
        let mut file = shared.file.lock().unwrap();
        file.write_all(coalesce).is_ok()
    };
    if !write_ok {
        // Return bytes to their shard fronts (reverse pop order per shard
        // preserves each FIFO exactly) and leave `durable` behind.
        let mut back: Vec<(usize, u64, Vec<u8>)> = std::mem::take(batch);
        while let Some((idx, start, data)) = back.pop() {
            shared.shards.shard(idx).push_front(start, data);
        }
        return Err(Error::Io("wal file write failed".into()));
    }
    shared.file_written.store(frontier, Ordering::Release);
    {
        let sync_file = shared.sync_file.lock().unwrap();
        let t0 = std::time::Instant::now();
        if sync_file.sync_data().is_err() {
            return Err(Error::Io("wal sync failed".into()));
        }
        shared
            .sync_us
            .fetch_add(t0.elapsed().as_micros() as u64, Ordering::Relaxed);
    }
    shared.durable.fetch_max(frontier, Ordering::AcqRel);
    shared.syncs.fetch_add(1, Ordering::Relaxed);
    shared.synced_bytes.fetch_add(frontier, Ordering::Relaxed);
    shared.work.notify_all();
    Ok(())
}

/// One fsync covers every commit appended since the previous iteration.
fn syncer_loop(shared: Arc<WalShared>) {
    use std::sync::atomic::Ordering;
    // Segments popped this round + their coalesced file bytes (both reused
    // across rounds to stay off the allocator in the hot loop).
    let mut batch: Vec<(usize, u64, Vec<u8>)> = Vec::new();
    let mut coalesce: Vec<u8> = Vec::new();
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

        let round: Result<()> = (|| {
            let _flush = shared.flush_lock.lock().unwrap();
            let frontier = drain_available(&shared, DRAIN_BATCH_CAP, &mut batch);
            if batch.is_empty() {
                return Ok(());
            }
            write_and_sync(&shared, &mut batch, frontier, &mut coalesce)
        })();
        if round.is_err() {
            return; // disk gone: leave `durable` behind so waiters error out
        }
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
    /// the header. Staged-but-unflushed bytes are flushed and synced first
    /// (under the same exclusion as the syncer), so the pre-truncate fsync
    /// covers every reserved byte; then the file, handles, and all three
    /// frontiers restart at the header, exactly like the legacy path.
    pub fn reset(&self) -> Result<()> {
        // Windows quirk: set_len is not permitted through an append-mode
        // handle, so truncate via a fresh write handle instead. Serialized
        // against appends/syncs through the shared file mutex; callers must
        // ensure no commits are in flight (checkpoint runs under the commit
        // lock in an idle window).
        const HEADER_LEN: u64 = 8; // magic(4) + version(4)
        // Bump the generation BEFORE truncating: a crash between the two
        // leaves the generation ahead of content, which only ever forces a
        // (safe) replica re-snapshot, never silent divergence.
        let generation = self
            .shared
            .generation
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
            + 1;
        write_generation(&self.path, generation)?;

        // Drain + sync everything staged (excludes the syncer via
        // flush_lock), so no reserved byte is lost to the truncate below.
        let mut batch: Vec<(usize, u64, Vec<u8>)> = Vec::new();
        let mut coalesce: Vec<u8> = Vec::new();
        {
            let _flush = self.shared.flush_lock.lock().unwrap();
            let frontier = drain_available(&self.shared, u64::MAX, &mut batch);
            if !batch.is_empty() {
                write_and_sync(&self.shared, &mut batch, frontier, &mut coalesce)?;
            }
        }

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
                .file_written
                .store(HEADER_LEN, std::sync::atomic::Ordering::Release);
            self.shared
                .durable
                .store(HEADER_LEN, std::sync::atomic::Ordering::Release);
            // The drain above emptied every queue; clear defensively so a
            // gap can never survive the generation boundary.
            self.shared.shards.clear_all();
        }
        // Wake replication feeders: their offsets just went stale (they
        // detect generation/offset mismatch and re-bootstrap).
        self.shared.work.notify_all();
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
        if version < 1 || version > WAL_FORMAT_VERSION {
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
        Record::Commit { txn, ts } => {
            out.push(KIND_COMMIT);
            out.extend_from_slice(&txn.to_le_bytes());
            // Trailing timestamp section (v4): older readers stop after the
            // txn id because Commit payloads are length-framed; older
            // writers emit 9-byte payloads which decode as `ts: None`.
            if let Some(ts) = ts {
                out.extend_from_slice(&ts.to_le_bytes());
            }
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
        KIND_COMMIT => {
            let ts = if buf.len() - *off >= 8 {
                let t = u64::from_le_bytes(buf[*off..*off + 8].try_into().unwrap());
                *off += 8;
                Some(t)
            } else {
                None
            };
            Record::Commit { txn, ts }
        }
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
    // Trailing ANALYZE stats section (same tolerant pattern): older images
    // end here and decode to None; presence byte keeps None explicit.
    match &def.stats {
        Some(s) => {
            out.push(1);
            crate::stats::encode_stats(s, out);
        }
        None => out.push(0),
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
    // Trailing stats section: absent in older images (None); a presence
    // byte distinguishes explicit None from truncation (which errors).
    let mut stats = None;
    if *off < buf.len() {
        let present = *buf
            .get(*off)
            .ok_or_else(|| Error::Corrupted("tabledef: EOF".into()))?;
        *off += 1;
        if present != 0 {
            stats = Some(crate::stats::decode_stats(buf, off)?);
        }
    }
    Ok(TableDef {
        name,
        schema: crate::table::Schema { columns, pk_idx },
        indexes,
        foreign_keys,
        stats,
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
    fn wal_codec_corruption_fuzz_and_robustness() {        let dir = std::env::temp_dir().join(format!("hdbwal_fuzz_{}", std::process::id()));
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

    /// Priority 17: 24 threads hammer durable multi-record transactions
    /// through sharded staging concurrently. Every commit must land exactly
    /// once, in global offset order, with no torn records and no deadlock.
    #[test]
    fn sharded_concurrent_appends_stay_ordered() {
        use std::sync::Arc;
        let dir = std::env::temp_dir().join(format!("hdbwal_shard_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("wal.log");
        let _ = std::fs::remove_file(&path);
        let wal = Arc::new(Wal::open(&path).unwrap());
        const THREADS: u64 = 24;
        const PER_THREAD: u64 = 50;
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
        // All staged bytes must drain (nothing stranded in shards): wait
        // for the syncer to catch the reservation frontier.
        let t0 = std::time::Instant::now();
        while wal.next_offset() != wal.durable_offset() {
            assert!(t0.elapsed() < std::time::Duration::from_secs(30), "drain stall");
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        drop(wal);
        // Reopen: every commit present exactly once, framing intact.
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

    /// Priority 17: hammered durable commits plus a dangling staged tail —
    /// reopening recovers every commit and drops the tail, torn or not.
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
        // Dangling staged Put with no Commit (crash between records).
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
        assert!(!recs.iter().any(|r| matches!(
            r,
            Record::Put { txn: 999_999, .. }
        )));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
