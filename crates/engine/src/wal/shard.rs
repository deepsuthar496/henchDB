//! Per-core WAL staging shards (Priority 17).
//!
//! Design: committing threads reserve globally-ordered WAL offsets with one
//! atomic `fetch_add` (the `written` reservation frontier in `WalShared`)
//! and stage their encoded bytes into a core-local FIFO — no file lock, no
//! `write_all` syscall inside the commit critical section. The single
//! background syncer drains all shards in monotone offset order (k-way
//! merge on segment start offsets) into one contiguous file write per
//! round, then issues one `sync_data`. File bytes, framing, CRCs, and
//! offset order are identical to the unsharded log, so recovery, replay,
//! PITR, archiving, and replication are untouched.
//!
//! Ordering argument (why the drain never sees a hole): reservation +
//! staged push happen atomically under the `stage_lock` sequencer, so
//! global reservation order == push order across ALL callers (commit-locked
//! commits and lock-free `append_batch` alike). Every shard FIFO is therefore
//! sorted, some shard always fronts the drain frontier, and the syncer only
//! ever waits for genuinely new appends.
//!
//! Locking: appends take stage_lock, then one shard mutex (released
//! before returning); the syncer and `reset()` share `flush_lock` across
//! drain+write (+truncate/swap for reset), taking shard mutexes only while
//! holding it. Order everywhere is flush -> shard or stage -> shard, and
//! the two families never nest — no cycles, no deadlock.
//!
//! Portability: the syncer's single ordered `write_all` in `super` is the
//! exact seam where a Linux `io_uring` (IOPOLL) submit-and-wait backend
//! plugs in. It is deliberately NOT implemented here: `engine` is std-only
//! (no `libc` for setup/mmap/enter), raw-syscall `asm!` would be untestable
//! `unsafe` in the durability core on this Windows-only CI, and an
//! unverified kernel-interface driver is worse than none. When Linux CI
//! exists, implement `flush_bytes_uring` behind `#[cfg(target_os =
//! "linux")]` at that seam and keep the portable path as fallback.

use std::collections::VecDeque;
use std::sync::{Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};

/// One staged reservation: bytes for `[start, start + data.len())`,
/// complete on push (never mutated afterwards).
pub(crate) struct Segment {
    pub start: u64,
    pub data: Vec<u8>,
}

/// FIFO staging queue for one shard.
pub(crate) struct Shard {
    queue: Mutex<VecDeque<Segment>>,
}

impl Shard {
    fn new() -> Self {
        Shard { queue: Mutex::new(VecDeque::new()) }
    }

    /// Pop the front segment iff it starts exactly at `frontier`.
    pub fn pop_at(&self, frontier: u64) -> Option<Vec<u8>> {
        let mut q = self.queue.lock().unwrap();
        match q.front() {
            Some(s) if s.start == frontier => q.pop_front().map(|s| s.data),
            _ => None,
        }
    }

    /// Push a complete staged segment (FIFO order).
    pub fn push(&self, start: u64, data: Vec<u8>) {
        self.queue.lock().unwrap().push_back(Segment { start, data });
    }

    /// Push a segment back to the FRONT (write-error rollback: the syncer
    /// returns unwritten bytes so a later round retries them in order).
    /// Callers must re-push in reverse pop order to preserve the sequence.
    pub fn push_front(&self, start: u64, data: Vec<u8>) {
        self.queue.lock().unwrap().push_front(Segment { start, data });
    }

    pub fn clear(&self) {
        self.queue.lock().unwrap().clear();
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.queue.lock().unwrap().len()
    }
}

/// Shard pool: `shards[i]` is staged into by whichever threads land on it.
/// Thread-local sticky assignment keeps one thread on one shard (cache
/// locality); first pick round-robins across shards for spread.
pub(crate) struct ShardPool {
    shards: Vec<Shard>,
}

thread_local! {
    static SHARD_CURSOR: std::cell::Cell<usize> = const { std::cell::Cell::new(usize::MAX) };
}

static NEXT_SHARD: AtomicUsize = AtomicUsize::new(0);

impl ShardPool {
    pub fn new() -> Self {
        let n = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .clamp(1, 32);
        let mut shards = Vec::with_capacity(n);
        for _ in 0..n {
            shards.push(Shard::new());
        }
        ShardPool { shards }
    }

    pub fn len(&self) -> usize {
        self.shards.len()
    }

    pub fn shard(&self, idx: usize) -> &Shard {
        &self.shards[idx % self.shards.len()]
    }

    /// Pick a shard for the calling thread (sticky after first pick).
    pub fn pick(&self) -> usize {
        SHARD_CURSOR.with(|c| {
            let cur = c.get();
            if cur == usize::MAX || cur >= self.shards.len() {
                let s = NEXT_SHARD.fetch_add(1, Ordering::Relaxed) % self.shards.len();
                c.set(s);
                s
            } else {
                cur
            }
        })
    }

    pub fn clear_all(&self) {
        for s in &self.shards {
            s.clear();
        }
    }

    #[cfg(test)]
    pub fn queued_segments(&self) -> usize {
        self.shards.iter().map(|s| s.len()).sum()
    }
}
