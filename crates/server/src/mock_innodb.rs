//! Mock InnoDB-style storage engine — an architectural simulation, NOT a
//! reimplementation of MySQL. It models the structural overheads the research
//! doc identifies as InnoDB's hot-path bottlenecks:
//!
//!  1. Buffer-pool translation: every page access probes a global hash table
//!     (page_id -> frame), then updates LRU bookkeeping and pin counts —
//!     shared-memory writes on every access.
//!  2. Pessimistic latch coupling: tree readers acquire a shared latch per
//!     level (a write to the latch word on every node of every lookup).
//!  3. Global transaction mutexes: `trx_sys` for begin/commit, `lock_sys`
//!     for row lock acquire/release.
//!  4. Redo log: single global LSN mutex; every commit copies records under
//!     the log mutex.
//!  5. Doublewrite: dirty pages are memcpy'd to a doublewrite buffer before
//!     the real write (2x write amplification).
//!
//! It deliberately does NOT model MySQL's SQL layer, network stack, or real
//! fsync behavior — `bench_compare.py` against real MySQL 8 covers full
//! fidelity. The mock isolates the data-path architecture difference in
//! equivalent portable Rust.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

/// Rows per "page" in the mock (16 KiB / ~250-byte rows, like InnoDB).
const ROWS_PER_PAGE: u64 = 64;

struct MockFrame {
    payload: Mutex<[u8; 64]>,
    pins: AtomicU64,
    last_touch: AtomicU64,
}

/// Global page translation table (the thing pointer swizzling eliminates).
struct MockBufferPool {
    table: RwLock<HashMap<u64, Arc<MockFrame>>>,
    clock: AtomicU64,
    translations: AtomicU64,
}

impl MockBufferPool {
    fn new() -> Self {
        MockBufferPool {
            table: RwLock::new(HashMap::new()),
            clock: AtomicU64::new(1),
            translations: AtomicU64::new(0),
        }
    }

    /// The full translation tax every access pays.
    fn get_page(&self, page_id: u64) -> Arc<MockFrame> {
        self.translations.fetch_add(1, Ordering::Relaxed);
        // 1. hash-table probe under a shared latch
        let hit = self.table.read().unwrap().get(&page_id).cloned();
        let frame = match hit {
            Some(f) => f,
            None => {
                // miss path: exclusive latch + insert (skip real I/O)
                let mut t = self.table.write().unwrap();
                t.entry(page_id)
                    .or_insert_with(|| {
                        Arc::new(MockFrame {
                            payload: Mutex::new([0u8; 64]),
                            pins: AtomicU64::new(0),
                            last_touch: AtomicU64::new(0),
                        })
                    })
                    .clone()
            }
        };
        // 2. pin (shared-memory write)
        frame.pins.fetch_add(1, Ordering::Relaxed);
        // 3. LRU bookkeeping (shared-memory write)
        let now = self.clock.fetch_add(1, Ordering::Relaxed);
        frame.last_touch.store(now, Ordering::Relaxed);
        frame
    }

    fn unpin(&self, frame: &MockFrame) {
        frame.pins.fetch_sub(1, Ordering::Relaxed);
    }
}

enum MockNode {
    Leaf { keys: Vec<u64>, vals: Vec<u64> },
    Internal {
        keys: Vec<u64>,
        children: Vec<RwLock<MockNode>>,
    },
}

/// Every level of every lookup takes a shared RwLock read guard — the
/// pessimistic-latch-coupling cost (a write to shared memory per level).
struct MockTree {
    root: RwLock<MockNode>,
}

impl MockTree {
    /// Build a 2-level fanout-64 tree (no dynamic splitting needed for a
    /// read-mostly microbenchmark).
    fn new(n_keys: u64) -> Self {
        let mut children = Vec::new();
        let mut keys = Vec::new();
        let mut k = 0u64;
        while k < n_keys {
            let mut leaf_keys = Vec::with_capacity(64);
            let mut leaf_vals = Vec::with_capacity(64);
            for i in 0..64 {
                if k + i >= n_keys {
                    break;
                }
                leaf_keys.push(k + i);
                leaf_vals.push((k + i) * 7);
            }
            children.push(RwLock::new(MockNode::Leaf {
                keys: leaf_keys,
                vals: leaf_vals,
            }));
            k += 64;
            if k < n_keys {
                keys.push(k - 1);
            }
        }
        MockTree {
            root: RwLock::new(MockNode::Internal { keys, children }),
        }
    }

    fn get(&self, key: u64) -> Option<u64> {
        let root = self.root.read().unwrap(); // shared latch (level 1)
        match &*root {
            MockNode::Leaf { keys, vals } => {
                keys.binary_search(&key).ok().map(|i| vals[i])
            }
            MockNode::Internal { keys, children } => {
                let idx = keys.partition_point(|&k| k < key).min(children.len() - 1);
                let guard = children[idx].read().unwrap(); // shared latch (level 2)
                match &*guard {
                    MockNode::Leaf { keys, vals } => {
                        keys.binary_search(&key).ok().map(|i| vals[i])
                    }
                    MockNode::Internal { .. } => None,
                }
            }
        }
    }
}

pub struct MockInnoDB {
    pool: MockBufferPool,
    tree: MockTree,
    /// Global lock-system mutex (every row lock acquire + release).
    lock_sys: Mutex<()>,
    /// Global transaction-system mutex (begin + commit).
    trx_sys: Mutex<()>,
    /// Global redo log buffer + LSN.
    log_sys: Mutex<Vec<u8>>,
    lsn: AtomicU64,
    /// Doublewrite buffer (memcpy target before the "real write").
    doublewrite: Mutex<Vec<u8>>,
    #[allow(dead_code)]
    n_keys: u64,
}

impl MockInnoDB {
    pub fn new(n_keys: u64) -> Self {
        MockInnoDB {
            pool: MockBufferPool::new(),
            tree: MockTree::new(n_keys),
            lock_sys: Mutex::new(()),
            trx_sys: Mutex::new(()),
            log_sys: Mutex::new(Vec::with_capacity(1024)),
            lsn: AtomicU64::new(0),
            doublewrite: Mutex::new(Vec::with_capacity(1 << 16)),
            n_keys,
        }
    }

    /// sysbench oltp_point_select equivalent through the full mock path.
    pub fn point_select(&self, key: u64) -> Option<u64> {
        let _trx = self.trx_sys.lock().unwrap(); // trx begin
        {
            let _row_lock = self.lock_sys.lock().unwrap(); // row lock
        }
        let page_id = key / ROWS_PER_PAGE;
        let frame = self.pool.get_page(page_id); // translation + pin + LRU
        let v = self.tree.get(key); // pessimistic shared latches
        self.pool.unpin(&frame);
        drop(_trx); // trx end
        v
    }

    /// sysbench oltp_update_index equivalent: row lock + page latch +
    /// doublewrite memcpy + global redo reservation.
    pub fn update_index(&self, key: u64) {
        let _trx = self.trx_sys.lock().unwrap();
        {
            let _row_lock = self.lock_sys.lock().unwrap();
            let page_id = key / ROWS_PER_PAGE;
            let frame = self.pool.get_page(page_id);
            {
                let mut payload = frame.payload.lock().unwrap();
                payload[0] = (key & 0xFF) as u8;
                // doublewrite: copy page to the dblwr buffer before writeback
                let mut dblwr = self.doublewrite.lock().unwrap();
                dblwr.clear();
                dblwr.extend_from_slice(&payload[..]);
            }
            self.pool.unpin(&frame);
        }
        // redo: global LSN reservation + record copy under the log mutex
        let mut log = self.log_sys.lock().unwrap();
        let _lsn = self.lsn.fetch_add(32, Ordering::Relaxed);
        log.extend_from_slice(&[0u8; 32]);
        if log.len() > 1 << 16 {
            log.clear(); // fake flush
        }
        drop(log);
        drop(_trx);
    }

    pub fn stats(&self) -> (u64, u64) {
        (
            self.pool.translations.load(Ordering::Relaxed),
            self.lsn.load(Ordering::Relaxed),
        )
    }

    #[allow(dead_code)]
    pub fn n_keys(&self) -> u64 {
        self.n_keys
    }
}
