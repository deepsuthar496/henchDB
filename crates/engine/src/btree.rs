//! Order-preserving B+ tree with Optimistic Lock Coupling (OLC).
//!
//! Read path: fully optimistic — traverse the tree taking only version
//! snapshots (no writes to shared memory), then validate. A mismatch means a
//! writer raced the read; the traversal restarts from the root.
//!
//! Write path: top-down lock coupling — a writer locks the child while still
//! holding the parent, then releases the parent. Deadlock-free because
//! latches are only acquired root→leaf. Splitting is eager: whenever a writer
//! is about to descend into a full child, it splits that child first while
//! holding the parent's latch, so the parent absorbs the separator atomically
//! from the readers' point of view (readers spin on the locked parent and
//! never observe the intermediate half-linked state).
//!
//! A full root is handled by *wrapping*: a fresh internal root with a single
//! child is swapped in under the root mutex while the old root is untouched,
//! so concurrent readers see either the complete old tree or the complete new
//! one. Every node on the descent is therefore guaranteed non-full when a
//! writer arrives, and no split result ever needs to propagate upward.
//!
//! Deletion mirrors insertion: `remove()` deletes the key, then runs
//! fix-up passes that borrow from (or merge with) a sibling whenever a node
//! drops below half full. Borrow/merge mutations happen while the parent's
//! exclusive latch is held, so optimistic readers spin through the whole
//! transition and re-validate afterwards. Merged-away nodes are unlinked
//! (parent pointer + leaf `next` chain) and their `Arc` is handed to the
//! `EpochManager` for reclamation; the `Arc` itself keeps stale readers
//! memory-safe until they restart. A root left with one child collapses
//! under the root mutex (the reverse of wrapping).
//!
//! Latch discipline for the fix-up pass: a writer holds the parent latch
//! while acquiring the child latch, then the sibling latch. This is
//! deadlock-free: same-level latches are only ever taken while the common
//! parent is exclusively held (serializing all such writers), and every
//! other path acquires latches strictly root→leaf, one at a time. Whole
//! fix-up passes additionally serialize against each other on the tree's
//! fix mutex, so a pass never descends into a node a concurrent pass just
//! unlinked (leaf entry removal and reads stay fully concurrent).
//!
//! # Memory-model note
//!
//! Node bodies are immutable, epoch-quarantined copy-on-write snapshots.
//! Each node holds an atomic pointer to its current body; a writer holding
//! the node's exclusive latch clones the body, mutates the private clone,
//! and publishes it with a single atomic swap. The superseded body is never
//! mutated again — only retired through the [`EpochManager`], which frees it
//! once every thread pinned at retirement time has moved on. Optimistic
//! readers therefore dereference memory that cannot be mutated or freed
//! under them: the read fast path is formally data-race-free (no `UnsafeCell`
//! anywhere on this path) while staying lock-free — readers take no latches
//! and write no shared cache lines. Per-op epoch pins cost two thread-local
//! atomics, and latch version snapshot/validate still guards *logical*
//! consistency (a concurrent split/merge restarts the traversal).
//!
//! Writers that never mutate (`height`, `node_count`, fix-up probes) use the
//! same [`WriteGuard`] but only its shared view: the private clone is built
//! lazily on first mutable access, so read-only latches allocate nothing.

use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::epoch::{EpochManager, Guard as EpochGuard};
use crate::latch::HybridLatch;

/// Split threshold per node. (Future: fixed 256 KiB slotted pages with
/// prefix compression + 4-byte-head SIMD search, per the research doc.)
const MAX_KEYS: usize = 128;
/// Merge threshold: a non-root node holding fewer keys (leaves) or children
/// (internals) than this borrows from a sibling, or merges when both are
/// sparse. Merged pairs always fit: each side holds < MIN_KEYS entries, so
/// their sum stays under MAX_KEYS.
const MIN_KEYS: usize = MAX_KEYS / 2;

#[derive(Clone)]
enum NodeBody {
    Leaf {
        keys: Vec<Vec<u8>>,
        vals: Vec<Vec<u8>>,
        /// Sibling chain for range scans. Stale pointers stay safe: the left
        /// half of a split keeps its node identity and gains a next pointer.
        next: Option<Arc<Node>>,
    },
    Internal {
        /// Separator keys: child[i] holds keys < keys[i] (and >= keys[i-1]).
        keys: Vec<Vec<u8>>,
        children: Vec<Arc<Node>>,
    },
}

pub struct Node {
    latch: HybridLatch,
    /// Atomic pointer to the current immutable body. Writers publish a new
    /// body with one atomic swap while the exclusive latch is held; the old
    /// body is retired through the epoch manager, never mutated or freed
    /// while a pinned reader may reference it.
    ptr: AtomicPtr<NodeBody>,
}

// `AtomicPtr` is `Send + Sync` and `NodeBody` is owned-value data, so the
// auto impls hold; no manual `Send`/`Sync` needed.

impl Drop for Node {
    fn drop(&mut self) {
        // The node itself is dropped only when no `Arc` (and hence no reader
        // traversing through one) references it, so the current body has no
        // outstanding readers. Superseded bodies were retired separately.
        let raw = *self.ptr.get_mut();
        if !raw.is_null() {
            drop(unsafe { Box::from_raw(raw) });
        }
    }
}

impl Node {
    fn new_leaf() -> Arc<Node> {
        Self::new_leaf_with(Vec::new(), Vec::new(), None)
    }

    fn new_leaf_with(
        keys: Vec<Vec<u8>>,
        vals: Vec<Vec<u8>>,
        next: Option<Arc<Node>>,
    ) -> Arc<Node> {
        Self::with_body(NodeBody::Leaf { keys, vals, next })
    }

    fn new_internal(keys: Vec<Vec<u8>>, children: Vec<Arc<Node>>) -> Arc<Node> {
        Self::with_body(NodeBody::Internal { keys, children })
    }

    fn with_body(body: NodeBody) -> Arc<Node> {
        Arc::new(Node {
            latch: HybridLatch::new(),
            ptr: AtomicPtr::new(Box::into_raw(Box::new(body))),
        })
    }

    /// Shared view of the current body snapshot. The result is valid as long
    /// as the caller is protected: either an epoch pin covering the whole
    /// operation (read fast path — the body cannot be freed while pinned) or
    /// the node's exclusive latch (writers — no other writer can swap).
    fn load(&self) -> &NodeBody {
        // SAFETY: bodies behind this pointer are immutable once published;
        // the pointer is only swapped (never mutated in place) and the old
        // body is freed solely via epoch retirement, which waits out every
        // thread pinned at retirement time. Callers uphold the pin/latch
        // contract documented above.
        unsafe { &*self.ptr.load(Ordering::Acquire) }
    }

    /// Snapshot of the node body for optimistic readers. Call only while an
    /// epoch pin covering the operation is alive, between
    /// `latch.optimistic()` and `latch.validate(version)`.
    fn body(&self) -> &NodeBody {
        self.load()
    }

    fn key_count(&self) -> usize {
        match self.load() {
            NodeBody::Leaf { keys, .. } => keys.len(),
            NodeBody::Internal { keys, .. } => keys.len(),
        }
    }

    fn lock(&self, epoch: &Arc<EpochManager>) -> WriteGuard<'_> {
        self.latch.lock_exclusive();
        WriteGuard {
            node: self,
            owned: None,
            epoch: epoch.clone(),
            dirty: false,
        }
    }
}

/// RAII exclusive latch with lazy copy-on-write: shared access reads the
/// published snapshot in place (no allocation); the first mutable access
/// clones it into a private buffer, and dropping a dirty guard atomically
/// publishes the buffer and retires the superseded body. Dropping always
/// releases the latch and bumps the version.
struct WriteGuard<'a> {
    node: &'a Node,
    owned: Option<NodeBody>,
    epoch: Arc<EpochManager>,
    dirty: bool,
}

impl WriteGuard<'_> {
    /// Private working copy, cloning the snapshot on first mutation.
    fn owned_mut(&mut self) -> &mut NodeBody {
        if self.owned.is_none() {
            self.owned = Some(self.node.load().clone());
        }
        self.dirty = true;
        self.owned.as_mut().expect("just cloned")
    }
}

impl Deref for WriteGuard<'_> {
    type Target = NodeBody;
    fn deref(&self) -> &NodeBody {
        // Exclusive latch held: no writer can swap under us, so borrowing the
        // published snapshot directly is stable for the guard's lifetime.
        self.owned.as_ref().unwrap_or_else(|| self.node.load())
    }
}

impl DerefMut for WriteGuard<'_> {
    fn deref_mut(&mut self) -> &mut NodeBody {
        self.owned_mut()
    }
}

impl Drop for WriteGuard<'_> {
    fn drop(&mut self) {
        if self.dirty {
            if let Some(body) = self.owned.take() {
                let old = self
                    .node
                    .ptr
                    .swap(Box::into_raw(Box::new(body)), Ordering::AcqRel);
                // The old body is immutable from here on; readers pinned
                // during the swap keep it alive via EBR quarantine.
                unsafe { self.epoch.retire_raw(old) };
            }
        }
        self.node.latch.unlock_exclusive();
    }
}

pub struct BTree {
    root: Mutex<Arc<Node>>,
    /// Monotonic structural-change counter, exposed for diagnostics.
    splits: AtomicU64,
    /// Sibling merges on delete (subset of structural changes, telementry).
    merges: AtomicU64,
    /// Successful zero-split in-place value updates (telemetry).
    in_place: AtomicU64,
    /// Live entry count (exact: every insert/upsert/remove funnels through
    /// the wrappers below, including restore paths). O(1) size estimates
    /// for the optimizer without scanning.
    entries: AtomicU64,
    /// Epoch manager quarantining superseded node bodies (every write swaps
    /// in a new body and retires the old one) and merged-away nodes.
    /// Always present — even standalone trees need quarantine for their
    /// lock-free readers; `Database` replaces it with its shared manager.
    epoch: Mutex<Arc<EpochManager>>,
    /// Serializes fix-up passes (and root collapses) against each other.
    /// Merges unlink nodes; two interleaved passes could descend into a node
    /// the other just evicted and then restructure its live-shared children
    /// from a stale sibling view, corrupting separators. One pass at a time
    /// keeps every unlink + relink atomic with respect to other fix-ups,
    /// while leaf entry removal (`delete_rec`) and all reads stay fully
    /// concurrent. Lock order is always fix → node latches → root mutex;
    /// no path takes a node latch before the fix mutex, so no deadlock.
    fix: Mutex<()>,
}

enum Descend {
    /// Insert completed; bool is false when the key already existed.
    Done(bool),
    /// The node we descended into was concurrently observed full (a stale
    /// root clone). Restart from the true root.
    Restart,
}

/// Per-tree counters for `SHOW ENGINE STATUS` / Prometheus.
#[derive(Debug, Clone, Copy, Default)]
pub struct TreeStats {
    pub splits: u64,
    pub merges: u64,
    pub in_place: u64,
    pub height: usize,
    pub nodes: usize,
}

impl BTree {
    pub fn new() -> Self {
        BTree {
            root: Mutex::new(Node::new_leaf()),
            splits: AtomicU64::new(0),
            merges: AtomicU64::new(0),
            in_place: AtomicU64::new(0),
            entries: AtomicU64::new(0),
            epoch: Mutex::new(EpochManager::new()),
            fix: Mutex::new(()),
        }
    }

    /// Attach the database's epoch manager so retired bodies and merged-away
    /// nodes quarantine through the shared EBR domain (called by
    /// `Database::open` for every table, mirroring the page-pool attachment).
    pub fn set_epoch_manager(&self, manager: Arc<EpochManager>) {
        *self.epoch.lock().unwrap() = manager;
    }

    /// Attached epoch manager (used to propagate to new indexes).
    pub(crate) fn epoch_manager(&self) -> Arc<EpochManager> {
        self.epoch.lock().unwrap().clone()
    }

    /// This tree's epoch manager (short alias for the hot paths).
    fn manager(&self) -> Arc<EpochManager> {
        self.epoch.lock().unwrap().clone()
    }

    /// Pin the calling thread for one tree operation: every body snapshot
    /// dereferenced below is quarantined until the returned guard drops.
    fn pin_op(&self) -> EpochGuard {
        self.manager().pin()
    }

    /// Reclaim retired bodies and nodes whose epochs have passed. Returns the
    /// reclaimed count.
    pub fn reclaim(&self) -> usize {
        self.manager().try_reclaim()
    }

    fn current_root(&self) -> Arc<Node> {
        self.root.lock().unwrap().clone()
    }

    fn bump_splits(&self) {
        self.splits.fetch_add(1, Ordering::Relaxed);
    }

    // ------------------------------------------------------------------
    // Optimistic read path
    // ------------------------------------------------------------------

    /// Point lookup. Zero writes to shared memory on the hot path: one
    /// epoch pin per call plus latch version snapshots.
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let _pin = self.pin_op();
        'restart: loop {
            let mut node = self.current_root();
            loop {
                let version = node.latch.wait_and_version();
                match node.body() {
                    NodeBody::Leaf { keys, vals, .. } => {
                        let idx = lower_bound(keys, key);
                        // One immutable snapshot: keys/vals are mutually
                        // consistent; the length check only guards a stale
                        // snapshot (validation below restarts on races).
                        let n = keys.len().min(vals.len());
                        let val = if idx < n && keys[idx] == key {
                            Some(vals[idx].clone())
                        } else {
                            None
                        };
                        if node.latch.validate(version) {
                            return val;
                        }
                        continue 'restart;
                    }
                    NodeBody::Internal { keys, children } => {
                        // Same stale-snapshot guard: validate() below fails
                        // and restarts if a concurrent writer replaced this
                        // node mid-read; clamping only avoids indexing past
                        // a snapshot taken at the boundary.
                        let idx = lower_bound(keys, key).min(children.len() - 1);
                        let child = children[idx].clone();
                        if !node.latch.validate(version) {
                            continue 'restart;
                        }
                        node = child;
                    }
                }
            }
        }
    }

    /// Range scan with explicit bound inclusivity, with per-leaf optimistic
    /// validation.
    pub fn range(
        &self,
        start: Option<&[u8]>,
        start_incl: bool,
        end: Option<&[u8]>,
        end_incl: bool,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let _pin = self.pin_op();
        let mut out = Vec::new();
        // Descend to the first leaf.
        let mut node = self.current_root();
        loop {
            let version = node.latch.wait_and_version();
            match node.body() {
                NodeBody::Leaf { .. } => {
                    if !node.latch.validate(version) {
                        node = self.current_root();
                        continue;
                    }
                    break;
                }
                NodeBody::Internal { keys, children } => {
                    let idx = match start {
                        Some(k) => lower_bound(keys, k),
                        None => 0,
                    }
                    .min(children.len() - 1); // clamp against a stale snapshot
                    let child = children[idx].clone();
                    if !node.latch.validate(version) {
                        node = self.current_root();
                        continue;
                    }
                    node = child;
                }
            }
        }
        // Walk the leaf chain, validating each leaf after copying it.
        loop {
            let version = node.latch.wait_and_version();
            let (keys, vals, next) = match node.body() {
                NodeBody::Leaf { keys, vals, next } => (keys.clone(), vals.clone(), next.clone()),
                NodeBody::Internal { .. } => break, // cannot happen at leaf level
            };
            if !node.latch.validate(version) {
                continue; // leaf changed mid-copy; re-read the same leaf
            }
            let mut exhausted = true;
            for (k, v) in keys.into_iter().zip(vals) {
                if let Some(lo) = start {
                    if k.as_slice() < lo || (k.as_slice() == lo && !start_incl) {
                        continue;
                    }
                }
                if let Some(hi) = end {
                    if k.as_slice() > hi || (k.as_slice() == hi && !end_incl) {
                        return out;
                    }
                }
                out.push((k, v));
            }
            if let Some(n) = next {
                node = n;
                exhausted = false;
            }
            if exhausted {
                return out;
            }
        }
        out
    }

    /// Full scan in key order (used by the executor for table scans).
    pub fn scan_all(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.range(None, true, None, true)
    }

    pub fn len(&self) -> usize {
        self.scan_all().len()
    }

    pub fn is_empty(&self) -> bool {
        self.scan_all().is_empty()
    }

    pub fn split_count(&self) -> u64 {
        self.splits.load(Ordering::Relaxed)
    }

    /// Sibling merges performed by delete fix-ups (telemetry).
    pub fn merge_count(&self) -> u64 {
        self.merges.load(Ordering::Relaxed)
    }

    /// Successful zero-split in-place value updates (telemetry).
    pub fn in_place_count(&self) -> u64 {
        self.in_place.load(Ordering::Relaxed)
    }

    /// Exact live entry count (maintained on every mutation).
    pub fn entry_count(&self) -> u64 {
        self.entries.load(Ordering::Relaxed)
    }

    /// Single-tree telemetry rollup for diagnostics.
    pub fn stats(&self) -> TreeStats {
        TreeStats {
            splits: self.split_count(),
            merges: self.merge_count(),
            in_place: self.in_place_count(),
            height: self.height(),
            nodes: self.node_count(),
        }
    }

    // ------------------------------------------------------------------
    // Write path (top-down lock coupling)
    // ------------------------------------------------------------------

    /// Insert `key -> val`. Returns false if the key already existed.
    pub fn insert(&self, key: &[u8], val: &[u8]) -> bool {
        let epoch = self.manager();
        let _pin = epoch.pin();
        loop {
            // Ensure the root is non-full before descending; a full root is
            // wrapped in a fresh parent (atomic under the root mutex, old
            // root untouched so concurrent readers stay consistent).
            if self.current_root().key_count() >= MAX_KEYS {
                let mut guard = self.root.lock().unwrap();
                let cur = guard.clone();
                if cur.key_count() >= MAX_KEYS {
                    let wrap = Node::new_internal(Vec::new(), vec![cur]);
                    *guard = wrap;
                    self.bump_splits();
                    continue;
                }
                drop(guard);
            }
            let root = self.current_root();
            match insert_rec(&root, key, val, self, &epoch) {
                Descend::Done(inserted) => {
                    if inserted {
                        self.entries.fetch_add(1, Ordering::Relaxed);
                    }
                    return inserted;
                }
                Descend::Restart => continue, // stale full root; loop re-wraps
            }
        }
    }

    /// Insert or replace in a single descent. Replaces value in-place when key exists.
    /// Returns the previous value if one existed.
    pub fn upsert(&self, key: &[u8], val: &[u8]) -> Option<Vec<u8>> {
        let epoch = self.manager();
        let _pin = epoch.pin();
        loop {
            if self.current_root().key_count() >= MAX_KEYS {
                let mut guard = self.root.lock().unwrap();
                let cur = guard.clone();
                if cur.key_count() >= MAX_KEYS {
                    let wrap = Node::new_internal(Vec::new(), vec![cur]);
                    *guard = wrap;
                    self.bump_splits();
                    continue;
                }
                drop(guard);
            }
            let root = self.current_root();
            match upsert_rec(&root, key, val, self, &epoch) {
                UpsertDescend::Done(prev) => {
                    if prev.is_none() {
                        self.entries.fetch_add(1, Ordering::Relaxed);
                    }
                    return prev;
                }
                UpsertDescend::Restart => continue,
            }
        }
    }

    /// Replace value strictly in-place if key exists in a single lock-coupled descent.
    /// Never splits nodes, wraps roots, or allocates new tree nodes.
    /// Returns Some(previous_value) if key existed and was updated in-place,
    /// or None if key was not found.
    pub fn update_in_place(&self, key: &[u8], val: &[u8]) -> Option<Vec<u8>> {
        let epoch = self.manager();
        let _pin = epoch.pin();
        let root = self.current_root();
        let prev = update_in_place_rec(&root, key, val, &epoch);
        if prev.is_some() {
            self.in_place.fetch_add(1, Ordering::Relaxed);
        }
        prev
    }

    /// Remove `key`, returning the removed value. Underflowing nodes are
    /// rebalanced (borrow or merge) and empty internal roots collapse, so
    /// heavy deletion shrinks the tree instead of leaving sparse leaves.
    pub fn remove(&self, key: &[u8]) -> Option<Vec<u8>> {
        let epoch = self.manager();
        let _pin = epoch.pin();
        let root = self.current_root();
        let removed = delete_rec(&root, key, &epoch)?;
        self.entries.fetch_sub(1, Ordering::Relaxed);
        // One fix-up descent per merge level; merges strictly reduce the
        // node count, so this terminates.
        while self.fix_pass(key) {}
        self.collapse_root();
        // Pump EBR so retired nodes drain instead of accumulating.
        self.reclaim();
        Some(removed)
    }

    /// Collapse single-child internal roots (the reverse of wrapping).
    /// Pointer swaps happen under the root mutex, so concurrent readers see
    /// either the old or the new root — both fully valid trees.
    fn collapse_root(&self) {
        let _fix = self.fix.lock().unwrap();
        let epoch = self.manager();
        loop {
            let cur = self.current_root();
            let single = {
                let g = cur.lock(&epoch);
                match &*g {
                    NodeBody::Internal { children, .. } if children.len() == 1 => {
                        Some(children[0].clone())
                    }
                    _ => None,
                }
            };
            let Some(child) = single else { break };
            let mut guard = self.root.lock().unwrap();
            if Arc::ptr_eq(&*guard, &cur) {
                *guard = child;
                self.bump_splits();
            } else {
                break; // root changed under us; owner of the new shape retries
            }
        }
    }

    /// One root→leaf lock-coupled descent along `key`'s path, fixing the
    /// first underflowed node found. Returns true when a merge removed a
    /// child (ancestors may now underflow — the caller repeats).
    fn fix_pass(&self, key: &[u8]) -> bool {
        let _fix = self.fix.lock().unwrap();
        let epoch = self.manager();
        let mut parent_arc = self.current_root();
        loop {
            let mut p_guard = parent_arc.lock(&epoch);
            // Latch the path child while holding the parent (root→leaf).
            let idx = match &*p_guard {
                NodeBody::Leaf { .. } => return false, // root leaf: always legal
                NodeBody::Internal { keys, children } => {
                    if children.len() <= 1 {
                        // Single-child root collapses separately; a 1-child
                        // non-root cannot arise from merges (a survivor keeps
                        // both sides' children, so >= 2), so there is nothing
                        // to fix here either way.
                        return false;
                    }
                    lower_bound(keys, key).min(children.len() - 1)
                }
            };
            let child = match &*p_guard {
                NodeBody::Internal { children, .. } => children[idx].clone(),
                NodeBody::Leaf { .. } => unreachable!(),
            };
            let c_guard = child.lock(&epoch);
            let child_is_leaf = matches!(&*c_guard, NodeBody::Leaf { .. });
            let underflowed = if child_is_leaf {
                match &*c_guard {
                    NodeBody::Leaf { keys, .. } => keys.len() < MIN_KEYS,
                    _ => unreachable!(),
                }
            } else {
                match &*c_guard {
                    NodeBody::Internal { children, .. } => children.len() < MIN_KEYS,
                    _ => unreachable!(),
                }
            };
            if !underflowed {
                if child_is_leaf {
                    return false; // leaf fine: bottom of the path
                }
                // Descend: release both latches, the child becomes parent.
                drop(p_guard);
                drop(c_guard);
                parent_arc = child;
                continue;
            }
            // Fix the child while the parent latch is held (invariant: no
            // structural change without the parent's exclusive latch).
            // Borrow cures the single-delete deficit exactly; a merge drops
            // one child from the parent (caller repeats for ancestors).
            return self.fix_child(&mut p_guard, idx, c_guard);
        }
    }

    /// Borrow from a rich sibling, else merge with one. `p_guard` (parent)
    /// and `c_guard` (child at `idx`) are latched; the sibling latch is
    /// taken last. Returns true when a merge removed a child.
    fn fix_child(&self, p_guard: &mut WriteGuard<'_>, idx: usize, c_guard: WriteGuard<'_>) -> bool {
        let epoch = self.manager();
        // Locate a sibling; prefer the right one (merging into the left
        // keeps `next`-chain edits to a single pointer).
        let (sib_idx, merge_into_left) = match &**p_guard {
            NodeBody::Internal { children, .. } => {
                if idx + 1 < children.len() {
                    (idx + 1, true)
                } else {
                    (idx - 1, false)
                }
            }
            NodeBody::Leaf { .. } => unreachable!(),
        };
        let sibling = match &**p_guard {
            NodeBody::Internal { children, .. } => children[sib_idx].clone(),
            NodeBody::Leaf { .. } => unreachable!(),
        };
        let mut s_guard = sibling.lock(&epoch);
        // Sibling fullness decides borrow vs merge.
        let sib_count = match &*s_guard {
            NodeBody::Leaf { keys, .. } => keys.len(),
            NodeBody::Internal { children, .. } => children.len(),
        };
        if sib_count > MIN_KEYS {
            Self::borrow(p_guard, idx, sib_idx, c_guard, &mut s_guard);
            return false;
        }
        Self::merge(p_guard, idx, sib_idx, merge_into_left, c_guard, s_guard, self);
        true
    }

    /// Move one entry from the richer sibling through the parent separator.
    /// Counts are unchanged elsewhere, so no further fix-up is needed.
    fn borrow(
        p_guard: &mut WriteGuard<'_>,
        idx: usize,
        sib_idx: usize,
        mut c_guard: WriteGuard<'_>,
        s_guard: &mut WriteGuard<'_>,
    ) {
        // Separator position between the two children (separator keys[i] is
        // the max key of children[i], per the split convention).
        let sep = idx.min(sib_idx);
        match (&mut *c_guard, &mut **s_guard) {
            (
                NodeBody::Leaf { keys: ck, vals: cv, .. },
                NodeBody::Leaf { keys: sk, vals: sv, .. },
            ) => {
                if sib_idx > idx {
                    // First entry of the right sibling appends to the child.
                    let k = sk.remove(0);
                    let v = sv.remove(0);
                    ck.push(k.clone());
                    cv.push(v);
                    if let NodeBody::Internal { keys, .. } = &mut **p_guard {
                        keys[sep] = k;
                    }
                } else {
                    // Last entry of the left sibling prepends to the child.
                    let k = sk.pop().expect("rich sibling");
                    let v = sv.pop().expect("rich sibling");
                    ck.insert(0, k);
                    cv.insert(0, v);
                    if let NodeBody::Internal { keys, .. } = &mut **p_guard {
                        keys[sep] = sk.last().cloned().unwrap_or_default();
                    }
                }
            }
            (
                NodeBody::Internal { keys: ck, children: cc },
                NodeBody::Internal { keys: sk, children: sc },
            ) => {
                if let NodeBody::Internal { keys: pk, .. } = &mut **p_guard {
                    if sib_idx > idx {
                        // Separator moves down as the child's new last key;
                        // the sibling's first child moves over with it.
                        let down = pk[sep].clone();
                        let up = sk.remove(0);
                        let mv = sc.remove(0);
                        ck.push(down);
                        cc.push(mv);
                        pk[sep] = up;
                    } else {
                        let down = pk[sep].clone();
                        let up = sk.pop().expect("rich sibling");
                        let mv = sc.pop().expect("rich sibling");
                        ck.insert(0, down);
                        cc.insert(0, mv);
                        pk[sep] = up;
                    }
                }
            }
            _ => unreachable!("borrow mixes leaf and internal nodes"),
        }
    }

    /// Fuse two sparse siblings, unlink the loser from the parent and the
    /// leaf chain, and retire it through the epoch manager.
    #[allow(clippy::too_many_arguments)]
    fn merge(
        p_guard: &mut WriteGuard<'_>,
        idx: usize,
        sib_idx: usize,
        merge_into_left: bool,
        mut c_guard: WriteGuard<'_>,
        mut s_guard: WriteGuard<'_>,
        tree: &BTree,
    ) {
        let sep = idx.min(sib_idx);
        // The evicted node's Arc: unlinked below, retired at the end.
        let evicted: Arc<Node>;
        if let NodeBody::Internal { keys: pk, children: pc } = &mut **p_guard {
            match (&mut *c_guard, &mut *s_guard) {
                (
                    NodeBody::Leaf { keys: ak, vals: av, next: an },
                    NodeBody::Leaf { keys: bk, vals: bv, next: bn },
                ) => {
                    if merge_into_left {
                        // Sibling (right) folds into the child (left).
                        ak.append(bk);
                        av.append(bv);
                        *an = bn.take();
                        evicted = pc.remove(sep + 1);
                    } else {
                        // Child (right) folds into the sibling (left).
                        bk.append(ak);
                        bv.append(av);
                        *bn = an.take();
                        evicted = pc.remove(sep + 1);
                        debug_assert!(sep + 1 == idx);
                    }
                    pk.remove(sep);
                }
                (
                    NodeBody::Internal { keys: ak, children: ac },
                    NodeBody::Internal { keys: bk, children: bc },
                ) => {
                    if merge_into_left {
                        ak.push(pk[sep].clone());
                        ak.append(bk);
                        ac.append(bc);
                        evicted = pc.remove(sep + 1);
                    } else {
                        bk.push(pk[sep].clone());
                        bk.append(ak);
                        bc.append(ac);
                        evicted = pc.remove(sep + 1);
                        debug_assert!(sep + 1 == idx);
                    }
                    pk.remove(sep);
                }
                _ => unreachable!("merge mixes leaf and internal nodes"),
            }
        } else {
            unreachable!("merge parent is a leaf");
        }
        drop(c_guard);
        drop(s_guard);
        // (The parent guard lives on in the caller and releases there.)
        // The loser leaves the tree here: no path can reach it anymore.
        // Its Arc goes through EBR so in-flight optimistic readers (which
        // may still hold clones) stay memory-safe; the allocation itself
        // frees once the last clone drops. Superseded bodies publish the
        // same way via the guards' drops above.
        tree.manager().retire(evicted);
        tree.bump_splits();
        tree.merges.fetch_add(1, Ordering::Relaxed);
    }

    /// Leftmost-descent height (root alone = 1). Test/diagnostic helper.
    pub fn height(&self) -> usize {
        let epoch = self.manager();
        let mut h = 0usize;
        let mut node = self.current_root();
        loop {
            h += 1;
            let next = {
                let g = node.lock(&epoch);
                match &*g {
                    NodeBody::Leaf { .. } => None,
                    NodeBody::Internal { children, .. } => children.first().cloned(),
                }
            };
            match next {
                Some(n) => node = n,
                None => return h,
            }
        }
    }

    /// Total node count (test/diagnostic helper; briefly latches each node).
    pub fn node_count(&self) -> usize {
        let epoch = self.manager();
        fn count(node: &Node, epoch: &Arc<EpochManager>) -> usize {
            let g = node.lock(epoch);
            match &*g {
                NodeBody::Leaf { .. } => 1,
                NodeBody::Internal { children, .. } => {
                    1 + children.iter().map(|c| count(c, epoch)).sum::<usize>()
                }
            }
        }
        count(&self.current_root(), &epoch)
    }
}

impl Default for BTree {
    fn default() -> Self {
        Self::new()
    }
}

enum UpsertDescend {
    Done(Option<Vec<u8>>),
    Restart,
}

fn upsert_rec(
    node: &Arc<Node>,
    key: &[u8],
    val: &[u8],
    tree: &BTree,
    epoch: &Arc<EpochManager>,
) -> UpsertDescend {
    let mut g = node.lock(epoch);
    match &mut *g {
        NodeBody::Leaf { keys, vals, .. } => {
            let idx = lower_bound(keys, key);
            if idx < keys.len() && keys[idx] == key {
                let prev = std::mem::replace(&mut vals[idx], val.to_vec());
                return UpsertDescend::Done(Some(prev));
            }
            if keys.len() >= MAX_KEYS {
                return UpsertDescend::Restart;
            }
            keys.insert(idx, key.to_vec());
            vals.insert(idx, val.to_vec());
            UpsertDescend::Done(None)
        }
        NodeBody::Internal { keys, children } => {
            let mut idx = lower_bound(keys, key);
            if children[idx].key_count() >= MAX_KEYS {
                split_child_in_place(keys, children, idx, tree, epoch);
                idx = lower_bound(keys, key);
            }
            let child = children[idx].clone();
            drop(g);
            upsert_rec(&child, key, val, tree, epoch)
        }
    }
}

fn update_in_place_rec(
    node: &Arc<Node>,
    key: &[u8],
    val: &[u8],
    epoch: &Arc<EpochManager>,
) -> Option<Vec<u8>> {
    let mut g = node.lock(epoch);
    match &mut *g {
        NodeBody::Leaf { keys, vals, .. } => {
            let idx = lower_bound(keys, key);
            if idx < keys.len() && keys[idx] == key {
                if vals[idx].len() == val.len() {
                    let prev = vals[idx].clone();
                    vals[idx].copy_from_slice(val);
                    Some(prev)
                } else {
                    let prev = std::mem::replace(&mut vals[idx], val.to_vec());
                    Some(prev)
                }
            } else {
                None
            }
        }
        NodeBody::Internal { keys, children } => {
            if children.is_empty() {
                return None;
            }
            let idx = lower_bound(keys, key);
            let child = children[idx].clone();
            drop(g);
            update_in_place_rec(&child, key, val, epoch)
        }
    }
}

/// Insert into a node the caller has not yet latched; this function acquires
/// the node's exclusive latch and releases it on all paths (via the guard).
/// The caller guarantees the node was non-full when it decided to descend; a
/// stale clone that turns out full returns `Restart`.
fn insert_rec(
    node: &Arc<Node>,
    key: &[u8],
    val: &[u8],
    tree: &BTree,
    epoch: &Arc<EpochManager>,
) -> Descend {
    let mut g = node.lock(epoch);
    match &mut *g {
        NodeBody::Leaf { keys, vals, .. } => {
            if keys.len() >= MAX_KEYS {
                return Descend::Restart; // guard drop releases the latch
            }
            let idx = lower_bound(keys, key);
            if idx < keys.len() && keys[idx] == key {
                return Descend::Done(false); // duplicate: no-op
            }
            keys.insert(idx, key.to_vec());
            vals.insert(idx, val.to_vec());
            Descend::Done(true)
        }
        NodeBody::Internal { keys, children } => {
            let mut idx = lower_bound(keys, key);
            if children[idx].key_count() >= MAX_KEYS {
                // Split the full child while we hold this node's latch, so
                // readers of this node are blocked for the whole transition.
                split_child_in_place(keys, children, idx, tree, epoch);
                idx = lower_bound(keys, key);
            }
            let child = children[idx].clone();
            // NOTE: no assert that the child is non-full here — key_count()
            // is a pinned snapshot read and may transiently disagree with a
            // concurrent writer. A full leaf is handled by
            // Descend::Restart; an internal node one key over threshold is
            // harmless (order and search are unaffected) and gets split by
            // its parent on the next descent.
            drop(g); // release parent latch before descending (lock coupling)
            insert_rec(&child, key, val, tree, epoch)
        }
    }
}

/// Split `children[idx]` while the parent's exclusive latch is held (the
/// caller's guard), then splice the separator and the new right sibling into
/// the parent.
fn split_child_in_place(
    p_keys: &mut Vec<Vec<u8>>,
    p_children: &mut Vec<Arc<Node>>,
    idx: usize,
    tree: &BTree,
    epoch: &Arc<EpochManager>,
) {
    let child = p_children[idx].clone();
    let (sep, right) = {
        let mut cg = child.lock(epoch);
        match &mut *cg {
            NodeBody::Leaf { keys, vals, next } => {
                let mid = keys.len() / 2;
                let right_keys = keys.split_off(mid);
                let right_vals = vals.split_off(mid);
                let right = Node::new_leaf_with(right_keys, right_vals, next.take());
                *next = Some(right.clone());
                let sep = keys.last().cloned().unwrap_or_default();
                (sep, right)
            }
            NodeBody::Internal { keys, children } => {
                let mid = keys.len() / 2;
                let sep = keys[mid].clone();
                let right_keys = keys.split_off(mid + 1);
                let right_children = children.split_off(mid + 1);
                keys.pop(); // separator moves up
                (sep, Node::new_internal(right_keys, right_children))
            }
        }
    }; // child latch released here
    let at = lower_bound(p_keys, &sep);
    p_keys.insert(at, sep);
    p_children.insert(at + 1, right);
    tree.bump_splits();
}

fn delete_rec(
    node: &Arc<Node>,
    key: &[u8],
    epoch: &Arc<EpochManager>,
) -> Option<Vec<u8>> {
    let mut g = node.lock(epoch);
    match &mut *g {
        NodeBody::Leaf { keys, vals, .. } => {
            let idx = lower_bound(keys, key);
            if idx < keys.len() && keys[idx] == key {
                let v = vals.remove(idx);
                keys.remove(idx);
                Some(v)
            } else {
                None
            }
        }
        NodeBody::Internal { keys, children } => {
            let idx = lower_bound(keys, key);
            let child = children[idx].clone();
            drop(g); // release parent latch before descending (lock coupling)
            delete_rec(&child, key, epoch)
        }
    }
}

/// First index whose key is >= `key`.
fn lower_bound(keys: &[Vec<u8>], key: &[u8]) -> usize {
    let mut lo = 0usize;
    let mut hi = keys.len();
    while lo < hi {
        let mid = (lo + hi) / 2;
        if keys[mid].as_slice() < key {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

#[cfg(test)]
mod tests;
