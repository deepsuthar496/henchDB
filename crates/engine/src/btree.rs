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
//! other path acquires latches strictly root→leaf, one at a time.
//!
//! # Memory-model note
//!
//! Node bodies live in `UnsafeCell`. Writers mutate only through
//! [`WriteGuard`], which exists exactly while the node's exclusive latch is
//! held. Optimistic readers read the body between a version snapshot and its
//! re-validation, per the OLC protocol: any read that overlaps a writer fails
//! validation and is discarded. This is the standard production OLC pattern
//! (LeanStore-style); a formally race-free variant (relaxed atomic loads for
//! header fields, or immutable copy-on-write nodes) is a roadmap item.

use std::cell::UnsafeCell;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::epoch::EpochManager;
use crate::latch::HybridLatch;

/// Split threshold per node. (Future: fixed 256 KiB slotted pages with
/// prefix compression + 4-byte-head SIMD search, per the research doc.)
const MAX_KEYS: usize = 128;
/// Merge threshold: a non-root node holding fewer keys (leaves) or children
/// (internals) than this borrows from a sibling, or merges when both are
/// sparse. Merged pairs always fit: each side holds < MIN_KEYS entries, so
/// their sum stays under MAX_KEYS.
const MIN_KEYS: usize = MAX_KEYS / 2;

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
    body: UnsafeCell<NodeBody>,
}

// SAFETY: `body` is mutated only through `WriteGuard`, which exists only
// while `latch` is exclusively held; readers follow the OLC protocol (see
// module docs). The tree is shared across threads via `Arc<Node>`.
unsafe impl Send for Node {}
unsafe impl Sync for Node {}

impl Node {
    fn new_leaf() -> Arc<Node> {
        Self::new_leaf_with(Vec::new(), Vec::new(), None)
    }

    fn new_leaf_with(
        keys: Vec<Vec<u8>>,
        vals: Vec<Vec<u8>>,
        next: Option<Arc<Node>>,
    ) -> Arc<Node> {
        Arc::new(Node {
            latch: HybridLatch::new(),
            body: UnsafeCell::new(NodeBody::Leaf { keys, vals, next }),
        })
    }

    fn new_internal(keys: Vec<Vec<u8>>, children: Vec<Arc<Node>>) -> Arc<Node> {
        Arc::new(Node {
            latch: HybridLatch::new(),
            body: UnsafeCell::new(NodeBody::Internal { keys, children }),
        })
    }

    /// Snapshot of the node body for optimistic readers. Call only between
    /// `latch.optimistic()` and `latch.validate(version)`.
    fn body(&self) -> &NodeBody {
        // SAFETY: see struct-level safety comment; immutably shared read.
        unsafe { &*self.body.get() }
    }

    fn key_count(&self) -> usize {
        match self.body() {
            NodeBody::Leaf { keys, .. } => keys.len(),
            NodeBody::Internal { keys, .. } => keys.len(),
        }
    }

    fn lock(&self) -> WriteGuard<'_> {
        self.latch.lock_exclusive();
        WriteGuard { node: self }
    }
}

/// RAII exclusive latch: mutation is possible only while this guard exists;
/// dropping it releases the latch and bumps the version.
struct WriteGuard<'a> {
    node: &'a Node,
}

impl Deref for WriteGuard<'_> {
    type Target = NodeBody;
    fn deref(&self) -> &NodeBody {
        // SAFETY: exclusive latch held.
        unsafe { &*self.node.body.get() }
    }
}

impl DerefMut for WriteGuard<'_> {
    fn deref_mut(&mut self) -> &mut NodeBody {
        // SAFETY: exclusive latch held.
        unsafe { &mut *self.node.body.get() }
    }
}

impl Drop for WriteGuard<'_> {
    fn drop(&mut self) {
        self.node.latch.unlock_exclusive();
    }
}

pub struct BTree {
    root: Mutex<Arc<Node>>,
    /// Monotonic structural-change counter, exposed for diagnostics.
    splits: AtomicU64,
    /// Epoch manager for merged-away nodes. `None` keeps `BTree` usable
    /// standalone (merges still unlink; the dropped `Arc` frees memory once
    /// stale readers release it); `Database` always attaches one.
    epoch: Mutex<Option<Arc<EpochManager>>>,
}

enum Descend {
    /// Insert completed; bool is false when the key already existed.
    Done(bool),
    /// The node we descended into was concurrently observed full (a stale
    /// root clone). Restart from the true root.
    Restart,
}

impl BTree {
    pub fn new() -> Self {
        BTree {
            root: Mutex::new(Node::new_leaf()),
            splits: AtomicU64::new(0),
            epoch: Mutex::new(None),
        }
    }

    /// Attach the database's epoch manager so merged-away nodes retire
    /// through EBR (called by `Database::open` for every table, mirroring
    /// the page-pool attachment).
    pub fn set_epoch_manager(&self, manager: Arc<EpochManager>) {
        *self.epoch.lock().unwrap() = Some(manager);
    }

    /// Attached epoch manager, if any (used to propagate to new indexes).
    pub(crate) fn epoch_manager(&self) -> Option<Arc<EpochManager>> {
        self.epoch.lock().unwrap().clone()
    }

    /// Reclaim retired nodes whose epochs have passed (no-op without an
    /// attached manager). Returns the reclaimed count.
    pub fn reclaim(&self) -> usize {
        self.epoch
            .lock()
            .unwrap()
            .as_ref()
            .map(|ep| ep.try_reclaim())
            .unwrap_or(0)
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

    /// Point lookup. Zero writes to shared memory on the hot path.
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        'restart: loop {
            let mut node = self.current_root();
            loop {
                let version = node.latch.wait_and_version();
                match node.body() {
                    NodeBody::Leaf { keys, vals, .. } => {
                        let idx = lower_bound(keys, key);
                        // A concurrent insert updates keys before vals; clamp
                        // to both lengths so a torn read cannot go out of
                        // range (validation below rejects torn reads anyway).
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
                        // Clamp: a concurrent split may have mutated this node
                        // between our key read and this read. If the state we
                        // used was torn, validate() below fails and we restart;
                        // clamping only prevents an out-of-range index panic.
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
                    .min(children.len() - 1); // clamp against concurrent splits
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

    // ------------------------------------------------------------------
    // Write path (top-down lock coupling)
    // ------------------------------------------------------------------

    /// Insert `key -> val`. Returns false if the key already existed.
    pub fn insert(&self, key: &[u8], val: &[u8]) -> bool {
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
            match insert_rec(&root, key, val, self) {
                Descend::Done(inserted) => return inserted,
                Descend::Restart => continue, // stale full root; loop re-wraps
            }
        }
    }

    /// Insert or replace in a single descent. Replaces value in-place when key exists.
    /// Returns the previous value if one existed.
    pub fn upsert(&self, key: &[u8], val: &[u8]) -> Option<Vec<u8>> {
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
            match upsert_rec(&root, key, val, self) {
                UpsertDescend::Done(prev) => return prev,
                UpsertDescend::Restart => continue,
            }
        }
    }

    /// Replace value strictly in-place if key exists in a single lock-coupled descent.
    /// Never splits nodes, wraps roots, or allocates new tree nodes.
    /// Returns Some(previous_value) if key existed and was updated in-place,
    /// or None if key was not found.
    pub fn update_in_place(&self, key: &[u8], val: &[u8]) -> Option<Vec<u8>> {
        let root = self.current_root();
        update_in_place_rec(&root, key, val)
    }

    /// Remove `key`, returning the removed value. Underflowing nodes are
    /// rebalanced (borrow or merge) and empty internal roots collapse, so
    /// heavy deletion shrinks the tree instead of leaving sparse leaves.
    pub fn remove(&self, key: &[u8]) -> Option<Vec<u8>> {
        let root = self.current_root();
        let removed = delete_rec(&root, key)?;
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
        loop {
            let cur = self.current_root();
            let single = {
                let g = cur.lock();
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
        let mut parent_arc = self.current_root();
        loop {
            let mut p_guard = parent_arc.lock();
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
            let c_guard = child.lock();
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
        let mut s_guard = sibling.lock();
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
        // frees once the last clone drops.
        if let Some(ep) = tree.epoch.lock().unwrap().as_ref() {
            ep.retire(evicted);
        }
        tree.bump_splits();
    }

    /// Leftmost-descent height (root alone = 1). Test/diagnostic helper.
    pub fn height(&self) -> usize {
        let mut h = 0usize;
        let mut node = self.current_root();
        loop {
            h += 1;
            let next = {
                let g = node.lock();
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
        fn count(node: &Node) -> usize {
            let g = node.lock();
            match &*g {
                NodeBody::Leaf { .. } => 1,
                NodeBody::Internal { children, .. } => {
                    1 + children.iter().map(|c| count(c)).sum::<usize>()
                }
            }
        }
        count(&self.current_root())
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

fn upsert_rec(node: &Arc<Node>, key: &[u8], val: &[u8], tree: &BTree) -> UpsertDescend {
    let mut g = node.lock();
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
                split_child_in_place(keys, children, idx, tree);
                idx = lower_bound(keys, key);
            }
            let child = children[idx].clone();
            drop(g);
            upsert_rec(&child, key, val, tree)
        }
    }
}

fn update_in_place_rec(node: &Arc<Node>, key: &[u8], val: &[u8]) -> Option<Vec<u8>> {
    let mut g = node.lock();
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
            update_in_place_rec(&child, key, val)
        }
    }
}

/// Insert into a node the caller has not yet latched; this function acquires
/// the node's exclusive latch and releases it on all paths (via the guard).
/// The caller guarantees the node was non-full when it decided to descend; a
/// stale clone that turns out full returns `Restart`.
fn insert_rec(node: &Arc<Node>, key: &[u8], val: &[u8], tree: &BTree) -> Descend {
    let mut g = node.lock();
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
                split_child_in_place(keys, children, idx, tree);
                idx = lower_bound(keys, key);
            }
            let child = children[idx].clone();
            // NOTE: no assert that the child is non-full here — key_count()
            // is an unsynchronized optimistic read and may transiently
            // disagree with a concurrent writer. A full leaf is handled by
            // Descend::Restart; an internal node one key over threshold is
            // harmless (order and search are unaffected) and gets split by
            // its parent on the next descent.
            drop(g); // release parent latch before descending (lock coupling)
            insert_rec(&child, key, val, tree)
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
) {
    let child = p_children[idx].clone();
    let (sep, right) = {
        let mut cg = child.lock();
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

fn delete_rec(node: &Arc<Node>, key: &[u8]) -> Option<Vec<u8>> {
    let mut g = node.lock();
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
            delete_rec(&child, key)
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
mod tests {
    use super::*;
    use crate::epoch::EpochManager;
    use std::thread;

    #[test]
    fn insert_get_sequential() {
        let t = BTree::new();
        for i in 0..10_000i64 {
            let k = i.to_be_bytes();
            assert!(t.insert(&k, &k), "dup at {i}");
        }
        for i in 0..10_000i64 {
            let k = i.to_be_bytes();
            assert_eq!(t.get(&k), Some(k.to_vec()));
        }
        assert_eq!(t.get(b"missing"), None);
        assert!(t.split_count() > 0);
    }

    #[test]
    fn duplicate_insert_returns_false() {
        let t = BTree::new();
        assert!(t.insert(b"a", b"1"));
        assert!(!t.insert(b"a", b"2"));
        assert_eq!(t.get(b"a"), Some(b"1".to_vec()));
    }

    #[test]
    fn range_scan_ordered() {
        let t = BTree::new();
        for i in (0..5_000i64).rev() {
            let k = i.to_be_bytes();
            t.insert(&k, &k);
        }
        let all = t.scan_all();
        assert_eq!(all.len(), 5_000);
        for (i, (k, v)) in all.iter().enumerate() {
            let expect = (i as i64).to_be_bytes();
            assert_eq!(k.as_slice(), &expect);
            assert_eq!(v.as_slice(), &expect);
        }
        let lo = 2_500i64.to_be_bytes();
        let hi = 2_600i64.to_be_bytes();
        assert_eq!(t.range(Some(&lo), true, Some(&hi), false).len(), 100);
        assert_eq!(t.range(Some(&lo), true, Some(&hi), true).len(), 101);
        assert_eq!(t.range(Some(&lo), false, Some(&hi), false).len(), 99);
    }

    #[test]
    fn remove_works() {
        let t = BTree::new();
        for i in 0..1_000i64 {
            let k = i.to_be_bytes();
            t.insert(&k, &k);
        }
        for i in (0..1_000i64).step_by(2) {
            let k = i.to_be_bytes();
            assert_eq!(t.remove(&k), Some(k.to_vec()));
        }
        for i in 0..1_000i64 {
            let k = i.to_be_bytes();
            let expected = if i % 2 == 0 { None } else { Some(k.to_vec()) };
            assert_eq!(t.get(&k), expected, "at {i}");
        }
    }

    #[test]
    fn concurrent_inserts_and_reads() {
        let t = Arc::new(BTree::new());
        let mut handles = vec![];
        for w in 0..4u64 {
            let t = t.clone();
            handles.push(thread::spawn(move || {
                for i in 0..5_000u64 {
                    let key = (w * 5_000 + i).to_be_bytes();
                    t.insert(&key, &key);
                }
            }));
        }
        // Concurrent readers exercising the optimistic path.
        for _ in 0..2 {
            let t = t.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..500 {
                    let n = t.scan_all().len();
                    assert!(n <= 20_000);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(t.len(), 20_000);
    }

    #[test]
    fn heavy_delete_merges_and_shrinks() {
        let t = BTree::new();
        for i in 0..10_000i64 {
            let k = i.to_be_bytes();
            assert!(t.insert(&k, &k));
        }
        let nodes_before = t.node_count();
        let height_before = t.height();
        assert!(nodes_before > 10, "nodes={nodes_before}");
        assert!(height_before >= 2, "height={height_before}");
        // Delete 90% spread across the key space: every leaf underflows and
        // must borrow or merge (no sparse-leaf residue).
        for i in 0..9_000i64 {
            let k = i.to_be_bytes();
            assert_eq!(t.remove(&k), Some(k.to_vec()), "at {i}");
        }
        assert_eq!(t.len(), 1_000);
        for i in 9_000..10_000i64 {
            let k = i.to_be_bytes();
            assert_eq!(t.get(&k), Some(k.to_vec()), "at {i}");
        }
        assert_eq!(t.get(&0i64.to_be_bytes()), None);
        let nodes_after = t.node_count();
        assert!(
            nodes_after < nodes_before / 2,
            "nodes {nodes_before} -> {nodes_after}"
        );
        assert!(t.height() <= height_before, "height={}", t.height());
        // Delete everything: the tree empties and stays usable.
        for i in 9_000..10_000i64 {
            let k = i.to_be_bytes();
            assert_eq!(t.remove(&k), Some(k.to_vec()), "at {i}");
        }
        assert!(t.is_empty());
        assert_eq!(t.height(), 1);
        assert_eq!(t.get(&42i64.to_be_bytes()), None);
        assert!(t.insert(&42i64.to_be_bytes(), b"again"));
        assert_eq!(t.get(&42i64.to_be_bytes()), Some(b"again".to_vec()));
    }

    #[test]
    fn delete_collapses_root() {
        let t = BTree::new();
        for i in 0..300i64 {
            let k = i.to_be_bytes();
            t.insert(&k, &k);
        }
        assert!(t.height() >= 2, "height={}", t.height());
        // Shrink to a handful of keys: merges must cascade until a single
        // leaf root remains (height 1).
        for i in 5..300i64 {
            let k = i.to_be_bytes();
            t.remove(&k);
        }
        assert_eq!(t.len(), 5);
        assert_eq!(t.height(), 1, "nodes={}", t.node_count());
        for i in 0..5i64 {
            let k = i.to_be_bytes();
            assert_eq!(t.get(&k), Some(k.to_vec()), "at {i}");
        }
        let all = t.scan_all();
        assert_eq!(all.len(), 5);
        assert_eq!(all[0].0, 0i64.to_be_bytes());
    }

    #[test]
    fn concurrent_deletes_and_reads() {
        let t = Arc::new(BTree::new());
        for i in 0..20_000u64 {
            let k = i.to_be_bytes();
            t.insert(&k, &k);
        }
        let mut handles = vec![];
        // Four deleters over disjoint quarters (merges fire throughout).
        for w in 0..4u64 {
            let t = t.clone();
            handles.push(thread::spawn(move || {
                for i in 0..5_000u64 {
                    let key = (w * 5_000 + i).to_be_bytes();
                    let expect = key.to_vec();
                    assert_eq!(t.remove(&key), Some(expect));
                }
            }));
        }
        // Optimistic readers must never observe torn state (only shrinking
        // counts — validation restarts them on any race).
        for _ in 0..2 {
            let t = t.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..500 {
                    let n = t.scan_all().len();
                    assert!(n <= 20_000);
                    let _ = t.get(&19_999u64.to_be_bytes());
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert!(t.is_empty());
        assert_eq!(t.height(), 1);
    }

    #[test]
    fn merged_nodes_retire_through_epoch() {
        let epoch = EpochManager::new();
        let t = BTree::new();
        t.set_epoch_manager(epoch.clone());
        for i in 0..5_000i64 {
            let k = i.to_be_bytes();
            t.insert(&k, &k);
        }
        // A pinned guard blocks reclamation: merges still unlink nodes, but
        // their Arcs must queue instead of dropping.
        let guard = epoch.pin();
        for i in 0..4_000i64 {
            let k = i.to_be_bytes();
            t.remove(&k);
        }
        assert_eq!(t.len(), 1_000);
        assert!(
            epoch.pending_count() > 0,
            "merges should have retired nodes"
        );
        drop(guard);
        // Unpinned: the next pump drains the queue.
        t.remove(&4_000i64.to_be_bytes());
        assert_eq!(t.len(), 999);
        assert_eq!(epoch.pending_count(), 0);
        assert_eq!(t.reclaim(), 0);
        for i in 4_001..5_000i64 {
            let k = i.to_be_bytes();
            assert_eq!(t.get(&k), Some(k.to_vec()), "at {i}");
        }
    }

    #[test]
    fn update_in_place_lifecycle_and_zero_split() {
        let t = BTree::new();
        // Insert 5,000 keys
        for i in 0..5_000u64 {
            let k = i.to_be_bytes();
            let v = (i * 10).to_be_bytes();
            t.insert(&k, &v);
        }
        let nodes_before = t.node_count();
        let height_before = t.height();

        // Update all 5,000 keys strictly in-place
        for i in 0..5_000u64 {
            let k = i.to_be_bytes();
            let old_v = (i * 10).to_be_bytes();
            let new_v = (i * 99).to_be_bytes();
            let prev = t.update_in_place(&k, &new_v);
            assert_eq!(prev, Some(old_v.to_vec()));
        }

        // Must not have split any nodes or increased height
        assert_eq!(t.node_count(), nodes_before, "in-place updates must not split nodes");
        assert_eq!(t.height(), height_before, "in-place updates must preserve height");

        // Verify updated values
        for i in 0..5_000u64 {
            let k = i.to_be_bytes();
            let new_v = (i * 99).to_be_bytes();
            assert_eq!(t.get(&k), Some(new_v.to_vec()));
        }

        // Non-existent key must return None without modifying tree
        let missing = 99_999u64.to_be_bytes();
        assert_eq!(t.update_in_place(&missing, &b"dummy"[..]), None);
        assert_eq!(t.len(), 5_000);
    }
}
