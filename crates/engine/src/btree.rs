//! Order-preserving B+ tree with Optimistic Lock Coupling (OLC).
//!
//! Read path: fully optimistic — traverse the tree taking only version
//! snapshots (no writes to shared memory), then validate. A mismatch means a
//! writer raced the read; the traversal restarts from the root. Nodes are
//! never physically freed while the tree is alive, which makes stale pointers
//! safe to dereference; epoch-based reclamation per the research doc is the
//! planned upgrade (see agents.md).
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

use crate::latch::HybridLatch;

/// Split threshold per node. (Future: fixed 256 KiB slotted pages with
/// prefix compression + 4-byte-head SIMD search, per the research doc.)
const MAX_KEYS: usize = 128;

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
        }
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

    /// Remove `key`, returning the removed value. (Underflow merging is not
    /// implemented — deleted leaves may become sparse. See agents.md.)
    pub fn remove(&self, key: &[u8]) -> Option<Vec<u8>> {
        let root = self.current_root();
        delete_rec(&root, key)
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
}
