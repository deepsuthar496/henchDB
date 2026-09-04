//! HybridLatch: optimistic lock coupling over a single 64-bit word.
//!
//! Layout (matching the Optimistic Lock Coupling scheme in the research doc):
//!   bit 0    : exclusive lock bit (1 = locked)
//!   bits 1-63: monotonically increasing version counter
//!
//! Readers never write to the latch word: they take a snapshot of the version,
//! speculatively read the node payload, then re-check the version. A mismatch
//! means the read raced with a writer and must be retried. This keeps the
//! latch cacheline in Shared state across all cores on the read hot path —
//! zero MESI invalidations for pure reads.
//!
//! Writers set bit 0 via CAS; unlocking adds 1, which clears the lock bit and
//! bumps the version in a single atomic op.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct HybridLatch {
    state: AtomicU64,
}

const LOCK_BIT: u64 = 1;

impl HybridLatch {
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot the version for an optimistic read. Returns `None` while a
    /// writer holds the exclusive bit; callers spin/retry.
    #[inline]
    pub fn optimistic(&self) -> Option<u64> {
        let s = self.state.load(Ordering::Acquire);
        if s & LOCK_BIT != 0 {
            None
        } else {
            Some(s)
        }
    }

    /// Re-check that the node has not changed since `version` was taken.
    #[inline]
    pub fn validate(&self, version: u64) -> bool {
        self.state.load(Ordering::Acquire) == version
    }

    /// Spin until the exclusive bit is clear, then return the version.
    #[inline]
    pub fn wait_and_version(&self) -> u64 {
        loop {
            if let Some(v) = self.optimistic() {
                return v;
            }
            std::hint::spin_loop();
        }
    }

    /// Acquire the exclusive latch (block other writers and invalidates
    /// optimistic readers on their validation check).
    #[inline]
    pub fn lock_exclusive(&self) {
        let mut s = self.state.load(Ordering::Relaxed);
        loop {
            if s & LOCK_BIT == 0 {
                match self.state.compare_exchange_weak(
                    s,
                    s | LOCK_BIT,
                    Ordering::Acquire,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => return,
                    Err(actual) => s = actual,
                }
            } else {
                std::hint::spin_loop();
                s = self.state.load(Ordering::Relaxed);
            }
        }
    }

    /// Release the exclusive latch: clears the lock bit and increments the
    /// version counter in one atomic fetch-add, so every concurrent
    /// optimistic reader's validation fails.
    #[inline]
    pub fn unlock_exclusive(&self) {
        debug_assert_eq!(self.state.load(Ordering::Relaxed) & LOCK_BIT, LOCK_BIT);
        self.state.fetch_add(1, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn unlock_bumps_version() {
        let l = HybridLatch::new();
        let v0 = l.wait_and_version();
        l.lock_exclusive();
        l.unlock_exclusive();
        assert!(!l.validate(v0));
        assert!(l.validate(l.wait_and_version()));
    }

    #[test]
    fn exclusive_blocks_optimistic() {
        let l = HybridLatch::new();
        l.lock_exclusive();
        assert!(l.optimistic().is_none());
        l.unlock_exclusive();
        assert!(l.optimistic().is_some());
    }

    #[test]
    fn concurrent_writers_serialize() {
        let l = Arc::new(HybridLatch::new());
        let counter = Arc::new(AtomicU64::new(0));
        let mut handles = vec![];
        for _ in 0..8 {
            let (l, c) = (l.clone(), counter.clone());
            handles.push(thread::spawn(move || {
                for _ in 0..1000 {
                    l.lock_exclusive();
                    let prev = c.load(Ordering::Relaxed);
                    c.store(prev + 1, Ordering::Relaxed);
                    l.unlock_exclusive();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(counter.load(Ordering::Relaxed), 8000);
    }
}
