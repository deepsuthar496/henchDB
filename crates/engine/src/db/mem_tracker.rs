//! Query Memory Accounting & Tracking (Assessment §9).
//!
//! Provides strict query memory accounting with overflow-safe arithmetic,
//! tracking intermediate allocations (hash tables, join buffers, sort buffers,
//! aggregation buckets, subquery materializations) against configured session limits.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::error::{Error, Result};

/// Thread-safe, atomic memory tracker for single queries or session scopes.
#[derive(Debug, Clone)]
pub struct QueryMemoryTracker {
    limit: Arc<AtomicUsize>,
    has_limit: Arc<std::sync::atomic::AtomicBool>,
    current_bytes: Arc<AtomicUsize>,
    peak_bytes: Arc<AtomicUsize>,
}

impl Default for QueryMemoryTracker {
    fn default() -> Self {
        Self::new(None)
    }
}

impl QueryMemoryTracker {
    pub fn new(limit: Option<usize>) -> Self {
        let (has, lim) = match limit {
            Some(l) => (true, l),
            None => (false, 0),
        };
        QueryMemoryTracker {
            limit: Arc::new(AtomicUsize::new(lim)),
            has_limit: Arc::new(std::sync::atomic::AtomicBool::new(has)),
            current_bytes: Arc::new(AtomicUsize::new(0)),
            peak_bytes: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Update the memory limit dynamically.
    pub fn set_limit(&self, limit: Option<usize>) {
        match limit {
            Some(l) => {
                self.limit.store(l, Ordering::SeqCst);
                self.has_limit.store(true, Ordering::SeqCst);
            }
            None => {
                self.limit.store(0, Ordering::SeqCst);
                self.has_limit.store(false, Ordering::SeqCst);
            }
        }
    }

    pub fn limit(&self) -> Option<usize> {
        if self.has_limit.load(Ordering::Relaxed) {
            Some(self.limit.load(Ordering::Relaxed))
        } else {
            None
        }
    }

    pub fn current_bytes(&self) -> usize {
        self.current_bytes.load(Ordering::Relaxed)
    }

    pub fn peak_bytes(&self) -> usize {
        self.peak_bytes.load(Ordering::Relaxed)
    }

    /// Attempt to reserve `bytes` of memory.
    pub fn reserve(&self, bytes: usize) -> Result<()> {
        self.reserve_context(bytes, "query execution")
    }

    /// Attempt to reserve `bytes` with a specific context description.
    pub fn reserve_context(&self, bytes: usize, context: &str) -> Result<()> {
        if bytes == 0 {
            return Ok(());
        }
        let has_lim = self.has_limit.load(Ordering::Relaxed);
        let lim = self.limit.load(Ordering::Relaxed);

        let mut curr = self.current_bytes.load(Ordering::Relaxed);
        loop {
            let next = curr.saturating_add(bytes);
            if has_lim && next > lim {
                return Err(Error::ExecutionError(format!(
                    "query exceeded max_intermediate_bytes limit ({lim}) during {context} (estimated {bytes} bytes)"
                )));
            }
            match self.current_bytes.compare_exchange_weak(
                curr,
                next,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    // Update peak atomically
                    let mut peak = self.peak_bytes.load(Ordering::Relaxed);
                    while next > peak {
                        match self.peak_bytes.compare_exchange_weak(
                            peak,
                            next,
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        ) {
                            Ok(_) => break,
                            Err(actual) => peak = actual,
                        }
                    }
                    return Ok(());
                }
                Err(actual) => curr = actual,
            }
        }
    }

    /// Release `bytes` previously reserved.
    pub fn release(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let mut curr = self.current_bytes.load(Ordering::Relaxed);
        loop {
            let next = curr.saturating_sub(bytes);
            match self.current_bytes.compare_exchange_weak(
                curr,
                next,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(actual) => curr = actual,
            }
        }
    }

    /// Reset current usage to zero (at statement boundaries).
    pub fn reset(&self) {
        self.current_bytes.store(0, Ordering::SeqCst);
    }
}

/// RAII reservation guard that releases memory upon drop.
pub struct MemoryReservation {
    tracker: QueryMemoryTracker,
    reserved: usize,
}

impl MemoryReservation {
    pub fn new(tracker: &QueryMemoryTracker, bytes: usize, context: &str) -> Result<Self> {
        tracker.reserve_context(bytes, context)?;
        Ok(MemoryReservation {
            tracker: tracker.clone(),
            reserved: bytes,
        })
    }

    pub fn increase(&mut self, additional: usize, context: &str) -> Result<()> {
        self.tracker.reserve_context(additional, context)?;
        self.reserved = self.reserved.saturating_add(additional);
        Ok(())
    }

    pub fn reserved_bytes(&self) -> usize {
        self.reserved
    }
}

impl Drop for MemoryReservation {
    fn drop(&mut self) {
        self.tracker.release(self.reserved);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_memory_tracker_lifecycle() {
        let tracker = QueryMemoryTracker::new(Some(1000));
        assert_eq!(tracker.limit(), Some(1000));
        assert_eq!(tracker.current_bytes(), 0);
        assert_eq!(tracker.peak_bytes(), 0);

        tracker.reserve(400).unwrap();
        assert_eq!(tracker.current_bytes(), 400);
        assert_eq!(tracker.peak_bytes(), 400);

        tracker.reserve(500).unwrap();
        assert_eq!(tracker.current_bytes(), 900);
        assert_eq!(tracker.peak_bytes(), 900);

        // Exceed limit
        let err = tracker.reserve(200).unwrap_err();
        assert!(err.to_string().contains("exceeded max_intermediate_bytes limit"));
        assert_eq!(tracker.current_bytes(), 900);

        tracker.release(500);
        assert_eq!(tracker.current_bytes(), 400);
        assert_eq!(tracker.peak_bytes(), 900); // peak preserved

        // Reserve again now that space is available
        tracker.reserve(300).unwrap();
        assert_eq!(tracker.current_bytes(), 700);

        tracker.reset();
        assert_eq!(tracker.current_bytes(), 0);
        assert_eq!(tracker.peak_bytes(), 900);
    }

    #[test]
    fn query_memory_tracker_raii_guard() {
        let tracker = QueryMemoryTracker::new(Some(500));
        {
            let mut guard = MemoryReservation::new(&tracker, 200, "sort").unwrap();
            assert_eq!(tracker.current_bytes(), 200);
            guard.increase(100, "sort").unwrap();
            assert_eq!(tracker.current_bytes(), 300);
        }
        // Guard dropped
        assert_eq!(tracker.current_bytes(), 0);
        assert_eq!(tracker.peak_bytes(), 300);
    }

    #[test]
    fn query_memory_tracker_overflow_safe() {
        let tracker = QueryMemoryTracker::new(Some(100));
        // Test saturating arithmetic with usize::MAX
        let err = tracker.reserve(usize::MAX).unwrap_err();
        assert!(err.to_string().contains("exceeded max_intermediate_bytes limit"));
        assert_eq!(tracker.current_bytes(), 0);
    }
}
