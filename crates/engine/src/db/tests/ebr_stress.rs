//! Long-running, reproducible Epoch-Based Reclamation (EBR) and B+ tree
//! concurrency stress tests (Assessment §2.2, §2.3, §2.5).
//!
//! Reproducibility:
//! Run with `HENCHDB_STRESS_SEED=<u64>` to reproduce any randomized run exactly.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::btree::BTree;
use crate::epoch::{EbrStats, EpochManager};
use crate::types::{encode_key, Datum};

/// Minimal fast deterministic PRNG (xorshift64*) for reproducible stress tests.
pub(crate) struct StressPrng {
    state: u64,
}

impl StressPrng {
    pub(crate) fn new(seed: u64) -> Self {
        Self {
            state: if seed == 0 { 0x853c49e6748fea9b } else { seed },
        }
    }

    pub(crate) fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.state = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }

    pub(crate) fn gen_range(&mut self, low: u64, high: u64) -> u64 {
        if low >= high {
            return low;
        }
        low + (self.next_u64() % (high - low))
    }
}

fn stress_seed() -> u64 {
    match std::env::var("HENCHDB_STRESS_SEED") {
        Ok(val) => val.parse::<u64>().unwrap_or(0xDEAD_BEEF_CAFE_BABE),
        Err(_) => 0xDEAD_BEEF_CAFE_BABE,
    }
}

#[test]
fn ebr_stress_tree_splits_merges_and_root_collapses() {
    let seed = stress_seed();
    println!("Running ebr_stress_tree_splits_merges_and_root_collapses with HENCHDB_STRESS_SEED={seed}");

    let tree = Arc::new(BTree::new());
    let ebr = EpochManager::new();
    tree.set_epoch_manager(ebr.clone());

    let stop = Arc::new(AtomicBool::new(false));
    let read_ops = Arc::new(AtomicUsize::new(0));

    // Spawn 3 concurrent reader threads scanning and looking up while tree mutates
    let mut readers = Vec::new();
    for r_idx in 0..3 {
        let tree_c = tree.clone();
        let ebr_c = ebr.clone();
        let stop_c = stop.clone();
        let read_ops_c = read_ops.clone();
        let r_seed = seed.wrapping_add(r_idx as u64 + 1);

        readers.push(std::thread::spawn(move || {
            let mut prng = StressPrng::new(r_seed);
            while !stop_c.load(Ordering::Relaxed) {
                let _guard = ebr_c.pin();
                let op = prng.next_u64() % 3;
                if op == 0 {
                    // Point lookup
                    let k = prng.gen_range(1, 1000);
                    let kb = encode_key(&Datum::Int(k as i64)).unwrap();
                    let _ = tree_c.get(&kb);
                } else {
                    // Range scan
                    let start = prng.gen_range(1, 800);
                    let end = start + prng.gen_range(5, 50);
                    let sb = encode_key(&Datum::Int(start as i64)).unwrap();
                    let eb = encode_key(&Datum::Int(end as i64)).unwrap();
                    let rows = tree_c.range(Some(&sb), true, Some(&eb), true);
                    // Ordering invariant: scanned keys must remain monotonically increasing
                    let mut prev: Option<Vec<u8>> = None;
                    for (k, _) in rows {
                        if let Some(ref p) = prev {
                            if &k <= p {
                                eprintln!("ORDERING VIOLATION: prev={:?}, curr={:?}", p, k);
                                panic!("OLC Range scan ordering violation during tree churn");
                            }
                        }
                        prev = Some(k);
                    }
                }
                read_ops_c.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    // Main writer thread: rapid inserts, splits, deletes, merges, root collapses, re-inserts
    let mut prng = StressPrng::new(seed);
    let n_cycles = 5;
    for _ in 0..n_cycles {
        // 1. Fill tree to force multiple levels of splits (MAX_KEYS=128)
        for i in 1..=600 {
            let val = prng.next_u64();
            let k = encode_key(&Datum::Int(i)).unwrap();
            let v = val.to_le_bytes().to_vec();
            tree.upsert(&k, &v);
        }

        // 2. Delete alternating ranges to trigger leaf borrows, merges, and root collapse
        for i in (1..=600).step_by(2) {
            let k = encode_key(&Datum::Int(i)).unwrap();
            tree.remove(&k);
        }

        // 3. Re-insert different values
        for i in (1..=600).step_by(2) {
            let val = prng.next_u64();
            let k = encode_key(&Datum::Int(i)).unwrap();
            let v = val.to_le_bytes().to_vec();
            tree.upsert(&k, &v);
        }

        // 4. Try reclaim while readers are still executing
        ebr.try_reclaim();
    }

    stop.store(true, Ordering::Relaxed);
    for r in readers {
        r.join().unwrap();
    }

    assert!(read_ops.load(Ordering::Relaxed) > 100);

    // Drain all retired nodes and verify zero pending reclamation leaks
    for _ in 0..10 {
        ebr.try_reclaim();
    }

    let stats: EbrStats = ebr.stats();
    assert_eq!(stats.active_guards, 0, "No guards should remain active");
    assert_eq!(stats.pending_reclamation, 0, "Zero memory leaks in EBR retirement queue");
    assert!(stats.reclaimed_total > 0, "Tree splits and merges must retire and reclaim nodes");
}

#[test]
fn ebr_stress_nested_guards_rapid_churn_and_reclamation() {
    let seed = stress_seed();
    let ebr = EpochManager::new();
    let reclaimed_count = Arc::new(AtomicUsize::new(0));

    struct Canary(Arc<AtomicUsize>);
    impl Drop for Canary {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let num_threads = 8;
    let ops_per_thread = 500;
    let mut handles = Vec::new();

    for t_idx in 0..num_threads {
        let ebr_c = ebr.clone();
        let rc_c = reclaimed_count.clone();
        let t_seed = seed.wrapping_add((t_idx + 10) as u64);

        handles.push(std::thread::spawn(move || {
            let mut prng = StressPrng::new(t_seed);
            for i in 0..ops_per_thread {
                let g1 = ebr_c.pin();
                if i % 3 == 0 {
                    ebr_c.retire(Canary(rc_c.clone()));
                }
                if i % 5 == 0 {
                    // Nested guard: proves inner guard does not prematurely unpin outer epoch
                    let g2 = ebr_c.pin();
                    if i % 7 == 0 {
                        ebr_c.retire(Canary(rc_c.clone()));
                    }
                    drop(g2);
                }
                if prng.next_u64() % 20 == 0 {
                    ebr_c.try_reclaim();
                }
                drop(g1);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // Drain reclamation
    for _ in 0..15 {
        ebr.try_reclaim();
    }

    let stats = ebr.stats();
    assert_eq!(stats.active_guards, 0);
    assert_eq!(stats.pending_reclamation, 0);
    assert_eq!(stats.retired_total, stats.reclaimed_total);
    assert_eq!(reclaimed_count.load(Ordering::SeqCst) as u64, stats.reclaimed_total);
}

#[test]
fn ebr_stress_long_lived_reader_soak() {
    let ebr = EpochManager::new();
    let reclaimed_count = Arc::new(AtomicUsize::new(0));

    struct Canary(Arc<AtomicUsize>);
    impl Drop for Canary {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let (tx_start, rx_start) = std::sync::mpsc::channel();
    let (tx_done, rx_done) = std::sync::mpsc::channel();

    // Reader holds an active pin
    let ebr_r = ebr.clone();
    let reader_handle = std::thread::spawn(move || {
        let _guard = ebr_r.pin();
        tx_start.send(()).unwrap();
        // Hold epoch pin across many writer retirement waves
        rx_done.recv().unwrap();
    });

    rx_start.recv().unwrap();

    // Writers retire 500 items while the reader is actively pinned
    for _ in 0..500 {
        ebr.retire(Canary(reclaimed_count.clone()));
    }

    // Active reader prevents reclamation
    ebr.try_reclaim();
    let stats_mid = ebr.stats();
    assert_eq!(stats_mid.active_guards, 1);
    assert_eq!(stats_mid.reclaimed_total, 0, "No object should be reclaimed while long reader is pinned");
    assert_eq!(stats_mid.pending_reclamation, 500);

    // Release the long reader
    tx_done.send(()).unwrap();
    reader_handle.join().unwrap();

    // Now all 500 items must be cleanly reclaimed
    for _ in 0..10 {
        ebr.try_reclaim();
    }

    let stats_end = ebr.stats();
    assert_eq!(stats_end.active_guards, 0);
    assert_eq!(stats_end.pending_reclamation, 0);
    assert_eq!(stats_end.reclaimed_total, 500);
    assert_eq!(reclaimed_count.load(Ordering::SeqCst) as u64, 500);
}

#[test]
fn ebr_stress_dynamic_thread_creation_and_termination() {
    let seed = stress_seed();
    let tree = Arc::new(BTree::new());
    let ebr = EpochManager::new();
    tree.set_epoch_manager(ebr.clone());

    let reclaimed_count = Arc::new(AtomicUsize::new(0));
    struct NodeCanary(Arc<AtomicUsize>);
    impl Drop for NodeCanary {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    // Populate initial tree
    for i in 1..=200 {
        let k = encode_key(&Datum::Int(i)).unwrap();
        let v = (i * 10).to_le_bytes().to_vec();
        tree.upsert(&k, &v);
    }

    let stop_churn = Arc::new(AtomicBool::new(false));
    let tree_w = tree.clone();
    let ebr_w = ebr.clone();
    let stop_w = stop_churn.clone();
    let rc_w = reclaimed_count.clone();

    // Background mutator thread constantly modifying tree and retiring objects
    let mutator = std::thread::spawn(move || {
        let mut prng = StressPrng::new(seed.wrapping_add(999));
        let mut step = 0u64;
        while !stop_w.load(Ordering::Relaxed) {
            let k_val = prng.gen_range(1, 200) as i64;
            let k = encode_key(&Datum::Int(k_val)).unwrap();
            if step % 2 == 0 {
                let v = (k_val * 100).to_le_bytes().to_vec();
                tree_w.upsert(&k, &v);
            } else {
                tree_w.remove(&k);
            }
            if step % 10 == 0 {
                ebr_w.retire(NodeCanary(rc_w.clone()));
            }
            step += 1;
            std::thread::yield_now();
        }
    });

    // Spawn 5 sequential waves of 8 short-lived worker threads
    let waves = 5;
    let threads_per_wave = 8;
    for wave in 0..waves {
        let mut handles = Vec::new();
        for t_idx in 0..threads_per_wave {
            let tree_r = tree.clone();
            let ebr_r = ebr.clone();
            let t_seed = seed.wrapping_add((wave * 100 + t_idx) as u64);

            handles.push(std::thread::spawn(move || {
                let mut prng = StressPrng::new(t_seed);
                for _ in 0..50 {
                    let _g = ebr_r.pin();
                    let k_val = prng.gen_range(1, 200) as i64;
                    let k = encode_key(&Datum::Int(k_val)).unwrap();
                    let _ = tree_r.get(&k);
                    if prng.next_u64() % 15 == 0 {
                        ebr_r.try_reclaim();
                    }
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Between waves, trigger reclamation: terminated thread participants must be pruned
        ebr.try_reclaim();
    }

    stop_churn.store(true, Ordering::Relaxed);
    mutator.join().unwrap();

    // Final drain of EBR
    for _ in 0..15 {
        ebr.try_reclaim();
    }

    let stats = ebr.stats();
    assert_eq!(stats.active_guards, 0, "All guards should be dropped");
    assert_eq!(stats.pending_reclamation, 0, "No retired objects leaked");
    // Only the main thread's participant should remain registered (if pinned) or pruned
    assert!(
        stats.participants <= 2,
        "Terminated thread participants must be automatically pruned from registry, found {}",
        stats.participants
    );
}

