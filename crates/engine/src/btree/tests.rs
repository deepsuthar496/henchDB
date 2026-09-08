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
        // Unpinned: retirements from the delete loop are reclaimable. (The
        // final op's own pin may hold that op's retirements back, so pump
        // once unpinned to drain everything.)
        t.remove(&4_000i64.to_be_bytes());
        assert_eq!(t.len(), 999);
        t.reclaim();
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

    /// Mixed hammer: writers cycle insert/upsert/update_in_place/remove over
    /// a shared keyspace (splitting, swapping bodies, borrowing, merging)
    /// while readers run point lookups and full scans. Bodies are immutable
    /// snapshots, so no reader can fault on a reallocated Vec; completion
    /// without panic plus exact post-join state proves it.
    #[test]
    fn cow_mixed_read_write_hammer() {
        let t = Arc::new(BTree::new());
        for i in 0..4_000u64 {
            let k = i.to_be_bytes();
            t.insert(&k, &k);
        }
        let mut handles = vec![];
        // Writers: disjoint quarters, churn values then delete odds.
        for w in 0..4u64 {
            let t = t.clone();
            handles.push(thread::spawn(move || {
                for i in 0..1_000u64 {
                    let k = (w * 1_000 + i).to_be_bytes();
                    let v = (i * 7 + w).to_be_bytes();
                    t.upsert(&k, &v);
                    assert!(t.update_in_place(&k, &v).is_some());
                    if i % 2 == 1 {
                        assert_eq!(t.remove(&k), Some(v.to_vec()));
                    }
                }
            }));
        }
        // Readers: hot point reads + scans + ranges through all the churn.
        for _ in 0..4 {
            let t = t.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..400 {
                    let n = t.scan_all().len();
                    assert!(n <= 4_000);
                    let _ = t.get(&1_234u64.to_be_bytes());
                    let _ = t.range(None, true, None, true).len();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        // Even keys survive with their last writer's value; odds are gone.
        assert_eq!(t.len(), 2_000);
        for w in 0..4u64 {
            for i in 0..1_000u64 {
                let k = (w * 1_000 + i).to_be_bytes();
                if i % 2 == 0 {
                    let v = (i * 7 + w).to_be_bytes();
                    assert_eq!(t.get(&k), Some(v.to_vec()), "at {k:?}");
                } else {
                    assert_eq!(t.get(&k), None, "at {k:?}");
                }
            }
        }
    }

    /// Focused on the old fault: same-length `update_in_place` used to scribble
    /// into a shared `Vec` while optimistic readers walked it. Hammer exactly
    /// that shape — many threads rewriting the same keys' values in place
    /// while others read — and require zero faults plus value coherence
    /// (every observed value is a fully-written generation, never garbage).
    #[test]
    fn cow_in_place_rewrite_vs_hot_reads() {
        let t = Arc::new(BTree::new());
        for i in 0..500u64 {
            let k = i.to_be_bytes();
            t.insert(&k, &[0u8; 8]);
        }
        let mut handles = vec![];
        for w in 0..4u64 {
            let t = t.clone();
            handles.push(thread::spawn(move || {
                for gen in 1..=200u64 {
                    for i in 0..500u64 {
                        let k = i.to_be_bytes();
                        // Same-length values: old code took the raw
                        // `copy_from_slice` path on the shared Vec.
                        let vv = [((gen + w) & 0xff) as u8; 8];
                        assert!(t.update_in_place(&k, &vv).is_some());
                    }
                }
            }));
        }
        for _ in 0..4 {
            let t = t.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..2_000 {
                    for i in (0..500u64).step_by(50) {
                        let got = t.get(&i.to_be_bytes()).expect("present");
                        // 8 uniform bytes from some generation: any torn
                        // snapshot would show mixed bytes.
                        assert_eq!(got.len(), 8);
                        assert!(got.iter().all(|&b| b == got[0]), "torn: {got:?}");
                    }
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(t.len(), 500);
    }

    /// Retired bodies must drain: after churn with an attached manager, a
    /// reclaim pump empties the quarantine (no unbounded growth), and the
    /// tree stays exact.
    #[test]
    fn cow_retired_bodies_reclaim() {
        let epoch = EpochManager::new();
        let t = BTree::new();
        t.set_epoch_manager(epoch.clone());
        for i in 0..2_000u64 {
            let k = i.to_be_bytes();
            t.insert(&k, &k);
        }
        for i in 0..2_000u64 {
            let k = i.to_be_bytes();
            let v = (i + 1).to_be_bytes();
            t.upsert(&k, &v);
        }
        assert!(epoch.pending_count() > 0, "writes must quarantine bodies");
        // No pins held: repeated pumps must fully drain.
        for _ in 0..8 {
            t.reclaim();
        }
        assert_eq!(epoch.pending_count(), 0);
        for i in 0..2_000u64 {
            let k = i.to_be_bytes();
            let v = (i + 1).to_be_bytes();
            assert_eq!(t.get(&k), Some(v.to_vec()), "at {i}");
        }
    }
