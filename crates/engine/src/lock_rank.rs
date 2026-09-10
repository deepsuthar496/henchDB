//! Mechanical runtime lock-ordering verification for deadlock prevention.
//!
//! Enforces the global acquisition hierarchy documented in AGENTS.md §6b:
//! CommitLock (1) -> InstallFrontier (2) -> WalStage (3) -> WalFlush (4)
//! -> BTree (5) -> BufferPool (6) -> VersionState (7) -> Catalog/Auth (8) -> Ebr (9).

use std::cell::RefCell;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum LockRank {
    CommitLock = 1,
    InstallFrontier = 2,
    WalStage = 3,
    WalFlush = 4,
    BTree = 5,
    BufferPool = 6,
    VersionState = 7,
    CatalogAuth = 8,
    Ebr = 9,
}

thread_local! {
    static HELD_RANKS: RefCell<Vec<LockRank>> = const { RefCell::new(Vec::new()) };
}

pub struct LockRankGuard {
    rank: LockRank,
}

impl LockRankGuard {
    #[inline]
    pub fn acquire(rank: LockRank) -> Self {
        HELD_RANKS.with(|held| {
            let mut h = held.borrow_mut();
            if let Some(&last) = h.last() {
                assert!(
                    last <= rank,
                    "Lock ordering violation! Attempted to acquire rank {:?} while holding higher-ranked {:?}",
                    rank, last
                );
            }
            h.push(rank);
        });
        LockRankGuard { rank }
    }

    #[inline]
    pub fn is_held(rank: LockRank) -> bool {
        HELD_RANKS.with(|held| held.borrow().contains(&rank))
    }

    #[inline]
    pub fn max_held() -> Option<LockRank> {
        HELD_RANKS.with(|held| held.borrow().last().copied())
    }
}

impl Drop for LockRankGuard {
    #[inline]
    fn drop(&mut self) {
        HELD_RANKS.with(|held| {
            let mut h = held.borrow_mut();
            if let Some(pos) = h.iter().rposition(|&r| r == self.rank) {
                h.remove(pos);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_lock_ordering_succeeds() {
        let _g1 = LockRankGuard::acquire(LockRank::CommitLock);
        assert_eq!(LockRankGuard::max_held(), Some(LockRank::CommitLock));

        let _g2 = LockRankGuard::acquire(LockRank::WalStage);
        assert_eq!(LockRankGuard::max_held(), Some(LockRank::WalStage));

        let _g3 = LockRankGuard::acquire(LockRank::BTree);
        assert_eq!(LockRankGuard::max_held(), Some(LockRank::BTree));

        let _g4 = LockRankGuard::acquire(LockRank::VersionState);
        assert_eq!(LockRankGuard::max_held(), Some(LockRank::VersionState));

        let _g5 = LockRankGuard::acquire(LockRank::Ebr);
        assert_eq!(LockRankGuard::max_held(), Some(LockRank::Ebr));
    }

    #[test]
    #[should_panic(expected = "Lock ordering violation")]
    fn lock_ordering_violation_panics() {
        let _g1 = LockRankGuard::acquire(LockRank::BTree);
        // Attempting to acquire a higher-ranked lock (lower numeric value) while holding BTree panics!
        let _g2 = LockRankGuard::acquire(LockRank::CommitLock);
    }
}
