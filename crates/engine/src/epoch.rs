//! Epoch-Based Reclamation (EBR) primitive.
//!
//! Research blueprint reference: `research.md` §105 ("Epoch-Based Memory Reclamation").
//!
//! In an engine using Optimistic Lock Coupling (OLC), reader threads traverse B+ tree
//! nodes without taking shared locks or incrementing atomic reference counts on
//! node headers. Consequently, when a node is split, merged, or unlinked, memory cannot
//! be immediately deallocated (`free`), because a concurrent optimistic reader may still
//! be traversing the old node before re-validating the version word.
//!
//! EBR provides lock-free memory reclamation:
//! 1. The engine maintains a monotonically increasing `global_epoch`.
//! 2. When a thread initiates a read or traversal, it pins an epoch guard, setting its
//!    `local_epoch = global_epoch`.
//! 3. When a writer unlinks a node or version, it places the object into a retirement
//!    queue tagged with the current `global_epoch`.
//! 4. A retired object is safely deallocated once all active threads have advanced past
//!    the epoch in which the object was retired (`min_active_epoch > retired_epoch`).

use std::cell::RefCell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Special marker indicating that a participant thread is inactive (not in a read phase).
pub const INACTIVE_EPOCH: u64 = u64::MAX;

/// A registered thread participant in the EBR subsystem.
pub struct Participant {
    active_epoch: AtomicU64,
}

impl Participant {
    fn new() -> Self {
        Self {
            active_epoch: AtomicU64::new(INACTIVE_EPOCH),
        }
    }

    #[inline]
    pub fn is_active(&self) -> bool {
        self.active_epoch.load(Ordering::Acquire) != INACTIVE_EPOCH
    }

    #[inline]
    pub fn current_epoch(&self) -> u64 {
        self.active_epoch.load(Ordering::Acquire)
    }
}

/// An object pending reclamation once its retirement epoch has passed all active readers.
struct Retired {
    ptr: *mut (),
    drop_fn: unsafe fn(*mut ()),
    epoch: u64,
}

unsafe impl Send for Retired {}

impl Drop for Retired {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe {
                (self.drop_fn)(self.ptr);
            }
            self.ptr = std::ptr::null_mut();
        }
    }
}

/// The centralized Epoch Manager.
pub struct EpochManager {
    global_epoch: AtomicU64,
    participants: Mutex<Vec<Arc<Participant>>>,
    retired: Mutex<Vec<Retired>>,
}

thread_local! {
    static LOCAL_PARTICIPANT: RefCell<Option<Arc<Participant>>> = const { RefCell::new(None) };
}

impl EpochManager {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            global_epoch: AtomicU64::new(1),
            participants: Mutex::new(Vec::new()),
            retired: Mutex::new(Vec::new()),
        })
    }

    /// Register the current calling thread as an active participant.
    pub fn register_thread(&self) -> Arc<Participant> {
        let p = Arc::new(Participant::new());
        let mut list = self.participants.lock().unwrap();
        list.push(p.clone());
        p
    }

    /// Obtain or register the thread-local participant handle.
    pub fn local_participant(&self) -> Arc<Participant> {
        LOCAL_PARTICIPANT.with(|cell| {
            let mut opt = cell.borrow_mut();
            if let Some(p) = opt.as_ref() {
                p.clone()
            } else {
                let p = self.register_thread();
                *opt = Some(p.clone());
                p
            }
        })
    }

    /// Enter an epoch-protected read phase.
    #[inline]
    pub fn pin(&self) -> Guard {
        let p_ptr: *const Participant = LOCAL_PARTICIPANT.with(|cell| {
            let mut opt = cell.borrow_mut();
            if let Some(p) = opt.as_ref() {
                Arc::as_ptr(p)
            } else {
                let p = self.register_thread();
                let ptr = Arc::as_ptr(&p);
                *opt = Some(p);
                ptr
            }
        });
        let e = self.global_epoch.load(Ordering::Acquire);
        unsafe {
            (*p_ptr).active_epoch.store(e, Ordering::Release);
        }
        Guard {
            participant: p_ptr,
        }
    }

    /// Retire a heap-allocated object. It will be safely dropped once all active
    /// threads have advanced past the current epoch.
    pub fn retire<T: 'static + Send>(&self, val: T) {
        let ptr = Box::into_raw(Box::new(val)) as *mut ();
        unsafe fn dropper<T>(p: *mut ()) {
            drop(Box::from_raw(p as *mut T));
        }
        let epoch = self.global_epoch.load(Ordering::Acquire);
        let mut queue = self.retired.lock().unwrap();
        queue.push(Retired {
            ptr,
            drop_fn: dropper::<T>,
            epoch,
        });
    }

    /// Number of objects currently awaiting reclamation.
    pub fn pending_count(&self) -> usize {
        self.retired.lock().unwrap().len()
    }

    /// Current global epoch.
    pub fn current_epoch(&self) -> u64 {
        self.global_epoch.load(Ordering::Acquire)
    }

    /// Attempt to advance the global epoch and reclaim retired objects.
    pub fn try_reclaim(&self) -> usize {
        // 1. Determine the oldest active epoch among all active threads.
        let mut participants = self.participants.lock().unwrap();
        participants.retain(|p| Arc::strong_count(p) > 1);

        let current_global = self.global_epoch.load(Ordering::Acquire);
        let mut min_active = None;
        let mut all_caught_up = true;

        for p in participants.iter() {
            let e = p.active_epoch.load(Ordering::Acquire);
            if e != INACTIVE_EPOCH {
                min_active = Some(min_active.map_or(e, |m: u64| m.min(e)));
                if e < current_global {
                    all_caught_up = false;
                }
            }
        }

        // If all active threads are in the current epoch, advance global epoch.
        let safe_epoch = if all_caught_up {
            self.global_epoch.fetch_add(1, Ordering::AcqRel) + 1
        } else {
            current_global
        };

        let horizon = min_active.unwrap_or(safe_epoch);

        // 2. Reclaim objects whose retirement epoch is strictly less than horizon.
        let mut queue = self.retired.lock().unwrap();
        let initial_len = queue.len();
        queue.retain(|item| item.epoch >= horizon);
        initial_len - queue.len()
    }
}

/// RAII Guard for an active epoch critical section.
pub struct Guard {
    participant: *const Participant,
}

impl Drop for Guard {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            (*self.participant)
                .active_epoch
                .store(INACTIVE_EPOCH, Ordering::Release);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;

    struct Droppable(Arc<AtomicBool>);
    impl Drop for Droppable {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[test]
    fn ebr_basic_lifecycle() {
        let ebr = EpochManager::new();
        let dropped = Arc::new(AtomicBool::new(false));

        // Retire an object while no guards are active
        ebr.retire(Droppable(dropped.clone()));
        assert!(!dropped.load(Ordering::SeqCst));

        // Advancing when no guards active reclaims immediately
        let reclaimed = ebr.try_reclaim();
        assert!(reclaimed >= 1);
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[test]
    fn ebr_protects_active_readers() {
        let ebr = EpochManager::new();
        let dropped = Arc::new(AtomicBool::new(false));

        let guard = ebr.pin();
        ebr.retire(Droppable(dropped.clone()));

        // Active guard protects the object
        ebr.try_reclaim();
        assert!(!dropped.load(Ordering::SeqCst));

        // Dropping guard unblocks reclamation
        drop(guard);
        ebr.try_reclaim();
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[test]
    fn ebr_multithreaded_reclamation() {
        let ebr = EpochManager::new();
        let dropped = Arc::new(AtomicBool::new(false));

        let ebr_clone = ebr.clone();
        let (tx, rx) = std::sync::mpsc::channel();

        let handle = std::thread::spawn(move || {
            let guard = ebr_clone.pin();
            tx.send(()).unwrap();
            std::thread::sleep(std::time::Duration::from_millis(50));
            drop(guard);
        });

        rx.recv().unwrap();
        ebr.retire(Droppable(dropped.clone()));
        ebr.try_reclaim();
        // Still held by worker thread
        assert!(!dropped.load(Ordering::SeqCst));

        handle.join().unwrap();
        // Worker exited, can reclaim now
        ebr.try_reclaim();
        assert!(dropped.load(Ordering::SeqCst));
    }
}
