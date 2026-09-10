//! Deterministic failpoint and fault-injection framework (P0.2).
//!
//! Allows tests and automated CI to trigger deterministic failures at critical
//! durability, replication, and recovery boundaries.
//!
//! Configurable programmatically via [`set`] or via environment variables:
//! - `HENCHDB_FAILPOINT`: Name of the active failpoint (e.g., `"before_wal_sync"`)
//! - `HENCHDB_FAILPOINT_MODE`: `"always"`, `"once"`, `"nth"` (default `"always"`)
//! - `HENCHDB_FAILPOINT_N`: Trigger count for `"nth"` mode (default `1`)
//! - `HENCHDB_FAILPOINT_ACTION`: `"panic"` (default) or `"error"`

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use crate::error::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailMode {
    Always,
    Once,
    Nth(usize),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailAction {
    Panic(&'static str),
    Error(String),
}

struct FailpointEntry {
    mode: FailMode,
    action: FailAction,
    hits: AtomicUsize,
}

thread_local! {
    static LOCAL_REGISTRY: RefCell<HashMap<String, Arc<FailpointEntry>>> = RefCell::new(HashMap::new());
}

static GLOBAL_REGISTRY: RwLock<Option<HashMap<String, Arc<FailpointEntry>>>> = RwLock::new(None);

/// Set a thread-local failpoint (isolated from other concurrent test threads).
pub fn set(name: &str, mode: FailMode, action: FailAction) {
    let entry = Arc::new(FailpointEntry {
        mode,
        action,
        hits: AtomicUsize::new(0),
    });
    LOCAL_REGISTRY.with(|reg| {
        reg.borrow_mut().insert(name.to_string(), entry);
    });
}

/// Set a global failpoint across all threads.
pub fn set_global(name: &str, mode: FailMode, action: FailAction) {
    let mut guard = GLOBAL_REGISTRY.write().unwrap();
    let map = guard.get_or_insert_with(HashMap::new);
    map.insert(
        name.to_string(),
        Arc::new(FailpointEntry {
            mode,
            action,
            hits: AtomicUsize::new(0),
        }),
    );
}

/// Clear all thread-local failpoints.
pub fn clear() {
    LOCAL_REGISTRY.with(|reg| {
        reg.borrow_mut().clear();
    });
}

/// Clear a specific thread-local failpoint by name.
pub fn clear_point(name: &str) {
    LOCAL_REGISTRY.with(|reg| {
        reg.borrow_mut().remove(name);
    });
}

/// Evaluate a failpoint by name.
///
/// Returns `Ok(())` if the failpoint is not set or did not trigger.
/// If triggered, it either returns an `Err(Error::ExecutionError)` or panics
/// according to the configured action.
pub fn eval(name: &str) -> Result<()> {
    // 1. Check thread-local registry (isolated for test runners)
    let local_entry = LOCAL_REGISTRY.with(|reg| {
        reg.borrow().get(name).cloned()
    });

    if let Some(entry) = local_entry {
        return fire_entry(name, &entry);
    }

    // 2. Check global registry
    let global_entry = {
        let guard = GLOBAL_REGISTRY.read().unwrap();
        guard.as_ref().and_then(|m| m.get(name).cloned())
    };

    if let Some(entry) = global_entry {
        return fire_entry(name, &entry);
    }

    // 3. Check environment variables
    if let Ok(target) = std::env::var("HENCHDB_FAILPOINT") {
        if target == name {
            let mode_str = std::env::var("HENCHDB_FAILPOINT_MODE").unwrap_or_else(|_| "always".into());
            let n: usize = std::env::var("HENCHDB_FAILPOINT_N")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(1);
            let action_str = std::env::var("HENCHDB_FAILPOINT_ACTION").unwrap_or_else(|_| "panic".into());

            let mode = match mode_str.to_ascii_lowercase().as_str() {
                "once" => FailMode::Once,
                "nth" => FailMode::Nth(n),
                _ => FailMode::Always,
            };
            let action = if action_str.eq_ignore_ascii_case("error") {
                FailAction::Error(format!("env failpoint triggered for {}", name))
            } else {
                FailAction::Panic("env failpoint panic triggered")
            };
            set(name, mode, action);
            return eval(name);
        }
    }

    Ok(())
}

fn fire_entry(name: &str, entry: &FailpointEntry) -> Result<()> {
    let count = entry.hits.fetch_add(1, Ordering::SeqCst) + 1;
    let triggers = match entry.mode {
        FailMode::Always => true,
        FailMode::Once => count == 1,
        FailMode::Nth(n) => count == n,
    };

    if triggers {
        match &entry.action {
            FailAction::Panic(msg) => {
                panic!("failpoint '{}' triggered: {}", name, msg);
            }
            FailAction::Error(msg) => {
                return Err(Error::ExecutionError(format!(
                    "failpoint '{}' triggered: {}",
                    name, msg
                )));
            }
        }
    }
    Ok(())
}

/// Macro to evaluate a failpoint cleanly inline with `?`.
#[macro_export]
macro_rules! failpoint {
    ($name:expr) => {
        $crate::failpoint::eval($name)?
    };
}
