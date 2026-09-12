//! Exclusive process-level directory lock (`server.lock`) and tmp sweep.
//!
//! Prevents concurrent embedded processes from opening an active directory
//! being served by another live server process, preventing split-brain
//! file corruption and snapshot races.

use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use crate::error::{Error, Result};

pub struct DirLock {
    #[allow(dead_code)]
    file: File,
    #[allow(dead_code)]
    path: PathBuf,
}

#[cfg(windows)]
pub fn acquire_dir_lock(dir: &Path) -> Result<DirLock> {
    use std::os::windows::fs::OpenOptionsExt;
    let lock_path = dir.join("server.lock");
    // share_mode(0): Exclusive lock at the OS kernel level. No sharing for read/write/delete.
    // If another live process holds this handle, Windows returns ERROR_SHARING_VIOLATION (code 32).
    match OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .share_mode(0)
        .open(&lock_path)
    {
        Ok(file) => Ok(DirLock { file, path: lock_path }),
        Err(e) if e.raw_os_error() == Some(32) => {
            Err(Error::InvalidOperation(
                "directory is locked by a live server process; use wire connection (e.g. wire CHECKPOINT)".into(),
            ))
        }
        Err(e) => Err(Error::Io(format!("cannot acquire server.lock: {e}"))),
    }
}

#[cfg(not(windows))]
pub fn acquire_dir_lock(dir: &Path) -> Result<DirLock> {
    let lock_path = dir.join("server.lock");
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&lock_path)?;
    Ok(DirLock { file, path: lock_path })
}

/// Sweep orphaned temporary files from aborted/killed checkpoints, dumps, or page writes.
pub fn sweep_tmp_files(dir: &Path) {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                if file_name.ends_with(".tmp") || file_name.contains(".tmp.") {
                    let _ = std::fs::remove_file(&path);
                }
            }
        }
    }
}
