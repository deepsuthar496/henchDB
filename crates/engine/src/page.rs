//! Slotted pages, swizzled pointers (swips) and a cooling-eviction buffer
//! pool — the Priority 2 (`research.md` §Storage) out-of-RAM foundation.
//!
//! v1 scope (deliberate): the pool backs **off-page overflow values** (wide
//! rows that exceed [`crate::table::MAX_INLINE_ROW`]), InnoDB-DYNAMIC-style,
//! while B+ tree structure stays heap-resident. Tree-node paging on swips is
//! the follow-up; every invariant below is designed so the tree can adopt
//! swips without rework.
//!
//! Layout — fixed [`PAGE_SIZE`] (256 KiB, matching NVMe block alignment so no
//! doublewrite-style torn-page guard is ever needed):
//! ```text
//! [0..4]    magic b"HDBP"          [4..8]    format version u32
//! [8..16]   page id u64            [16..20]  slot count u32
//! [20..24]  data-start offset u32 (records grow down from PAGE_SIZE)
//! [24..28]  crc32 (IEEE) over the whole page with this field zeroed
//! [28..32]  reserved
//! [32..]    slot directory: [offset u32][len u32] per slot; (0,0) = free
//! [..PAGE_SIZE] record bytes (a record = fragment header + payload)
//! ```
//! Fragment records chain values larger than one page:
//! `[flags u8][total_len u64][next_page u64][next_slot u32][payload]` —
//! `flags==0` terminal, `==1` chained. Single-value cap [`MAX_VALUE_LEN`].
//!
//! [`Swip`] is the 64-bit tagged pointer from the blueprint: bit 63 set =
//! swizzled (resident frame handle in the low bits), clear = unswizzled disk
//! page id. Unlike the paper's raw pointers, frames are owned by the pool and
//! a swip resolves only through it — identical single-owner semantics with no
//! `unsafe`. The page table holds exactly one frame per page
//! (single-owning-swip rule); the pool is flat in v1 so bottom-up eviction is
//! vacuous (noted for the tree-node follow-up).
//!
//! [`BufferPool`] is write-through: every store lands on disk before success
//! is reported, so eviction only drops frames and faults only re-read. No
//! dirty tracking, no writeback, crash-safe by construction; write-back
//! batching is a follow-up. Cooling follows the two-stage ring: hot frames
//! sampled to a bounded cooling FIFO on pressure, reheated on hit, dropped at
//! the head. Freed slots are quarantined behind the epoch manager (readers
//! resolve locators while pinned by `Database::execute`) and reused only once
//! an epoch horizon has passed; the page file itself grows monotonically with
//! a persisted superblock (page 0), so locators stay valid across restarts.
//!
//! Locking: one coarse mutex guards pool metadata (documented v1 choice —
//! only wide-row ops touch the pool; the hot small-row path never does).
//! All file I/O is portable `std` (`seek`+`read/write`); polled `io_uring` /
//! `O_DIRECT` fast paths are Linux follow-ups behind `cfg` gates.

use std::collections::{HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::epoch::EpochManager;
use crate::error::{Error, Result};

/// Fixed page size: 256 KiB, NVMe-aligned; doubles as the atomic write unit.
pub const PAGE_SIZE: usize = 256 * 1024;
/// On-disk magic for page images and the superblock.
pub const PAGE_MAGIC: &[u8; 4] = b"HDBP";
/// Page image format version; mismatches fail as `Corrupted`, never reinterpret.
pub const PAGE_FORMAT_VERSION: u32 = 1;
/// Largest single storable value (64 MiB); larger writes fail cleanly.
pub const MAX_VALUE_LEN: usize = 64 * 1024 * 1024;
/// Locator tag Bytes: 0xFF can never start an inline row (datum tags are 0-4).
pub const LOCATOR_TAG: u8 = 0xFF;
pub const LOCATOR_MAGIC: u8 = b'P';
/// Encoded locator length: tag + magic + page id + slot.
pub const LOCATOR_LEN: usize = 14;

const HEADER_LEN: usize = 32;
const SLOT_LEN: usize = 8;
const FRAG_SINGLE_OVERHEAD: usize = 9; // flags + total_len
const FRAG_CHAIN_OVERHEAD: usize = 21; // flags + total_len + next_page + next_slot
const MAX_FRAG_PAYLOAD: usize = PAGE_SIZE - HEADER_LEN - 2 * SLOT_LEN - FRAG_CHAIN_OVERHEAD;
const MAX_CHAIN_FRAGS: usize = 4096;

pub type PageId = u64;

/// 64-bit swizzled pointer: bit 63 = resident frame handle vs disk page id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Swip(u64);

const SWIZZLED_BIT: u64 = 1 << 63;

impl Swip {
    pub fn unswizzled(page: PageId) -> Self {
        debug_assert!(page & SWIZZLED_BIT == 0);
        Swip(page)
    }
    pub fn swizzled(frame_idx: usize) -> Self {
        debug_assert!((frame_idx as u64) & SWIZZLED_BIT == 0);
        Swip((frame_idx as u64) | SWIZZLED_BIT)
    }
    pub fn is_swizzled(self) -> bool {
        self.0 & SWIZZLED_BIT != 0
    }
    pub fn page_id(self) -> Option<PageId> {
        (!self.is_swizzled()).then_some(self.0)
    }
    pub fn frame_idx(self) -> Option<usize> {
        self.is_swizzled().then_some((self.0 & !SWIZZLED_BIT) as usize)
    }
    pub fn raw(self) -> u64 {
        self.0
    }
    pub fn from_raw(v: u64) -> Self {
        Swip(v)
    }
}

/// Locator stored in the B+ tree in place of an off-page row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Locator {
    pub page: PageId,
    pub slot: u32,
}

impl Locator {
    pub fn encode(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(LOCATOR_LEN);
        v.push(LOCATOR_TAG);
        v.push(LOCATOR_MAGIC);
        v.extend_from_slice(&self.page.to_le_bytes());
        v.extend_from_slice(&self.slot.to_le_bytes());
        v
    }
    pub fn decode(buf: &[u8]) -> Result<Locator> {
        if buf.len() != LOCATOR_LEN || buf[0] != LOCATOR_TAG || buf[1] != LOCATOR_MAGIC {
            return Err(Error::Corrupted("bad overflow locator".into()));
        }
        let mut b8 = [0u8; 8];
        b8.copy_from_slice(&buf[2..10]);
        let mut b4 = [0u8; 4];
        b4.copy_from_slice(&buf[10..14]);
        Ok(Locator {
            page: u64::from_le_bytes(b8),
            slot: u32::from_le_bytes(b4),
        })
    }
    pub fn is_locator(buf: &[u8]) -> bool {
        buf.len() == LOCATOR_LEN && buf[0] == LOCATOR_TAG && buf[1] == LOCATOR_MAGIC
    }
}

// ---------------------------------------------------------------------------
// Page image
// ---------------------------------------------------------------------------

fn read_u32(img: &[u8], off: usize) -> u32 {
    u32::from_le_bytes([img[off], img[off + 1], img[off + 2], img[off + 3]])
}

fn write_u32(img: &mut [u8], off: usize, v: u32) {
    img[off..off + 4].copy_from_slice(&v.to_le_bytes());
}

fn read_u64(img: &[u8], off: usize) -> u64 {
    let mut b = [0u8; 8];
    b.copy_from_slice(&img[off..off + 8]);
    u64::from_le_bytes(b)
}

fn write_u64(img: &mut [u8], off: usize, v: u64) {
    img[off..off + 8].copy_from_slice(&v.to_le_bytes());
}

/// In-memory image of one slotted page.
pub struct Page {
    id: PageId,
    img: Vec<u8>,
}

impl Page {
    pub fn create(id: PageId) -> Self {
        let mut img = vec![0u8; PAGE_SIZE];
        img[0..4].copy_from_slice(PAGE_MAGIC);
        write_u32(&mut img, 4, PAGE_FORMAT_VERSION);
        write_u64(&mut img, 8, id);
        write_u32(&mut img, 16, 0); // slot count
        write_u32(&mut img, 20, PAGE_SIZE as u32); // data start
        let mut p = Page { id, img };
        p.update_checksum();
        p
    }

    pub fn id(&self) -> PageId {
        self.id
    }

    fn slot_count(&self) -> usize {
        read_u32(&self.img, 16) as usize
    }

    fn data_start(&self) -> usize {
        read_u32(&self.img, 20) as usize
    }

    fn set_data_start(&mut self, v: usize) {
        write_u32(&mut self.img, 20, v as u32);
    }

    fn slot_off(&self, slot: usize) -> Option<(usize, usize)> {
        if slot >= self.slot_count() {
            return None;
        }
        let base = HEADER_LEN + slot * SLOT_LEN;
        if base + SLOT_LEN > PAGE_SIZE {
            return None;
        }
        Some((read_u32(&self.img, base) as usize, read_u32(&self.img, base + 4) as usize))
    }

    fn set_slot(&mut self, slot: usize, off: usize, len: usize) {
        let base = HEADER_LEN + slot * SLOT_LEN;
        write_u32(&mut self.img, base, off as u32);
        write_u32(&mut self.img, base + 4, len as u32);
        self.update_checksum();
    }

    /// Free bytes available for a new record (data region minus one slot).
    pub fn free_space(&self) -> usize {
        let dir_end = HEADER_LEN + (self.slot_count() + 1) * SLOT_LEN;
        self.data_start().saturating_sub(dir_end)
    }

    /// Insert a record, returning its slot. First-fit free slot, else append.
    pub fn insert(&mut self, data: &[u8]) -> Option<u32> {
        let need = data.len() + SLOT_LEN;
        if need > self.free_space() {
            return None;
        }
        // Reuse a free slot entry when one fits (avoids directory growth).
        let count = self.slot_count();
        for s in 0..count {
            let (o, l) = self.slot_off(s).expect("slot in range");
            if o == 0 && l == 0 {
                let start = self.data_start() - data.len();
                self.img[start..start + data.len()].copy_from_slice(data);
                self.set_data_start(start);
                self.set_slot(s, start, data.len());
                return Some(s as u32);
            }
        }
        let start = self.data_start() - data.len();
        self.img[start..start + data.len()].copy_from_slice(data);
        self.set_data_start(start);
        let s = count;
        write_u32(&mut self.img, 16, (count + 1) as u32);
        self.set_slot(s, start, data.len());
        Some(s as u32)
    }

    pub fn get(&self, slot: u32) -> Option<&[u8]> {
        let (o, l) = self.slot_off(slot as usize)?;
        if o == 0 && l == 0 {
            return None;
        }
        if o.checked_add(l)? > PAGE_SIZE || o < HEADER_LEN + self.slot_count() * SLOT_LEN {
            return None;
        }
        Some(&self.img[o..o + l])
    }

    pub fn free_slot(&mut self, slot: u32) -> bool {
        let s = slot as usize;
        let (o, l) = match self.slot_off(s) {
            Some(v) => v,
            None => return false,
        };
        if o == 0 && l == 0 {
            return false;
        }
        self.set_slot(s, 0, 0);
        true
    }

    /// Defragment: repack live records toward the end of the page.
    pub fn compact(&mut self) {
        let count = self.slot_count();
        let mut live: Vec<(usize, Vec<u8>)> = Vec::new();
        for s in 0..count {
            let (o, l) = self.slot_off(s).expect("slot in range");
            if o != 0 || l != 0 {
                live.push((s, self.img[o..o + l].to_vec()));
            }
        }
        let mut start = PAGE_SIZE;
        for (s, data) in &live {
            start -= data.len();
            self.img[start..start + data.len()].copy_from_slice(data);
            let base = HEADER_LEN + s * SLOT_LEN;
            write_u32(&mut self.img, base, start as u32);
            write_u32(&mut self.img, base + 4, data.len() as u32);
        }
        // Clear the reclaimed region so checksums stay deterministic.
        let dir_end = HEADER_LEN + count * SLOT_LEN;
        for b in &mut self.img[dir_end..start] {
            *b = 0;
        }
        self.set_data_start(start);
        // set_slot bumps the checksum per slot; refresh once at the end.
        self.update_checksum();
    }

    fn update_checksum(&mut self) {
        for b in &mut self.img[24..28] {
            *b = 0;
        }
        let c = crate::wal::crc32(&self.img);
        self.img[24..28].copy_from_slice(&c.to_le_bytes());
    }

    /// Validate magic, version and checksum of a page read from disk.
    pub fn verify(&self) -> Result<()> {
        if &self.img[0..4] != PAGE_MAGIC {
            return Err(Error::Corrupted("page: bad magic".into()));
        }
        if read_u32(&self.img, 4) != PAGE_FORMAT_VERSION {
            return Err(Error::Corrupted("page: version".into()));
        }
        if read_u64(&self.img, 8) != self.id {
            return Err(Error::Corrupted("page: id mismatch".into()));
        }
        let stored = read_u32(&self.img, 24);
        let mut tmp = self.img.clone();
        for b in &mut tmp[24..28] {
            *b = 0;
        }
        if crate::wal::crc32(&tmp) != stored {
            return Err(Error::Corrupted("page: checksum".into()));
        }
        // Structural sanity so torn reads fail instead of panicking.
        let count = self.slot_count();
        if HEADER_LEN + count * SLOT_LEN > PAGE_SIZE {
            return Err(Error::Corrupted("page: slot directory overflow".into()));
        }
        let ds = self.data_start();
        if ds < HEADER_LEN + count * SLOT_LEN || ds > PAGE_SIZE {
            return Err(Error::Corrupted("page: data-start out of range".into()));
        }
        Ok(())
    }

    fn from_image(id: PageId, img: Vec<u8>) -> Result<Self> {
        if img.len() != PAGE_SIZE {
            return Err(Error::Corrupted("page: short image".into()));
        }
        let p = Page { id, img };
        p.verify()?;
        Ok(p)
    }
}

// ---------------------------------------------------------------------------
// Page file (persistent, monotonically growing, superblock = page 0)
// ---------------------------------------------------------------------------

const SUPERBLOCK_PAGE: PageId = 0;

fn superblock_image(next_id: PageId) -> Vec<u8> {
    let mut img = vec![0u8; PAGE_SIZE];
    img[0..4].copy_from_slice(PAGE_MAGIC);
    write_u32(&mut img, 4, PAGE_FORMAT_VERSION);
    write_u64(&mut img, 8, SUPERBLOCK_PAGE);
    write_u64(&mut img, HEADER_LEN, next_id);
    let c = crate::wal::crc32(&img);
    img[24..28].copy_from_slice(&c.to_le_bytes());
    img
}

fn superblock_next_id(img: &[u8]) -> Result<PageId> {
    if &img[0..4] != PAGE_MAGIC || read_u32(img, 4) != PAGE_FORMAT_VERSION {
        return Err(Error::Corrupted("page file: bad superblock".into()));
    }
    let stored = read_u32(img, 24);
    let mut tmp = img.to_vec();
    for b in &mut tmp[24..28] {
        *b = 0;
    }
    if crate::wal::crc32(&tmp) != stored {
        return Err(Error::Corrupted("page file: superblock checksum".into()));
    }
    let next = read_u64(img, HEADER_LEN);
    if next < 1 {
        return Err(Error::Corrupted("page file: bad next-page".into()));
    }
    Ok(next)
}

struct PageFile {
    file: File,
    #[allow(dead_code)]
    path: PathBuf,
    next_id: PageId,
}

impl PageFile {
    fn open(path: &Path) -> Result<Self> {
        let exists = path.exists();
        let mut file = OpenOptions::new().read(true).write(true).create(true).open(path)?;
        let next_id = if !exists {
            file.write_all(&superblock_image(1))?;
            file.sync_data()?;
            1
        } else {
            let len = file.metadata()?.len();
            if len < PAGE_SIZE as u64 || len % PAGE_SIZE as u64 != 0 {
                return Err(Error::Corrupted("page file: bad size".into()));
            }
            let mut sb = vec![0u8; PAGE_SIZE];
            file.seek(SeekFrom::Start(0))?;
            file.read_exact(&mut sb)?;
            superblock_next_id(&sb)?
        };
        Ok(PageFile {
            file,
            path: path.to_path_buf(),
            next_id,
        })
    }

    fn alloc_page(&mut self) -> Result<PageId> {
        let id = self.next_id;
        if id == u64::MAX {
            return Err(Error::Io("page file exhausted".into()));
        }
        self.next_id += 1;
        // Persist the superblock first so a crash can never reassign the id.
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&superblock_image(self.next_id))?;
        // Extend the file with a fresh page image.
        let page = Page::create(id);
        self.file.seek(SeekFrom::Start(id * PAGE_SIZE as u64))?;
        self.file.write_all(&page.img)?;
        Ok(id)
    }

    fn read_page(&mut self, id: PageId) -> Result<Page> {
        if id == SUPERBLOCK_PAGE || id >= self.next_id {
            return Err(Error::Corrupted("page: id out of range".into()));
        }
        let mut img = vec![0u8; PAGE_SIZE];
        self.file.seek(SeekFrom::Start(id * PAGE_SIZE as u64))?;
        self.file.read_exact(&mut img).map_err(|_| Error::Corrupted("page: short read".into()))?;
        Page::from_image(id, img)
    }

    fn write_page(&mut self, page: &Page) -> Result<()> {
        if page.id == SUPERBLOCK_PAGE || page.id >= self.next_id {
            return Err(Error::Corrupted("page: id out of range".into()));
        }
        self.file.seek(SeekFrom::Start(page.id * PAGE_SIZE as u64))?;
        self.file.write_all(&page.img)?;
        Ok(())
    }

    fn sync_data(&mut self) -> Result<()> {
        self.file.sync_data()?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Buffer pool
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct PoolStats {
    pub hits: u64,
    pub faults: u64,
    pub evictions: u64,
    pub stores: u64,
    pub loads: u64,
    pub resident_pages: usize,
    pub cooling_pages: usize,
    pub frames: usize,
}

struct Frame {
    page: Option<PageId>,
    image: Option<Page>,
    cooling: bool,
}

struct Quarantined {
    frags: Vec<(PageId, u32)>,
    epoch: u64,
}

impl Inner {
    /// Write a resident frame's image through to the page file (best
    /// effort; pool callers already hold durability via the WAL backstop).
    fn write_frame_through(&mut self, f: usize) {
        // Disjoint field borrows on `Inner` itself (not through the guard).
        if let Some(img) = self.frames[f].image.as_ref() {
            let _ = self.file.write_page(img);
        }
    }
}

struct Inner {
    file: PageFile,
    frames: Vec<Frame>,
    /// Page table: the single owning swip per resident page.
    index: HashMap<PageId, usize>,
    /// Two-stage cooling FIFO (frame indices, oldest first).
    cooling: VecDeque<usize>,
    /// Freed slots awaiting their epoch horizon before reuse.
    quarantine: Vec<Quarantined>,
    /// Freed slots on evicted pages, applied lazily on next fault.
    pending_free: HashMap<PageId, Vec<u32>>,
    /// Known free bytes per page (resident-exact; evicted pages keep their
    /// last-known value, corrected on fault). Missing entries (e.g. after a
    /// restart) are treated as full, so the file only ever grows then —
    /// safe, with a page-GC pass as backlog.
    free_map: HashMap<PageId, usize>,
    stats: PoolStats,
    rng: u64,
}

pub struct BufferPool {
    inner: Mutex<Inner>,
    epoch: Arc<EpochManager>,
}

impl BufferPool {
    pub fn open(path: &Path, frames: usize, epoch: Arc<EpochManager>) -> Result<Self> {
        if frames == 0 {
            return Err(Error::Io("buffer pool needs at least one frame".into()));
        }
        let mut fr = Vec::with_capacity(frames);
        for _ in 0..frames {
            fr.push(Frame {
                page: None,
                image: None,
                cooling: false,
            });
        }
        Ok(BufferPool {
            inner: Mutex::new(Inner {
                file: PageFile::open(path)?,
                frames: fr,
                index: HashMap::new(),
                cooling: VecDeque::new(),
                quarantine: Vec::new(),
                pending_free: HashMap::new(),
                free_map: HashMap::new(),
                stats: PoolStats::default(),
                rng: 0x9E3779B97F4A7C15,
            }),
            epoch,
        })
    }

    pub fn stats(&self) -> PoolStats {
        let mut s = self.inner.lock().unwrap().stats.clone();
        let inner = self.inner.lock().unwrap();
        s.resident_pages = inner.index.len();
        s.cooling_pages = inner.cooling.len();
        s.frames = inner.frames.len();
        s
    }

    /// Durably flush file state (call before snapshots that reference pages).
    pub fn sync_data(&self) -> Result<()> {
        self.inner.lock().unwrap().file.sync_data()
    }

    // -- internal helpers (inner lock held) --------------------------------

    fn next_rand(inner: &mut Inner) -> usize {
        inner.rng ^= inner.rng << 13;
        inner.rng ^= inner.rng >> 7;
        inner.rng ^= inner.rng << 17;
        (inner.rng % inner.frames.len() as u64) as usize
    }

    /// Obtain a frame for `page`, faulting it in when necessary.
    /// Returns the frame index; the page is resident and hot afterwards.
    fn fix_page(inner: &mut Inner, page: PageId) -> Result<usize> {
        if let Some(&f) = inner.index.get(&page) {
            // Hit: reheat cooling frames (soft fault, no I/O).
            if inner.frames[f].cooling {
                inner.frames[f].cooling = false;
                inner.cooling.retain(|&x| x != f);
            }
            inner.stats.hits += 1;
            return Ok(f);
        }
        // Fault: fault the page in, evicting when every frame is busy.
        let f = Self::victim_frame(inner);
        let mut img_page = inner.file.read_page(page)?;
        // Apply lazily-tracked frees from before the page was evicted.
        if let Some(slots) = inner.pending_free.remove(&page) {
            let mut dirty = false;
            for s in slots {
                dirty |= img_page.free_slot(s);
            }
            if dirty {
                inner.file.write_page(&img_page)?;
            }
        }
        inner.free_map.insert(page, img_page.free_space());
        inner.frames[f].page = Some(page);
        inner.frames[f].image = Some(img_page);
        inner.frames[f].cooling = false;
        inner.index.insert(page, f);
        inner.stats.faults += 1;
        Ok(f)
    }

    /// Pick a frame to reuse: free frames first, then the cooling head
    /// (oldest), else force a hot victim into cooling and take the head.
    fn victim_frame(inner: &mut Inner) -> usize {
        for (i, fr) in inner.frames.iter().enumerate() {
            if fr.page.is_none() {
                return i;
            }
        }
        if let Some(f) = inner.cooling.pop_front() {
            Self::drop_frame(inner, f);
            return f;
        }
        // Extreme pressure: sample a hot victim into cooling, then evict it.
        // Degenerates to direct eviction for this fault (documented).
        let v = Self::next_rand(inner);
        inner.frames[v].cooling = true;
        inner.cooling.push_back(v);
        let f = inner.cooling.pop_front().unwrap();
        Self::drop_frame(inner, f);
        f
    }

    fn drop_frame(inner: &mut Inner, f: usize) {
        if let Some(p) = inner.frames[f].page.take() {
            inner.index.remove(&p);
            inner.stats.evictions += 1;
        }
        inner.frames[f].image = None;
        inner.frames[f].cooling = false;
    }

    /// Opportunistically cool one hot frame (keeps the cooling stage fed so
    /// faults find victims without forcing hot evictions).
    fn maybe_cool(inner: &mut Inner) {
        if inner.cooling.len() >= inner.frames.len() {
            return;
        }
        // Sample a few candidates; cool the first hot one found.
        for _ in 0..4 {
            let v = Self::next_rand(inner);
            if inner.frames[v].page.is_some() && !inner.frames[v].cooling {
                inner.frames[v].cooling = true;
                inner.cooling.push_back(v);
                return;
            }
        }
    }

    /// Move quarantine entries past their epoch horizon into lazy frees.
    fn reclaim_quarantine(inner: &mut Inner, epoch: &EpochManager) {
        epoch.try_reclaim();
        let current = epoch.current_epoch();
        let mut i = 0;
        while i < inner.quarantine.len() {
            // Safe when every live reader started strictly after the free:
            // entry.epoch + 1 < current global epoch.
            if inner.quarantine[i].epoch + 1 < current {
                let q = inner.quarantine.remove(i);
                for (page, slot) in q.frags {
                    if let Some(&f) = inner.index.get(&page) {
                        let img = inner.frames[f].image.as_mut().unwrap();
                        if img.free_slot(slot) {
                            let free = img.free_space();
                            inner.free_map.insert(page, free);
                            // Best-effort persistence of the slot directory.
                            let _ = inner.file.write_page(img);
                        }
                    } else {
                        inner.pending_free.entry(page).or_default().push(slot);
                    }
                }
            } else {
                i += 1;
            }
        }
    }

    fn insert_frag(inner: &mut Inner, frag: &[u8]) -> Result<(PageId, u32)> {
        debug_assert!(frag.len() + SLOT_LEN <= PAGE_SIZE - HEADER_LEN);
        let need = frag.len() + SLOT_LEN;
        // Best-fit over every page with known free space. The winner is
        // faulted in when evicted, so small tails pack into shared pages
        // instead of churning fresh ones under eviction pressure.
        let mut best: Option<PageId> = None;
        let mut best_free = usize::MAX;
        for (&page, &free) in inner.free_map.iter() {
            if free >= need && free < best_free {
                best = Some(page);
                best_free = free;
            }
        }
        if let Some(page) = best {
            let f = Self::fix_page(inner, page)?;
            let img = inner.frames[f].image.as_mut().unwrap();
            if let Some(slot) = img.insert(frag) {
                let (id, free) = (img.id(), img.free_space());
                inner.file.write_page(img)?;
                inner.free_map.insert(id, free);
                return Ok((id, slot));
            }
            // Map was stale; correct it and fall through to a fresh page.
            let free = img.free_space();
            inner.free_map.insert(page, free);
        }
        // No room anywhere known: allocate a fresh page (monotonic id,
        // persisted before use so a crash can never reassign it).
        let id = inner.file.alloc_page()?;
        let f = Self::victim_frame(inner);
        let mut page = Page::create(id);
        let slot = page.insert(frag).expect("fresh page fits");
        inner.free_map.insert(id, page.free_space());
        inner.file.write_page(&page)?;
        inner.frames[f].page = Some(id);
        inner.frames[f].image = Some(page);
        inner.frames[f].cooling = false;
        inner.index.insert(id, f);
        Self::maybe_cool(inner);
        Ok((id, slot))
    }

    // -- public ops --------------------------------------------------------

    /// Store bytes (chained across pages when large), write-through.
    pub fn store(&self, data: &[u8]) -> Result<Locator> {
        if data.is_empty() {
            return Err(Error::Io("cannot page empty value".into()));
        }
        if data.len() > MAX_VALUE_LEN {
            return Err(Error::Io("overflow value too large".into()));
        }
        let mut inner = self.inner.lock().unwrap();
        Self::reclaim_quarantine(&mut inner, &self.epoch);
        // Split into page-sized fragments.
        let mut frags: Vec<&[u8]> = Vec::new();
        let mut off = 0;
        while off < data.len() {
            let n = (data.len() - off).min(MAX_FRAG_PAYLOAD);
            frags.push(&data[off..off + n]);
            off += n;
        }
        let total = data.len() as u64;
        let mut first: Option<(PageId, u32)> = None;
        let mut alloced: Vec<(PageId, u32)> = Vec::new();
        for (i, chunk) in frags.iter().enumerate() {
            let chained = i + 1 < frags.len();
            let mut rec = Vec::with_capacity(chunk.len() + FRAG_CHAIN_OVERHEAD);
            rec.push(if chained { 1 } else { 0 });
            rec.extend_from_slice(&total.to_le_bytes());
            if chained {
                // Placeholder next pointer; patched after the next alloc.
                rec.extend_from_slice(&0u64.to_le_bytes());
                rec.extend_from_slice(&0u32.to_le_bytes());
            }
            rec.extend_from_slice(chunk);
            match Self::insert_frag(&mut inner, &rec) {
                Ok(loc) => {
                    // Patch the previous fragment's next pointer. Its page
                    // may have been evicted by this very alloc on tiny pools,
                    // so fault it back instead of assuming residency.
                    if let Some((pp, ps)) = alloced.last().copied() {
                        let pf = Self::fix_page(&mut inner, pp)?;
                        {
                            let img = inner.frames[pf].image.as_mut().unwrap();
                            let (ro, _) = img
                                .slot_off(ps as usize)
                                .ok_or_else(|| Error::Corrupted("overflow: chain slot lost".into()))?;
                            img.img[ro + 9..ro + 17].copy_from_slice(&loc.0.to_le_bytes());
                            img.img[ro + 17..ro + 21].copy_from_slice(&loc.1.to_le_bytes());
                            // Patched in place (slot api would rewrite the
                            // slot); refresh the checksum and write through.
                            img.update_checksum();
                        }
                        inner.write_frame_through(pf);
                    }
                    alloced.push(loc);
                    if first.is_none() {
                        first = Some(loc);
                    }
                }
                Err(e) => {
                    // Best-effort rollback of the partial chain.
                    for (p, s) in alloced {
                        if let Some(&f) = inner.index.get(&p) {
                            inner.frames[f].image.as_mut().unwrap().free_slot(s);
                            inner.write_frame_through(f);
                        }
                    }
                    return Err(e);
                }
            }
        }
        inner.stats.stores += 1;
        let (page, slot) = first.expect("non-empty data has a first fragment");
        Ok(Locator { page, slot })
    }

    /// Load a stored value, faulting evicted pages back from disk.
    pub fn load(&self, loc: Locator) -> Result<Vec<u8>> {
        let mut inner = self.inner.lock().unwrap();
        Self::reclaim_quarantine(&mut inner, &self.epoch);
        let mut out = Vec::new();
        let mut total: Option<u64> = None;
        let mut cur = loc;
        let mut frags = 0usize;
        loop {
            if frags >= MAX_CHAIN_FRAGS {
                return Err(Error::Corrupted("overflow chain too long".into()));
            }
            frags += 1;
            let f = Self::fix_page(&mut inner, cur.page)?;
            let rec = {
                let img = inner.frames[f].image.as_ref().unwrap();
                img.get(cur.slot).ok_or_else(|| Error::Corrupted("overflow: bad slot".into()))?.to_vec()
            };
            if rec.len() < FRAG_SINGLE_OVERHEAD {
                return Err(Error::Corrupted("overflow: short fragment".into()));
            }
            let flags = rec[0];
            if flags > 1 {
                return Err(Error::Corrupted("overflow: bad flags".into()));
            }
            let mut b8 = [0u8; 8];
            b8.copy_from_slice(&rec[1..9]);
            let t = u64::from_le_bytes(b8);
            if total.map_or(false, |x| x != t) {
                return Err(Error::Corrupted("overflow: total mismatch".into()));
            }
            total = Some(t);
            if t as usize > MAX_VALUE_LEN {
                return Err(Error::Corrupted("overflow: total too large".into()));
            }
            if flags == 1 {
                if rec.len() < FRAG_CHAIN_OVERHEAD {
                    return Err(Error::Corrupted("overflow: short chain head".into()));
                }
                let mut bp = [0u8; 8];
                bp.copy_from_slice(&rec[9..17]);
                let mut bs = [0u8; 4];
                bs.copy_from_slice(&rec[17..21]);
                out.extend_from_slice(&rec[FRAG_CHAIN_OVERHEAD..]);
                cur = Locator {
                    page: u64::from_le_bytes(bp),
                    slot: u32::from_le_bytes(bs),
                };
            } else {
                out.extend_from_slice(&rec[FRAG_SINGLE_OVERHEAD..]);
                break;
            }
        }
        let total = total.unwrap_or(0) as usize;
        if out.len() != total {
            return Err(Error::Corrupted("overflow: length mismatch".into()));
        }
        inner.stats.loads += 1;
        Ok(out)
    }

    /// Release a stored value. Physical slot reuse waits for the epoch
    /// horizon (readers resolve locators while pinned by `execute`).
    pub fn free(&self, loc: Locator) {
        let mut inner = self.inner.lock().unwrap();
        // Collect the full chain while resident info is handy; missing pages
        // simply contribute nothing (already-rolled-back or corrupt handles
        // are validated on load, and free must never fail a commit).
        let mut frags = vec![(loc.page, loc.slot)];
        let mut cur = loc;
        for _ in 0..MAX_CHAIN_FRAGS {
            let next = match inner.index.get(&cur.page) {
                Some(&f) => {
                    let img = inner.frames[f].image.as_ref().unwrap();
                    match img.get(cur.slot) {
                        Some(rec) if !rec.is_empty() && rec[0] == 1 && rec.len() >= FRAG_CHAIN_OVERHEAD => {
                            let mut bp = [0u8; 8];
                            bp.copy_from_slice(&rec[9..17]);
                            let mut bs = [0u8; 4];
                            bs.copy_from_slice(&rec[17..21]);
                            Some(Locator {
                                page: u64::from_le_bytes(bp),
                                slot: u32::from_le_bytes(bs),
                            })
                        }
                        _ => None,
                    }
                }
                None => None, // evicted: chain tail unknown; head freed anyway
            };
            match next {
                Some(n) => {
                    frags.push((n.page, n.slot));
                    cur = n;
                }
                None => break,
            }
        }
        let epoch = self.epoch.current_epoch();
        // Deduplicate (a corrupt cycle could repeat a slot).
        frags.sort_unstable();
        frags.dedup();
        inner.quarantine.push(Quarantined { frags, epoch });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static POOL_SEQ: AtomicU64 = AtomicU64::new(0);

    fn temp_pool(frames: usize) -> BufferPool {
        let id = POOL_SEQ.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "hench_pooltest_{}_{}_{}.bin",
            std::process::id(),
            id,
            frames
        ));
        let _ = std::fs::remove_file(&path);
        BufferPool::open(&path, frames, EpochManager::new()).unwrap()
    }

    #[test]
    fn swip_bit_semantics() {
        let u = Swip::unswizzled(12345);
        assert!(!u.is_swizzled());
        assert_eq!(u.page_id(), Some(12345));
        assert_eq!(u.frame_idx(), None);
        let s = Swip::swizzled(7);
        assert!(s.is_swizzled());
        assert_eq!(s.frame_idx(), Some(7));
        assert_eq!(s.page_id(), None);
        assert_eq!(Swip::from_raw(s.raw()), s);
    }

    #[test]
    fn locator_roundtrip_and_tag() {
        let l = Locator { page: 99, slot: 3 };
        let b = l.encode();
        assert_eq!(b.len(), LOCATOR_LEN);
        assert!(Locator::is_locator(&b));
        assert_eq!(Locator::decode(&b).unwrap(), l);
        assert!(Locator::decode(&[0u8; 5]).is_err());
        // Inline rows never look like locators (datum tags are 0-4).
        assert!(!Locator::is_locator(&[1, 2, 3]));
    }

    #[test]
    fn page_insert_get_free_compact() {
        let mut p = Page::create(5);
        assert!(p.verify().is_ok());
        let a = p.insert(b"hello").unwrap();
        let b = p.insert(&vec![7u8; 1000]).unwrap();
        assert_eq!(p.get(a), Some(b"hello".as_slice()));
        assert_eq!(p.get(b).unwrap().len(), 1000);
        assert!(p.free_slot(a));
        assert_eq!(p.get(a), None);
        assert!(!p.free_slot(a));
        assert!(!p.free_slot(9999));
        p.compact();
        assert!(p.verify().is_ok());
        assert_eq!(p.get(b).unwrap().len(), 1000);
        // Oversized record does not fit.
        assert!(p.insert(&vec![0u8; PAGE_SIZE]).is_none());
    }

    #[test]
    fn page_tamper_fails_verify() {
        let mut p = Page::create(9);
        p.insert(b"data").unwrap();
        p.img[100] ^= 0xFF;
        assert!(p.verify().is_err());
    }

    #[test]
    fn pool_small_values_roundtrip() {
        let pool = temp_pool(4);
        let mut locs = Vec::new();
        for i in 0..50u32 {
            let v = format!("value-{i}-{}", "x".repeat(i as usize * 37)).into_bytes();
            locs.push((v.clone(), pool.store(&v).unwrap()));
        }
        for (v, l) in &locs {
            assert_eq!(&pool.load(*l).unwrap(), v);
        }
        // Free half; the rest must stay intact.
        for (_, l) in locs.iter().step_by(2) {
            pool.free(*l);
        }
        for (i, (v, l)) in locs.iter().enumerate() {
            if i % 2 == 1 {
                assert_eq!(&pool.load(*l).unwrap(), v);
            }
        }
        let s = pool.stats();
        assert_eq!(s.stores, 50);
        assert!(s.loads >= 75);
    }

    #[test]
    fn pool_chained_large_value() {
        let pool = temp_pool(4);
        // ~300 KiB spans two pages.
        let big = (0..300 * 1024).map(|i| (i % 251) as u8).collect::<Vec<_>>();
        let loc = pool.store(&big).unwrap();
        assert_eq!(pool.load(loc).unwrap(), big);
        pool.free(loc);
        // Reuse after the horizon passes (no pinned readers in this test).
        let big2 = vec![0xABu8; 300 * 1024];
        let loc2 = pool.store(&big2).unwrap();
        assert_eq!(pool.load(loc2).unwrap(), big2);
    }

    #[test]
    fn pool_eviction_and_fault_transparent() {
        // 2 frames; enough distinct pages that eviction must happen.
        let pool = temp_pool(2);
        let mut locs = Vec::new();
        for i in 0..12u32 {
            // ~100 KiB each: about two fit per page, forcing many pages.
            let v = vec![(i % 251) as u8; 100 * 1024];
            locs.push((v.clone(), pool.store(&v).unwrap()));
        }
        let s = pool.stats();
        assert!(s.evictions > 0, "expected evictions, got {s:?}");
        // Every value still loads (faults bring evicted pages back).
        for (v, l) in &locs {
            assert_eq!(&pool.load(*l).unwrap(), v);
        }
        assert!(pool.stats().faults > 0);
    }

    #[test]
    fn pool_persists_across_reopen() {
        let path = std::env::temp_dir().join(format!("hench_poolpersist_{}.bin", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let loc = {
            let pool = BufferPool::open(&path, 2, EpochManager::new()).unwrap();
            let loc = pool.store(&vec![9u8; 5000]).unwrap();
            pool.sync_data().unwrap();
            loc
        };
        let pool2 = BufferPool::open(&path, 2, EpochManager::new()).unwrap();
        assert_eq!(pool2.load(loc).unwrap(), vec![9u8; 5000]);
        // New stores land in live pages when space fits (distinct slot).
        let loc2 = pool2.store(&vec![8u8; 10]).unwrap();
        assert_ne!(loc2, loc);
        assert_eq!(pool2.load(loc).unwrap(), vec![9u8; 5000]);
        assert_eq!(pool2.load(loc2).unwrap(), vec![8u8; 10]);
        // Page ids stay monotonic across restarts (never reassigned): a
        // fresh pool allocates ids beyond every persisted page.
        drop(pool2);
        let pool3 = BufferPool::open(&path, 2, EpochManager::new()).unwrap();
        let loc3 = pool3.store(&vec![7u8; 10]).unwrap();
        assert!(loc3.page > loc.page);
        assert_eq!(pool3.load(loc).unwrap(), vec![9u8; 5000]);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_corruption_is_error_not_panic() {
        let pool = temp_pool(2);
        let loc = pool.store(&vec![1u8; 100]).unwrap();
        // Unknown page / slot fail cleanly.
        assert!(pool.load(Locator { page: 1 << 40, slot: 0 }).is_err());
        assert!(pool.load(Locator { page: loc.page, slot: 777 }).is_err());
    }

    #[test]
    fn pool_rejects_empty_and_huge() {
        let pool = temp_pool(2);
        assert!(pool.store(&[]).is_err());
        assert!(pool.store(&vec![0u8; MAX_VALUE_LEN + 1]).is_err());
    }
}
