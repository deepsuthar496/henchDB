//! Engine telemetry: lock-free atomic counters, latency histograms, the
//! server connection registry backing `SHOW PROCESSLIST`, and the
//! Prometheus exposition renderer.
//!
//! Fast-path recording is `fetch_add(..., Relaxed)` on plain atomics — no
//! locks, no shared cache-line writes beyond the counter itself — so OLTP
//! throughput is unaffected. Expensive assembly (pool/WAL/tree rollups,
//! text rendering) happens only on the diagnostic path (`SHOW ...`,
//! `/metrics` scrapes), which reads atomics and formats snapshots.
//!
//! Metric name prefix derives from [`crate::PRODUCT_NAME`] (lowercased), so
//! renaming the product renames the exposition automatically; no brand name
//! is hardcoded here.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Instant;

/// Upper bounds (microseconds) of the query-latency histogram. Labels are
/// 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0 seconds, +Inf.
pub const HIST_BOUNDS_US: [u64; 9] = [
    100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 500_000, 1_000_000,
];
/// One counter per bound plus the implicit +Inf bucket.
pub const HIST_BUCKET_COUNT: usize = HIST_BOUNDS_US.len() + 1;

const HIST_LE_LABELS: [&str; HIST_BUCKET_COUNT] = [
    "0.0001", "0.0005", "0.001", "0.005", "0.01", "0.05", "0.1", "0.5", "1.0", "+Inf",
];

/// Statement class for `Com_*` accounting. Anything outside the classic
/// DML/commit set (BEGIN, SHOW, SET, USE, CHECKPOINT, ...) is `Other`:
/// counted in `Queries` + latency, but not in a `Com_*` counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StmtKind {
    Select,
    Insert,
    Update,
    Delete,
    Commit,
    Rollback,
    Ddl,
    Other,
}

impl StmtKind {
    /// Classify from the statement's first keyword (covers fast-path point
    /// statements and parsed statements uniformly).
    pub fn classify(sql: &str) -> StmtKind {
        let verb: String = sql
            .split_whitespace()
            .next()
            .unwrap_or_default()
            .trim_end_matches([';', '('])
            .to_ascii_uppercase();
        match verb.as_str() {
            "SELECT" => StmtKind::Select,
            "INSERT" => StmtKind::Insert,
            "UPDATE" => StmtKind::Update,
            "DELETE" => StmtKind::Delete,
            "COMMIT" => StmtKind::Commit,
            "ROLLBACK" => StmtKind::Rollback,
            "CREATE" | "DROP" | "ALTER" | "TRUNCATE" | "ANALYZE" => StmtKind::Ddl,
            _ => StmtKind::Other,
        }
    }
}

/// One row of `SHOW PROCESSLIST`.
#[derive(Debug, Clone)]
pub struct ProcessEntry {
    pub id: u64,
    pub user: String,
    pub host: String,
    pub db: String,
    pub command: String,
    pub state: String,
    pub info: String,
    /// Seconds since the entry entered its current state.
    pub time_secs: u64,
}

struct ProcessInner {
    user: String,
    host: String,
    db: String,
    command: String,
    state: String,
    info: String,
    state_since: Instant,
}

/// Plain snapshot of every counter (cheap to clone, safe to format).
#[derive(Debug, Clone, Default)]
pub struct MetricsSnapshot {
    pub uptime_secs: u64,
    pub queries: u64,
    pub com_select: u64,
    pub com_insert: u64,
    pub com_update: u64,
    pub com_delete: u64,
    pub com_commit: u64,
    pub com_rollback: u64,
    pub com_ddl: u64,
    pub query_us: u64,
    pub hist: [u64; HIST_BUCKET_COUNT],
    pub wal_records: u64,
    pub wal_bytes: u64,
    pub active_txns: usize,
    pub active_conns: usize,
    /// Replication (primary: connected replicas; replica: applied offset,
    /// lag bytes, and CONNECTING/STREAMING/DISCONNECTED status).
    pub repl_connected: usize,
    pub repl_applied: u64,
    pub repl_lag: u64,
    pub repl_status: String,
}

/// Live inputs gathered by the database for status/prometheus assembly.
#[derive(Debug, Clone, Default)]
pub struct EngineExtra {
    pub wal_syncs: u64,
    pub wal_sync_bytes: u64,
    pub wal_sync_us: u64,
    pub pool_frames: usize,
    pub pool_resident: usize,
    pub pool_hits: u64,
    pub pool_misses: u64,
    pub pool_evictions: u64,
    pub btree_splits: u64,
    pub btree_merges: u64,
    pub btree_in_place: u64,
    pub btree_height_max: usize,
    pub btree_nodes: usize,
    pub table_count: usize,
    pub mvcc_snapshots: usize,
    pub mvcc_chains: usize,
    /// Primary WAL written offset (log head) for `Rpl_master_wal_offset`.
    pub master_wal_offset: u64,
}

pub struct Metrics {
    start: Instant,
    queries: AtomicU64,
    com_select: AtomicU64,
    com_insert: AtomicU64,
    com_update: AtomicU64,
    com_delete: AtomicU64,
    com_commit: AtomicU64,
    com_rollback: AtomicU64,
    com_ddl: AtomicU64,
    query_us: AtomicU64,
    hist: [AtomicU64; HIST_BUCKET_COUNT],
    wal_records: AtomicU64,
    wal_bytes: AtomicU64,
    active_txns: AtomicUsize,
    active_conns: AtomicUsize,
    next_proc_id: AtomicU64,
    processes: Mutex<HashMap<u64, ProcessInner>>,
    repl_connected: AtomicUsize,
    repl_applied: AtomicU64,
    repl_lag: AtomicU64,
    repl_status: Mutex<String>,
}

impl Metrics {
    pub fn new() -> Self {
        Metrics {
            start: Instant::now(),
            queries: AtomicU64::new(0),
            com_select: AtomicU64::new(0),
            com_insert: AtomicU64::new(0),
            com_update: AtomicU64::new(0),
            com_delete: AtomicU64::new(0),
            com_commit: AtomicU64::new(0),
            com_rollback: AtomicU64::new(0),
            com_ddl: AtomicU64::new(0),
            query_us: AtomicU64::new(0),
            hist: std::array::from_fn(|_| AtomicU64::new(0)),
            wal_records: AtomicU64::new(0),
            wal_bytes: AtomicU64::new(0),
            active_txns: AtomicUsize::new(0),
            active_conns: AtomicUsize::new(0),
            next_proc_id: AtomicU64::new(1),
            processes: Mutex::new(HashMap::new()),
            repl_connected: AtomicUsize::new(0),
            repl_applied: AtomicU64::new(0),
            repl_lag: AtomicU64::new(0),
            repl_status: Mutex::new(String::new()),
        }
    }

    pub fn uptime_secs(&self) -> u64 {
        self.start.elapsed().as_secs()
    }

    /// Record one finished statement (hot path: atomics only).
    pub fn record_query(&self, kind: StmtKind, elapsed_us: u64) {
        self.queries.fetch_add(1, Ordering::Relaxed);
        self.query_us.fetch_add(elapsed_us, Ordering::Relaxed);
        let bucket = HIST_BOUNDS_US
            .iter()
            .position(|&b| elapsed_us <= b)
            .unwrap_or(HIST_BUCKET_COUNT - 1);
        self.hist[bucket].fetch_add(1, Ordering::Relaxed);
        let counter = match kind {
            StmtKind::Select => &self.com_select,
            StmtKind::Insert => &self.com_insert,
            StmtKind::Update => &self.com_update,
            StmtKind::Delete => &self.com_delete,
            StmtKind::Commit => &self.com_commit,
            StmtKind::Rollback => &self.com_rollback,
            StmtKind::Ddl => &self.com_ddl,
            StmtKind::Other => return,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one WAL append batch (hot path: atomics only).
    pub fn record_wal(&self, records: usize, bytes: u64) {
        self.wal_records.fetch_add(records as u64, Ordering::Relaxed);
        self.wal_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn txn_begin(&self) {
        self.active_txns.fetch_add(1, Ordering::Relaxed);
    }

    pub fn txn_end(&self) {
        // `fetch_sub` on Relaxed with a saturating guard would need a CAS
        // loop; underflow cannot happen (every end pairs with a begin), so
        // decrement and debug-assert instead of paying for the loop.
        let prev = self.active_txns.fetch_sub(1, Ordering::Relaxed);
        debug_assert!(prev > 0, "txn_end without matching txn_begin");
    }

    /// Register a new client connection; returns its processlist id.
    pub fn register_process(&self, user: &str, host: &str) -> u64 {
        let id = self.next_proc_id.fetch_add(1, Ordering::Relaxed);
        self.active_conns.fetch_add(1, Ordering::Relaxed);
        let now = Instant::now();
        self.processes.lock().unwrap().insert(
            id,
            ProcessInner {
                user: user.to_string(),
                host: host.to_string(),
                db: String::new(),
                command: "Connect".to_string(),
                state: "authenticating".to_string(),
                info: String::new(),
                state_since: now,
            },
        );
        id
    }

    /// Mark a connection as executing `command` (truncated info text).
    pub fn note_command(&self, id: u64, db: &str, command: &str, info: &str) {
        if let Some(p) = self.processes.lock().unwrap().get_mut(&id) {
            p.db = db.to_string();
            p.command = command.to_string();
            p.state = "executing".to_string();
            let mut text = info.trim().to_string();
            if text.len() > 256 {
                let cut = text.floor_char_boundary(256);
                text.truncate(cut);
            }
            p.info = text;
            p.state_since = Instant::now();
        }
    }

    /// Mark a connection idle after finishing its command.
    pub fn note_idle(&self, id: u64) {
        if let Some(p) = self.processes.lock().unwrap().get_mut(&id) {
            p.command = "Sleep".to_string();
            p.state = "idle".to_string();
            p.info.clear();
            p.state_since = Instant::now();
        }
    }

    /// Drop a connection from the registry.
    pub fn unregister_process(&self, id: u64) {
        if self.processes.lock().unwrap().remove(&id).is_some() {
            let prev = self.active_conns.fetch_sub(1, Ordering::Relaxed);
            debug_assert!(prev > 0, "unregister without matching register");
        }
    }

    /// Live processlist ordered by id.
    pub fn process_list(&self) -> Vec<ProcessEntry> {
        let guard = self.processes.lock().unwrap();
        let mut ids: Vec<u64> = guard.keys().copied().collect();
        ids.sort_unstable();
        ids.into_iter()
            .filter_map(|id| {
                guard.get(&id).map(|p| ProcessEntry {
                    id,
                    user: p.user.clone(),
                    host: p.host.clone(),
                    db: p.db.clone(),
                    command: p.command.clone(),
                    state: p.state.clone(),
                    info: p.info.clone(),
                    time_secs: p.state_since.elapsed().as_secs(),
                })
            })
            .collect()
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        let mut hist = [0u64; HIST_BUCKET_COUNT];
        for (i, h) in self.hist.iter().enumerate() {
            hist[i] = h.load(Ordering::Relaxed);
        }
        MetricsSnapshot {
            uptime_secs: self.uptime_secs(),
            queries: self.queries.load(Ordering::Relaxed),
            com_select: self.com_select.load(Ordering::Relaxed),
            com_insert: self.com_insert.load(Ordering::Relaxed),
            com_update: self.com_update.load(Ordering::Relaxed),
            com_delete: self.com_delete.load(Ordering::Relaxed),
            com_commit: self.com_commit.load(Ordering::Relaxed),
            com_rollback: self.com_rollback.load(Ordering::Relaxed),
            com_ddl: self.com_ddl.load(Ordering::Relaxed),
            query_us: self.query_us.load(Ordering::Relaxed),
            hist,
            wal_records: self.wal_records.load(Ordering::Relaxed),
            wal_bytes: self.wal_bytes.load(Ordering::Relaxed),
            active_txns: self.active_txns.load(Ordering::Relaxed),
            active_conns: self.active_conns.load(Ordering::Relaxed),
            repl_connected: self.repl_connected.load(Ordering::Relaxed),
            repl_applied: self.repl_applied.load(Ordering::Relaxed),
            repl_lag: self.repl_lag.load(Ordering::Relaxed),
            repl_status: self.repl_status.lock().unwrap().clone(),
        }
    }

    /// Primary side: a replica (un)subscribed to the WAL stream.
    pub fn repl_connected_inc(&self) {
        self.repl_connected.fetch_add(1, Ordering::Relaxed);
    }

    pub fn repl_connected_dec(&self) {
        let prev = self.repl_connected.fetch_sub(1, Ordering::Relaxed);
        debug_assert!(prev > 0, "repl disconnect without connect");
    }

    /// Replica side: streaming progress + health (hot path: atomics only;
    /// status string changes rarely).
    pub fn set_repl_applied(&self, offset: u64) {
        self.repl_applied.store(offset, Ordering::Relaxed);
    }

    pub fn set_repl_lag(&self, lag_bytes: u64) {
        self.repl_lag.store(lag_bytes, Ordering::Relaxed);
    }

    pub fn set_repl_status(&self, status: &str) {
        *self.repl_status.lock().unwrap() = status.to_string();
    }

    /// `SHOW STATUS` rows: `(Variable_name, Value)` in MySQL conventions.
    /// `like` filters case-insensitively (`%` = any run, `_` = one char).
    pub fn status_rows(
        &self,
        snap: &MetricsSnapshot,
        extra: &EngineExtra,
        like: Option<&str>,
    ) -> Vec<(String, String)> {
        let pool_requests = extra.pool_hits + extra.pool_misses;
        let all = [
            ("Uptime", snap.uptime_secs.to_string()),
            ("Queries", snap.queries.to_string()),
            ("Com_select", snap.com_select.to_string()),
            ("Com_insert", snap.com_insert.to_string()),
            ("Com_update", snap.com_update.to_string()),
            ("Com_delete", snap.com_delete.to_string()),
            ("Com_commit", snap.com_commit.to_string()),
            ("Com_rollback", snap.com_rollback.to_string()),
            ("Com_ddl", snap.com_ddl.to_string()),
            ("Threads_connected", snap.active_conns.to_string()),
            ("Threads_running", snap.active_txns.to_string()),
            ("Innodb_buffer_pool_pages_total", extra.pool_frames.to_string()),
            ("Innodb_buffer_pool_pages_data", extra.pool_resident.to_string()),
            ("Innodb_buffer_pool_reads", extra.pool_misses.to_string()),
            (
                "Innodb_buffer_pool_read_requests",
                pool_requests.to_string(),
            ),
            ("Innodb_os_log_fsyncs", extra.wal_syncs.to_string()),
            ("Innodb_os_log_written", snap.wal_bytes.to_string()),
            (
                "Rpl_semi_sync_master_clients",
                snap.repl_connected.to_string(),
            ),
            ("Rpl_master_wal_offset", extra.master_wal_offset.to_string()),
            ("Rpl_replica_status", snap.repl_status.clone()),
            ("Rpl_replica_lag_bytes", snap.repl_lag.to_string()),
            (
                "Rpl_replica_applied_offset",
                snap.repl_applied.to_string(),
            ),
        ];
        all.into_iter()
            .filter(|(name, _)| match like {
                Some(pat) => status_like(pat, name),
                None => true,
            })
            .map(|(name, val)| (name.to_string(), val))
            .collect()
    }

    /// Prometheus exposition text (v0.0.4) for this snapshot.
    pub fn render_prometheus(&self, snap: &MetricsSnapshot, extra: &EngineExtra) -> String {
        let p = crate::PRODUCT_NAME.to_lowercase();
        let mut out = String::with_capacity(2048);
        let gauge = |o: &mut String, name: &str, help: &str, val: &str| {
            use std::fmt::Write as _;
            let _ = writeln!(o, "# HELP {p}_{name} {help}");
            let _ = writeln!(o, "# TYPE {p}_{name} gauge");
            let _ = writeln!(o, "{p}_{name} {val}");
        };
        let counter = |o: &mut String, name: &str, help: &str, val: &str| {
            use std::fmt::Write as _;
            let _ = writeln!(o, "# HELP {p}_{name} {help}");
            let _ = writeln!(o, "# TYPE {p}_{name} counter");
            let _ = writeln!(o, "{p}_{name} {val}");
        };
        gauge(
            &mut out,
            "uptime_seconds",
            "Seconds since engine start.",
            &snap.uptime_secs.to_string(),
        );
        gauge(
            &mut out,
            "connections_active",
            "Currently registered client connections.",
            &snap.active_conns.to_string(),
        );
        gauge(
            &mut out,
            "transactions_active",
            "Currently open explicit transactions.",
            &snap.active_txns.to_string(),
        );
        for (kind, val) in [
            ("select", snap.com_select),
            ("insert", snap.com_insert),
            ("update", snap.com_update),
            ("delete", snap.com_delete),
            ("commit", snap.com_commit),
            ("rollback", snap.com_rollback),
            ("ddl", snap.com_ddl),
        ] {
            use std::fmt::Write as _;
            let _ = writeln!(
                &mut out,
                "# HELP {p}_queries_total Statements executed by class."
            );
            let _ = writeln!(&mut out, "# TYPE {p}_queries_total counter");
            let _ = writeln!(&mut out, "{p}_queries_total{{type=\"{kind}\"}} {val}");
        }
        {
            use std::fmt::Write as _;
            let _ = writeln!(
                &mut out,
                "# HELP {p}_query_duration_seconds Statement execution latency."
            );
            let _ = writeln!(&mut out, "# TYPE {p}_query_duration_seconds histogram");
            let mut cumulative = 0u64;
            for (i, le) in HIST_LE_LABELS.iter().enumerate() {
                cumulative += snap.hist[i];
                let _ = writeln!(
                    &mut out,
                    "{p}_query_duration_seconds_bucket{{le=\"{le}\"}} {cumulative}"
                );
            }
            let sum = snap.query_us as f64 / 1_000_000.0;
            let _ = writeln!(&mut out, "{p}_query_duration_seconds_sum {sum:.6}");
            let _ = writeln!(
                &mut out,
                "{p}_query_duration_seconds_count {}",
                snap.queries
            );
        }
        counter(
            &mut out,
            "wal_records_total",
            "WAL records appended.",
            &snap.wal_records.to_string(),
        );
        counter(
            &mut out,
            "wal_bytes_total",
            "WAL payload bytes appended.",
            &snap.wal_bytes.to_string(),
        );
        counter(
            &mut out,
            "wal_fsync_total",
            "WAL fsync (sync_data) calls issued by the group-commit syncer.",
            &extra.wal_syncs.to_string(),
        );
        counter(
            &mut out,
            "wal_fsync_duration_seconds_total",
            "Cumulative seconds spent inside WAL fsync.",
            &format!("{:.6}", extra.wal_sync_us as f64 / 1_000_000.0),
        );
        {
            let ratio = if extra.pool_hits + extra.pool_misses == 0 {
                1.0
            } else {
                extra.pool_hits as f64 / (extra.pool_hits + extra.pool_misses) as f64
            };
            gauge(
                &mut out,
                "buffer_pool_hit_ratio",
                "Overflow page pool hit ratio (hits / requests).",
                &format!("{ratio:.4}"),
            );
        }
        gauge(
            &mut out,
            "buffer_pool_pages",
            "Resident overflow pages.",
            &extra.pool_resident.to_string(),
        );
        gauge(
            &mut out,
            "buffer_pool_frames",
            "Configured overflow pool frames.",
            &extra.pool_frames.to_string(),
        );
        counter(
            &mut out,
            "buffer_pool_hits_total",
            "Overflow page pool resident hits.",
            &extra.pool_hits.to_string(),
        );
        counter(
            &mut out,
            "buffer_pool_misses_total",
            "Overflow page pool faults from storage.",
            &extra.pool_misses.to_string(),
        );
        counter(
            &mut out,
            "btree_splits_total",
            "B+ tree node splits (structural changes).",
            &extra.btree_splits.to_string(),
        );
        counter(
            &mut out,
            "btree_merges_total",
            "B+ tree sibling merges on delete.",
            &extra.btree_merges.to_string(),
        );
        counter(
            &mut out,
            "btree_in_place_updates_total",
            "B+ tree zero-split in-place value updates.",
            &extra.btree_in_place.to_string(),
        );
        gauge(
            &mut out,
            "replication_connected_replicas",
            "Replicas currently streaming from this primary.",
            &snap.repl_connected.to_string(),
        );
        gauge(
            &mut out,
            "replication_applied_offset",
            "Replica WAL offset applied locally (primary log space).",
            &snap.repl_applied.to_string(),
        );
        gauge(
            &mut out,
            "replication_lag_bytes",
            "Replica lag behind the primary durable offset, in bytes.",
            &snap.repl_lag.to_string(),
        );
        out
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Case-insensitive `SHOW ... LIKE` matcher: `%` spans any run (including
/// empty), `_` matches exactly one character. No escape sequences — the
/// parser delivers the raw pattern.
pub fn status_like(pattern: &str, name: &str) -> bool {
    let p: Vec<char> = pattern.to_lowercase().chars().collect();
    let t: Vec<char> = name.to_lowercase().chars().collect();
    fn go(p: &[char], t: &[char], pi: usize, ti: usize) -> bool {
        if pi == p.len() {
            return ti == t.len();
        }
        if p[pi] == '%' {
            let mut pj = pi;
            while pj < p.len() && p[pj] == '%' {
                pj += 1;
            }
            if pj == p.len() {
                return true;
            }
            (ti..=t.len()).any(|k| go(p, t, pj, k))
        } else if ti < t.len() && (p[pi] == '_' || p[pi] == t[ti]) {
            go(p, t, pi + 1, ti + 1)
        } else {
            false
        }
    }
    go(&p, &t, 0, 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stmt_kind_classifies_first_keyword() {
        assert_eq!(StmtKind::classify("SELECT 1"), StmtKind::Select);
        assert_eq!(StmtKind::classify("  insert into t values (1);"), StmtKind::Insert);
        assert_eq!(StmtKind::classify("UPDATE t SET a=1"), StmtKind::Update);
        assert_eq!(StmtKind::classify("delete from t"), StmtKind::Delete);
        assert_eq!(StmtKind::classify("COMMIT;"), StmtKind::Commit);
        assert_eq!(StmtKind::classify("rollback"), StmtKind::Rollback);
        assert_eq!(StmtKind::classify("CREATE TABLE t (a INT)"), StmtKind::Ddl);
        assert_eq!(StmtKind::classify("DROP TABLE t"), StmtKind::Ddl);
        assert_eq!(StmtKind::classify("SHOW STATUS"), StmtKind::Other);
        assert_eq!(StmtKind::classify("BEGIN"), StmtKind::Other);
        assert_eq!(StmtKind::classify(""), StmtKind::Other);
    }

    #[test]
    fn counters_and_histogram_bucketize() {
        let m = Metrics::new();
        m.record_query(StmtKind::Select, 50); // <= 100us bucket
        m.record_query(StmtKind::Select, 100); // boundary: first bucket
        m.record_query(StmtKind::Insert, 750); // <= 1000us bucket
        m.record_query(StmtKind::Update, 5_000_000); // +Inf bucket
        m.record_query(StmtKind::Other, 10); // counted, no Com_*
        let s = m.snapshot();
        assert_eq!(s.queries, 5);
        assert_eq!(s.com_select, 2);
        assert_eq!(s.com_insert, 1);
        assert_eq!(s.com_update, 1);
        assert_eq!(s.com_ddl, 0);
        assert_eq!(s.query_us, 50 + 100 + 750 + 5_000_000 + 10);
        assert_eq!(s.hist[0], 3); // 50, 100, 10
        assert_eq!(s.hist[2], 1); // 750
        assert_eq!(s.hist[HIST_BUCKET_COUNT - 1], 1);
        assert_eq!(s.hist.iter().sum::<u64>(), 5);
        m.record_wal(4, 512);
        m.record_wal(1, 64);
        let s = m.snapshot();
        assert_eq!(s.wal_records, 5);
        assert_eq!(s.wal_bytes, 576);
    }

    #[test]
    fn txn_and_conn_gauges_track() {
        let m = Metrics::new();
        m.txn_begin();
        m.txn_begin();
        m.txn_end();
        let id = m.register_process("root", "127.0.0.1");
        let s = m.snapshot();
        assert_eq!(s.active_txns, 1);
        assert_eq!(s.active_conns, 1);
        m.note_command(id, "test", "Query", "SELECT 1");
        let list = m.process_list();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].command, "Query");
        assert_eq!(list[0].state, "executing");
        m.note_idle(id);
        assert_eq!(m.process_list()[0].command, "Sleep");
        m.unregister_process(id);
        let s = m.snapshot();
        assert_eq!(s.active_conns, 0);
        assert!(m.process_list().is_empty());
    }

    #[test]
    fn status_like_cases() {
        assert!(status_like("%", "Uptime"));
        assert!(status_like("Com_%", "Com_select"));
        assert!(status_like("com_%", "COM_INSERT")); // case-insensitive
        assert!(status_like("Com_select", "com_select"));
        assert!(!status_like("Com_select", "Com_insert"));
        assert!(status_like("Thread_", "Threads")); // _ = one char
        assert!(!status_like("Thread_", "Thread"));
        assert!(status_like("U_time", "Uptime"));
        assert!(!status_like("Nope%", "Uptime"));
    }

    #[test]
    fn status_rows_filter_and_shape() {
        let m = Metrics::new();
        m.record_query(StmtKind::Select, 5);
        let snap = m.snapshot();
        let extra = EngineExtra::default();
        let all = m.status_rows(&snap, &extra, None);
        let names: Vec<&str> = all.iter().map(|(n, _)| n.as_str()).collect();
        for want in [
            "Uptime",
            "Queries",
            "Com_select",
            "Threads_connected",
            "Innodb_buffer_pool_reads",
            "Innodb_os_log_fsyncs",
        ] {
            assert!(names.contains(&want), "missing {want}");
        }
        assert_eq!(
            all.iter().find(|(n, _)| n == "Queries").unwrap().1,
            "1"
        );
        let filtered = m.status_rows(&snap, &extra, Some("Com_%"));
        assert!(!filtered.is_empty());
        assert!(filtered.iter().all(|(n, _)| n.starts_with("Com_")));
    }

    #[test]
    fn prometheus_render_shape() {
        let m = Metrics::new();
        m.record_query(StmtKind::Select, 50);
        m.record_query(StmtKind::Insert, 2_000_000);
        let snap = m.snapshot();
        let extra = EngineExtra {
            wal_syncs: 3,
            wal_sync_us: 1_500,
            pool_frames: 8,
            pool_resident: 5,
            pool_hits: 90,
            pool_misses: 10,
            btree_splits: 7,
            btree_merges: 2,
            ..Default::default()
        };
        let text = m.render_prometheus(&snap, &extra);
        let p = format!("{}_", crate::PRODUCT_NAME.to_lowercase());
        for probe in [
            format!("{p}uptime_seconds"),
            format!("{p}queries_total{{type=\"select\"}} 1"),
            format!("{p}queries_total{{type=\"insert\"}} 1"),
            format!("{p}query_duration_seconds_bucket{{le=\"+Inf\"}} 2"),
            format!("{p}query_duration_seconds_count 2"),
            format!("{p}wal_fsync_total 3"),
            format!("{p}buffer_pool_hit_ratio 0.9000"),
            format!("{p}btree_merges_total 2"),
        ] {
            assert!(text.contains(&probe), "missing {probe}");
        }
        assert!(text.contains("# HELP"));
        assert!(text.contains("# TYPE"));
        // Cumulative: the 1ms bucket already holds the fast query.
        let line = text
            .lines()
            .find(|l| l.contains("le=\"0.001\""))
            .expect("1ms bucket");
        assert!(line.ends_with(" 1"), "got {line}");
    }
}
