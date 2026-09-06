//! Engine diagnostics: `SHOW STATUS`, `SHOW ENGINE STATUS`,
//! `SHOW PROCESSLIST`, and the Prometheus exposition assembly.
//!
//! Kept out of `mod.rs` (file-size ceiling): gathering lives here, counter
//! storage and text rendering live in [`crate::metrics`].

use super::{Database, Output};
use crate::metrics::{EngineExtra, Metrics};
use crate::types::Datum;

impl Database {
    /// Shared telemetry handle (process registry, counters, histograms).
    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    /// Register a new client connection for `SHOW PROCESSLIST`.
    /// Returns the connection id the server must pass back on updates.
    pub fn register_process(&self, user: &str, host: &str) -> u64 {
        self.metrics.register_process(user, host)
    }

    /// Record the statement a connection is executing (info truncated).
    pub fn note_command(&self, id: u64, db: &str, command: &str, info: &str) {
        self.metrics.note_command(id, db, command, info);
    }

    /// Mark a connection idle between commands.
    pub fn note_idle(&self, id: u64) {
        self.metrics.note_idle(id);
    }

    /// Drop a connection from the registry.
    pub fn unregister_process(&self, id: u64) {
        self.metrics.unregister_process(id);
    }

    /// Live rollup of WAL / pool / tree / MVCC state for diagnostics.
    /// Locking is brief and read-only; safe on the scrape path.
    fn engine_extra(&self) -> EngineExtra {
        let (wal_syncs, wal_sync_bytes) = self.wal.sync_stats();
        let pool = self.pool.stats();
        let mut extra = EngineExtra {
            wal_syncs,
            wal_sync_bytes,
            wal_sync_us: self.wal.fsync_us(),
            pool_frames: pool.frames,
            pool_resident: pool.resident_pages,
            pool_hits: pool.hits,
            // `faults` are reads that required a storage fetch = misses.
            pool_misses: pool.faults,
            pool_evictions: pool.evictions,
            ..Default::default()
        };
        if let Ok(tables) = self.tables.read() {
            extra.table_count = tables.len();
            for table in tables.values() {
                let s = table.tree_stats();
                extra.btree_splits += s.splits;
                extra.btree_merges += s.merges;
                extra.btree_in_place += s.in_place;
                extra.btree_height_max = extra.btree_height_max.max(s.height);
                extra.btree_nodes += s.nodes;
            }
        }
        let (chains, snapshots) = self.snapshot_counts();
        extra.mvcc_chains = chains;
        extra.mvcc_snapshots = snapshots;
        extra
    }

    /// `SHOW STATUS [LIKE '<pattern>']`: two-column
    /// `(Variable_name, Value)` result in MySQL conventions.
    pub fn show_status(&self, like: Option<&str>) -> Output {
        let snap = self.metrics.snapshot();
        let extra = self.engine_extra();
        let rows = self
            .metrics
            .status_rows(&snap, &extra, like)
            .into_iter()
            .map(|(name, val)| vec![Datum::Text(name), Datum::Text(val)])
            .collect();
        Output {
            columns: vec!["Variable_name".into(), "Value".into()],
            rows,
            message: "OK".into(),
        }
    }

    /// `SHOW ENGINE STATUS` (and the `INNODB` spelling): one
    /// `(Type, Name, Status)` row whose Status blob carries buffer-pool,
    /// WAL/group-commit, B+ tree, MVCC, and connection sections.
    pub fn show_engine_status(&self) -> Output {
        let snap = self.metrics.snapshot();
        let extra = self.engine_extra();
        let pool_requests = extra.pool_hits + extra.pool_misses;
        let hit_ratio = if pool_requests == 0 {
            1.0
        } else {
            extra.pool_hits as f64 / pool_requests as f64
        };
        let avg_query_us = if snap.queries == 0 {
            0
        } else {
            snap.query_us / snap.queries
        };
        let avg_fsync_us = if extra.wal_syncs == 0 {
            0
        } else {
            extra.wal_sync_us / extra.wal_syncs
        };
        let avg_batch_bytes = if extra.wal_syncs == 0 {
            0
        } else {
            extra.wal_sync_bytes / extra.wal_syncs
        };
        let status = format!(
            "{product} {version} engine status\n\
             ------------------------\n\
             UPTIME\n\
             {uptime}s since start, {queries} statements executed\n\
             \n\
             LATENCY\n\
             cumulative execution {query_us}us over {queries} statements (avg {avg}us)\n\
             \n\
             BUFFER POOL\n\
             {resident}/{frames} pages resident, {hits} hits, {misses} misses \
             (hit ratio {ratio:.4}), {evict} evictions\n\
             \n\
             WAL / GROUP COMMIT\n\
             {records} records, {bytes} bytes appended; {syncs} fsyncs \
             (avg batch {batch} bytes, avg fsync {fsync}us, cumulative {sync_us}us)\n\
             \n\
             B+ TREES ({tables} tables)\n\
             max height {height}, {nodes} nodes, {splits} splits, \
             {merges} merges, {in_place} in-place updates\n\
             \n\
             MVCC\n\
             {snapshots} active snapshots, {chains} version chains\n\
             \n\
             CONNECTIONS\n\
             {conns} connected, {txns} transactions open",
            product = crate::PRODUCT_NAME,
            version = crate::VERSION,
            uptime = snap.uptime_secs,
            queries = snap.queries,
            query_us = snap.query_us,
            avg = avg_query_us,
            resident = extra.pool_resident,
            frames = extra.pool_frames,
            hits = extra.pool_hits,
            misses = extra.pool_misses,
            ratio = hit_ratio,
            evict = extra.pool_evictions,
            records = snap.wal_records,
            bytes = snap.wal_bytes,
            syncs = extra.wal_syncs,
            batch = avg_batch_bytes,
            fsync = avg_fsync_us,
            sync_us = extra.wal_sync_us,
            tables = extra.table_count,
            height = extra.btree_height_max,
            nodes = extra.btree_nodes,
            splits = extra.btree_splits,
            merges = extra.btree_merges,
            in_place = extra.btree_in_place,
            snapshots = extra.mvcc_snapshots,
            chains = extra.mvcc_chains,
            conns = snap.active_conns,
            txns = snap.active_txns,
        );
        Output {
            columns: vec!["Type".into(), "Name".into(), "Status".into()],
            rows: vec![vec![
                Datum::Text("InnoDB".into()),
                Datum::Text(String::new()),
                Datum::Text(status),
            ]],
            message: "OK".into(),
        }
    }

    /// `SHOW PROCESSLIST`: one row per registered connection —
    /// `(Id, User, Host, db, Command, Time, State, Info)`.
    pub fn show_processlist(&self) -> Output {
        let rows = self
            .metrics
            .process_list()
            .into_iter()
            .map(|p| {
                vec![
                    Datum::Int(p.id as i64),
                    Datum::Text(p.user),
                    Datum::Text(p.host),
                    Datum::Text(p.db),
                    Datum::Text(p.command),
                    Datum::Int(p.time_secs as i64),
                    Datum::Text(p.state),
                    Datum::Text(p.info),
                ]
            })
            .collect();
        Output {
            columns: vec![
                "Id".into(),
                "User".into(),
                "Host".into(),
                "db".into(),
                "Command".into(),
                "Time".into(),
                "State".into(),
                "Info".into(),
            ],
            rows,
            message: "OK".into(),
        }
    }

    /// Full Prometheus exposition text for `/metrics` scrapes.
    pub fn prometheus_text(&self) -> String {
        let snap = self.metrics.snapshot();
        let extra = self.engine_extra();
        self.metrics.render_prometheus(&snap, &extra)
    }
}
