# Status Overview — henchDB

This document tracks the current implementation status of henchDB against the architecture specification in `research.md`.

## Subsystem Implementation Status

| Subsystem | Status | Implementation Details |
|---|---|---|
| **B+ Tree Index** | Production Ready | Typed OLC B+ tree (`MAX_KEYS=128`, `MIN_KEYS=64`), optimistic lock coupling, eagerly split interior/leaves, borrow/merge rebalancing on delete, root collapse, EBR-managed memory retirement, 4-byte key-head prefix cache with prefiltered binary search. |
| **Storage & Buffer Pool** | Production Ready | 256 KiB slotted pages + 64-bit swips + write-through cooling pool (`page.rs`); rows >1 KiB spill off-page with epoch-quarantined reuse. Snapshot v2 carries key/value pairs, WAL carries full rows. |
| **Durability & WAL** | Production Ready | Self-describing WAL records, IEEE CRC32 checksum per record, atomic offset sequencer + per-core staging FIFOs (`wal/shard.rs`), group commit syncer (200µs batching window), snapshot checkpoints, HDBA immutable WAL segment archiving, PITR recovery engine (`pitr.rs`). |
| **Concurrency Control** | Production Ready | Multi-Version Concurrency Control (MVCC) with snapshot isolation (`db/mvcc.rs`). `RepeatableRead` default with auto-pinned read snapshots; `ReadCommitted` statement-scoped snapshots. Staged out-of-place writes with atomic batch installation and `visible_epoch` publishing. |
| **Query Optimizer** | Production Ready | Cascades memo query optimizer (`db/memo.rs`): equivalence classes, join commutativity/associativity, predicate pushdown, Scan/HashJoin/NestedLoopJoin/Aggregate lowering, branch-and-bound pruning, `EXPLAIN MEMO`. Cost-based optimizer (`db/cost.rs`) with `ANALYZE TABLE` histogram/stats persistence. |
| **Vectorized Columnar Execution** | Production Ready | Morsel-driven batch engine (`db/batch.rs`): 1024-row `ColumnBatch` chunks, selection vectors, pushed-down filters and aggregations (`SUM`, `COUNT`, `AVG`, `MIN`, `MAX`). |
| **SQL Engine** | Production Ready | Hand-written zero-dependency parser: `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `DDL` (`CREATE`/`DROP`/`ALTER TABLE`, `CREATE INDEX`), `BEGIN`/`COMMIT`/`ROLLBACK`, `CHECK TABLE`, `SHOW TABLES`, `CHECKPOINT`, subqueries (uncorrelated scalar/IN/EXISTS & correlated), CTE/derived joins, system catalog views (`pg_catalog`, `information_schema`). |
| **Server & Wire Protocols** | Production Ready | Bounded worker pool (`--threads`), non-blocking poller parking idle sockets, dual wire frontends: MySQL Protocol (HandshakeV10, COM_QUERY, prepared statements, TLS via rustls) and PostgreSQL Protocol 3.0 (simple & extended query, SSLRequest), Prometheus HTTP exporter (`/metrics`, `/health`), physical WAL streaming replication (`--repl-port`, `--replica-of`). |
| **Security & RBAC** | Production Ready | Role-Based Access Control (`db/privilege.rs`): principals, grant rules, deny-before-execute gate, `GRANT`/`REVOKE`/`CREATE USER`/`DROP USER`/`ALTER USER`, `auth.bin` v2 persistence, public-bind security warnings, execution resource limits (`max_result_rows`, `max_result_bytes`, `max_snapshot_age`). |
| **Diagnostics & Integrity** | Production Ready | Online `CHECK TABLE <name>` verifying B+ tree ordering, schema validity, secondary index bidirectional references, foreign key referential integrity, and deterministic CRC32 dataset hashing. Deterministic failpoint test framework (`failpoint.rs`). |
