# henchDB Production Release Gate

Based on the 10/10 Production Readiness Master Implementation Plan (`docs/assessment.md` §56).

## Pre-Release Verification Checklist

- [x] **P0 — Memory Safety**: Unsafe audit complete (encapsulated strictly to EBR pointer swapping and OS signals with safety contracts).
- [x] **P0 — EBR Telemetry & Leak Detection**: `EbrStats` telemetry wired to `SHOW STATUS` and Prometheus (`ebr_active_guards`, `ebr_retired_objects_total`, `ebr_reclaimed_objects_total`, `ebr_pending_reclamation`).
- [x] **P0 — B+ Tree & MVCC Concurrency**: OLC B+ tree invariants, lock coupling, RepeatableRead/ReadCommitted snapshot isolation verified under concurrency stress.
- [x] **P0 — Comprehensive Crash Matrix**: Automated WAL replay, torn-write truncation, CRC32 verification, and failpoint-driven recovery verified.
- [x] **P0 — Mechanical Lock Ordering**: Global hierarchy strictly enforced (`commit_lock` -> `install_frontier` -> `stage_lock` -> `flush_lock` -> `Node::lock` -> `BufferPool` -> `VersionState` -> `Catalog/Auth` -> `EBR pin`).
- [x] **P1 — Resource Governance**: Enforced `max_result_rows`, `max_result_bytes`, `max_intermediate_rows`, and `max_intermediate_bytes` across joins, sorts, aggregations, subqueries, and derived tables.
- [x] **P1 — Security Defaults**: Automatic refusal to bind to public interfaces (`0.0.0.0`, `::`) with empty administrator credentials unless explicit `--allow-insecure-bind` override is set (§24).
- [x] **P1 — Configuration Validation**: Strict startup validation of ports, connections, worker threads, IP addresses, port collisions, and TLS certificates (§49).
- [x] **P2 — Data Integrity Verification**: Live diagnostic `CHECK TABLE`, `CHECK DATABASE`, and deterministic CRC32 logical database hashing.
- [x] **P2 �% Backup & PITR Verification**: Base snapshot checkpointing, continuous WAL archiving (`HDBA`), and point-in-time recovery roll-forward.
- [x] **P2 �% Replication Consistency**: Physical WAL streaming, replica reconnect, read-only gating, and fence promotion.
- [x] **CI & Code Hygiene**: Zero warnings on `cargo check --release`, 100% passing tests, strict <= 1,500 line file ceiling.
- [ ] 24-Hour Continuous Workload Soak
- [ ] 72-Hour Release Soak
- [ ] Long-Term Upgrade Matrix
- [ ] Dual Reviewer Final Sign-off

## Production Acceptance Criteria

henchDB is certified **Production Ready** only when:
1. All items in the verification checklist are marked complete.
2. Two independent reviewers sign off:
   - **Storage / Concurrency Reviewer**: __________________ Date: __________
   - **Security / Operations Reviewer**: __________________ Date: ___________
