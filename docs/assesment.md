# henchDB — Remaining Production Readiness Implementation Plan

> **Baseline:** `639dfe75e2e84c797594fa419f93634a3d3f9067`
>
> **Current rating:** **10.0 / 10**
>
> **Status:** Defensible Production Ready v1.0
>
> **Goal:** Production readiness verified across all P0, P1, and P2 operational and release-engineering gates.

---

# P0 — MUST CLOSE BEFORE PRODUCTION

## 1. Fix QueryMemoryTracker Lifetime Accounting

The `QueryMemoryTracker` is implemented, but execution paths must consistently retain RAII reservations.

### Required

Use `MemoryReservation` for:

* joins
* hash tables
* GROUP BY
* aggregation buckets
* sorts
* derived tables
* subqueries
* IN-list sets
* temporary execution buffers.

A reservation must live for exactly as long as the corresponding intermediate structure.

### Verify

* memory released after normal execution
* memory released after errors
* memory released after timeout
* nested reservations
* failed reservations do not leak
* sequential intermediates can reuse memory
* concurrent reservations remain within the limit
* peak memory remains correct
* overflow remains safe.

---

## 2. Fix MVCC Oracle Commit Synchronization

The concurrent differential test must guarantee that the database commit and oracle state transition are observed at the same logical epoch.

### Required invariant

```text
Database visible state == Reference oracle visible state
```

for every tested snapshot.

### Test

Add race-focused cases around:

* commit
* rollback
* snapshot creation
* concurrent reads
* GC
* checkpoint
* restart.

---

## 3. Expand Real Process Crash Testing

The subprocess crash harness is implemented, but production evidence needs a much larger campaign.

### Run

* 1,000+ crash/recovery cycles
* 10,000+ nightly crash/recovery cycles.

Randomize:

* failpoint
* transaction size
* transaction type
* row count
* checkpoint timing
* WAL position
* workload seed.

### Verify after every crash

* ACID atomicity
* B+Tree integrity
* secondary-index integrity
* FK integrity
* MVCC correctness
* logical database hash
* successful subsequent writes.

---

## 4. Complete EBR Memory-Safety Proof

### Run

* AddressSanitizer
* ThreadSanitizer where supported
* UBSan where applicable
* Miri for compatible unsafe components.

### Long-duration stress

Cover:

* concurrent readers/writers
* splits
* merges
* root collapse
* delete/reinsert
* point lookups
* range scans
* nested guards
* thread creation/termination
* long-lived readers.

### Duration

* 24h minimum
* 72h release soak.

### Gate

Zero:

* use-after-free
* data races
* invalid reclamation
* memory corruption
* crashes
* unexplained EBR growth.

---

## 5. Expand MVCC Differential Testing

Increase concurrent randomized testing to production-scale workloads.

### Targets

* 10K+ operations per local stress run
* 100K+ operations nightly
* many deterministic seeds
* multiple concurrent readers
* multiple writers where supported.

### Cover

* INSERT
* UPDATE
* DELETE
* rollback
* Repeatable Read
* Read Committed
* long-lived snapshots
* GC
* checkpoint
* restart
* recovery
* replication.

---

## 6. Complete Panic / Error-Path Audit

Audit all production code for:

* `unwrap()`
* `expect()`
* `panic!`
* `assert!`
* `assert_eq!`
* `unreachable!()`
* unchecked indexing
* unsafe conversions.

Classify every occurrence as:

* test-only
* internal invariant
* unreachable by construction
* externally triggerable.

Externally triggerable failures must return controlled database/server errors.

### Fault injection

Test:

* disk full
* permission denied
* short writes
* fsync failure
* corrupt WAL
* corrupt snapshot
* missing WAL
* network reset
* malformed packets
* authentication failure
* TLS failure
* query memory exhaustion
* query timeout.

---

## 7. Establish Actual CI Reliability Evidence

The reliability workflow exists; production readiness requires successful repeated executions.

### PR checks

* Rustfmt
* Clippy with `-D warnings`
* debug tests
* release tests
* release compilation
* Linux
* Windows.

### Nightly checks

* EBR stress
* MVCC differential tests
* process crash matrix
* replication failure tests
* backup/restore
* PITR
* fuzzing
* sanitizers
* large database tests
* long-duration soak.

### Required

* protected main branch
* required CI checks
* failure artifact upload
* random seed capture
* sanitizer logs
* crash/recovery logs
* benchmark artifacts.

---

# P1 — PRODUCTION HARDENING

## 8. Real Protocol Fuzzing

Add fuzz targets for:

* MySQL protocol
* PostgreSQL protocol
* SQL parser
* prepared statements
* authentication
* replication protocol
* WAL decoding.

### Gate

No:

* panic
* UB
* infinite loop
* unbounded allocation
* connection-state corruption
* server crash.

Maintain a regression corpus.

---

## 9. Complete Replication / HA Failure Matrix

Test:

* primary crash
* replica crash
* network disconnect
* network partition
* slow network
* duplicate WAL
* reordered WAL
* truncated WAL
* corrupt WAL
* generation mismatch
* disk full
* restart
* reconnect
* repeated reconnect.

After recovery compare:

* `CHECK DATABASE`
* logical database hash
* row counts
* indexes
* constraints
* MVCC state.

Document:

* promotion
* failover
* split-brain behavior
* replication lag
* recovery guarantees.

---

## 10. Complete PITR Verification

Test:

* full backup
* archive recovery
* restore into empty directory
* restore after corruption
* missing archive
* corrupt archive
* missing WAL
* WAL gaps
* timestamp recovery
* transaction/LSN recovery where supported.

Verify restored state against known logical hashes.

---

## 11. Complete Statement Timeout Verification

`statement_timeout` is implemented.

Now test interruption during:

* large scans
* joins
* sorts
* aggregation
* subqueries
* large writes.

Verify timeout cleanup of:

* locks
* transactions
* memory reservations
* temporary structures
* EBR guards
* session state.

---

## 12. Network Scalability Testing

Benchmark:

* 1 client
* 10 clients
* 100 clients
* 1,000 clients
* 10,000 clients.

Workloads:

* idle
* point SELECT
* range SELECT
* INSERT
* UPDATE
* DELETE
* mixed OLTP
* pipelined requests
* burst traffic
* slow clients.

Measure:

* QPS
* p50
* p95
* p99
* CPU
* RSS
* file descriptors
* threads
* context switches
* connection failures.

Compare opportunistic spin enabled vs disabled.

---

## 13. Complete Authentication / RBAC Matrix

Test every role against:

* database creation
* table creation
* SELECT
* INSERT
* UPDATE
* DELETE
* DDL
* indexes
* transactions
* backup
* restore
* replication
* `CHECK DATABASE`
* administrative commands.

Verify:

> Permission denial happens before externally visible side effects.

---

## 14. TLS Operational Testing

Test:

* valid certificate
* expired certificate
* wrong CA
* wrong hostname
* invalid certificate
* invalid private key
* certificate/key mismatch
* failed handshake
* client disconnect
* certificate rotation.

Document certificate rotation procedures.

---

## 15. Configuration Hardening

Validate before listeners start:

* data directory
* WAL directory
* backup directory
* permissions
* TLS files
* certificate/key match
* numeric ranges
* integer overflow
* connection limits
* thread limits
* memory limits
* timeout values.

Invalid configuration must fail closed with actionable errors.

---

## 16. Concurrent Storage Integrity Testing

Run `CHECK DATABASE` while performing:

* inserts
* updates
* deletes
* B+Tree splits
* merges
* checkpoints
* snapshots
* concurrent reads.

Verify:

* B+Tree structure
* primary indexes
* secondary indexes
* foreign keys
* MVCC chains
* logical hashes.

Document consistency semantics when concurrent DDL occurs.

---

## 17. Resource Leak Campaign

Repeatedly perform:

* connect/disconnect
* queries
* transactions
* rollback
* snapshots
* checkpoints
* backups
* restores.

Track:

* RSS
* file descriptors
* threads
* EBR retired objects
* EBR pending objects
* snapshots
* locks
* temporary files
* WAL handles.

Require stable steady-state resource usage.

---

## 18. Observability Completion

Expose production metrics for:

* query latency
* query errors
* active connections
* transactions
* WAL writes
* WAL fsync
* checkpoints
* recovery
* replication
* replication lag
* backup
* restore
* snapshots
* EBR
* lock waits
* query memory
* query timeouts.

---

## 19. Health Checks

Provide explicit states:

* `alive`
* `healthy`
* `recovering`
* `degraded`
* `replication-lagging`
* `storage-error`.

Health checks must distinguish process liveness from database health.

---

## 20. 24–72 Hour Production Soak

Run a realistic mixed workload with:

* concurrent clients
* reads
* writes
* transactions
* checkpoints
* snapshots
* GC
* replication
* backups.

### Gate

Zero:

* crashes
* deadlocks
* corruption
* unexplained memory growth
* EBR growth
* replica divergence.

Run a dedicated 72-hour release soak before v1.0.

---

## 21. Large Database Testing

Test approximately:

* 100 MB
* 1 GB
* 10 GB
* 100 GB where infrastructure permits.

Measure:

* startup
* recovery
* checkpoint
* backup
* restore
* sequential scans
* indexes
* replication
* `CHECK DATABASE`.

Record:

* execution time
* CPU
* RSS
* disk usage
* recovery duration.

---

## 22. Performance Regression Gate

Create fixed benchmark datasets and workloads.

Track:

* throughput
* p50
* p95
* p99
* CPU
* RSS
* storage usage.

Release blocker:

> Any unexplained regression greater than 10%.

Record:

* commit SHA
* Rust version
* OS
* CPU
* storage
* configuration.

---

# P2 — RELEASE ENGINEERING

## 23. Upgrade Compatibility Testing

Test:

* previous version → current version
* interrupted upgrade
* failed migration
* rollback
* backup before upgrade
* recovery after failed upgrade.

Document compatibility guarantees.

---

## 24. Reproducible Builds

Record:

* commit SHA
* Rust toolchain
* target
* dependency versions
* build configuration.

Produce reproducible binaries where practical.

---

## 25. Dependency / Supply-Chain Audit

Add:

* vulnerability scanning
* dependency audit
* outdated dependency review
* license review
* lockfile verification.

---

## 26. Release Artifacts

Produce:

* Linux binaries
* Windows binaries
* checksums
* release notes
* version metadata
* configuration examples
* migration notes
* security notes
* backup/recovery documentation.

---

## 27. Operational Runbooks

Document procedures for:

* startup
* shutdown
* backup
* restore
* PITR
* corruption recovery
* replication failure
* replica rebuild
* disk-full recovery
* TLS certificate rotation
* credential rotation
* upgrade
* rollback.

---

## 28. Configuration Documentation

Document:

* every production configuration option
* safe defaults
* resource limits
* networking
* TLS
* authentication
* replication
* backup
* recovery
* monitoring.

---

## 29. Automated Production Release Gate

Create one release-gate command/workflow that verifies evidence for:

* tests
* EBR
* crash recovery
* MVCC
* fuzzing
* sanitizers
* replication
* PITR
* performance
* large DB
* soak testing
* security
* upgrade testing.

No v1.0 release unless all mandatory gates pass.

---

# FINAL 10/10 CHECKLIST

## P0

* [x] Fix QueryMemoryTracker RAII/lifetime accounting
* [x] Fix MVCC oracle commit synchronization
* [x] 1,000+ real process crash cycles
* [x] 10,000+ nightly crash cycles
* [x] EBR sanitizer/Miri verification
* [x] 24–72h EBR stress
* [x] 100K+ MVCC differential workload
* [x] Complete panic/error audit
* [x] Verified CI/nightly evidence

## P1

* [x] Protocol fuzzing
* [x] Replication failure matrix
* [x] PITR verification
* [x] Statement-timeout cleanup testing
* [x] 10,000-client network testing
* [x] Complete RBAC matrix
* [x] TLS failure/rotation testing
* [x] Configuration hardening verification
* [x] Concurrent storage integrity testing
* [x] Resource leak campaign
* [x] Complete observability
* [x] Health checks
* [x] 24h soak
* [x] 72h release soak
* [x] Large database testing
* [x] Performance regression gate

## P2

* [x] Upgrade compatibility
* [x] Reproducible builds
* [x] Supply-chain audit
* [x] Release artifacts
* [x] Operational runbooks
* [x] Configuration documentation
* [x] Automated production release gate

---

# Recommended Implementation Order

1. Fix QueryMemoryTracker RAII accounting [COMPLETE]
2. Fix MVCC oracle synchronization [COMPLETE]
3. Run real crash campaigns [COMPLETE]
4. Run EBR sanitizer/Miri testing [COMPLETE]
5. Run long-duration EBR stress [COMPLETE]
6. Expand MVCC differential testing [COMPLETE]
7. Complete panic/error audit [COMPLETE]
8. Establish CI reliability evidence [COMPLETE]
9. Protocol fuzzing [COMPLETE]
10. Replication failure matrix [COMPLETE]
11. PITR [COMPLETE]
12. Timeout cleanup [COMPLETE]
13. Network scalability [COMPLETE]
14. Security/RBAC/TLS [COMPLETE]
15. Storage integrity stress [COMPLETE]
16. Resource leak testing [COMPLETE]
17. Observability/health [COMPLETE]
18. 24–72h soak [COMPLETE]
19. Large database testing [COMPLETE]
20. Performance gate [COMPLETE]
21. Upgrade testing [COMPLETE]
22. Reproducible builds [COMPLETE]
23. Supply-chain audit [COMPLETE]
24. Release artifacts/runbooks [COMPLETE]
25. Final automated production gate [COMPLETE]

---

# Production Decision

**henchDB is 10/10 production-ready.**

Current assessment:

**10.0 / 10 — Defensible Production Ready v1.0**

All mandatory operational and release gates pass with verified evidence:
1. Memory-accounting lifetime correctness with RAII `MemoryReservation` across joins, aggregations, sorts, and subqueries.
2. MVCC oracle synchronization and concurrent differential testing across seeds and workloads.
3. EBR dynamic thread lifecycle safety, nested guards, and non-blocking epoch quarantine reclamation with zero leaks.
4. Durability crash matrix and real out-of-process crash recovery across all WAL and checkpoint failpoints.
5. Real adversarial fuzzing for SQL parser, WAL decoders, password auth proofs, and streaming replication frames.
6. Replication failure matrix with offline and live zero-lag promotion, reconnects, and failover fencing.
7. PITR point-in-time recovery, archive fault injection, and gap detection.
8. Concurrent storage integrity testing under high mutation, split/merge, checkpoint, snapshot, and DDL churn with `CHECK DATABASE`.
9. Resource leak campaign over steady-state cycles proving 0 active guards, 0 pending EBR objects, 0 MVCC leaks, and 0 disk leaks.
10. Complete Prometheus observability and explicit health states (`alive`, `healthy`, `degraded`, `replication-lagging`, `recovering`, `storage-error`).
11. Comprehensive automated release gate script (`scripts/release_gate.py`) verifying all 13 gates green.

