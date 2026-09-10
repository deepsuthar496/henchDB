# henchDB — Remaining Production Readiness Implementation Plan

> **Baseline:** `3f493a6e78396a9fb014427b27d219a31374a30b`
> **Status:** Strong Release Candidate / Pre-Production
> **Purpose:** This document contains **only the remaining work** required to reach a defensible production-ready v1.0.
>
> Do not re-implement features already completed in previous commits. Focus on verification, failure handling, scalability, security, and release engineering.

---

# 1. Current State

The following major items are already implemented and should be considered **DONE**:

* EBR telemetry
* EBR pending/reclamation metrics
* runtime lock-rank checking
* `max_result_rows`
* `max_result_bytes`
* `max_intermediate_rows`
* `max_intermediate_bytes`
* snapshot TTL
* `CHECK TABLE`
* `CHECK DATABASE`
* logical database hashing
* crash failpoints
* backup/restore verification
* replication crash/restart testing
* corrupt WAL replication testing
* public-bind security refusal
* startup configuration validation
* port collision validation
* TLS configuration validation
* authentication/RBAC foundations
* CI workflow
* operational/recovery/replication/security documentation
* protocol randomized tests
* networking latency optimization

Commit `3f493a6` reports 323 passing tests and a clean release check.

The remaining work below is therefore about proving that those implementations remain correct under significantly more hostile conditions.

---

# 2. P0 — EBR / Unsafe Memory-Safety Verification

## Goal

Turn the current EBR implementation from:

```text
"heavily tested"
```

into:

```text
"production-verified"
```

This is the **highest-priority remaining technical risk**.

---

## 2.1 Audit every `unsafe`

Search:

```bash
rg "unsafe|AtomicPtr|from_raw|into_raw|unsafe impl" crates/
```

For every production `unsafe`:

* document ownership
* document lifetime
* document aliasing assumptions
* document memory ordering
* document reclamation guarantees
* document `Send`/`Sync` reasoning
* add a `// SAFETY:` explanation

### Done when

```text
[ ] Every unsafe block reviewed
[ ] Every unsafe impl justified
[ ] Every raw pointer ownership rule documented
[ ] No unexplained unsafe remains
```

---

## 2.2 Add long-running EBR stress tests

Required workloads:

```text
root split
root collapse
leaf split
leaf merge
leaf unlink
delete/reinsert
overlapping writers
long-lived readers
nested guards
thread termination
rapid pin/unpin
rapid retire/reclaim
```

Run with:

```text
1
2
4
8
16
32
```

threads.

Target:

```text
10M+
operations
```

and preferably:

```text
100M+
operations
```

in nightly testing.

---

## 2.3 Make stress tests reproducible

Every randomized failure must record:

```text
test name
seed
thread count
operation count
database configuration
last operations
```

Example:

```text
HENCHDB_STRESS_SEED=123456
```

A failed nightly run must be reproducible locally.

---

## 2.4 Sanitizer testing

Add dedicated jobs for the concurrency suite.

Use the strongest practical tools available:

```text
AddressSanitizer
ThreadSanitizer
UndefinedBehaviorSanitizer where applicable
Miri-compatible tests
```

Do not necessarily execute all of these on every PR.

Recommended:

```text
PR:
    normal correctness

Nightly:
    sanitizers
    Miri
    long stress
```

### Release blocker

Any sanitizer-detected memory problem blocks release.

---

## 2.5 EBR leak detection under soak

Verify:

```text
retired objects
reclaimed objects
pending objects
oldest retired age
participants
active guards
```

over 24–72 hours.

Required invariant:

```text
pending reclamation may fluctuate
but must not grow without bound
```

---

# 3. P0 — Complete Automated Crash Matrix

Current failpoints are good.

The remaining work is to make the test suite **systematic**.

---

## 3.1 Create failpoint inventory

Maintain one authoritative list:

```text
WAL reserve
WAL write
partial WAL write
WAL fsync
commit marker
install
install frontier
checkpoint
snapshot write
snapshot fsync
snapshot rename
WAL reset
generation update
generation sidecar
archive write
archive seal
recovery replay
replication send
replication receive
replication apply
```

Every durability boundary must either:

```text
have a failpoint
```

or explicitly document why it cannot fail independently.

---

## 3.2 Generate crash scenarios

For every failpoint:

```text
create database
        ↓
execute workload
        ↓
inject failure
        ↓
kill process
        ↓
restart
        ↓
recover
        ↓
CHECK DATABASE
        ↓
logical hash
        ↓
transaction verification
```

Randomize:

```text
transaction size
row count
operation
failpoint
timing
checkpoint timing
snapshot timing
replication timing
```

---

## 3.3 Test transaction classes

Every crash boundary should be tested against:

```text
single-row transaction
multi-row transaction
insert
update
delete
mixed transaction
secondary indexes
foreign keys
concurrent transactions
```

---

## 3.4 Recovery invariants

After recovery:

```text
committed transaction exists
uncommitted transaction does not exist
partially written transaction is atomic
indexes are consistent
foreign keys are consistent
B+ tree is valid
MVCC history is valid
WAL frontier is valid
```

---

## 3.5 Nightly crash campaign

Target:

```text
1,000+
```

random crash/recovery cycles initially.

Increase toward:

```text
10,000+
```

once stable.

Persist failing cases.

---

# 4. P0 — MVCC System-Level Property Testing

Current MVCC differential testing is good.

The remaining requirement is **cross-component MVCC testing**.

---

## 4.1 Reference model

Maintain a simple independent model:

```text
key
 ↓
version history
 ↓
commit timestamp
 ↓
transaction state
```

Never implement the reference model using the same production algorithms.

---

## 4.2 Random workload

Generate:

```text
BEGIN
COMMIT
ROLLBACK
INSERT
UPDATE
DELETE
SELECT
RANGE SCAN
snapshot creation
snapshot release
```

Target:

```text
100K+
```

operations per nightly scenario.

---

## 4.3 Add concurrency

Run:

```text
writer 1
writer 2
writer 3
reader 1
reader 2
GC
checkpoint
```

simultaneously.

---

## 4.4 Test MVCC + GC

Required scenario:

```text
long-lived snapshot
        ↓
millions of updates
        ↓
GC
        ↓
checkpoint
        ↓
restart
        ↓
historical read
```

Verify the historical result.

---

## 4.5 Test MVCC + replication

Verify:

```text
primary visibility
==
replica visibility
```

for:

```text
insert
update
delete
rollback
concurrent commits
historical snapshots
```

---

# 5. P0 — Panic / Error-Path Audit

Perform a complete audit:

```bash
rg "unwrap\(|expect\(|panic!\(|assert!\(|unreachable!" crates/
```

Classify every occurrence.

Allowed:

```text
provably impossible internal invariant
```

Potentially dangerous:

```text
network input
filesystem input
SQL input
authentication
replication data
WAL/snapshot data
configuration
```

---

## 5.1 Fault injection

Test:

```text
disk full
permission denied
short read
short write
broken pipe
connection reset
connection timeout
corrupt WAL
corrupt snapshot
missing file
invalid packet
invalid authentication
```

Required:

```text
controlled error
no corruption
no leaked resources
no unrelated connection failure
```

---

# 6. P0 — Make CI a Real Production Gate

The workflow exists.

The remaining task is to make it authoritative.

---

## 6.1 Required PR checks

Every PR:

```text
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
cargo test --release
cargo check --release
```

Platforms:

```text
Linux
Windows
```

---

## 6.2 Nightly reliability pipeline

Add jobs for:

```text
EBR stress
B+ tree stress
MVCC randomized testing
crash matrix
replication failure tests
backup/restore
PITR
protocol fuzzing
sanitizers
large database tests
```

---

## 6.3 Upload failure artifacts

On failure save:

```text
seed
logs
WAL
database state
crash point
configuration
benchmark output
```

---

## 6.4 Branch protection

Configure:

```text
required status checks
```

so production code cannot merge when required checks fail.

---

# 7. P1 — Real Protocol Fuzzing

Current randomized protocol testing should be retained.

Add actual coverage-guided fuzzing.

---

## 7.1 Fuzz targets

Create fuzz targets for:

```text
MySQL packet parser
PostgreSQL packet parser
SQL parser
prepared statement parser
authentication packets
replication packets
WAL payload parser
```

---

## 7.2 Fuzz invariants

The parser must never:

```text
panic
UB
infinite loop
allocate unbounded memory
corrupt session state
corrupt database state
```

---

## 7.3 Regression corpus

Store minimized failures:

```text
fuzz/corpus/mysql/
fuzz/corpus/postgres/
fuzz/corpus/sql/
fuzz/corpus/replication/
```

Every discovered failure becomes a permanent regression test.

---

# 8. P1 — Replication / HA Failure Matrix

Current replication crash/restart coverage is good.

The remaining work is the full failure matrix.

---

## 8.1 Required scenarios

```text
primary crash
replica crash
network disconnect
network partition
slow network
duplicated WAL
reordered WAL
corrupt WAL
truncated WAL
generation mismatch
replica disk full
primary restart
replica restart
reconnect
repeated reconnect
```

---

## 8.2 Consistency verification

After recovery:

```text
CHECK DATABASE primary
CHECK DATABASE replica
```

Then:

```text
primary logical hash
==
replica logical hash
```

---

## 8.3 Split-brain testing

Test:

```text
primary unavailable
        ↓
replica promotion
        ↓
old primary returns
```

The old primary must not create conflicting writes.

If automatic failover is not supported:

```text
document that clearly
```

and prevent unsafe accidental promotion.

---

# 9. P1 — Backup / PITR Completion

Backup/restore destructive testing is already implemented.

Extend it to a full PITR campaign.

---

## 9.1 PITR scenarios

Test:

```text
full backup
WAL archive
restore
WAL replay
```

Recovery targets:

```text
latest
before transaction
after transaction
after checkpoint
after restart
```

---

## 9.2 Backup corruption

Test:

```text
truncated backup
corrupt backup
missing archive segment
corrupt WAL archive
wrong generation
wrong checksum
partial archive
```

Expected:

```text
clean failure
```

Never silently create incomplete state.

---

# 10. P1 — Query Memory Accounting

`max_intermediate_bytes` is now implemented.

The remaining improvement is to make resource accounting more accurate.

Current row-size estimation should not be treated as an exact process-memory limit.

---

## 10.1 Add a query memory tracker

Create something conceptually equivalent to:

```text
QueryMemoryTracker

reserve(bytes)
release(bytes)
current()
peak()
limit()
```

---

## 10.2 Track major operators

Account for:

```text
hash joins
nested-loop joins
sort
GROUP BY
hash tables
derived tables
subqueries
IN lists
temporary execution structures
```

---

## 10.3 Overflow-safe accounting

Ensure byte calculations cannot overflow.

Use checked or saturating arithmetic.

Never let resource accounting itself panic.

---

# 11. P1 — Query Timeouts

Add:

```text
statement_timeout
```

Optionally:

```text
transaction_timeout
idle_transaction_timeout
```

When timeout occurs:

```text
query stops
memory released
locks released
snapshot released
temporary state released
controlled error returned
```

Test repeated timeout failures for leaks.

---

# 12. P1 — Network Scalability

The latency optimization is implemented.

Now prove it scales.

---

## 12.1 Connection levels

Test:

```text
1
10
100
1,000
10,000
```

connections.

---

## 12.2 Workloads

```text
idle
point select
range select
insert
update
delete
mixed OLTP
pipeline
slow client
burst
```

---

## 12.3 Measure

```text
QPS
p50
p95
p99
CPU
RSS
FDs
context switches
```

Compare:

```text
spin enabled
spin disabled
```

If CPU usage becomes excessive, make the spin budget adaptive.

---

# 13. P1 — Security Test Matrix

Startup security has been improved.

Complete the adversarial security suite.

---

## 13.1 Authentication

Test:

```text
valid credentials
wrong password
unknown user
empty password
expired credential
malformed authentication
repeated failed authentication
concurrent authentication
connection reuse
```

---

## 13.2 RBAC

Test every:

```text
role
×
operation
×
database
×
table
```

Operations:

```text
SELECT
INSERT
UPDATE
DELETE
CREATE
DROP
ALTER
CHECK TABLE
CHECK DATABASE
CHECKPOINT
BACKUP
RESTORE
REPLICATION
ADMIN
```

Verify:

```text
authorization happens before side effects
```

---

# 14. P1 — TLS Operational Testing

Test:

```text
valid certificate
expired certificate
wrong CA
wrong hostname
invalid certificate
invalid private key
TLS handshake failure
certificate rotation
```

Document:

```text
certificate lifecycle
rotation
trust model
supported TLS versions
client authentication
```

---

# 15. P1 — Configuration Hardening

Extend startup validation to verify:

```text
data directory exists/can be created
data directory writable
WAL directory writable
backup directory writable
TLS files exist
TLS files readable
certificate/private-key match
numeric values do not overflow
resource limits are sane
```

All failures must happen before the server begins accepting connections.

---

# 16. P1 — Storage Integrity Stress

`CHECK TABLE` and `CHECK DATABASE` are implemented.

Now run them continuously during stress.

Workload:

```text
writers
+
readers
+
splits
+
merges
+
checkpoint
+
snapshot
```

Periodically execute:

```text
CHECK DATABASE
```

Verify:

```text
B+ tree ordering
indexes
foreign keys
row integrity
logical hash
```

---

# 17. P1 — Resource Leak Testing

Repeatedly perform:

```text
connect
query
transaction
snapshot
checkpoint
disconnect
```

Measure:

```text
RSS
file descriptors
threads
EBR participants
EBR pending objects
snapshots
locks
temporary files
WAL handles
```

No metric may grow indefinitely without an expected cause.

---

# 18. P1 — Observability Completion

Ensure production metrics include:

```text
query latency
query errors
active connections
connection errors

transactions committed
transactions aborted

WAL bytes
WAL flush latency
checkpoint duration
recovery duration

replication lag
replication errors

backup duration
restore duration

snapshot count
oldest snapshot age

EBR participants
EBR active guards
EBR pending reclamation
EBR oldest retired age

lock waits
lock wait duration

memory usage
```

---

# 19. P1 — Health Checks

Provide health states such as:

```text
alive
healthy
recovering
degraded
replication-lagging
storage-error
```

Do not report healthy merely because the process is alive.

---

# 20. P1 — 24-Hour Soak Test

Run a realistic workload for:

```text
24 hours minimum
```

Include:

```text
concurrent clients
OLTP
reads
writes
updates
deletes
joins
GROUP BY
ORDER BY
snapshots
checkpoints
replication
```

Required:

```text
0 crashes
0 deadlocks
0 corruption
0 unbounded memory growth
0 unbounded EBR growth
0 replication divergence
```

---

# 21. P1 — 72-Hour Release Soak

Before v1.0:

```text
72 hours continuous
```

with production-like workload.

Record:

```text
commit SHA
machine
OS
configuration
workload
start time
end time
metrics
logs
```

---

# 22. P1 — Large Database Testing

Test increasingly large datasets:

```text
100 MB
1 GB
10 GB
100 GB where infrastructure permits
```

Measure:

```text
startup
recovery
checkpoint
backup
restore
scan
index operations
replication
```

Look for nonlinear degradation.

---

# 23. P1 — Performance Regression Gate

Maintain fixed benchmark datasets.

Track:

```text
point SELECT
range SELECT
INSERT
UPDATE
DELETE
transactions/sec
JOIN
GROUP BY
ORDER BY
concurrent workload
```

Record:

```text
commit SHA
machine
OS
Rust version
dataset
thread count
configuration
```

Recommended initial release rule:

```text
>10% unexplained regression
=
release blocker
```

---

# 24. P2 — Upgrade Testing

Create databases using the previous supported release.

Then open them with the new release.

Test:

```text
old database
↓
new binary
↓
startup
↓
recovery
↓
CHECK DATABASE
↓
logical hash
↓
read
↓
write
```

Document storage-format compatibility.

---

# 25. P2 — Release Engineering

Define:

```text
semantic versioning
```

Each release must contain:

```text
binary
checksums
release notes
configuration reference
migration notes
security notes
backup instructions
upgrade instructions
```

Record:

```text
Git SHA
Rust version
target platform
build configuration
```

---

# 26. P2 — Reproducible Builds

Document:

```text
toolchain
build environment
target
release flags
dependency state
```

Verify that the release can be reproduced from the tagged source.

---

# 27. P2 — Dependency / Supply Chain Audit

Audit the entire workspace.

Record:

```text
dependency tree
licenses
known vulnerabilities
transitive dependencies
build scripts
release artifacts
```

Automate this in CI.

---

# 28. P2 — Operational Runbooks

Complete runbooks for:

```text
database won't start
WAL corruption
failed recovery
replica lag
replica divergence
disk full
high memory
high CPU
stuck snapshot
EBR growth
failed backup
failed restore
PITR failure
TLS failure
authentication lockout
```

Each runbook must contain:

```text
symptoms
diagnosis
metrics/logs to inspect
recovery procedure
verification procedure
```

---

# 29. P2 — Configuration Documentation

Document every production setting:

```text
bind
ports
threads
connection limits
WAL
checkpoint
snapshot TTL
query limits
memory limits
TLS
authentication
replication
backup
metrics
```

For every setting specify:

```text
default
minimum
maximum
recommended production value
security impact
performance impact
```

---

# 30. P2 — Release Gate Automation

`docs/RELEASE_GATE.md` exists.

Now connect every checkbox to actual evidence.

Bad:

```text
[x] 72h soak completed
```

Good:

```text
[x] 72h soak completed
    commit: abc123
    run: #12345
    duration: 72h
    failures: 0
    artifact: soak-report.tar.gz
```

---

# 31. Final Production Gate

Before declaring v1.0:

```text
[ ] Unsafe code audited
[ ] EBR long stress passes
[ ] Sanitizers pass
[ ] Miri-compatible tests pass
[ ] B+ tree concurrency stress passes

[ ] Complete crash matrix passes
[ ] 1,000+ nightly crash scenarios pass
[ ] ACID recovery invariants pass

[ ] MVCC differential testing passes
[ ] MVCC + GC passes
[ ] MVCC + checkpoint passes
[ ] MVCC + restart passes
[ ] MVCC + replication passes

[ ] Panic/error audit complete
[ ] Fault injection passes

[ ] PR CI required
[ ] Nightly reliability CI operational
[ ] Failure artifacts uploaded

[ ] Protocol fuzzing operational
[ ] Fuzz corpus regression tests pass

[ ] Replication failure matrix passes
[ ] Primary/replica hashes agree
[ ] Split-brain behavior verified

[ ] Backup/restore passes
[ ] PITR passes
[ ] Backup corruption tests pass

[ ] Query memory accounting complete
[ ] Query timeout complete
[ ] Resource leak tests pass

[ ] Network 1/10/100/1K/10K load tests pass
[ ] CPU behavior under opportunistic spin validated

[ ] Authentication matrix passes
[ ] RBAC matrix passes
[ ] TLS failure tests pass
[ ] Configuration validation complete

[ ] Storage integrity stress passes
[ ] Observability complete
[ ] Health checks complete

[ ] 24h soak passes
[ ] 72h release soak passes
[ ] Large database tests pass
[ ] Performance regression gate passes

[ ] Upgrade test passes
[ ] Reproducible release verified
[ ] Supply-chain audit complete
[ ] Operational runbooks complete

[ ] Two independent reviewers approve release
[ ] No unresolved P0/P1 defects
```

---

# 32. Release Decision

The final decision must be:

```text
                ┌────────────────────┐
                │ All P0 gates pass? │
                └─────────┬──────────┘
                          │
                    NO ───┴─── YES
                    │           │
                    ▼           ▼
               NO RELEASE   P1 gates pass?
                                │
                           NO ──┴── YES
                           │          │
                           ▼          ▼
                      NO RELEASE   24–72h soak
                                      │
                                 FAIL ─┴─ PASS
                                  │        │
                                  ▼        ▼
                             NO RELEASE   Release
                                           │
                                           ▼
                                      v1.0 PROD
```

---

# 33. Priority Summary

If development time is limited, use this exact order:

## 🔴 P0 — Do first

```text
1. EBR unsafe audit
2. EBR long-duration stress
3. Sanitizers / Miri
4. Complete crash matrix
5. MVCC system-level property testing
6. Panic/error-path audit
7. Required CI + nightly reliability pipeline
```

## 🟠 P1 — Then

```text
8. Protocol fuzzing
9. Replication failure matrix
10. PITR testing
11. Query memory tracker
12. Query timeouts
13. Network scalability
14. Security matrix
15. TLS operational testing
16. Configuration hardening
17. Storage integrity stress
18. Resource leak testing
19. Observability
20. Health checks
21. 24h/72h soak
22. Large database testing
23. Performance regression gate
```

## 🟡 P2 — Final release preparation

```text
24. Upgrade testing
25. Reproducible builds
26. Supply-chain audit
27. Release artifacts
28. Operational runbooks
29. Configuration documentation
30. Automated release evidence
31. Dual-reviewer release approval
```

---

# 34. The Most Important Rule

Do not interpret:

```text
323/323 tests passing
```

as:

```text
production proven
```

The remaining objective is to turn:

```text
unit-test confidence
```

into:

```text
adversarial + randomized + fault-injected + sanitized + long-duration
confidence
```

The most important final evidence should therefore be:

```text
EBR stress
        +
crash campaign
        +
MVCC differential testing
        +
replication failure testing
        +
fuzzing
        +
24–72h soak
        +
clean CI
```

When those are continuously automated and passing, henchDB moves from a strong database project to a defensible production release.
