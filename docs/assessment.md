# henchDB — 10/10 Production Readiness Master Implementation Plan

> **Target:** Production-grade v1.0
> **Baseline:** `47f9660a5babe5649cd6ead5b1a52756e4a6e89c`
> **Current state:** Pre-production / release-candidate hardening
> **Target state:** Production-ready for real workloads
> **Primary objective:** Correctness, durability, memory safety, failure recovery, security, scalability, and operational reliability.
>
> **Rule:** Do not add major database features until the production gates below are closed.

---

# 0. Definition of 10/10

henchDB is considered **10/10 production-ready** only when all of these are true:

```text
✓ No known correctness bugs
✓ No known memory-safety bugs
✓ No data corruption under tested failure scenarios
✓ Crash recovery is deterministic and verified
✓ MVCC correctness is property-tested
✓ EBR is stress-tested and instrumented
✓ Replication survives failures and rejoins safely
✓ Backup + restore + PITR are continuously tested
✓ Protocol parsers are fuzz-tested
✓ Resource usage is bounded
✓ Security defaults are safe
✓ Authentication/RBAC are comprehensively tested
✓ Network scalability is load-tested
✓ 24–72h soak tests pass
✓ CI gates every merge
✓ Nightly reliability suite runs automatically
✓ Releases are reproducible
✓ Upgrade/downgrade behavior is documented and tested
✓ Operational documentation is complete
✓ Every production claim has automated evidence
```

---

# 1. Current Completed Work

The following areas are already substantially implemented and must be preserved:

* WAL + CRC
* crash recovery
* failpoints
* snapshot/checkpoint handling
* MVCC
* B+ tree
* EBR stress tests
* randomized MVCC differential testing
* lock hierarchy
* runtime lock-rank checker
* result row/byte limits
* intermediate row limits
* `CHECK TABLE`
* logical database hashing
* backup/restore test
* replication crash/restart test
* corrupted WAL rejection
* authentication/RBAC
* configurable bind address
* metrics
* CI workflow
* protocol randomized tests
* operational documentation
* networking latency optimization

Do not regress these capabilities while completing the remaining work.

---

# 2. P0 — MEMORY SAFETY

## 2.1 Audit all `unsafe`

Search the complete repository for:

```text
unsafe
AtomicPtr
from_raw
into_raw
unsafe impl
transmute
MaybeUninit
ptr::
```

For every occurrence document:

```text
Ownership
Lifetime
Aliasing rules
Memory ordering
Thread-safety invariant
Reclamation invariant
Why the operation is safe
```

Every production `unsafe` block must have an explicit safety comment.

---

# 3. P0 — EBR Verification

Create a dedicated EBR/concurrency test suite.

## Required scenarios

```text
nested guards
thread exit
reader pinned during writer mutation
root split
root collapse
leaf split
leaf merge
leaf unlink
delete/reinsert
concurrent overlapping writes
long-lived reader
rapid reader restart
retire/reclaim cycles
```

## Required scale

Run:

```text
1 thread
2 threads
4 threads
8 threads
16 threads
32 threads
```

with:

```text
10K
100K
1M
10M
100M+
```

operations where practical.

## Reproducibility

Every randomized test must record:

```text
seed
thread count
operation count
test configuration
last successful operation
```

A failure must be reproducible with one command.

---

# 4. P0 — Memory Sanitizers

Create dedicated CI/nightly jobs for:

```text
AddressSanitizer
ThreadSanitizer
UndefinedBehaviorSanitizer where applicable
Miri-compatible tests
```

Do not necessarily run expensive tools on every PR.

Recommended:

```text
PR:
    normal tests

Nightly:
    sanitizers
    Miri
    stress
```

Any sanitizer failure is a release blocker.

---

# 5. P0 — EBR Leak Detection

Add metrics:

```text
ebr.active_guards
ebr.participants
ebr.retired_objects
ebr.reclaimed_objects
ebr.pending_reclamation
ebr.oldest_retired_age
```

Add assertions/tests that detect:

```text
retired objects growing forever
stuck participant
stuck guard
reclamation starvation
```

Run these during long soak tests.

---

# 6. P0 — B+ TREE CONCURRENCY

Build adversarial workloads around structural modifications.

Required:

```text
writers:
    insert
    update
    delete

readers:
    point lookup
    range scan
    full scan
```

Simultaneously force:

```text
splits
merges
root replacement
page redistribution
delete/reinsert
```

Verify continuously:

```text
B+ tree ordering
leaf linkage
parent/child relationships
key uniqueness
record visibility
secondary-index consistency
```

Run integrity checks during the workload, not only afterward.

---

# 7. P0 — COMPREHENSIVE CRASH MATRIX

Create a canonical list of every durability boundary.

Required failpoints:

```text
WAL reserve
WAL allocation
WAL write
partial WAL write
WAL fsync
commit marker
install
install frontier
checkpoint
snapshot temp creation
snapshot write
snapshot fsync
snapshot rename
WAL reset
generation update
generation sidecar write
generation sidecar rename
archive write
archive seal
recovery replay
replication send
replication receive
replication apply
```

Every point must be crash-testable.

---

# 8. P0 — AUTOMATED CRASH TEST GENERATOR

For each test:

```text
create database
↓
execute workload
↓
select random failpoint
↓
inject crash
↓
kill process
↓
restart
↓
recover
↓
CHECK TABLE
↓
verify logical hash
↓
verify transaction semantics
```

Repeat thousands of times.

Randomize:

```text
transaction size
operation type
row count
failpoint
crash timing
checkpoint timing
snapshot timing
replication timing
```

---

# 9. P0 — ACID RECOVERY PROOF

For every crash scenario verify:

```text
Atomicity:
    transaction is fully committed or fully absent

Consistency:
    constraints remain valid

Isolation:
    MVCC visibility is correct

Durability:
    acknowledged commits survive
```

Specifically test:

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

# 10. P0 — MVCC REFERENCE MODEL

Maintain an intentionally simple reference implementation.

Example conceptual model:

```text
Key
  ↓
Version list
  ↓
commit timestamp
  ↓
transaction state
```

Compare production henchDB against this model.

---

# 11. P0 — MVCC PROPERTY TESTING

Randomly generate:

```text
BEGIN
COMMIT
ROLLBACK
INSERT
UPDATE
DELETE
SELECT
RANGE SCAN
SNAPSHOT
```

Test:

```text
10K operations
100K operations
1M+ operations nightly
```

Verify:

```text
visible rows
historical values
deletes
reinserts
repeatable reads
read committed
snapshot expiration
```

---

# 12. P0 — MVCC + GC INTERACTION

Test:

```text
long-lived snapshot
+
millions of updates
+
GC
+
checkpoint
+
restart
```

Ensure:

```text
required history is never reclaimed
expired history is eventually reclaimed
```

No silent fallback to current values is permitted.

---

# 13. P0 — MVCC + REPLICATION

Test:

```text
primary transaction
↓
WAL
↓
replica
↓
MVCC visibility
```

Compare primary and replica logical state after:

```text
committed transaction
rollback
delete
update
concurrent transactions
restart
```

---

# 14. P0 — MECHANICAL LOCK ORDERING

Keep the runtime lock-rank checker.

Required hierarchy must remain explicit:

```text
CommitLock
    ↓
InstallFrontier
    ↓
WalStage
    ↓
WalFlush
    ↓
BTree
    ↓
BufferPool
    ↓
VersionState
    ↓
Catalog/Auth
    ↓
EBR
```

Add tests for:

```text
valid nested acquisition
invalid inversion
release/reacquire
thread isolation
panic cleanup
```

---

# 15. P0 — AUDIT ALL LOCKS

Runtime checking is useful only if every relevant lock participates.

Create a lock inventory:

```text
lock name
type
rank
owner
files
acquisition sites
release behavior
```

Every new lock must have a defined rank.

Add a code-review rule:

```text
NEW LOCK = MUST HAVE LOCK RANK
```

---

# 16. P0 — PANIC POLICY

Search all production code for:

```text
unwrap()
expect()
panic!()
assert!()
unreachable!()
```

Classify each.

Allowed:

```text
provably impossible internal invariant
```

Not allowed for normal external failures:

```text
bad SQL
bad packet
bad credentials
network disconnect
disk failure
corrupt external data
user-controlled values
```

---

# 17. P0 — FAULT-INJECTION ERROR TESTING

Inject:

```text
disk full
permission denied
short write
short read
broken pipe
connection reset
connection timeout
corrupt WAL
corrupt snapshot
missing file
invalid packet
invalid authentication
```

Verify:

```text
controlled error
no corruption
no process-wide crash
no leaked resources
```

---

# 18. P1 — RESOURCE GOVERNANCE

Current limits:

```text
max_result_rows
max_result_bytes
max_intermediate_rows
```

Add:

```text
max_intermediate_bytes
```

Track memory for:

```text
joins
sorts
GROUP BY
hash tables
subqueries
derived tables
IN lists
temporary execution state
```

When exceeded:

```text
abort query
release memory
release locks
release snapshots
return controlled error
```

---

# 19. P1 — QUERY TIMEOUT

Add configurable:

```text
statement_timeout
```

and optionally:

```text
transaction_timeout
idle_transaction_timeout
```

Test:

```text
CPU-heavy query
large join
large sort
blocked query
long snapshot
```

Timeout cleanup must be complete.

---

# 20. P1 — NETWORK LOAD TESTING

Test:

```text
1 connection
10
100
1,000
10,000
```

Measure:

```text
QPS
p50
p95
p99
CPU
RSS
file descriptors
context switches
```

Workloads:

```text
idle
point select
range select
insert
update
delete
mixed OLTP
pipeline
slow clients
bursts
```

---

# 21. P1 — OPPORTUNISTIC SPIN VALIDATION

Test the new networking spin behavior against:

```text
spin enabled
spin disabled
```

At:

```text
low concurrency
medium concurrency
high concurrency
```

Ensure latency improvements do not cause unacceptable CPU amplification.

If necessary make the spin budget adaptive.

---

# 22. P1 — PROTOCOL FUZZING

Create coverage-guided fuzz targets for:

```text
MySQL packets
PostgreSQL packets
SQL parser
prepared statements
authentication
replication packets
WAL payloads
```

Fuzz invariants:

```text
no panic
no UB
no infinite loop
no excessive allocation
no state corruption
```

Every discovered failure becomes a permanent regression test.

---

# 23. P1 — FUZZ CORPUS

Store minimized reproductions:

```text
fuzz/corpus/mysql/
fuzz/corpus/postgres/
fuzz/corpus/sql/
fuzz/corpus/replication/
```

Run corpus regression tests in CI.

---

# 24. P1 — SECURITY DEFAULTS

Public exposure with default/empty administrator credentials should be considered unsafe.

Recommended:

```text
public bind
+
uninitialized credentials
=
startup refusal
```

Allow only through an explicit development override.

Example:

```text
--allow-insecure-bind
```

The warning should remain even with the override.

---

# 25. P1 — AUTHENTICATION TEST MATRIX

Test:

```text
valid login
invalid password
unknown user
empty password
expired credentials
malformed authentication
repeated failures
multiple concurrent logins
connection reuse
```

---

# 26. P1 — RBAC MATRIX

Create a matrix:

```text
role × operation × object
```

Test:

```text
SELECT
INSERT
UPDATE
DELETE
CREATE
DROP
ALTER
CHECK TABLE
CHECKPOINT
BACKUP
RESTORE
REPLICATION
ADMIN
```

Verify deny-before-execute semantics.

No unauthorized operation may have side effects.

---

# 27. P1 — TLS HARDENING

Test:

```text
valid certificate
expired certificate
wrong certificate
wrong CA
hostname mismatch
invalid handshake
certificate rotation
```

Document:

```text
TLS trust model
certificate management
rotation procedure
supported TLS versions
client-authentication behavior
```

---

# 28. P1 — REPLICATION FAILURE MATRIX

Test:

```text
replica crash
primary crash
network disconnect
network partition
slow network
duplicate WAL
reordered WAL
corrupt WAL
truncated WAL
generation mismatch
replica disk full
replica restart
primary restart
reconnect
repeated reconnect
```

---

# 29. P1 — REPLICATION CONSISTENCY

After every successful recovery:

```text
primary CHECK TABLE
replica CHECK TABLE
```

Then:

```text
primary logical hash
==
replica logical hash
```

No divergence is acceptable.

---

# 30. P1 — SPLIT-BRAIN PROTECTION

Explicitly test:

```text
primary unavailable
↓
replica promoted
↓
old primary returns
```

The old primary must not silently accept conflicting writes.

If automatic failover is unsupported:

```text
document this
```

and enforce safe operational behavior.

---

# 31. P1 — BACKUP TESTING

Test:

```text
backup
restore
CHECK TABLE
logical hash
live writes
```

Already present as a strong foundation.

Extend with:

```text
large database
empty database
many indexes
foreign keys
large transactions
concurrent backup
backup during checkpoint
backup during writes
```

---

# 32. P1 — BACKUP CORRUPTION

Test:

```text
truncated backup
corrupt archive
missing archive
wrong checksum
wrong generation
partial backup
```

Recovery must fail safely.

Never silently create an incomplete database.

---

# 33. P1 — PITR

Test:

```text
full backup
+
WAL archive
+
restore
+
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

Validate using:

```text
CHECK TABLE
logical hash
transaction semantics
```

---

# 34. P1 — OBSERVABILITY

Expose:

```text
queries_total
query_errors_total
query_latency
slow_queries
active_connections
connection_errors

transactions_committed
transactions_aborted

wal_bytes
wal_flush_latency
checkpoint_duration
recovery_duration

replication_lag
replication_errors

backup_duration
restore_duration

snapshot_count
oldest_snapshot_age

ebr_pending_reclamation
ebr_oldest_retired_age

lock_waits
lock_wait_duration

memory_usage
```

---

# 35. P1 — STRUCTURED LOGGING

Every startup should report:

```text
version
git SHA
OS
architecture
data directory
bind address
protocols
TLS status
WAL generation
recovery status
replication status
```

Recovery should clearly state:

```text
clean shutdown
```

or:

```text
unclean shutdown — recovery performed
```

---

# 36. P1 — HEALTH ENDPOINT

Provide a health mechanism that distinguishes:

```text
process alive
database healthy
database recovering
replication healthy
replication degraded
storage unhealthy
```

Do not report healthy merely because the process is alive.

---

# 37. P1 — 24-HOUR SOAK

Run:

```text
24h minimum
```

with:

```text
concurrent clients
OLTP
reads
writes
updates
deletes
joins
snapshots
checkpoints
replication
```

Monitor:

```text
RSS
CPU
FDs
WAL
database size
EBR
snapshots
latency
replication lag
errors
```

---

# 38. P1 — 72-HOUR RELEASE SOAK

Before final v1.0 release:

```text
72h continuous workload
```

Required:

```text
0 crashes
0 deadlocks
0 corruption
0 memory leaks
0 replication divergence
0 unrecovered failures
```

Performance must remain stable.

---

# 39. P1 — LARGE DATABASE TESTING

Test progressively:

```text
10 MB
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
index build
replication
```

Look for nonlinear behavior.

---

# 40. P1 — PERFORMANCE REGRESSION GATE

Maintain stable benchmark datasets.

Track:

```text
point select
range scan
insert
update
delete
transaction throughput
join
GROUP BY
ORDER BY
concurrent workload
```

Suggested release rule:

```text
>10% unexplained regression = release blocker
```

Benchmark results must include:

```text
commit SHA
machine
OS
Rust version
configuration
dataset
thread count
```

---

# 41. P1 — FILE / RESOURCE LEAK TESTING

Repeatedly:

```text
open connection
execute workload
close connection
```

Check:

```text
FD count
threads
memory
snapshots
locks
EBR participants
temporary files
WAL handles
```

No resource should grow indefinitely.

---

# 42. P2 — UPGRADE TESTING

Define storage-format compatibility.

Test:

```text
old binary
↓
create database
↓
write data
↓
new binary
↓
open
↓
recover
↓
CHECK TABLE
↓
read/write
```

---

# 43. P2 — RELEASE ENGINEERING

Define:

```text
semantic versioning
```

Every release must contain:

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
Rust version
target platform
build flags
commit SHA
```

---

# 44. P2 — REPRODUCIBLE BUILDS

Document and automate:

```text
toolchain
dependencies
build environment
target
release flags
```

A release should be reproducible from the tagged source.

---

# 45. P2 — DEPENDENCY / SUPPLY-CHAIN AUDIT

Even with a std-only engine, audit the complete workspace.

Track:

```text
dependency tree
licenses
known vulnerabilities
transitive dependencies
build scripts
release artifacts
```

Run this automatically.

---

# 46. P2 — DOCUMENTATION

Required documentation:

```text
README.md
docs/OPERATIONS.md
docs/PRODUCTION_READINESS.md
docs/RECOVERY.md
docs/REPLICATION.md
docs/SECURITY.md
docs/STATUS.md
docs/ASSESSMENT.md
```

Fix naming consistency such as:

```text
assesment.md
```

→

```text
assessment.md
```

---

# 47. P2 — INCIDENT RUNBOOKS

Document procedures for:

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
TLS failure
authentication lockout
```

Each runbook should answer:

```text
What happened?
How do I diagnose it?
What metrics/logs should I inspect?
How do I recover?
How do I verify recovery?
```

---

# 48. P2 — DATA INTEGRITY COMMANDS

Production operators should have:

```text
CHECK TABLE
CHECK DATABASE
logical hash
WAL verification
backup verification
replication consistency check
```

These commands should be safe to run against live data where practical.

---

# 49. P2 — CONFIGURATION VALIDATION

At startup reject invalid configurations.

Examples:

```text
negative/invalid limits
impossible memory limits
invalid bind address
invalid TLS configuration
invalid WAL configuration
invalid replication configuration
invalid snapshot TTL
```

Errors must be clear and actionable.

---

# 50. P2 — GRACEFUL SHUTDOWN

Implement and test:

```text
SIGTERM
Ctrl-C
service stop
```

Shutdown sequence:

```text
stop accepting new connections
↓
finish/abort active work according to policy
↓
flush WAL
↓
persist required metadata
↓
close replication
↓
release resources
↓
exit
```

Verify restart after graceful shutdown.

---

# 51. P2 — UNGRACEFUL SHUTDOWN

Test:

```text
SIGKILL
power-loss simulation
process crash
machine reboot simulation
```

Every recovery must pass:

```text
CHECK TABLE
logical hash
transaction validation
```

---

# 52. P2 — TEST DATABASE GENERATOR

Build reusable generators for:

```text
schemas
tables
indexes
foreign keys
rows
transactions
queries
replication workloads
```

Use these generators across:

```text
MVCC tests
crash tests
backup tests
replication tests
fuzz tests
soak tests
```

This prevents each test suite from developing its own unrealistic workload.

---

# 53. P2 — FAILURE SEED DATABASE

Maintain a regression directory:

```text
tests/regressions/
```

Every production bug must become:

```text
reproducer
+
automated test
+
failure description
```

Never delete a regression test after fixing the bug.

---

# 54. P2 — CI ARCHITECTURE

## Pull Request

Run:

```text
fmt
clippy
unit tests
integration tests
release build
```

## Nightly

Run:

```text
sanitizers
Miri
concurrency stress
MVCC fuzzing
protocol fuzzing
crash matrix
replication failure tests
backup/restore
PITR
large workload
```

## Release

Run:

```text
everything above
+
24h soak
+
performance benchmark
+
upgrade test
+
backup/restore verification
```

---

# 55. RELEASE BLOCKERS

Never release if any of these occur:

```text
memory-safety failure
data corruption
lost committed data
uncommitted data becomes visible
MVCC inconsistency
CHECK TABLE failure
recovery failure
replication divergence
split-brain risk
security bypass
authentication bypass
RBAC bypass
panic from malicious input
unbounded memory growth
unbounded EBR growth
deadlock
backup restore mismatch
PITR mismatch
critical performance regression
```

---

# 56. Final Production Gate

Create:

```text
docs/RELEASE_GATE.md
```

with:

```text
[ ] All P0 items complete
[ ] All P1 items complete
[ ] All required P2 items complete
[ ] CI green
[ ] Nightly suite green
[ ] Sanitizers green
[ ] Fuzz corpus green
[ ] Crash matrix green
[ ] MVCC differential green
[ ] EBR stress green
[ ] Replication failure matrix green
[ ] Backup/restore green
[ ] PITR green
[ ] Security matrix green
[ ] Network load test green
[ ] 24h soak green
[ ] 72h soak green
[ ] Upgrade test green
[ ] Performance regression within limits
[ ] Documentation complete
[ ] Release artifacts reproducible
[ ] No known P0/P1 defects
```

Two independent reviewers should sign off:

```text
Storage/Concurrency reviewer
Security/Operations reviewer
```

---

# 57. Recommended Implementation Order

Do **not** work on all sections simultaneously.

Use this exact sequence:

```text
PHASE 1
═══════
EBR + unsafe audit
        ↓
sanitizers
        ↓
B+ tree concurrency
        ↓
lock-order audit

PHASE 2
═══════
complete crash matrix
        ↓
ACID recovery verification
        ↓
MVCC differential testing
        ↓
MVCC + GC + checkpoint + restart

PHASE 3
═══════
replication failure matrix
        ↓
primary/replica consistency
        ↓
backup/restore
        ↓
PITR
        ↓
split-brain safety

PHASE 4
═══════
query memory limits
        ↓
network load testing
        ↓
protocol fuzzing
        ↓
security/RBAC/TLS testing

PHASE 5
═══════
observability
        ↓
health checks
        ↓
failure runbooks
        ↓
resource leak testing

PHASE 6
═══════
24h soak
        ↓
fix every discovered issue
        ↓
72h soak
        ↓
performance regression gate

PHASE 7
═══════
upgrade testing
        ↓
reproducible release
        ↓
release audit
        ↓
v1.0
```

---

# 58. What NOT To Do Now

Until the production gates are closed, avoid spending significant time on:

```text
new SQL syntax
new SQL functions
new convenience APIs
new protocol features
new optimizer features
new benchmark optimizations
cosmetic refactoring
```

Unless they fix a production blocker, prioritize:

```text
correctness
durability
memory safety
failure recovery
security
resource limits
observability
testing
```

---

# 59. Final 10/10 Acceptance Criteria

The final standard is not:

```text
cargo test
```

It is:

```text
cargo test
        +
sanitizers
        +
fuzzing
        +
property testing
        +
concurrency stress
        +
crash injection
        +
backup/restore
        +
PITR
        +
replication failure testing
        +
security testing
        +
network load testing
        +
24–72h soak
        +
upgrade testing
        +
operational validation
```

The target architecture is:

```text
                    henchDB
                       │
       ┌───────────────┼────────────────┐
       │               │                │
   Correctness      Durability       Security
       │               │                │
     MVCC             WAL             Auth/RBAC
     BTree          Recovery             TLS
      EBR            Backup            Protocol
       │              PITR                │
       └───────────────┼─────────────────┘
                       │
                  Verification
                       │
          ┌────────────┼────────────┐
          │            │            │
       Fuzzing      Stress       Fault Injection
          │            │            │
          └────────────┼────────────┘
                       │
                  Long Soak
                       │
                    CI/CD
                       │
                    v1.0
```

---

# 60. Final Definition

henchDB should only be labeled:

> **Production Ready**

when the repository can demonstrate—not merely claim—that:

```text
arbitrary supported concurrency
        +
arbitrary tested crash points
        +
corrupted external input
        +
replication failures
        +
backup/restore cycles
        +
long-running workloads
```

do not produce:

```text
memory corruption
data corruption
lost acknowledged commits
incorrect MVCC visibility
replication divergence
security bypass
unbounded resource consumption
or unrecoverable operational state.
```

**That is the 10/10 target.**

Until then, use:

> **Pre-production / Release Candidate**

rather than `Production Ready`.
