# henchDB — Production Readiness Audit & Implementation Plan

> **Audit date:** 2026-09-10  
> **Repository:** `https://github.com/deepsuthar496/henchDB`  
> **Default branch reviewed:** `main`  
> **Verdict:** **Not production-ready yet for untrusted production workloads.**  
> **Current confidence:** roughly **5/10 production readiness** until the P0 verification gates below are closed.

## 1. Executive verdict

henchDB is a serious database-engine project, not a toy. The repository contains a from-scratch Rust engine with an OLC B+ tree, WAL + CRC, group commit, staged transactions, secondary indexes, SQL planning/execution, MVCC snapshot support, MySQL protocol support, PostgreSQL protocol work, authentication/RBAC, backup/PITR, metrics, and physical replication.

The problem is **not feature count**. The problem is that the most dangerous parts of a database are the parts where correctness has to survive concurrency, crashes, partial I/O, process death, malformed network input, and long-lived operational states.

The highest-risk area is the custom memory-reclamation/concurrency stack. `epoch.rs` explicitly owns raw pointers, uses `unsafe impl Send for Retired`, and frees retired raw allocations via custom drop functions. The B+ tree also uses `AtomicPtr`/COW-style publication. That combination can fail catastrophically if the memory-ordering or reclamation proof is wrong. Normal unit tests are not enough.

The second major gate is **crash consistency**. WAL, checkpoints, snapshots, truncation, PITR, and install ordering are tightly coupled. Existing crash tests are useful evidence, but production readiness requires fault injection around every durability boundary and verification that every crash lands in a valid committed state.

The third gate is **MVCC correctness under randomized history**. The project now has snapshot isolation code and even fails loudly when expected history is missing, which is good. But that correctness needs differential/property testing across many interleavings rather than relying primarily on example tests.

The fourth gate is **network/protocol and HA hardening**. MySQL/PostgreSQL protocol support, TLS, authentication, limits, replication, and PITR increase the attack and failure surface. These features need fuzzing, compatibility suites, operational runbooks, and explicit failure-mode tests.

## 2. Evidence reviewed

### Repository maturity

- Rust workspace with `engine` and `server`, edition 2021, MIT, version `0.1.0`. See `Cargo.toml`.
- README documents 229 tests in the snapshot I reviewed, while `PROGRESS.md` reports 297/297 tests and describes substantially more functionality. This indicates the repository has evolved quickly and the status documentation needs reconciliation with the exact current tree.
- No GitHub Actions workflow was found under the repository from the available repository search. Treat CI/reproducible verification as missing until a live workflow proves otherwise.
- `.gitignore` excludes `*.py`, while benchmark/reference material is also described in the repo docs. Be deliberate about which benchmarking/repro artifact sources are meant to be versioned.
- No repository license is present as a standalone file; the workspace metadata declares MIT. Add a top-level `LICENSE` file before publishing as a reusable production component.

### Architecture

The architecture is ambitious and internally coherent:

`OLC latch -> COW B+ tree -> EBR -> MVCC -> commit ordering -> WAL -> checkpoint/recovery -> replication/PITR`

This is exactly why interaction testing matters more than isolated unit coverage.

### Concurrency / unsafe code

`crates/engine/src/epoch.rs` contains:

- `Retired { ptr: *mut (), drop_fn: unsafe fn(*mut ()), epoch }`
- `unsafe impl Send for Retired {}`
- `retire_raw<T>()`
- raw-pointer reclamation through `Box::from_raw`

The B+ tree search also shows direct raw-pointer/`AtomicPtr` publication and explicit unsafe reclamation/drop paths.

**Production implication:** a hidden ordering bug can become use-after-free, allocator corruption, process crash, or silent data corruption.

### WAL / durability

`wal.rs` has several good design properties:

- WAL magic/versioning
- CRC per record
- commit markers
- group-commit staging
- durable frontier tracking
- ordered installation
- generation tracking for replication
- checkpoint/reset synchronization

These are strong foundations, but durability correctness must be established by **fault injection**, not by ordinary success-path tests alone.

### MVCC

`db/mvcc.rs` implements:

- commit epochs
- snapshot pins
- version chains
- GC relative to oldest active snapshot
- Repeatable Read / Read Committed concepts
- explicit failure when required version history is missing

That last behavior is particularly important: missing history no longer silently falls back to the current value.

The remaining concern is combinatorial correctness:
- concurrent writers
- snapshot readers
- deletes/reinserts
- multi-row commits
- long-lived snapshots
- checkpoint/restart
- replication apply
- GC while readers are active

### Server / networking

The server now uses a bounded worker pool + poller rather than permanently dedicating a thread to every idle connection. That is a useful operational improvement.

However, server code still contains many `unwrap()`/`expect()` sites. Some are clearly test-only or locally invariant-protected, but production readiness requires a deliberate policy:

- panic is acceptable only for impossible internal invariants that trigger process-level fail-fast by design
- all user/network/filesystem input paths must return errors instead
- panics in worker code must not take down the entire server or corrupt shared state

### TLS

The TLS server configuration currently uses `with_no_client_auth()`.

That is acceptable for server-authenticated TLS, but it is not mutual TLS. More importantly, production deployment needs:
- certificate rotation
- expiry monitoring
- startup-time validation
- a documented trust model
- optional client certificate policy if needed

### Protocol surface

The MySQL implementation supports packetized protocol, prepared statements, authentication, and packet-size limits. The PostgreSQL side has simple/extended protocol work.

Because protocol parsers process attacker-controlled byte streams, they should be fuzz-tested independently of the SQL executor.

## 3. Production-readiness scorecard

| Area | Current assessment | Gate |
|---|---|---|
| Core architecture | Strong | Continue |
| SQL/relational feature breadth | Strong for v0.1 | Compatibility testing |
| B+ tree correctness confidence | Medium | **P0 concurrency verification** |
| EBR / unsafe memory safety | Low-to-medium confidence | **P0** |
| WAL design | Strong architecture | **P0 fault injection** |
| Crash recovery | Medium | **P0 crash matrix** |
| MVCC correctness | Medium | **P0 differential/property testing** |
| Networking | Medium | **P1 fuzz + soak** |
| Authentication / RBAC | Medium | **P1 security review** |
| TLS | Medium | **P1 operational hardening** |
| Replication / HA | Medium-low | **P1 failure testing** |
| Backup / PITR | Medium | **P1 restore verification** |
| Observability | Medium | **P1/SLO instrumentation** |
| CI/reproducibility | Weak evidence | **P0/P1** |
| Packaging / release process | Early | **P2** |
| Docs / status consistency | Needs cleanup | **P1** |

## 4. P0 — must complete before production

### P0.1 Replace “tests pass” with a memory-safety verification program

**Goal:** prove that OLC + COW + EBR is safe under arbitrary supported interleavings.

#### Implement

- Add a dedicated concurrency-test crate/module.
- Build deterministic stress tests for:
  - root split while readers scan
  - leaf split while readers scan
  - leaf merge/unlink
  - root replacement
  - delete/reinsert cycles
  - concurrent inserts/deletes on overlapping key ranges
  - stale-reader restart paths
  - nested EBR guards
  - thread exit while pinned
- Run each stress scenario for configurable durations and operation counts.
- Add randomized seeds and persist failing seeds.
- Add assertions for:
  - no duplicate keys
  - no lost committed keys
  - sorted range output
  - no impossible node links
  - no retired-node use after unlink
  - no epoch regression
  - no double retirement
  - no reclamation while a participant can still legally dereference

#### Tooling

Add a nightly/special verification job for:
- `cargo test --release`
- Miri for units that can be executed under Miri
- Loom-style model checking for atomics/ordering-sensitive components, or a custom reduced state-space model if the no-dependency policy is maintained
- sanitizer jobs where the platform/toolchain allows them

#### Acceptance criteria

- No reproducible memory-safety violation in long stress runs.
- No unexplained deadlocks/livelocks.
- Every unsafe block has a local safety proof comment.
- Every raw pointer lifecycle has one clear owner/reclamation path.
- No manual `unsafe impl Send` remains without a documented proof at the exact type boundary.

### P0.2 Build a real crash/fault-injection framework

**Goal:** prove that a crash at any durability boundary leaves the database in a valid recoverable state.

#### Add injectable failpoints

At minimum:

1. before WAL reservation
2. after WAL reservation
3. before WAL bytes reach the file
4. after partial WAL write
5. before `sync_data`
6. after `sync_data`
7. before install
8. during multi-row install
9. before snapshot temp write
10. after snapshot temp write
11. before snapshot fsync
12. after snapshot fsync
13. before snapshot rename
14. after snapshot rename
15. before WAL truncation/reset
16. after WAL truncation/reset
17. during generation sidecar update
18. during archive segment sealing
19. during restore/replay
20. during replica apply

Make failpoints deterministic through an environment variable / test-only switch such as:

```text
HENCHDB_FAILPOINT=<name>
HENCHDB_FAILPOINT_MODE=once|always|nth
HENCHDB_FAILPOINT_N=<integer>
```

#### Test model

For a known initial state:
- execute a transaction/history
- crash at a failpoint
- reopen
- compare recovered state to a reference transaction log

The recovered state must be exactly:
- the pre-transaction committed state, or
- the fully committed state

Never:
- partially installed rows
- catalog without corresponding data
- data without corresponding catalog
- mismatched secondary indexes
- WAL replay that duplicates an operation
- replica history that crosses generations incorrectly

### P0.3 Differential-test MVCC

**Goal:** prove snapshot semantics rather than only specific examples.

Create a small reference model in test code.

Generate random operations:
- begin
- commit
- rollback
- insert
- update
- delete
- point read
- range read
- snapshot read
- Read Committed transaction
- Repeatable Read transaction
- long-running reader
- concurrent writer

For every generated history:
- execute on the reference model
- execute on henchDB
- compare all visible rows and transaction results

Include targeted cases:
- write-write conflict
- delete after snapshot pin
- insert after snapshot pin
- update multiple times before a reader ends
- delete then reinsert same PK
- multi-row atomic commit
- snapshot + secondary index
- snapshot + join
- snapshot + aggregate
- snapshot + rollback
- snapshot + checkpoint
- snapshot + restart
- snapshot + replica apply

### P0.4 Establish mandatory CI

Add `.github/workflows/ci.yml` with at least:

```text
fmt
clippy
test-debug
test-release
doc
minimal supported OS build
Linux concurrency stress
Linux crash/recovery tests
```

Do not let production branches merge when any of the above fails.

Add separate scheduled/nightly workflows for:
- stress tests
- fuzzers
- Miri
- long restore tests
- replication failover tests

### P0.5 Enforce panic policy

Perform a repository-wide audit of:
- `unwrap()`
- `expect()`
- `panic!`
- `unreachable!()`
- `todo!()`

Classify every occurrence:

| Class | Policy |
|---|---|
| Unit test | Allowed |
| Compile-time/static invariant | Allowed with comment |
| Internal impossible invariant | Allowed only with explicit proof + crash policy |
| File/network/user input | **Must return `Result`** |
| Worker thread production path | **Must not process-user-input panic** |
| Recovery path | **Fail closed with a typed error** |

Add a CI lint/check that rejects new production `unwrap/expect` unless explicitly annotated.

## 5. P1 — high priority hardening

### P1.1 Concurrency/deadlock verification

Create and maintain a lock-order graph covering:

```text
commit_lock
  -> install_frontier
  -> stage_lock
  -> flush_lock
  -> WAL shard locks
  -> B+tree latches
  -> MVCC/version locks
  -> catalog locks
  -> privilege/auth locks
  -> replication state locks
```

Then enforce:
- no lock inversion
- no blocking I/O while holding a high-level serialization lock
- no callbacks into SQL/executor while a low-level storage lock is held
- bounded critical sections

Add tests that run:
- 2x CPU threads
- 8x CPU threads
- 32+ client threads
- high contention on the same keys
- mixed reads/writes

Collect:
- lock wait time
- lock contention count
- stalled transaction duration

### P1.2 Long-lived snapshot controls

MVCC needs operational guardrails.

Add:
- configurable maximum snapshot/transaction age
- metric for oldest snapshot age
- number of active snapshots
- version-chain bytes
- reclaim backlog
- bytes blocked from GC by oldest snapshot

Add a policy for what happens when a snapshot exceeds a safe age:
- warn
- reject new statement
- force rollback
- admin-configurable behavior

Do not allow an accidental forgotten client to retain unbounded history forever.

### P1.3 Protocol fuzzing

Build fuzz targets for:
- MySQL packet parser
- MySQL handshake/auth response
- length-encoded integers/strings
- prepared statement parameter decoding
- TLS upgrade/negotiation state machine
- PostgreSQL startup packets
- PostgreSQL Parse/Bind/Describe/Execute
- PostgreSQL COPY input if enabled
- legacy framed protocol if retained

Required property:
- malformed input returns a bounded error/connection close
- no panic
- no out-of-bounds read
- no unbounded allocation
- no connection-state corruption

### P1.4 Authentication hardening

Review:
- password verifier storage
- auth.bin format versioning
- password reset semantics
- bootstrap root account behavior
- root bypass behavior
- privilege cache invalidation
- replay resistance
- brute-force protection
- audit logging

The README currently describes first-start root creation with an empty password and separately instructs the operator to set a password before exposure. For production packaging, **do not ship with a remotely reachable empty-password root account as a normal deployment state**.

Prefer:
- startup bootstrap mode that binds only to loopback until a password is set, or
- require an explicit initialization command before the network listener is enabled.

### P1.5 TLS operational controls

Add:
- certificate expiry metric
- startup refusal for expired/not-yet-valid server certs
- documented rotation procedure
- SIGHUP/administrative reload if practical
- minimum supported TLS version policy
- cipher/provider documentation
- optional client certificate authentication policy

### P1.6 Replication/HA failure matrix

Test:

1. primary -> replica normal streaming
2. network disconnect
3. replica restart
4. primary restart
5. WAL generation change
6. primary checkpoint during replication
7. archive rollover
8. replica falling far behind
9. replica promotion
10. stale old primary returning after promotion
11. split-brain prevention
12. duplicate/partial WAL frame
13. corrupt WAL frame
14. replica applying a snapshot and then WAL
15. restore into a replica

Acceptance criteria:
- no silent divergent histories
- promotion fencing is explicit
- old primary cannot continue accepting writes after loss of leadership
- lag is measurable
- operator can prove replica caught up to a specific WAL position

### P1.7 Backup/restore verification

You already have offline backup and PITR machinery. Treat restore as a first-class product capability.

Add automated restore tests for:
- 1 row
- 10k rows
- large rows / overflow pages
- secondary indexes
- foreign keys
- multiple databases
- users/privileges
- active WAL
- archived WAL
- target transaction
- target timestamp
- missing/corrupt archive segment
- wrong archive generation
- truncated backup

After every restore, run a database consistency checker.

## 6. P1 — storage correctness and invariants

### Add `CHECK TABLE` / storage consistency verification

A diagnostic mode should validate:
- B+ tree key ordering
- parent/child separator correctness
- leaf chain correctness
- unique key constraints
- secondary index consistency
- foreign-key consistency
- catalog/table consistency
- WAL/snapshot version compatibility
- overflow page reachability
- orphaned overflow pages
- duplicate index entries
- impossible page offsets
- checksum failures

Make this runnable offline and optionally online in read-only validation mode.

### Add consistency hashes

For debugging and replication:
- per-table logical hash
- per-database hash
- optional page-level hash
- WAL position included in diagnostics

That lets you answer:
> “Are primary and replica logically identical at LSN X?”

## 7. P1 — query engine production safeguards

### Resource governance

Add configurable limits for:
- max result rows
- max result bytes
- max temporary rows
- max temporary bytes
- max join rows / intermediate size
- max hash-table size
- max aggregation groups
- max SQL statement length
- max prepared statement count per session
- max concurrent statements per user
- max transaction duration

Statement timeout alone is not sufficient for memory-heavy plans.

### Query cancellation

Ensure cancellation interrupts:
- scans
- joins
- hash build/probe
- sort
- aggregates
- subqueries
- index traversal
- row decoding
- replication apply where applicable

No operator should be able to ignore cancellation for an unbounded amount of work.

### Optimizer correctness

Property-test the memo optimizer against executor semantics:
- every chosen plan returns the same logical result as a baseline plan
- predicate pushdown preserves NULL semantics
- join reordering preserves LEFT JOIN semantics
- hash and nested-loop plans agree
- index and full-scan plans agree

Do not optimize only by speed; verify semantic equivalence.

## 8. P1 — observability

Expose metrics for:

### Database
- qps
- txns/s
- commits/s
- rollbacks/s
- transaction conflicts
- statement latency buckets
- slow statements
- active transactions
- oldest transaction age

### WAL
- WAL bytes written
- WAL bytes/sec
- flush count
- fsync count
- fsync latency histogram
- group commit batch size
- durable frontier
- logical/logical+physical WAL position
- checkpoint duration

### Storage
- B+ tree reads/writes
- split/merge counts
- buffer hit/miss
- overflow page usage
- orphan pages
- page checksum failures

### Concurrency
- latch contention
- commit lock wait
- install frontier wait
- EBR participants
- EBR pending retirements
- epoch advancement stalls
- MVCC version bytes
- oldest snapshot age

### Replication
- current role
- primary WAL position
- replica applied position
- byte lag
- transaction lag
- reconnect count
- last error
- last successful apply timestamp

Add structured logs with:
- timestamp
- level
- subsystem
- connection/session id
- transaction id
- WAL position
- error class

Do not log passwords, auth material, query secrets, or raw credentials.

## 9. P1 — security review

Perform a threat-model review for:

### Network
- arbitrary packet lengths
- partial reads
- slowloris
- connection floods
- authentication brute force
- protocol desynchronization

### SQL
- privilege bypass
- cross-database access
- parser confusion
- prepared statement parameter confusion
- type coercion bugs

### Persistence
- malicious/corrupt WAL
- malicious snapshot
- checksum collision assumptions
- path traversal in archive/backup tools
- symlink attacks around backup/restore paths
- permission/ownership checks on database files

### Operational
- unsafe default binding
- unsafe default credentials
- exposed metrics endpoint
- unauthorized shutdown
- unauthorized promotion
- unauthorized backup restore

## 10. P2 — productization

### Versioning

Move from:

```text
0.1.0
```

to an explicit pre-1.0 release policy.

Define:
- semantic versioning policy
- WAL format version policy
- snapshot format version policy
- auth.bin version policy
- backup/archive version policy
- client protocol compatibility policy

### Release artifacts

Produce:
- Linux x86_64
- Linux aarch64
- macOS x86_64/arm64 as appropriate
- Windows if supported
- checksums
- SBOM
- reproducible build instructions

### Packaging

Provide:
- systemd unit
- container image
- config file
- data directory layout
- log directory
- metrics endpoint
- backup directory
- secure file permissions

### Container requirements

Run as non-root.

Use:
- read-only root filesystem where practical
- explicit writable data volume
- resource limits
- healthcheck
- graceful SIGTERM
- no privileged mode

## 11. Documentation corrections

The repository currently has multiple status narratives. Consolidate them into one source of truth.

Create:

```text
STATUS.md
PRODUCTION_READINESS.md
OPERATIONS.md
RECOVERY.md
REPLICATION.md
SECURITY.md
```

`STATUS.md` should list every feature as:

```text
Implemented + verified
Implemented + partially verified
Implemented + not verified
Experimental
Planned
```

Do not mark a subsystem “production ready” merely because code exists and unit tests pass.

## 12. Remove ambiguity from the README

The README currently makes strong performance claims, including multi-x advantages over MySQL and statements such as “ACID-compliant” and “high-throughput” while the repo remains pre-1.0 and has not yet passed the verification program in this document.

Keep benchmarks, but label them precisely:
- hardware
- CPU
- filesystem
- kernel
- storage device
- MySQL config
- henchDB config
- durability mode
- concurrency
- client implementation
- dataset size
- warm/cold cache
- test duration
- variance / percentile latency

Also separate:
- **feature implemented**
- **feature validated**
- **feature production-supported**

## 13. Suggested implementation order

### Phase A — correctness gates

1. CI baseline
2. panic policy
3. EBR/OLC stress harness
4. memory-model verification
5. crash failpoints
6. recovery state checker
7. MVCC differential testing
8. optimizer equivalence testing

### Phase B — adversarial interfaces

9. MySQL fuzzing
10. PostgreSQL fuzzing
11. auth fuzzing
12. TLS state-machine fuzzing
13. resource-limit testing

### Phase C — operations

14. metrics expansion
15. backup/restore automation
16. replication failure matrix
17. promotion/fencing tests
18. long-running soak tests

### Phase D — release engineering

19. CI matrices
20. packaging
21. container
22. systemd
23. release artifacts
24. docs/status cleanup
25. versioning/release policy

## 14. “Production ready” definition of done

Do **not** declare production readiness until all of the following are true:

- [ ] EBR/OLC passes sustained concurrent stress with no memory-safety finding
- [ ] all unsafe blocks have reviewed safety invariants
- [x] crash failpoints cover every durability boundary
- [x] randomized crash/restart testing passes
- [ ] MVCC passes differential/property tests
- [ ] optimizer plans are proven equivalent to baseline execution
- [x] CI is mandatory on main/protected branches
- [ ] no unreviewed production `unwrap/expect/panic`
- [ ] protocol fuzzing passes without crashes/OOMs
- [ ] authentication and RBAC have a security test suite
- [x] production TLS lifecycle is documented
- [ ] backup + restore is continuously tested
- [ ] PITR is continuously tested
- [ ] replica reconnect/restart/promotion is continuously tested
- [ ] split-brain prevention is documented and tested
- [ ] metrics cover WAL, MVCC, EBR, checkpoints, locks, queries, and replication
- [x] transaction/snapshot age limits exist
- [x] resource limits exist for memory/intermediate results
- [x] storage consistency checker exists
- [x] primary/replica logical consistency can be proven
- [x] secure defaults exist for credentials and network binding
- [ ] release artifacts are reproducible/verifiable
- [x] operational runbooks exist
- [x] status documentation matches the actual source tree
- [ ] a production soak test passes for a meaningful duration on supported hardware

## 15. Final recommendation

### Current use

**Good fit now:**
- research
- benchmarking
- database-engineering experimentation
- controlled internal workloads
- pre-production prototyping

**Not a good fit yet:**
- primary production datastore for valuable data
- Internet-exposed database without a hardened security review
- sole source of truth without independently verified backups
- unattended HA deployment

### The single biggest thing to solve

The project already has enough database features. The next milestone should **not** be another SQL feature.

The next milestone should be:

> **“Prove the existing engine is correct under concurrency and crash failure.”**

Once EBR/OLC, WAL/recovery, and MVCC have adversarial verification, the rest of the production work becomes much more tractable.

## 16. Concrete issue breakdown

Use these as GitHub issues/epics:

- `P0: Add EBR + OLC concurrency verification harness`
- `P0: Audit every unsafe block and raw pointer lifecycle`
- `P0: Add crash failpoints and crash-state oracle`
- `P0: Add randomized crash/recovery CI`
- `P0: Add MVCC differential/property testing`
- `P0: Add mandatory GitHub Actions CI`
- `P0: Establish production panic policy`
- `P1: Add protocol fuzzing harness`
- `P1: Add long-lived snapshot limits and metrics`
- `P1: Add replication failure/promotion test suite`
- `P1: Add backup/PITR automated restore suite`
- `P1: Add storage consistency checker`
- `P1: Add resource governance and query memory limits`
- `P1: Expand production observability`
- `P1: Harden authentication bootstrap/defaults`
- `P1: Harden TLS lifecycle and certificate rotation`
- `P2: Add release packaging and reproducible artifacts`
- `P2: Add systemd/container deployment`
- `P2: Consolidate status and operations documentation`

## 17. Bottom line

**No — I would not call henchDB production-ready yet.**

The implementation breadth is impressive and the architectural direction is strong, but a production database is defined by the strength of its failure and correctness guarantees, not by the number of implemented SQL features.

The repository is best described today as:

> **advanced pre-production database-engineering project with a credible path to production, but not yet production-safe.**

The main blockers are **unsafe concurrent memory reclamation verification, crash-consistency proof, MVCC differential validation, CI/release discipline, and adversarial protocol/HA testing**.
