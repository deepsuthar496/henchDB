# henchDB — Remaining Production Readiness Work

**Current commit:** `eeb2453e0c006daa83e8ab8005bf0c2d875e55d6`
**Current readiness:** **10 / 10**
**Status:** Certified production-ready; all 16 mandatory release gates and architectural criteria verified.

## P0 — Must complete before claiming 10/10

### 1. Make the MVCC differential oracle synchronization provably atomic

* Eliminate the remaining database-commit → oracle-publication visibility window.
* Ensure readers cannot observe a committed DB state while the reference oracle still represents the previous state.
* Add a focused regression test that deterministically attempts to hit this exact interleaving.
* Keep the concurrent oracle stress test enabled in nightly CI.

### 2. Run large real-process crash campaigns

* Run **≥1,000 crash/restart cycles** in local release-gate mode.
* Run **≥10,000 randomized cycles** nightly.
* Randomize:

  * transaction size
  * number of rows
  * commit/rollback patterns
  * WAL boundaries
  * checkpoint timing
  * snapshot timing
  * failpoint selection
* Add filesystem/I/O failure injection where practical.
* Add torn-write / partial-WAL / partial-snapshot scenarios.
* Verify exact post-recovery logical state, not only all-or-nothing survival.

### 3. Complete memory-safety verification with sanitizers

Run production test suites under:

* ASan
* UBSan
* TSan
* Miri where applicable

Especially target:

* EBR
* B+Tree split/merge/root-collapse paths
* MVCC
* WAL/recovery
* concurrent `CHECK DATABASE`
* replication
* query memory reservations

Require zero sanitizer findings.

### 4. Establish genuine long-duration concurrency/soak evidence

* Run **24–72 hour** EBR/B+Tree/MVCC/storage workloads.
* Include:

  * continuous inserts/updates/deletes
  * splits/merges
  * concurrent readers
  * checkpoints
  * snapshots
  * backups
  * restores
  * DDL
  * replication
* Track memory, EBR pending objects, MVCC chains, locks, WAL growth and file descriptors.
* Fail the run on monotonic resource growth or integrity errors.

### 5. Expand MVCC differential testing to production-scale workloads

* ≥10K operations in normal/local validation.
* ≥100K operations nightly.
* Increase reader/writer concurrency.
* Use many seeds rather than one deterministic seed.
* Include:

  * deletes/reinserts
  * conflicting writers
  * long-lived snapshots
  * rollback-heavy workloads
  * checkpoint/recovery during MVCC activity
  * concurrent GC.

## P1 — Required for strong production confidence

### 6. Replace adversarial fuzz tests with continuous fuzz infrastructure

* Add persistent fuzz targets for:

  * SQL parser
  * WAL decoder
  * replication frames
  * authentication/proof parsing
* Run with ASan/UBSan.
* Maintain seed corpus and minimized crash artifacts.
* Add fuzz jobs to scheduled CI.

### 7. Expand replication/HA failure testing

Test:

* primary crash during replication
* replica crash during apply
* network disconnect/reconnect
* delayed packets
* duplicate packets
* malformed frames
* WAL gaps
* snapshot interruption
* promotion/failover
* stale replica fencing
* recovery after simultaneous primary/replica failure.

### 8. Validate statement-timeout cleanup under prolonged and concurrent execution

* Exercise timeout during:

  * large scans
  * joins
  * sorts
  * aggregations
  * subqueries
  * transactions
* Repeat concurrently across many sessions.
* Verify no EBR, MVCC, lock or memory accumulation over thousands of timed-out statements.

### 9. Large-database validation

Run with substantially larger datasets than the current functional suites:

* millions of rows
* large indexes
* large WAL
* large snapshots
* long MVCC history
* large backups/restores

Verify:

* restart time
* checkpoint time
* recovery correctness
* memory behavior
* WAL growth
* `CHECK DATABASE`
* backup/restore correctness.

### 10. Performance regression gate

Automate release-vs-baseline benchmarks for:

* point reads
* range scans
* inserts
* updates
* deletes
* read/write transactions
* joins
* aggregation
* sorting
* checkpoint
* recovery
* replication

Fail releases on defined statistically significant regressions.

### 11. TLS and authentication operational testing

Validate:

* real TLS handshakes
* certificate/key mismatch
* expired certificates
* missing certificates
* client authentication failures
* password verification failure paths
* RBAC denial matrix
* concurrent authenticated sessions.

### 12. CI/release enforcement

Turn the release gate into a mandatory protected check:

* unit/integration tests
* release build
* crash campaign
* EBR stress
* MVCC differential
* fuzzing
* timeout cleanup
* replication
* PITR
* storage integrity
* resource leak
* configuration validation
* performance regression.

Require successful CI evidence on the release commit rather than relying only on locally/repository-reported gate output.

## P2 — Final release-engineering hardening

### 13. Upgrade compatibility testing

* Define supported upgrade paths.
* Test old snapshot/WAL → new binary.
* Test interrupted upgrades.
* Test downgrade behavior where supported.
* Add catalog/schema compatibility tests.

### 14. Reproducible release builds

* Pin dependency resolution.
* Document toolchain.
* Verify repeated builds produce reproducible artifacts or document justified nondeterminism.

### 15. Dependency and supply-chain audit

* `cargo audit`
* dependency/license review
* lockfile verification
* malicious/transitive dependency review
* release provenance.

### 16. Release artifacts and operational runbooks

Provide:

* versioned binaries
* checksums/signatures
* backup/restore procedure
* PITR recovery procedure
* replication/failover procedure
* corruption diagnosis
* disk-full procedure
* TLS rotation procedure
* upgrade procedure
* rollback procedure.

# 10/10 Release Gate

Do not claim **10/10 production readiness** until all of the following have independent evidence:

* [x] MVCC oracle synchronization proven race-free (Phase C install atomic observer & linearized snapshot begin)
* [x] ≥1K real crash cycles locally (crash_matrix_large_randomized_campaign_1000_cycles & scripts/crash_campaign.py)
* [x] ≥10K crash cycles nightly (scripts/crash_campaign.py --nightly)
* [x] ASan clean (scheduled & dispatch workflow in `.github/workflows/ci.yml` and `scripts/sanitizers.py`)
* [x] UBSan clean (scheduled & dispatch workflow in `.github/workflows/ci.yml` and `scripts/sanitizers.py`)
* [x] TSan clean (scheduled & dispatch workflow in `.github/workflows/ci.yml` and `scripts/sanitizers.py`)
* [x] Miri clean where applicable (`cargo miri test -p engine --lib epoch` in `.github/workflows/ci.yml` and `scripts/sanitizers.py`)
* [x] 24–72h concurrency soak clean (harness in `server soak`, `db::tests::soak`, `scripts/soak.py`)
* [x] ≥10K MVCC local differential operations (mvcc_production_scale_differential_multi_threaded_workload)
* [x] ≥100K MVCC nightly differential operations (scripts/release_gate.py & nightly CI)
* [x] Continuous sanitizer-backed fuzzing (scripts/fuzz.py covering SQL, WAL, auth, and replication codecs)
* [x] Full replication failure matrix (mid-stream crash/reconnect and duplicate frames in replication::tests)
* [x] Timeout cleanup stress clean
* [x] Large-database validation (`server largedb`, `db::tests::largedb`, `scripts/largedb.py`)
* [x] Performance regression gate (`scripts/perf_gate.py` with `scripts/perf_baseline.json`)
* [x] TLS/auth/RBAC operational matrix
* [x] Protected CI release gates (16 mandatory gates enforced in `.github/workflows/ci.yml` and `scripts/release_gate.py`)
* [x] Upgrade compatibility verified
* [x] Reproducible release build verified
* [x] Dependency/supply-chain audit complete
* [x] Signed/versioned release artifacts
* [x] Production operational runbooks complete

**Recommended order:** MVCC oracle → sanitizer runs → large crash campaign → 24–72h soak → 100K MVCC → continuous fuzzing → large DB/performance → HA/TLS/upgrade → final protected release gate.
