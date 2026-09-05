# AGENTS.md — henchDB (working title)

Context and rules for any agent (or human) working in this repository.
Read this before changing code. Two more documents are mandatory reading:

- **[`PROGRESS.md`](PROGRESS.md)** — the complete engineering log: what has
  been built and how, measured evidence, known weaknesses, and the
  prioritized speed + security backlog with the suggested next milestone.
  **Every agent must append a dated entry to its change log (§5) and update
  its status tables in the same commit as their code change.**
- **[`README.md`](README.md)** — public overview + benchmark chart. Update
  the chart data (`benchmark_chart.html`) when headline numbers change.

The authoritative architecture reference is `research.md` (the compiled
analysis of MySQL/InnoDB bottlenecks and the LeanStore/RCC/io_uring blueprint
this project follows).

## 1. Project identity and renaming rule

The **product name is `henchDB`**, but it is a working title. Therefore:

- **Never hardcode the product name anywhere in code, comments, protocols,
  file formats, or test data.** The single source of truth is
  `PRODUCT_NAME` in `crates/engine/src/lib.rs`. The user-visible banner uses
  only that constant.
- Code identifiers are generic: crates are `engine` and `server`, types are
  `Database`, `Table`, `BTree`, `Wal`, etc. Keep it that way.
- **To rename the whole project** you must touch exactly:
  1. `PRODUCT_NAME` (and optionally `PRODUCT_TAGLINE`) in
     `crates/engine/src/lib.rs`,
  2. folder name of the repo,
  3. the title of this file and `README.md`.
  Nothing else. Do not introduce new references to the brand name; if a
  feature needs a display name, read `PRODUCT_NAME`.
- WAL/snapshot file formats are identified by magic bytes (`HDBW`, `HDBS`),
  not by the product name — do not change them to brand-specific values
  without bumping the format version (see §5).

## 2. What this project is

A relational database engine written from scratch in Rust (edition 2021,
**zero external dependencies**) designed to outgrow MySQL/InnoDB by
following the architecture in `research.md`:

- OLC B+ trees (optimistic lock coupling) instead of latch-coupled InnoDB
  trees,
- staged out-of-place writes with instant abort (RCC direction) instead of
  undo logs,
- distributed-group-commit-ready WAL instead of a global log mutex,
- checksummed, self-describing persistence with fuzzy checkpointing.

Current status: **v0.1 — single-node, fully working OLTP core.** See §4 for
the roadmap from here to the full research blueprint.

## 3. Repository layout

```
Cargo.toml            workspace (members: engine, server)
research.md           architecture specification + bottleneck analysis (input doc)
crates/engine/src/
  lib.rs              PRODUCT_NAME, re-exports, module wiring
  error.rs            Error enum (single error type for the whole engine)
  latch.rs            HybridLatch: 64-bit version word, bit0 = exclusive lock
  btree.rs            OLC B+ tree (see §6 for the invariants)
  types.rs            Datum values, ColumnType, order-preserving key codec
  table.rs            schema, row codec, per-table tree operations
  wal.rs              WAL records, CRC32, group-commit seam, recovery scan
  catalog.rs          table registry + snapshot (checkpoint) file codec
  db/                 Database facade split per §9 ceiling: mod (sessions,
                      txns, commit pipeline, recovery), query (SELECT/JOIN/
                      GROUP BY execution), plan (access paths), tests
  sql.rs              hand-written lexer + recursive-descent parser + AST
crates/server/src/
  main.rs             CLI: interactive shell | `serve` (TCP) | `bench` | `gcbench` | `benchmock`
  mock_innodb.rs      mock InnoDB-style data path for architecture micro-benchmarks
bench_compare.py      real MySQL 8 vs henchDB harness (same Python client, both over TCP)
mysql/, mysql_data/   local MySQL 8.0.46 (portable, port 3307) used by bench_compare.py
benchmark_chart.html  chart source (data array at top) -> benchmark_chart.png
PROGRESS.md           engineering log, benchmark methodology, speed+security backlog
```

## 4. Architecture decisions (v0.1) and the roadmap

Each item maps a research.md section to code. Keep this table current; when
you upgrade a component, update the row and the module doc comment.

| Area | v0.1 implementation | Roadmap (research.md) |
|---|---|---|
| Index | Typed OLC B+ tree, `MAX_KEYS=128`, borrow/merge on delete (`MIN_KEYS=64`) + root collapse + EBR retire | Prefix + 4-byte-head SIMD search, swizzled tree nodes (`research.md` §Storage); value overflow paging is done (see Storage row) |
| Storage | 256 KiB slotted pages + 64-bit swips + write-through cooling pool (`page.rs`); rows >1 KiB spill off-page with epoch-quarantined reuse; snapshot v2 carries key/value pairs, WAL carries full rows | Swizzled tree nodes, page GC / free-space persistence across restart, write-back batching, io_uring `IOPOLL` (Linux-only, `cfg`-gate it) |
| Reads | Optimistic version snapshot + validate, restart on mismatch | Same (EBR now retires merged-away tree nodes) |
| Writes | Session-staged write set; commit takes one commit lock, validates, WAL-batches, installs (allocating an MVCC commit epoch); installs record superseded rows while snapshot readers are active | Per-core WAL shards, Early Lock Release, column-granular versioning (RCC) |
| Durability | Single WAL file, CRC32 per record, per-txn redo on recovery, snapshot + WAL truncate checkpoint. **Group commit implemented**: commits append under a short lock, one background syncer batches concurrent commits into one fsync (200us collection window), installs happen strictly in WAL-offset order (install frontier + condvar); DDL goes through the same sequencer via `Database::wal_commit` | Per-core WAL buffers (shard the current WAL), io_uring `IOPOLL` (Linux-only, `cfg`-gate it), parallel replay |
| Concurrency | Single commit lock (serializes installs) | Lock-free commit pipelines; keep commit lock only as the correctness fallback |
| SQL | Hand-written lexer/parser (`sql/` modules); SELECT/INSERT/UPDATE/DELETE/DDL/BEGIN/COMMIT/ROLLBACK/SHOW TABLES/CHECKPOINT; WHERE = AND/OR/NOT + IN/BETWEEN/LIKE over column-vs-literal ANDed (parens, precedence); index access paths on PK (point/multi-point/range) + secondary; AUTO_INCREMENT integer PKs; SUM/AVG/MIN/MAX (+COUNT); INNER/LEFT JOIN (hash join on equi-keys, nested-loop fallback, greedy smallest-ready-first ordering with LEFT barriers); FOREIGN KEY (RESTRICT/CASCADE/SET NULL, auto-indexed columns, PK/secondary/scan seeks) | sqlparser-rs MySQL dialect, Cascades memo optimizer (greedy covers common shapes), morsel-driven vectorized execution (Arrow), GROUP BY pushdown |
| Server | Thread-per-connection TCP, authenticated MySQL wire (text + binary prepares, `CLIENT_SSL` optional via `--tls-cert`/`--tls-key` rustls) + legacy framed text (auto-detected, `--no-legacy` to disable); max-connections + idle/handshake timeouts; COM_SHUTDOWN + signal graceful drain; `server passwd` manages `auth.bin` verifiers | Pinned thread-per-core runtime, io_uring sockets, server-side cursors, statement timeouts, per-user privileges |

Known v0.1 simplifications (intentional, do not "fix" silently — implement
the roadmap item instead):

- **Sparse-leaf residue**: deletes rebalance via borrow/merge, but branches
  never visited by a delete path stay sparse (correct, just sparse); a
  background defragmentation pass is the follow-up.
- **DDL is autocommit** and serialized through the commit lock.
- **No secondary indexes**: PK range scans only; the executor falls back to
  full scan + filter.
- **No in-place update primitive**: `upsert` = get + remove + insert (three
  descents). The mock comparison shows this is the single-thread update
  bottleneck; add an in-place value replacement when the value size fits.
- **Optimistic reads are torn-read-prone by design**: node bodies are read
  without latches; writers bump the version on unlock so torn reads fail
  validation and restart. Every read that indexes into Vecs must clamp the
  index (keys/vals are updated non-atomically) — see `get()` in btree.rs.
  Formally the UnsafeCell reads are a benign race per the OLC protocol.
- **No MVCC snapshots for long readers**: readers see committed state; a
  reader that starts mid-commit can see either before or after, never a torn
  state (commit installs are atomic per tree via the OLC latches).
- The B+ tree's node bodies use `UnsafeCell` with OLC as the safety argument
  (module doc in `btree.rs`). A formally race-free variant (relaxed atomics
  or COW nodes) is acceptable to pursue; keeping plain `RwLock` per node is
  NOT acceptable — it defeats the zero-invalidation read path.

## 5. Format stability rules

- WAL records: `[u32 len][u32 crc32][payload]`, payload starts with kind
  byte then u64 txn id. Every txn (including single-record DDL) ends with a
  `Commit` record — **recovery applies only transactions with a Commit
  marker**. Any new record kind must (a) get a new kind byte, (b) carry a
  txn id, (c) be replayable idempotently.
- Changing any on-disk format requires bumping `WAL_FORMAT_VERSION` /
  `SNAPSHOT_FORMAT_VERSION` and writing a migration path or a clean-reject
  (`Corrupted` error), never silent reinterpretation.
- CRC32 is table-based IEEE; the known vector test (`crc32(b"123456789") ==
  0xCBF43926`) must keep passing.

## 6. B+ tree invariants (do not break)

1. Every node entry to the write path is via `node.lock()` (RAII guard);
   never call `lock_exclusive`/`unlock_exclusive` manually except inside
   `latch.rs`/the guard.
2. Writers acquire latches strictly root→leaf (lock coupling); a writer
   holding a parent never blocks on that parent again.
3. A node is split **eagerly** while its parent is latched; the root is
   wrapped in a fresh parent under the root mutex when full. Therefore a
   writer never meets a full node except a stale root clone (`Descend::Restart`).
4. Readers: snapshot version → read → validate → retry from root on
   mismatch. Merged-away nodes unlink (parent + leaf `next`) and retire
   through EBR (`BTree::set_epoch_manager`); the `Arc` keeps stale readers
   memory-safe until they restart.
5. Structural changes (children lists, separators, leaf `next`) are always
   made while holding the parent's exclusive latch so optimistic readers of
   that parent spin through the transition.

## 7. Platform notes

- Development machine is Windows; the portable core must always build and
  pass tests here (`cargo test` green).
- Linux-only performance work (io_uring, `core_affinity` pinning, hugepages,
  `O_DIRECT`) must be behind `#[cfg(target_os = "linux")]` with a portable
  fallback so the crate still compiles on Windows/macOS.
- WAL `reset()` must truncate via a fresh write handle; `set_len` fails
  through append-mode handles on Windows (this exact bug was fixed once —
  keep the regression test).

## 8. Git and deployment rules

- **Never `git push` (or `git commit` unless part of a requested change) without
  explicit user instruction.** The user must say "push" or "commit and push"
  before any remote operation. If in doubt, ask.

## 9. Engineering rules

- **Zero external dependencies** in `engine` for now (std only). Adding the
  first dependency (e.g., `sqlparser-rs`) is a deliberate roadmap decision —
  record it in §4 when it happens. Footnote: `server` took its first
  dependencies in SEC2 (`rustls 0.23` + `rustls-pemfile 2` for TLS; pure Rust
  at runtime, C compiler at build time for `ring`); `engine` remains std-only.
- Every subsystem carries unit tests; concurrency bugs are found by the
  threaded tests (`concurrent_inserts_and_reads`, `concurrent_commits_all_persist`).
  When touching latch/tree/commit code, run the full suite repeatedly.
- New SQL features need: parser test in `sql.rs`, executor behavior test in
  `db/tests.rs`, and (if it changes durability) a recovery test.
- Errors: extend `error::Error`; never `panic!` on bad client input — parse
  and validation errors must return `Err`.
- Names stay generic per §1. Comments explain constraints and invariants,
  not history ("what the research says" lives in research.md; point to it).
- **Documentation is part of the change**: append to `PROGRESS.md` §5 (what +
  why + evidence), update its architecture/backlog tables if they moved, and
  refresh `README.md` numbers/chart if headlines changed — same commit.
- **File size ceiling**: When any source file approaches or exceeds 1,500 lines, divide it into logically scoped submodules within a directory module (e.g. `wire/` or `exec/`) to preserve agent context window efficiency, maintainability, and clean separation of concerns, without sacrificing performance (preserving zero-copy references and inlining).
- **Security baseline** (extend, never regress): memory-safe Rust only
  (UnsafeCell confined to `btree.rs` per its module doc); the 16 MiB frame
  guard on the wire protocol; recovery must fail with `Error::Corrupted`,
  never panic, on corrupt input. Roadmap: parser fuzzing, codec corruption
  corpus tests, auth, TLS, resource limits — see `PROGRESS.md` §6.2. No new
  `unsafe` blocks without a module-level safety argument and an entry in
  `agents.md`.

## 9. Commands

```
cargo test                     # full suite (engine unit tests)
cargo build --release
./target/release/server                       # interactive shell (dir: ./data)
./target/release/server serve --port 3307 --dir data
./target/release/server bench --rows 50000 --dir target/benchdata
```

Wire protocol (v0.1): request `[u32 BE len][utf8 sql]`, response
`[u32 BE len][utf8 payload]`; payload `ERR <msg>` or
`OK\n<tab-separated columns>\n<tab-separated rows>` (`\N` = NULL).
Changing the protocol means bumping a version byte in the handshake and
documenting it here.
