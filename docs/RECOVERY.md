# Disaster Recovery & Integrity Runbook — henchDB

## 1. Crash Recovery Architecture
henchDB follows write-ahead logging (WAL) with strict write-ordering guarantees:
1. Changes are staged in session memory.
2. At commit time, records are reserved and staged into per-core staging FIFOs (`wal/shard.rs`).
3. The background syncer drains records in reservation order, writes them to disk, and executes `fsync`.
4. The transaction's modifications are installed into the B+ tree in WAL-sequence order.
5. On startup after an unclean shutdown, the engine scans the WAL from the last checkpoint snapshot:
   - Records with valid IEEE CRC32 checksums are processed.
   - Any uncommitted transactions are rolled back.
   - Committed transactions missing from the snapshot are reapplied (Redo).

## 2. Point-In-Time Recovery (PITR)

### Continuous Archiving
To enable PITR, pass `--wal-archive-dir` on startup:
```bash
server serve --dir /var/lib/henchdb/data --wal-archive-dir /mnt/backups/wal_archive
```
During checkpointing, the truncated durable WAL prefix is sealed into immutable `HDBA` segment files in the archive directory.

### Restoring to a Specific Point in Time
To restore a base backup and roll archive segments forward:
```bash
# Restore to a specific timestamp (RFC 3339 format):
server restore \
  --dir /var/lib/henchdb/data \
  --archive-dir /mnt/backups/wal_archive \
  --target-time "2026-09-10T15:30:00Z"

# Restore up to a specific transaction ID:
server restore \
  --dir /var/lib/henchdb/data \
  --archive-dir /mnt/backups/wal_archive \
  --target-txn 104523
```

## 3. Data Integrity Auditing with `CHECK TABLE`
Use `CHECK TABLE <name>` to perform online structural and logical validation:
- **B+ Tree Monotonicity**: Verifies all keys are strictly sorted and interior separator keys accurately partition children.
- **Schema & Constraints**: Validates column counts and ensures columns defined as `NOT NULL` contain no null datums.
- **Secondary Index Consistency**: Ensures all secondary index keys point to valid primary rows, and all primary rows exist in secondary indexes.
- **Foreign Key Consistency**: Confirms child non-null foreign key references match existing parent primary keys.
- **Deterministic CRC32 Hash**: Produces a canonical CRC32 dataset hash (`table_logical_hash`) for comparing replicas against the primary.

Example:
```sql
CHECK TABLE accounts;
-- Output: accounts | check | status | OK (hash: 0x8F3A412C)
```
