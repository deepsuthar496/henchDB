# Physical Streaming Replication Runbook — henchDB

## 1. Architecture
henchDB supports streaming physical replication over a dedicated replication port (`--repl-port`, default `3308`):
- **Primary Node**: Feeds sequential WAL segments to connected replicas as transactions commit.
- **Replica Node**: Connects to the primary with `--replica-of <primary_host>:<repl_port>`, streams WAL records into its local engine, and applies updates in memory and on disk.
- **Read-Only Gate**: Replicas enforce read-only semantics; write statements return MySQL error `1290 (HY000): The server is running with the --read-only option so it cannot execute this statement`.

## 2. Setting Up Replication

### Primary Node Configuration
```bash
server serve \
  --dir /var/lib/henchdb/primary_data \
  --port 3307 \
  --repl-port 3308 \
  --bind 0.0.0.0
```

### Replica Node Configuration
```bash
server serve \
  --dir /var/lib/henchdb/replica_data \
  --port 3309 \
  --pg-port 5433 \
  --replica-of 192.168.1.10:3308 \
  --repl-user repuser \
  --repl-password "ReplSecret789"
```

## 3. Monitoring Replication Lag
Connect to the replica and execute:
```sql
SHOW STATUS;
```
Check `Replica_Lag_Bytes` and `Replica_Last_Applied_Txn` to observe replication progress relative to the primary.

## 4. Failover & Promotion
If the primary node fails or maintenance is scheduled, the replica can be promoted to primary:

### Online Promotion (via SQL)
Connect to the replica as administrative user `root`:
```sql
PROMOTE;
```
The replica flushes all remaining stream segments, removes the read-only gate, and begins accepting read-write traffic.

### Offline Promotion (CLI)
If the server process is stopped:
```bash
server promote --dir /var/lib/henchdb/replica_data
```
This flips the local configuration flag and allows standard `server serve` execution as a standalone read-write primary.
