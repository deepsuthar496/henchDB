# Operations & Deployment Runbook — henchDB

## 1. Running the Server

### Basic Startup
```bash
# Serve default MySQL wire on port 3307 and PostgreSQL on port 5432:
server serve --dir /var/lib/henchdb/data

# Bind to localhost only for secure local access:
server serve --dir /var/lib/henchdb/data --bind 127.0.0.1 --port 3307

# Custom configuration with worker thread pool, connection limits, and metrics:
server serve \
  --dir /var/lib/henchdb/data \
  --bind 0.0.0.0 \
  --port 3307 \
  --pg-port 5432 \
  --metrics-port 9100 \
  --repl-port 3308 \
  --threads 16 \
  --max-connections 2048 \
  --wait-timeout 28800 \
  --wal-archive-dir /var/lib/henchdb/archive
```

### CLI Parameters
- `--dir <PATH>`: Database storage directory (default: `./data`).
- `--bind <HOST>`: Interface IP to bind listeners (default: `0.0.0.0`).
- `--port <PORT>`: MySQL wire protocol port (default: `3307`).
- `--pg-port <PORT>`: PostgreSQL wire protocol port (default: `5432`, 0 = disabled).
- `--metrics-port <PORT>`: Prometheus exporter port (default: `9100`, 0 = disabled).
- `--repl-port <PORT>`: Physical replication stream port (default: `3308`, 0 = disabled).
- `--threads <NUM>`: Worker thread pool size (default: 2x CPU cores).
- `--max-connections <NUM>`: Concurrent client connection ceiling (default: `1024`).
- `--wait-timeout <SECONDS>`: Connection idle timeout before reaping (default: `28800`).
- `--wal-archive-dir <PATH>`: Directory for continuous PITR WAL segment archiving.
- `--tls-cert <PATH>` & `--tls-key <PATH>`: TLS credentials for encrypted wire connections.
- `--replica-of <HOST:PORT>`: Start server as a read-only streaming replica.
- `--read-only`: Start server in read-only mode rejecting mutating transactions.

## 2. User & Credential Management
Bootstrap password configuration:
```bash
# Set root password before public exposure:
server passwd --dir /var/lib/henchdb/data --user root --password "StrongMasterSecret123!"

# Create or update another user:
server passwd --dir /var/lib/henchdb/data --user app_user --password "AppSecret456!"
```

## 3. Monitoring & Telemetry
henchDB includes a Prometheus exporter on `--metrics-port` (default `9100`):
- `GET /metrics`: Standard Prometheus metrics format with latency histograms, transaction counters, cache hit ratios, and active connections.
- `GET /health`: Returns HTTP 200 `OK` when the server is healthy and accepting connections.

## 4. Online Maintenance
Trigger manual checkpoints and integrity audits via SQL:
```sql
-- Flush dirty pages and advance WAL truncation frontier:
CHECKPOINT;

-- Verify table B+ tree order, constraints, and indexes:
CHECK TABLE users;

-- Rebuild statistics for the cost-based query optimizer:
ANALYZE TABLE orders;
```
