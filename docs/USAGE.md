# henchDB User & Developer Guide

`henchDB` is a high-performance, single-binary relational database engine written in Rust with zero external runtime dependencies. It supports native MySQL and PostgreSQL wire protocols concurrently, OLC B+ tree storage, group-commit write-ahead logging (WAL), multi-version concurrency control (MVCC), and built-in continuous replication and point-in-time recovery (PITR).

---

## 1. Binary Portability & Deployment

`server.exe` (or `server` on Linux) is a **fully self-contained, statically linked binary**.

- **Zero dependencies**: No external DLLs, no runtime packages, no background daemon prerequisites.
- **Standalone deployment**: Copy `server.exe` into any empty directory and run it immediately. It creates and manages its own database files (`snapshot.bin`, `wal.log`, `pages.bin`, `auth.bin`) automatically.

---

## 2. CLI Reference & Subcommands

Run `server.exe --help` (or `server.exe <command> --help`) to view command options.

### General Usage
```text
server.exe [COMMAND] [OPTIONS]
```

If launched without a subcommand, `server.exe` starts an interactive local SQL REPL (default directory `./data`).

### Subcommand Summary

| Command | Description |
|---|---|
| `serve` | Run the network database server (MySQL wire, PG wire, metrics, replication) |
| `bench` | Run built-in concurrency and throughput micro-benchmarks |
| `passwd` | Manage database credentials and password verifiers in `auth.bin` |
| `dump` | Create a consistent offline physical snapshot/backup |
| `restore`| Restore from a backup and optionally roll forward WAL archive segments |
| `promote`| Promote an offline replica directory to primary |
| `gcbench`| Run garbage-collection and version-pruning benchmark |
| `clientbench` | Run multi-threaded client TCP benchmark against a running server |

---

### Starting the Server (`serve`)

```bash
# Minimal start on default ports (MySQL: 3307, PostgreSQL: 5432, Metrics: 9100, Replication: 3308)
server.exe serve --dir ./data

# Production start with custom ports, thread pool sizing, and idle reap
server.exe serve \
  --dir ./data \
  --port 3307 \
  --pg-port 5432 \
  --metrics-port 9100 \
  --repl-port 3308 \
  --threads 16 \
  --max-connections 1024 \
  --wait-timeout 28800 \
  --wal-archive-dir ./wal_archive
```

#### Key `serve` Flags

| Flag | Default | Description |
|---|---|---|
| `--dir <PATH>` | `./data` | Directory where data, snapshots, buffer pool, and WAL are stored |
| `--port <PORT>` | `3307` | TCP port for MySQL wire protocol |
| `--pg-port <PORT>` | `5432` | TCP port for PostgreSQL v3.0 wire protocol (--no-pg to disable) |
| `--metrics-port <PORT>`| `9100` | HTTP port for Prometheus `/metrics`, `/health`, and `/live` (--no-metrics to disable) |
| `--repl-port <PORT>` | `3308` | TCP port for physical WAL replication streaming (--no-repl to disable) |
| `--threads <N>` | `2 * CPU cores` | Worker thread pool count |
| `--max-connections <N>`| `1024` | Maximum concurrent active connections (rejects 1040/53300) |
| `--wait-timeout <SECS>`| `28800` | Idle connection timeout in seconds (0 = disabled) |
| `--tls-cert <PEM>` | None | TLS certificate chain for encrypted client connections |
| `--tls-key <PEM>` | None | TLS private key in PKCS#8 or RSA format |
| `--wal-archive-dir <DIR>`| None | Directory for continuous PITR WAL segment archiving |
| `--no-legacy` | false | Disable legacy framed plaintext protocol |

---

### Managing User Passwords (`passwd`)

Security rule: If binding to all interfaces (`0.0.0.0`), a root password is required.

```bash
# Set or update root password
server.exe passwd --dir ./data --user root --password "SecretPassword123"

# Create a regular application user
server.exe passwd --dir ./data --user app_user --password "AppSecret456"

# Remove a user
server.exe passwd --dir ./data --user app_user --delete
```

---

### Backup, Restore & Point-in-Time Recovery (PITR)

```bash
# Create a consistent offline snapshot
server.exe dump --dir ./data --out ./backups/base_backup.hdb

# Restore directly from base backup
server.exe restore --backup ./backups/base_backup.hdb --dir ./restored_data

# Point-in-time recovery (base backup + roll forward WAL archive segments)
server.exe restore \
  --backup ./backups/base_backup.hdb \
  --dir ./restored_data \
  --archive-dir ./wal_archive \
  --target-time "2026-09-11 12:00:00"
```

---

## 3. Client Connections & Drivers

`henchDB` implements both MySQL and PostgreSQL wire protocols natively. Standard client libraries, ORMs, and CLI tools work out of the box.

### Connecting via MySQL Protocol (Default Port: 3307)

#### 1. Official MySQL CLI
```bash
mysql -h 127.0.0.1 -P 3307 -u root -p
```

#### 2. Python (`pymysql`)
`COMMIT` and `ROLLBACK` succeed gracefully as no-ops when no explicit transaction is active, fully supporting Python DB-API 2.0 autocommit semantics.

```python
import pymysql

conn = pymysql.connect(
    host="127.0.0.1",
    port=3307,
    user="root",
    password="SecretPassword123",
    database="default",
    autocommit=True
)

with conn.cursor() as cursor:
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INT PRIMARY KEY,
            username TEXT,
            points INT
        )
    """)
    cursor.execute("INSERT INTO users (id, username, points) VALUES (1, 'alice', 100)")
    cursor.execute("SELECT * FROM users WHERE points > 50")
    print(cursor.fetchall())
```

#### 3. Python (`SQLAlchemy`)
```python
from sqlalchemy import create_engine, text

engine = create_engine("mysql+pymysql://root:SecretPassword123@127.0.0.1:3307/default")

with engine.connect() as conn:
    conn.execute(text("CREATE TABLE IF NOT EXISTS items (id INT PRIMARY KEY, name TEXT)"))
    conn.execute(text("INSERT INTO items (id, name) VALUES (1, 'Widget')"))
    conn.commit()
    result = conn.execute(text("SELECT * FROM items"))
    for row in result:
        print(row)
```

---

### Connecting via PostgreSQL Protocol (Default Port: 5432)

#### 1. Official `psql` CLI
```bash
psql -h 127.0.0.1 -p 5432 -U root -d default
```

#### 2. Python (`psycopg2` / `pg8000`)
```python
import pg8000.native

con = pg8000.native.Connection(
    user="root",
    password="SecretPassword123",
    host="127.0.0.1",
    port=5432,
    database="default"
)

con.run("CREATE TABLE IF NOT EXISTS events (id INT PRIMARY KEY, name TEXT)")
con.run("INSERT INTO events (id, name) VALUES (1, 'Login')")
for row in con.run("SELECT * FROM events"):
    print(row)
```

---

## 4. SQL Dialect Reference

### Data Definition Language (DDL)

```sql
-- Tables (supports IF NOT EXISTS and table-level PRIMARY KEY constraint)
CREATE TABLE IF NOT EXISTS users (
    id INT PRIMARY KEY,
    email TEXT,
    created_at BIGINT
);

CREATE TABLE IF NOT EXISTS orders (
    order_id INT,
    user_id INT,
    amount DOUBLE,
    PRIMARY KEY (order_id),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- Drop table
DROP TABLE IF EXISTS old_table;

-- Indexes
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
DROP INDEX IF EXISTS idx_users_email ON users;
```

#### Supported Types
- `INT` / `INTEGER` (64-bit signed integer)
- `BIGINT` (64-bit signed integer)
- `DOUBLE` / `FLOAT` (64-bit floating point)
- `BOOLEAN` / `BOOL` (true/false)
- `TEXT` / `VARCHAR` (UTF-8 strings)
- `BLOB` (binary data)

---

### Data Manipulation Language (DML)

```sql
-- Insert
INSERT INTO users (id, email, created_at) VALUES (1, 'user@example.com', 1700000000);

-- Update
UPDATE users SET email = 'new@example.com' WHERE id = 1;

-- Delete
DELETE FROM users WHERE id = 1;
```

---

### Transactions & Concurrency (MVCC)

`henchDB` features lock-free optimistic concurrency and multi-version concurrency control:
- **Default isolation**: `REPEATABLE READ` with read-snapshot pinning.
- **`READ COMMITTED`**: Statement-level snapshot evaluation.
- Safe autocommit: Issuing `COMMIT` or `ROLLBACK` when no transaction is opened returns an immediate success without error.

```sql
BEGIN;
UPDATE users SET email = 'updated@domain.com' WHERE id = 1;
COMMIT;

-- Safe no-op outside active txn:
COMMIT;    -- Returns OK
ROLLBACK;  -- Returns OK
```

---

### Queries, Joins & Aggregates

```sql
-- Filtering with standard operators (=, !=, <, <=, >, >=, AND, OR, NOT, IN, BETWEEN, LIKE)
SELECT * FROM users WHERE (id >= 10 AND id <= 50) OR email LIKE '%@gmail.com';

-- Joins (equi-joins lower to high-speed Hash Join, nested-loop fallback)
SELECT u.email, o.amount
FROM users u
JOIN orders o ON u.id = o.user_id
WHERE o.amount > 100.0;

-- Aggregations & Grouping
SELECT user_id, COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount)
FROM orders
GROUP BY user_id
ORDER BY user_id ASC
LIMIT 10;

-- Subqueries (correlated and uncorrelated IN / EXISTS / scalar / derived tables)
SELECT email FROM users WHERE id IN (SELECT user_id FROM orders WHERE amount > 500);
```

---

### System Catalog & Metadata

```sql
SHOW TABLES;
SHOW DATABASES;
SHOW STATUS;
SHOW ENGINE;
SHOW PROCESSLIST;
SHOW GRANTS;

-- Standard PostgreSQL & MySQL virtual catalog tables
SELECT * FROM information_schema.tables;
SELECT * FROM pg_catalog.pg_tables;

-- Built-in inspection functions
SELECT version(), current_database(), current_schema(), user();
```

---

## 5. Observability & Telemetry

`server.exe` exposes standard Prometheus metrics and health check probes on HTTP port `9100` (configured with `--metrics-port`):

- **Health Probe**: `GET http://localhost:9100/health` (Returns HTTP 200 `healthy`)
- **Liveness Probe**: `GET http://localhost:9100/live` (Returns HTTP 200 `alive`)
- **Prometheus Metrics**: `GET http://localhost:9100/metrics`

### Sample Prometheus Metrics Available
- `henchdb_queries_total`: Total query executions
- `henchdb_commits_total`: Transaction commit count
- `henchdb_rollbacks_total`: Transaction abort / rollback count
- `henchdb_active_connections`: Current active client socket count
- `henchdb_query_duration_seconds`: Histogram of query execution latencies
