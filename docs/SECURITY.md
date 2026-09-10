# Security Architecture & Hardening Guide — henchDB

## 1. Authentication & Password Security
- **Hashing Algorithm**: henchDB defaults to `caching_sha2_password` (SHA-256 with cryptographic salt), matching MySQL 8 security standards. Legacy `mysql_native_password` fallback is supported when configured.
- **Public Interface Guard**: If `--bind` is set to `0.0.0.0` or `::` and the administrative `root` account has an empty password, henchDB prints a loud warning to stderr at boot time.
- **Credential Storage**: Passwords and verifiers are stored in `auth.bin` v2 with strict file permissions and transactional update mechanics.

## 2. Role-Based Access Control (RBAC)
henchDB provides granular RBAC (`db/privilege.rs`):
- Supported privileges: `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `CREATE`, `DROP`, `ALL PRIVILEGES`.
- Scopes: Global (`*.*`), Database (`db.*`), and Table (`db.table`).
- Privilege management:
```sql
CREATE USER 'analyst'@'%' IDENTIFIED BY 'Password123!';
GRANT SELECT ON analytics.* TO 'analyst'@'%';
REVOKE INSERT, UPDATE, DELETE ON analytics.* FROM 'analyst'@'%';
SHOW GRANTS FOR 'analyst'@'%';
```
- **Deny-Before-Execute Gate**: Queries, prepared statements, and subquery references are checked prior to parsing tree desultory execution, preventing schema leakage.

## 3. Transport Layer Security (TLS)
TLS is supported for both MySQL and PostgreSQL wire connections using pure Rust `rustls`:
```bash
server serve \
  --dir /var/lib/henchdb/data \
  --tls-cert /etc/henchdb/cert.pem \
  --tls-key /etc/henchdb/key.pem
```
Clients can negotiate TLS via MySQL `CLIENT_SSL` or PostgreSQL `SSLRequest`.

## 4. Denial of Service (DoS) & Resource Governance
To prevent memory exhaustion and runaway queries:
- **Connection Ceilings**: `--max-connections` (default 1024) rejects connections with standard error codes (`1040: Too many connections` for MySQL, `53300` for PostgreSQL).
- **Result Size Clamping**:
  ```sql
  SET max_result_rows = 100000;
  SET max_result_bytes = 67108864; -- 64 MiB
  ```
- **Snapshot Age Limits**:
  ```sql
  SET max_snapshot_age = 60000; -- 60 seconds
  ```
  Prevents stale MVCC readers from pinning version buffers and delaying version garbage collection.
