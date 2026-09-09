//! Priority 16: RBAC enforcement — principals, grants, scopes, denials.

use super::*;
use crate::sql::Privilege;

fn setup(dir: &Path) -> Database {
    let db = Database::open(dir).unwrap();
    let mut s = db.new_session();
    db.execute(&mut s, "CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute(&mut s, "INSERT INTO t VALUES (1, 10), (2, 20)").unwrap();
    db.execute(&mut s, "CREATE DATABASE shop").unwrap();
    db.execute(&mut s, "USE shop").unwrap();
    db.execute(
        &mut s,
        "CREATE TABLE orders (id INT PRIMARY KEY, total INT)",
    )
    .unwrap();
    db.execute(&mut s, "INSERT INTO orders VALUES (1, 100)").unwrap();
    db.execute(&mut s, "CREATE TABLE items (id INT PRIMARY KEY)").unwrap();
    db.execute(&mut s, "USE default").unwrap();
    db
}

fn tmp(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("hdbpriv_{name}_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    dir
}

fn as_user(db: &Database, name: &str) -> Session {
    let mut s = db.new_session();
    s.user = name.into();
    s
}

fn denied_text(out: std::result::Result<Output, crate::error::Error>) -> String {
    format!("{}", out.unwrap_err())
}

#[test]
fn grant_revoke_lifecycle() {
    let dir = tmp("life");
    let db = setup(&dir);
    let mut root = db.new_session();
    // CREATE USER (+ idempotence flags, host normalization).
    db.execute(&mut root, "CREATE USER 'alice'@'localhost' IDENTIFIED BY 'pw'").unwrap();
    db.execute(&mut root, "CREATE USER IF NOT EXISTS alice IDENTIFIED BY 'pw'").unwrap();
    assert!(db.execute(&mut root, "CREATE USER alice IDENTIFIED BY 'pw'").is_err());
    assert!(db.execute(&mut root, "CREATE USER alice IDENTIFIED BY 42;").is_err());
    // GRANT then SHOW GRANTS as the user herself.
    db.execute(&mut root, "GRANT SELECT, INSERT ON shop.* TO alice").unwrap();
    let mut alice = as_user(&db, "alice");
    let out = db.execute(&mut alice, "SHOW GRANTS").unwrap();
    assert_eq!(out.columns, vec!["Grants".to_string()]);
    assert_eq!(out.rows.len(), 2);
    assert!(out.rows.iter().any(|r| r[0].to_string().contains("GRANT SELECT ON shop.*")));
    // REVOKE narrows; unknown revokes and unknown users fail cleanly.
    db.execute(&mut root, "REVOKE INSERT ON shop.* FROM alice").unwrap();
    let out = db.execute(&mut alice, "SHOW GRANTS").unwrap();
    assert_eq!(out.rows.len(), 1);
    assert!(db.execute(&mut root, "REVOKE INSERT ON shop.* FROM alice").is_err());
    assert!(db.execute(&mut root, "GRANT SELECT ON *.* TO ghost").is_err());
    // DROP USER (+ flags) removes grants; root is untouchable.
    db.execute(&mut root, "DROP USER alice").unwrap();
    assert!(db.execute(&mut root, "DROP USER alice").is_err());
    db.execute(&mut root, "DROP USER IF EXISTS alice").unwrap();
    assert!(db.execute(&mut root, "DROP USER root").is_err());
    assert!(db.execute(&mut root, "GRANT SELECT ON *.* TO root").is_err());
    assert!(db.execute(&mut root, "REVOKE SELECT ON *.* FROM root").is_err());
    // ALTER USER on missing users fails; on real ones stages a password.
    assert!(db.execute(&mut root, "ALTER USER ghost IDENTIFIED BY 'x'").is_err());
    db.execute(&mut root, "CREATE USER bob IDENTIFIED BY 'a'").unwrap();
    db.execute(&mut root, "ALTER USER bob IDENTIFIED BY 'b'").unwrap();
    let (_, _, pending) = db.export_privileges();
    assert!(pending.iter().any(|(u, p)| u == "bob" && p == "b"));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn select_only_enforcement() {
    let dir = tmp("sel");
    let db = setup(&dir);
    let mut root = db.new_session();
    db.execute(&mut root, "CREATE USER alice IDENTIFIED BY 'pw'").unwrap();
    db.execute(&mut root, "GRANT SELECT ON shop.orders TO alice").unwrap();
    let mut alice = as_user(&db, "alice");
    // Allowed read (point fast path + full scan + filtered).
    assert!(db.execute(&mut alice, "SELECT * FROM shop.orders").is_ok());
    assert!(db.execute(&mut alice, "SELECT total FROM shop.orders WHERE id = 1").is_ok());
    // DML names are bare and resolve to the session database: USE first.
    db.execute(&mut alice, "USE shop").unwrap();
    // Writes denied with the exact 1142 message shape.
    assert_eq!(
        denied_text(db.execute(&mut alice, "INSERT INTO orders VALUES (2, 5)")),
        "INSERT command denied to user 'alice'@'localhost' for table 'shop.orders'"
    );
    assert_eq!(
        denied_text(db.execute(&mut alice, "UPDATE orders SET total = 1 WHERE id = 1")),
        "UPDATE command denied to user 'alice'@'localhost' for table 'shop.orders'"
    );
    assert_eq!(
        denied_text(db.execute(&mut alice, "DELETE FROM orders WHERE id = 1")),
        "DELETE command denied to user 'alice'@'localhost' for table 'shop.orders'"
    );
    // Other tables (even in other databases) denied; subquery and JOIN
    // sources are collected too.
    assert!(db.execute(&mut alice, "SELECT * FROM items").is_err());
    db.execute(&mut alice, "USE default").unwrap();
    assert!(db.execute(&mut alice, "SELECT * FROM t").is_err());
    assert!(db
        .execute(&mut alice, "SELECT * FROM shop.orders WHERE id IN (SELECT id FROM shop.items)")
        .is_err());
    assert!(db
        .execute(
            &mut alice,
            "SELECT * FROM shop.orders JOIN shop.items ON shop.orders.id = shop.items.id"
        )
        .is_err());
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn wildcard_scope_matching() {
    let dir = tmp("wild");
    let db = setup(&dir);
    let mut root = db.new_session();
    // Global ALL passes everything, including DDL and admin statements.
    db.execute(&mut root, "CREATE USER bob IDENTIFIED BY 'pw'").unwrap();
    db.execute(&mut root, "GRANT ALL PRIVILEGES ON *.* TO bob").unwrap();
    let mut bob = as_user(&db, "bob");
    assert!(db.execute(&mut bob, "SELECT * FROM t").is_ok());
    assert!(db.execute(&mut bob, "INSERT INTO t VALUES (9, 9)").is_ok());
    assert!(db.execute(&mut bob, "CREATE TABLE bob_t (id INT PRIMARY KEY)").is_ok());
    assert!(db.execute(&mut bob, "CREATE DATABASE bobdb").is_ok());
    assert!(db.execute(&mut bob, "GRANT SELECT ON t TO bob").is_ok());
    // Database scope covers member tables only.
    db.execute(&mut root, "CREATE USER carol IDENTIFIED BY 'pw'").unwrap();
    db.execute(&mut root, "GRANT SELECT ON shop.* TO carol").unwrap();
    let mut carol = as_user(&db, "carol");
    assert!(db.execute(&mut carol, "SELECT * FROM shop.orders").is_ok());
    assert!(db.execute(&mut carol, "SELECT * FROM shop.items").is_ok());
    assert!(db.execute(&mut carol, "SELECT * FROM t").is_err());
    assert!(db.execute(&mut carol, "CREATE TABLE shop.newt (id INT PRIMARY KEY)").is_err());
    // Table scope is exact.
    db.execute(&mut root, "CREATE USER dave IDENTIFIED BY 'pw'").unwrap();
    db.execute(&mut root, "GRANT SELECT ON shop.orders TO dave").unwrap();
    let mut dave = as_user(&db, "dave");
    assert!(db.execute(&mut dave, "SELECT * FROM shop.orders").is_ok());
    assert!(db.execute(&mut dave, "SELECT * FROM shop.items").is_err());
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn bare_grant_resolves_to_session_db() {
    let dir = tmp("bare");
    let db = setup(&dir);
    let mut root = db.new_session();
    db.execute(&mut root, "CREATE USER alice IDENTIFIED BY 'pw'").unwrap();
    db.execute(&mut root, "USE shop").unwrap();
    db.execute(&mut root, "GRANT SELECT ON orders TO alice").unwrap();
    // Stored (and shown) as the concrete shop.orders scope.
    let out = db.execute(&mut root, "SHOW GRANTS FOR alice").unwrap();
    assert!(out.rows.iter().any(|r| r[0].to_string().contains("ON shop.orders")));
    let mut alice = as_user(&db, "alice");
    db.execute(&mut alice, "USE shop").unwrap();
    assert!(db.execute(&mut alice, "SELECT * FROM orders").is_ok());
    // From another database the bare grant does not apply (deny precedes
    // the missing-table error, so no existence oracle).
    db.execute(&mut alice, "USE default").unwrap();
    assert_eq!(
        denied_text(db.execute(&mut alice, "SELECT * FROM orders")),
        "SELECT command denied to user 'alice'@'localhost' for table 'default.orders'"
    );
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn admin_operations_require_admin() {
    let dir = tmp("admin");
    let db = setup(&dir);
    let mut root = db.new_session();
    db.execute(&mut root, "CREATE USER alice IDENTIFIED BY 'pw'").unwrap();
    db.execute(&mut root, "GRANT SELECT ON *.* TO alice").unwrap();
    let mut alice = as_user(&db, "alice");
    // User management, promotion, backup, checkpoint: admin only.
    for sql in [
        "GRANT SELECT ON t TO alice",
        "REVOKE SELECT ON *.* FROM alice",
        "CREATE USER x IDENTIFIED BY 'y'",
        "DROP USER alice",
        "PROMOTE",
        "BACKUP DATABASE TO '/tmp/x.hdb'",
        "CHECKPOINT",
    ] {
        assert!(db.execute(&mut alice, sql).is_err(), "allowed: {sql}");
    }
    // SHOW GRANTS FOR others is admin-only; self always allowed.
    assert!(db.execute(&mut alice, "SHOW GRANTS").is_ok());
    assert!(db.execute(&mut alice, "SHOW GRANTS FOR alice").is_ok());
    assert!(db.execute(&mut alice, "SHOW GRANTS FOR root").is_err());
    // A global-ALL holder (non-root) may administer.
    db.execute(&mut root, "CREATE USER bob IDENTIFIED BY 'pw'").unwrap();
    db.execute(&mut root, "GRANT ALL PRIVILEGES ON *.* TO bob").unwrap();
    let mut bob = as_user(&db, "bob");
    assert!(db.execute(&mut bob, "GRANT SELECT ON t TO alice").is_ok());
    assert!(db.execute(&mut bob, "SHOW GRANTS FOR alice").is_ok());
    // DDL privilege mapping: index ops follow CREATE/DROP (bare names
    // resolve to the session database; dotted DDL names do not parse).
    db.execute(&mut root, "GRANT CREATE ON shop.* TO alice").unwrap();
    assert!(db.execute(&mut alice, "CREATE INDEX i ON orders (total)").is_err());
    db.execute(&mut alice, "USE shop").unwrap();
    assert!(db.execute(&mut alice, "CREATE INDEX i ON orders (total)").is_ok());
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn ddl_and_unknown_users() {
    let dir = tmp("ddl");
    let db = setup(&dir);
    let mut root = db.new_session();
    // Never-created identities hold no grants and deny.
    let mut ghost = as_user(&db, "ghost");
    assert!(db.execute(&mut ghost, "SELECT * FROM t").is_err());
    // Database DDL needs global scope even with db-level CREATE; table
    // DDL accepts the database scope or the exact table scope (DDL names
    // are bare and resolve to the session database).
    db.execute(&mut root, "CREATE USER alice IDENTIFIED BY 'pw'").unwrap();
    db.execute(&mut root, "GRANT CREATE ON shop.* TO alice").unwrap();
    let mut alice = as_user(&db, "alice");
    assert!(db.execute(&mut alice, "CREATE DATABASE newdb").is_err());
    assert!(db.execute(&mut alice, "DROP DATABASE shop").is_err());
    db.execute(&mut alice, "USE shop").unwrap();
    assert!(db.execute(&mut alice, "CREATE TABLE nt (id INT PRIMARY KEY)").is_ok());
    assert!(db.execute(&mut alice, "DROP TABLE nt").is_err());
    db.execute(&mut root, "GRANT DROP ON shop.nt TO alice").unwrap();
    assert!(db.execute(&mut alice, "DROP TABLE nt").is_ok());
    // ANALYZE is a read of the table.
    assert!(db.execute(&mut alice, "ANALYZE TABLE orders").is_err());
    db.execute(&mut root, "GRANT SELECT ON shop.orders TO alice").unwrap();
    assert!(db.execute(&mut alice, "ANALYZE TABLE orders").is_ok());
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn privilege_import_export_roundtrip() {
    use crate::db::privilege::GrantRule;
    let dir = tmp("imp");
    let db = setup(&dir);
    let rules = vec![
        GrantRule { priv_: Privilege::Select, db: "shop".into(), tbl: "*".into() },
        GrantRule { priv_: Privilege::Insert, db: "shop".into(), tbl: "orders".into() },
    ];
    db.import_privileges(&[("alice".into(), rules.clone())]);
    let (users, tombstones, pending) = db.export_privileges();
    assert!(tombstones.is_empty() && pending.is_empty());
    assert_eq!(users, vec![("alice".to_string(), rules)]);
    // Imported grants enforce immediately.
    let mut alice = as_user(&db, "alice");
    assert!(db.execute(&mut alice, "SELECT * FROM shop.orders").is_ok());
    db.execute(&mut alice, "USE shop").unwrap();
    assert!(db.execute(&mut alice, "INSERT INTO orders VALUES (3, 3)").is_ok());
    assert!(db.execute(&mut alice, "DELETE FROM orders WHERE id = 1").is_err());
    assert_eq!(db.privilege_version(), 1);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn root_never_denied() {
    let dir = tmp("root");
    let db = setup(&dir);
    let mut root = db.new_session();
    // Root bypasses everything with no grants stored at all.
    assert!(db.execute(&mut root, "SELECT * FROM t").is_ok());
    assert!(db.execute(&mut root, "DELETE FROM t WHERE id = 1").is_ok());
    assert!(db.execute(&mut root, "DROP TABLE t").is_ok());
    // Root always reports its implicit global grant (nothing stored).
    let out = db.execute(&mut root, "SHOW GRANTS").unwrap();
    assert_eq!(out.rows.len(), 1);
    assert!(out.rows[0][0].to_string().contains("ALL PRIVILEGES ON *.*"));
    let out = db.execute(&mut root, "SHOW GRANTS FOR root").unwrap();
    assert_eq!(out.rows.len(), 1);
    assert!(out.rows[0][0].to_string().contains("ALL PRIVILEGES ON *.*"));
    let _ = fs::remove_dir_all(&dir);
}