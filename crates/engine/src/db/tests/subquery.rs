//! Subquery & derived-table suite: IN/EXISTS/scalar (uncorrelated and
//! correlated), NULL ternary logic, shape errors, derived FROM/JOIN,
//! DML with subqueries, and EXPLAIN smoke coverage.

use super::*;

fn subq_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("hdbsubq_{tag}_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    dir
}

fn shop(tag: &str) -> (Database, std::path::PathBuf) {
    // Unique per test: parallel tests share the process id.
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "hdbsubq_{tag}_{}_{}",
        std::process::id(),
        n
    ));
    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();
    db.execute(&mut s, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)").unwrap();
    db.execute(&mut s, "INSERT INTO users VALUES (1, 'ann'), (2, 'bob'), (3, 'cat')").unwrap();
    db.execute(&mut s, "CREATE TABLE orders (oid INT PRIMARY KEY, uid INT, amt INT)").unwrap();
    db.execute(
        &mut s,
        "INSERT INTO orders VALUES (10, 1, 100), (11, 1, 200), (12, 3, 50)",
    )
    .unwrap();
    (db, dir)
}

fn rows(db: &Database, sql: &str) -> Vec<Vec<Datum>> {
    db.execute(&mut db.new_session(), sql).unwrap().rows
}

#[test]
fn in_subquery_empty_matching_nonmatching() {
    let (db, dir) = shop("in");
    // Matching set.
    let r = rows(&db, "SELECT name FROM users WHERE id IN (SELECT uid FROM orders) ORDER BY name");
    assert_eq!(
        r,
        vec![vec![Datum::Text("ann".into())], vec![Datum::Text("cat".into())]]
    );
    // Empty set matches nothing.
    let r = rows(&db, "SELECT name FROM users WHERE id IN (SELECT uid FROM orders WHERE amt > 1000)");
    assert!(r.is_empty());
    // Non-matching literal-style set.
    let r = rows(&db, "SELECT name FROM users WHERE id IN (SELECT uid FROM orders WHERE uid = 2)");
    assert!(r.is_empty());
    // NOT IN complements (no NULLs in play).
    let r = rows(&db, "SELECT name FROM users WHERE id NOT IN (SELECT uid FROM orders) ORDER BY name");
    assert_eq!(r, vec![vec![Datum::Text("bob".into())]]);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn not_in_null_ternary_logic() {
    let dir = subq_dir("null");
    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();
    db.execute(&mut s, "CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    db.execute(&mut s, "INSERT INTO t VALUES (1), (2)").unwrap();
    db.execute(&mut s, "CREATE TABLE u (id INT PRIMARY KEY, v INT)").unwrap();
    // u.v holds (1, NULL): NOT IN must yield no rows at all (unknown),
    // while IN still matches the present value.
    db.execute(&mut s, "INSERT INTO u VALUES (1, 1), (2, NULL)").unwrap();
    let r = rows(&db, "SELECT id FROM t WHERE id NOT IN (SELECT v FROM u) ORDER BY id");
    assert!(r.is_empty(), "NOT IN with NULL must filter everything, got {r:?}");
    let r = rows(&db, "SELECT id FROM t WHERE id IN (SELECT v FROM u) ORDER BY id");
    assert_eq!(r, vec![vec![Datum::Int(1)]]);
    // NULL test value never matches, both directions.
    let r = rows(&db, "SELECT id FROM u WHERE v IN (SELECT id FROM t) ORDER BY id");
    assert_eq!(r, vec![vec![Datum::Int(1)]]);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn scalar_subquery_where_and_projection() {
    let (db, dir) = shop("scalar");
    // Scalar on the right.
    let r = rows(&db, "SELECT name FROM users WHERE id = (SELECT uid FROM orders WHERE oid = 12)");
    assert_eq!(r, vec![vec![Datum::Text("cat".into())]]);
    // Scalar on the left (parenthesized).
    let r = rows(&db, "SELECT name FROM users WHERE (SELECT MAX(amt) FROM orders) > 150");
    assert_eq!(r.len(), 3);
    let r = rows(&db, "SELECT name FROM users WHERE (SELECT MAX(amt) FROM orders) > 500");
    assert!(r.is_empty());
    // Empty scalar is NULL: comparison filters the row out.
    let r = rows(&db, "SELECT name FROM users WHERE id = (SELECT uid FROM orders WHERE oid = 999)");
    assert!(r.is_empty());
    // Projection scalar with and without alias.
    let out = db
        .execute(&mut db.new_session(), "SELECT name, (SELECT MAX(amt) FROM orders) AS m FROM users WHERE id = 1")
        .unwrap();
    assert_eq!(out.columns, vec!["name".to_string(), "m".to_string()]);
    assert_eq!(out.rows, vec![vec![Datum::Text("ann".into()), Datum::Int(200)]]);
    let out = db
        .execute(&mut db.new_session(), "SELECT (SELECT MAX(amt) FROM orders) FROM users WHERE id = 2")
        .unwrap();
    assert_eq!(out.columns, vec!["MAX(amt)".to_string()]);
    assert_eq!(out.rows, vec![vec![Datum::Int(200)]]);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn scalar_shape_errors() {
    let (db, dir) = shop("shape");
    // More than one row.
    let e = db.execute(
        &mut db.new_session(),
        "SELECT name FROM users WHERE id = (SELECT uid FROM orders)",
    );
    assert_eq!(
        e,
        Err(Error::ExecutionError("Subquery returns more than 1 row".into()))
    );
    // More than one column.
    let e = db.execute(
        &mut db.new_session(),
        "SELECT name FROM users WHERE id IN (SELECT oid, uid FROM orders)",
    );
    assert_eq!(
        e,
        Err(Error::InvalidQuery("Subquery must return only one column".into()))
    );
    let e = db.execute(
        &mut db.new_session(),
        "SELECT (SELECT oid, uid FROM orders) FROM users",
    );
    assert_eq!(
        e,
        Err(Error::InvalidQuery("Subquery must return only one column".into()))
    );
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn exists_uncorrelated_and_correlated() {
    let (db, dir) = shop("exists");
    // Uncorrelated EXISTS / NOT EXISTS.
    let r = rows(&db, "SELECT name FROM users WHERE EXISTS (SELECT oid FROM orders WHERE amt > 150) ORDER BY name");
    assert_eq!(r.len(), 3);
    let r = rows(&db, "SELECT name FROM users WHERE EXISTS (SELECT oid FROM orders WHERE amt > 5000)");
    assert!(r.is_empty());
    let r = rows(&db, "SELECT name FROM users WHERE NOT EXISTS (SELECT oid FROM orders WHERE amt > 5000) ORDER BY name");
    assert_eq!(r.len(), 3);
    // Correlated EXISTS: users with at least one big order.
    let r = rows(
        &db,
        "SELECT name FROM users WHERE EXISTS (SELECT oid FROM orders WHERE orders.uid = users.id AND amt >= 200) ORDER BY name",
    );
    assert_eq!(r, vec![vec![Datum::Text("ann".into())]]);
    // Correlated NOT EXISTS: users with no orders at all.
    let r = rows(
        &db,
        "SELECT name FROM users WHERE NOT EXISTS (SELECT oid FROM orders WHERE orders.uid = users.id) ORDER BY name",
    );
    assert_eq!(r, vec![vec![Datum::Text("bob".into())]]);
    // Correlated IN: same shape through membership.
    let r = rows(
        &db,
        "SELECT name FROM users WHERE users.id IN (SELECT uid FROM orders WHERE orders.amt > 150)",
    );
    assert_eq!(r, vec![vec![Datum::Text("ann".into())]]);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn derived_from_and_join() {
    let (db, dir) = shop("derived");
    // Derived FROM with filter + ordering on the outside.
    let r = rows(
        &db,
        "SELECT name FROM (SELECT id, name FROM users WHERE id > 1) AS d ORDER BY name",
    );
    assert_eq!(
        r,
        vec![vec![Datum::Text("bob".into())], vec![Datum::Text("cat".into())]]
    );
    // Join between a base table and a derived table.
    let r = rows(
        &db,
        "SELECT users.name FROM users JOIN (SELECT uid FROM orders WHERE amt > 100) AS big ON users.id = big.uid ORDER BY users.name",
    );
    assert_eq!(r, vec![vec![Datum::Text("ann".into())]]);
    // Derived with aggregation inside.
    let r = rows(
        &db,
        "SELECT users.name FROM users JOIN (SELECT uid, MAX(amt) FROM orders GROUP BY uid) AS m ON users.id = m.uid ORDER BY users.name",
    );
    assert_eq!(
        r,
        vec![vec![Datum::Text("ann".into())], vec![Datum::Text("cat".into())]]
    );
    // Star over derived exposes inner columns.
    let out = db
        .execute(&mut db.new_session(), "SELECT * FROM (SELECT id FROM users WHERE id = 2) AS d")
        .unwrap();
    assert_eq!(out.columns, vec!["id".to_string()]);
    assert_eq!(out.rows, vec![vec![Datum::Int(2)]]);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn dml_with_subqueries() {
    let (db, dir) = shop("dml");
    // UPDATE with an uncorrelated IN-subquery.
    let out = db
        .execute(&mut db.new_session(), "UPDATE users SET name = 'popular' WHERE id IN (SELECT uid FROM orders WHERE amt >= 200)")
        .unwrap();
    assert!(out.message.contains("1 row(s) updated"), "got {}", out.message);
    let r = rows(&db, "SELECT name FROM users WHERE id = 1");
    assert_eq!(r, vec![vec![Datum::Text("popular".into())]]);
    // DELETE with a correlated NOT EXISTS.
    let out = db
        .execute(
            &mut db.new_session(),
            "DELETE FROM users WHERE NOT EXISTS (SELECT oid FROM orders WHERE orders.uid = users.id)",
        )
        .unwrap();
    assert!(out.message.contains("1 row(s) deleted"), "got {}", out.message);
    let r = rows(&db, "SELECT name FROM users ORDER BY name");
    assert_eq!(
        r,
        vec![vec![Datum::Text("cat".into())], vec![Datum::Text("popular".into())]]
    );
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn explain_with_subqueries() {
    let (db, dir) = shop("explain");
    // EXPLAIN plans (not executes) and stays in shape over subqueries.
    let out = db
        .execute(&mut db.new_session(), "EXPLAIN SELECT * FROM users WHERE id IN (SELECT uid FROM orders)")
        .unwrap();
    assert_eq!(out.rows.len(), 1);
    let out = db
        .execute(
            &mut db.new_session(),
            "EXPLAIN ANALYZE SELECT * FROM (SELECT id FROM users) AS d WHERE d.id > 1",
        )
        .unwrap();
    assert_eq!(out.rows.len(), 1);
    assert_eq!(out.rows[0][5], Datum::Int(2));
    // prepare-time describe works over scalar subqueries.
    let cols = db
        .describe(&db.new_session(), "SELECT (SELECT MAX(amt) FROM orders) AS m FROM users")
        .unwrap();
    assert_eq!(cols.len(), 1);
    assert_eq!(cols[0].0, "m");
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn correlated_projection_scalar() {
    let (db, dir) = shop("corrproj");
    // Per-row correlated scalar in the projection list.
    let out = db
        .execute(
            &mut db.new_session(),
            "SELECT name, (SELECT MAX(amt) FROM orders WHERE orders.uid = users.id) AS m FROM users ORDER BY name",
        )
        .unwrap();
    assert_eq!(out.columns, vec!["name".to_string(), "m".to_string()]);
    assert_eq!(
        out.rows,
        vec![
            vec![Datum::Text("ann".into()), Datum::Int(200)],
            vec![Datum::Text("bob".into()), Datum::Null],
            vec![Datum::Text("cat".into()), Datum::Int(50)],
        ]
    );
    let _ = fs::remove_dir_all(&dir);
}
