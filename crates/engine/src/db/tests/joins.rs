//! Join tests: nested-loop + hash joins, ordering, access paths.

use super::*;

#[test]
fn join_inner_left_and_group_by() {
    let dir = std::env::temp_dir().join(format!("hdbjoin_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();
    db.execute(&mut s, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)").unwrap();
    db.execute(&mut s, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, qty INT)").unwrap();
    db.execute(&mut s, "INSERT INTO users VALUES (1, 'ann'), (2, 'bob'), (3, 'cat')").unwrap();
    db.execute(&mut s, "INSERT INTO orders VALUES (10, 1, 3), (11, 1, 5), (12, 2, 7), (13, 9, 1)").unwrap();

    // INNER JOIN with qualified refs + WHERE + ORDER BY.
    let out = db.execute(&mut s, "SELECT users.name, orders.qty FROM users JOIN orders ON users.id = orders.user_id WHERE orders.qty >= 5 ORDER BY orders.qty").unwrap();
    assert_eq!(out.columns, vec!["name", "qty"]);
    assert_eq!(out.rows.len(), 2);
    assert_eq!(out.rows[0], vec![Datum::Text("ann".into()), Datum::Int(5)]);
    assert_eq!(out.rows[1], vec![Datum::Text("bob".into()), Datum::Int(7)]);

    // LEFT JOIN keeps unmatched left rows with NULLs.
    let out = db.execute(&mut s, "SELECT users.name, orders.qty FROM users LEFT JOIN orders ON users.id = orders.user_id ORDER BY users.id, orders.qty").unwrap();
    assert_eq!(out.rows.len(), 4); // ann×2, bob×1, cat×NULL
    assert_eq!(out.rows[3], vec![Datum::Text("cat".into()), Datum::Null]);

    // Star over a join qualifies only colliding names.
    let out = db.execute(&mut s, "SELECT * FROM users JOIN orders ON users.id = orders.user_id WHERE users.id = 1").unwrap();
    assert!(out.columns.contains(&"users.id".to_string()));
    assert!(out.columns.contains(&"orders.id".to_string()));
    assert!(out.columns.contains(&"name".to_string()));
    assert_eq!(out.rows.len(), 2);

    // GROUP BY with aggregates + COUNT(*) per group (ordered by key).
    let out = db.execute(&mut s, "SELECT user_id, COUNT(*), SUM(qty) FROM orders GROUP BY user_id").unwrap();
    assert_eq!(out.columns, vec!["user_id", "COUNT(*)", "SUM(qty)"]);
    assert_eq!(out.rows.len(), 3);
    assert_eq!(out.rows[0], vec![Datum::Int(1), Datum::Int(2), Datum::Int(8)]);
    assert_eq!(out.rows[2], vec![Datum::Int(9), Datum::Int(1), Datum::Int(1)]);

    // GROUP BY over a join.
    let out = db.execute(&mut s, "SELECT users.name, SUM(orders.qty) FROM users JOIN orders ON users.id = orders.user_id GROUP BY users.name").unwrap();
    assert_eq!(out.rows.len(), 2);
    assert_eq!(out.rows[0], vec![Datum::Text("ann".into()), Datum::Int(8)]);
    assert_eq!(out.rows[1], vec![Datum::Text("bob".into()), Datum::Int(7)]);

    // Single-table GROUP BY.
    let out = db.execute(&mut s, "SELECT qty, COUNT(*) FROM orders GROUP BY qty ORDER BY qty DESC").unwrap();
    assert_eq!(out.rows[0], vec![Datum::Int(7), Datum::Int(1)]);

    // Errors: ambiguous bare column, non-grouped plain column, star grouping,
    // unknown table in qualifier, self-join without aliases.
    assert!(db.execute(&mut s, "SELECT id FROM users JOIN orders ON users.id = orders.user_id").is_err());
    assert!(db.execute(&mut s, "SELECT name, SUM(qty) FROM users JOIN orders ON users.id = orders.user_id").is_err());
    assert!(db.execute(&mut s, "SELECT * FROM orders GROUP BY qty").is_err());
    assert!(db.execute(&mut s, "SELECT nope.id FROM users JOIN orders ON users.id = orders.user_id").is_err());
    assert!(db.execute(&mut s, "SELECT * FROM users JOIN users ON users.id = users.id").is_err());

    // Joins honor read-your-own-writes inside explicit transactions.
    db.execute(&mut s, "BEGIN").unwrap();
    db.execute(&mut s, "INSERT INTO orders VALUES (20, 3, 4)").unwrap();
    let out = db.execute(&mut s, "SELECT users.name, orders.qty FROM users JOIN orders ON users.id = orders.user_id WHERE users.id = 3").unwrap();
    assert_eq!(out.rows.len(), 1);
    assert_eq!(out.rows[0], vec![Datum::Text("cat".into()), Datum::Int(4)]);
    db.execute(&mut s, "ROLLBACK").unwrap();
    let out = db.execute(&mut s, "SELECT users.name, orders.qty FROM users JOIN orders ON users.id = orders.user_id WHERE users.id = 3").unwrap();
    assert!(out.rows.is_empty());

    // Prepare-time describe works over joins (star qualifies collisions).
    let cols = db.describe(&s, "SELECT users.name, SUM(orders.qty) FROM users JOIN orders ON users.id = orders.user_id").unwrap();
    assert_eq!(cols[0].0, "name");
    assert_eq!(cols[1].0, "SUM(orders.qty)");
    let cols = db.describe(&s, "SELECT * FROM users JOIN orders ON users.id = orders.user_id").unwrap();
    assert!(cols.iter().any(|(n, _)| n == "users.id"));
    assert!(cols.iter().any(|(n, _)| n == "orders.id"));
    assert!(cols.iter().any(|(n, _)| n == "name"));
    assert!(db.describe(&s, "SELECT nope.x FROM users JOIN orders ON users.id = orders.user_id").is_err());
    let _ = fs::remove_dir_all(&dir);
}

// -- hash join (F5) ----------------------------------------------------------

fn hj_setup(dir: &Path) -> Database {
    let db = Database::open(dir).unwrap();
    let mut s = db.new_session();
    db.execute(&mut s, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute(
        &mut s,
        "CREATE TABLE orders (oid INT PRIMARY KEY, uid INT, amt FLOAT)",
    )
    .unwrap();
    db.execute(
        &mut s,
        "INSERT INTO users VALUES (1, 'ann'), (2, 'bob'), (3, 'cat')",
    )
    .unwrap();
    // uid 2 has two orders (one-to-many); uid 99 matches nothing; uid NULL
    // must never match (not even the NULL-uid row on the other side).
    db.execute(
        &mut s,
        "INSERT INTO orders VALUES (10, 1, 5.0), (11, 2, 7.5), (12, 2, 1.0), (13, 99, 3.0), (14, NULL, 9.0)",
    )
    .unwrap();
    db
}

#[test]
fn hash_join_inner_multirow() {
    let dir = std::env::temp_dir().join(format!("hdbhj1_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = hj_setup(&dir);
    let mut s = db.new_session();
    let out = db
        .execute(
            &mut s,
            "SELECT users.name, orders.amt FROM users JOIN orders ON users.id = orders.uid ORDER BY orders.oid",
        )
        .unwrap();
    assert_eq!(out.columns, vec!["name".to_string(), "amt".to_string()]);
    let got: Vec<(String, f64)> = out
        .rows
        .iter()
        .map(|r| match (&r[0], &r[1]) {
            (Datum::Text(n), Datum::Float(a)) => (n.clone(), *a),
            other => panic!("unexpected row {other:?}"),
        })
        .collect();
    assert_eq!(
        got,
        vec![
            ("ann".into(), 5.0),
            ("bob".into(), 7.5),
            ("bob".into(), 1.0),
        ]
    );
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn hash_join_left_padding_and_null_keys() {
    let dir = std::env::temp_dir().join(format!("hdbhj2_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = hj_setup(&dir);
    let mut s = db.new_session();
    // cat (id 3) has no orders -> NULL-padded.
    let out = db
        .execute(
            &mut s,
            "SELECT users.name, orders.oid FROM users LEFT JOIN orders ON users.id = orders.uid ORDER BY users.id, orders.oid",
        )
        .unwrap();
    assert_eq!(out.rows.len(), 4);
    assert_eq!(out.rows[3][0], Datum::Text("cat".into()));
    assert_eq!(out.rows[3][1], Datum::Null);
    // NULL uid row never joins: orders LEFT JOIN users pads uid 99 and
    // uid NULL alike (NULL = NULL does not match).
    let out = db
        .execute(
            &mut s,
            "SELECT orders.oid, users.name FROM orders LEFT JOIN users ON orders.uid = users.id ORDER BY orders.oid",
        )
        .unwrap();
    assert_eq!(out.rows.len(), 5);
    assert_eq!(out.rows[3][0], Datum::Int(13));
    assert_eq!(out.rows[3][1], Datum::Null);
    assert_eq!(out.rows[4][0], Datum::Int(14));
    assert_eq!(out.rows[4][1], Datum::Null);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn hash_join_chained_three_tables() {
    let dir = std::env::temp_dir().join(format!("hdbhj3_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = hj_setup(&dir);
    let mut s = db.new_session();
    db.execute(&mut s, "CREATE TABLE ship (oid INT PRIMARY KEY, method TEXT)")
        .unwrap();
    db.execute(
        &mut s,
        "INSERT INTO ship VALUES (10, 'air'), (11, 'sea')",
    )
    .unwrap();
    let out = db
        .execute(
            &mut s,
            "SELECT users.name, ship.method FROM users JOIN orders ON users.id = orders.uid JOIN ship ON orders.oid = ship.oid ORDER BY users.name, ship.method",
        )
        .unwrap();
    assert_eq!(out.rows.len(), 2);
    assert_eq!(out.rows[0][0], Datum::Text("ann".into()));
    assert_eq!(out.rows[0][1], Datum::Text("air".into()));
    assert_eq!(out.rows[1][0], Datum::Text("bob".into()));
    assert_eq!(out.rows[1][1], Datum::Text("sea".into()));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn hash_join_empty_sides() {
    let dir = std::env::temp_dir().join(format!("hdbhj4_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();
    db.execute(&mut s, "CREATE TABLE a (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute(&mut s, "CREATE TABLE b (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute(&mut s, "INSERT INTO a VALUES (1, 1), (2, 2)").unwrap();
    // Empty build/probe sides.
    let out = db
        .execute(&mut s, "SELECT * FROM a JOIN b ON a.v = b.v")
        .unwrap();
    assert!(out.rows.is_empty());
    let out = db
        .execute(&mut s, "SELECT a.id, b.id FROM a LEFT JOIN b ON a.v = b.v ORDER BY a.id")
        .unwrap();
    assert_eq!(out.rows.len(), 2);
    assert_eq!(out.rows[0][1], Datum::Null);
    let out = db
        .execute(&mut s, "SELECT * FROM b JOIN a ON b.v = a.v")
        .unwrap();
    assert!(out.rows.is_empty());
    let out = db
        .execute(&mut s, "SELECT * FROM b LEFT JOIN a ON b.v = a.v")
        .unwrap();
    assert!(out.rows.is_empty());
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn hash_join_compound_and_fallback_paths() {
    let dir = std::env::temp_dir().join(format!("hdbhj5_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = hj_setup(&dir);
    let mut s = db.new_session();
    // Equi-key + residual predicate: only bob's 7.5 order survives.
    let out = db
        .execute(
            &mut s,
            "SELECT users.name, orders.amt FROM users JOIN orders ON users.id = orders.uid AND orders.amt > 5.0 ORDER BY orders.oid",
        )
        .unwrap();
    assert_eq!(out.rows.len(), 1);
    assert_eq!(out.rows[0][0], Datum::Text("bob".into()));
    // No equi-key: nested-loop fallback stays correct.
    let out = db
        .execute(
            &mut s,
            "SELECT users.name FROM users JOIN orders ON users.id > orders.uid ORDER BY users.name",
        )
        .unwrap();
    assert!(!out.rows.is_empty());
    // Flipped operand order still hashes.
    let out = db
        .execute(
            &mut s,
            "SELECT COUNT(*) FROM users JOIN orders ON orders.uid = users.id",
        )
        .unwrap();
    assert_eq!(out.rows[0][0], Datum::Int(3));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn hash_join_key_normalization() {
    use super::super::plan::join_key;
    use crate::types::Datum;
    // INT/FLOAT coerce like the evaluator.
    assert_eq!(join_key(&Datum::Int(1)), join_key(&Datum::Float(1.0)));
    assert_ne!(join_key(&Datum::Int(1)), join_key(&Datum::Float(1.5)));
    // NULL and NaN never match.
    assert_eq!(join_key(&Datum::Null), None);
    assert_eq!(join_key(&Datum::Float(f64::NAN)), None);
    // Parseable text collapses to DateTime, like coerce_pair.
    let dt = Datum::DateTime(1_000_000);
    assert_eq!(join_key(&Datum::Text("1970-01-01 00:00:01".into())), join_key(&dt));
    assert_ne!(
        join_key(&Datum::Text("not-a-date".into())),
        join_key(&dt)
    );
}

#[test]
fn hash_join_large_scale() {
    let dir = std::env::temp_dir().join(format!("hdbhj6_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();
    db.execute(&mut s, "CREATE TABLE big_a (id INT PRIMARY KEY, g INT)")
        .unwrap();
    db.execute(&mut s, "CREATE TABLE big_b (id INT PRIMARY KEY, g INT)")
        .unwrap();
    // 1,500 rows per side in batched multi-row INSERTs; g cycles 0..150 so
    // every probe hits 10 build rows -> 15,000 output rows.
    for base in (0..1500).step_by(150) {
        let vals: Vec<String> =
            (base..base + 150).map(|i| format!("({i}, {})", i % 150)).collect();
        db.execute(&mut s, &format!("INSERT INTO big_a VALUES {}", vals.join(", ")))
            .unwrap();
        db.execute(&mut s, &format!("INSERT INTO big_b VALUES {}", vals.join(", ")))
            .unwrap();
    }
    let out = db
        .execute(&mut s, "SELECT COUNT(*) FROM big_a JOIN big_b ON big_a.g = big_b.g")
        .unwrap();
    assert_eq!(out.rows[0][0], Datum::Int(15_000));
    let _ = fs::remove_dir_all(&dir);
}

fn ord_join(table: &str, left: &str, right: &str) -> crate::sql::JoinClause {
    use crate::sql::{CmpOp, Expr};
    crate::sql::JoinClause {
        kind: crate::sql::JoinKind::Inner,
        table: table.into(),
        on: Expr::Cmp {
            left: Box::new(Expr::Column(left.into())),
            op: CmpOp::Eq,
            right: Box::new(Expr::Column(right.into())),
        },
    }
}

#[test]
fn join_order_smallest_ready_first() {
    use super::super::plan::order_joins;
    // a(0) <- b(1), a(0) <- c(2): both ready, smallest (c) wins.
    let joins = vec![ord_join("b", "a.id", "b.a_id"), ord_join("c", "a.id", "c.a_id")];
    let resolve = |n: &str| match n {
        "a.id" => Ok(0),
        "b.a_id" => Ok(1),
        "c.a_id" => Ok(2),
        _ => Err(Error::ColumnNotFound(n.into())),
    };
    assert_eq!(order_joins(&joins, &[1000, 800, 5], &resolve), vec![0, 2, 1]);
    assert_eq!(order_joins(&joins, &[1000, 5, 800], &resolve), vec![0, 1, 2]);
    // Chained dep (c needs b): b first regardless of size.
    let chained = vec![ord_join("b", "a.id", "b.a_id"), ord_join("c", "b.id", "c.b_id")];
    let resolve2 = |n: &str| match n {
        "a.id" => Ok(0),
        "b.a_id" | "b.id" => Ok(1),
        "c.b_id" => Ok(2),
        _ => Err(Error::ColumnNotFound(n.into())),
    };
    assert_eq!(order_joins(&chained, &[1000, 800, 5], &resolve2), vec![0, 1, 2]);
    // LEFT is a barrier: stays ahead of later INNER joins.
    let mut left_first = vec![ord_join("b", "a.id", "b.a_id"), ord_join("c", "a.id", "c.a_id")];
    left_first[0].kind = crate::sql::JoinKind::Left;
    assert_eq!(order_joins(&left_first, &[1000, 5, 800], &resolve), vec![0, 1, 2]);
    // INNER joins after the barrier still reorder among themselves.
    let mut after = vec![
        ord_join("b", "a.id", "b.a_id"),
        ord_join("c", "a.id", "c.a_id"),
        ord_join("d", "a.id", "d.a_id"),
    ];
    after[0].kind = crate::sql::JoinKind::Left;
    let resolve3 = |n: &str| match n {
        "a.id" => Ok(0),
        "b.a_id" => Ok(1),
        "c.a_id" => Ok(2),
        "d.a_id" => Ok(3),
        _ => Err(Error::ColumnNotFound(n.into())),
    };
    assert_eq!(
        order_joins(&after, &[1000, 900, 700, 5], &resolve3),
        vec![0, 1, 3, 2]
    );
}

#[test]
fn join_order_executes_correctly_star_stable() {
    let dir = std::env::temp_dir().join(format!("hdbjo_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();
    // Skewed sizes: written order joins mid (50) before tiny (5); greedy
    // flips them. Star column order must still follow written order.
    db.execute(&mut s, "CREATE TABLE big (id INT PRIMARY KEY, m INT, t INT)")
        .unwrap();
    db.execute(&mut s, "CREATE TABLE mid (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute(&mut s, "CREATE TABLE tiny (id INT PRIMARY KEY, w INT)").unwrap();
    let mut vals = Vec::new();
    for i in 0..500 {
        vals.push(format!("({i}, {}, {})", i % 50, i % 5));
    }
    db.execute(&mut s, &format!("INSERT INTO big VALUES {}", vals.join(", ")))
        .unwrap();
    let mut vals = Vec::new();
    for i in 0..50 {
        vals.push(format!("({i}, {i})"));
    }
    db.execute(&mut s, &format!("INSERT INTO mid VALUES {}", vals.join(", ")))
        .unwrap();
    db.execute(&mut s, "INSERT INTO tiny VALUES (0, 100), (1, 101), (2, 102), (3, 103), (4, 104)")
        .unwrap();
    let out = db
        .execute(
            &mut s,
            "SELECT * FROM big JOIN mid ON big.m = mid.id JOIN tiny ON big.t = tiny.id ORDER BY big.id",
        )
        .unwrap();
    // Written order columns, not execution order (`id` collides in all
    // three tables, so all three qualify).
    assert_eq!(
        out.columns,
        vec!["big.id", "m", "t", "mid.id", "v", "tiny.id", "w"]
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>()
    );
    assert_eq!(out.rows.len(), 500);
    assert_eq!(out.rows[0][0], Datum::Int(0));
    assert_eq!(out.rows[499][0], Datum::Int(499));
    // LEFT + reorder mix stays correct.
    let out = db
        .execute(
            &mut s,
            "SELECT COUNT(*) FROM tiny LEFT JOIN big ON tiny.id = big.t JOIN mid ON big.m = mid.id",
        )
        .unwrap();
    assert_eq!(out.rows[0][0], Datum::Int(500));
    let _ = fs::remove_dir_all(&dir);
}
