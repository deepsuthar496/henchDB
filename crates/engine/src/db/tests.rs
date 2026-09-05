use super::*;

fn setup(dir: &Path) -> Database {
    let db = Database::open(dir).unwrap();
    let mut s = db.new_session();
    db.execute(&mut s, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT NOT NULL, score FLOAT)")
        .unwrap();
    db
}

#[test]
fn crud_roundtrip() {
    let dir = std::env::temp_dir().join(format!("hdbcrud_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = setup(&dir);
    let mut s = db.new_session();
    assert!(db
        .execute(&mut s, "INSERT INTO t VALUES (1, 'ann', 9.5), (2, 'bob', 4.0), (3, 'cat', 7.25)")
        .is_ok());
    let out = db.execute(&mut s, "SELECT * FROM t").unwrap();
    assert_eq!(out.rows.len(), 3);
    let out = db.execute(&mut s, "SELECT name FROM t WHERE id = 2").unwrap();
    assert_eq!(out.rows[0][0], Datum::Text("bob".into()));
    let out = db
        .execute(&mut s, "SELECT id FROM t WHERE score >= 7 ORDER BY id DESC")
        .unwrap();
    assert_eq!(out.rows.len(), 2);
    assert_eq!(out.rows[0][0], Datum::Int(3));
    db.execute(&mut s, "UPDATE t SET score = 10.0 WHERE id = 1").unwrap();
    let out = db.execute(&mut s, "SELECT score FROM t WHERE id = 1").unwrap();
    assert_eq!(out.rows[0][0], Datum::Float(10.0));
    db.execute(&mut s, "DELETE FROM t WHERE id <= 2").unwrap();
    let out = db.execute(&mut s, "SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(out.rows[0][0], Datum::Int(1));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn query_execution_timeout() {
    let dir = std::env::temp_dir().join(format!("hdbtimeout_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();

    db.execute(&mut s, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").unwrap();
    db.execute(&mut s, "CREATE TABLE u (id INT PRIMARY KEY, v TEXT)").unwrap();
    for i in 0..1_000 {
        db.execute(&mut s, &format!("INSERT INTO t VALUES ({i}, 'row-{i}')")).unwrap();
        db.execute(&mut s, &format!("INSERT INTO u VALUES ({i}, 'row-{i}')")).unwrap();
    }

    db.execute(&mut s, "SET max_execution_time = 1").unwrap();
    assert_eq!(s.max_execution_time, Some(std::time::Duration::from_millis(1)));

    std::thread::sleep(std::time::Duration::from_millis(2));
    let res = db.execute(&mut s, "SELECT * FROM t JOIN u ON t.id = u.id WHERE t.v LIKE '%row%'");
    assert_eq!(res, Err(Error::QueryTimeout));

    db.execute(&mut s, "SET max_execution_time = 0").unwrap();
    assert_eq!(s.max_execution_time, None);
    assert!(db.execute(&mut s, "SELECT COUNT(*) FROM t").is_ok());

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn explicit_txn_commit_and_rollback() {
    let dir = std::env::temp_dir().join(format!("hdbtxn_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = setup(&dir);
    let mut s = db.new_session();
    db.execute(&mut s, "INSERT INTO t VALUES (1, 'ann', 1.0)").unwrap();

    // Rollback discards staged writes.
    db.execute(&mut s, "BEGIN").unwrap();
    db.execute(&mut s, "INSERT INTO t VALUES (2, 'bob', 2.0)").unwrap();
    let out = db.execute(&mut s, "SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(out.rows[0][0], Datum::Int(2), "own writes visible in txn");
    db.execute(&mut s, "ROLLBACK").unwrap();
    let out = db.execute(&mut s, "SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(out.rows[0][0], Datum::Int(1));

    // Commit persists staged writes.
    db.execute(&mut s, "BEGIN").unwrap();
    db.execute(&mut s, "INSERT INTO t VALUES (2, 'bob', 2.0)").unwrap();
    db.execute(&mut s, "UPDATE t SET name = 'ann2' WHERE id = 1").unwrap();
    db.execute(&mut s, "COMMIT").unwrap();
    let out = db.execute(&mut s, "SELECT name FROM t WHERE id = 1").unwrap();
    assert_eq!(out.rows[0][0], Datum::Text("ann2".into()));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn duplicate_key_rejected() {
    let dir = std::env::temp_dir().join(format!("hdbdup_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = setup(&dir);
    let mut s = db.new_session();
    db.execute(&mut s, "INSERT INTO t VALUES (1, 'a', 1.0)").unwrap();
    let err = db.execute(&mut s, "INSERT INTO t VALUES (1, 'b', 2.0)").unwrap_err();
    assert!(matches!(err, Error::DuplicateKey(_)));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn recovery_after_crash() {
    let dir = std::env::temp_dir().join(format!("hdbrec_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    {
        let db = setup(&dir);
        let mut s = db.new_session();
        db.execute(&mut s, "INSERT INTO t VALUES (1, 'ann', 1.0), (2, 'bob', 2.0)")
            .unwrap();
        db.execute(&mut s, "UPDATE t SET score = 3.0 WHERE id = 1").unwrap();
        db.execute(&mut s, "DELETE FROM t WHERE id = 2").unwrap();
        // Drop without checkpoint: recovery must replay everything.
    }
    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();
    let out = db.execute(&mut s, "SELECT id, name, score FROM t").unwrap();
    assert_eq!(out.rows.len(), 1);
    assert_eq!(out.rows[0][0], Datum::Int(1));
    assert_eq!(out.rows[0][1], Datum::Text("ann".into()));
    assert_eq!(out.rows[0][2], Datum::Float(3.0));

    // Checkpoint, mutate again, recover from snapshot + WAL.
    db.execute(&mut s, "CHECKPOINT").unwrap();
    db.execute(&mut s, "INSERT INTO t VALUES (5, 'eve', 5.0)").unwrap();
    drop(db);
    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();
    let out = db.execute(&mut s, "SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(out.rows[0][0], Datum::Int(2));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn uncommitted_tail_is_discarded() {
    // Simulates a crash between the last Put and its Commit record by
    // building a WAL manually, then opening the database over it.
    let dir = std::env::temp_dir().join(format!("hdgtail_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    let db = setup(&dir);
    let mut s = db.new_session();
    db.execute(&mut s, "INSERT INTO t VALUES (1, 'a', 1.0)").unwrap();
    drop(db);

    // Append a dangling Put without a Commit.
    let wal = Wal::open(&dir.join("wal.log")).unwrap();
    wal.append_unsynced(&[Record::Put {
        txn: 99,
        table: "t".into(),
        key: encode_key(&Datum::Int(2)).unwrap(),
        row: vec![],
    }])
    .unwrap();
    drop(wal);

    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();
    let out = db.execute(&mut s, "SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(out.rows[0][0], Datum::Int(1), "dangling txn must not apply");
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn concurrent_commits_all_persist() {
    let dir = std::env::temp_dir().join(format!("hdbconc_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = Arc::new(setup(&dir));
    let mut handles = vec![];
    for w in 0..4u64 {
        let db = db.clone();
        handles.push(std::thread::spawn(move || {
            let mut s = db.new_session();
            for i in 0..100u64 {
                let id = (w * 100 + i) as i64;
                db.execute(&mut s, &format!("INSERT INTO t VALUES ({id}, 'w{w}', {i}.0)"))
                    .unwrap();
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    let mut s = db.new_session();
    let out = db.execute(&mut s, "SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(out.rows[0][0], Datum::Int(400));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn secondary_index_sql_e2e_and_recovery() {
    let dir = std::env::temp_dir().join(format!("hdbsec_e2e_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    {
        let db = Database::open(&dir).unwrap();
        let mut s = db.new_session();
        db.execute(
            &mut s,
            "CREATE TABLE accounts (id INT PRIMARY KEY, email TEXT, balance FLOAT)",
        )
        .unwrap();

        db.execute(&mut s, "INSERT INTO accounts VALUES (1, 'alice@test.com', 100.5)").unwrap();
        db.execute(&mut s, "INSERT INTO accounts VALUES (2, 'bob@test.com', 50.0)").unwrap();
        db.execute(&mut s, "INSERT INTO accounts VALUES (3, 'charlie@test.com', 200.0)").unwrap();
        db.execute(&mut s, "INSERT INTO accounts VALUES (4, 'alice_work@test.com', 50.0)").unwrap();

        // Create secondary indexes
        db.execute(&mut s, "CREATE INDEX idx_email ON accounts (email)").unwrap();
        db.execute(&mut s, "CREATE INDEX idx_bal ON accounts (balance)").unwrap();

        // Point lookup via secondary index
        let out = db.execute(&mut s, "SELECT id, email, balance FROM accounts WHERE email = 'bob@test.com'").unwrap();
        assert_eq!(out.rows.len(), 1);
        assert_eq!(out.rows[0][0], Datum::Int(2));
        assert_eq!(out.rows[0][1], Datum::Text("bob@test.com".into()));

        // Range lookup via secondary index
        let out = db.execute(&mut s, "SELECT id, balance FROM accounts WHERE balance >= 50.0 AND balance <= 100.5").unwrap();
        assert_eq!(out.rows.len(), 3); // bob (50.0), alice_work (50.0), alice (100.5)

        // Update row via autocommit fast path or staged write
        db.execute(&mut s, "UPDATE accounts SET balance = 300.0 WHERE id = 2").unwrap();
        let out = db.execute(&mut s, "SELECT id, balance FROM accounts WHERE balance = 300.0").unwrap();
        assert_eq!(out.rows.len(), 1);
        assert_eq!(out.rows[0][0], Datum::Int(2));

        // Delete row
        db.execute(&mut s, "DELETE FROM accounts WHERE id = 1").unwrap();
        let out = db.execute(&mut s, "SELECT id FROM accounts WHERE email = 'alice@test.com'").unwrap();
        assert_eq!(out.rows.len(), 0);

        // Checkpoint: flushes snapshot containing table definition + secondary index metadata
        db.execute(&mut s, "CHECKPOINT").unwrap();
    }

    // Reopen database from snapshot + wal and verify secondary indexes still work seamlessly
    {
        let db = Database::open(&dir).unwrap();
        let mut s = db.new_session();

        let out = db.execute(&mut s, "SELECT id, balance FROM accounts WHERE balance = 300.0").unwrap();
        assert_eq!(out.rows.len(), 1);
        assert_eq!(out.rows[0][0], Datum::Int(2));

        let out = db.execute(&mut s, "SELECT id FROM accounts WHERE balance = 50.0").unwrap();
        assert_eq!(out.rows.len(), 1);
        assert_eq!(out.rows[0][0], Datum::Int(4));

        // Drop secondary index
        db.execute(&mut s, "DROP INDEX idx_bal ON accounts").unwrap();
        let out = db.execute(&mut s, "SELECT id FROM accounts WHERE balance = 50.0").unwrap();
        assert_eq!(out.rows.len(), 1); // Falls back to full scan correctly
    }

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn overflow_rows_sql_checkpoint_recovery() {
    let dir = std::env::temp_dir().join(format!("hdbpage_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let big = |c: char, n: usize| c.to_string().repeat(n);
    {
        let db = Database::open(&dir).unwrap();
        let mut s = db.new_session();
        db.execute(&mut s, "CREATE TABLE docs (id INT PRIMARY KEY, body TEXT)").unwrap();
        // Inline + single-page + chained rows through the SQL layer.
        db.execute(&mut s, &format!("INSERT INTO docs VALUES (1, '{}')", big('a', 20))).unwrap();
        db.execute(&mut s, &format!("INSERT INTO docs VALUES (2, '{}')", big('b', 5000))).unwrap();
        db.execute(&mut s, &format!("INSERT INTO docs VALUES (3, '{}')", big('c', 300 * 1024))).unwrap();
        let out = db.execute(&mut s, "SELECT body FROM docs WHERE id = 3").unwrap();
        assert_eq!(out.rows[0][0], Datum::Text(big('c', 300 * 1024)));
        // Fast point-update path on an overflow row (resolve + re-store).
        db.execute(&mut s, &format!("UPDATE docs SET body = '{}' WHERE id = 2", big('d', 6000))).unwrap();
        let out = db.execute(&mut s, "SELECT body FROM docs WHERE id = 2").unwrap();
        assert_eq!(out.rows[0][0], Datum::Text(big('d', 6000)));
        // Range + count over mixed storage.
        let out = db.execute(&mut s, "SELECT COUNT(*) FROM docs WHERE id >= 1").unwrap();
        assert_eq!(out.rows[0][0], Datum::Int(3));
        // Explicit txn abort leaves no trace.
        db.execute(&mut s, "BEGIN").unwrap();
        db.execute(&mut s, &format!("INSERT INTO docs VALUES (9, '{}')", big('z', 9000))).unwrap();
        db.execute(&mut s, "ROLLBACK").unwrap();
        let out = db.execute(&mut s, "SELECT COUNT(*) FROM docs").unwrap();
        assert_eq!(out.rows[0][0], Datum::Int(3));
        let st = db.pool_stats();
        assert!(st.stores >= 3, "wide rows must page, got {st:?}");
        db.execute(&mut s, "CHECKPOINT").unwrap();
    }
    // Reopen from snapshot (v2 key/value + persisted pool file).
    {
        let db = Database::open(&dir).unwrap();
        let mut s = db.new_session();
        let out = db.execute(&mut s, "SELECT body FROM docs WHERE id = 1").unwrap();
        assert_eq!(out.rows[0][0], Datum::Text(big('a', 20)));
        let out = db.execute(&mut s, "SELECT body FROM docs WHERE id = 2").unwrap();
        assert_eq!(out.rows[0][0], Datum::Text(big('d', 6000)));
        let out = db.execute(&mut s, "SELECT body FROM docs WHERE id = 3").unwrap();
        assert_eq!(out.rows[0][0], Datum::Text(big('c', 300 * 1024)));
        // Delete an overflow row, checkpoint again, reopen once more.
        db.execute(&mut s, "DELETE FROM docs WHERE id = 3").unwrap();
        db.execute(&mut s, "CHECKPOINT").unwrap();
    }
    {
        let db = Database::open(&dir).unwrap();
        let mut s = db.new_session();
        let out = db.execute(&mut s, "SELECT COUNT(*) FROM docs").unwrap();
        assert_eq!(out.rows[0][0], Datum::Int(2));
        let out = db.execute(&mut s, "SELECT body FROM docs WHERE id = 2").unwrap();
        assert_eq!(out.rows[0][0], Datum::Text(big('d', 6000)));
    }
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn describe_reports_prepare_metadata() {
    let dir = std::env::temp_dir().join(format!("hdbdesc_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = setup(&dir);
    let s = db.new_session();
    let cols = db.describe(&s, "SELECT * FROM t").unwrap();
    assert_eq!(cols.len(), 3);
    assert_eq!(cols[0].0, "id");
    let cols = db.describe(&s, "SELECT name FROM t").unwrap();
    assert_eq!(cols.len(), 1);
    let cols = db.describe(&s, "SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(cols[0].0, "COUNT(*)");
    assert!(db.describe(&s, "INSERT INTO t VALUES (1, 'x', 1.0)").unwrap().is_empty());
    assert!(db.describe(&s, "SELECT * FROM missing").is_err());
    assert!(db.describe(&s, "SELECT bogus FROM t").is_err());
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn overflow_uncommitted_tail_discarded() {
    // A committed overflow row replays; a dangling WAL tail does not.
    let dir = std::env::temp_dir().join(format!("hdbpgtail_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    {
        let db = Database::open(&dir).unwrap();
        let mut s = db.new_session();
        db.execute(&mut s, "CREATE TABLE docs (id INT PRIMARY KEY, body TEXT)").unwrap();
        let wide = "w".repeat(8000);
        db.execute(&mut s, &format!("INSERT INTO docs VALUES (1, '{wide}')")).unwrap();
    }
    // Dangling Put without Commit (wide row, never installed).
    let wal = Wal::open(&dir.join("wal.log")).unwrap();
    wal.append_unsynced(&[Record::Put {
        txn: 99,
        table: "docs".into(),
        key: encode_key(&Datum::Int(2)).unwrap(),
        row: Table::encode_row(&[Datum::Int(2), Datum::Text("x".repeat(8000))]),
    }])
    .unwrap();
    drop(wal);
    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();
    let out = db.execute(&mut s, "SELECT COUNT(*) FROM docs").unwrap();
    assert_eq!(out.rows[0][0], Datum::Int(1));
    let out = db.execute(&mut s, "SELECT body FROM docs WHERE id = 1").unwrap();
    assert_eq!(out.rows[0][0], Datum::Text("w".repeat(8000)));
    let _ = fs::remove_dir_all(&dir);
}

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

#[test]
fn auto_increment_assigns_and_persists() {
    let dir = std::env::temp_dir().join(format!("hdbai_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    {
        let db = Database::open(&dir).unwrap();
        let mut s = db.new_session();
        db.execute(&mut s, "CREATE TABLE seq (id INT PRIMARY KEY AUTO_INCREMENT, v TEXT)").unwrap();
        // NULL triggers the sequence.
        db.execute(&mut s, "INSERT INTO seq VALUES (NULL, 'a')").unwrap();
        db.execute(&mut s, "INSERT INTO seq VALUES (NULL, 'b'), (NULL, 'c')").unwrap();
        let out = db.execute(&mut s, "SELECT id FROM seq ORDER BY id").unwrap();
        let ids: Vec<i64> = out.rows.iter().map(|r| match r[0] {
            Datum::Int(v) => v,
            ref d => panic!("expected int, got {d:?}"),
        }).collect();
        assert_eq!(ids, vec![1, 2, 3]);
        // Explicit values pass through; higher ones bump the counter.
        db.execute(&mut s, "INSERT INTO seq VALUES (10, 'j')").unwrap();
        db.execute(&mut s, "INSERT INTO seq VALUES (NULL, 'k')").unwrap();
        let out = db.execute(&mut s, "SELECT id FROM seq WHERE v = 'k'").unwrap();
        assert_eq!(out.rows[0][0], Datum::Int(11));
        // Lower explicit values do not regress the counter.
        db.execute(&mut s, "INSERT INTO seq VALUES (5, 'e')").unwrap();
        db.execute(&mut s, "INSERT INTO seq VALUES (NULL, 'l')").unwrap();
        let out = db.execute(&mut s, "SELECT id FROM seq WHERE v = 'l'").unwrap();
        assert_eq!(out.rows[0][0], Datum::Int(12));
        // Rollback consumes (gaps allowed, MySQL-style).
        db.execute(&mut s, "BEGIN").unwrap();
        db.execute(&mut s, "INSERT INTO seq VALUES (NULL, 'tmp')").unwrap();
        db.execute(&mut s, "ROLLBACK").unwrap();
        db.execute(&mut s, "INSERT INTO seq VALUES (NULL, 'm')").unwrap();
        let out = db.execute(&mut s, "SELECT id FROM seq WHERE v = 'm'").unwrap();
        assert_eq!(out.rows[0][0], Datum::Int(14));
        db.execute(&mut s, "CHECKPOINT").unwrap();
    }
    // Reopen: the counter continues past every durable row.
    {
        let db = Database::open(&dir).unwrap();
        let mut s = db.new_session();
        db.execute(&mut s, "INSERT INTO seq VALUES (NULL, 'n')").unwrap();
        let out = db.execute(&mut s, "SELECT id FROM seq WHERE v = 'n'").unwrap();
        assert_eq!(out.rows[0][0], Datum::Int(15));
    }
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn auto_increment_rejects_bad_schemas() {
    let dir = std::env::temp_dir().join(format!("hdbai_bad_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();
    // Non-integer column.
    assert!(db.execute(&mut s, "CREATE TABLE t1 (id TEXT PRIMARY KEY AUTO_INCREMENT)").is_err());
    // Non-PK column.
    assert!(db.execute(&mut s, "CREATE TABLE t2 (id INT PRIMARY KEY, n INT AUTO_INCREMENT)").is_err());
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn aggregates_global() {
    let dir = std::env::temp_dir().join(format!("hdbagg_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();
    db.execute(&mut s, "CREATE TABLE m (id INT PRIMARY KEY, name TEXT, score FLOAT)").unwrap();
    db.execute(&mut s, "INSERT INTO m VALUES (1, 'a', 10.0), (2, 'b', 20.0), (3, NULL, 30.0)").unwrap();
    // NULL name on row 3 is fine (nullable); score never null here.
    let out = db.execute(&mut s, "SELECT SUM(id), AVG(score), MIN(name), MAX(score) FROM m").unwrap();
    assert_eq!(out.columns, vec!["SUM(id)", "AVG(score)", "MIN(name)", "MAX(score)"]);
    assert_eq!(out.rows[0][0], Datum::Int(6));
    assert_eq!(out.rows[0][1], Datum::Float(20.0));
    assert_eq!(out.rows[0][2], Datum::Text("a".into()));
    assert_eq!(out.rows[0][3], Datum::Float(30.0));
    // WHERE filters before aggregating.
    let out = db.execute(&mut s, "SELECT SUM(id), COUNT(*) FROM m WHERE id >= 2").unwrap();
    assert_eq!((out.rows[0][0].clone(), out.rows[0][1].clone()), (Datum::Int(5), Datum::Int(2)));
    // Empty set: COUNT is 0, the rest are NULL.
    let out = db.execute(&mut s, "SELECT SUM(id), AVG(id), MIN(id), MAX(id) FROM m WHERE id > 100").unwrap();
    assert_eq!(out.rows[0], vec![Datum::Null, Datum::Null, Datum::Null, Datum::Null]);
    // Mixing aggregates with plain columns is rejected.
    assert!(db.execute(&mut s, "SELECT id, SUM(id) FROM m").is_err());
    assert!(db.execute(&mut s, "SELECT SUM(missing) FROM m").is_err());
    assert!(db.execute(&mut s, "SELECT SUM(name) FROM m").is_err());
    // ORDER BY / LIMIT do not disturb the single aggregate row.
    let out = db.execute(&mut s, "SELECT SUM(id) FROM m ORDER BY id DESC LIMIT 1").unwrap();
    assert_eq!(out.rows[0][0], Datum::Int(6));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn rich_where_executor() {
    let dir = std::env::temp_dir().join(format!("hdbrich_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();
    db.execute(&mut s, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT, score FLOAT)").unwrap();
    db.execute(&mut s, "INSERT INTO t VALUES (1, 'ann', 9.5), (2, 'bob', 4.0), (3, NULL, 7.25), (4, 'anna', 8.0)").unwrap();
    let ids = |out: Output| {
        let mut v: Vec<i64> = out.rows.iter().map(|r| match r[0] {
            Datum::Int(n) => n,
            ref d => panic!("expected int, got {d:?}"),
        }).collect();
        v.sort_unstable();
        v
    };
    // OR + precedence: AND binds tighter.
    let out = db.execute(&mut s, "SELECT id FROM t WHERE id = 4 OR id = 1 AND score < 5").unwrap();
    assert_eq!(ids(out), vec![4]); // id=1 fails score<5, so only 4
    let out = db.execute(&mut s, "SELECT id FROM t WHERE (id = 4 OR id = 1) AND score < 5").unwrap();
    assert!(ids(out).is_empty());
    let out = db.execute(&mut s, "SELECT id FROM t WHERE id = 1 OR id = 2 OR id = 3").unwrap();
    assert_eq!(ids(out), vec![1, 2, 3]);
    // IN / NOT IN (NULL row never matches, even negated).
    let out = db.execute(&mut s, "SELECT id FROM t WHERE id IN (4, 1, 99)").unwrap();
    assert_eq!(ids(out), vec![1, 4]);
    let out = db.execute(&mut s, "SELECT id FROM t WHERE name NOT IN ('ann', 'bob')").unwrap();
    assert_eq!(ids(out), vec![4]); // NULL row excluded, not matched
    // BETWEEN inclusive + NOT BETWEEN.
    let out = db.execute(&mut s, "SELECT id FROM t WHERE score BETWEEN 4.0 AND 9.5").unwrap();
    assert_eq!(ids(out), vec![1, 2, 3, 4]);
    let out = db.execute(&mut s, "SELECT id FROM t WHERE score BETWEEN 4.0 AND 7.0").unwrap();
    assert_eq!(ids(out), vec![2]);
    let out = db.execute(&mut s, "SELECT id FROM t WHERE id NOT BETWEEN 2 AND 3").unwrap();
    assert_eq!(ids(out), vec![1, 4]);
    // LIKE: prefix, contains, underscore, negation, int coercion.
    let out = db.execute(&mut s, "SELECT id FROM t WHERE name LIKE 'ann%'").unwrap();
    assert_eq!(ids(out), vec![1, 4]);
    let out = db.execute(&mut s, "SELECT id FROM t WHERE name LIKE '%o%'").unwrap();
    assert_eq!(ids(out), vec![2]);
    let out = db.execute(&mut s, "SELECT id FROM t WHERE name LIKE 'a_n'").unwrap();
    assert_eq!(ids(out), vec![1]);
    let out = db.execute(&mut s, "SELECT id FROM t WHERE name NOT LIKE 'ann%'").unwrap();
    assert_eq!(ids(out), vec![2]);
    let out = db.execute(&mut s, "SELECT id FROM t WHERE id LIKE '1%'").unwrap();
    assert_eq!(ids(out), vec![1]);
    // Bare NOT.
    let out = db.execute(&mut s, "SELECT id FROM t WHERE NOT score < 8").unwrap();
    assert_eq!(ids(out), vec![1, 4]);
    // Uncoercible IN elements match nothing, cleanly.
    let out = db.execute(&mut s, "SELECT id FROM t WHERE id IN ('x', 'y')").unwrap();
    assert!(out.rows.is_empty());
    // DML honors rich predicates.
    db.execute(&mut s, "UPDATE t SET score = 0.0 WHERE id IN (1, 2)").unwrap();
    let out = db.execute(&mut s, "SELECT SUM(score) FROM t").unwrap();
    assert_eq!(out.rows[0][0], Datum::Float(15.25));
    db.execute(&mut s, "DELETE FROM t WHERE score BETWEEN 0.0 AND 1.0").unwrap();
    let out = db.execute(&mut s, "SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(out.rows[0][0], Datum::Int(2));
    // GROUP BY + JOIN see the same filter semantics.
    db.execute(&mut s, "CREATE TABLE o (oid INT PRIMARY KEY, tid INT)").unwrap();
    db.execute(&mut s, "INSERT INTO o VALUES (10, 3), (11, 4)").unwrap();
    let out = db.execute(&mut s, "SELECT t.name FROM t JOIN o ON t.id = o.tid WHERE o.oid IN (10, 11) OR t.id = 1").unwrap();
    assert_eq!(out.rows.len(), 2); // ids 1-2 were deleted above; IN matches both joins
    let out = db.execute(&mut s, "SELECT COUNT(*) FROM t WHERE name LIKE 'a%' GROUP BY score").unwrap();
    assert_eq!(out.rows.len(), 1); // only 'anna' still matches
    let _ = fs::remove_dir_all(&dir);
}

fn path_table() -> Table {
    use crate::table::ColumnDef;
    Table::new(TableDef::new(
        "t",
        Schema {
            columns: vec![
                ColumnDef { name: "id".into(), ctype: ColumnType::Int, nullable: false, auto_increment: false, default_value: None },
                ColumnDef { name: "name".into(), ctype: ColumnType::Text, nullable: true, auto_increment: false, default_value: None },
            ],
            pk_idx: 0,
        },
    ))
}

fn parse_pred(where_sql: &str) -> crate::sql::Expr {
    match crate::sql::parse_sql(&format!("SELECT * FROM t WHERE {where_sql}")) {
        Ok(crate::sql::Statement::Select { selection: Some(e), .. }) => e,
        other => panic!("bad predicate parse: {other:?}"),
    }
}

#[test]
fn access_path_rich_where() {
    use super::plan::{access_path, AccessPath};
    let t = path_table();
    // IN on PK -> multi-point seek.
    match access_path(&t, Some(&parse_pred("id IN (3, 1)"))) {
        Ok(AccessPath::PkIn(v)) => assert_eq!(v, vec![Datum::Int(3), Datum::Int(1)]),
        other => panic!("expected PkIn, got {other:?}"),
    }
    // BETWEEN -> inclusive range.
    match access_path(&t, Some(&parse_pred("id BETWEEN 2 AND 4"))) {
        Ok(AccessPath::Range { lo: Some((l, true)), hi: Some((h, true)) }) => {
            assert_eq!((l, h), (Datum::Int(2), Datum::Int(4)));
        }
        other => panic!("expected Range, got {other:?}"),
    }
    // Same-column OR equalities -> IN list; mixed OR -> full scan.
    match access_path(&t, Some(&parse_pred("id = 1 OR id = 2"))) {
        Ok(AccessPath::PkIn(_)) => {}
        other => panic!("expected PkIn, got {other:?}"),
    }
    match access_path(&t, Some(&parse_pred("id = 1 OR name = 'x'"))) {
        Ok(AccessPath::FullScan) => {}
        other => panic!("expected FullScan, got {other:?}"),
    }
    // LIKE prefix -> bounded secondary range; leading wildcard -> scan;
    // exact (no wildcard) -> point. (Prefix needs the text index.)
    t.add_index("idx_name".into(), "name".into()).unwrap();
    match access_path(&t, Some(&parse_pred("name LIKE 'ab%'"))) {
        Ok(AccessPath::SecondaryIndex { lo: Some((l, true)), hi: Some((h, false)), .. }) => {
            assert_eq!((l, h), (Datum::Text("ab".into()), Datum::Text("ac".into())));
        }
        other => panic!("expected prefix SecondaryIndex, got {other:?}"),
    }
    match access_path(&t, Some(&parse_pred("name LIKE '%ab'"))) {
        Ok(AccessPath::FullScan) => {}
        other => panic!("expected FullScan, got {other:?}"),
    }
    // Exact LIKE (no wildcard) becomes an index point probe.
    match access_path(&t, Some(&parse_pred("name LIKE 'ab'"))) {
        Ok(AccessPath::SecondaryIndex { lo: Some((l, true)), hi: Some((h, true)), .. }) => {
            assert_eq!((l, h), (Datum::Text("ab".into()), Datum::Text("ab".into())));
        }
        other => panic!("expected SecondaryIndex point, got {other:?}"),
    }
    // Uncoercible IN members drop out (empty list seeks nothing, cleanly).
    match access_path(&t, Some(&parse_pred("id IN ('x')"))) {
        Ok(AccessPath::PkIn(v)) => assert!(v.is_empty()),
        other => panic!("expected empty PkIn, got {other:?}"),
    }
    // Secondary index: IN + BETWEEN ride the index.
    match access_path(&t, Some(&parse_pred("name IN ('a', 'b')"))) {
        Ok(AccessPath::SecIn { values, .. }) => assert_eq!(values.len(), 2),
        other => panic!("expected SecIn, got {other:?}"),
    }
    match access_path(&t, Some(&parse_pred("name BETWEEN 'a' AND 'c'"))) {
        Ok(AccessPath::SecondaryIndex { .. }) => {}
        other => panic!("expected SecondaryIndex, got {other:?}"),
    }
}

#[test]
fn database_namespaces_and_use() {
    let dir = std::env::temp_dir().join(format!("hdbdb_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();

    let out = db.execute(&mut s, "SHOW DATABASES").unwrap();
    assert_eq!(out.rows, vec![vec![Datum::Text("default".into())]]);

    db.execute(&mut s, "CREATE DATABASE shop").unwrap();
    let out = db.execute(&mut s, "SHOW DATABASES").unwrap();
    assert_eq!(out.rows, vec![vec![Datum::Text("default".into())], vec![Datum::Text("shop".into())]]);

    db.execute(&mut s, "USE shop").unwrap();
    assert_eq!(s.current_db, "shop");

    db.execute(&mut s, "CREATE TABLE items (id INT PRIMARY KEY, title TEXT)").unwrap();
    db.execute(&mut s, "INSERT INTO items VALUES (1, 'Laptop')").unwrap();

    let out = db.execute(&mut s, "SELECT title FROM items WHERE id = 1").unwrap();
    assert_eq!(out.rows, vec![vec![Datum::Text("Laptop".into())]]);

    db.execute(&mut s, "USE default").unwrap();
    assert!(db.execute(&mut s, "SELECT title FROM items WHERE id = 1").is_err());

    db.execute(&mut s, "DROP DATABASE shop").unwrap();
    assert!(db.execute(&mut s, "USE shop").is_err());
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn schema_defaults_and_datetime_types() {
    let dir = std::env::temp_dir().join(format!("hdbdt_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();

    db.execute(
        &mut s,
        "CREATE TABLE events (id INT PRIMARY KEY, name TEXT DEFAULT 'unnamed', created_at DATETIME)",
    )
    .unwrap();

    db.execute(&mut s, "INSERT INTO events VALUES (1, 'Launch', '2026-09-04 12:00:00')").unwrap();
    db.execute(&mut s, "INSERT INTO events VALUES (2, 'Party', '2026-10-01 18:30:00')").unwrap();

    let out = db.execute(&mut s, "SELECT name, created_at FROM events WHERE created_at > '2026-09-15 00:00:00'").unwrap();
    assert_eq!(out.rows.len(), 1);
    assert_eq!(out.rows[0][0], Datum::Text("Party".into()));

    let dt_micros = crate::types::parse_datetime_str("2026-10-01 18:30:00").unwrap();
    assert_eq!(out.rows[0][1], Datum::DateTime(dt_micros));

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
    use super::plan::join_key;
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

// -- foreign keys ------------------------------------------------------------

fn fk_setup(dir: &Path) -> Database {
    let db = Database::open(dir).unwrap();
    let mut s = db.new_session();
    db.execute(&mut s, "CREATE TABLE dept (id INT PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute(
        &mut s,
        "CREATE TABLE emp (id INT PRIMARY KEY, dept_id INT, \
         FOREIGN KEY (dept_id) REFERENCES dept(id))",
    )
    .unwrap();
    db.execute(&mut s, "INSERT INTO dept VALUES (1, 'eng'), (2, 'ops')")
        .unwrap();
    db
}

fn fk_err_is_violation(r: crate::error::Result<Output>) -> bool {
    matches!(r, Err(Error::ForeignKeyViolation(_)))
}

#[test]
fn fk_valid_insert_and_orphan_rejected() {
    let dir = std::env::temp_dir().join(format!("hdbfk1_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = fk_setup(&dir);
    let mut s = db.new_session();
    // Valid reference + NULL (references nothing) both pass.
    db.execute(&mut s, "INSERT INTO emp VALUES (10, 1), (11, NULL)")
        .unwrap();
    // Orphan fails; failed statement inserts nothing.
    assert!(fk_err_is_violation(
        db.execute(&mut s, "INSERT INTO emp VALUES (12, 99)")
    ));
    let out = db.execute(&mut s, "SELECT COUNT(*) FROM emp").unwrap();
    assert_eq!(out.rows[0][0], Datum::Int(2));
    // Unknown parent table / column rejected at CREATE.
    assert!(db
        .execute(
            &mut s,
            "CREATE TABLE bad1 (id INT PRIMARY KEY, x INT, FOREIGN KEY (x) REFERENCES nosuch(id))"
        )
        .is_err());
    assert!(db
        .execute(
            &mut s,
            "CREATE TABLE bad2 (id INT PRIMARY KEY, x INT, FOREIGN KEY (x) REFERENCES dept(nosuch))"
        )
        .is_err());
    assert!(db
        .execute(
            &mut s,
            "CREATE TABLE bad3 (id INT PRIMARY KEY, x INT, FOREIGN KEY (nosuch) REFERENCES dept(id))"
        )
        .is_err());
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn fk_restrict_on_delete() {
    let dir = std::env::temp_dir().join(format!("hdbfk2_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = fk_setup(&dir);
    let mut s = db.new_session();
    db.execute(&mut s, "INSERT INTO emp VALUES (10, 1)").unwrap();
    // Referenced parent cannot go.
    assert!(fk_err_is_violation(
        db.execute(&mut s, "DELETE FROM dept WHERE id = 1")
    ));
    // Unreferenced parent can.
    db.execute(&mut s, "DELETE FROM dept WHERE id = 2").unwrap();
    // After the child goes, the parent can too.
    db.execute(&mut s, "DELETE FROM emp WHERE id = 10").unwrap();
    db.execute(&mut s, "DELETE FROM dept WHERE id = 1").unwrap();
    let out = db.execute(&mut s, "SELECT COUNT(*) FROM dept").unwrap();
    assert_eq!(out.rows[0][0], Datum::Int(0));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn fk_cascade_on_delete_transitive() {
    let dir = std::env::temp_dir().join(format!("hdbfk3_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();
    db.execute(&mut s, "CREATE TABLE a (id INT PRIMARY KEY)").unwrap();
    db.execute(
        &mut s,
        "CREATE TABLE b (id INT PRIMARY KEY, a_id INT, \
         FOREIGN KEY (a_id) REFERENCES a(id) ON DELETE CASCADE)",
    )
    .unwrap();
    db.execute(
        &mut s,
        "CREATE TABLE c (id INT PRIMARY KEY, b_id INT, \
         FOREIGN KEY (b_id) REFERENCES b(id) ON DELETE CASCADE)",
    )
    .unwrap();
    db.execute(&mut s, "INSERT INTO a VALUES (1), (2)").unwrap();
    db.execute(&mut s, "INSERT INTO b VALUES (10, 1), (11, 1), (12, 2)")
        .unwrap();
    db.execute(&mut s, "INSERT INTO c VALUES (20, 10), (21, 12)")
        .unwrap();
    db.execute(&mut s, "DELETE FROM a WHERE id = 1").unwrap();
    // b(10, 11) gone transitively with c(20); b(12)/c(21) survive.
    let out = db.execute(&mut s, "SELECT id FROM b ORDER BY id").unwrap();
    assert_eq!(out.rows.len(), 1);
    assert_eq!(out.rows[0][0], Datum::Int(12));
    let out = db.execute(&mut s, "SELECT id FROM c ORDER BY id").unwrap();
    assert_eq!(out.rows.len(), 1);
    assert_eq!(out.rows[0][0], Datum::Int(21));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn fk_set_null_on_delete() {
    let dir = std::env::temp_dir().join(format!("hdbfk4_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();
    db.execute(&mut s, "CREATE TABLE p (id INT PRIMARY KEY)").unwrap();
    db.execute(
        &mut s,
        "CREATE TABLE k (id INT PRIMARY KEY, p_id INT, \
         FOREIGN KEY (p_id) REFERENCES p(id) ON DELETE SET NULL)",
    )
    .unwrap();
    db.execute(&mut s, "INSERT INTO p VALUES (1)").unwrap();
    db.execute(&mut s, "INSERT INTO k VALUES (10, 1)").unwrap();
    db.execute(&mut s, "DELETE FROM p WHERE id = 1").unwrap();
    let out = db.execute(&mut s, "SELECT p_id FROM k").unwrap();
    assert_eq!(out.rows[0][0], Datum::Null);
    // SET NULL into NOT NULL fails instead of corrupting.
    let dir2 = std::env::temp_dir().join(format!("hdbfk4b_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir2);
    let db2 = Database::open(&dir2).unwrap();
    let mut s2 = db2.new_session();
    db2.execute(&mut s2, "CREATE TABLE p (id INT PRIMARY KEY)").unwrap();
    db2.execute(
        &mut s2,
        "CREATE TABLE n (id INT PRIMARY KEY, p_id INT NOT NULL, \
         FOREIGN KEY (p_id) REFERENCES p(id) ON DELETE SET NULL)",
    )
    .unwrap();
    db2.execute(&mut s2, "INSERT INTO p VALUES (1)").unwrap();
    db2.execute(&mut s2, "INSERT INTO n VALUES (20, 1)").unwrap();
    assert!(fk_err_is_violation(
        db2.execute(&mut s2, "DELETE FROM p WHERE id = 1")
    ));
    let _ = fs::remove_dir_all(&dir);
    let _ = fs::remove_dir_all(&dir2);
}

#[test]
fn fk_update_paths() {
    let dir = std::env::temp_dir().join(format!("hdbfk5_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = fk_setup(&dir);
    let mut s = db.new_session();
    db.execute(&mut s, "INSERT INTO emp VALUES (10, 1)").unwrap();
    // Child FK -> orphan rejected; -> valid parent ok (slow path).
    assert!(fk_err_is_violation(
        db.execute(&mut s, "UPDATE emp SET dept_id = 99 WHERE id = 10")
    ));
    db.execute(&mut s, "UPDATE emp SET dept_id = 2 WHERE id = 10")
        .unwrap();
    // Parent PK change with referencing children rejected (RESTRICT).
    assert!(fk_err_is_violation(
        db.execute(&mut s, "UPDATE dept SET id = 7 WHERE id = 2")
    ));
    let out = db.execute(&mut s, "SELECT dept_id FROM emp").unwrap();
    assert_eq!(out.rows[0][0], Datum::Int(2));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn fk_self_reference_and_txn_visibility() {
    let dir = std::env::temp_dir().join(format!("hdbfk6_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();
    db.execute(
        &mut s,
        "CREATE TABLE emp (id INT PRIMARY KEY, mgr INT, \
         FOREIGN KEY (mgr) REFERENCES emp(id))",
    )
    .unwrap();
    db.execute(&mut s, "INSERT INTO emp VALUES (1, NULL)").unwrap();
    db.execute(&mut s, "INSERT INTO emp VALUES (2, 1)").unwrap();
    assert!(fk_err_is_violation(
        db.execute(&mut s, "INSERT INTO emp VALUES (3, 99)")
    ));
    // Explicit txn: parent staged in-txn satisfies the child check.
    db.execute(&mut s, "BEGIN").unwrap();
    db.execute(&mut s, "INSERT INTO emp VALUES (4, NULL)").unwrap();
    db.execute(&mut s, "INSERT INTO emp VALUES (5, 4)").unwrap();
    db.execute(&mut s, "COMMIT").unwrap();
    let out = db.execute(&mut s, "SELECT COUNT(*) FROM emp").unwrap();
    assert_eq!(out.rows[0][0], Datum::Int(4));
    // Rollback discards both sides together.
    db.execute(&mut s, "BEGIN").unwrap();
    db.execute(&mut s, "INSERT INTO emp VALUES (6, NULL)").unwrap();
    db.execute(&mut s, "ROLLBACK").unwrap();
    let out = db.execute(&mut s, "SELECT COUNT(*) FROM emp").unwrap();
    assert_eq!(out.rows[0][0], Datum::Int(4));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn fk_drop_table_guarded() {
    let dir = std::env::temp_dir().join(format!("hdbfk7_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = fk_setup(&dir);
    let mut s = db.new_session();
    assert!(fk_err_is_violation(db.execute(&mut s, "DROP TABLE dept")));
    db.execute(&mut s, "DROP TABLE emp").unwrap();
    db.execute(&mut s, "DROP TABLE dept").unwrap();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn fk_recovery_snapshot_and_wal() {
    let dir = std::env::temp_dir().join(format!("hdbfk8_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    {
        let db = fk_setup(&dir);
        let mut s = db.new_session();
        db.execute(&mut s, "INSERT INTO emp VALUES (10, 1)").unwrap();
        db.execute(&mut s, "CHECKPOINT").unwrap();
    }
    // Snapshot path: FKs survive the checkpoint.
    {
        let db = Database::open(&dir).unwrap();
        let mut s = db.new_session();
        assert!(fk_err_is_violation(
            db.execute(&mut s, "INSERT INTO emp VALUES (11, 99)")
        ));
        assert!(fk_err_is_violation(
            db.execute(&mut s, "DELETE FROM dept WHERE id = 1")
        ));
        db.execute(&mut s, "INSERT INTO emp VALUES (11, 2)").unwrap();
    }
    // WAL path: post-checkpoint DDL + rows replay too.
    {
        let db = Database::open(&dir).unwrap();
        let mut s = db.new_session();
        let out = db.execute(&mut s, "SELECT COUNT(*) FROM emp").unwrap();
        assert_eq!(out.rows[0][0], Datum::Int(2));
        assert!(fk_err_is_violation(
            db.execute(&mut s, "INSERT INTO emp VALUES (12, 99)")
        ));
    }
    let _ = fs::remove_dir_all(&dir);
}

// -- MVCC snapshots (F3) -----------------------------------------------------

fn mvcc_setup(dir: &Path) -> Database {
    let db = Database::open(dir).unwrap();
    let mut s = db.new_session();
    db.execute(&mut s, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        .unwrap();
    db.execute(&mut s, "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .unwrap();
    db
}

fn mvcc_val(db: &Database, s: &mut Session, id: i64) -> Option<String> {
    let out = db
        .execute(s, &format!("SELECT v FROM t WHERE id = {id}"))
        .unwrap();
    out.rows.first().map(|r| match &r[0] {
        Datum::Text(v) => v.clone(),
        other => panic!("unexpected {other:?}"),
    })
}

#[test]
fn snapshot_repeatable_read() {
    let dir = std::env::temp_dir().join(format!("hdbmv1_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = mvcc_setup(&dir);
    let mut a = db.new_session();
    let mut b = db.new_session();
    db.execute(&mut a, "START TRANSACTION WITH CONSISTENT SNAPSHOT")
        .unwrap();
    assert_eq!(mvcc_val(&db, &mut a, 1).as_deref(), Some("a"));
    // Concurrent update commits after the pin.
    db.execute(&mut b, "UPDATE t SET v = 'b2' WHERE id = 1").unwrap();
    db.execute(&mut b, "INSERT INTO t VALUES (4, 'd')").unwrap();
    // Long reader still sees the pinned state; fresh sessions see new.
    assert_eq!(mvcc_val(&db, &mut a, 1).as_deref(), Some("a"));
    assert_eq!(mvcc_val(&db, &mut a, 4), None);
    assert_eq!(mvcc_val(&db, &mut b, 1).as_deref(), Some("b2"));
    assert_eq!(mvcc_val(&db, &mut b, 4).as_deref(), Some("d"));
    // Repeatable: re-reads within the snapshot never change.
    assert_eq!(mvcc_val(&db, &mut a, 1).as_deref(), Some("a"));
    db.execute(&mut a, "COMMIT").unwrap();
    assert_eq!(mvcc_val(&db, &mut a, 1).as_deref(), Some("b2"));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn snapshot_sees_deleted_rows() {
    let dir = std::env::temp_dir().join(format!("hdbmv2_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = mvcc_setup(&dir);
    let mut a = db.new_session();
    let mut b = db.new_session();
    db.execute(&mut a, "START TRANSACTION WITH CONSISTENT SNAPSHOT")
        .unwrap();
    db.execute(&mut b, "DELETE FROM t WHERE id = 2").unwrap();
    // Deleted-after-pin still visible to the snapshot (point + scan).
    assert_eq!(mvcc_val(&db, &mut a, 2).as_deref(), Some("b"));
    let out = db.execute(&mut a, "SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(out.rows[0][0], Datum::Int(3));
    let out = db.execute(&mut b, "SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(out.rows[0][0], Datum::Int(2));
    db.execute(&mut a, "ROLLBACK").unwrap();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn snapshot_rollback_isolation() {
    let dir = std::env::temp_dir().join(format!("hdbmv3_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = mvcc_setup(&dir);
    let mut a = db.new_session();
    let mut b = db.new_session();
    // Uncommitted writes never leak, pinned or not.
    db.execute(&mut b, "BEGIN").unwrap();
    db.execute(&mut b, "INSERT INTO t VALUES (9, 'uncommitted')").unwrap();
    db.execute(&mut a, "START TRANSACTION WITH CONSISTENT SNAPSHOT")
        .unwrap();
    assert_eq!(mvcc_val(&db, &mut a, 9), None);
    assert_eq!(mvcc_val(&db, &mut b, 9).as_deref(), Some("uncommitted"));
    db.execute(&mut b, "ROLLBACK").unwrap();
    assert_eq!(mvcc_val(&db, &mut a, 9), None);
    db.execute(&mut a, "COMMIT").unwrap();
    let out = db.execute(&mut b, "SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(out.rows[0][0], Datum::Int(3));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn snapshot_version_pruning() {
    let dir = std::env::temp_dir().join(format!("hdbmv4_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = mvcc_setup(&dir);
    let mut a = db.new_session();
    let mut b = db.new_session();
    db.execute(&mut a, "START TRANSACTION WITH CONSISTENT SNAPSHOT")
        .unwrap();
    for i in 0..5 {
        db.execute(&mut b, &format!("UPDATE t SET v = 'v{i}' WHERE id = 1"))
            .unwrap();
    }
    let (chains, entries, active) = db.version_stats();
    assert_eq!(active, 1);
    assert!(chains >= 1 && entries >= 5, "chains={chains} entries={entries}");
    // Releasing the only reader drains everything.
    db.execute(&mut a, "COMMIT").unwrap();
    assert_eq!(db.version_stats(), (0, 0, 0));
    // Plain OLTP records nothing at all.
    db.execute(&mut b, "UPDATE t SET v = 'final' WHERE id = 1")
        .unwrap();
    assert_eq!(db.version_stats(), (0, 0, 0));
    assert_eq!(mvcc_val(&db, &mut b, 1).as_deref(), Some("final"));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn snapshot_concurrent_writer_stress() {
    use std::sync::{Arc, Barrier};
    let dir = std::env::temp_dir().join(format!("hdbmv5_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = Arc::new(mvcc_setup(&dir));
    // Pin before the writer starts.
    let mut r = db.new_session();
    db.execute(&mut r, "START TRANSACTION WITH CONSISTENT SNAPSHOT")
        .unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let (dbw, barrierw) = (db.clone(), barrier.clone());
    let writer = std::thread::spawn(move || {
        let mut w = dbw.new_session();
        barrierw.wait();
        for i in 0..200 {
            dbw.execute(&mut w, &format!("UPDATE t SET v = 'w{i}' WHERE id = 1"))
                .unwrap();
        }
    });
    barrier.wait();
    // The pinned reader observes one stable value throughout the storm
    // (first read pins nothing new — every read re-resolves at read_epoch).
    let first = mvcc_val(&db, &mut r, 1);
    assert_eq!(first.as_deref(), Some("a"));
    for _ in 0..50 {
        assert_eq!(mvcc_val(&db, &mut r, 1), first);
    }
    writer.join().unwrap();
    assert_eq!(mvcc_val(&db, &mut r, 1).as_deref(), Some("a"));
    db.execute(&mut r, "COMMIT").unwrap();
    // After release the newest value is visible.
    let mut f = db.new_session();
    assert_eq!(mvcc_val(&db, &mut f, 1).as_deref(), Some("w199"));
    let _ = fs::remove_dir_all(&dir);
}

// -- FK auto-index + greedy join ordering ------------------------------------

fn fk_index_names(db: &Database, s: &mut Session, table: &str) -> Vec<String> {
    let key = format!("{}.{}", s.current_db, table);
    let guard = db.tables.read().unwrap();
    let t = guard.get(&key).unwrap();
    let mut names: Vec<String> = t.secondary_indexes().iter().map(|d| d.name.clone()).collect();
    names.sort();
    names
}

#[test]
fn fk_auto_index_created_and_guarded() {
    let dir = std::env::temp_dir().join(format!("hdbfki_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();
    db.execute(&mut s, "CREATE TABLE p (id INT PRIMARY KEY)").unwrap();
    // Bare FK column gains an automatic index.
    db.execute(
        &mut s,
        "CREATE TABLE c (id INT PRIMARY KEY, p_id INT, \
         FOREIGN KEY (p_id) REFERENCES p(id))",
    )
    .unwrap();
    assert_eq!(fk_index_names(&db, &mut s, "c"), vec!["fk_p_id".to_string()]);
    // Pre-indexed column: no duplicate auto index.
    db.execute(&mut s, "CREATE INDEX cov ON c (p_id)").unwrap();
    db.execute(
        &mut s,
        "CREATE TABLE c2 (id INT PRIMARY KEY, p_id INT, \
         FOREIGN KEY (p_id) REFERENCES p(id))",
    )
    .unwrap();
    // c2 gets its own auto index; a second covering index allows dropping
    // either one, but the last covering index stays protected.
    assert_eq!(fk_index_names(&db, &mut s, "c2"), vec!["fk_p_id".to_string()]);
    db.execute(&mut s, "DROP INDEX cov ON c").unwrap();
    assert_eq!(fk_index_names(&db, &mut s, "c"), vec!["fk_p_id".to_string()]);
    assert!(fk_err_is_violation(db.execute(&mut s, "DROP INDEX fk_p_id ON c")));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn fk_auto_index_recovery_and_seeks() {
    let dir = std::env::temp_dir().join(format!("hdbfkr_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    {
        let db = Database::open(&dir).unwrap();
        let mut s = db.new_session();
        db.execute(&mut s, "CREATE TABLE p (id INT PRIMARY KEY)").unwrap();
        db.execute(
            &mut s,
            "CREATE TABLE c (id INT PRIMARY KEY, p_id INT, \
             FOREIGN KEY (p_id) REFERENCES p(id) ON DELETE CASCADE)",
        )
        .unwrap();
        db.execute(&mut s, "INSERT INTO p VALUES (1), (2)").unwrap();
        db.execute(&mut s, "INSERT INTO c VALUES (10, 1), (11, 2)").unwrap();
        db.execute(&mut s, "CHECKPOINT").unwrap();
    }
    // Auto index survives the snapshot and CASCADE still works.
    {
        let db = Database::open(&dir).unwrap();
        let mut s = db.new_session();
        assert_eq!(fk_index_names(&db, &mut s, "c"), vec!["fk_p_id".to_string()]);
        db.execute(&mut s, "DELETE FROM p WHERE id = 1").unwrap();
        let out = db.execute(&mut s, "SELECT id FROM c ORDER BY id").unwrap();
        assert_eq!(out.rows.len(), 1);
        assert_eq!(out.rows[0][0], Datum::Int(11));
    }
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
    use super::plan::order_joins;
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
