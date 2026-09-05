use super::*;
use crate::types::Datum;

#[test]
fn parse_select_full() {
    let s = parse_sql(
        "SELECT id, name FROM users WHERE age >= 30 AND id < 100 ORDER BY id DESC LIMIT 10;",
    )
    .unwrap();
    match s {
        Statement::Select {
            items,
            from,
            selection,
            order_by,
            limit,
            ..
        } => {
            assert_eq!(
                items,
                vec![SelectItem::Column("id".into()), SelectItem::Column("name".into())]
            );
            assert_eq!(from, "users");
            assert!(selection.is_some());
            assert_eq!(order_by, vec![("id".into(), true)]);
            assert_eq!(limit, Some(10));
        }
        other => panic!("wrong stmt {other:?}"),
    }
}

#[test]
fn parse_create_and_insert() {
    let s = parse_sql(
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, score FLOAT)",
    )
    .unwrap();
    match s {
        Statement::CreateTable { name, columns, foreign_keys } => {
            assert_eq!(name, "users");
            assert_eq!(columns.len(), 3);
            assert!(columns[0].primary_key);
            assert!(columns[1].not_null);
            assert!(!columns[2].not_null);
            assert!(foreign_keys.is_empty());
        }
        other => panic!("wrong stmt {other:?}"),
    }
    let s = parse_sql("INSERT INTO users VALUES (1, 'ann', 2.5), (2, 'bob', -1.0)").unwrap();
    match s {
        Statement::Insert { table, rows } => {
            assert_eq!(table, "users");
            assert_eq!(rows.len(), 2);
        }
        other => panic!("wrong stmt {other:?}"),
    }
}

#[test]
fn parse_database_ddl_and_use() {
    let s = parse_sql("CREATE DATABASE IF NOT EXISTS app_db;").unwrap();
    assert_eq!(
        s,
        Statement::CreateDatabase {
            name: "app_db".into(),
            if_not_exists: true,
        }
    );

    let s = parse_sql("USE app_db;").unwrap();
    assert_eq!(s, Statement::UseDatabase { name: "app_db".into() });

    let s = parse_sql("SHOW DATABASES;").unwrap();
    assert_eq!(s, Statement::ShowDatabases);

    let s = parse_sql("DROP DATABASE IF EXISTS app_db;").unwrap();
    assert_eq!(
        s,
        Statement::DropDatabase {
            name: "app_db".into(),
            if_exists: true,
        }
    );
}

#[test]
fn parse_where_flips_literal_first() {
    let s = parse_sql("SELECT * FROM t WHERE 5 < id").unwrap();
    match s {
        Statement::Select { selection: Some(e), .. } => match e {
            Expr::Cmp { left, op, .. } => {
                assert_eq!(*left, Expr::Column("id".into()));
                assert_eq!(op, CmpOp::Gt);
            }
            other => panic!("wrong expr {other:?}"),
        },
        other => panic!("wrong stmt {other:?}"),
    }
}

#[test]
fn parse_create_and_drop_index() {
    let s = parse_sql("CREATE INDEX idx_age ON users (age)").unwrap();
    assert_eq!(
        s,
        Statement::CreateIndex {
            name: "idx_age".into(),
            table: "users".into(),
            column: "age".into(),
        }
    );
    let s = parse_sql("DROP INDEX idx_age ON users").unwrap();
    assert_eq!(
        s,
        Statement::DropIndex {
            name: "idx_age".into(),
            table: "users".into(),
        }
    );
}

#[test]
fn rejects_garbage() {
    assert!(parse_sql("FROB NICATE").is_err());
    assert!(parse_sql("SELECT FROM t").is_err());
    assert!(parse_sql("SELECT * FROM t extra").is_err());
}

#[test]
fn parse_or_precedence_and_parens() {
    let s = parse_sql("SELECT * FROM t WHERE a = 1 OR b = 2 AND c = 3").unwrap();
    match s {
        Statement::Select { selection: Some(e), .. } => match e {
            Expr::Or(left, right) => {
                assert!(matches!(*left, Expr::Cmp { .. }));
                assert!(matches!(*right, Expr::And(_, _)));
            }
            other => panic!("wrong shape {other:?}"),
        },
        other => panic!("wrong stmt {other:?}"),
    }
    let s = parse_sql("SELECT * FROM t WHERE (a = 1 OR b = 2) AND c = 3").unwrap();
    match s {
        Statement::Select { selection: Some(e), .. } => match e {
            Expr::And(left, _) => assert!(matches!(*left, Expr::Or(_, _))),
            other => panic!("wrong shape {other:?}"),
        },
        other => panic!("wrong stmt {other:?}"),
    }
    let s = parse_sql("SELECT * FROM t WHERE NOT a = 1").unwrap();
    match s {
        Statement::Select { selection: Some(Expr::Not(_)), .. } => {}
        other => panic!("wrong stmt {other:?}"),
    }
}

#[test]
fn parse_between_in_like() {
    let s = parse_sql("SELECT * FROM t WHERE id BETWEEN 1 AND 5 AND name NOT LIKE 'a%'").unwrap();
    match s {
        Statement::Select { selection: Some(Expr::And(left, right)), .. } => {
            match *left {
                Expr::Between { lo: Datum::Int(1), hi: Datum::Int(5), negated: false, .. } => {}
                other => panic!("wrong between {other:?}"),
            }
            match *right {
                Expr::Like { pattern, negated: true, .. } => assert_eq!(pattern, "a%"),
                other => panic!("wrong like {other:?}"),
            }
        }
        other => panic!("wrong stmt {other:?}"),
    }
    let s = parse_sql("SELECT * FROM t WHERE id IN (1, 2, 3) AND id NOT IN (9)").unwrap();
    match s {
        Statement::Select { selection: Some(Expr::And(left, right)), .. } => {
            match *left {
                Expr::In { values, negated: false, .. } => assert_eq!(values.len(), 3),
                other => panic!("wrong in {other:?}"),
            }
            match *right {
                Expr::In { values, negated: true, .. } => assert_eq!(values.len(), 1),
                other => panic!("wrong not-in {other:?}"),
            }
        }
        other => panic!("wrong stmt {other:?}"),
    }
    assert!(parse_sql("SELECT * FROM t WHERE id IN ()").is_err());
    assert!(parse_sql("SELECT * FROM t WHERE id IN (1, id)").is_err());
    assert!(parse_sql("SELECT * FROM t WHERE name LIKE 42").is_err());
    assert!(parse_sql("SELECT * FROM t WHERE id BETWEEN a AND b").is_err());
    assert!(parse_sql("SELECT * FROM t WHERE between = 1").is_ok());
    assert!(parse_sql("SELECT * FROM t WHERE like = 'a'").is_ok());
}

#[test]
fn like_match_cases() {
    assert!(like_match("abcdef", "abc%"));
    assert!(like_match("abc", "abc"));
    assert!(!like_match("abcd", "abc"));
    assert!(like_match("abc", "a_c"));
    assert!(!like_match("ac", "a_c"));
    assert!(like_match("", "%"));
    assert!(like_match("", ""));
    assert!(!like_match("a", ""));
    assert!(like_match("abXcd", "ab%cd"));
    assert!(!like_match("abXc", "ab%cd"));
    assert!(like_match("aaa", "%a%a%"));
    assert!(!like_match("ab", "a\\b"));
    assert!(like_match("a\\b", "a\\b"));
}

#[test]
fn parse_auto_increment() {
    let s = parse_sql("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, v TEXT)").unwrap();
    match s {
        Statement::CreateTable { columns, .. } => {
            assert!(columns[0].auto_increment);
            assert!(!columns[1].auto_increment);
        }
        other => panic!("wrong stmt {other:?}"),
    }
    let s = parse_sql("CREATE TABLE t (id INT AUTO_INCREMENT PRIMARY KEY)").unwrap();
    match s {
        Statement::CreateTable { columns, .. } => assert!(columns[0].auto_increment),
        other => panic!("wrong stmt {other:?}"),
    }
}

#[test]
fn parse_aggregates() {
    let s = parse_sql("SELECT SUM(score), AVG(score), MIN(id), MAX(name) FROM t WHERE id > 1").unwrap();
    match s {
        Statement::Select { items, from, selection, .. } => {
            assert_eq!(
                items,
                vec![
                    SelectItem::Aggregate { func: AggFunc::Sum, column: "score".into() },
                    SelectItem::Aggregate { func: AggFunc::Avg, column: "score".into() },
                    SelectItem::Aggregate { func: AggFunc::Min, column: "id".into() },
                    SelectItem::Aggregate { func: AggFunc::Max, column: "name".into() },
                ]
            );
            assert_eq!(from, "t");
            assert!(selection.is_some());
        }
        other => panic!("wrong stmt {other:?}"),
    }
    assert!(parse_sql("SELECT SUM(*) FROM t").is_err());
    assert!(parse_sql("SELECT SUM() FROM t").is_err());
}

#[test]
fn parse_join_and_group_by() {
    let s = parse_sql(
        "SELECT u.id, o.qty FROM users u JOIN orders o ON u.id = o.user_id WHERE o.qty > 1 GROUP BY u.id ORDER BY u.id LIMIT 5",
    );
    assert!(s.is_err());
    let s = parse_sql(
        "SELECT users.id, orders.qty FROM users JOIN orders ON users.id = orders.user_id WHERE orders.qty > 1 GROUP BY users.id ORDER BY users.id LIMIT 5",
    )
    .unwrap();
    match s {
        Statement::Select { items, from, joins, selection, order_by, limit, group_by } => {
            assert_eq!(from, "users");
            assert_eq!(items.len(), 2);
            assert_eq!(joins.len(), 1);
            assert_eq!(joins[0].table, "orders");
            assert_eq!(joins[0].kind, JoinKind::Inner);
            assert!(selection.is_some());
            assert_eq!(order_by, vec![("users.id".into(), false)]);
            assert_eq!(limit, Some(5));
            assert_eq!(group_by, vec!["users.id".to_string()]);
        }
        other => panic!("wrong stmt {other:?}"),
    }
    let s = parse_sql("SELECT * FROM a LEFT OUTER JOIN b ON a.id = b.id").unwrap();
    match s {
        Statement::Select { joins, .. } => assert_eq!(joins[0].kind, JoinKind::Left),
        other => panic!("wrong stmt {other:?}"),
    }
    assert!(parse_sql("SELECT * FROM a RIGHT JOIN b ON a.id = b.id").is_err());
    assert!(parse_sql("SELECT * FROM a FULL JOIN b ON a.id = b.id").is_err());
    let s = parse_sql("SELECT * FROM a JOIN b ON a.id = b.a_id JOIN c ON b.id = c.b_id").unwrap();
    match s {
        Statement::Select { joins, .. } => {
            assert_eq!(joins.len(), 2);
            assert_eq!(joins[1].table, "c");
        }
        other => panic!("wrong stmt {other:?}"),
    }
}

#[test]
fn parse_foreign_keys() {
    let s = parse_sql(
        "CREATE TABLE o (oid INT PRIMARY KEY, uid INT, \
         CONSTRAINT fk_user FOREIGN KEY (uid) REFERENCES users(id) ON DELETE CASCADE)",
    )
    .unwrap();
    match s {
        Statement::CreateTable { foreign_keys, .. } => {
            assert_eq!(foreign_keys.len(), 1);
            let fk = &foreign_keys[0];
            assert_eq!(fk.name.as_deref(), Some("fk_user"));
            assert_eq!(fk.column, "uid");
            assert_eq!(fk.ref_table, "users");
            assert_eq!(fk.ref_column, "id");
            assert_eq!(fk.on_delete, crate::table::FkAction::Cascade);
        }
        other => panic!("wrong stmt {other:?}"),
    }
    // Defaults to RESTRICT; unnamed; SET NULL parses.
    let s = parse_sql(
        "CREATE TABLE o (oid INT PRIMARY KEY, a INT, b INT, \
         FOREIGN KEY (a) REFERENCES p(id), \
         FOREIGN KEY (b) REFERENCES p(id) ON DELETE SET NULL)",
    )
    .unwrap();
    match s {
        Statement::CreateTable { foreign_keys, .. } => {
            assert_eq!(foreign_keys.len(), 2);
            assert_eq!(foreign_keys[0].on_delete, crate::table::FkAction::Restrict);
            assert!(foreign_keys[0].name.is_none());
            assert_eq!(foreign_keys[1].on_delete, crate::table::FkAction::SetNull);
        }
        other => panic!("wrong stmt {other:?}"),
    }
    // Non-RESTRICT ON UPDATE is rejected.
    assert!(parse_sql(
        "CREATE TABLE o (oid INT PRIMARY KEY, a INT, \
         FOREIGN KEY (a) REFERENCES p(id) ON UPDATE CASCADE)"
    )
    .is_err());
}

#[test]
fn parse_start_transaction() {
    match parse_sql("START TRANSACTION").unwrap() {
        Statement::StartTransaction { snapshot } => assert!(!snapshot),
        other => panic!("wrong stmt {other:?}"),
    }
    match parse_sql("START TRANSACTION WITH CONSISTENT SNAPSHOT").unwrap() {
        Statement::StartTransaction { snapshot } => assert!(snapshot),
        other => panic!("wrong stmt {other:?}"),
    }
    assert!(parse_sql("START TRANSACTION WITH FOO").is_err());
    assert!(parse_sql("START FOO").is_err());
}
