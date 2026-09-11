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
            assert_eq!(from.name(), "users");
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
        Statement::CreateTable { name, columns, foreign_keys, if_not_exists } => {
            assert_eq!(name, "users");
            assert_eq!(columns.len(), 3);
            assert!(columns[0].primary_key);
            assert!(columns[1].not_null);
            assert!(!columns[2].not_null);
            assert!(foreign_keys.is_empty());
            assert!(!if_not_exists);
        }
        other => panic!("wrong stmt {other:?}"),
    }
    let s = parse_sql("INSERT INTO users VALUES (1, 'ann', 2.5), (2, 'bob', -1.0)").unwrap();
    match s {
        Statement::Insert { table, columns, rows } => {
            assert_eq!(table, "users");
            assert!(columns.is_none());
            assert_eq!(rows.len(), 2);
        }
        other => panic!("wrong stmt {other:?}"),
    }

    let s = parse_sql("INSERT INTO users (id, name) VALUES (1, 'ann'), (2, 'bob')").unwrap();
    match s {
        Statement::Insert { table, columns, rows } => {
            assert_eq!(table, "users");
            assert_eq!(columns, Some(vec!["id".to_string(), "name".to_string()]));
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

    let s = parse_sql("SHOW STATUS;").unwrap();
    assert_eq!(s, Statement::ShowStatus { like: None });

    let s = parse_sql("SHOW STATUS LIKE 'Com_%';").unwrap();
    assert_eq!(
        s,
        Statement::ShowStatus {
            like: Some("Com_%".into())
        }
    );

    let s = parse_sql("SHOW ENGINE STATUS;").unwrap();
    assert_eq!(s, Statement::ShowEngineStatus);

    let s = parse_sql("SHOW ENGINE INNODB STATUS;").unwrap();
    assert_eq!(s, Statement::ShowEngineStatus);

    let s = parse_sql("SHOW ENGINE;").unwrap();
    assert_eq!(s, Statement::ShowEngineStatus);

    let s = parse_sql("SHOW ENGINES;").unwrap();
    assert_eq!(s, Statement::ShowEngineStatus);

    let s = parse_sql("SHOW PROCESSLIST;").unwrap();
    assert_eq!(s, Statement::ShowProcesslist);

    let s = parse_sql("PROMOTE;").unwrap();
    assert_eq!(s, Statement::Promote);

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
            if_not_exists: false,
        }
    );
    let s = parse_sql("DROP INDEX idx_age ON users").unwrap();
    assert_eq!(
        s,
        Statement::DropIndex {
            name: "idx_age".into(),
            table: "users".into(),
            if_exists: false,
        }
    );
}

#[test]
fn parse_if_exists_and_table_pk() {
    let s = parse_sql("CREATE TABLE IF NOT EXISTS items (id INT, name TEXT, PRIMARY KEY (id))").unwrap();
    match s {
        Statement::CreateTable { name, columns, if_not_exists, .. } => {
            assert_eq!(name, "items");
            assert!(if_not_exists);
            assert_eq!(columns.len(), 2);
            assert!(columns[0].primary_key);
            assert!(columns[0].not_null);
        }
        other => panic!("wrong stmt {other:?}"),
    }

    let s2 = parse_sql("CREATE TABLE IF NOT EXISTS orders (oid BIGINT, CONSTRAINT pk_orders PRIMARY KEY (oid))").unwrap();
    match s2 {
        Statement::CreateTable { name, columns, if_not_exists, .. } => {
            assert_eq!(name, "orders");
            assert!(if_not_exists);
            assert!(columns[0].primary_key);
        }
        other => panic!("wrong stmt {other:?}"),
    }

    let s3 = parse_sql("DROP TABLE IF EXISTS items").unwrap();
    assert_eq!(s3, Statement::DropTable { name: "items".into(), if_exists: true });

    let s4 = parse_sql("CREATE INDEX IF NOT EXISTS idx_name ON items (name)").unwrap();
    assert_eq!(
        s4,
        Statement::CreateIndex {
            name: "idx_name".into(),
            table: "items".into(),
            column: "name".into(),
            if_not_exists: true,
        }
    );

    let s5 = parse_sql("DROP INDEX IF EXISTS idx_name ON items").unwrap();
    assert_eq!(
        s5,
        Statement::DropIndex {
            name: "idx_name".into(),
            table: "items".into(),
            if_exists: true,
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
            assert_eq!(from.name(), "t");
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
            assert_eq!(from.name(), "users");
            assert_eq!(items.len(), 2);
            assert_eq!(joins.len(), 1);
            assert_eq!(joins[0].table.name(), "orders");
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
            assert_eq!(joins[1].table.name(), "c");
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
        Statement::StartTransaction { snapshot, isolation, read_only } => {
            assert!(!snapshot);
            assert_eq!(isolation, None);
            assert!(!read_only);
        }
        other => panic!("wrong stmt {other:?}"),
    }
    match parse_sql("START TRANSACTION WITH CONSISTENT SNAPSHOT").unwrap() {
        Statement::StartTransaction { snapshot, .. } => assert!(snapshot),
        other => panic!("wrong stmt {other:?}"),
    }
    match parse_sql("START TRANSACTION ISOLATION LEVEL READ COMMITTED").unwrap() {
        Statement::StartTransaction { isolation, .. } => {
            assert_eq!(isolation, Some(IsolationLevel::ReadCommitted));
        }
        other => panic!("wrong stmt {other:?}"),
    }
    match parse_sql("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY").unwrap() {
        Statement::Begin { isolation, read_only } => {
            assert_eq!(isolation, Some(IsolationLevel::RepeatableRead));
            assert!(read_only);
        }
        other => panic!("wrong stmt {other:?}"),
    }
    match parse_sql("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE").unwrap() {
        Statement::SetTransaction { isolation, global } => {
            assert_eq!(isolation, IsolationLevel::Serializable);
            assert!(!global);
        }
        other => panic!("wrong stmt {other:?}"),
    }
    match parse_sql("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED").unwrap() {
        Statement::SetTransaction { isolation, global } => {
            assert_eq!(isolation, IsolationLevel::ReadCommitted);
            assert!(!global);
        }
        other => panic!("wrong stmt {other:?}"),
    }
    assert!(parse_sql("START TRANSACTION WITH FOO").is_err());
    assert!(parse_sql("START FOO").is_err());
}

#[test]
fn parse_analyze_and_explain() {
    let s = parse_sql("ANALYZE TABLE users;").unwrap();
    assert_eq!(s, Statement::AnalyzeTable { table: "users".into() });

    let s = parse_sql("EXPLAIN SELECT * FROM t WHERE id = 1;").unwrap();
    match s {
        Statement::Explain { analyze, statement } => {
            assert!(!analyze);
            assert!(matches!(*statement, Statement::Select { .. }));
        }
        other => panic!("wrong stmt {other:?}"),
    }

    let s = parse_sql("EXPLAIN MEMO SELECT * FROM t WHERE id = 1;").unwrap();
    match s {
        Statement::ExplainMemo { statement } => {
            assert!(matches!(*statement, Statement::Select { .. }));
        }
        other => panic!("wrong stmt {other:?}"),
    }

    let s = parse_sql("EXPLAIN ANALYZE SELECT a FROM t JOIN u ON t.id = u.id;").unwrap();
    match s {
        Statement::Explain { analyze, statement } => {
            assert!(analyze);
            assert!(matches!(*statement, Statement::Select { .. }));
        }
        other => panic!("wrong stmt {other:?}"),
    }

    // MySQL DESCRIBE synonym over SELECT.
    let s = parse_sql("DESCRIBE SELECT * FROM t;").unwrap();
    match s {
        Statement::Explain { analyze, statement } => {
            assert!(!analyze);
            assert!(matches!(*statement, Statement::Select { .. }));
        }
        other => panic!("wrong stmt {other:?}"),
    }

    assert!(parse_sql("EXPLAIN DELETE FROM t;").is_err());
    assert!(parse_sql("DESCRIBE t;").is_err());
    assert!(parse_sql("ANALYZE t;").is_err());
}

#[test]
fn parse_subqueries() {
    // IN-subquery (and NOT IN).
    let s = parse_sql("SELECT * FROM t WHERE id IN (SELECT uid FROM orders);").unwrap();
    match s {
        Statement::Select { selection: Some(Expr::InSubquery { expr, query, negated }), .. } => {
            assert_eq!(*expr, Expr::Column("id".into()));
            assert_eq!(query.from, TableRef::Table("orders".into()));
            assert!(!negated);
        }
        other => panic!("wrong stmt {other:?}"),
    }
    let s = parse_sql("SELECT * FROM t WHERE id NOT IN (SELECT uid FROM orders);").unwrap();
    match s {
        Statement::Select { selection: Some(Expr::InSubquery { negated, .. }), .. } => {
            assert!(negated);
        }
        other => panic!("wrong stmt {other:?}"),
    }
    // Literal IN-lists still parse as value lists.
    let s = parse_sql("SELECT * FROM t WHERE id IN (1, 2);").unwrap();
    match s {
        Statement::Select { selection: Some(Expr::In { values, .. }), .. } => {
            assert_eq!(values.len(), 2);
        }
        other => panic!("wrong stmt {other:?}"),
    }
    // EXISTS / NOT EXISTS.
    let s = parse_sql("SELECT * FROM t WHERE EXISTS (SELECT 1 FROM orders);").unwrap();
    match s {
        Statement::Select { selection: Some(Expr::Exists { negated, .. }), .. } => {
            assert!(!negated);
        }
        other => panic!("wrong stmt {other:?}"),
    }
    let s = parse_sql("SELECT * FROM t WHERE NOT EXISTS (SELECT 1 FROM orders);").unwrap();
    match s {
        Statement::Select { selection: Some(Expr::Exists { negated, .. }), .. } => {
            assert!(negated);
        }
        other => panic!("wrong stmt {other:?}"),
    }
    // Scalar subqueries: both comparison sides + parenthesized-left form.
    let s = parse_sql("SELECT * FROM t WHERE g > (SELECT MAX(g) FROM u);").unwrap();
    match s {
        Statement::Select {
            selection: Some(Expr::Cmp { left, op: CmpOp::Gt, right }),
            ..
        } => {
            assert!(matches!(*left, Expr::Column(_)));
            assert!(matches!(*right, Expr::ScalarSubquery(_)));
        }
        other => panic!("wrong stmt {other:?}"),
    }
    let s = parse_sql("SELECT * FROM t WHERE (SELECT MAX(g) FROM u) > 5;").unwrap();
    match s {
        Statement::Select {
            selection: Some(Expr::Cmp { left, op: CmpOp::Gt, right }),
            ..
        } => {
            assert!(matches!(*left, Expr::ScalarSubquery(_)));
            assert!(matches!(*right, Expr::Literal(_)));
        }
        other => panic!("wrong stmt {other:?}"),
    }
    // Scalar in the projection list, with and without AS alias.
    let s = parse_sql("SELECT a, (SELECT MAX(g) FROM u) AS m FROM t;").unwrap();
    match s {
        Statement::Select { items, .. } => {
            assert_eq!(items.len(), 2);
            match &items[1] {
                SelectItem::Subquery { alias, .. } => assert_eq!(alias.as_deref(), Some("m")),
                other => panic!("wrong item {other:?}"),
            }
        }
        other => panic!("wrong stmt {other:?}"),
    }
    // Derived tables in FROM and JOIN, AS optional.
    let s = parse_sql("SELECT * FROM (SELECT id FROM t WHERE id > 1) AS d;").unwrap();
    match s {
        Statement::Select { from, .. } => match from {
            TableRef::Derived { alias, .. } => assert_eq!(alias, "d"),
            other => panic!("wrong from {other:?}"),
        },
        other => panic!("wrong stmt {other:?}"),
    }
    let s = parse_sql(
        "SELECT * FROM t JOIN (SELECT uid FROM orders) o ON t.id = o.uid;",
    )
    .unwrap();
    match s {
        Statement::Select { joins, .. } => {
            assert_eq!(joins.len(), 1);
            match &joins[0].table {
                TableRef::Derived { alias, .. } => assert_eq!(alias, "o"),
                other => panic!("wrong join table {other:?}"),
            }
        }
        other => panic!("wrong stmt {other:?}"),
    }
    // Rejections: derived without alias, non-SELECT parens in FROM.
    assert!(parse_sql("SELECT * FROM (SELECT id FROM t);").is_err());
    assert!(parse_sql("SELECT * FROM (t);").is_err());
}

#[test]
fn parse_sysfunc_fromless_and_casts() {
    // FROM-less system function: one row, no source.
    let s = parse_sql("SELECT version();").unwrap();
    match s {
        Statement::Select { items, from, .. } => {
            assert_eq!(
                items,
                vec![SelectItem::SysFunc { name: "version".into(), alias: None }]
            );
            assert!(matches!(from, TableRef::Empty));
        }
        other => panic!("wrong stmt {other:?}"),
    }
    // FROM-less literal keeps working (`EXISTS (SELECT 1 ...)` body).
    let s = parse_sql("SELECT 1;").unwrap();
    match s {
        Statement::Select { items, from, .. } => {
            assert_eq!(items, vec![SelectItem::Literal(Datum::Int(1))]);
            assert!(matches!(from, TableRef::Empty));
        }
        other => panic!("wrong stmt {other:?}"),
    }
    // Alias + trailing `::type` suffixes on system functions.
    let s = parse_sql("SELECT current_schema() AS s FROM t;").unwrap();
    match s {
        Statement::Select { items, from, .. } => {
            assert_eq!(
                items,
                vec![SelectItem::SysFunc {
                    name: "current_schema".into(),
                    alias: Some("s".into())
                }]
            );
            assert_eq!(from, TableRef::Table("t".into()));
        }
        other => panic!("wrong stmt {other:?}"),
    }
    let s = parse_sql("SELECT version()::text;").unwrap();
    match s {
        Statement::Select { items, .. } => {
            assert_eq!(
                items,
                vec![SelectItem::SysFunc { name: "version".into(), alias: None }]
            );
        }
        other => panic!("wrong stmt {other:?}"),
    }
    // Dotted FROM names (system views, cross-database tables).
    let s = parse_sql("SELECT * FROM pg_catalog.pg_class;").unwrap();
    match s {
        Statement::Select { from, .. } => {
            assert_eq!(from, TableRef::Table("pg_catalog.pg_class".into()));
        }
        other => panic!("wrong stmt {other:?}"),
    }
    assert!(parse_sql("SELECT * FROM a.b.c;").is_err());
    // `::` casts desugar at parse time.
    let s = parse_sql("SELECT * FROM t WHERE relname = 'x'::regclass;").unwrap();
    match s {
        Statement::Select { selection, .. } => match selection.unwrap() {
            Expr::Cmp { left, right, .. } => {
                assert!(matches!(*left, Expr::Column(_)));
                assert_eq!(*right, Expr::Literal(Datum::Text("x".into())));
            }
            other => panic!("wrong expr {other:?}"),
        },
        other => panic!("wrong stmt {other:?}"),
    }
    let s = parse_sql("SELECT * FROM t WHERE n = '5'::int4;").unwrap();
    match s {
        Statement::Select { selection, .. } => match selection.unwrap() {
            Expr::Cmp { right, .. } => {
                assert_eq!(*right, Expr::Literal(Datum::Int(5)));
            }
            other => panic!("wrong expr {other:?}"),
        },
        other => panic!("wrong stmt {other:?}"),
    }
    let s = parse_sql("SELECT * FROM t WHERE c::text = 'v';").unwrap();
    match s {
        Statement::Select { selection, .. } => match selection.unwrap() {
            Expr::Cmp { left, .. } => {
                assert_eq!(*left, Expr::Column("c".into()));
            }
            other => panic!("wrong expr {other:?}"),
        },
        other => panic!("wrong stmt {other:?}"),
    }
    // Unknown cast targets and unparsable literals fail closed.
    assert!(parse_sql("SELECT * FROM t WHERE c = 'v'::money;").is_err());
    assert!(parse_sql("SELECT * FROM t WHERE n = 'abc'::int4;").is_err());
    assert!(parse_sql("SELECT * FROM t WHERE n = 1::text::int4;").is_ok());
}

#[test]
fn parse_user_management_and_grants() {
    use super::ast::{GrantScope, Privilege};
    // CREATE USER with IF NOT EXISTS + host specifier.
    match parse_sql("CREATE USER IF NOT EXISTS 'alice'@'localhost' IDENTIFIED BY 's3cret';").unwrap() {
        Statement::CreateUser { name, if_not_exists, password } => {
            assert_eq!(name, "alice");
            assert!(if_not_exists);
            assert_eq!(password, "s3cret");
        }
        other => panic!("wrong stmt {other:?}"),
    }
    // Bare identifiers work too; non-string passwords fail.
    match parse_sql("CREATE USER bob IDENTIFIED BY 'pw';").unwrap() {
        Statement::CreateUser { name, .. } => assert_eq!(name, "bob"),
        other => panic!("wrong stmt {other:?}"),
    }
    assert!(parse_sql("CREATE USER bob IDENTIFIED BY 42;").is_err());
    // DROP / ALTER.
    match parse_sql("DROP USER IF EXISTS 'alice';").unwrap() {
        Statement::DropUser { name, if_exists } => {
            assert_eq!(name, "alice");
            assert!(if_exists);
        }
        other => panic!("wrong stmt {other:?}"),
    }
    match parse_sql("ALTER USER 'alice' IDENTIFIED BY 'new';").unwrap() {
        Statement::AlterUser { name, password } => {
            assert_eq!(name, "alice");
            assert_eq!(password, "new");
        }
        other => panic!("wrong stmt {other:?}"),
    }
    // GRANT with priv list, scopes, and host specifiers.
    match parse_sql("GRANT SELECT, INSERT ON shop.* TO 'alice'@'%';").unwrap() {
        Statement::Grant { privs, scope, user } => {
            assert_eq!(privs, vec![Privilege::Select, Privilege::Insert]);
            assert_eq!(scope, GrantScope::Database { db: "shop".into() });
            assert_eq!(user, "alice");
        }
        other => panic!("wrong stmt {other:?}"),
    }
    match parse_sql("GRANT ALL PRIVILEGES ON *.* TO root;").unwrap() {
        Statement::Grant { privs, scope, user } => {
            assert_eq!(privs, vec![Privilege::All]);
            assert_eq!(scope, GrantScope::Global);
            assert_eq!(user, "root");
        }
        other => panic!("wrong stmt {other:?}"),
    }
    match parse_sql("GRANT UPDATE ON shop.orders TO bob;").unwrap() {
        Statement::Grant { scope, .. } => {
            assert_eq!(
                scope,
                GrantScope::Table { db: Some("shop".into()), tbl: "orders".into() }
            );
        }
        other => panic!("wrong stmt {other:?}"),
    }
    match parse_sql("GRANT DELETE ON orders TO bob;").unwrap() {
        Statement::Grant { scope, .. } => {
            assert_eq!(scope, GrantScope::Table { db: None, tbl: "orders".into() });
        }
        other => panic!("wrong stmt {other:?}"),
    }
    // REVOKE mirrors GRANT with FROM.
    match parse_sql("REVOKE SELECT ON *.* FROM 'alice';").unwrap() {
        Statement::Revoke { privs, scope, user } => {
            assert_eq!(privs, vec![Privilege::Select]);
            assert_eq!(scope, GrantScope::Global);
            assert_eq!(user, "alice");
        }
        other => panic!("wrong stmt {other:?}"),
    }
    // SHOW GRANTS with and without FOR.
    match parse_sql("SHOW GRANTS;").unwrap() {
        Statement::ShowGrants { for_user } => assert_eq!(for_user, None),
        other => panic!("wrong stmt {other:?}"),
    }
    match parse_sql("SHOW GRANTS FOR 'alice'@localhost;").unwrap() {
        Statement::ShowGrants { for_user } => assert_eq!(for_user.as_deref(), Some("alice")),
        other => panic!("wrong stmt {other:?}"),
    }
    // Rejections: unknown privileges, missing ON/TO, ALTER non-USER.
    assert!(parse_sql("GRANT FROBNICATE ON *.* TO bob;").is_err());
    assert!(parse_sql("GRANT SELECT *.* TO bob;").is_err());
    assert!(parse_sql("GRANT SELECT ON *.* bob;").is_err());
    assert!(parse_sql("ALTER TABLE t ADD COLUMN c INT;").is_err());
}

#[test]
fn fuzz_sql_parser_adversarial_inputs() {
    let valid_samples = [
        "SELECT id, name, score FROM users WHERE age >= 30 AND score < 100.5 ORDER BY id DESC LIMIT 10;",
        "INSERT INTO accounts (id, balance, status) VALUES (1, 1000, 'active'), (2, 2500, 'pending');",
        "UPDATE accounts SET balance = balance + 50 WHERE id = 1 AND status = 'active';",
        "DELETE FROM orders WHERE created_at < '2025-01-01' AND (status = 'cancelled' OR amount = 0);",
        "CREATE TABLE customers (id INT PRIMARY KEY, name TEXT NOT NULL, email TEXT, balance FLOAT, FOREIGN KEY (id) REFERENCES org(id));",
        "SELECT a.id, b.val, COUNT(*), SUM(a.amt) FROM t1 a LEFT JOIN t2 b ON a.k = b.k GROUP BY a.id, b.val HAVING COUNT(*) > 1;",
        "SELECT * FROM (SELECT id, val FROM items WHERE val IN ('a', 'b', 'c')) AS sub WHERE id BETWEEN 10 AND 100;",
        "GRANT SELECT, INSERT ON db1.tbl TO 'alice'@'%';",
        "BEGIN TRANSACTION;",
        "SET TRANSACTION ISOLATION LEVEL READ COMMITTED;",
    ];

    // 1. Truncation stress: truncate valid queries at every single byte offset
    for sample in &valid_samples {
        for i in 0..=sample.len() {
            let prefix = &sample[..i];
            let result = std::panic::catch_unwind(|| {
                let _ = parse_sql(prefix);
            });
            assert!(result.is_ok(), "SQL parser panicked on truncated query prefix: {prefix:?}");
        }
    }

    // 2. Deeply nested parentheses & expressions (up to 300 levels)
    for depth in [10, 50, 100, 250] {
        let open_parens = "(".repeat(depth);
        let close_parens = ")".repeat(depth);
        let deep_expr = format!("SELECT {open_parens} 1 + 2 {close_parens};");
        let result = std::panic::catch_unwind(|| {
            let _ = parse_sql(&deep_expr);
        });
        assert!(result.is_ok(), "SQL parser panicked on nesting depth {depth}");

        // Mismatched / unclosed parentheses
        let mismatched = format!("SELECT {open_parens} 1 + 2;");
        let result2 = std::panic::catch_unwind(|| {
            let _ = parse_sql(&mismatched);
        });
        assert!(result2.is_ok(), "SQL parser panicked on unclosed parentheses at depth {depth}");
    }

    // 3. Deterministic XorShift PRNG for randomized fuzz mutations
    let mut state = 0x853c49e6748fea9bu64;
    let mut next_u64 = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };

    let fuzz_chars = [
        '\'', '"', '`', ';', ',', '(', ')', '[', ']', '{', '}',
        '+', '-', '*', '/', '%', '=', '<', '>', '!', '~', '^',
        '0', '9', ' ', '\t', '\n', '\r', '\0', '\\',
        '\u{00FF}', '\u{202E}', '\u{FEFF}', '\u{1F600}',
    ];

    for _ in 0..2_500 {
        let len = (next_u64() % 80) as usize;
        let mut text = String::with_capacity(len);
        for _ in 0..len {
            let idx = (next_u64() as usize) % fuzz_chars.len();
            text.push(fuzz_chars[idx]);
        }

        let result = std::panic::catch_unwind(|| {
            let _ = parse_sql(&text);
        });
        assert!(result.is_ok(), "SQL parser panicked on random adversarial string: {text:?}");
    }
}

