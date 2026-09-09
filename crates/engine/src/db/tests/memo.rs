//! Cascades Memo Query Optimizer & Equivalence Classes tests.

use super::*;
use crate::db::memo::{
    JoinBuildSide, LogicalOp, Memo, OptimizerContext, PhysicalOp, TableMeta,
};
use crate::sql::{CmpOp, Expr, JoinKind};
use crate::types::Datum;

fn test_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("hdbmemo_{tag}_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    dir
}

#[test]
fn memo_group_deduplication() {
    let dir = test_dir("dedup");
    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();
    db.execute(&mut s, "CREATE TABLE t1 (id INT PRIMARY KEY, a INT)").unwrap();
    db.execute(&mut s, "CREATE TABLE t2 (id INT PRIMARY KEY, b INT)").unwrap();

    let t1 = db.table(&s, "t1").unwrap();
    let t2 = db.table(&s, "t2").unwrap();

    let ctx = OptimizerContext::new(vec![
        TableMeta {
            name: "t1".into(),
            total_rows: 100.0,
            table: t1,
        },
        TableMeta {
            name: "t2".into(),
            total_rows: 50.0,
            table: t2,
        },
    ]);

    let mut memo = Memo::new();

    // Insert Scan(t1)
    let g1 = memo.insert_logical(
        LogicalOp::Scan {
            table_idx: 0,
            table_name: "t1".into(),
        },
        &ctx,
    );
    assert_eq!(g1, 0);
    assert_eq!(memo.num_groups(), 1);

    // Insert Scan(t1) again -> must return identical GroupId without adding a new group
    let g1_dup = memo.insert_logical(
        LogicalOp::Scan {
            table_idx: 0,
            table_name: "t1".into(),
        },
        &ctx,
    );
    assert_eq!(g1_dup, g1);
    assert_eq!(memo.num_groups(), 1);

    // Insert Scan(t2) -> creates group 1
    let g2 = memo.insert_logical(
        LogicalOp::Scan {
            table_idx: 1,
            table_name: "t2".into(),
        },
        &ctx,
    );
    assert_eq!(g2, 1);
    assert_eq!(memo.num_groups(), 2);

    // Add another logical expression to group 0
    let added = memo.add_logical_to_group(
        g1,
        LogicalOp::Filter {
            input: g1,
            predicate: Expr::Cmp {
                left: Box::new(Expr::Column("a".into())),
                op: CmpOp::Gt,
                right: Box::new(Expr::Literal(Datum::Int(10))),
            },
        },
    );
    assert!(added);
    assert_eq!(memo.group(g1).logical_exprs.len(), 2);

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn memo_join_commutativity() {
    let dir = test_dir("commute");
    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();
    db.execute(&mut s, "CREATE TABLE a (id INT PRIMARY KEY, x INT)").unwrap();
    db.execute(&mut s, "CREATE TABLE b (id INT PRIMARY KEY, y INT)").unwrap();

    let ta = db.table(&s, "a").unwrap();
    let tb = db.table(&s, "b").unwrap();

    let ctx = OptimizerContext::new(vec![
        TableMeta {
            name: "a".into(),
            total_rows: 1000.0,
            table: ta,
        },
        TableMeta {
            name: "b".into(),
            total_rows: 50.0,
            table: tb,
        },
    ]);

    let mut memo = Memo::new();
    let g_a = memo.insert_logical(
        LogicalOp::Scan {
            table_idx: 0,
            table_name: "a".into(),
        },
        &ctx,
    );
    let g_b = memo.insert_logical(
        LogicalOp::Scan {
            table_idx: 1,
            table_name: "b".into(),
        },
        &ctx,
    );

    let on_expr = Expr::Cmp {
        left: Box::new(Expr::Column("a.x".into())),
        op: CmpOp::Eq,
        right: Box::new(Expr::Column("b.y".into())),
    };

    let g_join = memo.insert_logical(
        LogicalOp::Join {
            left: g_a,
            right: g_b,
            on: Some(on_expr),
            kind: JoinKind::Inner,
        },
        &ctx,
    );

    assert_eq!(memo.group(g_join).logical_exprs.len(), 1);

    // Apply transformation rules: A ⋈ B === B ⋈ A
    memo.apply_transformation_rules(g_join, &ctx, 1);

    // Group should now have 2 equivalent logical expressions
    assert_eq!(memo.group(g_join).logical_exprs.len(), 2);

    let has_commuted = memo.group(g_join).logical_exprs.iter().any(|op| match op {
        LogicalOp::Join { left, right, kind: JoinKind::Inner, .. } => *left == g_b && *right == g_a,
        _ => false,
    });
    assert!(has_commuted, "Commuted join B ⋈ A should be in the same group");

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn memo_join_associativity() {
    let dir = test_dir("assoc");
    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();
    db.execute(&mut s, "CREATE TABLE a (id INT PRIMARY KEY, k INT)").unwrap();
    db.execute(&mut s, "CREATE TABLE b (id INT PRIMARY KEY, k INT)").unwrap();
    db.execute(&mut s, "CREATE TABLE c (id INT PRIMARY KEY, k INT)").unwrap();

    let ta = db.table(&s, "a").unwrap();
    let tb = db.table(&s, "b").unwrap();
    let tc = db.table(&s, "c").unwrap();

    let ctx = OptimizerContext::new(vec![
        TableMeta { name: "a".into(), total_rows: 100.0, table: ta },
        TableMeta { name: "b".into(), total_rows: 200.0, table: tb },
        TableMeta { name: "c".into(), total_rows: 300.0, table: tc },
    ]);

    let mut memo = Memo::new();
    let ga = memo.insert_logical(LogicalOp::Scan { table_idx: 0, table_name: "a".into() }, &ctx);
    let gb = memo.insert_logical(LogicalOp::Scan { table_idx: 1, table_name: "b".into() }, &ctx);
    let gc = memo.insert_logical(LogicalOp::Scan { table_idx: 2, table_name: "c".into() }, &ctx);

    let on_ab = Expr::Cmp {
        left: Box::new(Expr::Column("a.k".into())),
        op: CmpOp::Eq,
        right: Box::new(Expr::Column("b.k".into())),
    };
    let g_ab = memo.insert_logical(
        LogicalOp::Join { left: ga, right: gb, on: Some(on_ab), kind: JoinKind::Inner },
        &ctx,
    );

    let on_bc = Expr::Cmp {
        left: Box::new(Expr::Column("b.k".into())),
        op: CmpOp::Eq,
        right: Box::new(Expr::Column("c.k".into())),
    };
    let g_abc = memo.insert_logical(
        LogicalOp::Join { left: g_ab, right: gc, on: Some(on_bc), kind: JoinKind::Inner },
        &ctx,
    );

    // Apply associativity rule: (A ⋈ B) ⋈ C === A ⋈ (B ⋈ C)
    memo.apply_transformation_rules(g_abc, &ctx, 2);

    let has_assoc = memo.group(g_abc).logical_exprs.iter().any(|op| match op {
        LogicalOp::Join { left, right, kind: JoinKind::Inner, .. } => {
            *left == ga && memo.group(*right).props.tables == vec![1, 2]
        }
        _ => false,
    });
    assert!(has_assoc, "Associative join A ⋈ (B ⋈ C) should be derived");

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn memo_predicate_pushdown() {
    let dir = test_dir("pushdown");
    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();
    db.execute(&mut s, "CREATE TABLE a (id INT PRIMARY KEY, val INT)").unwrap();
    db.execute(&mut s, "CREATE TABLE b (id INT PRIMARY KEY, ref_id INT, score INT)").unwrap();

    let ta = db.table(&s, "a").unwrap();
    let tb = db.table(&s, "b").unwrap();

    let ctx = OptimizerContext::new(vec![
        TableMeta { name: "a".into(), total_rows: 1000.0, table: ta },
        TableMeta { name: "b".into(), total_rows: 1000.0, table: tb },
    ]);

    let mut memo = Memo::new();
    let ga = memo.insert_logical(LogicalOp::Scan { table_idx: 0, table_name: "a".into() }, &ctx);
    let gb = memo.insert_logical(LogicalOp::Scan { table_idx: 1, table_name: "b".into() }, &ctx);

    let on_join = Expr::Cmp {
        left: Box::new(Expr::Column("a.id".into())),
        op: CmpOp::Eq,
        right: Box::new(Expr::Column("b.ref_id".into())),
    };
    let g_join = memo.insert_logical(
        LogicalOp::Join { left: ga, right: gb, on: Some(on_join), kind: JoinKind::Inner },
        &ctx,
    );

    // Filter: a.val > 10 AND b.score < 50
    let pred = Expr::And(
        Box::new(Expr::Cmp {
            left: Box::new(Expr::Column("a.val".into())),
            op: CmpOp::Gt,
            right: Box::new(Expr::Literal(Datum::Int(10))),
        }),
        Box::new(Expr::Cmp {
            left: Box::new(Expr::Column("b.score".into())),
            op: CmpOp::Lt,
            right: Box::new(Expr::Literal(Datum::Int(50))),
        }),
    );

    let g_filter = memo.insert_logical(LogicalOp::Filter { input: g_join, predicate: pred }, &ctx);

    // Apply transformation rules: Filter(A ⋈ B) === Filter(A) ⋈ Filter(B)
    memo.apply_transformation_rules(g_filter, &ctx, 2);

    let has_pushed = memo.group(g_filter).logical_exprs.iter().any(|op| match op {
        LogicalOp::Join { left, right, kind: JoinKind::Inner, .. } => {
            memo.group(*left).props.tables == vec![0] && memo.group(*right).props.tables == vec![1]
        }
        _ => false,
    });
    assert!(has_pushed, "Predicate pushdown should generate Filter(A) ⋈ Filter(B)");

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn memo_hash_join_build_side_selection() {
    let dir = test_dir("build_side");
    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();
    db.execute(&mut s, "CREATE TABLE big (id INT PRIMARY KEY, g INT)").unwrap();
    db.execute(&mut s, "CREATE TABLE small (id INT PRIMARY KEY, g INT)").unwrap();

    let t_big = db.table(&s, "big").unwrap();
    let t_small = db.table(&s, "small").unwrap();

    let ctx = OptimizerContext::new(vec![
        TableMeta { name: "big".into(), total_rows: 10000.0, table: t_big },
        TableMeta { name: "small".into(), total_rows: 10.0, table: t_small },
    ]);

    let mut memo = Memo::new();
    let g_big = memo.insert_logical(LogicalOp::Scan { table_idx: 0, table_name: "big".into() }, &ctx);
    let g_small = memo.insert_logical(LogicalOp::Scan { table_idx: 1, table_name: "small".into() }, &ctx);

    let on = Expr::Cmp {
        left: Box::new(Expr::Column("big.g".into())),
        op: CmpOp::Eq,
        right: Box::new(Expr::Column("small.g".into())),
    };

    let g_join = memo.insert_logical(
        LogicalOp::Join { left: g_big, right: g_small, on: Some(on), kind: JoinKind::Inner },
        &ctx,
    );

    // Apply implementation rules to lower to physical operators
    memo.apply_implementation_rules(g_big, &ctx);
    memo.apply_implementation_rules(g_small, &ctx);
    memo.apply_implementation_rules(g_join, &ctx);

    // Find HashJoin candidate for g_join
    let hash_join_cand = memo.group(g_join).physical_exprs.iter().find(|m| {
        matches!(m.op, PhysicalOp::HashJoin { .. })
    });
    assert!(hash_join_cand.is_some(), "HashJoin candidate must be generated");

    if let PhysicalOp::HashJoin { build_side, .. } = hash_join_cand.unwrap().op {
        // Since right (small, 10 rows) < left (big, 10000 rows), build_side should be Right!
        assert_eq!(build_side, JoinBuildSide::Right, "Smaller table should be selected as build side");
    }

    // Optimize group and extract best plan
    memo.optimize_group(g_join, f64::INFINITY, 5, &ctx);
    let best = memo.extract_best_plan(g_join, &ctx).expect("Best plan must exist");

    match best {
        crate::db::memo::PhysicalPlan::HashJoin { build_side, .. } => {
            assert_eq!(build_side, JoinBuildSide::Right);
        }
        other => panic!("Expected HashJoin best plan, got {other:?}"),
    }

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn sql_explain_memo_integration() {
    let dir = test_dir("sql_explain_memo");
    let db = Database::open(&dir).unwrap();
    let mut s = db.new_session();
    db.execute(&mut s, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)").unwrap();
    db.execute(&mut s, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount FLOAT)").unwrap();
    db.execute(&mut s, "CREATE INDEX idx_user_id ON orders (user_id)").unwrap();

    for i in 1..=50 {
        db.execute(&mut s, &format!("INSERT INTO users VALUES ({i}, 'user{i}', {})", 20 + i % 30)).unwrap();
    }
    for i in 1..=200 {
        db.execute(&mut s, &format!("INSERT INTO orders VALUES ({i}, {}, {})", 1 + i % 50, 10.5 * (i as f64))).unwrap();
    }

    db.execute(&mut s, "ANALYZE TABLE users").unwrap();
    db.execute(&mut s, "ANALYZE TABLE orders").unwrap();

    // 1. Single table point seek explain memo
    let out = db.execute(&mut s, "EXPLAIN MEMO SELECT * FROM users WHERE id = 10").unwrap();
    assert_eq!(
        out.columns,
        vec!["group", "operator", "cost", "est_rows", "detail"]
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>()
    );
    assert!(!out.rows.is_empty());
    assert!(out.message.starts_with("OK (memo explored"));

    let has_index_scan = out.rows.iter().any(|r| {
        if let Datum::Text(op) = &r[1] {
            op.contains("IndexScan") || op.contains("PK POINT")
        } else {
            false
        }
    });
    assert!(has_index_scan, "PK point lookup should be chosen as IndexScan");

    // 2. Join query explain memo
    let out = db.execute(
        &mut s,
        "EXPLAIN MEMO SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id WHERE users.id = 5",
    ).unwrap();
    assert!(!out.rows.is_empty());
    assert!(out.message.starts_with("OK (memo explored"));

    // 3. Aggregate query explain memo
    let out = db.execute(
        &mut s,
        "EXPLAIN MEMO SELECT age, COUNT(*) FROM users GROUP BY age",
    ).unwrap();
    assert!(!out.rows.is_empty());
    let has_agg = out.rows.iter().any(|r| {
        if let Datum::Text(op) = &r[1] {
            op.contains("Aggregate")
        } else {
            false
        }
    });
    assert!(has_agg, "Aggregate operator should be in explain memo output");

    let _ = fs::remove_dir_all(&dir);
}
