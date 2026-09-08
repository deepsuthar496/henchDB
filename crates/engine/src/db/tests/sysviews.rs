//! Priority 13: system functions + virtual `pg_catalog` /
//! `information_schema` views (ORM introspection compatibility).

use super::*;

fn setup(dir: &Path) -> Database {
    let db = Database::open(dir).unwrap();
    let mut s = db.new_session();
    db.execute(
        &mut s,
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, score FLOAT)",
    )
    .unwrap();
    db.execute(&mut s, "INSERT INTO users VALUES (1, 'ann', 9.5), (2, 'bob', 4.0)")
        .unwrap();
    db
}

fn tmp(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("hdbsys_{name}_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    dir
}

#[test]
fn sysfunc_values() {
    let dir = tmp("func");
    let db = setup(&dir);
    let mut s = db.new_session();
    let out = db.execute(&mut s, "SELECT version()").unwrap();
    assert_eq!(out.columns, vec!["version".to_string()]);
    assert_eq!(out.rows.len(), 1);
    match &out.rows[0][0] {
        Datum::Text(v) => {
            assert!(v.starts_with("PostgreSQL 18.0 ("), "unexpected: {v}");
            assert!(v.contains(crate::PRODUCT_NAME), "unexpected: {v}");
        }
        other => panic!("wrong value {other:?}"),
    }
    let out = db.execute(&mut s, "SELECT current_schema()").unwrap();
    assert_eq!(out.rows[0][0], Datum::Text("public".into()));
    let out = db.execute(&mut s, "SELECT current_database()").unwrap();
    assert_eq!(out.rows[0][0], Datum::Text("default".into()));
    let out = db.execute(&mut s, "SELECT user()").unwrap();
    assert_eq!(out.rows[0][0], Datum::Text("root".into()));
    // Alias + cast suffix + multi-item FROM-less row.
    let out = db.execute(&mut s, "SELECT version() AS v").unwrap();
    assert_eq!(out.columns, vec!["v".to_string()]);
    let out = db.execute(&mut s, "SELECT version()::text").unwrap();
    assert_eq!(out.columns, vec!["version".to_string()]);
    let out = db.execute(&mut s, "SELECT 1, current_schema()").unwrap();
    assert_eq!(out.rows[0], vec![Datum::Int(1), Datum::Text("public".into())]);
    // Unknown functions fail cleanly (never a panic).
    assert!(db.execute(&mut s, "SELECT nosuchfunc()").is_err());
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn current_database_follows_use() {
    let dir = tmp("usedb");
    let db = setup(&dir);
    let mut s = db.new_session();
    db.execute(&mut s, "CREATE DATABASE shop").unwrap();
    db.execute(&mut s, "USE shop").unwrap();
    let out = db.execute(&mut s, "SELECT current_database()").unwrap();
    assert_eq!(out.rows[0][0], Datum::Text("shop".into()));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn pg_namespace_and_type() {
    let dir = tmp("ns");
    let db = setup(&dir);
    let mut s = db.new_session();
    let out = db.execute(&mut s, "SELECT oid, nspname FROM pg_catalog.pg_namespace ORDER BY oid").unwrap();
    assert_eq!(out.columns, vec!["oid".to_string(), "nspname".to_string()]);
    let names: Vec<String> = out.rows.iter().map(|r| r[1].to_string()).collect();
    assert_eq!(names, vec!["public", "pg_catalog", "information_schema"]);
    let out = db.execute(&mut s, "SELECT oid, typname FROM pg_catalog.pg_type").unwrap();
    assert_eq!(out.rows.len(), 8);
    let oids: Vec<i64> = out
        .rows
        .iter()
        .map(|r| match r[0] {
            Datum::Int(i) => i,
            ref o => panic!("wrong oid {o:?}"),
        })
        .collect();
    for want in [16, 20, 23, 25, 700, 701, 1114, 1184] {
        assert!(oids.contains(&want), "missing type {want}");
    }
    let out = db
        .execute(&mut s, "SELECT typname FROM pg_catalog.pg_type WHERE oid = 25")
        .unwrap();
    assert_eq!(out.rows[0][0], Datum::Text("text".into()));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn pg_class_lists_live_tables() {
    let dir = tmp("class");
    let db = setup(&dir);
    let mut s = db.new_session();
    let out = db
        .execute(&mut s, "SELECT relname, relkind, relnamespace, reltuples FROM pg_catalog.pg_class")
        .unwrap();
    assert_eq!(
        out.columns,
        vec!["relname", "relkind", "relnamespace", "reltuples"]
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>()
    );
    let row = out.rows.iter().find(|r| r[0] == Datum::Text("users".into())).unwrap();
    assert_eq!(row[1], Datum::Text("r".into()));
    assert_eq!(row[2], Datum::Int(11));
    assert_eq!(row[3], Datum::Int(2));
    // Filtering, ordering, limits, star, and count all flow through.
    let out = db
        .execute(&mut s, "SELECT relname FROM pg_catalog.pg_class WHERE relname = 'users'")
        .unwrap();
    assert_eq!(out.rows.len(), 1);
    let out = db
        .execute(&mut s, "SELECT relname FROM pg_catalog.pg_class ORDER BY relname DESC LIMIT 1")
        .unwrap();
    assert_eq!(out.rows.len(), 1);
    let out = db.execute(&mut s, "SELECT COUNT(*) FROM pg_catalog.pg_class").unwrap();
    assert!(matches!(out.rows[0][0], Datum::Int(n) if n >= 1));
    let out = db.execute(&mut s, "SELECT * FROM pg_catalog.pg_class").unwrap();
    assert_eq!(out.columns.len(), 5);
    // Short qualifiers resolve (`pg_class.relname` over `pg_catalog.pg_class`).
    let out = db
        .execute(
            &mut s,
            "SELECT pg_class.relname FROM pg_catalog.pg_class WHERE pg_class.relname = 'users'",
        )
        .unwrap();
    assert_eq!(out.rows.len(), 1);
    assert_eq!(out.rows[0][0], Datum::Text("users".into()));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn pg_attribute_mirrors_columns() {
    let dir = tmp("attr");
    let db = setup(&dir);
    let mut s = db.new_session();
    let out = db
        .execute(
            &mut s,
            "SELECT attname, atttypid, attnum, attnotnull FROM pg_catalog.pg_attribute ORDER BY attnum",
        )
        .unwrap();
    // `users` is the only table: id INT NOT NULL, name TEXT NOT NULL,
    // score FLOAT nullable.
    assert_eq!(out.rows.len(), 3);
    assert_eq!(out.rows[0][1], Datum::Int(23));
    assert_eq!(out.rows[0][2], Datum::Int(1));
    assert_eq!(out.rows[0][3], Datum::Bool(true));
    assert_eq!(out.rows[1], vec![
        Datum::Text("name".into()),
        Datum::Int(25),
        Datum::Int(2),
        Datum::Bool(true)
    ]);
    assert_eq!(out.rows[2][3], Datum::Bool(false));
    // Join across the catalog like real introspection queries do (no
    // table aliases in this dialect: qualify with the short view name).
    let out = db
        .execute(
            &mut s,
            "SELECT pg_attribute.attname FROM pg_catalog.pg_class JOIN pg_catalog.pg_attribute ON pg_class.oid = pg_attribute.attrelid WHERE pg_class.relname = 'users' ORDER BY pg_attribute.attnum",
        )
        .unwrap();
    assert_eq!(out.rows.len(), 3);
    assert_eq!(out.rows[0][0], Datum::Text("id".into()));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn pg_database_and_information_schema() {
    let dir = tmp("is");
    let db = setup(&dir);
    let mut s = db.new_session();
    db.execute(&mut s, "CREATE DATABASE shop").unwrap();
    let out = db.execute(&mut s, "SELECT datname FROM pg_catalog.pg_database").unwrap();
    let mut names: Vec<String> = out.rows.iter().map(|r| r[0].to_string()).collect();
    names.sort();
    assert_eq!(names, vec!["default", "shop"]);
    let out = db.execute(&mut s, "SELECT schema_name FROM information_schema.schemata").unwrap();
    assert_eq!(out.rows.len(), 3);
    let out = db
        .execute(
            &mut s,
            "SELECT table_catalog, table_schema, table_name, table_type FROM information_schema.tables WHERE table_name = 'users'",
        )
        .unwrap();
    assert_eq!(out.rows.len(), 1);
    assert_eq!(out.rows[0][0], Datum::Text("default".into()));
    assert_eq!(out.rows[0][3], Datum::Text("BASE TABLE".into()));
    let out = db
        .execute(
            &mut s,
            "SELECT column_name, ordinal_position, data_type, is_nullable FROM information_schema.columns WHERE table_name = 'users' ORDER BY ordinal_position",
        )
        .unwrap();
    assert_eq!(out.rows.len(), 3);
    assert_eq!(out.rows[0][0], Datum::Text("id".into()));
    assert_eq!(out.rows[0][1], Datum::Int(1));
    assert_eq!(out.rows[0][2], Datum::Text("int".into()));
    assert_eq!(out.rows[0][3], Datum::Text("NO".into()));
    assert_eq!(out.rows[2][3], Datum::Text("YES".into()));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn sysview_errors_and_cross_db_from() {
    let dir = tmp("err");
    let db = setup(&dir);
    let mut s = db.new_session();
    // Unknown relations inside system schemas fail like PG (42P01).
    assert!(db.execute(&mut s, "SELECT * FROM pg_catalog.pg_nope").is_err());
    // System views are read-only: dotted DML never parses to a write.
    assert!(db.execute(&mut s, "INSERT INTO pg_catalog.pg_class VALUES (1)").is_err());
    // Dotted FROM also routes cross-database user tables.
    db.execute(&mut s, "CREATE DATABASE shop").unwrap();
    db.execute(&mut s, "USE shop").unwrap();
    db.execute(&mut s, "CREATE TABLE items (id INT PRIMARY KEY)").unwrap();
    db.execute(&mut s, "USE default").unwrap();
    let out = db.execute(&mut s, "SELECT * FROM shop.items").unwrap();
    assert_eq!(out.columns, vec!["id".to_string()]);
    let out = db
        .execute(&mut s, "SELECT relname FROM pg_catalog.pg_class WHERE relname = 'items'")
        .unwrap();
    assert_eq!(out.rows.len(), 1);
    let _ = fs::remove_dir_all(&dir);
}
