//! Foreign key tests: RESTRICT, CASCADE, SET NULL, auto-indexing, recovery.

use super::*;

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
