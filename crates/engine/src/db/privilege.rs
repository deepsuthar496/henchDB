//! RBAC privilege engine: principals, grants, enforcement, user management.
//!
//! Model (MySQL-shaped, single-node v1):
//! - Principals are usernames (the `@host` part is normalized away at
//!   parse; permissions are per username, like `skip-name-resolve`).
//! - `GrantRule { priv_, db, tbl }`: `db`/`tbl` hold `"*"` wildcards or
//!   exact names. A bare-`tbl` scope in GRANT resolves to the grantor's
//!   session database immediately, so `SHOW GRANTS` always prints concrete
//!   scopes. Matching at check time: exact `(db, tbl)`, then `(db, *)`,
//!   then `(*, *)`; `ALL` implies every privilege.
//! - `root` bypasses every check in code, cannot be dropped, and cannot be
//!   restricted (REVOKE against root is rejected) — lockout is impossible.
//! - Passwords NEVER live here and never touch disk through this module:
//!   CREATE/ALTER USER stages the plaintext in a memory-only pending map;
//!   the server hashes it into `auth.bin` in the post-execute persist hook
//!   and drains the map. Grant-only state is what import/export carries.
//! - Enforcement denies before execution, so denied users cannot probe
//!   table existence (no `TableNotFound` oracle).
//! - System catalogs (`pg_catalog`, `information_schema`) are metadata and
//!   stay world-readable (like MySQL's `information_schema`); derived
//!   tables need no separate check (their inner sources are checked).
//!
//! v1 boundaries (clean errors, documented): no `WITH GRANT OPTION` (only
//! root and global-ALL holders may GRANT); partial REVOKE never decomposes
//! an ALL grant (revoke ALL explicitly); FK-cascade writes are not
//! re-checked against the parent tables; case-sensitive names.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

use super::{Database, Output, Session};
use crate::error::{Error, Result};
use crate::sql::{GrantScope, Privilege, SelectItem, SelectStmt, Statement, TableRef};
use crate::types::Datum;

/// The superuser name: bypasses all checks, cannot be dropped or revoked.
/// (MySQL-compatible convention; also the default session user and the
/// bootstrap account.)
pub(crate) const SUPERUSER: &str = "root";

/// One grant rule: `priv_` on `db.tbl`, either side `"*"` for wildcard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrantRule {
    pub priv_: Privilege,
    pub db: String,
    pub tbl: String,
}

/// In-memory privilege state (see module doc). Guarded by one `RwLock` on
/// `Database`: enforcement takes the read side per statement (comparable
/// to the existing catalog locks); mutations take the write side.
#[derive(Debug, Default)]
pub(crate) struct PrivilegeStore {
    users: HashSet<String>,
    grants: HashMap<String, Vec<GrantRule>>,
    /// Staged plaintext passwords (memory-only, never persisted by this
    /// crate): user -> password. Drained by the server persist hook.
    pending: HashMap<String, String>,
    /// Dropped users awaiting file prune by the server persist hook.
    tombstones: HashSet<String>,
    version: AtomicU64,
}

impl Database {
    /// Monotonic privilege mutation counter. The server snapshots it around
    /// `execute` and persists `auth.bin` when it moves — no `Output`
    /// plumbing needed and no execute site can silently skip persistence.
    pub fn privilege_version(&self) -> u64 {
        self.privs.read().unwrap().version.load(Ordering::Relaxed)
    }

    /// Admin check for server-side gates (COM_SHUTDOWN): root bypasses,
    /// otherwise a global ALL grant is required.
    pub fn is_admin(&self, user: &str) -> bool {
        if user == SUPERUSER {
            return true;
        }
        let store = self.privs.read().unwrap();
        is_admin(&store, user)
    }

    /// Replace principals + grants wholesale (server boot import from
    /// `auth.bin`; clears pending passwords and tombstones).
    pub fn import_privileges(&self, users: &[(String, Vec<GrantRule>)]) {
        let mut store = self.privs.write().unwrap();
        store.users.clear();
        store.grants.clear();
        store.pending.clear();
        store.tombstones.clear();
        for (name, rules) in users {
            store.users.insert(name.clone());
            store.grants.insert(name.clone(), rules.clone());
        }
        store.version.fetch_add(1, Ordering::Relaxed);
    }

    /// Add grantless principals for accounts that exist in the server
    /// credential file but not in the engine store (e.g. created with
    /// `server passwd` while serving). No version bump: nothing
    /// SQL-visible changed, and this runs inside the persist hook.
    pub fn import_missing_users(&self, names: &[String]) {
        let mut store = self.privs.write().unwrap();
        for n in names {
            store.users.insert(n.clone());
        }
    }

    /// Export `(grants by user (sorted), dropped-user tombstones, staged
    /// passwords)`, draining tombstones and pending passwords. The server
    /// persist hook calls this after a version move.
    pub fn export_privileges(
        &self,
    ) -> (
        Vec<(String, Vec<GrantRule>)>,
        Vec<String>,
        Vec<(String, String)>,
    ) {
        let mut store = self.privs.write().unwrap();
        let mut users: Vec<(String, Vec<GrantRule>)> = store
            .users
            .iter()
            .map(|u| (u.clone(), store.grants.get(u).cloned().unwrap_or_default()))
            .collect();
        users.sort_by(|a, b| a.0.cmp(&b.0));
        let tombstones: Vec<String> = std::mem::take(&mut store.tombstones).into_iter().collect();
        let pending: Vec<(String, String)> =
            std::mem::take(&mut store.pending).into_iter().collect();
        (users, tombstones, pending)
    }
}

fn denied(user: &str, command: &str, object: &str) -> Error {
    Error::AccessDenied {
        user: user.into(),
        command: command.into(),
        object: object.into(),
    }
}

/// Does `user` hold `priv_` on `(db, tbl)` (exact, then `db.*`, then `*.*`;
/// stored ALL implies everything)?
fn has_priv(store: &PrivilegeStore, user: &str, priv_: Privilege, db: &str, tbl: &str) -> bool {
    let Some(rules) = store.grants.get(user) else {
        return false;
    };
    rules.iter().any(|r| {
        (r.priv_ == priv_ || r.priv_ == Privilege::All)
            && (r.db == "*" || r.db == db)
            && (r.tbl == "*" || r.tbl == tbl)
    })
}

/// Root or a global-ALL holder: the only identities that may manage users
/// and grants, promote, back up, checkpoint, or shut down.
fn is_admin(store: &PrivilegeStore, user: &str) -> bool {
    user == SUPERUSER || has_priv(store, user, Privilege::All, "*", "*")
}

/// Resolve a table reference to a concrete `(db, tbl)` pair using the same
/// routing the executor will open (so checks never disagree with opens).
fn resolve_scope(db: &Database, session: &Session, table: &str) -> (String, String) {
    let key = db.resolve_table_key(session, table);
    match key.split_once('.') {
        Some((d, t)) => (d.to_string(), t.to_string()),
        None => (String::new(), key),
    }
}

/// Require `priv_` on one table (root bypasses; unknown users and missing
/// grants deny). Shared by the statement gate and the fast-path probes.
pub(crate) fn check_table(
    db: &Database,
    session: &Session,
    table: &str,
    priv_: Privilege,
) -> Result<()> {
    if session.user == SUPERUSER {
        return Ok(());
    }
    let (d, t) = resolve_scope(db, session, table);
    let store = db.privs.read().unwrap();
    if has_priv(&store, &session.user, priv_, &d, &t) {
        Ok(())
    } else {
        Err(denied(&session.user, priv_.name(), &format!("{d}.{t}")))
    }
}

/// System metadata schemas stay world-readable (never gated).
fn is_system_schema(table: &str) -> bool {
    match table.split('.').next() {
        Some(s) => {
            let l = s.to_ascii_lowercase();
            l == "pg_catalog" || l == "information_schema"
        }
        None => false,
    }
}

/// Collect every base-table reference a SELECT touches: FROM/JOIN sources
/// (recursing into derived tables) plus subquery bodies in projections,
/// filters, and ON clauses.
fn select_tables(
    from: &TableRef,
    joins: &[crate::sql::JoinClause],
    selection: &Option<crate::sql::Expr>,
    items: &[SelectItem],
    out: &mut Vec<String>,
) {
    fn push_ref(r: &TableRef, out: &mut Vec<String>) {
        match r {
            TableRef::Table(n) => {
                if !is_system_schema(n) {
                    out.push(n.clone());
                }
            }
            TableRef::Derived { query, .. } => stmt_tables(query, out),
            TableRef::Empty => {}
        }
    }
    push_ref(from, out);
    for j in joins {
        push_ref(&j.table, out);
        expr_tables(&j.on, out);
    }
    if let Some(s) = selection {
        expr_tables(s, out);
    }
    for item in items {
        if let SelectItem::Subquery { query, .. } = item {
            stmt_tables(query, out);
        }
    }
}

fn stmt_tables(q: &SelectStmt, out: &mut Vec<String>) {
    select_tables(&q.from, &q.joins, &q.selection, &q.items, out);
}

fn expr_tables(e: &crate::sql::Expr, out: &mut Vec<String>) {
    use crate::sql::Expr;
    match e {
        Expr::Column(_) | Expr::Literal(_) => {}
        Expr::Cmp { left, right, .. } => {
            expr_tables(left, out);
            expr_tables(right, out);
        }
        Expr::And(a, b) | Expr::Or(a, b) => {
            expr_tables(a, out);
            expr_tables(b, out);
        }
        Expr::Not(x) => expr_tables(x, out),
        Expr::In { expr, .. } | Expr::Between { expr, .. } | Expr::Like { expr, .. } => {
            expr_tables(expr, out)
        }
        Expr::InSubquery { expr, query, .. } => {
            expr_tables(expr, out);
            stmt_tables(query, out);
        }
        Expr::ScalarSubquery(q) => stmt_tables(q, out),
        Expr::Exists { query, .. } => stmt_tables(query, out),
    }
}

/// Statement gate: call at the top of `execute_stmt` (fast paths probe via
/// `check_table`). Denies precede execution, so failures never leak table
/// existence. Unknown (never-created) users hold no grants and deny.
pub(crate) fn enforce(db: &Database, session: &Session, stmt: &Statement) -> Result<()> {
    // Cheap admin pre-check only where needed below; the root bypass lives
    // in `check_table` and the admin arms.
    match stmt {
        Statement::Select { items, from, joins, selection, .. } => {
            let mut tables = Vec::new();
            select_tables(from, joins, selection, items, &mut tables);
            for t in tables {
                check_table(db, session, &t, Privilege::Select)?;
            }
            Ok(())
        }
        Statement::Insert { table, .. } => check_table(db, session, table, Privilege::Insert),
        Statement::Update { table, selection, assignments } => {
            check_table(db, session, table, Privilege::Update)?;
            // Subquery sources in SET/WHERE read other tables (P9).
            let mut tables = Vec::new();
            if let Some(s) = selection {
                expr_tables(s, &mut tables);
            }
            for (_, v) in assignments {
                expr_tables(v, &mut tables);
            }
            for t in tables {
                check_table(db, session, &t, Privilege::Select)?;
            }
            Ok(())
        }
        Statement::Delete { table, selection } => {
            check_table(db, session, table, Privilege::Delete)?;
            let mut tables = Vec::new();
            if let Some(s) = selection {
                expr_tables(s, &mut tables);
            }
            for t in tables {
                check_table(db, session, &t, Privilege::Select)?;
            }
            Ok(())
        }
        Statement::CreateTable { name, .. } => require_db_or_table(db, session, name, Privilege::Create),
        Statement::DropTable { name } => require_db_or_table(db, session, name, Privilege::Drop),
        Statement::CreateIndex { table, .. } => {
            require_db_or_table(db, session, table, Privilege::Create)
        }
        Statement::DropIndex { table, .. } => require_db_or_table(db, session, table, Privilege::Drop),
        Statement::CreateDatabase { .. } => require_global(db, session, Privilege::Create, "CREATE DATABASE"),
        Statement::DropDatabase { .. } => require_global(db, session, Privilege::Drop, "DROP DATABASE"),
        Statement::CreateUser { .. }
        | Statement::DropUser { .. }
        | Statement::AlterUser { .. }
        | Statement::Grant { .. }
        | Statement::Revoke { .. }
        | Statement::Promote
        | Statement::Backup { .. }
        | Statement::Checkpoint => require_admin(db, session, stmt_admin_command(stmt)),
        Statement::ShowGrants { for_user } => {
            if session.user == SUPERUSER {
                return Ok(());
            }
            match for_user {
                None => Ok(()),
                Some(u) if u == &session.user => Ok(()),
                Some(u) => {
                    let store = db.privs.read().unwrap();
                    if is_admin(&store, &session.user) {
                        Ok(())
                    } else {
                        Err(denied(&session.user, "SHOW GRANTS", u))
                    }
                }
            }
        }
        Statement::AnalyzeTable { table } | Statement::CheckTable { table } => {
            check_table(db, session, table, Privilege::Select)
        }
        Statement::CheckDatabase { database } => {
            if session.user == SUPERUSER {
                Ok(())
            } else {
                let db_name = database.as_deref().unwrap_or(&session.current_db);
                let store = db.privs.read().unwrap();
                if has_priv(&store, &session.user, Privilege::Select, "*", "*")
                    || has_priv(&store, &session.user, Privilege::Select, db_name, "*")
                {
                    Ok(())
                } else {
                    Err(denied(&session.user, "SELECT", &format!("{db_name}.*")))
                }
            }
        }
        Statement::Explain { statement, .. } | Statement::ExplainMemo { statement } => enforce(db, session, statement),
        // Transaction framing, session state, read-only diagnostics, and
        // timeouts need no privilege.
        _ => Ok(()),
    }
}

/// CREATE/DROP TABLE/INDEX: the privilege on the database or the table
/// itself (either suffices, mirroring MySQL's db-or-table CREATE/DROP).
fn require_db_or_table(
    db: &Database,
    session: &Session,
    table: &str,
    priv_: Privilege,
) -> Result<()> {
    if session.user == SUPERUSER {
        return Ok(());
    }
    let (d, t) = resolve_scope(db, session, table);
    let store = db.privs.read().unwrap();
    if has_priv(&store, &session.user, priv_, &d, "*")
        || has_priv(&store, &session.user, priv_, &d, &t)
    {
        Ok(())
    } else {
        Err(denied(&session.user, priv_.name(), &format!("{d}.{t}")))
    }
}

/// Database-level DDL needs the privilege globally (MySQL rule).
fn require_global(db: &Database, session: &Session, priv_: Privilege, command: &str) -> Result<()> {
    if session.user == SUPERUSER {
        return Ok(());
    }
    let store = db.privs.read().unwrap();
    if has_priv(&store, &session.user, priv_, "*", "*") {
        Ok(())
    } else {
        Err(denied(&session.user, command, "*.*"))
    }
}

fn stmt_admin_command(stmt: &Statement) -> &'static str {
    match stmt {
        Statement::CreateUser { .. } => "CREATE USER",
        Statement::DropUser { .. } => "DROP USER",
        Statement::AlterUser { .. } => "ALTER USER",
        Statement::Grant { .. } => "GRANT",
        Statement::Revoke { .. } => "REVOKE",
        Statement::Promote => "PROMOTE",
        Statement::Backup { .. } => "BACKUP",
        Statement::Checkpoint => "CHECKPOINT",
        _ => "ADMIN",
    }
}

fn require_admin(db: &Database, session: &Session, command: &str) -> Result<()> {
    if session.user == SUPERUSER {
        return Ok(());
    }
    let store = db.privs.read().unwrap();
    if is_admin(&store, &session.user) {
        Ok(())
    } else {
        Err(denied(&session.user, command, "*.*"))
    }
}

// ---------------------------------------------------------------------------
// User-management execution (runs inside `execute_stmt` via dispatch)
// ---------------------------------------------------------------------------

/// Execute the six user-management statements (admin gate already passed
/// in `enforce`, except self-`SHOW GRANTS` which needs no gate).
pub(crate) fn exec_user_mgmt(
    db: &Database,
    session: &mut Session,
    stmt: Statement,
) -> Result<Output> {
    match stmt {
        Statement::CreateUser { name, if_not_exists, password } => {
            if name == SUPERUSER {
                return Err(Error::InvalidQuery("user 'root' already exists".into()));
            }
            let mut store = db.privs.write().unwrap();
            if store.users.contains(&name) {
                if if_not_exists {
                    return Ok(Output::ok("user exists, skipped"));
                }
                return Err(Error::InvalidQuery(format!("user '{name}' already exists")));
            }
            store.users.insert(name.clone());
            store.pending.insert(name.clone(), password);
            store.tombstones.remove(&name);
            store.version.fetch_add(1, Ordering::Relaxed);
            Ok(Output::ok(format!("user '{name}' created")))
        }
        Statement::DropUser { name, if_exists } => {
            if name == SUPERUSER {
                return Err(Error::InvalidQuery("cannot drop user 'root'".into()));
            }
            let mut store = db.privs.write().unwrap();
            if !store.users.contains(&name) {
                if if_exists {
                    return Ok(Output::ok("no such user, skipped"));
                }
                return Err(Error::InvalidQuery(format!("no such user '{name}'")));
            }
            store.users.remove(&name);
            store.grants.remove(&name);
            store.pending.remove(&name);
            store.tombstones.insert(name.clone());
            store.version.fetch_add(1, Ordering::Relaxed);
            Ok(Output::ok(format!("user '{name}' dropped")))
        }
        Statement::AlterUser { name, password } => {
            let mut store = db.privs.write().unwrap();
            if !store.users.contains(&name) {
                return Err(Error::InvalidQuery(format!("no such user '{name}'")));
            }
            store.pending.insert(name.clone(), password);
            store.version.fetch_add(1, Ordering::Relaxed);
            Ok(Output::ok(format!("user '{name}' altered")))
        }
        Statement::Grant { privs, scope, user } => {
            let (d, t) = concrete_scope(session, &scope);
            let mut store = db.privs.write().unwrap();
            if !store.users.contains(&user) {
                return Err(Error::InvalidQuery(format!("no such user '{user}'")));
            }
            if user == SUPERUSER {
                return Err(Error::InvalidQuery("cannot grant to 'root'".into()));
            }
            let rules = store.grants.entry(user.clone()).or_default();
            let mut changed = false;
            for p in privs {
                let rule = GrantRule { priv_: p, db: d.clone(), tbl: t.clone() };
                if !rules.contains(&rule) {
                    rules.push(rule);
                    changed = true;
                }
            }
            if changed {
                store.version.fetch_add(1, Ordering::Relaxed);
            }
            Ok(Output::ok(format!("grants updated for '{user}'")))
        }
        Statement::Revoke { privs, scope, user } => {
            let (d, t) = concrete_scope(session, &scope);
            let mut store = db.privs.write().unwrap();
            if !store.users.contains(&user) {
                return Err(Error::InvalidQuery(format!("no such user '{user}'")));
            }
            if user == SUPERUSER {
                return Err(Error::InvalidQuery("cannot revoke from 'root'".into()));
            }
            let mut removed = false;
            if let Some(rules) = store.grants.get_mut(&user) {
                let before = rules.len();
                rules.retain(|r| {
                    !(privs.contains(&r.priv_) && (r.db == d || d == "*") && (r.tbl == t || t == "*"))
                });
                removed = rules.len() != before;
            }
            if !removed {
                return Err(Error::InvalidQuery(format!("no such grant for '{user}'")));
            }
            store.version.fetch_add(1, Ordering::Relaxed);
            Ok(Output::ok(format!("grants updated for '{user}'")))
        }
        Statement::ShowGrants { for_user } => {
            let target = for_user.unwrap_or_else(|| session.user.clone());
            let store = db.privs.read().unwrap();
            if !store.users.contains(&target) && target != SUPERUSER {
                return Err(Error::InvalidQuery(format!("no such user '{target}'")));
            }
            let mut rows: Vec<Vec<Datum>> = store
                .grants
                .get(&target)
                .map(|rules| {
                    let mut rs: Vec<(String, Vec<Datum>)> = rules
                        .iter()
                        .map(|r| {
                            let scope = if r.db == "*" && r.tbl == "*" {
                                "*.*".to_string()
                            } else if r.tbl == "*" {
                                format!("{}.{}", r.db, r.tbl)
                            } else {
                                format!("{}.{}", r.db, r.tbl)
                            };
                            (
                                scope.clone(),
                                vec![Datum::Text(format!(
                                    "GRANT {} ON {} TO '{}'@'%'",
                                    r.priv_.name(),
                                    scope,
                                    target
                                ))],
                            )
                        })
                        .collect();
                    rs.sort_by(|a, b| a.0.cmp(&b.0));
                    rs.into_iter().map(|(_, row)| row).collect()
                })
                .unwrap_or_default();
            if target == SUPERUSER {
                rows.insert(
                    0,
                    vec![Datum::Text(format!("GRANT ALL PRIVILEGES ON *.* TO '{target}'@'%'"))],
                );
            }
            Ok(Output {
                columns: vec!["Grants".into()],
                rows,
                message: "OK".into(),
            })
        }
        _ => Err(Error::NotSupported("not a user-management statement".into())),
    }
}

/// Resolve a GRANT/REVOKE scope to a concrete stored `(db, tbl)` pair
/// (bare tables bind the grantor's session database now, so stored rules
/// and `SHOW GRANTS` are always explicit).
fn concrete_scope(session: &Session, scope: &GrantScope) -> (String, String) {
    match scope {
        GrantScope::Global => ("*".into(), "*".into()),
        GrantScope::Database { db } => (db.clone(), "*".into()),
        GrantScope::Table { db: Some(db), tbl } => (db.clone(), tbl.clone()),
        GrantScope::Table { db: None, tbl } => (session.current_db.clone(), tbl.clone()),
    }
}
