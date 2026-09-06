//! Subqueries & derived tables: evaluation engine.
//!
//! Supported shapes (all through the normal executor, so nesting,
//! transactions, timeouts, and MVCC overlays compose for free):
//! - `WHERE col [NOT] IN (SELECT ...)` (uncorrelated cached as a hash set,
//!   correlated re-evaluated per row),
//! - scalar `WHERE col op (SELECT ...)` and `SELECT col, (SELECT ...)` (0
//!   rows → NULL, >1 row → error),
//! - `WHERE [NOT] EXISTS (SELECT ...)` (planned with LIMIT 1),
//! - `FROM (SELECT ...) AS alias` / `JOIN (SELECT ...) AS alias` (executed
//!   once per query level into ephemeral row-id-keyed tables).
//!
//! Correlation model: inner column references resolve against the inner
//! scope first (shadowing binds innermost, like MySQL); references that
//! escape are resolved against a LIFO stack of outer row frames carried on
//! the session (`SubqueryState::outer`). Uncorrelated nodes are constant-
//! folded once per statement (keyed by their debug shape) and never
//! re-executed. Rewriting never changes results: every folded value equals
//! what row-by-row evaluation would produce.
//!
//! v1 boundaries (clean errors, documented): subqueries in UPDATE/DELETE
//! WHERE, scalar subqueries under GROUP BY, `SET x = (SELECT ...)`, and
//! correlated references inside derived tables of the same level
//! (`LATERAL` is not supported — they fail to resolve, like MySQL without
//! the LATERAL keyword).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::plan::{join_key, JoinKey};
use super::{Database, Output, Session};
use crate::error::{Error, Result};
use crate::sql::{Expr, JoinClause, SelectItem, SelectStmt, TableRef};
use crate::table::{ColumnDef, Schema, Table};
use crate::types::{ColumnType, Datum};

/// Per-statement subquery state, carried on the session (see `Session`).
#[derive(Default)]
pub(crate) struct SubqueryState {
    /// Derived-table materializations visible at the current query level:
    /// alias → ephemeral table. Append-mostly within a statement (scopes
    /// hold resolved `Arc`s, so shadowing overwrites are harmless) and
    /// cleared at the top of `Database::execute`.
    pub eph: HashMap<String, Arc<Table>>,
    /// Correlated row frames, innermost last. Strict LIFO around one inner
    /// execution.
    pub outer: Vec<OuterFrame>,
    /// Uncorrelated fold cache: node debug shape → folded value. Only
    /// provably uncorrelated nodes are stored, so sharing a key across
    /// scopes is always sound.
    pub fold: HashMap<String, Folded>,
}

pub(crate) struct OuterFrame {
    pub tables: Vec<Arc<Table>>,
    pub row: Vec<Datum>,
}

#[derive(Clone)]
pub(crate) enum Folded {
    Bool(bool),
    Datum(Datum),
    /// Membership set + nullish-present flag (NULL/NaN never match, but
    /// their presence flips NOT IN to unknown).
    Set(HashSet<JoinKey>, bool),
}

// ---------------------------------------------------------------------------
// Derived tables
// ---------------------------------------------------------------------------

/// Resolve one FROM/JOIN source: base tables via the catalog, derived
/// aliases via the session materialization map (populated by
/// `setup_derived` before scope building).
pub(crate) fn resolve_table_ref(
    db: &Database,
    session: &Session,
    r: &TableRef,
) -> Result<Arc<Table>> {
    match r {
        TableRef::Table(name) => {
            if let Some(t) = session.subq.eph.get(name) {
                return Ok(t.clone());
            }
            db.table(session, name)
        }
        TableRef::Derived { alias, .. } => session.subq.eph.get(alias).cloned().ok_or_else(|| {
            Error::TableNotFound(format!("{alias} (derived table was not materialized)"))
        }),
    }
}

/// Execute one SELECT statement as a subquery (recursion core: nested
/// derived tables and WHERE-subqueries resolve through the same paths).
pub(crate) fn exec_subselect(
    db: &Database,
    session: &mut Session,
    stmt: &SelectStmt,
) -> Result<Output> {
    db.exec_select_stmt(session, stmt)
}

/// Materialize every derived table of one query level into the session map.
/// Returns saved entries for `teardown_derived` (shadowing-safe).
pub(crate) fn setup_derived(
    db: &Database,
    session: &mut Session,
    from: &TableRef,
    joins: &[JoinClause],
) -> Result<HashMap<String, Option<Arc<Table>>>> {
    let mut saved = HashMap::new();
    let mut refs = vec![from];
    refs.extend(joins.iter().map(|j| &j.table));
    for r in refs {
        if let TableRef::Derived { query, alias } = r {
            let out = match exec_subselect(db, session, query) {
                Ok(out) => out,
                Err(e) => {
                    teardown_derived(session, saved);
                    return Err(e);
                }
            };
            let table = match ephemeral_table(alias, &out) {
                Ok(t) => t,
                Err(e) => {
                    teardown_derived(session, saved);
                    return Err(e);
                }
            };
            saved
                .entry(alias.clone())
                .or_insert_with(|| session.subq.eph.get(alias).cloned());
            session.subq.eph.insert(alias.clone(), table);
        }
    }
    Ok(saved)
}

/// Restore pre-level materializations (see `setup_derived`).
pub(crate) fn teardown_derived(
    session: &mut Session,
    saved: HashMap<String, Option<Arc<Table>>>,
) {
    for (alias, old) in saved {
        match old {
            Some(t) => {
                session.subq.eph.insert(alias, t);
            }
            None => {
                session.subq.eph.remove(&alias);
            }
        }
    }
}

/// Build an ephemeral row-id-keyed table from a subquery output. Column
/// types are inferred from the first non-null value (all-null → Text);
/// names come straight from the projection.
fn ephemeral_table(alias: &str, out: &Output) -> Result<Arc<Table>> {
    let mut ctypes = Vec::with_capacity(out.columns.len());
    for (ci, _) in out.columns.iter().enumerate() {
        let mut ty = ColumnType::Text;
        for row in &out.rows {
            match &row[ci] {
                Datum::Null => {}
                Datum::Int(_) => {
                    ty = ColumnType::Int;
                    break;
                }
                Datum::Float(_) => {
                    ty = ColumnType::Float;
                    break;
                }
                Datum::Text(_) => {
                    ty = ColumnType::Text;
                    break;
                }
                Datum::Bool(_) => {
                    ty = ColumnType::Bool;
                    break;
                }
                Datum::DateTime(_) => {
                    ty = ColumnType::DateTime;
                    break;
                }
            }
        }
        ctypes.push(ty);
    }
    if out.columns.is_empty() {
        return Err(Error::InvalidQuery(
            "derived table must project at least one column".into(),
        ));
    }
    let schema = Schema {
        columns: out
            .columns
            .iter()
            .zip(ctypes)
            .map(|(n, ctype)| ColumnDef {
                name: n.clone(),
                ctype,
                nullable: true,
                auto_increment: false,
                default_value: None,
            })
            .collect(),
        pk_idx: 0,
    };
    let table = Arc::new(Table::new_ephemeral(alias.to_string(), schema));
    for (i, row) in out.rows.iter().enumerate() {
        table.append_ephemeral(i as u64, row)?;
    }
    Ok(table)
}

// ---------------------------------------------------------------------------
// Column collection (deep: descends into subquery bodies)
// ---------------------------------------------------------------------------

/// Every column reference in an expression, including inside nested
/// subqueries and derived tables (owned strings; used for correlation and
/// pushdown-safety analysis).
pub(crate) fn deep_columns(expr: &Expr, out: &mut Vec<String>) {
    match expr {
        Expr::Column(n) => out.push(n.clone()),
        Expr::Literal(_) => {}
        Expr::Cmp { left, right, .. } => {
            deep_columns(left, out);
            deep_columns(right, out);
        }
        Expr::And(a, b) | Expr::Or(a, b) => {
            deep_columns(a, out);
            deep_columns(b, out);
        }
        Expr::Not(e) => deep_columns(e, out),
        Expr::In { expr, .. } | Expr::Between { expr, .. } | Expr::Like { expr, .. } => {
            deep_columns(expr, out)
        }
        Expr::InSubquery { expr, query, .. } => {
            deep_columns(expr, out);
            deep_stmt(query, out);
        }
        Expr::ScalarSubquery(q) => deep_stmt(q, out),
        Expr::Exists { query, .. } => deep_stmt(query, out),
    }
}

/// Every column reference in a SELECT (projection scalars, sources,
/// filters, ordering, grouping).
pub(crate) fn deep_stmt(stmt: &SelectStmt, out: &mut Vec<String>) {
    for item in &stmt.items {
        if let SelectItem::Subquery { query, .. } = item {
            deep_stmt(query, out);
        }
    }
    for r in std::iter::once(&stmt.from).chain(stmt.joins.iter().map(|j| &j.table)) {
        if let TableRef::Derived { query, .. } = r {
            deep_stmt(query, out);
        }
    }
    if let Some(s) = stmt.selection.as_ref() {
        deep_columns(s, out);
    }
    for j in &stmt.joins {
        deep_columns(&j.on, out);
    }
    out.extend(stmt.order_by.iter().map(|(c, _)| c.clone()));
    out.extend(stmt.group_by.iter().cloned());
}

// ---------------------------------------------------------------------------
// Correlation analysis
// ---------------------------------------------------------------------------

/// Inner scope of one subquery level: resolved base tables plus derived
/// aliases (columns of derived tables are unknown without executing, so
/// alias-qualified references count as inner and fail cleanly at execution
/// when misspelled).
struct InnerScope {
    tables: Vec<Arc<Table>>,
    aliases: Vec<String>,
}

fn inner_scope(
    db: &Database,
    session: &Session,
    query: &SelectStmt,
) -> Result<InnerScope> {
    let mut tables = Vec::new();
    let mut aliases = Vec::new();
    for r in std::iter::once(&query.from).chain(query.joins.iter().map(|j| &j.table)) {
        match r {
            TableRef::Table(name) => tables.push(db.table(session, name)?),
            TableRef::Derived { alias, .. } => {
                if let Some(t) = session.subq.eph.get(alias) {
                    tables.push(t.clone());
                }
                aliases.push(alias.clone());
            }
        }
    }
    Ok(InnerScope { tables, aliases })
}

/// True when `name` binds inside the inner scope (shadowing: inner wins).
fn resolves_inner(scope: &InnerScope, name: &str) -> bool {
    match name.split_once('.') {
        Some((t, c)) => {
            if scope.aliases.iter().any(|a| a == t) {
                return true;
            }
            scope.tables.iter().any(|tab| {
                let def = &tab.def.name;
                let simple = def.split('.').last().unwrap_or(def);
                (def == t || simple == t) && tab.schema().index_of(c).is_some()
            })
        }
        None => scope.tables.iter().any(|tab| tab.schema().index_of(name).is_some()),
    }
}

/// References in `query` that escape the inner scope (correlated outer
/// references, or errors when they resolve nowhere).
fn escaping_refs(
    db: &Database,
    session: &Session,
    query: &SelectStmt,
) -> Result<Vec<String>> {
    let scope = inner_scope(db, session, query)?;
    let mut cols = Vec::new();
    if let Some(s) = query.selection.as_ref() {
        deep_columns(s, &mut cols);
    }
    for j in &query.joins {
        deep_columns(&j.on, &mut cols);
    }
    // ORDER BY / GROUP BY are bare column refs; a correlated one makes the
    // level row-dependent exactly like a correlated filter.
    cols.extend(query.order_by.iter().map(|(c, _)| c.clone()));
    cols.extend(query.group_by.iter().cloned());
    // Projection scalars and nested subqueries evaluate per inner row with
    // the same frames pushed, so their escaping refs share this level's
    // verdict; still, a ref resolving nowhere must fail fast here.
    let mut escaping = Vec::new();
    for c in cols {
        if resolves_inner(&scope, &c) {
            continue;
        }
        // Must resolve outward otherwise (checked for real at execution;
        // here we only need the correlated verdict... resolve cheaply).
        let mut found = false;
        for frame in session.subq.outer.iter().rev() {
            match Database::resolve_scope(&frame.tables, &c) {
                Ok(_) => {
                    found = true;
                    break;
                }
                Err(Error::ColumnNotFound(_)) | Err(Error::TableNotFound(_)) => continue,
                Err(_) => break,
            }
        }
        if found {
            escaping.push(c);
        } else {
            return Err(Error::ColumnNotFound(c));
        }
    }
    Ok(escaping)
}

// ---------------------------------------------------------------------------
// Folding evaluator
// ---------------------------------------------------------------------------

/// Resolve `name` against one scope row, falling back outward across the
/// correlation frames (innermost first). Shadowing binds innermost;
/// ambiguity inside one level is an error, never a silent pick.
pub(crate) fn resolve_row(
    scope: &[Arc<Table>],
    row: &[Datum],
    session: &Session,
    name: &str,
) -> Result<Datum> {
    match Database::resolve_scope(scope, name) {
        Ok(pos) => row.get(pos).cloned().ok_or_else(|| Error::ColumnNotFound(name.into())),
        Err(Error::ColumnNotFound(_)) | Err(Error::TableNotFound(_)) => {
            resolve_frame(session, name)
        }
        Err(e) => Err(e),
    }
}

/// Resolve `name` against the pushed correlation frames only
/// (innermost first). Used by validation paths that already checked the
/// local scope.
pub(crate) fn resolve_frame(session: &Session, name: &str) -> Result<Datum> {
    for frame in session.subq.outer.iter().rev() {
        match Database::resolve_scope(&frame.tables, name) {
            Ok(pos) => {
                return frame.row.get(pos).cloned().ok_or_else(|| Error::ColumnNotFound(name.into()));
            }
            Err(Error::ColumnNotFound(_)) | Err(Error::TableNotFound(_)) => continue,
            Err(e) => return Err(e),
        }
    }
    Err(Error::ColumnNotFound(name.into()))
}

/// Evaluate the outer test operand of an IN-subquery against the row.
fn test_operand(
    scope: &[Arc<Table>],
    row: &[Datum],
    session: &Session,
    expr: &Expr,
) -> Result<Datum> {
    match expr {
        Expr::Literal(d) => Ok(d.clone()),
        Expr::Column(name) => resolve_row(scope, row, session, name),
        other => Err(Error::NotSupported(format!(
            "IN-subquery test must be a column or literal, got {other:?}"
        ))),
    }
}

/// Run one subquery level with the current frames pushed (used for all
/// three shapes; callers post-process rows/columns).
fn run_inner(
    db: &Database,
    session: &mut Session,
    query: &SelectStmt,
    limit_one: bool,
) -> Result<Output> {
    let mut stmt = query.clone();
    if limit_one && stmt.limit.is_none() {
        stmt.limit = Some(1);
    }
    exec_subselect(db, session, &stmt)
}

/// Fold one IN-subquery node for a row: (hit, ) per SQL ternary logic.
/// Returns the boolean value directly (IN-lists cannot be cached as Exprs
/// once NULLs are involved, so folding yields literals).
fn fold_in(
    db: &Database,
    session: &mut Session,
    scope: &[Arc<Table>],
    row: &[Datum],
    test: &Expr,
    query: &SelectStmt,
    negated: bool,
    cache_key: &str,
) -> Result<bool> {
    let test_val = test_operand(scope, row, session, test)?;
    // Uncorrelated fast path: evaluate once per statement (cache presence
    // alone proves uncorrelated — only provably uncorrelated folds are
    // ever stored). The row frame goes up first so correlation detection
    // can resolve outward references.
    let (set, has_nullish) = if let Some(Folded::Set(s, h)) = session.subq.fold.get(cache_key) {
        (s.clone(), *h)
    } else {
        session.subq.outer.push(OuterFrame {
            tables: scope.to_vec(),
            row: row.to_vec(),
        });
        let r = fold_in_uncached(db, session, query, cache_key);
        session.subq.outer.pop();
        r?
    };
    Ok(eval_in(test_val, &set, has_nullish, negated))
}

/// Uncached IN evaluation with the row frame pushed: detect correlation,
/// execute, and cache only when uncorrelated.
fn fold_in_uncached(
    db: &Database,
    session: &mut Session,
    query: &SelectStmt,
    cache_key: &str,
) -> Result<(HashSet<JoinKey>, bool)> {
    let escaping = escaping_refs(db, session, query)?;
    // The frame is already pushed (caller); correlated evaluation simply
    // runs with it in place, uncorrelated results are cached.
    let (set, has_nullish) = build_set(db, session, query)?;
    if escaping.is_empty() {
        session.subq.fold.insert(
            cache_key.to_string(),
            Folded::Set(set.clone(), has_nullish),
        );
    }
    Ok((set, has_nullish))
}

/// Membership test with SQL ternary logic for NOT IN.
fn eval_in(test: Datum, set: &HashSet<JoinKey>, has_nullish: bool, negated: bool) -> bool {
    if matches!(test, Datum::Null) {
        return false;
    }
    let hit = join_key(&test).map_or(false, |k| set.contains(&k));
    if negated {
        if hit {
            false
        } else {
            !has_nullish // unknown when the set holds NULL/NaN: filter out
        }
    } else {
        hit
    }
}

/// Collect one subquery level into a membership set (+ nullish flag).
fn build_set(
    db: &Database,
    session: &mut Session,
    query: &SelectStmt,
) -> Result<(HashSet<JoinKey>, bool)> {
    let out = run_inner(db, session, query, false)?;
    if out.columns.len() != 1 {
        return Err(Error::InvalidQuery(
            "Subquery must return only one column".into(),
        ));
    }
    let mut set = HashSet::new();
    let mut has_nullish = false;
    for r in &out.rows {
        match join_key(&r[0]) {
            Some(k) => {
                set.insert(k);
            }
            None => has_nullish = true,
        }
    }
    Ok((set, has_nullish))
}

/// Fold one scalar subquery node for a row.
fn fold_scalar(
    db: &Database,
    session: &mut Session,
    scope: &[Arc<Table>],
    row: &[Datum],
    query: &SelectStmt,
    cache_key: &str,
) -> Result<Datum> {
    if let Some(Folded::Datum(d)) = session.subq.fold.get(cache_key) {
        return Ok(d.clone());
    }
    session.subq.outer.push(OuterFrame {
        tables: scope.to_vec(),
        row: row.to_vec(),
    });
    let escaping = escaping_refs(db, session, query)?;
    let value = eval_scalar(db, session, query)?;
    let correlated = !escaping.is_empty();
    session.subq.outer.pop();
    if !correlated {
        session.subq.fold.insert(
            cache_key.to_string(),
            Folded::Datum(value.clone()),
        );
    }
    Ok(value)
}

/// Execute a scalar subquery level and extract its single value.
fn eval_scalar(db: &Database, session: &mut Session, query: &SelectStmt) -> Result<Datum> {
    let out = run_inner(db, session, query, false)?;
    if out.columns.len() != 1 {
        return Err(Error::InvalidQuery(
            "Subquery must return only one column".into(),
        ));
    }
    match out.rows.len() {
        0 => Ok(Datum::Null),
        1 => Ok(out.rows[0][0].clone()),
        _ => Err(Error::ExecutionError(
            "Subquery returns more than 1 row".into(),
        )),
    }
}

/// Fold one EXISTS node for a row.
fn fold_exists(
    db: &Database,
    session: &mut Session,
    scope: &[Arc<Table>],
    row: &[Datum],
    query: &SelectStmt,
    negated: bool,
    cache_key: &str,
) -> Result<bool> {
    if let Some(Folded::Bool(b)) = session.subq.fold.get(cache_key) {
        return Ok(if negated { !b } else { *b });
    }
    session.subq.outer.push(OuterFrame {
        tables: scope.to_vec(),
        row: row.to_vec(),
    });
    let escaping = escaping_refs(db, session, query)?;
    let hit = !run_inner(db, session, query, true)?.rows.is_empty();
    let correlated = !escaping.is_empty();
    session.subq.outer.pop();
    if !correlated {
        session.subq.fold.insert(
            cache_key.to_string(),
            Folded::Bool(hit),
        );
    }
    Ok(if negated { !hit } else { hit })
}

/// A folded boolean as an expression: `= TRUE` comparison (evaluates
/// through the untouched `eval` path, which has no bare-literal predicate
/// arm by design).
fn folded_bool(b: bool) -> Expr {
    Expr::Cmp {
        left: Box::new(Expr::Literal(Datum::Bool(b))),
        op: crate::sql::CmpOp::Eq,
        right: Box::new(Expr::Literal(Datum::Bool(true))),
    }
}
/// Fold every subquery node in a predicate for one row, bottom-up. Plain
/// nodes rebuild identically, so subquery-free predicates cost one walk and
/// evaluate through the untouched `eval` path afterwards.
pub(crate) fn fold_predicate(
    db: &Database,
    session: &mut Session,
    scope: &[Arc<Table>],
    row: &[Datum],
    expr: &Expr,
) -> Result<Expr> {
    match expr {
        Expr::InSubquery { expr: test, query, negated } => {
            let test = fold_predicate(db, session, scope, row, test)?;
            let key = format!("IN{query:?}");
            // Fast lanes avoid frame traffic for uncorrelated leftovers;
            // fold_in handles caching + correlation internally.
            let b = fold_in(db, session, scope, row, &test, query, *negated, &key)?;
            Ok(folded_bool(b))
        }
        Expr::Exists { query, negated } => {
            let key = format!("EXISTS{query:?}");
            let b = fold_exists(db, session, scope, row, query, *negated, &key)?;
            Ok(folded_bool(b))
        }
        Expr::ScalarSubquery(q) => {
            let key = format!("SCALAR{q:?}");
            let d = fold_scalar(db, session, scope, row, q, &key)?;
            Ok(Expr::Literal(d))
        }
        Expr::Cmp { left, op, right } => Ok(Expr::Cmp {
            left: Box::new(fold_predicate(db, session, scope, row, left)?),
            op: op.clone(),
            right: Box::new(fold_predicate(db, session, scope, row, right)?),
        }),
        Expr::And(a, b) => Ok(Expr::And(
            Box::new(fold_predicate(db, session, scope, row, a)?),
            Box::new(fold_predicate(db, session, scope, row, b)?),
        )),
        Expr::Or(a, b) => Ok(Expr::Or(
            Box::new(fold_predicate(db, session, scope, row, a)?),
            Box::new(fold_predicate(db, session, scope, row, b)?),
        )),
        Expr::Not(e) => Ok(Expr::Not(Box::new(fold_predicate(db, session, scope, row, e)?))),
        Expr::In { expr, values, negated } => Ok(Expr::In {
            expr: Box::new(fold_predicate(db, session, scope, row, expr)?),
            values: values.clone(),
            negated: *negated,
        }),
        Expr::Between { expr, lo, hi, negated } => Ok(Expr::Between {
            expr: Box::new(fold_predicate(db, session, scope, row, expr)?),
            lo: lo.clone(),
            hi: hi.clone(),
            negated: *negated,
        }),
        Expr::Like { expr, pattern, negated } => Ok(Expr::Like {
            expr: Box::new(fold_predicate(db, session, scope, row, expr)?),
            pattern: pattern.clone(),
            negated: *negated,
        }),
        Expr::Column(_) | Expr::Literal(_) => Ok(expr.clone()),
    }
}

/// True when `expr` contains no subquery nodes (folding is then a no-op
/// walk; callers use this to keep the hot path allocation-free... in
/// practice one cheap walk).
pub(crate) fn has_subquery(expr: &Expr) -> bool {    match expr {
        Expr::InSubquery { .. } | Expr::ScalarSubquery(_) | Expr::Exists { .. } => true,
        Expr::Cmp { left, right, .. } => has_subquery(left) || has_subquery(right),
        Expr::And(a, b) | Expr::Or(a, b) => has_subquery(a) || has_subquery(b),
        Expr::Not(e) => has_subquery(e),
        Expr::In { expr, .. } | Expr::Between { expr, .. } | Expr::Like { expr, .. } => {
            has_subquery(expr)
        }
        Expr::Column(_) | Expr::Literal(_) => false,
    }
}

/// Split top-level AND conjuncts into (plain, with-subquery) parts.
/// Callers run the plain part through the indexed paths and evaluate the
/// subquery part per row via `filter_with_subqueries`.
pub(crate) fn split_subquery_parts(expr: &Expr) -> (Option<Expr>, Option<Expr>) {
    let mut conjuncts = Vec::new();
    let mut stack = vec![expr];
    while let Some(e) = stack.pop() {
        match e {
            Expr::And(a, b) => {
                stack.push(a);
                stack.push(b);
            }
            other => conjuncts.push(other),
        }
    }
    let mut plain: Vec<Expr> = Vec::new();
    let mut subq: Vec<Expr> = Vec::new();
    for c in conjuncts {
        if has_subquery(c) {
            subq.push(c.clone());
        } else {
            plain.push(c.clone());
        }
    }
    (join_and(plain), join_and(subq))
}

fn join_and(mut parts: Vec<Expr>) -> Option<Expr> {
    let mut out = parts.pop()?;
    while let Some(e) = parts.pop() {
        out = Expr::And(Box::new(e), Box::new(out));
    }
    Some(out)
}

/// Filter rows by a predicate containing subqueries: fold per row, then
/// evaluate through the standard path. Scope positions resolve exactly
/// like the joined executor (`resolve_scope`), with fallback outward
/// across the correlation frames (so correlated references surviving the
/// fold evaluate against the right row).
pub(crate) fn filter_with_subqueries(
    db: &Database,
    session: &mut Session,
    scope: &[Arc<Table>],
    rows: Vec<Vec<Datum>>,
    selection: &Expr,
) -> Result<Vec<Vec<Datum>>> {
    let mut kept = Vec::new();
    for r in rows {
        let folded = fold_predicate(db, session, scope, &r, selection)?;
        let pass = crate::sql::eval_with(&folded, &mut |name| {
            resolve_row(scope, &r, session, name)
        })?;
        if pass {
            kept.push(r);
        }
    }
    Ok(kept)
}

/// Output column name for a projection scalar: explicit AS alias, else the
/// inner single-column name (mirrors the executor's naming).
pub(crate) fn projection_scalar_name(query: &SelectStmt, alias: Option<&str>) -> String {
    if let Some(a) = alias {
        return a.to_string();
    }
    match query.items.as_slice() {
        [SelectItem::Column(c)] => c.rsplit('.').next().unwrap_or(c).to_string(),
        [SelectItem::CountStar] => "COUNT(*)".to_string(),
        [SelectItem::Aggregate { func, column }] => {
            format!("{}({column})", func.name())
        }
        _ => "subquery".to_string(),
    }
}

/// Evaluate one scalar subquery for projection (uncorrelated or correlated
/// against the source row). Returns the value plus the output column name
/// (explicit AS alias, else the inner single-column name).
pub(crate) fn eval_projection_scalar(
    db: &Database,
    session: &mut Session,
    scope: &[Arc<Table>],
    row: &[Datum],
    query: &SelectStmt,
    alias: Option<&str>,
) -> Result<(Datum, String)> {
    let key = format!("SCALAR{q:?}", q = query);
    let d = fold_scalar(db, session, scope, row, query, &key)?;
    if let Some(a) = alias {
        return Ok((d, a.to_string()));
    }
    // Name after the inner projection when it is a plain column.
    let name = match query.items.as_slice() {
        [SelectItem::Column(c)] => c.rsplit('.').next().unwrap_or(c).to_string(),
        [SelectItem::CountStar] => "COUNT(*)".to_string(),
        [SelectItem::Aggregate { func, column }] => {
            format!("{}({column})", func.name())
        }
        _ => "subquery".to_string(),
    };
    Ok((d, name))
}

/// Evaluate one scalar subquery with no row environment (global aggregates,
/// uncorrelated-only; correlation fails cleanly as unknown column).
pub(crate) fn eval_scalar_uncorrelated(
    db: &Database,
    session: &mut Session,
    query: &SelectStmt,
) -> Result<Datum> {
    let key = format!("SCALAR{q:?}", q = query);
    fold_scalar(db, session, &[], &[], query, &key)
}

/// Conjunct pushdown safety: every escaping reference of every subquery in
/// the conjunct must resolve to table `ord` (inner-resolving references are
/// evaluated inside the subquery and never constrain pushdown).
pub(crate) fn pushable_to(
    db: &Database,
    session: &Session,
    expr: &Expr,
    ord: usize,
    tables: &[Arc<Table>],
) -> bool {
    let mut nodes = Vec::new();
    collect_subqueries(expr, &mut nodes);
    for q in nodes {
        let scope = match inner_scope(db, session, q) {
            Ok(s) => s,
            Err(_) => return false,
        };
        let mut cols = Vec::new();
        if let Some(s) = q.selection.as_ref() {
            deep_columns(s, &mut cols);
        }
        for j in &q.joins {
            deep_columns(&j.on, &mut cols);
        }
        cols.extend(q.order_by.iter().map(|(c, _)| c.clone()));
        cols.extend(q.group_by.iter().cloned());
        for c in cols {
            if resolves_inner(&scope, &c) {
                continue;
            }
            // Must resolve outward to exactly this table.
            let mut ok = false;
            for (ti, t) in tables.iter().enumerate() {
                if ti != ord {
                    continue;
                }
                if Database::resolve_scope(std::slice::from_ref(t), &c).is_ok() {
                    ok = true;
                    break;
                }
            }
            if !ok {
                return false;
            }
        }
    }
    true
}

/// All subquery bodies under an expression (for pushdown analysis).
fn collect_subqueries<'a>(expr: &'a Expr, out: &mut Vec<&'a SelectStmt>) {
    match expr {
        Expr::InSubquery { expr: t, query, .. } => {
            collect_subqueries(t, out);
            collect_substmt(query, out);
        }
        Expr::ScalarSubquery(q) => collect_substmt(q, out),
        Expr::Exists { query, .. } => collect_substmt(query, out),
        Expr::Cmp { left, right, .. } => {
            collect_subqueries(left, out);
            collect_subqueries(right, out);
        }
        Expr::And(a, b) | Expr::Or(a, b) => {
            collect_subqueries(a, out);
            collect_subqueries(b, out);
        }
        Expr::Not(e) => collect_subqueries(e, out),
        Expr::In { expr, .. } | Expr::Between { expr, .. } | Expr::Like { expr, .. } => {
            collect_subqueries(expr, out)
        }
        Expr::Column(_) | Expr::Literal(_) => {}
    }
}

fn collect_substmt<'a>(stmt: &'a SelectStmt, out: &mut Vec<&'a SelectStmt>) {
    out.push(stmt);
    if let Some(s) = stmt.selection.as_ref() {
        collect_subqueries(s, out);
    }
    for j in &stmt.joins {
        collect_subqueries(&j.on, out);
    }
    for r in std::iter::once(&stmt.from).chain(stmt.joins.iter().map(|j| &j.table)) {
        if let TableRef::Derived { query, .. } = r {
            collect_substmt(query, out);
        }
    }
}
