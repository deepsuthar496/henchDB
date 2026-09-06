//! Query execution: SELECT planning/execution (single-table fast path,
//! multi-table nested-loop JOIN + GROUP BY), projection, ordering, and
//! aggregation helpers. `Database::describe` (prepare-time metadata) lives
//! here too.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use super::cost::{estimate_count, local_predicate, selectivity};
use super::plan::{equi_join, join_key, order_joins, JoinKey};
use super::subquery;

/// Per-table input actuals (execution order) for `EXPLAIN ANALYZE`.
pub(super) struct JoinCapture {
    pub input_rows: Vec<usize>,
}

/// One global-aggregation output column: a row count, a column aggregate,
/// or a pre-folded scalar subquery value.
enum AggSpec {
    Count,
    Agg(AggFunc, usize),
    Scalar(Datum),
}

/// Shared join layout: scope tables, per-table pushdown estimates, and the
/// greedy execution order. Used by the executor and EXPLAIN alike so the
/// displayed plan is the executed plan.
pub(super) struct JoinPlan {
    /// Scope tables in written order.
    pub(super) tables: Vec<Arc<Table>>,
    /// Display names in written order (as written in FROM/JOIN).
    pub(super) names: Vec<String>,
    /// Per-table estimates in written order.
    pub(super) estimates: Vec<TableEstimate>,
    /// Execution order (table indices, 0 = FROM first).
    pub(super) order: Vec<usize>,
}

/// Filtered-size estimate for one join input.
pub(super) struct TableEstimate {
    /// Pushable local predicate (None when none, or when the table is on
    /// the NULL-supplying side of a LEFT JOIN).
    pub(super) local: Option<Expr>,
    /// Selectivity of `local` (1.0 when None).
    pub(super) sel: f64,
    /// Committed row estimate before filtering.
    pub(super) total: f64,
    /// `total * sel`, rounded — the ordering key.
    pub(super) filtered: usize,
}
use super::{Database, Output, Session};
use crate::error::{Error, Result};
use crate::sql::{parse_sql, AggFunc, Expr, JoinClause, JoinKind, SelectItem, Statement};
use crate::sql::{SelectStmt, TableRef};
use crate::table::{Schema, Table};
use crate::types::{ColumnType, Datum};

impl Database {
    /// Parse a statement and describe its output columns (name + type) for
    /// the wire protocol's prepared-statement negotiation. SELECT expands
    /// `*` via the schema; anything without a result set yields no columns.
    /// Unknown tables/columns are errors (prepare-time validation, like
    /// MySQL).
    pub fn describe(&self, session: &Session, sql: &str) -> Result<Vec<(String, ColumnType)>> {
        let stmt = parse_sql(sql.trim())?;
        match stmt {
            Statement::Select { items, from, joins, .. } => {
                let mut tmp = Session {
                    current_db: session.current_db.clone(),
                    ..Default::default()
                };
                self.describe_select(session, &mut tmp, &items, &from, &joins)
            }
            Statement::ShowTables => Ok(vec![("table".into(), ColumnType::Text)]),
            Statement::ShowDatabases => Ok(vec![("Database".into(), ColumnType::Text)]),            Statement::ShowStatus { .. } => Ok(vec![
                ("Variable_name".into(), ColumnType::Text),
                ("Value".into(), ColumnType::Text),
            ]),
            Statement::ShowEngineStatus => Ok(vec![
                ("Type".into(), ColumnType::Text),
                ("Name".into(), ColumnType::Text),
                ("Status".into(), ColumnType::Text),
            ]),
            Statement::ShowProcesslist => Ok(vec![
                ("Id".into(), ColumnType::BigInt),
                ("User".into(), ColumnType::Text),
                ("Host".into(), ColumnType::Text),
                ("db".into(), ColumnType::Text),
                ("Command".into(), ColumnType::Text),
                ("Time".into(), ColumnType::BigInt),
                ("State".into(), ColumnType::Text),
                ("Info".into(), ColumnType::Text),
            ]),
            Statement::AnalyzeTable { .. } => Ok(vec![
                ("Table".into(), ColumnType::Text),
                ("Op".into(), ColumnType::Text),
                ("Msg_type".into(), ColumnType::Text),
                ("Msg_text".into(), ColumnType::Text),
            ]),
            Statement::Explain { analyze: false, .. } => Ok(vec![
                ("table".into(), ColumnType::Text),
                ("access_path".into(), ColumnType::Text),
                ("type".into(), ColumnType::Text),
                ("key".into(), ColumnType::Text),
                ("rows".into(), ColumnType::BigInt),
                ("filtered".into(), ColumnType::Text),
                ("cost".into(), ColumnType::Text),
            ]),
            Statement::Explain { analyze: true, .. } => Ok(vec![
                ("table".into(), ColumnType::Text),
                ("access_path".into(), ColumnType::Text),
                ("type".into(), ColumnType::Text),
                ("key".into(), ColumnType::Text),
                ("rows_est".into(), ColumnType::BigInt),
                ("rows_act".into(), ColumnType::BigInt),
                ("cost".into(), ColumnType::Text),
                ("time_ms".into(), ColumnType::Text),
            ]),
            _ => Ok(vec![]),
        }
    }

    /// Describe one SELECT level: resolve sources (materializing derived
    /// tables into a throwaway session — prepare-time only, never the
    /// caller's session) and expand the projection to (name, type).
    /// `tmp` carries nested derived scopes across recursion (same
    /// setup/teardown discipline as execution).
    fn describe_select(
        &self,
        session: &Session,
        tmp: &mut Session,
        items: &[SelectItem],
        from: &TableRef,
        joins: &[JoinClause],
    ) -> Result<Vec<(String, ColumnType)>> {
        // Derived sources need materialized schemas: run their inner
        // queries in the throwaway session (SELECT-only, no side effects).
        let saved = subquery::setup_derived(self, tmp, from, joins)?;
        let r = self.describe_select_resolved(session, tmp, items, from, joins);
        subquery::teardown_derived(tmp, saved);
        r
    }

    fn describe_select_resolved(
        &self,
        session: &Session,
        tmp: &mut Session,
        items: &[SelectItem],
        from: &TableRef,
        joins: &[JoinClause],
    ) -> Result<Vec<(String, ColumnType)>> {
        let mut tables: Vec<Arc<Table>> = vec![subquery::resolve_table_ref(self, tmp, from)?];
        for j in joins {
            tables.push(subquery::resolve_table_ref(self, tmp, &j.table)?);
        }
        // Self-join guard mirrors the executor (derived aliases included:
        // ephemeral def names are their aliases).
        {
            let mut seen: Vec<&str> = Vec::new();
            for t in &tables {
                let n = t.def.name.as_str();
                if seen.contains(&n) {
                    return Err(Error::NotSupported(
                        "self-joins need table aliases (unsupported)".into(),
                    ));
                }
                seen.push(n);
            }
        }
        // For bare star columns, qualify only on cross-table collision.
        let mut cols = Vec::new();
        for item in items {
            match item {
                SelectItem::Star => {
                    for (name, idx) in Self::star_columns(&tables) {
                        let (owner, local) = Self::scope_owner(&tables, idx);
                        cols.push((name, owner.schema().columns[local].ctype));
                    }
                }
                SelectItem::Column(c) => {
                    let idx = Self::resolve_scope(&tables, &c)?;
                    let (owner, local) = Self::scope_owner(&tables, idx);
                    cols.push((Self::proj_output_name(&item), owner.schema().columns[local].ctype));
                }
                SelectItem::CountStar => cols.push(("COUNT(*)".into(), ColumnType::BigInt)),
                SelectItem::Literal(d) => cols.push((
                    d.to_string(),
                    match d {
                        Datum::Int(_) => ColumnType::BigInt,
                        Datum::Float(_) => ColumnType::Double,
                        Datum::Text(_) => ColumnType::Text,
                        Datum::Bool(_) => ColumnType::Bool,
                        Datum::DateTime(_) => ColumnType::DateTime,
                        Datum::Null => ColumnType::Text,
                    },
                )),                SelectItem::Aggregate { func, column } => {
                    let idx = Self::resolve_scope(&tables, &column)?;
                    let (owner, local) = Self::scope_owner(&tables, idx);
                    let ctype = owner.schema().columns[local].ctype;
                    let out_type = match func {
                        AggFunc::Avg => ColumnType::Double,
                        AggFunc::Sum => match ctype {
                            ColumnType::Float | ColumnType::Double => ColumnType::Double,
                            _ => ColumnType::BigInt,
                        },
                        AggFunc::Min | AggFunc::Max => ctype,
                    };
                    cols.push((Self::proj_output_name(&item), out_type));
                }
                SelectItem::Subquery { query, alias } => {
                    let inner = self.describe_select(
                        session,
                        tmp,
                        &query.items,
                        &query.from,
                        &query.joins,
                    )?;
                    if inner.len() != 1 {
                        return Err(Error::InvalidQuery(
                            "Subquery must return only one column".into(),
                        ));
                    }
                    let name = alias.clone().unwrap_or_else(|| inner[0].0.clone());
                    cols.push((name, inner[0].1));
                }
            }
        }
        Ok(cols)
    }

    pub(super) fn exec_select(
        &self,
        session: &mut Session,
        items: Vec<SelectItem>,
        from: &TableRef,
        joins: Vec<crate::sql::JoinClause>,
        selection: Option<Expr>,
        order_by: Vec<(String, bool)>,
        limit: Option<usize>,
        group_by: Vec<String>,
    ) -> Result<Output> {
        // Derived-table scope: materialize this level's sources, run, then
        // restore shadowed aliases (nesting-safe by construction).
        let saved = subquery::setup_derived(self, session, from, &joins)?;
        let out = if joins.is_empty() && group_by.is_empty() {
            self.exec_select_single(session, items, from, selection, order_by, limit)
        } else {
            self.exec_select_joined(session, items, from, joins, selection, order_by, limit, group_by)
        };
        subquery::teardown_derived(session, saved);
        out
    }

    /// Entry point for subquery levels (same setup/teardown discipline via
    /// `exec_select`).
    pub(super) fn exec_select_stmt(
        &self,
        session: &mut Session,
        stmt: &SelectStmt,
    ) -> Result<Output> {
        self.exec_select(
            session,
            stmt.items.clone(),
            &stmt.from,
            stmt.joins.clone(),
            stmt.selection.clone(),
            stmt.order_by.clone(),
            stmt.limit,
            stmt.group_by.clone(),
        )
    }

    /// Bare column part of a possibly qualified `table.col` reference.
    fn bare_name(name: &str) -> &str {
        name.rsplit('.').next().unwrap_or(name)
    }

    /// Output header for one projection item (shared by executor + describe).
    fn proj_output_name(item: &SelectItem) -> String {
        match item {
            SelectItem::Star => unreachable!(),
            SelectItem::Column(c) => Self::bare_name(c).to_string(),
            SelectItem::CountStar => "COUNT(*)".into(),
            SelectItem::Aggregate { func, column } => format!("{}({column})", func.name()),
            // Scalar subqueries name after their inner column (or alias at
            // projection time); describe falls back to the same rule.
            SelectItem::Subquery { query, alias } => alias.clone().unwrap_or_else(|| {
                subquery::projection_scalar_name(query, None)
            }),
            SelectItem::Literal(d) => d.to_string(),
        }
    }

    /// Resolve `col` or `table.col` in a single-table context. A qualifier
    /// naming another table is an error here; joins resolve those instead.
    fn single_col_idx(schema: &Schema, table: &str, name: &str) -> Result<usize> {
        match name.split_once('.') {
            Some((t, c)) => {
                if t == table {
                    schema.index_of(c)
                } else {
                    None
                }
                .ok_or_else(|| Error::ColumnNotFound(name.into()))
            }
            None => schema.index_of(name).ok_or_else(|| Error::ColumnNotFound(name.into())),
        }
    }

    /// Rewrite `table.col` refs to `col` for single-table statements (the
    /// qualifier must name this table). Keeps qualified sugar working on the
    /// fast path without touching the index-aware machinery. References to
    /// other tables pass through untouched: inside subqueries they are
    /// correlated outer references resolved at execution; at the top level
    /// they fail then (like unknown bare columns, as empty results).
    pub(super) fn strip_qualifiers(expr: &Expr, table: &str) -> Result<Expr> {
        match expr {
            Expr::Literal(d) => Ok(Expr::Literal(d.clone())),
            Expr::Column(name) => match name.split_once('.') {
                Some((t, c)) if t == table => Ok(Expr::Column(c.into())),
                Some(_) => Ok(Expr::Column(name.clone())),
                None => Ok(Expr::Column(name.clone())),
            },
            Expr::Cmp { left, op, right } => Ok(Expr::Cmp {
                left: Box::new(Self::strip_qualifiers(left, table)?),
                op: op.clone(),
                right: Box::new(Self::strip_qualifiers(right, table)?),
            }),
            Expr::And(a, b) => Ok(Expr::And(
                Box::new(Self::strip_qualifiers(a, table)?),
                Box::new(Self::strip_qualifiers(b, table)?),
            )),
            Expr::Or(a, b) => Ok(Expr::Or(
                Box::new(Self::strip_qualifiers(a, table)?),
                Box::new(Self::strip_qualifiers(b, table)?),
            )),
            Expr::Not(e) => Ok(Expr::Not(Box::new(Self::strip_qualifiers(e, table)?))),
            // IN/BETWEEN/LIKE carry literal operands only; recurse the
            // tested expression. Subquery bodies keep their own scope:
            // only the outer test expression strips qualifiers here
            // (correlated qualified refs resolve outward at execution).
            Expr::In { expr, values, negated } => Ok(Expr::In {
                expr: Box::new(Self::strip_qualifiers(expr, table)?),
                values: values.clone(),
                negated: *negated,
            }),
            Expr::Between { expr, lo, hi, negated } => Ok(Expr::Between {
                expr: Box::new(Self::strip_qualifiers(expr, table)?),
                lo: lo.clone(),
                hi: hi.clone(),
                negated: *negated,
            }),
            Expr::Like { expr, pattern, negated } => Ok(Expr::Like {
                expr: Box::new(Self::strip_qualifiers(expr, table)?),
                pattern: pattern.clone(),
                negated: *negated,
            }),
            Expr::InSubquery { expr, query, negated } => Ok(Expr::InSubquery {
                expr: Box::new(Self::strip_qualifiers(expr, table)?),
                query: query.clone(),
                negated: *negated,
            }),
            Expr::ScalarSubquery(_) | Expr::Exists { .. } => Ok(expr.clone()),
        }
    }

    /// Single-table SELECT: index-aware fast path (unchanged hot path).
    pub(super) fn exec_select_single(
        &self,
        session: &mut Session,
        items: Vec<SelectItem>,
        from: &TableRef,
        selection: Option<Expr>,
        order_by: Vec<(String, bool)>,
        limit: Option<usize>,
    ) -> Result<Output> {
        let deadline = session.max_execution_time.map(|t| std::time::Instant::now() + t);
        if let Some(dl) = deadline {
            if std::time::Instant::now() > dl {
                return Err(Error::QueryTimeout);
            }
        }
        let table_arc = subquery::resolve_table_ref(self, session, from)?;
        let display = from.name();
        let schema = table_arc.schema();
        let selection = selection.map(|s| Self::strip_qualifiers(&s, display)).transpose()?;

        let count_only = items.len() == 1 && matches!(items[0], SelectItem::CountStar);
        // Global aggregates (no GROUP BY): mixing aggregates with plain
        // columns is rejected like MySQL's ONLY_FULL_GROUP_BY.
        // (The sole-COUNT(*) case keeps its legacy path below.)
        // Scalar-subquery items behave like plain per-row columns for the
        // mixing rule (they fold to one value per row); aggregates still
        // need GROUP BY when mixed with row values.
        let has_agg = items.iter().any(|i| matches!(i, SelectItem::Aggregate { .. } | SelectItem::CountStar));
        let all_agg = !items.is_empty()
            && items.iter().all(|i| {
                matches!(
                    i,
                    SelectItem::Aggregate { .. } | SelectItem::CountStar | SelectItem::Subquery { .. }
                )
            });
        if has_agg && !all_agg {
            return Err(Error::NotSupported(
                "mixing aggregates with plain columns requires GROUP BY".into(),
            ));
        }
        let agg_only = all_agg && !count_only;
        /// Projection spec: plain column positions, per-row scalar
        /// subqueries evaluated after ORDER BY / LIMIT truncation, or
        /// row-independent constants.
        enum ProjSpec {
            Col(usize),
            Subq(SelectStmt, Option<String>),
            Const(Datum),
        }
        let mut out_columns: Vec<String> = Vec::new();
        let mut proj: Vec<ProjSpec> = Vec::new();
        if !count_only && !agg_only {
            for item in &items {
                match item {
                    SelectItem::Star => {
                        out_columns.extend(schema.column_names());
                        proj.extend((0..schema.columns.len()).map(ProjSpec::Col));
                    }
                    SelectItem::Column(c) => {
                        let idx = Self::single_col_idx(schema, display, c)?;
                        out_columns.push(Self::bare_name(c).into());
                        proj.push(ProjSpec::Col(idx));
                    }
                    SelectItem::Literal(d) => {
                        out_columns.push(d.to_string());
                        proj.push(ProjSpec::Const(d.clone()));
                    }
                    SelectItem::Subquery { query, alias } => {
                        out_columns.push(subquery::projection_scalar_name(query, alias.as_deref()));
                        proj.push(ProjSpec::Subq((**query).clone(), alias.clone()));
                    }
                    SelectItem::CountStar => unreachable!(),
                    SelectItem::Aggregate { func, column } => {
                        Self::single_col_idx(schema, display, column)?;
                        out_columns.push(format!("{}({column})", func.name()));
                        // Unreachable here (agg_only split above), kept for
                        // exhaustiveness.
                        return Err(Error::NotSupported(
                            "mixing aggregates with plain columns requires GROUP BY".into(),
                        ));
                    }
                }
            }
        }

        // Subquery conjuncts bypass the indexed scan (planned opaque) and
        // filter here per row; plain conjuncts keep their index paths (the
        // post-filter re-checks everything, so the split is pure planning).
        let (plain_sel, sub_sel) = match selection.as_ref() {
            Some(s) if subquery::has_subquery(s) => {
                let (p, q) = subquery::split_subquery_parts(s);
                (p, q)
            }
            _ => (selection.clone(), None),
        };
        let mut rows = self.visible_rows(session, &table_arc, plain_sel.as_ref())?;
        if let Some(q) = sub_sel.as_ref() {
            let scope = std::slice::from_ref(&table_arc);
            rows = subquery::filter_with_subqueries(self, session, scope, rows, q)?;
        }

        if agg_only {
            // ORDER BY / LIMIT do not apply to a global aggregate row.
            // Scalar subqueries fold once (uncorrelated-only; correlation
            // has no row environment here and fails as unknown column).
            let mut aggs = Vec::with_capacity(items.len());
            for item in &items {
                match item {
                    SelectItem::CountStar => aggs.push((AggSpec::Count, "COUNT(*)".into())),
                    SelectItem::Aggregate { func, column } => {
                        let idx = Self::single_col_idx(schema, display, column)?;
                        aggs.push((AggSpec::Agg(*func, idx), format!("{}({column})", func.name())));
                    }
                    SelectItem::Subquery { query, alias } => {
                        let d = subquery::eval_scalar_uncorrelated(self, session, query)?;
                        let name = alias.clone().unwrap_or_else(|| {
                            subquery::projection_scalar_name(query, None)
                        });
                        aggs.push((AggSpec::Scalar(d), name));
                    }
                    _ => unreachable!(),
                }
            }
            return Self::exec_aggregate_rows(&aggs, rows);
        }

        if !order_by.is_empty() {
            let mut keys = Vec::with_capacity(order_by.len());
            for (col, _) in &order_by {
                keys.push(Self::single_col_idx(schema, display, col)?);
            }
            rows.sort_by(|a, b| {
                for (i, (_, desc)) in order_by.iter().enumerate() {
                    let ord = a[keys[i]].cmp(&b[keys[i]]);
                    if ord != std::cmp::Ordering::Equal {
                        return if *desc { ord.reverse() } else { ord };
                    }
                }
                std::cmp::Ordering::Equal
            });
        }
        if let Some(l) = limit {
            rows.truncate(l);
        }

        if count_only {
            return Ok(Output {
                columns: vec!["COUNT(*)".into()],
                rows: vec![vec![Datum::Int(rows.len() as i64)]],
                message: "OK".into(),
            });
        }

        let scope = std::slice::from_ref(&table_arc);
        let mut out_rows: Vec<Vec<Datum>> = Vec::with_capacity(rows.len());
        for r in rows {
            let mut out = Vec::with_capacity(proj.len());
            for p in &proj {
                match p {
                    ProjSpec::Col(i) => out.push(r[*i].clone()),
                    ProjSpec::Const(d) => out.push(d.clone()),
                    ProjSpec::Subq(q, a) => {
                        let (d, _) = subquery::eval_projection_scalar(
                            self,
                            session,
                            scope,
                            &r,
                            q,
                            a.as_deref(),
                        )?;
                        out.push(d);
                    }
                }
            }
            out_rows.push(out);
        }
        Ok(Output {
            columns: out_columns,
            rows: out_rows,
            message: "OK".into(),
        })
    }

    /// Global aggregation over filtered rows: one output row. NULLs are
    /// skipped (empty set: COUNT → 0, others → NULL). Non-numeric values in
    /// SUM/AVG are type errors; MIN/MAX use the total datum order.
    /// `aggs`: per-item (spec, output name).
    fn exec_aggregate_rows(aggs: &[(AggSpec, String)], rows: Vec<Vec<Datum>>) -> Result<Output> {
        let mut out_row = Vec::with_capacity(aggs.len());
        let mut out_columns = Vec::with_capacity(aggs.len());
        for (agg, name) in aggs {
            out_columns.push(name.clone());
            match agg {
                AggSpec::Count => out_row.push(Datum::Int(rows.len() as i64)),
                AggSpec::Agg(func, idx) => {
                    out_row.push(Self::compute_aggregate(*func, *idx, &rows)?);
                }
                AggSpec::Scalar(d) => out_row.push(d.clone()),
            }
        }
        Ok(Output {
            columns: out_columns,
            rows: vec![out_row],
            message: "OK".into(),
        })
    }

    // -- multi-table SELECT (JOIN + GROUP BY) -------------------------------
    //
    // Left-deep joins: each step hashes on an equi-key (`t1.a = t2.b`) when
    // the ON conjunction carries one, else nested loop. The full ON clause
    // always re-filters matches, so compound predicates stay correct.
    // Correctness notes: WHERE applies post-join (so single-table predicates
    // on either side see joined rows, exactly like the single-table filter);
    // LEFT JOIN pads missing right sides with NULLs, which fail predicates
    // as usual.

    /// Owning table + local column index for a concatenated-row position.
    /// Callers only pass indices from `resolve_scope`, so the fallback is
    /// unreachable in practice (no panic: first table, first column).
    fn scope_owner(tables: &[Arc<Table>], idx: usize) -> (&Arc<Table>, usize) {
        let mut base = 0usize;
        for t in tables {
            let n = t.schema().columns.len();
            if idx < base + n {
                return (t, idx - base);
            }
            base += n;
        }
        (&tables[0], 0)
    }

    /// Star expansion over a scope: bare names, qualified (`t.c`) only on
    /// cross-table collisions. Shared by executor and describe.
    fn star_columns(tables: &[Arc<Table>]) -> Vec<(String, usize)> {
        let mut use_count: HashMap<&str, usize> = HashMap::new();
        for t in tables {
            for c in &t.schema().columns {
                *use_count.entry(c.name.as_str()).or_insert(0) += 1;
            }
        }
        let mut out = Vec::new();
        let mut base = 0usize;
        for t in tables {
            let simple_name = t.def.name.split('.').last().unwrap_or(&t.def.name);
            for (i, c) in t.schema().columns.iter().enumerate() {
                let name = if use_count[c.name.as_str()] > 1 {
                    format!("{simple_name}.{}", c.name)
                } else {
                    c.name.clone()
                };
                out.push((name, base + i));
            }
            base += t.schema().columns.len();
        }
        out
    }

    /// Resolve `col` or `table.col` to a position in a concatenated joined
    /// row. Bare names must match exactly one table (ambiguity is an error).
    /// Shared with `db/subquery.rs` for correlated name resolution.
    pub(crate) fn resolve_scope(tables: &[Arc<Table>], name: &str) -> Result<usize> {
        let mut base = 0usize;
        if let Some((t, c)) = name.split_once('.') {
            for table in tables {
                let simple_name = table.def.name.split('.').last().unwrap_or(&table.def.name);
                if table.def.name == t || simple_name == t {
                    return Ok(base
                        + table
                            .schema()
                            .index_of(c)
                            .ok_or_else(|| Error::ColumnNotFound(name.into()))?);
                }
                base += table.schema().columns.len();
            }
            return Err(Error::TableNotFound(t.into()));
        }
        let mut hit: Option<usize> = None;
        for table in tables {
            if let Some(i) = table.schema().index_of(name) {
                if hit.is_some() {
                    return Err(Error::NotSupported(format!("ambiguous column '{name}'")));
                }
                hit = Some(base + i);
            }
            base += table.schema().columns.len();
        }
        hit.ok_or_else(|| Error::ColumnNotFound(name.into()))
    }

    /// Owning table ordinal (0-based over `tables`) for `col` or
    /// `table.col`. Join ordering permutes whole tables, so it needs table
    /// ordinals where the rest of the executor needs scope positions.
    fn scope_table_idx(tables: &[Arc<Table>], name: &str) -> Result<usize> {
        if let Some((t, c)) = name.split_once('.') {
            for (ord, table) in tables.iter().enumerate() {
                let simple_name = table.def.name.split('.').last().unwrap_or(&table.def.name);
                if table.def.name == t || simple_name == t {
                    table
                        .schema()
                        .index_of(c)
                        .ok_or_else(|| Error::ColumnNotFound(name.into()))?;
                    return Ok(ord);
                }
            }
            return Err(Error::TableNotFound(t.into()));
        }
        let mut hit: Option<usize> = None;
        for (ord, table) in tables.iter().enumerate() {
            if table.schema().index_of(name).is_some() {
                if hit.is_some() {
                    return Err(Error::NotSupported(format!("ambiguous column '{name}'")));
                }
                hit = Some(ord);
            }
        }
        hit.ok_or_else(|| Error::ColumnNotFound(name.into()))
    }

    /// Predicate evaluation over a joined row: same core as single-table
    /// `eval_expr` (shared via resolver), with scope-based resolution.
    fn eval_scoped(expr: &Expr, tables: &[Arc<Table>], row: &[Datum]) -> Result<bool> {
        crate::sql::eval_with(expr, &mut |name| {
            Ok(row[Self::resolve_scope(tables, name)?].clone())
        })
    }

    /// Frame-aware `eval_scoped`: names failing the join scope fall back
    /// outward across correlation frames (subquery row environments).
    /// With empty frames this is exactly `eval_scoped`.
    fn eval_scoped_framed(
        session: &Session,
        expr: &Expr,
        tables: &[Arc<Table>],
        row: &[Datum],
    ) -> Result<bool> {
        crate::sql::eval_with(expr, &mut |name| {
            subquery::resolve_row(tables, row, session, name)
        })
    }

    /// Frame-aware `validate_scoped`: names resolving in scope or in any
    /// pushed frame pass (correlated references validate against the row
    /// environment they execute with). Ambiguity still errors immediately.
    fn validate_scoped_framed(
        session: &Session,
        expr: &Expr,
        tables: &[Arc<Table>],
    ) -> Result<()> {
        let mut cols = Vec::new();
        crate::sql::collect_columns(expr, &mut cols);
        for name in cols {
            match Self::resolve_scope(tables, name) {
                Ok(_) => {}
                Err(Error::ColumnNotFound(_)) | Err(Error::TableNotFound(_)) => {
                    subquery::resolve_frame(session, name).map(|_| ())?;
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Fail cleanly on unknown/ambiguous columns before filtering (so a bad
    /// WHERE/ON errors instead of silently matching nothing).
    fn validate_scoped(expr: &Expr, tables: &[Arc<Table>]) -> Result<()> {
        let mut cols = Vec::new();
        crate::sql::collect_columns(expr, &mut cols);
        for name in cols {
            Self::resolve_scope(tables, name)?;
        }
        Ok(())
    }

    fn exec_select_joined(
        &self,
        session: &mut Session,
        items: Vec<SelectItem>,
        from: &TableRef,
        joins: Vec<JoinClause>,
        selection: Option<Expr>,
        order_by: Vec<(String, bool)>,
        limit: Option<usize>,
        group_by: Vec<String>,
    ) -> Result<Output> {
        self.exec_select_joined_impl(
            session, items, from, joins, selection, order_by, limit, group_by, None,
        )
    }

    /// Resolve scope tables, extract pushable per-table predicates, and
    /// order the join by filtered sizes (selective tables first). Shared by
    /// the executor and EXPLAIN so the displayed plan is the executed plan.
    pub(super) fn plan_join(
        &self,
        session: &mut Session,
        from: &TableRef,
        joins: &[JoinClause],
        selection: Option<&Expr>,
    ) -> Result<JoinPlan> {
        // 1. Scope tables; one table name per query (self-joins need aliases,
        //    which do not exist yet). Derived sources resolve from the
        //    session materialization map (populated by `exec_select` setup).
        let mut tables: Vec<Arc<Table>> = vec![subquery::resolve_table_ref(self, session, from)?];
        let mut names: Vec<String> = vec![from.name().to_string()];
        for j in joins {
            if let TableRef::Table(t) = &j.table {
                if tables.iter().any(|x| x.def.name == *t || x.def.name.ends_with(&format!(".{t}"))) {
                    return Err(Error::NotSupported(
                        "self-joins need table aliases (unsupported)".into(),
                    ));
                }
            } else if let TableRef::Derived { alias, .. } = &j.table {
                if tables.iter().any(|x| x.def.name == *alias) {
                    return Err(Error::NotSupported(
                        "self-joins need table aliases (unsupported)".into(),
                    ));
                }
            }
            tables.push(subquery::resolve_table_ref(self, session, &j.table)?);
            names.push(j.table.name().to_string());
        }
        // 2. Per-table estimates: local predicates only (single-table
        //    conjuncts). Tables introduced by LEFT JOIN are never pushdown
        //    targets — their predicates must see NULL-padded rows post-join.
        let mut estimates = Vec::with_capacity(tables.len());
        for (ti, t) in tables.iter().enumerate() {
            let eligible = ti == 0 || joins[ti - 1].kind == JoinKind::Inner;
            let mut local = if eligible {
                local_predicate(selection, ti, &|n| Self::scope_table_idx(&tables, n).ok())
            } else {
                None
            };
            // Subquery conjuncts push only when every escaping reference
            // stays on this table (cross-table correlation must see joined
            // rows post-join; pushing it would mis-evaluate or error).
            if let Some(e) = local.as_ref() {
                if subquery::has_subquery(e)
                    && !subquery::pushable_to(self, session, e, ti, &tables)
                {
                    local = None;
                }
            }
            let stats = t.stats();
            let total = estimate_count(t);
            let sel = local
                .as_ref()
                .map(|e| selectivity(t, stats.as_ref(), e))
                .unwrap_or(1.0);
            estimates.push(TableEstimate {
                local,
                sel,
                total,
                filtered: (total * sel).round().max(0.0) as usize,
            });
        }
        // 3. Greedy order over filtered sizes: smallest ready INNER join
        //    first (LEFT = barrier).
        let sizes: Vec<usize> = estimates.iter().map(|e| e.filtered).collect();
        let order = order_joins(joins, &sizes, &|n| Self::scope_table_idx(&tables, n));
        Ok(JoinPlan { tables, names, estimates, order })
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn exec_select_joined_impl(
        &self,
        session: &mut Session,
        items: Vec<SelectItem>,
        from: &TableRef,
        joins: Vec<JoinClause>,
        selection: Option<Expr>,
        order_by: Vec<(String, bool)>,
        limit: Option<usize>,
        group_by: Vec<String>,
        mut capture: Option<&mut JoinCapture>,
    ) -> Result<Output> {
        let deadline = session.max_execution_time.map(|t| std::time::Instant::now() + t);
        if let Some(dl) = deadline {
            if std::time::Instant::now() > dl {
                return Err(Error::QueryTimeout);
            }
        }
        let plan = self.plan_join(session, from, &joins, selection.as_ref())?;
        let tables = plan.tables;
        // 2. Inputs: committed scans with the txn overlay, pre-filtered by
        //    each table's pushable local predicates (the full WHERE still
        //    applies post-join, so pushdown only ever skips doomed rows).
        //    Locals holding subqueries fold per row against the single-table
        //    scope (push-safety was vetted during planning).
        let mut inputs = Vec::with_capacity(tables.len());
        for (ti, t) in tables.iter().enumerate() {
            let mut rows = self.visible_rows(session, t, None)?;
            if let Some(local) = plan.estimates[ti].local.as_ref() {
                let scope = std::slice::from_ref(t);
                Self::validate_scoped_framed(session, local, scope)?;
                if subquery::has_subquery(local) {
                    rows = subquery::filter_with_subqueries(self, session, scope, rows, local)?;
                } else {
                    let mut kept = Vec::with_capacity(rows.len());
                    for r in rows {
                        if Self::eval_scoped_framed(session, local, scope, &r)? {
                            kept.push(r);
                        }
                    }
                    rows = kept;
                }
            }
            inputs.push(rows);
        }
        // 3. Execution layout from the plan order; `tables` keeps written
        //    order for output naming, `exec_*` owns row positions.
        let exec_order = plan.order;
        let exec_tables: Vec<Arc<Table>> =
            exec_order.iter().map(|&i| tables[i].clone()).collect();
        let exec_joins: Vec<JoinClause> =
            exec_order.iter().skip(1).map(|&ti| joins[ti - 1].clone()).collect();
        let mut exec_inputs: Vec<Vec<Vec<Datum>>> = exec_order
            .iter()
            .map(|&i| std::mem::take(&mut inputs[i]))
            .collect();
        if let Some(cap) = capture.as_mut() {
            cap.input_rows = exec_inputs.iter().map(|r| r.len()).collect();
        }
        // 4. Left-deep joins (hash on equi-keys, nested loop otherwise).
        let mut rows = std::mem::take(&mut exec_inputs[0]);
        for (ji, j) in exec_joins.iter().enumerate() {
            Self::validate_scoped(&j.on, &exec_tables[..ji + 2])?;
            let scope: &[Arc<Table>] = &exec_tables[..ji + 2];
            rows = Self::join_step(scope, &j.on, j.kind, rows, &exec_inputs[ji + 1], deadline)?;
        }
        // 5. WHERE over joined rows (subquery conjuncts fold per row
        //    against the joined scope; plain conjuncts take the fast path).
        //    Both validate frame-aware: correlated references resolve
        //    against pushed row environments.
        if let Some(sel) = selection.as_ref() {
            Self::validate_scoped_framed(session, sel, &exec_tables)?;
            if subquery::has_subquery(sel) {
                rows = subquery::filter_with_subqueries(self, session, &exec_tables, rows, sel)?;
            } else {
                let mut kept = Vec::with_capacity(rows.len());
                for r in rows {
                    if Self::eval_scoped_framed(session, sel, &exec_tables, &r)? {
                        kept.push(r);
                    }
                }
                rows = kept;
            }
        }
        // 6. Project or group. Scalar subqueries behave like per-row
        //    columns for the mixing rule; aggregates still need GROUP BY
        //    when mixed with row values.
        let has_agg = items.iter().any(|i| matches!(i, SelectItem::Aggregate { .. } | SelectItem::CountStar));
        let all_agg = !items.is_empty()
            && items.iter().all(|i| {
                matches!(
                    i,
                    SelectItem::Aggregate { .. } | SelectItem::CountStar | SelectItem::Subquery { .. }
                )
            });
        if !group_by.is_empty() {
            return self.exec_grouped(&items, &exec_tables, rows, &group_by, order_by, limit);
        }
        if has_agg && !all_agg {
            return Err(Error::NotSupported(
                "mixing aggregates with plain columns requires GROUP BY".into(),
            ));
        }
        if all_agg {
            let mut aggs = Vec::with_capacity(items.len());
            for item in &items {
                match item {
                    SelectItem::CountStar => aggs.push((AggSpec::Count, "COUNT(*)".into())),
                    SelectItem::Aggregate { func, column } => {
                        let idx = Self::resolve_scope(&exec_tables, column)?;
                        aggs.push((AggSpec::Agg(*func, idx), format!("{}({column})", func.name())));
                    }
                    SelectItem::Subquery { query, alias } => {
                        let d = subquery::eval_scalar_uncorrelated(self, session, query)?;
                        let name = alias.clone().unwrap_or_else(|| {
                            subquery::projection_scalar_name(query, None)
                        });
                        aggs.push((AggSpec::Scalar(d), name));
                    }
                    _ => unreachable!(),
                }
            }
            let mut out = Self::exec_aggregate_rows(&aggs, rows)?;
            Self::apply_order_limit(&mut out, order_by, limit)?;
            return Ok(out);
        }
        // Plain projection (star expands per scope, collisions qualified).
        // ORDER BY resolves against scope columns pre-projection (MySQL
        // allows ordering by non-selected columns), then LIMIT applies.
        if !order_by.is_empty() {
            let mut keys = Vec::with_capacity(order_by.len());
            for (col, _) in &order_by {
                keys.push(Self::resolve_scope(&exec_tables, col)?);
            }
            rows.sort_by(|a, b| {
                for (i, (_, desc)) in order_by.iter().enumerate() {
                    let ord = a[keys[i]].cmp(&b[keys[i]]);
                    if ord != std::cmp::Ordering::Equal {
                        return if *desc { ord.reverse() } else { ord };
                    }
                }
                std::cmp::Ordering::Equal
            });
        }
        let mut out_columns = Vec::new();
        /// Joined projection spec: scope positions, or per-row scalars.
        enum JProj {
            Col(usize),
            Subq(SelectStmt, Option<String>),
            Const(Datum),
        }
        let mut proj: Vec<JProj> = Vec::new();
        for item in &items {
            match item {
                SelectItem::Star => {
                    // Names follow written order; positions resolve in the
                    // execution layout (identical row content either way).
                    for (name, _) in Self::star_columns(&tables) {
                        let idx = Self::resolve_scope(&exec_tables, &name)?;
                        out_columns.push(name);
                        proj.push(JProj::Col(idx));
                    }
                }
                SelectItem::Column(c) => {
                    let idx = Self::resolve_scope(&exec_tables, c)?;
                    out_columns.push(Self::proj_output_name(item));
                    proj.push(JProj::Col(idx));
                }
                SelectItem::Literal(d) => {
                    out_columns.push(d.to_string());
                    proj.push(JProj::Const(d.clone()));
                }
                SelectItem::Subquery { query, alias } => {
                    out_columns.push(subquery::projection_scalar_name(query, alias.as_deref()));
                    proj.push(JProj::Subq((**query).clone(), alias.clone()));
                }
                _ => unreachable!(),
            }
        }
        let mut out_rows: Vec<Vec<Datum>> = Vec::with_capacity(rows.len());
        for r in rows {
            let mut out = Vec::with_capacity(proj.len());
            for p in &proj {
                match p {
                    JProj::Col(i) => out.push(r[*i].clone()),
                    JProj::Const(d) => out.push(d.clone()),
                    JProj::Subq(q, a) => {
                        let (d, _) = subquery::eval_projection_scalar(
                            self,
                            session,
                            &exec_tables,
                            &r,
                            q,
                            a.as_deref(),
                        )?;
                        out.push(d);
                    }
                }
            }
            out_rows.push(out);
        }
        if let Some(l) = limit {
            out_rows.truncate(l);
        }
        Ok(Output {
            columns: out_columns,
            rows: out_rows,
            message: "OK".into(),
        })
    }



    /// One left-deep join step over accumulated `left_rows` and the next
    /// table's `right_rows`. Equi-keys hash; everything else nested-loops.
    /// Output rows keep canonical layout (left columns, then right).
    fn join_step(
        tables: &[Arc<Table>],
        on: &Expr,
        kind: JoinKind,
        left_rows: Vec<Vec<Datum>>,
        right_rows: &[Vec<Datum>],
        deadline: Option<std::time::Instant>,
    ) -> Result<Vec<Vec<Datum>>> {
        let left_width: usize = tables[..tables.len() - 1]
            .iter()
            .map(|t| t.schema().columns.len())
            .sum();
        let scope = tables;
        if let Some((lk, rk)) = equi_join(on, left_width, &|n| Self::resolve_scope(scope, n)) {
            return Self::hash_join_step(
                tables,
                on,
                kind,
                lk,
                rk - left_width,
                left_rows,
                right_rows,
                deadline,
            );
        }
        Self::nested_loop_step(tables, on, kind, &left_rows, right_rows, deadline)
    }

    /// Nested-loop fallback: full cross product filtered by ON, with LEFT
    /// padding for unmatched left rows.
    fn nested_loop_step(
        tables: &[Arc<Table>],
        on: &Expr,
        kind: JoinKind,
        left_rows: &[Vec<Datum>],
        right_rows: &[Vec<Datum>],
        deadline: Option<std::time::Instant>,
    ) -> Result<Vec<Vec<Datum>>> {
        let right_arity = tables[tables.len() - 1].schema().columns.len();
        let mut next = Vec::new();
        for l in left_rows {
            if let Some(dl) = deadline {
                if std::time::Instant::now() > dl {
                    return Err(Error::QueryTimeout);
                }
            }
            let mut matched = false;
            for r in right_rows {
                let mut combined = Vec::with_capacity(l.len() + r.len());
                combined.extend_from_slice(l);
                combined.extend_from_slice(r);
                if Self::eval_scoped(on, tables, &combined)? {
                    next.push(combined);
                    matched = true;
                }
            }
            if !matched && kind == JoinKind::Left {
                let mut combined = l.clone();
                combined.extend(std::iter::repeat(Datum::Null).take(right_arity));
                next.push(combined);
            }
        }
        Ok(next)
    }

    /// In-memory hash join in O(N + M). `lk` indexes the accumulated left
    /// rows, `rrk` the right rows (local). INNER builds the smaller side;
    /// LEFT always builds right and streams left (order + padding). Hash
    /// hits still pass the full ON filter (residual predicates); NULL/NaN
    /// keys never match per SQL semantics.
    #[allow(clippy::too_many_arguments)]
    fn hash_join_step(
        tables: &[Arc<Table>],
        on: &Expr,
        kind: JoinKind,
        lk: usize,
        rrk: usize,
        left_rows: Vec<Vec<Datum>>,
        right_rows: &[Vec<Datum>],
        deadline: Option<std::time::Instant>,
    ) -> Result<Vec<Vec<Datum>>> {
        let right_arity = tables[tables.len() - 1].schema().columns.len();
        // (build rows, build key idx, probe rows, probe key idx, build-is-left)
        let build_left = kind == JoinKind::Inner && left_rows.len() < right_rows.len();
        let mut table: HashMap<JoinKey, Vec<usize>> = HashMap::new();
        if build_left {
            for (bi, b) in left_rows.iter().enumerate() {
                if let Some(dl) = deadline {
                    if std::time::Instant::now() > dl {
                        return Err(Error::QueryTimeout);
                    }
                }
                if let Some(k) = join_key(&b[lk]) {
                    table.entry(k).or_default().push(bi);
                }
            }
        } else {
            for (bi, b) in right_rows.iter().enumerate() {
                if let Some(dl) = deadline {
                    if std::time::Instant::now() > dl {
                        return Err(Error::QueryTimeout);
                    }
                }
                if let Some(k) = join_key(&b[rrk]) {
                    table.entry(k).or_default().push(bi);
                }
            }
        }
        let mut out = Vec::new();
        if build_left {
            for p in right_rows {
                if let Some(dl) = deadline {
                    if std::time::Instant::now() > dl {
                        return Err(Error::QueryTimeout);
                    }
                }
                if let Some(k) = join_key(&p[rrk]) {
                    if let Some(cands) = table.get(&k) {
                        for &bi in cands {
                            let b = &left_rows[bi];
                            let mut combined = Vec::with_capacity(b.len() + p.len());
                            combined.extend_from_slice(b);
                            combined.extend_from_slice(p);
                            if Self::eval_scoped(on, tables, &combined)? {
                                out.push(combined);
                            }
                        }
                    }
                }
            }
            return Ok(out);
        }
        for p in &left_rows {
            if let Some(dl) = deadline {
                if std::time::Instant::now() > dl {
                    return Err(Error::QueryTimeout);
                }
            }
            let mut matched = false;
            if let Some(k) = join_key(&p[lk]) {
                if let Some(cands) = table.get(&k) {
                    for &bi in cands {
                        let b = &right_rows[bi];
                        let mut combined = Vec::with_capacity(p.len() + b.len());
                        combined.extend_from_slice(p);
                        combined.extend_from_slice(b);
                        if Self::eval_scoped(on, tables, &combined)? {
                            out.push(combined);
                            matched = true;
                        }
                    }
                }
            }
            if !matched && kind == JoinKind::Left {
                let mut combined = p.clone();
                combined.extend(std::iter::repeat(Datum::Null).take(right_arity));
                out.push(combined);
            }
        }
        Ok(out)
    }

    /// GROUP BY: group rows by key values (sorted by key via BTreeMap),
    /// then emit group keys + per-group aggregates. Plain columns must be
    /// group keys; star is rejected.
    fn exec_grouped(
        &self,
        items: &[SelectItem],
        tables: &[Arc<Table>],
        rows: Vec<Vec<Datum>>,
        group_by: &[String],
        order_by: Vec<(String, bool)>,
        limit: Option<usize>,
    ) -> Result<Output> {
        let mut key_idx = Vec::with_capacity(group_by.len());
        for g in group_by {
            key_idx.push(Self::resolve_scope(tables, g)?);
        }
        // Validate + resolve projection items.
        enum GProj {
            Key(usize), // position in group_by
            Agg(Option<(AggFunc, usize)>),
            Const(Datum),
        }
        let mut out_columns = Vec::with_capacity(items.len());
        let mut projs = Vec::with_capacity(items.len());
        for item in items {
            match item {
                SelectItem::Star => {
                    return Err(Error::NotSupported(
                        "SELECT * with GROUP BY is not supported".into(),
                    ))
                }
                SelectItem::Column(c) => {
                    let idx = Self::resolve_scope(tables, c)?;
                    let pos = key_idx.iter().position(|&k| k == idx).ok_or_else(|| {
                        Error::NotSupported(format!(
                            "column '{}' must appear in GROUP BY or be aggregated",
                            Self::bare_name(c)
                        ))
                    })?;
                    out_columns.push(Self::proj_output_name(item));
                    projs.push(GProj::Key(pos));
                }
                SelectItem::CountStar => {
                    out_columns.push("COUNT(*)".into());
                    projs.push(GProj::Agg(None));
                }
                SelectItem::Aggregate { func, column } => {
                    let idx = Self::resolve_scope(tables, column)?;
                    out_columns.push(Self::proj_output_name(item));
                    projs.push(GProj::Agg(Some((*func, idx))));
                }
                // Row-independent constants are coherent per group.
                SelectItem::Literal(d) => {
                    out_columns.push(d.to_string());
                    projs.push(GProj::Const(d.clone()));
                }
                // Scalar subqueries have no group-row environment (only
                // uncorrelated values would be coherent); reject cleanly
                // instead of picking an arbitrary row's value.
                SelectItem::Subquery { .. } => {
                    return Err(Error::NotSupported(
                        "scalar subqueries are not supported with GROUP BY".into(),
                    ))
                }
            }
        }
        let mut groups: BTreeMap<Vec<Datum>, Vec<usize>> = BTreeMap::new();
        for (ri, r) in rows.iter().enumerate() {
            let key: Vec<Datum> = key_idx.iter().map(|&i| r[i].clone()).collect();
            groups.entry(key).or_default().push(ri);
        }
        // Build (output row, group key) pairs so ORDER BY can address output
        // columns and unprojected group keys alike.
        let mut paired: Vec<(Vec<Datum>, Vec<Datum>)> = Vec::with_capacity(groups.len());
        for (key, members) in &groups {
            let member_rows: Vec<Vec<Datum>> =
                members.iter().map(|&i| rows[i].clone()).collect();
            let mut out_row = Vec::with_capacity(projs.len());
            for p in &projs {
                match p {
                    GProj::Key(pos) => out_row.push(key[*pos].clone()),
                    GProj::Agg(None) => out_row.push(Datum::Int(member_rows.len() as i64)),
                    GProj::Agg(Some((func, idx))) => {
                        out_row.push(Self::compute_aggregate(*func, *idx, &member_rows)?)
                    }
                    GProj::Const(d) => out_row.push(d.clone()),
                }
            }
            paired.push((out_row, key.clone()));
        }
        if !order_by.is_empty() {
            enum OKey {
                Out(usize),
                Key(usize),
            }
            let mut oks = Vec::with_capacity(order_by.len());
            for (col, _) in &order_by {
                if let Ok(i) = Self::output_index(&out_columns, col) {
                    oks.push((OKey::Out(i), false));
                } else {
                    let pos = group_by
                        .iter()
                        .position(|g| g == col || Self::bare_name(g) == Self::bare_name(col))
                        .ok_or_else(|| Error::ColumnNotFound(col.clone()))?;
                    oks.push((OKey::Key(pos), false));
                }
            }
            // Attach directions (resolved above without them for clarity).
            for (i, (_, desc)) in order_by.iter().enumerate() {
                oks[i].1 = *desc;
            }
            paired.sort_by(|a, b| {
                for (k, desc) in &oks {
                    let ord = match k {
                        OKey::Out(i) => a.0[*i].cmp(&b.0[*i]),
                        OKey::Key(p) => a.1[*p].cmp(&b.1[*p]),
                    };
                    if ord != std::cmp::Ordering::Equal {
                        return if *desc { ord.reverse() } else { ord };
                    }
                }
                std::cmp::Ordering::Equal
            });
        }
        let mut out_rows: Vec<Vec<Datum>> = paired.into_iter().map(|(r, _)| r).collect();
        if let Some(l) = limit {
            out_rows.truncate(l);
        }
        Ok(Output {
            columns: out_columns,
            rows: out_rows,
            message: "OK".into(),
        })
    }

    /// Single output-column position by exact or bare-suffix name; ambiguity
    /// and absence are errors.
    fn output_index(columns: &[String], col: &str) -> Result<usize> {
        let hits: Vec<usize> = columns
            .iter()
            .enumerate()
            .filter(|(_, c)| *c == col || Self::bare_name(c) == Self::bare_name(col))
            .map(|(i, _)| i)
            .collect();
        if hits.len() != 1 {
            return Err(Error::ColumnNotFound(col.into()));
        }
        Ok(hits[0])
    }

    /// ORDER BY against output column names (exact, else bare-suffix match),
    /// lexicographic over multiple keys.
    fn apply_output_order(
        rows: &mut Vec<Vec<Datum>>,
        columns: &[String],
        order_by: Vec<(String, bool)>,
    ) -> Result<()> {
        if order_by.is_empty() {
            return Ok(());
        }
        let mut keys = Vec::with_capacity(order_by.len());
        for (col, _) in &order_by {
            keys.push(Self::output_index(columns, col)?);
        }
        rows.sort_by(|a, b| {
            for (i, (_, desc)) in order_by.iter().enumerate() {
                let ord = a[keys[i]].cmp(&b[keys[i]]);
                if ord != std::cmp::Ordering::Equal {
                    return if *desc { ord.reverse() } else { ord };
                }
            }
            std::cmp::Ordering::Equal
        });
        Ok(())
    }

    /// ORDER BY + LIMIT for single-row aggregate outputs.
    fn apply_order_limit(out: &mut Output, order_by: Vec<(String, bool)>, limit: Option<usize>) -> Result<()> {
        Self::apply_output_order(&mut out.rows, &out.columns.clone(), order_by)?;
        if let Some(l) = limit {
            out.rows.truncate(l);
        }
        Ok(())
    }

/// Fold one global aggregate over a column. Integer sums accumulate in i128
/// (falling back to Float past i64::MAX); any Float input widens to Float.
fn compute_aggregate(func: AggFunc, idx: usize, rows: &[Vec<Datum>]) -> Result<Datum> {
    match func {
        AggFunc::Sum => {
            let mut ints: i128 = 0;
            let mut floats = 0.0f64;
            let mut any_float = false;
            let mut n = 0u64;
            for r in rows {
                match &r[idx] {
                    Datum::Null => {}
                    Datum::Int(v) => {
                        ints += *v as i128;
                        n += 1;
                    }
                    Datum::Float(v) => {
                        floats += *v;
                        any_float = true;
                        n += 1;
                    }
                    other => {
                        return Err(Error::TypeMismatch {
                            expected: "numeric".into(),
                            got: other.type_name().into(),
                        })
                    }
                }
            }
            if n == 0 {
                return Ok(Datum::Null);
            }
            if any_float {
                Ok(Datum::Float(floats + ints as f64))
            } else if ints <= i64::MAX as i128 && ints >= i64::MIN as i128 {
                Ok(Datum::Int(ints as i64))
            } else {
                Ok(Datum::Float(ints as f64))
            }
        }
        AggFunc::Avg => {
            let mut sum = 0.0f64;
            let mut n = 0u64;
            for r in rows {
                match &r[idx] {
                    Datum::Null => {}
                    Datum::Int(v) => {
                        sum += *v as f64;
                        n += 1;
                    }
                    Datum::Float(v) => {
                        sum += *v;
                        n += 1;
                    }
                    other => {
                        return Err(Error::TypeMismatch {
                            expected: "numeric".into(),
                            got: other.type_name().into(),
                        })
                    }
                }
            }
            if n == 0 {
                Ok(Datum::Null)
            } else {
                Ok(Datum::Float(sum / n as f64))
            }
        }
        AggFunc::Min | AggFunc::Max => {
            let mut best: Option<&Datum> = None;
            for r in rows {
                let d = &r[idx];
                if matches!(d, Datum::Null) {
                    continue;
                }
                best = Some(match best {
                    None => d,
                    Some(b) => {
                        if func == AggFunc::Min {
                            if d < b { d } else { b }
                        } else if d > b {
                            d
                        } else {
                            b
                        }
                    }
                });
            }
            Ok(best.cloned().unwrap_or(Datum::Null))
        }
    }
}
}

