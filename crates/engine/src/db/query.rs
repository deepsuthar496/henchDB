//! Query execution: SELECT planning/execution (single-table fast path,
//! multi-table nested-loop JOIN + GROUP BY), projection, ordering, and
//! aggregation helpers. `Database::describe` (prepare-time metadata) lives
//! here too.

use std::sync::Arc;

use super::subquery;
use super::sysviews;
use super::batch;

/// Per-table input actuals (execution order) for `EXPLAIN ANALYZE`.
pub(super) use super::join::JoinCapture;

/// One global-aggregation output column: a row count, a column aggregate,
/// or a pre-folded scalar subquery value.
pub(super) enum AggSpec {
    Count,
    Agg(AggFunc, usize),
    Scalar(Datum),
}
use super::{Database, Output, Session};
use crate::error::{Error, Result};
use crate::sql::{parse_sql, AggFunc, Expr, JoinClause, SelectItem, Statement};
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
            Statement::ShowGrants { .. } => Ok(vec![("Grants".into(), ColumnType::Text)]),
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
    pub(super) fn describe_select(
        &self,
        session: &Session,
        tmp: &mut Session,
        items: &[SelectItem],
        from: &TableRef,
        joins: &[JoinClause],
    ) -> Result<Vec<(String, ColumnType)>> {
        // FROM-less SELECT describes its row-independent items directly.
        if matches!(from, TableRef::Empty) {
            if !joins.is_empty() {
                return Err(Error::NotSupported("JOIN requires FROM".into()));
            }
            return sysviews::describe_nofrom(self, session, tmp, items);
        }
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
                    cols.push(sysviews::describe_subquery_item(
                        self, session, tmp, query, alias,
                    )?)
                }
                SelectItem::SysFunc { name, alias } => {
                    if sysviews::eval_sysfunc(session, name).is_none() {
                        return Err(Error::NotSupported(format!("unknown function '{name}'")));
                    }
                    cols.push((
                        alias.clone().unwrap_or_else(|| name.to_ascii_lowercase()),
                        ColumnType::Text,
                    ));
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
        if matches!(from, TableRef::Empty) {
            if !joins.is_empty() {
                return Err(Error::NotSupported("JOIN requires FROM".into()));
            }
            return sysviews::exec_select_nofrom(self, session, items, limit);
        }
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
    pub(super) fn bare_name(name: &str) -> &str {
        name.rsplit('.').next().unwrap_or(name)
    }

    /// Output header for one projection item (shared by executor + describe).
    pub(super) fn proj_output_name(item: &SelectItem) -> String {
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
            SelectItem::SysFunc { name, alias } => {
                alias.clone().unwrap_or_else(|| name.to_ascii_lowercase())
            }
        }
    }

    /// Resolve `col` or `table.col` in a single-table context. A qualifier
    /// naming another table is an error here; joins resolve those instead.
    fn single_col_idx(schema: &Schema, table: &str, name: &str) -> Result<usize> {
        match name.split_once('.') {
            Some((t, c)) => {
                let matches = t == table || table.rsplit('.').next() == Some(t);
                if matches {
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
                Some((t, c)) if t == table || table.rsplit('.').next() == Some(t) => {
                    Ok(Expr::Column(c.into()))
                }
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
            SysFunc(String),
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
                    SelectItem::SysFunc { name, alias } => {
                        if sysviews::eval_sysfunc(session, name).is_none() {
                            return Err(Error::NotSupported(format!("unknown function '{name}'")));
                        }
                        out_columns.push(alias.clone().unwrap_or_else(|| name.to_ascii_lowercase()));
                        proj.push(ProjSpec::SysFunc(name.clone()));
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
        // Sole-COUNT(*) fast path through the batch executor (falls back
        // below on Ok(None)); scalar-filtered rows are only sourced when
        // the batch path declines, so success never pays for both.
        if count_only && sub_sel.is_none() {
            let aggs = vec![(AggSpec::Count, "COUNT(*)".into())];
            if batch::applicable(session, &table_arc, plain_sel.as_ref())? {
                if let Some(out) =
                    batch::try_global_agg(self, session, &table_arc, plain_sel.as_ref(), &aggs)?
                {
                    return Ok(out);
                }
            }
        }
        // Global-aggregate candidates defer scalar sourcing the same way:
        // the batch runner sources its own rows on success.
        let batch_candidate = agg_only
            && sub_sel.is_none()
            && batch::applicable(session, &table_arc, plain_sel.as_ref())?;
        let mut rows = if batch_candidate {
            Vec::new()
        } else {
            self.visible_rows(session, &table_arc, plain_sel.as_ref())?
        };
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
                    SelectItem::SysFunc { name, alias } => {
                        let d = sysviews::eval_sysfunc(session, name)
                            .ok_or_else(|| Error::NotSupported(format!("unknown function '{name}'")))?;
                        let nm = alias.clone().unwrap_or_else(|| name.to_ascii_lowercase());
                        aggs.push((AggSpec::Scalar(d), nm));
                    }
                    _ => unreachable!(),
                }
            }
            // Morsel-driven fast path (re-sources inside); on decline the
            // scalar-filtered rows are sourced here for the legacy path.
            if batch_candidate {
                if let Some(out) =
                    batch::try_global_agg(self, session, &table_arc, plain_sel.as_ref(), &aggs)?
                {
                    return Ok(out);
                }
                rows = self.visible_rows(session, &table_arc, plain_sel.as_ref())?;
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
                    ProjSpec::SysFunc(name) => {
                        out.push(sysviews::eval_sysfunc(session, name).unwrap_or(Datum::Null));
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
    pub(super) fn exec_aggregate_rows(aggs: &[(AggSpec, String)], rows: Vec<Vec<Datum>>) -> Result<Output> {
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


    /// Single output-column position by exact or bare-suffix name; ambiguity
    /// and absence are errors.
    pub(super) fn output_index(columns: &[String], col: &str) -> Result<usize> {
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
    pub(super) fn apply_order_limit(out: &mut Output, order_by: Vec<(String, bool)>, limit: Option<usize>) -> Result<()> {
        Self::apply_output_order(&mut out.rows, &out.columns.clone(), order_by)?;
        if let Some(l) = limit {
            out.rows.truncate(l);
        }
        Ok(())
    }

/// Fold one global aggregate over a column. Integer sums accumulate in i128
/// (falling back to Float past i64::MAX); any Float input widens to Float.
pub(super) fn compute_aggregate(func: AggFunc, idx: usize, rows: &[Vec<Datum>]) -> Result<Datum> {
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

