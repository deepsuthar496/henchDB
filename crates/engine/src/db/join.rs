//! Multi-table JOIN planning, execution, and GROUP BY aggregation.
//! Extracted from `db/query.rs` per the 1,500-line file ceiling rule.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use super::batch;
use super::cost::{estimate_count, local_predicate, selectivity};use super::plan::{equi_join, join_key, order_joins, JoinKey};
use super::query::AggSpec;
use super::subquery;
use super::{Database, Output, Session};
use crate::error::{Error, Result};
use crate::sql::{AggFunc, Expr, JoinClause, JoinKind, SelectItem, SelectStmt, TableRef};
use crate::table::Table;
use crate::types::Datum;

pub(super) struct JoinCapture {
    pub input_rows: Vec<usize>,
}

/// Grouped projection slot: a group-key position, an aggregate (None =
/// `COUNT(*)`), or a row-independent constant. Shared by the scalar
/// executor and the batch pushdown.
pub(super) enum GProj {
    Key(usize),
    Agg(Option<(AggFunc, usize)>),
    Const(Datum),
}

/// One global-aggregation output column: a row count, a column aggregate,
/// or a pre-folded scalar subquery value.

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

impl Database {
    /// Owning table + local column index for a concatenated-row position.
    /// Callers only pass indices from `resolve_scope`, so the fallback is
    /// unreachable in practice (no panic: first table, first column).
    pub(super) fn scope_owner(tables: &[Arc<Table>], idx: usize) -> (&Arc<Table>, usize) {
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
    pub(super) fn star_columns(tables: &[Arc<Table>]) -> Vec<(String, usize)> {
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
    pub(super) fn scope_table_idx(tables: &[Arc<Table>], name: &str) -> Result<usize> {
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
    pub(super) fn eval_scoped(expr: &Expr, tables: &[Arc<Table>], row: &[Datum]) -> Result<bool> {
        crate::sql::eval_with(expr, &mut |name| {
            Ok(row[Self::resolve_scope(tables, name)?].clone())
        })
    }

    /// Frame-aware `eval_scoped`: names failing the join scope fall back
    /// outward across correlation frames (subquery row environments).
    /// With empty frames this is exactly `eval_scoped`.
    pub(super) fn eval_scoped_framed(
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
    pub(super) fn validate_scoped_framed(
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
    pub(super) fn validate_scoped(expr: &Expr, tables: &[Arc<Table>]) -> Result<()> {
        let mut cols = Vec::new();
        crate::sql::collect_columns(expr, &mut cols);
        for name in cols {
            Self::resolve_scope(tables, name)?;
        }
        Ok(())
    }

    pub(super) fn exec_select_joined(
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
        let mut _join_res: Option<crate::db::mem_tracker::MemoryReservation> = None;
        for (ji, j) in exec_joins.iter().enumerate() {
            Self::validate_scoped(&j.on, &exec_tables[..ji + 2])?;
            let scope: &[Arc<Table>] = &exec_tables[..ji + 2];
            rows = Self::join_step(scope, &j.on, j.kind, rows, &exec_inputs[ji + 1], deadline)?;
            if let Some(limit) = session.max_intermediate_rows {
                if rows.len() > limit {
                    return Err(Error::ExecutionError(format!(
                        "query exceeded max_intermediate_rows limit ({limit}) during join"
                    )));
                }
            }
            let bytes: usize = rows.iter().map(|r| Self::estimate_row_bytes(r)).sum();
            _join_res = Some(session.mem_tracker.reserve_guard(bytes, "join")?);
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
            if let Some(limit) = session.max_intermediate_rows {
                if rows.len() > limit {
                    return Err(Error::ExecutionError(format!(
                        "query exceeded max_intermediate_rows limit ({limit}) during aggregation"
                    )));
                }
            }
            let bytes: usize = rows.iter().map(|r| Self::estimate_row_bytes(r)).sum();
            let _agg_res = session.mem_tracker.reserve_guard(bytes, "aggregation")?;
            // Single-source GROUP BY through the batch pushdown (captured
            // EXPLAIN ANALYZE plans and multi-table scopes stay scalar);
            // decline falls through to the legacy path below.
            if joins.is_empty() && exec_tables.len() == 1 && capture.is_none() {
                if let Some(out) = batch::try_grouped_agg(
                    self,
                    session,
                    &exec_tables[0],
                    selection.as_ref(),
                    &items,
                    &group_by,
                    order_by.clone(),
                    limit,
                )? {
                    return Ok(out);
                }
            }
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
            if let Some(limit) = session.max_intermediate_rows {
                if rows.len() > limit {
                    return Err(Error::ExecutionError(format!(
                        "query exceeded max_intermediate_rows limit ({limit}) during sort"
                    )));
                }
            }
            let bytes: usize = rows.iter().map(|r| Self::estimate_row_bytes(r)).sum();
            let _sort_res = session.mem_tracker.reserve_guard(bytes, "sort")?;
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
    pub(super) fn exec_grouped(
        &self,
        items: &[SelectItem],
        tables: &[Arc<Table>],
        rows: Vec<Vec<Datum>>,
        group_by: &[String],
        order_by: Vec<(String, bool)>,
        limit: Option<usize>,
    ) -> Result<Output> {
        let (out_columns, projs, key_idx) = Self::resolve_grouped_projs(items, tables, group_by)?;
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
        Self::assemble_grouped_output(out_columns, paired, group_by, order_by, limit)
    }

    /// Validate + resolve grouped projection items (shared by the scalar
    /// executor and the batch pushdown so both reject the same shapes).
    pub(super) fn resolve_grouped_projs(
        items: &[SelectItem],
        tables: &[Arc<Table>],
        group_by: &[String],
    ) -> Result<(Vec<String>, Vec<GProj>, Vec<usize>)> {
        let mut key_idx = Vec::with_capacity(group_by.len());
        for g in group_by {
            key_idx.push(Self::resolve_scope(tables, g)?);
        }
        // Validate + resolve projection items.
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
                SelectItem::SysFunc { .. } => {
                    return Err(Error::NotSupported(
                        "system functions are not supported with GROUP BY".into(),
                    ))
                }
            }
        }
        Ok((out_columns, projs, key_idx))
    }

    /// ORDER BY (over output columns and unprojected group keys alike) +
    /// LIMIT for grouped outputs (shared by scalar and batch paths).
    pub(super) fn assemble_grouped_output(
        out_columns: Vec<String>,
        paired: Vec<(Vec<Datum>, Vec<Datum>)>,
        group_by: &[String],
        order_by: Vec<(String, bool)>,
        limit: Option<usize>,
    ) -> Result<Output> {
        let mut paired = paired;
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
}
