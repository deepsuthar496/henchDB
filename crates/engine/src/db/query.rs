//! Query execution: SELECT planning/execution (single-table fast path,
//! multi-table nested-loop JOIN + GROUP BY), projection, ordering, and
//! aggregation helpers. `Database::describe` (prepare-time metadata) lives
//! here too.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use super::plan::{equi_join, join_key, JoinKey};
use super::{Database, Output, Session};
use crate::error::{Error, Result};
use crate::sql::{parse_sql, AggFunc, Expr, JoinClause, JoinKind, SelectItem, Statement};
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
                let mut tables: Vec<Arc<Table>> = vec![self.table(session, &from)?];
                for j in joins {
                    if tables.iter().any(|t| t.def.name == *j.table || t.def.name.ends_with(&format!(".{}", j.table))) {
                        return Err(Error::NotSupported(
                            "self-joins need table aliases (unsupported)".into(),
                        ));
                    }
                    tables.push(self.table(session, &j.table)?);
                }
                // For bare star columns, qualify only on cross-table collision.
                let mut cols = Vec::new();
                for item in &items {
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
                        SelectItem::Aggregate { func, column } => {
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
                    }
                }
                Ok(cols)
            }
            Statement::ShowTables => Ok(vec![("table".into(), ColumnType::Text)]),
            _ => Ok(vec![]),
        }
    }

    pub(super) fn exec_select(
        &self,
        session: &mut Session,
        items: Vec<SelectItem>,
        from: &str,
        joins: Vec<crate::sql::JoinClause>,
        selection: Option<Expr>,
        order_by: Vec<(String, bool)>,
        limit: Option<usize>,
        group_by: Vec<String>,
    ) -> Result<Output> {
        if joins.is_empty() && group_by.is_empty() {
            return self.exec_select_single(session, items, from, selection, order_by, limit);
        }
        self.exec_select_joined(session, items, from, joins, selection, order_by, limit, group_by)
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
    /// fast path without touching the index-aware machinery.
    fn strip_qualifiers(expr: &Expr, table: &str) -> Result<Expr> {
        match expr {
            Expr::Literal(d) => Ok(Expr::Literal(d.clone())),
            Expr::Column(name) => match name.split_once('.') {
                Some((t, c)) if t == table => Ok(Expr::Column(c.into())),
                Some(_) => Err(Error::ColumnNotFound(name.clone())),
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
            // tested expression.
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
        }
    }

    /// Single-table SELECT: index-aware fast path (unchanged hot path).
    fn exec_select_single(
        &self,
        session: &mut Session,
        items: Vec<SelectItem>,
        from: &str,
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
        let table_arc = self.table(session, from)?;
        let schema = table_arc.schema();
        let selection = selection.map(|s| Self::strip_qualifiers(&s, from)).transpose()?;

        let count_only = items.len() == 1 && matches!(items[0], SelectItem::CountStar);
        // Global aggregates (no GROUP BY): mixing aggregates with plain
        // columns is rejected like MySQL's ONLY_FULL_GROUP_BY.
        // (The sole-COUNT(*) case keeps its legacy path below.)
        let has_agg = items.iter().any(|i| matches!(i, SelectItem::Aggregate { .. } | SelectItem::CountStar));
        let all_agg = !items.is_empty()
            && items.iter().all(|i| matches!(i, SelectItem::Aggregate { .. } | SelectItem::CountStar));
        if has_agg && !all_agg {
            return Err(Error::NotSupported(
                "mixing aggregates with plain columns requires GROUP BY".into(),
            ));
        }
        let agg_only = all_agg && !count_only;
        let mut out_columns: Vec<String> = Vec::new();
        if !count_only && !agg_only {
            for item in &items {
                match item {
                    SelectItem::Star => out_columns.extend(schema.column_names()),
                    SelectItem::Column(c) => {
                        Self::single_col_idx(schema, from, c)?;
                        out_columns.push(Self::bare_name(c).into());
                    }
                    SelectItem::CountStar => unreachable!(),
                    SelectItem::Aggregate { func, column } => {
                        Self::single_col_idx(schema, from, column)?;
                        out_columns.push(format!("{}({column})", func.name()));
                    }
                }
            }
        }

        let mut rows = self.visible_rows(session, &table_arc, selection.as_ref())?;

        if agg_only {
            // ORDER BY / LIMIT do not apply to a global aggregate row.
            let mut aggs = Vec::with_capacity(items.len());
            for item in &items {
                match item {
                    SelectItem::CountStar => aggs.push((None, "COUNT(*)".into())),
                    SelectItem::Aggregate { func, column } => {
                        let idx = Self::single_col_idx(schema, from, column)?;
                        aggs.push((Some((*func, idx)), format!("{}({column})", func.name())));
                    }
                    _ => unreachable!(),
                }
            }
            return Self::exec_aggregate_rows(&aggs, rows);
        }

        if !order_by.is_empty() {
            let mut keys = Vec::with_capacity(order_by.len());
            for (col, _) in &order_by {
                keys.push(Self::single_col_idx(schema, from, col)?);
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

        let proj: Vec<usize> = items
            .iter()
            .flat_map(|i| match i {
                SelectItem::Star => (0..schema.columns.len()).collect::<Vec<usize>>(),
                SelectItem::Column(c) => vec![Self::single_col_idx(schema, from, c).unwrap()],
                SelectItem::CountStar => unreachable!(),
                SelectItem::Aggregate { .. } => unreachable!(),
            })
            .collect();

        let out_rows = rows
            .into_iter()
            .map(|r| proj.iter().map(|&i| r[i].clone()).collect())
            .collect();
        Ok(Output {
            columns: out_columns,
            rows: out_rows,
            message: "OK".into(),
        })
    }

    /// Global aggregation over filtered rows: one output row. NULLs are
    /// skipped (empty set: COUNT → 0, others → NULL). Non-numeric values in
    /// SUM/AVG are type errors; MIN/MAX use the total datum order.
    /// `aggs`: per-item (optional (func, column idx), output name).
    fn exec_aggregate_rows(aggs: &[(Option<(AggFunc, usize)>, String)], rows: Vec<Vec<Datum>>) -> Result<Output> {
        let mut out_row = Vec::with_capacity(aggs.len());
        let mut out_columns = Vec::with_capacity(aggs.len());
        for (agg, name) in aggs {
            out_columns.push(name.clone());
            match agg {
                None => out_row.push(Datum::Int(rows.len() as i64)),
                Some((func, idx)) => {
                    out_row.push(Self::compute_aggregate(*func, *idx, &rows)?);
                }
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
    fn resolve_scope(tables: &[Arc<Table>], name: &str) -> Result<usize> {
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

    /// Predicate evaluation over a joined row: same core as single-table
    /// `eval_expr` (shared via resolver), with scope-based resolution.
    fn eval_scoped(expr: &Expr, tables: &[Arc<Table>], row: &[Datum]) -> Result<bool> {
        crate::sql::eval_with(expr, &mut |name| {
            Ok(row[Self::resolve_scope(tables, name)?].clone())
        })
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
        from: &str,
        joins: Vec<JoinClause>,
        selection: Option<Expr>,
        order_by: Vec<(String, bool)>,
        limit: Option<usize>,
        group_by: Vec<String>,
    ) -> Result<Output> {
        // 1. Scope tables; one table name per query (self-joins need aliases,
        //    which do not exist yet).
        let deadline = session.max_execution_time.map(|t| std::time::Instant::now() + t);
        if let Some(dl) = deadline {
            if std::time::Instant::now() > dl {
                return Err(Error::QueryTimeout);
            }
        }
        let mut tables: Vec<Arc<Table>> = vec![self.table(session, from)?];
        for j in &joins {
            if tables.iter().any(|t| t.def.name == j.table || t.def.name.ends_with(&format!(".{}", j.table))) {
                return Err(Error::NotSupported(
                    "self-joins need table aliases (unsupported)".into(),
                ));
            }
            tables.push(self.table(session, &j.table)?);
        }
        // 2. Inputs: full scans with the txn overlay (no per-table filter —
        //    WHERE applies post-join).
        let mut inputs = Vec::with_capacity(tables.len());
        for t in &tables {
            inputs.push(self.visible_rows(session, t, None)?);
        }
        // 3. Left-deep joins (hash on equi-keys, nested loop otherwise).
        let mut rows = inputs[0].clone();
        for (ji, j) in joins.iter().enumerate() {
            Self::validate_scoped(&j.on, &tables[..ji + 2])?;
            let scope: &[Arc<Table>] = &tables[..ji + 2];
            rows = Self::join_step(scope, &j.on, j.kind, rows, &inputs[ji + 1], deadline)?;
        }
        // 4. WHERE over joined rows.
        if let Some(sel) = selection.as_ref() {
            Self::validate_scoped(sel, &tables)?;
            let mut kept = Vec::with_capacity(rows.len());
            for r in rows {
                if Self::eval_scoped(sel, &tables, &r)? {
                    kept.push(r);
                }
            }
            rows = kept;
        }
        // 5. Project or group.
        let has_agg = items.iter().any(|i| matches!(i, SelectItem::Aggregate { .. } | SelectItem::CountStar));
        let all_agg = !items.is_empty()
            && items.iter().all(|i| matches!(i, SelectItem::Aggregate { .. } | SelectItem::CountStar));
        if !group_by.is_empty() {
            return self.exec_grouped(&items, &tables, rows, &group_by, order_by, limit);
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
                    SelectItem::CountStar => aggs.push((None, "COUNT(*)".into())),
                    SelectItem::Aggregate { func, column } => {
                        let idx = Self::resolve_scope(&tables, column)?;
                        aggs.push((Some((*func, idx)), format!("{}({column})", func.name())));
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
                keys.push(Self::resolve_scope(&tables, col)?);
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
        let mut proj = Vec::new();
        for item in &items {
            match item {
                SelectItem::Star => {
                    for (name, idx) in Self::star_columns(&tables) {
                        out_columns.push(name);
                        proj.push(idx);
                    }
                }
                SelectItem::Column(c) => {
                    let idx = Self::resolve_scope(&tables, c)?;
                    out_columns.push(Self::proj_output_name(item));
                    proj.push(idx);
                }
                _ => unreachable!(),
            }
        }
        let mut out_rows: Vec<Vec<Datum>> = rows
            .into_iter()
            .map(|r| proj.iter().map(|&i| r[i].clone()).collect())
            .collect();
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
