//! EXPLAIN [ANALYZE] plan inspection (query-plan display lives here per the
//! 1,500-line file ceiling; execution stays in query.rs).

use super::cost::{choose_access_path, full_scan_cost, path_key, path_name,
    path_type, selectivity};
use super::query::JoinCapture;
use super::subquery;
use super::{Database, Output, Session};
use crate::error::{Error, Result};
use crate::sql::{Expr, JoinClause, SelectItem, Statement, TableRef};
use crate::types::Datum;

impl Database {
    /// `EXPLAIN [ANALYZE] SELECT`: one plan row per table (execution
    /// order for joins) without executing — or with a single timed
    /// execution filling actuals when `analyze` is set.
    pub(super) fn exec_explain(
        &self,
        session: &mut Session,
        analyze: bool,
        inner: &Statement,
    ) -> Result<Output> {
        let Statement::Select {
            items,
            from,
            joins,
            selection,
            order_by,
            limit,
            group_by,
        } = inner.clone()
        else {
            return Err(Error::NotSupported("EXPLAIN supports SELECT only".into()));
        };
        if joins.is_empty() && group_by.is_empty() {
            return self.explain_single(session, analyze, &from, selection, items, order_by, limit);
        }
        self.explain_join(session, analyze, &from, joins, selection, items, order_by, limit, group_by)
    }

    #[allow(clippy::too_many_arguments)]
    fn explain_single(
        &self,
        session: &mut Session,
        analyze: bool,
        from: &TableRef,
        selection: Option<Expr>,
        items: Vec<SelectItem>,
        order_by: Vec<(String, bool)>,
        limit: Option<usize>,
    ) -> Result<Output> {
        let saved = subquery::setup_derived(self, session, from, &[])?;
        let out = self.explain_single_resolved(session, analyze, from, selection, items, order_by, limit);
        subquery::teardown_derived(session, saved);
        out
    }

    #[allow(clippy::too_many_arguments)]
    fn explain_single_resolved(
        &self,
        session: &mut Session,
        analyze: bool,
        from: &TableRef,
        selection: Option<Expr>,
        items: Vec<SelectItem>,
        order_by: Vec<(String, bool)>,
        limit: Option<usize>,
    ) -> Result<Output> {
        let table_arc = subquery::resolve_table_ref(self, session, from)?;
        let display = from.name();
        let sel = selection.map(|s| Self::strip_qualifiers(&s, display)).transpose()?;
        let stats = table_arc.stats();
        let sel_rate = sel
            .as_ref()
            .map(|e| selectivity(&table_arc, stats.as_ref(), e))
            .unwrap_or(1.0);
        let choice = choose_access_path(&table_arc, sel.as_ref())?;
        let key = path_key(&table_arc, &choice.path)
            .map(Datum::Text)
            .unwrap_or(Datum::Null);
        let est = choice.est_rows.round().max(0.0) as i64;
        let mut row = vec![
            Datum::Text(display.to_string()),
            Datum::Text(path_name(&choice.path).into()),
            Datum::Text(path_type(&choice.path).into()),
            key,
            Datum::Int(est),
        ];
        let columns;
        if analyze {
            let t0 = std::time::Instant::now();
            let out = self.exec_select_single(session, items, from, sel, order_by, limit)?;
            let ms = t0.elapsed().as_secs_f64() * 1000.0;
            row.push(Datum::Int(out.rows.len() as i64));
            row.push(Datum::Text(format!("{:.2}", choice.cost)));
            row.push(Datum::Text(format!("{ms:.3}")));
            columns = vec![
                "table".into(),
                "access_path".into(),
                "type".into(),
                "key".into(),
                "rows_est".into(),
                "rows_act".into(),
                "cost".into(),
                "time_ms".into(),
            ];
        } else {
            row.push(Datum::Text(format!("{:.1}%", sel_rate * 100.0)));
            row.push(Datum::Text(format!("{:.2}", choice.cost)));
            columns = vec![
                "table".into(),
                "access_path".into(),
                "type".into(),
                "key".into(),
                "rows".into(),
                "filtered".into(),
                "cost".into(),
            ];
        }
        Ok(Output { columns, rows: vec![row], message: "OK".into() })
    }

    #[allow(clippy::too_many_arguments)]
    fn explain_join(
        &self,
        session: &mut Session,
        analyze: bool,
        from: &TableRef,
        joins: Vec<JoinClause>,
        selection: Option<Expr>,
        items: Vec<SelectItem>,
        order_by: Vec<(String, bool)>,
        limit: Option<usize>,
        group_by: Vec<String>,
    ) -> Result<Output> {
        let saved = subquery::setup_derived(self, session, from, &joins)?;
        let out = self.explain_join_planned(
            session, analyze, from, joins, selection, items, order_by, limit, group_by,
        );
        subquery::teardown_derived(session, saved);
        out
    }

    #[allow(clippy::too_many_arguments)]
    fn explain_join_planned(
        &self,
        session: &mut Session,
        analyze: bool,
        from: &TableRef,
        joins: Vec<JoinClause>,
        selection: Option<Expr>,
        items: Vec<SelectItem>,
        order_by: Vec<(String, bool)>,
        limit: Option<usize>,
        group_by: Vec<String>,
    ) -> Result<Output> {
        let plan = self.plan_join(session, from, &joins, selection.as_ref())?;
        // Optional timed execution for actuals (inputs in exec order).
        let mut capture = JoinCapture { input_rows: Vec::new() };
        let ms = if analyze {
            let t0 = std::time::Instant::now();
            self.exec_select_joined_impl(
                session,
                items,
                from,
                joins,
                selection,
                order_by,
                limit,
                group_by,
                Some(&mut capture),
            )?;
            Some(t0.elapsed().as_secs_f64() * 1000.0)
        } else {
            None
        };
        let mut rows = Vec::new();
        for (pos, &ti) in plan.order.iter().enumerate() {
            let e = &plan.estimates[ti];
            let access = if e.local.is_some() { "FILTERED SCAN" } else { "FULL SCAN" };
            let cost = full_scan_cost(e.total);
            let mut row = vec![
                Datum::Text(plan.names[ti].clone()),
                Datum::Text(access.into()),
                Datum::Text("ALL".into()),
                Datum::Null,
                Datum::Int(e.filtered as i64),
            ];
            if let Some(ms) = ms {
                let act = capture.input_rows.get(pos).copied().unwrap_or(0) as i64;
                row.push(Datum::Int(act));
                row.push(Datum::Text(format!("{cost:.2}")));
                row.push(Datum::Text(format!("{ms:.3}")));
            } else {
                row.push(Datum::Text(format!("{:.1}%", e.sel * 100.0)));
                row.push(Datum::Text(format!("{cost:.2}")));
            }
            rows.push(row);
        }
        let columns = if analyze {
            vec![
                "table".into(),
                "access_path".into(),
                "type".into(),
                "key".into(),
                "rows_est".into(),
                "rows_act".into(),
                "cost".into(),
                "time_ms".into(),
            ]
        } else {
            vec![
                "table".into(),
                "access_path".into(),
                "type".into(),
                "key".into(),
                "rows".into(),
                "filtered".into(),
                "cost".into(),
            ]
        };
        Ok(Output { columns, rows, message: "OK".into() })
    }
}
