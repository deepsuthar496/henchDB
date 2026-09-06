//! Cost-based optimizer foundation: selectivity estimation, access-path
//! costing, and per-table predicate extraction for join planning.
//!
//! The model is deliberately simple and transparent (Postgres-style cost
//! weights, independence assumption for AND/OR): estimates only ever change
//! *which* correct plan runs, never the result — the executor re-filters
//! every row against the full predicate on all paths.
//!
//! Conventions:
//! - Selectivity 1.0 = keeps every row, 0.0 = keeps none.
//! - Costs are abstract units; only relative ordering matters.
//! - Missing statistics degrade to per-operator defaults (documented below),
//!   so unanalyzed tables still get sane plans.

use super::plan::{access_path, AccessPath};
use crate::error::Result;
use crate::sql::{CmpOp, Expr};
use crate::stats::{ColumnStats, TableStats};
use crate::table::Table;
use crate::types::Datum;

// ---------------------------------------------------------------------------
// Cost weights (Postgres-style abstract units)
// ---------------------------------------------------------------------------

pub const CPU_TUPLE_COST: f64 = 0.01;
pub const INDEX_PAGE_COST: f64 = 1.0;
pub const RANDOM_PAGE_COST: f64 = 4.0;
pub const SEQ_PAGE_COST: f64 = 1.0;
/// Average rows per page for I/O amortization.
pub const ROWS_PER_PAGE: f64 = 100.0;

// ---------------------------------------------------------------------------
// Selectivity defaults (used when no statistics exist)
// ---------------------------------------------------------------------------

/// Equality / IN-member selectivity without stats.
pub const DEFAULT_EQ_SEL: f64 = 0.05;
/// Range selectivity without stats (or over non-numeric bounds).
pub const DEFAULT_RANGE_SEL: f64 = 0.33;
/// LIKE selectivity without a usable prefix range.
pub const DEFAULT_LIKE_SEL: f64 = 0.1;

// ---------------------------------------------------------------------------
// Row-count estimates
// ---------------------------------------------------------------------------

/// Estimated live rows: analyzed count when available, else the tree's O(1)
/// entry counter (exact for committed state; staged txn writes excluded).
pub(crate) fn estimate_count(table: &Table) -> f64 {
    match table.stats() {
        Some(s) => s.row_count as f64,
        None => table.tree().entry_count() as f64,
    }
}

// ---------------------------------------------------------------------------
// Selectivity
// ---------------------------------------------------------------------------

fn num_value(d: &Datum) -> Option<f64> {
    match d {
        Datum::Int(v) => Some(*v as f64),
        Datum::Float(v) if !v.is_nan() => Some(*v),
        Datum::DateTime(v) => Some(*v as f64),
        _ => None,
    }
}

/// Equality selectivity of `col = lit` against column stats.
fn eq_selectivity(stats: Option<&ColumnStats>, lit: &Datum) -> f64 {
    if matches!(lit, Datum::Null) {
        return 0.0; // NULL never matches
    }
    let Some(cs) = stats else {
        return DEFAULT_EQ_SEL;
    };
    if cs.total == 0 {
        return 1.0; // empty table: keep (vacuous) estimate stable
    }
    // MCV hit carries the true frequency (handles skew exactly).
    for (d, count) in &cs.mcv {
        if d == lit {
            return (*count as f64 / cs.total as f64).clamp(0.0, 1.0);
        }
    }
    if cs.distinct_count > 0 {
        (1.0 / cs.distinct_count as f64).clamp(0.0, 1.0)
    } else {
        DEFAULT_EQ_SEL
    }
}

/// Range fraction of `col` below/above `lit` by linear interpolation over
/// [min, max]; `below == true` measures `col < lit`.
fn range_fraction(stats: Option<&ColumnStats>, lit: &Datum, below: bool) -> f64 {
    let (Some(lit_v), Some(min), Some(max)) = (
        num_value(lit),
        stats.and_then(|c| c.min.as_ref()).and_then(num_value),
        stats.and_then(|c| c.max.as_ref()).and_then(num_value),
    ) else {
        return DEFAULT_RANGE_SEL;
    };
    if !(max > min) {
        return DEFAULT_RANGE_SEL;
    }
    let frac = if below {
        (lit_v - min) / (max - min)
    } else {
        (max - lit_v) / (max - min)
    };
    frac.clamp(0.01, 0.99)
}

/// Resolve a column reference to (stats, ) for one table. Qualified names
/// must name this table; bare names resolve by schema. Returns None for
/// columns of other tables (join predicates are not single-table).
fn col_stats<'a>(
    table: &Table,
    stats: Option<&'a TableStats>,
    name: &str,
) -> Option<Option<&'a ColumnStats>> {
    let bare = match name.split_once('.') {
        Some((t, c)) => {
            let def = &table.def.name;
            let simple = def.split('.').last().unwrap_or(def);
            if t != def && t != simple {
                return None;
            }
            c
        }
        None => name,
    };
    if table.schema().index_of(bare).is_none() {
        return None;
    }
    Some(stats.and_then(|s| s.columns.get(bare)))
}

/// Compare-expression selectivity for column-vs-literal predicates.
/// `flip` handles literal-first (`5 < id`) comparisons.
fn cmp_selectivity(
    table: &Table,
    stats: Option<&TableStats>,
    col: &str,
    op: CmpOp,
    lit: &Datum,
    flip: bool,
) -> f64 {
    let Some(cs) = col_stats(table, stats, col) else {
        return DEFAULT_EQ_SEL;
    };
    // Normalize to column-first orientation.
    let op = if flip {
        match op {
            CmpOp::Eq => CmpOp::Eq,
            CmpOp::Ne => CmpOp::Ne,
            CmpOp::Lt => CmpOp::Gt,
            CmpOp::Le => CmpOp::Ge,
            CmpOp::Gt => CmpOp::Lt,
            CmpOp::Ge => CmpOp::Le,
        }
    } else {
        op
    };
    match op {
        CmpOp::Eq => eq_selectivity(cs, lit),
        CmpOp::Ne => 1.0 - eq_selectivity(cs, lit),
        CmpOp::Lt => range_fraction(cs, lit, true),
        CmpOp::Le => range_fraction(cs, lit, true),
        CmpOp::Gt => range_fraction(cs, lit, false),
        CmpOp::Ge => range_fraction(cs, lit, false),
    }
}

/// Fraction of rows of `table` kept by `expr` (single-table predicates; any
/// predicate touching another table degrades to a neutral estimate — callers
/// extract per-table slices with `local_predicate` first).
pub(crate) fn selectivity(table: &Table, stats: Option<&TableStats>, expr: &Expr) -> f64 {
    match expr {
        Expr::And(a, b) => {
            selectivity(table, stats, a) * selectivity(table, stats, b)
        }
        Expr::Or(a, b) => {
            let (x, y) = (selectivity(table, stats, a), selectivity(table, stats, b));
            x + y - x * y
        }
        Expr::Not(e) => 1.0 - selectivity(table, stats, e),
        Expr::Cmp { left, op, right } => match (left.as_ref(), right.as_ref()) {
            (Expr::Column(c), Expr::Literal(l)) => {
                cmp_selectivity(table, stats, c, op.clone(), l, false)
            }
            (Expr::Literal(l), Expr::Column(c)) => {
                cmp_selectivity(table, stats, c, op.clone(), l, true)
            }
            _ => DEFAULT_EQ_SEL,
        },
        Expr::In { expr, values, negated } => {
            let Expr::Column(c) = expr.as_ref() else {
                return DEFAULT_EQ_SEL;
            };
            let per: f64 = values
                .iter()
                .filter(|v| !matches!(v, Datum::Null))
                .map(|v| cmp_selectivity(table, stats, c, CmpOp::Eq, v, false))
                .sum();
            let sel = per.min(1.0);
            if *negated { 1.0 - sel } else { sel }
        }
        Expr::Between { expr, lo, hi, negated } => {
            let Expr::Column(c) = expr.as_ref() else {
                return DEFAULT_RANGE_SEL;
            };
            let cs = col_stats(table, stats, c).flatten();
            let sel = match (
                num_value(lo),
                num_value(hi),
                cs.and_then(|s| s.min.as_ref()).and_then(num_value),
                cs.and_then(|s| s.max.as_ref()).and_then(num_value),
            ) {
                (Some(l), Some(h), Some(min), Some(max)) if max > min && h >= l => {
                    ((h - l) / (max - min)).clamp(0.0, 1.0)
                }
                _ => DEFAULT_RANGE_SEL,
            };
            if *negated { 1.0 - sel } else { sel }
        }
        Expr::Like { expr, negated, .. } => {
            // Prefix ranges are priced as index bounds elsewhere; the
            // residual selectivity is a flat guess either way.
            let sel = match expr.as_ref() {
                Expr::Column(c) if col_stats(table, stats, c).is_some() => DEFAULT_LIKE_SEL,
                _ => DEFAULT_LIKE_SEL,
            };
            if *negated { 1.0 - sel } else { sel }
        }
        // Bare column / literal predicates (truthiness): neutral for
        // columns, exact for literals.
        Expr::Column(_) => 0.5,
        Expr::Literal(l) => match l {
            Datum::Null | Datum::Bool(false) => 0.0,
            Datum::Int(0) => 0.0,
            Datum::Float(v) if *v == 0.0 => 0.0,
            _ => 1.0,
        },
        // Subquery predicates: neutral membership/existence guesses (the
        // executor folds them exactly; costing only needs stability).
        Expr::InSubquery { .. } => 0.1,
        Expr::Exists { .. } => 0.5,
        Expr::ScalarSubquery(_) => 0.5,
    }
}

// ---------------------------------------------------------------------------
// Per-table predicate extraction (join pushdown + filtered sizes)
// ---------------------------------------------------------------------------

/// True when every column of `expr` belongs to table `ord` (positions
/// resolved over `tables`).
fn expr_local_to(expr: &Expr, ord: usize, resolve: &dyn Fn(&str) -> Option<usize>) -> bool {
    let mut cols = Vec::new();
    crate::sql::collect_columns(expr, &mut cols);
    if cols.is_empty() {
        return false;
    }
    cols.iter().all(|c| resolve(c) == Some(ord))
}

/// Conjunction of the WHERE clauses touching only table `ord` (bare and
/// qualified names both count). Returns None when nothing is pushable.
/// `tables` are scope-ordered; `resolve` maps a column name to its owning
/// table ordinal.
pub(crate) fn local_predicate(
    selection: Option<&Expr>,
    ord: usize,
    resolve: &dyn Fn(&str) -> Option<usize>,
) -> Option<Expr> {
    let sel = selection?;
    // Flatten the AND spine; each conjunct is independently pushable.
    let mut conjuncts = Vec::new();
    let mut stack = vec![sel];
    while let Some(e) = stack.pop() {
        match e {
            Expr::And(a, b) => {
                stack.push(a);
                stack.push(b);
            }
            other => conjuncts.push(other),
        }
    }
    let mut kept: Vec<Expr> = conjuncts
        .into_iter()
        .filter(|e| expr_local_to(e, ord, resolve))
        .cloned()
        .collect();
    if kept.is_empty() {
        return None;
    }
    let mut out = kept.pop().unwrap();
    while let Some(e) = kept.pop() {
        out = Expr::And(Box::new(e), Box::new(out));
    }
    Some(out)
}

// ---------------------------------------------------------------------------
// Access-path costing
// ---------------------------------------------------------------------------

/// Chosen plan for one table: the access path plus its estimates.
#[derive(Debug)]
pub(crate) struct Choice {
    pub path: AccessPath,
    pub est_rows: f64,
    pub cost: f64,
}

/// Cost the executor would pay to return `est` rows via the cheapest
/// available route; shared by the SecondaryIndex vs FullScan decision.
pub(crate) fn full_scan_cost(total_rows: f64) -> f64 {
    total_rows * (SEQ_PAGE_COST / ROWS_PER_PAGE + CPU_TUPLE_COST)
}

/// Pick the cheapest correct access path. Heuristic candidates come from
/// `plan::access_path`; the CBO only ever *downgrades* a secondary-index
/// path to a full scan when random PK dereferencing costs more than
/// scanning (PK paths are always kept: bounded single descent).
pub(crate) fn choose_access_path(table: &Table, selection: Option<&Expr>) -> Result<Choice> {
    let stats = table.stats();
    let total = estimate_count(table);
    let sel = selection.map(|e| selectivity(table, stats.as_ref(), e)).unwrap_or(1.0);
    let est_rows = total * sel;
    let full = full_scan_cost(total);
    let path = access_path(table, selection)?;
    let (path, cost) = match &path {
        AccessPath::Point(_) => (path, INDEX_PAGE_COST + CPU_TUPLE_COST),
        AccessPath::PkIn(vals) => {
            let c = vals.len() as f64 * (INDEX_PAGE_COST + CPU_TUPLE_COST);
            (path, c)
        }
        AccessPath::Range { .. } => {
            let c = INDEX_PAGE_COST + est_rows * (SEQ_PAGE_COST / ROWS_PER_PAGE + CPU_TUPLE_COST);
            (path, c)
        }
        AccessPath::SecondaryIndex { .. } => {
            let c = INDEX_PAGE_COST
                + est_rows * (INDEX_PAGE_COST / ROWS_PER_PAGE + CPU_TUPLE_COST + RANDOM_PAGE_COST);
            if full <= c {
                (AccessPath::FullScan, full)
            } else {
                (path, c)
            }
        }
        AccessPath::SecIn { values, .. } => {
            let n = values.len() as f64;
            let c = INDEX_PAGE_COST
                + n * (INDEX_PAGE_COST / ROWS_PER_PAGE + CPU_TUPLE_COST + RANDOM_PAGE_COST);
            if full <= c {
                (AccessPath::FullScan, full)
            } else {
                (path, c)
            }
        }
        AccessPath::FullScan => (path, full),
    };
    Ok(Choice { path, est_rows, cost })
}

// ---------------------------------------------------------------------------
// EXPLAIN presentation
// ---------------------------------------------------------------------------

/// Human-readable access-path name for EXPLAIN.
pub(crate) fn path_name(path: &AccessPath) -> &'static str {
    match path {
        AccessPath::Point(_) => "PK POINT",
        AccessPath::PkIn(_) => "PK IN",
        AccessPath::Range { .. } => "PK RANGE",
        AccessPath::SecondaryIndex { .. } => "SEC SEEK",
        AccessPath::SecIn { .. } => "SEC IN",
        AccessPath::FullScan => "FULL SCAN",
    }
}

/// MySQL-style `type` column for EXPLAIN.
pub(crate) fn path_type(path: &AccessPath) -> &'static str {
    match path {
        AccessPath::Point(_) => "const",
        AccessPath::PkIn(_) | AccessPath::SecIn { .. } => "range",
        AccessPath::Range { .. } => "range",
        AccessPath::SecondaryIndex { .. } => "ref",
        AccessPath::FullScan => "ALL",
    }
}

/// Index/column driving the path (`None` → NULL key column).
pub(crate) fn path_key(table: &Table, path: &AccessPath) -> Option<String> {
    let schema = table.schema();
    match path {
        AccessPath::Point(_)
        | AccessPath::PkIn(_)
        | AccessPath::Range { .. } => Some(schema.columns[schema.pk_idx].name.clone()),
        AccessPath::SecondaryIndex { col_idx, .. } | AccessPath::SecIn { col_idx, .. } => {
            Some(schema.columns[*col_idx].name.clone())
        }
        AccessPath::FullScan => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stats::analyze_rows;
    use crate::table::{ColumnDef, Schema};
    use crate::types::ColumnType;

    fn schema() -> Schema {
        Schema {
            columns: vec![
                ColumnDef { name: "id".into(), ctype: ColumnType::Int, nullable: false, auto_increment: false, default_value: None },
                ColumnDef { name: "grp".into(), ctype: ColumnType::Int, nullable: true, auto_increment: false, default_value: None },
                ColumnDef { name: "score".into(), ctype: ColumnType::Float, nullable: true, auto_increment: false, default_value: None },
            ],
            pk_idx: 0,
        }
    }

    fn tbl(rows: &[Vec<Datum>]) -> Table {
        use crate::table::TableDef;
        let t = Table::new(TableDef {
            name: "t".into(),
            schema: schema(),
            indexes: vec![],
            foreign_keys: vec![],
            stats: None,
        });
        for (i, r) in rows.iter().enumerate() {
            let key = crate::types::encode_key(&Datum::Int(i as i64)).unwrap();
            t.restore_kv(&key, &Table::encode_row(r)).unwrap();
        }
        t
    }

    fn stats_of(rows: &[Vec<Datum>]) -> TableStats {
        analyze_rows(&schema(), rows)
    }

    fn eq_col(col: &str, v: Datum) -> Expr {
        Expr::Cmp {
            left: Box::new(Expr::Column(col.into())),
            op: CmpOp::Eq,
            right: Box::new(Expr::Literal(v)),
        }
    }

    #[test]
    fn selectivity_equality_uses_distinct_and_mcv() {
        // 100 rows, grp in 0..10 uniformly.
        let rows: Vec<Vec<Datum>> = (0..100)
            .map(|i| vec![Datum::Int(i), Datum::Int(i % 10), Datum::Null])
            .collect();
        let t = tbl(&rows);
        let s = stats_of(&rows);
        let sel = selectivity(&t, Some(&s), &eq_col("grp", Datum::Int(3)));
        assert!((sel - 0.1).abs() < 1e-9, "sel={sel}");
        // Skewed: 90 x grp=1.
        let mut rows2: Vec<Vec<Datum>> = (0..90)
            .map(|i| vec![Datum::Int(i), Datum::Int(1), Datum::Null])
            .collect();
        for i in 90..100 {
            rows2.push(vec![Datum::Int(i), Datum::Int(i), Datum::Null]);
        }
        let t2 = tbl(&rows2);
        let s2 = stats_of(&rows2);
        let hot = selectivity(&t2, Some(&s2), &eq_col("grp", Datum::Int(1)));
        assert!((hot - 0.9).abs() < 1e-9, "hot={hot}");
        let cold = selectivity(&t2, Some(&s2), &eq_col("grp", Datum::Int(95)));
        // 95 sits inside the MCV cap (90..96 after the hot value): exact 1/100.
        assert!((cold - 0.01).abs() < 1e-9, "cold={cold}");
        // 99 falls outside the cap: uniform remainder 1/distinct.
        let tail = selectivity(&t2, Some(&s2), &eq_col("grp", Datum::Int(99)));
        assert!((tail - 1.0 / 11.0).abs() < 1e-9, "tail={tail}");
        // No stats: default.
        let d = selectivity(&t, None, &eq_col("grp", Datum::Int(3)));
        assert_eq!(d, DEFAULT_EQ_SEL);
        // NULL literal matches nothing.
        let n = selectivity(&t, Some(&s), &eq_col("grp", Datum::Null));
        assert_eq!(n, 0.0);
    }

    #[test]
    fn selectivity_ranges_between_in_and_logic() {
        let rows: Vec<Vec<Datum>> = (0..100)
            .map(|i| vec![Datum::Int(i), Datum::Int(i), Datum::Float(i as f64)])
            .collect();
        let t = tbl(&rows);
        let s = stats_of(&rows);
        let lt = |col: &str, op: CmpOp, v: Datum| Expr::Cmp {
            left: Box::new(Expr::Column(col.into())),
            op,
            right: Box::new(Expr::Literal(v)),
        };
        // id in 0..100: id < 50 keeps ~half.
        let sel = selectivity(&t, Some(&s), &lt("id", CmpOp::Lt, Datum::Int(50)));
        assert!((sel - 50.0 / 99.0).abs() < 1e-9, "sel={sel}");
        let sel = selectivity(&t, Some(&s), &lt("id", CmpOp::Gt, Datum::Int(90)));
        assert!((sel - 9.0 / 99.0).abs() < 1e-9, "sel={sel}");
        // BETWEEN 10 AND 20: width 10/99.
        let between = Expr::Between {
            expr: Box::new(Expr::Column("id".into())),
            lo: Datum::Int(10),
            hi: Datum::Int(20),
            negated: false,
        };
        let sel = selectivity(&t, Some(&s), &between);
        assert!((sel - 10.0 / 99.0).abs() < 1e-9, "sel={sel}");
        // IN with 3 uniform members ≈ 3 * 0.01.
        let inn = Expr::In {
            expr: Box::new(Expr::Column("grp".into())),
            values: vec![Datum::Int(1), Datum::Int(2), Datum::Int(3)],
            negated: false,
        };
        let sel = selectivity(&t, Some(&s), &inn);
        assert!((sel - 0.03).abs() < 1e-9, "sel={sel}");
        // AND multiplies, OR adds-minus-product, NOT complements.
        let a = eq_col("grp", Datum::Int(1)); // 0.01
        let b = lt("id", CmpOp::Lt, Datum::Int(50)); // ~0.505
        let and = Expr::And(Box::new(a.clone()), Box::new(b.clone()));
        let s_and = selectivity(&t, Some(&s), &and);
        assert!((s_and - 0.01 * (50.0 / 99.0)).abs() < 1e-9, "and={s_and}");
        let or = Expr::Or(Box::new(a.clone()), Box::new(b.clone()));
        let s_or = selectivity(&t, Some(&s), &or);
        let expect = 0.01 + 50.0 / 99.0 - 0.01 * (50.0 / 99.0);
        assert!((s_or - expect).abs() < 1e-9, "or={s_or}");
        let not = Expr::Not(Box::new(a));
        assert!((selectivity(&t, Some(&s), &not) - 0.99).abs() < 1e-9);
        // No-stats range default.
        assert_eq!(
            selectivity(&t, None, &lt("id", CmpOp::Lt, Datum::Int(50))),
            DEFAULT_RANGE_SEL
        );
    }

    #[test]
    fn cost_prefers_scan_for_unselective_secondary() {
        use crate::table::TableDef;
        // 1,000 rows, grp unique per row, secondary index on grp.
        let rows: Vec<Vec<Datum>> = (0..1000)
            .map(|i| vec![Datum::Int(i), Datum::Int(i), Datum::Null])
            .collect();
        let mut def = TableDef {
            name: "t".into(),
            schema: schema(),
            indexes: vec![crate::table::IndexDef { name: "i".into(), column: "grp".into() }],
            foreign_keys: vec![],
            stats: None,
        };
        def.stats = Some(stats_of(&rows));
        let t = Table::new(def);
        // Highly selective: grp = 3 keeps ~1 row → SEC SEEK wins
        // (1 + 4.02 < 20 for the full scan).
        let sel = eq_col("grp", Datum::Int(3));
        let c = choose_access_path(&t, Some(&sel)).unwrap();
        assert!(matches!(c.path, AccessPath::SecondaryIndex { .. }), "got {:?}", c.path);
        // Unselective: grp >= 0 keeps everything → FULL SCAN wins.
        let wide = Expr::Cmp {
            left: Box::new(Expr::Column("grp".into())),
            op: CmpOp::Ge,
            right: Box::new(Expr::Literal(Datum::Int(0))),
        };
        let c = choose_access_path(&t, Some(&wide)).unwrap();
        assert!(matches!(c.path, AccessPath::FullScan), "got {:?}", c.path);
    }
}
