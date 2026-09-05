//! Access-path selection: index-aware WHERE analysis (point/range seeks,
//! secondary-index bounds) plus literal coercion helpers.

use crate::error::Result;
use crate::sql::{CmpOp, Expr};
use crate::table::Table;
use crate::types::{ColumnType, Datum};

// ---------------------------------------------------------------------------
// Access-path selection (index-aware WHERE analysis)
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) enum AccessPath {
    Point(Datum),
    /// Multi-point seek for `pk IN (...)` (list order preserved).
    PkIn(Vec<Datum>),
    Range {
        lo: Option<(Datum, bool)>, // (bound, inclusive)
        hi: Option<(Datum, bool)>,
    },
    SecondaryIndex {
        col_idx: usize,
        lo: Option<(Datum, bool)>,
        hi: Option<(Datum, bool)>,
    },
    /// Secondary multi-point seek for `indexed IN (...)`.
    SecIn {
        col_idx: usize,
        values: Vec<Datum>,
    },
    FullScan,
}

fn coerce_for_col(ctype: ColumnType, lit: &Datum) -> Option<Datum> {
    match (ctype, lit) {
        (ColumnType::Int, Datum::Int(v)) | (ColumnType::BigInt, Datum::Int(v)) => {
            Some(Datum::Int(*v))
        }
        (ColumnType::Float, Datum::Int(v)) | (ColumnType::Double, Datum::Int(v)) => {
            Some(Datum::Float(*v as f64))
        }
        (ColumnType::Float, Datum::Float(v)) | (ColumnType::Double, Datum::Float(v)) => {
            Some(Datum::Float(*v))
        }
        (ColumnType::Text, Datum::Text(v)) | (ColumnType::VarChar, Datum::Text(v)) => {
            Some(Datum::Text(v.clone()))
        }
        (ColumnType::Bool, Datum::Bool(v)) => Some(Datum::Bool(*v)),
        (ColumnType::Int, Datum::Float(v)) | (ColumnType::BigInt, Datum::Float(v)) => {
            if v.fract() == 0.0 {
                Some(Datum::Int(*v as i64))
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Extract usable bounds on the primary key or secondary indexes from a
/// conjunction of predicates. OR subtrees limited to same-column equalities
/// become IN lists; anything else falls back to full scan (the executor
/// re-filters every row against the full predicate, so paths stay correct).
pub(crate) fn access_path(table: &Table, selection: Option<&Expr>) -> Result<AccessPath> {
    let Some(sel) = selection else {
        return Ok(AccessPath::FullScan);
    };
    let schema = table.schema();
    let pk_col = &schema.columns[schema.pk_idx];

    // 1. Check primary key first
    let mut eq: Option<Datum> = None;
    let mut lo: Option<(Datum, bool)> = None;
    let mut hi: Option<(Datum, bool)> = None;
    let mut in_vals: Vec<Datum> = Vec::new();
    let mut saw_in = false;
    let mut has_pk_pred = false;
    let pk_coerce = |lit: &Datum| coerce_for_col(pk_col.ctype, lit);
    if collect_col_bounds(sel, &pk_col.name, pk_col.ctype, &mut eq, &mut lo, &mut hi, &mut in_vals, &mut saw_in, &mut has_pk_pred, &pk_coerce)? {
        if let Some(v) = eq {
            return Ok(AccessPath::Point(v));
        }
        // An IN list (even fully uncoercible → empty) seeks exactly its
        // members: zero members, zero rows, no scan.
        if saw_in {
            return Ok(AccessPath::PkIn(in_vals));
        }
        if has_pk_pred {
            return Ok(AccessPath::Range { lo, hi });
        }
    }

    // 2. Check secondary indexes
    for idx_def in table.secondary_indexes() {
        if let Some(col_idx) = schema.index_of(&idx_def.column) {
            let col = &schema.columns[col_idx];
            let mut sec_eq: Option<Datum> = None;
            let mut sec_lo: Option<(Datum, bool)> = None;
            let mut sec_hi: Option<(Datum, bool)> = None;
            let mut sec_in: Vec<Datum> = Vec::new();
            let mut sec_saw_in = false;
            let mut has_sec_pred = false;
            let col_coerce = |lit: &Datum| coerce_for_col(col.ctype, lit);
            if collect_col_bounds(sel, &idx_def.column, col.ctype, &mut sec_eq, &mut sec_lo, &mut sec_hi, &mut sec_in, &mut sec_saw_in, &mut has_sec_pred, &col_coerce)? {
                if let Some(v) = sec_eq {
                    return Ok(AccessPath::SecondaryIndex {
                        col_idx,
                        lo: Some((v.clone(), true)),
                        hi: Some((v, true)),
                    });
                }
                if sec_saw_in {
                    return Ok(AccessPath::SecIn { col_idx, values: sec_in });
                }
                if has_sec_pred {
                    return Ok(AccessPath::SecondaryIndex {
                        col_idx,
                        lo: sec_lo,
                        hi: sec_hi,
                    });
                }
            }
        }
    }

    Ok(AccessPath::FullScan)
}

/// `a = 1 OR a = 2 [OR ...]` on one column becomes an IN list (coerced,
/// NULLs dropped — they never match). Anything else is not convertible.
fn or_eq_list(
    expr: &Expr,
    target_col: &str,
    coerce: &dyn Fn(&Datum) -> Option<Datum>,
) -> Option<Vec<Datum>> {
    match expr {
        Expr::Or(a, b) => {
            let mut l = or_eq_list(a, target_col, coerce)?;
            l.extend(or_eq_list(b, target_col, coerce)?);
            Some(l)
        }
        Expr::Cmp { left, op, right } => {
            let (Expr::Column(col), Expr::Literal(lit)) = (left.as_ref(), right.as_ref()) else {
                return None;
            };
            if col != target_col || !matches!(op, crate::sql::CmpOp::Eq) || matches!(lit, Datum::Null) {
                return None;
            }
            coerce(lit).map(|v| vec![v])
        }
        _ => None,
    }
}

/// Smallest string strictly greater than every string with `prefix`, for
/// LIKE-prefix range scans (`'ab%'` → `['ab', 'ac')`). Returns None when the
/// prefix is empty or saturates (open-ended upper bound then).
fn prefix_upper(prefix: &str) -> Option<String> {
    let mut chars: Vec<char> = prefix.chars().collect();
    while let Some(&c) = chars.last() {
        if c == char::MAX {
            chars.pop();
        } else {
            let last = chars.len() - 1;
            chars[last] = char::from_u32(c as u32 + 1)?;
            return Some(chars.into_iter().collect());
        }
    }
    None
}

#[allow(clippy::too_many_arguments)]
fn collect_col_bounds(
    expr: &Expr,
    target_col: &str,
    col_ctype: ColumnType,
    eq: &mut Option<Datum>,
    lo: &mut Option<(Datum, bool)>,
    hi: &mut Option<(Datum, bool)>,
    in_vals: &mut Vec<Datum>,
    saw_in: &mut bool,
    has_pred: &mut bool,
    coerce: &dyn Fn(&Datum) -> Option<Datum>,
) -> Result<bool> {
    match expr {
        Expr::And(a, b) => {
            let ok_a = collect_col_bounds(a, target_col, col_ctype, eq, lo, hi, in_vals, saw_in, has_pred, coerce)?;
            let ok_b = collect_col_bounds(b, target_col, col_ctype, eq, lo, hi, in_vals, saw_in, has_pred, coerce)?;
            Ok(ok_a && ok_b)
        }
        // Same-column equality ORs become a multi-point seek. Anything else
        // in an OR bails this column's path (the executor still filters).
        Expr::Or(_, _) => match or_eq_list(expr, target_col, coerce) {
            Some(mut vals) => {
                // An equality pin is tighter; otherwise the IN list drives
                // (empty included: zero members seek nothing).
                if eq.is_none() {
                    *has_pred = true;
                    *saw_in = true;
                    in_vals.append(&mut vals);
                }
                Ok(true)
            }
            None => Ok(true),
        },
        Expr::Cmp { left, op, right } => {
            let (Expr::Column(col), Expr::Literal(lit)) = (left.as_ref(), right.as_ref()) else {
                return Ok(true);
            };
            if col != target_col || matches!(lit, Datum::Null) {
                return Ok(true);
            }
            let Some(coerced) = coerce(lit) else {
                return Ok(false); // incompatible literal type: no index path
            };
            *has_pred = true;
            match op {
                crate::sql::CmpOp::Eq => *eq = Some(coerced),
                crate::sql::CmpOp::Ge => update_bound(lo, coerced, true, true),
                crate::sql::CmpOp::Gt => update_bound(lo, coerced, false, true),
                crate::sql::CmpOp::Le => update_bound(hi, coerced, true, false),
                crate::sql::CmpOp::Lt => update_bound(hi, coerced, false, false),
                crate::sql::CmpOp::Ne => {}
            }
            Ok(true)
        }
        Expr::In { expr, values, negated: false } => {
            let Expr::Column(col) = expr.as_ref() else {
                return Ok(true);
            };
            if col != target_col || eq.is_some() {
                return Ok(true);
            }
            *has_pred = true;
            *saw_in = true;
            for v in values {
                if matches!(v, Datum::Null) {
                    continue;
                }
                if let Some(c) = coerce(v) {
                    in_vals.push(c);
                }
            }
            Ok(true)
        }
        Expr::Between { expr, lo: blo, hi: bhi, negated: false } => {
            let Expr::Column(col) = expr.as_ref() else {
                return Ok(true);
            };
            if col != target_col {
                return Ok(true);
            }
            let (Some(l), Some(h)) = (coerce(blo), coerce(bhi)) else {
                return Ok(false);
            };
            *has_pred = true;
            update_bound(lo, l, true, true);
            update_bound(hi, h, true, false);
            Ok(true)
        }
        Expr::Like { expr, pattern, negated: false } => {
            let Expr::Column(col) = expr.as_ref() else {
                return Ok(true);
            };
            if col != target_col
                || !matches!(col_ctype, ColumnType::Text | ColumnType::VarChar)
            {
                return Ok(true);
            }
            // Literal prefix before the first wildcard bounds the scan;
            // a pattern without wildcards is an equality.
            let prefix: String = pattern.chars().take_while(|c| *c != '%' && *c != '_').collect();
            if prefix.len() == pattern.len() {
                if let Some(c) = coerce(&Datum::Text(prefix)) {
                    *has_pred = true;
                    *eq = Some(c);
                }
                return Ok(true);
            }
            if prefix.is_empty() {
                return Ok(true);
            }
            *has_pred = true;
            update_bound(lo, Datum::Text(prefix.clone()), true, true);
            if let Some(upper) = prefix_upper(&prefix) {
                update_bound(hi, Datum::Text(upper), false, false);
            }
            Ok(true)
        }
        _ => Ok(true),
    }
}

fn update_bound(slot: &mut Option<(Datum, bool)>, lit: Datum, inclusive: bool, is_lower: bool) {
    let better = match slot {
        None => true,
        Some((cur, cur_inc)) => {
            if is_lower {
                // keep the larger lower bound; ties prefer inclusive
                (lit > *cur) || (lit == *cur && inclusive && !*cur_inc)
            } else {
                // keep the smaller upper bound
                (lit < *cur) || (lit == *cur && inclusive && !*cur_inc)
            }
        }
    };
    if better {
        *slot = Some((lit, inclusive));
    }
}

// ---------------------------------------------------------------------------
// Hash-join planning: equi-key detection + hashable key normalization
// ---------------------------------------------------------------------------

/// Hashable join key. `Datum` cannot derive `Hash` (float), so probe/build
/// keys normalize through the same coercions the evaluator applies
/// (`coerce_pair` in `sql/eval.rs`): integral floats collapse to `Int`
/// (saturating cast, so extreme values agree with f64 comparison),
/// parseable text collapses to `DateTime`, NaN/NULL map to `None` and
/// therefore never match — exactly like `eval_with`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum JoinKey {
    Int(i64),
    Float(u64),
    Text(String),
    Bool(bool),
    DateTime(i64),
}

pub(crate) fn join_key(d: &Datum) -> Option<JoinKey> {
    match d {
        Datum::Null => None,
        Datum::Int(v) => Some(JoinKey::Int(*v)),
        Datum::Float(v) => {
            if v.is_nan() {
                None
            } else if v.fract() == 0.0 && *v >= i64::MIN as f64 && *v <= i64::MAX as f64 {
                Some(JoinKey::Int(*v as i64))
            } else {
                Some(JoinKey::Float(v.to_bits()))
            }
        }
        Datum::Text(s) => match crate::types::parse_datetime_str(s) {
            Some(m) => Some(JoinKey::DateTime(m)),
            None => Some(JoinKey::Text(s.clone())),
        },
        Datum::Bool(b) => Some(JoinKey::Bool(*b)),
        Datum::DateTime(m) => Some(JoinKey::DateTime(*m)),
    }
}

/// Find one equi-join pair (`left.col = right.col`) in an ON conjunction.
/// Returns `(left_scope_idx, right_scope_idx)` where the left index is
/// within the accumulated rows (`< left_width`) and the right index is in
/// the new table. Anything else (non-equi, same-side, unresolvable) yields
/// `None` and the executor falls back to nested loop. The full ON clause
/// still filters matches afterwards, so compound predicates stay correct.
pub(crate) fn equi_join(
    on: &Expr,
    left_width: usize,
    resolve: &dyn Fn(&str) -> Result<usize>,
) -> Option<(usize, usize)> {
    match on {
        Expr::And(a, b) => {
            equi_join(a, left_width, resolve).or_else(|| equi_join(b, left_width, resolve))
        }
        Expr::Cmp { left, op, right } => {
            if !matches!(op, CmpOp::Eq) {
                return None;
            }
            let (Expr::Column(x), Expr::Column(y)) = (left.as_ref(), right.as_ref()) else {
                return None;
            };
            let (i, k) = (resolve(x).ok()?, resolve(y).ok()?);
            match (i < left_width, k < left_width) {
                (true, false) => Some((i, k)),
                (false, true) => Some((k, i)),
                _ => None,
            }
        }
        _ => None,
    }
}
