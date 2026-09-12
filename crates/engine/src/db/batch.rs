//! Morsel-driven columnar batch execution for single-table analytics.
//!
//! The scalar executor threads `Vec<Datum>` rows through per-row closures
//! (`eval_with` + `compute_aggregate`): indirect calls, repeated matches,
//! and clones per row. This module processes the SAME sourced rows in
//! 1024-row morsels: decode once into contiguous primitive columns, filter
//! with a selection vector, and fold aggregates over active indices — no
//! per-row Datum dispatch in the hot loop.
//!
//! Correctness contract (parity with the scalar path, enforced by the
//! differential suite in `db/tests/batch.rs`):
//! - Row sourcing is shared: morsels decode `visible_rows(.., None)`
//!   output, so access paths, MVCC snapshots, staged-write overlays, and
//!   row ORDER (float summation order!) are identical by construction. Only
//!   FullScan plans enter the batch path; indexed seeks stay scalar.
//! - Filters mirror `eval_with` exactly, including the legacy quirks: NULL
//!   operands fail (the row drops), unknown columns drop the row, and OR
//!   short-circuit keeps rows the scalar path keeps. Combination uses
//!   tri-state sets (`Tri`) so short-circuiting is exact, and every
//!   selection vector stays ascending so summation order never changes.
//! - Aggregates mirror `compute_aggregate` exactly (i128 accumulation with
//!   FLOAT widening, NULL skipping, empty-set NULLs, `TypeMismatch` on the
//!   first non-numeric value, discovered in row order, aggs left to right).
//! - Anything outside the supported shapes (subqueries, correlation
//!   frames, type-mismatched ephemeral columns, non-FullScan plans) falls
//!   back to the scalar executor: `try_*` returns `Ok(None)` and the caller
//!   runs the legacy path untouched.

use std::sync::Arc;

use super::cost::choose_access_path;
use super::join::GProj;
use super::plan::AccessPath;
use super::query::AggSpec;
use super::subquery::has_subquery;
use super::{Database, Output, Session};
use crate::error::{Error, Result};
use crate::sql::{AggFunc, CmpOp, Expr};
use crate::table::{Schema, Table};
use crate::types::{parse_datetime_str, ColumnType, Datum};

/// Rows per morsel: small enough to stay L1/L2-resident across a few
/// primitive columns, large enough to amortize decode.
pub(super) const MORSEL: usize = 1024;

/// One decoded column. Kinds follow the table schema; values are
/// contiguous primitives (`null[i] != 0` marks SQL NULL). Text packs all
/// values into one byte buffer with `off` (length `n + 1`) delimiting rows.
pub(super) enum Column {
    I64 { vals: Vec<i64>, null: Vec<u8> },
    F64 { vals: Vec<f64>, null: Vec<u8> },
    Bool { vals: Vec<bool>, null: Vec<u8> },
    Str { data: Vec<u8>, off: Vec<usize>, null: Vec<u8> },
    DateTime { vals: Vec<i64>, null: Vec<u8> },
}

/// A decoded morsel: parallel columns plus the ascending selection vector
/// of active row positions (`sel` starts as all `0..n`).
pub(super) struct ColumnBatch {
    cols: Vec<Column>,
    sel: Vec<u16>,
}

fn col_kind(ctype: ColumnType) -> u8 {
    match ctype {
        ColumnType::Int | ColumnType::BigInt => 0,
        ColumnType::Float | ColumnType::Double => 1,
        ColumnType::Bool => 2,
        ColumnType::Text | ColumnType::VarChar => 3,
        ColumnType::DateTime | ColumnType::Timestamp => 4,
    }
}

impl Column {
    fn clear(&mut self) {
        match self {
            Column::I64 { vals, null } => {
                vals.clear();
                null.clear();
            }
            Column::F64 { vals, null } => {
                vals.clear();
                null.clear();
            }
            Column::Bool { vals, null } => {
                vals.clear();
                null.clear();
            }
            Column::Str { data, off, null } => {
                data.clear();
                off.clear();
                null.clear();
            }
            Column::DateTime { vals, null } => {
                vals.clear();
                null.clear();
            }
        }
    }

    fn reserve(&mut self, n: usize) {
        match self {
            Column::I64 { vals, null } => {
                vals.reserve(n);
                null.reserve(n);
            }
            Column::F64 { vals, null } => {
                vals.reserve(n);
                null.reserve(n);
            }
            Column::Bool { vals, null } => {
                vals.reserve(n);
                null.reserve(n);
            }
            Column::Str { data, off, null } => {
                off.reserve(n + 1);
                null.reserve(n);
                let _ = data;
            }
            Column::DateTime { vals, null } => {
                vals.reserve(n);
                null.reserve(n);
            }
        }
    }
}

impl ColumnBatch {
    pub(super) fn new() -> Self {
        ColumnBatch { cols: Vec::new(), sel: Vec::new() }
    }

    pub(super) fn clear(&mut self) {
        for c in self.cols.iter_mut() {
            c.clear();
        }
        self.sel.clear();
    }

    #[allow(dead_code)]
    pub(super) fn selection(&self) -> &[u16] {
        &self.sel
    }

    #[allow(dead_code)]
    pub(super) fn len(&self) -> usize {
        self.sel.len()
    }

    /// (Re)shape the batch columns on schema change, clearing old contents
    /// and pre-reserving capacity for active columns.
    pub(super) fn prepare_morsel(&mut self, schema: &Schema, need: &[bool], cap: usize) {
        let mut shaped = self.cols.len() == schema.columns.len();
        if shaped {
            for (col, c) in self.cols.iter().zip(schema.columns.iter()) {
                let kind = match col {
                    Column::I64 { .. } => 0,
                    Column::F64 { .. } => 1,
                    Column::Bool { .. } => 2,
                    Column::Str { .. } => 3,
                    Column::DateTime { .. } => 4,
                };
                if kind != col_kind(c.ctype) {
                    shaped = false;
                    break;
                }
            }
        }
        if !shaped {
            self.cols.clear();
            for c in &schema.columns {
                match col_kind(c.ctype) {
                    0 => self.cols.push(Column::I64 { vals: Vec::new(), null: Vec::new() }),
                    1 => self.cols.push(Column::F64 { vals: Vec::new(), null: Vec::new() }),
                    2 => self.cols.push(Column::Bool { vals: Vec::new(), null: Vec::new() }),
                    3 => self.cols.push(Column::Str { data: Vec::new(), off: Vec::new(), null: Vec::new() }),
                    _ => self.cols.push(Column::DateTime { vals: Vec::new(), null: Vec::new() }),
                }
            }
        }
        self.clear();
        for (ci, col) in self.cols.iter_mut().enumerate() {
            if need.get(ci).copied().unwrap_or(false) {
                col.reserve(cap);
            }
        }
    }

    /// Decode one raw encoded row directly into active column buffers without
    /// allocating intermediate Datum values. Unprojected columns (`need[ci] == false`)
    /// are skipped by offset with zero copies or string allocations.
    pub(super) fn push_raw_row(
        &mut self,
        _schema: &Schema,
        buf: &[u8],
        need: &[bool],
    ) -> Option<()> {
        let mut off = 0usize;
        for (ci, col) in self.cols.iter_mut().enumerate() {
            let needed = need.get(ci).copied().unwrap_or(false);
            let tag = *buf.get(off)?;
            off += 1;
            match tag {
                0 => {
                    if needed {
                        match col {
                            Column::I64 { vals, null } => {
                                vals.push(0);
                                null.push(1);
                            }
                            Column::F64 { vals, null } => {
                                vals.push(0.0);
                                null.push(1);
                            }
                            Column::Bool { vals, null } => {
                                vals.push(false);
                                null.push(1);
                            }
                            Column::Str { data, off: str_off, null } => {
                                if str_off.is_empty() {
                                    str_off.push(0);
                                }
                                str_off.push(data.len());
                                null.push(1);
                            }
                            Column::DateTime { vals, null } => {
                                vals.push(0);
                                null.push(1);
                            }
                        }
                    }
                }
                1 => {
                    if off + 8 > buf.len() {
                        return None;
                    }
                    if needed {
                        let v = i64::from_le_bytes(buf[off..off + 8].try_into().unwrap());
                        match col {
                            Column::I64 { vals, null } => {
                                vals.push(v);
                                null.push(0);
                            }
                            _ => return None,
                        }
                    }
                    off += 8;
                }
                2 => {
                    if off + 8 > buf.len() {
                        return None;
                    }
                    if needed {
                        let v = f64::from_le_bytes(buf[off..off + 8].try_into().unwrap());
                        match col {
                            Column::F64 { vals, null } => {
                                vals.push(v);
                                null.push(0);
                            }
                            _ => return None,
                        }
                    }
                    off += 8;
                }
                3 => {
                    if off + 4 > buf.len() {
                        return None;
                    }
                    let len = u32::from_le_bytes(buf[off..off + 4].try_into().unwrap()) as usize;
                    off += 4;
                    if off + len > buf.len() {
                        return None;
                    }
                    if needed {
                        match col {
                            Column::Str { data, off: str_off, null } => {
                                if str_off.is_empty() {
                                    str_off.push(0);
                                }
                                data.extend_from_slice(&buf[off..off + len]);
                                str_off.push(data.len());
                                null.push(0);
                            }
                            _ => return None,
                        }
                    }
                    off += len;
                }
                4 => {
                    if off + 1 > buf.len() {
                        return None;
                    }
                    if needed {
                        let v = buf[off] != 0;
                        match col {
                            Column::Bool { vals, null } => {
                                vals.push(v);
                                null.push(0);
                            }
                            _ => return None,
                        }
                    }
                    off += 1;
                }
                5 => {
                    if off + 8 > buf.len() {
                        return None;
                    }
                    if needed {
                        let v = i64::from_le_bytes(buf[off..off + 8].try_into().unwrap());
                        match col {
                            Column::DateTime { vals, null } => {
                                vals.push(v);
                                null.push(0);
                            }
                            _ => return None,
                        }
                    }
                    off += 8;
                }
                _ => return None,
            }
        }
        if off != buf.len() {
            return None;
        }
        Some(())
    }

    /// Decode one Datum-slice row (used by staged overlays and snapshot history).
    pub(super) fn push_datum_row(
        &mut self,
        _schema: &Schema,
        row: &[Datum],
        need: &[bool],
    ) -> Option<()> {
        for (ci, col) in self.cols.iter_mut().enumerate() {
            if !need.get(ci).copied().unwrap_or(false) {
                continue;
            }
            let d = row.get(ci)?;
            match (col, d) {
                (Column::I64 { vals, null }, Datum::Int(v)) => {
                    vals.push(*v);
                    null.push(0);
                }
                (Column::F64 { vals, null }, Datum::Float(v)) => {
                    vals.push(*v);
                    null.push(0);
                }
                (Column::Bool { vals, null }, Datum::Bool(v)) => {
                    vals.push(*v);
                    null.push(0);
                }
                (Column::Str { data, off, null }, Datum::Text(s)) => {
                    if off.is_empty() {
                        off.push(0);
                    }
                    data.extend_from_slice(s.as_bytes());
                    off.push(data.len());
                    null.push(0);
                }
                (Column::DateTime { vals, null }, Datum::DateTime(v)) => {
                    vals.push(*v);
                    null.push(0);
                }
                (col, Datum::Null) => match col {
                    Column::I64 { vals, null } => {
                        vals.push(0);
                        null.push(1);
                    }
                    Column::F64 { vals, null } => {
                        vals.push(0.0);
                        null.push(1);
                    }
                    Column::Bool { vals, null } => {
                        vals.push(false);
                        null.push(1);
                    }
                    Column::Str { data, off, null } => {
                        if off.is_empty() {
                            off.push(0);
                        }
                        off.push(data.len());
                        null.push(1);
                    }
                    Column::DateTime { vals, null } => {
                        vals.push(0);
                        null.push(1);
                    }
                },
                _ => return None,
            }
        }
        Some(())
    }

    /// Seal the current morsel: populate selection vector and initialize text offsets.
    pub(super) fn finish_morsel(&mut self, need: &[bool], count: usize) {
        self.sel.clear();
        self.sel.extend(0..count as u16);
        for (ci, col) in self.cols.iter_mut().enumerate() {
            if !need.get(ci).copied().unwrap_or(false) {
                continue;
            }
            if let Column::Str { off, .. } = col {
                if off.is_empty() {
                    off.push(0);
                }
            }
        }
    }
}

/// Decode up to `MORSEL` rows into columnar form, decoding only `need`
/// columns (projection pushdown). Reuses `prepare_morsel`, `push_datum_row`,
/// and `finish_morsel`.
#[allow(dead_code)]
pub(super) fn decode_morsel(
    batch: &mut ColumnBatch,
    schema: &Schema,
    rows: &[Vec<Datum>],
    need: &[bool],
) -> Option<()> {
    let n = rows.len().min(MORSEL);
    batch.prepare_morsel(schema, need, n);
    for r in rows.iter().take(n) {
        batch.push_datum_row(schema, r, need)?;
    }
    batch.finish_morsel(need, n);
    Some(())
}

/// Columns a query touches: predicate references plus caller-named extras
/// (aggregate and group-key positions). Unknown names resolve nowhere and
/// are skipped (the tri-state filter drops those rows, like scalar).
fn need_mask(schema: &Schema, selection: Option<&Expr>, extra: &[usize]) -> Vec<bool> {
    let mut need = vec![false; schema.columns.len()];
    if let Some(s) = selection {
        let mut cols = Vec::new();
        crate::sql::collect_columns(s, &mut cols);
        for c in cols {
            if let Some(i) = schema.index_of(c) {
                need[i] = true;
            }
        }
    }
    for &i in extra {
        if i < need.len() {
            need[i] = true;
        }
    }
    need
}

// ---------------------------------------------------------------------------
// Vectorized filters (tri-state combination mirrors eval short-circuiting)
// ---------------------------------------------------------------------------

/// Predicate outcome over an input selection: rows that pass (`t`) and rows
/// that errored (`e`, i.e. unknown columns — dropped like the scalar
/// `ColumnNotFound`-skip). Both ascending; disjoint; union is a subset of
/// the input. Combination rules replay Rust `&&`/`||` short-circuiting so
/// `col = 1 OR missing = 2` keeps exactly the rows the scalar path keeps.
struct Tri {
    t: Vec<u16>,
    e: Vec<u16>,
}

fn subtract_sorted(a: &[u16], b: &[u16]) -> Vec<u16> {
    let mut out = Vec::with_capacity(a.len());
    let (mut i, mut j) = (0, 0);
    while i < a.len() {
        if j < b.len() && b[j] < a[i] {
            j += 1;
        } else if j < b.len() && b[j] == a[i] {
            i += 1;
            j += 1;
        } else {
            out.push(a[i]);
            i += 1;
        }
    }
    out
}

fn union_sorted(a: &[u16], b: &[u16]) -> Vec<u16> {
    let mut out = Vec::with_capacity(a.len() + b.len());
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        if a[i] < b[j] {
            out.push(a[i]);
            i += 1;
        } else if b[j] < a[i] {
            out.push(b[j]);
            j += 1;
        } else {
            out.push(a[i]);
            i += 1;
            j += 1;
        }
    }
    out.extend_from_slice(&a[i..]);
    out.extend_from_slice(&b[j..]);
    out
}

/// A resolved leaf operand: borrowed column cell, owned literal, or NULL.
/// `Unknown` (unresolvable column) is tracked separately, never here.
enum PVal<'a> {
    I(i64),
    F(f64),
    B(bool),
    S(&'a str),
    D(i64),
    Null,
}

fn cell<'a>(batch: &'a ColumnBatch, ci: usize, row: usize) -> PVal<'a> {
    match &batch.cols[ci] {
        Column::I64 { vals, null } => {
            if null[row] != 0 { PVal::Null } else { PVal::I(vals[row]) }
        }
        Column::F64 { vals, null } => {
            if null[row] != 0 { PVal::Null } else { PVal::F(vals[row]) }
        }
        Column::Bool { vals, null } => {
            if null[row] != 0 { PVal::Null } else { PVal::B(vals[row]) }
        }
        Column::Str { data, off, null } => {
            if null[row] != 0 {
                PVal::Null
            } else {
                PVal::S(std::str::from_utf8(&data[off[row]..off[row + 1]]).unwrap_or(""))
            }
        }
        Column::DateTime { vals, null } => {
            if null[row] != 0 { PVal::Null } else { PVal::D(vals[row]) }
        }
    }
}

fn lit_val(d: &Datum) -> PVal<'_> {
    match d {
        Datum::Null => PVal::Null,
        Datum::Int(v) => PVal::I(*v),
        Datum::Float(v) => PVal::F(*v),
        Datum::Bool(v) => PVal::B(*v),
        Datum::Text(s) => PVal::S(s.as_str()),
        Datum::DateTime(v) => PVal::D(*v),
    }
}

/// Rank mirror of `Datum::type_rank` for non-null pairs (NULL is filtered
/// before comparison, exactly like the scalar NULL-fails rule).
fn rank_of(v: &PVal<'_>) -> u8 {
    match v {
        PVal::B(_) => 1,
        PVal::I(_) | PVal::F(_) => 2,
        PVal::D(_) => 3,
        PVal::S(_) => 4,
        PVal::Null => 0,
    }
}

/// Total comparison mirroring `Datum::Ord::cmp` for all pairs, including
/// the DateTime-Text parse attempt from `coerce_pair` and NULL rank
/// comparison (NULL bounds in BETWEEN compare by rank, like the scalar
/// path — NULL *targets* never reach here, they fail upstream).
fn cmp_pvals(l: &PVal<'_>, r: &PVal<'_>) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    if matches!(l, PVal::Null) || matches!(r, PVal::Null) {
        return rank_of(l).cmp(&rank_of(r));
    }
    match (l, r) {
        (PVal::I(a), PVal::I(b)) => a.cmp(b),
        (PVal::F(a), PVal::F(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        (PVal::I(a), PVal::F(b)) => (*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal),
        (PVal::F(a), PVal::I(b)) => a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal),
        (PVal::B(a), PVal::B(b)) => a.cmp(b),
        (PVal::B(a), PVal::I(b)) => (if *a { 1i64 } else { 0i64 }).cmp(b),
        (PVal::I(a), PVal::B(b)) => a.cmp(&(if *b { 1i64 } else { 0i64 })),
        (PVal::B(a), PVal::F(b)) => (if *a { 1.0f64 } else { 0.0f64 }).partial_cmp(b).unwrap_or(Ordering::Equal),
        (PVal::F(a), PVal::B(b)) => a.partial_cmp(&(if *b { 1.0f64 } else { 0.0f64 })).unwrap_or(Ordering::Equal),
        (PVal::D(a), PVal::D(b)) => a.cmp(b),
        (PVal::S(a), PVal::S(b)) => a.cmp(b),
        (PVal::D(a), PVal::S(b)) => match parse_datetime_str(b) {
            Some(m) => a.cmp(&m),
            None => 3u8.cmp(&4),
        },
        (PVal::S(a), PVal::D(b)) => match parse_datetime_str(a) {
            Some(m) => m.cmp(b),
            None => 4u8.cmp(&3),
        },
        _ => rank_of(l).cmp(&rank_of(r)),
    }
}

fn apply_cmp(op: &CmpOp, ord: std::cmp::Ordering) -> bool {
    use std::cmp::Ordering;
    match op {
        CmpOp::Eq => ord == Ordering::Equal,
        CmpOp::Ne => ord != Ordering::Equal,
        CmpOp::Lt => ord == Ordering::Less,
        CmpOp::Le => ord != Ordering::Greater,
        CmpOp::Gt => ord == Ordering::Greater,
        CmpOp::Ge => ord != Ordering::Less,
    }
}

/// `IN`-list membership mirroring the scalar loop: NULL target fails, NULL
/// list entries are skipped (their presence does NOT flip, unlike
/// `IN (subquery)`), each pair compared like the scalar coerce + derived
/// `==` (note: float equality here is bitwise `==`, NOT the `Ord`
/// NaN-collapsing compare used by `Cmp` — the scalar paths differ and so
/// do we).
fn in_match(t: &PVal<'_>, values: &[Datum]) -> bool {
    if matches!(t, PVal::Null) {
        return false;
    }
    for v in values {
        if matches!(v, Datum::Null) {
            continue;
        }
        let hit = match (t, v) {
            (PVal::I(a), Datum::Int(b)) => a == b,
            (PVal::F(a), Datum::Float(b)) => a == b,
            (PVal::I(a), Datum::Float(b)) => (*a as f64) == *b,
            (PVal::F(a), Datum::Int(b)) => *a == (*b as f64),
            (PVal::B(a), Datum::Bool(b)) => a == b,
            (PVal::B(a), Datum::Int(b)) => (if *a { 1 } else { 0 }) == *b,
            (PVal::I(a), Datum::Bool(b)) => *a == (if *b { 1 } else { 0 }),
            (PVal::B(a), Datum::Float(b)) => (if *a { 1.0 } else { 0.0 }) == *b,
            (PVal::F(a), Datum::Bool(b)) => *a == (if *b { 1.0 } else { 0.0 }),
            (PVal::D(a), Datum::DateTime(b)) => a == b,
            (PVal::S(a), Datum::Text(b)) => *a == b.as_str(),
            (PVal::D(a), Datum::Text(b)) => match parse_datetime_str(b) {
                Some(m) => *a == m,
                None => false,
            },
            (PVal::S(a), Datum::DateTime(b)) => match parse_datetime_str(a) {
                Some(m) => m == *b,
                None => false,
            },
            _ => false,
        };
        if hit {
            return true;
        }
    }
    false
}

/// Resolve a leaf operand: outer `None` = unsupported shape (non-column,
/// non-literal — the caller falls back to scalar); `Some(None)` = unknown
/// column (tri-state error — the row drops, like the scalar
/// `ColumnNotFound`-skip); otherwise the borrowed cell or owned literal.
fn operand<'a>(
    batch: &'a ColumnBatch,
    col_of: &dyn Fn(&str) -> Option<usize>,
    e: &'a Expr,
    row: usize,
) -> Option<Option<PVal<'a>>> {
    match e {
        Expr::Column(name) => Some(match col_of(name) {
            Some(ci) => Some(cell(batch, ci, row)),
            None => None,
        }),
        Expr::Literal(d) => Some(Some(lit_val(d))),
        _ => None,
    }
}

/// Evaluate one predicate node over `input`, returning passing rows and
/// error rows (both ascending). `None` = unsupported shape (subqueries and
/// anything outside column/literal leaves) — the caller falls back to the
/// scalar executor.
fn eval_node(
    batch: &ColumnBatch,
    col_of: &dyn Fn(&str) -> Option<usize>,
    expr: &Expr,
    input: &[u16],
) -> Option<Tri> {
    match expr {
        Expr::And(a, b) => {
            let ra = eval_node(batch, col_of, a, input)?;
            let rb = eval_node(batch, col_of, b, &ra.t)?;
            Some(Tri { t: rb.t, e: union_sorted(&ra.e, &rb.e) })
        }
        Expr::Or(a, b) => {
            let ra = eval_node(batch, col_of, a, input)?;
            let rest = subtract_sorted(&subtract_sorted(input, &ra.t), &ra.e);
            let rb = eval_node(batch, col_of, b, &rest)?;
            Some(Tri { t: union_sorted(&ra.t, &rb.t), e: union_sorted(&ra.e, &rb.e) })
        }
        Expr::Not(e) => {
            let r = eval_node(batch, col_of, e, input)?;
            let mut t = subtract_sorted(input, &r.t);
            t = subtract_sorted(&t, &r.e);
            Some(Tri { t, e: r.e })
        }
        Expr::Cmp { left, op, right } => {
            let mut t = Vec::new();
            let mut e = Vec::new();
            for &i in input {
                let r = operand(batch, col_of, left, i as usize)?;
                let s = operand(batch, col_of, right, i as usize)?;
                match (r, s) {
                    (None, _) | (_, None) => e.push(i),
                    (Some(PVal::Null), _) | (_, Some(PVal::Null)) => {}
                    (Some(a), Some(b)) => {
                        if apply_cmp(op, cmp_pvals(&a, &b)) {
                            t.push(i);
                        }
                    }
                }
            }
            Some(Tri { t, e })
        }
        Expr::In { expr, values, negated } => {
            let mut t = Vec::new();
            let mut e = Vec::new();
            for &i in input {
                match operand(batch, col_of, expr, i as usize)? {
                    None => e.push(i),
                    // NULL target fails for BOTH polarities (scalar returns
                    // Ok(false) before negation applies) — the row drops.
                    Some(PVal::Null) => {}
                    Some(v) => {
                        if in_match(&v, values) != *negated {
                            t.push(i);
                        }
                    }
                }
            }
            Some(Tri { t, e })
        }
        Expr::Between { expr, lo, hi, negated } => {
            let lo = lit_val(lo);
            let hi = lit_val(hi);
            let mut t = Vec::new();
            let mut e = Vec::new();
            for &i in input {
                match operand(batch, col_of, expr, i as usize)? {
                    None => e.push(i),
                    Some(PVal::Null) => {}
                    Some(v) => {
                        // Mirror the scalar double coercion exactly:
                        // coerce(target, lo), then coerce(result, hi).
                        let (t1, l) = coerce_vals(&v, &lo);
                        let (t2, h) = coerce_vals(&cval_as_pval(&t1), &hi);
                        let t2 = cval_as_pval(&t2);
                        let l = cval_as_pval(&l);
                        let h = cval_as_pval(&h);
                        let inside = apply_cmp(&CmpOp::Ge, cmp_pvals(&t2, &l))
                            && apply_cmp(&CmpOp::Le, cmp_pvals(&t2, &h));
                        if inside != *negated {
                            t.push(i);
                        }
                    }
                }
            }
            Some(Tri { t, e })
        }
        Expr::Like { expr, pattern, negated } => {
            let mut t = Vec::new();
            let mut e = Vec::new();
            for &i in input {
                match operand(batch, col_of, expr, i as usize)? {
                    None => e.push(i),
                    Some(PVal::Null) => {}
                    Some(v) => {
                        // Mirror `target.to_string()` exactly: text borrows
                        // (same bytes, no allocation), others Display.
                        let text;
                        let s: &str = match v {
                            PVal::S(s) => s,
                            ref other => {
                                text = pval_display(other);
                                &text
                            }
                        };
                        if crate::sql::like_match(s, pattern) != *negated {
                            t.push(i);
                        }
                    }
                }
            }
            Some(Tri { t, e })
        }
        Expr::Column(_) => {
            let mut t = Vec::new();
            let mut e = Vec::new();
            for &i in input {
                match operand(batch, col_of, expr, i as usize)? {
                    None => e.push(i),
                    Some(PVal::Null) => {}
                    Some(PVal::B(b)) => { if b { t.push(i); } }
                    Some(PVal::I(n)) => { if n != 0 { t.push(i); } }
                    Some(PVal::F(f)) => { if f != 0.0 { t.push(i); } }
                    Some(_) => {}
                }
            }
            Some(Tri { t, e })
        }
        Expr::Literal(d) => {
            let truthy = match d {
                Datum::Bool(b) => *b,
                Datum::Int(n) => *n != 0,
                Datum::Float(f) => *f != 0.0,
                _ => false,
            };
            Some(Tri {
                t: if truthy { input.to_vec() } else { Vec::new() },
                e: Vec::new(),
            })
        }
        _ => None,
    }
}

/// Owned single-value counterpart of `PVal` for coercion results
/// (NULL preserved: NULL bounds compare by rank, like the scalar path).
enum CVal {
    I(i64),
    F(f64),
    B(bool),
    S(String),
    D(i64),
    Null,
}

fn to_cval(v: &PVal<'_>) -> CVal {
    match v {
        PVal::I(a) => CVal::I(*a),
        PVal::F(a) => CVal::F(*a),
        PVal::B(a) => CVal::B(*a),
        PVal::S(a) => CVal::S(a.to_string()),
        PVal::D(a) => CVal::D(*a),
        PVal::Null => CVal::Null,
    }
}

fn cval_as_pval(v: &CVal) -> PVal<'_> {
    match v {
        CVal::I(a) => PVal::I(*a),
        CVal::F(a) => PVal::F(*a),
        CVal::B(a) => PVal::B(*a),
        CVal::S(a) => PVal::S(a.as_str()),
        CVal::D(a) => PVal::D(*a),
        CVal::Null => PVal::Null,
    }
}

/// Mirror `coerce_pair` on borrowed values: Int/Float widen, DateTime-Text
/// attempts a parse, everything else passes through unchanged.
fn coerce_vals<'a>(t: &'a PVal<'a>, bound: &'a PVal<'a>) -> (CVal, CVal) {
    match (t, bound) {
        (PVal::B(a), PVal::I(b)) => (CVal::I(if *a { 1 } else { 0 }), CVal::I(*b)),
        (PVal::I(a), PVal::B(b)) => (CVal::I(*a), CVal::I(if *b { 1 } else { 0 })),
        (PVal::B(a), PVal::F(b)) => (CVal::F(if *a { 1.0 } else { 0.0 }), CVal::F(*b)),
        (PVal::F(a), PVal::B(b)) => (CVal::F(*a), CVal::F(if *b { 1.0 } else { 0.0 })),
        (PVal::I(a), PVal::F(b)) => (CVal::F(*a as f64), CVal::F(*b)),
        (PVal::F(a), PVal::I(b)) => (CVal::F(*a), CVal::F(*b as f64)),
        (PVal::D(a), PVal::S(b)) => match parse_datetime_str(b) {
            Some(m) => (CVal::D(*a), CVal::D(m)),
            None => (CVal::D(*a), CVal::S(b.to_string())),
        },
        (PVal::S(a), PVal::D(b)) => match parse_datetime_str(a) {
            Some(m) => (CVal::D(m), CVal::D(*b)),
            None => (CVal::S(a.to_string()), CVal::D(*b)),
        },
        _ => (to_cval(t), to_cval(bound)),
    }
}

fn pval_display(v: &PVal<'_>) -> String {
    match v {
        PVal::I(a) => a.to_string(),
        PVal::F(a) => a.to_string(),
        PVal::B(a) => a.to_string(),
        PVal::S(a) => a.to_string(),
        PVal::D(a) => crate::types::format_datetime_micros(*a),
        PVal::Null => "NULL".to_string(),
    }
}

/// Does this predicate use only supported shapes (column/literal leaves
/// under AND/OR/NOT)? Anything else (subqueries live here) rejects the
/// batch path. Callers also require subquery-freedom separately; this is
/// the structural guard.
fn shapes_supported(expr: &Expr) -> bool {
    match expr {
        Expr::Column(_) | Expr::Literal(_) => true,
        Expr::Cmp { left, right, .. } => shapes_supported(left) && shapes_supported(right),
        Expr::And(a, b) | Expr::Or(a, b) => shapes_supported(a) && shapes_supported(b),
        Expr::Not(e) => shapes_supported(e),
        Expr::In { expr, .. } | Expr::Between { expr, .. } | Expr::Like { expr, .. } => {
            shapes_supported(expr)
        }
        _ => false,
    }
}

/// Apply the selection to the batch's selection vector (ascending). Unknown
/// columns and short-circuits behave exactly like the scalar filter via the
/// tri-state combination; only genuinely unsupported shapes return `None`.
pub(super) fn filter_batch(
    batch: &mut ColumnBatch,
    schema: &Schema,
    selection: &Expr,
) -> Option<()> {
    let col_of = |name: &str| schema.index_of(name);
    let input = batch.sel.clone();
    let r = eval_node(batch, &col_of, selection, &input)?;
    batch.sel = r.t;
    Some(())
}

// ---------------------------------------------------------------------------
// Aggregate pushdown (mirrors `compute_aggregate` exactly)
// ---------------------------------------------------------------------------

/// Incremental aggregate state: one fold per output item, updated over
/// active selection vectors in global row order (chunk after chunk), so
/// float summation order — and results — match the scalar executor bit for
/// bit. Group-ready: grouped execution keeps one fold vector per group key.
pub(super) enum AggFold {
    Sum { ints: i128, floats: f64, any_float: bool, n: u64 },
    Avg { sum: f64, n: u64 },
    Min { best: Option<Datum> },
    Max { best: Option<Datum> },
}

pub(super) fn fold_for(func: AggFunc) -> AggFold {
    match func {
        AggFunc::Sum => AggFold::Sum { ints: 0, floats: 0.0, any_float: false, n: 0 },
        AggFunc::Avg => AggFold::Avg { sum: 0.0, n: 0 },
        AggFunc::Min => AggFold::Min { best: None },
        AggFunc::Max => AggFold::Max { best: None },
    }
}

fn type_mismatch(got: &str) -> Error {
    Error::TypeMismatch { expected: "numeric".into(), got: got.into() }
}

impl AggFold {
    /// Fold one decoded cell. Bool/Text/DateTime cells are rejected for
    /// SUM/AVG on first sight (like the scalar `TypeMismatch`); MIN/MAX
    /// compare them with the total datum order.
    pub fn add_int(&mut self, v: i64) -> Result<()> {
        match self {
            AggFold::Sum { ints, n, .. } => {
                *ints += v as i128;
                *n += 1;
            }
            AggFold::Avg { sum, n } => {
                *sum += v as f64;
                *n += 1;
            }
            AggFold::Min { best } => {
                let d = Datum::Int(v);
                if best.as_ref().map_or(true, |b| d < *b) {
                    *best = Some(d);
                }
            }
            AggFold::Max { best } => {
                let d = Datum::Int(v);
                if best.as_ref().map_or(true, |b| d > *b) {
                    *best = Some(d);
                }
            }
        }
        Ok(())
    }

    pub fn add_float(&mut self, v: f64) -> Result<()> {
        match self {
            AggFold::Sum { floats, any_float, n, .. } => {
                *floats += v;
                *any_float = true;
                *n += 1;
            }
            AggFold::Avg { sum, n } => {
                *sum += v;
                *n += 1;
            }
            AggFold::Min { best } => {
                let d = Datum::Float(v);
                if best.as_ref().map_or(true, |b| d < *b) {
                    *best = Some(d);
                }
            }
            AggFold::Max { best } => {
                let d = Datum::Float(v);
                if best.as_ref().map_or(true, |b| d > *b) {
                    *best = Some(d);
                }
            }
        }
        Ok(())
    }

    pub fn add_other(&mut self, type_name: &str, d: &Datum) -> Result<()> {
        match self {
            AggFold::Sum { .. } | AggFold::Avg { .. } => Err(type_mismatch(type_name)),
            AggFold::Min { best } => {
                if best.as_ref().map_or(true, |b| *d < *b) {
                    *best = Some(d.clone());
                }
                Ok(())
            }
            AggFold::Max { best } => {
                if best.as_ref().map_or(true, |b| *d > *b) {
                    *best = Some(d.clone());
                }
                Ok(())
            }
        }
    }

    pub fn finish(self, func: AggFunc) -> Datum {
        match (self, func) {
            (AggFold::Sum { ints, floats, any_float, n }, _) => {
                if n == 0 {
                    Datum::Null
                } else if any_float {
                    Datum::Float(floats + ints as f64)
                } else if ints <= i64::MAX as i128 && ints >= i64::MIN as i128 {
                    Datum::Int(ints as i64)
                } else {
                    Datum::Float(ints as f64)
                }
            }
            (AggFold::Avg { sum, n }, _) => {
                if n == 0 {
                    Datum::Null
                } else {
                    Datum::Float(sum / n as f64)
                }
            }
            (AggFold::Min { best }, _) | (AggFold::Max { best }, _) => {
                best.unwrap_or(Datum::Null)
            }
        }
    }
}

/// Fold one decoded cell into `fold` (NULLs skip; non-numeric cells fail
/// SUM/AVG exactly like the scalar path). Shared by whole-column and
/// per-row (grouped) accumulation.
fn fold_cell(batch: &ColumnBatch, ci: usize, row: usize, fold: &mut AggFold) -> Result<()> {
    match &batch.cols[ci] {
        Column::I64 { vals, null } => {
            if null[row] == 0 {
                fold.add_int(vals[row])?;
            }
        }
        Column::F64 { vals, null } => {
            if null[row] == 0 {
                fold.add_float(vals[row])?;
            }
        }
        Column::Bool { vals, null } => {
            if null[row] == 0 {
                fold.add_other("BOOL", &Datum::Bool(vals[row]))?;
            }
        }
        Column::Str { data, off, null } => {
            if null[row] == 0 {
                let s = std::str::from_utf8(&data[off[row]..off[row + 1]]).unwrap_or("");
                // Borrowed compare (clone only when a new extremum wins);
                // SUM/AVG fall through to the type error below.
                match fold {
                    AggFold::Min { best } => {
                        let take = match best {
                            Some(Datum::Text(b)) => s < b.as_str(),
                            _ => true,
                        };
                        if take {
                            *best = Some(Datum::Text(s.to_string()));
                        }
                    }
                    AggFold::Max { best } => {
                        let take = match best {
                            Some(Datum::Text(b)) => s > b.as_str(),
                            _ => true,
                        };
                        if take {
                            *best = Some(Datum::Text(s.to_string()));
                        }
                    }
                    _ => fold.add_other("TEXT", &Datum::Text(s.to_string()))?,
                }
            }
        }
        Column::DateTime { vals, null } => {
            if null[row] == 0 {
                fold.add_other("DATETIME", &Datum::DateTime(vals[row]))?;
            }
        }
    }
    Ok(())
}

/// Fold the active rows of one column into `fold`.
fn fold_column(batch: &ColumnBatch, ci: usize, fold: &mut AggFold) -> Result<()> {
    for &i in &batch.sel {
        fold_cell(batch, ci, i as usize, fold)?;
    }
    Ok(())
}

/// Materialize one decoded cell as an owned Datum (group keys).
fn cell_datum(batch: &ColumnBatch, ci: usize, row: usize) -> Datum {
    match &batch.cols[ci] {
        Column::I64 { vals, null } => {
            if null[row] != 0 { Datum::Null } else { Datum::Int(vals[row]) }
        }
        Column::F64 { vals, null } => {
            if null[row] != 0 { Datum::Null } else { Datum::Float(vals[row]) }
        }
        Column::Bool { vals, null } => {
            if null[row] != 0 { Datum::Null } else { Datum::Bool(vals[row]) }
        }
        Column::Str { data, off, null } => {
            if null[row] != 0 {
                Datum::Null
            } else {
                Datum::Text(
                    std::str::from_utf8(&data[off[row]..off[row + 1]])
                        .unwrap_or("")
                        .to_string(),
                )
            }
        }
        Column::DateTime { vals, null } => {
            if null[row] != 0 { Datum::Null } else { Datum::DateTime(vals[row]) }
        }
    }
}

// ---------------------------------------------------------------------------
// Global aggregate runner
// ---------------------------------------------------------------------------

/// Eligibility for the batch path: top-level statement (no correlation
/// frames), subquery-free predicate of supported shapes, and a FullScan
/// access plan (indexed seeks are already optimal — they stay scalar, so
/// IN-list order and seek behavior never change).
pub(super) fn applicable(
    session: &Session,
    table: &Arc<Table>,
    selection: Option<&Expr>,
) -> Result<bool> {
    if !session.subq.outer.is_empty() {
        return Ok(false);
    }
    if let Some(s) = selection {
        if has_subquery(s) || !shapes_supported(s) {
            return Ok(false);
        }
    }
    Ok(matches!(
        choose_access_path(table, selection)?.path,
        AccessPath::FullScan
    ))
}

/// Stream morsels through the batch pipeline: direct raw leaf scan on the
/// fast path (no intermediate Vec<Datum>), falling back to visible_rows
/// when staged writes or consistent snapshots require overlay/history resolution.
fn scan_batch_morsels<F>(
    db: &Database,
    session: &Session,
    table: &Arc<Table>,
    schema: &Schema,
    selection: Option<&Expr>,
    need: &[bool],
    deadline: Option<std::time::Instant>,
    mut on_morsel: F,
) -> Result<Option<()>>
where
    F: FnMut(&mut ColumnBatch) -> Result<()>,
{
    let has_overlay = session.txn.as_ref().map_or(false, |t| {
        t.staged.iter().any(|((tbl, _), _)| tbl == &table.def.name)
    });
    let has_snapshot = session.snapshot.is_some();

    if has_overlay || has_snapshot {
        // Staged writes or consistent snapshot: route through visible_rows for
        // exact time-travel / overlay ordering, then morsel-decode.
        let rows = db.visible_rows(session, table, None)?;
        let mut batch = ColumnBatch::new();
        let mut off = 0usize;
        while off < rows.len() {
            if let Some(dl) = deadline {
                if std::time::Instant::now() > dl {
                    return Err(Error::QueryTimeout);
                }
            }
            let end = (off + MORSEL).min(rows.len());
            let n = end - off;
            batch.prepare_morsel(schema, need, n);
            for r in &rows[off..end] {
                if batch.push_datum_row(schema, r, need).is_none() {
                    return Ok(None);
                }
            }
            batch.finish_morsel(need, n);
            if let Some(sel) = selection {
                if filter_batch(&mut batch, schema, sel).is_none() {
                    return Ok(None);
                }
            }
            on_morsel(&mut batch)?;
            off = end;
        }
        return Ok(Some(()));
    }

    // Fast path: direct raw leaf scan from B+ tree without intermediate Vec<Datum>.
    let mut batch = ColumnBatch::new();
    batch.prepare_morsel(schema, need, MORSEL);
    let mut row_count = 0usize;
    let mut check_counter = 0usize;
    let mut decode_error = false;
    let mut timeout_error = false;
    let mut morsel_err: Option<Error> = None;

    table.tree().scan_leaves(|_keys, vals| {
        for raw in vals {
            check_counter += 1;
            if check_counter % 256 == 0 {
                if let Some(dl) = deadline {
                    if std::time::Instant::now() > dl {
                        timeout_error = true;
                        return false;
                    }
                }
            }
            let resolved = match table.resolve_value(raw) {
                Ok(r) => r,
                Err(_) => {
                    decode_error = true;
                    return false;
                }
            };
            if batch.push_raw_row(schema, &resolved, need).is_none() {
                decode_error = true;
                return false;
            }
            row_count += 1;
            if row_count == MORSEL {
                batch.finish_morsel(need, row_count);
                if let Some(sel) = selection {
                    if filter_batch(&mut batch, schema, sel).is_none() {
                        decode_error = true;
                        return false;
                    }
                }
                if let Err(e) = on_morsel(&mut batch) {
                    morsel_err = Some(e);
                    return false;
                }
                batch.prepare_morsel(schema, need, MORSEL);
                row_count = 0;
            }
        }
        true
    });

    if timeout_error {
        return Err(Error::QueryTimeout);
    }
    if let Some(err) = morsel_err {
        return Err(err);
    }
    if decode_error {
        return Ok(None);
    }
    if row_count > 0 {
        batch.finish_morsel(need, row_count);
        if let Some(sel) = selection {
            if filter_batch(&mut batch, schema, sel).is_none() {
                return Ok(None);
            }
        }
        on_morsel(&mut batch)?;
    }

    Ok(Some(()))
}

/// Global aggregation over a single table through the batch path.
/// `Ok(None)` = not eligible or undecodable — the caller runs the scalar
/// executor. Otherwise returns the finished single-row output.
pub(super) fn try_global_agg(
    db: &Database,
    session: &mut Session,
    table: &Arc<Table>,
    selection: Option<&Expr>,
    aggs: &[(AggSpec, String)],
) -> Result<Option<Output>> {
    if !applicable(session, table, selection)? {
        return Ok(None);
    }
    /// One output item's fold state: what to compute, where, and the state.
    enum Item {
        Count(u64),
        Fold { col: usize, fold: AggFold },
        Const,
    }
    let mut items = Vec::with_capacity(aggs.len());
    for (spec, _) in aggs {
        match spec {
            AggSpec::Count => items.push(Item::Count(0)),
            AggSpec::Agg(func, idx) => items.push(Item::Fold {
                col: *idx,
                fold: fold_for(*func),
            }),
            AggSpec::Scalar(_) => items.push(Item::Const),
        }
    }
    let schema = table.schema();
    // Decode only touched columns (filters + aggregates); extras would be
    // pure memcpy on the hot path.
    let extra: Vec<usize> = aggs
        .iter()
        .filter_map(|(spec, _)| match spec {
            AggSpec::Agg(_, idx) => Some(*idx),
            _ => None,
        })
        .collect();
    let need = need_mask(schema, selection, &extra);
    let deadline = session.max_execution_time.map(|t| std::time::Instant::now() + t);

    let res = scan_batch_morsels(
        db,
        session,
        table,
        schema,
        selection,
        &need,
        deadline,
        |batch| {
            for item in items.iter_mut() {
                match item {
                    Item::Count(n) => *n += batch.sel.len() as u64,
                    Item::Fold { col, fold, .. } => fold_column(batch, *col, fold)?,
                    Item::Const => {}
                }
            }
            Ok(())
        },
    )?;
    if res.is_none() {
        return Ok(None);
    }

    let mut out_row = Vec::with_capacity(aggs.len());
    let mut out_columns = Vec::with_capacity(aggs.len());
    for ((spec, name), item) in aggs.iter().zip(items.into_iter()) {
        out_columns.push(name.clone());
        match (spec, item) {
            (AggSpec::Count, Item::Count(n)) => out_row.push(Datum::Int(n as i64)),
            (AggSpec::Agg(func, _), Item::Fold { fold, .. }) => out_row.push(fold.finish(*func)),
            (AggSpec::Scalar(d), Item::Const) => out_row.push(d.clone()),
            _ => unreachable!("batch items mirror the agg specs"),
        }
    }
    Ok(Some(Output { columns: out_columns, rows: vec![out_row], message: "OK".into() }))
}

// ---------------------------------------------------------------------------
// Grouped aggregate runner (mirrors `exec_grouped` exactly)
// ---------------------------------------------------------------------------

/// Per-group accumulation: row count plus one fold per aggregate item
/// (`None` slots align with Key/Const projections).
struct GroupAcc {
    count: u64,
    folds: Vec<Option<AggFold>>,
}

/// Single-table GROUP BY through the batch path. Groups form in the same
/// `BTreeMap<Vec<Datum>, _>` keyed identically to the scalar executor, so
/// group membership, output order, and empty-group semantics match; folds
/// update incrementally per active row instead of materializing member-row
/// lists. Error values match because decode homogeneity guarantees every
/// failing cell carries the same type the scalar path would report.
/// `Ok(None)` = decline to scalar.
pub(super) fn try_grouped_agg(
    db: &Database,
    session: &mut Session,
    table: &Arc<Table>,
    selection: Option<&Expr>,
    items: &[crate::sql::SelectItem],
    group_by: &[String],
    order_by: Vec<(String, bool)>,
    limit: Option<usize>,
) -> Result<Option<Output>> {
    if !applicable(session, table, selection)? {
        return Ok(None);
    }
    let scope = std::slice::from_ref(table);
    let (out_columns, projs, key_idx) =
        Database::resolve_grouped_projs(items, scope, group_by)?;
    // Per-projection fold template (function per aggregate slot) +
    // aggregate column positions.
    let mut template: Vec<Option<AggFunc>> = Vec::with_capacity(projs.len());
    let mut agg_col: Vec<Option<usize>> = Vec::with_capacity(projs.len());
    for p in &projs {
        match p {
            GProj::Agg(Some((func, idx))) => {
                template.push(Some(*func));
                agg_col.push(Some(*idx));
            }
            _ => {
                template.push(None);
                agg_col.push(None);
            }
        }
    }
    let schema = table.schema();
    let mut extra: Vec<usize> = key_idx.clone();
    for c in agg_col.iter().flatten() {
        extra.push(*c);
    }
    let need = need_mask(schema, selection, &extra);
    let deadline = session.max_execution_time.map(|t| std::time::Instant::now() + t);
    let mut groups: std::collections::BTreeMap<Vec<Datum>, GroupAcc> = Default::default();

    let res = scan_batch_morsels(
        db,
        session,
        table,
        schema,
        selection,
        &need,
        deadline,
        |batch| {
            for &i in &batch.sel {
                let r = i as usize;
                let key: Vec<Datum> =
                    key_idx.iter().map(|&k| cell_datum(batch, k, r)).collect();
                let g = groups.entry(key).or_insert_with(|| GroupAcc {
                    count: 0,
                    folds: template.iter().map(|f| f.map(fold_for)).collect(),
                });
                g.count += 1;
                for (pi, f) in g.folds.iter_mut().enumerate() {
                    if let Some(fold) = f {
                        fold_cell(batch, agg_col[pi].expect("fold column"), r, fold)?;
                    }
                }
            }
            Ok(())
        },
    )?;
    if res.is_none() {
        return Ok(None);
    }
    // Abort before emitting anything when a fold failed: errors surface
    // identically to the scalar path (whole query fails).
    let mut paired: Vec<(Vec<Datum>, Vec<Datum>)> = Vec::with_capacity(groups.len());
    for (key, g) in groups {
        let mut out_row = Vec::with_capacity(projs.len());
        for (p, f) in projs.iter().zip(g.folds.into_iter()) {
            match (p, f) {
                (GProj::Key(pos), _) => out_row.push(key[*pos].clone()),
                (GProj::Agg(None), _) => out_row.push(Datum::Int(g.count as i64)),
                (GProj::Agg(Some((func, _))), Some(fold)) => out_row.push(fold.finish(*func)),
                _ => unreachable!("fold slots mirror aggregate projections"),
            }
        }
        paired.push((out_row, key));
    }
    Database::assemble_grouped_output(out_columns, paired, group_by, order_by, limit).map(Some)
}
