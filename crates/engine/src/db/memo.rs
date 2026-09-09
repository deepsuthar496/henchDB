//! Cascades Memo Query Optimizer & Equivalence Classes.
//!
//! Architecture:
//! - **Memo & Groups**: The Memo maintains equivalence classes (`Group`s) of
//!   expressions that produce the same logical relation. Each group records
//!   its logical properties (cardinality, schema, columns), logical operators,
//!   physical implementations, and memoized optimal physical plan (`best_plan`).
//! - **Deduplication**: Identical logical expressions map to the same `GroupId`
//!   via hash-consing (`expr_to_group`), preventing redundant exploration.
//! - **Transformation Rules**: Logical -> Logical equivalence exploration
//!   (Join Commutativity A ⋈ B == B ⋈ A, Join Associativity
//!   (A ⋈ B) ⋈ C == A ⋈ (B ⋈ C), and Predicate Pushdown).
//! - **Implementation Rules**: Logical -> Physical operator lowering:
//!   - TableScan vs IndexScan (PK Point, PK Range, PK In, Sec Seek, Sec In) vs
//!     Vectorized BatchScan (`ColumnBatch`).
//!   - HashJoin (Left-build vs Right-build) vs NestedLoopJoin.
//!   - BatchAggregate vs ScalarAggregate.
//! - **Cost-Based Search & Pruning**: Branch-and-bound cost limits prune
//!   suboptimal subtrees early, while bounded recursion depth guarantees
//!   sub-millisecond planning latency for OLTP workloads.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

use super::cost::{
    estimate_count, full_scan_cost, selectivity, CPU_TUPLE_COST, INDEX_PAGE_COST, RANDOM_PAGE_COST,
    ROWS_PER_PAGE, SEQ_PAGE_COST,
};
use super::{Database, Session};
use super::plan::{access_path, AccessPath};
use crate::sql::{CmpOp, Expr, JoinKind, SelectItem};
use crate::table::Table;
use crate::types::Datum;

/// Identifier for an Equivalence Class (Group) in the Memo.
pub type GroupId = usize;

/// Build side for physical hash joins.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JoinBuildSide {
    Left,
    Right,
}

// ---------------------------------------------------------------------------
// Logical Operators
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub enum LogicalOp {
    Scan {
        table_idx: usize,
        table_name: String,
    },
    Filter {
        input: GroupId,
        predicate: Expr,
    },
    Join {
        left: GroupId,
        right: GroupId,
        on: Option<Expr>,
        kind: JoinKind,
    },
    Project {
        input: GroupId,
        items: Vec<SelectItem>,
    },
    Aggregate {
        input: GroupId,
        group_by: Vec<String>,
        aggs: Vec<SelectItem>,
    },
}

impl Eq for LogicalOp {}

impl Hash for LogicalOp {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            LogicalOp::Scan { table_idx, table_name } => {
                table_idx.hash(state);
                table_name.hash(state);
            }
            LogicalOp::Filter { input, predicate } => {
                input.hash(state);
                hash_expr(predicate, state);
            }
            LogicalOp::Join { left, right, on, kind } => {
                left.hash(state);
                right.hash(state);
                (*kind as u8).hash(state);
                if let Some(e) = on {
                    1u8.hash(state);
                    hash_expr(e, state);
                } else {
                    0u8.hash(state);
                }
            }
            LogicalOp::Project { input, items } => {
                input.hash(state);
                items.len().hash(state);
                for it in items {
                    hash_select_item(it, state);
                }
            }
            LogicalOp::Aggregate { input, group_by, aggs } => {
                input.hash(state);
                group_by.hash(state);
                aggs.len().hash(state);
                for a in aggs {
                    hash_select_item(a, state);
                }
            }
        }
    }
}

impl LogicalOp {
    pub fn children(&self) -> Vec<GroupId> {
        match self {
            LogicalOp::Scan { .. } => Vec::new(),
            LogicalOp::Filter { input, .. }
            | LogicalOp::Project { input, .. }
            | LogicalOp::Aggregate { input, .. } => vec![*input],
            LogicalOp::Join { left, right, .. } => vec![*left, *right],
        }
    }
}

// ---------------------------------------------------------------------------
// Physical Operators
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub enum PhysicalOp {
    TableScan {
        table_idx: usize,
    },
    IndexScan {
        table_idx: usize,
        path: AccessPath,
    },
    BatchScan {
        table_idx: usize,
        pushed_filter: Option<Expr>,
    },
    HashJoin {
        left: GroupId,
        right: GroupId,
        equi_keys: (usize, usize),
        kind: JoinKind,
        build_side: JoinBuildSide,
    },
    NestedLoopJoin {
        left: GroupId,
        right: GroupId,
        on: Option<Expr>,
        kind: JoinKind,
    },
    BatchAggregate {
        input: GroupId,
        group_by: Vec<String>,
        aggs: Vec<SelectItem>,
    },
    ScalarAggregate {
        input: GroupId,
        group_by: Vec<String>,
        aggs: Vec<SelectItem>,
    },
}

impl PhysicalOp {
    pub fn children(&self) -> Vec<GroupId> {
        match self {
            PhysicalOp::TableScan { .. }
            | PhysicalOp::IndexScan { .. }
            | PhysicalOp::BatchScan { .. } => Vec::new(),
            PhysicalOp::HashJoin { left, right, .. }
            | PhysicalOp::NestedLoopJoin { left, right, .. } => vec![*left, *right],
            PhysicalOp::BatchAggregate { input, .. }
            | PhysicalOp::ScalarAggregate { input, .. } => vec![*input],
        }
    }
}

// ---------------------------------------------------------------------------
// Logical Properties & Memo Groups
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct LogicalProperties {
    pub tables: Vec<usize>,
    pub est_rows: f64,
    pub output_columns: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct PhysicalMember {
    pub op: PhysicalOp,
    pub self_cost: f64,
    pub total_cost: f64,
}

#[derive(Debug, Clone)]
pub struct BestPlan {
    pub physical_idx: usize,
    pub total_cost: f64,
}

#[derive(Debug, Clone)]
pub struct Group {
    pub id: GroupId,
    pub logical_exprs: Vec<LogicalOp>,
    pub physical_exprs: Vec<PhysicalMember>,
    pub props: LogicalProperties,
    pub best_plan: Option<BestPlan>,
    pub explored: bool,
}

// ---------------------------------------------------------------------------
// Table Metadata & Optimizer Context
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct TableMeta {
    pub table: Arc<Table>,
    pub name: String,
    pub total_rows: f64,
}

pub struct OptimizerContext {
    pub tables: Vec<TableMeta>,
    pub max_depth: usize,
    pub max_transformations: usize,
}

impl OptimizerContext {
    pub fn new(tables: Vec<TableMeta>) -> Self {
        OptimizerContext {
            tables,
            max_depth: 5,
            max_transformations: 64,
        }
    }

    pub fn table_idx(&self, name: &str) -> Option<usize> {
        let (t_name, col) = match name.split_once('.') {
            Some((t, c)) => (Some(t), c),
            None => (None, name),
        };
        for (i, t) in self.tables.iter().enumerate() {
            if let Some(tn) = t_name {
                if t.name == tn || t.table.def.name == tn || t.table.def.name.ends_with(&format!(".{tn}")) {
                    return Some(i);
                }
            } else if t.name == name || t.table.def.name == name || t.table.schema().index_of(col).is_some() {
                return Some(i);
            }
        }
        None
    }
}

// ---------------------------------------------------------------------------
// Memo Container
// ---------------------------------------------------------------------------

pub struct Memo {
    pub groups: Vec<Group>,
    pub expr_to_group: HashMap<LogicalOp, GroupId>,
}

impl Memo {
    pub fn new() -> Self {
        Memo {
            groups: Vec::new(),
            expr_to_group: HashMap::new(),
        }
    }

    pub fn num_groups(&self) -> usize {
        self.groups.len()
    }

    pub fn group(&self, id: GroupId) -> &Group {
        &self.groups[id]
    }

    pub fn group_mut(&mut self, id: GroupId) -> &mut Group {
        &mut self.groups[id]
    }

    /// Insert a logical expression into the Memo. If an equivalent expression
    /// already exists, its GroupId is returned.
    pub fn insert_logical(&mut self, op: LogicalOp, ctx: &OptimizerContext) -> GroupId {
        if let Some(&gid) = self.expr_to_group.get(&op) {
            return gid;
        }

        let id = self.groups.len();
        let props = self.derive_logical_props(&op, ctx);
        let group = Group {
            id,
            logical_exprs: vec![op.clone()],
            physical_exprs: Vec::new(),
            props,
            best_plan: None,
            explored: false,
        };

        self.groups.push(group);
        self.expr_to_group.insert(op, id);
        id
    }

    /// Add a logical expression to an existing group (e.g. from an equivalence rule).
    pub fn add_logical_to_group(&mut self, group_id: GroupId, op: LogicalOp) -> bool {
        if let Some(&existing_gid) = self.expr_to_group.get(&op) {
            if existing_gid == group_id {
                return false;
            }
            return false;
        }

        self.groups[group_id].logical_exprs.push(op.clone());
        self.expr_to_group.insert(op, group_id);
        true
    }

    /// Add a physical implementation candidate to a group.
    pub fn add_physical_to_group(&mut self, group_id: GroupId, op: PhysicalOp, self_cost: f64) {
        if self.groups[group_id].physical_exprs.iter().any(|m| m.op == op) {
            return;
        }

        self.groups[group_id].physical_exprs.push(PhysicalMember {
            op,
            self_cost,
            total_cost: self_cost,
        });
    }

    fn derive_logical_props(&self, op: &LogicalOp, ctx: &OptimizerContext) -> LogicalProperties {
        match op {
            LogicalOp::Scan { table_idx, .. } => {
                let tm = &ctx.tables[*table_idx];
                let output_columns = tm
                    .table
                    .schema()
                    .columns
                    .iter()
                    .map(|c| c.name.clone())
                    .collect();
                LogicalProperties {
                    tables: vec![*table_idx],
                    est_rows: tm.total_rows,
                    output_columns,
                }
            }
            LogicalOp::Filter { input, predicate } => {
                let in_props = &self.groups[*input].props;
                let sel = if in_props.tables.len() == 1 {
                    let ti = in_props.tables[0];
                    let tm = &ctx.tables[ti];
                    let stats = tm.table.stats();
                    selectivity(&tm.table, stats.as_ref(), predicate)
                } else {
                    0.2
                };
                LogicalProperties {
                    tables: in_props.tables.clone(),
                    est_rows: (in_props.est_rows * sel).max(1.0),
                    output_columns: in_props.output_columns.clone(),
                }
            }
            LogicalOp::Join { left, right, .. } => {
                let l_props = &self.groups[*left].props;
                let r_props = &self.groups[*right].props;
                let mut tables = l_props.tables.clone();
                for &t in &r_props.tables {
                    if !tables.contains(&t) {
                        tables.push(t);
                    }
                }
                tables.sort();

                let mut output_columns = l_props.output_columns.clone();
                output_columns.extend(r_props.output_columns.iter().cloned());

                let est_rows = (l_props.est_rows * r_props.est_rows * 0.1).max(1.0);
                LogicalProperties {
                    tables,
                    est_rows,
                    output_columns,
                }
            }
            LogicalOp::Project { input, items } => {
                let in_props = &self.groups[*input].props;
                let mut output_columns = Vec::new();
                for item in items {
                    match item {
                        SelectItem::Star => output_columns.push("*".into()),
                        SelectItem::Column(c) => output_columns.push(c.clone()),
                        SelectItem::CountStar => output_columns.push("count".into()),
                        SelectItem::Aggregate { func, column } => {
                            output_columns.push(format!("{func:?}({column})"))
                        }
                        SelectItem::Subquery { alias, .. } => {
                            output_columns.push(alias.clone().unwrap_or_else(|| "subquery".into()))
                        }
                        SelectItem::Literal(d) => output_columns.push(format!("{d:?}")),
                        SelectItem::SysFunc { name, alias } => {
                            output_columns.push(alias.clone().unwrap_or_else(|| name.clone()))
                        }
                    }
                }
                LogicalProperties {
                    tables: in_props.tables.clone(),
                    est_rows: in_props.est_rows,
                    output_columns,
                }
            }
            LogicalOp::Aggregate { input, group_by, aggs } => {
                let in_props = &self.groups[*input].props;
                let est_rows = if group_by.is_empty() {
                    1.0
                } else {
                    (in_props.est_rows * 0.2).max(1.0)
                };
                let mut output_columns = group_by.clone();
                for a in aggs {
                    match a {
                        SelectItem::Column(c) => output_columns.push(c.clone()),
                        SelectItem::CountStar => output_columns.push("count".into()),
                        SelectItem::Aggregate { func, column } => {
                            output_columns.push(format!("{func:?}({column})"))
                        }
                        _ => output_columns.push("agg".into()),
                    }
                }
                LogicalProperties {
                    tables: in_props.tables.clone(),
                    est_rows,
                    output_columns,
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Transformation & Implementation Rules
// ---------------------------------------------------------------------------

impl Memo {
    /// Explore transformation rules for a group (Logical -> Logical)
    pub fn apply_transformation_rules(
        &mut self,
        group_id: GroupId,
        ctx: &OptimizerContext,
        depth: usize,
    ) {
        if depth == 0 {
            return;
        }

        let exprs = self.groups[group_id].logical_exprs.clone();
        for op in exprs {
            match op {
                // Rule: Join Commutativity & Associativity
                LogicalOp::Join { left, right, on, kind: JoinKind::Inner } => {
                    // 1. Commutativity: A ⋈ B === B ⋈ A
                    let flipped_on = on.as_ref().map(flip_join_on);
                    let commuted = LogicalOp::Join {
                        left: right,
                        right: left,
                        on: flipped_on,
                        kind: JoinKind::Inner,
                    };
                    self.add_logical_to_group(group_id, commuted);

                    // 2. Associativity: (A ⋈ B) ⋈ C === A ⋈ (B ⋈ C)
                    let left_exprs = self.groups[left].logical_exprs.clone();
                    for left_op in left_exprs {
                        if let LogicalOp::Join {
                            left: a,
                            right: b,
                            on: on_ab,
                            kind: JoinKind::Inner,
                        } = left_op
                        {
                            let sub_join = self.insert_logical(
                                LogicalOp::Join {
                                    left: b,
                                    right,
                                    on: on.clone(),
                                    kind: JoinKind::Inner,
                                },
                                ctx,
                            );
                            let assoc_join = LogicalOp::Join {
                                left: a,
                                right: sub_join,
                                on: on_ab,
                                kind: JoinKind::Inner,
                            };
                            self.add_logical_to_group(group_id, assoc_join);
                        }
                    }
                }

                // Rule: Predicate Pushdown through Inner Join
                LogicalOp::Filter { input, predicate } => {
                    let input_exprs = self.groups[input].logical_exprs.clone();
                    for in_op in input_exprs {
                        if let LogicalOp::Join { left, right, on, kind: JoinKind::Inner } = in_op {
                            let l_tables = &self.groups[left].props.tables;
                            let r_tables = &self.groups[right].props.tables;

                            let conjuncts = split_conjuncts(&predicate);
                            let mut left_conj = Vec::new();
                            let mut right_conj = Vec::new();
                            let mut residual = Vec::new();

                            for c in conjuncts {
                                let mut cols = Vec::new();
                                crate::sql::collect_columns(&c, &mut cols);
                                let mut touches_l = false;
                                let mut touches_r = false;
                                let mut valid = true;

                                for col_name in &cols {
                                    if let Some(t_idx) = ctx.table_idx(col_name) {
                                        if l_tables.contains(&t_idx) {
                                            touches_l = true;
                                        } else if r_tables.contains(&t_idx) {
                                            touches_r = true;
                                        } else {
                                            valid = false;
                                        }
                                    } else {
                                        let in_l = self.groups[left].props.output_columns.iter().any(|s| s == col_name);
                                        let in_r = self.groups[right].props.output_columns.iter().any(|s| s == col_name);
                                        if in_l && !in_r {
                                            touches_l = true;
                                        } else if in_r && !in_l {
                                            touches_r = true;
                                        } else {
                                            valid = false;
                                        }
                                    }
                                }

                                if valid && touches_l && !touches_r {
                                    left_conj.push(c);
                                } else if valid && !touches_l && touches_r {
                                    right_conj.push(c);
                                } else {
                                    residual.push(c);
                                }
                            }

                            if !left_conj.is_empty() || !right_conj.is_empty() {
                                let new_left = if !left_conj.is_empty() {
                                    let l_pred = combine_conjuncts(left_conj).unwrap();
                                    self.insert_logical(LogicalOp::Filter { input: left, predicate: l_pred }, ctx)
                                } else {
                                    left
                                };

                                let new_right = if !right_conj.is_empty() {
                                    let r_pred = combine_conjuncts(right_conj).unwrap();
                                    self.insert_logical(LogicalOp::Filter { input: right, predicate: r_pred }, ctx)
                                } else {
                                    right
                                };

                                let mut all_on = Vec::new();
                                if let Some(orig_on) = on {
                                    all_on.extend(split_conjuncts(&orig_on));
                                }
                                all_on.extend(residual);
                                let final_on = combine_conjuncts(all_on);

                                let pushed_join = LogicalOp::Join {
                                    left: new_left,
                                    right: new_right,
                                    on: final_on,
                                    kind: JoinKind::Inner,
                                };
                                self.add_logical_to_group(group_id, pushed_join);
                            }
                        }
                    }
                }

                _ => {}
            }
        }
    }

    /// Apply implementation rules to lower Logical operators to Physical candidates.
    pub fn apply_implementation_rules(&mut self, group_id: GroupId, ctx: &OptimizerContext) {
        let logical_exprs = self.groups[group_id].logical_exprs.clone();
        for op in logical_exprs {
            match op {
                LogicalOp::Scan { table_idx, .. } => {
                    let tm = &ctx.tables[table_idx];
                    let total = tm.total_rows;
                    let full_cost = full_scan_cost(total);

                    self.add_physical_to_group(group_id, PhysicalOp::TableScan { table_idx }, full_cost);

                    if !tm.table.is_ephemeral() {
                        let batch_cost = (total / ROWS_PER_PAGE).ceil().max(1.0) * SEQ_PAGE_COST
                            + total * (CPU_TUPLE_COST * 0.4);
                        self.add_physical_to_group(
                            group_id,
                            PhysicalOp::BatchScan { table_idx, pushed_filter: None },
                            batch_cost,
                        );
                    }
                }

                LogicalOp::Filter { input, predicate } => {
                    let input_exprs = self.groups[input].logical_exprs.clone();
                    for in_op in input_exprs {
                        if let LogicalOp::Scan { table_idx, .. } = in_op {
                            let tm = &ctx.tables[table_idx];
                            let total = tm.total_rows;
                            let stats = tm.table.stats();
                            let sel = selectivity(&tm.table, stats.as_ref(), &predicate);
                            let est_rows = total * sel;
                            let full_cost = full_scan_cost(total);

                            if let Ok(path) = access_path(&tm.table, Some(&predicate)) {
                                match path {
                                    AccessPath::Point(_) => {
                                        let c = (INDEX_PAGE_COST + CPU_TUPLE_COST) * 0.5;
                                        self.add_physical_to_group(group_id, PhysicalOp::IndexScan { table_idx, path }, c);
                                    }
                                    AccessPath::PkIn(ref vals) => {
                                        let c = vals.len() as f64 * (INDEX_PAGE_COST + CPU_TUPLE_COST) * 0.5;
                                        self.add_physical_to_group(group_id, PhysicalOp::IndexScan { table_idx, path }, c);
                                    }
                                    AccessPath::Range { .. } => {
                                        let c = INDEX_PAGE_COST + est_rows * (SEQ_PAGE_COST / ROWS_PER_PAGE + CPU_TUPLE_COST);
                                        self.add_physical_to_group(group_id, PhysicalOp::IndexScan { table_idx, path }, c);
                                    }
                                    AccessPath::SecondaryIndex { .. } => {
                                        let c = INDEX_PAGE_COST
                                            + est_rows * (INDEX_PAGE_COST / ROWS_PER_PAGE + CPU_TUPLE_COST + RANDOM_PAGE_COST);
                                        if c < full_cost {
                                            self.add_physical_to_group(group_id, PhysicalOp::IndexScan { table_idx, path }, c);
                                        }
                                    }
                                    AccessPath::SecIn { ref values, .. } => {
                                        let n = values.len() as f64;
                                        let c = INDEX_PAGE_COST
                                            + n * (INDEX_PAGE_COST / ROWS_PER_PAGE + CPU_TUPLE_COST + RANDOM_PAGE_COST);
                                        if c < full_cost {
                                            self.add_physical_to_group(group_id, PhysicalOp::IndexScan { table_idx, path }, c);
                                        }
                                    }
                                    AccessPath::FullScan => {}
                                }
                            }

                            self.add_physical_to_group(group_id, PhysicalOp::TableScan { table_idx }, full_cost);

                            if !tm.table.is_ephemeral() {
                                let batch_cost = (total / ROWS_PER_PAGE).ceil().max(1.0) * SEQ_PAGE_COST
                                    + total * (CPU_TUPLE_COST * 0.4);
                                self.add_physical_to_group(
                                    group_id,
                                    PhysicalOp::BatchScan {
                                        table_idx,
                                        pushed_filter: Some(predicate.clone()),
                                    },
                                    batch_cost,
                                );
                            }
                        }
                    }
                }

                LogicalOp::Join { left, right, on, kind } => {
                    let l_rows = self.groups[left].props.est_rows;
                    let r_rows = self.groups[right].props.est_rows;

                    let mut equi_pair: Option<(usize, usize)> = None;
                    if let Some(ref on_expr) = on {
                        equi_pair = find_equi_join_indices(
                            on_expr,
                            &self.groups[left].props.output_columns,
                            &self.groups[right].props.output_columns,
                        );
                    }

                    if let Some(equi_keys) = equi_pair {
                        let r_build_cost = r_rows * (CPU_TUPLE_COST * 1.5) + l_rows * CPU_TUPLE_COST;
                        self.add_physical_to_group(
                            group_id,
                            PhysicalOp::HashJoin {
                                left,
                                right,
                                equi_keys,
                                kind,
                                build_side: JoinBuildSide::Right,
                            },
                            r_build_cost,
                        );

                        if kind == JoinKind::Inner {
                            let l_build_cost = l_rows * (CPU_TUPLE_COST * 1.5) + r_rows * CPU_TUPLE_COST;
                            self.add_physical_to_group(
                                group_id,
                                PhysicalOp::HashJoin {
                                    left,
                                    right,
                                    equi_keys,
                                    kind,
                                    build_side: JoinBuildSide::Left,
                                },
                                l_build_cost,
                            );
                        }
                    }

                    let nl_cost = l_rows * (r_rows * CPU_TUPLE_COST).max(1.0);
                    self.add_physical_to_group(
                        group_id,
                        PhysicalOp::NestedLoopJoin {
                            left,
                            right,
                            on: on.clone(),
                            kind,
                        },
                        nl_cost,
                    );
                }

                LogicalOp::Aggregate { input, group_by, aggs } => {
                    let in_rows = self.groups[input].props.est_rows;
                    let batch_cost = in_rows * (CPU_TUPLE_COST * 0.5);
                    let scalar_cost = in_rows * (CPU_TUPLE_COST * 1.2);

                    self.add_physical_to_group(
                        group_id,
                        PhysicalOp::BatchAggregate {
                            input,
                            group_by: group_by.clone(),
                            aggs: aggs.clone(),
                        },
                        batch_cost,
                    );
                    self.add_physical_to_group(
                        group_id,
                        PhysicalOp::ScalarAggregate { input, group_by, aggs },
                        scalar_cost,
                    );
                }

                LogicalOp::Project { .. } => {}
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Cascades Cost-Based Search & Pruning Engine
// ---------------------------------------------------------------------------

impl Memo {
    /// Optimize a group with branch-and-bound cost limits and bounded recursion depth.
    pub fn optimize_group(
        &mut self,
        group_id: GroupId,
        cost_limit: f64,
        depth: usize,
        ctx: &OptimizerContext,
    ) {
        if depth == 0 {
            return;
        }

        // 1. Explore logical equivalence transformations if not already explored
        if !self.groups[group_id].explored {
            self.groups[group_id].explored = true;
            self.apply_transformation_rules(group_id, ctx, depth);
        }

        // 2. Apply implementation rules
        self.apply_implementation_rules(group_id, ctx);

        // 3. Evaluate physical implementations with branch-and-bound pruning
        let num_phys = self.groups[group_id].physical_exprs.len();
        for i in 0..num_phys {
            let (children, self_cost) = {
                let p = &self.groups[group_id].physical_exprs[i];
                (p.op.children(), p.self_cost)
            };

            let mut total_cost = self_cost;
            let mut valid = true;

            for &child_gid in &children {
                if total_cost >= cost_limit {
                    valid = false;
                    break;
                }
                let child_budget = cost_limit - total_cost;
                self.optimize_group(child_gid, child_budget, depth - 1, ctx);
                if let Some(ref best) = self.groups[child_gid].best_plan {
                    total_cost += best.total_cost;
                } else {
                    valid = false;
                    break;
                }
            }

            if valid {
                self.groups[group_id].physical_exprs[i].total_cost = total_cost;
                let is_better = match self.groups[group_id].best_plan {
                    None => true,
                    Some(ref cur) => total_cost < cur.total_cost,
                };
                if is_better {
                    self.groups[group_id].best_plan = Some(BestPlan {
                        physical_idx: i,
                        total_cost,
                    });
                }
            }
        }
    }

    /// Extract the optimal physical execution plan tree from the optimized Memo.
    pub fn extract_best_plan(&self, group_id: GroupId, ctx: &OptimizerContext) -> Option<PhysicalPlan> {
        let group = &self.groups[group_id];
        let best = group.best_plan.as_ref()?;
        let member = &group.physical_exprs[best.physical_idx];
        let est_rows = group.props.est_rows;
        let cost = member.total_cost;

        match &member.op {
            PhysicalOp::TableScan { table_idx } => {
                let tm = &ctx.tables[*table_idx];
                Some(PhysicalPlan::TableScan {
                    table_idx: *table_idx,
                    table_name: tm.name.clone(),
                    cost,
                    est_rows,
                })
            }
            PhysicalOp::IndexScan { table_idx, path } => {
                let tm = &ctx.tables[*table_idx];
                Some(PhysicalPlan::IndexScan {
                    table_idx: *table_idx,
                    table_name: tm.name.clone(),
                    path: path.clone(),
                    cost,
                    est_rows,
                })
            }
            PhysicalOp::BatchScan { table_idx, pushed_filter } => {
                let tm = &ctx.tables[*table_idx];
                Some(PhysicalPlan::BatchScan {
                    table_idx: *table_idx,
                    table_name: tm.name.clone(),
                    pushed_filter: pushed_filter.clone(),
                    cost,
                    est_rows,
                })
            }
            PhysicalOp::HashJoin { left, right, equi_keys, kind, build_side } => {
                let left_plan = self.extract_best_plan(*left, ctx)?;
                let right_plan = self.extract_best_plan(*right, ctx)?;
                Some(PhysicalPlan::HashJoin {
                    left: Box::new(left_plan),
                    right: Box::new(right_plan),
                    equi_keys: *equi_keys,
                    kind: *kind,
                    build_side: *build_side,
                    cost,
                    est_rows,
                })
            }
            PhysicalOp::NestedLoopJoin { left, right, on, kind } => {
                let left_plan = self.extract_best_plan(*left, ctx)?;
                let right_plan = self.extract_best_plan(*right, ctx)?;
                Some(PhysicalPlan::NestedLoopJoin {
                    left: Box::new(left_plan),
                    right: Box::new(right_plan),
                    on: on.clone(),
                    kind: *kind,
                    cost,
                    est_rows,
                })
            }
            PhysicalOp::BatchAggregate { input, group_by, aggs } => {
                let in_plan = self.extract_best_plan(*input, ctx)?;
                Some(PhysicalPlan::BatchAggregate {
                    input: Box::new(in_plan),
                    group_by: group_by.clone(),
                    aggs: aggs.clone(),
                    cost,
                    est_rows,
                })
            }
            PhysicalOp::ScalarAggregate { input, group_by, aggs } => {
                let in_plan = self.extract_best_plan(*input, ctx)?;
                Some(PhysicalPlan::ScalarAggregate {
                    input: Box::new(in_plan),
                    group_by: group_by.clone(),
                    aggs: aggs.clone(),
                    cost,
                    est_rows,
                })
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Physical Plan Tree Representation
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    TableScan {
        table_idx: usize,
        table_name: String,
        cost: f64,
        est_rows: f64,
    },
    IndexScan {
        table_idx: usize,
        table_name: String,
        path: AccessPath,
        cost: f64,
        est_rows: f64,
    },
    BatchScan {
        table_idx: usize,
        table_name: String,
        pushed_filter: Option<Expr>,
        cost: f64,
        est_rows: f64,
    },
    HashJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        equi_keys: (usize, usize),
        kind: JoinKind,
        build_side: JoinBuildSide,
        cost: f64,
        est_rows: f64,
    },
    NestedLoopJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        on: Option<Expr>,
        kind: JoinKind,
        cost: f64,
        est_rows: f64,
    },
    BatchAggregate {
        input: Box<PhysicalPlan>,
        group_by: Vec<String>,
        aggs: Vec<SelectItem>,
        cost: f64,
        est_rows: f64,
    },
    ScalarAggregate {
        input: Box<PhysicalPlan>,
        group_by: Vec<String>,
        aggs: Vec<SelectItem>,
        cost: f64,
        est_rows: f64,
    },
}

impl PhysicalPlan {
    pub fn cost(&self) -> f64 {
        match self {
            PhysicalPlan::TableScan { cost, .. }
            | PhysicalPlan::IndexScan { cost, .. }
            | PhysicalPlan::BatchScan { cost, .. }
            | PhysicalPlan::HashJoin { cost, .. }
            | PhysicalPlan::NestedLoopJoin { cost, .. }
            | PhysicalPlan::BatchAggregate { cost, .. }
            | PhysicalPlan::ScalarAggregate { cost, .. } => *cost,
        }
    }

    pub fn est_rows(&self) -> f64 {
        match self {
            PhysicalPlan::TableScan { est_rows, .. }
            | PhysicalPlan::IndexScan { est_rows, .. }
            | PhysicalPlan::BatchScan { est_rows, .. }
            | PhysicalPlan::HashJoin { est_rows, .. }
            | PhysicalPlan::NestedLoopJoin { est_rows, .. }
            | PhysicalPlan::BatchAggregate { est_rows, .. }
            | PhysicalPlan::ScalarAggregate { est_rows, .. } => *est_rows,
        }
    }

    /// Extract table scan order from left-to-right in the execution tree.
    pub fn table_order(&self) -> Vec<usize> {
        let mut out = Vec::new();
        self.collect_table_indices(&mut out);
        out
    }

    fn collect_table_indices(&self, out: &mut Vec<usize>) {
        match self {
            PhysicalPlan::TableScan { table_idx, .. }
            | PhysicalPlan::IndexScan { table_idx, .. }
            | PhysicalPlan::BatchScan { table_idx, .. } => {
                if !out.contains(table_idx) {
                    out.push(*table_idx);
                }
            }
            PhysicalPlan::HashJoin { left, right, .. }
            | PhysicalPlan::NestedLoopJoin { left, right, .. } => {
                left.collect_table_indices(out);
                right.collect_table_indices(out);
            }
            PhysicalPlan::BatchAggregate { input, .. }
            | PhysicalPlan::ScalarAggregate { input, .. } => {
                input.collect_table_indices(out);
            }
        }
    }

    /// Formats the physical plan as an indented ASCII tree for inspection.
    pub fn collect_explain_rows(&self, group_id: GroupId, out: &mut Vec<Vec<Datum>>) {
        match self {
            PhysicalPlan::TableScan { table_name, cost, est_rows, .. } => {
                out.push(vec![
                    Datum::Int(group_id as i64),
                    Datum::Text("TableScan".into()),
                    Datum::Text(format!("{cost:.2}")),
                    Datum::Text(format!("{est_rows:.1}")),
                    Datum::Text(format!("table: {table_name}")),
                ]);
            }
            PhysicalPlan::IndexScan { table_name, path, cost, est_rows, .. } => {
                let name = super::cost::path_name(path);
                out.push(vec![
                    Datum::Int(group_id as i64),
                    Datum::Text(format!("IndexScan ({name})")),
                    Datum::Text(format!("{cost:.2}")),
                    Datum::Text(format!("{est_rows:.1}")),
                    Datum::Text(format!("table: {table_name}")),
                ]);
            }
            PhysicalPlan::BatchScan { table_name, pushed_filter, cost, est_rows, .. } => {
                let det = match pushed_filter {
                    Some(f) => format!("table: {table_name} (filter: {f:?})"),
                    None => format!("table: {table_name}"),
                };
                out.push(vec![
                    Datum::Int(group_id as i64),
                    Datum::Text("BatchScan (Vectorized)".into()),
                    Datum::Text(format!("{cost:.2}")),
                    Datum::Text(format!("{est_rows:.1}")),
                    Datum::Text(det),
                ]);
            }
            PhysicalPlan::HashJoin { left, right, build_side, cost, est_rows, .. } => {
                left.collect_explain_rows(group_id, out);
                right.collect_explain_rows(group_id, out);
                let bside = match build_side {
                    JoinBuildSide::Left => "BUILD LEFT",
                    JoinBuildSide::Right => "BUILD RIGHT",
                };
                out.push(vec![
                    Datum::Int(group_id as i64),
                    Datum::Text(format!("HashJoin [{bside}]")),
                    Datum::Text(format!("{cost:.2}")),
                    Datum::Text(format!("{est_rows:.1}")),
                    Datum::Null,
                ]);
            }
            PhysicalPlan::NestedLoopJoin { left, right, cost, est_rows, .. } => {
                left.collect_explain_rows(group_id, out);
                right.collect_explain_rows(group_id, out);
                out.push(vec![
                    Datum::Int(group_id as i64),
                    Datum::Text("NestedLoopJoin".into()),
                    Datum::Text(format!("{cost:.2}")),
                    Datum::Text(format!("{est_rows:.1}")),
                    Datum::Null,
                ]);
            }
            PhysicalPlan::BatchAggregate { input, group_by, cost, est_rows, .. } => {
                input.collect_explain_rows(group_id, out);
                out.push(vec![
                    Datum::Int(group_id as i64),
                    Datum::Text("BatchAggregate (Vectorized)".into()),
                    Datum::Text(format!("{cost:.2}")),
                    Datum::Text(format!("{est_rows:.1}")),
                    Datum::Text(format!("groups: {group_by:?}")),
                ]);
            }
            PhysicalPlan::ScalarAggregate { input, group_by, cost, est_rows, .. } => {
                input.collect_explain_rows(group_id, out);
                out.push(vec![
                    Datum::Int(group_id as i64),
                    Datum::Text("ScalarAggregate".into()),
                    Datum::Text(format!("{cost:.2}")),
                    Datum::Text(format!("{est_rows:.1}")),
                    Datum::Text(format!("groups: {group_by:?}")),
                ]);
            }
        }
    }

    pub fn format_tree(&self, indent: usize) -> String {
        let pad = "  ".repeat(indent);
        match self {
            PhysicalPlan::TableScan { table_name, cost, est_rows, .. } => {
                format!("{pad}-> TableScan: {table_name} (est_rows: {est_rows:.0}, cost: {cost:.2})\n")
            }
            PhysicalPlan::IndexScan { table_name, path, cost, est_rows, .. } => {
                let name = super::cost::path_name(path);
                format!("{pad}-> IndexScan [{name}]: {table_name} (est_rows: {est_rows:.0}, cost: {cost:.2})\n")
            }
            PhysicalPlan::BatchScan { table_name, pushed_filter, cost, est_rows, .. } => {
                let filter_str = match pushed_filter {
                    Some(f) => format!(" with pushed filter: {f:?}"),
                    None => String::new(),
                };
                format!("{pad}-> BatchScan: {table_name}{filter_str} (est_rows: {est_rows:.0}, cost: {cost:.2})\n")
            }
            PhysicalPlan::HashJoin { left, right, build_side, cost, est_rows, .. } => {
                let bside = match build_side {
                    JoinBuildSide::Left => "BUILD LEFT",
                    JoinBuildSide::Right => "BUILD RIGHT",
                };
                let mut s = format!("{pad}-> HashJoin [{bside}] (est_rows: {est_rows:.0}, cost: {cost:.2})\n");
                s.push_str(&left.format_tree(indent + 1));
                s.push_str(&right.format_tree(indent + 1));
                s
            }
            PhysicalPlan::NestedLoopJoin { left, right, cost, est_rows, .. } => {
                let mut s = format!("{pad}-> NestedLoopJoin (est_rows: {est_rows:.0}, cost: {cost:.2})\n");
                s.push_str(&left.format_tree(indent + 1));
                s.push_str(&right.format_tree(indent + 1));
                s
            }
            PhysicalPlan::BatchAggregate { input, group_by, cost, est_rows, .. } => {
                let mut s = format!("{pad}-> BatchAggregate (groups: {group_by:?}, est_rows: {est_rows:.0}, cost: {cost:.2})\n");
                s.push_str(&input.format_tree(indent + 1));
                s
            }
            PhysicalPlan::ScalarAggregate { input, group_by, cost, est_rows, .. } => {
                let mut s = format!("{pad}-> ScalarAggregate (groups: {group_by:?}, est_rows: {est_rows:.0}, cost: {cost:.2})\n");
                s.push_str(&input.format_tree(indent + 1));
                s
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers & Hashers
// ---------------------------------------------------------------------------

fn hash_datum<H: std::hash::Hasher>(d: &Datum, state: &mut H) {
    std::mem::discriminant(d).hash(state);
    match d {
        Datum::Null => {}
        Datum::Int(v) => v.hash(state),
        Datum::Float(v) => v.to_bits().hash(state),
        Datum::Text(s) => s.hash(state),
        Datum::Bool(b) => b.hash(state),
        Datum::DateTime(dt) => dt.hash(state),
    }
}

fn hash_expr<H: std::hash::Hasher>(expr: &Expr, state: &mut H) {
    std::mem::discriminant(expr).hash(state);
    match expr {
        Expr::Column(c) => c.hash(state),
        Expr::Literal(d) => hash_datum(d, state),
        Expr::And(a, b) | Expr::Or(a, b) => {
            hash_expr(a, state);
            hash_expr(b, state);
        }
        Expr::Not(e) => hash_expr(e, state),
        Expr::Cmp { left, op, right } => {
            hash_expr(left, state);
            (*op as u8).hash(state);
            hash_expr(right, state);
        }
        Expr::In { expr, values, negated } => {
            hash_expr(expr, state);
            negated.hash(state);
            values.len().hash(state);
            for v in values {
                hash_datum(v, state);
            }
        }
        Expr::Between { expr, lo, hi, negated } => {
            hash_expr(expr, state);
            hash_datum(lo, state);
            hash_datum(hi, state);
            negated.hash(state);
        }
        Expr::Like { expr, pattern, negated } => {
            hash_expr(expr, state);
            pattern.hash(state);
            negated.hash(state);
        }
        Expr::InSubquery { expr, negated, .. } => {
            hash_expr(expr, state);
            negated.hash(state);
        }
        Expr::ScalarSubquery(_) => 1u8.hash(state),
        Expr::Exists { negated, .. } => {
            negated.hash(state);
        }
    }
}

fn hash_select_item<H: std::hash::Hasher>(item: &SelectItem, state: &mut H) {
    std::mem::discriminant(item).hash(state);
    match item {
        SelectItem::Star => {}
        SelectItem::Column(c) => c.hash(state),
        SelectItem::CountStar => {}
        SelectItem::Aggregate { func, column } => {
            (*func as u8).hash(state);
            column.hash(state);
        }
        SelectItem::Subquery { alias, .. } => alias.hash(state),
        SelectItem::Literal(d) => hash_datum(d, state),
        SelectItem::SysFunc { name, alias } => {
            name.hash(state);
            alias.hash(state);
        }
    }
}

fn flip_join_on(expr: &Expr) -> Expr {
    match expr {
        Expr::And(a, b) => Expr::And(Box::new(flip_join_on(a)), Box::new(flip_join_on(b))),
        Expr::Cmp { left, op, right } => match op {
            CmpOp::Eq => Expr::Cmp { left: right.clone(), op: CmpOp::Eq, right: left.clone() },
            CmpOp::Lt => Expr::Cmp { left: right.clone(), op: CmpOp::Gt, right: left.clone() },
            CmpOp::Le => Expr::Cmp { left: right.clone(), op: CmpOp::Ge, right: left.clone() },
            CmpOp::Gt => Expr::Cmp { left: right.clone(), op: CmpOp::Lt, right: left.clone() },
            CmpOp::Ge => Expr::Cmp { left: right.clone(), op: CmpOp::Le, right: left.clone() },
            CmpOp::Ne => Expr::Cmp { left: right.clone(), op: CmpOp::Ne, right: left.clone() },
        },
        other => other.clone(),
    }
}

pub(crate) fn split_conjuncts(expr: &Expr) -> Vec<Expr> {
    let mut out = Vec::new();
    let mut stack = vec![expr];
    while let Some(e) = stack.pop() {
        match e {
            Expr::And(a, b) => {
                stack.push(a);
                stack.push(b);
            }
            other => out.push(other.clone()),
        }
    }
    out
}

pub(crate) fn combine_conjuncts(mut exprs: Vec<Expr>) -> Option<Expr> {
    let mut out = exprs.pop()?;
    while let Some(e) = exprs.pop() {
        out = Expr::And(Box::new(e), Box::new(out));
    }
    Some(out)
}

fn find_equi_join_indices(
    on: &Expr,
    left_cols: &[String],
    right_cols: &[String],
) -> Option<(usize, usize)> {
    match on {
        Expr::And(a, b) => find_equi_join_indices(a, left_cols, right_cols)
            .or_else(|| find_equi_join_indices(b, left_cols, right_cols)),
        Expr::Cmp { left, op: CmpOp::Eq, right } => {
            let (Expr::Column(l_col), Expr::Column(r_col)) = (left.as_ref(), right.as_ref()) else {
                return None;
            };

            let l_bare = l_col.split('.').last().unwrap_or(l_col);
            let r_bare = r_col.split('.').last().unwrap_or(r_col);

            let l_pos = left_cols.iter().position(|c| c == l_col || c == l_bare);
            let r_pos = right_cols.iter().position(|c| c == r_col || c == r_bare);

            if let (Some(l), Some(r)) = (l_pos, r_pos) {
                return Some((l, r));
            }

            let l_flipped = left_cols.iter().position(|c| c == r_col || c == r_bare);
            let r_flipped = right_cols.iter().position(|c| c == l_col || c == l_bare);

            if let (Some(l), Some(r)) = (l_flipped, r_flipped) {
                return Some((l, r));
            }

            None
        }
        _ => None,
    }
}


impl Database {
    /// Optimize a SELECT statement using the Cascades Memo framework.
    pub fn optimize_select_memo(
        &self,
        session: &mut Session,
        select: &crate::sql::SelectStmt,
    ) -> crate::error::Result<(Memo, GroupId, Option<PhysicalPlan>)> {
        use super::subquery;
        let mut tables_meta = Vec::new();
        let from_table = subquery::resolve_table_ref(self, session, &select.from)?;
        let from_name = select.from.name().to_string();
        let total_rows = estimate_count(&from_table);
        tables_meta.push(TableMeta {
            name: from_name,
            table: from_table,
            total_rows,
        });

        for j in &select.joins {
            let j_table = subquery::resolve_table_ref(self, session, &j.table)?;
            let j_name = j.table.name().to_string();
            let total_rows = estimate_count(&j_table);
            tables_meta.push(TableMeta {
                name: j_name,
                table: j_table,
                total_rows,
            });
        }

        let ctx = OptimizerContext::new(tables_meta);
        let mut memo = Memo::new();

        let mut curr_group = memo.insert_logical(
            LogicalOp::Scan {
                table_idx: 0,
                table_name: ctx.tables[0].name.clone(),
            },
            &ctx,
        );

        for (i, j) in select.joins.iter().enumerate() {
            let r_group = memo.insert_logical(
                LogicalOp::Scan {
                    table_idx: i + 1,
                    table_name: ctx.tables[i + 1].name.clone(),
                },
                &ctx,
            );
            curr_group = memo.insert_logical(
                LogicalOp::Join {
                    left: curr_group,
                    right: r_group,
                    on: Some(j.on.clone()),
                    kind: j.kind,
                },
                &ctx,
            );
        }

        if let Some(pred) = &select.selection {
            curr_group = memo.insert_logical(
                LogicalOp::Filter {
                    input: curr_group,
                    predicate: pred.clone(),
                },
                &ctx,
            );
        }

        let has_agg = !select.group_by.is_empty()
            || select.items.iter().any(|it| matches!(it, SelectItem::CountStar | SelectItem::Aggregate { .. }));
        if has_agg {
            curr_group = memo.insert_logical(
                LogicalOp::Aggregate {
                    input: curr_group,
                    group_by: select.group_by.clone(),
                    aggs: select.items.clone(),
                },
                &ctx,
            );
        }

        // Apply transformations with bounded passes
        for _ in 0..2 {
            let num = memo.num_groups();
            for gid in 0..num {
                memo.apply_transformation_rules(gid, &ctx, 2);
            }
        }

        // Apply implementation rules across all groups
        let num = memo.num_groups();
        for gid in 0..num {
            memo.apply_implementation_rules(gid, &ctx);
        }

        // Optimize root group with branch-and-bound pruning
        memo.optimize_group(curr_group, f64::INFINITY, 5, &ctx);

        let best = memo.extract_best_plan(curr_group, &ctx);
        Ok((memo, curr_group, best))
    }
}
