use crate::table::FkAction;
use crate::types::Datum;

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnSpec {
    pub name: String,
    pub ctype: String,
    pub not_null: bool,
    pub primary_key: bool,
    pub auto_increment: bool,
    pub default_value: Option<Datum>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunc {
    Sum,
    Avg,
    Min,
    Max,
}

impl AggFunc {
    pub fn name(self) -> &'static str {
        match self {
            AggFunc::Sum => "SUM",
            AggFunc::Avg => "AVG",
            AggFunc::Min => "MIN",
            AggFunc::Max => "MAX",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SelectItem {
    Star,
    Column(String),
    CountStar,
    Aggregate { func: AggFunc, column: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JoinClause {
    pub kind: JoinKind,
    pub table: String,
    pub on: Expr,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl CmpOp {
    pub(crate) fn apply(&self, a: &Datum, b: &Datum) -> crate::error::Result<bool> {
        Ok(match self {
            CmpOp::Eq => a == b,
            CmpOp::Ne => a != b,
            CmpOp::Lt => a < b,
            CmpOp::Le => a <= b,
            CmpOp::Gt => a > b,
            CmpOp::Ge => a >= b,
        })
    }

    pub(crate) fn flipped(&self) -> CmpOp {
        match self {
            CmpOp::Eq => CmpOp::Eq,
            CmpOp::Ne => CmpOp::Ne,
            CmpOp::Lt => CmpOp::Gt,
            CmpOp::Le => CmpOp::Ge,
            CmpOp::Gt => CmpOp::Lt,
            CmpOp::Ge => CmpOp::Le,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Column(String),
    Literal(Datum),
    Cmp {
        left: Box<Expr>,
        op: CmpOp,
        right: Box<Expr>,
    },
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
    In {
        expr: Box<Expr>,
        values: Vec<Datum>,
        negated: bool,
    },
    Between {
        expr: Box<Expr>,
        lo: Datum,
        hi: Datum,
        negated: bool,
    },
    Like {
        expr: Box<Expr>,
        pattern: String,
        negated: bool,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct ForeignKeySpec {
    pub name: Option<String>,
    pub column: String,
    pub ref_table: String,
    pub ref_column: String,
    pub on_delete: FkAction,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    CreateDatabase {
        name: String,
        if_not_exists: bool,
    },
    DropDatabase {
        name: String,
        if_exists: bool,
    },
    UseDatabase {
        name: String,
    },
    ShowDatabases,
    CreateTable {
        name: String,
        columns: Vec<ColumnSpec>,
        foreign_keys: Vec<ForeignKeySpec>,
    },
    DropTable {
        name: String,
    },
    Insert {
        table: String,
        rows: Vec<Vec<Expr>>,
    },
    Select {
        items: Vec<SelectItem>,
        from: String,
        joins: Vec<JoinClause>,
        selection: Option<Expr>,
        order_by: Vec<(String, bool)>,
        limit: Option<usize>,
        group_by: Vec<String>,
    },
    Update {
        table: String,
        assignments: Vec<(String, Expr)>,
        selection: Option<Expr>,
    },
    Delete {
        table: String,
        selection: Option<Expr>,
    },
    Begin,
    Commit,
    Rollback,
    StartTransaction {
        snapshot: bool,
    },
    ShowTables,
    Checkpoint,
    CreateIndex {
        name: String,
        table: String,
        column: String,
    },
    DropIndex {
        name: String,
        table: String,
    },
    SetVariable {
        name: String,
        value: Datum,
    },
}
