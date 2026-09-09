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
    /// Scalar subquery in the projection list (exactly one row/column).
    Subquery {
        query: Box<SelectStmt>,
        alias: Option<String>,
    },
    /// Bare literal in the projection list (`SELECT 1`, the canonical
    /// `EXISTS (SELECT 1 ...)` body).
    Literal(Datum),
    /// Zero-argument system function (`version()`, `current_schema()`,
    /// `current_database()`, `user()`): evaluated per statement from the
    /// session context, constant across all output rows.
    SysFunc {
        name: String,
        alias: Option<String>,
    },
}

/// A standalone SELECT: the reusable unit for top-level statements,
/// subqueries, and derived tables.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectStmt {
    pub items: Vec<SelectItem>,
    pub from: TableRef,
    pub joins: Vec<JoinClause>,
    pub selection: Option<Expr>,
    pub order_by: Vec<(String, bool)>,
    pub limit: Option<usize>,
    pub group_by: Vec<String>,
}

/// A FROM/JOIN source: a base table by name, or a materialized subquery.
#[derive(Debug, Clone, PartialEq)]
pub enum TableRef {
    Table(String),
    Derived { query: Box<SelectStmt>, alias: String },
    /// FROM-less SELECT (`SELECT version()`): exactly one row, no columns.
    Empty,
}

impl TableRef {
    /// Display name: the table name, the derived alias, or `DUAL` for the
    /// FROM-less single row (MySQL-compatible naming).
    pub fn name(&self) -> &str {
        match self {
            TableRef::Table(n) => n,
            TableRef::Derived { alias, .. } => alias,
            TableRef::Empty => "DUAL",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JoinClause {
    pub kind: JoinKind,
    pub table: TableRef,
    pub on: Expr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    /// `expr [NOT] IN (subquery)`: single-column membership test.
    InSubquery {
        expr: Box<Expr>,
        query: Box<SelectStmt>,
        negated: bool,
    },
    /// `(subquery)` used as a scalar operand: exactly one row/column.
    ScalarSubquery(Box<SelectStmt>),
    /// `[NOT] EXISTS (subquery)`: true when the subquery yields ≥ 1 row.
    Exists {
        query: Box<SelectStmt>,
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

/// Grantable privilege kinds (wire-visible names match MySQL).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Privilege {
    Select,
    Insert,
    Update,
    Delete,
    Create,
    Drop,
    All,
}

impl Privilege {
    pub fn parse(s: &str) -> Option<Privilege> {
        Some(match s.to_ascii_uppercase().as_str() {
            "SELECT" => Privilege::Select,
            "INSERT" => Privilege::Insert,
            "UPDATE" => Privilege::Update,
            "DELETE" => Privilege::Delete,
            "CREATE" => Privilege::Create,
            "DROP" => Privilege::Drop,
            "ALL" | "ALL PRIVILEGES" => Privilege::All,
            _ => return None,
        })
    }

    pub fn name(&self) -> &'static str {
        match self {
            Privilege::Select => "SELECT",
            Privilege::Insert => "INSERT",
            Privilege::Update => "UPDATE",
            Privilege::Delete => "DELETE",
            Privilege::Create => "CREATE",
            Privilege::Drop => "DROP",
            Privilege::All => "ALL PRIVILEGES",
        }
    }

    /// Stable codec byte for `auth.bin` (never reorder).
    pub fn codec_byte(&self) -> u8 {
        match self {
            Privilege::Select => 1,
            Privilege::Insert => 2,
            Privilege::Update => 3,
            Privilege::Delete => 4,
            Privilege::Create => 5,
            Privilege::Drop => 6,
            Privilege::All => 7,
        }
    }

    pub fn from_codec_byte(b: u8) -> Option<Privilege> {
        Some(match b {
            1 => Privilege::Select,
            2 => Privilege::Insert,
            3 => Privilege::Update,
            4 => Privilege::Delete,
            5 => Privilege::Create,
            6 => Privilege::Drop,
            7 => Privilege::All,
            _ => return None,
        })
    }
}

/// Privilege scope: global (`*.*`), database (`db.*`), or table
/// (`db.tbl`, bare `tbl` resolving to the session database at check time).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrantScope {
    Global,
    Database { db: String },
    Table { db: Option<String>, tbl: String },
}

impl GrantScope {
    /// Canonical display (`*.*`, `db.*`, `db.tbl` / `tbl`).
    pub fn display(&self) -> String {
        match self {
            GrantScope::Global => "*.*".into(),
            GrantScope::Database { db } => format!("{db}.*"),
            GrantScope::Table { db: Some(db), tbl } => format!("{db}.{tbl}"),
            GrantScope::Table { db: None, tbl } => tbl.clone(),
        }
    }
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
        from: TableRef,
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
    ShowStatus {
        like: Option<String>,
    },
    ShowEngineStatus,
    ShowProcesslist,
    Checkpoint,
    /// Promote a read-only replica to primary (fails on primaries).
    Promote,
    Backup {
        path: String,
    },
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
    AnalyzeTable {
        table: String,
    },
    Explain {
        analyze: bool,
        statement: Box<Statement>,
    },
    ExplainMemo {
        statement: Box<Statement>,
    },
    CreateUser {
        name: String,
        if_not_exists: bool,
        password: String,
    },
    DropUser {
        name: String,
        if_exists: bool,
    },
    AlterUser {
        name: String,
        password: String,
    },
    Grant {
        privs: Vec<Privilege>,
        scope: GrantScope,
        user: String,
    },
    Revoke {
        privs: Vec<Privilege>,
        scope: GrantScope,
        user: String,
    },
    ShowGrants {
        for_user: Option<String>,
    },
}
