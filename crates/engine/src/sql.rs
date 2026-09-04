//! SQL front-end: hand-written lexer + recursive-descent parser.
//!
//! v0.1 dialect (intentionally small, dependency-free):
//!   CREATE TABLE t (col TYPE [NOT NULL] [PRIMARY KEY] [AUTO_INCREMENT] [, ...])
//!   DROP TABLE t
//!   INSERT INTO t VALUES (lit, ...)[, (lit, ...)]  (NULL in an AUTO_INCREMENT
//!     column assigns the next sequence value, MySQL-style)
//!   SELECT * | cols | COUNT(*) | SUM(col) | AVG(col) | MIN(col) | MAX(col)
//!            FROM t [JOIN u ON ...] [WHERE expr] [GROUP BY col] [ORDER BY col [ASC|DESC]] [LIMIT n]
//!   UPDATE t SET col = lit[, ...] [WHERE expr]
//!   DELETE FROM t [WHERE expr]
//!   BEGIN / COMMIT / ROLLBACK / SHOW TABLES / CHECKPOINT
//!
//! Aggregates are global (no GROUP BY yet): every projection item must be an
//! aggregate, and ORDER BY / LIMIT are ignored for aggregate queries.
//!
//! WHERE expressions: comparisons between a column and a literal, combined
//! with AND. Swap-in of sqlparser-rs for the full MySQL dialect is a roadmap
//! item (see agents.md); the AST below is deliberately close to the algebra
//! the executor consumes.

use crate::error::{Error, Result};
use crate::table::Schema;
use crate::types::Datum;

// ---------------------------------------------------------------------------
// AST
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnSpec {
    pub name: String,
    pub ctype: String,
    pub not_null: bool,
    pub primary_key: bool,
    pub auto_increment: bool,
}

/// Global aggregate functions (no GROUP BY yet — mixing aggregates with
/// plain columns is rejected by the executor).
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

/// JOIN flavor. Only INNER and LEFT are parsed (RIGHT/FULL are rejected).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
}

/// One `JOIN table ON cond` clause. `on` allows column-vs-column
/// comparisons (unlike WHERE, which is column-vs-literal).
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
    pub(crate) fn apply(&self, a: &Datum, b: &Datum) -> Result<bool> {
        Ok(match self {
            CmpOp::Eq => a == b,
            CmpOp::Ne => a != b,
            CmpOp::Lt => a < b,
            CmpOp::Le => a <= b,
            CmpOp::Gt => a > b,
            CmpOp::Ge => a >= b,
        })
    }

    fn flipped(&self) -> CmpOp {
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

/// WHERE expression. Comparisons are column-vs-literal (ANDed); OR has
/// lower precedence than AND; IN/BETWEEN/LIKE test one operand.
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
pub enum Statement {
    CreateTable {
        name: String,
        columns: Vec<ColumnSpec>,
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
}

// ---------------------------------------------------------------------------
// Lexer
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Ident(String),
    Number(String),
    Str(String),
    Sym(char),
    Eof,
}

struct Lexer {
    chars: Vec<char>,
    pos: usize,
}

impl Lexer {
    fn new(src: &str) -> Lexer {
        Lexer {
            chars: src.chars().collect(),
            pos: 0,
        }
    }

    fn tokenize(mut self) -> Result<Vec<Token>> {
        let mut out = Vec::new();
        loop {
            self.skip_ws();
            let Some(&c) = self.chars.get(self.pos) else {
                out.push(Token::Eof);
                break;
            };
            match c {
                '\'' | '"' => out.push(self.read_string(c)?),
                c if c.is_ascii_digit() => out.push(self.read_number()),
                c if c.is_ascii_alphabetic() || c == '_' => {
                    out.push(Token::Ident(self.read_ident()));
                }
                '-' if self.peek_digit() => out.push(self.read_number()),
                ',' | '(' | ')' | '*' | ';' | '=' | '.' => {
                    out.push(Token::Sym(c));
                    self.pos += 1;
                }
                '<' => {
                    self.pos += 1;
                    if self.chars.get(self.pos) == Some(&'=') {
                        self.pos += 1;
                        out.push(Token::Sym('≤'));
                    } else if self.chars.get(self.pos) == Some(&'>') {
                        self.pos += 1;
                        out.push(Token::Sym('≠'));
                    } else {
                        out.push(Token::Sym('<'));
                    }
                }
                '>' => {
                    self.pos += 1;
                    if self.chars.get(self.pos) == Some(&'=') {
                        self.pos += 1;
                        out.push(Token::Sym('≥'));
                    } else {
                        out.push(Token::Sym('>'));
                    }
                }
                '!' => {
                    self.pos += 1;
                    if self.chars.get(self.pos) == Some(&'=') {
                        self.pos += 1;
                        out.push(Token::Sym('≠'));
                    } else {
                        return Err(Error::ParseError("expected '!='".into()));
                    }
                }
                other => {
                    return Err(Error::ParseError(format!("unexpected character '{other}'")));
                }
            }
        }
        Ok(out)
    }

    fn skip_ws(&mut self) {
        while let Some(&c) = self.chars.get(self.pos) {
            if c.is_whitespace() {
                self.pos += 1;
            } else if c == '-' && self.chars.get(self.pos + 1) == Some(&'-') {
                // line comment
                while self.pos < self.chars.len() && self.chars[self.pos] != '\n' {
                    self.pos += 1;
                }
            } else {
                break;
            }
        }
    }

    fn peek_digit(&self) -> bool {
        self.chars
            .get(self.pos + 1)
            .map(|c| c.is_ascii_digit())
            .unwrap_or(false)
    }

    fn read_string(&mut self, quote: char) -> Result<Token> {
        self.pos += 1;
        let mut s = String::new();
        loop {
            match self.chars.get(self.pos) {
                None => return Err(Error::ParseError("unterminated string".into())),
                Some(&c) if c == quote => {
                    self.pos += 1;
                    // doubled quote = escaped quote
                    if self.chars.get(self.pos) == Some(&quote) {
                        s.push(quote);
                        self.pos += 1;
                    } else {
                        break;
                    }
                }
                Some(&c) => {
                    s.push(c);
                    self.pos += 1;
                }
            }
        }
        Ok(Token::Str(s))
    }

    fn read_number(&mut self) -> Token {
        let start = self.pos;
        if self.chars[self.pos] == '-' {
            self.pos += 1;
        }
        let mut is_float = false;
        while let Some(&c) = self.chars.get(self.pos) {
            if c.is_ascii_digit() {
                self.pos += 1;
            } else if c == '.' && !is_float {
                is_float = true;
                self.pos += 1;
            } else {
                break;
            }
        }
        Token::Number(self.chars[start..self.pos].iter().collect())
    }

    fn read_ident(&mut self) -> String {
        let start = self.pos;
        while let Some(&c) = self.chars.get(self.pos) {
            if c.is_ascii_alphanumeric() || c == '_' {
                self.pos += 1;
            } else {
                break;
            }
        }
        self.chars[start..self.pos].iter().collect()
    }
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

pub struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

pub fn parse_sql(src: &str) -> Result<Statement> {
    let tokens = Lexer::new(src).tokenize()?;
    let mut p = Parser { tokens, pos: 0 };
    let stmt = p.parse_statement()?;
    p.eat_sym(';'); // optional trailing semicolon
    if p.peek() != &Token::Eof {
        return Err(Error::ParseError(format!(
            "unexpected trailing input: {:?}",
            p.peek()
        )));
    }
    Ok(stmt)
}

fn kw(tok: &Token) -> Option<String> {
    match tok {
        Token::Ident(s) => Some(s.to_ascii_uppercase()),
        _ => None,
    }
}

/// Aggregate function keyword, if any. These become reserved words in the
/// projection list (a column literally named `sum` needs quoting — the
/// engine has no quoting yet, so rename it).
fn parse_agg_func(keyword: &str) -> Option<AggFunc> {
    match keyword {
        "SUM" => Some(AggFunc::Sum),
        "AVG" => Some(AggFunc::Avg),
        "MIN" => Some(AggFunc::Min),
        "MAX" => Some(AggFunc::Max),
        _ => None,
    }
}

impl Parser {
    fn peek(&self) -> &Token {
        self.tokens.get(self.pos).unwrap_or(&Token::Eof)
    }

    fn next(&mut self) -> Token {
        let t = self.peek().clone();
        self.pos += 1;
        t
    }

    fn eat_sym(&mut self, c: char) -> bool {
        if self.peek() == &Token::Sym(c) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn expect_sym(&mut self, c: char) -> Result<()> {
        if self.eat_sym(c) {
            Ok(())
        } else {
            Err(Error::ParseError(format!("expected '{c}', got {:?}", self.peek())))
        }
    }

    fn eat_kw(&mut self, k: &str) -> bool {
        if kw(self.peek()).as_deref() == Some(k) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn expect_kw(&mut self, k: &str) -> Result<()> {
        if self.eat_kw(k) {
            Ok(())
        } else {
            Err(Error::ParseError(format!("expected {k}, got {:?}", self.peek())))
        }
    }

    fn expect_ident(&mut self) -> Result<String> {
        match self.next() {
            Token::Ident(s) => Ok(s),
            t => Err(Error::ParseError(format!("expected identifier, got {t:?}"))),
        }
    }

    fn parse_statement(&mut self) -> Result<Statement> {
        match kw(self.peek()).as_deref() {
            Some("CREATE") => self.parse_create(),
            Some("DROP") => {
                self.pos += 1;
                if self.eat_kw("TABLE") {
                    let name = self.expect_ident()?;
                    Ok(Statement::DropTable { name })
                } else if self.eat_kw("INDEX") {
                    let name = self.expect_ident()?;
                    self.expect_kw("ON")?;
                    let table = self.expect_ident()?;
                    Ok(Statement::DropIndex { name, table })
                } else {
                    Err(Error::ParseError(format!(
                        "expected TABLE or INDEX after DROP, got {:?}",
                        self.peek()
                    )))
                }
            }
            Some("INSERT") => self.parse_insert(),
            Some("SELECT") => self.parse_select(),
            Some("UPDATE") => self.parse_update(),
            Some("DELETE") => self.parse_delete(),
            Some("BEGIN") => {
                self.pos += 1;
                Ok(Statement::Begin)
            }
            Some("COMMIT") => {
                self.pos += 1;
                Ok(Statement::Commit)
            }
            Some("ROLLBACK") => {
                self.pos += 1;
                Ok(Statement::Rollback)
            }
            Some("SHOW") => {
                self.pos += 1;
                self.expect_kw("TABLES")?;
                Ok(Statement::ShowTables)
            }
            Some("CHECKPOINT") => {
                self.pos += 1;
                Ok(Statement::Checkpoint)
            }
            _ => Err(Error::ParseError(format!(
                "expected statement, got {:?}",
                self.peek()
            ))),
        }
    }

    fn parse_create(&mut self) -> Result<Statement> {
        self.pos += 1;
        if self.eat_kw("TABLE") {
            let name = self.expect_ident()?;
            self.expect_sym('(')?;
            let mut columns = Vec::new();
            loop {
                let cname = self.expect_ident()?;
                let ctype = self.expect_ident()?; // type token is an identifier
                // Optional length specifier: VARCHAR(32), CHAR(10), ...
                if self.eat_sym('(') {
                    match self.next() {
                        Token::Number(_) => {}
                        t => return Err(Error::ParseError(format!("expected type length, got {t:?}"))),
                    }
                    self.expect_sym(')')?;
                }
                let mut not_null = false;
                let mut primary_key = false;
                let mut auto_increment = false;
                loop {
                    if self.eat_kw("PRIMARY") {
                        self.expect_kw("KEY")?;
                        primary_key = true;
                        not_null = true;
                    } else if self.eat_kw("NOT") {
                        self.expect_kw("NULL")?;
                        not_null = true;
                    } else if self.eat_kw("AUTO_INCREMENT") {
                        auto_increment = true;
                    } else {
                        break;
                    }
                }
                columns.push(ColumnSpec {
                    name: cname,
                    ctype,
                    not_null,
                    primary_key,
                    auto_increment,
                });
                if !self.eat_sym(',') {
                    break;
                }
            }
            self.expect_sym(')')?;
            Ok(Statement::CreateTable { name, columns })
        } else if self.eat_kw("INDEX") {
            let name = self.expect_ident()?;
            self.expect_kw("ON")?;
            let table = self.expect_ident()?;
            self.expect_sym('(')?;
            let column = self.expect_ident()?;
            self.expect_sym(')')?;
            Ok(Statement::CreateIndex { name, table, column })
        } else {
            Err(Error::ParseError(format!(
                "expected TABLE or INDEX after CREATE, got {:?}",
                self.peek()
            )))
        }
    }

    fn parse_insert(&mut self) -> Result<Statement> {
        self.pos += 1;
        self.expect_kw("INTO")?;
        let table = self.expect_ident()?;
        self.expect_kw("VALUES")?;
        let mut rows = Vec::new();
        loop {
            self.expect_sym('(')?;
            let mut row = Vec::new();
            loop {
                row.push(self.parse_expr()?);
                if self.eat_sym(',') {
                    continue;
                }
                self.expect_sym(')')?;
                break;
            }
            rows.push(row);
            if self.eat_sym(',') {
                continue;
            }
            break;
        }
        Ok(Statement::Insert { table, rows })
    }

    fn parse_select(&mut self) -> Result<Statement> {
        self.pos += 1;
        let mut items = Vec::new();
        loop {
            if self.peek() == &Token::Sym('*') {
                self.pos += 1;
                items.push(SelectItem::Star);
            } else if kw(self.peek()).as_deref() == Some("COUNT") {
                self.pos += 1;
                self.expect_sym('(')?;
                self.expect_sym('*')?;
                self.expect_sym(')')?;
                items.push(SelectItem::CountStar);
            } else if let Some(func) = kw(self.peek()).as_deref().and_then(parse_agg_func) {
                self.pos += 1;
                self.expect_sym('(')?;
                let column = self.parse_col_ref()?;
                self.expect_sym(')')?;
                items.push(SelectItem::Aggregate { func, column });
            } else {
                items.push(SelectItem::Column(self.parse_col_ref()?));
            }
            if self.eat_sym(',') {
                continue;
            }
            break;
        }
        self.expect_kw("FROM")?;
        let from = self.expect_ident()?;
        let mut joins = Vec::new();
        loop {
            // [INNER | LEFT] JOIN table ON cond. RIGHT/FULL are rejected.
            let kind = if self.eat_kw("INNER") {
                self.eat_kw("JOIN");
                Some(JoinKind::Inner)
            } else if self.eat_kw("LEFT") {
                // LEFT [OUTER] JOIN
                self.eat_kw("OUTER");
                self.expect_kw("JOIN")?;
                Some(JoinKind::Left)
            } else if self.eat_kw("JOIN") {
                Some(JoinKind::Inner)
            } else {
                None
            };
            let Some(kind) = kind else {
                break;
            };
            let table = self.expect_ident()?;
            self.expect_kw("ON")?;
            let on = self.parse_join_cond()?;
            joins.push(JoinClause { kind, table, on });
        }
        let selection = if self.eat_kw("WHERE") {
            Some(self.parse_expr()?)
        } else {
            None
        };
        let group_by = if self.eat_kw("GROUP") {
            self.expect_kw("BY")?;
            let mut keys = vec![self.parse_col_ref()?];
            while self.eat_sym(',') {
                keys.push(self.parse_col_ref()?);
            }
            keys
        } else {
            Vec::new()
        };
        let order_by = if self.eat_kw("ORDER") {
            self.expect_kw("BY")?;
            let mut keys = Vec::new();
            loop {
                let col = self.parse_col_ref()?;
                let desc = if self.eat_kw("DESC") {
                    true
                } else {
                    self.eat_kw("ASC");
                    false
                };
                keys.push((col, desc));
                if !self.eat_sym(',') {
                    break;
                }
            }
            keys
        } else {
            Vec::new()
        };
        let limit = if self.eat_kw("LIMIT") {
            match self.next() {
                Token::Number(n) => Some(n.parse::<usize>().map_err(|_| {
                    Error::ParseError(format!("invalid LIMIT value '{n}'"))
                })?),
                t => return Err(Error::ParseError(format!("expected LIMIT number, got {t:?}"))),
            }
        } else {
            None
        };
        Ok(Statement::Select {
            items,
            from,
            joins,
            selection,
            order_by,
            limit,
            group_by,
        })
    }

    fn parse_update(&mut self) -> Result<Statement> {
        self.pos += 1;
        let table = self.expect_ident()?;
        self.expect_kw("SET")?;
        let mut assignments = Vec::new();
        loop {
            let col = self.expect_ident()?;
            self.expect_sym('=')?;
            let val = self.parse_expr()?;
            assignments.push((col, val));
            if self.eat_sym(',') {
                continue;
            }
            break;
        }
        let selection = if self.eat_kw("WHERE") {
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(Statement::Update {
            table,
            assignments,
            selection,
        })
    }

    fn parse_delete(&mut self) -> Result<Statement> {
        self.pos += 1;
        self.expect_kw("FROM")?;
        let table = self.expect_ident()?;
        let selection = if self.eat_kw("WHERE") {
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(Statement::Delete { table, selection })
    }

    /// Column reference: `col` or qualified `table.col`. Deeper nesting is
    /// rejected (no catalogs in v0.1).
    fn parse_col_ref(&mut self) -> Result<String> {
        let first = self.expect_ident()?;
        if self.eat_sym('.') {
            let second = self.expect_ident()?;
            Ok(format!("{first}.{second}"))
        } else {
            Ok(first)
        }
    }

    /// Join condition: like `parse_expr` but each comparison may also be
    /// column-vs-column (the equi-join case). Literals compare as in WHERE.
    fn parse_join_cond(&mut self) -> Result<Expr> {
        let mut left = self.parse_join_cmp()?;
        while self.eat_kw("AND") {
            let right = self.parse_join_cmp()?;
            left = Expr::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_join_cmp(&mut self) -> Result<Expr> {
        let first = self.parse_operand()?;
        if let Some(op) = self.parse_cmp_op() {
            let second = self.parse_operand()?;
            match (&first, &second) {
                (Expr::Column(_), Expr::Literal(_)) => {
                    return Ok(Expr::Cmp {
                        left: Box::new(first),
                        op,
                        right: Box::new(second),
                    })
                }
                (Expr::Literal(_), Expr::Column(_)) => {
                    return Ok(Expr::Cmp {
                        left: Box::new(second),
                        op: op.flipped(),
                        right: Box::new(first),
                    })
                }
                (Expr::Column(_), Expr::Column(_)) => {
                    return Ok(Expr::Cmp {
                        left: Box::new(first),
                        op,
                        right: Box::new(second),
                    })
                }
                _ => {
                    return Err(Error::NotSupported(
                        "JOIN conditions must compare columns and/or literals".into(),
                    ))
                }
            }
        }
        Ok(first)
    }

    /// expr := or
    /// or := and (OR and)*                      (OR binds loosest)
    /// and := not (AND not)*
    /// not := NOT not | predicate
    /// predicate := '(' expr ')' | cmp_tail
    pub fn parse_expr(&mut self) -> Result<Expr> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<Expr> {
        let mut left = self.parse_and()?;
        while self.eat_kw("OR") {
            let right = self.parse_and()?;
            left = Expr::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Expr> {
        let mut left = self.parse_not()?;
        while self.eat_kw("AND") {
            let right = self.parse_not()?;
            left = Expr::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_not(&mut self) -> Result<Expr> {
        if self.eat_kw("NOT") {
            return Ok(Expr::Not(Box::new(self.parse_not()?)));
        }
        self.parse_predicate()
    }

    fn parse_predicate(&mut self) -> Result<Expr> {
        if self.eat_sym('(') {
            let e = self.parse_expr()?;
            self.expect_sym(')')?;
            return Ok(e);
        }
        self.parse_cmp_tail()
    }

    /// Single comparison or rich predicate over one operand. `BETWEEN`'s
    /// inner AND is consumed here (as bounds), never as a conjunction.
    fn parse_cmp_tail(&mut self) -> Result<Expr> {
        let first = self.parse_operand()?;
        // Comparison operators take precedence over the keyword predicates,
        // so a column literally named `between`/`in`/`like` still compares.
        if let Some(op) = self.parse_cmp_op() {
            let second = self.parse_operand()?;
            // Normalize: column on the left, literal on the right.
            match (&first, &second) {
                (Expr::Column(_), Expr::Literal(_)) => {
                    return Ok(Expr::Cmp {
                        left: Box::new(first),
                        op,
                        right: Box::new(second),
                    })
                }
                (Expr::Literal(_), Expr::Column(_)) => {
                    return Ok(Expr::Cmp {
                        left: Box::new(second),
                        op: op.flipped(),
                        right: Box::new(first),
                    })
                }
                _ => {
                    return Err(Error::NotSupported(
                        "WHERE comparisons must be column vs literal".into(),
                    ))
                }
            }
        }
        if self.eat_kw("BETWEEN") {
            let (lo, hi) = self.parse_between_bounds()?;
            return Ok(Expr::Between { expr: Box::new(first), lo, hi, negated: false });
        }
        if self.eat_kw("NOT") {
            if self.eat_kw("IN") {
                return Ok(Expr::In {
                    expr: Box::new(first),
                    values: self.parse_in_list()?,
                    negated: true,
                });
            }
            if self.eat_kw("LIKE") {
                return Ok(Expr::Like {
                    expr: Box::new(first),
                    pattern: self.parse_like_pattern()?,
                    negated: true,
                });
            }
            if self.eat_kw("BETWEEN") {
                let (lo, hi) = self.parse_between_bounds()?;
                return Ok(Expr::Between { expr:Box::new(first), lo, hi, negated: true });
            }
            return Err(Error::ParseError(format!(
                "expected IN, LIKE or BETWEEN after NOT, got {:?}",
                self.peek()
            )));
        }
        if self.eat_kw("IN") {
            return Ok(Expr::In {
                expr: Box::new(first),
                values: self.parse_in_list()?,
                negated: false,
            });
        }
        if self.eat_kw("LIKE") {
            return Ok(Expr::Like {
                expr: Box::new(first),
                pattern: self.parse_like_pattern()?,
                negated: false,
            });
        }
        Ok(first)
    }

    fn parse_literal_operand(&mut self) -> Result<Datum> {
        match self.parse_operand()? {
            Expr::Literal(d) => Ok(d),
            other => Err(Error::ParseError(format!(
                "expected a literal, got {other:?}"
            ))),
        }
    }

    fn parse_between_bounds(&mut self) -> Result<(Datum, Datum)> {
        let lo = self.parse_literal_operand()?;
        self.expect_kw("AND")?;
        let hi = self.parse_literal_operand()?;
        Ok((lo, hi))
    }

    fn parse_in_list(&mut self) -> Result<Vec<Datum>> {
        self.expect_sym('(')?;
        let mut values = vec![self.parse_literal_operand()?];
        while self.eat_sym(',') {
            values.push(self.parse_literal_operand()?);
        }
        self.expect_sym(')')?;
        Ok(values)
    }

    fn parse_like_pattern(&mut self) -> Result<String> {
        match self.next() {
            Token::Str(s) => Ok(s),
            t => Err(Error::ParseError(format!("LIKE needs a string pattern, got {t:?}"))),
        }
    }

    fn parse_cmp_op(&mut self) -> Option<CmpOp> {
        let op = match self.peek() {
            Token::Sym('=') => Some(CmpOp::Eq),
            Token::Sym('≠') => Some(CmpOp::Ne),
            Token::Sym('<') => Some(CmpOp::Lt),
            Token::Sym('≤') => Some(CmpOp::Le),
            Token::Sym('>') => Some(CmpOp::Gt),
            Token::Sym('≥') => Some(CmpOp::Ge),
            _ => None,
        };
        if op.is_some() {
            self.pos += 1;
        }
        op
    }

    fn parse_operand(&mut self) -> Result<Expr> {
        match self.next() {
            Token::Str(s) => Ok(Expr::Literal(Datum::Text(s))),
            Token::Number(n) => {
                if n.contains('.') {
                    Ok(Expr::Literal(Datum::Float(
                        n.parse::<f64>()
                            .map_err(|_| Error::ParseError(format!("bad number '{n}'")))?,
                    )))
                } else {
                    Ok(Expr::Literal(Datum::Int(
                        n.parse::<i64>()
                            .map_err(|_| Error::ParseError(format!("bad integer '{n}'")))?,
                    )))
                }
            }
            Token::Sym('-') => match self.next() {
                Token::Number(n) => {
                    if n.contains('.') {
                        Ok(Expr::Literal(Datum::Float(
                            n.parse::<f64>()
                                .map_err(|_| Error::ParseError(format!("bad number '{n}'")))?,
                        )))
                    } else {
                        Ok(Expr::Literal(Datum::Int(
                            n.parse::<i64>()
                                .map_err(|_| Error::ParseError(format!("bad integer '{n}'")))?,
                        )))
                    }
                }
                t => Err(Error::ParseError(format!("expected number after '-', got {t:?}"))),
            },
            Token::Ident(s) => {
                let up = s.to_ascii_uppercase();
                Ok(match up.as_str() {
                    "TRUE" => Expr::Literal(Datum::Bool(true)),
                    "FALSE" => Expr::Literal(Datum::Bool(false)),
                    "NULL" => Expr::Literal(Datum::Null),
                    _ => {
                        if self.eat_sym('.') {
                            let col = self.expect_ident()?;
                            Expr::Column(format!("{s}.{col}"))
                        } else {
                            Expr::Column(s)
                        }
                    }
                })
            }
            t => Err(Error::ParseError(format!("expected operand, got {t:?}"))),
        }
    }
}

/// Evaluate a WHERE expression against a row using the schema for column
/// resolution. Numeric comparisons coerce INT<->FLOAT; NULL fails predicates
/// (including negated ones, per SQL three-valued logic in a WHERE filter).
pub fn eval_expr(expr: &Expr, columns: &Schema, row: &[Datum]) -> Result<bool> {
    eval_with(expr, &mut |name| {
        let idx = columns.index_of(name).ok_or_else(|| Error::ColumnNotFound(name.into()))?;
        Ok(row[idx].clone())
    })
}

/// Predicate core shared by single-table (`eval_expr`) and joined
/// (`query.rs`) evaluation: identical NULL/coercion semantics, with column
/// resolution supplied by the caller.
pub(crate) fn eval_with(expr: &Expr, resolve: &mut dyn FnMut(&str) -> Result<Datum>) -> Result<bool> {
    match expr {
        Expr::And(a, b) => Ok(eval_with(a, resolve)? && eval_with(b, resolve)?),
        Expr::Or(a, b) => Ok(eval_with(a, resolve)? || eval_with(b, resolve)?),
        Expr::Not(e) => Ok(!eval_with(e, resolve)?),
        Expr::Cmp { left, op, right } => {
            let l = eval_operand(left, resolve)?;
            let r = eval_operand(right, resolve)?;
            if matches!(l, Datum::Null) || matches!(r, Datum::Null) {
                return Ok(false);
            }
            let (l, r) = coerce_pair(l, r);
            op.apply(&l, &r)
        }
        Expr::In { expr, values, negated } => {
            let target = eval_operand(expr, resolve)?;
            if matches!(target, Datum::Null) {
                return Ok(false);
            }
            let mut hit = false;
            for v in values {
                if matches!(v, Datum::Null) {
                    continue;
                }
                let (t, e) = coerce_pair(target.clone(), v.clone());
                if t == e {
                    hit = true;
                    break;
                }
            }
            Ok(if *negated { !hit } else { hit })
        }
        Expr::Between { expr, lo, hi, negated } => {
            let target = eval_operand(expr, resolve)?;
            if matches!(target, Datum::Null) {
                return Ok(false);
            }
            let (t, l) = coerce_pair(target.clone(), lo.clone());
            let (t, h) = coerce_pair(t, hi.clone());
            let inside = t >= l && t <= h;
            Ok(if *negated { !inside } else { inside })
        }
        Expr::Like { expr, pattern, negated } => {
            let target = eval_operand(expr, resolve)?;
            if matches!(target, Datum::Null) {
                return Ok(false);
            }
            // Non-text values compare by their display form (MySQL coerces).
            let text = target.to_string();
            let matched = like_match(&text, pattern);
            Ok(if *negated { !matched } else { matched })
        }
        other => Err(Error::NotSupported(format!(
            "cannot evaluate {other:?} as a predicate"
        ))),
    }
}

fn eval_operand(e: &Expr, resolve: &mut dyn FnMut(&str) -> Result<Datum>) -> Result<Datum> {
    match e {
        Expr::Literal(d) => Ok(d.clone()),
        Expr::Column(name) => resolve(name),
        other => Err(Error::NotSupported(format!(
            "cannot evaluate {other:?} as a scalar"
        ))),
    }
}

/// Every column reference in an expression, for pre-validation.
pub(crate) fn collect_columns<'a>(expr: &'a Expr, out: &mut Vec<&'a str>) {
    match expr {
        Expr::Column(name) => out.push(name),
        Expr::Literal(_) => {}
        Expr::Cmp { left, right, .. } => {
            collect_columns(left, out);
            collect_columns(right, out);
        }
        Expr::And(a, b) | Expr::Or(a, b) => {
            collect_columns(a, out);
            collect_columns(b, out);
        }
        Expr::Not(e) => collect_columns(e, out),
        Expr::In { expr, .. } | Expr::Between { expr, .. } | Expr::Like { expr, .. } => {
            collect_columns(expr, out)
        }
    }
}

/// SQL LIKE matcher: `%` spans any run (including empty), `_` matches one
/// char, everything else is literal. Case-sensitive (the engine's datum
/// order is binary); backslash is literal, not an escape.
pub fn like_match(text: &str, pattern: &str) -> bool {
    let t: Vec<char> = text.chars().collect();
    let p: Vec<char> = pattern.chars().collect();
    // Memoized recursion over (text pos, pattern pos); patterns are short.
    fn go(t: &[char], p: &[char], memo: &mut std::collections::HashMap<(usize, usize), bool>, ti: usize, pi: usize) -> bool {
        if let Some(&r) = memo.get(&(ti, pi)) {
            return r;
        }
        let r = if pi == p.len() {
            ti == t.len()
        } else if p[pi] == '%' {
            // Collapse runs of %%, then try every split point.
            let mut pj = pi;
            while pj < p.len() && p[pj] == '%' {
                pj += 1;
            }
            (ti..=t.len()).any(|k| go(t, p, memo, k, pj))
        } else if ti < t.len() && (p[pi] == '_' || p[pi] == t[ti]) {
            go(t, p, memo, ti + 1, pi + 1)
        } else {
            false
        };
        memo.insert((ti, pi), r);
        r
    }
    go(&t, &p, &mut std::collections::HashMap::new(), 0, 0)
}

/// Coerce INT/FLOAT pairs so `WHERE f > 1` compares 1.0 against the float col.
pub(crate) fn coerce_pair(l: Datum, r: Datum) -> (Datum, Datum) {
    match (l, r) {
        (Datum::Int(a), Datum::Float(b)) => (Datum::Float(a as f64), Datum::Float(b)),
        (Datum::Float(a), Datum::Int(b)) => (Datum::Float(a), Datum::Float(b as f64)),
        (a, b) => (a, b),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_select_full() {
        let s = parse_sql(
            "SELECT id, name FROM users WHERE age >= 30 AND id < 100 ORDER BY id DESC LIMIT 10;",
        )
        .unwrap();
        match s {
            Statement::Select {
                items,
                from,
                selection,
                order_by,
                limit,
                ..
            } => {
                assert_eq!(
                    items,
                    vec![SelectItem::Column("id".into()), SelectItem::Column("name".into())]
                );
                assert_eq!(from, "users");
                assert!(selection.is_some());
                assert_eq!(order_by, vec![("id".into(), true)]);
                assert_eq!(limit, Some(10));
            }
            other => panic!("wrong stmt {other:?}"),
        }
    }

    #[test]
    fn parse_create_and_insert() {
        let s = parse_sql(
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, score FLOAT)",
        )
        .unwrap();
        match s {
            Statement::CreateTable { name, columns } => {
                assert_eq!(name, "users");
                assert_eq!(columns.len(), 3);
                assert!(columns[0].primary_key);
                assert!(columns[1].not_null);
                assert!(!columns[2].not_null);
            }
            other => panic!("wrong stmt {other:?}"),
        }
        let s = parse_sql("INSERT INTO users VALUES (1, 'ann', 2.5), (2, 'bob', -1.0)").unwrap();
        match s {
            Statement::Insert { table, rows } => {
                assert_eq!(table, "users");
                assert_eq!(rows.len(), 2);
            }
            other => panic!("wrong stmt {other:?}"),
        }
    }

    #[test]
    fn parse_where_flips_literal_first() {
        let s = parse_sql("SELECT * FROM t WHERE 5 < id").unwrap();
        match s {
            Statement::Select { selection: Some(e), .. } => match e {
                Expr::Cmp { left, op, .. } => {
                    assert_eq!(*left, Expr::Column("id".into()));
                    assert_eq!(op, CmpOp::Gt);
                }
                other => panic!("wrong expr {other:?}"),
            },
            other => panic!("wrong stmt {other:?}"),
        }
    }

    #[test]
    fn parse_create_and_drop_index() {
        let s = parse_sql("CREATE INDEX idx_age ON users (age)").unwrap();
        assert_eq!(
            s,
            Statement::CreateIndex {
                name: "idx_age".into(),
                table: "users".into(),
                column: "age".into(),
            }
        );
        let s = parse_sql("DROP INDEX idx_age ON users").unwrap();
        assert_eq!(
            s,
            Statement::DropIndex {
                name: "idx_age".into(),
                table: "users".into(),
            }
        );
    }

    #[test]
    fn rejects_garbage() {
        assert!(parse_sql("FROB NICATE").is_err());
        assert!(parse_sql("SELECT FROM t").is_err());
        assert!(parse_sql("SELECT * FROM t extra").is_err());
    }

    #[test]
    fn parse_or_precedence_and_parens() {
        // OR binds loosest: a=1 OR (b=2 AND c=3).
        let s = parse_sql("SELECT * FROM t WHERE a = 1 OR b = 2 AND c = 3").unwrap();
        match s {
            Statement::Select { selection: Some(e), .. } => match e {
                Expr::Or(left, right) => {
                    assert!(matches!(*left, Expr::Cmp { .. }));
                    assert!(matches!(*right, Expr::And(_, _)));
                }
                other => panic!("wrong shape {other:?}"),
            },
            other => panic!("wrong stmt {other:?}"),
        }
        // Parens override: (a OR b) AND c.
        let s = parse_sql("SELECT * FROM t WHERE (a = 1 OR b = 2) AND c = 3").unwrap();
        match s {
            Statement::Select { selection: Some(e), .. } => match e {
                Expr::And(left, _) => assert!(matches!(*left, Expr::Or(_, _))),
                other => panic!("wrong shape {other:?}"),
            },
            other => panic!("wrong stmt {other:?}"),
        }
        // Bare NOT prefix.
        let s = parse_sql("SELECT * FROM t WHERE NOT a = 1").unwrap();
        match s {
            Statement::Select { selection: Some(Expr::Not(_)), .. } => {}
            other => panic!("wrong stmt {other:?}"),
        }
    }

    #[test]
    fn parse_between_in_like() {
        let s = parse_sql("SELECT * FROM t WHERE id BETWEEN 1 AND 5 AND name NOT LIKE 'a%'").unwrap();
        match s {
            Statement::Select { selection: Some(Expr::And(left, right)), .. } => {
                match *left {
                    Expr::Between { lo: Datum::Int(1), hi: Datum::Int(5), negated: false, .. } => {}
                    other => panic!("wrong between {other:?}"),
                }
                match *right {
                    Expr::Like { pattern, negated: true, .. } => assert_eq!(pattern, "a%"),
                    other => panic!("wrong like {other:?}"),
                }
            }
            other => panic!("wrong stmt {other:?}"),
        }
        let s = parse_sql("SELECT * FROM t WHERE id IN (1, 2, 3) AND id NOT IN (9)").unwrap();
        match s {
            Statement::Select { selection: Some(Expr::And(left, right)), .. } => {
                match *left {
                    Expr::In { values, negated: false, .. } => assert_eq!(values.len(), 3),
                    other => panic!("wrong in {other:?}"),
                }
                match *right {
                    Expr::In { values, negated: true, .. } => assert_eq!(values.len(), 1),
                    other => panic!("wrong not-in {other:?}"),
                }
            }
            other => panic!("wrong stmt {other:?}"),
        }
        // Rejects: empty IN, non-literal IN, non-string LIKE, bare BETWEEN bounds.
        assert!(parse_sql("SELECT * FROM t WHERE id IN ()").is_err());
        assert!(parse_sql("SELECT * FROM t WHERE id IN (1, id)").is_err());
        assert!(parse_sql("SELECT * FROM t WHERE name LIKE 42").is_err());
        assert!(parse_sql("SELECT * FROM t WHERE id BETWEEN a AND b").is_err());
        // Columns literally named like keywords still compare (op first).
        assert!(parse_sql("SELECT * FROM t WHERE between = 1").is_ok());
        assert!(parse_sql("SELECT * FROM t WHERE like = 'a'").is_ok());
    }

    #[test]
    fn like_match_cases() {
        assert!(like_match("abcdef", "abc%"));
        assert!(like_match("abc", "abc"));
        assert!(!like_match("abcd", "abc"));
        assert!(like_match("abc", "a_c"));
        assert!(!like_match("ac", "a_c"));
        assert!(like_match("", "%"));
        assert!(like_match("", ""));
        assert!(!like_match("a", ""));
        assert!(like_match("abXcd", "ab%cd"));
        assert!(!like_match("abXc", "ab%cd"));
        assert!(like_match("aaa", "%a%a%"));
        // Backslash is literal, not an escape.
        assert!(!like_match("ab", "a\\b"));
        assert!(like_match("a\\b", "a\\b"));
    }

    #[test]
    fn parse_auto_increment() {
        let s = parse_sql("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, v TEXT)").unwrap();
        match s {
            Statement::CreateTable { columns, .. } => {
                assert!(columns[0].auto_increment);
                assert!(!columns[1].auto_increment);
            }
            other => panic!("wrong stmt {other:?}"),
        }
        // Modifier order is free.
        let s = parse_sql("CREATE TABLE t (id INT AUTO_INCREMENT PRIMARY KEY)").unwrap();
        match s {
            Statement::CreateTable { columns, .. } => assert!(columns[0].auto_increment),
            other => panic!("wrong stmt {other:?}"),
        }
    }

    #[test]
    fn parse_aggregates() {
        let s = parse_sql("SELECT SUM(score), AVG(score), MIN(id), MAX(name) FROM t WHERE id > 1").unwrap();
        match s {
            Statement::Select { items, from, selection, .. } => {
                assert_eq!(
                    items,
                    vec![
                        SelectItem::Aggregate { func: AggFunc::Sum, column: "score".into() },
                        SelectItem::Aggregate { func: AggFunc::Avg, column: "score".into() },
                        SelectItem::Aggregate { func: AggFunc::Min, column: "id".into() },
                        SelectItem::Aggregate { func: AggFunc::Max, column: "name".into() },
                    ]
                );
                assert_eq!(from, "t");
                assert!(selection.is_some());
            }
            other => panic!("wrong stmt {other:?}"),
        }
        // Aggregates need a column: SUM(*) is rejected.
        assert!(parse_sql("SELECT SUM(*) FROM t").is_err());
        assert!(parse_sql("SELECT SUM() FROM t").is_err());
    }

    #[test]
    fn parse_join_and_group_by() {
        let s = parse_sql(
            "SELECT u.id, o.qty FROM users u JOIN orders o ON u.id = o.user_id WHERE o.qty > 1 GROUP BY u.id ORDER BY u.id LIMIT 5",
        );
        // Aliases are not supported: `users u` must fail cleanly, never panic.
        assert!(s.is_err());
        let s = parse_sql(
            "SELECT users.id, orders.qty FROM users JOIN orders ON users.id = orders.user_id WHERE orders.qty > 1 GROUP BY users.id ORDER BY users.id LIMIT 5",
        )
        .unwrap();
        match s {
            Statement::Select { items, from, joins, selection, order_by, limit, group_by } => {
                assert_eq!(from, "users");
                assert_eq!(items.len(), 2);
                assert_eq!(joins.len(), 1);
                assert_eq!(joins[0].table, "orders");
                assert_eq!(joins[0].kind, JoinKind::Inner);
                assert!(selection.is_some());
                assert_eq!(order_by, vec![("users.id".into(), false)]);
                assert_eq!(limit, Some(5));
                assert_eq!(group_by, vec!["users.id".to_string()]);
            }
            other => panic!("wrong stmt {other:?}"),
        }
        // LEFT [OUTER] JOIN parses; RIGHT/FULL are rejected.
        let s = parse_sql("SELECT * FROM a LEFT OUTER JOIN b ON a.id = b.id").unwrap();
        match s {
            Statement::Select { joins, .. } => assert_eq!(joins[0].kind, JoinKind::Left),
            other => panic!("wrong stmt {other:?}"),
        }
        assert!(parse_sql("SELECT * FROM a RIGHT JOIN b ON a.id = b.id").is_err());
        assert!(parse_sql("SELECT * FROM a FULL JOIN b ON a.id = b.id").is_err());
        // Chained joins parse.
        let s = parse_sql("SELECT * FROM a JOIN b ON a.id = b.a_id JOIN c ON b.id = c.b_id").unwrap();
        match s {
            Statement::Select { joins, .. } => {
                assert_eq!(joins.len(), 2);
                assert_eq!(joins[1].table, "c");
            }
            other => panic!("wrong stmt {other:?}"),
        }
    }
}
