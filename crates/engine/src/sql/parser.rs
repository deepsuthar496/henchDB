use crate::error::{Error, Result};
use crate::table::FkAction;
use crate::types::Datum;

use super::ast::*;
use super::lexer::{Lexer, Token};

pub struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

pub fn parse_sql(src: &str) -> Result<Statement> {
    let tokens = Lexer::new(src).tokenize()?;
    let mut p = Parser { tokens, pos: 0 };
    let stmt = p.parse_statement()?;
    p.eat_sym(';');
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

    /// Parse `RESTRICT`, `CASCADE`, or `SET NULL` after ON DELETE / ON UPDATE.
    fn parse_fk_action(&mut self) -> Result<FkAction> {
        if self.eat_kw("RESTRICT") {
            Ok(FkAction::Restrict)
        } else if self.eat_kw("CASCADE") {
            Ok(FkAction::Cascade)
        } else if self.eat_kw("SET") {
            self.expect_kw("NULL")?;
            Ok(FkAction::SetNull)
        } else {
            Err(Error::ParseError(format!(
                "expected RESTRICT, CASCADE, or SET NULL, got {:?}",
                self.peek()
            )))
        }
    }

    fn parse_statement(&mut self) -> Result<Statement> {
        match kw(self.peek()).as_deref() {
            Some("CREATE") => self.parse_create(),
            Some("DROP") => self.parse_drop(),
            Some("USE") => {
                self.pos += 1;
                let name = self.expect_ident()?;
                Ok(Statement::UseDatabase { name })
            }
            Some("INSERT") => self.parse_insert(),
            Some("SELECT") => self.parse_select(),
            Some("UPDATE") => self.parse_update(),
            Some("DELETE") => self.parse_delete(),
            Some("BEGIN") => {
                self.pos += 1;
                Ok(Statement::Begin)
            }
            Some("START") => {
                // START TRANSACTION [WITH CONSISTENT SNAPSHOT]
                self.pos += 1;
                self.expect_kw("TRANSACTION")?;
                let mut snapshot = false;
                if self.eat_kw("WITH") {
                    self.expect_kw("CONSISTENT")?;
                    self.expect_kw("SNAPSHOT")?;
                    snapshot = true;
                }
                Ok(Statement::StartTransaction { snapshot })
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
                if self.eat_kw("TABLES") {
                    Ok(Statement::ShowTables)
                } else if self.eat_kw("DATABASES") || self.eat_kw("SCHEMAS") {
                    Ok(Statement::ShowDatabases)
                } else if self.eat_kw("STATUS") {
                    // SHOW STATUS [LIKE '<pattern>']
                    let like = if self.eat_kw("LIKE") {
                        match self.parse_literal_operand()? {
                            Datum::Text(p) => Some(p),
                            other => {
                                return Err(Error::ParseError(format!(
                                    "SHOW STATUS LIKE needs a string pattern, got {other:?}"
                                )))
                            }
                        }
                    } else {
                        None
                    };
                    Ok(Statement::ShowStatus { like })
                } else if self.eat_kw("ENGINE") {
                    // SHOW ENGINE STATUS | SHOW ENGINE INNODB STATUS
                    self.eat_kw("INNODB");
                    self.expect_kw("STATUS")?;
                    Ok(Statement::ShowEngineStatus)
                } else if self.eat_kw("PROCESSLIST") {
                    Ok(Statement::ShowProcesslist)
                } else {
                    Err(Error::ParseError(format!(
                        "expected TABLES, DATABASES, STATUS, ENGINE STATUS or PROCESSLIST after SHOW, got {:?}",
                        self.peek()
                    )))
                }
            }
            Some("CHECKPOINT") => {
                self.pos += 1;
                Ok(Statement::Checkpoint)
            }
            Some("PROMOTE") => {
                self.pos += 1;
                Ok(Statement::Promote)
            }
            Some("BACKUP") => {
                self.pos += 1;
                self.expect_kw("DATABASE")?;
                self.expect_kw("TO")?;
                match self.parse_literal_operand()? {
                    Datum::Text(path) => Ok(Statement::Backup { path }),
                    other => Err(Error::ParseError(format!(
                        "BACKUP path must be a string literal, got {other:?}"
                    ))),
                }
            }
            Some("SET") => {
                self.pos += 1;
                let name = self.expect_ident()?;
                self.expect_sym('=')?;
                let value = self.parse_literal_operand()?;
                Ok(Statement::SetVariable { name, value })
            }
            Some("ANALYZE") => {
                self.pos += 1;
                self.expect_kw("TABLE")?;
                let table = self.expect_ident()?;
                Ok(Statement::AnalyzeTable { table })
            }
            Some("EXPLAIN") => {
                self.pos += 1;
                let analyze = self.eat_kw("ANALYZE");
                if kw(self.peek()).as_deref() != Some("SELECT") {
                    return Err(Error::ParseError(format!(
                        "EXPLAIN supports SELECT only, got {:?}",
                        self.peek()
                    )));
                }
                Ok(Statement::Explain {
                    analyze,
                    statement: Box::new(self.parse_select()?),
                })
            }
            Some("DESCRIBE") => {
                // MySQL synonym for EXPLAIN over a SELECT (table describe
                // via DESCRIBE <ident> is not supported: use SHOW TABLES).
                self.pos += 1;
                if kw(self.peek()).as_deref() != Some("SELECT") {
                    return Err(Error::ParseError(format!(
                        "DESCRIBE supports SELECT only, got {:?}",
                        self.peek()
                    )));
                }
                Ok(Statement::Explain {
                    analyze: false,
                    statement: Box::new(self.parse_select()?),
                })
            }
            _ => Err(Error::ParseError(format!(
                "expected statement, got {:?}",
                self.peek()
            ))),
        }
    }

    fn parse_create(&mut self) -> Result<Statement> {
        self.pos += 1;
        if self.eat_kw("DATABASE") || self.eat_kw("SCHEMA") {
            let if_not_exists = if self.eat_kw("IF") {
                self.expect_kw("NOT")?;
                self.expect_kw("EXISTS")?;
                true
            } else {
                false
            };
            let name = self.expect_ident()?;
            Ok(Statement::CreateDatabase { name, if_not_exists })
        } else if self.eat_kw("TABLE") {
            let name = self.expect_ident()?;
            self.expect_sym('(')?;
            let mut columns = Vec::new();
            let mut foreign_keys = Vec::new();
            loop {
                // Table constraint: [CONSTRAINT [name]] FOREIGN KEY (col)
                // REFERENCES reftable(refcol) [ON DELETE action]
                // [ON UPDATE action].
                let mut constraint_name: Option<String> = None;
                if self.eat_kw("CONSTRAINT") {
                    if kw(self.peek()).as_deref() != Some("FOREIGN") {
                        constraint_name = Some(self.expect_ident()?);
                    }
                }
                if self.eat_kw("FOREIGN") {
                    self.expect_kw("KEY")?;
                    self.expect_sym('(')?;
                    let column = self.expect_ident()?;
                    self.expect_sym(')')?;
                    self.expect_kw("REFERENCES")?;
                    let ref_table = self.expect_ident()?;
                    self.expect_sym('(')?;
                    let ref_column = self.expect_ident()?;
                    self.expect_sym(')')?;
                    let mut on_delete = FkAction::Restrict;
                    loop {
                        if self.eat_kw("ON") {
                            if self.eat_kw("DELETE") {
                                on_delete = self.parse_fk_action()?;
                            } else if self.eat_kw("UPDATE") {
                                match self.parse_fk_action()? {
                                    FkAction::Restrict => {}
                                    _ => {
                                        return Err(Error::NotSupported(
                                            "ON UPDATE CASCADE/SET NULL is not supported".into(),
                                        ))
                                    }
                                }
                            } else {
                                return Err(Error::ParseError(format!(
                                    "expected DELETE or UPDATE after ON, got {:?}",
                                    self.peek()
                                )));
                            }
                        } else {
                            break;
                        }
                    }
                    foreign_keys.push(ForeignKeySpec {
                        name: constraint_name,
                        column,
                        ref_table,
                        ref_column,
                        on_delete,
                    });
                    if !self.eat_sym(',') {
                        break;
                    }
                    continue;
                }
                if constraint_name.is_some() {
                    return Err(Error::ParseError(
                        "CONSTRAINT must precede FOREIGN KEY".into(),
                    ));
                }
                let cname = self.expect_ident()?;
                let ctype = self.expect_ident()?;
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
                let mut default_value = None;
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
                    } else if self.eat_kw("DEFAULT") {
                        let lit = self.parse_literal_operand()?;
                        default_value = Some(lit);
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
                    default_value,
                });
                if !self.eat_sym(',') {
                    break;
                }
            }
            self.expect_sym(')')?;
            Ok(Statement::CreateTable { name, columns, foreign_keys })
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
                "expected DATABASE, TABLE, or INDEX after CREATE, got {:?}",
                self.peek()
            )))
        }
    }

    fn parse_drop(&mut self) -> Result<Statement> {
        self.pos += 1;
        if self.eat_kw("DATABASE") || self.eat_kw("SCHEMA") {
            let if_exists = if self.eat_kw("IF") {
                self.expect_kw("EXISTS")?;
                true
            } else {
                false
            };
            let name = self.expect_ident()?;
            Ok(Statement::DropDatabase { name, if_exists })
        } else if self.eat_kw("TABLE") {
            let name = self.expect_ident()?;
            Ok(Statement::DropTable { name })
        } else if self.eat_kw("INDEX") {
            let name = self.expect_ident()?;
            self.expect_kw("ON")?;
            let table = self.expect_ident()?;
            Ok(Statement::DropIndex { name, table })
        } else {
            Err(Error::ParseError(format!(
                "expected DATABASE, TABLE, or INDEX after DROP, got {:?}",
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
        let s = self.parse_select_stmt()?;
        Ok(Statement::Select {
            items: s.items,
            from: s.from,
            joins: s.joins,
            selection: s.selection,
            order_by: s.order_by,
            limit: s.limit,
            group_by: s.group_by,
        })
    }

    /// Parse `SELECT ...` with the cursor on the SELECT keyword (shared by
    /// top-level statements and parenthesized subqueries).
    fn parse_select_stmt(&mut self) -> Result<SelectStmt> {
        self.expect_kw("SELECT")?;
        self.parse_select_body()
    }

    /// Lookahead: keyword at `pos + off` (for `(` SELECT / `IN` SELECT tests).
    fn peek_kw_at(&self, off: usize) -> Option<String> {
        kw(self.tokens.get(self.pos + off).unwrap_or(&Token::Eof))
    }

    /// Parse a `(SELECT ...) [AS] alias` derived table. The cursor is on `(`.
    fn parse_derived(&mut self) -> Result<TableRef> {
        self.expect_sym('(')?;
        if kw(self.peek()).as_deref() != Some("SELECT") {
            return Err(Error::ParseError(format!(
                "expected SELECT after '(' in FROM, got {:?}",
                self.peek()
            )));
        }
        let query = self.parse_select_stmt()?;
        self.expect_sym(')')?;
        self.eat_kw("AS");
        let alias = self.expect_ident()?;
        Ok(TableRef::Derived { query: Box::new(query), alias })
    }

    fn parse_table_ref(&mut self) -> Result<TableRef> {
        if self.peek() == &Token::Sym('(') {
            self.parse_derived()
        } else {
            Ok(TableRef::Table(self.expect_ident()?))
        }
    }

    fn parse_select_body(&mut self) -> Result<SelectStmt> {
        let mut items = Vec::new();
        loop {
            if self.peek() == &Token::Sym('*') {
                self.pos += 1;
                items.push(SelectItem::Star);
            } else if self.peek() == &Token::Sym('(') && self.peek_kw_at(1).as_deref() == Some("SELECT") {
                // Scalar subquery in the projection list, optional AS alias.
                self.pos += 1;
                let query = self.parse_select_stmt()?;
                self.expect_sym(')')?;
                let alias = if self.eat_kw("AS") {
                    Some(self.expect_ident()?)
                } else {
                    None
                };
                items.push(SelectItem::Subquery { query: Box::new(query), alias });
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
            } else if matches!(self.peek(), Token::Number(_) | Token::Str(_))
                || (self.peek() == &Token::Sym('-')
                    && matches!(self.tokens.get(self.pos + 1), Some(Token::Number(_))))
            {
                match self.parse_operand()? {
                    Expr::Literal(d) => items.push(SelectItem::Literal(d)),
                    other => {
                        return Err(Error::ParseError(format!(
                            "expected literal in projection, got {other:?}"
                        )))
                    }
                }
            } else {
                items.push(SelectItem::Column(self.parse_col_ref()?));
            }
            if self.eat_sym(',') {
                continue;
            }
            break;
        }
        self.expect_kw("FROM")?;
        let from = self.parse_table_ref()?;
        let mut joins = Vec::new();
        loop {
            let kind = if self.eat_kw("INNER") {
                self.eat_kw("JOIN");
                Some(JoinKind::Inner)
            } else if self.eat_kw("LEFT") {
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
            let table = self.parse_table_ref()?;
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
        Ok(SelectStmt {
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

    fn parse_col_ref(&mut self) -> Result<String> {
        let first = self.expect_ident()?;
        if self.eat_sym('.') {
            let second = self.expect_ident()?;
            Ok(format!("{first}.{second}"))
        } else {
            Ok(first)
        }
    }

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
            // `NOT EXISTS (SELECT ...)` parses directly (same semantics as
            // NOT (EXISTS ...), but keeps the negated form for the planner).
            if kw(self.peek()).as_deref() == Some("EXISTS") {
                self.pos += 1;
                return Ok(Expr::Exists {
                    query: Box::new(self.parse_exists_body()?),
                    negated: true,
                });
            }
            return Ok(Expr::Not(Box::new(self.parse_not()?)));
        }
        self.parse_predicate()
    }

    /// Parse `(SELECT ...)` after EXISTS (cursor past the keyword).
    fn parse_exists_body(&mut self) -> Result<SelectStmt> {
        self.expect_sym('(')?;
        if kw(self.peek()).as_deref() != Some("SELECT") {
            return Err(Error::ParseError(format!(
                "expected SELECT after EXISTS (, got {:?}",
                self.peek()
            )));
        }
        let stmt = self.parse_select_stmt()?;
        self.expect_sym(')')?;
        Ok(stmt)
    }

    fn parse_predicate(&mut self) -> Result<Expr> {
        if kw(self.peek()).as_deref() == Some("EXISTS") {
            self.pos += 1;
            return Ok(Expr::Exists {
                query: Box::new(self.parse_exists_body()?),
                negated: false,
            });
        }
        // Scalar subquery in left-operand position: `(SELECT ...) [op x]`.
        // Bare `(SELECT ...)` parses (predicate-position truthiness is
        // rejected at execution, like other non-boolean predicates).
        if self.peek() == &Token::Sym('(') && self.peek_kw_at(1).as_deref() == Some("SELECT") {
            self.pos += 1;
            let stmt = self.parse_select_stmt()?;
            self.expect_sym(')')?;
            let scalar = Expr::ScalarSubquery(Box::new(stmt));
            if let Some(op) = self.parse_cmp_op() {
                let second = self.parse_operand()?;
                match &second {
                    Expr::Column(_) | Expr::Literal(_) | Expr::ScalarSubquery(_) => {
                        return Ok(Expr::Cmp {
                            left: Box::new(scalar),
                            op,
                            right: Box::new(second),
                        })
                    }
                    other => {
                        return Err(Error::NotSupported(format!(
                            "cannot compare a subquery with {other:?}"
                        )))
                    }
                }
            }
            return Ok(scalar);
        }
        if self.eat_sym('(') {
            let e = self.parse_expr()?;
            self.expect_sym(')')?;
            return Ok(e);
        }
        self.parse_cmp_tail()
    }

    fn parse_cmp_tail(&mut self) -> Result<Expr> {
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
                // Scalar subqueries compare like values (no flipping: the
                // folded literal lands exactly where the subquery stood,
                // except literal-first which normalizes like columns).
                (Expr::Column(_), Expr::ScalarSubquery(_))
                | (Expr::ScalarSubquery(_), Expr::Column(_))
                | (Expr::ScalarSubquery(_), Expr::Literal(_))
                | (Expr::ScalarSubquery(_), Expr::ScalarSubquery(_)) => {
                    return Ok(Expr::Cmp {
                        left: Box::new(first),
                        op,
                        right: Box::new(second),
                    })
                }
                (Expr::Literal(_), Expr::ScalarSubquery(_)) => {
                    return Ok(Expr::Cmp {
                        left: Box::new(second),
                        op: op.flipped(),
                        right: Box::new(first),
                    })
                }
                // Column-vs-column (same-table filters and correlated
                // subquery equalities): the evaluator compares row values.
                (Expr::Column(_), Expr::Column(_)) => {
                    return Ok(Expr::Cmp {
                        left: Box::new(first),
                        op,
                        right: Box::new(second),
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
                return self.parse_in_tail(first, true);
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
                return Ok(Expr::Between { expr: Box::new(first), lo, hi, negated: true });
            }
            return Err(Error::ParseError(format!(
                "expected IN, LIKE or BETWEEN after NOT, got {:?}",
                self.peek()
            )));
        }
        if self.eat_kw("IN") {
            return self.parse_in_tail(first, false);
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

    /// Parse `IN (...)`: a literal list, or `IN (SELECT ...)` when the
    /// paren holds a subquery.
    fn parse_in_tail(&mut self, first: Expr, negated: bool) -> Result<Expr> {
        if self.peek() == &Token::Sym('(') && self.peek_kw_at(1).as_deref() == Some("SELECT") {
            self.pos += 1;
            let query = self.parse_select_stmt()?;
            self.expect_sym(')')?;
            return Ok(Expr::InSubquery {
                expr: Box::new(first),
                query: Box::new(query),
                negated,
            });
        }
        Ok(Expr::In {
            expr: Box::new(first),
            values: self.parse_in_list()?,
            negated,
        })
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
        // Scalar subquery operand: `(SELECT ...)` (subqueries in any other
        // paren position stay a parse error).
        if self.peek() == &Token::Sym('(') && self.peek_kw_at(1).as_deref() == Some("SELECT") {
            self.pos += 1;
            let stmt = self.parse_select_stmt()?;
            self.expect_sym(')')?;
            return Ok(Expr::ScalarSubquery(Box::new(stmt)));
        }
        match self.next() {
            Token::Str(s) => Ok(Expr::Literal(Datum::Text(s))),
            Token::Number(n) => {
                if n.contains('.') {
                    Ok(Expr::Literal(Datum::Float(
                        n.parse::<f64>()
                            .map_err(|_| Error::ParseError(format!("bad number '{n}'")))?
                    )))
                } else {
                    Ok(Expr::Literal(Datum::Int(
                        n.parse::<i64>()
                            .map_err(|_| Error::ParseError(format!("bad integer '{n}'")))?
                    )))
                }
            }
            Token::Sym('-') => match self.next() {
                Token::Number(n) => {
                    if n.contains('.') {
                        Ok(Expr::Literal(Datum::Float(
                            n.parse::<f64>()
                                .map_err(|_| Error::ParseError(format!("bad number '{n}'")))?
                        )))
                    } else {
                        Ok(Expr::Literal(Datum::Int(
                            n.parse::<i64>()
                                .map_err(|_| Error::ParseError(format!("bad integer '{n}'")))?
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
