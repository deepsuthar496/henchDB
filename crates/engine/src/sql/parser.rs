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

    fn parse_isolation_level(&mut self) -> Result<IsolationLevel> {
        if self.eat_kw("REPEATABLE") {
            self.expect_kw("READ")?;
            Ok(IsolationLevel::RepeatableRead)
        } else if self.eat_kw("READ") {
            if self.eat_kw("COMMITTED") {
                Ok(IsolationLevel::ReadCommitted)
            } else if self.eat_kw("UNCOMMITTED") {
                Ok(IsolationLevel::ReadCommitted)
            } else {
                Err(Error::ParseError("expected COMMITTED or UNCOMMITTED after READ".into()))
            }
        } else if self.eat_kw("SERIALIZABLE") {
            Ok(IsolationLevel::Serializable)
        } else {
            Err(Error::ParseError(format!("unknown isolation level: {:?}", self.peek())))
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
                self.eat_kw("TRANSACTION");
                let mut isolation = None;
                if self.eat_kw("ISOLATION") {
                    self.expect_kw("LEVEL")?;
                    isolation = Some(self.parse_isolation_level()?);
                }
                let mut read_only = false;
                if self.eat_kw("READ") {
                    if self.eat_kw("ONLY") {
                        read_only = true;
                    } else {
                        self.eat_kw("WRITE");
                    }
                }
                Ok(Statement::Begin { isolation, read_only })
            }
            Some("START") => {
                // START TRANSACTION [WITH CONSISTENT SNAPSHOT] [ISOLATION LEVEL ...] [READ ONLY|READ WRITE]
                self.pos += 1;
                self.expect_kw("TRANSACTION")?;
                let mut snapshot = false;
                let mut isolation = None;
                let mut read_only = false;
                while self.peek() != &Token::Eof {
                    if self.eat_kw("WITH") {
                        self.expect_kw("CONSISTENT")?;
                        self.expect_kw("SNAPSHOT")?;
                        snapshot = true;
                    } else if self.eat_kw("ISOLATION") {
                        self.expect_kw("LEVEL")?;
                        isolation = Some(self.parse_isolation_level()?);
                    } else if self.eat_kw("READ") {
                        if self.eat_kw("ONLY") {
                            read_only = true;
                        } else {
                            self.eat_kw("WRITE");
                        }
                    } else {
                        break;
                    }
                }
                Ok(Statement::StartTransaction { snapshot, isolation, read_only })
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
                } else if self.eat_kw("ENGINE") || self.eat_kw("ENGINES") {
                    // SHOW ENGINE [INNODB] [STATUS] | SHOW ENGINES
                    self.eat_kw("INNODB");
                    let _ = self.eat_kw("STATUS");
                    Ok(Statement::ShowEngineStatus)
                } else if self.eat_kw("PROCESSLIST") {
                    Ok(Statement::ShowProcesslist)
                } else if self.eat_kw("GRANTS") {
                    // SHOW GRANTS [FOR user]
                    let for_user = if self.eat_kw("FOR") {
                        Some(self.parse_user_name()?)
                    } else {
                        None
                    };
                    Ok(Statement::ShowGrants { for_user })
                } else {
                    Err(Error::ParseError(format!(
                        "expected TABLES, DATABASES, STATUS, ENGINE STATUS, PROCESSLIST or GRANTS after SHOW, got {:?}",
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
                let is_session = self.eat_kw("SESSION");
                let is_global = if !is_session { self.eat_kw("GLOBAL") } else { false };
                if self.eat_kw("TRANSACTION") {
                    self.expect_kw("ISOLATION")?;
                    self.expect_kw("LEVEL")?;
                    let level = self.parse_isolation_level()?;
                    Ok(Statement::SetTransaction { isolation: level, global: is_global })
                } else if self.eat_kw("NAMES") {
                    let val = self.parse_literal_operand()?;
                    Ok(Statement::SetVariable { name: "names".into(), value: val })
                } else {
                    let name = if self.eat_sym('@') {
                        self.eat_sym('@');
                        self.expect_ident()?
                    } else {
                        self.expect_ident()?
                    };
                    self.expect_sym('=')?;
                    let value = self.parse_literal_operand()?;
                    Ok(Statement::SetVariable { name, value })
                }
            }
            Some("ANALYZE") => {
                self.pos += 1;
                self.expect_kw("TABLE")?;
                let table = self.expect_ident()?;
                Ok(Statement::AnalyzeTable { table })
            }
            Some("CHECK") => {
                self.pos += 1;
                if self.eat_kw("DATABASE") {
                    let db_name = match self.peek() {
                        Token::Ident(_) => Some(self.expect_ident()?),
                        _ => None,
                    };
                    Ok(Statement::CheckDatabase { database: db_name })
                } else {
                    self.expect_kw("TABLE")?;
                    let table = self.expect_ident()?;
                    Ok(Statement::CheckTable { table })
                }
            }
            Some("EXPLAIN") => {
                self.pos += 1;
                let analyze = self.eat_kw("ANALYZE");
                let memo = if !analyze { self.eat_kw("MEMO") } else { false };
                if kw(self.peek()).as_deref() != Some("SELECT") {
                    return Err(Error::ParseError(format!(
                        "EXPLAIN supports SELECT only, got {:?}",
                        self.peek()
                    )));
                }
                if memo {
                    Ok(Statement::ExplainMemo {
                        statement: Box::new(self.parse_select()?),
                    })
                } else {
                    Ok(Statement::Explain {
                        analyze,
                        statement: Box::new(self.parse_select()?),
                    })
                }
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
            Some("GRANT") => self.parse_grant(false),
            Some("REVOKE") => self.parse_grant(true),
            Some("ALTER") => {
                self.pos += 1;
                self.expect_kw("USER")?;
                let name = self.parse_user_name()?;
                self.expect_kw("IDENTIFIED")?;
                self.expect_kw("BY")?;
                let password = self.parse_password()?;
                Ok(Statement::AlterUser { name, password })
            }
            _ => Err(Error::ParseError(format!(
                "expected statement, got {:?}",
                self.peek()
            ))),
        }
    }

    /// Parse a `user` / `'user'` / `"user"` account name with an optional
    /// `@host` specifier (host validated, normalized away: permissions are
    /// per username).
    fn parse_user_name(&mut self) -> Result<String> {
        let name = match self.next() {
            Token::Str(s) => s,
            Token::Ident(s) => s,
            t => {
                return Err(Error::ParseError(format!(
                    "expected user name, got {t:?}"
                )))
            }
        };
        if self.eat_sym('@') {
            match self.next() {
                Token::Str(_) | Token::Ident(_) => {}
                t => {
                    return Err(Error::ParseError(format!(
                        "expected host after '@', got {t:?}"
                    )))
                }
            }
        }
        if name.is_empty() || name.len() > 256 {
            return Err(Error::ParseError("bad user name".into()));
        }
        Ok(name)
    }

    /// Parse `'password'` (string literal only — never an identifier).
    fn parse_password(&mut self) -> Result<String> {
        match self.parse_literal_operand()? {
            Datum::Text(p) => Ok(p),
            other => Err(Error::ParseError(format!(
                "password must be a string literal, got {other:?}"
            ))),
        }
    }

    /// Parse `priv1 [, priv2 ...]` (`ALL [PRIVILEGES]` allowed).
    fn parse_priv_list(&mut self) -> Result<Vec<Privilege>> {
        let mut privs = Vec::new();
        loop {
            let word = self.expect_ident()?;
            let mut p = Privilege::parse(&word).ok_or_else(|| {
                Error::ParseError(format!("unknown privilege '{word}'"))
            })?;
            if p == Privilege::All {
                self.eat_kw("PRIVILEGES");
                p = Privilege::All;
            }
            privs.push(p);
            if !self.eat_sym(',') {
                break;
            }
        }
        Ok(privs)
    }

    /// Parse a grant scope: `*.*` (or bare `*`), `db.*`, `db.tbl`, `tbl`.
    fn parse_grant_scope(&mut self) -> Result<GrantScope> {
        if self.peek() == &Token::Sym('*') {
            self.pos += 1;
            if self.eat_sym('.') {
                self.expect_sym('*')?;
            }
            return Ok(GrantScope::Global);
        }
        let first = match self.next() {
            Token::Str(s) => s,
            Token::Ident(s) => s,
            t => {
                return Err(Error::ParseError(format!(
                    "expected grant scope, got {t:?}"
                )))
            }
        };
        if self.eat_sym('.') {
            if self.peek() == &Token::Sym('*') {
                self.pos += 1;
                Ok(GrantScope::Database { db: first })
            } else {
                let tbl = self.expect_ident()?;
                Ok(GrantScope::Table { db: Some(first), tbl })
            }
        } else {
            Ok(GrantScope::Table { db: None, tbl: first })
        }
    }

    /// Parse `GRANT privs ON scope TO user` / `REVOKE privs ON scope FROM`.
    fn parse_grant(&mut self, revoke: bool) -> Result<Statement> {
        self.pos += 1;
        let privs = self.parse_priv_list()?;
        self.expect_kw("ON")?;
        let scope = self.parse_grant_scope()?;
        self.expect_kw(if revoke { "FROM" } else { "TO" })?;
        let user = self.parse_user_name()?;
        if revoke {
            Ok(Statement::Revoke { privs, scope, user })
        } else {
            Ok(Statement::Grant { privs, scope, user })
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
        } else if self.eat_kw("USER") {
            let if_not_exists = if self.eat_kw("IF") {
                self.expect_kw("NOT")?;
                self.expect_kw("EXISTS")?;
                true
            } else {
                false
            };
            let name = self.parse_user_name()?;
            self.expect_kw("IDENTIFIED")?;
            self.expect_kw("BY")?;
            let password = self.parse_password()?;
            Ok(Statement::CreateUser { name, if_not_exists, password })
        } else if self.eat_kw("TABLE") {
            let if_not_exists = if self.eat_kw("IF") {
                self.expect_kw("NOT")?;
                self.expect_kw("EXISTS")?;
                true
            } else {
                false
            };
            let name = self.expect_ident()?;
            self.expect_sym('(')?;
            let mut columns: Vec<ColumnSpec> = Vec::new();
            let mut foreign_keys = Vec::new();
            loop {
                // Table constraint: [CONSTRAINT [name]] FOREIGN KEY (col)
                // REFERENCES reftable(refcol) [ON DELETE action]
                // [ON UPDATE action] or [CONSTRAINT [name]] PRIMARY KEY (col).
                let mut constraint_name: Option<String> = None;
                if self.eat_kw("CONSTRAINT") {
                    if kw(self.peek()).as_deref() != Some("FOREIGN")
                        && kw(self.peek()).as_deref() != Some("PRIMARY")
                    {
                        constraint_name = Some(self.expect_ident()?);
                    }
                }
                if self.eat_kw("PRIMARY") {
                    self.expect_kw("KEY")?;
                    self.expect_sym('(')?;
                    let pk_col = self.expect_ident()?;
                    self.expect_sym(')')?;
                    let mut found = false;
                    for col in &mut columns {
                        if col.name.eq_ignore_ascii_case(&pk_col) {
                            col.primary_key = true;
                            col.not_null = true;
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        return Err(Error::ParseError(format!(
                            "PRIMARY KEY column '{pk_col}' not found in table definition"
                        )));
                    }
                    if !self.eat_sym(',') {
                        break;
                    }
                    continue;
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
                        "CONSTRAINT must precede FOREIGN KEY or PRIMARY KEY".into(),
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
            Ok(Statement::CreateTable { name, columns, foreign_keys, if_not_exists })
        } else if self.eat_kw("INDEX") {
            let if_not_exists = if self.eat_kw("IF") {
                self.expect_kw("NOT")?;
                self.expect_kw("EXISTS")?;
                true
            } else {
                false
            };
            let name = self.expect_ident()?;
            self.expect_kw("ON")?;
            let table = self.expect_ident()?;
            self.expect_sym('(')?;
            let column = self.expect_ident()?;
            self.expect_sym(')')?;
            Ok(Statement::CreateIndex { name, table, column, if_not_exists })
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
            let if_exists = if self.eat_kw("IF") {
                self.expect_kw("EXISTS")?;
                true
            } else {
                false
            };
            let name = self.expect_ident()?;
            Ok(Statement::DropTable { name, if_exists })
        } else if self.eat_kw("USER") {
            let if_exists = if self.eat_kw("IF") {
                self.expect_kw("EXISTS")?;
                true
            } else {
                false
            };
            let name = self.parse_user_name()?;
            Ok(Statement::DropUser { name, if_exists })
        } else if self.eat_kw("INDEX") {
            let if_exists = if self.eat_kw("IF") {
                self.expect_kw("EXISTS")?;
                true
            } else {
                false
            };
            let name = self.expect_ident()?;
            self.expect_kw("ON")?;
            let table = self.expect_ident()?;
            Ok(Statement::DropIndex { name, table, if_exists })
        } else {
            Err(Error::ParseError(format!(
                "expected DATABASE, TABLE, USER, or INDEX after DROP, got {:?}",
                self.peek()
            )))
        }
    }

    fn parse_insert(&mut self) -> Result<Statement> {
        self.pos += 1;
        self.expect_kw("INTO")?;
        let table = self.expect_ident()?;
        let columns = if self.eat_sym('(') {
            let mut cols = Vec::new();
            loop {
                cols.push(self.expect_ident()?);
                if self.eat_sym(',') {
                    continue;
                }
                self.expect_sym(')')?;
                break;
            }
            Some(cols)
        } else {
            None
        };
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
        Ok(Statement::Insert { table, columns, rows })
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
            // One optional `.` qualifier: `db.table` (cross-database) and
            // `pg_catalog.pg_class` (system views) route by their dotted
            // name; three-part names are rejected.
            let first = self.expect_ident()?;
            if self.eat_sym('.') {
                let second = self.expect_ident()?;
                if self.eat_sym('.') {
                    return Err(Error::ParseError(
                        "three-part table names are not supported".into(),
                    ));
                }
                Ok(TableRef::Table(format!("{first}.{second}")))
            } else {
                Ok(TableRef::Table(first))
            }
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
            } else if matches!(self.peek(), Token::Ident(_))
                && self.tokens.get(self.pos + 1) == Some(&Token::Sym('('))
            {
                // Zero-argument system function in the projection list
                // (`version()`, `current_schema()`, ...): the executor
                // validates the name and evaluates it from the session.
                let Token::Ident(name) = self.next() else {
                    unreachable!()
                };
                self.expect_sym('(')?;
                self.expect_sym(')')?;
                let alias = if self.eat_kw("AS") {
                    Some(self.expect_ident()?)
                } else {
                    None
                };
                // Trailing `::type` casts on system functions are accepted
                // and ignored (all four return text already).
                while self.eat_sym(':') {
                    self.expect_sym(':')?;
                    let target = self.expect_ident()?;
                    if !is_known_cast(&target) {
                        return Err(Error::ParseError(format!(
                            "unknown cast '::{target}'"
                        )));
                    }
                }
                items.push(SelectItem::SysFunc { name, alias });
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
        // FROM is optional: a FROM-less SELECT (`SELECT 1`,
        // `SELECT version()`) yields exactly one row; only
        // row-independent items are valid (checked at execution).
        let from = if self.eat_kw("FROM") {
            self.parse_table_ref()?
        } else {
            TableRef::Empty
        };
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
        let base = match self.next() {
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
        }?;
        // PostgreSQL `::type` casts (ORM introspection staples like
        // `'x'::regclass`, `col::text`): desugared at parse time — casts on
        // columns are comparison-identities, casts on literals fold to the
        // coerced literal. Anything else is a clean parse error.
        let mut e = base;
        while self.eat_sym(':') {
            self.expect_sym(':')?;
            let target = self.expect_ident()?;
            e = apply_cast(e, &target)?;
        }
        Ok(e)
    }
}

/// Cast target names accepted by `::` (and by system-function projection
/// suffixes): the text/identifier family, the integer family, bool, and
/// floats. Anything else fails closed at parse time.
fn is_known_cast(target: &str) -> bool {
    matches!(
        target.to_ascii_uppercase().as_str(),
        "TEXT" | "VARCHAR" | "CHAR" | "BPCHAR" | "NAME" | "REGCLASS" | "OID" | "INT2"
            | "INT4" | "INT8" | "BOOL" | "BOOLEAN" | "FLOAT4" | "FLOAT8"
    )
}

fn apply_cast(base: Expr, target: &str) -> Result<Expr> {
    let t = target.to_ascii_uppercase();
    if !is_known_cast(&t) {
        return Err(Error::ParseError(format!("unknown cast '::{target}'")));
    }
    match base {
        Expr::Column(_) => Ok(base),
        Expr::Literal(d) => Ok(Expr::Literal(coerce_literal(&d, &t, target)?)),
        other => Err(Error::ParseError(format!(
            "cannot cast {other:?} with '::{target}'"
        ))),
    }
}

fn coerce_literal(d: &Datum, t: &str, target: &str) -> Result<Datum> {
    if matches!(d, Datum::Null) {
        return Ok(Datum::Null);
    }
    Ok(match t {
        "TEXT" | "VARCHAR" | "CHAR" | "BPCHAR" | "NAME" | "REGCLASS" => {
            Datum::Text(d.to_string())
        }
        "OID" | "INT2" | "INT4" | "INT8" => match d {
            Datum::Int(i) => Datum::Int(*i),
            Datum::Float(f) => Datum::Int(*f as i64),
            Datum::Bool(b) => Datum::Int(i64::from(*b)),
            Datum::Text(s) => s.trim().parse::<i64>().map(Datum::Int).map_err(|_| {
                Error::ParseError(format!("cannot cast '{s}' to '::{target}'"))
            })?,
            Datum::Null => Datum::Null,
            Datum::DateTime(_) => {
                return Err(Error::ParseError(format!(
                    "cannot cast {d:?} to '::{target}'"
                )))
            }
        },
        "BOOL" | "BOOLEAN" => match d {
            Datum::Bool(b) => Datum::Bool(*b),
            Datum::Int(i) => Datum::Bool(*i != 0),
            Datum::Text(s) => match s.trim().to_ascii_lowercase().as_str() {
                "true" | "t" | "1" => Datum::Bool(true),
                "false" | "f" | "0" => Datum::Bool(false),
                _ => {
                    return Err(Error::ParseError(format!(
                        "cannot cast '{s}' to '::{target}'"
                    )))
                }
            },
            Datum::Null => Datum::Null,
            Datum::Float(_) | Datum::DateTime(_) => {
                return Err(Error::ParseError(format!(
                    "cannot cast {d:?} to '::{target}'"
                )))
            }
        },
        _ => match d {
            Datum::Float(f) => Datum::Float(*f),
            Datum::Int(i) => Datum::Float(*i as f64),
            Datum::Text(s) => s.trim().parse::<f64>().map(Datum::Float).map_err(|_| {
                Error::ParseError(format!("cannot cast '{s}' to '::{target}'"))
            })?,
            Datum::Null => Datum::Null,
            Datum::Bool(_) | Datum::DateTime(_) => {
                return Err(Error::ParseError(format!(
                    "cannot cast {d:?} to '::{target}'"
                )))
            }
        },
    })
}
