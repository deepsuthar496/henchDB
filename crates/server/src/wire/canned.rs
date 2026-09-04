//! Compatibility shims: interception and responses for common introspection queries
//! drivers and ORMs send during connection setup.

use engine::{Datum, Output, PRODUCT_NAME};

use super::constants::SERVER_VERSION_PREFIX;

pub fn strip_trailing_semicolon(s: &str) -> &str {
    let mut t = s.trim();
    while t.ends_with(';') {
        t = t[..t.len() - 1].trim();
    }
    t
}

pub fn canned_var_value(name: &str) -> &str {
    match name.to_ascii_lowercase().as_str() {
        "@@version" | "@@global.version" | "@@session.version" => SERVER_VERSION_PREFIX,
        "@@version_comment" | "@@global.version_comment" => PRODUCT_NAME,
        "@@max_allowed_packet" => "16777216",
        "@@character_set_client" | "@@character_set_connection" | "@@character_set_results" => {
            "utf8mb4"
        }
        "@@autocommit" => "1",
        "@@transaction_isolation" | "@@tx_isolation" => "REPEATABLE-READ",
        "@@lower_case_table_names" => "0",
        "@@sql_mode" => "",
        _ => "1",
    }
}

/// Split multi-statement text on top-level `;` (respects quotes).
pub fn split_statements(sql: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut quote: Option<char> = None;
    let mut cur = String::new();
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        if let Some(q) = quote {
            cur.push(c);
            if c == q {
                if chars.peek() == Some(&q) {
                    cur.push(chars.next().unwrap());
                } else {
                    quote = None;
                }
            }
            continue;
        }
        match c {
            '\'' | '"' | '`' => {
                quote = Some(c);
                cur.push(c);
            }
            ';' => {
                if !cur.trim().is_empty() {
                    parts.push(cur.trim().to_string());
                }
                cur.clear();
            }
            _ => cur.push(c),
        }
    }
    if !cur.trim().is_empty() {
        parts.push(cur.trim().to_string());
    }
    parts
}

/// Strip a trailing `LIMIT n[, m]` clause (drivers append `LIMIT 1` to
/// `SELECT @@var` probes; the value list must not include it).
pub fn strip_trailing_limit(list: &str) -> &str {
    let lower = list.to_ascii_lowercase();
    if let Some(pos) = lower.rfind(" limit ") {
        let tail = list[pos + 7..].trim();
        if !tail.is_empty()
            && tail
                .chars()
                .all(|c| c.is_ascii_digit() || c == ',' || c.is_whitespace())
        {
            return list[..pos].trim();
        }
    }
    list
}

/// Split a SELECT list on top-level commas (ignores commas in quotes/parens).
pub fn split_select_list(list: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut quote: Option<char> = None;
    let mut cur = String::new();
    let mut chars = list.chars().peekable();
    while let Some(c) = chars.next() {
        if let Some(q) = quote {
            cur.push(c);
            if c == q {
                // '' inside '' is an escaped quote.
                if chars.peek() == Some(&q) {
                    cur.push(chars.next().unwrap());
                } else {
                    quote = None;
                }
            }
            continue;
        }
        match c {
            '\'' | '"' | '`' => {
                quote = Some(c);
                cur.push(c);
            }
            '(' => {
                depth += 1;
                cur.push(c);
            }
            ')' => {
                depth = (depth - 1).max(0);
                cur.push(c);
            }
            ',' if depth == 0 => {
                parts.push(cur.trim().to_string());
                cur.clear();
            }
            _ => cur.push(c),
        }
    }
    if !cur.trim().is_empty() {
        parts.push(cur.trim().to_string());
    }
    parts
}

pub fn column_alias(expr: &str) -> String {
    let t = expr.trim();
    // Look for top-level " AS alias".
    let lower = t.to_ascii_lowercase();
    if let Some(pos) = lower.rfind(" as ") {
        let alias = t[pos + 4..].trim().trim_matches(|c| c == '`' || c == '"' || c == '\'');
        if !alias.is_empty() {
            return alias.to_string();
        }
    }
    t.to_string()
}

pub fn eval_bare_literal(expr: &str) -> Datum {
    let t = expr.trim();
    if t.eq_ignore_ascii_case("null") {
        return Datum::Null;
    }
    if t.eq_ignore_ascii_case("true") {
        return Datum::Int(1);
    }
    if t.eq_ignore_ascii_case("false") {
        return Datum::Int(0);
    }
    if t.len() >= 2
        && ((t.starts_with('\'') && t.ends_with('\'')) || (t.starts_with('"') && t.ends_with('"')))
    {
        let inner = &t[1..t.len() - 1].replace("''", "'");
        return Datum::Text(inner.clone());
    }
    if let Ok(n) = t.parse::<i64>() {
        return Datum::Int(n);
    }
    if let Ok(f) = t.parse::<f64>() {
        return Datum::Float(f);
    }
    let lower = t.to_ascii_lowercase();
    if lower == "version()" {
        return Datum::Text(SERVER_VERSION_PREFIX.to_string());
    }
    if lower == "database()" || lower == "schema()" {
        return Datum::Text(String::new());
    }
    if lower == "user()" || lower == "current_user()" || lower == "current_user" {
        return Datum::Text("root@localhost".to_string());
    }
    if lower == "connection_id()" {
        return Datum::Int(1);
    }
    if lower == "last_insert_id()" {
        return Datum::Int(0);
    }
    if t.starts_with("@@") {
        return Datum::Text(canned_var_value(t).to_string());
    }
    Datum::Text(t.to_string())
}

/// Bare `SELECT <literals>` without FROM (drivers send `SELECT 1`, `SELECT @@x`).
pub fn bare_select_output(list: &str) -> Option<Output> {
    if list.trim().is_empty() {
        return None;
    }
    let parts = split_select_list(list);
    if parts.is_empty() {
        return None;
    }
    let columns: Vec<String> = parts.iter().map(|p| column_alias(p)).collect();
    // Strip alias for evaluation: evaluate the expression before AS.
    let row: Vec<Datum> = parts
        .iter()
        .map(|p| {
            let lower = p.to_ascii_lowercase();
            let expr = if let Some(pos) = lower.rfind(" as ") {
                p[..pos].trim()
            } else {
                p.trim()
            };
            eval_bare_literal(expr)
        })
        .collect();
    Some(Output {
        columns,
        rows: vec![row],
        message: "OK".into(),
    })
}

pub fn ok_msg(message: &str) -> Output {
    Output {
        columns: vec![],
        rows: vec![],
        message: message.into(),
    }
}

pub fn info_schema_fallback(sql: &str) -> Option<Output> {
    // Return 0 rows with the requested projection so schema probes succeed.
    let lower = sql.to_ascii_lowercase();
    let sel_pos = lower.find("select")?;
    let from_pos = lower.find(" from ")?;
    if from_pos <= sel_pos {
        return None;
    }
    let list = sql[sel_pos + 6..from_pos].trim();
    if list == "*" {
        return Some(Output {
            columns: vec!["TABLE_SCHEMA".into()],
            rows: vec![],
            message: "OK".into(),
        });
    }
    let cols: Vec<String> = split_select_list(list)
        .into_iter()
        .map(|p| {
            let a = column_alias(&p);
            // `t.col` -> `col`.
            a.rsplit('.').next().unwrap_or(&a).trim().trim_matches('`').to_string()
        })
        .collect();
    Some(Output {
        columns: cols,
        rows: vec![],
        message: "OK".into(),
    })
}

/// Canned responses for MySQL-dialect introspection. Returns None when the
/// engine itself should answer (or error).
pub fn canned_output(sql: &str) -> Option<Output> {
    let t = strip_trailing_semicolon(sql);
    if t.is_empty() {
        return Some(ok_msg("OK"));
    }
    let lower = t.to_ascii_lowercase();
    let low_trim = lower.trim();

    // Session setup / no-ops.
    if low_trim.starts_with("set ") || low_trim == "set" {
        return Some(ok_msg("OK"));
    }
    if low_trim.starts_with("use ") {
        return Some(ok_msg("OK"));
    }
    if low_trim == "show warnings" {
        return Some(Output {
            columns: vec!["Level".into(), "Code".into(), "Message".into()],
            rows: vec![],
            message: "OK".into(),
        });
    }
    if low_trim.starts_with("show variables") {
        return Some(Output {
            columns: vec!["Variable_name".into(), "Value".into()],
            rows: vec![
                vec![
                    Datum::Text("version".into()),
                    Datum::Text(SERVER_VERSION_PREFIX.into()),
                ],
                vec![
                    Datum::Text("version_comment".into()),
                    Datum::Text(PRODUCT_NAME.into()),
                ],
                vec![
                    Datum::Text("max_allowed_packet".into()),
                    Datum::Text("16777216".into()),
                ],
                vec![
                    Datum::Text("character_set_client".into()),
                    Datum::Text("utf8mb4".into()),
                ],
                vec![
                    Datum::Text("autocommit".into()),
                    Datum::Text("ON".into()),
                ],
                vec![
                    Datum::Text("transaction_isolation".into()),
                    Datum::Text("REPEATABLE-READ".into()),
                ],
            ],
            message: "OK".into(),
        });
    }
    if low_trim.starts_with("show status") {
        return Some(Output {
            columns: vec!["Variable_name".into(), "Value".into()],
            rows: vec![],
            message: "OK".into(),
        });
    }
    if low_trim.starts_with("show engines") {
        return Some(Output {
            columns: vec!["Engine".into(), "Support".into(), "Comment".into()],
            rows: vec![vec![
                Datum::Text("InnoDB".into()),
                Datum::Text("DEFAULT".into()),
                Datum::Text("compatible".into()),
            ]],
            message: "OK".into(),
        });
    }
    if low_trim.starts_with("show databases") {
        return Some(Output {
            columns: vec!["Database".into()],
            rows: vec![vec![Datum::Text("main".into())]],
            message: "OK".into(),
        });
    }
    if low_trim.starts_with("show collation")
        || low_trim.starts_with("show charset")
        || low_trim.starts_with("show character set")
    {
        return Some(Output {
            columns: vec!["Collation".into(), "Charset".into()],
            rows: vec![vec![
                Datum::Text("utf8mb4_0900_ai_ci".into()),
                Datum::Text("utf8mb4".into()),
            ]],
            message: "OK".into(),
        });
    }
    if low_trim.starts_with("show grants") {
        return Some(Output {
            columns: vec!["Grants".into()],
            rows: vec![vec![Datum::Text(
                "GRANT ALL PRIVILEGES ON *.* TO 'root'@'%'".into(),
            )]],
            message: "OK".into(),
        });
    }
    if low_trim.starts_with("show columns")
        || low_trim.starts_with("show fields")
        || low_trim.starts_with("describe")
        || low_trim.starts_with("desc ")
    {
        return Some(Output {
            columns: vec![
                "Field".into(),
                "Type".into(),
                "Null".into(),
                "Key".into(),
                "Default".into(),
                "Extra".into(),
            ],
            rows: vec![],
            message: "OK".into(),
        });
    }
    if low_trim.starts_with("show index") || low_trim.starts_with("show table status") {
        return Some(Output {
            columns: vec!["Table".into(), "Column".into()],
            rows: vec![],
            message: "OK".into(),
        });
    }
    if low_trim.starts_with("show create table") {
        return Some(Output {
            columns: vec!["Table".into(), "Create Table".into()],
            rows: vec![],
            message: "OK".into(),
        });
    }
    if low_trim.contains("information_schema")
        || low_trim.contains("performance_schema")
        || low_trim.starts_with("select @@")
    {
        if low_trim.starts_with("select @@") {
            // `SELECT @@a, @@b ...` (no FROM; drivers append `LIMIT 1`).
            let mut list = t[6..].trim();
            // Strip trailing FROM if a driver appends one (rare).
            if let Some(i) = list.to_ascii_lowercase().find(" from ") {
                list = list[..i].trim();
            }
            return bare_select_output(strip_trailing_limit(list));
        }
        return info_schema_fallback(t);
    }
    // Bare SELECT without FROM.
    if low_trim.starts_with("select ") && !low_trim.contains(" from ") {
        let list = strip_trailing_limit(t[6..].trim());
        return bare_select_output(list);
    }
    None
}

/// Normalize MySQL-dialect session statements the engine spells differently.
pub fn normalize_dialect(sql: &str) -> &str {
    if sql.trim().eq_ignore_ascii_case("start transaction") {
        return "BEGIN";
    }
    sql
}
