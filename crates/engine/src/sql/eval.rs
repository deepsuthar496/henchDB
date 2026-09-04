use std::collections::HashMap;

use crate::error::{Error, Result};
use crate::table::Schema;
use crate::types::Datum;

use super::ast::Expr;

pub fn eval_expr(expr: &Expr, columns: &Schema, row: &[Datum]) -> Result<bool> {
    eval_with(expr, &mut |name| {
        let idx = columns.index_of(name).ok_or_else(|| Error::ColumnNotFound(name.into()))?;
        Ok(row[idx].clone())
    })
}

pub fn eval_with(expr: &Expr, resolve: &mut dyn FnMut(&str) -> Result<Datum>) -> Result<bool> {
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

pub fn collect_columns<'a>(expr: &'a Expr, out: &mut Vec<&'a str>) {
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

pub fn like_match(text: &str, pattern: &str) -> bool {
    let t: Vec<char> = text.chars().collect();
    let p: Vec<char> = pattern.chars().collect();
    fn go(t: &[char], p: &[char], memo: &mut HashMap<(usize, usize), bool>, ti: usize, pi: usize) -> bool {
        if let Some(&r) = memo.get(&(ti, pi)) {
            return r;
        }
        let r = if pi == p.len() {
            ti == t.len()
        } else if p[pi] == '%' {
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
    go(&t, &p, &mut HashMap::new(), 0, 0)
}

pub(crate) fn coerce_pair(l: Datum, r: Datum) -> (Datum, Datum) {
    match (l, r) {
        (Datum::Int(a), Datum::Float(b)) => (Datum::Float(a as f64), Datum::Float(b)),
        (Datum::Float(a), Datum::Int(b)) => (Datum::Float(a), Datum::Float(b as f64)),
        (Datum::DateTime(a), Datum::Text(b)) => {
            if let Some(m) = crate::types::parse_datetime_str(&b) {
                (Datum::DateTime(a), Datum::DateTime(m))
            } else {
                (Datum::DateTime(a), Datum::Text(b))
            }
        }
        (Datum::Text(a), Datum::DateTime(b)) => {
            if let Some(m) = crate::types::parse_datetime_str(&a) {
                (Datum::DateTime(m), Datum::DateTime(b))
            } else {
                (Datum::Text(a), Datum::DateTime(b))
            }
        }
        (a, b) => (a, b),
    }
}
