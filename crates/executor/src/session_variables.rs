//! Session-variable reads (design §22.9): `$name` placeholders in a
//! statement resolve against the session's SET variables at parse time
//! (Snowflake semantics: `SET v = 41; SELECT $v + 1` → 42).

use datafusion::sql::sqlparser::ast::{
    Expr as SqlExpr, Statement, Value, ValueWithSpan, visit_expressions_mut,
};
use datafusion::sql::sqlparser::tokenizer::Span;
use std::ops::ControlFlow;

/// Replace every `$name` placeholder whose `name` resolves via `lookup`
/// with the corresponding literal, so the statement plans like its
/// inlined form. Placeholders that do not match a session variable (e.g.
/// bind-style `$1`) are left untouched.
pub fn rewrite_session_variables(
    statement: &mut Statement,
    lookup: &dyn Fn(&str) -> Option<String>,
) {
    let _ = visit_expressions_mut(statement, &mut |expr: &mut SqlExpr| {
        if let SqlExpr::Value(vws) = expr {
            if let Value::Placeholder(ph) = vws.value.clone() {
                let name = ph.strip_prefix('$').unwrap_or(ph.as_str());
                if !name.is_empty() && !name.chars().all(|c| c.is_ascii_digit()) {
                    if let Some(value) = lookup(name) {
                        *expr = SqlExpr::Value(ValueWithSpan {
                            value: literal_from_str(&value),
                            span: Span::empty(),
                        });
                    }
                }
            }
        }
        ControlFlow::<()>::Continue(())
    });
}

/// Snowflake SET values are stored as strings; re-type them the way the
/// literal was written (numbers stay numeric so arithmetic still works).
#[must_use]
pub fn literal_from_str(value: &str) -> Value {
    let up = value.to_ascii_uppercase();
    if up == "TRUE" {
        return Value::Boolean(true);
    }
    if up == "FALSE" {
        return Value::Boolean(false);
    }
    if value.parse::<i64>().is_ok() || value.parse::<f64>().is_ok() {
        return Value::Number(value.to_string(), false);
    }
    Value::SingleQuotedString(value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::sqlparser::dialect::SnowflakeDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    fn parse(sql: &str) -> Statement {
        Parser::parse_sql(&SnowflakeDialect {}, sql).expect("parse")[0].clone()
    }

    #[test]
    fn test_replaces_matching_placeholder() {
        let mut stmt = parse("SELECT $ltvar + 1;");
        rewrite_session_variables(&mut stmt, &|name: &str| {
            (name == "ltvar").then(|| "41".to_string())
        });
        let text = stmt.to_string();
        assert!(text.contains("41"), "got: {text}");
        assert!(!text.contains('$'), "got: {text}");
    }

    #[test]
    fn test_keeps_non_session_placeholders() {
        let mut stmt = parse("SELECT $1 + $unknown_var;");
        rewrite_session_variables(&mut stmt, &|_| None);
        let text = stmt.to_string();
        assert!(text.contains("$1"), "got: {text}");
        assert!(text.contains("$unknown_var"), "got: {text}");
    }

    #[test]
    fn test_string_values_are_quoted() {
        let lit = literal_from_str("hello");
        assert!(matches!(lit, Value::SingleQuotedString(_)));
        let num = literal_from_str("41");
        assert!(matches!(num, Value::Number(_, _)));
        let b = literal_from_str("true");
        assert!(matches!(b, Value::Boolean(true)));
    }
}
