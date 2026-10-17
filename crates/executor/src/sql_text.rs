//! Raw-SQL text utilities for pre-parse interception.

/// Strip `/* ... */` block comments and `-- ...` line comments outside of
/// single-quoted string literals, replacing each block with a single space so
/// surrounding tokens do not fuse.
///
/// Needed because several SQL statements are matched pre-parse on the raw text
/// (CLONE, WAREHOUSE, STREAM/TASK, UNDROP, ACCOUNT DDL) with a whitespace
/// tokenizer that has no comment awareness — and ecosystem clients (dbt)
/// append a trailing `/* {"app": "dbt", ...} */` tag comment to every
/// statement they send.
#[must_use]
pub fn strip_comments(sql: &str) -> String {
    let chars: Vec<char> = sql.chars().collect();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    let mut in_string = false;
    while i < chars.len() {
        let c = chars[i];
        if in_string {
            out.push(c);
            if c == '\'' {
                in_string = false;
            }
            i += 1;
            continue;
        }
        match c {
            '\'' => {
                in_string = true;
                out.push(c);
                i += 1;
            }
            '-' if chars.get(i + 1) == Some(&'-') => {
                while i < chars.len() && chars[i] != '\n' {
                    i += 1;
                }
            }
            '/' if chars.get(i + 1) == Some(&'*') => {
                i += 2;
                while i + 1 < chars.len() && !(chars[i] == '*' && chars[i + 1] == '/') {
                    i += 1;
                }
                i += 2; // past the closing `*/` (or past end when unterminated)
                out.push(' ');
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::strip_comments;

    #[test]
    fn strips_block_comments() {
        assert_eq!(strip_comments("SELECT 1 /* {\"app\": \"dbt\"} */"), "SELECT 1  ");
        assert_eq!(
            strip_comments("/* tag */ CREATE TABLE t2 CLONE t1"),
            "  CREATE TABLE t2 CLONE t1"
        );
        assert_eq!(
            strip_comments("A /* c1 */ B /* c2 */ C"),
            "A   B   C"
        );
    }

    #[test]
    fn strips_multiline_block_comments() {
        assert_eq!(
            strip_comments("CREATE TABLE t2 CLONE t1\n/* {\"app\": \"dbt\",\n\"v\": 1} */"),
            "CREATE TABLE t2 CLONE t1\n "
        );
    }

    #[test]
    fn strips_line_comments() {
        assert_eq!(strip_comments("SELECT 1 -- done"), "SELECT 1 ");
        assert_eq!(
            strip_comments("CREATE TABLE t2 CLONE t1; -- trailing"),
            "CREATE TABLE t2 CLONE t1; "
        );
    }

    #[test]
    fn preserves_string_literals() {
        assert_eq!(
            strip_comments("INSERT INTO t VALUES ('a /* not a comment */', '--nope')"),
            "INSERT INTO t VALUES ('a /* not a comment */', '--nope')"
        );
        assert_eq!(
            strip_comments("SELECT 'it''s /* x */' -- end"),
            "SELECT 'it''s /* x */' "
        );
    }

    #[test]
    fn unterminated_block_comment_dropped() {
        assert_eq!(strip_comments("SELECT 1 /* open"), "SELECT 1  ");
    }
}

/// Re-escape backslashes inside single-quoted string literals so the text
/// survives another parse under a dialect whose tokenizer processes backslash
/// escapes (`supports_string_literal_backslash_escape == true`, e.g. the
/// Snowflake dialect).
///
/// Background (M18, Vector ingestion): `UserQuery::execute` parses the raw
/// query into an AST, re-serializes it via `Statement::to_string()` and later
/// re-parses that text (`execute_with_custom_plan` for DML, `execute_sql` as
/// the general fallback). The AST stores the *decoded* literal value and
/// `Display` only re-escapes quotes — so a backslash-escape dialect decodes
/// each literal a second time (NDJSON `\"` inside a Vector payload became
/// `"`), silently corrupting JSON string literals on INSERT/SELECT.
///
/// Re-escaping restores the invariant: serialize(decoded) + parse == decoded.
/// The scan works on the *serialized* text without decoding it, so it is
/// idempotent with respect to the value it carries.
#[must_use]
pub fn re_escape_string_literals(sql: &str, dialect_name: &str) -> String {
    use sqlparser::dialect::dialect_from_str;

    let Some(dialect) = dialect_from_str(dialect_name) else {
        return sql.to_string();
    };
    if !dialect.supports_string_literal_backslash_escape() {
        return sql.to_string();
    }
    let chars: Vec<char> = sql.chars().collect();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    let mut in_string = false;
    while i < chars.len() {
        let c = chars[i];
        if in_string {
            match c {
                '\'' => {
                    // `''` is a quote escape in the serialized text; anything
                    // else ends the literal.
                    if chars.get(i + 1) == Some(&'\'') {
                        out.push('\'');
                        out.push('\'');
                        i += 2;
                    } else {
                        in_string = false;
                        out.push('\'');
                        i += 1;
                    }
                }
                '\\' => {
                    out.push('\\');
                    out.push('\\');
                    i += 1;
                }
                _ => {
                    out.push(c);
                    i += 1;
                }
            }
        } else {
            out.push(c);
            if c == '\'' {
                in_string = true;
            }
            i += 1;
        }
    }
    out
}

#[cfg(test)]
mod re_escape_tests {
    use super::re_escape_string_literals;
    use sqlparser::dialect::SnowflakeDialect;
    use sqlparser::tokenizer::Tokenizer;

    fn decode(sql: &str) -> String {
        let mut tokenizer = Tokenizer::new(&SnowflakeDialect {}, sql);
        tokenizer
            .tokenize()
            .ok()
            .and_then(|tokens| {
                tokens.into_iter().find_map(|t| match t {
                    sqlparser::tokenizer::Token::SingleQuotedString(v) => Some(v),
                    _ => None,
                })
            })
            .expect("expected a single-quoted string literal")
    }

    #[test]
    fn round_trips_backslash_quote() {
        // Display output of an AST whose string value contains backslash-quote
        // (as produced by a Snowflake-decoded NDJSON payload).
        let serialized = r#"INSERT INTO t VALUES ('{"a":"\"x\""}')"#;
        let fixed = re_escape_string_literals(serialized, "snowflake");
        assert!(fixed.contains("\\\\\""));
        // one more decode round yields the originally-decoded value
        let value = decode(&fixed);
        assert_eq!(value, "{\"a\":\"\\\"x\\\"\"}");
    }

    #[test]
    fn round_trips_doubled_backslashes() {
        let serialized = "SELECT 'a\\b'"; // decoded AST value: a\b (backslash + b)
        let fixed = re_escape_string_literals(serialized, "snowflake");
        assert_eq!(decode(&fixed), "a\\b");
    }

    #[test]
    fn no_op_for_non_backslash_dialect() {
        let sql = r#"SELECT 'a\"b'"#;
        assert_eq!(re_escape_string_literals(sql, "postgres"), sql);
        assert_eq!(re_escape_string_literals(sql, "unknown-dialect"), sql);
    }

    #[test]
    fn preserves_quotes_and_surrounding_text() {
        let serialized = "SELECT 'it''s' AS v FROM t";
        let fixed = re_escape_string_literals(serialized, "snowflake");
        assert_eq!(decode(&fixed), "it's");
        assert!(fixed.starts_with("SELECT "));
    }

    #[test]
    fn malformed_input_returned_unchanged() {
        let sql = "SELECT 'unterminated";
        assert_eq!(re_escape_string_literals(sql, "snowflake"), sql);
    }
}