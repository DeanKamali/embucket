//! Pre-parse interception of `UNDROP TABLE` DDL (Avalanche design §15, M9.3).
//!
//! The sqlparser fork has no `UNDROP` grammar, so it is matched on the raw
//! SQL before DataFusion parsing and routed to the metastore's tombstone
//! restore.
//!
//! Supported (v1): `UNDROP TABLE <name>` — restores the most recent
//! tombstone. `UNDROP SCHEMA/DATABASE` are v2 (schema-level tombstones are
//! not kept); they produce a bounded error. Trailing modifiers (e.g.
//! `AT(...)`) are rejected.
//!
//! v1 notes: names are case-folded through the session's ident normalizer
//! (quote-style is not preserved in pre-parse).

use crate::error::{self as ex_error, Error, Result};
use crate::models::QueryResult;
use crate::session::UserSession;
use catalog_metastore::models::TableIdent;
use snafu::Location;

/// What is being undropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UndropKind {
    Table,
    Schema,
    Database,
}

/// A parsed undrop statement (v0 grammar, Snowflake subset).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UndropStatement {
    pub kind: UndropKind,
    /// Unnormalized target name parts as written (dots already split).
    pub target: Vec<String>,
}

/// Lower-case token, quotes and trailing `;` stripped.
fn token(s: &str) -> &str {
    s.trim_matches(|c| c == '"' || c == '`' || c == '\'')
        .trim_end_matches(';')
}

/// Match the grammar prefix; `None` when this is not an undrop statement.
pub fn parse(raw: &str) -> Option<UndropStatement> {
    let raw = raw.trim();
    let tok: Vec<&str> = raw.split_whitespace().map(token).collect();
    if tok.len() < 3 || !tok[0].eq_ignore_ascii_case("undrop") {
        return None;
    }
    // Well-formed but unsupported — must be a bounded error, not a
    // fall-through syntax error from the planner.
    if tok[1].eq_ignore_ascii_case("schema") || tok[1].eq_ignore_ascii_case("database") {
        return Some(UndropStatement {
            kind: if tok[1].eq_ignore_ascii_case("schema") {
                UndropKind::Schema
            } else {
                UndropKind::Database
            },
            target: Vec::new(),
        });
    }
    if !tok[1].eq_ignore_ascii_case("table") {
        return None;
    }
    // Exactly one name part expected; anything else (AT(...), RESTRICT, …)
    // is bounded-unsupported rather than a confusing planner error.
    if tok.len() != 3 {
        return Some(UndropStatement {
            kind: UndropKind::Table,
            target: Vec::new(),
        });
    }
    Some(UndropStatement {
        kind: UndropKind::Table,
        target: tok[2].split('.').map(str::to_string).collect(),
    })
}

fn external_error(context: &str, err: impl std::fmt::Display) -> Error {
    Error::DataFusion {
        error: Box::new(datafusion::common::DataFusionError::External(
            format!("{context}: {err}").into(),
        )),
        location: Location::default(),
    }
}

/// Pre-parse hook for `UserQuery::execute`. `None` = not an undrop statement.
pub async fn try_execute(raw_query: &str, session: &UserSession) -> Option<Result<QueryResult>> {
    let stmt = parse(raw_query)?;
    Some(execute(&stmt, session).await)
}

async fn execute(stmt: &UndropStatement, session: &UserSession) -> Result<QueryResult> {
    match stmt.kind {
        UndropKind::Table => {
            if stmt.target.is_empty() || stmt.target.len() > 3 {
                return Err(external_error(
                    "undrop",
                    "expected UNDROP TABLE [<db>.<schema>.]<name>",
                ));
            }
            let (db, schema) = (
                session
                    .get_session_variable("database")
                    .unwrap_or_else(|| "embucket".to_string()),
                session
                    .get_session_variable("schema")
                    .unwrap_or_else(|| "public".to_string()),
            );
            let norm = |p: &str| {
                session
                    .ident_normalizer
                    .normalize(datafusion::sql::sqlparser::ast::Ident::new(p))
            };
            let mut parts = vec![db, schema];
            let keep = 3 - stmt.target.len();
            parts.truncate(keep);
            parts.extend(stmt.target.iter().map(|p| norm(p)));
            let ident = TableIdent::new(&parts[0], &parts[1], &parts[2]);
            session
                .metastore
                .undrop_table(&ident)
                .await
                .map_err(|e| external_error("undrop", e))?;
            crate::clone_ddl::status_row()
        }
        UndropKind::Schema | UndropKind::Database => Err(external_error(
            "undrop",
            format!(
                "UNDROP {} is not supported in v1 (only UNDROP TABLE)",
                match stmt.kind {
                    UndropKind::Schema => "SCHEMA",
                    UndropKind::Database => "DATABASE",
                    UndropKind::Table => unreachable!(),
                }
            ),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::{UndropKind, UndropStatement, parse};

    #[test]
    fn parses_table_undrop() {
        let s = parse("UNDROP TABLE t1").expect("should parse");
        assert_eq!(s.kind, UndropKind::Table);
        assert_eq!(s.target, vec!["t1"]);
        let s = parse("undrop table mydb.mys.t1;").expect("should parse");
        assert_eq!(s.target, vec!["mydb", "mys", "t1"]);
        let s = parse("UNDROP TABLE \"T1\"").expect("should parse");
        assert_eq!(s.target, vec!["T1"]);
    }

    #[test]
    fn unsupported_forms_are_bounded_errors() {
        // SCHEMA/DATABASE parse but execute into a bounded error.
        for sql in ["UNDROP SCHEMA s1", "UNDROP DATABASE db1"] {
            let s = parse(sql).expect("should parse");
            assert_ne!(s.kind, UndropKind::Table);
        }
        // Trailing modifiers are rejected.
        let s = parse("UNDROP TABLE t1 AT(TIMESTAMP => 'x')").expect("should parse");
        assert!(s.target.is_empty());
        let s = parse("UNDROP TABLE t1 BEFORE").expect("should parse");
        assert!(s.target.is_empty());
    }

    #[test]
    fn non_undrop_statements_fall_through() {
        assert!(parse("DROP TABLE t1").is_none());
        assert!(parse("SELECT 1").is_none());
        assert!(parse("UNDROP").is_none());
        assert!(parse("UNDROP WAREHOUSE w").is_none());
    }
}