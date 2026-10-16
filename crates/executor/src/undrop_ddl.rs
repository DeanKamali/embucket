//! Pre-parse interception of `UNDROP` DDL (Avalanche design §15, M9.3).
//!
//! The sqlparser fork has no `UNDROP` grammar, so it is matched on the raw
//! SQL before DataFusion parsing and routed to the metastore's tombstone
//! restores.
//!
//! Supported: `UNDROP TABLE <name>` (restores the most recent table
//! tombstone), and `UNDROP SCHEMA`/`UNDROP DATABASE` (M17.2.2 — restores the
//! most recent container tombstone together with the contents its cascade
//! drop tombstoned). Trailing modifiers (e.g. `AT(...)`) are rejected.
//!
//! Notes: names are case-folded through the session's ident normalizer
//! (quote-style is not preserved in pre-parse).

use crate::error::{self as ex_error, Error, Result};
use crate::models::QueryResult;
use crate::session::UserSession;
use catalog_metastore::models::{SchemaIdent, TableIdent};
use snafu::Location;
use std::sync::Arc;

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
    if tok.is_empty() || !tok[0].eq_ignore_ascii_case("undrop") {
        return None;
    }
    // Well-formed — parse the target name parts (validated at execution).
    // Extra trailing tokens (AT(...), RESTRICT, …) yield an empty target →
    // bounded error at execution rather than a planner syntax error.
    if tok.len() >= 2 && (tok[1].eq_ignore_ascii_case("schema") || tok[1].eq_ignore_ascii_case("database")) {
        let kind = if tok[1].eq_ignore_ascii_case("schema") {
            UndropKind::Schema
        } else {
            UndropKind::Database
        };
        let target = if tok.len() == 3 {
            tok[2].split('.').map(str::to_string).collect()
        } else {
            Vec::new()
        };
        return Some(UndropStatement { kind, target });
    }
    if tok.len() < 3 {
        return None;
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
    let stmt = parse(&crate::sql_text::strip_comments(raw_query))?;
    Some(execute(&stmt, session).await)
}

async fn execute(stmt: &UndropStatement, session: &UserSession) -> Result<QueryResult> {
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
    match stmt.kind {
        UndropKind::Table => {
            if stmt.target.is_empty() || stmt.target.len() > 3 {
                return Err(external_error(
                    "undrop",
                    "expected UNDROP TABLE [<db>.<schema>.]<name>",
                ));
            }
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
        UndropKind::Schema => {
            if stmt.target.is_empty() || stmt.target.len() > 2 {
                return Err(external_error(
                    "undrop",
                    "expected UNDROP SCHEMA [<db>.]<name>",
                ));
            }
            let parts: Vec<String> = if stmt.target.len() == 1 {
                vec![db, norm(&stmt.target[0])]
            } else {
                stmt.target.iter().map(|p| norm(p)).collect()
            };
            let ident = SchemaIdent::new(parts[0].clone(), parts[1].clone());
            session
                .metastore
                .undrop_schema(&ident)
                .await
                .map_err(|e| external_error("undrop", e))?;
            crate::clone_ddl::status_row()
        }
        UndropKind::Database => {
            if stmt.target.len() != 1 {
                return Err(external_error("undrop", "expected UNDROP DATABASE <name>"));
            }
            let ident = norm(&stmt.target[0]);
            session
                .metastore
                .undrop_database(&ident)
                .await
                .map_err(|e| external_error("undrop", e))?;
            // Make the restored database queryable in this session's catalog
            // registry (mirrors the database-clone hook).
            let catalog_list: Arc<dyn datafusion::catalog::CatalogProviderList> =
                Arc::clone(session.ctx.state().catalog_list());
            if let Some(list) = crate::share_catalog::ShareCatalogList::unwrap_bare(&catalog_list)
            {
                list.register_catalog(&ident)
                    .await
                    .map_err(|e| external_error("undrop", e))?;
            }
            crate::clone_ddl::status_row()
        }
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
    fn parses_schema_and_database_undrop() {
        let s = parse("UNDROP SCHEMA s1").expect("should parse");
        assert_eq!(s.kind, UndropKind::Schema);
        assert_eq!(s.target, vec!["s1"]);
        let s = parse("UNDROP SCHEMA mydb.s1;").expect("should parse");
        assert_eq!(s.target, vec!["mydb", "s1"]);
        let s = parse("UNDROP DATABASE db1").expect("should parse");
        assert_eq!(s.kind, UndropKind::Database);
        assert_eq!(s.target, vec!["db1"]);
        // Trailing modifiers are rejected (empty target → bounded error).
        let s = parse("UNDROP SCHEMA s1 AT(TIMESTAMP => 'x')").expect("should parse");
        assert!(s.target.is_empty());
        // Missing name → bounded error.
        let s = parse("UNDROP DATABASE").expect("should parse");
        assert!(s.target.is_empty());
    }

    #[test]
    fn unsupported_forms_are_bounded_errors() {
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