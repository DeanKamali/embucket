//! Pre-parse interception of zero-copy clone DDL (Avalanche design §14).
//!
//! The sqlparser fork has no `CLONE` grammar, so clone statements are matched
//! on the raw SQL before DataFusion parsing and routed to the metastore's
//! catalog-layer clone operations — metadata-only, no data movement.
//! Supported (Snowflake subset):
//!
//! - `CREATE [OR REPLACE] TABLE [IF NOT EXISTS] <name> CLONE <source>`
//! - `CREATE [OR REPLACE] SCHEMA [IF NOT EXISTS] <name> CLONE <source>`
//! - `CREATE [OR REPLACE] DATABASE [IF NOT EXISTS] <name> CLONE <source>`
//!
//! v1 notes: names are case-folded through the session's ident normalizer
//! (quote-style is not preserved in pre-parse); a `DATABASE` clone keeps the
//! source's volume; every contained table is cloned against the source's
//! current metadata at clone time (consistent-refs set).

use crate::error::{self as ex_error, Error, Result};
use crate::models::QueryResult;
use crate::session::UserSession;
use catalog::catalog_list::EmbucketCatalogList;
use catalog_metastore::models::{SchemaIdent, TableIdent};
use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use snafu::Location;
use snafu::ResultExt;
use std::sync::Arc;

/// What is being cloned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloneKind {
    Table,
    Schema,
    Database,
}

/// A parsed clone statement (v0 grammar, Snowflake subset).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloneStatement {
    pub kind: CloneKind,
    /// Unnormalized target name parts as written (dots already split).
    pub target: Vec<String>,
    pub source: Vec<String>,
    pub if_not_exists: bool,
    pub or_replace: bool,
}

/// Lower-case token, quotes and trailing `;` stripped.
fn token(s: &str) -> &str {
    s.trim_matches(|c| c == '"' || c == '`' || c == '\'')
        .trim_end_matches(';')
}

/// Match the grammar prefix; `None` when this is not a clone statement.
pub fn parse(raw: &str) -> Option<CloneStatement> {
    let raw = raw.trim();
    let tok: Vec<&str> = raw.split_whitespace().map(token).collect();
    if tok.len() < 5 || !tok[0].eq_ignore_ascii_case("create") {
        return None;
    }
    let mut i = 1;
    let mut or_replace = false;
    if tok.get(i).is_some_and(|t| t.eq_ignore_ascii_case("or"))
        && tok.get(i + 1).is_some_and(|t| t.eq_ignore_ascii_case("replace"))
    {
        or_replace = true;
        i += 2;
    }
    let kind = match tok.get(i).map(|t| t.to_ascii_uppercase()).as_deref() {
        Some("TABLE") => CloneKind::Table,
        Some("SCHEMA") => CloneKind::Schema,
        Some("DATABASE") => CloneKind::Database,
        _ => return None,
    };
    i += 1;
    let mut if_not_exists = false;
    if tok.get(i).is_some_and(|t| t.eq_ignore_ascii_case("if"))
        && tok.get(i + 1).is_some_and(|t| t.eq_ignore_ascii_case("not"))
        && tok.get(i + 2).is_some_and(|t| t.eq_ignore_ascii_case("exists"))
    {
        if_not_exists = true;
        i += 3;
    }
    let target: Option<Vec<String>> = tok
        .get(i)
        .map(|t| t.split('.').map(str::to_string).collect());
    i += 1;
    // The `CLONE <source>` tail is what distinguishes a clone from any other
    // CREATE — without it we fall through to the normal planner.
    if !tok.get(i).is_some_and(|t| t.eq_ignore_ascii_case("clone")) {
        return None;
    }
    let source: Option<Vec<String>> = tok
        .get(i + 1)
        .map(|t| t.split('.').map(str::to_string).collect());
    let (Some(target), Some(source)) = (target, source) else {
        return Some(CloneStatement {
            kind,
            target: Vec::new(),
            source: Vec::new(),
            if_not_exists,
            or_replace,
        });
    };
    Some(CloneStatement {
        kind,
        target,
        source,
        if_not_exists,
        or_replace,
    })
}

/// Pre-parse hook for `UserQuery::execute`. `None` = not a clone statement.
pub async fn try_execute(raw_query: &str, session: &UserSession) -> Option<Result<QueryResult>> {
    let stmt = parse(raw_query)?;
    Some(execute(&stmt, session).await)
}

/// Session's current database/schema (mirrors `UserQuery::current_*`, which
/// is not reachable from a session-only hook).
fn current_parts(session: &UserSession) -> (String, String) {
    let database = session
        .get_session_variable("database")
        .unwrap_or_else(|| "embucket".to_string());
    let schema = session
        .get_session_variable("schema")
        .unwrap_or_else(|| "public".to_string());
    (database, schema)
}

fn normalized(session: &UserSession, name: &str) -> String {
    session
        .ident_normalizer
        .normalize(datafusion::sql::sqlparser::ast::Ident::new(name))
}

fn external_error(context: &str, err: impl std::fmt::Display) -> Error {
    Error::DataFusion {
        error: Box::new(datafusion::common::DataFusionError::External(
            format!("{context}: {err}").into(),
        )),
        location: Location::default(),
    }
}

async fn execute(stmt: &CloneStatement, session: &UserSession) -> Result<QueryResult> {
    let (db, schema) = current_parts(session);
    let metastore = session.metastore.clone();

    // Fill in unqualified names from the session context, normalizing each part.
    let norm = |parts: &[String]| -> Vec<String> {
        parts.iter().map(|p| normalized(session, p)).collect()
    };
    let target = resolve_name(&db, &schema, &norm(&stmt.target), stmt.kind)?;
    let source = resolve_name(&db, &schema, &norm(&stmt.source), stmt.kind)?;

    match stmt.kind {
        CloneKind::Table => {
            let to = table_ident(&target);
            let from = table_ident(&source);

            if metastore
                .table_exists(&to)
                .await
                .map_err(|e| external_error("clone", e))?
            {
                if stmt.if_not_exists {
                    return status_row();
                }
                if !stmt.or_replace {
                    return Err(external_error(
                        "clone",
                        format!("Table '{}' already exists", to.table),
                    ));
                }
                metastore
                    .delete_table(&to, true)
                    .await
                    .map_err(|e| external_error("clone", e))?;
            }
            metastore
                .clone_table(&from, &to)
                .await
                .map_err(|e| external_error("clone", e))?;
        }
        CloneKind::Schema => {
            let to = schema_ident(&target);
            let from = schema_ident(&source);

            if metastore
                .get_schema(&to)
                .await
                .map_err(|e| external_error("clone", e))?
                .is_some()
            {
                if stmt.if_not_exists {
                    return status_row();
                }
                if !stmt.or_replace {
                    return Err(external_error(
                        "clone",
                        format!("Schema '{}' already exists", to.schema),
                    ));
                }
                metastore
                    .delete_schema(&to, true)
                    .await
                    .map_err(|e| external_error("clone", e))?;
            }
            metastore
                .clone_schema(&from, &to)
                .await
                .map_err(|e| external_error("clone", e))?;
        }
        CloneKind::Database => {
            let to = one_part(&target, "database")?;
            let from = one_part(&source, "database")?;

            if metastore
                .get_database(&to)
                .await
                .map_err(|e| external_error("clone", e))?
                .is_some()
            {
                if stmt.if_not_exists {
                    return status_row();
                }
                if !stmt.or_replace {
                    return Err(external_error(
                        "clone",
                        format!("Database '{to}' already exists"),
                    ));
                }
                metastore
                    .delete_database(&to, true)
                    .await
                    .map_err(|e| external_error("clone", e))?;
            }
            metastore
                .clone_database(&from, &to)
                .await
                .map_err(|e| external_error("clone", e))?;
            // Make the new database queryable in this session's catalog
            // registry (upserts every metastore database, incl. the clone).
            let catalog_list: Arc<dyn datafusion::catalog::CatalogProviderList> =
                Arc::clone(session.ctx.state().catalog_list());
            if let Ok(list) = Arc::downcast::<EmbucketCatalogList>(catalog_list) {
                list.register_catalog(&to)
                    .await
                    .map_err(|e| external_error("clone", e))?;
            }
        }
    }
    status_row()
}

/// Resolve possibly-qualified name parts against the session context.
fn resolve_name(
    db: &str,
    schema: &str,
    parts: &[String],
    kind: CloneKind,
) -> std::result::Result<Vec<String>, Error> {
    let want = match kind {
        CloneKind::Table => 3,
        CloneKind::Schema => 2,
        CloneKind::Database => 1,
    };
    if parts.is_empty() || parts.len() > want {
        return ex_error::InvalidSchemaIdentifierSnafu {
            ident: parts.join("."),
        }
        .fail();
    }
    let mut out = vec![db.to_string(), schema.to_string()];
    // Keep `want - parts.len()` context components, then the written parts.
    let keep = want - parts.len();
    out.truncate(keep);
    out.extend(parts.iter().cloned());
    Ok(out)
}

fn one_part(parts: &[String], what: &str) -> Result<String> {
    match parts {
        [name] => Ok(name.clone()),
        _ => ex_error::InvalidSchemaIdentifierSnafu {
            ident: format!("{what}:{}", parts.join(".")),
        }
        .fail(),
    }
}

fn table_ident(parts: &[String]) -> TableIdent {
    TableIdent::new(&parts[0], &parts[1], &parts[2])
}

fn schema_ident(parts: &[String]) -> SchemaIdent {
    SchemaIdent::new(parts[0].clone(), parts[1].clone())
}

/// `Statement executed successfully.` row, matching other DDL responses.
#[allow(clippy::missing_errors_doc)]
pub fn status_row() -> Result<QueryResult> {
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "status",
        DataType::Utf8,
        false,
    )]));
    Ok(QueryResult::new(
        vec![RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![
                "Statement executed successfully.",
            ]))],
        )
        .context(ex_error::ArrowSnafu)?],
        schema,
    ))
}
#[cfg(test)]
mod tests {
    use super::{CloneKind, CloneStatement, parse};

    fn parsed(sql: &str) -> CloneStatement {
        parse(sql).unwrap_or_else(|| panic!("expected clone statement: {sql}"))
    }

    #[test]
    fn parses_table_clone() {
        let s = parsed("CREATE TABLE t2 CLONE t1");
        assert_eq!(s.kind, CloneKind::Table);
        assert_eq!(s.target, vec!["t2"]);
        assert_eq!(s.source, vec!["t1"]);
        assert!(!s.if_not_exists && !s.or_replace);
    }

    #[test]
    fn parses_qualified_and_optionals() {
        let s = parsed(
            "CREATE OR REPLACE TABLE mydb.myschema.t2 CLONE mydb.myschema.t1;",
        );
        assert_eq!(s.kind, CloneKind::Table);
        assert_eq!(s.target, vec!["mydb", "myschema", "t2"]);
        assert!(s.or_replace);
        let s = parsed("CREATE SCHEMA IF NOT EXISTS s2 CLONE s1");
        assert_eq!(s.kind, CloneKind::Schema);
        assert!(s.if_not_exists);
        assert_eq!(s.target, vec!["s2"]);
        let s = parsed("CREATE DATABASE db2 CLONE db1");
        assert_eq!(s.kind, CloneKind::Database);
        assert_eq!(s.source, vec!["db1"]);
    }

    #[test]
    fn parses_case_insensitive_and_quoted() {
        let s = parsed("create table \"T2\" clone \"T1\"");
        assert_eq!(s.target, vec!["T2"]);
        assert_eq!(s.source, vec!["T1"]);
    }

    #[test]
    fn non_clone_statements_fall_through() {
        assert!(parse("CREATE TABLE t2 (a INT)").is_none());
        assert!(parse("CREATE TABLE t2 AS SELECT * FROM t1").is_none());
        assert!(parse("CREATE WAREHOUSE w WITH SIZE = 'XSMALL'").is_none());
        assert!(parse("SELECT 1").is_none());
        // Malformed (no source) falls through to the planner, which reports
        // the syntax error.
        assert!(parse("CREATE TABLE t2 CLONE").is_none());
    }
}
