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
use crate::time_travel::{TimePoint, parse_timestamp, resolve_snapshot};
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

/// The optional trailing `AT(...)` / `BEFORE(...)` spec on a clone (M9.4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AtSpec {
    /// Resolvable time point (TIMESTAMP/OFFSET).
    Point(TimePoint),
    /// Well-formed but unsupported key (BEFORE, STATEMENT, …) — bounded
    /// error at execution time rather than a fall-through syntax error.
    Unsupported(String),
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
    /// Trailing `AT(...)` time-travel spec (v1: TABLE clones only).
    pub at: Option<AtSpec>,
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
    // Optional trailing time-travel spec: `AT(...)` / `BEFORE(...)`.
    let at = match tok.get(i + 2) {
        None => None,
        Some(t) if is_at_or_before(t) => Some(parse_at_tail(&tok[i + 2..])?),
        // Unknown trailing tokens — fall through to the planner.
        Some(_) => return None,
    };
    let (Some(target), Some(source)) = (target, source) else {
        return Some(CloneStatement {
            kind,
            target: Vec::new(),
            source: Vec::new(),
            if_not_exists,
            or_replace,
            at,
        });
    };
    Some(CloneStatement {
        kind,
        target,
        source,
        if_not_exists,
        or_replace,
        at,
    })
}

/// True when the token starts an `AT(` or `BEFORE(` spec.
fn is_at_or_before(tok: &str) -> bool {
    let lower = tok.to_ascii_lowercase();
    lower == "at" || lower == "before" || lower.starts_with("at(") || lower.starts_with("before(")
}

/// Parse the `AT(TIMESTAMP => '<ts>' | OFFSET => <secs>)` / `BEFORE(...)`
/// tail following the source name. `None` = malformed → planner syntax error.
fn parse_at_tail(t: &[&str]) -> Option<AtSpec> {
    let joined = t.join(" ");
    if joined.eq_ignore_ascii_case("before")
        || joined
            .get(..7)
            .is_some_and(|p| p.eq_ignore_ascii_case("before("))
    {
        return Some(AtSpec::Unsupported("BEFORE".to_string()));
    }
    let open = joined.find('(')?;
    let close = joined.rfind(')')?;
    if !joined[close + 1..].trim().is_empty() {
        return None; // trailing junk after the closing paren
    }
    let inner = joined[open + 1..close].trim();
    let (key, value) = inner.split_once("=>")?;
    let key = key.trim().to_ascii_uppercase();
    let value = value.trim().trim_matches('\'');
    match key.as_str() {
        "TIMESTAMP" => Some(AtSpec::Point(TimePoint::TimestampMs(
            parse_timestamp(value)?,
        ))),
        "OFFSET" => Some(AtSpec::Point(TimePoint::OffsetSecs(value.parse().ok()?))),
        other => Some(AtSpec::Unsupported(other.to_string())),
    }
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

    // AT(...) is a TABLE-only feature in v1 (M9.4).
    if stmt.at.is_some() && stmt.kind != CloneKind::Table {
        return Err(external_error(
            "clone",
            "CLONE ... AT() is only supported for TABLE clones in v1",
        ));
    }

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
            match &stmt.at {
                Some(AtSpec::Unsupported(key)) => {
                    return Err(external_error(
                        "clone",
                        format!("AT({key}) is not supported in v1 (TIMESTAMP and OFFSET only)"),
                    ));
                }
                Some(AtSpec::Point(point)) => {
                    let point_ms = match point {
                        TimePoint::TimestampMs(ms) => *ms,
                        TimePoint::OffsetSecs(secs) => {
                            chrono::Utc::now().timestamp_millis() - secs * 1000
                        }
                    };
                    let table = metastore
                        .get_table(&from)
                        .await
                        .map_err(|e| external_error("clone", e))?
                        .ok_or_else(|| {
                            external_error(
                                "clone",
                                format!("Table '{}' does not exist", from.table),
                            )
                        })?;
                    if table.data.format != catalog_metastore::models::TableFormat::Iceberg {
                        return Err(external_error(
                            "clone",
                            format!(
                                "time-travel clone requires an Iceberg table, '{}' is not one",
                                from.table
                            ),
                        ));
                    }
                    let Some(snapshot_id) = resolve_snapshot(&table.data.metadata, point_ms) else {
                        return Err(external_error(
                            "clone",
                            format!(
                                "requested time is before table '{}' creation or first commit",
                                from.table
                            ),
                        ));
                    };
                    metastore
                        .clone_table_as_of(&from, &to, snapshot_id, point_ms)
                        .await
                        .map_err(|e| external_error("clone", e))?;
                }
                None => {
                    metastore
                        .clone_table(&from, &to)
                        .await
                        .map_err(|e| external_error("clone", e))?;
                }
            }
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
            if let Some(list) = crate::share_catalog::ShareCatalogList::unwrap_bare(&catalog_list) {
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
/// Shared with the other pre-parse DDL hooks (warehouse_ddl, undrop_ddl).
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
    use super::{AtSpec, CloneKind, CloneStatement, TimePoint, parse, parse_timestamp};

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

    #[test]
    fn parses_at_spec() {
        let s = parsed("CREATE TABLE t2 CLONE t1 AT(TIMESTAMP => '2026-10-14 10:00:00')");
        assert_eq!(
            s.at,
            Some(AtSpec::Point(TimePoint::TimestampMs(
                parse_timestamp("2026-10-14 10:00:00").unwrap()
            )))
        );
        let s = parsed("CREATE TABLE t2 CLONE t1 AT(OFFSET => 86400)");
        assert_eq!(s.at, Some(AtSpec::Point(TimePoint::OffsetSecs(86400))));
        // Spaced paren form.
        let s = parsed("CREATE TABLE t2 CLONE t1 AT ( TIMESTAMP => '2026-10-14 10:00:00' )");
        assert!(matches!(s.at, Some(AtSpec::Point(_))));
        // Qualified names + trailing semicolon.
        let s = parsed("CREATE TABLE d.s.t2 CLONE d.s.t1 AT(OFFSET => 60);");
        assert_eq!(s.source, vec!["d", "s", "t1"]);
        assert_eq!(s.at, Some(AtSpec::Point(TimePoint::OffsetSecs(60))));
        // Unsupported keys are bounded, not fall-through.
        let s = parsed("CREATE TABLE t2 CLONE t1 AT(STATEMENT => 'x')");
        assert_eq!(
            s.at,
            Some(AtSpec::Unsupported("STATEMENT".to_string()))
        );
        let s = parsed("CREATE TABLE t2 CLONE t1 BEFORE(STATEMENT => 'x')");
        assert_eq!(s.at, Some(AtSpec::Unsupported("BEFORE".to_string())));
        // No AT → None.
        let s = parsed("CREATE TABLE t2 CLONE t1");
        assert_eq!(s.at, None);
        // Malformed spec → fall-through.
        assert!(parse("CREATE TABLE t2 CLONE t1 AT(TIMESTAMP => 'nope')").is_none());
        assert!(parse("CREATE TABLE t2 CLONE t1 AT(GARBAGE").is_none());
        // Unknown trailing tokens → fall-through.
        assert!(parse("CREATE TABLE t2 CLONE t1 EXTRA").is_none());
    }
}

