//! Pre-parse interception of warehouse DDL (Avalanche design §5).
//!
//! The sqlparser fork has no `WAREHOUSE` statements, so these are matched on
//! the raw SQL before DataFusion parsing and routed to the
//! [`WarehouseManager`]. Supported:
//!
//! - `CREATE [OR REPLACE] WAREHOUSE [IF NOT EXISTS] name [WITH] opt = value ...`
//! - `ALTER WAREHOUSE [IF EXISTS] name SET opt = value ...` (also `SUSPEND`/`RESUME`)
//! - `DROP WAREHOUSE [IF EXISTS] name`
//! - `SHOW WAREHOUSES [LIKE 'pattern']`
//! - `DESC[RIBE] WAREHOUSE name`

use crate::error::{self as ex_error, Error, Result};
use crate::models::QueryResult;
use crate::session::UserSession;
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use avalanche_warehouse::{
    CreateWarehouseRequest, WarehouseManager, WarehouseSize,
};
use datafusion::arrow::array::{RecordBatch, StringArray};
use snafu::ResultExt;
use snafu::Location;
use std::sync::Arc;

/// A parsed warehouse statement (v0 grammar, Snowflake subset).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WarehouseStatement {
    Create {
        name: String,
        size: Option<WarehouseSize>,
        auto_suspend_secs: Option<u64>,
        auto_resume: Option<bool>,
        initially_suspended: bool,
        if_not_exists: bool,
    },
    AlterSet {
        name: String,
        size: Option<WarehouseSize>,
        auto_suspend_secs: Option<u64>,
        auto_resume: Option<bool>,
    },
    Suspend {
        name: String,
    },
    Resume {
        name: String,
    },
    Drop {
        name: String,
        if_exists: bool,
    },
    Show,
    Describe {
        name: String,
    },
}

/// Tokenize raw SQL: whitespace/`,`/`;` separated, single-quoted strings kept
/// as one token (quotes preserved for later stripping).
fn tokenize(sql: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;
    for ch in sql.trim_start().chars() {
        match ch {
            '\'' => {
                in_quote = !in_quote;
                current.push(ch);
            }
            c if c.is_whitespace() && !in_quote => {
                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
            }
            ',' | ';' if !in_quote => {
                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
            }
            c => current.push(c),
        }
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    tokens
}

/// Strip surrounding quotes. An unquoted identifier is case-insensitive in
/// Snowflake → uppercased; a quoted one keeps its exact case.
fn normalize_ident(token: &str) -> String {
    if token.starts_with('"') {
        token.trim_matches('"').to_string()
    } else if token.starts_with('\'') {
        token.trim_matches('\'').to_string()
    } else {
        token.to_ascii_uppercase()
    }
}

fn parse_size(token: &str) -> Option<WarehouseSize> {
    WarehouseSize::parse(&normalize_ident(token))
}

fn parse_u64(token: &str) -> Option<u64> {
    normalize_ident(token).parse().ok()
}

fn parse_bool(token: &str) -> Option<bool> {
    match normalize_ident(token).as_str() {
        "TRUE" | "T" | "YES" | "Y" | "ON" => Some(true),
        "FALSE" | "F" | "NO" | "N" | "OFF" => Some(false),
        _ => None,
    }
}

/// Parse raw SQL into a [`WarehouseStatement`]; `None` if the statement is
/// not warehouse DDL (in which case it goes to the regular pipeline).
#[must_use]
pub fn parse(raw: &str) -> Option<WarehouseStatement> {
    let tokens = tokenize(raw);
    if tokens.is_empty() {
        return None;
    }
    let upper: Vec<String> = tokens.iter().map(|t| t.to_ascii_uppercase()).collect();
    match upper[0].as_str() {
        "CREATE" => parse_create(&tokens, &upper),
        "ALTER" => parse_alter(&tokens, &upper),
        "DROP" => parse_drop(&tokens, &upper),
        "SHOW" => {
            if upper.len() >= 2 && (upper[1] == "WAREHOUSES" || upper[1] == "WAREHOUSE") {
                Some(WarehouseStatement::Show)
            } else {
                None
            }
        }
        "DESC" | "DESCRIBE" => {
            if upper.len() >= 3 && upper[1] == "WAREHOUSE" {
                Some(WarehouseStatement::Describe {
                    name: normalize_ident(&tokens[2]),
                })
            } else {
                None
            }
        }
        _ => None,
    }
}

fn parse_create(tokens: &[String], upper: &[String]) -> Option<WarehouseStatement> {
    // CREATE [OR REPLACE] WAREHOUSE ...
    let mut i = 1;
    if upper.get(i).map(String::as_str) == Some("OR") && upper.get(i + 1).map(String::as_str) == Some("REPLACE") {
        i += 2;
    }
    if upper.get(i).map(String::as_str) != Some("WAREHOUSE") {
        return None;
    }
    i += 1;
    let mut if_not_exists = false;
    if upper.get(i).map(String::as_str) == Some("IF")
        && upper.get(i + 1).map(String::as_str) == Some("NOT")
        && upper.get(i + 2).map(String::as_str) == Some("EXISTS")
    {
        if_not_exists = true;
        i += 3;
    }
    let Some(name) = tokens.get(i) else {
        return None;
    };
    let name = normalize_ident(name);
    i += 1;

    let mut stmt = WarehouseStatement::Create {
        name,
        size: None,
        auto_suspend_secs: None,
        auto_resume: None,
        initially_suspended: false,
        if_not_exists,
    };
    // option list: `WITH` keyword, then `KEY [=] VALUE` pairs
    while i < upper.len() {
        match upper[i].as_str() {
            "WITH" => i += 1,
            "SIZE" | "WAREHOUSE_SIZE" => {
                let WarehouseStatement::Create { size, .. } = &mut stmt else {
                    return None;
                };
                *size = value_at(tokens, upper, i + 1).and_then(|(v, _)| parse_size(v));
                i = next_index(upper, i + 1);
            }
            "AUTO_SUSPEND" => {
                let secs = value_at(tokens, upper, i + 1).and_then(|(v, _)| parse_u64(v));
                if let WarehouseStatement::Create {
                    auto_suspend_secs, ..
                } = &mut stmt
                {
                    *auto_suspend_secs = secs;
                }
                i = next_index(upper, i + 1);
            }
            "AUTO_RESUME" => {
                let val = value_at(tokens, upper, i + 1).and_then(|(v, _)| parse_bool(v));
                if let WarehouseStatement::Create { auto_resume, .. } = &mut stmt {
                    *auto_resume = val;
                }
                i = next_index(upper, i + 1);
            }
            "INITIALLY_SUSPENDED" => {
                let val = value_at(tokens, upper, i + 1).and_then(|(v, _)| parse_bool(v));
                if let WarehouseStatement::Create {
                    initially_suspended, ..
                } = &mut stmt
                {
                    *initially_suspended = val.unwrap_or(true);
                }
                i = next_index(upper, i + 1);
            }
            _ => return None, // unsupported option — let the pipeline fail loudly
        }
    }
    Some(stmt)
}

/// Returns `(value_token, next_index)` if `index` holds an `=` sign (skipping
/// it) or directly the value.
fn value_at<'a>(tokens: &'a [String], upper: &[String], index: usize) -> Option<(&'a String, usize)> {
    match upper.get(index).map(String::as_str) {
        Some("=") => {
            let value = tokens.get(index + 1)?;
            Some((value, index + 2))
        }
        Some(_) => {
            let value = tokens.get(index)?;
            Some((value, index + 1))
        }
        None => None,
    }
}

/// Index after consuming `KEY [=] VALUE` starting at `index` (the value slot).
fn next_index(upper: &[String], index: usize) -> usize {
    match upper.get(index).map(String::as_str) {
        Some("=") => index + 2,
        Some(_) => index + 1,
        None => index,
    }
}

fn parse_alter(tokens: &[String], upper: &[String]) -> Option<WarehouseStatement> {
    // ALTER WAREHOUSE [IF EXISTS] name (SET opt = value ... | SUSPEND | RESUME)
    if upper.get(1).map(String::as_str) != Some("WAREHOUSE") {
        return None;
    }
    let mut i = 2;
    if upper.get(i).map(String::as_str) == Some("IF")
        && upper.get(i + 1).map(String::as_str) == Some("EXISTS")
    {
        i += 2;
    }
    let name = normalize_ident(tokens.get(i)?);
    i += 1;
    match upper.get(i).map(String::as_str) {
        Some("SUSPEND") => Some(WarehouseStatement::Suspend { name }),
        Some("RESUME") => Some(WarehouseStatement::Resume { name }),
        Some("SET") => {
            i += 1;
            let mut size = None;
            let mut auto_suspend_secs = None;
            let mut auto_resume = None;
            while i < upper.len() {
                match upper[i].as_str() {
                    "SIZE" | "WAREHOUSE_SIZE" => {
                        size = value_at(tokens, upper, i + 1).and_then(|(v, _)| parse_size(v));
                        i = next_index(upper, i + 1);
                    }
                    "AUTO_SUSPEND" => {
                        auto_suspend_secs =
                            value_at(tokens, upper, i + 1).and_then(|(v, _)| parse_u64(v));
                        i = next_index(upper, i + 1);
                    }
                    "AUTO_RESUME" => {
                        auto_resume =
                            value_at(tokens, upper, i + 1).and_then(|(v, _)| parse_bool(v));
                        i = next_index(upper, i + 1);
                    }
                    _ => return None,
                }
            }
            Some(WarehouseStatement::AlterSet {
                name,
                size,
                auto_suspend_secs,
                auto_resume,
            })
        }
        _ => None,
    }
}

fn parse_drop(tokens: &[String], upper: &[String]) -> Option<WarehouseStatement> {
    if upper.get(1).map(String::as_str) != Some("WAREHOUSE") {
        return None;
    }
    let mut i = 2;
    let mut if_exists = false;
    if upper.get(i).map(String::as_str) == Some("IF")
        && upper.get(i + 1).map(String::as_str) == Some("EXISTS")
    {
        if_exists = true;
        i += 2;
    }
    let name = normalize_ident(tokens.get(i)?);
    Some(WarehouseStatement::Drop { name, if_exists })
}

// ------------------------------------------------------------- execution

/// Execute a parsed warehouse statement against the session's manager.
pub async fn execute(stmt: &WarehouseStatement, session: &UserSession) -> Result<QueryResult> {
    let manager: &Arc<WarehouseManager> = &session.warehouse;
    match stmt {
        WarehouseStatement::Create {
            name,
            size,
            auto_suspend_secs,
            auto_resume,
            initially_suspended,
            if_not_exists,
        } => {
            if manager.get(name).await.is_ok() {
                if *if_not_exists {
                    return status_row();
                }
                return Err(exists_error(name));
            }
            let req = CreateWarehouseRequest {
                name: name.clone(),
                size: size.unwrap_or(WarehouseSize::Xsmall),
                auto_suspend_secs: auto_suspend_secs.unwrap_or(0),
                auto_resume: auto_resume.unwrap_or(true),
                initially_suspended: *initially_suspended,
            };
            manager.create(req).await.map_err(warehouse_error)?;
            status_row()
        }
        WarehouseStatement::AlterSet {
            name,
            size,
            auto_suspend_secs,
            auto_resume,
        } => {
            manager
                .alter(name, *size, *auto_suspend_secs, *auto_resume)
                .await
                .map_err(warehouse_error)?;
            status_row()
        }
        WarehouseStatement::Suspend { name } => {
            manager.suspend(name).await.map_err(warehouse_error)?;
            status_row()
        }
        WarehouseStatement::Resume { name } => {
            manager.resume(name).await.map_err(warehouse_error)?;
            status_row()
        }
        WarehouseStatement::Drop { name, if_exists } => {
            if let Err(err) = manager.drop_warehouse(name).await {
                if *if_exists && matches!(err, avalanche_warehouse::Error::NotFound { .. }) {
                    return status_row();
                }
                return Err(warehouse_error(err));
            }
            status_row()
        }
        WarehouseStatement::Show => {
            let warehouses = manager.list().await.map_err(warehouse_error)?;
            show_result(&warehouses)
        }
        WarehouseStatement::Describe { name } => {
            let warehouse = manager.get(name).await.map_err(warehouse_error)?;
            show_result(&[warehouse])
        }
    }
}

/// Pre-parse hook for `UserQuery::execute`. `None` = not warehouse DDL.
pub async fn try_execute(raw_query: &str, session: &UserSession) -> Option<Result<QueryResult>> {
    let stmt = parse(raw_query)?;
    Some(execute(&stmt, session).await)
}

fn exists_error(name: &str) -> Error {
    Error::DataFusion {
        error: Box::new(datafusion::common::DataFusionError::External(
            format!("Warehouse '{name}' already exists").into(),
        )),
        location: Location::default(),
    }
}

fn warehouse_error(err: avalanche_warehouse::Error) -> Error {
    Error::DataFusion {
        error: Box::new(datafusion::common::DataFusionError::External(
            err.to_string().into(),
        )),
        location: Location::default(),
    }
}

/// `Statement executed successfully.` row, matching the shape of other
/// DDL responses in this engine.
fn status_row() -> Result<QueryResult> {
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

/// Snowflake-shaped `SHOW WAREHOUSES` / `DESC WAREHOUSE` result (v0 subset).
fn show_result(warehouses: &[avalanche_warehouse::Warehouse]) -> Result<QueryResult> {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("CREATED_ON", DataType::Utf8, false),
        Field::new("NAME", DataType::Utf8, false),
        Field::new("STATE", DataType::Utf8, false),
        Field::new("TYPE", DataType::Utf8, false),
        Field::new("SIZE", DataType::Utf8, false),
        Field::new("AUTO_SUSPEND", DataType::Utf8, true),
        Field::new("AUTO_RESUME", DataType::Utf8, false),
    ]));

    let created_on: Vec<Option<String>> = warehouses
        .iter()
        .map(|wh| Some(format_unix(wh.created_at)))
        .collect();
    let names: Vec<Option<String>> = warehouses
        .iter()
        .map(|wh| Some(wh.name.clone()))
        .collect();
    let states: Vec<Option<String>> = warehouses
        .iter()
        .map(|wh| Some(wh.state.as_str().to_string()))
        .collect();
    let types: Vec<Option<String>> = warehouses.iter().map(|_| Some("STANDARD".to_string())).collect();
    let sizes: Vec<Option<String>> = warehouses
        .iter()
        .map(|wh| Some(wh.size.as_str().to_string()))
        .collect();
    let auto_suspend: Vec<Option<String>> = warehouses
        .iter()
        .map(|wh| wh.auto_suspend_secs.filter(|s| *s > 0).map(|s| s.to_string()))
        .collect();
    let auto_resume: Vec<Option<String>> = warehouses
        .iter()
        .map(|wh| Some(wh.auto_resume.to_string()))
        .collect();

    let record = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(StringArray::from(created_on)),
        Arc::new(StringArray::from(names)),
        Arc::new(StringArray::from(states)),
        Arc::new(StringArray::from(types)),
        Arc::new(StringArray::from(sizes)),
        Arc::new(StringArray::from(auto_suspend)),
        Arc::new(StringArray::from(auto_resume)),
    ])
    .context(ex_error::ArrowSnafu)?;

    Ok(QueryResult::new(vec![record], schema))
}

fn format_unix(secs: u64) -> String {
    chrono::DateTime::from_timestamp(secs.min(i64::MAX as u64) as i64, 0)
        .map_or_else(|| secs.to_string(), |t| t.to_rfc3339())
}

/// Unit tests for the parser (no docker involved).
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_create_with_options() {
        let stmt = parse(
            "CREATE WAREHOUSE analytics WITH SIZE = 'MEDIUM' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = FALSE;",
        )
        .expect("should parse");
        assert_eq!(
            stmt,
            WarehouseStatement::Create {
                name: "ANALYTICS".to_string(),
                size: Some(WarehouseSize::Medium),
                auto_suspend_secs: Some(60),
                auto_resume: Some(true),
                initially_suspended: false,
                if_not_exists: false,
            }
        );
    }

    #[test]
    fn parses_create_minimal() {
        let stmt = parse("create warehouse my_wh").expect("should parse");
        assert_eq!(
            stmt,
            WarehouseStatement::Create {
                name: "MY_WH".to_string(),
                size: None,
                auto_suspend_secs: None,
                auto_resume: None,
                initially_suspended: false,
                if_not_exists: false,
            }
        );
    }

    #[test]
    fn parses_alter_set_and_suspend() {
        assert_eq!(
            parse("ALTER WAREHOUSE wh SET WAREHOUSE_SIZE = 'LARGE' AUTO_SUSPEND = 300;"),
            Some(
                WarehouseStatement::AlterSet {
                    name: "WH".to_string(),
                    size: Some(WarehouseSize::Large),
                    auto_suspend_secs: Some(300),
                    auto_resume: None,
                }
            )
        );
        assert_eq!(
            parse("ALTER WAREHOUSE IF EXISTS wh SUSPEND"),
            Some(WarehouseStatement::Suspend {
                name: "WH".to_string()
            })
        );
        assert_eq!(
            parse("ALTER WAREHOUSE wh RESUME"),
            Some(WarehouseStatement::Resume {
                name: "WH".to_string()
            })
        );
    }

    #[test]
    fn parses_drop_show_describe() {
        assert_eq!(
            parse("DROP WAREHOUSE IF EXISTS wh;"),
            Some(WarehouseStatement::Drop {
                name: "WH".to_string(),
                if_exists: true,
            })
        );
        assert!(matches!(parse("SHOW WAREHOUSES;"), Some(WarehouseStatement::Show)));
        assert_eq!(
            parse("DESC WAREHOUSE wh"),
            Some(WarehouseStatement::Describe {
                name: "WH".to_string()
            })
        );
        assert_eq!(
            parse("DESCRIBE WAREHOUSE wh"),
            parse("DESC WAREHOUSE wh")
        );
    }

    #[test]
    fn ignores_non_warehouse_sql() {
        assert!(parse("SELECT 1").is_none());
        assert!(parse("SHOW TABLES").is_none());
        assert!(parse("CREATE TABLE t (a int)").is_none());
        assert!(parse("USE WAREHOUSE wh").is_none()); // handled by sqlparser
        assert!(parse("").is_none());
    }
}
