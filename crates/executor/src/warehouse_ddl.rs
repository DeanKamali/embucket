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
use avalanche_warehouse::{CreateWarehouseRequest, WarehouseManager, WarehouseSize};
use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use snafu::Location;
use snafu::ResultExt;
use state_store::StorageCredentials;
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
        statement_timeout_secs: Option<u64>,
        storage_credentials: Option<StorageCredentials>,
        /// `TLS = TRUE/FALSE` (M7.2).
        tls: Option<bool>,
        /// `SCHEDULER_REPLICAS = n` (M7.3).
        scheduler_replicas: Option<u16>,
        if_not_exists: bool,
    },
    AlterSet {
        name: String,
        size: Option<WarehouseSize>,
        auto_suspend_secs: Option<u64>,
        auto_resume: Option<bool>,
        statement_timeout_secs: Option<u64>,
        storage: Option<StorageOp>,
        /// `SET/UNSET TLS` (M7.2): `Some(true/false)` sets, `Some(false)` is
        /// equivalent to unset (plaintext).
        tls: Option<bool>,
        /// `SET/UNSET SCHEDULER_REPLICAS` (M7.3): `Some(n)` sets the count;
        /// UNSET resets to 1 (`Some(1)`).
        scheduler_replicas: Option<u16>,
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

/// Storage-credential mutation on `ALTER WAREHOUSE ... SET/UNSET` (M7.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageOp {
    Set(StorageCredentials),
    Unset,
}

/// Tokenize raw SQL: whitespace/`,`/`;`/`(`/`)` separated, single-quoted
/// strings kept as one token (quotes preserved for later stripping).
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
            '(' | ')' if !in_quote => {
                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
                tokens.push(ch.to_string());
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
    if upper.get(i).map(String::as_str) == Some("OR")
        && upper.get(i + 1).map(String::as_str) == Some("REPLACE")
    {
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
        statement_timeout_secs: None,
        storage_credentials: None,
        tls: None,
        scheduler_replicas: None,
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
            "STORAGE_CREDENTIALS" => {
                // `STORAGE_CREDENTIALS = ( KEY = VALUE ... )`
                let open_idx = if upper.get(i + 1).map(String::as_str) == Some("=") {
                    i + 2
                } else {
                    i + 1
                };
                let (creds, next) = parse_storage_block(tokens, upper, open_idx)?;
                if let WarehouseStatement::Create {
                    storage_credentials,
                    ..
                } = &mut stmt
                {
                    *storage_credentials = creds;
                }
                i = next;
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
                    initially_suspended,
                    ..
                } = &mut stmt
                {
                    *initially_suspended = val.unwrap_or(true);
                }
                i = next_index(upper, i + 1);
            }
            "TLS" => {
                let val = value_at(tokens, upper, i + 1).and_then(|(v, _)| parse_bool(v));
                if let WarehouseStatement::Create { tls, .. } = &mut stmt {
                    *tls = val;
                }
                i = next_index(upper, i + 1);
            }
            "SCHEDULER_REPLICAS" => {
                let val = value_at(tokens, upper, i + 1)
                    .and_then(|(v, _)| parse_u64(v))
                    .map(|v| v.min(16) as u16);
                if let WarehouseStatement::Create {
                    scheduler_replicas, ..
                } = &mut stmt
                {
                    *scheduler_replicas = val;
                }
                i = next_index(upper, i + 1);
            }
            "STATEMENT_TIMEOUT" => {
                let secs = value_at(tokens, upper, i + 1).and_then(|(v, _)| parse_u64(v));
                if let WarehouseStatement::Create {
                    statement_timeout_secs,
                    ..
                } = &mut stmt
                {
                    *statement_timeout_secs = secs;
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
fn value_at<'a>(
    tokens: &'a [String],
    upper: &[String],
    index: usize,
) -> Option<(&'a String, usize)> {
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

/// Strip surrounding quotes without case-folding — for option *values*
/// (bucket names, endpoints and keys are case-sensitive, unlike identifiers).
fn strip_quotes(token: &str) -> String {
    if (token.starts_with('\'') && token.ends_with('\'') && token.len() >= 2)
        || (token.starts_with('"') && token.ends_with('"') && token.len() >= 2)
    {
        token[1..token.len() - 1].to_string()
    } else {
        token.to_string()
    }
}

/// Parse a `STORAGE_CREDENTIALS = ( KEY [=] VALUE ... )` block (M7.1).
/// Returns the credentials (or `None` for an empty block) and the index just
/// past the closing paren. Unknown keys or a missing required key fail the
/// whole parse loudly (`None`) so bad DDL never half-applies.
fn parse_storage_block(
    tokens: &[String],
    upper: &[String],
    mut i: usize,
) -> Option<(Option<StorageCredentials>, usize)> {
    if upper.get(i).map(String::as_str) != Some("(") {
        return None;
    }
    i += 1;
    let mut endpoint = None;
    let mut bucket = None;
    let mut access_key_id = None;
    let mut secret_key = None;
    let mut session_token = None;
    let mut region = None;
    while i < upper.len() && upper[i] != ")" {
        let key = upper[i].as_str();
        let (val, next) = value_at(tokens, upper, i + 1)?;
        let val = strip_quotes(val);
        match key {
            "ENDPOINT" => endpoint = Some(val),
            "BUCKET" => bucket = Some(val),
            "AWS_ACCESS_KEY_ID" | "ACCESS_KEY_ID" => access_key_id = Some(val),
            "AWS_SECRET_ACCESS_KEY" | "SECRET_ACCESS_KEY" => secret_key = Some(val),
            "AWS_SESSION_TOKEN" | "SESSION_TOKEN" => session_token = Some(val),
            "REGION" => region = Some(val),
            // `TYPE` is accepted and must be S3 (the only v1 kind)
            "TYPE" => {
                if !val.eq_ignore_ascii_case(StorageCredentials::KIND_S3) {
                    return None;
                }
            }
            _ => return None,
        }
        i = next;
    }
    if upper.get(i).map(String::as_str) != Some(")") {
        return None;
    }
    let creds = StorageCredentials {
        kind: StorageCredentials::KIND_S3.to_string(),
        endpoint: endpoint?,
        bucket: bucket?,
        access_key_id: access_key_id?,
        secret_key: secret_key?,
        session_token,
        region,
    };
    Some((Some(creds), i + 1))
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
        Some("UNSET") => {
            // `ALTER WAREHOUSE wh UNSET STORAGE_CREDENTIALS | TLS`
            if upper.get(i + 1).map(String::as_str) == Some("STORAGE_CREDENTIALS") {
                Some(WarehouseStatement::AlterSet {
                    name,
                    size: None,
                    auto_suspend_secs: None,
                    auto_resume: None,
                    statement_timeout_secs: None,
                    storage: Some(StorageOp::Unset),
                    tls: None,
                    scheduler_replicas: None,
                })
            } else if upper.get(i + 1).map(String::as_str) == Some("TLS") {
                Some(WarehouseStatement::AlterSet {
                    name,
                    size: None,
                    auto_suspend_secs: None,
                    auto_resume: None,
                    statement_timeout_secs: None,
                    storage: None,
                    tls: Some(false),
                    scheduler_replicas: None,
                })
            } else if upper.get(i + 1).map(String::as_str) == Some("SCHEDULER_REPLICAS") {
                Some(WarehouseStatement::AlterSet {
                    name,
                    size: None,
                    auto_suspend_secs: None,
                    auto_resume: None,
                    statement_timeout_secs: None,
                    storage: None,
                    tls: None,
                    scheduler_replicas: Some(1),
                })
            } else {
                None
            }
        }
        Some("SET") => {
            i += 1;
            let mut size = None;
            let mut auto_suspend_secs = None;
            let mut auto_resume = None;
            let mut statement_timeout_secs = None;
            let mut storage = None;
            let mut tls = None;
            let mut scheduler_replicas = None;
            while i < upper.len() {
                match upper[i].as_str() {
                    "SIZE" | "WAREHOUSE_SIZE" => {
                        size = value_at(tokens, upper, i + 1).and_then(|(v, _)| parse_size(v));
                        i = next_index(upper, i + 1);
                    }
                    "STORAGE_CREDENTIALS" => {
                        // `SET STORAGE_CREDENTIALS = ( ... )`
                        let open_idx = if upper.get(i + 1).map(String::as_str) == Some("=") {
                            i + 2
                        } else {
                            i + 1
                        };
                        let (creds, next) = parse_storage_block(tokens, upper, open_idx)?;
                        storage = creds.map(StorageOp::Set);
                        i = next;
                    }
                    "UNSET" => {
                        // `SET UNSET STORAGE_CREDENTIALS` (and bare
                        // `ALTER WAREHOUSE wh UNSET STORAGE_CREDENTIALS`)
                        if upper.get(i + 1).map(String::as_str)
                            == Some("STORAGE_CREDENTIALS")
                        {
                            storage = Some(StorageOp::Unset);
                            i += 2;
                        } else if upper.get(i + 1).map(String::as_str) == Some("TLS") {
                            tls = Some(false);
                            i += 2;
                        } else if upper.get(i + 1).map(String::as_str)
                            == Some("SCHEDULER_REPLICAS")
                        {
                            scheduler_replicas = Some(1);
                            i += 2;
                        } else {
                            return None;
                        }
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
                    "STATEMENT_TIMEOUT" => {
                        statement_timeout_secs =
                            value_at(tokens, upper, i + 1).and_then(|(v, _)| parse_u64(v));
                        i = next_index(upper, i + 1);
                    }
                    "TLS" => {
                        tls = value_at(tokens, upper, i + 1).and_then(|(v, _)| parse_bool(v));
                        i = next_index(upper, i + 1);
                    }
                    "SCHEDULER_REPLICAS" => {
                        scheduler_replicas = value_at(tokens, upper, i + 1)
                            .and_then(|(v, _)| parse_u64(v))
                            .map(|v| v.min(16) as u16);
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
                statement_timeout_secs,
                storage,
                tls,
                scheduler_replicas,
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
            statement_timeout_secs,
            storage_credentials,
            tls,
            scheduler_replicas,
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
                statement_timeout_secs: *statement_timeout_secs,
                storage_credentials: storage_credentials.clone(),
                tls: tls.unwrap_or(false),
                scheduler_replicas: scheduler_replicas.unwrap_or(1),
            };
            manager.create(req).await.map_err(warehouse_error)?;
            status_row()
        }
        WarehouseStatement::AlterSet {
            name,
            size,
            auto_suspend_secs,
            auto_resume,
            statement_timeout_secs,
            storage,
            tls,
            scheduler_replicas,
        } => {
            manager
                .alter(
                    name,
                    *size,
                    *auto_suspend_secs,
                    *auto_resume,
                    *statement_timeout_secs,
                )
                .await
                .map_err(warehouse_error)?;
            // Storage-credential changes restart a running warehouse's
            // compute so containers pick up the new env (M7.1).
            match storage {
                Some(StorageOp::Set(creds)) => {
                    manager
                        .set_storage_credentials(name, Some(creds.clone()))
                        .await
                        .map_err(warehouse_error)?;
                }
                Some(StorageOp::Unset) => {
                    manager
                        .set_storage_credentials(name, None)
                        .await
                        .map_err(warehouse_error)?;
                }
                None => {}
            }
            // TLS toggle (M7.2): restart a running warehouse's compute so
            // containers pick up the new mounts; also flips the advertised
            // scheduler URL scheme.
            if let Some(tls) = tls {
                manager.set_tls(name, *tls).await.map_err(warehouse_error)?;
            }
            // Scheduler replicas (M7.3): restarts a running warehouse's
            // schedulers so the replica set takes effect.
            if let Some(replicas) = scheduler_replicas {
                manager
                    .set_scheduler_replicas(name, *replicas)
                    .await
                    .map_err(warehouse_error)?;
            }
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
        vec![
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(StringArray::from(vec![
                    "Statement executed successfully.",
                ]))],
            )
            .context(ex_error::ArrowSnafu)?,
        ],
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
        // M7.1: display-safe credential summary — never secret values.
        Field::new("STORAGE", DataType::Utf8, true),
        // M7.2: mutual TLS on/off.
        Field::new("TLS", DataType::Utf8, false),
        // M7.3: scheduler replicas sharing the warehouse's cluster state.
        Field::new("SCHEDULER_REPLICAS", DataType::Utf8, false),
    ]));

    let created_on: Vec<Option<String>> = warehouses
        .iter()
        .map(|wh| Some(format_unix(wh.created_at)))
        .collect();
    let names: Vec<Option<String>> = warehouses.iter().map(|wh| Some(wh.name.clone())).collect();
    let states: Vec<Option<String>> = warehouses
        .iter()
        .map(|wh| Some(wh.state.as_str().to_string()))
        .collect();
    let types: Vec<Option<String>> = warehouses
        .iter()
        .map(|_| Some("STANDARD".to_string()))
        .collect();
    let sizes: Vec<Option<String>> = warehouses
        .iter()
        .map(|wh| Some(wh.size.as_str().to_string()))
        .collect();
    let auto_suspend: Vec<Option<String>> = warehouses
        .iter()
        .map(|wh| {
            wh.auto_suspend_secs
                .filter(|s| *s > 0)
                .map(|s| s.to_string())
        })
        .collect();
    let auto_resume: Vec<Option<String>> = warehouses
        .iter()
        .map(|wh| Some(wh.auto_resume.to_string()))
        .collect();
    let storage: Vec<Option<String>> = warehouses
        .iter()
        .map(|wh| wh.storage_summary.clone())
        .collect();
    let tls: Vec<Option<String>> = warehouses
        .iter()
        .map(|wh| Some(wh.tls.to_string()))
        .collect();
    let replicas: Vec<Option<String>> = warehouses
        .iter()
        .map(|wh| Some(wh.scheduler_replicas.to_string()))
        .collect();

    let record = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(created_on)),
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(states)),
            Arc::new(StringArray::from(types)),
            Arc::new(StringArray::from(sizes)),
            Arc::new(StringArray::from(auto_suspend)),
            Arc::new(StringArray::from(auto_resume)),
            Arc::new(StringArray::from(storage)),
            Arc::new(StringArray::from(tls)),
            Arc::new(StringArray::from(replicas)),
        ],
    )
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
                statement_timeout_secs: None,
                storage_credentials: None,
                tls: None,
                scheduler_replicas: None,
                if_not_exists: false,
            }
        );
    }

    #[test]
    fn parses_storage_credentials_on_create() {
        let stmt = parse(
            "CREATE WAREHOUSE etl WITH SIZE = 'SMALL' STORAGE_CREDENTIALS = ( \
             TYPE = S3 ENDPOINT = 'http://minio:9000' BUCKET = 'lake' \
             AWS_ACCESS_KEY_ID = 'minio' AWS_SECRET_ACCESS_KEY = 'minio123' \
             REGION = 'us-east-1' );",
        )
        .expect("should parse");
        let WarehouseStatement::Create {
            storage_credentials, ..
        } = stmt
        else {
            panic!("expected Create");
        };
        let creds = storage_credentials.expect("credentials parsed");
        assert_eq!(creds.kind, "s3");
        assert_eq!(creds.endpoint, "http://minio:9000");
        assert_eq!(creds.bucket, "lake"); // case preserved (quoted value)
        assert_eq!(creds.access_key_id, "minio");
        assert_eq!(creds.secret_key, "minio123");
        assert_eq!(creds.region.as_deref(), Some("us-east-1"));
        assert_eq!(creds.session_token, None);
        // redacted summary never leaks secrets
        assert!(!creds.summary().contains("minio123"));
    }

    #[test]
    fn parses_tls_on_create() {
        let stmt = parse("CREATE WAREHOUSE secure WITH TLS = TRUE SIZE = XSMALL;")
            .expect("should parse");
        assert!(
            matches!(&stmt, WarehouseStatement::Create { tls: Some(true), .. }),
            "TLS = TRUE should parse into Create {{ tls: Some(true) }}"
        );
        let stmt =
            parse("CREATE WAREHOUSE plain WITH TLS = FALSE;").expect("should parse");
        assert!(matches!(&stmt, WarehouseStatement::Create { tls: Some(false), .. }));
        // no TLS option → None (plaintext default)
        let stmt = parse("CREATE WAREHOUSE plain;").expect("should parse");
        assert!(matches!(&stmt, WarehouseStatement::Create { tls: None, .. }));
    }

    #[test]
    fn parses_alter_tls_set_and_unset() {
        let stmt = parse("ALTER WAREHOUSE etl SET TLS = TRUE;").expect("should parse");
        assert!(matches!(
            &stmt,
            WarehouseStatement::AlterSet { tls: Some(true), storage: None, .. }
        ));
        let stmt = parse("ALTER WAREHOUSE etl SET TLS = FALSE;").expect("should parse");
        assert!(matches!(
            &stmt,
            WarehouseStatement::AlterSet { tls: Some(false), .. }
        ));
        let stmt = parse("ALTER WAREHOUSE etl UNSET TLS;").expect("should parse");
        assert!(matches!(
            &stmt,
            WarehouseStatement::AlterSet { tls: Some(false), storage: None, .. }
        ));
    }

    #[test]
    fn storage_block_requires_required_keys() {
        // missing AWS_SECRET_ACCESS_KEY → whole statement fails loudly
        assert!(parse(
            "CREATE WAREHOUSE wh WITH STORAGE_CREDENTIALS = ( ENDPOINT = 'http://x:1' BUCKET = 'b' AWS_ACCESS_KEY_ID = 'a' );"
        )
        .is_none());
        // unsupported TYPE → rejected
        assert!(parse(
            "CREATE WAREHOUSE wh WITH STORAGE_CREDENTIALS = ( TYPE = AZURE ENDPOINT = 'http://x:1' BUCKET = 'b' AWS_ACCESS_KEY_ID = 'a' AWS_SECRET_ACCESS_KEY = 's' );"
        )
        .is_none());
        // unknown key → rejected
        assert!(parse(
            "CREATE WAREHOUSE wh WITH STORAGE_CREDENTIALS = ( FOO = 1 ENDPOINT = 'http://x:1' BUCKET = 'b' AWS_ACCESS_KEY_ID = 'a' AWS_SECRET_ACCESS_KEY = 's' );"
        )
        .is_none());
    }

    #[test]
    fn parses_alter_storage_set_and_unset() {
        let stmt = parse(
            "ALTER WAREHOUSE etl SET STORAGE_CREDENTIALS = ( \
             ENDPOINT = 'http://minio:9000' BUCKET = 'lake2' \
             AWS_ACCESS_KEY_ID = 'k' AWS_SECRET_ACCESS_KEY = 's' );",
        )
        .expect("should parse");
        let WarehouseStatement::AlterSet { storage, .. } = stmt else {
            panic!("expected AlterSet");
        };
        assert!(matches!(storage, Some(StorageOp::Set(_))));

        assert_eq!(
            parse("ALTER WAREHOUSE etl UNSET STORAGE_CREDENTIALS;"),
            Some(WarehouseStatement::AlterSet {
                name: "ETL".to_string(),
                size: None,
                auto_suspend_secs: None,
                auto_resume: None,
                statement_timeout_secs: None,
                storage: Some(StorageOp::Unset),
                tls: None,
                scheduler_replicas: None,
            })
        );
        assert_eq!(
            parse("ALTER WAREHOUSE etl SET UNSET STORAGE_CREDENTIALS;"),
            parse("ALTER WAREHOUSE etl UNSET STORAGE_CREDENTIALS;")
        );
    }

    #[test]
    fn parses_statement_timeout() {
        let stmt = parse(
            "CREATE WAREHOUSE analytics WITH SIZE = 'MEDIUM' STATEMENT_TIMEOUT = 90;",
        )
        .expect("should parse");
        let WarehouseStatement::Create {
            statement_timeout_secs,
            ..
        } = stmt
        else {
            panic!("expected Create");
        };
        assert_eq!(statement_timeout_secs, Some(90));

        assert_eq!(
            parse("ALTER WAREHOUSE wh SET STATEMENT_TIMEOUT = 0;"),
            Some(WarehouseStatement::AlterSet {
                name: "WH".to_string(),
                size: None,
                auto_suspend_secs: None,
                auto_resume: None,
                statement_timeout_secs: Some(0),
                storage: None,
                tls: None,
                scheduler_replicas: None,
            })
        );
    }

    #[test]
    fn parses_create_minimal() {
        assert_eq!(
            parse("create warehouse my_wh").expect("should parse"),
            WarehouseStatement::Create {
                name: "MY_WH".to_string(),
                size: None,
                auto_suspend_secs: None,
                auto_resume: None,
                initially_suspended: false,
                statement_timeout_secs: None,
                storage_credentials: None,
                tls: None,
                scheduler_replicas: None,
                if_not_exists: false,
            }
        );
    }

    #[test]
    fn parses_scheduler_replicas() {
        let stmt = parse(
            "CREATE WAREHOUSE etl WITH SCHEDULER_REPLICAS = 2;",
        )
        .expect("should parse");
        let WarehouseStatement::Create {
            scheduler_replicas, ..
        } = stmt
        else {
            panic!("expected Create");
        };
        assert_eq!(scheduler_replicas, Some(2));

        assert_eq!(
            parse("ALTER WAREHOUSE wh SET SCHEDULER_REPLICAS = 3;"),
            Some(WarehouseStatement::AlterSet {
                name: "WH".to_string(),
                size: None,
                auto_suspend_secs: None,
                auto_resume: None,
                statement_timeout_secs: None,
                storage: None,
                tls: None,
                scheduler_replicas: Some(3),
            })
        );
        assert_eq!(
            parse("ALTER WAREHOUSE wh UNSET SCHEDULER_REPLICAS;"),
            Some(WarehouseStatement::AlterSet {
                name: "WH".to_string(),
                size: None,
                auto_suspend_secs: None,
                auto_resume: None,
                statement_timeout_secs: None,
                storage: None,
                tls: None,
                scheduler_replicas: Some(1),
            })
        );
    }

    #[test]
    fn parses_alter_set_and_suspend() {
        assert_eq!(
            parse("ALTER WAREHOUSE wh SET WAREHOUSE_SIZE = 'LARGE' AUTO_SUSPEND = 300;"),
            Some(WarehouseStatement::AlterSet {
                name: "WH".to_string(),
                size: Some(WarehouseSize::Large),
                auto_suspend_secs: Some(300),
                auto_resume: None,
                statement_timeout_secs: None,
                storage: None,
                tls: None,
                scheduler_replicas: None,
            })
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
        assert!(matches!(
            parse("SHOW WAREHOUSES;"),
            Some(WarehouseStatement::Show)
        ));
        assert_eq!(
            parse("DESC WAREHOUSE wh"),
            Some(WarehouseStatement::Describe {
                name: "WH".to_string()
            })
        );
        assert_eq!(parse("DESCRIBE WAREHOUSE wh"), parse("DESC WAREHOUSE wh"));
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
