//! Pre-parse interception of automation DDL (Avalanche design §18, M11):
//! streams, tasks, and dynamic tables. The sqlparser fork has no grammar
//! for these Snowflake statements, so they are matched on the raw SQL.
//! Supported (Snowflake subset, v1):
//!
//! - `CREATE [OR REPLACE] [IF NOT EXISTS] STREAM <name> ON TABLE <src>
//!   [APPEND_ONLY = TRUE|FALSE] [COMMENT = '<c>']`
//! - `DROP STREAM <name>`, `SHOW STREAMS [LIKE …]`, `DESCRIBE STREAM <name>`
//! - `CREATE [OR REPLACE] [IF NOT EXISTS] TASK <name> [WAREHOUSE <wh>]
//!   [SCHEDULE '<n MINUTE' | 'USING CRON <5-field> [TZ]'>] [AFTER <t>]
//!   AS <sql>`
//! - `ALTER TASK <name> RESUME|SUSPEND`, `DROP TASK <name>`, `SHOW TASKS
//!   [LIKE …]`, `EXECUTE TASK <name>`
//! - `CREATE [OR REPLACE] DYNAMIC TABLE <name> [TARGET_LAG '<n <unit>'>]
//!   [WAREHOUSE <wh>] AS <query>`
//! - `ALTER DYNAMIC TABLE <name> REFRESH`, `DROP DYNAMIC TABLE <name>`,
//!   `SHOW DYNAMIC TABLES [LIKE …]`
//!
//! Stream reads resolve through the catalog (`stream_table.rs` in the
//! catalog crate); DML consumption advances the stream offset via
//! [`advance_consumed_streams`].

use crate::error::{self as ex_error, Error, Result};
use crate::models::{QueryContext, QueryResult};
use crate::query::UserQuery;
use crate::session::UserSession;
use catalog_metastore::models::{
    DynamicTable, SchemaIdent, Stream, TableIdent, Task, TaskRunState, TaskSchedule, TaskState,
};
use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::sql::sqlparser::ast::{
    Ident, ObjectName, Query as SqlQuery, SetExpr, Statement as SqlStatement, TableFactor,
};
use datafusion::sql::parser::Statement as DFStatement;
use snafu::Location;
use snafu::ResultExt;
use std::str::FromStr;
use std::sync::Arc;

/// A parsed automation statement (v1 grammar).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AutomationStatement {
    CreateStream {
        name: String,
        source: String,
        append_only: bool,
        comment: Option<String>,
        or_replace: bool,
        if_not_exists: bool,
    },
    DropStream {
        name: String,
    },
    ShowStreams,
    DescribeStream {
        name: String,
    },
    CreateTask {
        name: String,
        warehouse: Option<String>,
        schedule: Option<TaskSchedule>,
        predecessor: Option<String>,
        definition: String,
        or_replace: bool,
        if_not_exists: bool,
    },
    AlterTask {
        name: String,
        resume: bool,
    },
    DropTask {
        name: String,
    },
    ShowTasks,
    ExecuteTask {
        name: String,
    },
    CreateDynamicTable {
        name: String,
        warehouse: Option<String>,
        target_lag_seconds: Option<i64>,
        definition: String,
        or_replace: bool,
    },
    AlterDynamicTableRefresh {
        name: String,
    },
    DropDynamicTable {
        name: String,
    },
    ShowDynamicTables,
}

/// A lexed token: whitespace-separated; single/double-quoted strings and
/// backticked identifiers kept whole (quotes included), with source byte
/// offsets so `AS <body>` can capture the raw remainder.
struct Token {
    text: String,
    quoted: bool,
    start: usize,
    end: usize,
}

/// Quote-aware lexer (v1: `''` escapes inside single quotes).
fn lex(raw: &str) -> Vec<Token> {
    let chars: Vec<char> = raw.chars().collect();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        if chars[i].is_whitespace() {
            i += 1;
            continue;
        }
        let start = i;
        let c = chars[i];
        if c == '\'' || c == '"' || c == '`' {
            let quote = c;
            let mut j = i + 1;
            while j < chars.len() {
                if chars[j] == quote {
                    if quote == '\'' && j + 1 < chars.len() && chars[j + 1] == '\'' {
                        j += 2; // '' escape
                        continue;
                    }
                    break;
                }
                j += 1;
            }
            let end = (j + 1).min(chars.len());
            tokens.push(Token {
                text: chars[start..end].iter().collect(),
                quoted: true,
                start,
                end,
            });
            i = end;
        } else {
            let mut j = i;
            while j < chars.len() && !chars[j].is_whitespace() && chars[j] != ';' {
                j += 1;
            }
            tokens.push(Token {
                text: chars[start..j].iter().collect(),
                quoted: false,
                start,
                end: j,
            });
            i = j; // ';' acts as a terminator (consumed on next loop)
        }
    }
    tokens
}

fn unquote(text: &str) -> String {
    text.trim_matches(|c| c == '\'' || c == '"' || c == '`')
        .replace("''", "'")
}

/// Parse `<n> <unit>` (second(s)/minute(s)/hour(s)/day(s)) into seconds.
fn lag_seconds(spec: &str) -> Option<i64> {
    let mut parts = spec.split_whitespace();
    let n: i64 = parts.next()?.parse().ok()?;
    match parts.next()?.to_ascii_lowercase().as_str() {
        "second" | "seconds" => Some(n),
        "minute" | "minutes" => Some(n * 60),
        "hour" | "hours" => Some(n * 3600),
        "day" | "days" => Some(n * 86_400),
        _ => None,
    }
}

/// Parse a schedule literal: `'5 MINUTE'` or `'USING CRON 0 9 * * * UTC'`.
fn parse_schedule(spec: &str) -> Option<TaskSchedule> {
    let spec = spec.trim();
    if let Some(cron) = spec.strip_prefix("USING CRON") {
        let expr = cron.trim();
        if expr.is_empty() {
            return None;
        }
        return Some(TaskSchedule::Cron(expr.to_string()));
    }
    let mut parts = spec.split_whitespace();
    let n: i64 = parts.next()?.parse().ok()?;
    if n <= 0 {
        return None;
    }
    let unit = parts.next()?.to_ascii_lowercase();
    let unit = unit.trim_end_matches('s');
    if unit != "minute" {
        return None;
    }
    Some(TaskSchedule::Minutes(n))
}

/// Match the automation grammar; `None` when this is not an automation
/// statement (falls through to the normal planner).
#[allow(clippy::too_many_lines)]
pub fn parse(raw: &str) -> Option<AutomationStatement> {
    let toks = lex(raw);
    let t = |i: usize| -> Option<&str> { toks.get(i).map(|tok| tok.text.as_str()) };
    let up = |i: usize| -> Option<String> { t(i).map(|s| s.to_ascii_uppercase()) };

    match up(0)?.as_str() {
        "CREATE" | "DROP" | "ALTER" | "EXECUTE" | "SHOW" | "DESCRIBE" | "DESC" => {}
        _ => return None,
    }

    if up(0).as_deref() == Some("CREATE") {
        let mut i = 1;
        let mut or_replace = false;
        if up(1).as_deref() == Some("OR") && up(2).as_deref() == Some("REPLACE") {
            or_replace = true;
            i = 3;
        }
        let kind = up(i)?;
        let mut j = i + 1;
        let mut if_not_exists = false;
        if up(j).as_deref() == Some("IF")
            && up(j + 1).as_deref() == Some("NOT")
            && up(j + 2).as_deref() == Some("EXISTS")
        {
            if_not_exists = true;
            j += 3;
        }
        if kind == "DYNAMIC" {
            // CREATE [OR REPLACE] DYNAMIC TABLE <name> [options] AS <query>
            if up(j).as_deref() != Some("TABLE") {
                return None;
            }
            let dt_name = toks.get(j + 1)?.text.clone();
            let mut warehouse = None;
            let mut target_lag = None;
            let mut definition = None;
            let mut k = j + 2;
            while k < toks.len() {
                let key = up(k)?;
                if key == "AS" {
                    definition = Some(raw[toks.get(k + 1)?.start..].trim().to_string());
                    break;
                }
                if key == "WAREHOUSE" {
                    if up(k + 1).as_deref() == Some("=") {
                        warehouse = Some(unquote(&toks.get(k + 2)?.text));
                        k += 3;
                    } else {
                        warehouse = Some(unquote(&toks.get(k + 1)?.text));
                        k += 2;
                    }
                    continue;
                }
                if key == "TARGET_LAG" {
                    let value_idx = if up(k + 1).as_deref() == Some("=") {
                        k + 2
                    } else {
                        k + 1
                    };
                    let value = unquote(&toks.get(value_idx)?.text);
                    target_lag = Some(lag_seconds(&value)?);
                    k = value_idx + 1;
                    continue;
                }
                return None;
            }
            return Some(AutomationStatement::CreateDynamicTable {
                name: dt_name,
                warehouse,
                target_lag_seconds: target_lag,
                definition: definition?,
                or_replace,
            });
        }
        let name = toks.get(j)?.text.clone();
        j += 1;
        match kind.as_str() {
            "STREAM" => {
                if up(j).as_deref() != Some("ON") || up(j + 1).as_deref() != Some("TABLE") {
                    return None;
                }
                let source = toks.get(j + 2)?.text.clone();
                let mut append_only = false;
                let mut comment = None;
                let mut k = j + 3;
                while k < toks.len() {
                    let key = up(k)?;
                    if t(k + 1).is_none() || !t(k + 1)?.eq_ignore_ascii_case("=") {
                        return None;
                    }
                    let value = toks.get(k + 2)?;
                    match key.as_str() {
                        "APPEND_ONLY" => match value.text.to_ascii_uppercase().as_str() {
                            "TRUE" => append_only = true,
                            "FALSE" => append_only = false,
                            _ => return None,
                        },
                        "COMMENT" => {
                            if !value.quoted {
                                return None;
                            }
                            comment = Some(unquote(&value.text));
                        }
                        _ => return None,
                    }
                    k += 3;
                }
                return Some(AutomationStatement::CreateStream {
                    name,
                    source,
                    append_only,
                    comment,
                    or_replace,
                    if_not_exists,
                });
            }
            "TASK" => {
                let mut warehouse = None;
                let mut schedule = None;
                let mut predecessor = None;
                let mut definition = None;
                let mut k = j;
                while k < toks.len() {
                    let key = up(k)?;
                    if key == "AS" {
                        definition = Some(raw[toks.get(k + 1)?.start..].trim().to_string());
                        break;
                    }
                    if key == "WAREHOUSE" {
                        if up(k + 1).as_deref() == Some("=") {
                            warehouse = Some(unquote(&toks.get(k + 2)?.text));
                            k += 3;
                        } else {
                            warehouse = Some(unquote(&toks.get(k + 1)?.text));
                            k += 2;
                        }
                        continue;
                    }
                    if key == "SCHEDULE" {
                        let value_idx = if up(k + 1).as_deref() == Some("=") {
                            k + 2
                        } else {
                            k + 1
                        };
                        let value = unquote(&toks.get(value_idx)?.text);
                        schedule = Some(parse_schedule(&value)?);
                        k = value_idx + 1;
                        continue;
                    }
                    if key == "AFTER" {
                        predecessor = Some(unquote(&toks.get(k + 1)?.text));
                        k += 2;
                        if t(k) == Some(",") {
                            return None; // v1: single predecessor
                        }
                        continue;
                    }
                    if key == "WHEN" {
                        return None; // v1: WHEN predicate unsupported
                    }
                    return None;
                }
                let definition = definition?;
                return Some(AutomationStatement::CreateTask {
                    name,
                    warehouse,
                    schedule,
                    predecessor,
                    definition,
                    or_replace,
                    if_not_exists,
                });
            }
            _ => return None,
        }
    }

    match up(0)?.as_str() {
        "DROP" => match up(1)?.as_str() {
            "STREAM" => Some(AutomationStatement::DropStream {
                name: unquote(&toks.get(2)?.text),
            }),
            "TASK" => Some(AutomationStatement::DropTask {
                name: unquote(&toks.get(2)?.text),
            }),
            "DYNAMIC" if up(2).as_deref() == Some("TABLE") => {
                Some(AutomationStatement::DropDynamicTable {
                    name: unquote(&toks.get(3)?.text),
                })
            }
            _ => None,
        },
        "SHOW" => match up(1)?.as_str() {
            "STREAMS" => Some(AutomationStatement::ShowStreams),
            "TASKS" => Some(AutomationStatement::ShowTasks),
            "DYNAMIC" if up(2).as_deref() == Some("TABLES") => {
                Some(AutomationStatement::ShowDynamicTables)
            }
            _ => None,
        },
        "DESCRIBE" | "DESC" if up(1).as_deref() == Some("STREAM") => {
            Some(AutomationStatement::DescribeStream {
                name: unquote(&toks.get(2)?.text),
            })
        }
        "ALTER" => match up(1)?.as_str() {
            "TASK" => match up(3)?.as_str() {
                "RESUME" => Some(AutomationStatement::AlterTask {
                    name: unquote(&toks.get(2)?.text),
                    resume: true,
                }),
                "SUSPEND" => Some(AutomationStatement::AlterTask {
                    name: unquote(&toks.get(2)?.text),
                    resume: false,
                }),
                _ => None,
            },
            "DYNAMIC" if up(2).as_deref() == Some("TABLE") => {
                if up(4).as_deref() != Some("REFRESH") {
                    return None;
                }
                Some(AutomationStatement::AlterDynamicTableRefresh {
                    name: unquote(&toks.get(3)?.text),
                })
            }
            _ => None,
        },
        "EXECUTE" if up(1).as_deref() == Some("TASK") => Some(AutomationStatement::ExecuteTask {
            name: unquote(&toks.get(2)?.text),
        }),
        _ => None,
    }
}

/// Entry point: run automation DDL when the raw SQL matches; `None` = fall
/// through to the normal planner.
pub async fn try_execute(raw: &str, session: &Arc<UserSession>) -> Option<Result<QueryResult>> {
    let statement = parse(raw)?;
    // Boxed: nested DDL (dynamic-table CTAS, DROP TABLE, EXECUTE TASK)
    // re-enters the hook chain, so this async-fn cycle needs a box.
    Some(Box::pin(execute(&statement, session)).await)
}

/// Metastore/planner errors → executor errors (Snowflake-shaped messages).
pub(crate) fn automation_error(context: &str, error: impl std::fmt::Display) -> Error {
    Error::DataFusion {
        error: Box::new(datafusion::common::DataFusionError::External(
            format!("{context}: {error}").into(),
        )),
        location: Location::default(),
    }
}

fn table_ident(ident: &crate::utils::NormalizedIdent) -> TableIdent {
    let metastore_ident: catalog_metastore::TableIdent = ident.clone().into();
    metastore_ident
}

fn display(ident: &crate::utils::NormalizedIdent) -> String {
    String::from(ident)
}

/// Resolve a possibly-unqualified object name against the session context.
fn resolve(session: &Arc<UserSession>, name: &str) -> Result<crate::utils::NormalizedIdent> {
    let object = parse_object_name(name)?;
    let query = UserQuery::new(Arc::clone(session), String::new(), QueryContext::default());
    query
        .resolve_table_object_name(object)
        .map_err(|e| automation_error("resolve", e))
}

/// Parse a dotted object name into `Vec<ObjectNamePart>` without a full SQL
/// parse (dotted identifiers only).
fn parse_object_name(
    name: &str,
) -> Result<Vec<datafusion::sql::sqlparser::ast::ObjectNamePart>> {
    use datafusion::sql::sqlparser::ast::{Ident, ObjectNamePart};
    name.split('.')
        .map(|part| {
            let part = part.trim();
            if part.is_empty() {
                return Err(automation_error(
                    "resolve",
                    format!("invalid object name '{name}'"),
                ));
            }
            let first = part.chars().next();
            if first.is_some_and(|c| matches!(c, '"' | '`'))
                && part.len() >= 2
                && part.ends_with(first.unwrap())
            {
                Ok(ObjectNamePart::Identifier(Ident::with_quote(
                    first.unwrap(),
                    &part[1..part.len() - 1],
                )))
            } else {
                Ok(ObjectNamePart::Identifier(Ident::new(part)))
            }
        })
        .collect()
}

/// Source's current snapshot id, flattened (`None` = table has no snapshots).
fn current_snapshot_id(
    table: &catalog_metastore::models::Table,
) -> Result<Option<i64>> {
    let snapshot = table
        .metadata
        .current_snapshot(None)
        .map_err(|e| automation_error("stream", e))?;
    Ok(snapshot.map(|s| *s.snapshot_id()))
}

#[allow(clippy::too_many_lines)]
async fn execute(stmt: &AutomationStatement, session: &Arc<UserSession>) -> Result<QueryResult> {
    let metastore = session.metastore.clone();
    let norm = |name: &str| -> String {
        session
            .ident_normalizer
            .normalize(Ident::new(name))
    };
    let current_schema = || {
        SchemaIdent::new(
            norm(&session_current_database(session)),
            norm(&session_current_schema(session)),
        )
    };
    match stmt {
        AutomationStatement::CreateStream {
            name,
            source,
            append_only,
            comment,
            or_replace: _,
            if_not_exists,
        } => {
            let ident = resolve(session, name).map_err(|e| automation_error("create stream", e))?;
            let existing = metastore
                .get_stream(&table_ident(&ident))
                .await
                .map_err(|e| automation_error("create stream", e))?;
            if existing.is_some() && *if_not_exists {
                return crate::clone_ddl::status_row();
            }
            let source_ident =
                resolve(session, source).map_err(|e| automation_error("create stream", e))?;
            let source_table = metastore
                .get_table(&table_ident(&source_ident))
                .await
                .map_err(|e| automation_error("create stream", e))?
                .ok_or_else(|| {
                    automation_error(
                        "create stream",
                        format!("table '{}' does not exist or not authorized", display(&source_ident)),
                    )
                })?;
            let offset = current_snapshot_id(&source_table)?;
            let stream = Stream {
                ident: table_ident(&ident),
                source: table_ident(&source_ident),
                offset_snapshot: offset,
                append_only: *append_only,
                comment: comment.clone(),
            };
            metastore
                .create_stream(&table_ident(&ident), stream)
                .await
                .map_err(|e| automation_error("create stream", e))?;
            crate::clone_ddl::status_row()
        }
        AutomationStatement::DropStream { name } => {
            let ident = resolve(session, name).map_err(|e| automation_error("drop stream", e))?;
            metastore
                .delete_stream(&table_ident(&ident))
                .await
                .map_err(|e| automation_error("drop stream", e))?;
            crate::clone_ddl::status_row()
        }
        AutomationStatement::ShowStreams => {
            let streams = metastore
                .list_streams(&current_schema())
                .await
                .map_err(|e| automation_error("show streams", e))?;
            let mut created = Vec::new();
            let mut names = Vec::new();
            let mut dbs = Vec::new();
            let mut schemas = Vec::new();
            let mut tables = Vec::new();
            let mut modes = Vec::new();
            for s in streams {
                created.push(s.created_at.to_string());
                names.push(s.ident.table.clone());
                dbs.push(s.ident.database.clone());
                schemas.push(s.ident.schema.clone());
                tables.push(s.source.table.clone());
                modes.push(
                    if s.append_only {
                        "APPEND_ONLY".to_owned()
                    } else {
                        "DEFAULT".to_owned()
                    },
                );
            }
            stream_row_result(created, names, dbs, schemas, tables, modes)
        }
        AutomationStatement::DescribeStream { name } => {
            let ident = resolve(session, name).map_err(|e| automation_error("describe stream", e))?;
            let stream = metastore
                .get_stream(&table_ident(&ident))
                .await
                .map_err(|e| automation_error("describe stream", e))?
                .ok_or_else(|| {
                    automation_error(
                        "describe stream",
                        format!("stream '{}' does not exist or not authorized", display(&ident)),
                    )
                })?;
            stream_row_result(
                vec![stream.created_at.to_string()],
                vec![stream.ident.table.clone()],
                vec![stream.ident.database.clone()],
                vec![stream.ident.schema.clone()],
                vec![stream.source.table.clone()],
                vec![
                    if stream.append_only {
                        "APPEND_ONLY".to_owned()
                    } else {
                        "DEFAULT".to_owned()
                    },
                ],
            )
        }
        AutomationStatement::CreateTask {
            name,
            warehouse,
            schedule,
            predecessor,
            definition,
            or_replace: _,
            if_not_exists,
        } => {
            let ident = resolve(session, name).map_err(|e| automation_error("create task", e))?;
            let existing = metastore
                .get_task(&table_ident(&ident))
                .await
                .map_err(|e| automation_error("create task", e))?;
            if existing.is_some() && *if_not_exists {
                return crate::clone_ddl::status_row();
            }
            if let Some(TaskSchedule::Cron(expr)) = schedule {
                validate_cron(expr).map_err(|e| automation_error("create task", e))?;
            }
            let task = Task {
                ident: table_ident(&ident),
                definition: definition.clone(),
                schedule: schedule.clone(),
                state: TaskState::Suspended,
                warehouse: warehouse.as_ref().map(|w| norm(w)),
                predecessor: predecessor.as_ref().map(|p| norm(p)),
                last_run_ms: None,
                last_run_state: None,
            };
            metastore
                .create_task(&table_ident(&ident), task)
                .await
                .map_err(|e| automation_error("create task", e))?;
            crate::clone_ddl::status_row()
        }
        AutomationStatement::AlterTask { name, resume } => {
            let ident = resolve(session, name).map_err(|e| automation_error("alter task", e))?;
            let existing = metastore
                .get_task(&table_ident(&ident))
                .await
                .map_err(|e| automation_error("alter task", e))?
                .ok_or_else(|| {
                    automation_error(
                        "alter task",
                        format!("task '{}' does not exist", display(&ident)),
                    )
                })?;
            let updated = Task {
                state: if *resume {
                    TaskState::Started
                } else {
                    TaskState::Suspended
                },
                ..existing.data.clone()
            };
            metastore
                .update_task(&table_ident(&ident), updated)
                .await
                .map_err(|e| automation_error("alter task", e))?;
            crate::clone_ddl::status_row()
        }
        AutomationStatement::DropTask { name } => {
            let ident = resolve(session, name).map_err(|e| automation_error("drop task", e))?;
            metastore
                .delete_task(&table_ident(&ident))
                .await
                .map_err(|e| automation_error("drop task", e))?;
            crate::clone_ddl::status_row()
        }
        AutomationStatement::ShowTasks => {
            let tasks = metastore
                .list_tasks(&current_schema())
                .await
                .map_err(|e| automation_error("show tasks", e))?;
            let mut created = Vec::new();
            let mut names = Vec::new();
            let mut dbs = Vec::new();
            let mut schemas = Vec::new();
            let mut states = Vec::new();
            let mut schedules = Vec::new();
            let mut definitions = Vec::new();
            for t in tasks {
                created.push(t.created_at.to_string());
                names.push(t.ident.table.clone());
                dbs.push(t.ident.database.clone());
                schemas.push(t.ident.schema.clone());
                states.push(match t.state {
                    TaskState::Started => "started".to_owned(),
                    TaskState::Suspended => "suspended".to_owned(),
                });
                schedules.push(t.schedule.as_ref().map(ToString::to_string).unwrap_or_default());
                definitions.push(t.definition.clone());
            }
            task_row_result(created, names, dbs, schemas, states, schedules, definitions)
        }
        AutomationStatement::ExecuteTask { name } => {
            let ident = resolve(session, name).map_err(|e| automation_error("execute task", e))?;
            let existing = metastore
                .get_task(&table_ident(&ident))
                .await
                .map_err(|e| automation_error("execute task", e))?
                .ok_or_else(|| {
                    automation_error(
                        "execute task",
                        format!("task '{}' does not exist", display(&ident)),
                    )
                })?;
            // v1 deviation: EXECUTE TASK runs in the caller's session
            // context (scheduled runs use the task's warehouse instead,
            // §18.1-4).
            let definition = existing.data.definition.clone();
            let run = UserQuery::new(
                Arc::clone(session),
                definition,
                QueryContext::default(),
            )
            .execute()
            .await
            .map_err(|e| automation_error("execute task", e))?;
            let updated = Task {
                last_run_state: Some(TaskRunState::Succeeded),
                ..existing.data.clone()
            };
            metastore
                .update_task(&table_ident(&ident), updated)
                .await
                .map_err(|e| automation_error("execute task", e))?;
            Ok(run)
        }
        AutomationStatement::CreateDynamicTable {
            name,
            warehouse,
            target_lag_seconds,
            definition,
            or_replace,
        } => {
            let ident =
                resolve(session, name).map_err(|e| automation_error("create dynamic table", e))?;
            let target = display(&ident);
            // Create the target table via the normal CTAS path — this
            // reuses registration, schema inference, and the initial load.
            let replace = if *or_replace { "OR REPLACE " } else { "" };
            let ctas = format!("CREATE {replace}TABLE {target} AS {definition}");
            UserQuery::new(Arc::clone(session), ctas, QueryContext::default())
                .execute()
                .await
                .map_err(|e| automation_error("create dynamic table", e))?;
            let record = DynamicTable {
                ident: table_ident(&ident),
                definition: definition.clone(),
                warehouse: warehouse.as_ref().map(|w| norm(w)),
                target_lag_seconds: *target_lag_seconds,
                last_refresh_ms: None,
            };
            metastore
                .create_dynamic_table(&table_ident(&ident), record)
                .await
                .map_err(|e| automation_error("create dynamic table", e))?;
            crate::clone_ddl::status_row()
        }
        AutomationStatement::AlterDynamicTableRefresh { name } => {
            let ident = resolve(session, name)
                .map_err(|e| automation_error("refresh dynamic table", e))?;
            refresh_dynamic_table(session, &table_ident(&ident)).await?;
            crate::clone_ddl::status_row()
        }
        AutomationStatement::DropDynamicTable { name } => {
            let ident =
                resolve(session, name).map_err(|e| automation_error("drop dynamic table", e))?;
            let target = display(&ident);
            metastore
                .delete_dynamic_table(&table_ident(&ident))
                .await
                .map_err(|e| automation_error("drop dynamic table", e))?;
            UserQuery::new(
                Arc::clone(session),
                format!("DROP TABLE {target}"),
                QueryContext::default(),
            )
            .execute()
            .await
            .map_err(|e| automation_error("drop dynamic table", e))?;
            crate::clone_ddl::status_row()
        }
        AutomationStatement::ShowDynamicTables => {
            let dts = metastore
                .list_dynamic_tables(&current_schema())
                .await
                .map_err(|e| automation_error("show dynamic tables", e))?;
            let mut created = Vec::new();
            let mut names = Vec::new();
            let mut dbs = Vec::new();
            let mut schemas = Vec::new();
            let mut lags = Vec::new();
            let mut warehouses = Vec::new();
            let mut definitions = Vec::new();
            for dt in dts {
                created.push(dt.created_at.to_string());
                names.push(dt.ident.table.clone());
                dbs.push(dt.ident.database.clone());
                schemas.push(dt.ident.schema.clone());
                lags.push(dt.target_lag_seconds.map_or_else(String::new, |s| format!("{s}s")));
                warehouses.push(dt.warehouse.clone().unwrap_or_default());
                definitions.push(dt.definition.clone());
            }
            dynamic_table_row_result(created, names, dbs, schemas, lags, warehouses, definitions)
        }
    }
}

/// Full-overwrite refresh of a dynamic table's target from its definition
/// (design §18.1-5): one INSERT OVERWRITE commit = one new snapshot.
pub(crate) async fn refresh_dynamic_table(
    session: &Arc<UserSession>,
    ident: &TableIdent,
) -> Result<QueryResult> {
    let metastore = session.metastore.clone();
    let record = metastore
        .get_dynamic_table(ident)
        .await
        .map_err(|e| automation_error("refresh", e))?
        .ok_or_else(|| {
            automation_error(
                "refresh",
                format!("dynamic table '{}' does not exist", ident.to_string()),
            )
        })?;
    let target = ident.to_string();
    let overwrite = format!("INSERT OVERWRITE INTO {target} {}", record.definition);
    let result = UserQuery::new(
        Arc::clone(session),
        overwrite,
        QueryContext::default(),
    )
    .execute()
    .await
    .map_err(|e| automation_error("refresh", e))?;
    let updated = DynamicTable {
        last_refresh_ms: Some(chrono::Utc::now().timestamp_millis()),
        ..record.data.clone()
    };
    metastore
        .update_dynamic_table(ident, updated)
        .await
        .map_err(|e| automation_error("refresh", e))?;
    Ok(result)
}

fn session_current_database(session: &UserSession) -> String {
    session
        .get_session_variable("database")
        .unwrap_or_else(|| "embucket".to_owned())
}

fn session_current_schema(session: &UserSession) -> String {
    session
        .get_session_variable("schema")
        .unwrap_or_else(|| "public".to_owned())
}

fn validate_cron(expr: &str) -> std::result::Result<(), String> {
    // 5-field Snowflake cron → 6-field cron-crate expression (seconds=0).
    let six_field = format!("0 {expr}");
    cron::Schedule::from_str(&six_field)
        .map(|_| ())
        .map_err(|e| format!("invalid cron expression '{expr}': {e}"))
}

fn stream_row_result(
    created: Vec<String>,
    names: Vec<String>,
    dbs: Vec<String>,
    schemas: Vec<String>,
    tables: Vec<String>,
    modes: Vec<String>,
) -> Result<QueryResult> {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("created_on", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("database_name", DataType::Utf8, false),
        Field::new("schema_name", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("mode", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(created)),
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(dbs)),
            Arc::new(StringArray::from(schemas)),
            Arc::new(StringArray::from(tables)),
            Arc::new(StringArray::from(modes)),
        ],
    )
    .context(ex_error::ArrowSnafu)?;
    Ok(QueryResult::new(vec![batch], schema))
}

fn task_row_result(
    created: Vec<String>,
    names: Vec<String>,
    dbs: Vec<String>,
    schemas: Vec<String>,
    states: Vec<String>,
    schedules: Vec<String>,
    definitions: Vec<String>,
) -> Result<QueryResult> {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("created_on", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("database_name", DataType::Utf8, false),
        Field::new("schema_name", DataType::Utf8, false),
        Field::new("state", DataType::Utf8, false),
        Field::new("schedule", DataType::Utf8, true),
        Field::new("definition", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(created)),
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(dbs)),
            Arc::new(StringArray::from(schemas)),
            Arc::new(StringArray::from(states)),
            Arc::new(StringArray::from(schedules)),
            Arc::new(StringArray::from(definitions)),
        ],
    )
    .context(ex_error::ArrowSnafu)?;
    Ok(QueryResult::new(vec![batch], schema))
}

#[allow(clippy::too_many_arguments)]
fn dynamic_table_row_result(
    created: Vec<String>,
    names: Vec<String>,
    dbs: Vec<String>,
    schemas: Vec<String>,
    lags: Vec<String>,
    warehouses: Vec<String>,
    definitions: Vec<String>,
) -> Result<QueryResult> {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("created_on", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("database_name", DataType::Utf8, false),
        Field::new("schema_name", DataType::Utf8, false),
        Field::new("target_lag", DataType::Utf8, true),
        Field::new("warehouse", DataType::Utf8, true),
        Field::new("definition", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(created)),
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(dbs)),
            Arc::new(StringArray::from(schemas)),
            Arc::new(StringArray::from(lags)),
            Arc::new(StringArray::from(warehouses)),
            Arc::new(StringArray::from(definitions)),
        ],
    )
    .context(ex_error::ArrowSnafu)?;
    Ok(QueryResult::new(vec![batch], schema))
}

/// Advance every stream consumed by an executed DML statement (design
/// §18.1-1): after a successful INSERT or CTAS that reads a stream, the
/// stream's offset moves to the source's current snapshot. Plain SELECTs
/// never advance. V1 walks FROM/JOIN table factors including derived
/// tables; streams referenced only inside scalar subqueries in projections
/// or predicates are re-read (never lost) when missed.
pub async fn advance_consumed_streams(query: &UserQuery) -> Result<()> {
    let Some(statement) = query.raw_statement.clone() else {
        return Ok(());
    };
    let DFStatement::Statement(s) = statement else {
        return Ok(());
    };
    let names: Vec<ObjectName> = match *s {
        SqlStatement::Insert(insert) => match insert.source {
            Some(source) => collect_query_table_names(&source),
            None => vec![],
        },
        SqlStatement::CreateTable(create_table) => match create_table.query {
            Some(query) => collect_query_table_names(&query),
            None => vec![],
        },
        _ => vec![],
    };
    let session = query.session.clone();
    let metastore = session.metastore.clone();
    for name in names {
        let Ok(resolved) = query.resolve_table_object_name(name.0.clone()) else {
            continue;
        };
        let ident = table_ident(&resolved);
        let Ok(Some(stream)) = metastore.get_stream(&ident).await else {
            continue;
        };
        let Ok(Some(source_table)) = metastore.get_table(&stream.source).await else {
            continue;
        };
        let Ok(offset) = current_snapshot_id(&source_table) else {
            continue;
        };
        if offset != stream.offset_snapshot {
            let updated = Stream {
                offset_snapshot: offset,
                ..stream.data.clone()
            };
            metastore
                .update_stream(&stream.ident, updated)
                .await
                .map_err(|e| automation_error("consume stream", e))?;
        }
    }
    Ok(())
}

/// Top-level table factors of a query: FROM/JOIN targets, walked through
/// set operations, derived tables, and nested joins.
fn collect_query_table_names(query: &SqlQuery) -> Vec<ObjectName> {
    let mut names = Vec::new();
    collect_set_expr_tables(&query.body, &mut names);
    names
}

fn collect_set_expr_tables(set_expr: &SetExpr, names: &mut Vec<ObjectName>) {
    match set_expr {
        SetExpr::Select(select) => {
            for table_with_joins in &select.from {
                collect_table_with_joins(table_with_joins, names);
            }
        }
        SetExpr::Query(query) => names.extend(collect_query_table_names(query)),
        SetExpr::SetOperation { left, right, .. } => {
            collect_set_expr_tables(left, names);
            collect_set_expr_tables(right, names);
        }
        _ => {}
    }
}

fn collect_table_with_joins(table: &datafusion::sql::sqlparser::ast::TableWithJoins, names: &mut Vec<ObjectName>) {
    collect_table_factor(&table.relation, names);
    for join in &table.joins {
        collect_table_factor(&join.relation, names);
    }
}

fn collect_table_factor(factor: &TableFactor, names: &mut Vec<ObjectName>) {
    match factor {
        TableFactor::Table { name, .. } => names.push(name.clone()),
        TableFactor::Derived { subquery, .. } => {
            names.extend(collect_query_table_names(subquery));
        }
        _ => {}
    }
}
#[cfg(test)]
mod parse_tests {
    use super::{
        parse, AutomationStatement, TaskRunState, TaskSchedule, TaskState,
    };

    fn parsed(sql: &str) -> AutomationStatement {
        parse(sql).unwrap_or_else(|| panic!("expected automation statement: {sql}"))
    }

    #[test]
    fn parses_create_stream() {
        assert_eq!(
            parsed("CREATE STREAM st ON TABLE d.s.src"),
            AutomationStatement::CreateStream {
                name: "st".into(),
                source: "d.s.src".into(),
                append_only: false,
                comment: None,
                or_replace: false,
                if_not_exists: false,
            }
        );
        assert_eq!(
            parsed(
                "CREATE OR REPLACE STREAM IF NOT EXISTS \"St\" ON TABLE src APPEND_ONLY = TRUE COMMENT = 'feed'"
            ),
            AutomationStatement::CreateStream {
                name: "\"St\"".into(),
                source: "src".into(),
                append_only: true,
                comment: Some("feed".into()),
                or_replace: true,
                if_not_exists: true,
            }
        );
        // quoted source names with dots stay one token
        assert_eq!(
            parsed("CREATE STREAM s2 ON TABLE \"d.s.src\""),
            AutomationStatement::CreateStream {
                name: "s2".into(),
                source: "\"d.s.src\"".into(),
                append_only: false,
                comment: None,
                or_replace: false,
                if_not_exists: false,
            }
        );
    }

    #[test]
    fn parses_create_task() {
        assert_eq!(
            parsed("CREATE TASK tk WAREHOUSE wh SCHEDULE '5 MINUTE' AS INSERT INTO d.s.t SELECT * FROM d.s.src"),
            AutomationStatement::CreateTask {
                name: "tk".into(),
                warehouse: Some("wh".into()),
                schedule: Some(TaskSchedule::Minutes(5)),
                predecessor: None,
                definition: "INSERT INTO d.s.t SELECT * FROM d.s.src".into(),
                or_replace: false,
                if_not_exists: false,
            }
        );
        assert_eq!(
            parsed("CREATE TASK tk2 SCHEDULE = 'USING CRON 0 9 * * * UTC' AFTER tk AS SELECT 1"),
            AutomationStatement::CreateTask {
                name: "tk2".into(),
                warehouse: None,
                schedule: Some(TaskSchedule::Cron("0 9 * * * UTC".into())),
                predecessor: Some("tk".into()),
                definition: "SELECT 1".into(),
                or_replace: false,
                if_not_exists: false,
            }
        );
        // AS inside the body is not the statement AS
        assert_eq!(
            parsed("CREATE TASK tk3 AS SELECT CAST('1' AS INT)"),
            AutomationStatement::CreateTask {
                name: "tk3".into(),
                warehouse: None,
                schedule: None,
                predecessor: None,
                definition: "SELECT CAST('1' AS INT)".into(),
                or_replace: false,
                if_not_exists: false,
            }
        );
    }

    #[test]
    fn parses_create_dynamic_table() {
        assert_eq!(
            parsed("CREATE DYNAMIC TABLE dt TARGET_LAG = '5 minutes' WAREHOUSE wh AS SELECT * FROM src"),
            AutomationStatement::CreateDynamicTable {
                name: "dt".into(),
                warehouse: Some("wh".into()),
                target_lag_seconds: Some(300),
                definition: "SELECT * FROM src".into(),
                or_replace: false,
            }
        );
        assert_eq!(
            parsed("CREATE OR REPLACE DYNAMIC TABLE dt2 AS SELECT 1"),
            AutomationStatement::CreateDynamicTable {
                name: "dt2".into(),
                warehouse: None,
                target_lag_seconds: None,
                definition: "SELECT 1".into(),
                or_replace: true,
            }
        );
    }

    #[test]
    fn parses_other_statements() {
        assert_eq!(
            parsed("DROP STREAM st;"),
            AutomationStatement::DropStream { name: "st".into() }
        );
        assert_eq!(
            parsed("DROP TASK tk"),
            AutomationStatement::DropTask { name: "tk".into() }
        );
        assert_eq!(
            parsed("DROP DYNAMIC TABLE dt"),
            AutomationStatement::DropDynamicTable { name: "dt".into() }
        );
        assert_eq!(parsed("SHOW STREAMS"), AutomationStatement::ShowStreams);
        assert_eq!(parsed("SHOW TASKS"), AutomationStatement::ShowTasks);
        assert_eq!(parsed("SHOW DYNAMIC TABLES"), AutomationStatement::ShowDynamicTables);
        assert_eq!(
            parsed("DESCRIBE STREAM st"),
            AutomationStatement::DescribeStream { name: "st".into() }
        );
        assert_eq!(
            parsed("ALTER TASK tk RESUME"),
            AutomationStatement::AlterTask { name: "tk".into(), resume: true }
        );
        assert_eq!(
            parsed("ALTER TASK tk SUSPEND"),
            AutomationStatement::AlterTask { name: "tk".into(), resume: false }
        );
        assert_eq!(
            parsed("EXECUTE TASK tk"),
            AutomationStatement::ExecuteTask { name: "tk".into() }
        );
        assert_eq!(
            parsed("ALTER DYNAMIC TABLE dt REFRESH"),
            AutomationStatement::AlterDynamicTableRefresh { name: "dt".into() }
        );
    }

    #[test]
    fn non_automation_statements_fall_through() {
        assert!(parse("CREATE TABLE t (a int)").is_none());
        assert!(parse("CREATE WAREHOUSE wh").is_none());
        assert!(parse("CREATE SCHEMA s").is_none());
        assert!(parse("DROP TABLE t").is_none());
        assert!(parse("CREATE STREAM st").is_none()); // missing ON TABLE
        assert!(parse("CREATE TASK tk").is_none()); // missing AS body
        assert!(parse("SELECT * FROM t").is_none());
        assert!(parse("CREATE TASK tk WHEN TRUE AS SELECT 1").is_none()); // WHEN unsupported
        assert!(parse("CREATE TASK tk SCHEDULE 'N MINUTE' AS SELECT 1").is_none());
        assert!(parse("CREATE DYNAMIC TABLE dt TARGET_LAG = 'downstream' AS SELECT 1").is_none());
    }
}
