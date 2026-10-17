//! M14.2 — Iceberg maintenance DDL (design §20.1-2).
//!
//! Intercepted pre-parse like the other Avalanche statement families: the
//! sqlparser fork has no grammar for these (and `CLUSTER BY` parses into an
//! operation the engine otherwise rejects). Three statements:
//!
//! - `ALTER TABLE <t> COMPACT` — rewrites every data file of the current
//!   snapshot into writer-sized files via the same one-snapshot
//!   `INSERT OVERWRITE` path DELETE/UPDATE use (binpack small-file merge).
//! - `ALTER TABLE <t> CLUSTER BY (<exprs>)` — sorted rewrite (our clustering
//!   key): `INSERT OVERWRITE INTO t SELECT * FROM t ORDER BY <exprs>`, which
//!   both re-clusters the data and compacts.
//! - `ALTER TABLE <t> EXPIRE SNAPSHOTS [RETAIN [LAST] <n> SNAPSHOTS]
//!   [OLDER THAN '<ts>'] [DRY RUN]` — runs the fork's `ExpireSnapshots`
//!   transaction (never the current snapshot or refs; epoch-ms threshold).
//!
//! COMPACT/CLUSTER BY run as nested queries, so metering accrues and RBAC is
//! enforced at the catalog boundary exactly like DML. EXPIRE mutates
//! metadata only (no metering event).
use crate::automation_ddl::{automation_error, run_nested};
use crate::error::{self as ex_error, Error, Result};
use crate::models::QueryResult;
use crate::session::UserSession;
use crate::utils::NormalizedIdent;
use catalog::table::CachingTable;
use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion_iceberg::DataFusionTable;
use futures::{StreamExt, TryStreamExt};
use iceberg_rust::catalog::tabular::Tabular;
use iceberg_rust::table::Table;
use iceberg_rust_spec::spec::manifest::Content;
use std::collections::HashSet;
use std::sync::{Arc, RwLock};

const MAINTENANCE_CTX: &str = "MAINTENANCE";

/// Statement families handled here (case-insensitive).
#[derive(Debug)]
enum MaintenanceStatement {
    /// `ALTER TABLE <t> COMPACT`
    Compact { table: String },
    /// `ALTER TABLE <t> CLUSTER BY (<exprs>)`
    ClusterBy { table: String, exprs: String },
    /// `ALTER TABLE <t> EXPIRE SNAPSHOTS ...`
    ExpireSnapshots {
        table: String,
        retain_last: Option<u64>,
        older_than_ms: Option<i64>,
        dry_run: bool,
    },
}

/// Pre-parse entry point: returns `None` when `raw_query` is not a
/// maintenance statement.
pub async fn try_execute(raw_query: &str, session: &Arc<UserSession>) -> Option<Result<QueryResult>> {
    let stmt = parse(&crate::sql_text::strip_comments(raw_query))?;
    Some(execute(&stmt, session).await)
}

fn maintenance_error(msg: impl Into<String>) -> Error {
    automation_error("maintenance DDL", msg.into())
}

/// Parses the raw text (case-insensitive, comments already stripped).
fn parse(sql: &str) -> Option<MaintenanceStatement> {
    let sql = sql.trim().trim_end_matches(';').trim();
    if !sql.to_ascii_uppercase().starts_with("ALTER TABLE") {
        return None;
    }
    // `ALTER TABLE <t> COMPACT`
    if let Some(rest) = sql.strip_prefix("ALTER TABLE").or_else(|| sql.strip_prefix("alter table")) {
        let rest = rest.trim();
        let upper = rest.to_ascii_uppercase();
        if let Some(exprs_pos) = upper.find(" CLUSTER BY") {
            let table = rest[..exprs_pos].trim();
            let after = &rest[exprs_pos + " CLUSTER BY".len()..];
            let exprs = after.trim().trim_start_matches('(').trim_end_matches(')');
            if table.is_empty() || exprs.is_empty() {
                return None;
            }
            return Some(MaintenanceStatement::ClusterBy {
                table: table.to_string(),
                exprs: exprs.trim().to_string(),
            });
        }
        if let Some(args_pos) = upper.find(" EXPIRE SNAPSHOTS") {
            let table = rest[..args_pos].trim();
            let args = &rest[args_pos + " EXPIRE SNAPSHOTS".len()..];
            if table.is_empty() {
                return None;
            }
            return parse_expire(table, args);
        }
        if let Some(without_compact) = upper.strip_suffix("COMPACT") {
            let table = rest[..without_compact.len()].trim();
            if !table.is_empty() {
                return Some(MaintenanceStatement::Compact { table: table.to_string() });
            }
        }
    }
    None
}

/// Parses `EXPIRE SNAPSHOTS` options: `[RETAIN [LAST] <n> SNAPSHOTS]
/// [OLDER THAN '<ts>'] [DRY RUN]`, in any order.
fn parse_expire(table: &str, args: &str) -> Option<MaintenanceStatement> {
    let upper = args.to_ascii_uppercase();
    let mut retain_last = None;
    let mut older_than_ms = None;
    let mut dry_run = false;

    if let Some(pos) = upper.find("DRY RUN") {
        dry_run = true;
        let _ = pos;
    }
    if let Some(pos) = upper.find("RETAIN") {
        // `RETAIN [LAST] <n> SNAPSHOTS`
        let after = &upper[pos + "RETAIN".len()..];
        let after = after.trim_start().strip_prefix("LAST").unwrap_or(after.trim_start());
        let n: u64 = after.trim_start().split_whitespace().next()?.parse().ok()?;
        retain_last = Some(n);
    }
    if let Some(pos) = upper.find("OLDER THAN") {
        let after = &args[pos + "OLDER THAN".len()..];
        let ts = after.trim().trim_start_matches('\'').split('\'').next()?.trim();
        older_than_ms = Some(parse_timestamp_ms(ts)?);
    }
    Some(MaintenanceStatement::ExpireSnapshots {
        table: table.to_string(),
        retain_last,
        older_than_ms,
        dry_run,
    })
}

/// Timestamp → epoch-ms; accepts `YYYY-MM-DD[ HH:MM[:SS]]` and RFC3339.
fn parse_timestamp_ms(ts: &str) -> Option<i64> {
    use chrono::TimeZone;
    let ts = ts.trim();
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"] {
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(ts, fmt) {
            return chrono::Utc
                .from_utc_datetime(&dt)
                .timestamp_millis()
                .into();
        }
    }
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(ts) {
        return dt.timestamp_millis().into();
    }
    if let Ok(d) = chrono::NaiveDate::parse_from_str(ts, "%Y-%m-%d") {
        return chrono::Utc
            .from_utc_datetime(&d.and_hms_opt(0, 0, 0)?)
            .timestamp_millis()
            .into();
    }
    None
}

async fn execute(stmt: &MaintenanceStatement, session: &Arc<UserSession>) -> Result<QueryResult> {
    match stmt {
        MaintenanceStatement::Compact { table } => compact(table, session).await,
        MaintenanceStatement::ClusterBy { table, exprs } => cluster_by(table, exprs, session).await,
        MaintenanceStatement::ExpireSnapshots {
            table,
            retain_last,
            older_than_ms,
            dry_run,
        } => expire_snapshots(table, *retain_last, *older_than_ms, *dry_run, session).await,
    }
}

/// Resolves the maintenance target and returns the Iceberg table plus the
/// shared `tabular` handle (refreshed in place by sink commits, so
/// before/after counts off the same instance are accurate).
async fn resolve_table(
    session: &Arc<UserSession>,
    name: &str,
) -> Result<(Table, Arc<RwLock<Tabular>>)> {
    let ident = resolve_table_name(session, name)?;
    let cached = session
        .ctx
        .table_provider(&ident)
        .await
        .map_err(|e| maintenance_error(e.to_string()))?;
    let provider = cached
        .downcast_ref::<CachingTable>()
        .ok_or_else(|| maintenance_error("target is not an Iceberg table"))?
        .table
        .clone();
    let dft = provider
        .downcast_ref::<DataFusionTable>()
        .ok_or_else(|| maintenance_error("target must be an Iceberg table"))?;
    let tabular = Arc::clone(&dft.tabular);
    let table = {
        let guard = tabular
            .read()
            .map_err(|_| maintenance_error("table lock poisoned"))?;
        match &*guard {
            Tabular::Table(table) => table.clone(),
            _ => return Err(maintenance_error("target must be a table")),
        }
    };
    Ok((table, tabular))
}

/// Resolves a possibly-unqualified name against the session's database/schema.
fn resolve_table_name(session: &Arc<UserSession>, name: &str) -> Result<NormalizedIdent> {
    let object = parse_object_name(name)?;
    let query = crate::query::UserQuery::new(Arc::clone(session), String::new(), Default::default());
    query.resolve_table_object_name(object)
}

/// Parse a dotted object name into `Vec<ObjectNamePart>` (dotted identifiers
/// only; quoted parts supported).
fn parse_object_name(
    name: &str,
) -> Result<Vec<datafusion::sql::sqlparser::ast::ObjectNamePart>> {
    use datafusion::sql::sqlparser::ast::{Ident, ObjectNamePart};
    name.split('.')
        .map(|part| {
            let part = part.trim();
            if part.is_empty() {
                return Err(maintenance_error(format!("invalid object name '{name}'")));
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

/// Counts live `Content::Data` manifest entries of the current snapshot
/// (status `Deleted` entries are excluded, matching the scan path's
/// `scan_manifest_status` predicate).
async fn count_data_files(table: &Table) -> Result<u64> {
    use iceberg_rust_spec::spec::manifest::Status;
    let manifests = table
        .manifests(None, None)
        .await
        .map_err(|e| maintenance_error(e.to_string()))?;
    let files = table
        .datafiles(&manifests, None, (None, None))
        .await
        .map_err(|e| maintenance_error(e.to_string()))?;
    let data = files
        .try_filter(|(_, entry)| {
            let is_live_data = matches!(entry.data_file().content(), Content::Data)
                && *entry.status() != Status::Deleted;
            std::future::ready(is_live_data)
        })
        .count()
        .await;
    Ok(u64::try_from(data).unwrap_or_default())
}

/// `ALTER TABLE <t> COMPACT` — one-snapshot full rewrite through the same
/// `INSERT OVERWRITE` machinery as predicated DELETE/UPDATE (M16.4), so the
/// writer's target file size governs the binpack merge and the commit is a
/// regular Iceberg snapshot (metering accrues; time travel history grows by
/// exactly one snapshot).
async fn compact(table: &str, session: &Arc<UserSession>) -> Result<QueryResult> {
    let ident = resolve_table_name(session, table)?;
    let qualified = String::from(&ident);
    let (iceberg, _tabular) = resolve_table(session, table).await?;
    let files_before = count_data_files(&iceberg).await?;
    let rows = run_nested(
        session,
        format!("INSERT OVERWRITE INTO {qualified} SELECT * FROM {qualified}"),
        Default::default(),
        MAINTENANCE_CTX,
    )
    .await?;
    let rows_rewritten = affected_rows(&rows);
    let (iceberg, _) = resolve_table(session, table).await?;
    let files_after = count_data_files(&iceberg).await?;
    compact_report(&qualified, files_before, files_after, rows_rewritten)
}

/// `ALTER TABLE <t> CLUSTER BY (<exprs>)` — sorted rewrite. The clustering
/// expressions are carried into an `ORDER BY` over the full-table scan, so
/// the rewrite lands in sorted files (better pruning for future scans).
async fn cluster_by(table: &str, exprs: &str, session: &Arc<UserSession>) -> Result<QueryResult> {
    let ident = resolve_table_name(session, table)?;
    let qualified = String::from(&ident);
    let (iceberg, _tabular) = resolve_table(session, table).await?;
    let files_before = count_data_files(&iceberg).await?;
    let rows = run_nested(
        session,
        format!("INSERT OVERWRITE INTO {qualified} SELECT * FROM {qualified} ORDER BY {exprs}"),
        Default::default(),
        MAINTENANCE_CTX,
    )
    .await?;
    let rows_rewritten = affected_rows(&rows);
    let (iceberg, _) = resolve_table(session, table).await?;
    let files_after = count_data_files(&iceberg).await?;
    compact_report(&qualified, files_before, files_after, rows_rewritten)
}

/// DML count of a nested result (single-column count shape).
fn affected_rows(result: &QueryResult) -> i64 {
    let Some(records) = result.records.first() else {
        return 0;
    };
    let Some(col) = records.columns().first() else {
        return 0;
    };
    if let Some(a) = col.as_any().downcast_ref::<datafusion::arrow::array::Int64Array>() {
        return a.value(0);
    }
    if let Some(a) = col.as_any().downcast_ref::<datafusion::arrow::array::UInt64Array>() {
        return i64::try_from(a.value(0)).unwrap_or_default();
    }
    0
}

/// `(table, files_before, files_after, rows_rewritten)` report.
fn compact_report(
    qualified: &str,
    files_before: u64,
    files_after: u64,
    rows_rewritten: i64,
) -> Result<QueryResult> {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("table", DataType::Utf8, false),
        Field::new("files_before", DataType::Int64, false),
        Field::new("files_after", DataType::Int64, false),
        Field::new("rows_rewritten", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec![qualified.to_string()])),
            Arc::new(Int64Array::from(vec![i64::try_from(files_before).unwrap_or_default()])),
            Arc::new(Int64Array::from(vec![i64::try_from(files_after).unwrap_or_default()])),
            Arc::new(Int64Array::from(vec![rows_rewritten])),
        ],
    )
    .map_err(|e| maintenance_error(e.to_string()))?;
    Ok(QueryResult::new(vec![batch], schema))
}

/// Number of snapshots the fork's retention rules would expire.
fn would_expire(
    metadata: &iceberg_rust_spec::spec::table_metadata::TableMetadata,
    retain_last: Option<u64>,
    older_than_ms: Option<i64>,
) -> usize {
    let mut snapshots: Vec<_> = metadata.snapshots.values().collect();
    snapshots.sort_by(|a, b| b.timestamp_ms().cmp(a.timestamp_ms()));
    let current = metadata.current_snapshot_id;
    let refs: HashSet<i64> = metadata.refs.values().map(|r| r.snapshot_id).collect();
    let expired = snapshots
        .iter()
        .enumerate()
        .filter(|(index, snapshot)| {
            let id = *snapshot.snapshot_id();
            let mut retain = Some(id) == current || refs.contains(&id);
            if !retain {
                if let Some(n) = retain_last {
                    retain = *index < n as usize;
                }
            }
            if !retain {
                if let Some(threshold) = older_than_ms {
                    retain = *snapshot.timestamp_ms() >= threshold;
                }
            }
            !retain
        })
        .count();
    u64::try_from(expired).unwrap_or_default() as usize
}

/// `ALTER TABLE <t> EXPIRE SNAPSHOTS ...` — runs the fork's
/// `ExpireSnapshots` transaction (metadata-only commit). The current
/// snapshot and branch/tag refs are always retained; `DRY RUN` reports the
/// would-expire count without committing.
async fn expire_snapshots(
    table: &str,
    retain_last: Option<u64>,
    older_than_ms: Option<i64>,
    dry_run: bool,
    session: &Arc<UserSession>,
) -> Result<QueryResult> {
    if retain_last.is_none() && older_than_ms.is_none() {
        return Err(maintenance_error(
            "EXPIRE SNAPSHOTS requires RETAIN <n> SNAPSHOTS and/or OLDER THAN '<timestamp>'",
        ));
    }
    let (mut iceberg, tabular) = resolve_table(session, table).await?;
    if dry_run {
        let count = would_expire(iceberg.metadata(), retain_last, older_than_ms);
        return expire_report(count, true);
    }
    let before = iceberg.metadata().snapshots.len();
    {
        let transaction = iceberg
            .new_transaction(None)
            .expire_snapshots(older_than_ms, retain_last.map(|n| n as usize), false, true, false);
        transaction
            .commit()
            .await
            .map_err(|e| maintenance_error(e.to_string()))?;
    }
    let after = iceberg.metadata().snapshots.len();
    {
        let mut guard = tabular
            .write()
            .map_err(|_| maintenance_error("table lock poisoned"))?;
        *guard = Tabular::Table(iceberg.clone());
    }
    let _ = before;
    expire_report(before.saturating_sub(after), false)
}

/// Single `expired` count (DML-affected-style wire shape).
fn expire_report(expired: usize, dry_run: bool) -> Result<QueryResult> {
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "snapshots_expired",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![
            i64::try_from(expired).unwrap_or_default(),
        ]))],
    )
    .map_err(|e| maintenance_error(e.to_string()))?;
    let _ = dry_run;
    Ok(QueryResult::new(vec![batch], schema))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_compact() {
        let s = parse("ALTER TABLE db.s.t COMPACT").expect("compact");
        assert!(matches!(s, MaintenanceStatement::Compact { table } if table == "db.s.t"));
        assert!(parse("alter table t compact;").is_some());
        assert!(parse("ALTER TABLE t COMPACT EXTRA").is_none());
    }

    #[test]
    fn parses_cluster_by() {
        let s = parse("ALTER TABLE db.t CLUSTER BY (date_trunc('day', ts), region)")
            .expect("cluster by");
        match s {
            MaintenanceStatement::ClusterBy { table, exprs } => {
                assert_eq!(table, "db.t");
                assert_eq!(exprs, "date_trunc('day', ts), region");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn parses_expire_variants() {
        let s = parse("ALTER TABLE t EXPIRE SNAPSHOTS RETAIN LAST 5 SNAPSHOTS")
            .expect("retain");
        match s {
            MaintenanceStatement::ExpireSnapshots { retain_last, older_than_ms, dry_run, .. } => {
                assert_eq!(retain_last, Some(5));
                assert_eq!(older_than_ms, None);
                assert!(!dry_run);
            }
            _ => panic!("wrong variant"),
        }
        let s = parse("ALTER TABLE t EXPIRE SNAPSHOTS OLDER THAN '2026-01-01 00:00:00' DRY RUN")
            .expect("older than");
        match s {
            MaintenanceStatement::ExpireSnapshots { retain_last, older_than_ms, dry_run, .. } => {
                assert_eq!(retain_last, None);
                assert!(older_than_ms.is_some());
                assert!(dry_run);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn parses_timestamp_formats() {
        assert_eq!(parse_timestamp_ms("1970-01-01 00:00:00"), Some(0));
        assert_eq!(parse_timestamp_ms("1970-01-01"), Some(0));
        assert_eq!(
            parse_timestamp_ms("2026-01-01T00:00:00Z"),
            chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
                .unwrap()
                .timestamp_millis()
                .into()
        );
        assert_eq!(parse_timestamp_ms("not-a-date"), None);
    }

    #[test]
    fn non_maintenance_statements_are_ignored() {
        assert!(parse("SELECT 1").is_none());
        assert!(parse("ALTER TABLE t ADD COLUMN c INT").is_none());
        assert!(parse("ALTER SESSION SET x = 1").is_none());
    }
}