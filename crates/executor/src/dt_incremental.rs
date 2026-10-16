//! Incremental refresh for dynamic tables (M16.2, design §22.1-2).
//!
//! A dynamic table whose definition is a plain single-table `GROUP BY`
//! aggregate is created in *incremental* refresh mode: refreshes consume the
//! base table's change feed (file-granular changed rows via the M11 stream
//! machinery) instead of full-overwrite rescans.
//!
//! v1 honest scope (design §22.5 follow-ups):
//! - Affected groups are recomputed from the base restricted by a key
//!   predicate; unaffected groups are carried over from the target. The
//!   target write is ONE Iceberg overwrite commit (full file replace — no
//!   partial delete machinery in the fork yet).
//! - A refresh with no changed source rows commits nothing at all (no new
//!   snapshot), unlike the full-overwrite refresh which always advances.
//! - Only plain-column GROUP BY over a single base table is eligible;
//!   anything else (joins, set ops, DISTINCT/HAVING/QUALIFY/LIMIT/WINDOW,
//!   expression or quoted-cased keys) falls back to full refresh.
//! - Oversized changed-key sets (guard: >50k literals) fall back to full
//!   refresh.

use crate::automation_ddl::run_nested;
use crate::error::{Error, Result};
use crate::models::{QueryContext, QueryResult};
use crate::session::UserSession;
use catalog_metastore::models::{DynamicTable, Stream, TableIdent};
use catalog_metastore::Metastore;
use datafusion::arrow::array::{ArrayRef, RecordBatch, StringArray, UInt64Array};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use datafusion::execution::session_state::SessionState;
use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::sqlparser::ast::{
    BinaryOperator, Expr, Query, SetExpr, Statement as SqlStatement,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Guard: fall back to full refresh when the changed-key IN lists would
/// exceed this many SQL literals.
const MAX_KEY_LITERALS: usize = 50_000;

/// Shape of a definition eligible for incremental refresh.
#[derive(Debug, Clone)]
pub(crate) struct IncrementalShape {
    /// Base table name as written in the definition (resolvable against the
    /// creating session).
    pub base_table: String,
    /// GROUP BY key columns.
    pub keys: Vec<KeyColumn>,
}

/// One GROUP BY key column.
#[derive(Debug, Clone)]
pub(crate) struct KeyColumn {
    /// Column name as referenced in the base table (normalized).
    pub base_name: String,
    /// Column name in the target table (alias or same, normalized).
    pub target_name: String,
}

/// DataFusion normalizes unquoted identifiers to lowercase; quoted ones
/// keep their case. Incremental mode requires lowercase-safe keys — a
/// quoted key falls back to full refresh, so this always yields a
/// lowercase-safe name.
fn normalize_ident_value(value: &str, quote_style: Option<char>) -> Option<String> {
    if quote_style.is_some() {
        None
    } else {
        Some(value.to_ascii_lowercase())
    }
}

/// Analyze a dynamic-table definition for incremental eligibility: a single
/// plain table source, plain-column GROUP BY keys that appear in the
/// projection (optionally aliased), and none of the constructs v1 cannot
/// handle (CTEs, joins, set ops, DISTINCT/HAVING/QUALIFY/LIMIT/OFFSET/
/// ORDER BY/WINDOW, table functions, quoted keys).
pub(crate) fn analyze_definition(sql: &str, session: &UserSession) -> Option<IncrementalShape> {
    let state = session.ctx.state();
    let dialect = state.config().options().sql_parser.dialect;
    analyze_definition_with_state(sql, &state, &dialect)
}

fn analyze_definition_with_state(
    sql: &str,
    state: &SessionState,
    dialect: &datafusion::config::Dialect,
) -> Option<IncrementalShape> {
    let statement = state.sql_to_statement(sql, dialect).ok()?;
    let DFStatement::Statement(s) = statement else {
        return None;
    };
    let SqlStatement::Query(query) = *s else {
        return None;
    };
    eligible_query_shape(&query)
}

fn eligible_query_shape(query: &Query) -> Option<IncrementalShape> {
    if query.with.is_some()
        || query.order_by.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
    {
        return None;
    }
    let SetExpr::Select(select) = &*query.body else {
        return None;
    };
    if select.distinct.is_some()
        || select.having.is_some()
        || select.qualify.is_some()
        || select.into.is_some()
        || select.value_table_mode.is_some()
        || !select.optimizer_hints.is_empty()
        || !select.lateral_views.is_empty()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || !select.named_window.is_empty()
        || !select.connect_by.is_empty()
        || select.prewhere.is_some()
        || select.exclude.is_some()
        || select.top.is_some()
    {
        return None;
    }
    // Single plain table source, no joins, no table-function args.
    if select.from.len() != 1 {
        return None;
    }
    let from = &select.from[0];
    if !from.joins.is_empty() {
        return None;
    }
    let datafusion::sql::sqlparser::ast::TableFactor::Table {
        name,
        alias: _,
        args,
        with_hints,
        version,
        partitions,
        json_path,
        sample,
        index_hints,
        with_ordinality,
    } = &from.relation
    else {
        return None;
    };
    if args.is_some()
        || !with_hints.is_empty()
        || version.is_some()
        || !partitions.is_empty()
        || json_path.is_some()
        || sample.is_some()
        || !index_hints.is_empty()
        || *with_ordinality
    {
        return None;
    }
    // Plain-column GROUP BY keys (GROUP BY ALL is not eligible).
    let datafusion::sql::sqlparser::ast::GroupByExpr::Expressions(exprs, modifiers) =
        &select.group_by
    else {
        return None;
    };
    if !modifiers.is_empty() || exprs.is_empty() {
        return None;
    }
    let mut keys = Vec::with_capacity(exprs.len());
    for expr in exprs {
        let Expr::Identifier(ident) = expr else {
            return None; // expression or positional keys: v1 not eligible
        };
        let base_name = normalize_ident_value(&ident.value, ident.quote_style)?;
        // The key must appear in the projection as a plain column
        // (optionally aliased) so the target-side predicate can name it.
        let mut target_name = None;
        for item in &select.projection {
            let (item_expr, item_alias) = match item {
                datafusion::sql::sqlparser::ast::SelectItem::UnnamedExpr(e) => (e, None),
                datafusion::sql::sqlparser::ast::SelectItem::ExprWithAlias { expr, alias } => {
                    (expr, Some(alias))
                }
                _ => continue,
            };
            let Expr::Identifier(item_ident) = item_expr else {
                continue;
            };
            let Some(item_name) = normalize_ident_value(&item_ident.value, item_ident.quote_style)
            else {
                continue;
            };
            if item_name == base_name {
                let alias_name = item_alias
                    .and_then(|a| normalize_ident_value(&a.value, a.quote_style));
                target_name = Some(alias_name.unwrap_or(item_name));
                break;
            }
        }
        keys.push(KeyColumn {
            base_name,
            target_name: target_name?,
        });
    }
    Some(IncrementalShape {
        base_table: name.to_string(),
        keys,
    })
}

/// Incremental refresh of one dynamic table (design §22.1-2):
/// consume the base's change feed via the hidden stream, recompute the
/// affected groups from the base, carry over unaffected target groups, and
/// commit once. Falls back to the M11 full-overwrite refresh whenever v1
/// cannot handle the delta safely (stale stream offset, oversized key set,
/// unusable key types).
pub(crate) async fn refresh_incremental(
    session: &Arc<UserSession>,
    dt: &DynamicTable,
    metastore: &Arc<dyn Metastore>,
) -> Result<QueryResult> {
    let target = dt.ident.to_string();

    let Some(shape) = analyze_definition(&dt.definition, session) else {
        tracing::info!(
            dynamic_table = %target,
            "incremental refresh falling back to full refresh: definition no longer eligible"
        );
        let result = full_overwrite(session, target.clone(), &dt.definition).await?;
        return finish_fallback(dt, metastore, result).await;
    };

    let base_ident = dt.base_table.clone().ok_or_else(|| {
        automation_err("incremental dynamic table has no base table recorded")
    })?;
    let stream_ident = dt
        .stream_ident
        .clone()
        .ok_or_else(|| automation_err("incremental dynamic table has no change stream"))?;

    let source = metastore
        .get_table(&base_ident)
        .await
        .map_err(|e| automation_err(format!("get base table: {e}")))?
        .ok_or_else(|| {
            automation_err(format!(
                "base table '{}' no longer exists",
                base_ident.to_string()
            ))
        })?;
    let end = crate::automation_ddl::current_snapshot_id(&source)?;

    let stream = metastore
        .get_stream(&stream_ident)
        .await
        .map_err(|e| automation_err(format!("get change stream: {e}")))?
        .ok_or_else(|| automation_err("change stream no longer exists"))?;

    // Stale offset or broken ancestry (source snapshots expired, the stream
    // was created before the base's first commit, or a write path committed
    // a snapshot that forked the lineage): full refresh and re-anchor.
    let stale = match (stream.data.offset_snapshot, end) {
        (Some(offset), Some(end)) => {
            !source.data.metadata.snapshots.contains_key(&offset)
                || !offset_is_ancestor(&source.data.metadata, offset, end)
        }
        (Some(offset), None) => !source.data.metadata.snapshots.contains_key(&offset),
        (None, Some(_)) => true,
        (None, None) => false,
    };
    if stale {
        tracing::info!(
            dynamic_table = %target,
            "incremental refresh falling back to full refresh: change-stream offset expired"
        );
        let result = full_overwrite(session, target.clone(), &dt.definition).await?;
        finish_fallback(dt, metastore, result).await
    } else {
        refresh_from_changes(session, dt, metastore, &shape, &stream, end).await
    }
}

/// The change-feed-driven path: extract changed keys, recompute affected
/// groups, commit once, then advance the stream offset.
async fn refresh_from_changes(
    session: &Arc<UserSession>,
    dt: &DynamicTable,
    metastore: &Arc<dyn Metastore>,
    shape: &IncrementalShape,
    stream: &catalog_metastore::models::Stream,
    end: Option<i64>,
) -> Result<QueryResult> {
    let target = dt.ident.to_string();
    let stream_ident = dt
        .stream_ident
        .clone()
        .ok_or_else(|| automation_err("incremental dynamic table has no change stream"))?;

    // 1. Changed keys from the base's change feed (changed-file scan only).
    let key_cols: Vec<String> = shape
        .keys
        .iter()
        .map(|k| k.base_name.clone())
        .collect();
    let key_list = key_cols.join(", ");
    let stream_name = stream_ident.to_string();
    let changed_keys_sql = format!("SELECT DISTINCT {key_list} FROM {stream_name}");
    let changed = run_nested(
        session,
        changed_keys_sql,
        QueryContext::default(),
        "incremental refresh changed keys",
    )
    .await?;

    let changed_rows: usize = changed.records.iter().map(RecordBatch::num_rows).sum();
    if changed_rows == 0 {
        // Nothing changed in the base: commit nothing (no new snapshot).
        tracing::info!(
            dynamic_table = %target,
            "incremental refresh: no changed source rows, target not rewritten"
        );
        reset_stream_offset(metastore, &stream_ident, stream, end).await?;
        return Ok(count_result(0));
    }

    let literals = extract_key_literals(&changed, &key_cols).map_err(automation_err)?;
    let total_literals: usize = literals.values().map(Vec::len).sum();
    if total_literals > MAX_KEY_LITERALS {
        let overwrite = format!("INSERT OVERWRITE INTO {target} {}", dt.definition);
        let result = run_nested(session, overwrite, QueryContext::default(), "refresh").await?;
        reset_stream_offset(metastore, &stream_ident, stream, end).await?;
        return Ok(result);
    }

    // 2. One commit: unchanged target groups ∪ recomputed affected groups.
    //
    // P_i = (k_i IN (vals) OR k_i IS NULL) — affected rows satisfy every
    // P_i; unaffected rows fail at least one. The IS NULL clauses keep
    // NULL-key groups correct (over-selecting them is safe: recomputation
    // yields the current truth for any over-selected group).
    let key_preds_base: Vec<String> = shape
        .keys
        .iter()
        .map(|k| key_predicate_sql(&k.base_name, literals.get(&k.base_name)))
        .collect();
    let key_preds_target: Vec<String> = shape
        .keys
        .iter()
        .map(|k| key_predicate_sql(&k.target_name, literals.get(&k.base_name)))
        .collect();
    let conj_base = join_and(&key_preds_base);
    let conj_target = join_and(&key_preds_target);

    let modified_definition = inject_where(session, &dt.definition, &conj_base)?;

    let merge_sql = format!(
        "INSERT OVERWRITE INTO {target} SELECT * FROM (SELECT * FROM {target} WHERE NOT ({conj_target}) UNION ALL {modified_definition}) AS __mv_delta"
    );
    let result =
        run_nested(session, merge_sql, QueryContext::default(), "incremental refresh").await?;

    // 3. Consume: advance the hidden stream to the source's current snapshot.
    reset_stream_offset(metastore, &stream_ident, stream, end).await?;

    tracing::info!(
        dynamic_table = %target,
        changed_source_rows = changed_rows,
        "incremental refresh committed from base change feed"
    );
    Ok(result)
}

async fn finish_fallback(
    dt: &DynamicTable,
    metastore: &Arc<dyn Metastore>,
    result: QueryResult,
) -> Result<QueryResult> {
    // Re-anchor the hidden stream (if any) so later refreshes are
    // incremental again.
    if let (Some(stream_ident), Some(source_ident)) =
        (dt.stream_ident.clone(), dt.base_table.clone())
        && let Ok(Some(source)) = metastore.get_table(&source_ident).await
        && let Ok(end) = crate::automation_ddl::current_snapshot_id(&source)
        && let Ok(Some(stream)) = metastore.get_stream(&stream_ident).await
    {
        reset_stream_offset(metastore, &stream_ident, &stream.data, end).await?;
    }
    Ok(result)
}

async fn full_overwrite(
    session: &Arc<UserSession>,
    target: String,
    definition: &str,
) -> Result<QueryResult> {
    let overwrite = format!("INSERT OVERWRITE INTO {target} {definition}");
    run_nested(session, overwrite, QueryContext::default(), "refresh").await
}

async fn reset_stream_offset(
    metastore: &Arc<dyn Metastore>,
    stream_ident: &TableIdent,
    stream: &Stream,
    end: Option<i64>,
) -> Result<()> {
    if stream.offset_snapshot == end {
        return Ok(());
    }
    let updated = Stream {
        offset_snapshot: end,
        ..stream.clone()
    };
    metastore
        .update_stream(stream_ident, updated)
        .await
        .map_err(|e| automation_err(format!("update change stream: {e}")))?;
    Ok(())
}

/// `offset` is reachable from `end` by walking parent snapshot links.
fn offset_is_ancestor(
    metadata: &iceberg_rust::spec::table_metadata::TableMetadata,
    offset: i64,
    end: i64,
) -> bool {
    let mut current = end;
    for _ in 0..metadata.snapshots.len() + 1 {
        if current == offset {
            return true;
        }
        let Some(parent) = metadata
            .snapshots
            .get(&current)
            .and_then(|s| s.parent_snapshot_id().clone())
        else {
            return false;
        };
        current = parent;
    }
    false
}

/// `(P1) AND (P2) ...`
fn join_and(predicates: &[String]) -> String {
    predicates
        .iter()
        .map(|p| format!("({p})"))
        .collect::<Vec<_>>()
        .join(" AND ")
}

/// `k IN (vals) OR k IS NULL` (the IN part is omitted when `vals` is empty).
fn key_predicate_sql(key: &str, vals: Option<&Vec<String>>) -> String {
    let mut parts = Vec::new();
    if let Some(vals) = vals
        && !vals.is_empty()
    {
        parts.push(format!("{key} IN ({})", vals.join(", ")));
    }
    parts.push(format!("{key} IS NULL"));
    parts.join(" OR ")
}

/// Inject `WHERE ({predicate})` into the definition's SELECT (AND-combined
/// with an existing WHERE) and re-emit the SQL. The predicate references
/// base column names, so it must land before aggregation.
fn inject_where(session: &UserSession, definition: &str, predicate_sql: &str) -> Result<String> {
    let state = session.ctx.state();
    let dialect = state.config().options().sql_parser.dialect;
    let statement = state
        .sql_to_statement(definition, &dialect)
        .map_err(|e| automation_err(format!("parse definition: {e}")))?;
    let DFStatement::Statement(s) = statement else {
        return Err(automation_err("definition is not a SQL statement"));
    };
    let SqlStatement::Query(mut query) = *s else {
        return Err(automation_err("definition is not a SELECT query"));
    };
    let predicate_expr = parse_predicate_expr(&state, &dialect, predicate_sql)?;
    let SetExpr::Select(select) = &mut *query.body else {
        return Err(automation_err("definition is not a SELECT query"));
    };
    select.selection = Some(match select.selection.take() {
        Some(existing) => Expr::BinaryOp {
            left: Box::new(existing),
            op: BinaryOperator::And,
            right: Box::new(predicate_expr),
        },
        None => predicate_expr,
    });
    Ok(query.to_string())
}

fn parse_predicate_expr(
    state: &SessionState,
    dialect: &datafusion::config::Dialect,
    predicate_sql: &str,
) -> Result<Expr> {
    let sql = format!("SELECT 1 WHERE {predicate_sql}");
    let statement = state
        .sql_to_statement(&sql, dialect)
        .map_err(|e| automation_err(format!("parse refresh predicate: {e}")))?;
    let DFStatement::Statement(s) = statement else {
        return Err(automation_err("predicate is not a SQL statement"));
    };
    let SqlStatement::Query(query) = *s else {
        return Err(automation_err("predicate is not a SELECT query"));
    };
    let SetExpr::Select(select) = &*query.body else {
        return Err(automation_err("predicate is not a SELECT query"));
    };
    select
        .selection
        .clone()
        .ok_or_else(|| automation_err("predicate expression missing"))
}

/// Distinct SQL literals per key column from the changed-key result.
/// NULLs are skipped (the predicate carries an IS NULL clause).
fn extract_key_literals(
    changed: &QueryResult,
    key_cols: &[String],
) -> std::result::Result<HashMap<String, Vec<String>>, String> {
    if changed.schema.fields().len() != key_cols.len() {
        return Err(format!(
            "changed-key query returned {} columns, expected {}",
            changed.schema.fields().len(),
            key_cols.len()
        ));
    }
    let mut out = HashMap::new();
    for (i, key) in key_cols.iter().enumerate() {
        let data_type = changed.schema.field(i).data_type().clone();
        let mut vals: Vec<String> = Vec::new();
        for batch in &changed.records {
            if i >= batch.num_columns() {
                return Err("changed-key batch column mismatch".to_owned());
            }
            let column = batch.column(i);
            let as_string: ArrayRef = if data_type == DataType::Utf8 {
                Arc::clone(column)
            } else {
                cast(column, &DataType::Utf8)
                    .map_err(|e| format!("changed key type {data_type} unusable: {e}"))?
            };
            let arr = as_string
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "changed key cast produced non-string".to_owned())?;
            for v in arr.iter().flatten() {
                vals.push(sql_literal(&data_type, v).ok_or_else(|| {
                    format!("changed key type {data_type} has no SQL literal form")
                })?);
            }
        }
        let mut seen = HashSet::new();
        vals.retain(|v| seen.insert(v.clone()));
        out.insert(key.clone(), vals);
    }
    Ok(out)
}

fn sql_literal(data_type: &DataType, value: &str) -> Option<String> {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 => {
            Some(format!("'{}'", value.replace('\'', "''")))
        }
        DataType::Date32
        | DataType::Date64
        | DataType::Timestamp(_, _)
        | DataType::Time32(_)
        | DataType::Time64(_) => Some(format!("'{value}'")),
        DataType::Boolean => Some(value.to_ascii_lowercase()),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64 => Some(value.to_owned()),
        _ => None,
    }
}

/// INSERT-shaped no-op result: one `count` column with 0.
fn count_result(rows: u64) -> QueryResult {
    let schema: SchemaRef = Arc::new(ArrowSchema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]));
    let batch =
        RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(UInt64Array::from(vec![rows]))])
            .expect("statically valid count batch");
    QueryResult::new(vec![batch], schema)
}

fn automation_err(message: impl std::fmt::Display) -> Error {
    crate::automation_ddl::automation_error("incremental refresh", message)
}

#[cfg(test)]
mod tests {
    use super::analyze_definition_with_state;

    fn shape(sql: &str) -> Option<super::IncrementalShape> {
        let ctx = datafusion::prelude::SessionContext::new();
        let state = ctx.state();
        let dialect = state.config().options().sql_parser.dialect;
        analyze_definition_with_state(sql, &state, &dialect)
    }

    #[test]
    fn simple_group_by_is_incremental() {
        let s = shape("SELECT region, SUM(amount) AS total FROM sales GROUP BY region")
            .expect("eligible");
        assert_eq!(s.base_table, "sales");
        assert_eq!(s.keys.len(), 1);
        assert_eq!(s.keys[0].base_name, "region");
        assert_eq!(s.keys[0].target_name, "region");
    }

    #[test]
    fn aliased_key_maps_to_target_name() {
        let s = shape("SELECT region AS r, SUM(amount) FROM sales GROUP BY region")
            .expect("eligible");
        assert_eq!(s.keys[0].base_name, "region");
        assert_eq!(s.keys[0].target_name, "r");
    }

    #[test]
    fn multi_key_group_by_is_incremental() {
        let s = shape("SELECT region, product, COUNT(*) FROM sales GROUP BY region, product")
            .expect("eligible");
        assert_eq!(s.keys.len(), 2);
        assert_eq!(s.keys[1].target_name, "product");
    }

    #[test]
    fn join_is_not_incremental() {
        assert!(shape(
            "SELECT r.name, COUNT(*) FROM sales s JOIN regions r ON s.region = r.id GROUP BY r.name"
        )
        .is_none());
    }

    #[test]
    fn distinct_is_not_incremental() {
        assert!(shape("SELECT DISTINCT region, SUM(amount) FROM sales GROUP BY region").is_none());
    }

    #[test]
    fn having_is_not_incremental() {
        assert!(shape(
            "SELECT region, SUM(amount) FROM sales GROUP BY region HAVING SUM(amount) > 10"
        )
        .is_none());
    }

    #[test]
    fn limit_is_not_incremental() {
        assert!(shape("SELECT region, SUM(amount) FROM sales GROUP BY region LIMIT 10").is_none());
    }

    #[test]
    fn order_by_is_not_incremental() {
        assert!(shape(
            "SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY region"
        )
        .is_none());
    }

    #[test]
    fn expression_group_by_is_not_incremental() {
        assert!(shape("SELECT region, SUM(amount) FROM sales GROUP BY UPPER(region)").is_none());
    }

    #[test]
    fn quoted_key_is_not_incremental() {
        assert!(shape("SELECT \"Region\", SUM(amount) FROM sales GROUP BY \"Region\"").is_none());
    }

    #[test]
    fn key_missing_from_projection_is_not_incremental() {
        assert!(shape("SELECT region, SUM(amount) FROM sales GROUP BY region, product").is_none());
    }

    #[test]
    fn no_group_by_is_not_incremental() {
        assert!(shape("SELECT region, SUM(amount) FROM sales").is_none());
    }

    #[test]
    fn set_operation_is_not_incremental() {
        assert!(shape(
            "SELECT region, SUM(amount) FROM sales GROUP BY region UNION ALL SELECT region, 0 FROM sales GROUP BY region"
        )
        .is_none());
    }

    #[test]
    fn predicate_sql_shape() {
        let vals = vec!["'a'".to_owned(), "'b'".to_owned()];
        assert_eq!(
            super::key_predicate_sql("k", Some(&vals)),
            "k IN ('a', 'b') OR k IS NULL"
        );
        assert_eq!(super::key_predicate_sql("k", Some(&Vec::new())), "k IS NULL");
        assert_eq!(super::key_predicate_sql("k", None), "k IS NULL");
    }
}