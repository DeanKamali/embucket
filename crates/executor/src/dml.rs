//! DELETE / UPDATE execution for base tables (design §22.7, M16.4).
//!
//! v1 strategy: rewrite both statements into an equivalent single-commit
//! `INSERT OVERWRITE` — the same file-granular copy-on-write path the
//! dynamic-table refresh uses (§22.6). Snowflake rewrites the affected files
//! too, so this is honest COW semantics, not a shortcut:
//!
//! - `DELETE FROM t [WHERE p]` → `INSERT OVERWRITE INTO t
//!   SELECT * FROM t [WHERE NOT (p)]` (no predicate → keep nothing).
//! - `UPDATE t SET c = v [, …] [WHERE p]` → `INSERT OVERWRITE INTO t
//!   SELECT * REPLACE ({CASE WHEN p THEN v ELSE c END AS c} [, …]) FROM t`.
//!
//! The affected-row count is computed with a `COUNT(*)` pre-scan and
//! reported in the same `count` column shape the INSERT path uses (M13).
//! Inside a transaction both are still rejected earlier by the txn
//! pre-parse (§22.5 v1 whitelist). Unsupported clauses (USING, RETURNING,
//! multi-table, …) are rejected explicitly so they never silently rewrite.
use crate::automation_ddl::run_nested;
use crate::error::Result;
use crate::models::{QueryContext, QueryResult};
use crate::query::UserQuery;
use datafusion::arrow::array::{Int64Array, RecordBatch, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use sqlparser::ast::{AssignmentTarget, Delete, Expr, FromTable, ObjectName, TableWithJoins, Update};
use std::sync::Arc;
use tracing_attributes::instrument;

/// Failure context for nested-query errors.
const DML_CTX: &str = "DML";

fn not_supported<T>(what: &str) -> Result<T> {
    crate::error::NotSupportedStatementSnafu { statement: what }.fail()
}

/// Extract the target table from the first FROM relation; reject joins.
/// Returns the bare table name (for the overwrite target) and the rendered
/// relation *including any alias* (for the scan FROM, so predicates that
/// use the alias qualifier — `s.region` — still plan).
fn single_table(twj: &TableWithJoins, what: &str) -> Result<(ObjectName, String)> {
    if !twj.joins.is_empty() {
        return not_supported(&format!("{what} with joins"));
    }
    match &twj.relation {
        sqlparser::ast::TableFactor::Table { name, .. } => {
            Ok((name.clone(), twj.relation.to_string()))
        }
        _ => not_supported(&format!("{what} from non-table relation")),
    }
}

/// Last component of an ObjectName as the bare column reference (strips any
/// table qualifier so it both names the projected column and reads the
/// source row's original value).
fn bare_name(name: &ObjectName) -> Result<String> {
    match name.0.last() {
        Some(sqlparser::ast::ObjectNamePart::Identifier(ident)) => Ok(ident.value.clone()),
        _ => not_supported("UPDATE with non-identifier column name"),
    }
}

/// `COUNT(*)` value from a nested count-scan result (DataFusion reports
/// Int64; be liberal).
fn first_count(qr: &QueryResult) -> Option<u64> {
    let col = qr.records.first()?.column(0);
    if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        return Some(a.value(0).max(0) as u64);
    }
    col.as_any()
        .downcast_ref::<UInt64Array>()
        .map(|a| a.value(0))
}

/// Wire-shaped single-row result: the `count` column carries the affected
/// row total.
fn dml_count_result(rows: u64) -> QueryResult {
    let schema: Arc<ArrowSchema> = Arc::new(ArrowSchema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]));
    let batch =
        RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(UInt64Array::from(vec![rows]))])
            .expect("statically valid count batch");
    QueryResult::new(vec![batch], schema)
}

impl UserQuery {
    /// `DELETE FROM t [WHERE p]` — rewrite + one-snapshot overwrite commit.
    #[instrument(name = "UserQuery::delete_query", level = "trace", skip(self, d), err)]
    pub async fn delete_query(&self, d: Delete) -> Result<QueryResult> {
        if !d.tables.is_empty() {
            return not_supported("multi-table DELETE");
        }
        let from = match &d.from {
            FromTable::WithFromKeyword(v) | FromTable::WithoutKeyword(v) => v,
        };
        if from.len() > 1 || d.using.is_some() || d.returning.is_some() || d.output.is_some()
            || !d.order_by.is_empty() || d.limit.is_some()
        {
            return not_supported("DELETE clause (USING/RETURNING/OUTPUT/ORDER BY/LIMIT)");
        }
        let (table, scan_from) = match from.first() {
            Some(twj) => single_table(twj, "DELETE")?,
            None => return not_supported("DELETE without FROM"),
        };
        let table = table.to_string();
        let pred = d.selection.as_ref().map(Expr::to_string);
        let pred_sql = pred.clone().unwrap_or_else(|| "TRUE".to_string());

        // Affected rows = rows matching the predicate. The pre-scan also
        // validates the table resolves before the overwrite commits.
        let count_sql = format!("SELECT COUNT(*) FROM {scan_from} WHERE {pred_sql}");
        let counted = run_nested(&self.session, count_sql, QueryContext::default(), DML_CTX).await?;
        let affected = first_count(&counted).unwrap_or(0);

        let keep = match &pred {
            Some(p) => format!("SELECT * FROM {scan_from} WHERE NOT ({p})"),
            None => format!("SELECT * FROM {scan_from} WHERE FALSE"),
        };
        let overwrite = format!("INSERT OVERWRITE INTO {table} {keep}");
        let _ = run_nested(&self.session, overwrite, QueryContext::default(), DML_CTX).await?;

        tracing::info!(table = %table, affected, "DELETE rewritten as one-snapshot overwrite");
        Ok(dml_count_result(affected))
    }

    /// `UPDATE t SET c = v [, …] [WHERE p]` — rewrite + one-snapshot
    /// overwrite commit.
    #[instrument(name = "UserQuery::update_query", level = "trace", skip(self, u), err)]
    pub async fn update_query(&self, u: Update) -> Result<QueryResult> {
        if u.from.is_some() || u.returning.is_some() || u.output.is_some() || u.or.is_some()
            || !u.order_by.is_empty() || u.limit.is_some()
        {
            return not_supported("UPDATE clause (FROM/RETURNING/OUTPUT/ORDER BY/LIMIT)");
        }
        if u.assignments.is_empty() {
            return not_supported("UPDATE without assignments");
        }
        let (table, scan_from) = single_table(&u.table, "UPDATE")?;
        let table = table.to_string();
        let pred = u.selection.as_ref().map(Expr::to_string);
        let pred_sql = pred.clone().unwrap_or_else(|| "TRUE".to_string());

        let count_sql = format!("SELECT COUNT(*) FROM {scan_from} WHERE {pred_sql}");
        let counted = run_nested(&self.session, count_sql, QueryContext::default(), DML_CTX).await?;
        let affected = first_count(&counted).unwrap_or(0);

        let mut replaces = Vec::with_capacity(u.assignments.len());
        for a in &u.assignments {
            let col = match &a.target {
                AssignmentTarget::ColumnName(name) => bare_name(name)?,
                AssignmentTarget::Tuple(_) => {
                    return not_supported("UPDATE with tuple assignment");
                }
            };
            let value = a.value.to_string();
            // The replacement expression is evaluated per source row, so
            // `ELSE col` keeps the row's original value for non-matching
            // rows (UPDATE preserves all rows; only values change).
            let case = match &pred {
                Some(p) => format!("CASE WHEN {p} THEN {value} ELSE {col} END"),
                None => value,
            };
            replaces.push(format!("{case} AS {col}"));
        }
        let overwrite = format!(
            "INSERT OVERWRITE INTO {table} SELECT * REPLACE ({}) FROM {scan_from}",
            replaces.join(", ")
        );
        let _ = run_nested(&self.session, overwrite, QueryContext::default(), DML_CTX).await?;

        tracing::info!(table = %table, affected, "UPDATE rewritten as one-snapshot overwrite");
        Ok(dml_count_result(affected))
    }
}