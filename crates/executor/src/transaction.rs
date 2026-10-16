//! Session transactions (design §22.1-1, M16.1): `BEGIN`/`COMMIT`/`ROLLBACK`
//! with a session-scoped write buffer.
//!
//! v1 contract (honest): writes issued inside an explicit transaction are
//! buffered in the session and applied at `COMMIT` — all buffered appends
//! for one table replay through **one** `INSERT INTO … SELECT …` so the
//! table advances a **single** Iceberg snapshot, no matter how many
//! statements targeted it. Cross-table commits apply per table in order
//! (each table gets its own snapshot advance): cross-table atomicity is a
//! v2 goal (fork change-feed granularity).
//!
//! Semantics honored (Snowflake shape):
//! - `BEGIN` inside an active transaction errors.
//! - A failed statement inside a transaction **aborts** it: every later
//!   statement errors (`Transaction is aborted, statements rolled back`)
//!   until `COMMIT`/`ROLLBACK` terminates it (both close an aborted
//!   transaction as no-ops — nothing was committed anyway).
//! - DDL (and `TRUNCATE`) inside a transaction auto-commits the open
//!   transaction first (Snowflake behavior), then the statement runs.
//! - `DELETE`/`UPDATE`/`MERGE`/`COPY INTO` inside a transaction are rejected
//!   in v1 (buffering row-level predicates is v2 work).
//! - Reads inside a transaction see committed data only (no
//!   read-your-writes overlay in v1 — documented gap).
//! - Uncommitted buffers are lost on process restart — matching real
//!   uncommitted-transaction crash semantics; session state itself
//!   survives via the state store, the buffer deliberately does not.

use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::array::{new_null_array, RecordBatch, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use datafusion::common::TableReference;
use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::sqlparser::ast::Statement;
use snafu::ResultExt;

use crate::error as ex_error;
use crate::error::Result;
use crate::models::QueryResult;
use crate::query::UserQuery;

/// Buffered appends for one target table.
#[derive(Debug)]
pub struct TxnTableBuffer {
    /// Fully-qualified, normalized target name (`db.schema.table`).
    pub table: String,
    /// Target table schema the batches are aligned to.
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
    pub rows: usize,
}

/// Session transaction state. Lives in `UserSession::txn` behind a
/// `Mutex` — a session's statements serialize on the wire, but async
/// request handling may interleave, so every access is locked.
#[derive(Debug, Default)]
pub struct TxnState {
    pub started_ms: u64,
    /// A statement inside the transaction failed; everything is logically
    /// rolled back and further statements are rejected.
    pub aborted: bool,
    /// Buffered appends per normalized table name (BTreeMap for stable
    /// commit order in diagnostics).
    pub buffers: BTreeMap<String, TxnTableBuffer>,
    /// Buffered statement texts (diagnostics only).
    pub statements: Vec<String>,
}

/// Whether `session` currently has an open (possibly aborted) transaction.
#[must_use]
pub fn active(session: &Arc<crate::session::UserSession>) -> bool {
    session.txn.lock().expect("txn lock").is_some()
}

/// Entry hook for `BEGIN`/`COMMIT`/`ROLLBACK` statements. Returns
/// `Some(result)` when the statement was one of them (handled), `None`
/// otherwise. `COMMIT`/`ROLLBACK` with no open transaction are successful
/// no-ops (Snowflake parity).
pub async fn try_txn_statement(
    df_statement: &DFStatement,
    q: &UserQuery,
) -> Result<Option<QueryResult>> {
    let DFStatement::Statement(s) = df_statement else {
        return Ok(None);
    };
    match &**s {
        Statement::StartTransaction { .. } => {
            begin(&q.session)?;
            Ok(Some(q.status_response()?))
        }
        Statement::Commit { .. } => {
            txn_commit(q).await?;
            Ok(Some(q.status_response()?))
        }
        Statement::Rollback { .. } => {
            txn_rollback(&q.session);
            Ok(Some(q.status_response()?))
        }
        _ => Ok(None),
    }
}

fn begin(session: &Arc<crate::session::UserSession>) -> Result<()> {
    let mut txn = session.txn.lock().expect("txn lock");
    if txn.is_some() {
        return ex_error::TransactionAlreadyActiveSnafu.fail();
    }
    *txn = Some(TxnState {
        started_ms: now_ms(),
        ..TxnState::default()
    });
    Ok(())
}

fn txn_rollback(session: &Arc<crate::session::UserSession>) {
    *session.txn.lock().expect("txn lock") = None;
}

/// `COMMIT`: replay every buffered table's appends through one
/// `INSERT INTO t SELECT * FROM <memtable>` each (single snapshot per
/// table), then close the transaction. On a replay failure the transaction
/// is closed and the error propagates (already-applied tables keep their
/// committed snapshots — best-effort, v1).
async fn txn_commit(q: &UserQuery) -> Result<()> {
    let state = {
        let mut txn = q.session.txn.lock().expect("txn lock");
        txn.take()
    };
    let Some(state) = state else {
        return Ok(()); // no-op commit (Snowflake parity)
    };
    if state.aborted {
        return Ok(()); // aborted transaction closes with nothing committed
    }
    for (name, buf) in &state.buffers {
        replay_buffer(q, name, buf).await?;
    }
    Ok(())
}

/// Replay one table's buffered batches as a single append.
async fn replay_buffer(q: &UserQuery, table: &str, buf: &TxnTableBuffer) -> Result<()> {
    let table_ref: TableReference = table.into();
    let target = q
        .session
        .ctx
        .table_provider(table_ref.clone())
        .await
        .context(ex_error::DataFusionSnafu)?;
    q.replay_txn_insert(table_ref, target, buf.batches.clone())
        .await
}

/// Auto-commit an open transaction before a DDL statement runs (Snowflake:
/// DDL auto-commits). A failed auto-commit aborts the transaction.
pub async fn auto_commit(q: &UserQuery) -> Result<()> {
    let result = txn_commit(q).await;
    if result.is_err() {
        abort(q);
    }
    result
}

/// Pre-statement hook for statements other than `BEGIN`/`COMMIT`/`ROLLBACK`
/// (those went through [`try_txn_statement`]). Returns `Some(result)` when
/// the statement was fully handled (buffered INSERT / rejected DML),
/// `None` to proceed with normal execution (DDL returns `None` after
/// auto-committing).
pub async fn pre_statement_hook(
    df_statement: &DFStatement,
    q: &mut UserQuery,
) -> Result<Option<QueryResult>> {
    if !active(&q.session) {
        return Ok(None);
    }
    // Every statement in an aborted transaction errors until COMMIT /
    // ROLLBACK (which close it as no-ops).
    if q
        .session
        .txn
        .lock()
        .expect("txn lock")
        .as_ref()
        .is_some_and(|t| t.aborted)
    {
        return ex_error::TransactionAbortedSnafu.fail().map(Some);
    }
    match df_statement {
        DFStatement::Statement(s) => match &**s {
            // DDL auto-commits the open transaction, then runs normally.
            Statement::CreateTable { .. }
            | Statement::CreateView { .. }
            | Statement::CreateDatabase { .. }
            | Statement::CreateSchema { .. }
            | Statement::CreateStage { .. }
            | Statement::CreateExternalVolume { .. }
            | Statement::AlterTable { .. }
            | Statement::Drop { .. }
            | Statement::Truncate { .. } => {
                auto_commit(q).await?;
                Ok(None)
            }
            // Buffered writes.
            Statement::Insert { .. } => {
                let Statement::Insert(insert) = (**s).clone() else {
                    return Ok(None);
                };
                Ok(Some(buffer_insert(q, insert).await?))
            }
            // Row-level writes are not bufferable in v1.
            Statement::Delete { .. }
            | Statement::Update { .. }
            | Statement::Merge { .. }
            | Statement::CopyIntoSnowflake { .. } => {
                ex_error::NotSupportedStatementSnafu { statement: "DML" }.fail().map(Some)
            }
            _ => Ok(None),
        },
        // CREATE EXTERNAL TABLE is DDL.
        DFStatement::CreateExternalTable(_) => {
            auto_commit(q).await?;
            Ok(None)
        }
        _ => Ok(None),
    }
}

/// Buffer one `INSERT INTO t …` against the session transaction: execute
/// the source query, align the result batches to the target schema, and
/// store them. A failed buffer aborts the transaction.
async fn buffer_insert(
    q: &mut UserQuery,
    insert: datafusion::sql::sqlparser::ast::Insert,
) -> Result<QueryResult> {
    let Some(source) = insert.source else {
        return ex_error::TransactionInsertNoSourceSnafu {
            statement: q.raw_query.clone(),
        }
        .fail();
    };
    let datafusion::sql::sqlparser::ast::TableObject::TableName(table_name) = insert.table
    else {
        return ex_error::NotSupportedStatementSnafu { statement: "INSERT target" }
            .fail();
    };
    let resolved = q.resolve_table_object_name(table_name.0.clone())?;
    let target_ref: TableReference = (&resolved).into();
    let provider = q
        .session
        .ctx
        .table_provider(target_ref)
        .await
        .context(ex_error::DataFusionSnafu)?;
    let target_schema = provider.schema();

    let result = q.execute_query_statement(source).await;
    let QueryResult { records, .. } = result.inspect_err(|_| abort(q))?;
    let rows: usize = records.iter().map(RecordBatch::num_rows).sum();
    let aligned = records
        .iter()
        .map(|b| align_batch(b, &target_schema, &insert.columns))
        .collect::<Result<Vec<_>>>()
        .inspect_err(|_| abort(q))?;

    let table = resolved.to_string().to_ascii_lowercase();
    {
        let mut txn = q.session.txn.lock().expect("txn lock");
        let Some(state) = txn.as_mut() else {
            return ex_error::TransactionAbortedSnafu.fail();
        };
        let entry = state.buffers.entry(table).or_insert_with(|| TxnTableBuffer {
            table: String::new(),
            schema: Arc::clone(&target_schema),
            batches: Vec::new(),
            rows: 0,
        });
        entry.table = resolved.to_string();
        entry.batches.extend(aligned);
        entry.rows += rows;
        state.statements.push(q.raw_query.clone());
    }
    Ok(rows_response(rows))
}

fn abort(q: &UserQuery) {
    if let Some(t) = q.session.txn.lock().expect("txn lock").as_mut() {
        t.aborted = true;
    }
}

/// Project `batch` onto `schema`: columns matched by (case-insensitive)
/// name and cast to the target type; missing columns become nulls; the
/// optional explicit `INSERT INTO t (cols…)` list selects/reorders first.
fn align_batch(
    batch: &RecordBatch,
    schema: &SchemaRef,
    columns: &[datafusion::sql::sqlparser::ast::ObjectName],
) -> Result<RecordBatch> {
    let batch_schema = batch.schema();
    // With an explicit column list, source column i maps to target column
    // `columns[i]` (SQL positional semantics). Without one, match by name.
    let selection: Option<Vec<String>> = (!columns.is_empty()).then(|| {
        columns
            .iter()
            .map(|c| {
                c.0.last()
                    .map(|p| p.as_ident().map(|i| i.value.clone()))
                    .unwrap_or(None)
                    .unwrap_or_else(|| c.to_string())
                    .to_ascii_lowercase()
            })
            .collect()
    });
    // Pre-fill nulls: columns absent from the mapping default to NULL.
    let mut out: Vec<Arc<dyn datafusion::arrow::array::Array>> = schema
        .fields()
        .iter()
        .map(|f| new_null_array(f.data_type(), batch.num_rows()))
        .collect();
    match &selection {
        Some(sel) => {
            for (i, col) in sel.iter().enumerate() {
                if i >= batch_schema.fields().len() {
                    continue;
                }
                let target_idx = match schema
                    .fields()
                    .iter()
                    .position(|f| f.name().to_ascii_lowercase() == *col)
                {
                    Some(t) => t,
                    None => {
                        return ex_error::TransactionInsertUnknownColumnSnafu {
                            column: col.clone(),
                        }
                        .fail()
                    }
                };
                let field = schema.field(target_idx);
                let array: Arc<dyn datafusion::arrow::array::Array> =
                    if field.data_type() == batch_schema.field(i).data_type() {
                        Arc::clone(batch.column(i))
                    } else {
                        datafusion::arrow::compute::cast(batch.column(i), field.data_type())
                            .context(ex_error::ArrowSnafu)?
                    };
                out[target_idx] = array;
            }
        }
        None => {
            for (j, field) in schema.fields().iter().enumerate() {
                let name = field.name().to_ascii_lowercase();
                let idx = batch_schema
                    .fields()
                    .iter()
                    .position(|f| f.name().to_ascii_lowercase() == name);
                let array: Arc<dyn datafusion::arrow::array::Array> = match idx {
                    Some(i) if field.data_type() == batch_schema.field(i).data_type() => {
                        Arc::clone(batch.column(i))
                    }
                    Some(i) => datafusion::arrow::compute::cast(batch.column(i), field.data_type())
                        .context(ex_error::ArrowSnafu)?,
                    None => new_null_array(field.data_type(), batch.num_rows()),
                };
                out[j] = array;
            }
        }
    }
    RecordBatch::try_new(Arc::clone(schema), out).context(ex_error::ArrowSnafu)
}

/// INSERT-shaped result for a buffered statement: one `count` column
/// (the wire layer renames it to `number of rows inserted`).
fn rows_response(rows: usize) -> QueryResult {
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(UInt64Array::from(vec![rows as u64]))],
    )
    .expect("statically valid count batch");
    QueryResult::new(vec![batch], schema)
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};

    fn schema() -> SchemaRef {
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("v", DataType::Utf8, true),
        ]))
    }

    #[test]
    fn aligns_missing_columns_to_nulls() {
        let s = schema();
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![7]))],
        )
        .unwrap();
        let out = align_batch(&batch, &s, &[]).unwrap();
        assert_eq!(out.num_columns(), 2);
        assert_eq!(out.column(0).as_ref(), &Int64Array::from(vec![7]));
        assert!(out.column(1).is_null(0));
    }

    #[test]
    fn aligns_explicit_column_list_and_casts() {
        let s = schema();
        // Source columns follow the INSERT column list order positionally:
        // `INSERT INTO t (v, id) SELECT name, num FROM src` → batch (name, num).
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                Field::new("name", DataType::Utf8, true),
                Field::new("num", DataType::Float64, true),
            ])),
            vec![
                Arc::new(StringArray::from(vec![Some("z")])),
                Arc::new(Float64Array::from(vec![10.0])),
            ],
        )
        .unwrap();
        // INSERT INTO t (v, id) — maps source col 0 → v, col 1 → id (cast).
        let cols = vec![
            datafusion::sql::sqlparser::ast::ObjectName(vec![
                datafusion::sql::sqlparser::ast::ObjectNamePart::Identifier(
                    datafusion::sql::sqlparser::ast::Ident::new("v"),
                ),
            ]),
            datafusion::sql::sqlparser::ast::ObjectName(vec![
                datafusion::sql::sqlparser::ast::ObjectNamePart::Identifier(
                    datafusion::sql::sqlparser::ast::Ident::new("id"),
                ),
            ]),
        ];
        let out = align_batch(&batch, &s, &cols).unwrap();
        assert_eq!(
            out.column(0).as_ref(),
            &Int64Array::from(vec![10])
        );
        assert_eq!(
            out.column(1).as_ref(),
            &StringArray::from(vec![Some("z")])
        );
    }

    #[test]
    fn aligns_case_insensitively() {
        let s = schema();
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                Field::new("ID", DataType::Int64, false),
                Field::new("V", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec![Some("a")])),
            ],
        )
        .unwrap();
        let out = align_batch(&batch, &s, &[]).unwrap();
        assert_eq!(out.column(0).as_ref(), &Int64Array::from(vec![1]));
        assert_eq!(
            out.column(1).as_ref(),
            &StringArray::from(vec![Some("a")])
        );
    }
}