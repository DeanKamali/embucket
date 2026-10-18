//! Sequences (design §22.8, M16.4 deferred item): `CREATE SEQUENCE` state,
//! `seq.NEXTVAL` / `seq.CURRVAL` evaluation, and the pre-parse AST rewrite
//! that turns Snowflake's compound-identifier syntax into hidden UDF calls.
//!
//! `seq.NEXTVAL` cannot be a normal function call because Snowflake spells
//! it `seq.NEXTVAL`; the rewrite in [`rewrite_sequences`] replaces
//! `db.schema.seq.NEXTVAL` with `__nextval('db.schema.seq')` (and `CURRVAL`
//! with `__currval(...)`), resolving the sequence name against the session's
//! current database/schema first. The hidden UDFs are registered per
//! session and carry a [`SequenceSessionState`] handle.
//!
//! Value semantics (Snowflake CACHE model): a `NEXTVAL` reference reserves
//! a chunk of [`RESERVE_CHUNK`] values from the metastore at parse time —
//! the metastore's reservation cursor (`Sequence.current`) advances by the
//! whole chunk and is journaled, so restarts continue after the reserved
//! chunk and values already handed out are never reused. The UDF then
//! hands successive values to each row. Like Snowflake: reserved-but-
//! undelivered values are skipped after a restart, gaps are possible, and
//! sequence values do **not** roll back with the enclosing transaction.
//!
//! v1 limits: `MINVALUE`/`MAXVALUE`/`CACHE`/`ORDER` options are accepted
//! and ignored; `CYCLE` is rejected; `TEMPORARY` sequences are not
//! supported. Sequences on warehouse-distributed queries run locally on
//! the router (see `is_session_local_table_function`): the reservation
//! state is router-session-local.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use catalog_metastore::models::{Sequence as SequenceModel, TableIdent};
use catalog_metastore::Metastore;
use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::{TableFunctionImpl, TableProvider};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, Volatility};
use datafusion_common::{exec_err, DataFusionError, ScalarValue};
use datafusion_expr::Expr;
use sqlparser::ast::visit_expressions_mut;
use snafu::ResultExt;
use sqlparser::ast::{
    Expr as SqlExpr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList,
    FunctionArguments, Ident, ObjectName, ObjectNamePart, Statement, Value, ValueWithSpan,
};

use crate::account_usage::LazySnapshot;
use crate::error::{self as ex_error, Error, Result};
use crate::session::UserSession;

/// Values reserved per `NEXTVAL` chunk (Snowflake's default cache size
/// order of magnitude; reserved-but-undelivered values are skipped on
/// restart, matching documented CACHE behavior).
pub const RESERVE_CHUNK: i64 = 1000;

const NEXTVAL_UDF: &str = "__nextval";
const CURRVAL_UDF: &str = "__currval";
const SEQUENCES_TVF: &str = "__sequences_catalog";

// ---------------------------------------------------------------------------
// Per-session value state
// ---------------------------------------------------------------------------

/// A reserved value range for one sequence (session-local).
#[derive(Debug)]
struct SeqRange {
    /// Next value to hand out.
    next: AtomicI64,
    /// Last value covered by the reservation (inclusive).
    end: i64,
    increment: i64,
    /// Last value actually delivered by this session (CURRVAL).
    last: Mutex<Option<i64>>,
}

impl SeqRange {
    /// Deliver the next value; one per call. `end` may be overshot only by
    /// an in-flight compare-exchange, which surfaces as an exhausted-cache
    /// error.
    fn next_value(&self) -> Result<i64> {
        loop {
            let cur = self.next.load(Ordering::SeqCst);
            if cur > self.end {
                return ex_error::SequenceCacheExhaustedSnafu {}.fail();
            }
            let nxt = cur
                .checked_add(self.increment)
                .ok_or_else(|| ex_error::SequenceOverflowSnafu {}.build())?;
            if self
                .next
                .compare_exchange(cur, nxt, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                *self
                    .last
                    .lock()
                    .map_err(|_| ex_error::SequenceStatePoisonedSnafu {}.build())? =
                    Some(cur);
                return Ok(cur);
            }
        }
    }

    fn last_value(&self) -> Result<i64> {
        self.last
            .lock()
            .map_err(|_| ex_error::SequenceStatePoisonedSnafu {}.build())?
            .ok_or_else(|| ex_error::CurrvalNotDefinedSnafu {}.build())
    }
}

/// Session-local sequence value state: reserved ranges keyed by the fully
/// qualified, lowercased sequence name.
#[derive(Debug, Default)]
pub struct SequenceSessionState {
    ranges: Mutex<HashMap<String, Arc<SeqRange>>>,
}

impl SequenceSessionState {
    fn range(&self, key: &str) -> Result<Arc<SeqRange>> {
        self.ranges
            .lock()
            .map_err(|_| ex_error::SequenceStatePoisonedSnafu {}.build())?
            .get(key)
            .cloned()
            .ok_or_else(|| ex_error::CurrvalNotDefinedSnafu {}.build())
    }
}

// ---------------------------------------------------------------------------
// Reservation
// ---------------------------------------------------------------------------

/// Reserve [`RESERVE_CHUNK`] values for `ident`, advancing the metastore's
/// reservation cursor by the whole chunk (journaled). Skips the work when
/// the session already holds an unexhausted range for this sequence.
async fn reserve_chunk(session: &UserSession, ident: &TableIdent) -> Result<()> {
    let key = table_key_string(ident);
    // Existence first: a session may still hold a range for a sequence
    // that was ALTERed or DROPped — those must not keep delivering values.
    let Some(row) = session
        .metastore
        .get_sequence(ident)
        .await
        .context(ex_error::MetastoreSnafu)?
    else {
        return ex_error::SequenceNotFoundSnafu {
            sequence: ident.table.clone(),
            schema: ident.schema.clone(),
            db: ident.database.clone(),
        }
        .fail();
    };
    {
        let state = &session.sequence_state;
        let ranges = state
            .ranges
            .lock()
            .map_err(|_| ex_error::SequenceStatePoisonedSnafu {}.build())?;
        if let Some(range) = ranges.get(&key)
            && range.next.load(Ordering::SeqCst) <= range.end
        {
            return Ok(());
        }
    }
    let seq = &row.data;
    let first = seq.next();
    let Some(chunk_span) = RESERVE_CHUNK.checked_mul(seq.increment) else {
        return ex_error::SequenceOverflowSnafu {}.fail();
    };
    let Some(new_current) = seq.current.checked_add(chunk_span) else {
        return ex_error::SequenceOverflowSnafu {}.fail();
    };
    let updated = SequenceModel {
        ident: ident.clone(),
        start: seq.start,
        increment: seq.increment,
        current: new_current,
        comment: seq.comment.clone(),
    };
    session
        .metastore
        .update_sequence(ident, updated)
        .await
        .context(ex_error::MetastoreSnafu)?;
    let range = SeqRange {
        next: AtomicI64::new(first),
        end: new_current,
        increment: seq.increment,
        last: Mutex::new(None),
    };
    session
        .sequence_state
        .ranges
        .lock()
        .map_err(|_| ex_error::SequenceStatePoisonedSnafu {}.build())?
        .insert(key, Arc::new(range));
    Ok(())
}

/// Drop the session's reserved range for `ident` (after ALTER/DROP): the
/// next `NEXTVAL` re-reserves against the metastore cursor, picking up a
/// changed `INCREMENT`/`START` and refusing dropped sequences.
pub fn invalidate_session_range(session: &UserSession, ident: &TableIdent) {
    if let Ok(mut ranges) = session.sequence_state.ranges.lock() {
        ranges.remove(&table_key_string(ident));
    }
}

// ---------------------------------------------------------------------------
// Hidden UDFs
// ---------------------------------------------------------------------------

/// `__nextval('<db>.<schema>.<seq>')` — hands successive values per row.
///
/// Equality/hash by name only: the state handle is per-session and the
/// function is volatile, so plan-deduction never folds it.
#[derive(Debug)]
struct NextValFunc {
    state: Arc<SequenceSessionState>,
    signature: Signature,
}

impl PartialEq for NextValFunc {
    fn eq(&self, other: &Self) -> bool {
        self.name() == other.name()
    }
}

impl Eq for NextValFunc {}

impl std::hash::Hash for NextValFunc {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for NextValFunc {
    fn name(&self) -> &'static str {
        NEXTVAL_UDF
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let key = arg_key(&args.args)?;
        let range = self.state.range(&key)?;
        let mut values = Vec::with_capacity(args.number_rows);
        for _ in 0..args.number_rows {
            values.push(
                range
                    .next_value()
                    .map_err(|e| DataFusionError::Execution(format!("{NEXTVAL_UDF}: {e}")))?,
            );
        }
        Ok(ColumnarValue::Array(Arc::new(Int64Array::from(values))))
    }
}

/// `__currval('<db>.<schema>.<seq>')` — the last value this session
/// delivered for the sequence (Snowflake: session-scoped CURRVAL).
#[derive(Debug)]
struct CurrValFunc {
    state: Arc<SequenceSessionState>,
    signature: Signature,
}

impl PartialEq for CurrValFunc {
    fn eq(&self, other: &Self) -> bool {
        self.name() == other.name()
    }
}

impl Eq for CurrValFunc {}

impl std::hash::Hash for CurrValFunc {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for CurrValFunc {
    fn name(&self) -> &'static str {
        CURRVAL_UDF
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let key = arg_key(&args.args)?;
        let range = self.state.range(&key)?;
        let value = range
            .last_value()
            .map_err(|e| DataFusionError::Execution(format!("{CURRVAL_UDF}: {e}")))?;
        Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(value))))
    }
}

/// Extract the single literal-string sequence key from a UDF argument list.
fn arg_key(args: &[ColumnarValue]) -> datafusion_common::Result<String> {
    let [ColumnarValue::Scalar(ScalarValue::Utf8(Some(key)))] = args else {
        return Err(DataFusionError::Execution(
            "sequence UDF expects a single literal sequence name argument".to_string(),
        ));
    };
    Ok(key.clone())
}

/// Register the hidden sequence UDFs on a session context.
pub fn register_sequence_udfs(ctx: &SessionContext, state: Arc<SequenceSessionState>) {
    ctx.register_udf(ScalarUDF::from(NextValFunc {
        state: Arc::clone(&state),
        signature: Signature::uniform(1, vec![DataType::Utf8], Volatility::Volatile),
    }));
    ctx.register_udf(ScalarUDF::from(CurrValFunc {
        state,
        signature: Signature::uniform(1, vec![DataType::Utf8], Volatility::Volatile),
    }));
}

// ---------------------------------------------------------------------------
// AST rewrite
// ---------------------------------------------------------------------------

/// Rewrite `seq.NEXTVAL` / `seq.CURRVAL` compound identifiers into hidden
/// UDF calls and reserve value chunks for every `NEXTVAL` reference.
///
/// `resolve` maps the identifier prefix (the sequence name parts as written)
/// to a fully-qualified [`TableIdent`] using the session's current
/// database/schema and identifier normalization.
///
/// Returns the sequences referenced with `NEXTVAL` (first-seen order), for
/// diagnostics.
pub async fn rewrite_sequences(
    session: &UserSession,
    statement: &mut Statement,
    resolve: impl Fn(Vec<ObjectNamePart>) -> Result<TableIdent>,
) -> Result<Vec<TableIdent>> {
    let reserved = rewrite_statement(statement, resolve)?;
    for (_, ident) in &reserved {
        reserve_chunk(session, ident).await?;
    }
    Ok(reserved.into_iter().map(|(_, ident)| ident).collect())
}

/// Pure AST rewrite: replace `seq.NEXTVAL` / `seq.CURRVAL` compound
/// identifiers with hidden UDF calls and return the NEXTVAL-referenced
/// sequences (first-seen order). No state is touched — reservation happens
/// in [`rewrite_sequences`].
pub fn rewrite_statement(
    statement: &mut Statement,
    resolve: impl Fn(Vec<ObjectNamePart>) -> Result<TableIdent>,
) -> Result<Vec<(String, TableIdent)>> {
    let mut reserved: Vec<(String, TableIdent)> = Vec::new(); // NEXTVAL refs, first-seen order
    let mut failed: Option<Error> = None;

    let _ = visit_expressions_mut(statement, |expr| {
        let SqlExpr::CompoundIdentifier(parts) = expr else {
            return std::ops::ControlFlow::<()>::Continue(());
        };
        let Some(last) = parts.last() else {
            return std::ops::ControlFlow::<()>::Continue(());
        };
        // Only bare (unquoted) NEXTVAL/CURRVAL qualify — a quoted "NEXTVAL"
        // is a column name.
        if last.quote_style.is_some() {
            return std::ops::ControlFlow::<()>::Continue(());
        }
        let is_nextval = last.value.eq_ignore_ascii_case("NEXTVAL");
        let is_currval = last.value.eq_ignore_ascii_case("CURRVAL");
        if !is_nextval && !is_currval {
            return std::ops::ControlFlow::<()>::Continue(());
        }
        if parts.len() < 2 {
            failed = Some(
                ex_error::InvalidTableIdentifierSnafu {
                    ident: parts
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join("."),
                }
                .build(),
            );
            return std::ops::ControlFlow::<()>::Break(());
        }
        let name_parts: Vec<ObjectNamePart> = parts[..parts.len() - 1]
            .iter()
            .map(|ident| ObjectNamePart::Identifier(ident.clone()))
            .collect();
        let resolved = match resolve(name_parts) {
            Ok(ident) => ident,
            Err(e) => {
                failed = Some(e);
                return std::ops::ControlFlow::<()>::Break(());
            }
        };
        let key = table_key_string(&resolved);
        let func_name = if is_nextval { NEXTVAL_UDF } else { CURRVAL_UDF };
        if is_nextval && !reserved.iter().any(|(k, _)| *k == key) {
            reserved.push((key.clone(), resolved));
        }
        *expr = udf_call(func_name, &key);
        std::ops::ControlFlow::<()>::Continue(())
    });

    if let Some(err) = failed {
        return Err(err);
    }

    Ok(reserved)
}

/// Build `__nextval('<key>')` / `__currval('<key>')` as an AST function call.
fn udf_call(func_name: &str, key: &str) -> SqlExpr {
    SqlExpr::Function(Function {
        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(func_name))]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlExpr::Value(
                ValueWithSpan {
                    value: Value::SingleQuotedString(key.to_string()),
                    span: sqlparser::tokenizer::Span::empty(),
                },
            )))],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}

// ---------------------------------------------------------------------------
// SHOW SEQUENCES support
// ---------------------------------------------------------------------------

/// One batch: `created_on, name, database_name, schema_name, comment` per
/// sequence (the Snowflake `SHOW SEQUENCES` column shape).
fn sequences_schema() -> Schema {
    Schema::new(vec![
        Field::new("created_on", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, false),
        Field::new("database_name", DataType::Utf8, false),
        Field::new("schema_name", DataType::Utf8, false),
        Field::new("comment", DataType::Utf8, true),
    ])
}

fn sequences_batch(
    rows: &[catalog_metastore::models::RwObject<SequenceModel>],
) -> datafusion_common::Result<RecordBatch> {
    let schema = sequences_schema();
    let names: Vec<Option<String>> = rows
        .iter()
        .map(|r| Some(r.data.ident.table.clone()))
        .collect();
    let databases: Vec<Option<String>> = rows
        .iter()
        .map(|r| Some(r.data.ident.database.clone()))
        .collect();
    let schemas: Vec<Option<String>> = rows
        .iter()
        .map(|r| Some(r.data.ident.schema.clone()))
        .collect();
    let comments: Vec<Option<String>> =
        rows.iter().map(|r| r.data.comment.clone()).collect();
    let created_on: Vec<Option<String>> = rows
        .iter()
        .map(|r| Some(r.created_at.format("%Y-%m-%d %H:%M:%S%.3f").to_string()))
        .collect();
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(StringArray::from(created_on)),
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(databases)),
            Arc::new(StringArray::from(schemas)),
            Arc::new(StringArray::from(comments)),
        ],
    )
    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

/// Register the hidden SHOW-SEQUENCES TVF on a session context.
///
/// SHOW SEQUENCES rewrites into `SELECT … FROM __sequences_catalog()`; the
/// TVF resolves the metastore's sequence list lazily at scan time (the
/// [`crate::account_usage::LazySnapshot`] pattern — `call` is sync but the
/// metastore is async).
pub fn register_sequences_tvf(ctx: &SessionContext, metastore: Arc<dyn Metastore>) {
    let schema = sequences_schema();
    ctx.register_udtf(
        SEQUENCES_TVF,
        Arc::new(SequencesCatalogFunc {
            store: metastore,
            schema,
        }),
    );
}

/// Zero-arg TVF yielding one row per sequence in the metastore.
#[derive(Debug)]
struct SequencesCatalogFunc {
    store: Arc<dyn Metastore>,
    schema: Schema,
}

impl TableFunctionImpl for SequencesCatalogFunc {
    fn call(&self, args: &[Expr]) -> datafusion_common::Result<Arc<dyn TableProvider>> {
        if !args.is_empty() {
            return exec_err!(
                "{SEQUENCES_TVF}: no arguments are supported; scope SHOW SEQUENCES with IN instead"
            );
        }
        let store = Arc::clone(&self.store);
        let schema = self.schema.clone();
        Ok(Arc::new(LazySnapshot::new(schema, move || {
            let store = Arc::clone(&store);
            Box::pin(async move {
                let rows = store
                    .list_all_sequences()
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                Ok(vec![sequences_batch(&rows)?])
            })
        })))
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn table_key_string(ident: &TableIdent) -> String {
    format!(
        "{}.{}.{}",
        ident.database.to_ascii_lowercase(),
        ident.schema.to_ascii_lowercase(),
        ident.table.to_ascii_lowercase()
    )
}

/// Extract `START`/`INCREMENT`/`COMMENT` from sequence options. `START`
/// and `INCREMENT` are `None` when absent (CREATE defaults to 1/1; ALTER
/// keeps the existing value). Unsupported-but-accepted options
/// (`MINVALUE`/`MAXVALUE`/`CACHE`/`ORDER`, `NO CYCLE`) are ignored; `CYCLE`
/// is rejected outright.
///
/// # Errors
/// When a numeric option is not an integer, `INCREMENT` is zero, or
/// `CYCLE` is requested.
pub fn parse_sequence_options(
    options: &[sqlparser::ast::SequenceOptions],
) -> Result<(Option<i64>, Option<i64>, Option<String>)> {
    use sqlparser::ast::SequenceOptions;
    let mut start = None;
    let mut increment = None;
    let mut comment = None;
    for option in options {
        match option {
            SequenceOptions::StartWith(expr, _) => {
                start = Some(sequence_number(expr, "START")?);
            }
            SequenceOptions::IncrementBy(expr, _) => {
                let value = sequence_number(expr, "INCREMENT")?;
                if value == 0 {
                    return ex_error::InvalidSequenceOptionSnafu {
                        message: "INCREMENT must not be zero".to_string(),
                    }
                    .fail();
                }
                increment = Some(value);
            }
            SequenceOptions::Comment(value) => {
                comment = Some(value.clone());
            }
            SequenceOptions::Cycle(true) => {
                return ex_error::InvalidSequenceOptionSnafu {
                    message: "CYCLE is not supported".to_string(),
                }
                .fail();
            }
            // MINVALUE/MAXVALUE/CACHE/ORDER/NO CYCLE: accepted, ignored in
            // v1 (documented limitation).
            SequenceOptions::MinValue(_)
            | SequenceOptions::MaxValue(_)
            | SequenceOptions::Cache(_)
            | SequenceOptions::Order(_)
            | SequenceOptions::Cycle(false) => {}
        }
    }
    Ok((start, increment, comment))
}

/// Parse a numeric sequence option value (`START 10`, `INCREMENT = 5`).
fn sequence_number(expr: &sqlparser::ast::Expr, option: &str) -> Result<i64> {
    let sqlparser::ast::Expr::Value(value) = expr else {
        return ex_error::InvalidSequenceOptionSnafu {
            message: format!("{option} expects a numeric literal"),
        }
        .fail();
    };
    let sqlparser::ast::Value::Number(text, _) = &value.value else {
        return ex_error::InvalidSequenceOptionSnafu {
            message: format!("{option} expects a numeric literal"),
        }
        .fail();
    };
    text.parse::<i64>().map_err(|_| {
        ex_error::InvalidSequenceOptionSnafu {
            message: format!("{option} value '{text}' is not a 64-bit integer"),
        }
        .build()
    })
}

/// Initial reservation cursor: `start - increment` so the first `NEXTVAL`
/// yields `start`.
///
/// # Errors
/// On 64-bit overflow.
pub fn initial_current(start: i64, increment: i64) -> Result<i64> {
    start.checked_sub(increment).ok_or_else(|| {
        ex_error::InvalidSequenceOptionSnafu {
            message: "START - INCREMENT overflows a 64-bit integer".to_string(),
        }
        .build()
    })
}
// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    fn parse(sql: &str) -> Statement {
        Parser::parse_sql(&GenericDialect {}, sql)
            .unwrap_or_else(|e| panic!("parse: {e}"))
            .remove(0)
    }

    fn ok_resolve(parts: Vec<ObjectNamePart>) -> Result<TableIdent> {
        let names: Vec<String> = parts
            .iter()
            .filter_map(|p| match p {
                ObjectNamePart::Identifier(ident) => Some(ident.value.to_ascii_lowercase()),
                ObjectNamePart::Function(_) => None,
            })
            .collect();
        let mut db = "db".to_string();
        let mut schema = "public".to_string();
        let mut table = names.last().cloned().unwrap_or_default();
        if names.len() == 2 {
            schema = names[0].clone();
        } else if names.len() == 3 {
            db = names[0].clone();
            schema = names[1].clone();
            table = names[2].clone();
        }
        Ok(TableIdent { database: db, schema, table })
    }

    #[test]
    fn rewrites_nextval_and_currval() {
        let mut stmt = parse(
            "INSERT INTO t(id, prev) SELECT s.nextval, s.currval FROM t1 AS s",
        );
        let reserved = rewrite_statement(&mut stmt, ok_resolve).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(reserved.len(), 1);
        assert_eq!(reserved[0].0, "db.public.s");
        let text = stmt.to_string();
        let up = text.to_uppercase();
        assert!(up.contains("__NEXTVAL('DB.PUBLIC.S')"), "{text}");
        assert!(up.contains("__CURRVAL('DB.PUBLIC.S')"), "{text}");
        assert!(!up.contains(".NEXTVAL"), "{text}");
    }

    #[test]
    fn rewrites_qualified_and_cased_names() {
        let mut stmt = parse(
            "SELECT MyDb.MySchema.MySeq.NEXTVAL, other.CURRVAL FROM x",
        );
        let reserved =
            rewrite_statement(&mut stmt, ok_resolve).unwrap_or_else(|e| panic!("{e}"));
        // Only NEXTVAL references are reserved (CURRVAL just reads state).
        assert_eq!(reserved.len(), 1);
        assert_eq!(reserved[0].0, "mydb.myschema.myseq");
        let text = stmt.to_string().to_uppercase();
        assert!(text.contains("__NEXTVAL('MYDB.MYSCHEMA.MYSEQ')"), "{text}");
        assert!(text.contains("__CURRVAL('DB.PUBLIC.OTHER')"), "{text}");
    }

    #[test]
    fn quoted_nextval_is_a_column_not_a_sequence() {
        let mut stmt = parse("SELECT s.\"NEXTVAL\", s.\"currval\" FROM t");
        let reserved = rewrite_statement(&mut stmt, ok_resolve).unwrap_or_else(|e| panic!("{e}"));
        assert!(reserved.is_empty());
        assert!(stmt.to_string().contains("\"NEXTVAL\""));
    }

    #[test]
    fn bare_nextval_is_untouched() {
        // A bare (non-compound) `NEXTVAL` is not a sequence reference.
        let mut stmt = parse("SELECT nextval FROM t");
        let reserved = rewrite_statement(&mut stmt, ok_resolve).unwrap_or_else(|e| panic!("{e}"));
        assert!(reserved.is_empty());
        assert_eq!(stmt.to_string(), "SELECT nextval FROM t");
    }

    #[test]
    fn resolver_error_propagates() {
        let mut stmt = parse("SELECT s.nextval FROM t");
        let err = rewrite_statement(&mut stmt, |_| {
            ex_error::SequenceNotFoundSnafu {
                sequence: "s".to_string(),
                schema: "public".to_string(),
                db: "db".to_string(),
            }
            .fail()
        })
        .err()
        .unwrap_or_else(|| panic!("expected error"));
        assert!(matches!(err, Error::SequenceNotFound { .. }), "{err:?}");
    }

    #[test]
    fn non_sequence_statements_untouched() {
        let mut stmt = parse("SELECT a.b, t1.c FROM t WHERE x.y.z = 1");
        let reserved = rewrite_statement(&mut stmt, ok_resolve).unwrap_or_else(|e| panic!("{e}"));
        assert!(reserved.is_empty());
        assert_eq!(
            stmt.to_string(),
            "SELECT a.b, t1.c FROM t WHERE x.y.z = 1"
        );
    }

    #[test]
    fn seq_range_delivery_and_currval() {
        let range = SeqRange {
            next: AtomicI64::new(1),
            end: 5,
            increment: 2,
            last: Mutex::new(None),
        };
        // CURRVAL before NEXTVAL errors.
        assert!(range.last_value().is_err());
        assert_eq!(range.next_value().unwrap_or_else(|e| panic!("{e}")), 1);
        assert_eq!(range.last_value().unwrap_or_else(|e| panic!("{e}")), 1);
        assert_eq!(range.next_value().unwrap_or_else(|e| panic!("{e}")), 3);
        assert_eq!(range.next_value().unwrap_or_else(|e| panic!("{e}")), 5);
        assert_eq!(range.last_value().unwrap_or_else(|e| panic!("{e}")), 5);
        // Range exhausted.
        assert!(range.next_value().is_err());
    }

    #[test]
    fn parses_start_increment_comment() {
        use sqlparser::ast::SequenceOptions;
        let (start, increment, comment) = parse_sequence_options(&[
            SequenceOptions::StartWith(
                sqlparser::ast::Expr::Value(ValueWithSpan {
                    value: Value::Number("10".to_string(), false),
                    span: sqlparser::tokenizer::Span::empty(),
                }),
                false,
            ),
            SequenceOptions::IncrementBy(
                sqlparser::ast::Expr::Value(ValueWithSpan {
                    value: Value::Number("5".to_string(), false),
                    span: sqlparser::tokenizer::Span::empty(),
                }),
                false,
            ),
            SequenceOptions::Comment("hi".to_string()),
            SequenceOptions::MinValue(None),
            SequenceOptions::Cache(
                sqlparser::ast::Expr::Value(ValueWithSpan {
                    value: Value::Number("20".to_string(), false),
                    span: sqlparser::tokenizer::Span::empty(),
                }),
            ),
            SequenceOptions::Cycle(false),
            SequenceOptions::Order(true),
        ])
        .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(start, Some(10));
        assert_eq!(increment, Some(5));
        assert_eq!(comment, Some("hi".to_string()));
    }

    #[test]
    fn rejects_cycle_zero_increment_and_non_literals() {
        use sqlparser::ast::SequenceOptions;
        let num = |n: &str| {
            sqlparser::ast::Expr::Value(ValueWithSpan {
                value: Value::Number(n.to_string(), false),
                span: sqlparser::tokenizer::Span::empty(),
            })
        };
        let err = parse_sequence_options(&[SequenceOptions::Cycle(true)])
            .err()
            .unwrap_or_else(|| panic!("expected error"));
        assert!(err.to_string().contains("CYCLE"));
        let err = parse_sequence_options(&[SequenceOptions::IncrementBy(num("0"), false)])
            .err()
            .unwrap_or_else(|| panic!("expected error"));
        assert!(err.to_string().contains("zero"));
        let err = parse_sequence_options(&[SequenceOptions::StartWith(
            sqlparser::ast::Expr::Identifier("x".into()),
            false,
        )])
        .err()
        .unwrap_or_else(|| panic!("expected error"));
        assert!(err.to_string().contains("numeric literal"));
    }

    #[test]
    fn initial_current_starts_before_start() {
        assert_eq!(initial_current(1, 1).unwrap_or_else(|e| panic!("{e}")), 0);
        assert_eq!(initial_current(10, 5).unwrap_or_else(|e| panic!("{e}")), 5);
        assert!(initial_current(i64::MIN, 1).is_err());
    }
}
