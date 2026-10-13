//! `WAREHOUSE_METERING_HISTORY` table function (Avalanche design §5, M5).
//!
//! Snowflake-compatible shape:
//! `SELECT * FROM TABLE(WAREHOUSE_METERING_HISTORY([WAREHOUSE_NAME => '<name>']))`
//!
//! Returns the hourly credit-usage buckets accrued by the
//! [`MeteringLedger`](crate::metering::MeteringLedger). The `TABLE(...)`
//! wrapper is stripped by the executor's `TableFunctionVisitor` (same as
//! `RESULT_SCAN`/`FLATTEN`), so bare `WAREHOUSE_METERING_HISTORY(...)` in a
//! FROM clause also resolves.

use crate::metering::MeteringLedger;
use datafusion::arrow::array::{
    ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray, TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::catalog::{TableFunctionImpl, TableProvider};
use datafusion::common::{ScalarValue, exec_err};
use datafusion::datasource::MemTable;
use datafusion::logical_expr::Expr;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// `WAREHOUSE_METERING_HISTORY` — hourly credit usage per warehouse.
pub struct MeteringHistoryTableFunc {
    ledger: Arc<MeteringLedger>,
}

impl std::fmt::Debug for MeteringHistoryTableFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MeteringHistoryTableFunc")
    }
}

impl MeteringHistoryTableFunc {
    pub fn new(ledger: Arc<MeteringLedger>) -> Self {
        Self { ledger }
    }

    /// Snowflake's column subset we expose in v1. Lowercase names so
    /// unquoted (ident-normalized) refs like `credits_used` resolve —
    /// DataFusion lowercases unquoted identifiers at parse time.
    #[must_use]
    pub fn schema() -> Schema {
        Schema::new(vec![
            Field::new("start_time", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("end_time", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("warehouse_id", DataType::Int64, false),
            Field::new("warehouse_name", DataType::Utf8, false),
            Field::new("credits_used", DataType::Float64, false),
        ])
    }
}

/// Stable pseudo-id for a warehouse name (Snowflake exposes a numeric
/// `WAREHOUSE_ID`; ours is a 64-bit hash of the name).
fn warehouse_id(name: &str) -> i64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    name.hash(&mut hasher);
    i64::from_ne_bytes(hasher.finish().to_ne_bytes())
}

/// Extract an optional string argument: a positional string literal, or a
/// named argument (`WAREHOUSE_NAME => '...'`) presented as an aliased
/// literal. Anything else is an error.
fn warehouse_filter(args: &[Expr]) -> datafusion::common::Result<Option<String>> {
    let mut filter = None;
    for expr in args {
        let (arg_name, value): (Option<&str>, &Expr) = match expr {
            Expr::Alias(alias) => (Some(alias.name.as_str()), &alias.expr),
            other => (None, other),
        };
        if let Some(arg_name) = arg_name
            && !arg_name.eq_ignore_ascii_case("warehouse_name")
        {
            return exec_err!(
                "WAREHOUSE_METERING_HISTORY: unsupported argument {arg_name}; \
                 expected WAREHOUSE_NAME"
            );
        }
        let Expr::Literal(ScalarValue::Utf8(Some(name)), _) = value else {
            return exec_err!(
                "WAREHOUSE_METERING_HISTORY: WAREHOUSE_NAME must be a string literal"
            );
        };
        if filter.replace(name.clone()).is_some() {
            return exec_err!("WAREHOUSE_METERING_HISTORY: WAREHOUSE_NAME given twice");
        }
    }
    Ok(filter)
}

impl TableFunctionImpl for MeteringHistoryTableFunc {
    fn call(&self, args: &[Expr]) -> datafusion::common::Result<Arc<dyn TableProvider>> {
        let warehouse = warehouse_filter(args)?;
        let records = self.ledger.snapshot(warehouse.as_deref());

        let schema = Arc::new(Self::schema());
        let secs_to_nanos = |secs: u64| (secs * 1_000_000_000) as i64;
        let start: Vec<i64> = records.iter().map(|r| secs_to_nanos(r.start_time)).collect();
        let end: Vec<i64> = records.iter().map(|r| secs_to_nanos(r.end_time)).collect();
        let ids: Vec<i64> = records.iter().map(|r| warehouse_id(&r.warehouse)).collect();
        let names: Vec<&str> = records.iter().map(|r| r.warehouse.as_str()).collect();
        let credits: Vec<f64> = records.iter().map(|r| r.credits_used).collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(start)) as ArrayRef,
                Arc::new(TimestampNanosecondArray::from(end)) as ArrayRef,
                Arc::new(Int64Array::from(ids)) as ArrayRef,
                Arc::new(StringArray::from(names)) as ArrayRef,
                Arc::new(Float64Array::from(credits)) as ArrayRef,
            ],
        )?;
        let table = MemTable::try_new(schema, vec![vec![batch]])?;
        Ok(Arc::new(table))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metering::MeteringLedger;
    use datafusion::arrow::array::Array;
    use state_store::InMemoryStateStore;

    #[tokio::test]
    async fn returns_snapshotted_buckets() {
        let ledger = Arc::new(MeteringLedger::new(Arc::new(InMemoryStateStore::new())));
        ledger.accrue("M4WH", 2, 1_760_000_400, 1_760_001_000).await.unwrap();
        let func = MeteringHistoryTableFunc::new(ledger);
        let provider = func.call(&[]).expect("call with no args");
        let schema = provider.schema();
        assert_eq!(schema.fields().len(), 5);
        assert_eq!(schema.field(3).name(), "warehouse_name");
        assert_eq!(schema.field(4).name(), "credits_used");
    }

    #[tokio::test]
    async fn filters_by_warehouse() {
        let ledger = Arc::new(MeteringLedger::new(Arc::new(InMemoryStateStore::new())));
        ledger.accrue("A", 1, 1_760_000_400, 1_760_000_405).await.unwrap();
        ledger.accrue("B", 1, 1_760_000_400, 1_760_000_405).await.unwrap();
        let func = MeteringHistoryTableFunc::new(ledger);
        let snapshot_a = func.ledger.snapshot(Some("A"));
        assert_eq!(snapshot_a.len(), 1);
        assert_eq!(snapshot_a[0].warehouse, "A");
        let all = func.ledger.snapshot(None);
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn rejects_non_string_arg() {
        let ledger = Arc::new(MeteringLedger::new(Arc::new(InMemoryStateStore::new())));
        let func = MeteringHistoryTableFunc::new(ledger);
        let err = func.call(&[Expr::Literal(ScalarValue::Int64(Some(1)), None)]);
        assert!(err.is_err());
    }
}
