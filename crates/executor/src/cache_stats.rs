//! M14.3 — `WAREHOUSE_CACHE_HISTORY()` (design §20.1.5): observability for
//! the warm block cache. One row per cached object-store identity with the
//! cumulative [`catalog_metastore::block_cache`] counters: hits, misses,
//! bytes served from the local disk cache, bytes fetched from the source,
//! and evictions. `hit_ratio` and `cached_bytes_served` prove "the blocks
//! came from disk, not S3" (M14 exit criterion 1).
//!
//! Counters are process-global (the cache registry outlives sessions); this
//! TVF is warehouse-adjacent like `WAREHOUSE_METERING_HISTORY()` — the
//! stores are owned by volume-backed warehouses.
use crate::account_usage::{LazySnapshot, str_field};
use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::TableProvider;
use datafusion::common::exec_err;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::Expr;
use datafusion::catalog::TableFunctionImpl;
use std::sync::Arc;

/// `WAREHOUSE_CACHE_HISTORY()` — per-store warm-cache counters.
#[derive(Clone, Debug)]
pub struct WarehouseCacheHistoryTableFunc;

impl WarehouseCacheHistoryTableFunc {
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    #[must_use]
    pub fn schema() -> Schema {
        Schema::new(vec![
            str_field("store_id"),
            Field::new("hits", DataType::Int64, false),
            Field::new("misses", DataType::Int64, false),
            Field::new("cached_bytes_served", DataType::Int64, false),
            Field::new("fetched_bytes", DataType::Int64, false),
            Field::new("evictions", DataType::Int64, false),
            Field::new("hit_ratio", DataType::Float64, true),
        ])
    }

    fn batch(rows: Vec<(String, [u64; 5])>) -> DFResult<RecordBatch> {
        let schema = Self::schema();
        let n = rows.len();
        let mut store_ids = Vec::with_capacity(n);
        let mut hits = Vec::with_capacity(n);
        let mut misses = Vec::with_capacity(n);
        let mut served = Vec::with_capacity(n);
        let mut fetched = Vec::with_capacity(n);
        let mut evictions = Vec::with_capacity(n);
        let mut ratios = Vec::with_capacity(n);
        for (store_id, [h, m, s, f, e]) in rows {
            store_ids.push(store_id);
            hits.push(i64::try_from(h).unwrap_or(i64::MAX));
            misses.push(i64::try_from(m).unwrap_or(i64::MAX));
            served.push(i64::try_from(s).unwrap_or(i64::MAX));
            fetched.push(i64::try_from(f).unwrap_or(i64::MAX));
            evictions.push(i64::try_from(e).unwrap_or(i64::MAX));
            let attempts = h + m;
            ratios.push(if attempts == 0 {
                None
            } else {
                Some(h as f64 / attempts as f64)
            });
        }
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(store_ids)),
            Arc::new(Int64Array::from(hits)),
            Arc::new(Int64Array::from(misses)),
            Arc::new(Int64Array::from(served)),
            Arc::new(Int64Array::from(fetched)),
            Arc::new(Int64Array::from(evictions)),
            Arc::new(Float64Array::from(ratios)),
        ];
        RecordBatch::try_new(Arc::new(schema) as SchemaRef, arrays)
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
    }
}

impl Default for WarehouseCacheHistoryTableFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl TableFunctionImpl for WarehouseCacheHistoryTableFunc {
    fn call(&self, args: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        if !args.is_empty() {
            return exec_err!(
                "WAREHOUSE_CACHE_HISTORY: no arguments are supported; call it with no arguments"
            );
        }
        let schema = Self::schema();
        Ok(Arc::new(LazySnapshot::new(schema, || {
            Box::pin(async {
                let mut rows = catalog_metastore::block_cache::snapshot_cache_stats();
                rows.sort_by(|a, b| a.0.cmp(&b.0));
                Ok(vec![Self::batch(rows)?])
            })
        })))
    }
}