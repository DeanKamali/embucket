//! Executor-seconds metering (Avalanche design §5, M5).
//!
//! A running warehouse accrues credits at `executor_count` credits/hour
//! (X-SMALL = 1 credit/hour, doubling per size — the Snowflake ladder).
//! Usage is aggregated into hourly buckets per warehouse (one row per
//! (warehouse, hour, executor count)) and persisted to the state store, so
//! `WAREHOUSE_METERING_HISTORY` can serve it back.

use crate::error::{Result, StateStoreSnafu};
use snafu::ResultExt;
use state_store::StateStore;
use state_store::WarehouseMeteringRecord;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

pub const HOUR_SECS: u64 = 3600;

/// In-memory ledger of hourly usage buckets, write-through to the
/// [`StateStore`]. Reads are sync (the table function snapshots it); writes
/// are awaited by the warehouse manager's lifecycle hooks.
pub struct MeteringLedger {
    store: Arc<dyn StateStore>,
    /// (warehouse, hour bucket start, executor count) → accrued credits.
    /// A std mutex: never held across an await (state-store writes happen
    /// after the lock is released), so sync snapshot readers can share it.
    buckets: Mutex<HashMap<(String, u64, u32), f64>>,
}

impl MeteringLedger {
    pub fn new(store: Arc<dyn StateStore>) -> Self {
        Self {
            store,
            buckets: Mutex::new(HashMap::new()),
        }
    }

    /// Load persisted buckets from the state store into memory. Called once
    /// at manager construction so history survives process restarts.
    pub async fn load(&self) -> Result<()> {
        let records = self
            .store
            .list_metering(None)
            .await
            .context(StateStoreSnafu)?;
        let mut buckets = self.buckets.lock().expect("metering lock");
        for record in records {
            buckets.insert(
                (record.warehouse, record.start_time, record.executor_count),
                record.credits_used,
            );
        }
        Ok(())
    }

    /// Accrue `executor_count` executors running for `secs` seconds ending at
    /// `window_end` (seconds since epoch). Splits the window across hourly
    /// buckets and write-through persists each touched bucket.
    pub async fn accrue(
        &self,
        warehouse: &str,
        executor_count: u32,
        window_start: u64,
        window_end: u64,
    ) -> Result<()> {
        if executor_count == 0 || window_end <= window_start {
            return Ok(());
        }
        let mut touched: Vec<WarehouseMeteringRecord> = Vec::new();
        {
            let mut buckets = self.buckets.lock().expect("metering lock");
            // Walk hour boundaries, attributing each slice to its bucket.
            let mut start = window_start;
            while start < window_end {
                let bucket_start = start - (start % HOUR_SECS);
                let bucket_end = bucket_start + HOUR_SECS;
                let slice_end = window_end.min(bucket_end);
                let credits =
                    (slice_end - start) as f64 * f64::from(executor_count) / HOUR_SECS as f64;
                let key = (warehouse.to_string(), bucket_start, executor_count);
                let total = buckets.entry(key).or_insert(0.0);
                *total += credits;
                touched.push(WarehouseMeteringRecord {
                    warehouse: warehouse.to_string(),
                    start_time: bucket_start,
                    end_time: bucket_end,
                    executor_count,
                    credits_used: *total,
                });
                start = slice_end;
            }
        }
        for record in touched {
            self.store.put_metering(record).await.context(StateStoreSnafu)?;
        }
        Ok(())
    }

    /// Sync snapshot of all buckets (for `WAREHOUSE_METERING_HISTORY`),
    /// ordered by (start time, warehouse). Optionally filtered by warehouse
    /// (exact name).
    pub fn snapshot(&self, warehouse: Option<&str>) -> Vec<WarehouseMeteringRecord> {
        let buckets = self.buckets.lock().expect("metering lock");
        let mut records: Vec<WarehouseMeteringRecord> = buckets
            .iter()
            .filter(|((name, ..), _)| {
                warehouse.is_none_or(|filter| name.eq_ignore_ascii_case(filter))
            })
            .map(|((warehouse, start, count), credits)| WarehouseMeteringRecord {
                warehouse: warehouse.clone(),
                start_time: *start,
                end_time: start + HOUR_SECS,
                executor_count: *count,
                credits_used: *credits,
            })
            .collect();
        records.sort_by(|a, b| {
            (a.start_time, &a.warehouse, a.executor_count)
                .cmp(&(b.start_time, &b.warehouse, b.executor_count))
        });
        records
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use state_store::InMemoryStateStore;

    fn record(warehouse: &str, start: u64, count: u32, credits: f64) -> WarehouseMeteringRecord {
        WarehouseMeteringRecord {
            warehouse: warehouse.to_string(),
            start_time: start,
            end_time: start + HOUR_SECS,
            executor_count: count,
            credits_used: credits,
        }
    }

    const BASE: u64 = 1_760_000_400; // exactly 488889 × 3600 (hour-aligned)

    #[tokio::test]
    async fn accrues_one_hour_single_bucket() {
        let ledger = MeteringLedger::new(Arc::new(InMemoryStateStore::new()));
        // 1 executor for 1800s = 0.5 credits, within one hour bucket.
        ledger
            .accrue("WH", 1, BASE, BASE + 1800)
            .await
            .unwrap();
        let snap = ledger.snapshot(None);
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].start_time, BASE, "bucket is hour-aligned");
        assert!((snap[0].credits_used - 0.5).abs() < 1e-9);
        assert_eq!(snap[0].executor_count, 1);
    }

    #[tokio::test]
    async fn accrual_splits_across_hour_boundary() {
        let ledger = MeteringLedger::new(Arc::new(InMemoryStateStore::new()));
        // 40 min before the boundary + 40 min after it, 2 executors.
        let start = BASE + 2400;
        let end = start + 2400;
        ledger.accrue("WH", 2, start, end).await.unwrap();
        let snap = ledger.snapshot(None);
        assert_eq!(snap.len(), 2, "window spans two hourly buckets");
        let total: f64 = snap.iter().map(|r| r.credits_used).sum();
        // 2 executors × 2400s = 4800 executor-seconds = 1.3333 credits
        assert!((total - 4800.0 / 3600.0).abs() < 1e-9);
        // buckets are hour-aligned
        for r in &snap {
            assert_eq!(r.start_time % HOUR_SECS, 0);
        }
    }

    #[tokio::test]
    async fn accrual_accumulates_and_persists() {
        let store = Arc::new(InMemoryStateStore::new());
        let ledger = MeteringLedger::new(store.clone());
        ledger
            .accrue("WH", 1, BASE, BASE + 600)
            .await
            .unwrap();
        ledger
            .accrue("WH", 1, BASE + 600, BASE + 1200)
            .await
            .unwrap();
        let persisted = store.list_metering(Some("WH")).await.unwrap();
        assert_eq!(persisted.len(), 1);
        assert!((persisted[0].credits_used - 1200.0 / 3600.0).abs() < 1e-9);
    }

    #[tokio::test]
    async fn resize_creates_separate_bucket_rows() {
        let ledger = MeteringLedger::new(Arc::new(InMemoryStateStore::new()));
        ledger
            .accrue("WH", 1, BASE, BASE + 600)
            .await
            .unwrap();
        ledger
            .accrue("WH", 2, BASE + 600, BASE + 1200)
            .await
            .unwrap();
        let snap = ledger.snapshot(None);
        assert_eq!(snap.len(), 2);
        assert_eq!(snap[0].executor_count, 1);
        assert_eq!(snap[1].executor_count, 2);
    }

    #[tokio::test]
    async fn load_restores_buckets_from_store() {
        let store = Arc::new(InMemoryStateStore::new());
        store
            .put_metering(record("WH", BASE, 4, 2.5))
            .await
            .unwrap();
        let ledger = MeteringLedger::new(store);
        ledger.load().await.unwrap();
        let snap = ledger.snapshot(None);
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].executor_count, 4);
        assert!((snap[0].credits_used - 2.5).abs() < 1e-9);
    }

    #[tokio::test]
    async fn zero_executor_window_is_noop() {
        let ledger = MeteringLedger::new(Arc::new(InMemoryStateStore::new()));
        ledger
            .accrue("WH", 0, BASE, BASE + 600)
            .await
            .unwrap();
        assert!(ledger.snapshot(None).is_empty());
    }
}
