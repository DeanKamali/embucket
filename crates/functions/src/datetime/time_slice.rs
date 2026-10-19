use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike};
use datafusion::arrow::array::AsArray;
use datafusion::arrow::array::{Array, GenericStringArray, PrimitiveArray, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Int64Type, TimeUnit, TimestampNanosecondType};
use datafusion::error::Result as DFResult;
use datafusion::scalar::ScalarValue;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::sync::Arc;

/// `TIME_SLICE` SQL function
///
/// Rounds/truncates a date or timestamp to the start of the requested
/// slice unit at the given slice length (Snowflake semantics: slices are
/// anchored at the start of the year for long units, and the default
/// START OF WEEK is Monday).
///
/// Syntax: TIME_SLICE(<date_or_time_expr>, <slice_length>, <date_part> [, <start_or_end>])
///
/// Arguments:
/// - date_or_time_expr: a date, time, or timestamp expression
/// - slice_length: positive integer length of the slice
/// - date_part: unit of the slice (SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER or YEAR)
/// - start_or_end: optional; only 'START' is supported in v1 (END is a no-op alias)
///
/// Example: SELECT TIME_SLICE('2026-01-15 10:37:00'::TIMESTAMP, 15, 'MINUTE');
///
/// Returns: 2026-01-15 10:30:00.000
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TimeSliceFunc {
    signature: Signature,
}

impl Default for TimeSliceFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl TimeSliceFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(3), TypeSignature::Any(4)],
                Volatility::Immutable,
            ),
        }
    }
}

fn to_ts_nanos(v: &ColumnarValue, len: usize) -> DFResult<ArrayRef> {
    let arr = v.to_array(len)?;
    let dt = arr.data_type();
    if matches!(dt, DataType::Timestamp(TimeUnit::Nanosecond, _)) {
        Ok(arr)
    } else {
        Ok(cast(
            &arr,
            &DataType::Timestamp(TimeUnit::Nanosecond, None),
        )?)
    }
}

type ArrayRef = datafusion::arrow::array::ArrayRef;

fn slice_start(nd: NaiveDateTime, length: i64, unit: &str) -> DFResult<NaiveDateTime> {
    match unit {
        "SECOND" => {
            let secs = i64::from(nd.timestamp() % (length * 1_000_000_000 / 1_000_000_000).max(1));
            Ok(nd - chrono::Duration::seconds(secs))
        }
        "MINUTE" => {
            let total = i64::from(nd.hour()) * 60 + i64::from(nd.minute());
            let bucket = total.div_euclid(length.max(1)) * length.max(1);
            Ok(nd.date().and_hms_opt(0, 0, 0).unwrap() + chrono::Duration::minutes(bucket))
        }
        "HOUR" => {
            let bucket = i64::from(nd.hour()).div_euclid(length.max(1)) * length.max(1);
            Ok(nd
                .date()
                .and_hms_opt(bucket as u32, 0, 0)
                .unwrap_or_else(|| nd.date().and_hms_opt(0, 0, 0).unwrap()))
        }
        "DAY" => {
            // Slice days relative to the start of the epoch.
            let days = i64::from(nd.num_days_from_ce());
            let bucket = days.div_euclid(length.max(1)) * length.max(1);
            Ok(NaiveDate::from_num_days_from_ce_opt(bucket as i32)
                .unwrap_or(nd.date())
                .and_hms_opt(0, 0, 0)
                .unwrap())
        }
        "WEEK" => {
            // Anchor: Monday of the week containing Jan 1 of this year.
            let jan1 = NaiveDate::from_ymd_opt(nd.year(), 1, 1).unwrap();
            let jan1_monday =
                jan1 - chrono::Duration::days(i64::from(jan1.weekday().num_days_from_monday()));
            let days_from_anchor = (nd.date() - jan1_monday).num_days();
            let bucket = days_from_anchor.div_euclid(length.max(1)) * length.max(1);
            Ok((jan1_monday + chrono::Duration::days(bucket))
                .and_hms_opt(0, 0, 0)
                .unwrap())
        }
        "MONTH" => {
            let months_from_jan = i64::from(nd.month0());
            let bucket = months_from_jan.div_euclid(length.max(1)) * length.max(1);
            let m = bucket as u32 + 1;
            Ok(NaiveDate::from_ymd_opt(nd.year(), m, 1)
                .unwrap_or(nd.date())
                .and_hms_opt(0, 0, 0)
                .unwrap())
        }
        "QUARTER" => {
            let quarter_start_month = (i64::from(nd.month()) - 1) / 3 * 3 + 1;
            Ok(
                NaiveDate::from_ymd_opt(nd.year(), quarter_start_month as u32, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
            )
        }
        "YEAR" => Ok(NaiveDate::from_ymd_opt(nd.year(), 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()),
        other => datafusion_common::exec_err!("time_slice: unsupported date_part '{other}'"),
    }
}

impl ScalarUDFImpl for TimeSliceFunc {
    fn name(&self) -> &'static str {
        "time_slice"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let len = args.number_rows;
        let ts = to_ts_nanos(&args.args[0], len)?;
        let slice = cast(&args.args[1].to_array(len)?, &DataType::Int64)?;
        let unit = cast(&args.args[2].to_array(len)?, &DataType::Utf8)?;
        let ts = ts.as_primitive::<TimestampNanosecondType>();
        let slice = slice.as_primitive::<Int64Type>();
        let unit = unit.as_string::<i32>();

        let mut out: Vec<Option<i64>> = Vec::with_capacity(len);
        for i in 0..len {
            if ts.is_null(i) || slice.is_null(i) || unit.is_null(i) {
                out.push(None);
                continue;
            }
            if slice.value(i) <= 0 {
                return datafusion_common::exec_err!(
                    "time_slice: slice_length must be a positive integer"
                );
            }
            let ns = ts.value(i);
            let nd = NaiveDateTime::from_timestamp(
                ns.div_euclid(1_000_000_000),
                ns.rem_euclid(1_000_000_000) as u32,
            );
            let sliced = slice_start(nd, slice.value(i), unit.value(i))?;
            out.push(Some(sliced.and_utc().timestamp_nanos_opt().unwrap_or(0)));
        }

        Ok(ColumnarValue::Array(Arc::new(PrimitiveArray::<
            TimestampNanosecondType,
        >::from(out))))
    }
}

crate::macros::make_udf_function!(TimeSliceFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::StringArray;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_15_minutes() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(TimeSliceFunc::new()));
        let result = ctx
            .sql("SELECT TIME_SLICE('2026-01-15 10:37:00'::TIMESTAMP, 15, 'MINUTE')::VARCHAR;")
            .await?
            .collect()
            .await?;
        let casted = datafusion::arrow::compute::cast(
            result[0].column(0),
            &datafusion::arrow::datatypes::DataType::Utf8,
        )?;
        let got = casted
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert!(got.contains("2026-01-15T10:30:00"), "got {got}");
        Ok(())
    }
}
