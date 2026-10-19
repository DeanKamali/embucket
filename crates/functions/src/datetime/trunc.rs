use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike};
use datafusion::arrow::array::AsArray;
use datafusion::arrow::array::{Array, Float64Array, Int64Array, PrimitiveArray, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{
    DataType, Float64Type, Int64Type, TimeUnit, TimestampNanosecondType,
};
use datafusion::error::Result as DFResult;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::sync::Arc;

/// `TRUNC` SQL function (Snowflake overloads, unified)
///
/// 1. Numeric overload — `TRUNC(<numeric_expr> [, <scale>])`: truncates the
///    number toward zero to `scale` decimal places (default 0). The
///    Snowflake alias `TRUNCATE` maps here too.
/// 2. Date overload — `TRUNC(<date_or_time_expr>, <date_part>)`: truncates
///    the date or timestamp to the precision of `date_part` (YEAR, QUARTER,
///    MONTH, WEEK, DAY, HOUR, MINUTE, SECOND and their abbreviations).
///
/// Example: SELECT TRUNC(2.7), TRUNC('2026-03-15'::DATE, 'MONTH');
///
/// Returns: 2 and 2026-03-01.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TruncFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for TruncFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl TruncFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(1), TypeSignature::Any(2)],
                Volatility::Immutable,
            ),
            aliases: vec!["truncate".to_string()],
        }
    }
}

const PART_ALIASES: &[(&str, &str)] = &[
    ("YEAR", "YEAR"),
    ("YYYY", "YEAR"),
    ("YY", "YEAR"),
    ("Y", "YEAR"),
    ("QUARTER", "QUARTER"),
    ("QQQ", "QUARTER"),
    ("Q", "QUARTER"),
    ("MONTH", "MONTH"),
    ("MM", "MONTH"),
    ("MON", "MONTH"),
    ("WEEK", "WEEK"),
    ("WW", "WEEK"),
    ("W", "WEEK"),
    ("DAY", "DAY"),
    ("DD", "DAY"),
    ("D", "DAY"),
    ("HOUR", "HOUR"),
    ("HH", "HOUR"),
    ("H", "HOUR"),
    ("MINUTE", "MINUTE"),
    ("MI", "MINUTE"),
    ("N", "MINUTE"),
    ("SECOND", "SECOND"),
    ("SS", "SECOND"),
    ("S", "SECOND"),
];

fn normalize_part(p: &str) -> Option<&'static str> {
    let up = p.trim().to_uppercase();
    PART_ALIASES.iter().find(|(k, _)| *k == up).map(|(_, v)| *v)
}

/// Truncate a timestamp to the precision of `part`. WEEK truncates to the
/// Monday of the week (Snowflake default).
#[allow(clippy::too_many_lines)]
fn truncate_ts(nd: NaiveDateTime, part: &str) -> DFResult<NaiveDateTime> {
    let day_start = || -> NaiveDateTime { nd.date().and_hms_opt(0, 0, 0).unwrap() };
    match part {
        "YEAR" => Ok(NaiveDate::from_ymd_opt(nd.year(), 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()),
        "QUARTER" => {
            let m = (i64::from(nd.month()) - 1) / 3 * 3 + 1;
            Ok(NaiveDate::from_ymd_opt(nd.year(), m as u32, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap())
        }
        "MONTH" => Ok(NaiveDate::from_ymd_opt(nd.year(), nd.month(), 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()),
        "WEEK" => {
            let monday =
                nd.date() - chrono::Duration::days(i64::from(nd.weekday().num_days_from_monday()));
            Ok(monday.and_hms_opt(0, 0, 0).unwrap())
        }
        "DAY" => Ok(day_start()),
        "HOUR" => Ok(nd.date().and_hms_opt(nd.hour(), 0, 0).unwrap()),
        "MINUTE" => Ok(nd.date().and_hms_opt(nd.hour(), nd.minute(), 0).unwrap()),
        "SECOND" => {
            Ok(nd - chrono::Duration::nanoseconds(i64::from(nd.nanosecond() % 1_000_000_000)))
        }
        other => datafusion_common::exec_err!("trunc: unsupported date part '{other}'"),
    }
}

fn truncate_numeric(x: f64, scale: i64) -> f64 {
    let factor = 10_f64.powi(scale.clamp(-30, 30) as i32);
    (x * factor).trunc() / factor
}

impl ScalarUDFImpl for TruncFunc {
    fn name(&self) -> &'static str {
        "trunc"
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        let first = &arg_types[0];
        if matches!(
            first,
            DataType::Float16 | DataType::Float32 | DataType::Float64
        ) {
            return Ok(DataType::Float64);
        }
        if matches!(
            first,
            DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8
        ) {
            return Ok(DataType::Int64);
        }
        if matches!(
            first,
            DataType::Timestamp(_, _)
                | DataType::Date32
                | DataType::Date64
                | DataType::Time32(_)
                | DataType::Time64(_)
        ) {
            return Ok(DataType::Timestamp(TimeUnit::Nanosecond, None));
        }
        // Decimal / unknown: keep the input type for 1-arg numeric, or
        // timestamp for the date overload (2 args with a string part).
        if arg_types.len() == 2 {
            Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
        } else {
            Ok(first.clone())
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let len = args.number_rows;
        let first = args.args[0].to_array(len)?;

        // Date/time overload (2 args) — or 1-arg date/timestamp (TRUNC(ts)).
        let first_is_temporal = matches!(
            first.data_type(),
            DataType::Timestamp(_, _)
                | DataType::Date32
                | DataType::Date64
                | DataType::Time32(_)
                | DataType::Time64(_)
        );
        if args.args.len() == 2 || first_is_temporal {
            let part = if args.args.len() == 2 {
                let p = cast(&args.args[1].to_array(len)?, &DataType::Utf8)?;
                let p = p.as_string::<i32>();
                if p.is_null(0) {
                    return datafusion_common::exec_err!("trunc: date part must not be NULL");
                }
                normalize_part(p.value(0)).ok_or_else(|| {
                    datafusion_common::DataFusionError::Execution(format!(
                        "trunc: invalid date part '{}'",
                        p.value(0)
                    ))
                })?
            } else {
                "DAY"
            };
            let casted = cast(&first, &DataType::Timestamp(TimeUnit::Nanosecond, None))?;
            let ts = casted.as_primitive::<TimestampNanosecondType>();
            let mut out: Vec<Option<i64>> = Vec::with_capacity(len);
            for i in 0..len {
                if ts.is_null(i) {
                    out.push(None);
                    continue;
                }
                let ns = ts.value(i);
                let nd = NaiveDateTime::from_timestamp(
                    ns.div_euclid(1_000_000_000),
                    ns.rem_euclid(1_000_000_000) as u32,
                );
                let truncated = truncate_ts(nd, part)?;
                out.push(Some(truncated.and_utc().timestamp_nanos_opt().unwrap_or(0)));
            }
            return Ok(ColumnarValue::Array(Arc::new(PrimitiveArray::<
                TimestampNanosecondType,
            >::from(out))));
        }

        // Numeric overload.
        let scale: i64 = if args.args.len() == 2 {
            let s = cast(&args.args[1].to_array(len)?, &DataType::Int64)?;
            s.as_primitive::<Int64Type>().value(0)
        } else {
            0
        };
        let is_float = matches!(
            first.data_type(),
            DataType::Float16 | DataType::Float32 | DataType::Float64
        );
        if is_float {
            let f = cast(&first, &DataType::Float64)?;
            let f = f.as_primitive::<Float64Type>();
            let mut out: Vec<Option<f64>> = Vec::with_capacity(len);
            for i in 0..len {
                if f.is_null(i) {
                    out.push(None);
                } else {
                    out.push(Some(truncate_numeric(f.value(i), scale)));
                }
            }
            Ok(ColumnarValue::Array(Arc::new(Float64Array::from(out))))
        } else {
            let c = cast(&first, &DataType::Int64)?;
            let c = c.as_primitive::<Int64Type>();
            if scale >= 0 {
                // Integer truncation is a no-op (digits truncated past the
                // decimal point).
                return Ok(ColumnarValue::Array(Arc::new(c.clone())));
            }
            let factor = 10_i64.pow((-scale).clamp(0, 18) as u32);
            let mut out: Vec<Option<i64>> = Vec::with_capacity(len);
            for i in 0..len {
                if c.is_null(i) {
                    out.push(None);
                } else {
                    out.push(Some(c.value(i) / factor * factor));
                }
            }
            Ok(ColumnarValue::Array(Arc::new(Int64Array::from(out))))
        }
    }
}

crate::macros::make_udf_function!(TruncFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Float64Array, StringArray};
    use datafusion::prelude::SessionContext;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_both_overloads() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(TruncFunc::new()));
        let result = ctx
            .sql("SELECT TRUNC(2.7), TRUNC('2026-03-15'::DATE, 'MONTH')::VARCHAR, TRUNCATE(-2.7::FLOAT);")
            .await?
            .collect()
            .await?;
        let f = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert!((f - 2.0).abs() < 1e-9);
        let casted = datafusion::arrow::compute::cast(
            result[0].column(1),
            &datafusion::arrow::datatypes::DataType::Utf8,
        )?;
        let s = casted
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert!(s.starts_with("2026-03-01"), "got {s}");
        let f2 = result[0]
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert!((f2 + 2.0).abs() < 1e-9);
        Ok(())
    }
}
