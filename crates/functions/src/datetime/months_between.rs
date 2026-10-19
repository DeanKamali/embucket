use chrono::{Datelike, NaiveDateTime, Timelike};
use datafusion::arrow::array::AsArray;
use datafusion::arrow::array::{Array, Float64Array};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{
    DataType, Date32Type, Date64Type, TimeUnit, TimestampNanosecondType,
};
use datafusion::error::Result as DFResult;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use std::sync::Arc;

/// `MONTHS_BETWEEN` SQL function
///
/// Returns the number of months between two dates/timestamps. When both
/// inputs have the same day-of-month (and no time components, or identical
/// time components) the result is an exact integer; otherwise a fractional
/// component is computed on the 31-day scale, matching Snowflake semantics.
///
/// Syntax: MONTHS_BETWEEN(<date_expr1>, <date_expr2>)
///
/// Arguments:
/// - date_expr1: the later date expression
/// - date_expr2: the earlier date expression
///
/// Example: SELECT MONTHS_BETWEEN('2026-03-15'::DATE, '2026-01-15'::DATE);
///
/// Returns: 2.0
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct MonthsBetweenFunc {
    signature: Signature,
}

impl Default for MonthsBetweenFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl MonthsBetweenFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

/// A resolved input row: (year, month, day, seconds-of-day) plus whether
/// any time component was present (pure DATE inputs have none).
struct Resolved {
    year: i32,
    month: u32,
    day: u32,
    secs: f64,
    has_time: bool,
}

fn from_ts_nanos(ns: i64) -> Resolved {
    let nd = NaiveDateTime::from_timestamp(
        ns.div_euclid(1_000_000_000),
        ns.rem_euclid(1_000_000_000) as u32,
    );
    let secs = f64::from(nd.hour() * 3600 + nd.minute() * 60 + nd.second())
        + f64::from(nd.nanosecond() % 1_000_000_000) / 1e9;
    Resolved {
        year: nd.year(),
        month: nd.month(),
        day: nd.day(),
        secs,
        has_time: true,
    }
}

/// Resolve one argument column to per-row `(Resolved, is_null)` pairs.
fn resolve(v: &ColumnarValue, len: usize) -> DFResult<(Vec<Option<Resolved>>, bool)> {
    let arr = v.to_array(len)?;
    let mut has_time = false;
    let rows = match arr.data_type() {
        DataType::Date32 => {
            let p = arr.as_primitive::<Date32Type>();
            (0..len)
                .map(|i| {
                    if p.is_null(i) {
                        None
                    } else {
                        let days = i64::from(p.value(i));
                        let nd = NaiveDateTime::from_timestamp(days * 86_400, 0);
                        Some(Resolved {
                            year: nd.year(),
                            month: nd.month(),
                            day: nd.day(),
                            secs: 0.0,
                            has_time: false,
                        })
                    }
                })
                .collect::<Vec<_>>()
        }
        DataType::Date64 => {
            let p = arr.as_primitive::<Date64Type>();
            (0..len)
                .map(|i| {
                    if p.is_null(i) {
                        None
                    } else {
                        let ms = p.value(i);
                        let nd = NaiveDateTime::from_timestamp(
                            ms.div_euclid(1000),
                            (ms.rem_euclid(1000) as u32) * 1_000_000,
                        );
                        Some(Resolved {
                            year: nd.year(),
                            month: nd.month(),
                            day: nd.day(),
                            secs: 0.0,
                            has_time: false,
                        })
                    }
                })
                .collect::<Vec<_>>()
        }
        _ => {
            has_time = true;
            let casted = cast(&arr, &DataType::Timestamp(TimeUnit::Nanosecond, None))?;
            let p = casted.as_primitive::<TimestampNanosecondType>();
            (0..len)
                .map(|i| {
                    if p.is_null(i) {
                        None
                    } else {
                        Some(from_ts_nanos(p.value(i)))
                    }
                })
                .collect::<Vec<_>>()
        }
    };
    Ok((rows, has_time))
}

impl ScalarUDFImpl for MonthsBetweenFunc {
    fn name(&self) -> &'static str {
        "months_between"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let len = args.number_rows;
        let (a, a_any_time) = resolve(&args.args[0], len)?;
        let (b, b_any_time) = resolve(&args.args[1], len)?;
        let any_time = a_any_time || b_any_time;

        let mut out: Vec<Option<f64>> = Vec::with_capacity(len);
        for i in 0..len {
            match (&a[i], &b[i]) {
                (Some(x), Some(y)) => {
                    let whole = f64::from((x.year - y.year) * 12)
                        + f64::from(x.month as i32 - y.month as i32);
                    let same_day_no_time = x.day == y.day && (!any_time || x.secs == y.secs);
                    if same_day_no_time {
                        out.push(Some(whole));
                    } else {
                        let fraction = (f64::from(x.day) + x.secs / 86_400.0
                            - (f64::from(y.day) + y.secs / 86_400.0))
                            / 31.0;
                        out.push(Some(whole + fraction));
                    }
                }
                _ => out.push(None),
            }
        }

        Ok(ColumnarValue::Array(Arc::new(Float64Array::from(out))))
    }
}

crate::macros::make_udf_function!(MonthsBetweenFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Float64Array;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_it_works() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(MonthsBetweenFunc::new()));
        let result = ctx
            .sql("SELECT MONTHS_BETWEEN('2026-03-15'::DATE, '2026-01-15'::DATE), MONTHS_BETWEEN('2026-03-20'::DATE, '2026-01-15'::DATE);")
            .await?
            .collect()
            .await?;
        let c0 = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        let c1 = result[0]
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert!((c0 - 2.0).abs() < 1e-9);
        // Snowflake: (5 days extra) / 31 = 0.16129...
        assert!((c1 - (2.0 + 5.0 / 31.0)).abs() < 1e-9, "got {c1}");
        Ok(())
    }
}
