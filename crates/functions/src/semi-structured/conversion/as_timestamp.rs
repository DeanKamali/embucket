use crate::semi_structured::conversion::common;
use datafusion::arrow::array::{TimestampNanosecondArray};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimestampKind {
    Ntz,
    Ltz,
    Tz,
}

use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use serde_json::Value;

/// `AS_TIMESTAMP_NTZ` / `AS_TIMESTAMP_LTZ` / `AS_TIMESTAMP_TZ` — extracts a
/// TIMESTAMP from a VARIANT.
///
/// Syntax: `AS_TIMESTAMP_<FLAVOR>(<variant>)`
///
/// Returns the timestamp if the VARIANT holds an ISO-8601 timestamp string,
/// else NULL. `NTZ` keeps the wall-clock value; `LTZ`/`TZ` normalize
/// offset-bearing values to UTC.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct AsTimestampFunc {
    signature: Signature,
    kind: TimestampKind,
}

impl AsTimestampFunc {
    #[must_use]
    pub fn new(kind: TimestampKind) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            kind,
        }
    }

    #[must_use]
    pub const fn name_of(kind: TimestampKind) -> &'static str {
        match kind {
            TimestampKind::Ntz => "as_timestamp_ntz",
            TimestampKind::Ltz => "as_timestamp_ltz",
            TimestampKind::Tz => "as_timestamp_tz",
        }
    }
}

impl ScalarUDFImpl for AsTimestampFunc {
    fn name(&self) -> &'static str {
        Self::name_of(self.kind)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(match self.kind {
            TimestampKind::Ntz => DataType::Timestamp(TimeUnit::Nanosecond, None),
            TimestampKind::Ltz | TimestampKind::Tz => {
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
            }
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let len = common::input_len(&args)?;
        let values = common::json_values(&args.args)?;
        let out: TimestampNanosecondArray = values
            .into_iter()
            .map(|v| match v {
                Some(Value::String(s)) => {
                    let normalized = s.replace('T', " ");
                    if matches!(self.kind, TimestampKind::Ntz) {
                        chrono::NaiveDateTime::parse_from_str(&normalized, "%Y-%m-%d %H:%M:%S%.f")
                            .ok()
                            .and_then(|t| t.and_utc().timestamp_nanos_opt())
                    } else {
                        chrono::DateTime::parse_from_rfc3339(&s)
                            .ok()
                            .and_then(|t| t.with_timezone(&chrono::Utc).timestamp_nanos_opt())
                    }
                }
                _ => None,
            })
            .collect();
        // LTZ/TZ promise a UTC-tz'd timestamp at planning time; tag the array
        // so the physical result type matches.
        let out = if matches!(self.kind, TimestampKind::Ntz) {
            out
        } else {
            out.with_timezone("UTC")
        };
        common::to_columnar_value(out, len)
    }
}

