use crate::macros::make_udf_function;
use crate::semi_structured::conversion::common;
use datafusion::arrow::array::{Time64NanosecondArray};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use chrono::Timelike;
use serde_json::Value;

/// `AS_TIME` — extracts a TIME from a VARIANT.
///
/// Syntax: `AS_TIME(<variant>)`
///
/// Returns the time if the VARIANT holds a `HH:MM:SS[.ffffff]` string,
/// else NULL.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct AsTimeFunc {
    signature: Signature,
}

impl Default for AsTimeFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl AsTimeFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for AsTimeFunc {
    fn name(&self) -> &'static str {
        "as_time"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Time64(TimeUnit::Nanosecond))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let len = common::input_len(&args)?;
        let values = common::json_values(&args.args)?;
        let out: Time64NanosecondArray = values
            .into_iter()
            .map(|v| match v {
                Some(Value::String(s)) => chrono::NaiveTime::parse_from_str(&s, "%H:%M:%S%.f")
                    .ok()
                    .map(|t| t.num_seconds_from_midnight() as i64 * 1_000_000_000 + t.nanosecond() as i64),
                _ => None,
            })
            .collect();
        common::to_columnar_value(out, len)
    }
}

make_udf_function!(AsTimeFunc);
