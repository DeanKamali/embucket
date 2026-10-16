use crate::macros::make_udf_function;
use crate::semi_structured::conversion::common;
use datafusion::arrow::array::{Date32Array};
use datafusion::arrow::datatypes::{DataType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use chrono::Datelike;
use serde_json::Value;

/// `AS_DATE` — extracts a DATE from a VARIANT.
///
/// Syntax: `AS_DATE(<variant>)`
///
/// Returns the date if the VARIANT holds a `YYYY-MM-DD` string, else NULL.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct AsDateFunc {
    signature: Signature,
}

impl Default for AsDateFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl AsDateFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for AsDateFunc {
    fn name(&self) -> &'static str {
        "as_date"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Date32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let len = common::input_len(&args)?;
        let values = common::json_values(&args.args)?;
        let out: Date32Array = values
            .into_iter()
            .map(|v| match v {
                Some(Value::String(s)) => s
                    .split(['T', ' '])
                    .next()
                    .and_then(|d| chrono::NaiveDate::parse_from_str(d, "%Y-%m-%d").ok())
                    // Date32 = days since 1970-01-01 (not since year 1 CE).
                    .map(|d| {
                        d.signed_duration_since(chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                                .expect("epoch is a valid date"))
                            .num_days() as i32
                    }),
                _ => None,
            })
            .collect();
        common::to_columnar_value(out, len)
    }
}

make_udf_function!(AsDateFunc);