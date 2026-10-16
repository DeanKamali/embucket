use crate::macros::make_udf_function;
use crate::semi_structured::conversion::common;
use datafusion::arrow::array::{BinaryArray};
use datafusion::arrow::datatypes::{DataType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use serde_json::Value;

/// `AS_BINARY` — extracts a BINARY from a VARIANT.
///
/// Syntax: `AS_BINARY(<variant>)`
///
/// Snowflake stores binary values inside VARIANT as a hex-encoded string;
/// the function decodes it. Returns NULL for anything else.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct AsBinaryFunc {
    signature: Signature,
}

impl Default for AsBinaryFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl AsBinaryFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for AsBinaryFunc {
    fn name(&self) -> &'static str {
        "as_binary"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Binary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let len = common::input_len(&args)?;
        let values = common::json_values(&args.args)?;
        let out: BinaryArray = values
            .into_iter()
            .map(|v| match v {
                Some(Value::String(s)) => {
                    if s.len() % 2 == 0 {
                        (0..s.len())
                            .step_by(2)
                            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
                            .collect::<Option<Vec<u8>>>()
                    } else {
                        None
                    }
                }
                _ => None,
            })
            .collect();
        common::to_columnar_value(out, len)
    }
}

make_udf_function!(AsBinaryFunc);