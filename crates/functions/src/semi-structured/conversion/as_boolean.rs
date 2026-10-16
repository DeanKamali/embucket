use crate::macros::make_udf_function;
use crate::semi_structured::conversion::common;
use datafusion::arrow::array::{BooleanArray};
use datafusion::arrow::datatypes::{DataType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use serde_json::Value;

/// `AS_BOOLEAN` — extracts a BOOLEAN from a VARIANT.
///
/// Syntax: `AS_BOOLEAN(<variant>)`
///
/// Returns the boolean value if the VARIANT holds a boolean, else NULL.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct AsBooleanFunc {
    signature: Signature,
}

impl Default for AsBooleanFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl AsBooleanFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for AsBooleanFunc {
    fn name(&self) -> &'static str {
        "as_boolean"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let len = common::input_len(&args)?;
        let values = common::json_values(&args.args)?;
        let out: BooleanArray = values
            .into_iter()
            .map(|v| match v {
                Some(Value::Bool(b)) => Some(b),
                _ => None,
            })
            .collect();
        common::to_columnar_value(out, len)
    }
}

make_udf_function!(AsBooleanFunc);