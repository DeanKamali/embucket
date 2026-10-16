use crate::macros::make_udf_function;
use crate::semi_structured::conversion::common;
use datafusion::arrow::array::{Int64Array};
use datafusion::arrow::datatypes::{DataType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use serde_json::Value;

/// `AS_INTEGER` — extracts an INTEGER from a VARIANT.
///
/// Syntax: `AS_INTEGER(<variant>)`
///
/// Returns the integer value if the VARIANT holds an integral number, else
/// NULL (a floating-point value yields NULL, as in Snowflake).
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct AsIntegerFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for AsIntegerFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl AsIntegerFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            aliases: vec!["as_int".to_string(), "as_bigint".to_string()],
        }
    }
}

impl ScalarUDFImpl for AsIntegerFunc {
    fn name(&self) -> &'static str {
        "as_integer"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let len = common::input_len(&args)?;
        let values = common::json_values(&args.args)?;
        let out: Int64Array = values
            .into_iter()
            .map(|v| match v {
                Some(Value::Number(n)) => n.as_i64(),
                _ => None,
            })
            .collect();
        common::to_columnar_value(out, len)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

make_udf_function!(AsIntegerFunc);