use crate::macros::make_udf_function;
use crate::semi_structured::conversion::common;
use datafusion::arrow::array::{Float64Array};
use datafusion::arrow::datatypes::{DataType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use serde_json::Value;

/// `AS_DOUBLE` / `AS_REAL` — extracts a DOUBLE from a VARIANT.
///
/// Syntax: `AS_DOUBLE(<variant>)`
///
/// Returns the floating-point value if the VARIANT holds a number, else NULL.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct AsDoubleFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for AsDoubleFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl AsDoubleFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            aliases: vec!["as_real".to_string()],
        }
    }
}

impl ScalarUDFImpl for AsDoubleFunc {
    fn name(&self) -> &'static str {
        "as_double"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let len = common::input_len(&args)?;
        let values = common::json_values(&args.args)?;
        let out: Float64Array = values
            .into_iter()
            .map(|v| match v {
                Some(Value::Number(n)) => n.as_f64(),
                _ => None,
            })
            .collect();
        common::to_columnar_value(out, len)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

make_udf_function!(AsDoubleFunc);