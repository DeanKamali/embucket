use crate::macros::make_udf_function;
use crate::semi_structured::conversion::common;
use datafusion::arrow::array::{StringArray};
use datafusion::arrow::datatypes::{DataType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use serde_json::Value;

/// `AS_CHAR` / `AS_VARCHAR` — extracts a VARCHAR from a VARIANT.
///
/// Syntax: `AS_CHAR(<variant>)`
///
/// Returns the string value if the VARIANT holds a string, else NULL.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct AsCharFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for AsCharFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl AsCharFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            aliases: vec!["as_varchar".to_string()],
        }
    }
}

impl ScalarUDFImpl for AsCharFunc {
    fn name(&self) -> &'static str {
        "as_char"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let len = common::input_len(&args)?;
        let values = common::json_values(&args.args)?;
        let out: StringArray = values
            .into_iter()
            .map(|v| match v {
                Some(Value::String(s)) => Some(s),
                _ => None,
            })
            .collect();
        common::to_columnar_value(out, len)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

make_udf_function!(AsCharFunc);