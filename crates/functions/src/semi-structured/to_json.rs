use datafusion::arrow::array::{Array, AsArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use serde_json::Value as JsonValue;
use std::sync::Arc;

/// `TO_JSON` SQL function
///
/// Converts a VARIANT (or JSON string produced by `PARSE_JSON`) to a
/// string containing the JSON representation of the value. The inverse of
/// `PARSE_JSON`.
///
/// Syntax: TO_JSON(<expr>)
///
/// Arguments:
/// - expr: a VARIANT value (or JSON string) to serialize
///
/// Example: SELECT TO_JSON(PARSE_JSON('{"a": [1,2]}'));
///
/// Returns: '{"a":[1,2]}'
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ToJsonFunc {
    signature: Signature,
}

impl Default for ToJsonFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ToJsonFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ToJsonFunc {
    fn name(&self) -> &'static str {
        "to_json"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let arr = args.args[0].to_array(args.number_rows)?;
        let casted = datafusion::arrow::compute::cast(&arr, &DataType::Utf8)?;
        let input = casted.as_string::<i32>();
        let mut out: Vec<Option<String>> = Vec::with_capacity(input.len());
        for i in 0..input.len() {
            if input.is_null(i) {
                out.push(None);
                continue;
            }
            let raw = input.value(i);
            // PARSE_JSON output is a JSON document string; re-serialize it
            // in canonical compact form. Inputs that are already compact
            // JSON pass through unchanged semantically.
            match serde_json::from_str::<JsonValue>(raw) {
                Ok(v) => out.push(Some(v.to_string())),
                Err(_) => {
                    return datafusion_common::exec_err!("to_json: input is not valid JSON: {raw}");
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(StringArray::from(
            out.iter().map(|o| o.as_deref()).collect::<Vec<_>>(),
        ))))
    }
}

crate::macros::make_udf_function!(ToJsonFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::StringArray;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_it_works() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToJsonFunc::new()));
        let result = ctx
            .sql("SELECT TO_JSON('{\"a\": [1,2]}');")
            .await?
            .collect()
            .await?;
        let got = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert_eq!(got, "{\"a\":[1,2]}");
        Ok(())
    }
}
