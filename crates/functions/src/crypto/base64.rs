use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use datafusion::arrow::array::AsArray;
use datafusion::arrow::array::{Array, ArrayRef, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use std::sync::Arc;

/// `BASE64_DECODE_STRING` SQL function
///
/// Decodes a Base64-encoded string back to the original UTF-8 string.
///
/// Syntax: BASE64_DECODE_STRING(<input>)
///
/// Arguments:
/// - input: Base64-encoded string
///
/// Example: SELECT BASE64_DECODE_STRING('aGVsbG8=');
///
/// Returns: 'hello'
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Base64DecodeStringFunc {
    signature: Signature,
}

impl Default for Base64DecodeStringFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl Base64DecodeStringFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

fn to_binary(v: &ColumnarValue, len: usize) -> DFResult<ArrayRef> {
    let arr = v.to_array(len)?;
    if matches!(arr.data_type(), DataType::Binary) {
        Ok(arr)
    } else {
        Ok(cast(&arr, &DataType::Binary)?)
    }
}

impl ScalarUDFImpl for Base64DecodeStringFunc {
    fn name(&self) -> &'static str {
        "base64_decode_string"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let len = args.number_rows;
        let input = to_binary(&args.args[0], len)?;
        let input = input.as_binary::<i32>();
        let mut out: Vec<Option<String>> = Vec::with_capacity(len);
        for i in 0..len {
            if input.is_null(i) {
                out.push(None);
                continue;
            }
            let decoded = BASE64_STANDARD.decode(input.value(i)).map_err(|e| {
                datafusion_common::DataFusionError::Execution(format!(
                    "base64_decode_string: invalid base64 input: {e}"
                ))
            })?;
            out.push(Some(String::from_utf8(decoded).map_err(|e| {
                datafusion_common::DataFusionError::Execution(format!(
                    "base64_decode_string: decoded value is not valid UTF-8: {e}"
                ))
            })?));
        }
        Ok(ColumnarValue::Array(Arc::new(StringArray::from(
            out.iter().map(|o| o.as_deref()).collect::<Vec<_>>(),
        ))))
    }
}

crate::macros::make_udf_function!(Base64DecodeStringFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_it_works() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(Base64DecodeStringFunc::new()));
        let result = ctx
            .sql("SELECT BASE64_DECODE_STRING('aGVsbG8=');")
            .await?
            .collect()
            .await?;
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 1);
        Ok(())
    }
}
