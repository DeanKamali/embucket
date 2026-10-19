use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use datafusion::arrow::array::AsArray;
use datafusion::arrow::array::{Array, ArrayRef, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::sync::Arc;

/// `BASE64_ENCODE` SQL function
///
/// Encodes the input (string or binary) using Base64 and returns the
/// encoded string.
///
/// Syntax: BASE64_ENCODE(<input>)
///
/// Arguments:
/// - input: string or binary expression to encode
///
/// Example: SELECT BASE64_ENCODE('hello');
///
/// Returns: 'aGVsbG8='
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Base64EncodeFunc {
    signature: Signature,
}

impl Default for Base64EncodeFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl Base64EncodeFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(vec![TypeSignature::Any(1)], Volatility::Immutable),
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

impl ScalarUDFImpl for Base64EncodeFunc {
    fn name(&self) -> &'static str {
        "base64_encode"
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
            out.push(Some(BASE64_STANDARD.encode(input.value(i))));
        }
        Ok(ColumnarValue::Array(Arc::new(StringArray::from(
            out.iter().map(|o| o.as_deref()).collect::<Vec<_>>(),
        ))))
    }
}

crate::macros::make_udf_function!(Base64EncodeFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_it_works() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(Base64EncodeFunc::new()));
        let result = ctx
            .sql("SELECT BASE64_ENCODE('hello');")
            .await?
            .collect()
            .await?;
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 1);
        Ok(())
    }
}
