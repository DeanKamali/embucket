use datafusion::arrow::array::{Array, BooleanArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use std::sync::Arc;

/// `IS_NOT_NULL` SQL function
///
/// Reports whether its single argument does not evaluate to NULL.
///
/// Syntax: IS_NOT_NULL(<expr>)
///
/// Example: SELECT IS_NOT_NULL(NULL), IS_NOT_NULL(1);
///
/// Returns: TRUE if the argument is not NULL, FALSE otherwise.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct IsNotNullFunc {
    signature: Signature,
}

impl Default for IsNotNullFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl IsNotNullFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for IsNotNullFunc {
    fn name(&self) -> &'static str {
        "is_not_null"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let arr = args.args[0].to_array(args.number_rows)?;
        let non_nulls: Vec<bool> = (0..arr.len()).map(|i| !arr.is_null(i)).collect();
        Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(
            non_nulls,
        ))))
    }
}

crate::macros::make_udf_function!(IsNotNullFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_it_works() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(IsNotNullFunc::new()));
        let result = ctx
            .sql("SELECT IS_NOT_NULL(NULL), IS_NOT_NULL(1);")
            .await?
            .collect()
            .await?;
        let batch = &result[0];
        assert!(!batch.column(0).is_null(0));
        assert!(!batch.column(1).is_null(0));
        Ok(())
    }
}
