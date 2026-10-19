use datafusion::arrow::array::{Array, AsArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use std::sync::Arc;

/// `IS_NULL` SQL function
///
/// Reports whether its single argument evaluates to NULL.
///
/// Syntax: IS_NULL(<expr>)
///
/// Example: SELECT IS_NULL(NULL), IS_NULL(1);
///
/// Returns: TRUE if the argument is NULL, FALSE otherwise.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct IsNullFunc {
    signature: Signature,
}

impl Default for IsNullFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl IsNullFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for IsNullFunc {
    fn name(&self) -> &'static str {
        "is_null"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let arr = args.args[0].to_array(args.number_rows)?;
        let nulls: Vec<bool> = (0..arr.len()).map(|i| arr.is_null(i)).collect();
        Ok(ColumnarValue::Array(Arc::new(
            datafusion::arrow::array::BooleanArray::from(nulls),
        )))
    }
}

crate::macros::make_udf_function!(IsNullFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::BooleanArray;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_it_works() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(IsNullFunc::new()));
        ctx.sql("CREATE TABLE t AS VALUES (NULL), (1);")
            .await?
            .collect()
            .await?;
        let result = ctx
            .sql("SELECT IS_NULL(column1) FROM t ORDER BY column1 NULLS FIRST;")
            .await?
            .collect()
            .await?;
        let b = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(b.value(0));
        assert!(!b.value(1));
        Ok(())
    }
}
