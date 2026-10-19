use crate::array_to_boolean;
use datafusion::arrow::array::builder::BooleanBuilder;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use std::sync::Arc;

/// `BOOLNOT` SQL function
///
/// Computes the Boolean NOT of a single numeric expression. In accordance
/// with Boolean semantics:
///
/// Syntax: BOOLNOT(<expr1>)
///
/// Arguments:
/// - <expr1>: Non-zero values (including negative numbers) are regarded as
///   True. Zero values are regarded as False.
///
/// Example: SELECT BOOLNOT(1), BOOLNOT(0), BOOLNOT(NULL);
///
/// Returns:
/// - True if the expression is zero.
/// - False if the expression is non-zero.
/// - NULL if the expression is NULL.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct BoolNotFunc {
    signature: Signature,
}

impl Default for BoolNotFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl BoolNotFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::comparable(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for BoolNotFunc {
    fn name(&self) -> &'static str {
        "boolnot"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let input = match &args.args[0] {
            ColumnarValue::Scalar(v) => array_to_boolean(&v.to_array()?),
            ColumnarValue::Array(arr) => array_to_boolean(arr),
        }?;

        let mut b = BooleanBuilder::with_capacity(input.len());
        for val in &input {
            match val {
                None => b.append_null(),
                Some(v) => b.append_value(!v),
            }
        }

        Ok(ColumnarValue::Array(Arc::new(b.finish())))
    }
}

crate::macros::make_udf_function!(BoolNotFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, BooleanArray};
    use datafusion::prelude::SessionContext;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_it_works() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(BoolNotFunc::new()));
        let result = ctx
            .sql("SELECT BOOLNOT(1), BOOLNOT(0), BOOLNOT(-2), BOOLNOT(NULL);")
            .await?
            .collect()
            .await?;
        let col = |i: usize| {
            result[0]
                .column(i)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(0)
        };
        let col_null = |i: usize| {
            result[0]
                .column(i)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .is_null(0)
        };
        assert!(!col(0));
        assert!(col(1));
        assert!(!col(2));
        assert!(col_null(3));
        Ok(())
    }
}
