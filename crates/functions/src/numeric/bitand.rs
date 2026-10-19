use datafusion::arrow::array::AsArray;
use datafusion::arrow::array::{Array, ArrayRef, Int64Array};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Int64Type};
use datafusion::error::Result as DFResult;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use std::sync::Arc;

/// Scalar bitwise function (Snowflake `BITAND`).
///
/// Operates on two integer expressions and returns the bitwise AND as an
/// integer. NULL inputs propagate to NULL.
///
/// Syntax: BITAND(<expr1>, <expr2>)
///
/// Example: SELECT BITAND(6, 3);
///
/// Returns: 2.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct BitAndFunc {
    signature: Signature,
}

impl Default for BitAndFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl BitAndFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::comparable(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for BitAndFunc {
    fn name(&self) -> &'static str {
        "bitand"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let len = args.number_rows;
        let a = args.args[0].to_array(args.number_rows)?;
        let b = args.args[1].to_array(args.number_rows)?;
        let a = cast(&a, &DataType::Int64)?;
        let b = cast(&b, &DataType::Int64)?;
        let a = a.as_primitive::<Int64Type>();
        let b = b.as_primitive::<Int64Type>();
        let mut out: Vec<Option<i64>> = Vec::with_capacity(len);
        for (x, y) in a.iter().zip(b.iter()) {
            match (x, y) {
                (Some(x), Some(y)) => out.push(Some(x & y)),
                _ => out.push(None),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(Int64Array::from(out))))
    }
}

crate::macros::make_udf_function!(BitAndFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_it_works() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(BitAndFunc::new()));
        let result = ctx.sql("SELECT BITAND(6, 3);").await?.collect().await?;
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 1);
        Ok(())
    }
}
