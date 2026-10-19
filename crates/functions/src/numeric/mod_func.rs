use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use std::sync::Arc;

/// `MOD` SQL function
///
/// Returns the remainder of `n1` divided by `n2`.
/// Sign of the result follows the sign of `n1` (like Snowflake/`%`).
///
/// Syntax: MOD(<n1>, <n2>)
///
/// Arguments:
/// - n1: dividend (integer or float)
/// - n2: divisor (integer or float); zero divisor errors
///
/// Example: SELECT MOD(10, 3), MOD(2.5, 2);
///
/// Returns:
/// - Integer remainder for integer inputs, float remainder otherwise.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ModFunc {
    signature: Signature,
}

impl Default for ModFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ModFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::comparable(2, Volatility::Immutable),
        }
    }
}

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn rem_arrays(a: &ArrayRef, b: &ArrayRef, len: usize) -> DFResult<ArrayRef> {
    use datafusion::arrow::array::{
        Float64Array, Int64Array, Scalar as ScalarArray, new_null_array,
    };

    // Cast both sides to a common numeric representation.
    let float_mode = matches!(
        a.data_type(),
        DataType::Float16 | DataType::Float32 | DataType::Float64 | DataType::Decimal128(_, _)
    ) || matches!(
        b.data_type(),
        DataType::Float16 | DataType::Float32 | DataType::Float64 | DataType::Decimal128(_, _)
    );

    if float_mode {
        let a = datafusion::arrow::compute::cast(a, &DataType::Float64)?;
        let b = datafusion::arrow::compute::cast(b, &DataType::Float64)?;
        let a = a.as_primitive::<datafusion::arrow::datatypes::Float64Type>();
        let b = b.as_primitive::<datafusion::arrow::datatypes::Float64Type>();
        let mut out = Vec::with_capacity(len);
        for (x, y) in a.iter().zip(b.iter()) {
            match (x, y) {
                (Some(x), Some(y)) => {
                    if y == 0.0 {
                        return datafusion_common::exec_err!("Division by zero");
                    }
                    out.push(Some(x % y));
                }
                _ => out.push(None),
            }
        }
        Ok(Arc::new(Float64Array::from(out)))
    } else {
        let a = datafusion::arrow::compute::cast(a, &DataType::Int64)?;
        let b = datafusion::arrow::compute::cast(b, &DataType::Int64)?;
        let a = a.as_primitive::<datafusion::arrow::datatypes::Int64Type>();
        let b = b.as_primitive::<datafusion::arrow::datatypes::Int64Type>();
        let mut out = Vec::with_capacity(len);
        for (x, y) in a.iter().zip(b.iter()) {
            match (x, y) {
                (Some(x), Some(y)) => {
                    if y == 0 {
                        return datafusion_common::exec_err!("Division by zero");
                    }
                    out.push(Some(x % y));
                }
                _ => out.push(None),
            }
        }
        Ok(Arc::new(Int64Array::from(out)))
    }
}

impl ScalarUDFImpl for ModFunc {
    fn name(&self) -> &'static str {
        "mod"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        let float_mode = arg_types.iter().any(|t| {
            matches!(
                t,
                DataType::Float16
                    | DataType::Float32
                    | DataType::Float64
                    | DataType::Decimal128(_, _)
            )
        });
        if float_mode {
            Ok(DataType::Float64)
        } else {
            Ok(DataType::Int64)
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let len = args.number_rows;
        let a = args.args[0].to_array(args.number_rows)?;
        let b = args.args[1].to_array(args.number_rows)?;
        let out = rem_arrays(&a, &b, len)?;
        Ok(ColumnarValue::Array(out))
    }
}

crate::macros::make_udf_function!(ModFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Float64Array, Int64Array};
    use datafusion::prelude::SessionContext;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_it_works() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ModFunc::new()));
        let result = ctx
            .sql("SELECT MOD(10, 3), MOD(-10, 3), MOD(2.5, 2);")
            .await?
            .collect()
            .await?;
        let c0 = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let c1 = result[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let f = result[0]
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert_eq!(c0, 1);
        assert_eq!(c1, -1);
        assert!((f - 0.5).abs() < 1e-9);
        Ok(())
    }
}
