use datafusion::arrow::array::{new_null_array, Array, BooleanBuilder};
use datafusion::arrow::compute::cast;
use datafusion::arrow::compute::kernels::zip::zip;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion_common::exec_err;
use datafusion_expr::type_coercion::binary::comparison_coercion;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use std::sync::Arc;

// DECODE SQL function (Snowflake semantics).
// Compares <expr> to each <search> argument in order; if they are equal,
// returns the paired <result>. If no search matches, returns <default>
// when given, otherwise NULL.
// Syntax: DECODE( <expr> , <search1> , <result1> [, <searchN> , <resultN> ...]
//         [, <default> ] )
//
// NULL-safe comparison: two NULLs compare as equal (Snowflake DECODE uses
// EQUAL_NULL-like semantics), so `DECODE(v, NULL, 'null-val', 'other')`
// matches NULL values.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct DecodeFunc {
    signature: Signature,
}

impl Default for DecodeFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl DecodeFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

type ArrayRef = Arc<dyn datafusion::arrow::array::Array>;

/// Row-null check that also treats a Null-typed array as all-NULL
/// (arrow's `is_null` returns false for `NullArray`, which has no
/// validity bitmap).
fn row_null(arr: &ArrayRef, k: usize) -> bool {
    arr.data_type().is_null() || arr.is_null(k)
}

/// Index layout: arg 0 is the expr; the remaining args form
/// `pairs * 2 (+ 1 default when odd)`.
fn layout(arg_count: usize) -> DFResult<(usize, bool)> {
    let Some(remaining) = arg_count.checked_sub(1) else {
        return exec_err!("DECODE requires at least 2 arguments");
    };
    if remaining < 2 {
        return exec_err!("DECODE requires at least one search/result pair");
    }
    Ok((remaining / 2, remaining % 2 == 1))
}

/// Type of the results: the first non-Null result argument (Snowflake
/// returns the type of the first result).
fn result_type(arg_types: &[DataType]) -> DFResult<DataType> {
    layout(arg_types.len())?;
    // All args from index 2 onward are results (plus the optional default
    // last) — results and default share one coerced type.
    if let Some(t) = arg_types[2..].iter().find(|t| !t.is_null()) {
        Ok(t.clone())
    } else {
        exec_err!("DECODE result type could not be determined")
    }
}

impl ScalarUDFImpl for DecodeFunc {
    fn name(&self) -> &'static str {
        "decode"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        result_type(arg_types)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let arg_count = args.args.len();
        let (pairs, has_default) = layout(arg_count)?;
        let arg_types = args
            .args
            .iter()
            .map(ColumnarValue::data_type)
            .collect::<Vec<_>>();
        let return_dt = result_type(&arg_types)?;

        // Evaluate everything to row-length-normalized arrays.
        let num_rows = args
            .args
            .iter()
            .map(|v| match v {
                ColumnarValue::Array(a) => a.len(),
                ColumnarValue::Scalar(_) => 1,
            })
            .max()
            .unwrap_or(1);
        let value = |v: &ColumnarValue| -> DFResult<ArrayRef> {
            Ok(match v {
                ColumnarValue::Array(a) => Arc::clone(a),
                ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows)?,
            })
        };
        let expr = value(&args.args[0])?;
        let expr_dt = expr.data_type().clone();

        // Start from the default (or all-NULL) accumulator.
        let default_idx = if has_default { arg_count - 1 } else { usize::MAX };
        let mut acc: ArrayRef = if has_default {
            cast(&value(&args.args[default_idx])?, &return_dt)?
        } else {
            new_null_array(&return_dt, num_rows)
        };

        // Fold each (search, result) pair over the accumulator.
        let mut acc = acc;
        for i in 0..pairs {
            let search_idx = 1 + i * 2;
            let result_idx = search_idx + 1;
            let search = value(&args.args[search_idx])?;
            let search_dt = search.data_type().clone();
            let cmp_dt =
                comparison_coercion(&expr_dt, &search_dt).unwrap_or_else(|| expr_dt.clone());
            let lhs = cast(&expr, &cmp_dt)?;
            let rhs = cast(&search, &cmp_dt)?;

            // NULL-safe equality: rows where both sides are NULL match.
            // Arrow's cmp kernels do not compare Null-typed arrays; a Null
            // comparison can only match row-for-row both-NULL.
            if cmp_dt.is_null() {
                let mut mask = BooleanBuilder::with_capacity(num_rows);
                for k in 0..num_rows {
                    mask.append_value(row_null(&lhs, k) && row_null(&rhs, k));
                }
                let result = cast(&value(&args.args[result_idx])?, &return_dt)?;
                acc = zip(&mask.finish(), &result, &acc)?;
                continue;
            }
            let eq = datafusion::arrow::compute::kernels::cmp::eq(&lhs, &rhs)?;
            let mut mask = BooleanBuilder::with_capacity(num_rows);
            for k in 0..num_rows {
                if eq.is_null(k) {
                    mask.append_value(row_null(&lhs, k) && row_null(&rhs, k));
                } else {
                    mask.append_value(eq.value(k));
                }
            }
            let result = cast(&value(&args.args[result_idx])?, &return_dt)?;
            acc = zip(&mask.finish(), &result, &acc)?;
        }

        Ok(ColumnarValue::Array(acc))
    }
}

crate::macros::make_udf_function!(DecodeFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_decode_basic() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf((*get_udf()).clone());

        // Match, no-match -> default, no-match without default -> NULL.
        let sql = "SELECT DECODE('b','a','x','b','y','?') AS d1, \
                   DECODE('z','a','x','b','y','?') AS d2, \
                   DECODE('z','a','x','b','y') AS d3";
        let df = ctx.sql(sql).await?;
        let batches = df.collect().await?;
        assert_batches_eq!(
            [
                "+----+----+----+",
                "| d1 | d2 | d3 |",
                "+----+----+----+",
                "| y  | ?  |    |",
                "+----+----+----+",
            ],
            &batches
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_decode_null_match() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf((*get_udf()).clone());

        // NULL == NULL in DECODE (Snowflake semantics).
        let sql = "SELECT DECODE(NULL, NULL, 'null-val', 'other') AS d";
        let df = ctx.sql(sql).await?;
        let batches = df.collect().await?;
        assert_batches_eq!(
            [
                "+----------+",
                "| d        |",
                "+----------+",
                "| null-val |",
                "+----------+",
            ],
            &batches
        );
        Ok(())
    }
}
