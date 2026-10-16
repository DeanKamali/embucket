//! Shared plumbing for the Snowflake `AS_*` VARIANT-extraction family.
//!
//! VARIANT columns are string-typed JSON (see `variant/visitors/type_rewrite.rs`);
//! each `AS_<TYPE>` returns the value if the underlying JSON value holds that
//! type, and NULL otherwise — Snowflake semantics where a failed typed
//! extraction yields NULL instead of an error.

use datafusion::arrow::array::{Array, ArrayRef};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::ScalarValue;
use datafusion_expr::ScalarFunctionArgs;
use serde_json::Value;
use std::sync::Arc;

/// Extract the per-row JSON value of the first argument (cast to Utf8).
/// `None` for NULL / unparsable rows.
#[must_use]
pub fn json_values(args: &[ColumnarValue]) -> DFResult<Vec<Option<Value>>> {
    let arr = match &args[0] {
        ColumnarValue::Array(arr) => arr.clone(),
        ColumnarValue::Scalar(v) => v.to_array()?,
    };
    let strings = cast(&arr, &DataType::Utf8)?;
    let strings = strings
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .ok_or_else(|| {
            datafusion_common::exec_datafusion_err!("AS_* function requires a VARIANT (string) argument")
        })?;
    Ok(strings
        .iter()
        // Snowflake implicitly casts a VARCHAR argument to VARIANT, so a
        // string that isn't valid JSON is a string-typed VARIANT (e.g. the
        // result of `col:key` on a string member, which arrives unquoted).
        // Treat it as `Value::String(raw)`.
        .map(|opt| {
            opt.map(|s| {
                serde_json::from_str(s).unwrap_or_else(|_| Value::String(s.to_string()))
            })
        })
        .collect())
}

/// Wrap the result as a `ColumnarValue`, collapsing single-row results to scalars.
pub fn to_columnar_value<A: Array + 'static>(array: A, len: usize) -> DFResult<ColumnarValue> {
    let arr: ArrayRef = Arc::new(array);
    Ok(if len == 1 {
        ColumnarValue::Scalar(ScalarValue::try_from_array(&arr, 0)?)
    } else {
        ColumnarValue::Array(arr)
    })
}

/// Number of rows implied by the first argument (never zero for scalars).
pub fn input_len(args: &ScalarFunctionArgs) -> DFResult<usize> {
    args.args[0]
        .clone()
        .into_array(args.number_rows.max(1))
        .map(|a| a.len())
}
