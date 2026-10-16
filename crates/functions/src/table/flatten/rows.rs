//! Correlated FLATTEN support.
//!
//! The `FLATTEN` table function cannot receive correlated (column-referencing)
//! arguments: table-function args are planned against an empty schema, so
//! `FROM t, LATERAL FLATTEN(INPUT => t.col)` either fails or falls back to
//! evaluating the input once against a single value (wrong for multi-row
//! inputs — see the M16.3.1 spike in the design doc).
//!
//! Instead, the AST visitor [`crate::visitors::flatten_correlation`] rewrites a
//! correlated flatten factor into a derived table that UNNESTs six parallel
//! list columns produced by the scalar UDFs in this module:
//!
//! ```sql
//! FROM (SELECT *,
//!         UNNEST(__flatten_seq(input, path, outer, recursive, mode)) AS "SEQ",
//!         UNNEST(__flatten_key(...)) AS "KEY",
//!         UNNEST(__flatten_path(...)) AS "PATH",
//!         UNNEST(__flatten_index(...)) AS "INDEX",
//!         UNNEST(__flatten_value(...)) AS "VALUE",
//!         UNNEST(__flatten_this(...)) AS "THIS"
//!       FROM <original FROM items minus flatten factors>)
//! ```
//!
//! Each `__flatten_<field>` UDF evaluates its input expression **per row**
//! (all column references resolve naturally against the inner FROM) and
//! returns the corresponding FLATTEN output column for that row as a list.
//! DataFusion zips sibling UNNEST expressions positionally, reproducing
//! lateral-flatten semantics. All six UDFs share one core so their lists are
//! always equal-length per row.
//!
//! Semantics mirror `FlattenTableProvider` for single-row inputs (the
//! pre-existing snapshots stay stable) and extend them to multi-row inputs:
//! - NULL input: `OUTER=false` drops the row; `OUTER=true` emits one row of
//!   NULLs (SEQ set to the input row ordinal, matching `append_null`).
//! - No expansion (path not found, scalar at path, empty container):
//!   `OUTER=false` drops the row; `OUTER=true` emits one row with
//!   `PATH=<path>` and `THIS=<container>` when the container is an
//!   array/object matching MODE (mirrors `empty_record_batch`).
//! - SEQ is the 1-based input row ordinal (Snowflake: "the sequence number of
//!   the record" — per input row when flattening a table column).
use crate::json::{get_json_value, tokenize_path};
use crate::table::flatten::func::{FlattenTableFunc, path_to_string};
use crate::table::flatten::provider::{FlattenMode, Out};
use datafusion::arrow::array::{Array, ArrayRef, ListArray, StringArray, UInt64Array};
use datafusion::arrow::buffer::{OffsetBuffer, ScalarBuffer};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion_common::Result as DFResult;
use datafusion_common::exec_err;
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use serde_json::Value;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

/// Placeholder SEQ-id array for non-SEQ field variants (never read).
static NULL_SEQ_IDS: std::sync::LazyLock<UInt64Array> =
    std::sync::LazyLock::new(|| UInt64Array::new_null(0));

/// Which FLATTEN output column a `__flatten_*` UDF produces.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FlattenField {
    Seq,
    Key,
    Path,
    Index,
    Value,
    This,
}

impl FlattenField {
    fn name(self) -> &'static str {
        match self {
            Self::Seq => "__flatten_seq",
            Self::Key => "__flatten_key",
            Self::Path => "__flatten_path",
            Self::Index => "__flatten_index",
            Self::Value => "__flatten_value",
            Self::This => "__flatten_this",
        }
    }

    fn data_type(self) -> DataType {
        match self {
            Self::Seq | Self::Index => DataType::UInt64,
            Self::Key | Self::Path | Self::Value | Self::This => DataType::Utf8,
        }
    }
}

/// Per-input-row FLATTEN output, one vector per output column.
#[derive(Debug, Default)]
struct FlattenRowOutputs {
    seq: Vec<Option<u64>>,
    key: Vec<Option<String>>,
    path: Vec<Option<String>>,
    index: Vec<Option<u64>>,
    value: Vec<Option<String>>,
    this: Vec<Option<String>>,
}

impl FlattenRowOutputs {
    fn len(&self) -> usize {
        self.seq.len()
    }
}

/// Computes the FLATTEN output for a single input row.
///
/// `seq` is the input row ordinal (from `ROW_NUMBER() OVER ()`) used as the
/// SEQ value, repeated across the row's output records.
#[allow(clippy::too_many_lines, clippy::unwrap_used, clippy::as_conversions)]
fn flatten_row_outputs(
    input: Option<&str>,
    path: Option<&str>,
    is_outer: bool,
    is_recursive: bool,
    mode: FlattenMode,
    seq: u64,
) -> DFResult<FlattenRowOutputs> {
    let mut out = FlattenRowOutputs::default();

    // NULL input: OUTER=TRUE keeps one all-NULL row (SEQ set), else dropped.
    let Some(input) = input else {
        if is_outer {
            out.seq.push(Some(seq));
            out.key.push(None);
            out.path.push(None);
            out.index.push(None);
            out.value.push(None);
            out.this.push(None);
        }
        return Ok(out);
    };

    let path_str = path.unwrap_or_default();
    let tokens = if path.is_some() {
        tokenize_path(path_str).ok_or_else(|| {
            datafusion_common::DataFusionError::Execution("Invalid JSON path".to_string())
        })?
    } else {
        vec![]
    };

    let json: Value =
        serde_json::from_str(input).map_err(|e| datafusion_common::DataFusionError::Execution(
            format!("Failed to deserialize JSON: {e}"),
        ))?;

    let Some(container) = get_json_value(&json, &tokens) else {
        // Path not found: OUTER=TRUE keeps one row with PATH set, else dropped.
        if is_outer {
            out.seq.push(Some(seq));
            out.key.push(None);
            out.path.push(Some(path_to_string(&tokens)));
            out.index.push(None);
            out.value.push(None);
            out.this.push(None);
        }
        return Ok(out);
    };

    // Reuse the table function's flattening logic so semantics (PATH/Index/
    // VALUE/THIS formatting, recursion, mode filtering) stay identical.
    let func = FlattenTableFunc::new();
    func.row_id.store(seq, std::sync::atomic::Ordering::Release);
    let builders = Rc::new(RefCell::new(Out {
        seq: datafusion::arrow::array::UInt64Builder::new(),
        key: datafusion::arrow::array::StringBuilder::new(),
        path: datafusion::arrow::array::StringBuilder::new(),
        index: datafusion::arrow::array::UInt64Builder::new(),
        value: datafusion::arrow::array::StringBuilder::new(),
        this: datafusion::arrow::array::StringBuilder::new(),
        last_outer: None,
    }));
    func.flatten(container, &tokens, is_outer, is_recursive, &mode, &builders)?;

    let mut builders = builders.borrow_mut();
    let seq_arr = builders.seq.finish();
    let key_arr = builders.key.finish();
    let path_arr = builders.path.finish();
    let index_arr = builders.index.finish();
    let value_arr = builders.value.finish();
    let this_arr = builders.this.finish();
    let last_outer = builders
        .last_outer
        .as_ref()
        .map(|v| serde_json::to_string_pretty(v).unwrap_or_default());

    if seq_arr.is_empty() {
        // No expansion (scalar at path, or empty array/object):
        // OUTER=TRUE keeps one row mirroring `empty_record_batch`.
        if is_outer {
            out.seq.push(Some(seq));
            out.key.push(None);
            out.path.push(Some(path_to_string(&tokens)));
            out.index.push(None);
            out.value.push(None);
            out.this.push(last_outer);
        }
        return Ok(out);
    }

    for i in 0..seq_arr.len() {
        out.seq.push(Some(seq_arr.value(i)));
        out.key.push(if key_arr.is_null(i) { None } else { Some(key_arr.value(i).to_string()) });
        out.path.push(if path_arr.is_null(i) { None } else { Some(path_arr.value(i).to_string()) });
        out.index.push(if index_arr.is_null(i) { None } else { Some(index_arr.value(i)) });
        out.value.push(if value_arr.is_null(i) { None } else { Some(value_arr.value(i).to_string()) });
        out.this.push(if this_arr.is_null(i) { None } else { Some(this_arr.value(i).to_string()) });
    }
    Ok(out)
}

/// Scalar UDF producing one FLATTEN output column per input row as a list.
///
/// Arguments: `INPUT`, `PATH`, `IS_OUTER`, `IS_RECURSIVE`, `MODE` (defaults:
/// `''`, `false`, `false`, `'both'`). The `__flatten_seq` variant takes an
/// additional 6th argument: the input row's ordinal (computed by the rewrite
/// as `ROW_NUMBER() OVER ()`), repeated across the row's output records —
/// matching Snowflake's SEQ ("the row number of the input record").
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct FlattenRowsFunc {
    field: FlattenField,
    signature: Signature,
}

impl FlattenRowsFunc {
    #[must_use]
    pub fn new(field: FlattenField) -> Self {
        Self {
            field,
            signature: Signature::any(
                if matches!(field, FlattenField::Seq) { 6 } else { 5 },
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for FlattenRowsFunc {
    fn name(&self) -> &'static str {
        self.field.name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            self.field.data_type(),
            true,
        ))))
    }

    #[allow(clippy::too_many_lines)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let n = args.number_rows;
        let expected = if matches!(self.field, FlattenField::Seq) { 6 } else { 5 };
        let arrays: Vec<ArrayRef> = args
            .args
            .iter()
            .map(|v| v.to_array_of_size(n))
            .collect::<DFResult<Vec<_>>>()?;
        if arrays.len() != expected {
            return exec_err!("{} expects {expected} arguments", self.name());
        }

        let inputs = datafusion::arrow::compute::cast(&arrays[0], &DataType::Utf8)?;
        let paths = datafusion::arrow::compute::cast(&arrays[1], &DataType::Utf8)?;
        let outers = datafusion::arrow::compute::cast(&arrays[2], &DataType::Boolean)?;
        let recursives = datafusion::arrow::compute::cast(&arrays[3], &DataType::Boolean)?;
        let modes = datafusion::arrow::compute::cast(&arrays[4], &DataType::Utf8)?;
        // SEQ source: the input row ordinal supplied by the rewrite.
        let seq_ids = if expected == 6 {
            datafusion::arrow::compute::cast(&arrays[5], &DataType::UInt64)?
        } else {
            arrays[0].clone() // unused placeholder for non-SEQ fields
        };
        let inputs = inputs.as_any().downcast_ref::<StringArray>();
        let paths = paths.as_any().downcast_ref::<StringArray>();
        let outers = outers.as_any().downcast_ref::<datafusion::arrow::array::BooleanArray>();
        let recursives = recursives
            .as_any()
            .downcast_ref::<datafusion::arrow::array::BooleanArray>();
        let modes = modes.as_any().downcast_ref::<StringArray>();
        let seq_ids = if expected == 6 {
            seq_ids.as_any().downcast_ref::<UInt64Array>()
        } else {
            None
        };
        let (Some(inputs), Some(paths), Some(outers), Some(recursives), Some(modes)) =
            (inputs, paths, outers, recursives, modes)
        else {
            return exec_err!("{} argument cast failed", self.name());
        };
        if expected == 6 && seq_ids.is_none() {
            return exec_err!("{} argument cast failed", self.name());
        }

        // Collect per-row outputs for the requested field.
        let mut num_lists: Vec<Vec<Option<u64>>> = Vec::with_capacity(n);
        let mut text_lists: Vec<Vec<Option<String>>> = Vec::with_capacity(n);
        let mut offsets: Vec<i32> = Vec::with_capacity(n + 1);
        offsets.push(0);

        for i in 0..n {
            let mode = match modes.value(i).to_lowercase().as_str() {
                "object" => FlattenMode::Object,
                "array" => FlattenMode::Array,
                _ => FlattenMode::Both,
            };
            let seq_value = if expected == 6 {
                let ids = seq_ids.unwrap_or(&NULL_SEQ_IDS);
                if ids.is_null(i) {
                    1
                } else {
                    ids.value(i)
                }
            } else {
                1
            };
            let row = flatten_row_outputs(
                (!inputs.is_null(i)).then(|| inputs.value(i)),
                (!paths.is_null(i)).then(|| paths.value(i)),
                outers.is_null(i) || outers.value(i),
                recursives.is_null(i) || recursives.value(i),
                mode,
                seq_value,
            )?;
            offsets.push(offsets.last().unwrap_or(&0) + row.len() as i32);
            match self.field {
                FlattenField::Seq => num_lists.push(row.seq),
                FlattenField::Index => num_lists.push(row.index),
                FlattenField::Key => text_lists.push(row.key),
                FlattenField::Path => text_lists.push(row.path),
                FlattenField::Value => text_lists.push(row.value),
                FlattenField::This => text_lists.push(row.this),
            }
        }

        let field = Arc::new(Field::new("item", self.field.data_type(), true));
        let values: ArrayRef = match self.field {
            FlattenField::Seq | FlattenField::Index => {
                let flat: Vec<Option<u64>> =
                    num_lists.iter().flat_map(|v| v.iter().copied()).collect();
                Arc::new(UInt64Array::from(flat))
            }
            _ => {
                let flat: Vec<Option<String>> =
                    text_lists.iter().flat_map(|v| v.iter().cloned()).collect();
                Arc::new(StringArray::from(flat))
            }
        };
        let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets));
        let list = ListArray::new(field, offsets, values, None);

        Ok(ColumnarValue::Array(Arc::new(list)))
    }
}

/// Registers the six `__flatten_*` scalar UDFs.
pub fn register_udfs(
    registry: &mut dyn datafusion::execution::FunctionRegistry,
) -> DFResult<()> {
    for field in [
        FlattenField::Seq,
        FlattenField::Key,
        FlattenField::Path,
        FlattenField::Index,
        FlattenField::Value,
        FlattenField::This,
    ] {
        registry.register_udf(Arc::new(datafusion_expr::ScalarUDF::from(
            FlattenRowsFunc::new(field),
        )))?;
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;

    #[tokio::test]
    async fn test_lists_basic() -> DFResult<()> {
        let ctx = SessionContext::new();
        for field in [
            FlattenField::Seq,
            FlattenField::Key,
            FlattenField::Path,
            FlattenField::Index,
            FlattenField::Value,
            FlattenField::This,
        ] {
            ctx.register_udf(datafusion_expr::ScalarUDF::from(FlattenRowsFunc::new(field)));
        }
        // Multi-row correlated input: two rows, each an object with an array.
        let sql = r#"
            SELECT __flatten_seq(v, 'arr', false, false, 'both', 1) AS seq,
                   __flatten_key(v, 'arr', false, false, 'both') AS key,
                   __flatten_path(v, 'arr', false, false, 'both') AS path,
                   __flatten_index(v, 'arr', false, false, 'both') AS idx,
                   __flatten_value(v, 'arr', false, false, 'both') AS val,
                   __flatten_this(v, 'arr', false, false, 'both') AS this
            FROM (VALUES ('{"arr": [10, 20]}'), ('{"arr": [30]}'), ('{"x": 1}'), (NULL)) t(v)"#;
        let result = ctx.sql(sql).await?.collect().await?;
        // Rows 3 (no 'arr' path) and 4 (NULL input) yield empty lists.
        assert_batches_eq!(
            &[
                "+--------+------+------------------+--------+----------+-------+",
                "| seq    | key  | path             | idx    | val      | this  |",
                "+--------+------+------------------+--------+----------+-------+",
                "| [1, 1] | [, ] | [arr[0], arr[1]] | [0, 1] | [10, 20] | [[    |",
                "|        |      |                  |        |          |   10, |",
                "|        |      |                  |        |          |   20  |",
                "|        |      |                  |        |          | ], [  |",
                "|        |      |                  |        |          |   10, |",
                "|        |      |                  |        |          |   20  |",
                "|        |      |                  |        |          | ]]    |",
                "| [1]    | []   | [arr[0]]         | [0]    | [30]     | [[    |",
                "|        |      |                  |        |          |   30  |",
                "|        |      |                  |        |          | ]]    |",
                "| []     | []   | []               | []     | []       | []    |",
                "| []     | []   | []               | []     | []       | []    |",
                "+--------+------+------------------+--------+----------+-------+",
            ],
            &result
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_lists_unnest_zip() -> DFResult<()> {
        let ctx = SessionContext::new();
        for field in [
            FlattenField::Seq,
            FlattenField::Key,
            FlattenField::Path,
            FlattenField::Index,
            FlattenField::Value,
            FlattenField::This,
        ] {
            ctx.register_udf(datafusion_expr::ScalarUDF::from(FlattenRowsFunc::new(field)));
        }
        let sql = r#"
            SELECT v, seq, idx, val FROM (
              SELECT v,
                     UNNEST(__flatten_seq(v, 'arr', false, false, 'both', 1)) AS seq,
                     UNNEST(__flatten_index(v, 'arr', false, false, 'both')) AS idx,
                     UNNEST(__flatten_value(v, 'arr', false, false, 'both')) AS val
              FROM (VALUES ('{"arr": [10, 20]}'), ('{"arr": [30]}'), ('{"x": 1}')) t(v))"#;
        let result = ctx.sql(sql).await?.collect().await?;
        // Lateral semantics: outer rows repeat per element; row without
        // expansion drops (OUTER=false).
        assert_batches_eq!(
            &[
                "+-------------------+-----+-----+-----+",
                "| v                 | seq | idx | val |",
                "+-------------------+-----+-----+-----+",
                "| {\"arr\": [10, 20]} | 1   | 0   | 10  |",
                "| {\"arr\": [10, 20]} | 1   | 1   | 20  |",
                "| {\"arr\": [30]}     | 1   | 0   | 30  |",
                "+-------------------+-----+-----+-----+",
            ],
            &result
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_lists_outer() -> DFResult<()> {
        let ctx = SessionContext::new();
        for field in [
            FlattenField::Seq,
            FlattenField::Path,
            FlattenField::Value,
            FlattenField::This,
        ] {
            ctx.register_udf(datafusion_expr::ScalarUDF::from(FlattenRowsFunc::new(field)));
        }
        // OUTER=true: rows without expansion still produce one row.
        let sql = r#"
            SELECT v, seq, path, val, this FROM (
              SELECT v,
                     UNNEST(__flatten_seq(v, 'arr', true, false, 'both', 1)) AS seq,
                     UNNEST(__flatten_path(v, 'arr', true, false, 'both')) AS path,
                     UNNEST(__flatten_value(v, 'arr', true, false, 'both')) AS val,
                     UNNEST(__flatten_this(v, 'arr', true, false, 'both')) AS this
              FROM (VALUES ('{"arr": [10]}'), ('{"x": 1}'), (NULL)) t(v))"#;
        let result = ctx.sql(sql).await?.collect().await?;
        println!("{result:#?}");
        Ok(())
    }
}
