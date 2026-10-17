use crate::SqlState;
use crate::models::{ColumnInfo, JsonResponse, ResponseData, RowSet};
use crate::server::error::{self as api_snowflake_rest_error, Error, Result};
use base64;
use base64::engine::general_purpose::STANDARD as engine_base64;
use base64::prelude::*;
use datafusion::arrow::ipc::MetadataVersion;
use datafusion::arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use datafusion::arrow::json::{StructMode, WriterBuilder, writer::JsonArray};
use datafusion::arrow::record_batch::RecordBatch;
use executor::models::QueryResult;
use executor::utils::{
    DataSerializationFormat, convert_record_batches, convert_struct_to_timestamp,
};
use snafu::ResultExt;
use tracing;
use uuid::Uuid;

// https://arrow.apache.org/docs/format/Columnar.html#buffer-alignment-and-padding
// Buffer Alignment and Padding: Implementations are recommended to allocate memory
// on aligned addresses (multiple of 8- or 64-bytes) and pad (overallocate) to a
// length that is a multiple of 8 or 64 bytes. When serializing Arrow data for interprocess
// communication, these alignment and padding requirements are enforced.
// For more info see issue #115
const ARROW_IPC_ALIGNMENT: usize = 8;

fn records_to_arrow_string(recs: &[RecordBatch]) -> std::result::Result<String, Error> {
    let mut buf = Vec::new();
    let options = IpcWriteOptions::try_new(ARROW_IPC_ALIGNMENT, false, MetadataVersion::V5)
        .context(api_snowflake_rest_error::ArrowSnafu)?;
    if !recs.is_empty() {
        let mut writer =
            StreamWriter::try_new_with_options(&mut buf, recs[0].schema_ref(), options)
                .context(api_snowflake_rest_error::ArrowSnafu)?;
        for rec in recs {
            writer
                .write(rec)
                .context(api_snowflake_rest_error::ArrowSnafu)?;
        }
        writer
            .finish()
            .context(api_snowflake_rest_error::ArrowSnafu)?;
        drop(writer);
    }
    Ok(engine_base64.encode(buf))
}

/// Stringify rowset cells for the JSON wire format (M13): real Snowflake
/// serializes every JSON rowset cell as a string (numbers, dates, booleans —
/// everything except null); the typed shape is carried by `rowtype`. The
/// driver core rejects native JSON types ("invalid type: integer … expected
/// a string").
fn stringify_json_columns(recs: &[RecordBatch]) -> std::result::Result<Vec<RecordBatch>, Error> {
    use datafusion::arrow::array::ArrayRef;
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::DataType;
    use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
    use std::sync::Arc;

    let opts = FormatOptions::default()
        .with_date_format(Some("%Y-%m-%d"))
        .with_time_format(Some("%H:%M:%S%.f"))
        .with_timestamp_format(Some("%Y-%m-%d %H:%M:%S%.f"))
        .with_timestamp_tz_format(Some("%Y-%m-%d %H:%M:%S%.f %:z"));

    let mut out = Vec::with_capacity(recs.len());
    for batch in recs {
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
        let mut fields = Vec::with_capacity(batch.num_columns());
        for (i, column) in batch.columns().iter().enumerate() {
            let field = batch.schema_ref().field(i).clone();
            // Snowflake JSON rowsets are all-string (the core's rowset deserializer
                // rejects native numbers — "invalid type: integer, expected a string").
                let stringified: ArrayRef = match field.data_type() {
                DataType::Null | DataType::Utf8 | DataType::LargeUtf8 => Arc::clone(column),
                DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Float16
                | DataType::Float32
                | DataType::Float64
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _) => {
                    // Native scalars: format via display (keeps fixed-point
                    // rendering identical to Snowflake text rendering).
                    let formatter =
                        ArrayFormatter::try_new(column.as_ref(), &opts)
                            .context(api_snowflake_rest_error::ArrowSnafu)?;
                    let strings: datafusion::arrow::array::StringArray = (0..column.len())
                        .map(|i| {
                            if column.is_null(i) {
                                None
                            } else {
                                Some(formatter.value(i).to_string())
                            }
                        })
                        .collect();
                    Arc::new(strings)
                }
                DataType::Date32 | DataType::Date64 | DataType::Time32(_) | DataType::Time64(_)
                | DataType::Timestamp(_, _) => {
                    let formatter =
                        ArrayFormatter::try_new(column.as_ref(), &opts)
                            .context(api_snowflake_rest_error::ArrowSnafu)?;
                    let strings: datafusion::arrow::array::StringArray = (0..column.len())
                        .map(|i| {
                            if column.is_null(i) {
                                None
                            } else {
                                Some(formatter.value(i).to_string())
                            }
                        })
                        .collect();
                    Arc::new(strings)
                }
                _ => cast(column, &DataType::Utf8).unwrap_or_else(|_| Arc::clone(column)),
            };
            let field_type = stringified.data_type().clone();
            columns.push(stringified);
            fields.push(datafusion::arrow::datatypes::Field::new(
                field.name().clone(),
                // Derive from the actual array: Null columns keep their type
                // (cast may also fail for exotic types — leave as-is).
                field_type,
                field.is_nullable(),
            ));
        }
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(fields));
        out.push(
            RecordBatch::try_new(schema, columns)
                .context(api_snowflake_rest_error::ArrowSnafu)?,
        );
    }
    Ok(out)
}

fn records_to_json_string(recs: &[RecordBatch]) -> std::result::Result<String, Error> {
    let recs = recs.iter().collect::<Vec<_>>();

    let buf = Vec::new();
    let mut writer = WriterBuilder::new()
        .with_struct_mode(StructMode::ListOnly)
        .with_explicit_nulls(true)
        .build::<_, JsonArray>(buf);

    writer
        .write_batches(&recs)
        .context(api_snowflake_rest_error::ArrowSnafu)?;
    writer
        .finish()
        .context(api_snowflake_rest_error::ArrowSnafu)?;
    let buf = writer.into_inner();
    // it is expected to be cheap, as no allocations just reuses underlying buffer
    String::from_utf8(buf).context(api_snowflake_rest_error::Utf8Snafu)
}

/// Snowflake statement-type classification for the wire response (M13). The
/// connector core classifies by family nibble: `0x1000` cursor (SELECT),
/// `0x3xxx` DML (rowcount from `stats`), `0x4xxx` session/system (rowcount 1),
/// `0x5xxx` TCL, `0x6xxx` DDL.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WireStatementType {
    Select,
    Insert,
    Update,
    Delete,
    Merge,
    Copy,
    NoResult(u16),
}

impl WireStatementType {
    /// Statement type id over the wire.
    pub fn id(self) -> i64 {
        match self {
            Self::Select => 0x1000,
            Self::Insert => 0x3001,
            Self::Update => 0x3002,
            Self::Delete => 0x3003,
            Self::Merge => 0x3004,
            Self::Copy => 0x3005,
            Self::NoResult(family) => i64::from(family),
        }
    }

    fn dml_kind(self) -> Option<&'static str> {
        match self {
            Self::Insert => Some("inserted"),
            Self::Update | Self::Merge => Some("updated"),
            Self::Delete => Some("deleted"),
            Self::Copy => Some("copied"),
            _ => None,
        }
    }
}

/// Best-effort statement classification from the SQL text (first keyword).
fn classify_statement(sql_text: &str) -> WireStatementType {
    let trimmed = sql_text.trim_start();
    // Skip leading line comments and parens.
    let mut rest = trimmed;
    loop {
        if let Some(after) = rest.strip_prefix("--") {
            rest = after.split_once('\n').map_or("", |(_, r)| r);
        } else if rest.starts_with("/*") {
            rest = match rest.split_once("*/") {
                Some((_, r)) => r,
                None => "",
            };
        } else if rest.starts_with('(') {
            rest = rest[1..].trim_start();
        } else {
            break;
        }
    }
    let first = rest
        .split(|c: char| c.is_whitespace() || c == '(')
        .find(|t| !t.is_empty())
        .unwrap_or_default()
        .to_ascii_uppercase();
    match first.as_str() {
        "SELECT" | "WITH" => WireStatementType::Select,
        "INSERT" | "PUT" => WireStatementType::Insert,
        "UPDATE" => WireStatementType::Update,
        "DELETE" => WireStatementType::Delete,
        "MERGE" => WireStatementType::Merge,
        "COPY" => WireStatementType::Copy,
        "BEGIN" => WireStatementType::NoResult(0x5000),
        "COMMIT" => WireStatementType::NoResult(0x5100),
        "ROLLBACK" => WireStatementType::NoResult(0x5400),
        "CREATE" | "DROP" | "ALTER" | "TRUNCATE" | "GRANT" | "REVOKE" | "UNDROP" | "RENAME" => {
            WireStatementType::NoResult(0x6000)
        }
        "USE" => WireStatementType::NoResult(0x4300),
        "SET" | "ALTERSESSION" => WireStatementType::NoResult(0x4100),
        "SHOW" => WireStatementType::NoResult(0x4400),
        "DESCRIBE" | "DESC" => WireStatementType::NoResult(0x4500),
        _ => WireStatementType::NoResult(0x8000),
    }
}

/// Affected-row count from a DML result: the executor returns a single-row,
/// single-numeric-column count for INSERT/UPDATE/DELETE/MERGE.
fn extract_dml_affected(records: &[RecordBatch]) -> Option<i64> {
    if records.len() != 1 || records[0].num_rows() != 1 || records[0].num_columns() != 1 {
        return None;
    }
    let col = records[0].column(0).as_ref();
    use datafusion::arrow::array::{Int32Array, Int64Array, StringArray, UInt32Array, UInt64Array};
    if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
        return arr.value(0).try_into().ok();
    }
    if let Some(arr) = col.as_any().downcast_ref::<Int32Array>() {
        return i64::from(arr.value(0)).try_into().ok();
    }
    if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
        return i64::try_from(arr.value(0)).ok();
    }
    if let Some(arr) = col.as_any().downcast_ref::<UInt32Array>() {
        return Some(i64::from(arr.value(0)));
    }
    // Stringified count (JSON serialization may have run first).
    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
        return arr.value(0).parse().ok();
    }
    None
}

fn dml_stats(stmt: WireStatementType, affected: Option<i64>) -> Option<crate::models::DmlStats> {
    let affected = affected?;
    let kind = stmt.dml_kind()?;
    let mut stats = crate::models::DmlStats {
        num_rows_inserted: 0,
        num_rows_updated: 0,
        num_rows_deleted: 0,
        num_dml_duplicates: 0,
    };
    match kind {
        "inserted" => stats.num_rows_inserted = affected,
        "updated" => stats.num_rows_updated = affected,
        "deleted" => stats.num_rows_deleted = affected,
        "copied" => stats.num_rows_inserted = affected,
        _ => return None,
    }
    Some(stats)
}

#[tracing::instrument(
    name = "handle_query_ok_result",
    level = "debug",
    err,
    ret(level = tracing::Level::TRACE)
)]
pub fn handle_query_ok_result(
    sql_text: &str,
    query_id: Uuid,
    query_result: QueryResult,
    ser_fmt: DataSerializationFormat,
) -> Result<JsonResponse> {
    let stmt = classify_statement(sql_text);
    let affected = extract_dml_affected(&query_result.records);
    let stats = dml_stats(stmt, affected);
    // Convert the QueryResult to RecordBatches using the specified serialization format
    // Add columns dbt metadata to each field
    let records = convert_record_batches(&query_result, ser_fmt)?;
    let total_rows = records
        .iter()
        .map(|batch| i64::try_from(batch.num_rows()).unwrap_or(i64::MAX))
        .sum();

    let row_set = if ser_fmt == DataSerializationFormat::Json {
        // Convert struct timestamp columns to string representation, then
        // stringify every cell (Snowflake JSON rowsets are all-string).
        let records = convert_struct_to_timestamp(&records)?;
        let records = stringify_json_columns(&records)?;
        let serialized_rowset = records_to_json_string(&records)?;
        Some(RowSet::Raw(serialized_rowset))
    } else {
        None
    };
    let row_set_base_64 = if ser_fmt == DataSerializationFormat::Arrow {
        Option::from(records_to_arrow_string(&records)?)
    } else {
        None
    };
    let returned_rows = total_rows;

    let json_resp = JsonResponse {
        data: Option::from(ResponseData {
            row_type: {
                let mut rt: Vec<ColumnInfo> = query_result
                    .column_info()
                    .into_iter()
                    .map(Into::into)
                    .collect();
                // DML count columns must carry Snowflake's canonical names — the driver
                // core keys affected-row extraction on them. INSERT/UPDATE/
                // DELETE/COPY return one count column; MERGE returns two
                // (inserted, updated) in that order. A multi-column COPY INTO
                // result is the Snowflake-shaped per-file load report and is
                // passed through untouched.
                if let Some(kind) = stmt.dml_kind() {
                    if matches!(stmt, WireStatementType::Merge) {
                        let names = ["number of rows inserted", "number of rows updated"];
                        for (c, n) in rt.iter_mut().zip(names) {
                            c.set_name(n.to_string());
                        }
                    } else if rt.len() == 1
                        && let Some(c) = rt.first_mut()
                    {
                        c.set_name(format!("number of rows {kind}"));
                    }
                }
                rt
            },
            query_result_format: Some(ser_fmt.to_string().to_lowercase()),
            row_set,
            row_set_base_64,
            total: Some(total_rows),
            returned: Some(returned_rows),
            query_id: Some(query_id.to_string()),
            statement_type_id: Some(stmt.id()),
            stats,
            error_code: None,
            sql_state: Some(SqlState::Success.to_string()),
        }),
        success: true,
        message: Option::from("successfully executed".to_string()),
        code: None,
    };
    Ok(json_resp)
}
