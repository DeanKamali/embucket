use crate::server::helpers::handle_query_ok_result;
use executor::models::QueryResult;
use datafusion::arrow::array::{Int64Array, NullArray, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use executor::utils::DataSerializationFormat;
use std::sync::Arc;
use uuid::Uuid;

#[tokio::test]
async fn null_projection_serializes() {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("created_on", DataType::Null, true),
        Field::new("x", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(NullArray::new(1)),
            Arc::new(Int64Array::from(vec![1])),
        ],
    )
    .unwrap();
    let qr = QueryResult::new(vec![batch], schema);
    let resp = handle_query_ok_result(
        "SELECT NULL as created_on, 1 as x",
        Uuid::new_v4(),
        qr,
        DataSerializationFormat::Json,
    )
    .unwrap();
    assert!(resp.success);
}