pub mod as_array;
pub mod as_binary;
pub mod as_boolean;
pub mod as_char;
pub mod as_date;
pub mod as_decimal;
pub mod as_double;
pub mod as_integer;
pub mod as_object;
pub mod as_time;
pub mod as_timestamp;
pub mod common;

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::StringArray;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    fn ctx_with_as_funcs() -> SessionContext {
        use std::sync::Arc;
        let ctx = SessionContext::new();
        let funcs: Vec<Arc<ScalarUDF>> = vec![
            as_char::get_udf(),
            as_integer::get_udf(),
            as_double::get_udf(),
            as_boolean::get_udf(),
            as_binary::get_udf(),
            as_date::get_udf(),
            as_time::get_udf(),
            Arc::new(ScalarUDF::from(as_timestamp::AsTimestampFunc::new(
                as_timestamp::TimestampKind::Ntz,
            ))),
            Arc::new(ScalarUDF::from(as_timestamp::AsTimestampFunc::new(
                as_timestamp::TimestampKind::Ltz,
            ))),
            as_decimal::get_udf(),
            as_array::get_udf(),
            as_object::get_udf(),
        ];
        for f in funcs {
            ctx.register_udf(Arc::unwrap_or_clone(f));
        }
        ctx
    }

    async fn eval(ctx: &SessionContext, q: &str) -> Vec<RecordBatch> {
        ctx.sql(q)
            .await
            .expect("sql should parse/plan")
            .collect()
            .await
            .expect("collect should succeed")
    }

    /// Typed extraction returns the value only when the underlying JSON value
    /// holds that type, and NULL otherwise (Snowflake semantics).
    #[tokio::test]
    async fn test_typed_extraction_match_and_mismatch() {
        let ctx = ctx_with_as_funcs();
        let r = eval(
            &ctx,
            r#"SELECT
                AS_CHAR('42') a,
                AS_CHAR('"abc"') b,
                AS_INTEGER('42') c,
                AS_INTEGER('"42"') d,
                AS_INTEGER('3.5') e,
                AS_DOUBLE('3.5') f,
                AS_BOOLEAN('true') g,
                AS_BOOLEAN('"true"') h"#,
        )
        .await;
        assert_batches_eq!(
            &[
                "+---+-----+----+---+---+-----+------+---+",
                "| a | b   | c  | d | e | f   | g    | h |",
                "+---+-----+----+---+---+-----+------+---+",
                "|   | abc | 42 |   |   | 3.5 | true |   |",
                "+---+-----+----+---+---+-----+------+---+",
            ],
            &r
        );
    }

    #[tokio::test]
    async fn test_aliases() {
        let ctx = ctx_with_as_funcs();
        let r = eval(
            &ctx,
            r#"SELECT AS_INT('5') a, AS_BIGINT('7') b, AS_VARCHAR('"x"') c"#,
        )
        .await;
        assert_batches_eq!(
            &[
                "+---+---+---+",
                "| a | b | c |",
                "+---+---+---+",
                "| 5 | 7 | x |",
                "+---+---+---+",
            ],
            &r
        );
    }

    #[tokio::test]
    async fn test_date_time_timestamp_extraction() {
        let ctx = ctx_with_as_funcs();
        // Valid date/time/timestamp strings extract; wrong-typed values are NULL.
        let r = eval(
            &ctx,
            r#"SELECT
                AS_DATE('"2024-01-15"') IS NOT NULL a,
                AS_DATE('"2024-01-15T10:00:00"') IS NOT NULL b,
                AS_DATE('"not a date"') IS NULL c,
                AS_TIME('"13:30:00"') IS NOT NULL d,
                AS_TIME('"25:99:99"') IS NULL e,
                AS_TIMESTAMP_NTZ('"2024-01-15 10:30:00"') IS NOT NULL f,
                AS_TIMESTAMP_LTZ('"2024-01-15T10:30:00Z"') IS NOT NULL g,
                AS_TIMESTAMP_LTZ('"2024-01-15 10:30:00"') IS NULL h"#,
        )
        .await;
        assert_batches_eq!(
            &[
                "+------+------+------+------+------+------+------+------+",
                "| a    | b    | c    | d    | e    | f    | g    | h    |",
                "+------+------+------+------+------+------+------+------+",
                "| true | true | true | true | true | true | true | true |",
                "+------+------+------+------+------+------+------+------+",
            ],
            &r
        );
    }

    #[tokio::test]
    async fn test_binary_hex_decode() {
        let ctx = ctx_with_as_funcs();
        // Snowflake stores binary in VARIANT as a hex string; '4142' -> "AB".
        let r = eval(&ctx, r#"SELECT AS_BINARY('"4142"') IS NOT NULL a, AS_BINARY('"xyz"') IS NULL b, AS_BINARY('"414"') IS NULL c"#).await;
        assert_batches_eq!(
            &[
                "+------+------+------+",
                "| a    | b    | c    |",
                "+------+------+------+",
                "| true | true | true |",
                "+------+------+------+",
            ],
            &r
        );
    }

    #[tokio::test]
    async fn test_decimal_args() {
        let ctx = ctx_with_as_funcs();
        let r = eval(
            &ctx,
            r#"SELECT CAST(AS_DECIMAL('42') AS VARCHAR) a, CAST(AS_DECIMAL('3.5', 10, 1) AS VARCHAR) b"#,
        )
        .await;
        assert_batches_eq!(
            &[
                "+----+-----+",
                "| a  | b   |",
                "+----+-----+",
                "| 42 | 3.5 |",
                "+----+-----+",
            ],
            &r
        );
    }

    #[tokio::test]
    async fn test_null_and_unparsable_input() {
        let ctx = ctx_with_as_funcs();
        // An unparsable string is a string-typed VARIANT (Snowflake's implicit
        // VARCHAR -> VARIANT cast): AS_CHAR returns it, AS_INTEGER is NULL.
        let r = eval(
            &ctx,
            r#"SELECT AS_CHAR(NULL) a, AS_INTEGER(NULL) b, AS_CHAR('not json') c, AS_INTEGER('not json') d"#,
        )
        .await;
        assert_batches_eq!(
            &[
                "+---+---+----------+---+",
                "| a | b | c        | d |",
                "+---+---+----------+---+",
                "|   |   | not json |   |",
                "+---+---+----------+---+",
            ],
            &r
        );
    }

    #[allow(clippy::expect_used)]
    #[tokio::test]
    async fn test_batch_input_row_alignment() {
        use std::sync::Arc;
        // Columnar input: each row is extracted independently.
        let ctx = ctx_with_as_funcs();
        let batch = RecordBatch::try_from_iter(vec![(
            "v",
            std::sync::Arc::new(StringArray::from(vec![
                Some("2"),
                Some(r#""1""#),
                Some(r#""x""#),
                None,
            ])) as datafusion::arrow::array::ArrayRef,
        )])
        .expect("batch should build");
        ctx.register_batch("t_vals", batch).expect("register");
        let r = eval(&ctx, "SELECT AS_INTEGER(v) IS NOT NULL ok FROM t_vals").await;
        assert_batches_eq!(
            &[
                "+-------+",
                "| ok    |",
                "+-------+",
                "| true  |",
                "| false |",
                "| false |",
                "| false |",
                "+-------+",
            ],
            &r
        );
    }
}
