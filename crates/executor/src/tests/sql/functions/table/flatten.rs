use crate::test_query;

test_query!(
    flatten_cte,
    r#"WITH base AS (SELECT '{"a": 1}' AS jsontext),
        intermediate AS (SELECT value FROM base, LATERAL FLATTEN(INPUT => parse_json(jsontext)) d)
    SELECT * FROM intermediate;"#,
    snapshot_path = "flatten"
);

// M16.3: lateral FLATTEN whose INPUT is a `col:key` path expression over an
// outer column — exercises correlated table-function args end to end
// (visitor rewrite + planner arg handling).
test_query!(
    flatten_lateral_path_expr,
    r#"WITH base AS (SELECT PARSE_JSON('{"arr": [10, 20, 30]}') AS v)
    SELECT v, f.value FROM base, LATERAL FLATTEN(INPUT => v:arr) f;"#,
    snapshot_path = "flatten"
);

test_query!(
    flatten_lateral_bare_col_cte,
    r#"WITH base AS (SELECT PARSE_JSON('{"arr": [10, 20, 30]}') AS v)
    SELECT v, f.value FROM base, LATERAL FLATTEN(INPUT => v) f;"#,
    snapshot_path = "flatten"
);

test_query!(
    flatten_lateral_upper_expr_cte,
    r#"WITH base AS (SELECT '{"arr": [10, 20, 30]}' AS v)
    SELECT v, f.value FROM base, LATERAL FLATTEN(INPUT => upper(v)) f;"#,
    snapshot_path = "flatten"
);

test_query!(
    flatten_lateral_path_expr_real_table,
    "SELECT v, f.value FROM m163_ft, LATERAL FLATTEN(INPUT => v:arr) f",
    setup_queries = [
        "CREATE TABLE m163_ft AS SELECT PARSE_JSON('{\"arr\": [10, 20, 30]}') AS v"
    ],
    snapshot_path = "flatten"
);
