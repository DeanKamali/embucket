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

// M16.3.1: multi-row correlated FLATTEN — the flatten must apply per row of
// the base table (lateral semantics), not multiply the flattened stream.
test_query!(
    flatten_lateral_multi_row,
    r#"WITH base AS (
        SELECT 1 AS id, PARSE_JSON('{"arr": [10, 20, 30]}') AS v
        UNION ALL SELECT 2, PARSE_JSON('{"arr": [40]}')
        UNION ALL SELECT 3, PARSE_JSON('{"x": 1}'))
    SELECT id, f.value, f.index, f.seq FROM base, LATERAL FLATTEN(INPUT => v:arr) f;"#,
    snapshot_path = "flatten"
);

// M16.3.1: multi-row with OUTER=true — rows without expansion keep one
// all-NULL row; NULL input row also keeps one row.
test_query!(
    flatten_lateral_multi_row_outer,
    r#"WITH base AS (
        SELECT 1 AS id, PARSE_JSON('{"arr": [10]}') AS v
        UNION ALL SELECT 2, PARSE_JSON('{"x": 1}')
        UNION ALL SELECT 3, NULL)
    SELECT id, f.value FROM base, LATERAL FLATTEN(INPUT => v:arr, IS_OUTER => true) f;"#,
    snapshot_path = "flatten"
);

// M16.3.1: correlated flatten over a join — each joined row flattens its own
// value.
test_query!(
    flatten_lateral_join_multi_row,
    r#"WITH t1 AS (
        SELECT 1 AS id, PARSE_JSON('{"arr": [10, 20]}') AS v
        UNION ALL SELECT 2, PARSE_JSON('{"arr": [30]}')),
       t2 AS (SELECT 'a' AS tag UNION ALL SELECT 'b')
    SELECT id, tag, f.value FROM t1 CROSS JOIN t2, LATERAL FLATTEN(INPUT => v:arr) f;"#,
    snapshot_path = "flatten"
);
