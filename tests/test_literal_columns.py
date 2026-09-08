import json

import pytest
from pyspark.sql import Row
from pyspark.sql.types import LongType, StringType, StructField, StructType

from databay import (
    cardinality_check,
    cardinality_check_tables,
    compare_columns_by_key,
    compare_datasets,
    duplicate_check,
    find_key_set,
    null_rate,
    numeric_diff_check,
    pk_uniqueness_check,
    regex_check,
    row_level_rules,
    select_informative_columns,
)
from databay.runtime.docker import DockConfig, dock
from databay.runtime.spark import spark_connect


pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def spark():
    try:
        dock(DockConfig(image="spark-pg-delta", name="spark-pg-delta"))
        session = spark_connect(app_name="pytest-databay-literal-columns", timeout=20)
    except Exception as exc:
        pytest.skip(f"Runtime Spark startup unavailable: {exc}")
    yield session
    session.stop()


@pytest.fixture(params=["customer.id", "customer`id", "customer id"])
def column_name(request):
    return request.param


def test_profiling_preserves_literal_names(spark, column_name):
    df = spark.createDataFrame(
        [("A",), ("B",), (None,)],
        StructType([StructField(column_name, StringType())]),
    )
    clean, report = select_informative_columns(df, min_distinct_values=2, return_report=True)
    assert clean.columns == [column_name]
    assert clean.collect() == df.collect()
    stats = report.collect()[0]
    assert stats["column_name"] == column_name
    assert stats["distinct_values"] == 2
    assert stats["status"] == "KEPT"
    nulls = null_rate(df).collect()[0]
    assert nulls["column_name"] == column_name
    assert nulls["null_records"] == 1


def test_duplicates_and_regex_use_literal_names(spark, column_name):
    df = spark.createDataFrame(
        [("A",), ("A",), ("b",), (None,)],
        StructType([StructField(column_name, StringType())]),
    )
    duplicates = duplicate_check(df, cols=[column_name], show_summary_only=False).collect()
    assert [row.asDict() for row in duplicates] == [{"duplicate_count": 2, column_name: "A"}]
    pk = {row["metric"]: row["value"] for row in pk_uniqueness_check(df, [column_name]).collect()}
    assert pk["rows_in_duplicate_groups"] == 2
    assert pk[f"null_in_{column_name}"] == 1
    summary = regex_check(df, {column_name: "^A$"}).collect()[0]
    assert summary["matching_rows"] == 2
    assert summary["non_matching_rows"] == 2
    detail = regex_check(df, {column_name: "^A$"}, show_summary_only=False).collect()
    assert {row["non_matching_value"] for row in detail} == {"b", "<NULL>"}


@pytest.mark.parametrize("summary", [True, False])
def test_comparisons_use_literal_keys_and_derived_names(spark, column_name, summary):
    key_name = f"key.{column_name}"
    schema = StructType([StructField(key_name, LongType()), StructField(column_name, LongType())])
    left = spark.createDataFrame([(1, 10), (2, 20)], schema)
    right = spark.createDataFrame([(1, 10), (2, 25)], schema)
    comparisons = compare_datasets(left, right, ["*"]).collect()
    assert next(row["count_value"] for row in comparisons if row["metric"] == "common_rows") == 1
    columns = compare_columns_by_key(left, right, [key_name], ["*"], show_summary_only=summary).collect()
    numeric = numeric_diff_check(left, right, [key_name], show_summary_only=summary, diff_type="both").collect()
    if summary:
        assert columns[0]["matching_rows"] == 1
        assert columns[0]["non_matching_rows"] == 1
        assert numeric[0]["differing_values"] == 1
    else:
        assert len(columns) == len(numeric) == 1
        assert columns[0][key_name] == 2
        assert columns[0][f"{column_name}_a"] == 20
        assert columns[0][f"{column_name}_b"] == 25
        assert numeric[0][f"{column_name}_diff"] == 5
        assert numeric[0][f"{column_name}_pct_diff"] == -20
    found = find_key_set({"target": right}, left, column_name, [column_name]).collect()[0]
    assert found["matched_count"] == 1
    assert found["coverage_pct"] == 50.0


@pytest.mark.parametrize("top_n", [0, 10])
def test_cardinality_detail_and_empty_schema_preserve_names(spark, column_name, top_n):
    right_name = f"ref.{column_name}"
    df = spark.createDataFrame(
        [("A", "X"), ("A", "Y")],
        StructType([StructField(column_name, StringType()), StructField(right_name, StringType())]),
    )
    assert cardinality_check(df, column_name, right_name).collect()[0]["relationship_type"] == "1:N"
    detail = cardinality_check(df, column_name, right_name, show_summary_only=False, top_n=top_n)
    assert detail.columns == [column_name, right_name, "rows_in_left", "rows_in_right"]
    assert detail.count() == (0 if top_n == 0 else 2)
    other = spark.createDataFrame([("A",)], StructType([StructField(right_name, StringType())]))
    summary = cardinality_check_tables(df, other, column_name, right_name).collect()[0]
    assert summary["relationship_type"] == "N:1"
    detail = cardinality_check_tables(df, other, column_name, right_name, show_summary_only=False, top_n=top_n)
    assert detail.columns == [column_name, "rows_in_a", "rows_in_b"]
    assert detail.count() == (0 if top_n == 0 else 1)
    if top_n:
        assert detail.collect()[0][column_name] == "A"


def test_literal_column_wins_over_nested_field_and_sql_rules_remain_expressions(spark):
    schema = StructType([
        StructField("customer.id", StringType()),
        StructField("customer", StructType([StructField("id", StringType())])),
    ])
    df = spark.createDataFrame(
        [("A", Row(id="nested")), ("B", Row(id="nested")), (None, Row(id="nested"))], schema
    )
    nulls = {row["column_name"]: row["null_records"] for row in null_rate(df).collect()}
    assert nulls["customer.id"] == 1
    clean, report = select_informative_columns(df, preserve=["customer.id"], return_report=True)
    assert clean.collect() == df.collect()
    literal = next(row for row in report.collect() if row["column_name"] == "customer.id")
    assert literal["non_null_count"] == 2
    keys = spark.createDataFrame([("A",)], "key string")
    assert find_key_set({"target": df}, keys, "key", ["customer.id"]).collect()[0]["matched_count"] == 1
    rules = {"literal.rule": "`customer.id` = 'A' AND customer.id = 'nested'"}
    assert row_level_rules(df, rules).collect()[0]["matching_rows"] == 1
    detail = row_level_rules(df, rules, show_summary_only=False).collect()
    assert len(detail) == 2
    failed_rows = [json.loads(row["non_matching_value"]) for row in detail]
    assert {row.get("customer.id") for row in failed_rows} == {"B", None}
    assert all(row["customer"] == {"id": "nested"} for row in failed_rows)
