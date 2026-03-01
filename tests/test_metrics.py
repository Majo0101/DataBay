import pytest
from pyspark.sql import Row

from databay import (
    compare_columns_by_key,
    compare_datasets,
    compare_schema,
    find_key_set,
    numeric_diff_check,
)
from databay.runtime.docker import DockConfig, dock
from databay.runtime.spark import spark_connect


@pytest.fixture(scope="module")
def spark():
    cfg = DockConfig(
        image="spark-pg-delta",
        name="spark-pg-delta",
        bind_mounts={"C/landing": "/data/apache"},
    )

    try:
        dock(cfg)
        spark_session = spark_connect(
            app_name="pytest-databay-metrics",
            host="localhost",
            port=15002,
            timeout=20,
            check_interval=0.5,
        )
    except Exception as exc:
        pytest.skip(f"Runtime Spark startup unavailable: {exc}")

    yield spark_session
    spark_session.stop()


def _to_metric_dict(df):
    return {f"{r['metric']}_{r['scope']}": r for r in df.collect()}


def test_compare_datasets_identical(spark):
    data = [
        Row(id=1, name="Alice", age=25),
        Row(id=2, name="Bob", age=30),
        Row(id=3, name="Charlie", age=35),
    ]
    df_a = spark.createDataFrame(data)
    df_b = spark.createDataFrame(data)

    result = compare_datasets(df_a, df_b, cols=["*"], name_a="A", name_b="B")
    m = _to_metric_dict(result)

    assert m["row_count_A"]["count_value"] == 3
    assert m["row_count_B"]["count_value"] == 3
    assert m["diff_rows_A"]["count_value"] == 0
    assert m["diff_rows_B"]["count_value"] == 0
    assert m["common_rows_both"]["count_value"] == 3
    assert m["jaccard_pct_both"]["percent_value"] == 100.0


def test_compare_datasets_partial_match(spark):
    df_a = spark.createDataFrame(
        [
            Row(id=1, name="Alice", age=25),
            Row(id=2, name="Bob", age=30),
            Row(id=3, name="Charlie", age=35),
            Row(id=4, name="David", age=28),
        ]
    )
    df_b = spark.createDataFrame(
        [
            Row(id=1, name="Alice", age=25),
            Row(id=2, name="Bob", age=31),
            Row(id=3, name="Charlie", age=35),
            Row(id=5, name="Eve", age=32),
        ]
    )

    result = compare_datasets(df_a, df_b, cols=["*"])
    m = _to_metric_dict(result)

    assert m["row_count_Dataset A"]["count_value"] == 4
    assert m["row_count_Dataset B"]["count_value"] == 4
    assert m["common_rows_both"]["count_value"] == 2
    assert m["union_rows_both"]["count_value"] == 6


def test_compare_columns_by_key_summary_and_detail(spark):
    df_a = spark.createDataFrame(
        [
            Row(id=1, name="Alice", city="NY"),
            Row(id=2, name="Bob", city=None),
            Row(id=3, name="Charlie", city="Paris"),
        ]
    )
    df_b = spark.createDataFrame(
        [
            Row(id=1, name="Alice", city="NY"),
            Row(id=2, name="Bob", city="Berlin"),
            Row(id=3, name="Charles", city="Paris"),
        ]
    )

    summary = compare_columns_by_key(
        df_a=df_a,
        df_b=df_b,
        key_cols=["id"],
        compare_cols=["name", "city"],
        show_summary_only=True,
    )
    summary_rows = {r["column"]: r for r in summary.collect()}

    assert summary_rows["name"]["matching_rows"] == 2
    assert summary_rows["name"]["non_matching_rows"] == 1
    assert summary_rows["city"]["matching_rows"] == 2
    assert summary_rows["city"]["non_matching_rows"] == 1

    detail = compare_columns_by_key(
        df_a=df_a,
        df_b=df_b,
        key_cols=["id"],
        compare_cols=["name", "city"],
        show_summary_only=False,
    )
    assert detail.count() == 2


def test_compare_schema_detects_mismatch_and_missing_columns(spark):
    df_a = spark.createDataFrame([Row(id=1, amount=10.0, only_a="x")])
    df_b = spark.createDataFrame([Row(id="1", amount=10, only_b="y")])

    result = compare_schema(df_a, df_b, name_a="left", name_b="right")
    rows = {r["column_name"]: r["status"] for r in result.collect()}

    assert rows["id"] == "TYPE_MISMATCH"
    assert rows["amount"] == "TYPE_MISMATCH"
    assert rows["only_a"] == "ONLY_IN_LEFT"
    assert rows["only_b"] == "ONLY_IN_RIGHT"


def test_numeric_diff_check_summary_with_tolerance(spark):
    df_a = spark.createDataFrame(
        [
            Row(id=1, value=100.0),
            Row(id=2, value=200.05),
            Row(id=3, value=300.0),
        ]
    )
    df_b = spark.createDataFrame(
        [
            Row(id=1, value=100.0),
            Row(id=2, value=200.0),
            Row(id=3, value=305.0),
        ]
    )

    result = numeric_diff_check(
        df_a=df_a,
        df_b=df_b,
        key_cols=["id"],
        numeric_cols=["value"],
        tolerance=0.1,
        show_summary_only=True,
    )
    row = result.collect()[0]

    assert row["column"] == "value"
    assert row["total_compared"] == 3
    assert row["matching_values"] == 2
    assert row["differing_values"] == 1


def test_numeric_diff_check_detail_with_percentage(spark):
    df_a = spark.createDataFrame([Row(id=1, value=120.0), Row(id=2, value=50.0)])
    df_b = spark.createDataFrame([Row(id=1, value=100.0), Row(id=2, value=50.0)])

    result = numeric_diff_check(
        df_a=df_a,
        df_b=df_b,
        key_cols=["id"],
        numeric_cols=["value"],
        diff_type="both",
        show_summary_only=False,
    )

    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["id"] == 1
    assert rows[0]["value_diff"] == 20.0
    assert rows[0]["value_pct_diff"] == 20.0


def test_numeric_diff_check_raises_for_missing_key(spark):
    df_a = spark.createDataFrame([Row(id=1, value=1.0)])
    df_b = spark.createDataFrame([Row(id=1, value=1.0)])

    with pytest.raises(ValueError, match="Column 'missing_key' not found in df_a"):
        numeric_diff_check(df_a=df_a, df_b=df_b, key_cols=["missing_key"])


def test_numeric_diff_check_raises_when_no_numeric_columns(spark):
    df_a = spark.createDataFrame([Row(id=1, name="a")])
    df_b = spark.createDataFrame([Row(id=1, name="a")])

    with pytest.raises(ValueError, match="No numeric columns found to compare"):
        numeric_diff_check(df_a=df_a, df_b=df_b, key_cols=["id"])


def test_find_key_set_reports_column_coverage(spark):
    customers = spark.createDataFrame(
        [
            Row(customer_id="C1", email="a@test.com"),
            Row(customer_id="C2", email="b@test.com"),
            Row(customer_id="C3", email="c@test.com"),
        ]
    )
    orders = spark.createDataFrame(
        [
            Row(order_id=101, customer_ref="C1"),
            Row(order_id=102, customer_ref="C4"),
        ]
    )

    result = find_key_set(
        tables={"customers": customers, "orders": orders},
        keys_df=spark.createDataFrame([Row(k="C1"), Row(k="C2"), Row(k="C9")]),
        key_column="k",
        candidate_columns=["customer_id", "customer_ref"],
    )

    rows = {(r["table_name"], r["column_name"]): r for r in result.collect()}

    cust = rows[("customers", "customer_id")]
    assert cust["total_keys"] == 3
    assert cust["matched_count"] == 2
    assert cust["missing_count"] == 1
    assert cust["coverage_pct"] == 66.6667

    order = rows[("orders", "customer_ref")]
    assert order["matched_count"] == 1
    assert order["missing_count"] == 2


def test_find_key_set_checks_all_columns_when_candidate_columns_none(spark):
    table_a = spark.createDataFrame(
        [
            Row(id="A1", ref="R1", payload="x"),
            Row(id="A2", ref="R2", payload="y"),
        ]
    )

    result = find_key_set(
        tables={"table_a": table_a},
        keys_df=spark.createDataFrame([Row(k="A1"), Row(k="R2")]),
        key_column="k",
        candidate_columns=None,
    )

    rows = {(r["table_name"], r["column_name"]): r for r in result.collect()}
    assert ("table_a", "id") in rows
    assert ("table_a", "ref") in rows
    assert ("table_a", "payload") in rows


def test_find_key_set_raises_for_invalid_inputs(spark):
    df = spark.createDataFrame([Row(id="A1")])

    with pytest.raises(ValueError, match="tables must be a non-empty dict"):
        find_key_set(
            tables={},
            keys_df=spark.createDataFrame([Row(k="A1")]),
            key_column="k",
        )

    with pytest.raises(ValueError, match="key_column must be a non-empty string"):
        find_key_set(
            tables={"t": df},
            keys_df=spark.createDataFrame([Row(k="A1")]),
            key_column="",
        )

    with pytest.raises(ValueError, match="Column 'missing' not found in keys_df"):
        find_key_set(
            tables={"t": df},
            keys_df=spark.createDataFrame([Row(k="A1")]),
            key_column="missing",
        )

    with pytest.raises(ValueError, match="at least one non-null key value"):
        find_key_set(
            tables={"t": df},
            keys_df=spark.createDataFrame([Row(k=None), Row(k=None)], "k string"),
            key_column="k",
        )


def test_find_key_set_skips_missing_candidate_columns_per_table(spark):
    t1 = spark.createDataFrame([Row(id="A1", ref="R1")])
    t2 = spark.createDataFrame([Row(id="A1", payload="x")])
    keys_df = spark.createDataFrame([Row(k="A1")])

    result = find_key_set(
        tables={"t1": t1, "t2": t2},
        keys_df=keys_df,
        key_column="k",
        candidate_columns=["id", "ref"],
    )

    rows = {(r["table_name"], r["column_name"]) for r in result.collect()}
    assert ("t1", "id") in rows
    assert ("t1", "ref") in rows
    assert ("t2", "id") in rows
    assert ("t2", "ref") not in rows


def test_find_key_set_normalizes_types_via_string_cast(spark):
    table = spark.createDataFrame([Row(customer_id="1"), Row(customer_id="3")])
    keys_df = spark.createDataFrame([Row(k=1), Row(k=2), Row(k=3)])

    result = find_key_set(
        tables={"customers": table},
        keys_df=keys_df,
        key_column="k",
        candidate_columns=["customer_id"],
    ).collect()[0]

    assert result["total_keys"] == 3
    assert result["matched_count"] == 2
    assert result["missing_count"] == 1
    assert result["coverage_pct"] == 66.6667


def test_find_key_set_uses_distinct_keys_from_keys_df(spark):
    table = spark.createDataFrame([Row(code="A")])
    keys_df = spark.createDataFrame([Row(k="A"), Row(k="A"), Row(k="B")])

    result = find_key_set(
        tables={"codes": table},
        keys_df=keys_df,
        key_column="k",
        candidate_columns=["code"],
    ).collect()[0]

    assert result["total_keys"] == 2
    assert result["matched_count"] == 1
    assert result["missing_count"] == 1
    assert result["coverage_pct"] == 50.0


def test_find_key_set_smoke_with_larger_input(spark):
    keys_rows = [Row(k=str(i)) for i in range(200)]
    data_rows = [Row(kcol=str(i)) for i in range(150)]

    result = find_key_set(
        tables={"big_table": spark.createDataFrame(data_rows)},
        keys_df=spark.createDataFrame(keys_rows),
        key_column="k",
        candidate_columns=["kcol"],
    ).collect()[0]

    assert result["total_keys"] == 200
    assert result["matched_count"] == 150
    assert result["missing_count"] == 50
    assert result["coverage_pct"] == 75.0
