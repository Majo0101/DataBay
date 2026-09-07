import pytest
from pyspark.sql import Row

from databay import (
    cardinality_check,
    cardinality_check_tables,
    duplicate_check,
    null_rate,
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
    cfg = DockConfig(
        image="spark-pg-delta",
        name="spark-pg-delta",
        bind_mounts={},
    )

    try:
        dock(cfg)
        spark_session = spark_connect(
            app_name="pytest-databay-quality",
            host="localhost",
            port=15002,
            timeout=20,
            check_interval=0.5,
        )
    except Exception as exc:
        pytest.skip(f"Runtime Spark startup unavailable: {exc}")

    yield spark_session
    spark_session.stop()


def _summary_dict(df):
    return {r["metric"]: r for r in df.collect()}


def _quality_summary_by_name(df):
    return {r["column_name"]: r for r in df.collect()}


def test_null_rate_returns_expected_counts_and_percentages(spark):
    df = spark.createDataFrame(
        [
            Row(id=1, name="A", city=None),
            Row(id=2, name=None, city="X"),
            Row(id=3, name="C", city=None),
            Row(id=4, name="D", city="Y"),
        ]
    )

    result = null_rate(df)
    rows = {r["column_name"]: r for r in result.collect()}

    assert rows["id"]["null_records"] == 0
    assert rows["name"]["null_records"] == 1
    assert rows["city"]["null_records"] == 2
    assert rows["city"]["null_percentage"] == 50.0


def test_null_rate_threshold_filters_columns(spark):
    df = spark.createDataFrame(
        [
            Row(a=1, b=None),
            Row(a=2, b=None),
            Row(a=3, b=3),
        ]
    )

    result = null_rate(df, threshold=50.0)
    cols = [r["column_name"] for r in result.collect()]

    assert cols == ["b"]


@pytest.mark.parametrize("sort_desc", [True, False])
def test_null_rate_returns_empty_report_when_all_columns_below_threshold(spark, sort_desc):
    df = spark.createDataFrame([Row(id=1, name="A"), Row(id=2, name="B")])

    result = null_rate(df, threshold=5.0, sort_desc=sort_desc)

    assert result.collect() == []
    assert result.dtypes == [
        ("column_name", "string"),
        ("total_records", "bigint"),
        ("null_records", "bigint"),
        ("non_null_records", "bigint"),
        ("null_percentage", "double"),
        ("data_type", "string"),
    ]


def test_duplicate_check_summary_with_null_stats(spark):
    df = spark.createDataFrame(
        [
            Row(id=1, name="A"),
            Row(id=1, name="A"),
            Row(id=2, name=None),
            Row(id=3, name="C"),
        ]
    )

    result = duplicate_check(
        df=df,
        cols=["id", "name"],
        show_summary_only=True,
        check_nulls=True,
    )
    s = _summary_dict(result)

    assert s["total_rows"]["value"] == 4
    assert s["rows_in_duplicate_groups"]["value"] == 2
    assert s["combinations_with_duplicates"]["value"] == 1
    assert s["total_null_values"]["value"] == 1
    assert s["null_in_name"]["value"] == 1


def test_duplicate_check_detailed_top_n(spark):
    df = spark.createDataFrame(
        [
            Row(id=1, cat="x"),
            Row(id=1, cat="x"),
            Row(id=1, cat="x"),
            Row(id=2, cat="y"),
            Row(id=2, cat="y"),
            Row(id=3, cat="z"),
        ]
    )

    result = duplicate_check(
        df=df,
        cols=["id", "cat"],
        top_n=1,
        show_summary_only=False,
    )
    rows = result.collect()

    assert len(rows) == 1
    assert rows[0]["duplicate_count"] == 3
    assert rows[0]["id"] == 1
    assert rows[0]["cat"] == "x"


def test_duplicate_check_raises_for_invalid_top_n_and_cols(spark):
    df = spark.createDataFrame([Row(id=1, cat="x")])

    with pytest.raises(ValueError, match="top_n must be >= 0"):
        duplicate_check(df=df, cols=["id"], top_n=-1)

    with pytest.raises(ValueError, match="cols must be a non-empty list"):
        duplicate_check(df=df, cols=[])

    with pytest.raises(ValueError, match="Column 'missing' not found in DataFrame"):
        duplicate_check(df=df, cols=["missing"])


def test_pk_uniqueness_check_detects_duplicates_and_nulls(spark):
    df = spark.createDataFrame(
        [
            Row(pk=1, value="A"),
            Row(pk=1, value="B"),
            Row(pk=None, value="C"),
            Row(pk=3, value="D"),
        ]
    )

    result = pk_uniqueness_check(df, pk_cols=["pk"])
    s = _summary_dict(result)

    assert s["rows_in_duplicate_groups"]["value"] == 2
    assert s["total_null_values"]["value"] == 1
    assert s["null_in_pk"]["value"] == 1


def test_regex_check_summary_handles_nulls_and_percentages(spark):
    df = spark.createDataFrame(
        [
            Row(email="a@test.com"),
            Row(email="bad-email"),
            Row(email=None),
            Row(email="b@example.org"),
        ]
    )

    result = regex_check(
        df,
        rules={"email": r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"},
        show_summary_only=True,
    )
    rows = _quality_summary_by_name(result)

    assert rows["email"]["total_rows"] == 4
    assert rows["email"]["matching_rows"] == 2
    assert rows["email"]["non_matching_rows"] == 2
    assert rows["email"]["match_percentage"] == 50.0


def test_regex_check_detailed_includes_null_placeholder_and_top_n(spark):
    df = spark.createDataFrame(
        [
            Row(phone="+421-123-4567"),
            Row(phone="invalid"),
            Row(phone="invalid"),
            Row(phone=None),
            Row(phone="123-4567"),
        ]
    )

    result = regex_check(
        df,
        rules={"phone": r"^\+[0-9]{1,3}-[0-9]{3}-[0-9]{4}$"},
        show_summary_only=False,
        top_n=2,
    )
    rows = result.collect()

    assert len(rows) == 2
    assert rows[0]["column_name"] == "phone"
    assert rows[0]["non_matching_value"] == "invalid"
    assert rows[0]["non_matching_count"] == 2
    assert any(r["non_matching_value"] == "<NULL>" for r in rows)


def test_regex_check_raises_for_invalid_rules(spark):
    df = spark.createDataFrame([Row(email="a@test.com")])

    with pytest.raises(ValueError, match="rules must be a non-empty dict"):
        regex_check(df, rules={})

    with pytest.raises(ValueError, match="Column 'missing' not found"):
        regex_check(df, rules={"missing": r".+"})

    with pytest.raises(ValueError, match="must be a non-empty string"):
        regex_check(df, rules={"email": ""})

    with pytest.raises(ValueError, match="top_n must be >= 0"):
        regex_check(df, rules={"email": r".+"}, show_summary_only=False, top_n=-1)


def test_regex_check_handles_empty_dataframe(spark):
    empty_regex_df = spark.createDataFrame([], "email string")
    regex_summary = regex_check(
        empty_regex_df,
        rules={"email": r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"},
        show_summary_only=True,
    ).collect()
    assert len(regex_summary) == 1
    assert regex_summary[0]["total_rows"] == 0
    assert regex_summary[0]["matching_rows"] == 0
    assert regex_summary[0]["non_matching_rows"] == 0
    assert regex_summary[0]["match_percentage"] == 0.0

def test_regex_check_top_n_zero_returns_no_details(spark):
    regex_df = spark.createDataFrame([Row(email="bad"), Row(email="still_bad")])
    regex_details = regex_check(
        regex_df,
        rules={"email": r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"},
        show_summary_only=False,
        top_n=0,
    ).collect()
    assert regex_details == []

def test_regex_check_multi_rule_summary_and_detail_include_all_rules(spark):
    df = spark.createDataFrame(
        [
            Row(email="ok@test.com", phone="+421-123-4567"),
            Row(email="bad", phone="bad"),
            Row(email=None, phone="+420-999-0000"),
        ]
    )

    summary = regex_check(
        df,
        rules={
            "email": r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$",
            "phone": r"^\+[0-9]{1,3}-[0-9]{3}-[0-9]{4}$",
        },
        show_summary_only=True,
    )
    summary_rows = _quality_summary_by_name(summary)
    assert set(summary_rows.keys()) == {"email", "phone"}

    details = regex_check(
        df,
        rules={
            "email": r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$",
            "phone": r"^\+[0-9]{1,3}-[0-9]{3}-[0-9]{4}$",
        },
        show_summary_only=False,
        top_n=10,
    ).collect()
    assert any(r["column_name"] == "email" for r in details)
    assert any(r["column_name"] == "phone" for r in details)


def test_row_level_rules_summary_matches_regex_check_shape(spark):
    df = spark.createDataFrame(
        [
            Row(age=20, country="SK"),
            Row(age=17, country="SK"),
            Row(age=None, country="US"),
            Row(age=40, country=""),
        ]
    )

    result = row_level_rules(
        df,
        rules={
            "adult_age": "age >= 18",
            "country_present": "country IS NOT NULL AND country <> ''",
        },
        show_summary_only=True,
    )
    rows = _quality_summary_by_name(result)

    assert rows["adult_age"]["pattern"] == "age >= 18"
    assert rows["adult_age"]["matching_rows"] == 2
    assert rows["adult_age"]["non_matching_rows"] == 2
    assert rows["adult_age"]["match_percentage"] == 50.0

    assert rows["country_present"]["matching_rows"] == 3
    assert rows["country_present"]["non_matching_rows"] == 1
    assert rows["country_present"]["match_percentage"] == 75.0


def test_row_level_rules_detailed_uses_json_rows_and_top_n(spark):
    df = spark.createDataFrame(
        [
            Row(id=1, amount=10),
            Row(id=1, amount=10),
            Row(id=2, amount=-5),
            Row(id=3, amount=15),
        ]
    )

    result = row_level_rules(
        df,
        rules={"positive_amount": "amount > 0"},
        show_summary_only=False,
        top_n=1,
    )
    rows = result.collect()

    assert len(rows) == 1
    assert rows[0]["column_name"] == "positive_amount"
    assert rows[0]["pattern"] == "amount > 0"
    assert rows[0]["non_matching_count"] == 1
    assert rows[0]["non_matching_value"] == '{"id":2,"amount":-5}'


def test_row_level_rules_detailed_preserves_rule_order(spark):
    df = spark.createDataFrame(
        [
            Row(id=1, amount=10),
            Row(id=2, amount=-1),
            Row(id=3, amount=3),
        ]
    )

    result = row_level_rules(
        df,
        rules={
            "id_even": "id % 2 = 0",
            "amount_positive": "amount > 0",
        },
        show_summary_only=False,
        top_n=5,
    )
    rows = result.collect()

    assert rows[0]["column_name"] == "id_even"
    assert rows[-1]["column_name"] == "amount_positive"


def test_row_level_rules_raises_for_invalid_rules(spark):
    df = spark.createDataFrame([Row(x=1)])

    with pytest.raises(ValueError, match="rules must be a non-empty dict"):
        row_level_rules(df, rules={})

    with pytest.raises(ValueError, match="rule name must be a non-empty string"):
        row_level_rules(df, rules={"": "x > 0"})

    with pytest.raises(ValueError, match="rule expression must be a non-empty string"):
        row_level_rules(df, rules={"valid_name": ""})

    with pytest.raises(ValueError, match="top_n must be >= 0"):
        row_level_rules(df, rules={"valid_name": "x > 0"}, show_summary_only=False, top_n=-1)


def test_row_level_rules_raises_for_invalid_sql_expression(spark):
    df = spark.createDataFrame([Row(age=20)])

    with pytest.raises(Exception):
        row_level_rules(
            df,
            rules={"broken_sql": "age >"},
            show_summary_only=True,
        ).collect()


def test_row_level_rules_handles_empty_dataframe(spark):
    empty_rule_df = spark.createDataFrame([], "age int")
    rule_summary = row_level_rules(
        empty_rule_df,
        rules={"adult": "age >= 18"},
        show_summary_only=True,
    ).collect()
    assert len(rule_summary) == 1
    assert rule_summary[0]["total_rows"] == 0
    assert rule_summary[0]["matching_rows"] == 0
    assert rule_summary[0]["non_matching_rows"] == 0
    assert rule_summary[0]["match_percentage"] == 0.0


def test_row_level_rules_top_n_zero_returns_no_details(spark):
    rule_df = spark.createDataFrame([Row(amount=-1), Row(amount=-2)])
    rule_details = row_level_rules(
        rule_df,
        rules={"positive_amount": "amount > 0"},
        show_summary_only=False,
        top_n=0,
    ).collect()
    assert rule_details == []


def test_row_level_rules_multi_rule_detail_and_tie_counts(spark):
    df = spark.createDataFrame(
        [
            Row(id=1, amount=-1),
            Row(id=2, amount=-2),
            Row(id=3, amount=5),
            Row(id=4, amount=10),
        ]
    )

    details = row_level_rules(
        df,
        rules={
            "id_even": "id % 2 = 0",
            "amount_positive": "amount > 0",
        },
        show_summary_only=False,
        top_n=10,
    ).collect()

    assert any(r["column_name"] == "id_even" for r in details)
    assert any(r["column_name"] == "amount_positive" for r in details)

    positive_rule_rows = [r for r in details if r["column_name"] == "amount_positive"]
    assert len(positive_rule_rows) == 2
    assert all(r["non_matching_count"] == 1 for r in positive_rule_rows)


def test_row_level_rules_detailed_handles_complex_types_json(spark):
    df = spark.createDataFrame(
        [
            Row(id=1, tags=["a", "b"], meta={"k": "v"}, amount=-1),
            Row(id=2, tags=["x"], meta={"k": "z"}, amount=5),
        ]
    )

    rows = row_level_rules(
        df,
        rules={"amount_positive": "amount > 0"},
        show_summary_only=False,
        top_n=5,
    ).collect()

    assert len(rows) == 1
    value = rows[0]["non_matching_value"]
    assert '"id":1' in value
    assert '"tags":["a","b"]' in value
    assert '"meta":{"k":"v"}' in value


def test_cardinality_check_summary_classifies_1_to_1(spark):
    df = spark.createDataFrame(
        [
            Row(a="L1", b="R1"),
            Row(a="L2", b="R2"),
            Row(a="L3", b="R3"),
        ]
    )

    row = cardinality_check(df, "a", "b", show_summary_only=True).collect()[0]
    assert row["distinct_left"] == 3
    assert row["distinct_right"] == 3
    assert row["distinct_pairs"] == 3
    assert row["left_to_right_max"] == 1
    assert row["right_to_left_max"] == 1
    assert row["relationship_type"] == "1:1"


def test_cardinality_check_summary_classifies_1_to_n(spark):
    df = spark.createDataFrame(
        [
            Row(a="L1", b="R1"),
            Row(a="L1", b="R2"),
            Row(a="L2", b="R3"),
        ]
    )

    row = cardinality_check(df, "a", "b", show_summary_only=True).collect()[0]
    assert row["left_to_right_max"] == 2
    assert row["right_to_left_max"] == 1
    assert row["relationship_type"] == "1:N"


def test_cardinality_check_summary_classifies_n_to_1(spark):
    df = spark.createDataFrame(
        [
            Row(a="L1", b="R1"),
            Row(a="L2", b="R1"),
            Row(a="L3", b="R2"),
        ]
    )

    row = cardinality_check(df, "a", "b", show_summary_only=True).collect()[0]
    assert row["left_to_right_max"] == 1
    assert row["right_to_left_max"] == 2
    assert row["relationship_type"] == "N:1"


def test_cardinality_check_summary_classifies_n_to_n(spark):
    df = spark.createDataFrame(
        [
            Row(a="L1", b="R1"),
            Row(a="L1", b="R2"),
            Row(a="L2", b="R1"),
        ]
    )

    row = cardinality_check(df, "a", "b", show_summary_only=True).collect()[0]
    assert row["left_to_right_max"] == 2
    assert row["right_to_left_max"] == 2
    assert row["relationship_type"] == "N:N"


def test_cardinality_check_summary_handles_empty_non_null_pairs(spark):
    df = spark.createDataFrame(
        [
            Row(a=None, b="R1"),
            Row(a="L1", b=None),
            Row(a=None, b=None),
        ]
    )

    row = cardinality_check(df, "a", "b", show_summary_only=True).collect()[0]
    assert row["total_rows"] == 3
    assert row["non_null_pair_rows"] == 0
    assert row["relationship_type"] == "EMPTY"


def test_cardinality_check_detailed_returns_top_violations(spark):
    df = spark.createDataFrame(
        [
            Row(a="L1", b="R1"),
            Row(a="L1", b="R2"),
            Row(a="L2", b="R1"),
            Row(a="L3", b="R3"),
        ]
    )

    rows = cardinality_check(
        df,
        "a",
        "b",
        show_summary_only=False,
        top_n=10,
    ).collect()

    assert any(r["a"] == "L1" and r["b"] == "R1" and r["rows_in_left"] == 2 and r["rows_in_right"] == 2 for r in rows)
    assert any(r["a"] == "L1" and r["b"] == "R2" and r["rows_in_left"] == 2 and r["rows_in_right"] == 1 for r in rows)
    assert any(r["a"] == "L2" and r["b"] == "R1" and r["rows_in_left"] == 1 and r["rows_in_right"] == 2 for r in rows)


def test_cardinality_check_raises_for_invalid_input(spark):
    df = spark.createDataFrame([Row(a="L1", b="R1")])

    with pytest.raises(ValueError, match="Column 'missing_left' not found"):
        cardinality_check(df, "missing_left", "b")

    with pytest.raises(ValueError, match="Column 'missing_right' not found"):
        cardinality_check(df, "a", "missing_right")

    with pytest.raises(ValueError, match="top_n must be >= 0"):
        cardinality_check(df, "a", "b", show_summary_only=False, top_n=-1)

    with pytest.raises(ValueError, match="summary_view must be 'full' or 'short'"):
        cardinality_check(df, "a", "b", show_summary_only=True, summary_view="compact")


def test_cardinality_check_supports_multi_column_input(spark):
    df = spark.createDataFrame(
        [
            Row(a1="L1", a2="X", b1="R1", b2="P"),
            Row(a1="L1", a2="X", b1="R2", b2="Q"),
            Row(a1="L2", a2="Y", b1="R3", b2="Z"),
        ]
    )

    row = cardinality_check(df, ["a1", "a2"], ["b1", "b2"], show_summary_only=True).collect()[0]
    assert row["col_left"] == "a1,a2"
    assert row["col_right"] == "b1,b2"
    assert row["relationship_type"] == "1:N"

    detail = cardinality_check(df, ["a1", "a2"], ["b1", "b2"], show_summary_only=False, top_n=10).collect()
    assert any(r["a1"] == "L1" and r["a2"] == "X" and r["rows_in_left"] == 2 for r in detail)
    assert any(r["b1"] == "R1" and r["b2"] == "P" for r in detail)


def test_cardinality_check_short_summary_view(spark):
    df = spark.createDataFrame(
        [
            Row(a="L1", b="R1"),
            Row(a="L1", b="R2"),
            Row(a="L2", b="R3"),
        ]
    )

    row = cardinality_check(df, "a", "b", show_summary_only=True, summary_view="short").collect()[0]
    assert row["total_rows"] == 3
    assert row["non_null_pair_rows"] == 3
    assert row["distinct_pairs"] == 3
    assert row["relationship_type"] == "1:N"


def test_cardinality_check_tables_summary_and_detail(spark):
    df_a = spark.createDataFrame(
        [
            Row(k="K1"),
            Row(k="K1"),
            Row(k="K2"),
            Row(k="K3"),
        ]
    )
    df_b = spark.createDataFrame(
        [
            Row(ref="K1"),
            Row(ref="K2"),
            Row(ref="K2"),
            Row(ref="K4"),
        ]
    )

    summary = cardinality_check_tables(
        df_a,
        df_b,
        cols_a="k",
        cols_b="ref",
        show_summary_only=True,
        name_a="left",
        name_b="right",
    ).collect()[0]
    assert summary["distinct_keys_left"] == 3
    assert summary["distinct_keys_right"] == 3
    assert summary["overlap_distinct_keys"] == 2
    assert summary["relationship_type"] == "N:N"

    detail = cardinality_check_tables(
        df_a,
        df_b,
        cols_a="k",
        cols_b="ref",
        show_summary_only=False,
        name_a="left",
        name_b="right",
        top_n=10,
    ).collect()
    assert len(detail) == 2
    assert any(r["k"] == "K1" and r["rows_in_left"] == 2 and r["rows_in_right"] == 1 for r in detail)
    assert any(r["k"] == "K2" and r["rows_in_left"] == 1 and r["rows_in_right"] == 2 for r in detail)


def test_cardinality_check_tables_supports_multi_column_keys(spark):
    df_a = spark.createDataFrame(
        [
            Row(k1="A", k2="1"),
            Row(k1="A", k2="1"),
            Row(k1="B", k2="2"),
        ]
    )
    df_b = spark.createDataFrame(
        [
            Row(r1="A", r2="1"),
            Row(r1="B", r2="2"),
        ]
    )

    row = cardinality_check_tables(
        df_a,
        df_b,
        cols_a=["k1", "k2"],
        cols_b=["r1", "r2"],
        show_summary_only=True,
    ).collect()[0]
    assert row["cols_a"] == "k1,k2"
    assert row["cols_b"] == "r1,r2"
    assert row["relationship_type"] == "N:1"


def test_cardinality_check_tables_detail_returns_key_columns_not_key_value(spark):
    df_a = spark.createDataFrame(
        [
            Row(id="1", type="A"),
            Row(id="1", type="A"),
            Row(id="2", type="B"),
        ]
    )
    df_b = spark.createDataFrame(
        [
            Row(id_ref="1", type_ref="A"),
            Row(id_ref="2", type_ref="B"),
            Row(id_ref="2", type_ref="B"),
        ]
    )

    rows = cardinality_check_tables(
        df_a,
        df_b,
        cols_a=["id", "type"],
        cols_b=["id_ref", "type_ref"],
        show_summary_only=False,
        name_a="left",
        name_b="right",
        top_n=10,
    ).collect()

    assert rows[0].asDict().get("id") is not None
    assert rows[0].asDict().get("type") is not None
    assert "key_value" not in rows[0].asDict()
    assert any(r["id"] == "1" and r["type"] == "A" for r in rows)
    assert any(r["id"] == "2" and r["type"] == "B" for r in rows)


def test_cardinality_check_tables_raises_for_invalid_input(spark):
    df_a = spark.createDataFrame([Row(a="X")])
    df_b = spark.createDataFrame([Row(b="X")])

    with pytest.raises(ValueError, match="Column 'missing' not found in df_a"):
        cardinality_check_tables(df_a, df_b, cols_a="missing", cols_b="b")

    with pytest.raises(ValueError, match="Column 'missing' not found in df_b"):
        cardinality_check_tables(df_a, df_b, cols_a="a", cols_b="missing")

    with pytest.raises(ValueError, match="top_n must be >= 0"):
        cardinality_check_tables(df_a, df_b, cols_a="a", cols_b="b", show_summary_only=False, top_n=-1)

    with pytest.raises(ValueError, match="same number of columns"):
        cardinality_check_tables(df_a, df_b, cols_a=["a", "x"], cols_b="b")

    with pytest.raises(ValueError, match="summary_view must be 'full' or 'short'"):
        cardinality_check_tables(df_a, df_b, cols_a="a", cols_b="b", summary_view="compact")


def test_cardinality_check_tables_short_summary_and_custom_names(spark):
    df_a = spark.createDataFrame([Row(id="1"), Row(id="2"), Row(id="2")])
    df_b = spark.createDataFrame([Row(id="2"), Row(id="3")])

    row = cardinality_check_tables(
        df_a,
        df_b,
        cols_a="id",
        cols_b="id",
        show_summary_only=True,
        summary_view="short",
        name_a="orders raw",
        name_b="master",
    ).collect()[0]

    assert row["total_rows_orders_raw"] == 3
    assert row["total_rows_master"] == 2
    assert row["overlap_distinct_keys"] == 1
    assert row["relationship_type"] == "N:1"


def test_select_informative_columns_returns_clean_dataframe_and_report(spark):
    df = spark.createDataFrame(
        [
            Row(customer_id=1001, name="Ana", country="SK", legacy_code=None, note=""),
            Row(customer_id=1002, name="Peter", country="SK", legacy_code=None, note="  "),
            Row(customer_id=1003, name="Lena", country="SK", legacy_code=None, note=None),
        ],
        "customer_id long, name string, country string, legacy_code string, note string",
    )

    clean_df, report = select_informative_columns(
        df,
        min_distinct_values=2,
        preserve=["customer_id"],
        return_report=True,
    )

    assert clean_df.columns == ["customer_id", "name"]
    statuses = {row["column_name"]: row["status"] for row in report.collect()}
    assert statuses == {
        "customer_id": "PRESERVED",
        "name": "KEPT",
        "country": "DROPPED_CONSTANT",
        "legacy_code": "DROPPED_EMPTY",
        "note": "DROPPED_EMPTY",
    }


def test_select_informative_columns_filters_sparse_columns_and_can_keep_blanks(spark):
    df = spark.createDataFrame(
        [
            Row(id=1, sparse="value", blank=""),
            Row(id=2, sparse=None, blank=""),
            Row(id=3, sparse=None, blank=""),
            Row(id=4, sparse=None, blank=""),
        ]
    )

    clean_df, report = select_informative_columns(
        df,
        min_non_null_percentage=50,
        treat_blank_as_null=False,
        return_report=True,
    )

    assert clean_df.columns == ["id", "blank"]
    statuses = {row["column_name"]: row["status"] for row in report.collect()}
    assert statuses["sparse"] == "DROPPED_SPARSE"
    assert statuses["blank"] == "KEPT"


def test_select_informative_columns_validates_configuration(spark):
    df = spark.createDataFrame([Row(empty=None)], "empty string")

    with pytest.raises(ValueError, match="between 0 and 100"):
        select_informative_columns(df, min_non_null_percentage=101)
    with pytest.raises(ValueError, match="integer >= 1"):
        select_informative_columns(df, min_distinct_values=1.5)
    with pytest.raises(ValueError, match="Preserved column 'missing'"):
        select_informative_columns(df, preserve=["missing"])
    with pytest.raises(ValueError, match="No informative columns remain"):
        select_informative_columns(df)
