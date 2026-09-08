import pytest
from typing import Any, cast
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from databay.etl import Octopus
from databay.runtime.docker import DockConfig, dock
from databay.runtime.spark import spark_connect


TEST_PG_CONFIG = {
    "HOST": "host.docker.internal",
    "PORT": "5432",
    "DATABASE": "testdb",
    "USER": "test_user",
    "PASSWORD": "test_pass",
}

WRONG_TEST_PG_CONFIG = {
    "HOST": "host.docker.internal",
    "PORT": "5432",
    "DATABASE": "testdb",
    "USER": "test_user",
    "PASSWORD": "wrong_password",
}

def _set_env_vars(octopus: Octopus, env: dict[str, str]) -> None:
    cast(Any, octopus).env_vars = env


def _build_fact_df(spark, row_count: int):
    day_offset = (F.col("id") % F.lit(365)).cast("int")
    base_date = F.to_date(F.lit("2026-01-01"))

    return (
        spark.range(0, row_count)
        .withColumn("partition_col", F.col("id").cast("long"))
        .withColumn("int_col", (F.col("id") % F.lit(100000)).cast("int"))
        .withColumn("string_col", F.concat(F.lit("row_"), F.col("id").cast("string")))
        .withColumn("bool_col", (F.col("id") % F.lit(2) == F.lit(0)))
        .withColumn("double_col", (F.col("id") / F.lit(3.0)).cast("double"))
        .withColumn("decimal_col", (F.col("id") / F.lit(100)).cast(DecimalType(18, 2)))
        .withColumn("date_col", F.date_add(base_date, day_offset))
        .withColumn("timestamp_col", F.to_timestamp(F.concat(F.col("date_col").cast("string"), F.lit(" 12:00:00"))))
        .withColumn(
            "nullable_col",
            F.when((F.col("id") % F.lit(10)) == F.lit(0), F.lit(None)).otherwise(F.col("id").cast("string")),
        )
    )


def _build_dim_df(spark, row_count: int):
    return (
        spark.range(0, row_count)
        .withColumn("partition_col", F.col("id").cast("long"))
        .withColumn("category", F.concat(F.lit("cat_"), (F.col("id") % F.lit(20)).cast("string")))
        .withColumn("score", (F.col("id") / F.lit(10.0)).cast("double"))
    )


@pytest.fixture(scope="module")
def spark_session():
    cfg = DockConfig(
        image="spark-pg-delta",
        name="spark-pg-delta",
        bind_mounts={},
    )

    try:
        dock(cfg)
        spark = spark_connect(
            app_name="pytest-databay-write-jdbc",
            host="localhost",
            port=15002,
            timeout=20,
            check_interval=0.5,
        )
    except Exception as exc:
        pytest.skip(f"Runtime Spark startup unavailable: {exc}")

    yield spark
    spark.stop()


def test_write_jdbc_raises_when_spark_is_missing():
    octopus = Octopus(spark=None, engine="postgresql")
    with pytest.raises(RuntimeError, match="SparkSession is not set"):
        octopus.write_jdbc(data=cast(Any, "dummy_df"), target_table="t")


def test_write_jdbc_raises_when_engine_is_missing():
    octopus = Octopus(spark=cast(Any, "dummy_spark"), engine=None)
    with pytest.raises(RuntimeError, match="Engine is not set"):
        octopus.write_jdbc(data=cast(Any, "dummy_df"), target_table="t")


def test_write_jdbc_raises_for_invalid_mode():
    octopus = Octopus(spark=cast(Any, "dummy_spark"), engine="postgresql")
    with pytest.raises(ValueError, match="Unsupported write mode"):
        octopus.write_jdbc(data=cast(Any, "dummy_df"), target_table="t", mode="badmode")


@pytest.mark.parametrize("value", [0, -1, True, False, 1.5, "4", None])
def test_write_jdbc_rejects_invalid_num_partitions(value):
    octopus = Octopus(spark=cast(Any, "dummy_spark"), engine="postgresql")
    with pytest.raises(ValueError, match="num_partitions must be an integer >= 1"):
        octopus.write_jdbc(
            data=cast(Any, "dummy_df"), target_table="t", num_partitions=value
        )


@pytest.mark.integration
@pytest.mark.parametrize(
    ("source_partitions", "limit", "expected_transactions", "multiple_tables"),
    [(8, None, 4, False), (8, 2, 2, True), (1, 4, 1, False)],
)
def test_write_jdbc_partition_limit(
    spark_session, source_partitions, limit, expected_transactions, multiple_tables
):
    octopus = Octopus(spark=spark_session, engine="postgresql")
    _set_env_vars(octopus, TEST_PG_CONFIG)
    df = spark_session.range(0, 24, numPartitions=source_partitions)
    table = f"octopus_partition_limit_{source_partitions}_{limit}"
    tables = [table, table + "_second"] if multiple_tables else [table]
    options = {} if limit is None else {"num_partitions": limit}
    if multiple_tables:
        octopus.write_jdbc(
            {name: df for name in tables}, mode="overwrite", **options
        )
    else:
        octopus.write_jdbc(df, target_table=table, mode="overwrite", **options)

    for name in tables:
        # PostgreSQL records the inserting transaction in xmin. Spark commits
        # each nonempty JDBC write partition in its own transaction, so this
        # checks the actual write partition count, not just an option mock.
        result = octopus.read_jdbc(queries=[(
            f"SELECT id, xmin::text AS insert_transaction FROM {name}", "written"
        )])["written"].collect()
        assert sorted(row["id"] for row in result) == list(range(24))
        assert len({row["insert_transaction"] for row in result}) == expected_transactions


def test_write_jdbc_raises_when_single_dataframe_missing_target_table():
    octopus = Octopus(spark=cast(Any, "dummy_spark"), engine="postgresql")
    _set_env_vars(octopus, TEST_PG_CONFIG)
    with pytest.raises(ValueError, match="target_table is required"):
        octopus.write_jdbc(data=cast(Any, "dummy_df"))


def test_write_jdbc_raises_when_dict_data_has_target_table():
    octopus = Octopus(spark=cast(Any, "dummy_spark"), engine="postgresql")
    _set_env_vars(octopus, TEST_PG_CONFIG)
    with pytest.raises(ValueError, match="target_table must be None"):
        octopus.write_jdbc(data={"t1": cast(Any, "dummy_df")}, target_table="not_allowed")


@pytest.mark.integration
def test_write_jdbc_raises_for_wrong_credentials(spark_session):
    octopus = Octopus(spark=spark_session, engine="postgresql")
    _set_env_vars(octopus, WRONG_TEST_PG_CONFIG)

    df = spark_session.range(0, 10).withColumn("value", F.lit("x"))

    with pytest.raises(Exception, match="password authentication failed|FATAL"):
        octopus.write_jdbc(
            data=df,
            target_table="octopus_wrong_credentials_test",
            mode="overwrite",
        )


@pytest.mark.integration
@pytest.mark.slow
def test_octopus_write_jdbc_with_ten_million_rows_multiple_tables(spark_session):
    octopus = Octopus(spark=spark_session, engine="postgresql")
    _set_env_vars(octopus, TEST_PG_CONFIG)

    row_count = 10_000_000
    fact_df = _build_fact_df(spark_session, row_count)
    dim_df = _build_dim_df(spark_session, row_count)

    fact_table = "octopus_fact_test"
    dim_table = "octopus_dim_test"

    octopus.write_jdbc(
        data={
            fact_table: fact_df,
            dim_table: dim_df,
        },
        mode="overwrite",
        batch_size=50_000,
    )

    counts = octopus.read_jdbc(
        queries=[
            (f"SELECT COUNT(*) AS cnt FROM {fact_table}", "fact_count"),
            (f"SELECT COUNT(*) AS cnt FROM {dim_table}", "dim_count"),
        ]
    )

    fact_count = counts["fact_count"].collect()[0]["cnt"]
    dim_count = counts["dim_count"].collect()[0]["cnt"]

    assert fact_count == row_count
    assert dim_count == row_count
