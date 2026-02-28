import pytest
from typing import Any, cast
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

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


@pytest.fixture(scope="module")
def spark_session():
    cfg = DockConfig(
        image="spark-pg-delta",
        name="spark-pg-delta",
        bind_mounts={"C/landing": "/data/apache"},
    )

    try:
        dock(cfg)
        spark = spark_connect(
            app_name="pytest-databay-feed-spark",
            host="localhost",
            port=15002,
            timeout=20,
            check_interval=0.5,
        )
    except Exception as exc:
        pytest.skip(f"Runtime Spark startup unavailable: {exc}")

    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def seeded_source_tables(spark_session):
    octopus = Octopus(spark=spark_session, engine="postgresql")
    _set_env_vars(octopus, TEST_PG_CONFIG)

    row_count = 100_000
    source_fact = (
        spark_session.range(0, row_count)
        .withColumn("partition_col", F.col("id").cast("long"))
        .withColumn("name", F.concat(F.lit("name_"), F.col("id").cast("string")))
        .withColumn("amount", (F.col("id") / F.lit(10.0)).cast("double"))
    )
    source_dim = (
        spark_session.range(0, row_count)
        .withColumn("partition_col", F.col("id").cast("long"))
        .withColumn("category", F.concat(F.lit("cat_"), (F.col("id") % F.lit(10)).cast("string")))
    )

    source_fact_table = "octopus_feed_source_fact"
    source_dim_table = "octopus_feed_source_dim"

    octopus.write_jdbc(
        data={source_fact_table: source_fact, source_dim_table: source_dim},
        mode="overwrite",
        batch_size=20000,
    )

    return {
        "source_fact_table": source_fact_table,
        "source_dim_table": source_dim_table,
        "row_count": row_count,
        "target_schema": "octopus_feed_test",
    }


def test_feed_spark_raises_when_spark_is_missing():
    octopus = Octopus(spark=None, engine="postgresql")
    with pytest.raises(RuntimeError, match="SparkSession is not set"):
        octopus.feed_spark(queries=[("SELECT 1", "x")], target_schema="x")


def test_feed_spark_raises_when_engine_is_missing():
    octopus = Octopus(spark=cast(Any, "dummy_spark"), engine=None)
    with pytest.raises(RuntimeError, match="Engine is not set"):
        octopus.feed_spark(queries=[("SELECT 1", "x")], target_schema="x")


def test_feed_spark_raises_when_parallel_read_without_partition_column():
    octopus = Octopus(spark=cast(Any, "dummy_spark"), engine="postgresql")
    _set_env_vars(octopus, TEST_PG_CONFIG)

    with pytest.raises(ValueError, match="partition_column is required"):
        octopus.feed_spark(
            queries=[],
            target_schema="x",
            parallel_read=True,
            lower_bound=0,
            upper_bound=10,
            num_partitions=2,
        )


def test_feed_spark_raises_when_parallel_read_without_bounds():
    octopus = Octopus(spark=cast(Any, "dummy_spark"), engine="postgresql")
    _set_env_vars(octopus, TEST_PG_CONFIG)

    with pytest.raises(ValueError, match="lower_bound and upper_bound are required"):
        octopus.feed_spark(
            queries=[],
            target_schema="x",
            parallel_read=True,
            partition_column="partition_col",
            num_partitions=2,
        )


def test_feed_spark_raises_when_parallel_read_invalid_num_partitions():
    octopus = Octopus(spark=cast(Any, "dummy_spark"), engine="postgresql")
    _set_env_vars(octopus, TEST_PG_CONFIG)

    with pytest.raises(ValueError, match="num_partitions must be >= 1"):
        octopus.feed_spark(
            queries=[],
            target_schema="x",
            parallel_read=True,
            partition_column="partition_col",
            lower_bound=0,
            upper_bound=10,
            num_partitions=0,
        )


def test_feed_spark_raises_when_parallel_read_invalid_bounds():
    octopus = Octopus(spark=cast(Any, "dummy_spark"), engine="postgresql")
    _set_env_vars(octopus, TEST_PG_CONFIG)

    with pytest.raises(ValueError, match="lower_bound must be less than upper_bound"):
        octopus.feed_spark(
            queries=[],
            target_schema="x",
            parallel_read=True,
            partition_column="partition_col",
            lower_bound=10,
            upper_bound=10,
            num_partitions=2,
        )


@pytest.mark.integration
def test_feed_spark_parallel_read_off_explicit(spark_session, seeded_source_tables):
    octopus = Octopus(spark=spark_session, engine="postgresql")
    _set_env_vars(octopus, TEST_PG_CONFIG)

    source_fact_table = seeded_source_tables["source_fact_table"]
    row_count = seeded_source_tables["row_count"]
    target_schema = seeded_source_tables["target_schema"]

    octopus.feed_spark(
        queries=[(f"SELECT * FROM {source_fact_table}", "fact_no_parallel")],
        target_schema=target_schema,
        parallel_read=False,
    )

    loaded = spark_session.table(f"{target_schema}.fact_no_parallel")
    assert loaded.count() == row_count


@pytest.mark.integration
def test_feed_spark_parallel_read_on_with_partition_column(spark_session, seeded_source_tables):
    octopus = Octopus(spark=spark_session, engine="postgresql")
    _set_env_vars(octopus, TEST_PG_CONFIG)

    source_fact_table = seeded_source_tables["source_fact_table"]
    row_count = seeded_source_tables["row_count"]
    target_schema = seeded_source_tables["target_schema"]

    octopus.feed_spark(
        queries=[(f"SELECT * FROM {source_fact_table}", "fact_parallel")],
        target_schema=target_schema,
        parallel_read=True,
        partition_column="partition_col",
        lower_bound=0,
        upper_bound=row_count,
        num_partitions=8,
    )

    loaded = spark_session.table(f"{target_schema}.fact_parallel")
    assert loaded.count() == row_count


@pytest.mark.integration
def test_feed_spark_multiple_queries_to_delta_tables(spark_session, seeded_source_tables):
    octopus = Octopus(spark=spark_session, engine="postgresql")
    _set_env_vars(octopus, TEST_PG_CONFIG)

    source_fact_table = seeded_source_tables["source_fact_table"]
    source_dim_table = seeded_source_tables["source_dim_table"]
    row_count = seeded_source_tables["row_count"]
    target_schema = seeded_source_tables["target_schema"]

    octopus.feed_spark(
        queries=[
            (f"SELECT * FROM {source_fact_table}", "fact_multi"),
            (f"SELECT * FROM {source_dim_table}", "dim_multi"),
        ],
        target_schema=target_schema,
    )

    loaded_fact = spark_session.table(f"{target_schema}.fact_multi")
    loaded_dim = spark_session.table(f"{target_schema}.dim_multi")
    assert loaded_fact.count() == row_count
    assert loaded_dim.count() == row_count


@pytest.mark.integration
def test_feed_spark_with_infer_schema_false_casts_to_string(spark_session, seeded_source_tables):
    octopus = Octopus(spark=spark_session, engine="postgresql")
    _set_env_vars(octopus, TEST_PG_CONFIG)

    source_fact_table = seeded_source_tables["source_fact_table"]
    target_schema = seeded_source_tables["target_schema"]

    octopus.feed_spark(
        queries=[(f"SELECT id, amount, name FROM {source_fact_table}", "fact_as_string")],
        target_schema=target_schema,
        infer_schema=False,
    )

    loaded = spark_session.table(f"{target_schema}.fact_as_string")
    dtypes = dict(loaded.dtypes)
    assert dtypes["id"] == "string"
    assert dtypes["amount"] == "string"
    assert dtypes["name"] == "string"


@pytest.mark.integration
def test_feed_spark_schema_override_applied(spark_session, seeded_source_tables):
    octopus = Octopus(spark=spark_session, engine="postgresql")
    _set_env_vars(octopus, TEST_PG_CONFIG)

    source_fact_table = seeded_source_tables["source_fact_table"]
    target_schema = seeded_source_tables["target_schema"]
    schema = StructType(
        [
            StructField("id", LongType(), True),
            StructField("amount", DoubleType(), True),
            StructField("name", StringType(), True),
        ]
    )

    octopus.feed_spark(
        queries=[(f"SELECT id, amount, name FROM {source_fact_table}", "fact_schema_override")],
        target_schema=target_schema,
        infer_schema=False,
        schema=schema,
    )

    loaded = spark_session.table(f"{target_schema}.fact_schema_override")
    dtypes = dict(loaded.dtypes)
    assert dtypes["id"] == "bigint"
    assert dtypes["amount"] == "double"
    assert dtypes["name"] == "string"


@pytest.mark.integration
def test_feed_spark_raises_for_wrong_credentials(spark_session, seeded_source_tables):
    octopus = Octopus(spark=spark_session, engine="postgresql")
    _set_env_vars(octopus, WRONG_TEST_PG_CONFIG)

    source_fact_table = seeded_source_tables["source_fact_table"]
    target_schema = seeded_source_tables["target_schema"]

    with pytest.raises(Exception, match="password authentication failed|FATAL"):
        octopus.feed_spark(
            queries=[(f"SELECT * FROM {source_fact_table}", "fact_wrong_credentials")],
            target_schema=target_schema,
        )
