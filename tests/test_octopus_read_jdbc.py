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
        bind_mounts={},
    )

    try:
        dock(cfg)
        spark = spark_connect(
            app_name="pytest-databay-read-jdbc",
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
def seeded_tables(spark_session):
    octopus = Octopus(spark=spark_session, engine="postgresql")
    _set_env_vars(octopus, TEST_PG_CONFIG)

    row_count = 100_000
    fact_df = (
        spark_session.range(0, row_count)
        .withColumn("partition_col", F.col("id").cast("long"))
        .withColumn("name", F.concat(F.lit("name_"), F.col("id").cast("string")))
        .withColumn("amount", (F.col("id") / F.lit(10.0)).cast("double"))
    )
    dim_df = (
        spark_session.range(0, row_count)
        .withColumn("partition_col", F.col("id").cast("long"))
        .withColumn("category", F.concat(F.lit("cat_"), (F.col("id") % F.lit(10)).cast("string")))
    )

    fact_table = "octopus_read_fact_test"
    dim_table = "octopus_read_dim_test"

    octopus.write_jdbc(
        data={fact_table: fact_df, dim_table: dim_df},
        mode="overwrite",
        batch_size=20000,
    )

    return {"fact_table": fact_table, "dim_table": dim_table, "row_count": row_count}


def test_read_jdbc_raises_when_spark_is_missing():
    octopus = Octopus(spark=None, engine="postgresql")
    with pytest.raises(RuntimeError, match="SparkSession is not set"):
        octopus.read_jdbc(queries=[("SELECT 1", "x")])


def test_read_jdbc_raises_when_engine_is_missing():
    octopus = Octopus(spark=cast(Any, "dummy_spark"), engine=None)
    with pytest.raises(RuntimeError, match="Engine is not set"):
        octopus.read_jdbc(queries=[("SELECT 1", "x")])


def test_read_jdbc_raises_when_parallel_read_without_partition_column():
    octopus = Octopus(spark=cast(Any, "dummy_spark"), engine="postgresql")
    _set_env_vars(octopus, TEST_PG_CONFIG)

    with pytest.raises(ValueError, match="partition_column is required"):
        octopus.read_jdbc(
            queries=[],
            parallel_read=True,
            lower_bound=0,
            upper_bound=10,
            num_partitions=2,
        )


def test_read_jdbc_raises_when_parallel_read_without_bounds():
    octopus = Octopus(spark=cast(Any, "dummy_spark"), engine="postgresql")
    _set_env_vars(octopus, TEST_PG_CONFIG)

    with pytest.raises(ValueError, match="lower_bound and upper_bound are required"):
        octopus.read_jdbc(
            queries=[],
            parallel_read=True,
            partition_column="partition_col",
            num_partitions=2,
        )


def test_read_jdbc_raises_when_parallel_read_invalid_num_partitions():
    octopus = Octopus(spark=cast(Any, "dummy_spark"), engine="postgresql")
    _set_env_vars(octopus, TEST_PG_CONFIG)

    with pytest.raises(ValueError, match="num_partitions must be >= 1"):
        octopus.read_jdbc(
            queries=[],
            parallel_read=True,
            partition_column="partition_col",
            lower_bound=0,
            upper_bound=10,
            num_partitions=0,
        )


def test_read_jdbc_raises_when_parallel_read_invalid_bounds():
    octopus = Octopus(spark=cast(Any, "dummy_spark"), engine="postgresql")
    _set_env_vars(octopus, TEST_PG_CONFIG)

    with pytest.raises(ValueError, match="lower_bound must be less than upper_bound"):
        octopus.read_jdbc(
            queries=[],
            parallel_read=True,
            partition_column="partition_col",
            lower_bound=10,
            upper_bound=10,
            num_partitions=2,
        )


@pytest.mark.integration
def test_read_jdbc_returns_dataframes_for_multiple_queries(spark_session, seeded_tables):
    octopus = Octopus(spark=spark_session, engine="postgresql")
    _set_env_vars(octopus, TEST_PG_CONFIG)

    fact_table = seeded_tables["fact_table"]
    dim_table = seeded_tables["dim_table"]
    row_count = seeded_tables["row_count"]

    result = octopus.read_jdbc(
        queries=[
            (f"SELECT * FROM {fact_table}", "fact_df"),
            (f"SELECT * FROM {dim_table}", "dim_df"),
        ]
    )

    assert "fact_df" in result
    assert "dim_df" in result
    assert result["fact_df"].count() == row_count
    assert result["dim_df"].count() == row_count


@pytest.mark.integration
def test_read_jdbc_parallel_read_off_explicit(spark_session, seeded_tables):
    octopus = Octopus(spark=spark_session, engine="postgresql")
    _set_env_vars(octopus, TEST_PG_CONFIG)

    fact_table = seeded_tables["fact_table"]
    row_count = seeded_tables["row_count"]

    result = octopus.read_jdbc(
        queries=[(f"SELECT * FROM {fact_table}", "fact_df")],
        parallel_read=False,
    )

    assert "fact_df" in result
    assert result["fact_df"].count() == row_count


@pytest.mark.integration
def test_read_jdbc_parallel_read_with_partition_column(spark_session, seeded_tables):
    octopus = Octopus(spark=spark_session, engine="postgresql")
    _set_env_vars(octopus, TEST_PG_CONFIG)

    fact_table = seeded_tables["fact_table"]
    row_count = seeded_tables["row_count"]

    result = octopus.read_jdbc(
        queries=[(f"SELECT * FROM {fact_table}", "fact_df")],
        parallel_read=True,
        partition_column="partition_col",
        lower_bound=0,
        upper_bound=row_count,
        num_partitions=8,
    )

    assert "fact_df" in result
    assert result["fact_df"].count() == row_count


@pytest.mark.integration
def test_read_jdbc_with_infer_schema_false_casts_to_string(spark_session, seeded_tables):
    octopus = Octopus(spark=spark_session, engine="postgresql")
    _set_env_vars(octopus, TEST_PG_CONFIG)

    fact_table = seeded_tables["fact_table"]
    result = octopus.read_jdbc(
        queries=[(f"SELECT id, amount, name FROM {fact_table}", "fact_df")],
        infer_schema=False,
    )

    dtypes = dict(result["fact_df"].dtypes)
    assert dtypes["id"] == "string"
    assert dtypes["amount"] == "string"
    assert dtypes["name"] == "string"


@pytest.mark.integration
def test_read_jdbc_schema_override_applied(spark_session, seeded_tables):
    octopus = Octopus(spark=spark_session, engine="postgresql")
    _set_env_vars(octopus, TEST_PG_CONFIG)

    fact_table = seeded_tables["fact_table"]
    schema = StructType(
        [
            StructField("id", LongType(), True),
            StructField("amount", DoubleType(), True),
            StructField("name", StringType(), True),
        ]
    )

    result = octopus.read_jdbc(
        queries=[(f"SELECT id, amount, name FROM {fact_table}", "fact_df")],
        infer_schema=False,
        schema=schema,
    )

    dtypes = dict(result["fact_df"].dtypes)
    assert dtypes["id"] == "bigint"
    assert dtypes["amount"] == "double"
    assert dtypes["name"] == "string"


@pytest.mark.integration
def test_read_jdbc_raises_for_wrong_credentials(spark_session):
    octopus = Octopus(spark=spark_session, engine="postgresql")
    _set_env_vars(octopus, WRONG_TEST_PG_CONFIG)

    with pytest.raises(Exception, match="password authentication failed|FATAL"):
        result = octopus.read_jdbc(queries=[("SELECT 1", "x")])
        result["x"].count()
