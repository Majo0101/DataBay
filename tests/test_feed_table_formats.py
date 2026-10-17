"""JDBC-to-lakehouse tests against a caller-provided Spark server."""

import os
import uuid

from dotenv import dotenv_values
from typing import Any, cast

import pytest
from pyspark.sql.types import StringType, StructField, StructType

from databay.etl import Octopus


@pytest.mark.parametrize("value", [None, "parquet", "ICEBERG", "", 1, []])
def test_feed_rejects_invalid_table_format_before_io(value):
    octopus = Octopus(spark=cast(Any, "unused"), engine="postgresql")
    with pytest.raises(ValueError, match="table_format must be"):
        octopus.feed_spark([], "unused", table_format=value)


@pytest.fixture(params=["delta", "iceberg"])
def format_runtime(request, spark_session):
    formats = os.environ.get("DATABAY_TEST_TABLE_FORMATS", "").split(",")
    if request.param not in formats:
        pytest.skip("Enable configured formats with DATABAY_TEST_TABLE_FORMATS")
    return request.param, spark_session


@pytest.mark.integration
def test_feed_table_formats_create_replace_and_schema(format_runtime):
    table_format, spark = format_runtime
    namespace = "feed_format_" + uuid.uuid4().hex[:12]
    if table_format == "iceberg":
        namespace = os.environ.get("DATABAY_TEST_ICEBERG_CATALOG", "lake") + "." + namespace
    octopus = Octopus(spark=spark, engine="postgresql")
    cast(Any, octopus).env_vars = dotenv_values(
        os.environ.get("DATABAY_TEST_JDBC_ENV", ".env.test")
    )
    options = {"table_format": table_format}
    try:
        # Omitting the option still writes Delta; the explicit option is used below.
        initial_options = {} if table_format == "delta" else options
        octopus.feed_spark(
            [("SELECT 1::bigint AS id", "first"),
             ("SELECT NULL::bigint AS id", "second")],
            namespace, **initial_options,
        )
        assert spark.table(namespace + ".first").collect()[0]["id"] == 1
        assert spark.table(namespace + ".second").collect()[0]["id"] is None
        provider = next(
            row["data_type"] for row in spark.sql(
                f"DESCRIBE TABLE EXTENDED {namespace}.first"
            ).collect() if row["col_name"] == "Provider"
        )
        assert provider.lower() == table_format

        octopus.feed_spark([("SELECT 2::bigint AS id", "first")], namespace, **options)
        assert [row["id"] for row in spark.table(namespace + ".first").collect()] == [2]
        assert spark.table(namespace + ".second").count() == 1

        octopus.feed_spark(
            [("SELECT 123::bigint AS id", "typed")], namespace,
            schema=StructType([StructField("id", StringType())]),
            infer_schema=False, **options,
        )
        typed = spark.table(namespace + ".typed")
        assert typed.dtypes == [("id", "string")]
        assert typed.collect()[0]["id"] == "123"

        octopus.feed_spark(
            [("SELECT 1::bigint AS id WHERE FALSE", "first")], namespace, **options,
        )
        assert spark.table(namespace + ".first").count() == 0
    finally:
        for name in ["first", "second", "typed"]:
            spark.sql(f"DROP TABLE IF EXISTS {namespace}.{name}")
        spark.sql(f"DROP NAMESPACE IF EXISTS {namespace}")
