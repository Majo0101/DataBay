"""JDBC-to-lakehouse format tests; build both project runtime images first."""

import uuid
from typing import Any, cast

import docker
import pytest
from pyspark.sql.types import StringType, StructField, StructType

from databay.etl import Octopus
from databay.runtime.spark import spark_connect


@pytest.mark.parametrize("value", [None, "parquet", "ICEBERG", "", 1, []])
def test_feed_rejects_invalid_table_format_before_io(value):
    octopus = Octopus(spark=cast(Any, "unused"), engine="postgresql")
    with pytest.raises(ValueError, match="table_format must be"):
        octopus.feed_spark([], "unused", table_format=value)


@pytest.fixture(params=["delta", "iceberg"])
def format_runtime(request):
    table_format = request.param
    container = None
    client = None
    spark = None
    try:
        port = 15002
        if table_format == "iceberg":
            client = docker.from_env()
            container = client.containers.run(
                "spark-pg-iceberg",
                detach=True,
                ports={"15002/tcp": ("127.0.0.1", None)},
                environment={"SPARK_MEMORY": "2", "SPARK_CORES": "2"},
                extra_hosts={"host.docker.internal": "host-gateway"},
            )
            container.reload()
            port = int(container.attrs["NetworkSettings"]["Ports"]["15002/tcp"][0]["HostPort"])
        spark = spark_connect(port=port, timeout=60)
        yield table_format, spark
    finally:
        if spark is not None:
            spark.stop()
        if container is not None:
            container.remove(force=True, v=True)
        if client is not None:
            client.close()


@pytest.mark.integration
def test_feed_table_formats_create_replace_and_schema(format_runtime):
    table_format, spark = format_runtime
    namespace = "feed_format_" + uuid.uuid4().hex[:12]
    if table_format == "iceberg":
        namespace = "lake." + namespace
    octopus = Octopus(spark=spark, engine="postgresql")
    cast(Any, octopus).env_vars = {
        "HOST": "host.docker.internal", "PORT": "5432", "DATABASE": "testdb",
        "USER": "test_user", "PASSWORD": "test_pass",
    }
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
