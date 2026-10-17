"""Integration tests use an explicitly selected direct or Connect runtime."""

import os

import pytest
from pyspark.sql import SparkSession
from databay.runtime import spark_connect, spark_direct


@pytest.fixture(scope="module")
def spark_session():
    mode = os.environ.get("DATABAY_TEST_SPARK_MODE", "connect")
    if mode == "direct":
        session = spark_direct(
            master=os.environ.get("DATABAY_TEST_SPARK_MASTER", "local[2]"),
        )
    elif mode == "connect":
        remote = os.environ.get("DATABAY_TEST_SPARK_REMOTE")
        if not remote:
            pytest.skip("Set DATABAY_TEST_SPARK_REMOTE to run Spark integration tests")
        if remote.startswith("local"):
            # Explicit opt-in for a disposable test server, outside library helpers.
            session = SparkSession.builder.remote(remote).getOrCreate()
            session = spark_connect(remote="sc://localhost:15002")
        else:
            session = spark_connect(remote=remote)
    else:
        raise ValueError("DATABAY_TEST_SPARK_MODE must be direct or connect")
    session.conf.set("spark.sql.shuffle.partitions", "2")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def spark(spark_session):
    return spark_session


def pytest_collection_modifyitems(items):
    """Skip unconfigured JDBC tests before module fixtures can write seed data."""
    from dotenv import dotenv_values

    config = dotenv_values(os.environ.get("DATABAY_TEST_JDBC_ENV", ".env.test"))
    required = {"HOST", "PORT", "DATABASE", "USER", "PASSWORD"}
    if required.issubset(config):
        return
    jdbc_modules = {
        "test_octopus_read_jdbc", "test_octopus_write_jdbc",
        "test_octopus_feed_spark", "test_feed_table_formats",
    }
    skip = pytest.mark.skip(reason="Provide JDBC credentials through DATABAY_TEST_JDBC_ENV")
    for item in items:
        if item.path.stem in jdbc_modules and item.get_closest_marker("integration"):
            item.add_marker(skip)
