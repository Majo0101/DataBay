"""Real Spark transformations with JDBC I/O replaced by an in-memory source."""

from types import SimpleNamespace

import pytest
from pyspark.sql.types import LongType, StructField, StructType

from databay.etl import Octopus

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("column_name", ["customer.name", "customer`name", "customer name"])
@pytest.mark.parametrize("operation", ["read", "feed"])
@pytest.mark.parametrize("use_provider", [False, True])
def test_jdbc_string_conversion_preserves_literal_names(monkeypatch, spark_session, column_name, operation, use_provider):
    schema = StructType([
        StructField(column_name, LongType()),
        StructField("customer", StructType([StructField("name", LongType())])),
    ])
    source = spark_session.createDataFrame([(123, (999,)), (None, (888,))], schema)
    reader = SimpleNamespace()
    reader.format = lambda *args: reader
    reader.option = lambda *args: reader
    options_log = []
    reader.options = lambda **kwargs: options_log.append(kwargs) or reader
    statements = []
    runtime = SimpleNamespace(read=reader, sql=statements.append)
    password_calls = []
    def password_provider():
        password_calls.append(True)
        return "provider-password"
    octopus = Octopus(spark=runtime, engine="postgresql", password_provider=password_provider if use_provider else None)
    octopus.env_vars = {"HOST": "unused.invalid", "DATABASE": "unused", "USER": "unused", "PASSWORD": "unused"}
    monkeypatch.setattr(octopus, "_load_jdbc", lambda reader, schema: source)

    if operation == "read":
        result = octopus.read_jdbc([("SELECT placeholder", "customers")], infer_schema=False)["customers"]
    else:
        captured = {}
        writer = SimpleNamespace()
        writer.using = lambda provider: writer
        writer.createOrReplace = lambda: None
        def write_to(frame, target):
            captured.update(frame=frame, target=target)
            return writer
        monkeypatch.setattr(type(source), "writeTo", write_to)
        octopus.feed_spark([("SELECT placeholder", "customers")], "raw", infer_schema=False, table_format="iceberg")
        result = captured["frame"]
        assert captured["target"] == "raw.customers"
        assert statements == ["CREATE NAMESPACE IF NOT EXISTS raw"]

    assert result.columns == [column_name, "customer"]
    assert all(data_type == "string" for _, data_type in result.dtypes)
    assert sorted((row[column_name] for row in result.collect()), key=lambda value: value or "") == [None, "123"]

    assert len(password_calls) == (1 if use_provider else 0)
    assert options_log[0]["password"] == ("provider-password" if use_provider else "unused")
