"""Connection validation uses placeholders and never contacts a database."""

import pytest
from pyspark.sql.types import StringType, StructField, StructType

from databay.etl import Octopus


def test_engine_is_normalized_before_provider_use():
    octopus = Octopus(engine=" MSSQL ", connection_provider=lambda: {
        "HOST": "sql.example", "DATABASE": "analytics", "USER": "example", "PASSWORD": "placeholder",
    })
    url, options = octopus._jdbc_connection()
    assert octopus.engine == "mssql"
    assert url == "jdbc:sqlserver://sql.example;databaseName=analytics"
    assert options["driver"] == "com.microsoft.sqlserver.jdbc.SQLServerDriver"


@pytest.mark.parametrize("engine", ["unknown", "", 123])
def test_invalid_engine_fails_at_construction(engine):
    with pytest.raises(ValueError):
        Octopus(engine=engine, connection_provider=lambda: pytest.fail("Must not retrieve settings"))


@pytest.mark.parametrize("host", ["db;user=example", "db/path", "db?sslmode=disable", "db:5432", " db", "db\n", "[invalid]", "db\\instance"])
def test_invalid_endpoint_does_not_call_password_provider(host):
    octopus = Octopus(engine="postgresql", password_provider=lambda: pytest.fail("Must not retrieve password"))
    octopus.env_vars = {"HOST": host, "DATABASE": "analytics"}
    with pytest.raises(ValueError, match="HOST"):
        octopus._jdbc_connection()


@pytest.mark.parametrize("engine,host,database,expected", [
    ("postgresql", "localhost", "analytics?sslmode=disable", "jdbc:postgresql://localhost/analytics%3Fsslmode%3Ddisable"),
    ("postgresql", "db_service", "sales / 2026", "jdbc:postgresql://db_service/sales%20%2F%202026"),
    ("postgresql", "::1", "analytics", "jdbc:postgresql://[::1]/analytics"),
    ("mssql", "sql\\INSTANCE", "analytics", "jdbc:sqlserver://sql\\INSTANCE;databaseName=analytics"),
    ("mssql", "sql", "sales;user=example", "jdbc:sqlserver://sql;databaseName={sales;user=example}"),
    ("mssql", "sql", "sales} 2026", "jdbc:sqlserver://sql;databaseName={sales}} 2026}"),
    ("oracle", "oracle", "service.example", "jdbc:oracle:thin:@//oracle/service.example"),
])
def test_database_names_and_hosts_are_formatted_safely(engine, host, database, expected):
    octopus = Octopus(engine=engine)
    assert octopus._jdbc_url(engine, settings={"HOST": host, "DATABASE": database}) == expected


@pytest.mark.parametrize("port", ["0", "65536", "1433;user=example", "abc"])
def test_invalid_environment_port_is_rejected(port):
    octopus = Octopus(engine="mssql", password_provider=lambda: pytest.fail("Must not retrieve password"))
    octopus.env_vars = {"HOST": "sql", "DATABASE": "analytics", "PORT": port}
    with pytest.raises(ValueError, match="PORT"):
        octopus._jdbc_connection()


@pytest.mark.parametrize("operation", ["read", "feed", "write"])
@pytest.mark.parametrize("provider_kind", ["password", "connection"])
def test_invalid_operation_arguments_do_not_call_providers(operation, provider_kind):
    provider = lambda: pytest.fail("Must validate before retrieving credentials")
    octopus = Octopus(spark=object(), engine="postgresql", **{f"{provider_kind}_provider": provider})
    with pytest.raises(ValueError):
        if operation == "read":
            octopus.read_jdbc([], parallel_read=True)
        elif operation == "feed":
            octopus.feed_spark([("SELECT 1", "one")], " ")
        else:
            octopus.write_jdbc(object())


@pytest.mark.parametrize("operation", ["read", "feed"])
@pytest.mark.parametrize("schema", ["invalid", StructType([StructField("id", StringType()), StructField("id", StringType())])])
def test_invalid_schema_is_rejected_before_provider(operation, schema):
    octopus = Octopus(spark=object(), engine="postgresql", connection_provider=lambda: pytest.fail("Must validate schema first"))
    with pytest.raises((ValueError, TypeError)):
        if operation == "read":
            octopus.read_jdbc([], schema=schema)
        else:
            octopus.feed_spark([], "raw", schema=schema)


def test_all_query_entries_are_checked_before_provider():
    octopus = Octopus(spark=object(), engine="postgresql", connection_provider=lambda: pytest.fail("Must validate every query first"))
    with pytest.raises(ValueError, match="pairs"):
        octopus.read_jdbc(iter([("SELECT 1", "one"), ("malformed",)]))
