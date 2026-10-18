"""Timeout properties are checked without Spark, vault or database access."""

from types import SimpleNamespace

import pytest

from databay.etl import Octopus


def _runtime():
    captured = []
    io = SimpleNamespace()
    io.format = lambda *a: io
    io.option = lambda *a: io
    io.options = lambda **kw: captured.append(dict(kw)) or io
    io.mode = lambda *a: io
    io.save = lambda: None
    io.saveAsTable = lambda *a: None
    frame = SimpleNamespace(write=io)
    io.load = lambda: frame
    return SimpleNamespace(read=io, sql=lambda *a: None), frame, captured


def _invoke(octopus, operation, frame, **timeouts):
    if operation == "read":
        return octopus.read_jdbc([("SELECT 1", "one"), ("SELECT 2", "two")], **timeouts)
    if operation == "feed":
        return octopus.feed_spark([("SELECT 1", "one"), ("SELECT 2", "two")], "raw", **timeouts)
    return octopus.write_jdbc({"one": frame, "two": frame}, **timeouts)


@pytest.mark.parametrize("operation", ["read", "feed", "write"])
@pytest.mark.parametrize("engine,expected", [
    ("postgresql", {"connectTimeout": "15", "loginTimeout": "15", "socketTimeout": "360"}),
    ("postgres", {"connectTimeout": "15", "loginTimeout": "15", "socketTimeout": "360"}),
    ("postgre", {"connectTimeout": "15", "loginTimeout": "15", "socketTimeout": "360"}),
    ("mssql", {"loginTimeout": "15", "socketTimeout": "360000"}),
    ("sqlserver", {"loginTimeout": "15", "socketTimeout": "360000"}),
    ("oracle", {"oracle.net.CONNECT_TIMEOUT": "15000", "oracle.net.OUTBOUND_CONNECT_TIMEOUT": "15", "oracle.jdbc.ReadTimeout": "360000"}),
])
def test_all_operations_forward_driver_specific_timeout_properties(operation, engine, expected):
    runtime, frame, captured = _runtime()
    calls = []
    def provider():
        calls.append(True)
        return {"HOST": "db.example", "DATABASE": "analytics", "USER": "example", "PASSWORD": "placeholder"}
    octopus = Octopus(spark=runtime, engine=engine, connection_provider=provider)
    _invoke(octopus, operation, frame, connect_timeout=15, query_timeout=300, socket_timeout=360)
    assert len(calls) == 1
    expected = {**expected, "queryTimeout": "300"}
    for properties in captured:
        actual = {k: v for k, v in properties.items() if k not in {"driver", "user", "password", "encrypt", "trustServerCertificate"}}
        assert actual == expected
    assert len(captured) == 2
    # A new operation without limits must not inherit the previous values.
    captured.clear()
    _invoke(octopus, operation, frame)
    assert len(calls) == 2
    for properties in captured:
        assert not set(expected).intersection(properties)


@pytest.mark.parametrize("argument", ["connect_timeout", "query_timeout", "socket_timeout"])
@pytest.mark.parametrize("value", [0, -1, True, 1.5, "15", float("inf"), float("nan"), 2147483648])
def test_invalid_timeout_values_are_rejected_before_provider(argument, value):
    octopus = Octopus(spark=object(), engine="postgresql", connection_provider=lambda: pytest.fail("Must not retrieve credentials"))
    with pytest.raises(ValueError, match=argument):
        octopus.read_jdbc([], **{argument: value})


@pytest.mark.parametrize("operation", ["read", "feed", "write"])
@pytest.mark.parametrize("kind", ["password", "connection"])
def test_timeout_validation_precedes_each_provider(operation, kind):
    provider = lambda: pytest.fail("Must not retrieve credentials")
    octopus = Octopus(spark=object(), engine="mssql", **{f"{kind}_provider": provider})
    with pytest.raises(ValueError, match="connect_timeout"):
        _invoke(octopus, operation, object(), connect_timeout=65536)


@pytest.mark.parametrize("engine", ["postgresql", "mssql", "oracle"])
def test_timeouts_can_be_selected_independently(engine):
    runtime, frame, captured = _runtime()
    octopus = Octopus(spark=runtime, engine=engine, connection_provider=lambda: {
        "HOST": "db.example", "DATABASE": "analytics", "USER": "example", "PASSWORD": "placeholder",
    })
    _invoke(octopus, "read", frame, query_timeout=1)
    for properties in captured:
        assert properties["queryTimeout"] == "1"
        assert not {"loginTimeout", "connectTimeout", "socketTimeout", "oracle.net.CONNECT_TIMEOUT", "oracle.jdbc.ReadTimeout"}.intersection(properties)


@pytest.mark.parametrize("engine,argument,maximum,key,expected", [
    ("mssql", "connect_timeout", 65535, "loginTimeout", "65535"),
    ("postgresql", "connect_timeout", 2147483, "connectTimeout", "2147483"),
    ("oracle", "connect_timeout", 2147483, "oracle.net.CONNECT_TIMEOUT", "2147483000"),
    ("mssql", "socket_timeout", 2147483, "socketTimeout", "2147483000"),
    ("oracle", "socket_timeout", 2147483, "oracle.jdbc.ReadTimeout", "2147483000"),
    ("postgresql", "query_timeout", 2147483647, "queryTimeout", "2147483647"),
])
def test_timeout_boundaries_do_not_overflow_driver_integers(engine, argument, maximum, key, expected):
    octopus = Octopus(engine=engine)
    values = dict(connect_timeout=None, query_timeout=None, socket_timeout=None)
    values[argument] = maximum
    assert octopus._jdbc_timeout_options(**values)[key] == expected
    values[argument] += 1
    with pytest.raises(ValueError, match=argument):
        octopus._jdbc_timeout_options(**values)
