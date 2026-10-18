"""JDBC password providers are lazy, operation-scoped and explicit."""

from types import SimpleNamespace

import pytest

from databay.etl import Octopus


def _runtime():
    captured = []
    reader = SimpleNamespace()
    reader.format = lambda *args: reader
    reader.option = lambda *args: reader
    reader.options = lambda **options: captured.append(dict(options)) or reader
    frame = SimpleNamespace()
    reader.load = lambda: frame
    writer = SimpleNamespace()
    writer.format = lambda *args: writer
    writer.option = lambda *args: writer
    writer.options = lambda **options: captured.append(dict(options)) or writer
    writer.mode = lambda *args: writer
    writer.save = lambda: None
    writer.saveAsTable = lambda *args: None
    frame.write = writer
    return SimpleNamespace(read=reader, sql=lambda query: None), frame, captured


def _invoke(octopus, operation, frame):
    if operation == "read":
        return octopus.read_jdbc([("SELECT 1", "one"), ("SELECT 2", "two")], infer_schema=True)
    if operation == "feed":
        return octopus.feed_spark([("SELECT 1", "one"), ("SELECT 2", "two")], "raw", infer_schema=True)
    return octopus.write_jdbc({"one": frame, "two": frame})


@pytest.mark.parametrize("operation", ["read", "feed", "write"])
def test_provider_is_called_once_per_operation_and_refreshed_next_time(tmp_path, operation):
    path = tmp_path / "database.env"
    text = "HOST=database-host\nDATABASE=analytics\nUSER=db-user\nPASSWORD=old-password\n"
    path.write_text(text)
    calls = []
    def provider():
        calls.append(True)
        return f"password-{len(calls)}"
    runtime, frame, captured = _runtime()
    octopus = Octopus(env_file=path, spark=runtime, engine="postgresql", password_provider=provider)
    assert calls == []
    _invoke(octopus, operation, frame)
    assert len(calls) == 1
    assert [options["password"] for options in captured] == ["password-1", "password-1"]
    captured.clear()
    _invoke(octopus, operation, frame)
    assert len(calls) == 2
    assert [options["password"] for options in captured] == ["password-2", "password-2"]
    assert octopus.env_vars["PASSWORD"] == "old-password"
    assert path.read_text() == text


@pytest.mark.parametrize("operation", ["read", "feed", "write"])
def test_provider_failure_is_redacted_and_never_uses_env_password(operation):
    runtime, frame, captured = _runtime()
    def fail():
        raise RuntimeError("sensitive-provider-output")
    octopus = Octopus(spark=runtime, engine="postgresql", password_provider=fail)
    octopus.env_vars = {"HOST": "host", "DATABASE": "db", "PASSWORD": "fallback"}
    with pytest.raises(RuntimeError, match="no fallback") as error:
        _invoke(octopus, operation, frame)
    assert "sensitive-provider-output" not in str(error.value)
    assert error.value.__suppress_context__
    assert captured == []


@pytest.mark.parametrize("value", [None, "", "  ", 123])
def test_invalid_provider_result_does_not_fall_back(value):
    octopus = Octopus(engine="postgresql", password_provider=lambda: value)
    octopus.env_vars = {"HOST": "host", "DATABASE": "db", "PASSWORD": "fallback"}
    with pytest.raises(ValueError, match="nonempty string; no fallback"):
        octopus._jdbc_connection()


def test_provider_selects_mssql_password_auth_without_env_password(tmp_path):
    path = tmp_path / "database.env"
    path.write_text("HOST=db-host\nDATABASE=analytics\nUSER=db-user\n")
    calls = []
    octopus = Octopus(env_file=path, engine="mssql", password_provider=lambda: calls.append(True) or "secret")
    url, options = octopus._jdbc_connection()
    assert len(calls) == 1
    assert "authentication=" not in url
    assert "authentication" not in options
    assert options["password"] == "secret"


def test_env_only_mssql_does_not_start_interactive_auth(monkeypatch):
    monkeypatch.delenv("PASSWORD", raising=False)
    monkeypatch.setenv("HOST", "db-host")
    monkeypatch.setenv("DATABASE", "analytics")
    octopus = Octopus(engine="mssql")
    url, options = octopus._jdbc_connection()
    assert url == "jdbc:sqlserver://db-host;databaseName=analytics"
    assert options["password"] is None
    assert "authentication" not in options
    assert "enableDeviceCodeFlow" not in options


def test_provider_is_not_called_for_csv():
    octopus = Octopus(password_provider=lambda: pytest.fail("CSV must not retrieve database passwords"))
    with pytest.raises(RuntimeError, match="SparkSession is not set"):
        octopus.load_csv([])


def test_provider_must_be_callable():
    with pytest.raises(TypeError, match="must be callable"):
        Octopus(password_provider="not-a-function")


@pytest.mark.parametrize("operation", ["read", "feed", "write"])
def test_connection_provider_is_operation_scoped_without_env_fallback(monkeypatch, operation):
    monkeypatch.setenv("HOST", "wrong-host")
    monkeypatch.setenv("PORT", "9999")
    calls = []
    def provider():
        calls.append(True)
        return {"HOST": "postgres", "DATABASE": "test", "USER": "db-user", "PASSWORD": f"secret-{len(calls)}"}
    runtime, frame, captured = _runtime()
    octopus = Octopus(spark=runtime, engine="postgresql", connection_provider=provider)
    assert calls == []
    _invoke(octopus, operation, frame)
    assert len(calls) == 1
    assert [o["password"] for o in captured] == ["secret-1", "secret-1"]
    assert octopus._jdbc_url("postgresql", settings=provider()) == "jdbc:postgresql://postgres/test"
    assert octopus.env_vars is None
    captured.clear()
    _invoke(octopus, operation, frame)
    assert [o["password"] for o in captured] == ["secret-3", "secret-3"]


def test_connection_provider_failure_never_falls_back(monkeypatch):
    monkeypatch.setenv("PASSWORD", "fallback-secret")
    def fail():
        raise ValueError("sensitive-output")
    octopus = Octopus(engine="postgresql", connection_provider=fail)
    with pytest.raises(RuntimeError, match="no fallback") as caught:
        octopus._jdbc_connection()
    assert "sensitive" not in str(caught.value)


@pytest.mark.parametrize("settings", [None, {}, {"HOST": "postgres"}])
def test_incomplete_connection_provider_is_rejected(settings):
    octopus = Octopus(engine="postgresql", connection_provider=lambda: settings)
    with pytest.raises(ValueError):
        octopus._jdbc_connection()


def test_connection_provider_conflicts_are_rejected():
    with pytest.raises(ValueError, match="cannot be combined"):
        Octopus(connection_provider=lambda: {}, password_provider=lambda: "secret")
    with pytest.raises(ValueError, match="cannot be combined"):
        Octopus(connection_provider=lambda: {}, env_file="not-read.env")
