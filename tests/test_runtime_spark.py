import socket
import time
from types import SimpleNamespace
from typing import Any, Callable, cast

import pytest
import pandas as pd
import warnings

from databay.runtime import spark as runtime_spark


@pytest.fixture(autouse=True)
def isolate_runtime_environment(monkeypatch, request):
    if request.node.get_closest_marker("integration"):
        return
    for name in ("SPARK_REMOTE", "SPARK_CONNECT_MODE_ENABLED", "SPARK_API_MODE"):
        monkeypatch.delenv(name, raising=False)


def _builder(monkeypatch):
    calls = {}
    session = SimpleNamespace(client=SimpleNamespace(
        enable_reattachable_execute=lambda: None,
        disable_reattachable_execute=lambda: None,
    ))
    builder = SimpleNamespace()
    def config(key=None, value=None, *, map=None):
        calls.setdefault("config", {}).update(map if map is not None else {key: value})
        return builder
    builder.config = config
    builder.appName = lambda name: calls.update(app_name=name) or builder
    builder.master = lambda master: calls.update(master=master) or builder
    builder.remote = lambda remote: calls.update(remote=remote) or builder
    builder.getOrCreate = lambda: session
    monkeypatch.setattr(runtime_spark, "SparkSession", SimpleNamespace(builder=builder))
    return calls, session


def test_direct_inherits_infrastructure_config_and_forces_classic(monkeypatch):
    calls, session = _builder(monkeypatch)
    monkeypatch.setenv("SPARK_API_MODE", "connect")
    assert runtime_spark.spark_direct() is session
    assert "master" not in calls and "remote" not in calls
    assert calls["config"] == {"spark.api.mode": "classic"}
    assert runtime_spark.os.environ["SPARK_API_MODE"] == "connect"


def test_direct_supports_explicit_master(monkeypatch):
    calls, session = _builder(monkeypatch)
    assert runtime_spark.spark_direct("Analysis", "local[2]") is session
    assert calls["master"] == "local[2]"
    assert calls["app_name"] == "Analysis"


@pytest.mark.parametrize("variable", ["SPARK_REMOTE", "SPARK_CONNECT_MODE_ENABLED"])
def test_direct_rejects_connect_environment_without_mutating_it(monkeypatch, variable):
    calls, _ = _builder(monkeypatch)
    monkeypatch.setenv(variable, "configured")
    with pytest.raises(RuntimeError, match="separate Python process"):
        runtime_spark.spark_direct()
    assert calls == {}
    assert runtime_spark.os.environ[variable] == "configured"


@pytest.mark.parametrize("master", ["", "sc://localhost:15002"])
def test_direct_rejects_invalid_master_before_builder(monkeypatch, master):
    calls, _ = _builder(monkeypatch)
    with pytest.raises(ValueError, match="Spark master URL"):
        runtime_spark.spark_direct(master=master)
    assert calls == {}


def test_connect_preserves_full_uri(monkeypatch):
    calls, session = _builder(monkeypatch)
    probes = []
    monkeypatch.setattr(runtime_spark, "is_port_open", lambda host, port: probes.append((host, port)) or True)
    remote = "sc://spark.example:443/;use_ssl=true;token=secret"
    assert runtime_spark.spark_connect(remote=remote) is session
    assert calls["remote"] == remote
    assert probes == [("spark.example", 443)]


def test_connect_uses_infrastructure_remote_and_allows_explicit_override(monkeypatch):
    calls, session = _builder(monkeypatch)
    monkeypatch.setenv("SPARK_REMOTE", "sc://infra:15002")
    monkeypatch.setattr(runtime_spark, "is_port_open", lambda *args: True)
    assert runtime_spark.spark_connect() is session
    assert calls["remote"] == "sc://infra:15002"
    runtime_spark.spark_connect(remote="sc://other:15003")
    assert calls["remote"] == "sc://other:15003"


def test_connect_handles_ipv6_host(monkeypatch):
    calls, _ = _builder(monkeypatch)
    probes = []
    monkeypatch.setattr(runtime_spark, "is_port_open", lambda host, port: probes.append((host, port)) or True)
    runtime_spark.spark_connect(host="::1")
    assert calls["remote"] == "sc://[::1]:15002"
    assert probes == [("::1", 15002)]


@pytest.mark.parametrize("remote", ["local[2]", "https://host", "sc://", "sc://host:bad", "sc://host:0"])
def test_connect_rejects_invalid_uri_without_starting_spark(monkeypatch, remote):
    calls, _ = _builder(monkeypatch)
    with pytest.raises(ValueError, match="valid sc://"):
        runtime_spark.spark_connect(remote=remote)
    assert calls == {}


@pytest.mark.parametrize("options", [{"timeout": -1}, {"check_interval": 0}, {"check_interval": -1}])
def test_connect_rejects_invalid_wait_options(options):
    with pytest.raises(ValueError, match="timeout must be"):
        runtime_spark.spark_connect(**options)


def test_connect_timeout_does_not_expose_uri_credentials(monkeypatch):
    monkeypatch.setattr(runtime_spark, "is_port_open", lambda *args: False)
    with pytest.raises(TimeoutError) as error:
        runtime_spark.spark_connect(remote="sc://host:443/;token=secret", timeout=0)
    assert "secret" not in str(error.value)


def test_magic_registration_requires_notebook_and_session(monkeypatch):
    with pytest.raises(RuntimeError, match="SparkSession is not set"):
        runtime_spark.sparksql_magic(None)
    monkeypatch.setattr("IPython.get_ipython", lambda: None)
    with pytest.raises(RuntimeError, match="IPython/Jupyter"):
        runtime_spark.sparksql_magic(object())


def test_is_port_open_returns_true_with_real_listening_socket():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("127.0.0.1", 0))
    server.listen(1)
    host, port = server.getsockname()

    try:
        result = runtime_spark.is_port_open(host, port, timeout=0.6)
    finally:
        server.close()

    assert result is True


def test_is_port_open_returns_false_when_socket_closed():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("127.0.0.1", 0))
    host, port = server.getsockname()
    server.close()

    result = runtime_spark.is_port_open(host, port, timeout=0.2)

    assert result is False


def test_spark_connect_waits_for_open_port_and_builds_session(monkeypatch):
    calls = {
        "port_checks": 0,
        "sleep": [],
        "app_name": None,
        "remote": None,
        "created": 0,
        "reattachable_disabled": 0,
    }

    def _fake_is_port_open(host, port):
        calls["port_checks"] += 1
        return calls["port_checks"] >= 3

    monkeypatch.setattr(runtime_spark, "is_port_open", _fake_is_port_open)
    monkeypatch.setattr(time, "sleep", lambda n: calls["sleep"].append(n))

    builder = SimpleNamespace()

    def _app_name(name):
        calls["app_name"] = name
        return builder

    def _remote(url):
        calls["remote"] = url
        return builder

    client = SimpleNamespace(
        disable_reattachable_execute=lambda: calls.__setitem__(
            "reattachable_disabled", calls["reattachable_disabled"] + 1
        )
    )

    def _get_or_create():
        calls["created"] += 1
        return SimpleNamespace(client=client)

    builder.appName = _app_name
    builder.remote = _remote
    builder.config = lambda **kwargs: builder
    builder.getOrCreate = _get_or_create

    monkeypatch.setattr(runtime_spark, "SparkSession", SimpleNamespace(builder=builder))

    spark = runtime_spark.spark_connect(
        app_name="pytest-runtime",
        host="127.0.0.1",
        port=15003,
        timeout=10,
        check_interval=0.4,
    )

    assert spark.client is client
    assert calls["sleep"] == [0.4, 0.4]
    assert calls["app_name"] == "pytest-runtime"
    assert calls["remote"] == "sc://127.0.0.1:15003"
    assert calls["created"] == 1
    assert calls["reattachable_disabled"] == 0


def test_spark_connect_can_keep_reattachable_execution_enabled(monkeypatch):
    client = SimpleNamespace(
        enable_reattachable_execute=lambda: None,
        disable_reattachable_execute=lambda: pytest.fail(
            "reattachable execution should remain enabled"
        )
    )
    session = SimpleNamespace(client=client)
    builder = SimpleNamespace()
    builder.appName = lambda name: builder
    builder.remote = lambda url: builder
    builder.config = lambda **kwargs: builder
    builder.getOrCreate = lambda: session
    monkeypatch.setattr(runtime_spark, "is_port_open", lambda host, port: True)
    monkeypatch.setattr(runtime_spark, "SparkSession", SimpleNamespace(builder=builder))

    assert runtime_spark.spark_connect(reattachable_execute=True) is session


def test_spark_connect_changes_reattachment_on_reused_session(monkeypatch):
    state = {"enabled": True}
    client = SimpleNamespace(
        enable_reattachable_execute=lambda: state.update(enabled=True),
        disable_reattachable_execute=lambda: state.update(enabled=False),
    )
    session = SimpleNamespace(client=client)
    builder = SimpleNamespace()
    builder.appName = lambda name: builder
    builder.remote = lambda url: builder
    builder.config = lambda **kwargs: builder
    builder.getOrCreate = lambda: session
    monkeypatch.setattr(runtime_spark, "is_port_open", lambda *args: True)
    monkeypatch.setattr(runtime_spark, "SparkSession", SimpleNamespace(builder=builder))

    for enabled in [False, True, False]:
        assert runtime_spark.spark_connect(reattachable_execute=enabled) is session
        assert state["enabled"] is enabled


def test_spark_connect_raises_timeout_when_port_never_opens(monkeypatch):
    monkeypatch.setattr(runtime_spark, "is_port_open", lambda host, port: False)

    ticks = iter([0.0, 0.3, 0.7, 1.2])
    monkeypatch.setattr(time, "monotonic", lambda: next(ticks))
    monkeypatch.setattr(time, "sleep", lambda _: None)

    with pytest.raises(TimeoutError, match="Spark Connect not available on localhost:15002 within 1s"):
        runtime_spark.spark_connect(timeout=1, check_interval=0.1)


def _setup_magic(monkeypatch, user_ns, spark_sql_impl):
    captured = {"fn": None}

    shell = SimpleNamespace(
        user_ns=user_ns,
        register_magic_function=lambda fn, kind, name: captured.__setitem__("fn", fn),
    )
    monkeypatch.setattr("IPython.get_ipython", lambda: shell)

    spark = cast(Any, SimpleNamespace(sql=spark_sql_impl))
    runtime_spark.sparksql_magic(spark)
    magic = captured["fn"]
    assert magic is not None
    return cast(Callable[[str, str], object], magic)


def test_sparksql_magic_default_shows_dataframe(monkeypatch):
    state = {"query": None, "show_called": False}

    def _spark_sql(query):
        state["query"] = query
        return SimpleNamespace(
            show=lambda truncate=False: state.__setitem__("show_called", True),
            toPandas=lambda: None,
            createOrReplaceTempView=lambda _: None,
        )

    magic = _setup_magic(monkeypatch, {}, _spark_sql)
    magic("", "SELECT 1")

    assert state["query"] == "SELECT 1"
    assert state["show_called"] is True


def test_sparksql_magic_pandas_and_view_branches(monkeypatch):
    calls = {"display": [], "view": None}

    def _spark_sql(query):
        return SimpleNamespace(
            show=lambda truncate=False: None,
            limit=lambda n: SimpleNamespace(toPandas=lambda: pd.DataFrame({"rows": [1]})),
            createOrReplaceTempView=lambda name: calls.__setitem__("view", name),
        )

    monkeypatch.setattr("IPython.display.display", lambda obj: calls["display"].append(obj))

    magic = _setup_magic(monkeypatch, {}, _spark_sql)
    magic("pandas", "SELECT 1")
    magic("view tmp_v", "SELECT 2")

    assert len(calls["display"]) == 1
    assert calls["display"][0].to_dict("list") == {"rows": [1]}
    assert calls["view"] == "tmp_v"


@pytest.mark.parametrize("rows", [0, 2, 3, 4])
@pytest.mark.parametrize("named", [False, True])
def test_sparksql_magic_pandas_limit_before_conversion(monkeypatch, rows, named):
    namespace = {"customers_pd": "old value"}
    calls = []
    displayed = []
    source = pd.DataFrame({"id": range(rows)})

    def limit(count):
        calls.append(("limit", count))

        def convert():
            calls.append(("toPandas", count))
            return source.iloc[:count].copy()

        return SimpleNamespace(toPandas=convert)

    # No toPandas on the original object: unbounded conversion would fail.
    magic = _setup_magic(monkeypatch, namespace, lambda query: SimpleNamespace(limit=limit))
    monkeypatch.setattr("IPython.display.display", displayed.append)
    line = "pandas" + (" customers_pd" if named else "") + " --limit=3"
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        magic(line, "SELECT id FROM customers ORDER BY id")
    assert calls == [("limit", 4), ("toPandas", 4)]
    pd.testing.assert_frame_equal(displayed[0], source.iloc[:3])
    assert len(caught) == (1 if rows > 3 else 0)
    if caught:
        assert "truncated to 3 rows" in str(caught[0].message)
    if named:
        assert namespace["customers_pd"] is displayed[0]
    else:
        assert namespace["customers_pd"] == "old value"


def test_sparksql_magic_pandas_default_limit(monkeypatch):
    limits = []
    displayed = []

    def limit(count):
        limits.append(count)
        return SimpleNamespace(toPandas=lambda: pd.DataFrame({"id": range(count)}))

    magic = _setup_magic(monkeypatch, {}, lambda query: SimpleNamespace(limit=limit))
    monkeypatch.setattr("IPython.display.display", displayed.append)
    with pytest.warns(UserWarning, match="truncated to 10000 rows"):
        magic("pandas", "SELECT * FROM customers")
    assert limits == [10001]
    assert len(displayed[0]) == 10000


@pytest.mark.parametrize("arguments", [
    "pandas --limit", "pandas --limit=", "pandas --limit 3",
    "pandas --limit =3", "pandas --limit= 3", "pandas --limit==3", "pandas --limit=0", "pandas --limit=-1",
    "pandas --limit=1.5", "pandas --limit=text", "pandas --limit=2147483647",
    "pandas --unknown 3", "pandas a b", "pandas class", "pandas bad.name",
    "pandas --limit=3 --limit=4",
])
def test_sparksql_magic_invalid_pandas_arguments_do_not_execute_sql(monkeypatch, arguments):
    def unexpected_sql(query):
        pytest.fail("Invalid arguments must be rejected before SQL execution")

    magic = _setup_magic(monkeypatch, {}, unexpected_sql)
    with pytest.raises(ValueError):
        magic(arguments, "SELECT 1")


def test_sparksql_magic_pandas_failed_conversion_preserves_variable(monkeypatch):
    namespace = {"customers_pd": "original"}

    def fail():
        raise RuntimeError("conversion failed")

    magic = _setup_magic(monkeypatch, namespace, lambda query: SimpleNamespace(
        limit=lambda count: SimpleNamespace(toPandas=fail)
    ))
    with pytest.raises(RuntimeError, match="conversion failed"):
        magic("pandas customers_pd", "SELECT 1")
    assert namespace["customers_pd"] == "original"


@pytest.mark.integration
def test_sparksql_magic_pandas_real_session(monkeypatch, spark_session):
    spark = spark_session
    namespace = {}
    displayed = []
    magic = _setup_magic(monkeypatch, namespace, spark.sql)
    monkeypatch.setattr("IPython.display.display", displayed.append)
    with pytest.warns(UserWarning, match="truncated to 3 rows"):
        magic("pandas customers_pd --limit=3", "SELECT id FROM range(10) ORDER BY id")
    assert namespace["customers_pd"] is displayed[0]
    assert displayed[0]["id"].tolist() == [0, 1, 2]


def test_sparksql_magic_variable_assignment_and_template_errors(monkeypatch):
    user_ns: dict[str, Any] = {"table_name": "orders"}

    def _spark_sql(query):
        return SimpleNamespace(
            query=query,
            show=lambda truncate=False: None,
            toPandas=lambda: None,
            createOrReplaceTempView=lambda _: None,
        )

    magic = _setup_magic(monkeypatch, user_ns, _spark_sql)

    magic("result_df", "SELECT * FROM {table_name}")
    assert "result_df" in user_ns
    assert user_ns["result_df"].query == "SELECT * FROM orders"

    with pytest.raises(KeyError, match="Missing Python variable in SQL template"):
        magic("", "SELECT * FROM {missing_var}")


@pytest.mark.parametrize("arguments", [
    "result --limit=10", "--limit=10", "result extra", "bad.name", "class",
    "view", "view result extra", "view result --limit=10", "view --limit=10",
])
def test_sparksql_invalid_non_pandas_arguments_do_not_execute_sql(monkeypatch, arguments):
    namespace = {"result": "original"}
    def unexpected_sql(query):
        pytest.fail("Invalid arguments must be rejected before SQL execution")
    magic = _setup_magic(monkeypatch, namespace, unexpected_sql)
    with pytest.raises(ValueError, match="Usage:"):
        magic(arguments, "SELECT 1")
    assert namespace == {"result": "original"}


@pytest.mark.parametrize("query", [
    """SELECT '{"id": 1, "nested": {"x": 2}}' AS payload""",
    "SELECT 'ab' RLIKE '^[a-z]{2}$' AS matched",
    "SELECT 'abc' RLIKE '^[a-z]{2,4}$' AS matched",
    "SELECT '{{literal}}' AS braces",
    "SELECT '{' AS opening, '}' AS closing",
])
def test_sparksql_preserves_non_placeholder_braces(monkeypatch, query):
    seen = []
    magic = _setup_magic(monkeypatch, {}, lambda sql: seen.append(sql) or SimpleNamespace(show=lambda **kwargs: None))
    magic("", query)
    assert seen == [query]


def test_sparksql_replaces_only_identifier_placeholders_once(monkeypatch):
    namespace = {"table_name": "orders", "value": '{"id": 1}'}
    seen = []
    magic = _setup_magic(monkeypatch, namespace, lambda sql: seen.append(sql) or SimpleNamespace(show=lambda **kwargs: None))
    magic("", "SELECT '{value}' AS payload FROM {table_name} WHERE id = {2}")
    assert seen == ["""SELECT '{"id": 1}' AS payload FROM orders WHERE id = {2}"""]


@pytest.mark.integration
def test_sparksql_json_regex_and_placeholder_execute(monkeypatch, spark_session):
    namespace = {"value": 42}
    magic = _setup_magic(monkeypatch, namespace, spark_session.sql)
    magic("result", """SELECT '{"id": 1}' AS payload,
          'ab' RLIKE '^[a-z]{2}$' AS matched, {value} AS value""")
    assert namespace["result"].first().asDict() == {
        "payload": '{"id": 1}', "matched": True, "value": 42,
    }


def test_connect_default_does_not_modify_client(monkeypatch):
    _, session = _builder(monkeypatch)
    session.client = SimpleNamespace(
        enable_reattachable_execute=lambda: pytest.fail("Default must preserve client settings"),
        disable_reattachable_execute=lambda: pytest.fail("Default must preserve client settings"),
    )
    monkeypatch.setattr(runtime_spark, "is_port_open", lambda *args: True)
    assert runtime_spark.spark_connect() is session
    assert runtime_spark.spark_connect(reattachable_execute=None) is session


def test_core_import_does_not_require_ipython():
    import subprocess
    import sys
    code = """
import builtins
original_import = builtins.__import__
def without_notebooks(name, *args, **kwargs):
    if name == 'IPython' or name.startswith('IPython.'):
        raise AssertionError('Core import must not load IPython')
    return original_import(name, *args, **kwargs)
builtins.__import__ = without_notebooks
from databay import null_rate, compare_datasets
from databay.runtime import spark_direct, spark_connect
"""
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_sparksql_magic_registers_with_real_ipython(monkeypatch):
    from IPython.core.interactiveshell import InteractiveShell
    shell = InteractiveShell()
    monkeypatch.setattr("IPython.get_ipython", lambda: shell)
    shown = []
    spark = SimpleNamespace(sql=lambda sql: SimpleNamespace(show=lambda **kwargs: shown.append(sql)))
    assert runtime_spark.sparksql_magic(spark) is True
    shell.run_cell_magic("sparksql", "", "SELECT 1")
    assert shown == ["SELECT 1"]
