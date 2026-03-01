import socket
import time
from types import SimpleNamespace
from typing import Any, Callable, cast

import pytest

from databay.runtime import spark as runtime_spark


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

    def _get_or_create():
        calls["created"] += 1
        return "spark-session"

    builder.appName = _app_name
    builder.remote = _remote
    builder.getOrCreate = _get_or_create

    monkeypatch.setattr(runtime_spark, "SparkSession", SimpleNamespace(builder=builder))

    spark = runtime_spark.spark_connect(
        app_name="pytest-runtime",
        host="127.0.0.1",
        port=15003,
        timeout=10,
        check_interval=0.4,
    )

    assert spark == "spark-session"
    assert calls["sleep"] == [0.4, 0.4]
    assert calls["app_name"] == "pytest-runtime"
    assert calls["remote"] == "sc://127.0.0.1:15003"
    assert calls["created"] == 1


def test_spark_connect_raises_timeout_when_port_never_opens(monkeypatch):
    monkeypatch.setattr(runtime_spark, "is_port_open", lambda host, port: False)

    ticks = iter([0.0, 0.3, 0.7, 1.2])
    monkeypatch.setattr(time, "time", lambda: next(ticks))
    monkeypatch.setattr(time, "sleep", lambda _: None)

    with pytest.raises(TimeoutError, match="Spark Connect not available on localhost:15002 within 1s"):
        runtime_spark.spark_connect(timeout=1, check_interval=0.1)


def _setup_magic(monkeypatch, user_ns, spark_sql_impl):
    captured = {"fn": None}

    monkeypatch.setattr(runtime_spark, "register_cell_magic", lambda fn: captured.__setitem__("fn", fn) or fn)
    monkeypatch.setattr(runtime_spark, "get_ipython", lambda: SimpleNamespace(user_ns=user_ns))

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
            toPandas=lambda: {"rows": [1]},
            createOrReplaceTempView=lambda name: calls.__setitem__("view", name),
        )

    monkeypatch.setattr(runtime_spark, "display", lambda obj: calls["display"].append(obj))

    magic = _setup_magic(monkeypatch, {}, _spark_sql)
    magic("pandas", "SELECT 1")
    magic("view tmp_v", "SELECT 2")

    assert calls["display"] == [{"rows": [1]}]
    assert calls["view"] == "tmp_v"


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
