import socket
import time
from types import SimpleNamespace
from typing import Any, Callable, cast

import pytest
import pandas as pd
import warnings

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
            limit=lambda n: SimpleNamespace(toPandas=lambda: pd.DataFrame({"rows": [1]})),
            createOrReplaceTempView=lambda name: calls.__setitem__("view", name),
        )

    monkeypatch.setattr(runtime_spark, "display", lambda obj: calls["display"].append(obj))

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
    monkeypatch.setattr(runtime_spark, "display", displayed.append)
    line = "pandas" + (" customers_pd" if named else "") + " --limit 3"
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
    monkeypatch.setattr(runtime_spark, "display", displayed.append)
    with pytest.warns(UserWarning, match="truncated to 10000 rows"):
        magic("pandas", "SELECT * FROM customers")
    assert limits == [10001]
    assert len(displayed[0]) == 10000


@pytest.mark.parametrize("arguments", [
    "pandas --limit", "pandas --limit 0", "pandas --limit -1",
    "pandas --limit 1.5", "pandas --limit text", "pandas --limit 2147483647",
    "pandas --unknown 3", "pandas a b", "pandas class", "pandas bad.name",
    "pandas --limit 3 --limit 4",
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
def test_sparksql_magic_pandas_real_connect(monkeypatch):
    spark = runtime_spark.spark_connect(timeout=20)
    namespace = {}
    displayed = []
    try:
        magic = _setup_magic(monkeypatch, namespace, spark.sql)
        monkeypatch.setattr(runtime_spark, "display", displayed.append)
        with pytest.warns(UserWarning, match="truncated to 3 rows"):
            magic("pandas customers_pd --limit 3", "SELECT id FROM range(10) ORDER BY id")
        assert namespace["customers_pd"] is displayed[0]
        assert displayed[0]["id"].tolist() == [0, 1, 2]
    finally:
        spark.stop()


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
