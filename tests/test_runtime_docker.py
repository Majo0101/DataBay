from types import SimpleNamespace

import pytest
from docker.errors import APIError, NotFound

from databay.runtime import docker as runtime_docker


def _make_container(container_id: str, status: str = "running"):
    state = {
        "reload_called": False,
        "start_called": False,
        "stop_called": False,
        "remove_called": False,
    }

    container = SimpleNamespace(id=container_id, status=status)

    def reload():
        state["reload_called"] = True

    def start():
        state["start_called"] = True
        container.status = "running"

    def stop():
        state["stop_called"] = True
        container.status = "exited"

    def remove():
        state["remove_called"] = True

    container.reload = reload
    container.start = start
    container.stop = stop
    container.remove = remove
    return container, state


def _make_client(existing_container=None, missing_volumes=None, run_error=None, run_id="run-999"):
    missing_volumes = missing_volumes or set()
    state = {
        "created_volumes": [],
        "run_kwargs": None,
    }

    def volume_get(name):
        if name in missing_volumes:
            raise NotFound("missing")
        return object()

    def volume_create(name):
        state["created_volumes"].append(name)

    def containers_get(name):
        if existing_container is None:
            raise NotFound("missing")
        return existing_container

    def containers_run(image, **kwargs):
        if run_error is not None:
            raise run_error
        state["run_kwargs"] = {"image": image, **kwargs}
        return SimpleNamespace(id=run_id)

    volumes = SimpleNamespace(get=volume_get, create=volume_create)
    containers = SimpleNamespace(get=containers_get, run=containers_run)
    client = SimpleNamespace(volumes=volumes, containers=containers)

    return client, state


def test_dock_starts_existing_stopped_container(monkeypatch):
    existing, container_state = _make_container("existing-1", status="exited")
    client, client_state = _make_client(existing_container=existing)

    monkeypatch.setattr("databay.runtime.docker.docker.from_env", lambda: client)

    cfg = runtime_docker.DockConfig(name="spark-a", named_volumes={"v1": "/x"}, bind_mounts={})
    container_id = runtime_docker.dock(cfg)

    assert container_id == "existing-1"
    assert container_state["reload_called"] is True
    assert container_state["start_called"] is True
    assert client_state["run_kwargs"] is None


def test_dock_creates_missing_volume_and_runs_new_container(monkeypatch):
    client, state = _make_client(existing_container=None, missing_volumes={"lake"})

    monkeypatch.setattr("databay.runtime.docker.docker.from_env", lambda: client)

    cfg = runtime_docker.DockConfig(
        image="spark-pg-delta",
        name="spark-b",
        named_volumes={"lake": "/lakehouse"},
        bind_mounts={"/host/landing": "/data/apache"},
        ports={"15002/tcp": 15002},
        env={"A": "B"},
        extra_hosts={"host.docker.internal": "host-gateway"},
        network="net1",
        restart_policy={"Name": "unless-stopped"},
        wsl_uid=1000,
        wsl_gid=1001,
    )

    container_id = runtime_docker.dock(cfg)

    assert container_id == "run-999"
    assert state["created_volumes"] == ["lake"]
    assert state["run_kwargs"] is not None
    assert state["run_kwargs"]["name"] == "spark-b"
    assert state["run_kwargs"]["detach"] is True
    assert state["run_kwargs"]["user"] == "1000:1001"
    assert state["run_kwargs"]["extra_hosts"] == {
        "host.docker.internal": "host-gateway"
    }
    assert state["run_kwargs"]["volumes"] == {
        "lake": {"bind": "/lakehouse", "mode": "rw"},
        "/host/landing": {"bind": "/data/apache", "mode": "rw"},
    }


def test_dock_wraps_api_error(monkeypatch):
    err = APIError("daemon unavailable", explanation="daemon unavailable")
    client, _ = _make_client(existing_container=None, run_error=err)

    monkeypatch.setattr("databay.runtime.docker.docker.from_env", lambda: client)

    cfg = runtime_docker.DockConfig(named_volumes={}, bind_mounts={})

    with pytest.raises(RuntimeError, match="Docker failed: daemon unavailable"):
        runtime_docker.dock(cfg)


def test_dock_spark_init_orchestrates_steps(monkeypatch):
    calls = {
        "dock": 0,
        "sleep": [],
        "connect": [],
        "magic": [],
    }

    existing_stopped = {"called": False}

    def _stop_existing():
        existing_stopped["called"] = True

    existing = SimpleNamespace(stop=_stop_existing)
    spark_obj = SimpleNamespace(version="3.5.1")

    monkeypatch.setattr(runtime_docker, "dock", lambda cfg: calls.__setitem__("dock", calls["dock"] + 1) or "abc123456789")
    monkeypatch.setattr("databay.runtime.docker.time.sleep", lambda delay: calls["sleep"].append(delay))
    monkeypatch.setattr(runtime_docker, "SparkSession", SimpleNamespace(getActiveSession=lambda: existing))

    def _fake_connect(**kwargs):
        calls["connect"].append(kwargs)
        return spark_obj

    monkeypatch.setattr(runtime_docker, "spark_connect", _fake_connect)

    def _fake_magic(spark):
        calls["magic"].append(spark)
        return True

    monkeypatch.setattr(runtime_docker, "sparksql_magic", _fake_magic)

    cfg = runtime_docker.DockConfig(name="spark-c")
    result = runtime_docker.dock_spark_init(
        cfg,
        app_name="pytest-app",
        host="127.0.0.1",
        port=15003,
        container_delay=0.4,
        connect_timeout=9,
        check_interval=0.3,
    )

    assert result is spark_obj
    assert calls["dock"] == 1
    assert calls["sleep"] == [0.4]
    assert existing_stopped["called"] is True
    assert calls["connect"] == [
        {
            "app_name": "pytest-app",
            "host": "127.0.0.1",
            "port": 15003,
            "timeout": 9,
            "check_interval": 0.3,
        }
    ]
    assert calls["magic"] == [spark_obj]


def test_dock_shutdown_returns_when_container_missing(monkeypatch):
    client, _ = _make_client(existing_container=None)

    monkeypatch.setattr("databay.runtime.docker.docker.from_env", lambda: client)

    cfg = runtime_docker.DockConfig(name="missing")
    runtime_docker.dock_shutdown(cfg, remove=True)


def test_dock_shutdown_stops_and_removes_running_container(monkeypatch):
    container, state = _make_container("c1", status="running")
    client, _ = _make_client(existing_container=container)

    monkeypatch.setattr("databay.runtime.docker.docker.from_env", lambda: client)

    cfg = runtime_docker.DockConfig(name="running")
    runtime_docker.dock_shutdown(cfg, remove=True)

    assert state["stop_called"] is True
    assert state["remove_called"] is True


def test_dock_shutdown_swallows_remove_api_error(monkeypatch):
    container, state = _make_container("c2", status="exited")

    def _remove_raises():
        state["remove_called"] = True
        raise APIError("cannot remove", explanation="cannot remove")

    container.remove = _remove_raises
    client, _ = _make_client(existing_container=container)

    monkeypatch.setattr("databay.runtime.docker.docker.from_env", lambda: client)

    cfg = runtime_docker.DockConfig(name="exited")
    runtime_docker.dock_shutdown(cfg, remove=True)

    assert state["remove_called"] is True
