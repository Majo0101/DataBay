"""Combined failures are simulated; no real vault is used."""

import subprocess
import traceback
from types import SimpleNamespace

import pytest

from databay.etl import BitwardenError, Octopus, bitwarden_connection, bitwarden_password, credentials


@pytest.fixture
def lifecycle(monkeypatch):
    calls = []
    failures = {}
    monkeypatch.setattr(credentials.getpass, "getpass", lambda prompt: "private-master")
    def run(command, **kwargs):
        stage = command[3]
        calls.append(stage)
        failure = failures.get(stage)
        if isinstance(failure, BaseException):
            raise failure
        return SimpleNamespace(
            returncode=1 if failure else 0,
            stdout={"unlock": "private-session", "get": "private-password", "lock": ""}[stage],
            stderr="private-cli-output",
        )
    monkeypatch.setattr(credentials.subprocess, "run", run)
    return calls, failures


def _assert_redacted(error):
    output = "".join(traceback.format_exception(type(error), error, error.__traceback__))
    assert "private-" not in output


@pytest.mark.parametrize("stage", ["unlock", "get", "lock"])
def test_single_failure_reports_stage_and_lock_status(lifecycle, stage):
    calls, failures = lifecycle
    failures[stage] = True
    with pytest.raises(BitwardenError) as caught:
        bitwarden_password("item-id")
    error = caught.value
    assert error.stage == stage
    assert error.lock_failed is (stage == "lock")
    assert ("bw lock manually" in str(error)) is (stage == "lock")
    assert calls[-1] == "lock"
    _assert_redacted(error)


@pytest.mark.parametrize("stage", ["unlock", "get"])
@pytest.mark.parametrize("cleanup", [True, OSError("private-os-error"), subprocess.TimeoutExpired("bw", 1, output="private-timeout-output")])
def test_primary_and_cleanup_failures_are_both_reported(lifecycle, stage, cleanup):
    calls, failures = lifecycle
    failures.update({stage: True, "lock": cleanup})
    with pytest.raises(BitwardenError) as caught:
        bitwarden_password("item-id")
    error = caught.value
    assert error.stage == stage
    assert error.lock_failed
    assert f"during {stage}" in str(error)
    assert "bw lock manually" in str(error)
    assert calls.count("lock") == 1
    _assert_redacted(error)


def test_empty_password_reason_survives_failed_cleanup(lifecycle, monkeypatch):
    _, failures = lifecycle
    failures["lock"] = True
    run = credentials.subprocess.run
    def empty_password(command, **kwargs):
        result = run(command, **kwargs)
        if command[3] == "get":
            result.stdout = ""
        return result
    monkeypatch.setattr(credentials.subprocess, "run", empty_password)
    with pytest.raises(BitwardenError, match="empty password") as caught:
        bitwarden_password("item-id")
    assert caught.value.stage == "get"
    assert caught.value.reason == "empty_password"
    assert caught.value.lock_failed


@pytest.mark.parametrize("stage", ["prompt", "unlock", "get"])
@pytest.mark.parametrize("lock_fails", [False, True])
def test_cancellation_is_preserved_even_if_lock_fails(lifecycle, monkeypatch, stage, lock_fails):
    calls, failures = lifecycle
    cancellation = KeyboardInterrupt()
    if stage == "prompt":
        def cancel(prompt):
            raise cancellation
        monkeypatch.setattr(credentials.getpass, "getpass", cancel)
    else:
        failures[stage] = cancellation
    if lock_fails:
        failures["lock"] = True
    with pytest.raises(KeyboardInterrupt) as caught:
        bitwarden_password("item-id")
    assert caught.value is cancellation
    assert calls[-1] == "lock"
    if lock_fails:
        assert cancellation.stage == stage
        assert cancellation.lock_failed
        assert "bw lock manually" in str(cancellation)
    _assert_redacted(cancellation)


@pytest.mark.parametrize("primary_fails", [False, True])
def test_interrupting_lock_does_not_return_credentials(lifecycle, primary_fails):
    _, failures = lifecycle
    cancellation = KeyboardInterrupt()
    failures["lock"] = cancellation
    if primary_fails:
        failures["get"] = True
    with pytest.raises(KeyboardInterrupt) as caught:
        bitwarden_password("item-id")
    assert caught.value is cancellation
    assert cancellation.lock_failed
    assert cancellation.stage == ("get" if primary_fails else "lock")
    _assert_redacted(cancellation)


@pytest.mark.parametrize("kind", ["password", "connection"])
@pytest.mark.parametrize("cancelled", [False, True])
def test_cleanup_information_survives_octopus_providers(lifecycle, kind, cancelled):
    _, failures = lifecycle
    failures.update({"get": KeyboardInterrupt() if cancelled else True, "lock": True})
    provider = (lambda: bitwarden_password("item-id")) if kind == "password" else (lambda: bitwarden_connection("item-id"))
    octopus = Octopus(engine="postgresql", **{f"{kind}_provider": provider})
    octopus.env_vars = {"HOST": "database.example", "DATABASE": "analytics"}
    with pytest.raises(KeyboardInterrupt if cancelled else BitwardenError) as caught:
        octopus._jdbc_connection()
    assert caught.value.stage == "get"
    assert caught.value.lock_failed
    assert "bw lock manually" in str(caught.value)
    _assert_redacted(caught.value)


def test_callers_active_exception_is_not_mistaken_for_vault_failure(lifecycle):
    _, failures = lifecycle
    failures["lock"] = True
    try:
        raise ValueError("private-caller-error")
    except ValueError:
        with pytest.raises(BitwardenError) as caught:
            bitwarden_password("item-id")
    assert caught.value.stage == "lock"
    assert caught.value.lock_failed
    _assert_redacted(caught.value)
