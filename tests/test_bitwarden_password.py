"""Interactive Bitwarden lifecycle tests never access a real vault."""

import subprocess
import warnings
from types import SimpleNamespace

import pytest

from databay.etl import bitwarden_password, credentials


@pytest.fixture
def vault(monkeypatch):
    calls = []
    state = {"fail": None, "password": "  literal-password  \n"}
    monkeypatch.setattr(credentials.getpass, "getpass", lambda prompt: "master-secret")
    monkeypatch.setenv("BW_SESSION", "existing-session")

    def run(command, **kwargs):
        calls.append((command, {**kwargs, "env": dict(kwargs["env"])}))
        operation = command[3]
        failure = state["fail"]
        if failure == operation:
            return SimpleNamespace(returncode=1, stdout="sensitive-output", stderr="secret")
        if isinstance(failure, BaseException) and operation == "get":
            raise failure
        output = {"unlock": "new-session\n", "get": state["password"], "lock": ""}[operation]
        return SimpleNamespace(returncode=0, stdout=output, stderr="")

    monkeypatch.setattr(credentials.subprocess, "run", run)
    return calls, state


def test_prompt_unlock_read_lock_and_isolate_environment(vault):
    calls, _ = vault
    assert bitwarden_password("item-id", timeout=3) == "  literal-password  "
    assert [call[0][3:] for call in calls] == [
        ["unlock", "--passwordenv", "_DATABAY_BW_MASTER"],
        ["get", "password", "item-id"], ["lock"],
    ]
    assert calls[0][1]["env"]["_DATABAY_BW_MASTER"] == "master-secret"
    assert "BW_SESSION" not in calls[0][1]["env"]
    for command, options in calls:
        assert "master-secret" not in command
        assert "new-session" not in command
        assert options["stdin"] == subprocess.DEVNULL
        assert options["timeout"] == 3
        assert options["encoding"] == "utf-8"
        assert "shell" not in options
    for _, options in calls[1:]:
        assert "_DATABAY_BW_MASTER" not in options["env"]
        assert options["env"]["BW_SESSION"] == "new-session"
    assert credentials.os.environ["BW_SESSION"] == "existing-session"
    assert "_DATABAY_BW_MASTER" not in credentials.os.environ


@pytest.mark.parametrize("operation", ["unlock", "get", "lock"])
def test_failures_lock_and_never_expose_output(vault, operation):
    calls, state = vault
    state["fail"] = operation
    with pytest.raises(RuntimeError) as caught:
        bitwarden_password("item-id")
    assert calls[-1][0][3] == "lock"
    assert "sensitive" not in str(caught.value)
    assert "secret" not in str(caught.value)


@pytest.mark.parametrize("failure", [FileNotFoundError("secret"), subprocess.TimeoutExpired("bw", 1, output="secret"), KeyboardInterrupt()])
def test_retrieval_exception_still_locks(vault, failure):
    calls, state = vault
    state["fail"] = failure
    with pytest.raises(KeyboardInterrupt if isinstance(failure, KeyboardInterrupt) else RuntimeError):
        bitwarden_password("item-id")
    assert calls[-1][0][3] == "lock"


@pytest.mark.parametrize("output", ["", "\n", "   \n"])
def test_empty_password_is_rejected_and_locked(vault, output):
    calls, state = vault
    state["password"] = output
    with pytest.raises(RuntimeError, match="empty password"):
        bitwarden_password("item-id")
    assert calls[-1][0][3] == "lock"


@pytest.mark.parametrize("item", [None, "", "  ", "--help"])
def test_invalid_item_is_rejected_without_prompt(monkeypatch, item):
    monkeypatch.setattr(credentials.getpass, "getpass", lambda prompt: pytest.fail("Must not prompt"))
    with pytest.raises(ValueError, match="item ID"):
        bitwarden_password(item)


@pytest.mark.parametrize("timeout", [0, -1, float("inf"), float("nan")])
def test_invalid_timeout_is_rejected(timeout):
    with pytest.raises(ValueError, match="finite and positive"):
        bitwarden_password("item-id", timeout=timeout)


def test_connection_reads_one_item_and_locks(vault):
    import json
    from databay.etl import bitwarden_connection

    calls, state = vault
    state["password"] = json.dumps({
        "login": {"username": "db-user", "password": "  db-secret  "},
        "fields": [
            {"name": "HOST", "value": "postgres"},
            {"name": "port", "value": "5432"},
            {"name": "database", "value": "test"},
        ],
    })
    assert bitwarden_connection("postgre_local") == {
        "HOST": "postgres", "PORT": "5432", "DATABASE": "test",
        "USER": "db-user", "PASSWORD": "  db-secret  ",
    }
    assert calls[1][0][3:] == ["get", "item", "postgre_local"]
    assert calls[-1][0][3] == "lock"


@pytest.mark.parametrize("payload", ["sensitive-invalid-json", "null", "{}", '{"login": {"password": "secret"}}'])
def test_invalid_connection_data_is_redacted_and_locked(vault, payload):
    from databay.etl import bitwarden_connection

    calls, state = vault
    state["password"] = payload
    with pytest.raises(ValueError, match="Invalid Bitwarden connection") as caught:
        bitwarden_connection("item")
    assert "secret" not in str(caught.value)
    assert "sensitive" not in str(caught.value)
    assert calls[-1][0][3] == "lock"


def test_connection_requires_login_password(vault):
    import json
    from databay.etl import bitwarden_connection

    calls, state = vault
    state["password"] = json.dumps({
        "login": {"username": "db-user"},
        "fields": [
            {"name": "host", "value": "database.example"},
            {"name": "database", "value": "analytics"},
        ],
    })
    with pytest.raises(ValueError, match="login username/password"):
        bitwarden_connection("item-id")
    assert calls[-1][0][3] == "lock"


def test_visible_password_fallback_aborts_before_reading(vault, monkeypatch):
    calls, _ = vault
    monkeypatch.setattr(credentials.getpass, "getpass", credentials.getpass.fallback_getpass)
    monkeypatch.setattr(credentials.getpass, "_raw_input", lambda *a, **kw: pytest.fail("Must not read visible input"))
    with pytest.raises(RuntimeError, match="requires hidden password input"):
        bitwarden_password("item-id")
    assert [call[0][3] for call in calls] == ["lock"]


def test_hidden_input_error_does_not_change_global_warning_filters(vault, monkeypatch):
    def unavailable(prompt):
        warnings.warn("Echo is available", credentials.getpass.GetPassWarning)
    monkeypatch.setattr(credentials.getpass, "getpass", unavailable)
    filters = list(warnings.filters)
    with pytest.raises(RuntimeError, match="requires hidden password input"):
        bitwarden_password("item-id")
    assert warnings.filters == filters


def test_unicode_cli_decode_error_is_redacted_and_locked(vault):
    calls, state = vault
    state["fail"] = UnicodeDecodeError("utf-8", b"private-output", 0, 1, "private-error")
    with pytest.raises(RuntimeError, match="Bitwarden command failed") as caught:
        bitwarden_password("item-id")
    assert "private" not in str(caught.value)
    assert calls[-1][0][3] == "lock"


def test_cli_decodes_utf8_without_using_locale(monkeypatch):
    import sys
    run = subprocess.run
    def child(command, **kwargs):
        assert kwargs["encoding"] == "utf-8"
        return run([sys.executable, "-c", "import sys; sys.stdout.buffer.write(bytes([197, 190, 195, 161, 240, 159, 148, 145]))"], **kwargs)
    monkeypatch.setattr(credentials.subprocess, "run", child)
    assert credentials._run_bw(["get", "password", "fake-item"], {}, 10) == "\u017e\u00e1\U0001f511"


def test_password_acceptance_is_reported_after_lock(vault, capsys, monkeypatch):
    run = credentials._run_bw

    def checked_run(arguments, env, timeout):
        expected = "Checking password..." if arguments[0] == "unlock" else ""
        assert capsys.readouterr().out == expected
        return run(arguments, env, timeout)

    monkeypatch.setattr(credentials, "_run_bw", checked_run)
    bitwarden_password("item-id")
    assert capsys.readouterr().out == "\rPassword accepted - vault locked.\n"


@pytest.mark.parametrize("operation", ["unlock", "get", "lock"])
def test_failed_vault_operation_does_not_report_acceptance(vault, capsys, operation):
    _, state = vault
    state["fail"] = operation
    with pytest.raises(RuntimeError):
        bitwarden_password("item-id")
    assert capsys.readouterr().out == "Checking password..."



def test_notebook_password_status_replaces_pending_message(monkeypatch):
    from unittest.mock import MagicMock
    import IPython
    import IPython.display

    handle = MagicMock()
    display = MagicMock(return_value=handle)
    monkeypatch.setattr(IPython, "get_ipython", lambda: MagicMock(kernel=True))
    monkeypatch.setattr(IPython.display, "display", display)
    accepted = credentials._password_status()
    display.assert_called_once_with("Checking password...", display_id=True)
    handle.update.assert_not_called()
    accepted()
    handle.update.assert_called_once_with("Password accepted - vault locked.")



@pytest.mark.parametrize("stage", ["initial", "accepted"])
@pytest.mark.parametrize("failure", [None, "lock"])
def test_display_failure_never_changes_vault_result(vault, monkeypatch, stage, failure):
    from unittest.mock import MagicMock
    import IPython
    import IPython.display

    calls, state = vault
    state["fail"] = failure
    handle = MagicMock()
    handle.update.side_effect = OSError("private display failure")
    display = MagicMock(return_value=handle)
    if stage == "initial":
        display.side_effect = OSError("private display failure")
    monkeypatch.setattr(IPython, "get_ipython", lambda: MagicMock(kernel=True))
    monkeypatch.setattr(IPython.display, "display", display)
    if failure:
        with pytest.raises(credentials.BitwardenError) as caught:
            bitwarden_password("item-id")
        assert caught.value.lock_failed
        assert "private" not in str(caught.value)
        handle.update.assert_not_called()
    else:
        assert bitwarden_password("item-id") == "  literal-password  "
    assert [call[0][3] for call in calls] == ["unlock", "get", "lock"]


def test_broken_terminal_does_not_abort_vault_cleanup(vault, monkeypatch):
    import IPython
    monkeypatch.setattr(IPython, "get_ipython", lambda: None)
    def broken_print(*args, **kwargs):
        raise BrokenPipeError("private output error")
    monkeypatch.setattr("builtins.print", broken_print)
    assert bitwarden_password("item-id") == "  literal-password  "
    calls, _ = vault
    assert calls[-1][0][3] == "lock"
