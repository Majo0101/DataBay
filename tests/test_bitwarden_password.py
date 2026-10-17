"""Bitwarden adapter tests never access a real vault."""

import subprocess
from types import SimpleNamespace

import pytest

from databay.etl import bitwarden_password
from databay.etl import credentials


def test_cli_is_noninteractive_and_preserves_password_spaces(monkeypatch):
    captured = {}
    def run(command, **kwargs):
        captured.update(command=command, options=kwargs)
        return SimpleNamespace(returncode=0, stdout="  literal-password  \n", stderr="")
    monkeypatch.setattr(credentials.subprocess, "run", run)
    assert bitwarden_password("item-id", timeout=3) == "  literal-password  "
    assert captured["command"] == ["bw", "--nointeraction", "--raw", "get", "password", "item-id"]
    assert captured["options"]["stdin"] == subprocess.DEVNULL
    assert captured["options"]["timeout"] == 3
    assert "shell" not in captured["options"]


@pytest.mark.parametrize("error", [
    FileNotFoundError("sensitive-output"),
    subprocess.TimeoutExpired("bw", 1, output="sensitive-output"),
])
def test_cli_errors_are_redacted(monkeypatch, error):
    def run(*args, **kwargs):
        raise error
    monkeypatch.setattr(credentials.subprocess, "run", run)
    with pytest.raises(RuntimeError, match="Bitwarden password retrieval failed") as caught:
        bitwarden_password("item-id")
    assert "sensitive-output" not in str(caught.value)
    assert caught.value.__suppress_context__


def test_nonzero_exit_does_not_return_or_expose_output(monkeypatch):
    monkeypatch.setattr(credentials.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(
        returncode=1, stdout="sensitive-stdout", stderr="sensitive-stderr"))
    with pytest.raises(RuntimeError) as caught:
        bitwarden_password("item-id")
    assert "sensitive" not in str(caught.value)


@pytest.mark.parametrize("output", ["", "\n", "   \n"])
def test_empty_password_is_rejected(monkeypatch, output):
    monkeypatch.setattr(credentials.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(
        returncode=0, stdout=output))
    with pytest.raises(RuntimeError, match="empty password"):
        bitwarden_password("item-id")


@pytest.mark.parametrize("item", [None, "", "  ", "--help"])
def test_invalid_item_is_rejected_without_cli(monkeypatch, item):
    monkeypatch.setattr(credentials.subprocess, "run", lambda *args, **kwargs: pytest.fail("Must not invoke bw"))
    with pytest.raises(ValueError, match="item ID"):
        bitwarden_password(item)


@pytest.mark.parametrize("timeout", [0, -1, float("inf"), float("nan")])
def test_invalid_timeout_is_rejected(timeout):
    with pytest.raises(ValueError, match="finite and positive"):
        bitwarden_password("item-id", timeout=timeout)
