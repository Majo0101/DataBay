"""Interactive password retrieval through infrastructure-provided Bitwarden CLI."""

import getpass
import json
import math
import os
import subprocess
import warnings
from typing import Literal


class BitwardenError(RuntimeError):
    """Sanitized vault failure; stage identifies the original failed operation.

    lock_failed means the final lock command failed or was interrupted. It does
    not prove the vault is unlocked; its lock state is unknown. No CLI output or
    original exception is included in the message.
    """

    def __init__(
        self, stage: Literal["prompt", "unlock", "get", "lock"], *,
        lock_failed: bool = False,
        reason: Literal["command", "hidden_input", "empty_session", "empty_password"] = "command",
    ):
        if stage not in ("prompt", "unlock", "get", "lock"):
            raise ValueError("Invalid Bitwarden failure stage")
        messages = {
            "command": f"Bitwarden command failed during {stage}; check CLI availability and vault access",
            "hidden_input": "Bitwarden requires hidden password input; use a supported terminal or notebook",
            "empty_session": "Bitwarden returned an empty session",
            "empty_password": "Bitwarden returned an empty password",
        }
        if reason not in messages:
            raise ValueError("Invalid Bitwarden failure reason")
        self.stage = stage
        self.lock_failed = bool(lock_failed)
        self.reason = reason
        message = messages[reason]
        if self.lock_failed:
            message += "; vault lock failed or was interrupted; run bw lock manually"
        super().__init__(message)


def _run_bw(arguments, env, timeout):
    try:
        result = subprocess.run(
            ["bw", "--nointeraction", "--raw", *arguments],
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=timeout,
            stdin=subprocess.DEVNULL,
            env=env,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired, UnicodeError):
        raise BitwardenError(arguments[0]) from None
    if result.returncode != 0:
        raise BitwardenError(arguments[0])
    return result.stdout


def _bitwarden_value(item_id: str, kind: str, *, timeout: float = 10) -> str:
    """Prompt, unlock, read one login password, and lock the vault before returning.

    Infrastructure provides bw and an already logged-in account. The master
    password is requested with getpass; unavailable hidden input is an error.
    CLI output is decoded as UTF-8. Secrets are passed only in child-process
    environments, never command arguments or the parent OS environment. Lock is
    attempted even if unlock or retrieval fails. Lock failure prevents returning
    the password. Local references are dropped; Python cannot guarantee memory
    erasure, and the returned database password remains available to its caller.
    """
    if not isinstance(item_id, str) or not item_id.strip() or item_id.startswith("-"):
        raise ValueError("A Bitwarden item ID is required")
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("Bitwarden timeout must be finite and positive")

    child_env = dict(os.environ)
    child_env.pop("BW_SESSION", None)
    master_password = None
    session = None
    password = None
    stage = "prompt"
    original = None
    try:
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("error", getpass.GetPassWarning)
                master_password = getpass.getpass("Bitwarden master password: ")
        except getpass.GetPassWarning:
            raise BitwardenError("prompt", reason="hidden_input") from None
        child_env["_DATABAY_BW_MASTER"] = master_password
        stage = "unlock"
        try:
            session = _run_bw(
                ["unlock", "--passwordenv", "_DATABAY_BW_MASTER"], child_env, timeout
            ).strip()
        finally:
            child_env.pop("_DATABAY_BW_MASTER", None)
            master_password = None
        if not session:
            raise BitwardenError("unlock", reason="empty_session")
        child_env["BW_SESSION"] = session
        stage = "get"
        password = _run_bw(["get", kind, item_id], child_env, timeout).rstrip("\r\n")
        if not password.strip():
            raise BitwardenError("get", reason="empty_password")
        return password
    except BitwardenError as error:
        original = error
        raise
    except Exception:
        original = BitwardenError(stage)
        raise original from None
    except BaseException as error:
        original = error
        raise
    finally:
        try:
            try:
                _run_bw(["lock"], child_env, timeout)
            except BaseException as cleanup_error:
                if isinstance(original, KeyboardInterrupt) or isinstance(cleanup_error, KeyboardInterrupt):
                    cancelled = original if isinstance(original, KeyboardInterrupt) else cleanup_error
                    cancelled.stage = getattr(original, "stage", stage if original is not None else "lock")
                    cancelled.lock_failed = True
                    cancelled.args = (
                        f"Bitwarden operation interrupted (stage: {cancelled.stage}); "
                        "vault lock failed or was interrupted; run bw lock manually",
                    )
                    raise cancelled from None
                if original is not None and not isinstance(original, Exception):
                    # Preserve process-exit semantics as well as cancellation.
                    original.lock_failed = True
                    if hasattr(original, "add_note"):
                        original.add_note("Bitwarden vault lock failed; run bw lock manually")
                    raise original from None
                if not isinstance(cleanup_error, Exception):
                    raise
                if isinstance(original, BitwardenError):
                    raise BitwardenError(original.stage, reason=original.reason, lock_failed=True) from None
                raise BitwardenError("lock", lock_failed=True) from None
        finally:
            child_env.clear()
            master_password = None
            session = None
            password = None
            original = None


def bitwarden_password(item_id: str, *, timeout: float = 10) -> str:
    """Prompt for master password, retrieve one password, and lock before returning."""
    return _bitwarden_value(item_id, "password", timeout=timeout)


def bitwarden_connection(item_id: str, *, timeout: float = 10) -> dict[str, str]:
    """Read login username/password and custom host, port, database fields.

    Custom field names are case-insensitive. HOST, DATABASE, USER and PASSWORD
    must be nonempty; PORT is optional. The vault is locked before returning
    settings. No process environment or credentials file is modified. Engine
    is selected explicitly on Octopus.
    """
    raw = _bitwarden_value(item_id, "item", timeout=timeout)
    item = None
    fields = None
    login = None
    try:
        try:
            item = json.loads(raw)
            fields = {}
            for field in item.get("fields") or []:
                key = field["name"].lower()
                if key in {"host", "port", "database"}:
                    if key in fields:
                        raise ValueError()
                    fields[key] = field.get("value")
            login = item["login"]
            settings = {
                "HOST": fields.get("host"),
                "PORT": fields.get("port"),
                "DATABASE": fields.get("database"),
                "USER": login.get("username"),
                "PASSWORD": login.get("password"),
            }
            required = ("HOST", "DATABASE", "USER", "PASSWORD")
            for key in required:
                if not isinstance(settings[key], str) or not settings[key].strip():
                    raise ValueError()
            port = settings["PORT"]
            if port is not None and (not isinstance(port, str) or not port.isdecimal() or not 1 <= int(port) <= 65535):
                raise ValueError()
            settings = {key: value for key, value in settings.items() if value is not None}
        except (ValueError, TypeError, KeyError, AttributeError):
            raise ValueError(
                "Invalid Bitwarden connection: require login username/password, "
                "custom host/database fields, an optional valid port and no duplicate fields"
            ) from None
        return settings
    finally:
        raw = None
        item = None
        fields = None
        login = None
