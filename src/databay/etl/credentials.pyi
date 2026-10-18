from typing import Literal


class BitwardenError(RuntimeError):
    """Sanitized operation stage and final vault lock status."""
    stage: Literal["prompt", "unlock", "get", "lock"]
    lock_failed: bool
    reason: Literal["command", "hidden_input", "empty_session", "empty_password"]

    def __init__(
        self, stage: Literal["prompt", "unlock", "get", "lock"], *,
        lock_failed: bool = False,
        reason: Literal["command", "hidden_input", "empty_session", "empty_password"] = "command",
    ) -> None: ...


def bitwarden_password(item_id: str, *, timeout: float = 10) -> str:
    """Prompt for master password, unlock, retrieve the login password, and lock."""
    ...


def bitwarden_connection(item_id: str, *, timeout: float = 10) -> dict[str, str]:
    """Prompt, read login credentials and custom host/port/database fields, and lock."""
    ...
