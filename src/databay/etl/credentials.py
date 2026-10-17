"""Optional password retrieval through infrastructure-provided Bitwarden CLI."""

import math
import subprocess


def bitwarden_password(item_id: str, *, timeout: float = 10) -> str:
    """Read a login item's password using an already unlocked Bitwarden CLI.

    Infrastructure owns bw installation, login, vault unlock and BW_SESSION.
    This function never logs in, unlocks, syncs or writes a credentials file.
    Pass it through a zero-argument password_provider callback to Octopus.
    CLI failures raise a generic error without stdout/stderr or session values.
    """
    if not isinstance(item_id, str) or not item_id.strip() or item_id.startswith("-"):
        raise ValueError("A Bitwarden item ID is required")
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("Bitwarden timeout must be finite and positive")
    try:
        result = subprocess.run(
            ["bw", "--nointeraction", "--raw", "get", "password", item_id],
            capture_output=True,
            text=True,
            timeout=timeout,
            stdin=subprocess.DEVNULL,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        raise RuntimeError("Bitwarden password retrieval failed; check CLI availability and vault session") from None
    if result.returncode != 0:
        raise RuntimeError("Bitwarden password retrieval failed; check vault session and item access")
    password = result.stdout.rstrip("\r\n")
    if not password.strip():
        raise RuntimeError("Bitwarden returned an empty password")
    return password
