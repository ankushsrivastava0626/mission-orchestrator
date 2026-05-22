"""GPG + pass vault initialization and per-mission secret/cookie access.

All operations shell out to `gpg` and `pass`. The daemon process is expected to
have `ORCH_MASTER_PASSPHRASE` set in its environment, which gpg-agent uses (via
loopback pinentry) to decrypt the vault key.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

from . import config

GPG_KEY_NAME = "orch-vault"
GPG_KEY_EMAIL = "orch@localhost"


class VaultError(RuntimeError):
    pass


def _passphrase() -> str:
    pw = os.environ.get(config.ENV_MASTER_PASSPHRASE)
    if not pw:
        raise VaultError(
            f"{config.ENV_MASTER_PASSPHRASE} not set; daemon cannot access vault"
        )
    return pw


def _gpg_env() -> dict[str, str]:
    env = os.environ.copy()
    # Allow loopback pinentry across child gpg invocations.
    env.setdefault("GPG_TTY", "")
    return env


def _run(
    cmd: list[str], *, input_data: str | bytes | None = None, check: bool = True
) -> subprocess.CompletedProcess:
    if isinstance(input_data, str):
        input_bytes: bytes | None = input_data.encode("utf-8")
    else:
        input_bytes = input_data
    res = subprocess.run(
        cmd,
        input=input_bytes,
        capture_output=True,
        env=_gpg_env(),
    )
    if check and res.returncode != 0:
        raise VaultError(
            f"command failed: {' '.join(cmd)}\nstderr: {res.stderr.decode('utf-8', 'replace')}"
        )
    return res


def _existing_key_fingerprint() -> str | None:
    res = _run(
        ["gpg", "--list-secret-keys", "--with-colons", GPG_KEY_EMAIL], check=False
    )
    if res.returncode != 0:
        return None
    for line in res.stdout.decode("utf-8", "replace").splitlines():
        parts = line.split(":")
        if parts and parts[0] == "fpr":
            return parts[9]
    return None


def _generate_key() -> str:
    passphrase = _passphrase()
    batch = (
        "%no-protection\n"
        "Key-Type: RSA\n"
        "Key-Length: 2048\n"
        "Subkey-Type: RSA\n"
        "Subkey-Length: 2048\n"
        f"Name-Real: {GPG_KEY_NAME}\n"
        f"Name-Email: {GPG_KEY_EMAIL}\n"
        "Expire-Date: 0\n"
        f"Passphrase: {passphrase}\n"
        "%commit\n"
    )
    _run(
        [
            "gpg",
            "--batch",
            "--pinentry-mode",
            "loopback",
            "--passphrase",
            passphrase,
            "--gen-key",
        ],
        input_data=batch,
    )
    fp = _existing_key_fingerprint()
    if not fp:
        raise VaultError("generated key but could not read its fingerprint")
    return fp


def _pass_init(fingerprint: str) -> None:
    _run(["pass", "init", fingerprint])


def _preset_passphrase(fingerprint: str) -> None:
    """Warm up gpg-agent so subsequent `pass show` calls don't prompt.

    Best-effort: tries gpg-preset-passphrase; if unavailable, performs a no-op
    decrypt to seed the agent cache.
    """
    passphrase = _passphrase()
    # Try gpg-preset-passphrase first.
    libexec = subprocess.run(
        ["gpgconf", "--list-dirs", "libexecdir"], capture_output=True
    )
    if libexec.returncode == 0:
        libexec_dir = libexec.stdout.decode().strip()
        preset = Path(libexec_dir) / "gpg-preset-passphrase"
        if preset.exists():
            keygrip = _keygrip_for(fingerprint)
            if keygrip:
                subprocess.run(
                    [str(preset), "--preset", keygrip],
                    input=passphrase.encode("utf-8"),
                    capture_output=True,
                )
                return
    # Fallback: trigger a decrypt to cache the passphrase.
    enc = _run(
        [
            "gpg",
            "--batch",
            "--pinentry-mode",
            "loopback",
            "--passphrase",
            passphrase,
            "-r",
            fingerprint,
            "--encrypt",
            "--armor",
        ],
        input_data=b"warmup",
    )
    _run(
        [
            "gpg",
            "--batch",
            "--pinentry-mode",
            "loopback",
            "--passphrase",
            passphrase,
            "--decrypt",
        ],
        input_data=enc.stdout,
    )


def _keygrip_for(fingerprint: str) -> str | None:
    res = _run(
        ["gpg", "--list-secret-keys", "--with-keygrip", "--with-colons", fingerprint],
        check=False,
    )
    if res.returncode != 0:
        return None
    for line in res.stdout.decode("utf-8", "replace").splitlines():
        parts = line.split(":")
        if parts and parts[0] == "grp":
            return parts[9]
    return None


def init_vault() -> str:
    """Idempotent. Returns fingerprint of the vault key."""
    gpg_id = config.PASSWORD_STORE_DIR / ".gpg-id"
    if gpg_id.exists():
        fp = gpg_id.read_text().strip()
        # Warm agent cache so later show/insert calls don't prompt.
        try:
            _preset_passphrase(fp)
        except VaultError:
            pass
        return fp
    fp = _existing_key_fingerprint()
    if not fp:
        fp = _generate_key()
    _pass_init(fp)
    try:
        _preset_passphrase(fp)
    except VaultError:
        pass
    return fp


# ---------- per-mission secret access ----------


def _ns(mission_id: str, kind: str, name: str) -> str:
    return f"mission-{mission_id}/{kind}/{name}"


def put_secret(mission_id: str, name: str, value: str) -> None:
    path = _ns(mission_id, "secrets", name)
    _run(["pass", "insert", "-m", "-f", path], input_data=value)


def get_secret(mission_id: str, name: str) -> str:
    path = _ns(mission_id, "secrets", name)
    res = _run(["pass", "show", path])
    out = res.stdout.decode("utf-8")
    # `pass insert -m` preserves trailing newline; strip one.
    return out.rstrip("\n")


def delete_secret(mission_id: str, name: str) -> None:
    path = _ns(mission_id, "secrets", name)
    _run(["pass", "rm", "-f", path])


def put_cookies(mission_id: str, name: str, content: str) -> None:
    path = _ns(mission_id, "cookies", name)
    _run(["pass", "insert", "-m", "-f", path], input_data=content)


def get_cookies(mission_id: str, name: str) -> str:
    path = _ns(mission_id, "cookies", name)
    res = _run(["pass", "show", path])
    return res.stdout.decode("utf-8").rstrip("\n")


def delete_cookies(mission_id: str, name: str) -> None:
    path = _ns(mission_id, "cookies", name)
    _run(["pass", "rm", "-f", path])


def _list_namespace(mission_id: str, kind: str) -> list[str]:
    base = config.PASSWORD_STORE_DIR / f"mission-{mission_id}" / kind
    if not base.exists():
        return []
    out = []
    for entry in sorted(base.iterdir()):
        if entry.is_file() and entry.suffix == ".gpg":
            out.append(entry.stem)
    return out


def list_secrets(mission_id: str) -> list[str]:
    return _list_namespace(mission_id, "secrets")


def list_cookies(mission_id: str) -> list[str]:
    return _list_namespace(mission_id, "cookies")


def purge_mission(mission_id: str) -> None:
    """Best-effort: remove the mission's namespace from the password store."""
    base = config.PASSWORD_STORE_DIR / f"mission-{mission_id}"
    if base.exists():
        _run(["pass", "rm", "-r", "-f", f"mission-{mission_id}"], check=False)
