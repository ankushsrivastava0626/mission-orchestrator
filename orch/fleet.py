"""orch-fleet: run many isolated orch instances as Docker containers.

Each instance is one container: its own orchd, its own tmux, its own fleet of
workers, its own Telegram bot. State and credentials live on a per-instance
named volume mounted at /root, so a container is disposable and the volume is
the memory. Kill a container and start a new one on the same volume and orchd's
crash recovery resumes every in-flight mission.

Shared config (agent keys, default agent, vault passphrase) is inherited by
every instance from a base env file, so you never configure a CLI per instance.
Per-instance settings are just the bot token and chat id. Agent CLI logins are
seeded once into the volume by copying the host's existing credentials.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

HOME = Path(os.path.expanduser("~"))
FLEET_DIR = HOME / ".orch-fleet"
BASE_ENV = FLEET_DIR / "base.env"
INSTANCES_DIR = FLEET_DIR / "instances"
IMAGE = os.environ.get("ORCH_IMAGE", "orch:latest")

# Keys that are per-instance (never shared through the base config).
_PER_INSTANCE_KEYS = {
    "ORCH_HOST_BOT_TOKEN", "ORCH_HOST_ALLOWED_CHAT_IDS", "ORCH_DEFAULT_CHAT_ID",
    "ORCH_TOPICS_CHAT_ID", "ORCH_TELEGRAM_BOT_TOKEN", "ORCH_INSTANCE_NAME",
    "HOME", "ORCH_ENV_FILE",
}

# Host credential files seeded into a new instance's volume (relative to HOME).
# Auth only, never the host's mission data, so each instance starts clean but
# already logged in to every agent CLI.
_CRED_PATHS = [
    ".claude/.credentials.json",
    ".claude.json",
    ".codex/auth.json",
    ".gemini/oauth_creds.json",
    ".gemini/google_accounts.json",
    ".gemini/settings.json",
    ".gemini/installation_id",
    ".gemini/antigravity-cli/oauth_creds.json",
    ".gemini/antigravity-cli/antigravity-oauth-token",
    ".config/gh/hosts.yml",
]


class FleetError(RuntimeError):
    pass


def _docker(*args: str, check: bool = True, capture: bool = True) -> str:
    res = subprocess.run(["docker", *args], capture_output=capture, text=True)
    if check and res.returncode != 0:
        raise FleetError((res.stderr or res.stdout or "").strip()
                         or f"docker {' '.join(args)} failed")
    return (res.stdout or "").strip()


def _require_docker() -> None:
    if shutil.which("docker") is None:
        raise FleetError("docker is not installed. Install it first (https://get.docker.com).")
    _docker("info", capture=True)


def _valid_name(name: str) -> str:
    if not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,40}", name or ""):
        raise FleetError("instance name must be lowercase letters, digits, '-' or '_'")
    return name


def _read_env_file(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if path.is_file():
        for line in path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                out[k.strip()] = v.strip()
    return out


def _write_env_file(path: Path, env: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(f"{k}={v}\n" for k, v in env.items()))
    os.chmod(path, 0o600)


def ensure_base_env() -> dict[str, str]:
    """Create the shared base config on first use, seeded from the host's own
    orchd.env (keys, default agent, passphrase) with per-instance keys stripped."""
    if BASE_ENV.is_file():
        return _read_env_file(BASE_ENV)
    seed: dict[str, str] = {}
    for cand in ("/etc/orchd.env", str(HOME / ".orch" / "orchd.env")):
        if os.path.isfile(cand):
            seed = _read_env_file(Path(cand))
            break
    base = {k: v for k, v in seed.items() if k not in _PER_INSTANCE_KEYS}
    base.setdefault("ORCH_AGENT", "claude")
    if not base.get("ORCH_MASTER_PASSPHRASE"):
        import secrets
        base["ORCH_MASTER_PASSPHRASE"] = secrets.token_urlsafe(24)
    _write_env_file(BASE_ENV, base)
    print(f"created shared base config at {BASE_ENV} (edit it to set shared keys)")
    return base


# ---------- lifecycle ----------

def _cname(name: str) -> str:
    return f"orch-{name}"


def _vol(name: str) -> str:
    return f"orch_{name}"


def _exists(name: str) -> bool:
    out = _docker("ps", "-aq", "-f", f"name=^{_cname(name)}$", check=False)
    return bool(out)


def create(name: str, *, bot_token: str, chat_id: str | None = None,
           agent: str | None = None, topics_chat: str | None = None,
           model: str | None = None, image: str = IMAGE,
           no_seed: bool = False) -> None:
    _require_docker()
    _valid_name(name)
    if _exists(name):
        raise FleetError(f"instance '{name}' already exists (use `orch-fleet start {name}`)")
    if not bot_token:
        raise FleetError("a Telegram bot token is required (--bot-token)")

    env = ensure_base_env().copy()
    if agent:
        env["ORCH_AGENT"] = agent
    if model:
        env["ORCH_OPENCODE_MODEL"] = model
    env["ORCH_HOST_BOT_TOKEN"] = bot_token
    if chat_id:
        env["ORCH_HOST_ALLOWED_CHAT_IDS"] = chat_id
        env["ORCH_DEFAULT_CHAT_ID"] = chat_id
    if topics_chat:
        env["ORCH_TOPICS_CHAT_ID"] = topics_chat
    env["ORCH_INSTANCE_NAME"] = name
    _write_env_file(INSTANCES_DIR / f"{name}.env", env)

    _docker("volume", "create", _vol(name))

    with tempfile.NamedTemporaryFile("w", suffix=".env", delete=False) as tf:
        tf.write("".join(f"{k}={v}\n" for k, v in env.items()))
        env_file = tf.name
    try:
        _docker(
            "create", "--name", _cname(name),
            "--restart", "unless-stopped",
            # tini as PID 1: reaps the tmux/worker child processes and makes an
            # orchd crash exit cleanly so the restart policy brings it back.
            "--init",
            "--label", "orch.fleet=1", "--label", f"orch.instance={name}",
            "-v", f"{_vol(name)}:/root",
            "--env-file", env_file,
            image,
        )
    finally:
        os.unlink(env_file)

    if not no_seed:
        _seed_credentials(name)
    _docker("start", _cname(name))
    print(f"instance '{name}' created and started (image {image}, volume {_vol(name)}).")
    print("  logs:   orch-fleet logs " + name)
    print("  shell:  orch-fleet exec " + name + " bash")


def _seed_credentials(name: str) -> None:
    """Copy host agent logins into the new instance's volume so every CLI is
    already authenticated. Auth files only, never the host's mission data."""
    staged = 0
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "root"
        for rel in _CRED_PATHS:
            src = HOME / rel
            if src.is_file():
                dst = root / rel
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src, dst)
                staged += 1
        if staged:
            # Copy the whole staged tree into /root; docker cp creates parents.
            _docker("cp", f"{root}/.", f"{_cname(name)}:/root/")
    print(f"  seeded {staged} host credential file(s) into the instance")


def start(name: str) -> None:
    _require_docker(); _valid_name(name)
    _docker("start", _cname(name)); print(f"started {name}")


def stop(name: str) -> None:
    _require_docker(); _valid_name(name)
    _docker("stop", _cname(name)); print(f"stopped {name}")


def restart(name: str) -> None:
    _require_docker(); _valid_name(name)
    _docker("restart", _cname(name)); print(f"restarted {name}")


def remove(name: str, *, purge: bool = False) -> None:
    _require_docker(); _valid_name(name)
    _docker("rm", "-f", _cname(name), check=False)
    if purge:
        _docker("volume", "rm", _vol(name), check=False)
        (INSTANCES_DIR / f"{name}.env").unlink(missing_ok=True)
        print(f"removed {name} and PURGED its volume (all data gone)")
    else:
        print(f"removed the {name} container (volume {_vol(name)} kept; recreate to resume)")


def logs(name: str, *, follow: bool = False, tail: int = 200) -> None:
    _require_docker(); _valid_name(name)
    args = ["logs", "--tail", str(tail)]
    if follow:
        args.append("-f")
    args.append(_cname(name))
    subprocess.run(["docker", *args])


def exec_in(name: str, cmd: list[str]) -> int:
    _require_docker(); _valid_name(name)
    if not cmd:
        cmd = ["bash"]
    return subprocess.run(["docker", "exec", "-it", _cname(name), *cmd]).returncode


def ls() -> list[dict]:
    _require_docker()
    fmt = "{{.Names}}\t{{.State}}\t{{.Status}}"
    out = _docker("ps", "-a", "--filter", "label=orch.fleet=1", "--format", fmt, check=False)
    rows = []
    for line in out.splitlines():
        parts = line.split("\t")
        if len(parts) < 3:
            continue
        cname, state, status = parts[0], parts[1], parts[2]
        name = cname[len("orch-"):] if cname.startswith("orch-") else cname
        missions = ""
        if state == "running":
            try:
                missions = _docker("exec", cname, "orchctl", "status", check=False) or ""
            except Exception:
                missions = ""
        rows.append({"name": name, "state": state, "status": status,
                     "missions": missions.strip()})
    return rows
