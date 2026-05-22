"""Paths, env-var contracts, and constants."""

from __future__ import annotations

import os
from pathlib import Path

HOME = Path(os.path.expanduser("~"))

ORCH_DIR = HOME / ".orch"
DB_PATH = ORCH_DIR / "orch.db"
SOCKET_PATH = ORCH_DIR / "orchd.sock"
PID_PATH = ORCH_DIR / "orchd.pid"
LOG_PATH = ORCH_DIR / "orchd.log"

PASSWORD_STORE_DIR = HOME / ".password-store"

TMUX_PREFIX = "mission-"
WORKER_TMP_PREFIX = "/tmp/orch-"

HEARTBEAT_DEFAULT_S = 86_400
HEARTBEAT_MAX_S = 86_400
HEARTBEAT_DIRECTIVE = (
    "Send a brief status update to the user via Telegram. Use the `notify` tool "
    "from the orch MCP server. Include what you're currently working on and any "
    "relevant context - the user has not heard from you in a while."
)
COMPLETION_DIRECTIVE = (
    "All your scheduled work for this mission is finished. Compose a short summary "
    "of what you accomplished and send it to the user via the `notify` tool from "
    "the orch MCP server. After sending, the mission will be torn down."
)
RESUME_NOTIFY_PREFIX = (
    "You were interrupted and have just resumed. Before continuing, briefly notify "
    "the user via the `notify` tool that you were interrupted and what you're picking "
    "back up. Then continue with: "
)

TICK_INTERVAL_S = 1.0

ENV_MASTER_PASSPHRASE = "ORCH_MASTER_PASSPHRASE"
ENV_TELEGRAM_TOKEN = "ORCH_TELEGRAM_BOT_TOKEN"
ENV_DEFAULT_CHAT_ID = "ORCH_DEFAULT_CHAT_ID"
ENV_MISSION_ID = "ORCH_MISSION_ID"
ENV_SOCKET = "ORCH_SOCKET"


def default_chat_id() -> str | None:
    v = os.environ.get(ENV_DEFAULT_CHAT_ID)
    return v.strip() if v and v.strip() else None


def socket_path() -> Path:
    """Allow override via ORCH_SOCKET for client connections (worker side)."""
    override = os.environ.get(ENV_SOCKET)
    if override:
        return Path(override)
    return SOCKET_PATH


def tmux_session_name(mission_id: str) -> str:
    return f"{TMUX_PREFIX}{mission_id}"


def worker_tmpdir(mission_id: str) -> Path:
    return Path(f"{WORKER_TMP_PREFIX}{mission_id}")


def ensure_dirs() -> None:
    ORCH_DIR.mkdir(parents=True, exist_ok=True)
