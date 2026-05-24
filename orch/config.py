"""Paths, env-var contracts, and constants."""

from __future__ import annotations

import os
from pathlib import Path

HOME = Path(os.path.expanduser("~"))

ORCH_DIR = HOME / ".orch"
DB_PATH = ORCH_DIR / "orch.db"
SOCKET_PATH = ORCH_DIR / "orchd.sock"

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
SCRIPTED_PING_SETUP_DIRECTIVE = """\
Set up a SCRIPTED PING - an autonomous watcher script. Do NOT poll this condition
yourself with your own turns; the whole point is to offload the watching to a
script so it costs no tokens until something actually happens.

  scripted-ping id : {spid}
  condition        : {condition}
  action on fire   : {action}
  watchdog timeout : {timeout_s}s

Build it like this:
  1. Write a standalone script (bash or python) that loops and checks the
     condition. Put it somewhere durable, e.g. /root/.orch-scripts/{spid}.sh.
  2. Inside the loop, AT LEAST every {half_timeout}s, run:
         owatch alive {spid}
     This is the heartbeat that proves the script is still running.
  3. When the condition becomes TRUE, run:
         owatch fire {spid} "<short context about what you detected>"
     That wakes you to compose and send the real notify. Decide whether the
     script should then keep running (recurring) or exit (one-shot).
  4. TEST the script before activating it: run the condition-check logic by
     hand to confirm it correctly detects true AND false, and run
     `owatch alive {spid}` once to confirm the heartbeat reaches the daemon.
     Do NOT call `owatch fire` during testing - fire is real and will notify
     the user. Trust that the fire line is correct by reading it.
  5. Launch it in the background so it survives after you exit this turn - e.g.
         nohup bash /root/.orch-scripts/{spid}.sh >/root/.orch-scripts/{spid}.log 2>&1 &
  6. Only once it is tested and running, register it:
         owatch ready {spid} /root/.orch-scripts/{spid}.sh

If the daemon stops seeing alive heartbeats for {timeout_s}s it will ask you to
repair the script. `owatch` is on PATH and already knows this mission.
"""

SCRIPTED_PING_FIRE_DIRECTIVE = """\
Your scripted ping fired.
  condition : {condition}
  action    : {action}
  context from the script : {context}

Do the action and report to the user via the `notify` tool. If this watcher is
one-shot, you may also delete it; if recurring, leave it running.
"""

SCRIPTED_PING_REPAIR_DIRECTIVE = """\
Your scripted-ping watcher has gone SILENT - no `owatch alive {spid}` heartbeat
for over {timeout_s}s, so it likely crashed or was killed.
  condition : {condition}
  script    : {script_path}

Inspect the script and its log, fix or recreate it, re-test it, relaunch it in
the background, and call `owatch ready {spid} <path>` when it's running again.
"""

CANCEL_GOODBYE_DIRECTIVE = (
    "The host has requested that this mission be cancelled. Before exiting, send a "
    "brief goodbye to the user via the `notify` tool. Explain in 1-2 sentences where "
    "you got to and any partial results or state that may matter. Then stop - the "
    "mission will be torn down immediately after you exit."
)

TICK_INTERVAL_S = 1.0
MAX_RESTARTS = 5

ENV_MASTER_PASSPHRASE = "ORCH_MASTER_PASSPHRASE"
ENV_TELEGRAM_TOKEN = "ORCH_TELEGRAM_BOT_TOKEN"
ENV_DEFAULT_CHAT_ID = "ORCH_DEFAULT_CHAT_ID"
ENV_MISSION_ID = "ORCH_MISSION_ID"
ENV_SOCKET = "ORCH_SOCKET"
# Inbound command bot (separate from the notification bot above).
ENV_HOST_BOT_TOKEN = "ORCH_HOST_BOT_TOKEN"
ENV_HOST_ALLOWED_CHATS = "ORCH_HOST_ALLOWED_CHAT_IDS"


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
