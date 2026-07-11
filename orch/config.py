"""Paths, env-var contracts, and constants."""

from __future__ import annotations

import os
from pathlib import Path

HOME = Path(os.path.expanduser("~"))

ORCH_DIR = HOME / ".orch"


def _load_env_file() -> None:
    """Load KEY=VALUE lines from the first existing config file so orch works
    the same under systemd (EnvironmentFile) and plain `orchd start`.
    Real environment variables always win over file values."""
    candidates = [
        os.environ.get("ORCH_ENV_FILE"),
        str(ORCH_DIR / "orchd.env"),
        "/etc/orchd.env",
    ]
    for cand in candidates:
        if not cand or not os.path.isfile(cand):
            continue
        try:
            for line in open(cand):
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))
        except OSError:
            pass
        break


_load_env_file()

# Agent CLIs (claude, codex, agy, …) typically install into ~/.local/bin,
# which systemd's minimal PATH lacks - make sure the daemon can find them.
for _extra_bin in (str(HOME / ".local" / "bin"), str(HOME / ".opencode" / "bin")):
    if _extra_bin not in os.environ.get("PATH", "").split(":"):
        os.environ["PATH"] = os.environ.get("PATH", "") + ":" + _extra_bin


def _env_int(var: str, default: int) -> int:
    try:
        return int(os.environ.get(var, "") or default)
    except ValueError:
        return default


def _env_float(var: str, default: float) -> float:
    try:
        return float(os.environ.get(var, "") or default)
    except ValueError:
        return default




def active_env_file() -> str:
    """The env file live config changes should be written to."""
    for cand in (os.environ.get("ORCH_ENV_FILE"),
                 str(ORCH_DIR / "orchd.env"), "/etc/orchd.env"):
        if cand and os.path.isfile(cand):
            return cand
    return str(ORCH_DIR / "orchd.env")


def update_env_file(key: str, value: str) -> str:
    """Set KEY=value in the active env file (replace or append). Returns the
    file path. Keeps changes durable across daemon restarts."""
    path = active_env_file()
    lines: list[str] = []
    if os.path.isfile(path):
        lines = open(path).read().splitlines()
    hit = False
    for i, line in enumerate(lines):
        if line.split("=", 1)[0].strip().lstrip("#").strip() == key and "=" in line:
            lines[i] = f"{key}={value}"
            hit = True
            break
    if not hit:
        lines.append(f"{key}={value}")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as fh:
        fh.write("\n".join(lines) + "\n")
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass
    return path
DB_PATH = ORCH_DIR / "orch.db"
SOCKET_PATH = ORCH_DIR / "orchd.sock"

PASSWORD_STORE_DIR = HOME / ".password-store"
MAILBOX_DIR = ORCH_DIR / "mailbox"  # worker -> host file attachments

# Extra MCP servers merged into every worker's .mcp.json (e.g. Playwright).
# JSON with an "mcpServers" object; '{mission_id}' is substituted in values.
EXTRA_WORKER_MCPS_PATH = Path(
    os.environ.get("ORCH_EXTRA_WORKER_MCPS", "/etc/orch/worker_mcp.json")
)

TMUX_PREFIX = "mission-"
WORKER_TMP_PREFIX = "/tmp/orch-"

HEARTBEAT_DEFAULT_S = _env_int("ORCH_HEARTBEAT_DEFAULT_S", 86_400)
HEARTBEAT_MAX_S = _env_int("ORCH_HEARTBEAT_MAX_S", 86_400)
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

TICK_INTERVAL_S = _env_float("ORCH_TICK_INTERVAL_S", 1.0)
MAX_RESTARTS = _env_int("ORCH_MAX_RESTARTS", 5)

# Auto-compaction: when an idle worker's live context crosses this many tokens,
# the engine fires a headless /compact so future wakes stay cheap. Set to 0 to
# disable. CHECK_S throttles the (file-reading) measurement per mission;
# COOLDOWN_S prevents re-firing while a compaction is still settling.
AUTO_COMPACT_THRESHOLD = _env_int("ORCH_AUTO_COMPACT_THRESHOLD", 200_000)
AUTO_COMPACT_CHECK_S = _env_int("ORCH_AUTO_COMPACT_CHECK_S", 60)
AUTO_COMPACT_COOLDOWN_S = _env_int("ORCH_AUTO_COMPACT_COOLDOWN_S", 600)

# Agent health: consecutive dead turns before fallback/unpin, and how short a
# quiet turn must be to count as dead (longer quiet turns are neutral).
AGENT_FAIL_LIMIT = _env_int("ORCH_AGENT_FAIL_LIMIT", 2)
AGENT_FAST_FAIL_S = _env_int("ORCH_AGENT_FAST_FAIL_S", 45)


def agent_auto_fallback() -> bool:
    """Whether orch may EVER change the agent on its own (revert to last-good,
    auto-unpin a dead pinned backend). Default OFF: whatever the user set
    stays set, forever - failures are logged, never silently rerouted."""
    return (os.environ.get("ORCH_AGENT_AUTO_FALLBACK", "off").strip().lower()
            in ("1", "true", "on", "yes"))

# Cross-agent handoff document sizing.
HANDOFF_TAIL_CHARS = _env_int("ORCH_HANDOFF_TAIL_CHARS", 6000)
HANDOFF_CAP_CHARS = _env_int("ORCH_HANDOFF_CAP_CHARS", 9000)

ENV_MASTER_PASSPHRASE = "ORCH_MASTER_PASSPHRASE"
ENV_TELEGRAM_TOKEN = "ORCH_TELEGRAM_BOT_TOKEN"
ENV_DEFAULT_CHAT_ID = "ORCH_DEFAULT_CHAT_ID"
ENV_MISSION_ID = "ORCH_MISSION_ID"
ENV_SOCKET = "ORCH_SOCKET"
# Inbound command bot (separate from the notification bot above).
ENV_HOST_BOT_TOKEN = "ORCH_HOST_BOT_TOKEN"
ENV_HOST_ALLOWED_CHATS = "ORCH_HOST_ALLOWED_CHAT_IDS"
# Optional forum supergroup that hosts one Telegram Topic per mission. When set,
# worker notify messages post into the mission's topic and replies typed in a
# topic route to that mission's worker. The command bot must be an admin there
# with Manage Topics.
ENV_TOPICS_CHAT_ID = "ORCH_TOPICS_CHAT_ID"

# Rapid user messages to the same mission within this many seconds are coalesced
# into ONE directive, so the worker wakes once with all of them.
REPLY_COALESCE_S = _env_float("ORCH_REPLY_COALESCE_S", 5.0)


def default_chat_id() -> str | None:
    v = os.environ.get(ENV_DEFAULT_CHAT_ID)
    return v.strip() if v and v.strip() else None


def topics_chat_id() -> str | None:
    """Forum supergroup id for per-mission topics, or None if topics mode off."""
    v = os.environ.get(ENV_TOPICS_CHAT_ID)
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


# ---------------------------------------------------------------------------
# Settings registry - the single source of truth for `orchctl config`.
# key -> (env var, type, default, attr-on-this-module-or-None, description)
# attr set => the daemon live-applies changes by rebinding config.<attr>;
# attr None => the value is read from os.environ at call time anyway.
SETTINGS: dict[str, tuple[str, str, str, str | None, str]] = {
    # behavior tunables
    "auto_compact_threshold": ("ORCH_AUTO_COMPACT_THRESHOLD", "int", "200000",
                               "AUTO_COMPACT_THRESHOLD",
                               "tokens at which an idle worker auto-compacts (0=off)"),
    "auto_compact_check_s": ("ORCH_AUTO_COMPACT_CHECK_S", "int", "60",
                             "AUTO_COMPACT_CHECK_S",
                             "seconds between context-size measurements per mission"),
    "auto_compact_cooldown_s": ("ORCH_AUTO_COMPACT_COOLDOWN_S", "int", "600",
                                "AUTO_COMPACT_COOLDOWN_S",
                                "wait after a compaction before another may fire"),
    "reply_coalesce_s": ("ORCH_REPLY_COALESCE_S", "float", "5.0",
                         "REPLY_COALESCE_S",
                         "window merging rapid Telegram messages into one directive"),
    "heartbeat_default_s": ("ORCH_HEARTBEAT_DEFAULT_S", "int", "86400",
                            "HEARTBEAT_DEFAULT_S",
                            "heartbeat interval for new missions"),
    "heartbeat_max_s": ("ORCH_HEARTBEAT_MAX_S", "int", "86400",
                        "HEARTBEAT_MAX_S", "ceiling for heartbeat.set"),
    "max_restarts": ("ORCH_MAX_RESTARTS", "int", "5", "MAX_RESTARTS",
                     "crash-recovery attempts before a mission is failed"),
    "tick_interval_s": ("ORCH_TICK_INTERVAL_S", "float", "1.0",
                        "TICK_INTERVAL_S", "engine scheduler tick"),
    "agent_auto_fallback": ("ORCH_AGENT_AUTO_FALLBACK", "str", "off", None,
                            "on = orch may auto-revert/unpin a dead backend; "
                            "off = your choice is permanent (failures just log)"),
    "agent_fail_limit": ("ORCH_AGENT_FAIL_LIMIT", "int", "2",
                         "AGENT_FAIL_LIMIT",
                         "consecutive dead turns before fallback/unpin"),
    "agent_fast_fail_s": ("ORCH_AGENT_FAST_FAIL_S", "int", "45",
                          "AGENT_FAST_FAIL_S",
                          "quiet turns shorter than this count as dead"),
    "handoff_tail_chars": ("ORCH_HANDOFF_TAIL_CHARS", "int", "6000",
                           "HANDOFF_TAIL_CHARS",
                           "old-conversation tail included in migration handoffs"),
    "handoff_cap_chars": ("ORCH_HANDOFF_CAP_CHARS", "int", "9000",
                          "HANDOFF_CAP_CHARS", "total handoff document cap"),
    # identity / integration (env-read at call time; no live attr needed)
    "agent": ("ORCH_AGENT", "str", "claude", None,
              "global worker backend (prefer `orchctl agent set`)"),
    "api_provider": ("ORCH_API_PROVIDER", "str", "anthropic", None,
                     "api backend: anthropic | openai(-compatible)"),
    "api_key": ("ORCH_API_KEY", "secret", "", None, "api backend key"),
    "api_model": ("ORCH_API_MODEL", "str", "", None,
                  "api backend model id (e.g. openrouter model)"),
    "api_base_url": ("ORCH_API_BASE_URL", "str", "", None,
                     "api backend endpoint (e.g. https://openrouter.ai/api/v1)"),
    "custom_first_cmd": ("ORCH_CUSTOM_FIRST_CMD", "str", "", None,
                         "custom backend first-launch template"),
    "custom_resume_cmd": ("ORCH_CUSTOM_RESUME_CMD", "str", "", None,
                          "custom backend resume template"),
    "claude_bin": ("ORCH_CLAUDE_BIN", "str", "claude", None, "claude CLI binary"),
    "codex_bin": ("ORCH_CODEX_BIN", "str", "codex", None, "codex CLI binary"),
    "agy_bin": ("ORCH_AGY_BIN", "str", "agy", None, "antigravity CLI binary"),
    "gemini_bin": ("ORCH_GEMINI_BIN", "str", "gemini", None, "gemini CLI binary"),
    "gemini_resume_args": ("ORCH_GEMINI_RESUME_ARGS", "str", "--resume latest",
                           None, "gemini CLI resume flags"),
    "gemini_api_key": ("GEMINI_API_KEY", "secret", "", None,
                       "forwarded to gemini workers for headless auth"),
    "opencode_bin": ("ORCH_OPENCODE_BIN", "str", "opencode", None,
                     "opencode CLI binary"),
    "opencode_model": ("ORCH_OPENCODE_MODEL", "str", "", None,
                       "opencode model, e.g. openrouter/anthropic/claude-sonnet-4.5"),
    "openrouter_api_key": ("OPENROUTER_API_KEY", "secret", "", None,
                           "forwarded to opencode workers (any model, one key)"),
    "host_bot_token": ("ORCH_HOST_BOT_TOKEN", "secret", "", None,
                       "Telegram command bot token"),
    "allowed_chat_ids": ("ORCH_HOST_ALLOWED_CHAT_IDS", "str", "", None,
                         "comma-separated Telegram chat ids allowed to command"),
    "default_chat_id": ("ORCH_DEFAULT_CHAT_ID", "str", "", None,
                        "default chat for worker notifications"),
    "topics_chat_id": ("ORCH_TOPICS_CHAT_ID", "str", "", None,
                       "forum supergroup for per-mission topics"),
    "notify_bot_token": ("ORCH_TELEGRAM_BOT_TOKEN", "secret", "", None,
                         "legacy notification bot token (fallback)"),
    "master_passphrase": ("ORCH_MASTER_PASSPHRASE", "secret", "", None,
                          "secrets-vault master passphrase"),
    "extra_worker_mcps": ("ORCH_EXTRA_WORKER_MCPS", "str",
                          "/etc/orch/worker_mcp.json", None,
                          "JSON file of extra MCP servers for all workers"),
}
