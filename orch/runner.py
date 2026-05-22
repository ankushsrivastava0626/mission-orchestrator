"""tmux + claude process management for missions."""

from __future__ import annotations

import json
import logging
import shlex
import subprocess
from pathlib import Path

from . import config

log = logging.getLogger(__name__)

CLAUDE_BIN = "claude"


class RunnerError(RuntimeError):
    pass


def _run(cmd: list[str], *, check: bool = True) -> subprocess.CompletedProcess:
    res = subprocess.run(cmd, capture_output=True)
    if check and res.returncode != 0:
        raise RunnerError(
            f"command failed: {' '.join(cmd)}\nstderr: {res.stderr.decode('utf-8', 'replace')}"
        )
    return res


# ---------- tmux ----------


def tmux_session_exists(name: str) -> bool:
    res = _run(["tmux", "has-session", "-t", name], check=False)
    return res.returncode == 0


def tmux_create_session(mission_id: str) -> str:
    name = config.tmux_session_name(mission_id)
    if tmux_session_exists(name):
        return name
    _run(["tmux", "new-session", "-d", "-s", name])
    _run(
        [
            "tmux",
            "set-environment",
            "-t",
            name,
            config.ENV_MISSION_ID,
            mission_id,
        ]
    )
    _run(
        [
            "tmux",
            "set-environment",
            "-t",
            name,
            config.ENV_SOCKET,
            str(config.SOCKET_PATH),
        ]
    )
    return name


def tmux_kill_session(mission_id: str) -> None:
    name = config.tmux_session_name(mission_id)
    if tmux_session_exists(name):
        _run(["tmux", "kill-session", "-t", name], check=False)


def tmux_send(session: str, command_line: str) -> None:
    _run(["tmux", "send-keys", "-t", session, command_line, "Enter"])


def tmux_pane_current_command(session: str) -> str | None:
    res = _run(
        ["tmux", "list-panes", "-t", session, "-F", "#{pane_current_command}"],
        check=False,
    )
    if res.returncode != 0:
        return None
    out = res.stdout.decode("utf-8", "replace").strip().splitlines()
    return out[0] if out else None


def tmux_pane_pid(session: str) -> int | None:
    res = _run(
        ["tmux", "list-panes", "-t", session, "-F", "#{pane_pid}"], check=False
    )
    if res.returncode != 0:
        return None
    out = res.stdout.decode("utf-8", "replace").strip().splitlines()
    if not out:
        return None
    try:
        return int(out[0])
    except ValueError:
        return None


# ---------- worker MCP config ----------


def write_worker_mcp_config(mission_id: str) -> Path:
    tmp = config.worker_tmpdir(mission_id)
    tmp.mkdir(parents=True, exist_ok=True)
    (tmp / "cookies").mkdir(parents=True, exist_ok=True)
    cfg = tmp / ".mcp.json"
    payload = {
        "mcpServers": {
            "orch": {
                "command": "orch-mcp",
                "args": ["--mode", "worker", "--mission-id", mission_id],
                "env": {
                    config.ENV_MISSION_ID: mission_id,
                    config.ENV_SOCKET: str(config.SOCKET_PATH),
                },
            }
        }
    }
    cfg.write_text(json.dumps(payload, indent=2))
    return cfg


def cleanup_worker_tmp(mission_id: str) -> None:
    tmp = config.worker_tmpdir(mission_id)
    if tmp.exists():
        subprocess.run(["rm", "-rf", str(tmp)], capture_output=True)


# ---------- claude invocation ----------


def _build_claude_cmd(
    mission_id: str, directive: str, *, first_step: bool, mcp_config: Path
) -> str:
    if first_step:
        flag = f"--session-id {shlex.quote(mission_id)}"
    else:
        flag = f"--resume {shlex.quote(mission_id)}"
    return (
        f"{CLAUDE_BIN} {flag} --mcp-config {shlex.quote(str(mcp_config))}"
        f" --dangerously-skip-permissions"
        f" --print {shlex.quote(directive)}"
    )


def launch_step(
    mission_id: str, directive: str, *, first_step: bool
) -> None:
    session = config.tmux_session_name(mission_id)
    if not tmux_session_exists(session):
        tmux_create_session(mission_id)
    mcp_config = write_worker_mcp_config(mission_id)
    cmd = _build_claude_cmd(
        mission_id, directive, first_step=first_step, mcp_config=mcp_config
    )
    tmux_send(session, cmd)


def launch_oob(mission_id: str, directive: str) -> None:
    """Out-of-band directive (heartbeat / ping). Same as resume."""
    launch_step(mission_id, directive, first_step=False)


def step_running(mission_id: str) -> bool:
    """Heuristic: claude is still the pane's foreground process."""
    session = config.tmux_session_name(mission_id)
    cmd = tmux_pane_current_command(session)
    if cmd is None:
        return False
    return cmd == CLAUDE_BIN or cmd.endswith("/claude")
