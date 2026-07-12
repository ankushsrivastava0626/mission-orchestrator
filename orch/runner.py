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


def tmux_interrupt(mission_id: str) -> None:
    """Send Ctrl-C (SIGINT) to whatever's running in the mission's tmux pane."""
    session = config.tmux_session_name(mission_id)
    if tmux_session_exists(session):
        _run(["tmux", "send-keys", "-t", session, "C-c"], check=False)


def tmux_pane_current_command(session: str) -> str | None:
    res = _run(
        ["tmux", "list-panes", "-t", session, "-F", "#{pane_current_command}"],
        check=False,
    )
    if res.returncode != 0:
        return None
    out = res.stdout.decode("utf-8", "replace").strip().splitlines()
    return out[0] if out else None


def capture_pane_full(mission_id: str, scrollback: int = 2000) -> str:
    """Grab the last `scrollback` lines from the mission's pane. '' if no tmux."""
    session = config.tmux_session_name(mission_id)
    if not tmux_session_exists(session):
        return ""
    res = _run(
        ["tmux", "capture-pane", "-t", session, "-p", "-S", f"-{scrollback}"],
        check=False,
    )
    if res.returncode != 0:
        return ""
    return res.stdout.decode("utf-8", "replace")


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


def _load_extra_worker_mcps(mission_id: str) -> dict:
    """Merge in extra MCP servers shared by all workers from EXTRA_WORKER_MCPS
    (default /etc/orch/worker_mcp.json). The string '{mission_id}' is
    substituted in any args/env values so each worker can get its own paths
    (e.g. a per-mission Playwright browser profile). 'orch' is reserved."""
    path = config.EXTRA_WORKER_MCPS_PATH
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text())
    except Exception as e:
        log.warning("extra worker MCPs: failed to read %s: %s", path, e)
        return {}
    servers = raw.get("mcpServers", raw)  # accept either {mcpServers:{..}} or {..}
    if not isinstance(servers, dict):
        return {}

    def _sub(v):
        if isinstance(v, str):
            return v.replace("{mission_id}", mission_id)
        if isinstance(v, list):
            return [_sub(x) for x in v]
        if isinstance(v, dict):
            return {k: _sub(x) for k, x in v.items()}
        return v

    out = {}
    for name, conf in servers.items():
        if name == "orch":
            continue
        out[name] = _sub(conf)
    return out


def write_worker_mcp_config(mission_id: str) -> Path:
    tmp = config.worker_tmpdir(mission_id)
    tmp.mkdir(parents=True, exist_ok=True)
    (tmp / "cookies").mkdir(parents=True, exist_ok=True)
    cfg = tmp / ".mcp.json"
    servers = {
        "orch": {
            "command": "orch-mcp",
            "args": ["--mode", "worker", "--mission-id", mission_id],
            "env": {
                config.ENV_MISSION_ID: mission_id,
                config.ENV_SOCKET: str(config.SOCKET_PATH),
            },
        }
    }
    servers.update(_load_extra_worker_mcps(mission_id))
    cfg.write_text(json.dumps({"mcpServers": servers}, indent=2))
    return cfg


def cleanup_worker_tmp(mission_id: str) -> None:
    tmp = config.worker_tmpdir(mission_id)
    if tmp.exists():
        subprocess.run(["rm", "-rf", str(tmp)], capture_output=True)


# ---------- agent invocation (via the configured adapter) ----------


def launch_step(
    mission_id: str, directive: str, *, first_step: bool, adapter=None
) -> None:
    from . import agents
    if adapter is None:
        adapter = agents.get_adapter()
    session = config.tmux_session_name(mission_id)
    if not tmux_session_exists(session):
        tmux_create_session(mission_id)
    adapter.prepare(mission_id)
    cmd = adapter.step_cmd(mission_id, directive, first_step)
    tmux_send(session, cmd)


def launch_oob(mission_id: str, directive: str) -> None:
    """Out-of-band directive (heartbeat / ping). Same as resume."""
    launch_step(mission_id, directive, first_step=False)


def _pane_children(pane_pid: int) -> list[int]:
    """All descendant PIDs of the tmux pane's shell (one /proc pass)."""
    import glob
    ppid_map: dict[int, list[int]] = {}
    for stat in glob.glob("/proc/[0-9]*/stat"):
        try:
            parts = open(stat).read().rsplit(") ", 1)[1].split()
            pid = int(stat.split("/")[2])
            ppid = int(parts[1])
            ppid_map.setdefault(ppid, []).append(pid)
        except (OSError, IndexError, ValueError):
            continue
    out: list[int] = []
    stack = [pane_pid]
    while stack:
        for child in ppid_map.get(stack.pop(), []):
            out.append(child)
            stack.append(child)
    return out


def step_running(mission_id: str) -> bool:
    """Is a worker-agent process for this mission currently running?

    Primary signal (backend-agnostic): the mission's tmux pane shell has ANY
    child process - i.e. a foreground job is running. This works for every
    adapter because env-var-carried mission ids vanish from process cmdlines
    after exec, so string matching alone misses codex/opencode/agy workers.

    Secondary signals: a legacy cmdline match (claude puts the id in argv),
    and a live detached compaction process (pidfile written by adapters) -
    a compact IS a session resume and must count as busy.
    """
    session = config.tmux_session_name(mission_id)
    if tmux_session_exists(session):
        pane_pid = tmux_pane_pid(session)
        if pane_pid and _pane_children(pane_pid):
            return True
    # Detached compaction in flight?
    import os as _os
    pidfile = _os.path.expanduser(f"~/.orch/compact-{mission_id}.pid")
    try:
        pid = int(open(pidfile).read().strip())
        _os.kill(pid, 0)
        return True
    except (OSError, ValueError):
        pass
    # Legacy cmdline match (claude-style argv ids).
    from . import agents
    try:
        res = subprocess.run(["pgrep", "-af", mission_id], capture_output=True)
    except FileNotFoundError:
        return False
    for line in res.stdout.decode("utf-8", "replace").splitlines():
        if "pgrep" in line:
            continue
        if agents.any_worker_line(line, mission_id):
            return True
    return False
