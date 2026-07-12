"""Claude Code CLI backend (the reference adapter).

Sessions: mission_id doubles as the Claude session UUID (--session-id on the
first launch, --resume after). Claude keys sessions by project = cwd, so all
launches happen from the tmux pane's cwd; compaction runs from "/" where the
daemon-spawned workers live.
"""

from __future__ import annotations

import os
import shutil
import subprocess

from .base import Adapter
from .. import config

CLAUDE_BIN = os.environ.get("ORCH_CLAUDE_BIN", "claude")


def _claude_abs() -> str:
    return (shutil.which(CLAUDE_BIN)
            or os.path.expanduser("~/.local/bin/claude")
            or CLAUDE_BIN)


class ClaudeCodeAdapter(Adapter):
    name = "claude"
    supports_compact = True

    def available(self) -> tuple[bool, str]:
        import shutil as _sh
        if _sh.which(CLAUDE_BIN) or os.path.isfile(os.path.expanduser("~/.local/bin/claude")):
            return True, "ok"
        return False, f"`{CLAUDE_BIN}` not found on PATH"

    def prepare(self, mission_id: str) -> None:
        from .. import runner
        runner.write_worker_mcp_config(mission_id)

    def step_cmd(self, mission_id: str, directive: str, first: bool) -> str:
        mcp_config = config.worker_tmpdir(mission_id) / ".mcp.json"
        flag = (f"--session-id {self.q(mission_id)}" if first
                else f"--resume {self.q(mission_id)}")
        return (
            f"{CLAUDE_BIN} {flag} --mcp-config {self.q(str(mcp_config))}"
            f" --dangerously-skip-permissions"
            f" --print {self.q(directive)}"
        )

    def is_running_line(self, line: str, mission_id: str) -> bool:
        return ("claude" in line
                and ("--resume " in line or "--session-id " in line))

    # ---- session size + compaction --------------------------------------

    def session_jsonl(self, mission_id: str) -> str | None:
        import glob
        base = os.path.expanduser("~/.claude/projects")
        hits = glob.glob(f"{base}/*/{mission_id}.jsonl")
        return hits[0] if hits else None

    def session_path(self, mission_id: str) -> str | None:
        return self.session_jsonl(mission_id)

    def has_session(self, mission_id: str) -> bool | None:
        return self.session_jsonl(mission_id) is not None

    def context_tokens(self, mission_id: str) -> tuple[int, int] | None:
        path = self.session_jsonl(mission_id)
        if not path:
            return None
        return _jsonl_context_tokens(path)

    def compact(self, mission_id: str) -> bool:
        """Detached headless `/compact`. IS_SANDBOX=1 lets
        --dangerously-skip-permissions run as root (the tmux server sets it
        for workers; a daemon-spawned process must set it explicitly)."""
        logpath = os.path.expanduser(f"~/.orch/compact-{mission_id}.log")
        logf = open(logpath, "ab")
        logf.write(f"\n=== compact start mid={mission_id} ===\n".encode())
        logf.flush()
        proc = subprocess.Popen(
            [_claude_abs(), "--resume", mission_id,
             "--dangerously-skip-permissions", "--verbose", "-p", "/compact"],
            stdin=subprocess.DEVNULL, stdout=logf, stderr=subprocess.STDOUT,
            start_new_session=True, cwd="/",
            env={**os.environ,
                 "PATH": os.environ.get("PATH", "") + ":" + os.path.expanduser("~/.local/bin"),
                 "IS_SANDBOX": "1"},
        )
        _write_compact_pid(mission_id, proc.pid)
        return True


def _write_compact_pid(mission_id: str, pid: int) -> None:
    """Busy-detection reads this so a detached compact counts as a running
    turn (it IS a session resume - nothing else may touch the session)."""
    try:
        with open(os.path.expanduser(f"~/.orch/compact-{mission_id}.pid"), "w") as fh:
            fh.write(str(pid))
    except OSError:
        pass


def _jsonl_context_tokens(path: str) -> tuple[int, int]:
    """(effective context tokens for the next resume, turn count).

    Normally the last non-zero `usage` block. But `/compact` rewrites the
    conversation to a summary WITHOUT emitting a new usage block; when a
    compact-summary boundary appears AFTER the last usage line, estimate the
    post-compact size from bytes-since-boundary (~4 chars/token) instead.
    """
    import json as _json
    last = 0
    last_usage_i = -1
    last_compact_i = -1
    lines: list[str] = []
    try:
        with open(path) as fh:
            for i, line in enumerate(fh):
                lines.append(line)
                try:
                    o = _json.loads(line)
                except Exception:
                    continue
                if o.get("isCompactSummary"):
                    last_compact_i = i
                u = (o.get("message") or {}).get("usage") or {}
                tot = ((u.get("input_tokens") or 0)
                       + (u.get("cache_read_input_tokens") or 0)
                       + (u.get("cache_creation_input_tokens") or 0))
                if tot > 0:
                    last = tot
                    last_usage_i = i
    except OSError:
        return 0, 0
    turns = len(lines)
    if last_compact_i >= 0 and last_compact_i > last_usage_i:
        chars = sum(len(lines[j]) for j in range(last_compact_i, len(lines)))
        return chars // 4, turns
    return last, turns
