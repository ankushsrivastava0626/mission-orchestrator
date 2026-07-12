"""OpenAI Codex CLI backend.

Isolation trick: each mission gets its own CODEX_HOME (under the mission's
tmp dir) so its rollout/session store is private - `codex exec resume --last`
is then unambiguous per mission, and no session-id discovery is needed.
Auth is shared by symlinking auth.json from the real ~/.codex.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from .base import Adapter
from .. import config

CODEX_BIN = os.environ.get("ORCH_CODEX_BIN", "codex")

# Codex (since the server-side "code mode" rollout) hides MCP tools in a
# DEFERRED tool catalog under names like `mcp__orch__notify` - models often
# fail to find them and end turns silently. Every directive gets this hint.
TOOL_NOTE = """\
[TOOL NOTE] Your orch tools are MCP tools that may be DEFERRED: if `notify` /
`orch` tools aren't directly visible, search your deferred tool catalog for
`mcp__orch__*` (e.g. `mcp__orch__notify`) and load/call them from there.
The user ONLY sees messages you send via the orch notify tool - always send
your reply/result through it before ending your turn.
PRIVACY BOUNDARY: Interact with the orchestrator ONLY through your provided tools. NEVER read or modify orch internals - ~/.orch/orch.db, other missions' workdirs/sessions, /etc/orchd.env - other missions' data is strictly off-limits, even if asked about 'other agents'.

"""


class CodexAdapter(Adapter):
    name = "codex"
    supports_compact = True

    def available(self) -> tuple[bool, str]:
        import shutil as _sh
        if _sh.which(CODEX_BIN):
            return True, "ok"
        return False, f"`{CODEX_BIN}` not found on PATH"

    def _home(self, mission_id: str) -> Path:
        # Durable (NOT /tmp): sessions must survive reboots and mission
        # teardown/reopen, so the user's backend choice stays seamless.
        return Path(os.path.expanduser("~/.orch/codex-work")) / mission_id / "codex-home"

    def prepare(self, mission_id: str) -> None:
        home = self._home(mission_id)
        home.mkdir(parents=True, exist_ok=True)
        # Share the user's Codex auth with the per-mission home.
        real = Path(os.path.expanduser("~/.codex"))
        for fname in ("auth.json",):
            src, dst = real / fname, home / fname
            if src.exists() and not dst.exists():
                try:
                    dst.symlink_to(src)
                except OSError:
                    pass
        # Per-mission config: orch worker MCP + any extra shared MCP servers.
        from ..runner import _load_extra_worker_mcps
        servers: dict = {
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
        lines = []
        for name, s in servers.items():
            lines.append(f'[mcp_servers.{name}]')
            lines.append(f'command = {json.dumps(s.get("command", ""))}')
            lines.append(f'args = {json.dumps(s.get("args", []))}')
            env = s.get("env") or {}
            if env:
                lines.append(f'[mcp_servers.{name}.env]')
                for k, v in env.items():
                    lines.append(f'{k} = {json.dumps(str(v))}')
        (home / "config.toml").write_text("\n".join(lines) + "\n")

    def step_cmd(self, mission_id: str, directive: str, first: bool) -> str:
        home = self._home(mission_id)
        sub = "exec" if first else "exec resume --last"
        # --yolo = codex's alias for --dangerously-bypass-approvals-and-sandbox.
        # ORCH_MISSION_ID in the env keeps the mission id visible to pgrep.
        return (
            f"env CODEX_HOME={self.q(str(home))} ORCH_MISSION_ID={mission_id}"
            f" {CODEX_BIN} {sub} --yolo"
            f" --skip-git-repo-check {self.q(TOOL_NOTE + directive)}"
        )

    def is_running_line(self, line: str, mission_id: str) -> bool:
        return "codex" in line and mission_id in line

    def has_session(self, mission_id: str) -> bool | None:
        return self._latest_rollout(mission_id) is not None

    # ---- session size + compaction --------------------------------------

    def _latest_rollout(self, mission_id: str) -> str | None:
        import glob
        home = self._home(mission_id)
        hits = glob.glob(str(home / "sessions" / "**" / "*.jsonl"), recursive=True)
        return max(hits, key=os.path.getmtime) if hits else None

    def session_path(self, mission_id: str) -> str | None:
        return self._latest_rollout(mission_id)

    def context_tokens(self, mission_id: str) -> tuple[int, int] | None:
        """Codex rollouts log token_count events; last_token_usage.input_tokens
        is what the most recent turn re-read - i.e. the live context size."""
        path = self._latest_rollout(mission_id)
        if not path:
            return None
        last = 0
        turns = 0
        try:
            for line in open(path):
                if '"token_count"' not in line and '"user_message"' not in line:
                    continue
                try:
                    o = json.loads(line)
                except Exception:
                    continue
                payload = o.get("payload") or o
                if payload.get("type") == "user_message":
                    turns += 1
                    continue
                if payload.get("type") == "token_count":
                    info = payload.get("info") or {}
                    ltu = info.get("last_token_usage") or {}
                    v = int(ltu.get("input_tokens") or 0)
                    if v > 0:
                        last = v
        except OSError:
            return None
        return (last, turns) if last else None

    def compact(self, mission_id: str) -> bool:
        """Headless `/compact` - verified: `codex exec resume --last "/compact"`
        summarizes the session in place ("Context compacted.")."""
        import subprocess
        home = self._home(mission_id)
        logf = open(os.path.expanduser(f"~/.orch/compact-{mission_id}.log"), "ab")
        logf.write(f"\n=== codex compact start mid={mission_id} ===\n".encode())
        logf.flush()
        proc = subprocess.Popen(
            [CODEX_BIN, "exec", "resume", "--last", "--yolo",
             "--skip-git-repo-check", "/compact"],
            stdin=subprocess.DEVNULL, stdout=logf, stderr=subprocess.STDOUT,
            start_new_session=True, cwd="/",
            env={**os.environ, "CODEX_HOME": str(home)},
        )
        from .claude_code import _write_compact_pid
        _write_compact_pid(mission_id, proc.pid)
        return True
