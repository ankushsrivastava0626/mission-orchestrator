"""Google Gemini CLI backend (also covers Antigravity's CLI workflows).

Isolation trick: each mission runs from its own workdir with a project-level
.gemini/settings.json (MCP wiring). Gemini keys its session store by project
directory, so `--resume latest`-style continuation stays per-mission.

Session continuation flags differ across gemini-cli versions; override with
ORCH_GEMINI_RESUME_ARGS if your version uses a different spelling.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from .base import Adapter
from .. import config

GEMINI_BIN = os.environ.get("ORCH_GEMINI_BIN", "gemini")
RESUME_ARGS = os.environ.get("ORCH_GEMINI_RESUME_ARGS", "--resume latest")


class GeminiAdapter(Adapter):
    name = "gemini"

    def available(self) -> tuple[bool, str]:
        import shutil as _sh
        if _sh.which(GEMINI_BIN):
            return True, "ok"
        return False, f"`{GEMINI_BIN}` not found on PATH"

    def _workdir(self, mission_id: str) -> Path:
        return config.worker_tmpdir(mission_id) / "gemini-work"

    def prepare(self, mission_id: str) -> None:
        wd = self._workdir(mission_id)
        (wd / ".gemini").mkdir(parents=True, exist_ok=True)
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
        settings = {"mcpServers": servers}
        (wd / ".gemini" / "settings.json").write_text(json.dumps(settings, indent=2))

    def step_cmd(self, mission_id: str, directive: str, first: bool) -> str:
        wd = self._workdir(mission_id)
        resume = "" if first else f" {RESUME_ARGS}"
        # Trust the per-mission workdir (gemini refuses headless runs in
        # untrusted folders) and forward API-key auth from the daemon env -
        # the tmux pane doesn't inherit it (Google retired the CLI's free
        # login tier, so an API key is the reliable headless auth).
        envs = [f"ORCH_MISSION_ID={mission_id}", "GEMINI_CLI_TRUST_WORKSPACE=true"]
        for key in ("GEMINI_API_KEY", "GOOGLE_API_KEY", "GOOGLE_CLOUD_PROJECT"):
            val = os.environ.get(key)
            if val:
                envs.append(f"{key}={self.q(val)}")
        return (
            f"cd {self.q(str(wd))} && env {' '.join(envs)}"
            f" {GEMINI_BIN} --yolo{resume} -p {self.q(directive)}"
        )

    def is_running_line(self, line: str, mission_id: str) -> bool:
        return "gemini" in line and mission_id in line
