"""OpenCode backend (opencode.ai) - the open-source agent harness.

Verified against opencode 1.17.18:
- headless: `opencode run <msg>` with `--auto` (auto-approve permissions)
- continuity: sessions are project(cwd)-scoped; each mission gets its own
  workdir so `--continue` resumes that mission's last session unambiguously
- MCP: project ./opencode.json {"mcp": {...}} - workers get the REAL orch
  worker MCP (notify/queue/pings/…), same as Claude Code workers
- model: `-m provider/model` per launch via ORCH_OPENCODE_MODEL - with an
  OPENROUTER_API_KEY this is "any model on the market" behind one key

Auth: opencode picks up provider keys from env (OPENROUTER_API_KEY,
ANTHROPIC_API_KEY, OPENAI_API_KEY, …) or its own `opencode auth login` store;
orch forwards the common key envs into worker panes.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from .base import Adapter
from .. import config

OPENCODE_BIN = os.environ.get("ORCH_OPENCODE_BIN", "opencode")
_KEY_ENVS = ("OPENROUTER_API_KEY", "ANTHROPIC_API_KEY", "OPENAI_API_KEY",
             "GEMINI_API_KEY", "GOOGLE_API_KEY")


class OpenCodeAdapter(Adapter):
    name = "opencode"

    def available(self) -> tuple[bool, str]:
        import shutil as _sh
        if _sh.which(OPENCODE_BIN):
            return True, "ok"
        return False, f"`{OPENCODE_BIN}` not found on PATH"

    def _workdir(self, mission_id: str) -> Path:
        return Path(os.path.expanduser("~/.orch/opencode-work")) / f"oc-{mission_id}"

    def prepare(self, mission_id: str) -> None:
        wd = self._workdir(mission_id)
        wd.mkdir(parents=True, exist_ok=True)
        from ..runner import _load_extra_worker_mcps
        mcp: dict = {
            "orch": {
                "type": "local",
                "command": ["orch-mcp", "--mode", "worker",
                            "--mission-id", mission_id],
                "environment": {
                    config.ENV_MISSION_ID: mission_id,
                    config.ENV_SOCKET: str(config.SOCKET_PATH),
                },
                "enabled": True,
            }
        }
        for sname, s in _load_extra_worker_mcps(mission_id).items():
            mcp[sname] = {
                "type": "local",
                "command": [s.get("command", "")] + list(s.get("args", [])),
                "environment": {k: str(v) for k, v in (s.get("env") or {}).items()},
                "enabled": True,
            }
        (wd / "opencode.json").write_text(json.dumps(
            {"$schema": "https://opencode.ai/config.json", "mcp": mcp}, indent=2))

    # ---- per-mission model choice ----------------------------------------

    def model_file(self, mission_id: str) -> Path:
        return self._workdir(mission_id) / ".opencode-model"

    def set_model(self, mission_id: str, model: str | None) -> None:
        self._workdir(mission_id).mkdir(parents=True, exist_ok=True)
        f = self.model_file(mission_id)
        if model:
            f.write_text(model.strip())
        else:
            f.unlink(missing_ok=True)

    def get_model(self, mission_id: str) -> str:
        """Mission-pinned model wins; else the global ORCH_OPENCODE_MODEL."""
        f = self.model_file(mission_id)
        if f.exists():
            m = f.read_text().strip()
            if m:
                return m
        return os.environ.get("ORCH_OPENCODE_MODEL", "").strip()

    def step_cmd(self, mission_id: str, directive: str, first: bool) -> str:
        wd = self._workdir(mission_id)
        envs = [f"ORCH_MISSION_ID={mission_id}"]
        for key in _KEY_ENVS:
            val = os.environ.get(key)
            if val:
                envs.append(f"{key}={self.q(val)}")
        model = self.get_model(mission_id)
        # OpenRouter model ids ("vendor/model[:tag]") need the provider prefix.
        if model and "/" in model and not model.startswith(("openrouter/",)) \
                and os.environ.get("OPENROUTER_API_KEY"):
            model = f"openrouter/{model}"
        model_flag = f" -m {self.q(model)}" if model else ""
        cont = "" if first else " --continue"
        return (
            f"cd {self.q(str(wd))} && env {' '.join(envs)}"
            f" {OPENCODE_BIN} run --auto{model_flag}{cont} {self.q(directive)}"
        )

    def is_running_line(self, line: str, mission_id: str) -> bool:
        return "opencode" in line and mission_id in line

    def has_session(self, mission_id: str) -> bool | None:
        # opencode stores per-project state under ~/.local/share/opencode;
        # project keying is by directory path. Cheap reliable signal: did this
        # mission's workdir ever launch a session? Track via a marker the
        # storage writes... fall back to unknown so DB history decides.
        return None
