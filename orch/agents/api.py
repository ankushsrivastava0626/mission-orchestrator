"""Raw-API backend - no coding-agent CLI needed, just an API key.

Workers are turns of orch.agents.api_worker, a small built-in agent loop that
talks to either the Anthropic API or any OpenAI-compatible endpoint (OpenAI,
OpenRouter, Ollama, vLLM, Gemini's compat endpoint, …) with function calling:
a shell tool plus the orch worker tools bridged straight over the daemon
socket. Conversation state persists per mission in ~/.orch/api_sessions/.

Env:
  ORCH_API_PROVIDER  anthropic | openai        (default: anthropic)
  ORCH_API_KEY       the key (or set ANTHROPIC_API_KEY / OPENAI_API_KEY)
  ORCH_API_MODEL     model id (default: claude-sonnet-5 / gpt-5.2)
  ORCH_API_BASE_URL  override endpoint (for OpenAI-compatible servers)
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from .base import Adapter


def session_file(mission_id: str) -> Path:
    return Path(os.path.expanduser("~/.orch/api_sessions")) / f"{mission_id}.json"


class ApiAdapter(Adapter):
    name = "api"
    supports_compact = True

    def available(self) -> tuple[bool, str]:
        provider = (os.environ.get("ORCH_API_PROVIDER") or "anthropic").lower()
        key = (os.environ.get("ORCH_API_KEY")
               or os.environ.get("ANTHROPIC_API_KEY" if provider == "anthropic"
                                 else "OPENAI_API_KEY"))
        if not key:
            return False, "no API key (ORCH_API_KEY)"
        if provider != "anthropic" and not os.environ.get("ORCH_API_MODEL"):
            return False, "ORCH_API_MODEL required for openai-compatible providers"
        return True, "ok"

    def step_cmd(self, mission_id: str, directive: str, first: bool) -> str:
        py = self.q(sys.executable)
        return (
            f"{py} -m orch.agents.api_worker"
            f" --mission-id {self.q(mission_id)}"
            f" --directive {self.q(directive)}"
        )

    def is_running_line(self, line: str, mission_id: str) -> bool:
        return "api_worker" in line and mission_id in line

    def context_tokens(self, mission_id: str) -> tuple[int, int] | None:
        f = session_file(mission_id)
        if not f.exists():
            return None
        try:
            import json
            raw = f.read_text()
            msgs = json.loads(raw)
            return len(raw) // 4, len(msgs)
        except Exception:
            return None

    def session_path(self, mission_id: str) -> str | None:
        f = session_file(mission_id)
        return str(f) if f.exists() else None

    def has_session(self, mission_id: str) -> bool | None:
        # The api worker appends to its session json regardless of first/resume,
        # so create-vs-resume is moot - report reality anyway.
        return session_file(mission_id).exists()

    def compact(self, mission_id: str) -> bool:
        logf = open(os.path.expanduser(f"~/.orch/compact-{mission_id}.log"), "ab")
        proc = subprocess.Popen(
            [sys.executable, "-m", "orch.agents.api_worker",
             "--mission-id", mission_id, "--compact"],
            stdin=subprocess.DEVNULL, stdout=logf, stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        from .claude_code import _write_compact_pid
        _write_compact_pid(mission_id, proc.pid)
        return True

    def cleanup(self, mission_id: str) -> None:
        # Session json is kept (like Claude transcripts) - mission.delete-level
        # purges can remove it manually if ever needed.
        pass
