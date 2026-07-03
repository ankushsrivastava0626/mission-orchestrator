"""Agent adapters - one worker backend per supported coding agent.

The orchestrator core (daemon/engine/runner) never talks to a specific CLI;
it goes through the Adapter interface. Select a backend with ORCH_AGENT:

  claude  - Claude Code CLI (default; richest support: resume, compact, tokens)
  codex   - OpenAI Codex CLI (per-mission CODEX_HOME keeps sessions isolated)
  gemini  - Google Gemini CLI (per-mission workdir keys its session store)
  api     - built-in worker loop on raw API keys (Anthropic or any
            OpenAI-compatible endpoint: OpenAI, OpenRouter, Ollama, …)
  custom  - any CLI via command templates (ORCH_CUSTOM_FIRST_CMD / _RESUME_CMD)
"""

from __future__ import annotations

import os

from .base import Adapter

_ADAPTER: Adapter | None = None


def get_adapter() -> Adapter:
    global _ADAPTER
    if _ADAPTER is None:
        name = (os.environ.get("ORCH_AGENT") or "claude").strip().lower()
        if name in ("claude", "claude-code", "claude_code"):
            from .claude_code import ClaudeCodeAdapter
            _ADAPTER = ClaudeCodeAdapter()
        elif name == "codex":
            from .codex import CodexAdapter
            _ADAPTER = CodexAdapter()
        elif name in ("gemini", "gemini-cli", "antigravity"):
            from .gemini import GeminiAdapter
            _ADAPTER = GeminiAdapter()
        elif name == "api":
            from .api import ApiAdapter
            _ADAPTER = ApiAdapter()
        elif name == "custom":
            from .custom import CustomAdapter
            _ADAPTER = CustomAdapter()
        else:
            raise RuntimeError(f"unknown ORCH_AGENT: {name!r}")
    return _ADAPTER
