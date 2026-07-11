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

AGENT_NAMES = ("claude", "codex", "gemini", "api", "custom")

_ADAPTER: Adapter | None = None


def canonical(name: str) -> str:
    n = (name or "").strip().lower()
    aliases = {"claude-code": "claude", "claude_code": "claude",
               "gemini-cli": "gemini", "antigravity": "gemini"}
    return aliases.get(n, n)


def make_adapter(name: str) -> Adapter:
    """Fresh adapter instance for an explicit backend name (no caching)."""
    n = canonical(name)
    if n == "claude":
        from .claude_code import ClaudeCodeAdapter
        return ClaudeCodeAdapter()
    if n == "codex":
        from .codex import CodexAdapter
        return CodexAdapter()
    if n == "gemini":
        from .gemini import GeminiAdapter
        return GeminiAdapter()
    if n == "api":
        from .api import ApiAdapter
        return ApiAdapter()
    if n == "custom":
        from .custom import CustomAdapter
        return CustomAdapter()
    raise RuntimeError(f"unknown agent backend: {name!r}")


def get_adapter() -> Adapter:
    global _ADAPTER
    if _ADAPTER is None:
        _ADAPTER = make_adapter(os.environ.get("ORCH_AGENT") or "claude")
    return _ADAPTER


def reset() -> None:
    """Drop the cached adapter so the next get_adapter() re-reads ORCH_AGENT.
    Called after a live agent switch."""
    global _ADAPTER
    _ADAPTER = None


def availability(name: str) -> tuple[bool, str]:
    """(ok, reason) for a backend name - never raises."""
    try:
        return make_adapter(name).available()
    except Exception as e:  # noqa: BLE001 - e.g. custom templates unset
        return False, str(e)


_BY_NAME: dict[str, Adapter] = {}


def adapter_named(name: str) -> Adapter:
    """Cached adapter instance for an explicit backend (for per-mission pins
    and for reading sessions that live on a non-global backend)."""
    n = canonical(name)
    if n not in _BY_NAME:
        _BY_NAME[n] = make_adapter(n)
    return _BY_NAME[n]


def any_worker_line(line: str, mission_id: str) -> bool:
    """Does this pgrep line look like a live worker turn for the mission on
    ANY backend? Missions can be pinned to a different backend than the
    global one, so busy-detection must recognize every signature."""
    for n in AGENT_NAMES:
        if n == "custom" and not os.environ.get("ORCH_CUSTOM_FIRST_CMD"):
            # custom's matcher is loose (any line with the id); only let it
            # vote when a custom backend is actually configured.
            continue
        try:
            if adapter_named(n).is_running_line(line, mission_id):
                return True
        except Exception:  # noqa: BLE001 - custom may be unconfigured
            continue
    return False
