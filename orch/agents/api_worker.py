"""Built-in worker agent loop for ORCH_AGENT=api.

One process = one worker turn: load the mission's saved conversation, append
the directive, run a tool-calling loop against the configured model, persist,
exit. The orchestrator treats it exactly like any other agent CLI.

Providers:
  anthropic - Anthropic Messages API (default model: claude-sonnet-5)
  openai    - any OpenAI-compatible /chat/completions endpoint
              (OpenAI, OpenRouter, Ollama, vLLM, …; set ORCH_API_MODEL)
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys

import httpx

from .api import session_file
from ..client import DaemonClient

MAX_TOOL_ROUNDS = 40
SHELL_TIMEOUT = 300
OUTPUT_CAP = 20_000

SYSTEM_PROMPT = """\
You are an autonomous WORKER agent in the orch mission system, running
unattended on the mission owner's machine. Each invocation hands you one
directive (a queued step, heartbeat nudge, user reply, or scheduled task).
Do the work using your tools, then stop.

Rules:
- The user only ever sees what you send via `notify` / `send_file`. Report
  results there; keep messages concise and useful.
- Use `shell` for real work on this machine (full access - be careful).
- Plan future work with `queue_add` (cue at_time/on_timeout/on_current_complete):
  the mission stays alive while steps are pending. Queue follow-ups BEFORE
  finishing your turn if the job isn't done.
- `message_host` reaches the orchestrating host (a mailbox), not the human.
"""


# ---------- tool definitions (provider-neutral) ----------

TOOLS: list[dict] = [
    {"name": "shell",
     "description": "Run a shell command on this machine and get stdout+stderr. "
                    f"Times out after {SHELL_TIMEOUT}s.",
     "params": {"command": {"type": "string"}}, "required": ["command"]},
    {"name": "notify",
     "description": "Send a Telegram text message to the human user.",
     "params": {"text": {"type": "string"}}, "required": ["text"]},
    {"name": "send_file",
     "description": "Send a local file (any type; images show inline) to the "
                    "user via Telegram. Absolute path; optional caption.",
     "params": {"path": {"type": "string"}, "caption": {"type": "string"}},
     "required": ["path"]},
    {"name": "message_host",
     "description": "Message the orchestrating host's mailbox (not the human). "
                    "Optional list of absolute file paths to attach.",
     "params": {"text": {"type": "string"},
                "files": {"type": "array", "items": {"type": "string"}}},
     "required": ["text"]},
    {"name": "queue_add",
     "description": "Queue a future step for yourself. cue examples: "
                    '{"type":"at_time","at":"YYYY-MM-DD HH:MM"} | '
                    '{"type":"on_timeout","seconds":N} | '
                    '{"type":"on_current_complete"}',
     "params": {"directive": {"type": "string"}, "cue": {"type": "object"}},
     "required": ["directive", "cue"]},
    {"name": "queue_list",
     "description": "List this mission's queued steps.",
     "params": {}, "required": []},
    {"name": "mission_status",
     "description": "This mission's full state (steps, pings, heartbeat).",
     "params": {}, "required": []},
    {"name": "heartbeat_set",
     "description": "Set the heartbeat interval in seconds.",
     "params": {"interval_s": {"type": "integer"}}, "required": ["interval_s"]},
    {"name": "get_user_location",
     "description": "The user's latest shared location, if any.",
     "params": {}, "required": []},
]


def _exec_tool(mission_id: str, name: str, args: dict) -> str:
    cl = DaemonClient()
    try:
        if name == "shell":
            try:
                r = subprocess.run(
                    args.get("command", ""), shell=True, capture_output=True,
                    timeout=SHELL_TIMEOUT, text=True,
                )
                out = (r.stdout or "") + (("\n[stderr]\n" + r.stderr) if r.stderr else "")
                out = out or f"(no output, exit {r.returncode})"
            except subprocess.TimeoutExpired:
                out = f"(timed out after {SHELL_TIMEOUT}s)"
            return out[:OUTPUT_CAP]
        if name == "notify":
            return json.dumps(cl.call("notify", {"mission_id": mission_id,
                                                 "text": args.get("text", "")}))
        if name == "send_file":
            return json.dumps(cl.call("notify_file", {
                "mission_id": mission_id, "path": args.get("path", ""),
                "caption": args.get("caption", "")}))
        if name == "message_host":
            return json.dumps(cl.call("host.message", {
                "mission_id": mission_id, "text": args.get("text", ""),
                "files": args.get("files", [])}))
        if name == "queue_add":
            return json.dumps(cl.call("step.add", {
                "mission_id": mission_id, "directive": args.get("directive", ""),
                "cue": args.get("cue", {}), "created_by": "worker"}))
        if name == "queue_list":
            return json.dumps(cl.call("step.list", {"mission_id": mission_id}))
        if name == "mission_status":
            return json.dumps(cl.call("mission.get", {"mission_id": mission_id}), default=str)
        if name == "heartbeat_set":
            return json.dumps(cl.call("heartbeat.set", {
                "mission_id": mission_id, "interval_s": args.get("interval_s")}))
        if name == "get_user_location":
            return json.dumps(cl.call("location.get", {"mission_id": mission_id}))
        return f"unknown tool: {name}"
    except Exception as e:  # noqa: BLE001
        return f"tool error: {e}"


# ---------- providers ----------

def _cfg() -> dict:
    provider = (os.environ.get("ORCH_API_PROVIDER") or "anthropic").lower()
    key = (os.environ.get("ORCH_API_KEY")
           or os.environ.get("ANTHROPIC_API_KEY" if provider == "anthropic" else "OPENAI_API_KEY")
           or "")
    model = os.environ.get("ORCH_API_MODEL") or (
        "claude-sonnet-5" if provider == "anthropic" else "")
    base = os.environ.get("ORCH_API_BASE_URL") or (
        "https://api.anthropic.com" if provider == "anthropic"
        else "https://api.openai.com/v1")
    if not key:
        sys.exit("ORCH_API_KEY (or provider key env) not set")
    if not model:
        sys.exit("ORCH_API_MODEL required for openai-compatible providers")
    return {"provider": provider, "key": key, "model": model, "base": base.rstrip("/")}


def _anthropic_tools() -> list[dict]:
    return [{"name": t["name"], "description": t["description"],
             "input_schema": {"type": "object", "properties": t["params"],
                              "required": t["required"]}} for t in TOOLS]


def _openai_tools() -> list[dict]:
    return [{"type": "function",
             "function": {"name": t["name"], "description": t["description"],
                          "parameters": {"type": "object", "properties": t["params"],
                                         "required": t["required"]}}} for t in TOOLS]


def _run_anthropic(cfg: dict, mission_id: str, messages: list) -> list:
    headers = {"x-api-key": cfg["key"], "anthropic-version": "2023-06-01",
               "content-type": "application/json"}
    for _ in range(MAX_TOOL_ROUNDS):
        r = httpx.post(f"{cfg['base']}/v1/messages", headers=headers, timeout=300, json={
            "model": cfg["model"], "max_tokens": 8192, "system": SYSTEM_PROMPT,
            "tools": _anthropic_tools(), "messages": messages,
        })
        r.raise_for_status()
        d = r.json()
        messages.append({"role": "assistant", "content": d["content"]})
        if d.get("stop_reason") != "tool_use":
            break
        results = []
        for block in d["content"]:
            if block.get("type") == "tool_use":
                out = _exec_tool(mission_id, block["name"], block.get("input") or {})
                results.append({"type": "tool_result", "tool_use_id": block["id"],
                                "content": out})
        messages.append({"role": "user", "content": results})
    return messages


def _run_openai(cfg: dict, mission_id: str, messages: list) -> list:
    headers = {"Authorization": f"Bearer {cfg['key']}", "content-type": "application/json"}
    if not messages or messages[0].get("role") != "system":
        messages.insert(0, {"role": "system", "content": SYSTEM_PROMPT})
    for _ in range(MAX_TOOL_ROUNDS):
        r = httpx.post(f"{cfg['base']}/chat/completions", headers=headers, timeout=300, json={
            "model": cfg["model"], "tools": _openai_tools(), "messages": messages,
        })
        r.raise_for_status()
        msg = r.json()["choices"][0]["message"]
        messages.append(msg)
        calls = msg.get("tool_calls") or []
        if not calls:
            break
        for call in calls:
            fn = call["function"]
            try:
                args = json.loads(fn.get("arguments") or "{}")
            except Exception:
                args = {}
            out = _exec_tool(mission_id, fn["name"], args)
            messages.append({"role": "tool", "tool_call_id": call["id"], "content": out})
    return messages


# ---------- compaction ----------

def _compact(cfg: dict, mission_id: str, messages: list) -> list:
    prompt = ("Summarize this working session so a fresh instance of you can "
              "continue seamlessly: mission goal, current state, key facts/paths, "
              "pending obligations, and how you talk to the user. Be thorough "
              "but under 1500 words.")
    if cfg["provider"] == "anthropic":
        msgs = messages + [{"role": "user", "content": prompt}]
        out = _run_anthropic(cfg, mission_id, msgs)
        summary = "".join(b.get("text", "") for b in out[-1]["content"]
                          if isinstance(b, dict)) if isinstance(out[-1].get("content"), list) else str(out[-1]["content"])
        return [{"role": "user", "content": f"[Session summary from compaction]\n{summary}"},
                {"role": "assistant", "content": [{"type": "text", "text": "Understood - continuing from that state."}]}]
    msgs = messages + [{"role": "user", "content": prompt}]
    out = _run_openai(cfg, mission_id, msgs)
    summary = out[-1].get("content") or ""
    return [{"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": f"[Session summary from compaction]\n{summary}"},
            {"role": "assistant", "content": "Understood - continuing from that state."}]


# ---------- entry ----------

def main() -> None:
    ap = argparse.ArgumentParser(prog="orch-api-worker")
    ap.add_argument("--mission-id", required=True)
    ap.add_argument("--directive", default=None)
    ap.add_argument("--compact", action="store_true")
    a = ap.parse_args()

    cfg = _cfg()
    f = session_file(a.mission_id)
    f.parent.mkdir(parents=True, exist_ok=True)
    messages: list = json.loads(f.read_text()) if f.exists() else []

    if a.compact:
        messages = _compact(cfg, a.mission_id, messages)
    elif a.directive is not None:
        messages.append({"role": "user", "content": a.directive})
        if cfg["provider"] == "anthropic":
            messages = _run_anthropic(cfg, a.mission_id, messages)
        else:
            messages = _run_openai(cfg, a.mission_id, messages)
    else:
        sys.exit("need --directive or --compact")

    f.write_text(json.dumps(messages, default=str))


if __name__ == "__main__":
    main()
