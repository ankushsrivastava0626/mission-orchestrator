"""Cross-agent mission handoff.

When the active backend changes (manual switch or automatic fallback), a
mission's old session can't be resumed by the new agent - different CLI,
different transcript format. Instead, the next launch starts a FRESH session
seeded with a handoff document: mission metadata from the orch DB plus a tail
of the old session's actual conversation (extracted mechanically, no tokens
spent), so the new agent picks up where the old one left off.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import time

from . import db

log = logging.getLogger(__name__)

TRANSCRIPT_TAIL_CHARS = 6000
HANDOFF_CAP_CHARS = 9000
HANDOFF_DIR = os.path.expanduser("~/.orch/handoff")


def _tail_claude(mission_id: str) -> str:
    """Extract readable conversation text from a Claude Code session jsonl."""
    from .agents.claude_code import ClaudeCodeAdapter
    path = ClaudeCodeAdapter().session_jsonl(mission_id)
    if not path:
        return ""
    parts: list[str] = []
    try:
        for line in open(path):
            try:
                o = json.loads(line)
            except Exception:
                continue
            msg = o.get("message")
            if not isinstance(msg, dict):
                continue
            role = msg.get("role")
            content = msg.get("content")
            texts: list[str] = []
            if isinstance(content, str):
                texts.append(content)
            elif isinstance(content, list):
                for b in content:
                    if isinstance(b, dict) and b.get("type") == "text":
                        texts.append(b.get("text") or "")
            t = "\n".join(x for x in texts if x).strip()
            if t and role in ("user", "assistant"):
                parts.append(f"[{role}] {t}")
    except OSError:
        return ""
    return "\n".join(parts)[-TRANSCRIPT_TAIL_CHARS:]


def _tail_api(mission_id: str) -> str:
    """Extract conversation text from an api-backend session json."""
    from .agents.api import session_file
    f = session_file(mission_id)
    if not f.exists():
        return ""
    try:
        msgs = json.loads(f.read_text())
    except Exception:
        return ""
    parts: list[str] = []
    for m in msgs:
        role = m.get("role")
        c = m.get("content")
        if isinstance(c, list):
            c = "\n".join(b.get("text", "") for b in c
                          if isinstance(b, dict) and b.get("type") == "text")
        if isinstance(c, str) and c.strip() and role in ("user", "assistant"):
            parts.append(f"[{role}] {c.strip()}")
    return "\n".join(parts)[-TRANSCRIPT_TAIL_CHARS:]


def _transcript_tail(mission_id: str, old_agent: str) -> str:
    try:
        if old_agent == "claude":
            return _tail_claude(mission_id)
        if old_agent == "api":
            return _tail_api(mission_id)
    except Exception:
        log.exception("handoff transcript extraction failed for %s", mission_id)
    return ""  # codex/gemini/custom transcripts aren't parsed - DB state only


def build_handoff(conn: sqlite3.Connection, mission_id: str,
                  old_agent: str, new_agent: str) -> str:
    """Assemble the handoff document and persist a copy for auditing."""
    m = db.get_mission(conn, mission_id)
    lines = [
        f"mission: {m['name']}  (id {mission_id})",
        f"previous agent: {old_agent}  ->  new agent: {new_agent}",
        f"heartbeat: every {m['heartbeat_interval_s']}s",
    ]
    steps = conn.execute(
        "SELECT position, state, directive, created_by FROM steps"
        " WHERE mission_id = ? ORDER BY position DESC LIMIT 12",
        (mission_id,),
    ).fetchall()
    if steps:
        lines.append("\nrecent steps (newest first):")
        for s in steps:
            d = (s["directive"] or "").replace("\n", " ")[:160]
            lines.append(f"  [{s['position']}] {s['state']:9} ({s['created_by'] or 'host'}) {d}")
    sps = conn.execute(
        "SELECT id, condition, action, state FROM scripted_pings"
        " WHERE mission_id = ? AND state != 'deleted'", (mission_id,),
    ).fetchall()
    if sps:
        lines.append("\nactive watcher scripts (scripted pings):")
        for sp in sps:
            lines.append(f"  {sp['id']}: when [{sp['condition']}] -> {sp['action']} ({sp['state']})")
        lines.append("  (their scripts keep running; `owatch` still works for this mission)")
    tail = _transcript_tail(mission_id, old_agent)
    if tail:
        lines.append("\n--- tail of the previous agent's conversation ---")
        lines.append(tail)
    doc = "\n".join(lines)[:HANDOFF_CAP_CHARS]
    try:
        os.makedirs(HANDOFF_DIR, exist_ok=True)
        with open(os.path.join(HANDOFF_DIR, f"{mission_id}-{int(time.time())}.md"), "w") as fh:
            fh.write(doc)
    except OSError:
        pass
    return doc


def wrap_directive(handoff_doc: str, directive: str, old_agent: str, new_agent: str) -> str:
    return (
        f"[AGENT HANDOFF] This mission previously ran on the `{old_agent}` agent; "
        f"you (`{new_agent}`) are taking over in a FRESH session. The notes below "
        f"summarize the prior state - absorb them, then carry out the directive. "
        f"Do not re-do work marked completed.\n\n"
        f"===== HANDOFF NOTES =====\n{handoff_doc}\n"
        f"===== END HANDOFF =====\n\n"
        f"DIRECTIVE:\n{directive}"
    )
