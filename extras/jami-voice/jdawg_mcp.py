#!/usr/bin/python3
"""J-dawg "talk to the user" MCP server - exposed to every orch worker.

When a worker (a Claude running a mission step) hits a point where it genuinely
needs the human - a decision, an ambiguity, a go/no-go - it calls `talk_to_user`.
That does NOT block the worker waiting on a phone call. Instead it drops a request
file that the always-on J-dawg Jami agent watches; J-dawg then reads this worker's
mission + live pane for context, PHONES the user, talks it through, and relays the
user's decision back into this worker's session as a new directive (via orch
step.add). So the worker should finish its current turn after calling this tool;
the answer arrives shortly as a fresh directive.

Identity: the worker's mission_id comes from --mission-id (substituted by orch's
worker_mcp.json) or the ORCH_MISSION_ID env var. The worker never has to know it.

Run (orch injects this automatically via /etc/orch/worker_mcp.json):
    /usr/bin/python3 /root/jdawg/jdawg_mcp.py --mission-id <id>
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

REQUESTS_DIR = Path("/root/.jdawg/requests")
ORCH_SOCKET = os.environ.get("ORCH_SOCKET", "/root/.orch/orchd.sock")


def _orch_hold(mission_id: str, seconds: int) -> None:
    """Best-effort: tell the orch daemon to hold this mission open while we
    wait for the user's phoned-in answer. Silent on any failure."""
    import socket as _socket
    try:
        s = _socket.socket(_socket.AF_UNIX, _socket.SOCK_STREAM)
        s.settimeout(3)
        s.connect(ORCH_SOCKET)
        req = {"id": 1, "method": "mission.hold",
               "params": {"mission_id": mission_id, "seconds": seconds,
                          "reason": "talk_to_user awaiting phone answer"}}
        s.sendall((json.dumps(req) + "\n").encode())
        s.recv(4096)
        s.close()
    except Exception:
        pass

SERVER_INSTRUCTIONS = """\
You have a direct line to the user by PHONE through their Chief of Staff (an AI
voice assistant that calls the user for you).

WHEN TO CALL (`talk_to_user`) - two cases:

1. DECISION / BLOCKER you genuinely can't resolve yourself: a real decision, a
   go/no-go, an ambiguity that materially changes the outcome, or a blocker only
   the human can clear.

2. URGENT ESCALATION - something needs the user's attention NOW and a Telegram
   text won't get it in time. Use the phone when:
     • a deadline is imminent and inaction has real cost,
     • a time-critical event needs a yes/no before it's too late, or
     • you already sent a `notify` about something important and the user has
       NOT responded, yet you still need them.
   A phone rings; a Telegram message can sit unread. Escalate to a call when
   silence-on-Telegram is itself the problem.

Do NOT call for routine progress updates - that's what `notify` (Telegram) is
for. The bar is: "this genuinely needs a human, now."

ESCALATION LADDER: notify (async text) → if it's urgent or unanswered and still
matters → talk_to_user (live call). Prefer notify first for anything that isn't
already time-critical; jump straight to a call when time is the constraint.

When you call it, the Chief of Staff phones the user, explains the situation on your behalf
(you do not need to draft the call - just give a clear summary and the specific
question), resolves it in conversation, and sends the user's decision back to you
as a NEW directive in this session shortly after. So: call the tool, then WRAP UP
your current turn and stop. Act on the user's decision when it arrives.
"""


def _tool() -> Tool:
    return Tool(
        name="talk_to_user",
        description=(
            "Phone the user (via their Chief of Staff voice assistant) to resolve something you "
            "genuinely need a human decision on. NON-BLOCKING: this returns "
            "immediately - the Chief of Staff makes the call and delivers the user's decision "
            "back to you as a new directive in this session shortly. After calling "
            "this, finish your turn and wait for that follow-up directive.\n\n"
            "Use for: (1) a real decision/blocker only the human can resolve, OR "
            "(2) URGENT escalation - a deadline is imminent, a time-critical yes/no "
            "is needed, or you already sent a notify about something important and "
            "the user hasn't responded but you still need them. A phone rings; a "
            "Telegram text can sit unread - escalate to a call when getting their "
            "attention in time is the problem. Not for routine updates (use notify).\n\n"
            "Args:\n"
            "  summary (required): plain, self-contained explanation of the "
            "situation and why you need the user - written so a person with no "
            "prior context understands it. The Chief of Staff will also read your live screen, "
            "but lead with a clear summary.\n"
            "  question (optional): the specific decision/answer you need.\n\n"
            "Returns: {ok, request_id, status} - confirmation the user is being called."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "summary": {
                    "type": "string",
                    "description": "Self-contained explanation of the situation + why you need the user.",
                },
                "question": {
                    "type": "string",
                    "description": "The specific decision or answer you need from the user.",
                },
            },
            "required": ["summary"],
            "additionalProperties": False,
        },
    )


def build_server(mission_id: str) -> Server:
    srv: Server = Server(
        name=f"jdawg-talk-{mission_id}", instructions=SERVER_INSTRUCTIONS
    )

    @srv.list_tools()
    async def _list() -> list[Tool]:
        return [_tool()]

    @srv.call_tool()
    async def _call(name: str, args: dict[str, Any]) -> list[TextContent]:
        if name != "talk_to_user":
            return [TextContent(type="text", text=json.dumps({"error": f"unknown tool {name}"}))]
        request_id = uuid.uuid4().hex[:12]
        req = {
            "request_id": request_id,
            "mission_id": mission_id,
            "summary": args.get("summary", ""),
            "question": args.get("question", ""),
            "ts": int(time.time()),
        }
        REQUESTS_DIR.mkdir(parents=True, exist_ok=True)
        # write atomically so the watcher never reads a half-written file
        tmp = REQUESTS_DIR / f".{request_id}.json.tmp"
        final = REQUESTS_DIR / f"{request_id}.json"
        tmp.write_text(json.dumps(req, indent=2))
        tmp.rename(final)
        # Hold the orch mission open so it does not auto-complete while we wait
        # for the user's answer (which arrives later as a new step). Best-effort.
        _orch_hold(mission_id, 3600)
        out = {
            "ok": True,
            "request_id": request_id,
            "status": (
                "The user is being called now. Their decision will arrive as a new "
                "directive in this session shortly - finish your current turn and "
                "wait for it."
            ),
        }
        return [TextContent(type="text", text=json.dumps(out))]

    return srv


async def _serve(srv: Server) -> None:
    async with stdio_server() as (r, w):
        await srv.run(r, w, srv.create_initialization_options())


def main() -> None:
    ap = argparse.ArgumentParser(prog="jdawg-mcp")
    ap.add_argument("--mission-id", default=None)
    a = ap.parse_args()
    mission_id = a.mission_id or os.environ.get("ORCH_MISSION_ID")
    if not mission_id:
        print("jdawg-mcp requires --mission-id or ORCH_MISSION_ID", file=sys.stderr)
        sys.exit(2)
    asyncio.run(_serve(build_server(mission_id)))


if __name__ == "__main__":
    main()
