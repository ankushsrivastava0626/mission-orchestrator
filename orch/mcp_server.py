"""stdio MCP server. Two modes: host (full surface) and worker (scoped)."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from typing import Any

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

from . import client, config


# ---------- shared helpers ----------


def _text(payload: Any) -> list[TextContent]:
    if isinstance(payload, str):
        return [TextContent(type="text", text=payload)]
    return [TextContent(type="text", text=json.dumps(payload, indent=2, default=str))]


def _call(method: str, params: dict[str, Any]) -> Any:
    with client.DaemonClient() as c:
        return c.call(method, params)


# ---------- tool schemas ----------


_CUE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "description": "Entry condition for a step",
    "properties": {
        "type": {
            "type": "string",
            "enum": [
                "immediate",
                "on_prev_complete",
                "on_prev_complete_or_timeout",
                "on_timeout",
            ],
        },
        "seconds": {"type": "integer", "minimum": 1},
    },
    "required": ["type"],
}

_MODE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "description": "Status-ping firing mode",
    "properties": {
        "type": {"type": "string", "enum": ["on_step_complete", "on_schedule"]},
        "seconds": {"type": "integer", "minimum": 1},
    },
    "required": ["type"],
}


def _obj(props: dict[str, Any], required: list[str]) -> dict[str, Any]:
    return {
        "type": "object",
        "properties": props,
        "required": required,
        "additionalProperties": False,
    }


HOST_INSTRUCTIONS = """\
Mission Orchestrator - HOST surface.

You delegate long-running, stateful work to a separate WORKER Claude running in
a tmux session on this machine. The worker keeps full conversational context
across every directive (via `claude --resume <mission_id>`). The user only ever
sees Telegram messages composed by the worker via its `notify` tool - you do
not send Telegram messages directly.

============================  CORE CONCEPTS  ============================

Mission
  One long-lived worker Claude session in one tmux. mission_id IS the Claude
  session UUID. Created by mission.create, lives until cancelled/completed/
  failed, fully removed only by mission.delete.

Step
  A directive (prompt string) sent to the worker, with an entry condition (a
  Cue). Steps are LINEAR - they execute in `position` order, one at a time.

Cue (entry condition for a step)
  {"type": "immediate"}                                 - fire as soon as queued (first step only)
  {"type": "on_prev_complete"}                          - wait until previous step finishes
  {"type": "on_prev_complete_or_timeout", "seconds": N} - whichever happens first
  {"type": "on_timeout", "seconds": N}                  - fire N seconds after previous step started, regardless

Heartbeat
  Exactly one per mission. MANDATORY. Default 24h (86400s), max 24h. Cannot
  be deleted. When the timer fires, the worker is prompted to compose a
  free-form status update and send it via `notify`. Use heartbeat.set to
  change the interval; heartbeat.get to read it.

Status Ping (0..N per mission)
  Host-defined recurring or post-step nudge:
    mode = {"type": "on_step_complete"}        - fires after each completed step
    mode = {"type": "on_schedule", "seconds": N} - fires on a wall-clock cadence
  When firing, the worker receives your `command` text plus an instruction to
  post the result to Telegram via `notify`.

Secrets & Cookies
  Per-mission encrypted vault. Use secret.put / cookies.put to store. The
  worker fetches values via the `msec` CLI inside its tmux - values NEVER
  travel through MCP responses (secret.put returns just {ok, name}).

==========================  TYPICAL WORKFLOW  ==========================

  1. mission_id = mission.create({name})
  2. (optional) secret.put / cookies.put for any credentials the worker needs
  3. step.add({mission_id, directive, cue: {type: "immediate"}})  ← first step
  4. step.add(...) more steps with cue: {type: "on_prev_complete"}
  5. mission.get(mission_id) to monitor progress
  6. Auto-completion fires ONLY when ALL of these hold:
       - no pending or running steps
       - no pings configured on the mission (pings keep the mission alive)
       - the worker Claude is idle in tmux
     When triggered, the engine injects a wrap-up directive asking the worker
     to summarize via `notify`, then tears down the tmux and purges the vault.
     State becomes "completed".
     If you've configured pings, the mission will stay in 'running' state
     indefinitely - call ping.delete to remove them (then auto-complete kicks
     in on the next tick), or mission.cancel to end the mission explicitly.
  7. mission.delete(mission_id) to remove the row entirely (terminal states only)

=========================  WORKER CAPABILITIES  =========================

Inside the tmux, the worker has its own (scoped) MCP server with these tools:
  notify(text)                - send a Telegram message (the ONLY user-facing channel)
  queue.list/add/update/delete - recursively manage its own pending steps
  pings.list/add/update/delete - manage its own status pings
  mission.status              - read its own mission row
  secrets.list / cookies.list - names only (values via the `msec` CLI)

So the worker can extend its own plan, add follow-ups, or stop posting pings.
It cannot create other missions, change Telegram destinations, or silence its
own heartbeat.

============================  STATE MACHINES  ============================

Mission states: running → completed | cancelled | failed
Step states:    pending → running → completed | timed_out | cancelled | failed

Terminal mission states are required before mission.delete will succeed.

=============================  GOTCHAS  =============================

• The first step's cue must be {"type": "immediate"} - no previous step to wait on.
• step.update and step.delete only work on pending steps.
• heartbeat is read-only on the worker side; only host can change interval.
• cancel kills tmux immediately and purges the vault (no Telegram goodbye).
  If you want the worker to say goodbye, add a final step before cancelling.
• On daemon restart, missions with missing tmux are auto-recovered: tmux is
  recreated, the current step is re-launched with a "[Resumed]" prefix that
  asks the worker to notify the user about the interruption.
"""


HOST_TOOLS: list[Tool] = [
    Tool(
        name="defaults.get",
        description=(
            "Read daemon-wide defaults.\n\n"
            "Use this when you want to know what Telegram chat new missions will go to by default, "
            "or to check whether the daemon has Telegram configured at all.\n\n"
            "Args: none.\n\n"
            "Returns: {\n"
            "  default_chat_id: string | null,\n"
            "  heartbeat_default_s: int,    // 86400\n"
            "  heartbeat_max_s: int,        // 86400\n"
            "  telegram_configured: bool    // ORCH_TELEGRAM_BOT_TOKEN is set\n"
            "}\n\n"
            "Read-only. Defaults are sysadmin-managed via /etc/orchd.env."
        ),
        inputSchema=_obj({}, []),
    ),
    Tool(
        name="mission.create",
        description=(
            "Create a new mission. Spawns a tmux session `mission-<id>`, prepares the worker MCP "
            "config, and primes the worker Claude for `claude --session-id <id>` on the first step.\n\n"
            "Call this as step 1 of any delegated workflow. After creation, queue work via step.add.\n\n"
            "Args:\n"
            "  name (required): human-readable label, e.g. 'reddit-watch'.\n"
            "  telegram_chat_id (optional): Telegram chat to route worker notifications to. "
            "If omitted, falls back to the daemon's ORCH_DEFAULT_CHAT_ID. Fails if neither is set.\n"
            "  heartbeat_interval_s (optional, 1..86400, default 86400): how often the worker is "
            "nudged to send a status update via notify when it's idle.\n\n"
            "Example: {\"name\": \"reddit-watch\"}\n"
            "Example: {\"name\": \"scrape\", \"telegram_chat_id\": \"123456789\", \"heartbeat_interval_s\": 3600}\n\n"
            "Returns: {mission_id: \"<uuid>\"}\n\n"
            "The mission starts with no steps - call step.add to queue the first directive."
        ),
        inputSchema=_obj(
            {
                "name": {"type": "string", "description": "Human label for the mission."},
                "telegram_chat_id": {
                    "type": "string",
                    "description": "Telegram chat id. If omitted, the daemon's ORCH_DEFAULT_CHAT_ID is used.",
                },
                "heartbeat_interval_s": {
                    "type": "integer", "minimum": 1, "maximum": 86400,
                    "description": "Heartbeat cadence in seconds. Default 86400 (24h). Max 86400.",
                },
            },
            ["name"],
        ),
    ),
    Tool(
        name="mission.list",
        description=(
            "List all missions known to the daemon (running + terminal states).\n\n"
            "Use this to find a mission_id when you don't have it, or to survey state across "
            "missions before deciding what to do next.\n\n"
            "Args: none.\n\n"
            "Returns: array of mission rows. Each row contains: id, name, state, telegram_chat_id, "
            "heartbeat_interval_s, created_at (unix), finished_at (unix or null), restart_count, "
            "last_heartbeat_at, tmux_session.\n\n"
            "Sorted by creation time, newest first."
        ),
        inputSchema=_obj({}, []),
    ),
    Tool(
        name="mission.get",
        description=(
            "Get a single mission's full snapshot: the mission row, its steps in order, and its pings.\n\n"
            "Use this to monitor progress: each step has a state (pending/running/completed/...) and "
            "directive text. Useful before adding a follow-up step (to see what's already queued).\n\n"
            "Args: {mission_id (required)}.\n\n"
            "Returns: {\n"
            "  mission: {...},\n"
            "  steps: [{id, position, directive, state, cue_type, cue_payload, started_at, "
            "finished_at, created_by}, ...],\n"
            "  pings: [{id, command, mode_type, interval_s, last_fired_at, created_by}, ...]\n"
            "}"
        ),
        inputSchema=_obj({"mission_id": {"type": "string"}}, ["mission_id"]),
    ),
    Tool(
        name="mission.cancel",
        description=(
            "Cancel a running mission. Two modes:\n\n"
            "SOFT (default, force=false): the engine interrupts the current step (Ctrl-C), then "
            "injects a goodbye directive asking the worker to compose a 1-2 sentence Telegram "
            "farewell via `notify` describing where it got to. Mission state goes to 'cancelling'; "
            "once the goodbye delivers and the worker exits idle, the engine tears down tmux + "
            "vault and marks the mission 'cancelled'. Typical latency: 5-30 seconds.\n\n"
            "HARD (force=true): immediate teardown. Tmux killed, pending+running steps marked "
            "cancelled, vault purged. No Telegram goodbye. Use when the worker is stuck or you "
            "need to stop right now.\n\n"
            "Args: {mission_id (required), force (optional, default false)}.\n\n"
            "Returns: {ok: true, mode: 'soft'|'hard', goodbye_queued?: true} or {ok: true, "
            "already: 'completed'|'cancelled'|'failed'} if no-op.\n\n"
            "After terminal state, the mission row remains until you call mission.delete."
        ),
        inputSchema=_obj(
            {
                "mission_id": {"type": "string"},
                "force": {"type": "boolean", "description": "true = hard cancel (immediate, no goodbye). false (default) = soft cancel."},
            },
            ["mission_id"],
        ),
    ),
    Tool(
        name="mission.delete",
        description=(
            "Permanently delete a mission from the database. Only allowed when the mission is in a "
            "terminal state (completed, cancelled, or failed). If the mission is still running, call "
            "mission.cancel first.\n\n"
            "Cascades: deletes all steps and pings for the mission via foreign keys. Best-effort "
            "cleanup of tmux + vault + worker tmpdir. The events log entries are preserved (their "
            "mission_id is a free reference, not a foreign key), so you keep an audit trail.\n\n"
            "Args: {mission_id (required)}.\n\n"
            "Returns: {ok: true}.\n\n"
            "Errors:\n"
            "  not_terminal - mission is still running; call mission.cancel first.\n"
            "  not_found    - no mission with that id."
        ),
        inputSchema=_obj({"mission_id": {"type": "string"}}, ["mission_id"]),
    ),
    Tool(
        name="mission.events",
        description=(
            "Read the structured audit log for a mission. Returns timestamped events the daemon "
            "recorded: step launches, completions, timeouts, heartbeat fires, ping fires, "
            "notify_sent (when the worker actually called notify), restarts, cancellations, "
            "completions, errors.\n\n"
            "Use this to understand what actually happened on a mission without parsing the "
            "tmux pane. Especially useful for debugging missions where you suspect the worker "
            "didn't do what you asked.\n\n"
            "Args:\n"
            "  mission_id (required)\n"
            "  since (optional): unix timestamp; only return events with ts >= since.\n"
            "  limit (optional, default 100, max 1000): max events to return (most recent first).\n\n"
            "Returns: array of {id, ts (unix), kind, step_id (or null), ping_id (or null), "
            "payload (JSON object or null)}, ordered newest first.\n\n"
            "Common `kind` values: mission_created, step_added, step_launched, step_completed, "
            "step_timed_out, completion_directive_sent, oob_completion_wrap_up_launched, "
            "oob_heartbeat_launched, oob_ping_launched, notify_sent, heartbeat_fired, ping_fired, "
            "mission_resumed, mission_completed, mission_cancelled, mission_cancelling, "
            "mission_failed_max_restarts, secret_accessed, cookies_accessed."
        ),
        inputSchema=_obj(
            {
                "mission_id": {"type": "string"},
                "since": {"type": "integer", "description": "Unix timestamp lower bound. Default 0 (all)."},
                "limit": {"type": "integer", "minimum": 1, "maximum": 1000, "description": "Max rows. Default 100."},
            },
            ["mission_id"],
        ),
    ),
    Tool(
        name="mission.pane_snapshot",
        description=(
            "Capture the last N lines from the mission's tmux pane - a live look at what the "
            "worker Claude is currently displaying.\n\n"
            "Use this when you want to see what the worker is doing in real time, including its "
            "in-progress responses, tool calls, etc. Cheap and read-only.\n\n"
            "Args:\n"
            "  mission_id (required)\n"
            "  lines (optional, default 80, max 2000): how many lines to grab from the bottom.\n\n"
            "Returns: {\n"
            "  pane_content: string,        // raw pane text\n"
            "  alive: boolean,              // is the tmux session present\n"
            "  claude_running: boolean      // is claude the foreground process right now\n"
            "}\n\n"
            "If `alive` is false, the mission's tmux has been torn down (cancelled/completed)."
        ),
        inputSchema=_obj(
            {
                "mission_id": {"type": "string"},
                "lines": {"type": "integer", "minimum": 1, "maximum": 2000, "description": "Default 80."},
            },
            ["mission_id"],
        ),
    ),
    Tool(
        name="mission.attach_info",
        description=(
            "Return the shell command a human can run on the daemon machine to attach to the "
            "mission's tmux pane and watch the worker live.\n\n"
            "Use this when you want to give the user a debug handle - they can SSH into the daemon "
            "host and paste the command to see exactly what the worker Claude is doing.\n\n"
            "Args: {mission_id (required)}.\n\n"
            "Returns: {tmux_cmd: \"tmux attach -t mission-<id>\"}.\n\n"
            "Note: the host (you) cannot read the pane content via MCP today. This is a hint for the "
            "human user, not a remote-introspection tool."
        ),
        inputSchema=_obj({"mission_id": {"type": "string"}}, ["mission_id"]),
    ),
    Tool(
        name="step.add",
        description=(
            "Append a directive (prompt) to a mission's step queue. Steps execute in `position` "
            "order, one at a time. The worker reads each directive as a new user message in its "
            "ongoing Claude session - so full context from prior steps carries over.\n\n"
            "Use this to queue work. The first step of a mission must have cue {\"type\": "
            "\"immediate\"} (there's no previous step to wait on). Subsequent steps typically use "
            "{\"type\": \"on_prev_complete\"}.\n\n"
            "Args:\n"
            "  mission_id (required)\n"
            "  directive (required): the prompt text the worker Claude will receive. Write it as you "
            "would write any instruction to Claude - it can include multi-paragraph guidance, code, "
            "data, etc. The worker has full tool access (Bash, Read, Write, web tools, plus its own "
            "MCP - notify, queue.add, etc.).\n"
            "  cue (required): entry condition (see Cue types in server instructions).\n"
            "  position (optional): explicit queue position. Default: append at the end.\n\n"
            "Example first step:\n"
            "  {mission_id, directive: \"Scrape r/python for the top 10 posts today and save to "
            "/tmp/posts.json\", cue: {type: \"immediate\"}}\n\n"
            "Example follow-up with timeout:\n"
            "  {mission_id, directive: \"Summarize /tmp/posts.json and notify the user.\", "
            "cue: {type: \"on_prev_complete_or_timeout\", seconds: 300}}\n\n"
            "Returns: {step_id: \"<uuid>\"}\n\n"
            "Tip: end your directive with explicit instructions about Telegram if you want a user-facing "
            "update - e.g. \"...then post a summary to the user via the notify tool.\""
        ),
        inputSchema=_obj(
            {
                "mission_id": {"type": "string"},
                "directive": {"type": "string", "description": "Prompt text the worker Claude will receive."},
                "cue": _CUE_SCHEMA,
                "position": {"type": "integer", "minimum": 0, "description": "Optional explicit queue position. Default: tail."},
            },
            ["mission_id", "directive", "cue"],
        ),
    ),
    Tool(
        name="step.list",
        description=(
            "List a mission's steps in position order. Convenience for when you only want the steps "
            "and not the full mission.get payload.\n\n"
            "Args: {mission_id (required)}.\n\n"
            "Returns: array of step rows (same shape as mission.get's `steps`)."
        ),
        inputSchema=_obj({"mission_id": {"type": "string"}}, ["mission_id"]),
    ),
    Tool(
        name="step.update",
        description=(
            "Modify a step that has not started yet. Only `directive` and/or `cue` can be changed, "
            "and only on steps in state 'pending'. Use this to fix a typo or change a wait condition "
            "before the step launches.\n\n"
            "Args: {step_id (required), directive?, cue?}\n\n"
            "Returns: {ok: true}\n\n"
            "Errors: not_pending - the step is already running or terminal."
        ),
        inputSchema=_obj(
            {
                "step_id": {"type": "string"},
                "directive": {"type": "string"},
                "cue": _CUE_SCHEMA,
            },
            ["step_id"],
        ),
    ),
    Tool(
        name="step.delete",
        description=(
            "Delete a pending step from the queue. Only works on steps in state 'pending'. To stop a "
            "currently-running step, use step.cancel_current instead.\n\n"
            "Args: {step_id (required)}\n\n"
            "Returns: {ok: true}"
        ),
        inputSchema=_obj({"step_id": {"type": "string"}}, ["step_id"]),
    ),
    Tool(
        name="step.cancel_current",
        description=(
            "Interrupt the step that is currently running. Sends Ctrl-C to the tmux pane so the "
            "worker Claude is killed mid-inference. The step is marked 'cancelled'; the engine then "
            "advances to the next pending step (if any) on the next tick.\n\n"
            "Use this when a step is stuck or you've decided the work is no longer needed.\n\n"
            "Args: {mission_id (required)}\n\n"
            "Returns: {ok: true}"
        ),
        inputSchema=_obj({"mission_id": {"type": "string"}}, ["mission_id"]),
    ),
    Tool(
        name="ping.add",
        description=(
            "Add a recurring status ping. When the ping fires, the worker is sent the directive: "
            "`<command>\\n\\nWhen done, post the result via the notify tool.` Worker runs it and "
            "(if it complies) posts to Telegram.\n\n"
            "Use this when the user wants periodic updates without you having to queue steps. Two "
            "modes:\n"
            "  on_step_complete - fires once after every completed step\n"
            "  on_schedule      - fires on a wall-clock cadence; needs `seconds`\n\n"
            "Args:\n"
            "  mission_id (required)\n"
            "  command (required): the text directive sent to the worker.\n"
            "  mode (required): one of the two shapes above.\n\n"
            "Example periodic uptime check:\n"
            "  {mission_id, command: \"Run `uptime` and tell me the load average.\", "
            "mode: {type: \"on_schedule\", seconds: 1800}}\n\n"
            "Returns: {ping_id: \"<uuid>\"}"
        ),
        inputSchema=_obj(
            {
                "mission_id": {"type": "string"},
                "command": {"type": "string", "description": "Directive the worker runs when the ping fires."},
                "mode": _MODE_SCHEMA,
            },
            ["mission_id", "command", "mode"],
        ),
    ),
    Tool(
        name="ping.list",
        description=(
            "List all pings configured for a mission.\n\n"
            "Args: {mission_id (required)}.\n\n"
            "Returns: array of ping rows: {id, command, mode_type, interval_s (null if on_step_complete), "
            "last_fired_at, created_by}."
        ),
        inputSchema=_obj({"mission_id": {"type": "string"}}, ["mission_id"]),
    ),
    Tool(
        name="ping.update",
        description=(
            "Change a ping's command and/or mode.\n\n"
            "Args: {ping_id (required), command?, mode?}\n\n"
            "Returns: {ok: true}"
        ),
        inputSchema=_obj(
            {
                "ping_id": {"type": "string"},
                "command": {"type": "string"},
                "mode": _MODE_SCHEMA,
            },
            ["ping_id"],
        ),
    ),
    Tool(
        name="ping.delete",
        description=(
            "Remove a ping from a mission. It will no longer fire.\n\n"
            "Args: {ping_id (required)}\n\n"
            "Returns: {ok: true}"
        ),
        inputSchema=_obj({"ping_id": {"type": "string"}}, ["ping_id"]),
    ),
    Tool(
        name="heartbeat.set",
        description=(
            "Change a mission's heartbeat interval. The heartbeat is mandatory (every mission has "
            "exactly one) and cannot be deleted - only adjusted. Max 86400s (24h).\n\n"
            "Args: {mission_id (required), interval_s (required, 1..86400)}.\n\n"
            "Returns: {ok: true}\n\n"
            "Tip: shorten this to 60s during testing to verify your notify wiring quickly."
        ),
        inputSchema=_obj(
            {
                "mission_id": {"type": "string"},
                "interval_s": {"type": "integer", "minimum": 1, "maximum": 86400},
            },
            ["mission_id", "interval_s"],
        ),
    ),
    Tool(
        name="heartbeat.get",
        description=(
            "Read a mission's heartbeat configuration.\n\n"
            "Args: {mission_id (required)}.\n\n"
            "Returns: {interval_s: int, last_heartbeat_at: int}"
        ),
        inputSchema=_obj({"mission_id": {"type": "string"}}, ["mission_id"]),
    ),
    Tool(
        name="secret.put",
        description=(
            "Store a password (or any short secret string) in the mission's encrypted vault, under "
            "the name you choose. The worker fetches it with `msec get <name>` inside its tmux - "
            "values NEVER flow back through MCP responses or transcripts after this call.\n\n"
            "The value lives in this call's transcript exactly once; after that it's in the "
            "`pass`-encrypted store on the daemon machine.\n\n"
            "Use this BEFORE the first step.add that needs the secret, so the worker can read it "
            "in its directive.\n\n"
            "Args: {mission_id (required), name (required), value (required)}\n\n"
            "Example: {mission_id, name: \"github_token\", value: \"ghp_...\"}\n\n"
            "In the worker's directive you might then say:\n"
            "  \"Use the GitHub token from `msec get github_token` to call the API.\"\n\n"
            "Returns: {ok: true, name} - the value is NOT echoed."
        ),
        inputSchema=_obj(
            {
                "mission_id": {"type": "string"},
                "name": {"type": "string", "description": "The label the worker will use to retrieve it (msec get <name>)."},
                "value": {"type": "string", "description": "The secret. Never echoed back."},
            },
            ["mission_id", "name", "value"],
        ),
    ),
    Tool(
        name="secret.list",
        description=(
            "List the secret names stored for a mission. Values are NEVER returned - use this only "
            "to see what's available.\n\n"
            "Args: {mission_id (required)}\n\n"
            "Returns: array of strings (the names)."
        ),
        inputSchema=_obj({"mission_id": {"type": "string"}}, ["mission_id"]),
    ),
    Tool(
        name="secret.delete",
        description=(
            "Delete a single secret from a mission's vault.\n\n"
            "Args: {mission_id (required), name (required)}\n\n"
            "Returns: {ok: true}"
        ),
        inputSchema=_obj(
            {"mission_id": {"type": "string"}, "name": {"type": "string"}},
            ["mission_id", "name"],
        ),
    ),
    Tool(
        name="cookies.put",
        description=(
            "Store a cookie blob (multiline text - Netscape jar or JSON) in the mission's vault. "
            "Same encryption as secrets, separate namespace (mission-<id>/cookies/<name>).\n\n"
            "The worker materializes it to a tmpfs file with `msec cookies <name>` and feeds the "
            "path to whatever needs it (Playwright, curl --cookie, etc.). The tmpfs file is cleaned "
            "up when the mission ends.\n\n"
            "Args: {mission_id (required), name (required), content (required)}\n\n"
            "Example: {mission_id, name: \"reddit\", content: \"# Netscape HTTP Cookie File\\n.reddit.com\\tTRUE\\t/\\tTRUE\\t1893456000\\tsession\\tabc123\\n\"}\n\n"
            "Returns: {ok: true, name}"
        ),
        inputSchema=_obj(
            {
                "mission_id": {"type": "string"},
                "name": {"type": "string"},
                "content": {"type": "string", "description": "Multi-line cookie jar content."},
            },
            ["mission_id", "name", "content"],
        ),
    ),
    Tool(
        name="cookies.delete",
        description=(
            "Delete a cookie entry from a mission's vault.\n\n"
            "Args: {mission_id (required), name (required)}\n\n"
            "Returns: {ok: true}"
        ),
        inputSchema=_obj(
            {"mission_id": {"type": "string"}, "name": {"type": "string"}},
            ["mission_id", "name"],
        ),
    ),
]


def build_host_server() -> Server:
    srv: Server = Server(
        name="mission-orchestrator-host",
        instructions=HOST_INSTRUCTIONS,
    )

    @srv.list_tools()
    async def _list_tools() -> list[Tool]:
        return HOST_TOOLS

    @srv.call_tool()
    async def _call_tool(name: str, args: dict[str, Any]) -> list[TextContent]:
        # value-omission for secret.put response
        if name == "secret.put":
            params = {k: v for k, v in args.items()}
            result = _call("secret.put", {**params, "caller": "host"})
            return _text({"ok": True, "name": args.get("name")})
        params = {**args, "caller": "host"}
        result = _call(name, params)
        return _text(result if result is not None else {"ok": True})

    return srv


# ---------- worker mode ----------


def _worker_tools() -> list[Tool]:
    return [
        Tool(
            name="notify",
            description=(
                "Send a Telegram message to the user. Use this to report progress, "
                "results, warnings, errors, or status updates. You decide the content "
                "- the user only sees Telegram messages YOU compose via this tool. "
                "Keep messages concise and include relevant context."
            ),
            inputSchema=_obj({"text": {"type": "string"}}, ["text"]),
        ),
        Tool(
            name="queue.list",
            description="List the mission's steps (current and pending).",
            inputSchema=_obj({}, []),
        ),
        Tool(
            name="queue.add",
            description="Append a step to this mission's queue.",
            inputSchema=_obj(
                {
                    "directive": {"type": "string"},
                    "cue": _CUE_SCHEMA,
                    "position": {"type": "integer", "minimum": 0},
                },
                ["directive", "cue"],
            ),
        ),
        Tool(
            name="queue.update",
            description="Update a pending step's directive and/or cue.",
            inputSchema=_obj(
                {
                    "step_id": {"type": "string"},
                    "directive": {"type": "string"},
                    "cue": _CUE_SCHEMA,
                },
                ["step_id"],
            ),
        ),
        Tool(
            name="queue.delete",
            description="Delete a pending step.",
            inputSchema=_obj({"step_id": {"type": "string"}}, ["step_id"]),
        ),
        Tool(
            name="pings.list",
            description="List this mission's pings.",
            inputSchema=_obj({}, []),
        ),
        Tool(
            name="pings.add",
            description="Add a status ping.",
            inputSchema=_obj(
                {"command": {"type": "string"}, "mode": _MODE_SCHEMA},
                ["command", "mode"],
            ),
        ),
        Tool(
            name="pings.update",
            description="Update a ping.",
            inputSchema=_obj(
                {
                    "ping_id": {"type": "string"},
                    "command": {"type": "string"},
                    "mode": _MODE_SCHEMA,
                },
                ["ping_id"],
            ),
        ),
        Tool(
            name="pings.delete",
            description="Delete a ping.",
            inputSchema=_obj({"ping_id": {"type": "string"}}, ["ping_id"]),
        ),
        Tool(
            name="heartbeat.get",
            description="Read-only: get this mission's heartbeat configuration.",
            inputSchema=_obj({}, []),
        ),
        Tool(
            name="mission.status",
            description="Get this mission's status snapshot.",
            inputSchema=_obj({}, []),
        ),
        Tool(
            name="secrets.list",
            description="List secret names (values via the msec CLI).",
            inputSchema=_obj({}, []),
        ),
        Tool(
            name="cookies.list",
            description="List cookies names (values via the msec CLI).",
            inputSchema=_obj({}, []),
        ),
    ]


def build_worker_server(mission_id: str) -> Server:
    srv: Server = Server(f"mission-orchestrator-worker-{mission_id}")
    tools = _worker_tools()

    @srv.list_tools()
    async def _list_tools() -> list[Tool]:
        return tools

    @srv.call_tool()
    async def _call_tool(name: str, args: dict[str, Any]) -> list[TextContent]:
        a = {**args, "mission_id": mission_id, "caller": "worker"}
        if name == "notify":
            return _text(_call("notify", {"mission_id": mission_id, "text": args.get("text", "")}))
        if name == "queue.list":
            return _text(_call("step.list", {"mission_id": mission_id}))
        if name == "queue.add":
            return _text(_call("step.add", {**a, "created_by": "worker"}))
        if name == "queue.update":
            return _text(_call("step.update", a))
        if name == "queue.delete":
            return _text(_call("step.delete", a))
        if name == "pings.list":
            return _text(_call("ping.list", {"mission_id": mission_id}))
        if name == "pings.add":
            return _text(_call("ping.add", {**a, "created_by": "worker"}))
        if name == "pings.update":
            return _text(_call("ping.update", a))
        if name == "pings.delete":
            return _text(_call("ping.delete", a))
        if name == "heartbeat.get":
            return _text(_call("heartbeat.get", {"mission_id": mission_id}))
        if name == "mission.status":
            return _text(_call("mission.get", {"mission_id": mission_id}))
        if name == "secrets.list":
            return _text(_call("secret.list", {"mission_id": mission_id}))
        if name == "cookies.list":
            return _text(_call("cookies.list", {"mission_id": mission_id}))
        return _text({"error": f"unknown worker tool: {name}"})

    return srv


# ---------- entry point ----------


async def _serve(srv: Server) -> None:
    async with stdio_server() as (read_stream, write_stream):
        await srv.run(read_stream, write_stream, srv.create_initialization_options())


def main() -> None:
    ap = argparse.ArgumentParser(prog="orch-mcp")
    ap.add_argument("--mode", choices=["host", "worker"], required=True)
    ap.add_argument("--mission-id", default=None)
    args = ap.parse_args()

    if args.mode == "worker":
        mission_id = args.mission_id or os.environ.get(config.ENV_MISSION_ID)
        if not mission_id:
            print("worker mode requires --mission-id or ORCH_MISSION_ID", file=sys.stderr)
            sys.exit(2)
        srv = build_worker_server(mission_id)
    else:
        srv = build_host_server()

    asyncio.run(_serve(srv))


if __name__ == "__main__":
    main()
