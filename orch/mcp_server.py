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
    "description": (
        "Entry condition for a step. Types:\n"
        "  immediate                       - fire now (first step only)\n"
        "  on_current_complete  - run NEXT, as soon as the currently running step finishes. "
        "Jumps ahead of any other pending steps (use this to queue follow-up work that should run "
        "immediately after the current step, even if a long queue is already waiting).\n"
        "  on_timeout           - `seconds` after the current step started, regardless of completion\n"
        "  at_time              - at an absolute wall-clock time; give `at` "
        "(local datetime 'YYYY-MM-DD HH:MM', machine timezone) or `epoch` (unix seconds). "
        "Fires once that time passes and the worker is idle.\n"
        "(Legacy: on_prev_complete still works - it chains to the TAIL instead of jumping ahead. "
        "Prefer on_current_complete.)"
    ),
    "properties": {
        "type": {
            "type": "string",
            "enum": [
                "immediate",
                "on_current_complete",
                "on_timeout",
                "at_time",
            ],
        },
        "seconds": {"type": "integer", "minimum": 1, "description": "For on_timeout."},
        "at": {"type": "string", "description": "For at_time: local datetime 'YYYY-MM-DD HH:MM'."},
        "epoch": {"type": "integer", "description": "For at_time: absolute unix seconds (alternative to `at`)."},
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
  {"type": "immediate"}                                    - fire as soon as queued (first step only)
  {"type": "on_current_complete"}                          - run NEXT, when the current step finishes;
                                                             jumps ahead of other pending steps
  {"type": "on_timeout", "seconds": N}                     - fire N seconds after the current step started
  {"type": "at_time", "at": "YYYY-MM-DD HH:MM"}         - fire at an absolute wall-clock time (machine tz);
                                                          or {"type": "at_time", "epoch": <unix seconds>}.
                                                          Past times fire as soon as the worker is idle.

Heartbeat
  Exactly one per mission. MANDATORY. Default 24h (86400s), max 24h. Cannot
  be deleted. When the timer fires, the worker is prompted to compose a
  free-form status update and send it via `notify`. Use heartbeat.set to
  change the interval; heartbeat.get to read it.

Ping (0..N per mission) - autonomous watcher
  ping.add(condition, action, timeout_s) does NOT poll using Claude turns.
  Instead the worker writes + tests a standalone script ONCE; that script polls
  the condition on its own (zero tokens) and only wakes the worker when the
  condition fires (or when the watchdog sees the script die for timeout_s).
  Use for any repeated check: a file appears, disk fills, an API has new data,
  a process dies, a build finishes. Track via mission.events:
    scripted_ping_added → scripted_ping_ready → scripted_ping_fired.
  (Pings keep a mission alive - it won't auto-complete while any ping exists.)

Secrets & Cookies
  Per-mission encrypted vault. Use secret.put / cookies.put to store. The
  worker fetches values via the `msec` CLI inside its tmux - values NEVER
  travel through MCP responses (secret.put returns just {ok, name}).

==========================  TYPICAL WORKFLOW  ==========================

  1. mission_id = mission.create({name})
  2. (optional) secret.put / cookies.put for any credentials the worker needs
  3. step.add({mission_id, directive, cue: {type: "immediate"}})  ← first step
  4. step.add(...) more steps with cue: {type: "on_current_complete"}
  5. mission.get(mission_id) to monitor progress
  6. Auto-completion fires ONLY when ALL of these hold:
       - no pending or running steps
       - no pings configured on the mission (pings keep the mission alive)
       - the worker Claude is idle in tmux
     When triggered, the engine injects a wrap-up directive asking the worker
     to summarize via `notify`, then tears down the tmux. State becomes
     "completed". The vault (secrets + cookies) IS PRESERVED so the mission
     can be reopened later. The final pane content is archived as an event,
     accessible via mission.pane_snapshot.
     If you've configured pings, the mission will stay in 'running' state
     indefinitely - call ping.delete to remove them (then auto-complete kicks
     in on the next tick), or mission.cancel to end the mission explicitly.
  7. REOPENING: call step.add on a 'completed' mission and it auto-transitions
     back to 'running'. The Claude session is restored via --resume (full
     prior context), tmux is recreated, the vault is still there. New step's
     cue cannot be 'immediate' (use on_current_complete instead - position > 0).
  8. mission.delete(mission_id) to remove the row entirely (terminal states
     only). This is the ONLY operation that purges the vault.

=========================  WORKER CAPABILITIES  =========================

Inside the tmux, the worker has its own (scoped) MCP server with these tools:
  notify(text)                - message the human USER over Telegram (user-facing)
  talk_to_user(summary, q?)   - PHONE the user (via the J-dawg voice agent) for a
                                genuine decision/blocker. Non-blocking: the worker
                                finishes its turn; the user's spoken decision comes
                                back as a NEW step (step.add) in the same mission.
  message_host(text, files?)  - message YOU (the host) up the mailbox; can attach
                                files. You read these via host.inbox / host.fetch_file
                                / host.ack. Workers use this to escalate, ask you to
                                spawn follow-up missions, or hand back results+files.
  queue.list/add/update/delete - recursively manage its own pending steps
  pings.list/add/delete       - manage its own watcher pings (scripted)
  mission.status              - read its own mission row
  secrets.list / cookies.list - names only (values via the `msec` CLI)

Plus full Claude Code tooling (Bash, Read/Write, web) and any extra MCP servers
configured for all workers (e.g. Playwright for browser automation), run with
--dangerously-skip-permissions.

So the worker can extend its own plan, add follow-ups, stop posting pings, retime
its own heartbeat (heartbeat.set, but not disable it), browse the web, phone the
user, and push messages/files up to you. It cannot create other missions or
change the Telegram destination.

Three upward channels - keep them straight (the worker picks by audience+urgency):
  notify       → the human user, async text (Telegram). Routine updates/results.
  talk_to_user → the human user, live PHONE call. For real decisions/blockers AND
                 as an URGENT-ESCALATION path: imminent deadline, time-critical
                 yes/no, or a notify went unanswered and still needs the user. A
                 phone rings; a Telegram text can sit unread. The answer returns as
                 a new step automatically.
  message_host → you, the orchestrator (mailbox). Coordination/escalation between
                 worker and host. Poll host.inbox to receive these.
Escalation ladder for the user: notify (async) → talk_to_user (call) when it's
time-critical or the text went unanswered.
(The extra worker MCPs like Playwright and talk_to_user are configured in
/etc/orch/worker_mcp.json and may change; this list reflects the current set.)

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
        name="agent.get",
        description=(
            "Which AI backend runs the workers (claude / codex / gemini / api / "
            "custom), the last backend known to work, and each backend's "
            "availability on this machine.\n\nArgs: {}."
        ),
        inputSchema=_obj({}, []),
    ),
    Tool(
        name="agent.set",
        description=(
            "Switch the worker AI backend live (e.g. claude -> gemini). "
            "Persists across restarts; each running mission migrates on its "
            "next wake - fresh session on the new agent seeded with a handoff "
            "summary of its old one. If the new backend turns out broken, orch "
            "auto-falls back to the last one that worked.\n\n"
            "Args: {agent (required), force? (skip availability check)}."
        ),
        inputSchema=_obj(
            {"agent": {"type": "string",
                       "enum": ["claude", "codex", "antigravity", "gemini", "api", "custom"]},
             "force": {"type": "boolean"}},
            ["agent"],
        ),
    ),
    Tool(
        name="host.inbox",
        description=(
            "Read messages workers have sent up to you (the host) via their message_host tool. "
            "This is a PULL mailbox - workers can't interrupt you, so call this to see what they've "
            "said (escalations, questions, structured status, file deliveries). Poll it on your own "
            "cadence (e.g. each loop, or when the user asks you to check).\n\n"
            "Args: {include_acked? (default false), limit? (default 50)}.\n\n"
            "Returns: array of {message_id, mission_id, ts, text, acked, files:[{file_id, name, "
            "size}]}. Fetch a file's bytes with host.fetch_file(file_id). Mark handled with "
            "host.ack(message_id)."
        ),
        inputSchema=_obj(
            {
                "include_acked": {"type": "boolean"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 200},
            },
            [],
        ),
    ),
    Tool(
        name="host.ack",
        description=(
            "Mark a worker→host message as handled so it stops showing in host.inbox. Also frees "
            "its attached files.\n\nArgs: {message_id (required)}.\nReturns: {ok: true}."
        ),
        inputSchema=_obj({"message_id": {"type": "string"}}, ["message_id"]),
    ),
    Tool(
        name="host.fetch_file",
        description=(
            "Fetch the bytes of a file a worker attached to a host message, base64-encoded (works "
            "across SSH). Decode and save it on your side.\n\n"
            "Args: {file_id (required)} - from host.inbox files[].file_id, format '<message_id>:<index>'.\n\n"
            "Returns: {name, size, base64}. Files over 25MB are rejected (fetch them another way)."
        ),
        inputSchema=_obj({"file_id": {"type": "string"}}, ["file_id"]),
    ),
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
            "terminal state (completed, cancelled, or failed). If still running, call mission.cancel "
            "first.\n\n"
            "This is also the ONLY operation that purges the mission's pass vault (secrets + "
            "cookies). Completion and cancellation no longer purge it - they leave the vault intact "
            "so the mission can be REOPENED via step.add and credentials are still available.\n\n"
            "Cascades: removes all steps and pings via foreign keys. The events log entries are "
            "preserved (no FK), so the audit trail survives the delete.\n\n"
            "Args: {mission_id (required)}.\n\n"
            "Returns: {ok: true}.\n\n"
            "Errors:\n"
            "  not_terminal - mission is still running; cancel first.\n"
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
            "Capture the worker's tmux pane content. Works on running AND terminal-state missions:\n"
            "  - alive tmux → live capture (`source: \"live\"`)\n"
            "  - torn-down tmux → the final pane snapshot captured at teardown is returned "
            "(`source: \"archived\"`)\n"
            "  - no archive present → empty content (`source: \"none\"`)\n\n"
            "Use this to see what the worker is doing in real time, OR to inspect what it was "
            "doing right before completion/cancellation. The final-pane snapshot survives until "
            "the mission is deleted via mission.delete.\n\n"
            "Args:\n"
            "  mission_id (required)\n"
            "  lines (optional, default 80, max 2000): tail size to return.\n\n"
            "Returns: {pane_content: string, alive: boolean, claude_running: boolean, source: "
            "\"live\"|\"archived\"|\"none\"}"
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
            "REOPENING: if the mission is in state 'completed', step.add automatically reopens it: "
            "the tmux session is recreated, the worker MCP config is rewritten, state goes back to "
            "'running'. The Claude session resumes with full prior context (via --resume). The "
            "vault (secrets + cookies) is preserved across completion, so credentials are still "
            "available. NOTE: on reopen, position is > 0, so cue must be on_current_complete (or one "
            "of the timeout variants), NOT immediate.\n\n"
            "Use this to queue work. The first step of a NEW mission must have cue {\"type\": "
            "\"immediate\"} (there's no previous step to wait on). Subsequent steps typically use "
            "{\"type\": \"on_current_complete\"}.\n\n"
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
            "Example follow-up that runs next:\n"
            "  {mission_id, directive: \"Summarize /tmp/posts.json and notify the user.\", "
            "cue: {type: \"on_current_complete\"}}\n\n"
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
            "Add a ping - a recurring autonomous watcher. The worker writes and TESTS a standalone "
            "script ONCE; that script then polls your condition on its own (costing ZERO Claude "
            "tokens) and only wakes the worker when the condition actually becomes true (or when "
            "the watchdog notices the script died). This is the token-efficient way to do periodic "
            "checks - Claude is not re-invoked every interval.\n\n"
            "Use it for anything checked repeatedly: a file appears, disk fills up, an API returns "
            "something new, a process dies, a build finishes, a price crosses a threshold, etc.\n\n"
            "Args:\n"
            "  mission_id (required)\n"
            "  condition (required): plain-language description of what to watch for. The worker "
            "turns this into real check logic inside the script.\n"
            "  action (required): what to report / do when it fires (the worker composes a notify "
            "with this in mind).\n"
            "  timeout_s (optional, default 600, min 30): watchdog window. If the script stops "
            "sending alive heartbeats for this long, the worker is automatically re-tasked to "
            "inspect and repair it.\n\n"
            "Example:\n"
            "  {mission_id, condition: \"disk usage on / exceeds 90%\", action: \"warn me with the "
            "current usage %\", timeout_s: 300}\n\n"
            "Returns: {scripted_ping_id}. Track setup via mission.events: scripted_ping_added → "
            "scripted_ping_ready (script tested + running) → scripted_ping_fired (condition hit)."
        ),
        inputSchema=_obj(
            {
                "mission_id": {"type": "string"},
                "condition": {"type": "string", "description": "What to watch for (plain language)."},
                "action": {"type": "string", "description": "What to report when it fires."},
                "timeout_s": {"type": "integer", "minimum": 30, "description": "Watchdog window. Default 600."},
            },
            ["mission_id", "condition", "action"],
        ),
    ),
    Tool(
        name="ping.list",
        description=(
            "List a mission's pings (watchers) with state (setup | active | broken), condition, "
            "action, timeout, script path, and last-alive timestamp.\n\n"
            "Args: {mission_id (required)}.\n\n"
            "Returns: array of ping rows."
        ),
        inputSchema=_obj({"mission_id": {"type": "string"}}, ["mission_id"]),
    ),
    Tool(
        name="ping.delete",
        description=(
            "Delete a ping (watcher). Stops the daemon's watchdog for it. NOTE: this does not by "
            "itself kill the worker's already-running background script - if you want it stopped, "
            "also tell the worker (via a reply or step) to kill the script.\n\n"
            "Args: {scripted_ping_id (required)} - the id returned by ping.add.\n\n"
            "Returns: {ok: true}."
        ),
        inputSchema=_obj({"scripted_ping_id": {"type": "string"}}, ["scripted_ping_id"]),
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
        # The host-facing `ping.*` tools are the scripted watcher under the hood.
        # The old interval-polling ping RPCs (ping.*) remain in the daemon but
        # are no longer exposed as tools (archived; re-list to revive).
        rpc = _HOST_PING_RPC.get(name, name)
        params = {**args, "caller": "host"}
        result = _call(rpc, params)
        return _text(result if result is not None else {"ok": True})

    return srv


# Host-facing ping tools map to the scripted-ping RPCs (transparent swap).
_HOST_PING_RPC: dict[str, str] = {
    "ping.add": "scripted_ping.add",
    "ping.list": "scripted_ping.list",
    "ping.delete": "scripted_ping.delete",
}


# ---------- worker mode ----------


WORKER_INSTRUCTIONS = """\
You are a MISSION WORKER in the orch system - a Claude Code session running inside
a tmux on this machine, driving one long-lived "mission." A host (a separate
orchestrating Claude) or the user set this mission up; you carry out its steps.
You keep full conversational context across every step, heartbeat, and reply
within this mission (you resume the same session each turn).

HOW YOU RECEIVE WORK
  Each turn arrives as a directive (a user-style message). It may be a queued
  step, a heartbeat nudge, a status ping, a reply from the user, or a scheduled
  step you queued for yourself earlier. Do the work, then end your turn.
  If the user sends an attachment, the directive includes a local file path -
  Read/open it to act on it (it's already downloaded to this machine).

THREE WAYS TO REACH PEOPLE - pick by audience and urgency:
  notify(text)              → the human USER, async text (Telegram). Progress,
                              results, anything they should see. Not urgent-only.
  send_file(path,caption?)  → the human USER, a FILE over Telegram (image, PDF,
                              screenshot, CSV, audio, any type). Use when they
                              should SEE or download something, not just read text.
  talk_to_user(summary,q?)  → the human USER, a live PHONE call (via J-dawg).
                              Use ONLY for a real decision/blocker, OR urgent
                              escalation: imminent deadline, time-critical yes/no,
                              or you sent a notify and they haven't responded but
                              you still need them. A phone rings; a text can sit
                              unread. NON-BLOCKING - after calling, finish your
                              turn; the user's answer returns as a new step.
  message_host(text,files?) → the orchestrating HOST (a mailbox; can attach
                              files). For coordination/escalation between you and
                              the orchestrator, or handing back results.

PLAN YOUR OWN FUTURE - queue.add(directive, cue):
  You can schedule steps for yourself. The directive is what future-you will
  receive; the cue decides when:
    {type:"at_time", at:"YYYY-MM-DD HH:MM"}  → fire at an absolute local time
    {type:"at_time", epoch:<unix seconds>}   → same, tz-independent
    {type:"on_timeout", seconds:N}           → N seconds after the prev step started
    {type:"on_current_complete"}              → run next, when the current step finishes
  Use this to plan ahead: e.g. "call the user at 9:00am", "retry in 30 minutes",
  "follow up tomorrow". The mission stays alive for scheduled steps - it won't
  auto-complete while steps are pending. So if you have future work, QUEUE IT
  before ending your turn rather than trying to stay running.

WATCH FOR CONDITIONS WITHOUT BURNING TOKENS - pings.add(condition, action):
  Instead of polling something yourself turn after turn, set a ping: you'll be
  asked to write+test a small watcher script that runs on its own and only wakes
  you when the condition fires (or if it breaks). Good for "tell me when X
  happens" / "watch until the deadline."

LOCATION: get_user_location returns the user's latest shared location (and the
  live position as they move, if they shared live location). Combine with a ping
  to watch geofences ("notify when I'm within 500m of home").

OTHER TOOLS: queue.list/update/delete (manage your pending steps), pings.list/
  delete, mission.status, secrets.list/cookies.list (values via the `msec` CLI),
  heartbeat.get/heartbeat.set (retime your own status-update cadence). Plus full
  Claude Code tooling (Bash, Read/Write, web) and any
  shared MCPs (e.g. Playwright for browsers). You run with full permissions.

LIFECYCLE: when your queue is empty, no pings exist, and you go idle, the mission
auto-completes (you'll get a wrap-up directive to summarize via notify first).
So: to keep going later, queue a future step; to finish, just stop.
"""


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
            name="send_file",
            description=(
                "Send a FILE to the user via Telegram (into this mission's chat) - an "
                "image, PDF, screenshot, CSV, log, audio, video, any type. Pass an "
                "absolute path to a file on this machine. Images show inline; other "
                "types arrive as a downloadable document. Add an optional caption. "
                "Max 50 MB. Use this when the user wants to SEE or download something, "
                "not just read text.\n\n"
                "Args: {path (required, absolute), caption (optional)}.\n"
                "Example: {path: \"/tmp/chart.png\", caption: \"Here's the weekly chart.\"}"
            ),
            inputSchema=_obj(
                {"path": {"type": "string"}, "caption": {"type": "string"}},
                ["path"],
            ),
        ),
        Tool(
            name="message_host",
            description=(
                "Send a message UP to the orchestrating host (not the human user). Use this to "
                "escalate a blocker, ask the host to spawn a follow-up mission, hand back "
                "structured results, or deliver files. The host reads these from its inbox on its "
                "own schedule (it's a mailbox, not an interrupt). For messages meant for the human, "
                "use `notify` instead.\n\n"
                "Args:\n"
                "  text (required): your message to the host.\n"
                "  files (optional): list of absolute file paths on this machine to attach; the "
                "daemon copies them so the host can fetch their bytes.\n\n"
                "Example: {text: \"Scrape done, results attached. Want me to start the analysis "
                "mission?\", files: [\"/tmp/results.json\"]}"
            ),
            inputSchema=_obj(
                {
                    "text": {"type": "string"},
                    "files": {"type": "array", "items": {"type": "string"}},
                },
                ["text"],
            ),
        ),
        Tool(
            name="queue.list",
            description="List the mission's steps (current and pending).",
            inputSchema=_obj({}, []),
        ),
        Tool(
            name="queue.add",
            description=(
                "Queue a future step for YOURSELF - a directive you'll receive later, gated by a "
                "cue. This is how you plan ahead: schedule a follow-up at a specific time with "
                "cue {type:'at_time', at:'YYYY-MM-DD HH:MM'} (machine local time) or {type:'at_time', "
                "epoch:<unix>}; or chain work with on_current_complete / on_timeout. The directive is "
                "the instruction future-you will act on (you keep full session context). "
                "Example: schedule a 9am check-in, or re-attempt a call after 30 min."
            ),
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
            description="List this mission's pings (autonomous watcher scripts) and their state.",
            inputSchema=_obj({}, []),
        ),
        Tool(
            name="pings.add",
            description=(
                "Add a ping - a watcher you (the worker) implement as a standalone script that "
                "polls a condition without spending tokens, then wakes you only when it fires. "
                "After this call you'll receive a setup directive telling you to write, test, "
                "background the script, and register it with `owatch ready`."
            ),
            inputSchema=_obj(
                {
                    "condition": {"type": "string", "description": "What to watch for."},
                    "action": {"type": "string", "description": "What to report when it fires."},
                    "timeout_s": {"type": "integer", "minimum": 30},
                },
                ["condition", "action"],
            ),
        ),
        Tool(
            name="pings.delete",
            description="Delete one of this mission's pings by id (stops the watchdog).",
            inputSchema=_obj({"scripted_ping_id": {"type": "string"}}, ["scripted_ping_id"]),
        ),
        Tool(
            name="heartbeat.get",
            description="Get this mission's heartbeat interval and last-fired time.",
            inputSchema=_obj({}, []),
        ),
        Tool(
            name="heartbeat.set",
            description=(
                "Change your own heartbeat interval (seconds, max 86400 = 24h). The heartbeat "
                "is the periodic nudge that asks you to send a status update via notify; it's "
                "mandatory and can't be disabled, only retimed. Shorten it if the user wants more "
                "frequent check-ins, lengthen it for quiet long-running work."
            ),
            inputSchema=_obj(
                {"interval_s": {"type": "integer", "minimum": 1, "maximum": 86400}},
                ["interval_s"],
            ),
        ),
        Tool(
            name="mission.status",
            description="Get this mission's status snapshot.",
            inputSchema=_obj({}, []),
        ),
        Tool(
            name="get_user_location",
            description=(
                "Get the user's latest shared location (latitude/longitude). If the user shared a "
                "LIVE location, this returns the most recent fix as they move - call it again any "
                "time for an updated position. Use for location-aware tasks (e.g. 'tell me when I'm "
                "near home', distance/ETA, where-are-they checks).\n\n"
                "Returns: {available, latitude, longitude, accuracy_m, heading, live, updated_at, "
                "age_seconds, maps_url} - or {available:false} if the user never shared one. Check "
                "age_seconds: a large value means the location is stale / live sharing ended."
            ),
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
    srv: Server = Server(
        name=f"mission-orchestrator-worker-{mission_id}",
        instructions=WORKER_INSTRUCTIONS,
    )
    tools = _worker_tools()

    @srv.list_tools()
    async def _list_tools() -> list[Tool]:
        return tools

    @srv.call_tool()
    async def _call_tool(name: str, args: dict[str, Any]) -> list[TextContent]:
        a = {**args, "mission_id": mission_id, "caller": "worker"}
        if name == "notify":
            return _text(_call("notify", {"mission_id": mission_id, "text": args.get("text", "")}))
        if name == "send_file":
            return _text(_call("notify_file", {
                "mission_id": mission_id,
                "path": args.get("path", ""),
                "caption": args.get("caption", ""),
            }))
        if name == "message_host":
            return _text(_call("host.message", {
                "mission_id": mission_id,
                "text": args.get("text", ""),
                "files": args.get("files", []),
            }))
        if name == "queue.list":
            return _text(_call("step.list", {"mission_id": mission_id}))
        if name == "queue.add":
            return _text(_call("step.add", {**a, "created_by": "worker"}))
        if name == "queue.update":
            return _text(_call("step.update", a))
        if name == "queue.delete":
            return _text(_call("step.delete", a))
        if name == "pings.list":
            return _text(_call("scripted_ping.list", {"mission_id": mission_id}))
        if name == "pings.add":
            return _text(_call("scripted_ping.add", {**a, "created_by": "worker"}))
        if name == "pings.delete":
            return _text(_call("scripted_ping.delete", a))
        if name == "heartbeat.get":
            return _text(_call("heartbeat.get", {"mission_id": mission_id}))
        if name == "heartbeat.set":
            return _text(_call("heartbeat.set", {"mission_id": mission_id, "interval_s": args.get("interval_s")}))
        if name == "mission.status":
            return _text(_call("mission.get", {"mission_id": mission_id}))
        if name == "get_user_location":
            return _text(_call("location.get", {"mission_id": mission_id}))
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
