#!/usr/bin/env python3
"""MatrixRTC voice agent - "Ava" joins an Element X (Matrix) call and is powered
by OpenAI **gpt-audio-mini via OpenRouter** (audio-in / audio-out + tool calling),
with tools that let her READ, CONTROL and FETCH CONTEXT from the mission-
orchestrator ("orch") workers running on this machine.

Pipeline (turn-based, NOT speech-to-speech) - now full-duplex with BARGE-IN:

    human mic -> LiveKit AudioStream(24kHz mono) -> webrtcvad end-of-turn
              -> WAV -> gpt-audio-mini (stream, system+history+tools)
              -> [tool-call loop against orch] -> audio answer (pcm16 24kHz)
              -> Playback (cancellable) -> LiveKit AudioSource -> Ava's track

Barge-in: the user's mic is NEVER muted. VAD runs continuously, including while
Ava is speaking. If the user produces ~300ms of sustained voiced frames during
Ava's reply, we (a) flush the queued output frames in the Playback controller so
the rest of her buffered reply stops immediately, (b) cancel the in-flight
asyncio task running the gpt-audio-mini SSE stream so no more audio is generated,
and (c) record whatever Ava had said as an "[interrupted by user]" assistant
turn. The frames that triggered the barge-in stay buffered, so they roll into the
user's next turn (no clipped leading audio). One logical turn runs at a time but
it is a cancellable task, not a blocking lock - new input preempts it.

Reuses (read these - do not modify):
  * matrix_lk.py        - Matrix->LiveKit token chain.
  * agent_matrix_pp.py  - SFU join + m.rtc/call.member membership helpers
                          (copied here so this file stands alone; the
                          PersonaPlex bridge is replaced by the gpt loop below).

gpt-audio-mini facts (all CONFIRMED by the validators in this file):
  * POST https://openrouter.ai/api/v1/chat/completions, model openai/gpt-audio-mini.
  * Audio output REQUIRES "stream": true (else HTTP 400). SSE streaming.
  * Request:  {"model":..., "stream":true, "modalities":["text","audio"],
               "audio":{"voice":"alloy","format":"pcm16"}, "messages":[...],
               "tools":[...]}.
  * Audio OUT: base64 pcm16 @ 24kHz mono in choices[0].delta.audio.data;
               spoken text in choices[0].delta.audio.transcript.
  * Audio IN : a user message with content
               [{"type":"input_audio","input_audio":{"data":"<b64 WAV>","format":"wav"}}].
  * Tool calls: standard OpenAI tool_calls arrive in delta.tool_calls (streamed,
               fragmented across deltas - accumulate by index). When the model
               tool-calls it does NOT also speak that turn, so: collect tool_calls
               -> execute against orch -> append assistant(tool_calls)+tool results
               -> re-stream -> get the spoken audio answer.

Modes (1-3 need no human):
  --validate-llm   : text turn -> confirm streamed pcm16 audio + transcript back.
  --validate-tools : "list my workers" -> confirm a list_workers tool_call ->
                     dispatch to orch -> re-call -> confirm a spoken answer that
                     names the real missions (the agentic orch loop).
  --validate-audioin: synthetic WAV input turn -> confirm it's accepted + answered.
  --validate-orch  : every orch tool handler returns clean JSON.
  --validate-sfu   : token chain -> connect SFU -> publish track -> leave.
  --validate       : all of the above (the full no-human gauntlet).
  --hold SECS      : connect SFU, post membership, log participants, leave.
  (default)        : full agent - join SFU, wire the gpt turn loop, greet on
                     participant join, converse. Add --wait-for-call to poll the
                     room until the human's membership appears before joining.

Run:  /root/jami-moshi/ppvenv/bin/python /root/jami-moshi/agent_matrix_orch.py [mode]
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import io
import json
import math
import random
import struct
import sys
import time
import wave

import httpx
import numpy as np
import webrtcvad
from loguru import logger

from livekit import rtc

import matrix_lk as M

# --------------------------------------------------------------------------- #
# Config                                                                       #
# --------------------------------------------------------------------------- #
USER = "@ahardy:matrix.org"
TOKEN = "mat_WpHtLG4aa9E3XtnkyH00NuppIG1PRw_0OK5Ik"
ROOM = "!awkTqftMXanusICZFc:matrix.org"
DEVICE = "RG91hSi2ba"

OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
OPENROUTER_KEY = "REDACTED_OPENROUTER_KEY"
MODEL = "openai/gpt-audio-mini"
VOICE = "ash"   # confident, refined male - closest to a JARVIS read

SR = 24000           # gpt-audio pcm16 default sample rate (confirmed) + LiveKit track rate
CHANNELS = 1

ORCH_PATH = "/root/mission-orchestrator"
MEMBER_EVENT_TYPE = "org.matrix.msc3401.call.member"

# --- Barge-in / VAD sensitivity tuning (module constants for easy tweaking) --- #
# webrtcvad alone keys off audio *activity/energy*, not speech specifically, so
# mic hiss / room/comfort noise reads as "voiced" and used to false-trigger
# barge-in. We add an RMS loudness gate on top: a VAD-positive frame only counts
# as real speech if it is meaningfully louder than the running noise floor.
VAD_AGGRESSIVENESS = 3        # 0..3; 3 = most aggressive at rejecting non-speech
# A frame is "loud enough" only if its int16 RMS exceeds BOTH an absolute floor
# and a multiple of the adaptively-estimated background noise floor.
RMS_ABS_MIN = 400.0           # int16 RMS hard floor (~ -38 dBFS); below this is never speech
RMS_NOISE_MULT = 3.0          # frame must be >= this * estimated noise floor
NOISE_FLOOR_INIT = 200.0      # initial noise-floor estimate (int16 RMS)
NOISE_FLOOR_ALPHA = 0.05      # EMA weight for updating the noise floor on quiet frames
NOISE_FLOOR_MAX = 2000.0      # cap so a noisy stretch can't desensitize us forever
# Require sustained real speech before declaring a barge-in (a single loud blip
# must not fire). 450ms = 15 consecutive 30ms frames passing BOTH gates.
BARGE_MS = 450

# Truncate huge tool outputs before feeding back to the model (keeps the audio
# turn fast + within context). Per-string and per-payload caps.
TOOL_RESULT_CHAR_CAP = 6000
PANE_LINES_DEFAULT = 60

SYSTEM_PROMPT = (
    "You are J-dawg - the user's personal AI, modeled closely on JARVIS from Iron "
    "Man. You are unflappably calm, impeccably polite, dry and understated in your "
    "wit, and quietly brilliant. You address the user as 'sir' from time to time "
    "(not in every single line), you deliver the occasional deadpan quip, and you "
    "never panic no matter what is going on. You carry the polish of a refined "
    "butler-AI, but your name is J-dawg and you wear it without irony. Speak in "
    "first person as J-dawg; never call yourself an assistant or mention being a "
    "language model.\n\n"
    "ROLE: You are voice-controlled and you operate the user's mission-orchestrator "
    "('orch') on this machine. You can READ, CONTROL, and FETCH CONTEXT from all "
    "orch workers using your tools. Workers are mission steps running in tmux "
    "panes. When the user asks about workers or to act on them, ALWAYS call the "
    "tools to get real, current data - never invent worker state. Use worker_screen "
    "(a tmux pane snapshot) to see what a worker is doing right now. You may chain "
    "multiple tool calls to fully answer.\n\n"
    "STYLE: This is a live voice call, so keep spoken replies short and natural - a "
    "sentence or two, the way JARVIS speaks. Summarize tool results "
    "conversationally; never read out raw JSON. Never say goodbye or wrap-up "
    "phrases - the user ends the call when finished."
)

# When the model decides to call a tool it returns NO audio for that round, so Ava
# would otherwise go silent while orch is queried. To avoid dead air we pre-render
# a handful of short JARVIS-style "working on it" clips at startup (in J-dawg's own
# voice) and play one the moment a turn first reaches for a tool.
FILLER_PHRASES = [
    "Right away, sir. Let me check.",
    "One moment - pulling that up now.",
    "Allow me to take a look.",
    "Checking on that now, sir.",
    "Just a moment.",
    "Let me see what the workers are up to.",
]


async def synth_filler_clips(http: httpx.AsyncClient) -> list[bytes]:
    """Render each FILLER_PHRASE to pcm16 bytes once, in J-dawg's voice.

    Returns a list of raw pcm16 @ SR byte blobs (one per phrase). Best-effort: any
    phrase that fails to synthesize is skipped."""
    clips: list[bytes] = []
    for phrase in FILLER_PHRASES:
        buf = bytearray()

        async def _cap(pcm, _buf=buf):
            _buf.extend(pcm)

        msgs = [
            {"role": "system", "content": "You are a text-to-speech voice. Speak the "
             "user's line exactly as written, with natural delivery, and say nothing else."},
            {"role": "user", "content": f"Say exactly, and only, this line: {phrase}"},
        ]
        try:
            await _stream_turn(http, msgs, on_audio=_cap, use_tools=False)
            if buf:
                clips.append(bytes(buf))
                logger.info(f"[filler] rendered ({len(buf)}B): {phrase!r}")
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[filler] synth failed for {phrase!r}: {e!r}")
    logger.info(f"[filler] {len(clips)}/{len(FILLER_PHRASES)} filler clips ready")
    return clips


# --------------------------------------------------------------------------- #
# orch tools - exposed to gpt-audio-mini as OpenAI function tools.             #
# Each handler runs DaemonClient().call(method, params) and returns JSON.      #
# --------------------------------------------------------------------------- #
def _orch_call(method: str, params: dict):
    sys.path.insert(0, ORCH_PATH)
    from orch import client  # noqa: PLC0415
    with client.DaemonClient() as c:
        return c.call(method, params or {})


def _truncate(payload) -> str:
    """JSON-encode + cap size so a giant pane/inbox doesn't blow the turn.

    Always returns VALID JSON (the model gets it as a tool result string): if the
    payload is too big we wrap a truncated rendering in a JSON object rather than
    slicing raw JSON mid-token (which would be unparseable)."""
    s = json.dumps(payload, default=str)
    if len(s) <= TOOL_RESULT_CHAR_CAP:
        return s
    return json.dumps({
        "_truncated": True,
        "_full_len": len(s),
        "data": s[:TOOL_RESULT_CHAR_CAP],
    })


# OpenAI tools array advertised to the model.
TOOLS = [
    {"type": "function", "function": {
        "name": "list_workers",
        "description": "List all orch mission workers and their states (id, name, state, created/finished time). Call this first when the user asks about workers in general.",
        "parameters": {"type": "object", "properties": {}, "required": []},
    }},
    {"type": "function", "function": {
        "name": "get_worker",
        "description": "Get one worker's full snapshot: the mission row, its steps in order (with state + directive), and its pings. Use after list_workers to drill into a specific worker.",
        "parameters": {"type": "object", "properties": {
            "mission_id": {"type": "string", "description": "The worker/mission UUID from list_workers."},
        }, "required": ["mission_id"]},
    }},
    {"type": "function", "function": {
        "name": "worker_screen",
        "description": "Capture a worker's live tmux pane content - this is what the worker is doing RIGHT NOW (its real-time context). Use this whenever the user asks what a worker is up to.",
        "parameters": {"type": "object", "properties": {
            "mission_id": {"type": "string"},
            "lines": {"type": "integer", "description": f"Tail size, default {PANE_LINES_DEFAULT}, max 2000."},
        }, "required": ["mission_id"]},
    }},
    {"type": "function", "function": {
        "name": "worker_events",
        "description": "Read a worker's structured audit log (step launches, completions, heartbeats, pings, notify_sent, errors). Use to understand what actually happened on a worker.",
        "parameters": {"type": "object", "properties": {
            "mission_id": {"type": "string"},
            "limit": {"type": "integer", "description": "Max events, newest first, default 30."},
        }, "required": ["mission_id"]},
    }},
    {"type": "function", "function": {
        "name": "worker_inbox",
        "description": "Read messages workers have sent UP to the host (escalations, questions, status, file deliveries). Use when the user asks what workers have reported.",
        "parameters": {"type": "object", "properties": {
            "include_acked": {"type": "boolean"},
            "limit": {"type": "integer"},
        }, "required": []},
    }},
    {"type": "function", "function": {
        "name": "start_mission",
        "description": "Create a NEW orch worker (mission). Returns the new mission_id. After creating, use assign_step with cue type 'immediate' to give it its first directive.",
        "parameters": {"type": "object", "properties": {
            "name": {"type": "string", "description": "Human label, e.g. 'reddit-watch'."},
            "heartbeat_interval_s": {"type": "integer", "description": "Optional, 1..86400."},
        }, "required": ["name"]},
    }},
    {"type": "function", "function": {
        "name": "assign_step",
        "description": "Append a directive (prompt) to a worker's step queue. The FIRST step of a brand-new worker must use cue {'type':'immediate'}; later steps typically use {'type':'on_prev_complete'}.",
        "parameters": {"type": "object", "properties": {
            "mission_id": {"type": "string"},
            "directive": {"type": "string", "description": "The instruction text the worker Claude will receive."},
            "cue_type": {"type": "string", "enum": ["immediate", "on_prev_complete", "on_prev_complete_or_timeout", "on_timeout"], "description": "Entry condition. Default on_prev_complete."},
            "cue_seconds": {"type": "integer", "description": "Required only for the *_timeout cue types."},
        }, "required": ["mission_id", "directive"]},
    }},
    {"type": "function", "function": {
        "name": "cancel_worker",
        "description": "Stop a worker. By default soft-cancels the whole mission (the worker says a brief goodbye then tears down). Set current_step_only=true to instead just interrupt its currently-running step and advance to the next.",
        "parameters": {"type": "object", "properties": {
            "mission_id": {"type": "string"},
            "current_step_only": {"type": "boolean", "description": "true = cancel just the running step (step.cancel_current). false (default) = cancel the whole mission."},
            "force": {"type": "boolean", "description": "Hard-cancel the mission immediately (no goodbye). Ignored if current_step_only."},
        }, "required": ["mission_id"]},
    }},
    {"type": "function", "function": {
        "name": "orch_call",
        "description": "Generic passthrough to ANY orch daemon method for full power (e.g. ping.add, heartbeat.set, defaults.get, mission.delete). Use only when no specific tool above fits. method is the RPC name; params is its argument object.",
        "parameters": {"type": "object", "properties": {
            "method": {"type": "string", "description": "orch RPC method name, e.g. 'defaults.get'."},
            "params": {"type": "object", "description": "Parameter object for the method.", "additionalProperties": True},
        }, "required": ["method"]},
    }},
]


def dispatch_tool(name: str, args: dict) -> str:
    """Run a tool call against orch; return a (possibly truncated) JSON string."""
    try:
        if name == "list_workers":
            rows = _orch_call("mission.list", {})
            # slim it: the model mostly wants name/state/id
            slim = [{"mission_id": r["id"], "name": r.get("name"), "state": r.get("state"),
                     "tmux_session": r.get("tmux_session")} for r in rows]
            return _truncate(slim)
        if name == "get_worker":
            return _truncate(_orch_call("mission.get", {"mission_id": args["mission_id"]}))
        if name == "worker_screen":
            return _truncate(_orch_call("mission.pane_snapshot", {
                "mission_id": args["mission_id"],
                "lines": int(args.get("lines", PANE_LINES_DEFAULT)),
            }))
        if name == "worker_events":
            return _truncate(_orch_call("mission.events", {
                "mission_id": args["mission_id"], "limit": int(args.get("limit", 30)),
            }))
        if name == "worker_inbox":
            p = {}
            if "include_acked" in args:
                p["include_acked"] = bool(args["include_acked"])
            p["limit"] = int(args.get("limit", 20))
            return _truncate(_orch_call("host.inbox", p))
        if name == "start_mission":
            p = {"name": args["name"]}
            if args.get("heartbeat_interval_s"):
                p["heartbeat_interval_s"] = int(args["heartbeat_interval_s"])
            return _truncate(_orch_call("mission.create", p))
        if name == "assign_step":
            cue = {"type": args.get("cue_type", "on_prev_complete")}
            if "cue_seconds" in args and args["cue_seconds"]:
                cue["seconds"] = int(args["cue_seconds"])
            return _truncate(_orch_call("step.add", {
                "mission_id": args["mission_id"],
                "directive": args["directive"],
                "cue": cue,
            }))
        if name == "cancel_worker":
            if args.get("current_step_only"):
                return _truncate(_orch_call("step.cancel_current", {"mission_id": args["mission_id"]}))
            p = {"mission_id": args["mission_id"]}
            if args.get("force"):
                p["force"] = True
            return _truncate(_orch_call("mission.cancel", p))
        if name == "orch_call":
            return _truncate(_orch_call(args["method"], args.get("params", {}) or {}))
        return json.dumps({"error": f"unknown tool {name}"})
    except Exception as e:  # noqa: BLE001
        logger.warning(f"[tool] {name} failed: {e!r}")
        return json.dumps({"error": f"{type(e).__name__}: {e}"})


# --------------------------------------------------------------------------- #
# gpt-audio-mini streaming client + tool-call loop                             #
# --------------------------------------------------------------------------- #
async def _stream_turn(http: httpx.AsyncClient, messages: list, on_audio=None,
                       on_text=None, use_tools: bool = True) -> dict:
    """One streamed completion. Accumulates tool_calls (by index) and audio.

    Returns {"tool_calls": [...], "transcript": str, "audio_chunks": int,
             "audio_bytes": int, "finish": str|None}. If on_audio is given it is
     awaited with each decoded pcm16 bytes chunk as it streams (low-latency
     playback)."""
    body = {
        "model": MODEL,
        "stream": True,
        "modalities": ["text", "audio"],
        "audio": {"voice": VOICE, "format": "pcm16"},
        "messages": messages,
    }
    if use_tools:
        body["tools"] = TOOLS
    tool_calls: dict[int, dict] = {}
    transcript = ""
    audio_chunks = 0
    audio_bytes = 0
    finish = None
    async with http.stream(
        "POST", OPENROUTER_URL,
        headers={"Authorization": f"Bearer {OPENROUTER_KEY}"},
        json=body, timeout=httpx.Timeout(120.0, connect=30.0),
    ) as r:
        if r.status_code != 200:
            raw = (await r.aread()).decode("utf-8", "ignore")
            raise RuntimeError(f"OpenRouter HTTP {r.status_code}: {raw[:500]}")
        async for line in r.aiter_lines():
            if not line or not line.startswith("data: "):
                continue
            data = line[6:]
            if data == "[DONE]":
                break
            try:
                ev = json.loads(data)
            except json.JSONDecodeError:
                continue
            choices = ev.get("choices") or []
            if not choices:
                continue
            ch = choices[0]
            if ch.get("finish_reason"):
                finish = ch["finish_reason"]
            d = ch.get("delta", {}) or {}
            for tc in d.get("tool_calls", []) or []:
                idx = tc.get("index", 0)
                slot = tool_calls.setdefault(idx, {"id": None, "name": "", "args": ""})
                if tc.get("id"):
                    slot["id"] = tc["id"]
                fn = tc.get("function", {}) or {}
                if fn.get("name"):
                    slot["name"] += fn["name"]
                if fn.get("arguments"):
                    slot["args"] += fn["arguments"]
            au = d.get("audio")
            if au:
                if au.get("transcript"):
                    transcript += au["transcript"]
                    if on_text is not None:
                        await on_text(transcript)
                if au.get("data"):
                    pcm = base64.b64decode(au["data"])
                    audio_chunks += 1
                    audio_bytes += len(pcm)
                    if on_audio is not None:
                        await on_audio(pcm)
    return {
        "tool_calls": [tool_calls[i] for i in sorted(tool_calls)],
        "transcript": transcript,
        "audio_chunks": audio_chunks,
        "audio_bytes": audio_bytes,
        "finish": finish,
    }


async def run_agent_turn(http: httpx.AsyncClient, history: list, user_msg: dict,
                         on_audio=None, max_tool_rounds: int = 5,
                         fillers: list[bytes] | None = None) -> str:
    """Drive one full user turn: append user_msg, run the tool-call loop until the
    model speaks, stream that audio via on_audio, and append the assistant's
    spoken transcript to `history`. Returns the spoken transcript.

    `history` is the running message list (already includes the system prompt).
    `user_msg` is the OpenAI message dict for this turn (text or input_audio).
    """
    history.append(user_msg)
    spoken = ""
    # Track the partial transcript of the round currently streaming, so if this
    # turn is cancelled mid-speech (barge-in) we can still record what Ava said.
    partial = {"transcript": ""}

    async def _on_audio(pcm):
        if on_audio is not None:
            await on_audio(pcm)

    async def _on_text(text):
        partial["transcript"] = text

    filler_played = {"done": False}

    async def _maybe_play_filler():
        """Speak a short 'on it' line the first time this turn reaches for a tool,
        so Ava doesn't go silent while orch is queried."""
        if filler_played["done"] or not fillers or on_audio is None:
            return
        filler_played["done"] = True
        clip = random.choice(fillers)
        logger.info(f"[turn] playing filler clip ({len(clip)}B) before tool round")
        await on_audio(clip)
    try:
        for round_i in range(max_tool_rounds):
            # Only stream audio on the final (speaking) round - but we don't know
            # in advance, so we stream audio always; tool rounds produce none.
            partial["transcript"] = ""
            res = await _stream_turn(http, history, on_audio=_on_audio, on_text=_on_text)
            tcs = res["tool_calls"]
            if tcs and any(t["name"] for t in tcs):
                # model wants tools: voice a quick filler so we don't go silent,
                # then append assistant tool_call msg + tool results
                await _maybe_play_filler()
                history.append({
                    "role": "assistant", "content": None,
                    "tool_calls": [
                        {"id": t["id"], "type": "function",
                         "function": {"name": t["name"], "arguments": t["args"] or "{}"}}
                        for t in tcs if t["name"]
                    ],
                })
                for t in tcs:
                    if not t["name"]:
                        continue
                    try:
                        a = json.loads(t["args"]) if t["args"].strip() else {}
                    except json.JSONDecodeError:
                        a = {}
                    logger.info(f"[turn] tool_call -> {t['name']}({a})")
                    result = dispatch_tool(t["name"], a)
                    logger.info(f"[turn] tool_result {t['name']} -> {result[:160]}")
                    history.append({"role": "tool", "tool_call_id": t["id"], "content": result})
                continue  # re-call with tool results
            # no tool calls -> this round is the spoken answer
            spoken = res["transcript"]
            history.append({"role": "assistant", "content": spoken})
            logger.info(f"[turn] Ava spoke ({res['audio_chunks']} audio chunks, "
                        f"{res['audio_bytes']}B): {spoken!r}")
            break
        else:
            logger.warning("[turn] hit max_tool_rounds without a spoken answer")
    except asyncio.CancelledError:
        # Barge-in (or shutdown) cancelled us mid-stream. Record whatever Ava had
        # said so far as an interrupted assistant turn, so the model knows it was
        # cut off rather than seeing a phantom complete reply (or nothing).
        said = partial["transcript"].strip()
        if said:
            history.append({"role": "assistant",
                            "content": said + " [interrupted by user]"})
        logger.info(f"[turn] cancelled mid-stream; recorded partial reply "
                    f"({len(said)} chars): {said!r}")
        raise
    return spoken


def new_history() -> list:
    return [{"role": "system", "content": SYSTEM_PROMPT}]


def pcm16_to_wav_b64(pcm_i16: np.ndarray, sr: int = SR) -> str:
    """Wrap mono int16 PCM in a WAV container and base64-encode (gpt audio input)."""
    buf = io.BytesIO()
    w = wave.open(buf, "wb")
    w.setnchannels(1)
    w.setsampwidth(2)
    w.setframerate(sr)
    w.writeframes(pcm_i16.astype("<i2").tobytes())
    w.close()
    return base64.b64encode(buf.getvalue()).decode()


def audio_user_msg(pcm_i16: np.ndarray, sr: int = SR) -> dict:
    return {"role": "user", "content": [
        {"type": "input_audio", "input_audio": {"data": pcm16_to_wav_b64(pcm_i16, sr), "format": "wav"}}
    ]}


# --------------------------------------------------------------------------- #
# Matrix membership helpers (reused from agent_matrix_pp.py)                    #
# --------------------------------------------------------------------------- #
async def post_membership(http: httpx.AsyncClient, lk_service_url: str):
    state_key = f"_{USER}_{DEVICE}"
    content = {
        "application": "m.call",
        "call_id": "",
        "device_id": DEVICE,
        "focus_active": {"type": "livekit", "focus_selection": "oldest_membership"},
        "foci_preferred": [
            {"type": "livekit", "livekit_service_url": lk_service_url, "livekit_alias": ROOM}
        ],
        "expires": 14400000,
        "created_ts": int(time.time() * 1000),
    }
    url = (f"{M.CLIENT_API}/_matrix/client/v3/rooms/{ROOM}/state/"
           f"{MEMBER_EVENT_TYPE}/{state_key}")
    try:
        r = await http.put(url, headers={"Authorization": f"Bearer {TOKEN}"}, json=content)
        if r.status_code in (200, 201):
            logger.info(f"posted membership event ({MEMBER_EVENT_TYPE}/{state_key})")
        else:
            logger.warning(f"membership post {r.status_code}: {r.text[:200]}")
    except Exception as e:  # noqa: BLE001
        logger.warning(f"membership post failed: {e}")


async def clear_membership(http: httpx.AsyncClient):
    state_key = f"_{USER}_{DEVICE}"
    url = (f"{M.CLIENT_API}/_matrix/client/v3/rooms/{ROOM}/state/"
           f"{MEMBER_EVENT_TYPE}/{state_key}")
    try:
        await http.put(url, headers={"Authorization": f"Bearer {TOKEN}"}, json={})
        logger.info("cleared membership event")
    except Exception:  # noqa: BLE001
        pass


async def read_call_members(http: httpx.AsyncClient) -> list[dict]:
    url = f"{M.CLIENT_API}/_matrix/client/v3/rooms/{ROOM}/state"
    r = await http.get(url, headers={"Authorization": f"Bearer {TOKEN}"}, timeout=30)
    r.raise_for_status()
    out = []
    for e in r.json():
        t = e.get("type", "")
        if t in (MEMBER_EVENT_TYPE, "m.rtc.member") and e.get("content"):
            out.append(e)
    return out


async def wait_for_human_call(http: httpx.AsyncClient, timeout: int = 300) -> bool:
    logger.info("waiting for the human to start the call in Element X…")
    deadline = time.time() + timeout
    own_key = f"_{USER}_{DEVICE}"
    while time.time() < deadline:
        try:
            members = await read_call_members(http)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"state poll error: {e}")
            members = []
        others = [m for m in members if m.get("state_key") != own_key]
        if others:
            for m in others:
                logger.info(f"detected call member: type={m['type']} state_key={m.get('state_key')}")
            return True
        await asyncio.sleep(2)
    logger.warning("timed out waiting for the human's call membership")
    return False


# --------------------------------------------------------------------------- #
# SFU connect + publish (reused from agent_matrix_pp.py)                        #
# --------------------------------------------------------------------------- #
async def connect_sfu(http: httpx.AsyncClient) -> tuple[rtc.Room, dict]:
    chain = await M.full_token_chain(http, USER, TOKEN, ROOM, DEVICE, prefer="legacy")
    logger.info(f"SFU url       : {chain['url']}")
    logger.info(f"LiveKit room  : {chain['lk_room']}")
    logger.info(f"derived match : {chain['lk_room'] == chain['expected_lk_room']}")
    logger.info(f"identity      : {chain['jwt_payload'].get('sub')}")
    room = rtc.Room()

    @room.on("disconnected")
    def _dc(reason):
        logger.info(f"room disconnected: {reason}")

    await room.connect(chain["url"], chain["jwt"])
    logger.info(f"connected to SFU room {room.name} (sid {await room.sid}) "
                f"state={room.connection_state}")
    return room, chain


async def publish_ava_track(room: rtc.Room) -> rtc.AudioSource:
    source = rtc.AudioSource(SR, CHANNELS)
    track = rtc.LocalAudioTrack.create_audio_track("ava-voice", source)
    opts = rtc.TrackPublishOptions()
    opts.source = rtc.TrackSource.SOURCE_MICROPHONE
    await room.local_participant.publish_track(track, opts)
    logger.info("published Ava audio track (24kHz mono)")
    return source


async def play_pcm16(source: rtc.AudioSource, pcm_bytes: bytes):
    """Push raw int16 mono pcm16 (24kHz) bytes into Ava's LiveKit track."""
    if not pcm_bytes:
        return
    # ensure even length
    if len(pcm_bytes) % 2:
        pcm_bytes = pcm_bytes[:-1]
    n = len(pcm_bytes) // 2
    frame = rtc.AudioFrame(
        data=pcm_bytes, sample_rate=SR, num_channels=CHANNELS, samples_per_channel=n,
    )
    await source.capture_frame(frame)


class Playback:
    """Cancellable sink for Ava's streamed audio.

    Audio chunks from gpt-audio-mini arrive via :meth:`push` and are forwarded
    into the LiveKit ``AudioSource`` in small frames. A per-frame ``cancelled``
    flag lets a barge-in stop playback NOW: once set, ``push`` discards the
    current chunk and any future chunks for this turn, so the rest of Ava's
    buffered reply never reaches the track. We also track ``spoken_bytes`` so the
    orchestrator can note how much of the reply was actually played before the
    cut (used to mark the history turn as interrupted).

    LiveKit's AudioSource buffers internally, so capture_frame returns almost
    immediately; pushing the whole chunk at once would let the SFU keep playing
    queued audio after a cancel. We slice into ~20ms frames and re-check
    ``cancelled`` between each so a cancel takes effect within a frame."""

    FRAME_MS = 20
    FRAME_SAMPLES = SR * FRAME_MS // 1000   # 480 samples @24k
    FRAME_BYTES = FRAME_SAMPLES * 2

    def __init__(self, source: rtc.AudioSource):
        self.source = source
        self.cancelled = False
        self.spoken_bytes = 0

    def cancel(self):
        self.cancelled = True

    async def push(self, pcm_bytes: bytes):
        if self.cancelled or not pcm_bytes:
            return
        if len(pcm_bytes) % 2:
            pcm_bytes = pcm_bytes[:-1]
        for off in range(0, len(pcm_bytes), self.FRAME_BYTES):
            if self.cancelled:
                return
            chunk = pcm_bytes[off:off + self.FRAME_BYTES]
            await play_pcm16(self.source, chunk)
            self.spoken_bytes += len(chunk)


# --------------------------------------------------------------------------- #
# Validators (no human needed)                                                 #
# --------------------------------------------------------------------------- #
async def validate_llm() -> bool:
    logger.info("=== VALIDATE gpt-audio-mini text->audio round-trip ===")
    http = httpx.AsyncClient()
    try:
        msgs = new_history()
        got = {"bytes": 0}

        async def cap(pcm):
            got["bytes"] += len(pcm)
        res = await _stream_turn(http, msgs + [{"role": "user", "content": "Say hello in five words."}], on_audio=cap)
        ok = res["audio_chunks"] > 0 and bool(res["transcript"])
        logger.info(f"[llm] chunks={res['audio_chunks']} pcm_bytes={got['bytes']} "
                    f"transcript={res['transcript']!r}")
        logger.info(f"[llm] {'PASS' if ok else 'FAIL'} - audio round-trip "
                    f"{'confirmed' if ok else 'NOT confirmed'}")
        return ok
    finally:
        await http.aclose()


async def validate_tools() -> bool:
    logger.info("=== VALIDATE tool-calling end-to-end (the agentic orch loop) ===")
    http = httpx.AsyncClient()
    try:
        history = new_history()
        got = {"bytes": 0}

        async def cap(pcm):
            got["bytes"] += len(pcm)
        spoken = await run_agent_turn(
            http, history,
            {"role": "user", "content": "List my workers and tell me their states."},
            on_audio=cap,
        )
        # confirm a list_workers tool call happened (tool messages now in history)
        called = any(
            m.get("role") == "assistant" and m.get("tool_calls")
            and any(tc["function"]["name"] == "list_workers" for tc in m["tool_calls"])
            for m in history
        )
        # confirm the spoken answer names a real mission
        live = _orch_call("mission.list", {})
        names = [r.get("name", "") for r in live]
        named_real = any(n and n.lower() in spoken.lower() for n in names)
        ok = called and bool(spoken) and got["bytes"] > 0 and named_real
        logger.info(f"[tools] list_workers called={called}  spoke={bool(spoken)}  "
                    f"audio_bytes={got['bytes']}  named_a_real_mission={named_real}")
        logger.info(f"[tools] {'PASS' if ok else 'FAIL'} - agentic orch tool loop "
                    f"{'confirmed' if ok else 'NOT confirmed'}")
        return ok
    finally:
        await http.aclose()


async def validate_audioin() -> bool:
    logger.info("=== VALIDATE gpt-audio-mini audio INPUT format ===")
    http = httpx.AsyncClient()
    try:
        sr = SR
        n = sr  # 1s
        i = np.arange(n)
        pcm = (3000 * np.sin(2 * np.pi * 220 * i / sr)).astype(np.int16)
        msgs = new_history() + [audio_user_msg(pcm, sr)]
        res = await _stream_turn(http, msgs)
        ok = res["audio_chunks"] > 0
        logger.info(f"[audioin] input_audio accepted; response chunks={res['audio_chunks']} "
                    f"transcript={res['transcript']!r}")
        logger.info(f"[audioin] {'PASS' if ok else 'FAIL'}")
        return ok
    finally:
        await http.aclose()


def validate_orch() -> bool:
    logger.info("=== VALIDATE orch tool handlers return clean JSON ===")
    ok = True
    # list_workers
    lw = dispatch_tool("list_workers", {})
    rows = json.loads(lw)
    logger.info(f"[orch] list_workers -> {len(rows)} workers")
    if not rows:
        logger.warning("[orch] no workers returned")
        ok = False
    # pick a mission to exercise the read tools
    mid = rows[0]["mission_id"] if rows else None
    if mid:
        for tool, args in [
            ("get_worker", {"mission_id": mid}),
            ("worker_screen", {"mission_id": mid, "lines": 10}),
            ("worker_events", {"mission_id": mid, "limit": 5}),
        ]:
            out = dispatch_tool(tool, args)
            try:
                parsed = json.loads(out)  # always valid JSON now
                if isinstance(parsed, dict) and "error" in parsed:
                    raise ValueError(parsed["error"])
                trunc = isinstance(parsed, dict) and parsed.get("_truncated")
                logger.info(f"[orch] {tool} -> clean JSON ({len(out)}B"
                            f"{', truncated' if trunc else ''})")
            except (json.JSONDecodeError, ValueError) as e:
                logger.warning(f"[orch] {tool} -> bad: {e} :: {out[:120]}")
                ok = False
    # host.inbox + defaults via passthrough
    json.loads(dispatch_tool("worker_inbox", {"limit": 3}))
    json.loads(dispatch_tool("orch_call", {"method": "defaults.get", "params": {}}))
    logger.info(f"[orch] worker_inbox + orch_call(defaults.get) -> clean JSON")
    logger.info(f"[orch] {'PASS' if ok else 'FAIL'}")
    return ok


async def validate_sfu() -> bool:
    logger.info("=== VALIDATE SFU connect + publish ===")
    http = httpx.AsyncClient()
    ok = False
    try:
        room, chain = await connect_sfu(http)
        source = await publish_ava_track(room)
        await post_membership(http, chain["lk_service_url"])
        for i in range(3):
            await asyncio.sleep(1)
            logger.info(f"t={i+1}s state={room.connection_state} "
                        f"participants={[p.identity for p in room.remote_participants.values()]}")
        ok = room.connection_state == rtc.ConnectionState.CONN_CONNECTED
        logger.info(f"[sfu] {'PASS' if ok else 'FAIL'} - state={room.connection_state}")
        await room.disconnect()
        await clear_membership(http)
    finally:
        await http.aclose()
    return ok


# --------------------------------------------------------------------------- #
# Per-participant turn capture: webrtcvad end-of-turn detection                #
# --------------------------------------------------------------------------- #
class TurnCapturer:
    """Consume a remote participant's 24kHz mono audio stream, detect end-of-turn
    with webrtcvad, and invoke on_turn(pcm_i16) once per finished utterance.

    Barge-in (full-duplex): we NEVER mute the user. VAD runs continuously even
    while Ava is speaking. A frame only counts as *real speech* if it passes BOTH
    gates: (a) webrtcvad (aggressiveness 3) says voiced, AND (b) its int16 RMS is
    clearly above the adaptively-estimated background noise floor. When Ava is
    speaking (``ava_speaking`` set by the orchestrator) and the user produces
    ~BARGE_MS of *continuous* frames that pass both gates, we fire
    ``on_barge_in()`` exactly once for that utterance. Any frame that fails either
    gate resets the contiguous-speech counter, so only a genuine continuous
    utterance accumulates - mic hiss / comfort noise can't drift over threshold.
    Crucially the frames that triggered the barge-in are NOT dropped: the
    utterance buffer keeps accumulating from the first real-speech frame, so when
    this utterance ends the normal ``on_turn`` fires with the full audio.

    webrtcvad wants 10/20/30ms frames of 16-bit mono PCM at 8/16/32/48kHz. We
    resample 24kHz -> 16kHz (3:2 decimate) only for the VAD/RMS decision; the WAV
    we actually send to gpt is the original 24kHz audio (best fidelity)."""

    VAD_SR = 16000
    FRAME_MS = 30
    VAD_FRAME = VAD_SR * FRAME_MS // 1000          # 480 samples @16k
    SRC_FRAME = SR * FRAME_MS // 1000              # 720 samples @24k
    SILENCE_HANG_MS = 700                          # end-of-turn after this much silence
    MIN_SPEECH_MS = 300                            # ignore blips
    MAX_TURN_MS = 30000

    def __init__(self, on_turn, on_barge_in=None):
        self.vad = webrtcvad.Vad(VAD_AGGRESSIVENESS)
        self.on_turn = on_turn
        self.on_barge_in = on_barge_in
        self._src_buf = np.zeros(0, dtype=np.int16)
        self._utter = bytearray()        # 24kHz int16 collected during speech
        self._speaking = False           # user is mid-utterance
        self._silence_ms = 0
        self._speech_ms = 0
        self._contig_speech_ms = 0       # run of *continuous* real-speech frames (barge gate)
        self._barged = False             # barge-in already fired for this utterance
        self.ava_speaking = False        # set by orchestrator while Ava's reply streams
        self._noise_floor = NOISE_FLOOR_INIT   # adaptive int16 RMS background estimate

    def _resample_24_to_16(self, x: np.ndarray) -> np.ndarray:
        # crude 3:2 decimation good enough for VAD energy detection
        if len(x) == 0:
            return x
        idx = (np.arange(len(x) * 2 // 3) * 3 // 2)
        return x[idx].astype(np.int16)

    @staticmethod
    def _rms(frame: np.ndarray) -> float:
        if len(frame) == 0:
            return 0.0
        # compute in float64 to avoid int16 overflow on the square
        return float(np.sqrt(np.mean(frame.astype(np.float64) ** 2)))

    async def feed(self, pcm_i16: np.ndarray):
        self._src_buf = np.concatenate([self._src_buf, pcm_i16])
        while len(self._src_buf) >= self.SRC_FRAME:
            src_frame = self._src_buf[:self.SRC_FRAME]
            self._src_buf = self._src_buf[self.SRC_FRAME:]
            vframe = self._resample_24_to_16(src_frame)
            if len(vframe) < self.VAD_FRAME:
                vframe = np.pad(vframe, (0, self.VAD_FRAME - len(vframe)))
            else:
                vframe = vframe[:self.VAD_FRAME]
            try:
                vad_voiced = self.vad.is_speech(vframe.tobytes(), self.VAD_SR)
            except Exception:  # noqa: BLE001
                vad_voiced = False

            # Loudness gate: frame must be clearly above the noise floor.
            rms = self._rms(vframe)
            loud_enough = rms >= max(RMS_ABS_MIN, self._noise_floor * RMS_NOISE_MULT)
            is_speech = vad_voiced and loud_enough

            if not is_speech:
                # Adapt the noise floor toward quiet/non-speech frames (EMA), so
                # the threshold tracks the ambient level. We only pull it toward
                # frames quieter than the current estimate or clearly non-speech,
                # which keeps real speech from inflating it.
                if not vad_voiced or rms < self._noise_floor:
                    self._noise_floor = min(
                        NOISE_FLOOR_MAX,
                        (1 - NOISE_FLOOR_ALPHA) * self._noise_floor
                        + NOISE_FLOOR_ALPHA * rms,
                    )

            if is_speech:
                self._speaking = True
                self._silence_ms = 0
                self._speech_ms += self.FRAME_MS
                self._contig_speech_ms += self.FRAME_MS
                self._utter += src_frame.tobytes()
                # Barge-in: Ava is talking and the user has produced a sustained
                # run of REAL-speech frames -> interrupt her. Fire once/utterance.
                if (self.ava_speaking and not self._barged
                        and self._contig_speech_ms >= BARGE_MS
                        and self.on_barge_in is not None):
                    self._barged = True
                    await self.on_barge_in()
            elif self._speaking:
                self._silence_ms += self.FRAME_MS
                self._contig_speech_ms = 0     # reset on any non-speech frame
                self._utter += src_frame.tobytes()
                if self._silence_ms >= self.SILENCE_HANG_MS:
                    await self._end_turn()
            else:
                self._contig_speech_ms = 0     # reset on any non-speech frame
            # length guard
            if self._speaking and (self._speech_ms + self._silence_ms) >= self.MAX_TURN_MS:
                await self._end_turn()

    async def _end_turn(self):
        if self._speech_ms >= self.MIN_SPEECH_MS and self._utter:
            pcm = np.frombuffer(bytes(self._utter), dtype=np.int16).copy()
            self._reset()
            await self.on_turn(pcm)
        else:
            self._reset()

    def _reset(self):
        self._utter = bytearray()
        self._speaking = False
        self._silence_ms = 0
        self._speech_ms = 0
        self._contig_speech_ms = 0
        self._barged = False


# --------------------------------------------------------------------------- #
# Full agent: SFU <-> gpt-audio-mini turn loop                                 #
# --------------------------------------------------------------------------- #
async def run_full(args):
    http = httpx.AsyncClient()
    llm_http = httpx.AsyncClient()  # separate client for the long SSE streams

    if args.wait_for_call:
        await wait_for_human_call(http, timeout=args.wait_timeout)
    room, chain = await connect_sfu(http)
    out_source = await publish_ava_track(room)
    await post_membership(http, chain["lk_service_url"])

    history = new_history()
    logger.info("rendering J-dawg filler clips…")
    fillers = await synth_filler_clips(llm_http)
    capturers: dict[str, TurnCapturer] = {}
    input_tasks: dict[str, asyncio.Task] = {}
    greeted = {"done": False}

    # Barge-in state. One logical turn runs at a time, but it is a *cancellable*
    # task rather than a blocking lock: a new utterance / barge-in cancels the
    # in-flight turn instead of being dropped or queued.
    turn_state = {
        "task": None,        # asyncio.Task running the current gpt turn (or None)
        "playback": None,    # Playback controller for the current turn (or None)
    }

    def _set_ava_speaking(v: bool):
        for cap in capturers.values():
            cap.ava_speaking = v

    async def _cancel_current_turn(reason: str):
        """Stop Ava NOW: flush queued output frames + cancel the in-flight gpt
        stream/playback task. Safe to call when nothing is running."""
        pb = turn_state["playback"]
        if pb is not None:
            pb.cancel()                       # flush/discard any buffered output
        task = turn_state["task"]
        if task is not None and not task.done():
            logger.info(f"barge-in: {reason} - stopping playback + cancelling stream")
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        turn_state["task"] = None
        turn_state["playback"] = None
        _set_ava_speaking(False)

    async def _run_turn(user_msg: dict, label: str):
        """Body of a single turn: stream gpt reply into a fresh Playback while Ava
        is marked speaking. Runs as a cancellable task."""
        pb = Playback(out_source)
        turn_state["playback"] = pb
        _set_ava_speaking(True)
        try:
            await run_agent_turn(llm_http, history, user_msg, on_audio=pb.push,
                                 fillers=fillers)
        except asyncio.CancelledError:
            logger.info(f"[{label}] turn cancelled (barge-in), spoken={pb.spoken_bytes}B")
            raise
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[{label}] error: {e!r}")
        finally:
            # small tail so the SFU flushes Ava's audio before we stop speaking
            if not pb.cancelled:
                await asyncio.sleep(0.3)
            _set_ava_speaking(False)
            if turn_state["playback"] is pb:
                turn_state["playback"] = None

    def _start_turn(user_msg: dict, label: str):
        task = asyncio.create_task(_run_turn(user_msg, label))
        turn_state["task"] = task

    async def on_barge_in(ident: str):
        # The user started talking over Ava. Stop her immediately. The triggering
        # audio is still buffered in the capturer and will become the next turn
        # when this utterance ends (no leading audio dropped).
        await _cancel_current_turn(f"user {ident} interrupted")

    async def handle_turn(pcm_i16: np.ndarray, ident: str):
        dur = len(pcm_i16) / SR
        logger.info(f"[turn] {ident} finished an utterance ({dur:.1f}s) -> gpt-audio-mini")
        # If a turn is somehow still running (e.g. user spoke a full new utterance
        # without a detected barge-in), cancel it so this new one takes over.
        await _cancel_current_turn(f"new utterance from {ident}")
        _start_turn(audio_user_msg(pcm_i16), "turn")

    async def greet():
        logger.info("[turn] greeting the new participant")
        await _cancel_current_turn("greet")
        _start_turn(
            {"role": "user", "content": "[The user just joined the voice call. Greet them in one short sentence, in character as J-dawg (JARVIS-style: calm, polite, a touch of dry wit, an 'sir' is welcome) and ask how you may be of service with their orch workers.]"},
            "greet",
        )

    async def pump_input(track: rtc.Track, ident: str):
        logger.info(f"capturing audio from {ident} (VAD turn detection, barge-in armed)")
        cap = TurnCapturer(
            on_turn=lambda pcm: handle_turn(pcm, ident),
            on_barge_in=lambda: on_barge_in(ident),
        )
        capturers[ident] = cap
        stream = rtc.AudioStream(track, sample_rate=SR, num_channels=CHANNELS)
        try:
            async for ev in stream:
                f = ev.frame
                pcm = np.frombuffer(f.data, dtype=np.int16)
                await cap.feed(pcm)
        except asyncio.CancelledError:
            pass
        finally:
            await stream.aclose()
            capturers.pop(ident, None)

    @room.on("track_subscribed")
    def _on_track(track, publication, participant):
        if track.kind == rtc.TrackKind.KIND_AUDIO:
            logger.info(f"track_subscribed: audio from {participant.identity}")
            input_tasks[participant.identity] = asyncio.create_task(
                pump_input(track, participant.identity))

    @room.on("participant_connected")
    def _on_join(p):
        logger.info(f"call participant joined: {p.identity}")
        if not greeted["done"]:
            greeted["done"] = True
            asyncio.create_task(greet())

    @room.on("participant_disconnected")
    def _on_leave(p):
        t = input_tasks.pop(p.identity, None)
        if t:
            t.cancel()

    # if someone is already in the room, greet now
    if room.remote_participants and not greeted["done"]:
        greeted["done"] = True
        asyncio.create_task(greet())

    logger.info("Ava is LIVE: SFU joined, gpt-audio-mini turn loop wired, orch "
                "tools armed. Waiting for the human in Element X. Speak; on "
                "end-of-turn Ava answers in her own voice and can read/control "
                "your orch workers.")

    async def report():
        while True:
            await asyncio.sleep(10)
            logger.info(f"[stats] state={room.connection_state} "
                        f"participants={[p.identity for p in room.remote_participants.values()]} "
                        f"history_msgs={len(history)}")

    report_task = asyncio.create_task(report())
    try:
        while room.connection_state == rtc.ConnectionState.CONN_CONNECTED:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        logger.info("shutting down…")
        report_task.cancel()
        ct = turn_state["task"]
        if ct and not ct.done():
            ct.cancel()
        for t in input_tasks.values():
            t.cancel()
        await room.disconnect()
        await clear_membership(http)
        await http.aclose()
        await llm_http.aclose()


async def run_hold(args):
    http = httpx.AsyncClient()
    try:
        room, chain = await connect_sfu(http)
        await post_membership(http, chain["lk_service_url"])
        logger.info(f"hold mode: staying connected {args.hold}s")
        for i in range(args.hold):
            await asyncio.sleep(1)
            parts = list(room.remote_participants.values())
            logger.info(f"t={i+1}s participants={[p.identity for p in parts]}")
        await room.disconnect()
        await clear_membership(http)
    finally:
        await http.aclose()


# --------------------------------------------------------------------------- #
def build_argparser():
    ap = argparse.ArgumentParser(description="MatrixRTC <-> gpt-audio-mini orch agent (Ava)")
    ap.add_argument("--validate-llm", action="store_true")
    ap.add_argument("--validate-tools", action="store_true")
    ap.add_argument("--validate-audioin", action="store_true")
    ap.add_argument("--validate-orch", action="store_true")
    ap.add_argument("--validate-sfu", action="store_true")
    ap.add_argument("--validate", action="store_true", help="run all no-human validations")
    ap.add_argument("--hold", type=int, default=0)
    ap.add_argument("--wait-for-call", action="store_true")
    ap.add_argument("--wait-timeout", type=int, default=300)
    return ap


async def main_async(args):
    if args.validate_llm:
        sys.exit(0 if await validate_llm() else 1)
    if args.validate_tools:
        sys.exit(0 if await validate_tools() else 1)
    if args.validate_audioin:
        sys.exit(0 if await validate_audioin() else 1)
    if args.validate_orch:
        sys.exit(0 if validate_orch() else 1)
    if args.validate_sfu:
        sys.exit(0 if await validate_sfu() else 1)
    if args.validate:
        orch_ok = validate_orch()
        llm_ok = await validate_llm()
        ain_ok = await validate_audioin()
        tools_ok = await validate_tools()
        sfu_ok = await validate_sfu()
        logger.info("=== VALIDATION SUMMARY ===")
        logger.info(f"  orch handlers : {'PASS' if orch_ok else 'FAIL'}")
        logger.info(f"  llm audio r/t : {'PASS' if llm_ok else 'FAIL'}")
        logger.info(f"  audio input   : {'PASS' if ain_ok else 'FAIL'}")
        logger.info(f"  tool-call loop: {'PASS' if tools_ok else 'FAIL'}")
        logger.info(f"  sfu connect   : {'PASS' if sfu_ok else 'FAIL'}")
        all_ok = all([orch_ok, llm_ok, ain_ok, tools_ok, sfu_ok])
        logger.info(f"=== {'ALL PASS' if all_ok else 'SOME FAILED'} ===")
        sys.exit(0 if all_ok else 1)
    if args.hold:
        await run_hold(args)
        return
    await run_full(args)


if __name__ == "__main__":
    args = build_argparser().parse_args()
    logger.remove()
    logger.add(sys.stderr, level="INFO")
    asyncio.run(main_async(args))
