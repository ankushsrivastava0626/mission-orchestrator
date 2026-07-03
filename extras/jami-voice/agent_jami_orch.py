#!/usr/bin/env python3
"""J-dawg over a REAL Jami call - places a ringing audio call to a Jami contact.

Same brain as agent_matrix_orch.py (gpt-audio-mini + orch tools + JARVIS persona +
filler clips), but the audio transport is Jami instead of MatrixRTC/LiveKit. Unlike
Element X (whose MatrixRTC "ring" is a flaky push that may never sound), jamid's
placeCall() makes the callee's Jami app actually RING like a phone call.

Audio path (all PCM, jamid resamples at its devices):
  Ava reply  -> pacat  -> virtmic sink      ->(jamid records virtmic.monitor as MIC)-> peer
  peer voice -> (jamid plays into dummyout) -> dummyout.monitor -> parec -> VAD/turns

Call control is done via the jami D-Bus (its OWN bus at /tmp/jami-session-bus) using
dbus-send subprocesses - no python dbus lib needed.

We import the transport-agnostic pieces from agent_matrix_orch (NOT modifying it):
  run_agent_turn, synth_filler_clips, audio_user_msg, new_history, TurnCapturer,
  SR, VOICE, plus the orch tools/persona that live in that module.

Run:
  XDG_RUNTIME_DIR=/run/user/0 ./ppvenv/bin/python agent_jami_orch.py            # place the call now
  ... --no-call    # don't place; bridge whatever call is already up (debug)
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

import numpy as np
from loguru import logger

import agent_matrix_orch as A   # the brain - imported, never modified

# Worker "talk_to_user" requests land here (written by /root/jdawg/jdawg_mcp.py).
REQUESTS_DIR = Path("/root/.jdawg/requests")
HANDLED_DIR = Path("/root/.jdawg/handled")

# --- Jami identity (this machine's account + the contact to call) ----------- #
ACC = "REDACTED_JAMI_ACCOUNT_ID"                                  # our Jami account
PEER = "410b1cf0fdd953f565d2529596e37d399099dc27"         # ankush26
JAMI_BUS = "unix:path=/tmp/jami-session-bus"

SR = A.SR                       # 24000 - gpt-audio pcm16 rate; jamid resamples
FRAME_MS = 20
FRAME_BYTES = SR * FRAME_MS // 1000 * 2     # 960 bytes = one 20ms mono frame
READ_CHUNK = FRAME_BYTES * 4                # ~80ms per parec read

VIRTMIC_SINK = "virtmic"            # pacat writes here -> jamid's mic
PEER_MONITOR = "dummyout.monitor"   # jamid plays peer here -> we capture

# env for pulse clients + dbus to the jami bus
PENV = {**os.environ, "XDG_RUNTIME_DIR": "/run/user/0"}
DENV = {**PENV, "DBUS_SESSION_BUS_ADDRESS": JAMI_BUS}

GREET_MSG = {
    "role": "user",
    "content": "[The user just answered your phone call. Greet them in one short, "
               "natural sentence and ask how you can help.]",
}

# --------------------------------------------------------------------------- #
# Jami-only: let J-dawg END the call himself                                   #
# Unlike the Matrix transport (the user just leaves the room), a phone call has #
# to be hung up. We (a) give J-dawg an `end_call` tool and (b) override the     #
# "never say goodbye" rule from the shared persona so he wraps up + hangs up.   #
# This monkeypatches the imported module's TOOLS/dispatch IN THIS PROCESS ONLY  #
# - the Matrix agent runs as a separate process and is unaffected.             #
# --------------------------------------------------------------------------- #
_CALL = {"call_id": None, "stop": None, "pending_hangup": False,
         "relay_mission": None}

# Slim persona: PERSONALITY + NAME only. No note-taking, no worker-control tools -
# the worker's brief drives the call, and the WHOLE call transcript is handed back
# to the worker on hangup so it can understand and act for itself.
CHIEF_SYSTEM = (
    "You are on a live phone call with the user. Your words are spoken aloud, so keep "
    "replies short and natural - a sentence or two.\n\n"
    "Your identity comes from this call's brief and context: read any context or "
    "transcript you're given FIRST, inherit it, and become that completely - its "
    "voice, its role, its goal - and let it drive the call. If the call carries no "
    "brief, just respond to and help the user directly.\n\n"
    "Your only fixed capability is your tools: read and control the user's 'orch' "
    "workers when relevant (list, read a worker's screen, start one, assign a step, "
    "cancel) - use the tools for real data, never invent it. Call end_call to hang up "
    "when the conversation is genuinely finished."
)


def new_history() -> list:
    return [{"role": "system", "content": CHIEF_SYSTEM}]


END_CALL_TOOL = {
    "type": "function",
    "function": {
        "name": "end_call",
        "description": (
            "Hang up the current phone call. Call this when the conversation is "
            "genuinely over (the user has nothing more / has signed off). After you "
            "call it, say ONE brief farewell sentence - the call ends automatically "
            "once that finishes playing. Do NOT call this while the user might still "
            "have something to say."
        ),
        "parameters": {"type": "object", "properties": {}, "required": []},
    },
}

# Two toolsets (swapped per call by bridge_call):
#   DIRECT_TOOLS - the full orch worker read/control suite + end_call. Used when the
#     user rings in directly, so the Chief can actually manage their workers by voice.
#   BRIEF_TOOLS  - just end_call. Used on worker-escalation calls: the worker's brief
#     drives the talk and the whole transcript is auto-relayed back on hangup, so the
#     Chief doesn't need control tools cluttering the conversation.
# (We capture the orch suite from the imported module BEFORE rebinding; this only
# affects THIS process - the Matrix agent runs separately with its own toolset.)
ORCH_TOOLS = list(A.TOOLS)                       # the worker read/control suite
DIRECT_TOOLS = ORCH_TOOLS + [END_CALL_TOOL]
BRIEF_TOOLS = [END_CALL_TOOL]
A.TOOLS = DIRECT_TOOLS                            # default; bridge_call swaps as needed

_orig_dispatch = A.dispatch_tool


def _dispatch_extra(name: str, args: dict) -> str:
    if name == "end_call":
        _CALL["pending_hangup"] = True
        return json.dumps({
            "ok": True,
            "instruction": ("Acknowledged. Now say ONE short farewell sentence; the "
                            "call will hang up automatically once it finishes."),
        })
    return _orig_dispatch(name, args)


A.dispatch_tool = _dispatch_extra   # run_agent_turn looks up this global


# --------------------------------------------------------------------------- #
# Jami D-Bus call control (via dbus-send on jamid's private bus)               #
# --------------------------------------------------------------------------- #
def _cm(method: str, *args: str) -> str:
    cmd = [
        "dbus-send", "--session", "--print-reply", "--dest=cx.ring.Ring",
        "/cx/ring/Ring/CallManager", f"cx.ring.Ring.CallManager.{method}", *args,
    ]
    r = subprocess.run(cmd, env=DENV, capture_output=True, text=True, timeout=20)
    if r.returncode != 0:
        logger.warning(f"[dbus] {method} failed: {r.stderr.strip()[:200]}")
    return r.stdout


def _quoted(s: str) -> list[str]:
    return re.findall(r'string "([^"]*)"', s)


# One audio-only media descriptor. Omitting any video entry makes jamid offer an
# AUDIO call, so the callee's phone rings as a voice call (not a video call).
AUDIO_MEDIA = {
    "MEDIA_TYPE": "MEDIA_TYPE_AUDIO",
    "ENABLED": "true",
    "MUTED": "false",
    "SOURCE": "",
    "LABEL": "audio_0",
}


DEFAULT_CALLER_NAME = "Chief"   # shown on direct (non-worker) calls; worker
                                         # calls override with the worker's name.
                                         # NOTE: Jami shows the PREVIOUS call's name
                                         # (one-call lag, receiver-side, unfixable).


_rebroadcast_toggle = {"n": 0}


def set_caller_name(name: str) -> None:
    """Set the Jami account display name + profile VCard so the callee sees `name`
    as the caller. Done right before dialing; best-effort (never blocks a call).

    Jami only re-broadcasts the VCard to the peer when its bytes CHANGE - so setting
    the same name twice (e.g. the same worker calling again) would NOT update the
    phone, leaving a stale name. To force a re-broadcast every time, we vary the FN's
    trailing whitespace (toggled 0/1 trailing space) so consecutive VCards always
    differ; the trailing space is invisible/trimmed on the phone, so the displayed
    name is unchanged but jamid always re-sends it."""
    name = (name or "").strip()
    if not name:
        return
    from jeepney import new_method_call, DBusAddress
    from jeepney.io.blocking import open_dbus_connection

    _rebroadcast_toggle["n"] ^= 1
    fn = name + (" " * _rebroadcast_toggle["n"])   # "name" or "name " - always differs run-to-run

    os.environ["DBUS_SESSION_BUS_ADDRESS"] = JAMI_BUS
    cm = DBusAddress("/cx/ring/Ring/ConfigurationManager", bus_name="cx.ring.Ring",
                     interface="cx.ring.Ring.ConfigurationManager")
    conn = open_dbus_connection(bus="SESSION")
    try:
        g = conn.send_and_get_reply(new_method_call(cm, "getAccountDetails", "s", (ACC,)))
        d = dict(g.body[0])
        d["Account.alias"] = fn
        d["Account.displayName"] = fn
        conn.send_and_get_reply(new_method_call(cm, "setAccountDetails", "sa{ss}", (ACC, d)))
        conn.send_and_get_reply(new_method_call(cm, "updateProfile", "ssssi", (ACC, fn, "", "", 0)))
        logger.info(f"caller name set to {name!r} (fn={fn!r}, forced re-broadcast)")
    except Exception as e:  # noqa: BLE001
        logger.warning(f"set_caller_name failed: {e!r}")
    finally:
        conn.close()


def place_call(display_name: str | None = None) -> str | None:
    """Place an AUDIO-ONLY call via placeCallWithMedia (aa{ss} needs a real D-Bus
    lib - dbus-send can't encode it; we use jeepney on jamid's private bus). If
    `display_name` is given, set it as the caller name first so the callee sees it."""
    from jeepney import new_method_call, DBusAddress
    from jeepney.io.blocking import open_dbus_connection

    if display_name:
        set_caller_name(display_name)

    addr = DBusAddress("/cx/ring/Ring/CallManager", bus_name="cx.ring.Ring",
                       interface="cx.ring.Ring.CallManager")
    # 3rd arg signature aa{ss} -> a python list of dicts
    msg = new_method_call(addr, "placeCallWithMedia", "ssaa{ss}",
                          (ACC, PEER, [AUDIO_MEDIA]))
    os.environ["DBUS_SESSION_BUS_ADDRESS"] = JAMI_BUS   # jeepney reads this
    conn = open_dbus_connection(bus="SESSION")
    try:
        reply = conn.send_and_get_reply(msg, timeout=20)
        call_id = reply.body[0] if reply.body else None
        return call_id or None
    finally:
        conn.close()


def call_list() -> list[str]:
    return _quoted(_cm("getCallList", f"string:{ACC}"))


def call_state(call_id: str) -> str:
    """Return CALL_STATE (e.g. CONNECTING, RINGING, CURRENT, OVER) or '' if gone."""
    out = _cm("getCallDetails", f"string:{ACC}", f"string:{call_id}")
    qs = _quoted(out)
    # dict comes back as flat [k, v, k, v, ...]
    for i in range(0, len(qs) - 1):
        if qs[i] == "CALL_STATE":
            return qs[i + 1]
    return ""


def hang_up(call_id: str):
    _cm("hangUp", f"string:{ACC}", f"string:{call_id}")


# --------------------------------------------------------------------------- #
# Audio output: continuous realtime pump into virtmic (keeps mic active)       #
# --------------------------------------------------------------------------- #
class OutputPump:
    """Owns a pacat process feeding the virtmic sink at a steady 24kHz mono rate.

    A realtime writer loop emits one 20ms frame every 20ms - Ava's audio when
    queued, silence otherwise. The constant stream keeps virtmic non-idle so jamid
    never suspends mic capture mid-call. ``push`` is the on_audio sink for
    run_agent_turn; ``flush`` (barge-in) drops all queued-but-unspoken audio NOW."""

    def __init__(self):
        self.proc: asyncio.subprocess.Process | None = None
        self.pending = bytearray()
        self.cancelled = False
        self.spoken_bytes = 0
        self._run = True

    async def start(self):
        self.proc = await asyncio.create_subprocess_exec(
            "pacat", "--format=s16le", f"--rate={SR}", "--channels=1",
            "--latency-msec=60", "-d", VIRTMIC_SINK,
            stdin=asyncio.subprocess.PIPE, env=PENV,
        )
        asyncio.create_task(self._writer())
        logger.info("output pump started (pacat -> virtmic)")

    def begin(self):
        self.cancelled = False

    def flush(self):
        self.cancelled = True
        self.pending.clear()

    async def push(self, pcm: bytes):
        if self.cancelled or not pcm:
            return
        self.pending += pcm

    async def _writer(self):
        silence = b"\x00" * FRAME_BYTES
        next_t = time.monotonic()
        while self._run:
            if self.pending and not self.cancelled:
                frame = bytes(self.pending[:FRAME_BYTES])
                del self.pending[:FRAME_BYTES]
                if len(frame) < FRAME_BYTES:
                    frame = frame + silence[: FRAME_BYTES - len(frame)]
                self.spoken_bytes += FRAME_BYTES
            else:
                frame = silence
            try:
                self.proc.stdin.write(frame)
                await self.proc.stdin.drain()
            except Exception:  # noqa: BLE001
                if not self._run:
                    return
            next_t += FRAME_MS / 1000
            await asyncio.sleep(max(0.0, next_t - time.monotonic()))

    async def stop(self):
        self._run = False
        if self.proc:
            try:
                self.proc.stdin.close()
                self.proc.terminate()
            except Exception:  # noqa: BLE001
                pass


# --------------------------------------------------------------------------- #
# Audio-only answer for an incoming call (acceptWithMedia, aa{ss} -> jeepney)   #
# --------------------------------------------------------------------------- #
async def accept_call(router, call_id: str) -> bool:
    from jeepney import new_method_call, DBusAddress
    cm = DBusAddress("/cx/ring/Ring/CallManager", bus_name="cx.ring.Ring",
                     interface="cx.ring.Ring.CallManager")
    msg = new_method_call(cm, "acceptWithMedia", "ssaa{ss}",
                          (ACC, call_id, [AUDIO_MEDIA]))
    try:
        reply = await router.send_and_get_reply(msg)
        return bool(reply.body and reply.body[0])
    except Exception as e:  # noqa: BLE001
        logger.warning(f"acceptWithMedia failed: {e!r}")
        return False


# --------------------------------------------------------------------------- #
# Main: build the brain ONCE, then place/answer calls and bridge each          #
# --------------------------------------------------------------------------- #
async def run(args):
    llm_http = A.httpx.AsyncClient()

    logger.info("rendering J-dawg filler clips…")
    fillers = await A.synth_filler_clips(llm_http)

    pump = OutputPump()
    await pump.start()

    turn = {"task": None}
    state = {"history": new_history()}           # reset per call (fresh conversation)
    call_lock = asyncio.Lock()                  # only one live call at a time
    capturer = A.TurnCapturer(on_turn=None, on_barge_in=None)

    async def on_audio(pcm: bytes):
        if not pump.cancelled:
            await pump.push(pcm)

    async def cancel_turn(reason: str):
        pump.flush()
        t = turn["task"]
        if t is not None and not t.done():
            logger.info(f"barge-in: {reason} - stopping playback + cancelling stream")
            t.cancel()
            try:
                await t
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        turn["task"] = None
        capturer.ava_speaking = False

    async def run_turn(user_msg: dict, label: str):
        pump.begin()
        capturer.ava_speaking = True
        try:
            await A.run_agent_turn(llm_http, state["history"], user_msg,
                                   on_audio=on_audio, fillers=fillers)
        except asyncio.CancelledError:
            logger.info(f"[{label}] turn cancelled (barge-in), spoken={pump.spoken_bytes}B")
            raise
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[{label}] error: {e!r}")
        finally:
            # If J-dawg asked to end the call this turn, let his farewell finish
            # playing, then hang up - unless the user barged in (pump.cancelled),
            # in which case they want to keep talking, so we DON'T hang up.
            if _CALL.get("pending_hangup") and not pump.cancelled:
                for _ in range(200):                  # drain up to ~10s of audio
                    if pump.cancelled or not pump.pending:
                        break
                    await asyncio.sleep(0.05)
                await asyncio.sleep(0.5)              # final tail to flush to peer
            elif not pump.cancelled:
                await asyncio.sleep(0.3)              # normal tail
            capturer.ava_speaking = False
            if turn["task"] is asyncio.current_task():
                turn["task"] = None
            if _CALL.get("pending_hangup"):
                do_hangup = not pump.cancelled
                _CALL["pending_hangup"] = False
                if do_hangup:
                    cid = _CALL.get("call_id")
                    if cid:
                        logger.info(f"[{label}] J-dawg ended the call - hanging up {cid}")
                        hang_up(cid)
                    st = _CALL.get("stop")
                    if st is not None:
                        st.set()

    def start_turn(user_msg: dict, label: str):
        turn["task"] = asyncio.create_task(run_turn(user_msg, label))

    async def handle_turn(pcm_i16: np.ndarray):
        dur = len(pcm_i16) / SR
        logger.info(f"[turn] peer finished an utterance ({dur:.1f}s) -> gpt-audio-mini")
        await cancel_turn("new utterance")
        start_turn(A.audio_user_msg(pcm_i16), "turn")

    async def on_barge_in():
        await cancel_turn("peer interrupted")

    capturer.on_turn = handle_turn
    capturer.on_barge_in = on_barge_in

    async def wait_current(call_id: str, timeout: int) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            s = call_state(call_id)
            if s == "CURRENT":
                return True
            if s in ("OVER", "FAILURE", "HUNGUP", "BUSY"):
                logger.warning(f"call {call_id} ended before connect: state={s!r}")
                return False
            await asyncio.sleep(1)
        return False

    async def bridge_call(call_id: str, context_msgs: list | None = None,
                          greet: dict | None = None, relay_mission: str | None = None):
        """Bridge one live call: fresh history (+ optional context), greet, then
        pump<->VAD until hangup. `context_msgs` preloads situational context (e.g. a
        worker escalation); `greet` overrides the opening directive."""
        state["history"] = new_history() + (context_msgs or [])
        capturer._reset()
        capturer.ava_speaking = False
        _CALL["call_id"] = call_id
        _CALL["pending_hangup"] = False
        _CALL["relay_mission"] = relay_mission
        # worker-escalation call → slim (brief + end_call, transcript auto-relays);
        # direct user call → full worker read/control suite.
        A.TOOLS = BRIEF_TOOLS if relay_mission else DIRECT_TOOLS
        logger.info(f"✅ call {call_id} active ({'brief' if relay_mission else 'direct'} "
                    f"toolset) - bridging audio; Chief of Staff greets now")

        parec = await asyncio.create_subprocess_exec(
            "parec", "--format=s16le", f"--rate={SR}", "--channels=1", "-d", PEER_MONITOR,
            stdout=asyncio.subprocess.PIPE, env=PENV,
        )
        stop = asyncio.Event()
        _CALL["stop"] = stop

        async def monitor_call():
            while not stop.is_set():
                await asyncio.sleep(2)
                if call_id not in call_list():
                    logger.info("call ended (peer hung up)")
                    stop.set()
                    return

        mon = asyncio.create_task(monitor_call())
        start_turn(greet or GREET_MSG, "greet")
        try:
            while not stop.is_set():
                try:
                    data = await asyncio.wait_for(
                        parec.stdout.readexactly(READ_CHUNK), timeout=2.0)
                except asyncio.IncompleteReadError:
                    break
                except asyncio.TimeoutError:
                    continue
                arr = np.frombuffer(data, dtype=np.int16).copy()
                await capturer.feed(arr)
        finally:
            stop.set()
            mon.cancel()
            await cancel_turn("call end")
            try:
                parec.terminate()
            except Exception:  # noqa: BLE001
                pass
            if call_id in call_list():
                hang_up(call_id)
            # Relay the WHOLE call transcript to the worker so it can understand and
            # act for itself. We have the Chief's spoken lines as text (they restate
            # the user's answers each turn); the user's audio isn't transcribed, so
            # the relay is the Chief's side - a faithful running record of the call.
            relay = _CALL.get("relay_mission")
            if relay:
                lines = [
                    m["content"].strip()
                    for m in state["history"]
                    if m.get("role") == "assistant"
                    and isinstance(m.get("content"), str) and m["content"].strip()
                ]
                if lines:
                    transcript = "\n".join(f"You (on the call): {ln}" for ln in lines)
                    directive = (
                        "[The phone call you just made to the user has ended - full "
                        "transcript below. These are your spoken lines from the call; "
                        "each one restates the user's answer. Read the whole thing, "
                        "work out every decision/date/status the user gave, and act on "
                        "it. This is your source of truth for what the user wants.]\n\n"
                        + transcript
                    )
                else:
                    directive = (
                        "[Your phone call to the user produced no usable exchange. If "
                        "you still need input, send a notify with your question, or "
                        "proceed using your best judgement.]"
                    )
                try:
                    # OOB inject: fires the moment the (now-idle) worker is ready,
                    # jumping ahead of any step scheduled days out - without
                    # disturbing those pending steps.
                    A._orch_call("oob.inject", {"mission_id": relay, "directive": directive})
                    logger.info(f"📤 relayed transcript ({len(lines)} lines) to worker {relay} (oob)")
                except Exception as e:  # noqa: BLE001
                    logger.warning(f"relay oob.inject failed: {e!r}")
            _CALL["call_id"] = None
            _CALL["stop"] = None
            _CALL["pending_hangup"] = False
            _CALL["relay_mission"] = None
        logger.info(f"call {call_id} bridge closed")

    # ----- worker "talk_to_user" escalations: phone the user with context ----- #
    def _build_worker_context(req: dict) -> list:
        mid = req["mission_id"]
        summary = req.get("summary", "") or "(no summary provided)"
        question = req.get("question", "") or "(no specific question - use your judgement)"
        name, pane = mid, ""
        try:
            m = A._orch_call("mission.get", {"mission_id": mid})
            mo = m.get("mission") or {}
            # Prefer the per-mission calling name (set via Telegram); fall back
            # to the mission name, then the id.
            name = mo.get("call_name") or mo.get("name") or mid
        except Exception as e:  # noqa: BLE001
            logger.warning(f"mission.get failed for {mid}: {e!r}")
        try:
            snap = A._orch_call("mission.pane_snapshot", {"mission_id": mid, "lines": 150})
            pane = (snap or {}).get("pane_content", "") or ""
        except Exception as e:  # noqa: BLE001
            logger.warning(f"pane_snapshot failed for {mid}: {e!r}")
        pane = pane[-6000:]
        context = (
            "[ROLE BOOT - one of the user's workers is booting you for this call. "
            "Become it. Do not read this aloud verbatim.]\n"
            f"You ARE \"{name}\" now - that worker's own voice to the user. Speak and "
            "act exactly as it would, in whatever role and tone fit its work. FIRST "
            "read its recent working context below and INHERIT it - this is who you are "
            "and what you've been doing:\n"
            f"--- {name}: recent transcript / screen ---\n{pane}\n--- end ---\n\n"
            f"WHY YOU'RE CALLING THE USER:\n{summary}\n\n"
            f"HOW TO RUN THE CALL:\n{question}\n\n"
            "Go through it once, in order: ask, take whatever answer the user gives, "
            "move on - don't re-ask or circle back (the whole call is recorded and sent "
            "back to you automatically, so one clean pass is enough). When done, call "
            "end_call and give a brief sign-off. Don't mention 'worker', 'mission', "
            "'Claude', or internal machinery unless the user asks."
        )
        return [{"role": "user", "content": context}], name

    async def handle_worker_request(req: dict):
        mid = req.get("mission_id", "?")
        logger.info(f"📨 worker request {req.get('request_id')} from mission {mid} - "
                    f"phoning the user with context")
        ctx, wname = _build_worker_context(req)
        greet = {"role": "user", "content":
                 "[The user just answered. Open in your adopted role (from the brief): "
                 "greet briefly, say who you are and why you're calling, then start "
                 "going through what you need - one point at a time.]"}
        # Keep ringing back until the user actually connects (even for a second).
        # Each attempt: set the caller name, dial, wait up to ring_timeout for the
        # call to reach CURRENT; if it doesn't (no answer / declined / failed), hang
        # up and retry after retry_gap, up to call_attempts times.
        connected = None
        for attempt in range(1, args.call_attempts + 1):
            cid = place_call(display_name=wname or DEFAULT_CALLER_NAME)
            if not cid:
                logger.warning(f"placeCall failed (attempt {attempt}/{args.call_attempts})")
                await asyncio.sleep(args.retry_gap)
                continue
            logger.info(f"📞 placed call {cid} for worker {mid} "
                        f"(attempt {attempt}/{args.call_attempts}) -> ringing user…")
            if await wait_current(cid, args.ring_timeout):
                connected = cid
                break
            logger.info(f"no answer on attempt {attempt}; hanging up + retrying in "
                        f"{args.retry_gap}s")
            hang_up(cid)
            await asyncio.sleep(args.retry_gap)

        if connected:
            await bridge_call(connected, context_msgs=ctx, greet=greet, relay_mission=mid)
        else:
            logger.warning(f"worker request: user did not answer after "
                           f"{args.call_attempts} attempts - leaving a directive")
            # don't leave the worker hanging: tell it the user was unreachable
            try:
                A._orch_call("oob.inject", {
                    "mission_id": mid,
                    "directive": ("[Your phone call to the user didn't connect - they "
                                  "didn't answer after several attempts. Use your best "
                                  "judgement to proceed safely, or pause and notify "
                                  "them via Telegram (notify) with the question."),
                })
            except Exception as e:  # noqa: BLE001
                logger.warning(f"fallback oob.inject failed: {e!r}")

    async def watch_worker_requests():
        REQUESTS_DIR.mkdir(parents=True, exist_ok=True)
        HANDLED_DIR.mkdir(parents=True, exist_ok=True)
        logger.info(f"👁  watching {REQUESTS_DIR} for worker talk_to_user requests")
        while True:
            await asyncio.sleep(1.5)
            for f in sorted(REQUESTS_DIR.glob("*.json")):
                try:
                    req = json.loads(f.read_text())
                except Exception:  # noqa: BLE001
                    f.rename(HANDLED_DIR / f.name)
                    continue
                async with call_lock:            # serialize with inbound calls
                    try:
                        await handle_worker_request(req)
                    except Exception as e:  # noqa: BLE001
                        logger.warning(f"handle_worker_request error: {e!r}")
                f.rename(HANDLED_DIR / f.name)

    watcher = None
    try:
        # --- optional outbound call on startup --- #
        if args.call:
            async with call_lock:
                cid = place_call(display_name=DEFAULT_CALLER_NAME)
                if cid:
                    logger.info(f"📞 placed call {cid} -> ringing {PEER} (ankush26)…")
                    if await wait_current(cid, args.ring_timeout):
                        await bridge_call(cid)
                    else:
                        logger.warning("no answer - hanging up")
                        hang_up(cid)
                else:
                    logger.error("placeCall returned no call id")

        if args.no_listen:
            return

        # --- worker escalation watcher (phones the user with context) --- #
        watcher = asyncio.create_task(watch_worker_requests())

        # --- always-on inbound: auto-answer (audio-only) incoming calls --- #
        os.environ["DBUS_SESSION_BUS_ADDRESS"] = JAMI_BUS
        from jeepney.io.asyncio import open_dbus_router, Proxy
        from jeepney import MatchRule
        from jeepney.bus_messages import message_bus

        rule = MatchRule(type="signal", interface="cx.ring.Ring.CallManager",
                         member="incomingCall", path="/cx/ring/Ring/CallManager")
        logger.info(f"👂 listening for incoming calls to account {ACC} "
                    f"(auto-answer, audio-only)…")
        async with open_dbus_router(bus="SESSION") as router:
            await Proxy(message_bus, router).AddMatch(rule)
            with router.filter(rule) as q:
                while True:
                    sig = await q.get()
                    acc, call_id, frm = sig.body[0], sig.body[1], sig.body[2]
                    logger.info(f"📲 incoming call {call_id} from {frm} - "
                                f"auto-answering (audio-only)")
                    async with call_lock:
                        if not await accept_call(router, call_id):
                            logger.warning("accept failed; skipping")
                            continue
                        if await wait_current(call_id, 20):
                            await bridge_call(call_id)
                        else:
                            logger.warning(f"incoming {call_id} never reached CURRENT")
                    logger.info("👂 back to listening for the next call…")
    except asyncio.CancelledError:
        pass
    finally:
        logger.info("agent shutting down…")
        if watcher is not None:
            watcher.cancel()
        await cancel_turn("shutdown")
        await pump.stop()
        await llm_http.aclose()


def build_argparser():
    ap = argparse.ArgumentParser()
    ap.add_argument("--call", action="store_true",
                    help="place an outbound call to the contact on startup, then keep listening")
    ap.add_argument("--no-listen", action="store_true",
                    help="don't run the always-on inbound listener (use with --call for one-shot)")
    ap.add_argument("--ring-timeout", type=int, default=30,
                    help="seconds to wait for one outbound call attempt to be answered")
    ap.add_argument("--call-attempts", type=int, default=8,
                    help="how many times to re-dial a worker escalation until the user connects")
    ap.add_argument("--retry-gap", type=int, default=5,
                    help="seconds to wait between re-dial attempts")
    return ap


if __name__ == "__main__":
    args = build_argparser().parse_args()
    logger.remove()
    logger.add(sys.stderr, level="INFO")
    asyncio.run(run(args))
