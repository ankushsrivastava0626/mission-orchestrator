"""Cue engine + scheduler. Runs as an asyncio task inside the daemon."""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
from typing import Any

from . import config, db, runner, telegram, vault

log = logging.getLogger(__name__)


class Engine:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self._stop = asyncio.Event()
        # Per-mission flag: an OOB (heartbeat / on_step_complete) directive is
        # awaiting the current step to finish.
        self._pending_oob: dict[str, list[dict[str, Any]]] = {}
        # Missions that should finalize WITHOUT a wrap-up summary on the next
        # idle tick (used for reply-driven one-shot interactions, where the
        # worker has already responded to the user via notify).
        self._suppress_wrapup: set[str] = set()

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        log.info("engine: starting")
        while not self._stop.is_set():
            try:
                self.tick()
            except Exception:
                log.exception("engine tick failed")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=config.TICK_INTERVAL_S)
            except asyncio.TimeoutError:
                pass
        log.info("engine: stopped")

    # ---------- tick ----------

    def tick(self) -> None:
        missions = db.list_missions(self.conn)
        now = db.now_ts()
        for m in missions:
            if m["state"] == "running":
                self._tick_mission(m, now)
            elif m["state"] == "cancelling":
                self._tick_cancelling(m, now)

    def _tick_mission(self, m: sqlite3.Row, now: int) -> None:
        mission_id = m["id"]

        # 1. Detect step completion.
        running = self.conn.execute(
            "SELECT * FROM steps WHERE mission_id = ? AND state = 'running'"
            " ORDER BY position ASC LIMIT 1",
            (mission_id,),
        ).fetchone()
        if running is not None and not runner.step_running(mission_id):
            db.set_step_state(self.conn, running["id"], "completed", finished=True)
            db.log_event(
                self.conn,
                mission_id=mission_id,
                kind="step_completed",
                step_id=running["id"],
            )
            self._on_step_complete(mission_id, running)
            running = None

        # claude_busy: running is None in DB (no tracked step) but claude is still
        # the foreground process in tmux - i.e. an OOB is in flight. We must NOT
        # fire another OOB or send keys into the pane in this state.
        claude_busy = running is None and runner.step_running(mission_id)

        # 2. If any OOB queued and claude is idle, fire one now.
        if running is None and not claude_busy and self._pending_oob.get(mission_id):
            directive = self._pending_oob[mission_id].pop(0)
            self._launch_oob(mission_id, directive)
            return

        # 3. Heartbeat. Queue is safe to grow regardless of busy state.
        interval = int(m["heartbeat_interval_s"])
        last = m["last_heartbeat_at"] or m["created_at"]
        if now - int(last) >= interval:
            self._enqueue_oob(
                mission_id,
                {"kind": "heartbeat", "directive": config.HEARTBEAT_DIRECTIVE},
            )
            db.update_mission_heartbeat(self.conn, mission_id, now)
            db.log_event(
                self.conn, mission_id=mission_id, kind="heartbeat_fired"
            )

        # 4. on_schedule pings. Same - queue is safe to grow.
        for ping in db.list_pings(self.conn, mission_id):
            if ping["mode_type"] != "on_schedule":
                continue
            ping_interval = ping["interval_s"] or 0
            ping_last = ping["last_fired_at"] or m["created_at"]
            if ping_interval > 0 and now - int(ping_last) >= int(ping_interval):
                self._enqueue_oob(
                    mission_id,
                    {
                        "kind": "ping",
                        "ping_id": ping["id"],
                        "directive": self._ping_directive(ping["command"]),
                    },
                )
                db.mark_ping_fired(self.conn, ping["id"], now)

        # 4b. Scripted-ping watchdog: re-task the worker to repair silent scripts.
        self._tick_scripted_pings(mission_id, now)

        # 5. Pop next pending step if its cue is satisfied AND claude is idle.
        if running is None and not claude_busy and not self._pending_oob.get(mission_id):
            self._maybe_launch_next_step(mission_id, now)

        # 6. Auto-complete if no work remains and claude has exited.
        if running is None and not claude_busy and not self._pending_oob.get(mission_id):
            self._maybe_complete_mission(m, now)

    # ---------- helpers ----------

    @staticmethod
    def _ping_directive(command: str) -> str:
        return (
            f"{command}\n\nWhen done, post the result to the user via the `notify` "
            f"tool from the orch MCP server."
        )

    def _enqueue_oob(self, mission_id: str, payload: dict[str, Any]) -> None:
        self._pending_oob.setdefault(mission_id, []).append(payload)

    def _launch_oob(self, mission_id: str, payload: dict[str, Any]) -> None:
        try:
            first = not db.session_started(self.conn, mission_id)
            runner.launch_step(mission_id, payload["directive"], first_step=first)
            db.log_event(
                self.conn,
                mission_id=mission_id,
                kind=f"oob_{payload['kind']}_launched",
                payload=payload,
            )
        except runner.RunnerError as e:
            log.error("oob launch failed for %s: %s", mission_id, e)
            db.log_event(
                self.conn,
                mission_id=mission_id,
                kind="oob_launch_failed",
                payload={"error": str(e), **payload},
            )

    def _on_step_complete(self, mission_id: str, step: sqlite3.Row) -> None:
        # Fire all on_step_complete pings.
        for ping in db.list_pings(self.conn, mission_id):
            if ping["mode_type"] != "on_step_complete":
                continue
            self._enqueue_oob(
                mission_id,
                {
                    "kind": "ping",
                    "ping_id": ping["id"],
                    "directive": self._ping_directive(ping["command"]),
                },
            )
            db.mark_ping_fired(self.conn, ping["id"], db.now_ts())

    def _maybe_launch_next_step(self, mission_id: str, now: int) -> None:
        # "Run-next" cues (on_current_complete*) jump ahead of the rest of the
        # queue: a worker can queue a step to run NEXT even when a long tail of
        # steps is already pending. Among run-next steps, oldest (lowest
        # position) first, so multiple interjections keep their insertion order.
        nxt = self.conn.execute(
            "SELECT * FROM steps WHERE mission_id = ? AND state = 'pending'"
            " ORDER BY CASE WHEN cue_type = 'on_current_complete'"
            " THEN 0 ELSE 1 END, position ASC LIMIT 1",
            (mission_id,),
        ).fetchone()
        if nxt is None:
            return

        cue = json.loads(nxt["cue_payload"]) if nxt["cue_payload"] else {"type": nxt["cue_type"]}
        prev = db.previous_step(self.conn, mission_id, nxt["position"])
        first_step = prev is None

        ready = False
        if cue["type"] == "immediate":
            ready = first_step or (prev is not None and prev["state"] in {"completed"})
        elif cue["type"] == "on_current_complete":
            # We're only here when no step is running, i.e. the CURRENT step has
            # already finished - so a run-next step is ready immediately. (It also
            # sorted to the front above, so it preempts the rest of the queue.)
            ready = True
        elif cue["type"] == "on_prev_complete":
            # Legacy: gate on the immediately-preceding step (tail-chained plans).
            ready = prev is not None and prev["state"] in {"completed"}
        elif cue["type"] == "on_timeout":
            seconds = int(cue.get("seconds", 0))
            if prev is not None and prev["started_at"] is not None:
                ready = now - int(prev["started_at"]) >= seconds
            elif prev is None:
                # No previous step to anchor the timer to; treat as immediate.
                ready = True
        elif cue["type"] == "at_time":
            # Absolute wall-clock trigger. Fires once now >= the target epoch,
            # regardless of the previous step. (Launch still only happens when
            # the worker is idle, guarded by the caller.)
            target = int(cue.get("epoch", 0))
            ready = target > 0 and now >= target
        else:
            log.warning("unknown cue type for step %s: %s", nxt["id"], cue["type"])
            return

        if not ready:
            return

        try:
            # The first Claude launch (step or OOB) creates the session with
            # --session-id; everything after resumes it.
            session_first = not db.session_started(self.conn, mission_id)
            runner.launch_step(mission_id, nxt["directive"], first_step=session_first)
            db.set_step_state(self.conn, nxt["id"], "running", started=True)
            db.log_event(
                self.conn,
                mission_id=mission_id,
                kind="step_launched",
                step_id=nxt["id"],
            )
        except runner.RunnerError as e:
            log.error("step launch failed for %s: %s", nxt["id"], e)
            db.set_step_state(self.conn, nxt["id"], "failed", finished=True)
            db.log_event(
                self.conn,
                mission_id=mission_id,
                kind="step_launch_failed",
                step_id=nxt["id"],
                payload={"error": str(e)},
            )

    # ---------- scripted ping watchdog ----------

    def _tick_scripted_pings(self, mission_id: str, now: int) -> None:
        for sp in db.list_scripted_pings(self.conn, mission_id):
            if sp["state"] != "active":
                continue
            last = sp["last_alive_at"] or 0
            if now - int(last) <= int(sp["timeout_s"]):
                continue
            # Script went silent - mark broken (stops re-nagging) and ask the
            # worker to repair it.
            db.set_scripted_ping_state(self.conn, sp["id"], "broken")
            directive = config.SCRIPTED_PING_REPAIR_DIRECTIVE.format(
                spid=sp["id"], timeout_s=sp["timeout_s"],
                condition=sp["condition"], script_path=sp["script_path"] or "(unknown)",
            )
            self._enqueue_oob(
                mission_id, {"kind": "scripted_ping_repair", "directive": directive}
            )
            db.log_event(
                self.conn, mission_id=mission_id, kind="scripted_ping_silent",
                payload={"spid": sp["id"]},
            )

    # ---------- soft cancel ----------

    def _tick_cancelling(self, m: sqlite3.Row, now: int) -> None:
        mission_id = m["id"]
        if runner.step_running(mission_id):
            # Wait - goodbye OOB or interrupted step is still wrapping up.
            return
        if self._pending_oob.get(mission_id):
            directive = self._pending_oob[mission_id].pop(0)
            self._launch_oob(mission_id, directive)
            return
        # Idle: goodbye is done, finalize.
        final_pane = runner.capture_pane_full(mission_id)
        db.log_event(
            self.conn, mission_id=mission_id, kind="mission_final_pane",
            payload={"content": final_pane},
        )
        db.set_mission_state(self.conn, mission_id, "cancelled", finished=True)
        runner.tmux_kill_session(mission_id)
        runner.cleanup_worker_tmp(mission_id)
        # Vault preserved across cancellation - only mission.delete purges.
        db.log_event(
            self.conn, mission_id=mission_id, kind="mission_cancelled",
            payload={"mode": "soft"},
        )
        log.info("mission %s soft-cancelled (goodbye delivered)", mission_id)

    # ---------- mission completion ----------

    def _maybe_complete_mission(self, m: sqlite3.Row, now: int) -> None:
        mission_id = m["id"]
        pending = self.conn.execute(
            "SELECT 1 FROM steps WHERE mission_id = ? AND state = 'pending' LIMIT 1",
            (mission_id,),
        ).fetchone()
        if pending is not None:
            return
        any_step = self.conn.execute(
            "SELECT 1 FROM steps WHERE mission_id = ? LIMIT 1", (mission_id,),
        ).fetchone()
        if any_step is None:
            # Empty mission with no steps ever - don't auto-complete; host may still be adding.
            return
        if runner.step_running(mission_id):
            return
        # Any host-configured pings mean the mission still has scheduled work,
        # even if the step queue is empty. Stay running until the host removes
        # them (ping.delete) or explicitly ends the mission (mission.cancel).
        if db.list_pings(self.conn, mission_id):
            return
        # Scripted pings keep the mission alive too - their watcher scripts run
        # in the background and may fire at any time.
        if db.list_scripted_pings(self.conn, mission_id):
            return
        # Hold: the worker escalated (e.g. talk_to_user) and is awaiting an
        # async answer that will arrive as a new step. Stay alive until the hold
        # expires so the answer has a live session to land in.
        hold_until = int(m["hold_until"] or 0)
        if hold_until and now < hold_until:
            return

        # Reply-driven sessions: the worker already answered the user via notify,
        # so skip the wrap-up summary and finalize silently.
        if mission_id in self._suppress_wrapup:
            self._suppress_wrapup.discard(mission_id)
            self._finalize_completed(mission_id)
            return

        # Two-phase completion: first inject a wrap-up directive so Claude composes
        # the final Telegram message itself; on the next idle tick, finalize.
        # Mission can be reopened after completion, so check for wrap-up only
        # AFTER the most recent mission_created / mission_reopened event.
        last_cycle = self.conn.execute(
            "SELECT COALESCE(MAX(ts), 0) AS ts FROM events"
            " WHERE mission_id = ? AND kind IN ('mission_created', 'mission_reopened')",
            (mission_id,),
        ).fetchone()
        cycle_start = int(last_cycle["ts"] or 0)
        wrap_sent = self.conn.execute(
            "SELECT 1 FROM events WHERE mission_id = ? AND kind = 'completion_directive_sent'"
            " AND ts >= ? LIMIT 1",
            (mission_id, cycle_start),
        ).fetchone()
        if wrap_sent is None:
            self._enqueue_oob(
                mission_id,
                {"kind": "completion_wrap_up", "directive": config.COMPLETION_DIRECTIVE},
            )
            db.log_event(
                self.conn, mission_id=mission_id, kind="completion_directive_sent"
            )
            return

        self._finalize_completed(mission_id)

    def _finalize_completed(self, mission_id: str) -> None:
        final_pane = runner.capture_pane_full(mission_id)
        db.log_event(
            self.conn, mission_id=mission_id, kind="mission_final_pane",
            payload={"content": final_pane},
        )
        db.set_mission_state(self.conn, mission_id, "completed", finished=True)
        runner.tmux_kill_session(mission_id)
        runner.cleanup_worker_tmp(mission_id)
        # Vault preserved across completion - secrets/cookies stay until mission.delete.
        db.log_event(self.conn, mission_id=mission_id, kind="mission_completed")
        log.info("mission %s auto-completed", mission_id)

    # ---------- crash recovery (best effort) ----------

    def recover_on_start(self) -> None:
        """Sweep running missions; if tmux is gone, re-create & relaunch."""
        for m in db.list_missions(self.conn):
            if m["state"] != "running":
                continue
            session = m["tmux_session"]
            if runner.tmux_session_exists(session):
                continue
            running = self.conn.execute(
                "SELECT * FROM steps WHERE mission_id = ? AND state = 'running'"
                " ORDER BY position ASC LIMIT 1",
                (m["id"],),
            ).fetchone()
            if running is None:
                # No running step to recover. Let the normal tick auto-complete
                # (or sit idle if there are no pending steps yet). Don't bump
                # restart_count for a benign tmux disappearance.
                log.info("mission %s: tmux gone but no running step; skipping recovery", m["id"])
                continue
            if int(m["restart_count"]) >= config.MAX_RESTARTS:
                log.error(
                    "mission %s: %d restart attempts already; marking failed",
                    m["id"], m["restart_count"],
                )
                db.set_mission_state(self.conn, m["id"], "failed", finished=True)
                db.set_step_state(self.conn, running["id"], "failed", finished=True)
                db.log_event(
                    self.conn, mission_id=m["id"],
                    kind="mission_failed_max_restarts",
                    payload={"restart_count": m["restart_count"]},
                )
                # Catastrophic-fallback telegram: worker cannot be revived to
                # compose its own message, so the daemon sends a notice. This
                # is the only place daemon-direct telegram remains.
                telegram.send(
                    m["telegram_chat_id"],
                    (
                        f"Mission '{m['name']}' failed after {m['restart_count']} "
                        f"restart attempts. The worker could not be revived. "
                        f"Last step directive (truncated): "
                        f"{(running['directive'] or '')[:200]}"
                    ),
                )
                continue
            log.warning("mission %s: tmux session missing, attempting recovery", m["id"])
            db.bump_restart_count(self.conn, m["id"])
            try:
                runner.tmux_create_session(m["id"])
            except runner.RunnerError as e:
                log.error("recovery tmux create failed for %s: %s", m["id"], e)
                continue
            if running is not None:
                directive = config.RESUME_NOTIFY_PREFIX + running["directive"]
                try:
                    runner.launch_step(m["id"], directive, first_step=False)
                    db.log_event(
                        self.conn,
                        mission_id=m["id"],
                        kind="mission_resumed",
                        step_id=running["id"],
                    )
                except runner.RunnerError as e:
                    log.error("recovery launch failed for %s: %s", m["id"], e)
