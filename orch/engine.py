"""Cue engine + scheduler. Runs as an asyncio task inside the daemon."""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
from typing import Any

from . import config, db, runner, vault

log = logging.getLogger(__name__)


class Engine:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self._stop = asyncio.Event()
        # Per-mission flag: an OOB (heartbeat / on_step_complete) directive is
        # awaiting the current step to finish.
        self._pending_oob: dict[str, list[dict[str, Any]]] = {}

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
            if m["state"] != "running":
                continue
            self._tick_mission(m, now)

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

        # 2. If any OOB queued and no step is running, fire one now.
        if running is None and self._pending_oob.get(mission_id):
            directive = self._pending_oob[mission_id].pop(0)
            self._launch_oob(mission_id, directive)
            return

        # 3. Heartbeat.
        if running is None:
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

        # 4. on_schedule pings.
        for ping in db.list_pings(self.conn, mission_id):
            if ping["mode_type"] != "on_schedule":
                continue
            interval = ping["interval_s"] or 0
            last = ping["last_fired_at"] or m["created_at"]
            if interval > 0 and now - int(last) >= int(interval):
                self._enqueue_oob(
                    mission_id,
                    {
                        "kind": "ping",
                        "ping_id": ping["id"],
                        "directive": self._ping_directive(ping["command"]),
                    },
                )
                db.mark_ping_fired(self.conn, ping["id"], now)

        # 5. Pop next pending step if its cue is satisfied.
        if running is None and not self._pending_oob.get(mission_id):
            self._maybe_launch_next_step(mission_id, now)

        # 6. Auto-complete if no work remains and claude has exited.
        if running is None and not self._pending_oob.get(mission_id):
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
            runner.launch_oob(mission_id, payload["directive"])
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
        nxt = self.conn.execute(
            "SELECT * FROM steps WHERE mission_id = ? AND state = 'pending'"
            " ORDER BY position ASC LIMIT 1",
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
        elif cue["type"] == "on_prev_complete":
            ready = prev is not None and prev["state"] in {"completed"}
        elif cue["type"] == "on_prev_complete_or_timeout":
            seconds = int(cue.get("seconds", 0))
            if prev is None:
                ready = True
            elif prev["state"] == "completed":
                ready = True
            elif prev["started_at"] is not None and now - int(prev["started_at"]) >= seconds:
                # Timeout the previous step.
                db.set_step_state(self.conn, prev["id"], "timed_out", finished=True)
                db.log_event(
                    self.conn,
                    mission_id=mission_id,
                    kind="step_timed_out",
                    step_id=prev["id"],
                )
                ready = True
        elif cue["type"] == "on_timeout":
            seconds = int(cue.get("seconds", 0))
            if prev is not None and prev["started_at"] is not None:
                ready = now - int(prev["started_at"]) >= seconds
            elif prev is None:
                # No previous step to anchor the timer to; treat as immediate.
                ready = True
        else:
            log.warning("unknown cue type for step %s: %s", nxt["id"], cue["type"])
            return

        if not ready:
            return

        try:
            runner.launch_step(mission_id, nxt["directive"], first_step=first_step)
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

        # Two-phase completion: first inject a wrap-up directive so Claude composes
        # the final Telegram message itself; on the next idle tick, finalize.
        wrap_sent = self.conn.execute(
            "SELECT 1 FROM events WHERE mission_id = ? AND kind = 'completion_directive_sent' LIMIT 1",
            (mission_id,),
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

        db.set_mission_state(self.conn, mission_id, "completed", finished=True)
        runner.tmux_kill_session(mission_id)
        runner.cleanup_worker_tmp(mission_id)
        try:
            vault.purge_mission(mission_id)
        except Exception:
            log.exception("vault purge failed for %s", mission_id)
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
            log.warning("mission %s: tmux session missing, attempting recovery", m["id"])
            running = self.conn.execute(
                "SELECT * FROM steps WHERE mission_id = ? AND state = 'running'"
                " ORDER BY position ASC LIMIT 1",
                (m["id"],),
            ).fetchone()
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
