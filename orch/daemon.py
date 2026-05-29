"""orchd: long-running daemon. Owns the DB and the Unix socket RPC server."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sqlite3
from typing import Any, Awaitable, Callable

from . import config, db, engine, runner, telegram, telegram_host, vault

log = logging.getLogger(__name__)


class RPCError(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


Handler = Callable[["Daemon", dict[str, Any]], Awaitable[dict[str, Any]] | dict[str, Any]]


def _validate_cue(cue: dict[str, Any], *, position: int) -> None:
    if not isinstance(cue, dict) or "type" not in cue:
        raise RPCError("bad_cue", "cue must be a dict with 'type'")
    t = cue["type"]
    if t == "immediate":
        if position != 0:
            raise RPCError("bad_cue", "'immediate' is only valid for the first step")
    elif t == "on_current_complete":
        pass
    elif t == "on_prev_complete":  # legacy alias (tail-chained); still accepted
        pass
    elif t == "on_timeout":
        if not isinstance(cue.get("seconds"), int) or cue["seconds"] <= 0:
            raise RPCError("bad_cue", "on_timeout requires positive 'seconds'")
    elif t == "at_time":
        # Fire at an absolute wall-clock time. Accept either:
        #   epoch: unix seconds (timezone-independent), or
        #   at:    local datetime string parsed in the machine's timezone
        #          ("YYYY-MM-DD HH:MM[:SS]" or ISO "YYYY-MM-DDTHH:MM[:SS]").
        # Normalize to cue["epoch"] so the engine never re-parses.
        epoch = cue.get("epoch")
        if epoch is None:
            at = cue.get("at")
            if not isinstance(at, str) or not at.strip():
                raise RPCError("bad_cue", "at_time requires 'epoch' (int) or 'at' (datetime string)")
            import time as _t
            from datetime import datetime as _dt
            s = at.strip().replace("T", " ")
            parsed = None
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
                try:
                    parsed = _dt.strptime(s, fmt)
                    break
                except ValueError:
                    continue
            if parsed is None:
                raise RPCError("bad_cue", f"could not parse 'at' time: {at!r} (use 'YYYY-MM-DD HH:MM')")
            # mktime interprets the struct as LOCAL time (machine tz).
            epoch = int(_t.mktime(parsed.timetuple()))
        if not isinstance(epoch, int) or epoch <= 0:
            raise RPCError("bad_cue", "at_time 'epoch' must be a positive int")
        cue["epoch"] = epoch  # normalized; stored with the step
        cue.pop("at", None)
    else:
        raise RPCError("bad_cue", f"unknown cue type: {t}")


def _validate_mode(mode: dict[str, Any]) -> None:
    if not isinstance(mode, dict) or "type" not in mode:
        raise RPCError("bad_mode", "mode must be a dict with 'type'")
    t = mode["type"]
    if t == "on_step_complete":
        pass
    elif t == "on_schedule":
        if not isinstance(mode.get("seconds"), int) or mode["seconds"] <= 0:
            raise RPCError("bad_mode", "on_schedule requires positive 'seconds'")
    else:
        raise RPCError("bad_mode", f"unknown ping mode: {t}")


class Daemon:
    def __init__(self) -> None:
        config.ensure_dirs()
        self.conn: sqlite3.Connection = db.connect()
        db.init_schema(self.conn)
        self.engine = engine.Engine(self.conn)
        self.tg_host = telegram_host.TelegramHost(self)
        self._server: asyncio.base_events.Server | None = None
        self._engine_task: asyncio.Task | None = None
        self._tg_host_task: asyncio.Task | None = None
        self._stop = asyncio.Event()

    # ---------- lifecycle ----------

    async def start(self) -> None:
        try:
            vault.init_vault()
        except vault.VaultError as e:
            log.warning("vault init skipped: %s", e)

        # Crash recovery sweep before engine starts ticking.
        try:
            self.engine.recover_on_start()
        except Exception:
            log.exception("recovery sweep failed")

        if config.SOCKET_PATH.exists():
            config.SOCKET_PATH.unlink()
        self._server = await asyncio.start_unix_server(
            self._on_client, path=str(config.SOCKET_PATH)
        )
        os.chmod(config.SOCKET_PATH, 0o600)
        log.info("daemon: listening on %s", config.SOCKET_PATH)

        self._engine_task = asyncio.create_task(self.engine.run())
        if self.tg_host.configured():
            self._tg_host_task = asyncio.create_task(self.tg_host.run())

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._stop.set)
            except NotImplementedError:
                pass

        await self._stop.wait()
        await self.shutdown()

    async def shutdown(self) -> None:
        log.info("daemon: shutting down")
        self.engine.stop()
        self.tg_host.stop()
        if self._engine_task:
            try:
                await asyncio.wait_for(self._engine_task, timeout=5.0)
            except asyncio.TimeoutError:
                self._engine_task.cancel()
        if self._tg_host_task:
            try:
                await asyncio.wait_for(self._tg_host_task, timeout=5.0)
            except asyncio.TimeoutError:
                self._tg_host_task.cancel()
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        try:
            self.conn.close()
        except Exception:
            pass
        if config.SOCKET_PATH.exists():
            try:
                config.SOCKET_PATH.unlink()
            except OSError:
                pass

    # ---------- RPC server ----------

    async def _on_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            while True:
                line = await reader.readline()
                if not line:
                    return
                try:
                    req = json.loads(line.decode("utf-8"))
                except json.JSONDecodeError:
                    await self._reply(writer, {"error": {"code": "bad_json", "message": "bad json"}})
                    continue
                resp = await self._dispatch(req)
                await self._reply(writer, resp)
        except (ConnectionResetError, BrokenPipeError):
            pass
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    @staticmethod
    async def _reply(writer: asyncio.StreamWriter, payload: dict[str, Any]) -> None:
        writer.write((json.dumps(payload) + "\n").encode("utf-8"))
        await writer.drain()

    async def _dispatch(self, req: dict[str, Any]) -> dict[str, Any]:
        method = req.get("method")
        params = req.get("params") or {}
        req_id = req.get("id")
        handler = _HANDLERS.get(method)
        if handler is None:
            return {"id": req_id, "error": {"code": "no_method", "message": f"unknown method {method}"}}
        try:
            result = handler(self, params)
            if asyncio.iscoroutine(result):
                result = await result
            return {"id": req_id, "result": result}
        except RPCError as e:
            return {"id": req_id, "error": {"code": e.code, "message": e.message}}
        except Exception as e:
            log.exception("dispatch %s failed", method)
            return {"id": req_id, "error": {"code": "internal", "message": str(e)}}


# ---------- handlers ----------


def _require(params: dict[str, Any], *names: str) -> tuple[Any, ...]:
    out = []
    for n in names:
        if n not in params:
            raise RPCError("missing_param", f"missing param: {n}")
        out.append(params[n])
    return tuple(out)


def _mission_or_raise(conn: sqlite3.Connection, mission_id: str) -> sqlite3.Row:
    row = db.get_mission(conn, mission_id)
    if row is None:
        raise RPCError("not_found", f"no mission {mission_id}")
    return row


def _step_or_raise(conn: sqlite3.Connection, step_id: str) -> sqlite3.Row:
    row = db.get_step(conn, step_id)
    if row is None:
        raise RPCError("not_found", f"no step {step_id}")
    return row


def _ping_or_raise(conn: sqlite3.Connection, ping_id: str) -> sqlite3.Row:
    row = db.get_ping(conn, ping_id)
    if row is None:
        raise RPCError("not_found", f"no ping {ping_id}")
    return row


# --- missions ---


def h_mission_create(d: Daemon, p: dict[str, Any]) -> dict[str, Any]:
    (name,) = _require(p, "name")
    chat_id = p.get("telegram_chat_id") or config.default_chat_id()
    if not chat_id:
        raise RPCError(
            "missing_chat_id",
            f"telegram_chat_id not provided and {config.ENV_DEFAULT_CHAT_ID} not set in daemon env",
        )
    interval = int(p.get("heartbeat_interval_s") or config.HEARTBEAT_DEFAULT_S)
    if interval <= 0 or interval > config.HEARTBEAT_MAX_S:
        raise RPCError("bad_interval", f"heartbeat must be 1..{config.HEARTBEAT_MAX_S}s")
    mission_id = db.new_id()
    tmux = config.tmux_session_name(mission_id)
    try:
        runner.tmux_create_session(mission_id)
    except runner.RunnerError as e:
        raise RPCError("tmux_failed", str(e))
    runner.write_worker_mcp_config(mission_id)
    db.create_mission(
        d.conn,
        mission_id=mission_id,
        name=str(name),
        tmux_session=tmux,
        telegram_chat_id=str(chat_id),
        heartbeat_interval_s=interval,
    )
    db.log_event(d.conn, mission_id=mission_id, kind="mission_created")
    return {"mission_id": mission_id}


def h_mission_list(d: Daemon, p: dict[str, Any]) -> list[dict[str, Any]]:
    return db.rows_to_dicts(db.list_missions(d.conn))


def h_mission_get(d: Daemon, p: dict[str, Any]) -> dict[str, Any]:
    (mission_id,) = _require(p, "mission_id")
    m = _mission_or_raise(d.conn, mission_id)
    return {
        "mission": dict(m),
        "steps": db.rows_to_dicts(db.list_steps(d.conn, mission_id)),
        "pings": db.rows_to_dicts(db.list_pings(d.conn, mission_id)),
    }


def h_location_get(d: Daemon, p: dict[str, Any]) -> dict[str, Any]:
    """Latest known location of the mission's user (the telegram chat that owns
    this mission). Returns {available: false} if the user never shared one."""
    (mission_id,) = _require(p, "mission_id")
    m = _mission_or_raise(d.conn, mission_id)
    row = db.get_location(d.conn, str(m["telegram_chat_id"]))
    if row is None:
        return {"available": False}
    age = db.now_ts() - int(row["updated_at"])
    return {
        "available": True,
        "latitude": row["latitude"],
        "longitude": row["longitude"],
        "accuracy_m": row["accuracy"],
        "heading": row["heading"],
        "live": bool(row["live"]),
        "updated_at": row["updated_at"],
        "age_seconds": age,
        "maps_url": f"https://maps.google.com/?q={row['latitude']},{row['longitude']}",
    }


def h_mission_events(d: Daemon, p: dict[str, Any]) -> list[dict[str, Any]]:
    """Return the structured audit log for a mission."""
    (mission_id,) = _require(p, "mission_id")
    _mission_or_raise(d.conn, mission_id)
    since = int(p.get("since") or 0)
    limit = int(p.get("limit") or 100)
    if limit < 1:
        limit = 1
    if limit > 1000:
        limit = 1000
    rows = d.conn.execute(
        "SELECT id, ts, kind, step_id, ping_id, payload FROM events"
        " WHERE mission_id = ? AND ts >= ? ORDER BY ts DESC, id DESC LIMIT ?",
        (mission_id, since, limit),
    ).fetchall()
    out = []
    for r in rows:
        item = dict(r)
        if item.get("payload"):
            try:
                item["payload"] = json.loads(item["payload"])
            except Exception:
                pass
        out.append(item)
    return out


def h_mission_pane_snapshot(d: Daemon, p: dict[str, Any]) -> dict[str, Any]:
    """Capture the worker's tmux pane content. Live if alive, archived if not."""
    (mission_id,) = _require(p, "mission_id")
    _mission_or_raise(d.conn, mission_id)
    lines = int(p.get("lines") or 80)
    if lines < 1:
        lines = 1
    if lines > 2000:
        lines = 2000
    session = config.tmux_session_name(mission_id)

    def _tail(text: str) -> str:
        ls = text.split("\n")
        return "\n".join(ls[-lines:]) if len(ls) > lines else text

    if runner.tmux_session_exists(session):
        import subprocess as _sp
        res = _sp.run(
            ["tmux", "capture-pane", "-t", session, "-p", "-S", f"-{lines}"],
            capture_output=True,
        )
        return {
            "pane_content": res.stdout.decode("utf-8", "replace"),
            "alive": True,
            "claude_running": runner.step_running(mission_id),
            "source": "live",
        }
    # Tmux gone - fall back to the final-pane snapshot logged at teardown.
    row = d.conn.execute(
        "SELECT payload FROM events WHERE mission_id = ? AND kind = 'mission_final_pane'"
        " ORDER BY ts DESC LIMIT 1",
        (mission_id,),
    ).fetchone()
    content = ""
    if row and row["payload"]:
        try:
            content = (json.loads(row["payload"]) or {}).get("content", "") or ""
        except Exception:
            content = ""
    return {
        "pane_content": _tail(content),
        "alive": False,
        "claude_running": False,
        "source": "archived" if content else "none",
    }


def h_mission_delete(d: Daemon, p: dict[str, Any]) -> dict[str, Any]:
    """Permanently delete a mission from the DB. Only terminal-state missions."""
    (mission_id,) = _require(p, "mission_id")
    m = _mission_or_raise(d.conn, mission_id)
    if m["state"] not in ("completed", "cancelled", "failed"):
        raise RPCError(
            "not_terminal",
            f"mission state is '{m['state']}'; only completed/cancelled/failed missions can be deleted. "
            f"Call mission.cancel first.",
        )
    # Best-effort cleanup in case any artifacts lingered.
    runner.tmux_kill_session(mission_id)
    runner.cleanup_worker_tmp(mission_id)
    try:
        vault.purge_mission(mission_id)
    except Exception:
        pass
    d.conn.execute("DELETE FROM missions WHERE id = ?", (mission_id,))
    db.log_event(d.conn, mission_id=mission_id, kind="mission_deleted")
    return {"ok": True}


def h_mission_cancel(d: Daemon, p: dict[str, Any]) -> dict[str, Any]:
    (mission_id,) = _require(p, "mission_id")
    force = bool(p.get("force", False))
    m = _mission_or_raise(d.conn, mission_id)
    if m["state"] in ("completed", "cancelled", "failed"):
        return {"ok": True, "already": m["state"]}

    # Pending steps are cancelled either way.
    d.conn.execute(
        "UPDATE steps SET state = 'cancelled', finished_at = ?"
        " WHERE mission_id = ? AND state = 'pending'",
        (db.now_ts(), mission_id),
    )

    if force or m["state"] == "cancelling":
        # Hard cancel: immediate teardown, no goodbye.
        d.conn.execute(
            "UPDATE steps SET state = 'cancelled', finished_at = ?"
            " WHERE mission_id = ? AND state = 'running'",
            (db.now_ts(), mission_id),
        )
        final_pane = runner.capture_pane_full(mission_id)
        db.log_event(
            d.conn, mission_id=mission_id, kind="mission_final_pane",
            payload={"content": final_pane},
        )
        db.set_mission_state(d.conn, mission_id, "cancelled", finished=True)
        runner.tmux_kill_session(mission_id)
        runner.cleanup_worker_tmp(mission_id)
        # Vault preserved across cancellation - only mission.delete purges.
        db.log_event(
            d.conn, mission_id=mission_id, kind="mission_cancelled",
            payload={"mode": "hard"},
        )
        return {"ok": True, "mode": "hard"}

    # Soft cancel: interrupt current step if any, queue goodbye OOB, mark
    # cancelling. Engine drives the goodbye -> teardown transition.
    running_step = d.conn.execute(
        "SELECT id FROM steps WHERE mission_id = ? AND state = 'running' LIMIT 1",
        (mission_id,),
    ).fetchone()
    if running_step is not None:
        runner.tmux_interrupt(mission_id)
        d.conn.execute(
            "UPDATE steps SET state = 'cancelled', finished_at = ? WHERE id = ?",
            (db.now_ts(), running_step["id"]),
        )
    db.set_mission_state(d.conn, mission_id, "cancelling", finished=False)
    d.engine._enqueue_oob(
        mission_id,
        {"kind": "cancel_goodbye", "directive": config.CANCEL_GOODBYE_DIRECTIVE},
    )
    db.log_event(d.conn, mission_id=mission_id, kind="mission_cancelling")
    return {"ok": True, "mode": "soft", "goodbye_queued": True}


def h_mission_attach_info(d: Daemon, p: dict[str, Any]) -> dict[str, Any]:
    (mission_id,) = _require(p, "mission_id")
    _mission_or_raise(d.conn, mission_id)
    return {"tmux_cmd": f"tmux attach -t {config.tmux_session_name(mission_id)}"}


# --- steps ---


def h_step_add(d: Daemon, p: dict[str, Any]) -> dict[str, Any]:
    mission_id, directive, cue = _require(p, "mission_id", "directive", "cue")
    m = _mission_or_raise(d.conn, mission_id)
    # Reopen a completed mission: recreate tmux + worker MCP config, transition
    # state back to 'running'. Claude session UUID persists in claude's storage,
    # so `claude --resume <mission_id>` brings back full prior context.
    if m["state"] == "completed":
        try:
            runner.tmux_create_session(mission_id)
            runner.write_worker_mcp_config(mission_id)
        except runner.RunnerError as e:
            raise RPCError("reopen_failed", str(e))
        d.conn.execute(
            "UPDATE missions SET state = 'running', finished_at = NULL,"
            " last_heartbeat_at = ? WHERE id = ?",
            (db.now_ts(), mission_id),
        )
        db.log_event(d.conn, mission_id=mission_id, kind="mission_reopened")
    elif m["state"] != "running":
        raise RPCError(
            "not_writable",
            f"mission state is '{m['state']}'; only running or completed missions "
            f"accept new steps. completed → automatically reopens; cancelled/failed "
            f"require mission.delete and a fresh mission.create.",
        )
    created_by = p.get("created_by", "host")
    position = p.get("position")
    if position is None:
        row = d.conn.execute(
            "SELECT COALESCE(MAX(position), -1) + 1 AS p FROM steps WHERE mission_id = ?",
            (mission_id,),
        ).fetchone()
        position = row["p"]
    _validate_cue(cue, position=int(position))
    sid = db.add_step(
        d.conn,
        mission_id=mission_id,
        directive=directive,
        cue=cue,
        created_by=created_by,
        position=int(position),
    )
    db.log_event(d.conn, mission_id=mission_id, kind="step_added", step_id=sid)
    # A new step means awaited work/answers arrived - release any hold so the
    # mission resumes normal completion logic after this step runs.
    db.clear_mission_hold(d.conn, mission_id)
    return {"step_id": sid}


def h_oob_inject(d: Daemon, p: dict[str, Any]) -> dict[str, Any]:
    """Inject an out-of-band directive that fires the MOMENT the worker is idle,
    jumping ahead of any queued/long-scheduled steps - without disturbing them
    (pending steps keep their order and run afterward). Used to deliver a phoned-in
    answer back to a worker that called talk_to_user and then went idle, so the
    answer isn't tailed behind a step scheduled days out.

    Args: {mission_id, directive, kind?}. Reopens a completed mission so its tmux
    is live to receive it. Returns {ok, queued}."""
    mission_id, directive = _require(p, "mission_id", "directive")
    m = _mission_or_raise(d.conn, mission_id)
    kind = p.get("kind") or "phone_relay"
    if m["state"] == "completed":
        try:
            runner.tmux_create_session(mission_id)
            runner.write_worker_mcp_config(mission_id)
        except runner.RunnerError as e:
            raise RPCError("reopen_failed", str(e))
        d.conn.execute(
            "UPDATE missions SET state = 'running', finished_at = NULL,"
            " last_heartbeat_at = ? WHERE id = ?",
            (db.now_ts(), mission_id),
        )
        db.log_event(d.conn, mission_id=mission_id, kind="mission_reopened")
    elif m["state"] != "running":
        raise RPCError(
            "not_writable",
            f"mission state is '{m['state']}'; only running/completed missions "
            f"accept an injection.",
        )
    d.engine._enqueue_oob(mission_id, {"kind": kind, "directive": directive})
    db.clear_mission_hold(d.conn, mission_id)  # answer arrived; resume normal flow
    db.log_event(d.conn, mission_id=mission_id, kind="oob_injected",
                 payload={"kind": kind})
    return {"ok": True, "queued": True}


def h_step_list(d: Daemon, p: dict[str, Any]) -> list[dict[str, Any]]:
    (mission_id,) = _require(p, "mission_id")
    return db.rows_to_dicts(db.list_steps(d.conn, mission_id))


def h_step_update(d: Daemon, p: dict[str, Any]) -> dict[str, Any]:
    (step_id,) = _require(p, "step_id")
    s = _step_or_raise(d.conn, step_id)
    if s["state"] != "pending":
        raise RPCError("bad_state", "only pending steps can be updated")
    if "cue" in p:
        _validate_cue(p["cue"], position=int(s["position"]))
    db.update_step(d.conn, step_id, directive=p.get("directive"), cue=p.get("cue"))
    return {"ok": True}


def h_step_delete(d: Daemon, p: dict[str, Any]) -> dict[str, Any]:
    (step_id,) = _require(p, "step_id")
    s = _step_or_raise(d.conn, step_id)
    if s["state"] != "pending":
        raise RPCError("bad_state", "only pending steps can be deleted")
    db.delete_step(d.conn, step_id)
    return {"ok": True}


def h_step_cancel_current(d: Daemon, p: dict[str, Any]) -> dict[str, Any]:
    import time as _time
    (mission_id,) = _require(p, "mission_id")
    _mission_or_raise(d.conn, mission_id)
    running = d.conn.execute(
        "SELECT * FROM steps WHERE mission_id = ? AND state = 'running'"
        " ORDER BY position ASC LIMIT 1",
        (mission_id,),
    ).fetchone()
    if running is None:
        return {"ok": False, "reason": "no_running_step"}
    runner.tmux_interrupt(mission_id)
    # Wait briefly for claude to actually exit before marking cancelled, so
    # the DB state matches reality. Bounded at 3 seconds; if claude is wedged
    # we proceed anyway (the cancel was best-effort).
    deadline = _time.time() + 3.0
    while _time.time() < deadline and runner.step_running(mission_id):
        _time.sleep(0.1)
    exited = not runner.step_running(mission_id)
    db.set_step_state(d.conn, running["id"], "cancelled", finished=True)
    db.log_event(
        d.conn, mission_id=mission_id, kind="step_cancelled",
        step_id=running["id"], payload={"exited_cleanly": exited},
    )
    return {"ok": True, "exited_cleanly": exited}


# --- pings ---


def h_ping_add(d: Daemon, p: dict[str, Any]) -> dict[str, Any]:
    mission_id, command, mode = _require(p, "mission_id", "command", "mode")
    _mission_or_raise(d.conn, mission_id)
    _validate_mode(mode)
    created_by = p.get("created_by", "host")
    pid = db.add_ping(
        d.conn,
        mission_id=mission_id,
        command=command,
        mode=mode,
        created_by=created_by,
    )
    db.log_event(d.conn, mission_id=mission_id, kind="ping_added", ping_id=pid)
    return {"ping_id": pid}


def h_ping_list(d: Daemon, p: dict[str, Any]) -> list[dict[str, Any]]:
    (mission_id,) = _require(p, "mission_id")
    return db.rows_to_dicts(db.list_pings(d.conn, mission_id))


def h_ping_update(d: Daemon, p: dict[str, Any]) -> dict[str, Any]:
    (ping_id,) = _require(p, "ping_id")
    _ping_or_raise(d.conn, ping_id)
    if "mode" in p:
        _validate_mode(p["mode"])
    db.update_ping(d.conn, ping_id, command=p.get("command"), mode=p.get("mode"))
    return {"ok": True}


def h_ping_delete(d: Daemon, p: dict[str, Any]) -> dict[str, Any]:
    (ping_id,) = _require(p, "ping_id")
    _ping_or_raise(d.conn, ping_id)
    db.delete_ping(d.conn, ping_id)
    return {"ok": True}


# --- scripted pings (autonomous watcher scripts) ---


def _sping_or_raise(d: Daemon, spid: str) -> Any:
    row = db.get_scripted_ping(d.conn, spid)
    if row is None:
        raise RPCError("not_found", f"no scripted ping {spid}")
    return row


def h_scripted_ping_add(d: Daemon, p: dict[str, Any]) -> dict[str, Any]:
    mission_id, condition, action = _require(p, "mission_id", "condition", "action")
    _mission_or_raise(d.conn, mission_id)
    timeout_s = int(p.get("timeout_s") or 600)
    if timeout_s < 30:
        raise RPCError("bad_timeout", "timeout_s must be >= 30")
    created_by = p.get("created_by", "host")
    spid = db.add_scripted_ping(
        d.conn, mission_id=mission_id, condition=condition, action=action,
        timeout_s=timeout_s, created_by=created_by,
    )
    # Inject the setup directive so the worker writes & tests the watcher script.
    directive = config.SCRIPTED_PING_SETUP_DIRECTIVE.format(
        spid=spid, condition=condition, action=action,
        timeout_s=timeout_s, half_timeout=max(10, timeout_s // 2),
    )
    d.engine._enqueue_oob(
        mission_id, {"kind": "scripted_ping_setup", "directive": directive}
    )
    db.log_event(
        d.conn, mission_id=mission_id, kind="scripted_ping_added",
        payload={"spid": spid, "timeout_s": timeout_s},
    )
    return {"scripted_ping_id": spid}


def h_scripted_ping_list(d: Daemon, p: dict[str, Any]) -> list[dict[str, Any]]:
    (mission_id,) = _require(p, "mission_id")
    return db.rows_to_dicts(db.list_scripted_pings(d.conn, mission_id))


def h_scripted_ping_delete(d: Daemon, p: dict[str, Any]) -> dict[str, Any]:
    (spid,) = _require(p, "scripted_ping_id")
    _sping_or_raise(d, spid)
    db.delete_scripted_ping(d.conn, spid)
    return {"ok": True}


def h_scripted_ping_alive(d: Daemon, p: dict[str, Any]) -> dict[str, Any]:
    """Heartbeat from a running watcher script (via the owatch CLI)."""
    (spid,) = _require(p, "scripted_ping_id")
    sp = _sping_or_raise(d, spid)
    db.touch_scripted_ping_alive(d.conn, spid)
    # An alive signal from a previously-broken script means it recovered.
    if sp["state"] == "broken":
        db.set_scripted_ping_state(d.conn, spid, "active")
    return {"ok": True}


def h_scripted_ping_ready(d: Daemon, p: dict[str, Any]) -> dict[str, Any]:
    """Worker signals the script is tested and running."""
    (spid,) = _require(p, "scripted_ping_id")
    sp = _sping_or_raise(d, spid)
    script_path = p.get("script_path")
    db.set_scripted_ping_state(
        d.conn, spid, "active", script_path=script_path, touch_alive=True
    )
    db.log_event(
        d.conn, mission_id=sp["mission_id"], kind="scripted_ping_ready",
        payload={"spid": spid, "script_path": script_path},
    )
    return {"ok": True}


def h_scripted_ping_fire(d: Daemon, p: dict[str, Any]) -> dict[str, Any]:
    """The watcher script reports its condition became true."""
    (spid,) = _require(p, "scripted_ping_id")
    sp = _sping_or_raise(d, spid)
    db.touch_scripted_ping_alive(d.conn, spid)
    context = str(p.get("context") or "(none)")
    directive = config.SCRIPTED_PING_FIRE_DIRECTIVE.format(
        condition=sp["condition"], action=sp["action"], context=context,
    )
    d.engine._enqueue_oob(
        sp["mission_id"], {"kind": "scripted_ping_fire", "directive": directive}
    )
    db.log_event(
        d.conn, mission_id=sp["mission_id"], kind="scripted_ping_fired",
        payload={"spid": spid, "context": context[:200]},
    )
    return {"ok": True}


# --- heartbeat ---


def h_heartbeat_set(d: Daemon, p: dict[str, Any]) -> dict[str, Any]:
    mission_id, interval = _require(p, "mission_id", "interval_s")
    interval = int(interval)
    if interval <= 0 or interval > config.HEARTBEAT_MAX_S:
        raise RPCError("bad_interval", f"interval must be 1..{config.HEARTBEAT_MAX_S}")
    _mission_or_raise(d.conn, mission_id)
    db.set_heartbeat_interval(d.conn, mission_id, interval)
    return {"ok": True, "interval_s": interval}


def h_heartbeat_get(d: Daemon, p: dict[str, Any]) -> dict[str, Any]:
    (mission_id,) = _require(p, "mission_id")
    m = _mission_or_raise(d.conn, mission_id)
    return {
        "interval_s": int(m["heartbeat_interval_s"]),
        "last_heartbeat_at": m["last_heartbeat_at"],
    }


# --- secrets / cookies ---


def h_secret_put(d: Daemon, p: dict[str, Any]) -> dict[str, Any]:
    mission_id, name, value = _require(p, "mission_id", "name", "value")
    _mission_or_raise(d.conn, mission_id)
    try:
        vault.put_secret(mission_id, name, value)
    except vault.VaultError as e:
        raise RPCError("vault_failed", str(e))
    db.log_event(
        d.conn,
        mission_id=mission_id,
        kind="secret_put",
        payload={"name": name, "caller": p.get("caller", "host")},
    )
    return {"ok": True}


def h_secret_list(d: Daemon, p: dict[str, Any]) -> list[str]:
    (mission_id,) = _require(p, "mission_id")
    _mission_or_raise(d.conn, mission_id)
    return vault.list_secrets(mission_id)


def h_secret_delete(d: Daemon, p: dict[str, Any]) -> dict[str, Any]:
    mission_id, name = _require(p, "mission_id", "name")
    _mission_or_raise(d.conn, mission_id)
    try:
        vault.delete_secret(mission_id, name)
    except vault.VaultError as e:
        raise RPCError("vault_failed", str(e))
    db.log_event(
        d.conn,
        mission_id=mission_id,
        kind="secret_deleted",
        payload={"name": name, "caller": p.get("caller", "host")},
    )
    return {"ok": True}


def h_secret_get(d: Daemon, p: dict[str, Any]) -> dict[str, Any]:
    """Used by msec CLI (worker side). Returns the value."""
    mission_id, name = _require(p, "mission_id", "name")
    _mission_or_raise(d.conn, mission_id)
    try:
        value = vault.get_secret(mission_id, name)
    except vault.VaultError as e:
        raise RPCError("vault_failed", str(e))
    db.log_event(
        d.conn,
        mission_id=mission_id,
        kind="secret_accessed",
        payload={"name": name, "caller": p.get("caller", "msec")},
    )
    return {"value": value}


def h_cookies_put(d: Daemon, p: dict[str, Any]) -> dict[str, Any]:
    mission_id, name, content = _require(p, "mission_id", "name", "content")
    _mission_or_raise(d.conn, mission_id)
    try:
        vault.put_cookies(mission_id, name, content)
    except vault.VaultError as e:
        raise RPCError("vault_failed", str(e))
    db.log_event(
        d.conn,
        mission_id=mission_id,
        kind="cookies_put",
        payload={"name": name, "caller": p.get("caller", "host")},
    )
    return {"ok": True}


def h_cookies_delete(d: Daemon, p: dict[str, Any]) -> dict[str, Any]:
    mission_id, name = _require(p, "mission_id", "name")
    _mission_or_raise(d.conn, mission_id)
    try:
        vault.delete_cookies(mission_id, name)
    except vault.VaultError as e:
        raise RPCError("vault_failed", str(e))
    db.log_event(
        d.conn,
        mission_id=mission_id,
        kind="cookies_deleted",
        payload={"name": name, "caller": p.get("caller", "host")},
    )
    return {"ok": True}


def h_cookies_list(d: Daemon, p: dict[str, Any]) -> list[str]:
    (mission_id,) = _require(p, "mission_id")
    _mission_or_raise(d.conn, mission_id)
    return vault.list_cookies(mission_id)


def h_cookies_materialize(d: Daemon, p: dict[str, Any]) -> dict[str, Any]:
    """Write cookies file to tmpfs and return path. Used by msec CLI."""
    mission_id, name = _require(p, "mission_id", "name")
    _mission_or_raise(d.conn, mission_id)
    try:
        content = vault.get_cookies(mission_id, name)
    except vault.VaultError as e:
        raise RPCError("vault_failed", str(e))
    tmp = config.worker_tmpdir(mission_id) / "cookies"
    tmp.mkdir(parents=True, exist_ok=True)
    out = tmp / name
    out.write_text(content)
    os.chmod(out, 0o600)
    db.log_event(
        d.conn,
        mission_id=mission_id,
        kind="cookies_accessed",
        payload={"name": name, "caller": p.get("caller", "msec")},
    )
    return {"path": str(out)}


def h_notify(d: Daemon, p: dict[str, Any]) -> dict[str, Any]:
    """Send a Telegram message composed by the worker Claude to the mission's chat."""
    mission_id, text = _require(p, "mission_id", "text")
    m = _mission_or_raise(d.conn, mission_id)
    text = str(text)
    if not text.strip():
        raise RPCError("empty_text", "notify text is empty")
    if len(text) > 4000:
        text = text[:3990] + "…"
    chat_id = m["telegram_chat_id"]
    # Prefer the command bot (bot #2) so the user can reply to this message and
    # have the reply routed back to this worker. Fall back to the notification
    # bot (bot #1) if the command bot isn't configured.
    host_tok = telegram.host_token()
    routed = "host_bot"
    if host_tok:
        msg_id = telegram.send_via(host_tok, chat_id, text)
        if msg_id is not None:
            db.map_notify_message(d.conn, msg_id, mission_id)
        else:
            routed = "host_bot_failed"
    else:
        telegram.send(chat_id, text)
        routed = "notification_bot"
    db.log_event(
        d.conn, mission_id=mission_id, kind="notify_sent",
        payload={"chars": len(text), "via": routed},
    )
    return {"ok": True}


def h_mission_hold(d: Daemon, p: dict[str, Any]) -> dict[str, Any]:
    """Hold a mission open (suppress auto-complete) for `seconds`, because it is
    awaiting an async answer that will arrive as a new step. Default 3600s,
    max 86400s. seconds<=0 clears the hold."""
    (mission_id,) = _require(p, "mission_id")
    _mission_or_raise(d.conn, mission_id)
    seconds = int(p.get("seconds", 3600))
    if seconds <= 0:
        db.clear_mission_hold(d.conn, mission_id)
        return {"ok": True, "held": False}
    if seconds > 86400:
        seconds = 86400
    until = db.now_ts() + seconds
    db.set_mission_hold(d.conn, mission_id, until)
    db.log_event(
        d.conn, mission_id=mission_id, kind="mission_held",
        payload={"seconds": seconds, "reason": p.get("reason")},
    )
    return {"ok": True, "held": True, "until": until}


def h_host_message(d: Daemon, p: dict[str, Any]) -> dict[str, Any]:
    """Worker -> host mailbox. Optional file paths are copied into the mailbox
    store so the host can fetch their bytes later (works across SSH)."""
    mission_id, text = _require(p, "mission_id", "text")
    file_paths = p.get("files") or []
    if isinstance(file_paths, str):
        file_paths = [file_paths]
    mid = db.new_id()
    stored: list[dict[str, Any]] = []
    if file_paths:
        import shutil
        dest_dir = config.MAILBOX_DIR / mid
        dest_dir.mkdir(parents=True, exist_ok=True)
        for fp in file_paths:
            src = os.path.expanduser(str(fp))
            if not os.path.isfile(src):
                stored.append({"name": os.path.basename(src), "error": "not found", "path": src})
                continue
            name = os.path.basename(src) or "file"
            dest = dest_dir / name
            try:
                shutil.copy2(src, dest)
                stored.append({"name": name, "path": str(dest), "size": os.path.getsize(dest)})
            except Exception as e:
                stored.append({"name": name, "error": str(e), "path": src})
    # Reuse the generated id as the row id for tidy file_id mapping.
    d.conn.execute(
        "INSERT INTO host_messages (id, mission_id, text, files, created_at)"
        " VALUES (?, ?, ?, ?, ?)",
        (mid, mission_id, str(text), json.dumps(stored), db.now_ts()),
    )
    db.log_event(
        d.conn, mission_id=mission_id, kind="host_message_sent",
        payload={"message_id": mid, "files": len(stored)},
    )
    return {"ok": True, "message_id": mid, "files": len(stored)}


def h_host_inbox(d: Daemon, p: dict[str, Any]) -> list[dict[str, Any]]:
    """Host reads pending worker messages. file_id = <message_id>:<index>."""
    include_acked = bool(p.get("include_acked", False))
    limit = int(p.get("limit") or 50)
    rows = db.list_host_messages(d.conn, include_acked=include_acked, limit=limit)
    out = []
    for r in rows:
        files = json.loads(r["files"] or "[]")
        file_meta = [
            {"file_id": f"{r['id']}:{i}", "name": f.get("name"),
             "size": f.get("size"), "error": f.get("error")}
            for i, f in enumerate(files)
        ]
        out.append({
            "message_id": r["id"], "mission_id": r["mission_id"],
            "ts": r["created_at"], "text": r["text"],
            "acked": bool(r["acked"]), "files": file_meta,
        })
    return out


def h_host_ack(d: Daemon, p: dict[str, Any]) -> dict[str, Any]:
    (message_id,) = _require(p, "message_id")
    db.ack_host_message(d.conn, message_id)
    return {"ok": True}


def h_host_fetch_file(d: Daemon, p: dict[str, Any]) -> dict[str, Any]:
    """Return a mailbox file's bytes (base64) so a remote host can save it."""
    import base64
    (file_id,) = _require(p, "file_id")
    try:
        msg_id, idx_s = str(file_id).rsplit(":", 1)
        idx = int(idx_s)
    except ValueError:
        raise RPCError("bad_file_id", "file_id must be '<message_id>:<index>'")
    row = db.get_host_message(d.conn, msg_id)
    if row is None:
        raise RPCError("not_found", "no such message")
    files = json.loads(row["files"] or "[]")
    if idx < 0 or idx >= len(files):
        raise RPCError("not_found", "no such file index")
    f = files[idx]
    path = f.get("path")
    if not path or not os.path.isfile(path):
        raise RPCError("gone", "file no longer available")
    size = os.path.getsize(path)
    if size > 25 * 1024 * 1024:
        raise RPCError("too_large", f"file is {size} bytes; > 25MB inline limit")
    with open(path, "rb") as fh:
        b64 = base64.b64encode(fh.read()).decode("ascii")
    return {"name": f.get("name"), "size": size, "base64": b64}


def h_defaults_get(d: Daemon, p: dict[str, Any]) -> dict[str, Any]:
    return {
        "default_chat_id": config.default_chat_id(),
        "heartbeat_default_s": config.HEARTBEAT_DEFAULT_S,
        "heartbeat_max_s": config.HEARTBEAT_MAX_S,
        "telegram_configured": bool(os.environ.get(config.ENV_TELEGRAM_TOKEN)),
    }


_HANDLERS: dict[str, Handler] = {
    "defaults.get": h_defaults_get,
    "notify": h_notify,
    "mission.hold": h_mission_hold,
    "host.message": h_host_message,
    "host.inbox": h_host_inbox,
    "host.ack": h_host_ack,
    "host.fetch_file": h_host_fetch_file,
    "mission.create": h_mission_create,
    "mission.list": h_mission_list,
    "mission.get": h_mission_get,
    "mission.cancel": h_mission_cancel,
    "mission.delete": h_mission_delete,
    "mission.events": h_mission_events,
    "location.get": h_location_get,
    "mission.pane_snapshot": h_mission_pane_snapshot,
    "mission.attach_info": h_mission_attach_info,
    "step.add": h_step_add,
    "oob.inject": h_oob_inject,
    "step.list": h_step_list,
    "step.update": h_step_update,
    "step.delete": h_step_delete,
    "step.cancel_current": h_step_cancel_current,
    "scripted_ping.add": h_scripted_ping_add,
    "scripted_ping.list": h_scripted_ping_list,
    "scripted_ping.delete": h_scripted_ping_delete,
    "scripted_ping.alive": h_scripted_ping_alive,
    "scripted_ping.ready": h_scripted_ping_ready,
    "scripted_ping.fire": h_scripted_ping_fire,
    "ping.add": h_ping_add,
    "ping.list": h_ping_list,
    "ping.update": h_ping_update,
    "ping.delete": h_ping_delete,
    "heartbeat.set": h_heartbeat_set,
    "heartbeat.get": h_heartbeat_get,
    "secret.put": h_secret_put,
    "secret.list": h_secret_list,
    "secret.delete": h_secret_delete,
    "secret.get": h_secret_get,
    "cookies.put": h_cookies_put,
    "cookies.delete": h_cookies_delete,
    "cookies.list": h_cookies_list,
    "cookies.materialize": h_cookies_materialize,
}


async def run() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    d = Daemon()
    await d.start()
