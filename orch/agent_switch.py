"""Live agent switching + last-good fallback.

- set_active_agent(): validates the target backend, persists ORCH_AGENT to the
  active env file, updates the running process env, and resets the adapter
  cache - the very next worker launch uses the new backend (missions migrate
  one by one via handoff on their next wake; see engine._pre_launch).
- record_turn_result(): tracks whether worker turns actually produce activity.
  A turn that dies fast with zero worker activity counts as a failure; after
  config.AGENT_FAIL_LIMIT consecutive failures the daemon auto-reverts to the last agent
  that demonstrably worked ("fallback to whichever last worked").
"""

from __future__ import annotations

import logging
import os
import sqlite3

from . import agents, config, db

log = logging.getLogger(__name__)

META_LAST_GOOD = "last_good_agent"
META_FAILS = "agent_consecutive_failures"


def status(conn: sqlite3.Connection) -> dict:
    cur = agents.get_adapter().name
    out = {
        "active": cur,
        "last_good": db.get_meta(conn, META_LAST_GOOD),
        "consecutive_failures": int(db.get_meta(conn, META_FAILS, "0") or 0),
        "backends": {},
    }
    for name in agents.AGENT_NAMES:
        ok, reason = agents.availability(name)
        out["backends"][name] = {"available": ok, "reason": reason}
    return out


def set_active_agent(conn: sqlite3.Connection, name: str, *, by: str,
                     force: bool = False) -> dict:
    """Switch the global worker backend, live. Raises ValueError if the target
    is unknown/unavailable (unless force)."""
    name = agents.canonical(name)
    if name not in agents.AGENT_NAMES:
        raise ValueError(f"unknown backend {name!r} (choose from {', '.join(agents.AGENT_NAMES)})")
    old = agents.get_adapter().name
    ok, reason = agents.availability(name)
    if not ok and not force:
        raise ValueError(f"backend '{name}' is not usable here: {reason} "
                         f"(pass force to override)")
    path = config.update_env_file("ORCH_AGENT", name)
    os.environ["ORCH_AGENT"] = name
    agents.reset()
    db.set_meta(conn, META_FAILS, "0")
    log.info("agent switched %s -> %s (by %s; persisted to %s)", old, name, by, path)
    return {"ok": True, "from": old, "to": name, "persisted_to": path,
            "note": ("running missions migrate to the new agent on their next "
                     "wake, each seeded with a handoff summary of its prior "
                     "session")}


def record_turn_result(conn: sqlite3.Connection, mission_id: str,
                       agent_name: str, *, started_ts: int, ended_ts: int) -> None:
    """Called by the engine when a worker turn ends. Detects worker activity
    since launch; updates last-good / consecutive-failure state and triggers
    the automatic fallback when the active backend looks dead."""
    active = agents.get_adapter().name
    produced = _worker_activity_since(conn, mission_id, started_ts)
    if agent_name != active:
        # A pinned-backend turn (or a straggler from before a switch). Grade
        # it against the PIN: two dead turns on a pinned backend auto-unpin
        # the mission back to the global agent instead of letting it stall.
        m = db.get_mission(conn, mission_id)
        pinned = (m["pinned_agent"] if m and "pinned_agent" in m.keys() else None)
        if not pinned or agents.canonical(pinned) != agent_name:
            return
        key = f"{META_FAILS}:{mission_id}"
        if produced:
            db.set_meta(conn, key, "0")
            return
        if ended_ts - started_ts >= config.AGENT_FAST_FAIL_S:
            return
        fails = int(db.get_meta(conn, key, "0") or 0) + 1
        db.set_meta(conn, key, str(fails))
        log.warning("pinned agent '%s': fast quiet turn on %s (%d consecutive)",
                    agent_name, mission_id, fails)
        if config.agent_auto_fallback() and fails >= config.AGENT_FAIL_LIMIT:
            db.set_mission_pinned_agent(conn, mission_id, None)
            db.set_meta(conn, key, "0")
            db.log_event(conn, mission_id=mission_id, kind="agent_pin_dropped",
                         payload={"pinned": agent_name,
                                  "reason": f"{fails} consecutive dead turns"})
            log.error("mission %s: pinned agent '%s' looks dead - unpinned back "
                      "to global '%s'", mission_id, agent_name, active)
        return
    if produced:
        db.set_meta(conn, META_LAST_GOOD, agent_name)
        db.set_meta(conn, META_FAILS, "0")
        return
    if ended_ts - started_ts >= config.AGENT_FAST_FAIL_S:
        return  # long quiet turn - could be legitimate silent work; neutral
    fails = int(db.get_meta(conn, META_FAILS, "0") or 0) + 1
    db.set_meta(conn, META_FAILS, str(fails))
    log.warning("agent '%s': fast quiet turn on %s (%d consecutive)",
                agent_name, mission_id, fails)
    if not config.agent_auto_fallback() or fails < config.AGENT_FAIL_LIMIT:
        return
    last_good = db.get_meta(conn, META_LAST_GOOD)
    if not last_good or agents.canonical(last_good) == agent_name:
        return  # nowhere better to go
    ok, _ = agents.availability(last_good)
    if not ok:
        return
    try:
        res = set_active_agent(conn, last_good, by="auto-fallback")
    except ValueError:
        return
    db.log_event(conn, mission_id=mission_id, kind="agent_fallback",
                 payload={"from": agent_name, "to": last_good,
                          "after_failures": fails})
    log.error("agent '%s' looks dead - auto-fell back to '%s' (%s)",
              agent_name, last_good, res["persisted_to"])


def _worker_activity_since(conn: sqlite3.Connection, mission_id: str, ts: int) -> bool:
    """Did the worker demonstrably run since ts? Any user-visible output,
    self-queued step, or watcher heartbeat counts."""
    row = conn.execute(
        "SELECT 1 FROM events WHERE mission_id = ? AND ts >= ?"
        " AND kind IN ('notify_sent','notify_file_sent','host_message_sent',"
        "'step_added','heartbeat_set','scripted_ping_added','secret_accessed',"
        "'cookies_accessed','mission_held')"
        " LIMIT 1", (mission_id, ts),
    ).fetchone()
    if row:
        return True
    row = conn.execute(
        "SELECT 1 FROM scripted_pings WHERE mission_id = ?"
        " AND last_alive_at >= ? LIMIT 1", (mission_id, ts),
    ).fetchone()
    return row is not None
