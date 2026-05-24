"""SQLite schema + thin access helpers. Synchronous; daemon serializes access."""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any, Iterable

from . import config

SCHEMA = """
CREATE TABLE IF NOT EXISTS missions (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  state TEXT NOT NULL,
  tmux_session TEXT NOT NULL,
  telegram_chat_id TEXT NOT NULL,
  heartbeat_interval_s INTEGER NOT NULL DEFAULT 86400,
  created_at INTEGER NOT NULL,
  finished_at INTEGER,
  restart_count INTEGER NOT NULL DEFAULT 0,
  last_heartbeat_at INTEGER
);
CREATE TABLE IF NOT EXISTS steps (
  id TEXT PRIMARY KEY,
  mission_id TEXT NOT NULL REFERENCES missions(id) ON DELETE CASCADE,
  position INTEGER NOT NULL,
  directive TEXT NOT NULL,
  cue_type TEXT NOT NULL,
  cue_payload TEXT,
  state TEXT NOT NULL,
  started_at INTEGER,
  finished_at INTEGER,
  created_by TEXT NOT NULL,
  UNIQUE(mission_id, position)
);
CREATE TABLE IF NOT EXISTS pings (
  id TEXT PRIMARY KEY,
  mission_id TEXT NOT NULL REFERENCES missions(id) ON DELETE CASCADE,
  command TEXT NOT NULL,
  mode_type TEXT NOT NULL,
  interval_s INTEGER,
  last_fired_at INTEGER,
  created_by TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  mission_id TEXT NOT NULL,
  ts INTEGER NOT NULL,
  kind TEXT NOT NULL,
  step_id TEXT,
  ping_id TEXT,
  payload TEXT
);
CREATE TABLE IF NOT EXISTS notify_map (
  telegram_message_id INTEGER PRIMARY KEY,
  mission_id TEXT NOT NULL,
  ts INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS scripted_pings (
  id TEXT PRIMARY KEY,
  mission_id TEXT NOT NULL REFERENCES missions(id) ON DELETE CASCADE,
  condition TEXT NOT NULL,
  action TEXT NOT NULL,
  timeout_s INTEGER NOT NULL,
  state TEXT NOT NULL,            -- setup | active | broken
  script_path TEXT,
  last_alive_at INTEGER,
  created_at INTEGER NOT NULL,
  created_by TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_steps_mission ON steps(mission_id, position);
CREATE INDEX IF NOT EXISTS idx_events_mission_ts ON events(mission_id, ts);
CREATE INDEX IF NOT EXISTS idx_notify_map_mission ON notify_map(mission_id);
"""


def now_ts() -> int:
    return int(time.time())


def new_id() -> str:
    return str(uuid.uuid4())


def connect(path: Path | None = None) -> sqlite3.Connection:
    config.ensure_dirs()
    p = path or config.DB_PATH
    conn = sqlite3.connect(str(p), isolation_level=None, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)


def map_notify_message(conn: sqlite3.Connection, message_id: int, mission_id: str) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO notify_map (telegram_message_id, mission_id, ts)"
        " VALUES (?, ?, ?)",
        (int(message_id), mission_id, now_ts()),
    )


def mission_for_notify(conn: sqlite3.Connection, message_id: int) -> str | None:
    row = conn.execute(
        "SELECT mission_id FROM notify_map WHERE telegram_message_id = ?",
        (int(message_id),),
    ).fetchone()
    return row["mission_id"] if row else None


# ---------- mission helpers ----------


def create_mission(
    conn: sqlite3.Connection,
    *,
    mission_id: str,
    name: str,
    tmux_session: str,
    telegram_chat_id: str,
    heartbeat_interval_s: int,
) -> None:
    conn.execute(
        "INSERT INTO missions (id, name, state, tmux_session, telegram_chat_id,"
        " heartbeat_interval_s, created_at, last_heartbeat_at)"
        " VALUES (?, ?, 'running', ?, ?, ?, ?, ?)",
        (
            mission_id,
            name,
            tmux_session,
            telegram_chat_id,
            heartbeat_interval_s,
            now_ts(),
            now_ts(),
        ),
    )


def get_mission(conn: sqlite3.Connection, mission_id: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM missions WHERE id = ?", (mission_id,)).fetchone()


def list_missions(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute("SELECT * FROM missions ORDER BY created_at DESC").fetchall()


def set_mission_state(
    conn: sqlite3.Connection, mission_id: str, state: str, *, finished: bool = False
) -> None:
    if finished:
        conn.execute(
            "UPDATE missions SET state = ?, finished_at = ? WHERE id = ?",
            (state, now_ts(), mission_id),
        )
    else:
        conn.execute("UPDATE missions SET state = ? WHERE id = ?", (state, mission_id))


def update_mission_heartbeat(conn: sqlite3.Connection, mission_id: str, ts: int) -> None:
    conn.execute(
        "UPDATE missions SET last_heartbeat_at = ? WHERE id = ?", (ts, mission_id)
    )


def set_heartbeat_interval(
    conn: sqlite3.Connection, mission_id: str, interval_s: int
) -> None:
    conn.execute(
        "UPDATE missions SET heartbeat_interval_s = ? WHERE id = ?",
        (interval_s, mission_id),
    )


def bump_restart_count(conn: sqlite3.Connection, mission_id: str) -> None:
    conn.execute(
        "UPDATE missions SET restart_count = restart_count + 1 WHERE id = ?",
        (mission_id,),
    )


# ---------- step helpers ----------


def add_step(
    conn: sqlite3.Connection,
    *,
    mission_id: str,
    directive: str,
    cue: dict[str, Any],
    created_by: str,
    position: int | None = None,
) -> str:
    step_id = new_id()
    if position is None:
        row = conn.execute(
            "SELECT COALESCE(MAX(position), -1) + 1 AS p FROM steps WHERE mission_id = ?",
            (mission_id,),
        ).fetchone()
        position = row["p"]
    conn.execute(
        "INSERT INTO steps (id, mission_id, position, directive, cue_type, cue_payload,"
        " state, created_by) VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)",
        (
            step_id,
            mission_id,
            position,
            directive,
            cue["type"],
            json.dumps(cue),
            created_by,
        ),
    )
    return step_id


def list_steps(conn: sqlite3.Connection, mission_id: str) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM steps WHERE mission_id = ? ORDER BY position ASC", (mission_id,)
    ).fetchall()


def get_step(conn: sqlite3.Connection, step_id: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM steps WHERE id = ?", (step_id,)).fetchone()


def update_step(
    conn: sqlite3.Connection,
    step_id: str,
    *,
    directive: str | None = None,
    cue: dict[str, Any] | None = None,
) -> None:
    sets: list[str] = []
    args: list[Any] = []
    if directive is not None:
        sets.append("directive = ?")
        args.append(directive)
    if cue is not None:
        sets.append("cue_type = ?")
        args.append(cue["type"])
        sets.append("cue_payload = ?")
        args.append(json.dumps(cue))
    if not sets:
        return
    args.append(step_id)
    conn.execute(f"UPDATE steps SET {', '.join(sets)} WHERE id = ?", args)


def delete_step(conn: sqlite3.Connection, step_id: str) -> None:
    conn.execute("DELETE FROM steps WHERE id = ?", (step_id,))


def set_step_state(
    conn: sqlite3.Connection,
    step_id: str,
    state: str,
    *,
    started: bool = False,
    finished: bool = False,
) -> None:
    sets = ["state = ?"]
    args: list[Any] = [state]
    if started:
        sets.append("started_at = ?")
        args.append(now_ts())
    if finished:
        sets.append("finished_at = ?")
        args.append(now_ts())
    args.append(step_id)
    conn.execute(f"UPDATE steps SET {', '.join(sets)} WHERE id = ?", args)


def current_step(conn: sqlite3.Connection, mission_id: str) -> sqlite3.Row | None:
    """The step that is running, or the earliest pending step."""
    row = conn.execute(
        "SELECT * FROM steps WHERE mission_id = ? AND state = 'running'"
        " ORDER BY position ASC LIMIT 1",
        (mission_id,),
    ).fetchone()
    if row:
        return row
    return conn.execute(
        "SELECT * FROM steps WHERE mission_id = ? AND state = 'pending'"
        " ORDER BY position ASC LIMIT 1",
        (mission_id,),
    ).fetchone()


def previous_step(
    conn: sqlite3.Connection, mission_id: str, position: int
) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM steps WHERE mission_id = ? AND position < ?"
        " ORDER BY position DESC LIMIT 1",
        (mission_id, position),
    ).fetchone()


# ---------- ping helpers ----------


def add_ping(
    conn: sqlite3.Connection,
    *,
    mission_id: str,
    command: str,
    mode: dict[str, Any],
    created_by: str,
) -> str:
    pid = new_id()
    interval_s = mode.get("seconds") if mode.get("type") == "on_schedule" else None
    conn.execute(
        "INSERT INTO pings (id, mission_id, command, mode_type, interval_s, created_by)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        (pid, mission_id, command, mode["type"], interval_s, created_by),
    )
    return pid


def list_pings(conn: sqlite3.Connection, mission_id: str) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM pings WHERE mission_id = ?", (mission_id,)
    ).fetchall()


def get_ping(conn: sqlite3.Connection, ping_id: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM pings WHERE id = ?", (ping_id,)).fetchone()


def update_ping(
    conn: sqlite3.Connection,
    ping_id: str,
    *,
    command: str | None = None,
    mode: dict[str, Any] | None = None,
) -> None:
    sets: list[str] = []
    args: list[Any] = []
    if command is not None:
        sets.append("command = ?")
        args.append(command)
    if mode is not None:
        sets.append("mode_type = ?")
        args.append(mode["type"])
        sets.append("interval_s = ?")
        args.append(mode.get("seconds"))
    if not sets:
        return
    args.append(ping_id)
    conn.execute(f"UPDATE pings SET {', '.join(sets)} WHERE id = ?", args)


def delete_ping(conn: sqlite3.Connection, ping_id: str) -> None:
    conn.execute("DELETE FROM pings WHERE id = ?", (ping_id,))


def mark_ping_fired(conn: sqlite3.Connection, ping_id: str, ts: int) -> None:
    conn.execute("UPDATE pings SET last_fired_at = ? WHERE id = ?", (ts, ping_id))


# ---------- scripted pings (autonomous watcher scripts) ----------


def add_scripted_ping(
    conn: sqlite3.Connection, *, mission_id: str, condition: str,
    action: str, timeout_s: int, created_by: str,
) -> str:
    spid = new_id()
    conn.execute(
        "INSERT INTO scripted_pings (id, mission_id, condition, action, timeout_s,"
        " state, created_at, created_by) VALUES (?, ?, ?, ?, ?, 'setup', ?, ?)",
        (spid, mission_id, condition, action, int(timeout_s), now_ts(), created_by),
    )
    return spid


def list_scripted_pings(conn: sqlite3.Connection, mission_id: str) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM scripted_pings WHERE mission_id = ?", (mission_id,)
    ).fetchall()


def get_scripted_ping(conn: sqlite3.Connection, spid: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM scripted_pings WHERE id = ?", (spid,)).fetchone()


def delete_scripted_ping(conn: sqlite3.Connection, spid: str) -> None:
    conn.execute("DELETE FROM scripted_pings WHERE id = ?", (spid,))


def set_scripted_ping_state(
    conn: sqlite3.Connection, spid: str, state: str,
    script_path: str | None = None, touch_alive: bool = False,
) -> None:
    sets = ["state = ?"]
    args: list[Any] = [state]
    if script_path is not None:
        sets.append("script_path = ?")
        args.append(script_path)
    if touch_alive:
        sets.append("last_alive_at = ?")
        args.append(now_ts())
    args.append(spid)
    conn.execute(f"UPDATE scripted_pings SET {', '.join(sets)} WHERE id = ?", args)


def touch_scripted_ping_alive(conn: sqlite3.Connection, spid: str) -> None:
    conn.execute(
        "UPDATE scripted_pings SET last_alive_at = ? WHERE id = ?", (now_ts(), spid)
    )


def session_started(conn: sqlite3.Connection, mission_id: str) -> bool:
    """Has any Claude process been launched for this mission yet? The first
    launch must use `claude --session-id`; all later ones use `--resume`.
    Inferred from the event log so it survives daemon restarts."""
    row = conn.execute(
        "SELECT 1 FROM events WHERE mission_id = ? AND ("
        " kind = 'step_launched' OR kind = 'mission_resumed'"
        " OR kind LIKE 'oob_%_launched') LIMIT 1",
        (mission_id,),
    ).fetchone()
    return row is not None


# ---------- events ----------


def log_event(
    conn: sqlite3.Connection,
    *,
    mission_id: str,
    kind: str,
    step_id: str | None = None,
    ping_id: str | None = None,
    payload: dict[str, Any] | None = None,
) -> None:
    conn.execute(
        "INSERT INTO events (mission_id, ts, kind, step_id, ping_id, payload)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        (
            mission_id,
            now_ts(),
            kind,
            step_id,
            ping_id,
            json.dumps(payload) if payload is not None else None,
        ),
    )


def row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    return dict(row) if row is not None else None


def rows_to_dicts(rows: Iterable[sqlite3.Row]) -> list[dict[str, Any]]:
    return [dict(r) for r in rows]
