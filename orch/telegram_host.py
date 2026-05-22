"""Optional inbound Telegram command host.

Long-polls a dedicated bot (token from ORCH_HOST_BOT_TOKEN) for chat messages
and dispatches /commands to the daemon's RPC handlers in-process. Replies via
sendMessage on the same bot.

The bot used here MUST be different from the outbound notification bot (the
one worker.notify sends to). Telegram allows exactly one getUpdates consumer
per bot at a time, and the user's existing telegram plugin already owns that
slot on the notification bot.

Allowlist is via ORCH_HOST_ALLOWED_CHAT_IDS (comma-separated chat ids).
Unauthenticated messages are silently dropped (and logged).
"""

from __future__ import annotations

import asyncio
import dataclasses
import datetime as _dt
import json
import logging
import os
import shlex
from typing import Any, Awaitable, Callable

import httpx

from . import config, db, runner

log = logging.getLogger(__name__)


class CommandError(Exception):
    pass


@dataclasses.dataclass
class Reply:
    text: str
    markup: dict | None = None  # Telegram InlineKeyboardMarkup dict


CommandFn = Callable[["TelegramHost", list[str]], "str | Reply"]


def _btn(text: str, data: str) -> dict:
    return {"text": text, "callback_data": data}


def _ikb(rows: list[list[dict]]) -> dict:
    return {"inline_keyboard": rows}


def _main_menu_kb() -> dict:
    return _ikb([
        [_btn("📋 list missions", "missions")],
        [_btn("❓ all commands (text help)", "helptext")],
    ])


def _mission_actions_kb(state: str, sid: str) -> dict:
    rows: list[list[dict]] = [
        [_btn("📺 pane", f"pane:{sid}"), _btn("📜 events", f"events:{sid}")],
    ]
    if state == "running":
        rows.append([
            _btn("✋ soft cancel", f"cancel:{sid}"),
            _btn("💥 force cancel", f"forcecancel:{sid}"),
        ])
    elif state in ("completed", "cancelled", "failed"):
        rows.append([_btn("🗑 delete", f"delete:{sid}")])
    rows.append([
        _btn("🔄 refresh", f"m:{sid}"),
        _btn("← back to missions", "missions"),
    ])
    return _ikb(rows)


def _missions_list_kb(rows_db: list[Any]) -> dict:
    rows: list[list[dict]] = []
    for r in rows_db:
        marker = {
            "running": "▶", "cancelling": "…",
            "completed": "✓", "cancelled": "✗", "failed": "!",
        }.get(r["state"], "?")
        # Show name + state on the button label; callback resolves the short id.
        label = f"{marker} {r['name']} ({r['state']})"
        if len(label) > 50:
            label = label[:47] + "…"
        rows.append([_btn(label, f"m:{_short(r['id'])}")])
    rows.append([_btn("🔄 refresh", "missions"), _btn("❓ help", "help")])
    return _ikb(rows)


def _short(mid: str) -> str:
    return mid.split("-", 1)[0]


def _ts(ts: int) -> str:
    return _dt.datetime.fromtimestamp(ts).strftime("%H:%M:%S")


class TelegramHost:
    def __init__(self, daemon: Any) -> None:
        self.daemon = daemon
        self.token = os.environ.get(config.ENV_HOST_BOT_TOKEN, "").strip()
        allowed_raw = os.environ.get(config.ENV_HOST_ALLOWED_CHATS, "")
        self.allowed: set[str] = {
            cid.strip() for cid in allowed_raw.split(",") if cid.strip()
        }
        self._stop = asyncio.Event()
        self._offset = 0
        self._bot_username: str | None = None

    def configured(self) -> bool:
        return bool(self.token and self.allowed)

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        if not self.configured():
            log.info(
                "telegram_host: disabled (set %s and %s to enable)",
                config.ENV_HOST_BOT_TOKEN, config.ENV_HOST_ALLOWED_CHATS,
            )
            return
        log.info(
            "telegram_host: starting (allowlist: %d chats)", len(self.allowed)
        )
        async with httpx.AsyncClient(timeout=35) as client:
            await self._fetch_bot_info(client)
            while not self._stop.is_set():
                try:
                    await self._poll_once(client)
                except Exception:
                    log.exception("telegram_host poll error")
                    await asyncio.sleep(5)
        log.info("telegram_host: stopped")

    async def _fetch_bot_info(self, client: httpx.AsyncClient) -> None:
        try:
            r = await client.get(
                f"https://api.telegram.org/bot{self.token}/getMe", timeout=10
            )
            data = r.json()
            if data.get("ok"):
                self._bot_username = data["result"].get("username")
                log.info("telegram_host: connected as @%s", self._bot_username)
        except Exception:
            log.exception("telegram_host getMe failed")

    async def _poll_once(self, client: httpx.AsyncClient) -> None:
        url = f"https://api.telegram.org/bot{self.token}/getUpdates"
        params = {
            "offset": self._offset,
            "timeout": 25,
            "allowed_updates": json.dumps(["message", "callback_query"]),
        }
        try:
            r = await client.get(url, params=params)
        except httpx.RequestError as e:
            log.warning("telegram_host getUpdates network: %s", e)
            await asyncio.sleep(3)
            return
        if r.status_code != 200:
            await asyncio.sleep(3)
            return
        data = r.json()
        if not data.get("ok"):
            await asyncio.sleep(3)
            return
        for update in data.get("result", []):
            self._offset = max(self._offset, update["update_id"] + 1)
            if "message" in update and "text" in update["message"]:
                msg = update["message"]
                chat_id = str(msg["chat"]["id"])
                if chat_id not in self.allowed:
                    log.warning(
                        "telegram_host: dropping msg from non-allowlisted chat %s",
                        chat_id,
                    )
                    continue
                await self._handle(client, chat_id, msg["text"], msg.get("message_id"))
            elif "callback_query" in update:
                cq = update["callback_query"]
                chat_id = str(cq["message"]["chat"]["id"])
                if chat_id not in self.allowed:
                    log.warning(
                        "telegram_host: dropping callback from non-allowlisted chat %s",
                        chat_id,
                    )
                    continue
                # Answer the callback first so Telegram clears the spinner.
                await self._answer_cb(client, cq["id"])
                await self._handle_callback(client, chat_id, cq.get("data") or "")

    async def _send(
        self, client: httpx.AsyncClient, chat_id: str, text: str,
        reply_to: int | None = None, markup: dict | None = None,
    ) -> None:
        if len(text) > 3900:
            text = text[:3890] + "\n…[truncated]"
        body: dict[str, Any] = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "Markdown",
        }
        if reply_to is not None:
            body["reply_to_message_id"] = reply_to
        if markup is not None:
            body["reply_markup"] = markup
        try:
            r = await client.post(
                f"https://api.telegram.org/bot{self.token}/sendMessage",
                json=body, timeout=10,
            )
            if r.status_code != 200:
                body.pop("parse_mode", None)
                await client.post(
                    f"https://api.telegram.org/bot{self.token}/sendMessage",
                    json=body, timeout=10,
                )
        except Exception:
            log.exception("telegram_host send failed")

    async def _answer_cb(self, client: httpx.AsyncClient, cb_id: str) -> None:
        try:
            await client.post(
                f"https://api.telegram.org/bot{self.token}/answerCallbackQuery",
                json={"callback_query_id": cb_id}, timeout=5,
            )
        except Exception:
            pass

    async def _dispatch_and_send(
        self, client: httpx.AsyncClient, chat_id: str,
        cmd: str, args: list[str], reply_to: int | None = None,
    ) -> None:
        fn = _COMMANDS.get(cmd)
        if fn is None:
            await self._send(client, chat_id, f"❌ unknown command `{cmd}`. Try /help", reply_to)
            return
        try:
            result = fn(self, args)
        except CommandError as e:
            await self._send(client, chat_id, f"❌ {e}", reply_to)
            return
        except Exception as e:
            log.exception("command %s failed", cmd)
            await self._send(client, chat_id, f"❌ internal error: {e}", reply_to)
            return
        if isinstance(result, Reply):
            await self._send(client, chat_id, result.text, reply_to, markup=result.markup)
        elif isinstance(result, str) and result:
            await self._send(client, chat_id, result, reply_to)

    async def _handle_callback(
        self, client: httpx.AsyncClient, chat_id: str, data: str,
    ) -> None:
        # Callback data format: <action>[:<arg>]
        if ":" in data:
            action, arg = data.split(":", 1)
        else:
            action, arg = data, ""
        # Map callback action codes to command names + args.
        if action == "missions":
            await self._dispatch_and_send(client, chat_id, "missions", [])
        elif action == "help":
            await self._dispatch_and_send(client, chat_id, "help", [])
        elif action == "helptext":
            await self._dispatch_and_send(client, chat_id, "helptext", [])
        elif action == "m":
            await self._dispatch_and_send(client, chat_id, "m", [arg])
        elif action == "pane":
            await self._dispatch_and_send(client, chat_id, "pane", [arg])
        elif action == "events":
            await self._dispatch_and_send(client, chat_id, "events", [arg])
        elif action == "cancel":
            await self._dispatch_and_send(client, chat_id, "cancel", [arg])
        elif action == "forcecancel":
            await self._dispatch_and_send(client, chat_id, "forcecancel", [arg])
        elif action == "delete":
            await self._dispatch_and_send(client, chat_id, "delete", [arg])
        else:
            await self._send(client, chat_id, f"❌ unknown action `{action}`")

    async def _handle(
        self, client: httpx.AsyncClient, chat_id: str, text: str,
        msg_id: int | None,
    ) -> None:
        text = text.strip()
        if not text.startswith("/"):
            return
        try:
            parts = shlex.split(text)
        except ValueError as e:
            await self._send(client, chat_id, f"❌ parse error: {e}", msg_id)
            return
        cmd_raw = parts[0].lstrip("/")
        if "@" in cmd_raw:
            cmd_raw = cmd_raw.split("@", 1)[0]
        cmd = cmd_raw.lower()
        args = parts[1:]
        await self._dispatch_and_send(client, chat_id, cmd, args, reply_to=msg_id)

    # ---------- helpers ----------

    def _resolve_mid(self, prefix: str) -> str:
        if not prefix:
            raise CommandError("missing mission id (use /missions to list)")
        # Exact match wins.
        row = self.daemon.conn.execute(
            "SELECT id FROM missions WHERE id = ?", (prefix,)
        ).fetchone()
        if row is not None:
            return row["id"]
        # Try prefix.
        rows = self.daemon.conn.execute(
            "SELECT id, name FROM missions WHERE id LIKE ? LIMIT 5",
            (prefix + "%",),
        ).fetchall()
        if not rows:
            # Try by exact name.
            rows = self.daemon.conn.execute(
                "SELECT id, name FROM missions WHERE name = ? LIMIT 5",
                (prefix,),
            ).fetchall()
            if not rows:
                raise CommandError(f"no mission matches `{prefix}`")
        if len(rows) > 1:
            opts = "\n".join(f"  {_short(r['id'])}  {r['name']}" for r in rows)
            raise CommandError(f"ambiguous `{prefix}`:\n{opts}")
        return rows[0]["id"]

    def _rpc(self, method: str, params: dict[str, Any]) -> Any:
        # Late import to avoid circular import at module load.
        from . import daemon as _d
        handler = _d._HANDLERS.get(method)
        if handler is None:
            raise CommandError(f"no such RPC: {method}")
        return handler(self.daemon, params)


# ---------- commands ----------


def _cmd_help(self: TelegramHost, args: list[str]) -> Reply:
    return Reply(
        text=(
            "*Mission Orchestrator*\n\n"
            "Tap a button to navigate. Or type any of the commands listed under "
            "`❓ all commands`."
        ),
        markup=_main_menu_kb(),
    )


def _cmd_helptext(self: TelegramHost, args: list[str]) -> str:
    return (
        "*all commands*\n"
        "`/missions` - list all missions (also gives a button menu)\n"
        "`/m <id>` - mission detail with action buttons\n"
        "`/create <name>` - create mission\n"
        "`/step <id> <directive…>` - add a step (cue auto-detected)\n"
        "`/cancel <id>` - soft cancel\n"
        "`/forcecancel <id>` - hard cancel\n"
        "`/delete <id>` - remove a terminal mission\n"
        "`/pane <id> [lines]` - tmux pane content\n"
        "`/events <id>` - recent audit events\n"
        "`/secret <id> <name> <value>` - store a secret\n"
        "`/heartbeat <id> <seconds>` - set heartbeat\n"
        "`/help` - main menu\n\n"
        "Mission ids accept unambiguous prefixes (e.g. `c3b4` for "
        "`c3b4f684-...`) or exact mission names."
    )


def _cmd_missions(self: TelegramHost, args: list[str]) -> Reply:
    rows = self.daemon.conn.execute(
        "SELECT id, name, state, restart_count FROM missions"
        " ORDER BY (state = 'running') DESC, created_at DESC"
    ).fetchall()
    if not rows:
        return Reply(
            text="_(no missions)_\n\nTap below or use `/create <name>` to make one.",
            markup=_ikb([[_btn("❓ help", "help")]]),
        )
    # Limit displayed text rows to 30 to fit Telegram message length; keyboard
    # buttons also capped at 30 for the same reason.
    rows = rows[:30]
    lines = ["*missions* - tap one for details"]
    for r in rows:
        marker = {
            "running": "▶", "cancelling": "…",
            "completed": "✓", "cancelled": "✗", "failed": "!",
        }.get(r["state"], "?")
        lines.append(
            f"`{_short(r['id'])}` {marker} {r['state']:10} {r['name']}"
        )
    return Reply(text="\n".join(lines), markup=_missions_list_kb(rows))


def _cmd_get(self: TelegramHost, args: list[str]) -> Reply:
    if not args:
        raise CommandError("usage: /m <id>")
    mid = self._resolve_mid(args[0])
    snap = self._rpc("mission.get", {"mission_id": mid})
    m = snap["mission"]
    lines = [
        f"*{m['name']}*  `{_short(mid)}`",
        f"state: {m['state']}  restarts: {m['restart_count']}",
        f"heartbeat: every {m['heartbeat_interval_s']}s",
        f"chat: {m['telegram_chat_id']}",
        "",
        f"*steps* ({len(snap['steps'])}):",
    ]
    for s in snap["steps"]:
        body = (s["directive"] or "").splitlines()[0][:70]
        lines.append(f"  [{s['position']}] {s['state']:10} {body}")
    if snap["pings"]:
        lines.append("")
        lines.append(f"*pings* ({len(snap['pings'])}):")
        for p in snap["pings"]:
            mode = (
                f"{p['mode_type']} every {p['interval_s']}s"
                if p["mode_type"] == "on_schedule"
                else p["mode_type"]
            )
            lines.append(f"  {mode}: {p['command'][:60]}")
    return Reply(
        text="\n".join(lines),
        markup=_mission_actions_kb(m["state"], _short(mid)),
    )


def _cmd_create(self: TelegramHost, args: list[str]) -> str:
    if not args:
        raise CommandError("usage: /create <name>")
    name = " ".join(args)
    r = self._rpc("mission.create", {"name": name})
    return f"created `{_short(r['mission_id'])}` ({name})"


def _cmd_step(self: TelegramHost, args: list[str]) -> str:
    if len(args) < 2:
        raise CommandError("usage: /step <id> <directive…>")
    mid = self._resolve_mid(args[0])
    directive = " ".join(args[1:])
    # Auto-detect cue: immediate if no steps yet, on_prev_complete otherwise.
    existing = self.daemon.conn.execute(
        "SELECT COUNT(*) AS n FROM steps WHERE mission_id = ?", (mid,),
    ).fetchone()["n"]
    cue = {"type": "immediate"} if existing == 0 else {"type": "on_prev_complete"}
    r = self._rpc(
        "step.add",
        {"mission_id": mid, "directive": directive, "cue": cue, "created_by": "telegram_host"},
    )
    return f"step `{_short(r['step_id'])}` queued (cue: {cue['type']})"


def _cmd_cancel(self: TelegramHost, args: list[str]) -> str:
    if not args:
        raise CommandError("usage: /cancel <id>")
    mid = self._resolve_mid(args[0])
    r = self._rpc("mission.cancel", {"mission_id": mid})
    return f"cancel: {json.dumps(r)}"


def _cmd_forcecancel(self: TelegramHost, args: list[str]) -> str:
    if not args:
        raise CommandError("usage: /forcecancel <id>")
    mid = self._resolve_mid(args[0])
    r = self._rpc("mission.cancel", {"mission_id": mid, "force": True})
    return f"hard-cancelled `{_short(mid)}`: {json.dumps(r)}"


def _cmd_delete(self: TelegramHost, args: list[str]) -> str:
    if not args:
        raise CommandError("usage: /delete <id>")
    mid = self._resolve_mid(args[0])
    self._rpc("mission.delete", {"mission_id": mid})
    return f"deleted `{_short(mid)}`"


def _cmd_pane(self: TelegramHost, args: list[str]) -> str:
    if not args:
        raise CommandError("usage: /pane <id> [lines]")
    mid = self._resolve_mid(args[0])
    lines = int(args[1]) if len(args) > 1 else 30
    r = self._rpc("mission.pane_snapshot", {"mission_id": mid, "lines": lines})
    header = f"pane `{_short(mid)}` (alive={r['alive']}, src={r.get('source','?')})"
    body = r["pane_content"] or "(empty)"
    return f"{header}\n```\n{body}\n```"


def _cmd_events(self: TelegramHost, args: list[str]) -> str:
    if not args:
        raise CommandError("usage: /events <id>")
    mid = self._resolve_mid(args[0])
    evs = self._rpc("mission.events", {"mission_id": mid, "limit": 20})
    if not evs:
        return f"(no events for `{_short(mid)}`)"
    lines = [f"*events* `{_short(mid)}`"]
    for e in evs[::-1]:
        lines.append(f"  {_ts(e['ts'])}  {e['kind']}")
    return "\n".join(lines)


def _cmd_secret(self: TelegramHost, args: list[str]) -> str:
    if len(args) < 3:
        raise CommandError("usage: /secret <id> <name> <value>")
    mid = self._resolve_mid(args[0])
    name = args[1]
    value = " ".join(args[2:])
    self._rpc("secret.put", {"mission_id": mid, "name": name, "value": value})
    return f"stored secret `{name}` on `{_short(mid)}` (value not echoed)"


def _cmd_heartbeat(self: TelegramHost, args: list[str]) -> str:
    if len(args) < 2:
        raise CommandError("usage: /heartbeat <id> <seconds>")
    mid = self._resolve_mid(args[0])
    try:
        seconds = int(args[1])
    except ValueError:
        raise CommandError("seconds must be an integer")
    self._rpc("heartbeat.set", {"mission_id": mid, "interval_s": seconds})
    return f"heartbeat set to {seconds}s on `{_short(mid)}`"


def _cmd_start(self: TelegramHost, args: list[str]) -> str:
    return _cmd_help(self, args)


_COMMANDS: dict[str, CommandFn] = {
    "help": _cmd_help,
    "helptext": _cmd_helptext,
    "start": _cmd_start,
    "missions": _cmd_missions,
    "m": _cmd_get,
    "get": _cmd_get,
    "create": _cmd_create,
    "step": _cmd_step,
    "cancel": _cmd_cancel,
    "forcecancel": _cmd_forcecancel,
    "delete": _cmd_delete,
    "pane": _cmd_pane,
    "events": _cmd_events,
    "secret": _cmd_secret,
    "heartbeat": _cmd_heartbeat,
}
