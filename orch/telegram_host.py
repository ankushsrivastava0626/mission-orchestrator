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

from . import agents, config, db, runner, telegram

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
        [_btn("🧠 context sizes", "context")],
        [_btn("❓ all commands (text help)", "helptext")],
    ])


def _mission_actions_kb(state: str, sid: str) -> dict:
    rows: list[list[dict]] = [
        [_btn("📺 pane", f"pane:{sid}"), _btn("📜 events", f"events:{sid}")],
    ]
    # Reply is available for running and completed missions (completed reopens).
    if state in ("running", "cancelling", "completed"):
        rows.append([_btn("💬 reply / talk to worker", f"reply:{sid}")])
    rows.append([_btn("📞 calling name", f"callname:{sid}"),
                 _btn("🗜 compact", f"compact:{sid}")])
    rows.append([_btn("🤖 agent for this mission", f"magent:{sid}")])
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
    rows.append([_btn("🧠 context sizes", "context"),
                 _btn("🔄 refresh", "missions")])
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
        # Forum supergroup hosting one topic per mission (optional). Auto-allow
        # it so messages from its topics aren't dropped.
        self.topics_chat_id: str | None = config.topics_chat_id()
        if self.topics_chat_id:
            self.allowed.add(self.topics_chat_id)
        self._stop = asyncio.Event()
        self._offset = 0
        self._bot_username: str | None = None
        # Sticky conversation: chat_id -> mission_id. A reply pins; plain text
        # then routes to the pinned mission until /unpin or a reply elsewhere.
        self._pinned: dict[str, str] = {}
        # One-shot input prompts: prompt_message_id -> (action, mission_id).
        # The user's reply to that prompt is consumed by the action, not routed.
        self._pending_input: dict[int, tuple] = {}
        # Live "typing…" keeper tasks, kept referenced so they aren't GC'd.
        self._typing_tasks: set = set()
        # Reply coalescing: buffer rapid user messages to one mission and flush
        # them as a SINGLE directive after a short quiet window, so firing off
        # several quick messages wakes the worker once with all of them.
        self._reply_buf: dict[str, list[str]] = {}
        self._reply_ctx: dict[str, tuple] = {}   # mission_id -> (chat_id, thread_id)
        self._reply_timers: dict[str, asyncio.Task] = {}

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
            "allowed_updates": json.dumps(["message", "edited_message", "callback_query"]),
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
            # Location shares (initial or live-edit) arrive as message/edited_message
            # with a `location` field - handle those first.
            loc_msg = None
            edited = "edited_message" in update
            if "message" in update and "location" in update["message"]:
                loc_msg = update["message"]
            elif edited and "location" in update.get("edited_message", {}):
                loc_msg = update["edited_message"]
            if loc_msg is not None:
                chat_id = str(loc_msg["chat"]["id"])
                if chat_id in self.allowed:
                    await self._handle_location(client, chat_id, loc_msg, is_edit=edited)
                continue
            # New forum topic created in the topics group → spin up a mission
            # bound to it (the "create a topic = create a mission" flow).
            tmsg = update.get("message") or {}
            if (self.topics_chat_id and "forum_topic_created" in tmsg
                    and str(tmsg.get("chat", {}).get("id")) == self.topics_chat_id):
                await self._handle_topic_created(client, tmsg)
                continue
            # Attachment (photo / document / audio / video / voice / …) → download
            # it and hand the worker the local path. Captions come along too.
            _media_keys = ("photo", "document", "audio", "video", "voice",
                           "animation", "video_note")
            if "message" in update and any(k in update["message"] for k in _media_keys):
                mmsg = update["message"]
                chat_id = str(mmsg["chat"]["id"])
                if chat_id in self.allowed:
                    await self._handle_media(client, chat_id, mmsg)
                continue
            if "message" in update and "text" in update["message"]:
                msg = update["message"]
                chat_id = str(msg["chat"]["id"])
                if chat_id not in self.allowed:
                    log.warning(
                        "telegram_host: dropping msg from non-allowlisted chat %s",
                        chat_id,
                    )
                    continue
                text = msg["text"]
                msg_id = msg.get("message_id")
                reply_to = msg.get("reply_to_message")
                # 0. Reply to a one-shot input prompt (e.g. set calling name) →
                #    consume it as that action, don't route to a worker.
                if reply_to is not None:
                    pend = self._pending_input.pop(reply_to.get("message_id", -1), None)
                    if pend is not None:
                        await self._consume_input(client, chat_id, pend, text, msg_id)
                        continue
                # 0b. Message inside a mission's forum topic → route straight to
                #     that mission's worker. The topic IS the binding, so no pin
                #     and no banner; replies post back into the same topic.
                thread_id = msg.get("message_thread_id")
                if (self.topics_chat_id and chat_id == self.topics_chat_id
                        and thread_id):
                    tmid = db.mission_for_topic(self.daemon.conn, thread_id)
                    log.info(
                        "telegram_host: topic msg thread=%s -> mission=%s",
                        thread_id, tmid,
                    )
                    if tmid is not None:
                        await self._route_reply(
                            client, chat_id, tmid, text, msg_id, thread_id=thread_id
                        )
                    else:
                        # Unknown topic - say so instead of eating the message.
                        await self._send(
                            client, chat_id,
                            "🤷 this topic isn't bound to any mission, so nobody is "
                            "listening here. Create a new topic to spawn a mission, "
                            "or use a mission's own topic.",
                            msg_id, thread_id=thread_id,
                        )
                    continue
                # 1. Reply to a mapped worker/prompt message → route + pin the
                #    conversation to that mission.
                if reply_to is not None:
                    replied_mid = db.mission_for_notify(
                        self.daemon.conn, reply_to.get("message_id", -1)
                    )
                    if replied_mid is not None:
                        await self._route_reply(client, chat_id, replied_mid, text, msg_id)
                        continue
                # 2. Pin-management commands (need chat_id, handled inline).
                low = text.strip().lower()
                if low in ("/unpin", "/leave", "/exit"):
                    if self._pinned.pop(chat_id, None):
                        await self._send(client, chat_id, "📌 conversation unpinned. Plain messages won't route to a mission now.", msg_id)
                    else:
                        await self._send(client, chat_id, "no active conversation to unpin.", msg_id)
                    continue
                if low in ("/here", "/pinned"):
                    pin = self._pinned.get(chat_id)
                    if pin:
                        row = self.daemon.conn.execute("SELECT name FROM missions WHERE id=?", (pin,)).fetchone()
                        await self._send(client, chat_id, f"📌 pinned to *{row['name'] if row else pin[:8]}*. Plain messages go there; /unpin to stop.", msg_id)
                    else:
                        await self._send(client, chat_id, "no mission pinned. Reply to one (or tap 💬 reply) to start a conversation.", msg_id)
                    continue
                # 3. Other slash commands → normal command dispatch.
                if text.strip().startswith("/"):
                    await self._handle(client, chat_id, text, msg_id)
                    continue
                # 4. Plain text → route to the pinned mission (sticky conversation).
                pinned = self._pinned.get(chat_id)
                if pinned:
                    await self._route_reply(client, chat_id, pinned, text, msg_id)
                    continue
                # 5. Nothing pinned: gently guide.
                await self._send(
                    client, chat_id,
                    "No active conversation. Reply to a mission's message (or tap 💬 reply on its "
                    "detail) to start one - after that, just type and it keeps going to that mission. "
                    "Use /missions to browse.",
                    msg_id,
                )
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
        force_reply: bool = False, thread_id: int | None = None,
    ) -> int | None:
        """Send a message. Returns the sent message_id (or None on failure)."""
        if len(text) > 3900:
            text = text[:3890] + "\n…[truncated]"
        body: dict[str, Any] = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "Markdown",
        }
        if thread_id is not None:
            body["message_thread_id"] = thread_id
        if reply_to is not None:
            body["reply_to_message_id"] = reply_to
        if markup is not None:
            body["reply_markup"] = markup
        elif force_reply:
            body["reply_markup"] = {"force_reply": True}
        try:
            r = await client.post(
                f"https://api.telegram.org/bot{self.token}/sendMessage",
                json=body, timeout=10,
            )
            if r.status_code != 200:
                body.pop("parse_mode", None)
                r = await client.post(
                    f"https://api.telegram.org/bot{self.token}/sendMessage",
                    json=body, timeout=10,
                )
            data = r.json()
            if data.get("ok"):
                return data["result"]["message_id"]
        except Exception:
            log.exception("telegram_host send failed")
        return None

    async def _consume_input(
        self, client: httpx.AsyncClient, chat_id: str, pend: tuple,
        text: str, msg_id: int | None,
    ) -> None:
        action, mid = pend
        if action == "oc_model":
            # mid holds the short id from the picker; text is the model id.
            await self._apply_opencode(client, chat_id, mid, text.strip())
            return
        if action == "call_name":
            val = text.strip()
            clear = val in ("-", "")
            try:
                r = self._rpc("mission.set_call_name", {
                    "mission_id": mid, "call_name": None if clear else val,
                })
            except Exception as e:  # noqa: BLE001
                await self._send(client, chat_id, f"❌ {e}", msg_id)
                return
            row = self.daemon.conn.execute("SELECT name FROM missions WHERE id=?", (mid,)).fetchone()
            mname = row["name"] if row else mid
            if r.get("call_name"):
                await self._send(client, chat_id, f"📞 calling name for *{mname}* set to *{r['call_name']}*.", msg_id)
            else:
                await self._send(client, chat_id, f"📞 calling name for *{mname}* cleared (will use the mission name).", msg_id)
            return
        await self._send(client, chat_id, f"❌ unknown input action {action}", msg_id)

    async def _do_compact(
        self, client: httpx.AsyncClient, chat_id: str, sid: str,
    ) -> None:
        try:
            mid = self._resolve_mid(sid)
        except CommandError as e:
            await self._send(client, chat_id, f"❌ {e}")
            return
        info = self._rpc("mission.context_info", {"mission_id": mid})
        row = self.daemon.conn.execute("SELECT name FROM missions WHERE id=?", (mid,)).fetchone()
        mname = row["name"] if row else sid
        if not info.get("available"):
            tok_line = "context size: unknown (no transcript yet)"
        else:
            mb = f", {info['transcript_mb']} MB" if info.get("transcript_mb") is not None else ""
            tok_line = (f"context now: *{info['context_tokens']:,} tokens* "
                        f"({info['turns']} turns{mb}) - "
                        f"re-read on every wake")
        try:
            r = self._rpc("mission.compact", {"mission_id": mid})
        except Exception as e:  # noqa: BLE001
            await self._send(client, chat_id, f"🗜 *{mname}*\n{tok_line}\n\n❌ {e}")
            return
        await self._send(
            client, chat_id,
            f"🗜 *{mname}*\n{tok_line}\n\nCompaction started - it'll shrink to a small "
            f"summary shortly; future wakes get much cheaper. Tap 🔄 refresh / 🗜 again "
            f"in a minute to see the new size.",
        )

    async def _prompt_mission_agent(
        self, client: httpx.AsyncClient, chat_id: str, sid: str,
    ) -> None:
        """Per-mission agent picker: pin this one mission to a backend while
        every other mission keeps the global agent."""
        try:
            mid = self._resolve_mid(sid)
        except CommandError as e:
            await self._send(client, chat_id, f"❌ {e}")
            return
        row = self.daemon.conn.execute(
            "SELECT name, agent, pinned_agent FROM missions WHERE id = ?", (mid,)
        ).fetchone()
        st = self._rpc("agent.get", {})
        cur = row["pinned_agent"] or f"global ({st['active']})"
        lines = [f"🤖 *{row['name']}* - worker AI for this mission only",
                 f"now: *{cur}*  (session lives on: {row['agent'] or 'claude'})",
                 "", "pick a backend (✅ = usable on this machine):"]
        kb_rows: list[list[dict]] = []
        for name, b in st["backends"].items():
            mark = "✅" if b["available"] else "🚫"
            kb_rows.append([_btn(f"{mark} {name}", f"magentset:{sid}:{name}")])
        kb_rows.append([_btn("🌐 use global agent", f"magentset:{sid}:-")])
        kb_rows.append([_btn("← back", f"m:{sid}")])
        await self._send(client, chat_id, "\n".join(lines), markup=_ikb(kb_rows))

    async def _set_mission_agent(
        self, client: httpx.AsyncClient, chat_id: str, arg: str,
    ) -> None:
        sid, _, name = arg.partition(":")
        if name == "opencode":
            # OpenCode = any model behind one key - ask WHICH model first.
            await self._prompt_opencode_model(client, chat_id, sid)
            return
        try:
            mid = self._resolve_mid(sid)
            res = self._rpc("mission.set_agent", {"mission_id": mid, "agent":
                                                  None if name == "-" else name})
        except Exception as e:  # noqa: BLE001
            await self._send(client, chat_id, f"❌ {e}")
            return
        row = self.daemon.conn.execute(
            "SELECT name FROM missions WHERE id=?", (mid,)).fetchone()
        mname = row["name"] if row else sid
        if res.get("pinned"):
            await self._send(
                client, chat_id,
                f"🤖 *{mname}* pinned to *{res['pinned']}* - it migrates on its "
                f"next wake (fresh session + handoff notes). All other missions "
                f"stay on the global agent.",
            )
        else:
            await self._send(
                client, chat_id,
                f"🌐 *{mname}* follows the global agent again.",
            )

    def _oc_recents(self) -> list[str]:
        import json as _json
        try:
            return _json.loads(db.get_meta(self.daemon.conn, "opencode_recent_models") or "[]")
        except Exception:  # noqa: BLE001
            return []

    async def _prompt_opencode_model(
        self, client: httpx.AsyncClient, chat_id: str, sid: str,
    ) -> None:
        """Model picker for OpenCode: recently used as buttons + free typing."""
        recents = self._oc_recents()
        rows: list[list[dict]] = []
        for i, m in enumerate(recents[:6]):
            label = m if len(m) <= 48 else m[:45] + "…"
            rows.append([_btn(f"🕘 {label}", f"ocm:{sid}:{i}")])
        rows.append([_btn("✏️ type a model name", f"ocmt:{sid}")])
        rows.append([_btn("▶ use default/global model", f"ocm:{sid}:-")])
        rows.append([_btn("← back", f"magent:{sid}")])
        await self._send(
            client, chat_id,
            "🧩 *OpenCode* - pick the model for this mission.\n"
            "Any OpenRouter id works (e.g. `tencent/hy3:free`, "
            "`deepseek/deepseek-chat`, `anthropic/claude-sonnet-4.5`).",
            markup=_ikb(rows),
        )

    async def _pick_opencode_model(
        self, client: httpx.AsyncClient, chat_id: str, arg: str,
    ) -> None:
        sid, _, idx = arg.partition(":")
        model = ""
        if idx != "-":
            recents = self._oc_recents()
            try:
                model = recents[int(idx)]
            except (ValueError, IndexError):
                await self._send(client, chat_id, "❌ that entry expired - pick again.")
                return
        await self._apply_opencode(client, chat_id, sid, model)

    async def _prompt_opencode_custom(
        self, client: httpx.AsyncClient, chat_id: str, sid: str,
    ) -> None:
        prompt_id = await self._send(
            client, chat_id,
            "✏️ Reply with the model id to use (OpenRouter format), e.g. "
            "`tencent/hy3:free` or `qwen/qwen3-coder:free`.",
            force_reply=True,
        )
        if prompt_id is not None:
            self._pending_input[prompt_id] = ("oc_model", sid)

    async def _apply_opencode(
        self, client: httpx.AsyncClient, chat_id: str, sid: str, model: str,
    ) -> None:
        try:
            mid = self._resolve_mid(sid)
            res = self._rpc("mission.set_agent",
                            {"mission_id": mid, "agent": "opencode",
                             "model": model})
        except Exception as e:  # noqa: BLE001
            await self._send(client, chat_id, f"❌ {e}")
            return
        row = self.daemon.conn.execute(
            "SELECT name FROM missions WHERE id=?", (mid,)).fetchone()
        mname = row["name"] if row else sid
        mtxt = f" running *{res['model']}*" if res.get("model") else " (default model)"
        await self._send(
            client, chat_id,
            f"🧩 *{mname}* pinned to *opencode*{mtxt} - migrates on its next "
            f"wake with handoff notes. Other missions keep the global agent.",
        )

    async def _prompt_call_name(
        self, client: httpx.AsyncClient, chat_id: str, sid: str,
    ) -> None:
        try:
            mid = self._resolve_mid(sid)
        except CommandError as e:
            await self._send(client, chat_id, f"❌ {e}")
            return
        row = self.daemon.conn.execute(
            "SELECT name, call_name FROM missions WHERE id = ?", (mid,)
        ).fetchone()
        cur = (row["call_name"] if row else None) or f"(none - uses mission name '{row['name'] if row else sid}')"
        prompt_id = await self._send(
            client, chat_id,
            f"📞 Current calling name: *{cur}*\n\nReply with the new calling name "
            f"(what shows on your phone when this mission's worker calls you). "
            f"Send `-` to clear it.",
            force_reply=True,
        )
        if prompt_id is not None:
            self._pending_input[prompt_id] = ("call_name", mid)

    async def _prompt_reply(
        self, client: httpx.AsyncClient, chat_id: str, sid: str,
    ) -> None:
        try:
            mid = self._resolve_mid(sid)
        except CommandError as e:
            await self._send(client, chat_id, f"❌ {e}")
            return
        row = self.daemon.conn.execute(
            "SELECT name FROM missions WHERE id = ?", (mid,)
        ).fetchone()
        name = row["name"] if row else sid
        # Pin the conversation directly - no intermediate force_reply prompt.
        # The user can now just type and every message routes to this mission.
        self._pinned[chat_id] = mid
        await self._send(
            client, chat_id,
            f"💬 now talking to *{name}* - just type your message and it goes "
            f"straight to it. Reply to another mission to switch, /unpin to stop.",
        )

    async def _answer_cb(self, client: httpx.AsyncClient, cb_id: str) -> None:
        try:
            await client.post(
                f"https://api.telegram.org/bot{self.token}/answerCallbackQuery",
                json={"callback_query_id": cb_id}, timeout=5,
            )
        except Exception:
            pass

    async def _send_typing(
        self, client: httpx.AsyncClient, chat_id: str, thread_id: int | None = None
    ) -> None:
        try:
            body: dict[str, Any] = {"chat_id": chat_id, "action": "typing"}
            if thread_id is not None:
                body["message_thread_id"] = thread_id
            await client.post(
                f"https://api.telegram.org/bot{self.token}/sendChatAction",
                json=body, timeout=5,
            )
        except Exception:
            pass

    def _start_typing(
        self, client: httpx.AsyncClient, chat_id: str, mission_id: str,
        thread_id: int | None = None,
    ) -> None:
        """Show 'typing…' in the chat until the worker emits its next notify."""
        since = db.now_ts()
        task = asyncio.create_task(
            self._typing_loop(client, chat_id, mission_id, since, thread_id)
        )
        self._typing_tasks.add(task)
        task.add_done_callback(self._typing_tasks.discard)

    async def _typing_loop(
        self, client: httpx.AsyncClient, chat_id: str, mission_id: str, since: int,
        thread_id: int | None = None,
    ) -> None:
        # Telegram's typing action lasts ~5s; refresh it every 4s until the
        # worker sends a notify (its reply) or we hit a safety timeout.
        import time as _t
        deadline = _t.time() + 240
        while _t.time() < deadline:
            await self._send_typing(client, chat_id, thread_id)
            await asyncio.sleep(4)
            row = self.daemon.conn.execute(
                "SELECT 1 FROM events WHERE mission_id = ? AND kind = 'notify_sent'"
                " AND ts >= ? LIMIT 1",
                (mission_id, since),
            ).fetchone()
            if row is not None:
                return

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
        elif action == "context":
            await self._dispatch_and_send(client, chat_id, "context", [])
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
        elif action == "reply":
            await self._prompt_reply(client, chat_id, arg)
        elif action == "callname":
            await self._prompt_call_name(client, chat_id, arg)
        elif action == "compact":
            await self._do_compact(client, chat_id, arg)
        elif action == "magent":
            await self._prompt_mission_agent(client, chat_id, arg)
        elif action == "magentset":
            await self._set_mission_agent(client, chat_id, arg)
        elif action == "ocm":
            await self._pick_opencode_model(client, chat_id, arg)
        elif action == "ocmt":
            await self._prompt_opencode_custom(client, chat_id, arg)
        else:
            await self._send(client, chat_id, f"❌ unknown action `{action}`")

    async def _handle_location(
        self, client: httpx.AsyncClient, chat_id: str, msg: dict, is_edit: bool,
    ) -> None:
        loc = msg.get("location", {})
        lat, lon = loc.get("latitude"), loc.get("longitude")
        if lat is None or lon is None:
            return
        live = "live_period" in loc
        db.upsert_location(
            self.daemon.conn, chat_id=chat_id, latitude=lat, longitude=lon,
            accuracy=loc.get("horizontal_accuracy"), heading=loc.get("heading"),
            live=live,
        )
        # Only react on the INITIAL share (not every live-movement edit, which
        # would spam). Edits silently update the stored fix.
        if is_edit:
            return
        kind = "live location" if live else "location"
        # If a mission is pinned to this chat, let its worker know a fresh share
        # started, so it can act (and call get_user_location for the live value).
        pinned = self._pinned.get(chat_id)
        if pinned:
            m = self.daemon.conn.execute(
                "SELECT state, name FROM missions WHERE id = ?", (pinned,)
            ).fetchone()
            if m and m["state"] in ("running", "cancelling"):
                directive = (
                    f"[The user just shared their {kind}: {lat}, {lon}"
                    f"{' (live, updating)' if live else ''}. "
                    f"https://maps.google.com/?q={lat},{lon}] "
                    f"Use the get_user_location tool any time to read the latest fix. "
                    f"Act on this if relevant, and reply via notify."
                )
                self.daemon.engine._enqueue_oob(
                    pinned, {"kind": "user_location", "directive": directive}
                )
                await self._send(
                    client, chat_id,
                    f"📍 {kind} shared - forwarded to *{m['name']}*.",
                )
                return
        await self._send(
            client, chat_id,
            f"📍 {kind} saved. Workers can read it via get_user_location. "
            f"(Pin a mission first if you want a worker to act on it now.)",
        )

    async def _handle_topic_created(
        self, client: httpx.AsyncClient, msg: dict[str, Any]
    ) -> None:
        """A user created a forum topic in the topics group → create a mission
        bound to it. Topics orch creates itself (for new missions) are skipped."""
        thread_id = msg.get("message_thread_id")
        if thread_id is None:
            return
        # Skip topics the bot itself created (orch auto-creates one per mission).
        frm = msg.get("from") or {}
        bot_id = self.token.split(":")[0]
        if str(frm.get("id")) == bot_id:
            return
        # Already bound to a mission? (race / duplicate update) → nothing to do.
        if db.mission_for_topic(self.daemon.conn, thread_id) is not None:
            return
        name = (msg.get("forum_topic_created") or {}).get("name") or f"topic-{thread_id}"
        try:
            res = self._rpc("mission.create", {"name": name, "tg_topic_id": thread_id})
            mid = res.get("mission_id")
        except Exception as e:  # noqa: BLE001
            await self._send(
                client, self.topics_chat_id,
                f"❌ couldn't create a mission for this topic: {e}",
                thread_id=thread_id,
            )
            return
        db.log_event(self.daemon.conn, mission_id=mid, kind="mission_from_topic")
        await self._send(
            client, self.topics_chat_id,
            f"🤖 Mission *{name}* created and bound to this topic. Just type what "
            f"you'd like me to do - I'll get started and report back right here.",
            thread_id=thread_id,
        )

    async def _handle_media(
        self, client: httpx.AsyncClient, chat_id: str, msg: dict[str, Any]
    ) -> None:
        """User sent an attachment → download it and route the local path to the
        target worker (the mission's topic, or the pinned DM conversation)."""
        msg_id = msg.get("message_id")
        thread_id = msg.get("message_thread_id")
        caption = msg.get("caption") or ""
        # Pick the file_id + a human label for the kind of attachment.
        file_id = None
        kind = "file"
        if msg.get("photo"):
            file_id = msg["photo"][-1]["file_id"]  # largest rendition
            kind = "photo"
        else:
            for k in ("document", "video", "audio", "voice", "animation", "video_note"):
                if k in msg and isinstance(msg[k], dict):
                    file_id = msg[k].get("file_id")
                    kind = k
                    break
        if not file_id:
            return
        # Resolve which mission this file is for.
        if (self.topics_chat_id and chat_id == self.topics_chat_id and thread_id):
            mission_id = db.mission_for_topic(self.daemon.conn, thread_id)
            if mission_id is None:
                return  # unknown topic → ignore
        else:
            mission_id = self._pinned.get(chat_id)
            thread_id = None
        if mission_id is None:
            await self._send(
                client, chat_id,
                "📎 Got a file, but no mission is active here. Open a mission's topic "
                "(or reply to one) first, then resend.",
                msg_id,
            )
            return
        info = telegram.download_file_via(self.token, file_id, f"/root/.orch/incoming/{mission_id}")
        if info is None:
            await self._send(
                client, chat_id,
                "❌ couldn't fetch that file from Telegram (bot download cap is 20 MB).",
                msg_id, thread_id=thread_id,
            )
            return
        note = (f"{caption}\n\n" if caption else "") + (
            f"[The user sent a {kind} attachment. It is saved on this machine at: "
            f"{info['path']} ({info['size']} bytes). Open or Read it as needed to act on it.]"
        )
        db.log_event(
            self.daemon.conn, mission_id=mission_id, kind="user_file_received",
            payload={"name": info["name"], "size": info["size"], "kind": kind},
        )
        await self._route_reply(client, chat_id, mission_id, note, msg_id, thread_id=thread_id)

    async def _route_reply(
        self, client: httpx.AsyncClient, chat_id: str, mission_id: str,
        text: str, msg_id: int | None, thread_id: int | None = None,
    ) -> None:
        """Route a user's Telegram message to a worker. Pins/banners immediately
        for responsiveness, then BUFFERS the text: several messages fired within
        a short window flush as ONE directive (see _buffer_reply), so the worker
        wakes once with all of them instead of once per message.

        When thread_id is set the message came from the mission's own forum
        topic - the topic is the binding, so we route silently (no pin/banner).
        """
        in_topic = thread_id is not None
        m = self.daemon.conn.execute(
            "SELECT * FROM missions WHERE id = ?", (mission_id,)
        ).fetchone()
        if m is None:
            if not in_topic:
                self._pinned.pop(chat_id, None)
            await self._send(client, chat_id, "❌ that mission no longer exists (conversation unpinned)", msg_id, thread_id=thread_id)
            return
        state = m["state"]
        # Terminal & non-reopenable: can't route.
        if state in ("cancelled", "failed"):
            if not in_topic:
                self._pinned.pop(chat_id, None)
            await self._send(
                client, chat_id,
                f"❌ mission *{m['name']}* is {state}; can't route a message to it.",
                msg_id, thread_id=thread_id,
            )
            return
        # Pin + banner immediately (DM only - topics bind by thread).
        continuing = in_topic or self._pinned.get(chat_id) == mission_id
        if not in_topic:
            self._pinned[chat_id] = mission_id
        if not continuing:
            await self._send(
                client, chat_id,
                f"💬 now talking to *{m['name']}* - just type to continue (/unpin to stop).",
                msg_id,
            )
        # Buffer; a single combined directive is delivered after the quiet window.
        self._buffer_reply(client, mission_id, chat_id, thread_id, text)

    def _buffer_reply(
        self, client: httpx.AsyncClient, mission_id: str, chat_id: str,
        thread_id: int | None, text: str,
    ) -> None:
        """Append a message to the mission's reply buffer and (on the first one)
        start the coalescing window + typing indicator."""
        first = mission_id not in self._reply_buf
        self._reply_buf.setdefault(mission_id, []).append(text)
        self._reply_ctx[mission_id] = (chat_id, thread_id)
        if first:
            self._start_typing(client, chat_id, mission_id, thread_id)
            t = asyncio.create_task(self._flush_after(client, mission_id))
            self._reply_timers[mission_id] = t
            t.add_done_callback(lambda _t, mid=mission_id: self._reply_timers.pop(mid, None))

    async def _flush_after(self, client: httpx.AsyncClient, mission_id: str) -> None:
        try:
            await asyncio.sleep(config.REPLY_COALESCE_S)
        except asyncio.CancelledError:
            return
        try:
            await self._flush_reply(client, mission_id)
        except Exception:
            log.exception("flush_reply failed for %s", mission_id)

    async def _flush_reply(self, client: httpx.AsyncClient, mission_id: str) -> None:
        """Deliver all buffered messages for a mission as one directive."""
        texts = self._reply_buf.pop(mission_id, [])
        chat_id, thread_id = self._reply_ctx.pop(mission_id, (None, None))
        if not texts:
            return
        combined = texts[0] if len(texts) == 1 else "\n".join(f"• {t}" for t in texts)
        m = self.daemon.conn.execute(
            "SELECT * FROM missions WHERE id = ?", (mission_id,)
        ).fetchone()
        if m is None:
            return
        state = m["state"]
        directive = (
            f"[The user is in a Telegram conversation with you and said]: {combined}\n\n"
            f"Act on this, then reply to the user via the `notify` tool with your response. "
            f"They may keep the conversation going, so be ready for follow-ups."
        )
        if state == "completed":
            # Reopen, run the reply as a one-shot, finalize silently afterward.
            try:
                runner.tmux_create_session(mission_id)
                agents.get_adapter().prepare(mission_id)
            except runner.RunnerError as e:
                if chat_id:
                    await self._send(client, chat_id, f"❌ reopen failed: {e}", thread_id=thread_id)
                return
            self.daemon.conn.execute(
                "UPDATE missions SET state = 'running', finished_at = NULL,"
                " last_heartbeat_at = ? WHERE id = ?",
                (db.now_ts(), mission_id),
            )
            db.log_event(self.daemon.conn, mission_id=mission_id, kind="mission_reopened")
            self.daemon.engine._suppress_wrapup.add(mission_id)
        elif state not in ("running", "cancelling"):
            return  # became terminal during the window → drop
        self.daemon.engine._enqueue_oob(
            mission_id, {"kind": "user_reply", "directive": directive}
        )
        db.log_event(
            self.daemon.conn, mission_id=mission_id, kind="user_reply_routed",
            payload={"messages": len(texts)},
        )

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
        "`/context` - all missions + their live context-token size\n"
        "`/agent [name]` - show or switch the worker AI backend\n"
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
        "`/unpin` - leave the current conversation\n"
        "`/here` - show which mission you're talking to\n"
        "`/help` - main menu\n\n"
        "💬 *Conversation mode*: reply to any mission's message (or tap 💬 reply) "
        "to pin it - after that, just type normally and every message goes to "
        "that mission, like a chat. Reply to a different mission to switch. "
        "`/unpin` to stop.\n\n"
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


def _cmd_agent(self: TelegramHost, args: list[str]) -> str:
    """Show or switch the worker agent backend: /agent [claude|codex|gemini|api|custom]"""
    if args:
        try:
            res = self._rpc("agent.set", {"agent": args[0], "by": "telegram",
                                          "model": args[1] if len(args) > 1 else ""})
        except Exception as e:  # noqa: BLE001
            return f"❌ {e}"
        mtxt = f" (model *{res['model']}*)" if res.get("model") else ""
        return (f"🤖 agent switched *{res['from']}* → *{res['to']}*{mtxt}.\n"
                f"Running missions migrate on their next wake (fresh session + "
                f"handoff notes). If *{res['to']}* turns out broken, orch falls "
                f"back to the last agent that worked.")
    st = self._rpc("agent.get", {})
    lines = [f"🤖 active agent: *{st['active']}*",
             f"last good: {st['last_good'] or '(none yet)'}"]
    lines.append("\nbackends on this machine:")
    for name, b in st["backends"].items():
        mark = "✅" if b["available"] else "🚫"
        extra = "" if b["available"] else f" - {b['reason']}"
        lines.append(f"  {mark} `{name}`{extra}")
    lines.append("\nswitch with `/agent <name>`")
    return "\n".join(lines)


def _cmd_context(self: TelegramHost, args: list[str]) -> Reply:
    """List every mission with its current context-token size (what each wake
    re-ingests), biggest first so bloat is obvious. Tap a mission to compact."""
    rows = self.daemon.conn.execute(
        "SELECT id, name, state FROM missions"
        " ORDER BY (state = 'running') DESC, created_at DESC"
    ).fetchall()
    if not rows:
        return Reply(text="_(no missions)_", markup=_ikb([[_btn("❓ help", "help")]]))
    entries = []  # (tokens, turns, available, row)
    for r in rows:
        try:
            ci = self._rpc("mission.context_info", {"mission_id": r["id"]})
        except Exception:
            ci = {"available": False}
        if ci.get("available"):
            entries.append((int(ci["context_tokens"]), int(ci["turns"]), True, r))
        else:
            entries.append((-1, 0, False, r))
    # Biggest context first; missions with no transcript sink to the bottom.
    entries.sort(key=lambda e: e[0], reverse=True)
    lines = ["*context per mission* - tokens re-read on every wake"]
    total = 0
    for tokens, turns, available, r in entries[:40]:
        marker = {
            "running": "▶", "cancelling": "…",
            "completed": "✓", "cancelled": "✗", "failed": "!",
        }.get(r["state"], "?")
        if not available:
            lines.append(f"`{_short(r['id'])}` {marker} {r['name']} - _no transcript_")
            continue
        total += tokens
        warn = " ⚠️" if tokens >= 200000 else ""
        lines.append(
            f"`{_short(r['id'])}` {marker} ~{tokens:,} ({turns} turns) {r['name']}{warn}"
        )
    lines.append("")
    lines.append(f"*total live context:* ~{total:,} tokens")
    lines.append("_tap a mission below, then 🗜 to compact a big one_")
    return Reply(text="\n".join(lines), markup=_missions_list_kb(rows[:30]))


def _cmd_get(self: TelegramHost, args: list[str]) -> Reply:
    if not args:
        raise CommandError("usage: /m <id>")
    mid = self._resolve_mid(args[0])
    snap = self._rpc("mission.get", {"mission_id": mid})
    m = snap["mission"]
    cname = m.get("call_name")
    pinned = m.get("pinned_agent")
    agent_line = (f"agent: {m.get('agent') or 'claude'}"
                  + (f"  📌 pinned: {pinned}" if pinned else "  (global)"))
    lines = [
        f"*{m['name']}*  `{_short(mid)}`",
        f"state: {m['state']}  restarts: {m['restart_count']}",
        f"heartbeat: every {m['heartbeat_interval_s']}s  |  {agent_line}",
        f"chat: {m['telegram_chat_id']}" + (f"  caller: {cname}" if cname else ""),
    ]
    try:
        ci = self._rpc("mission.context_info", {"mission_id": mid})
        if ci.get("available"):
            warn = " ⚠️" if ci["context_tokens"] >= 200000 else ""
            lines.append(f"🧠 context: ~{ci['context_tokens']:,} tokens "
                         f"({ci['turns']} turns){warn}  - re-read each wake")
    except Exception:
        pass
    lines += ["", f"*steps* ({len(snap['steps'])}):"]
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
    "context": _cmd_context,
    "tokens": _cmd_context,
    "ctx": _cmd_context,
    "agent": _cmd_agent,
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
