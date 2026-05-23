"""Outbound Telegram messaging via Bot API. Synchronous; uses httpx."""

from __future__ import annotations

import logging
import os

import httpx

from . import config

log = logging.getLogger(__name__)


class TelegramError(RuntimeError):
    pass


def _token() -> str | None:
    return os.environ.get(config.ENV_TELEGRAM_TOKEN)


def host_token() -> str | None:
    """The inbound command bot (bot #2). If set, notify routes through it so
    the user can reply to worker messages and have them routed back."""
    return os.environ.get(config.ENV_HOST_BOT_TOKEN) or None


def send_via(token: str, chat_id: str, text: str) -> int | None:
    """Send via a specific bot token. Returns the telegram message_id or None."""
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        r = httpx.post(url, json={"chat_id": chat_id, "text": text}, timeout=10.0)
        if r.status_code >= 400:
            log.warning("telegram send %s -> %s: %s", chat_id, r.status_code, r.text)
            return None
        data = r.json()
        if data.get("ok"):
            return data["result"]["message_id"]
    except httpx.HTTPError as e:
        log.warning("telegram send %s failed: %s", chat_id, e)
    return None


def send(chat_id: str, text: str) -> None:
    """Send via the notification bot (bot #1). Legacy / fallback path."""
    token = _token()
    if not token:
        log.warning("telegram: %s not set; skipping send to %s", config.ENV_TELEGRAM_TOKEN, chat_id)
        return
    send_via(token, chat_id, text)
