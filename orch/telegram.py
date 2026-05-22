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


def send(chat_id: str, text: str) -> None:
    token = _token()
    if not token:
        log.warning("telegram: %s not set; skipping send to %s", config.ENV_TELEGRAM_TOKEN, chat_id)
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        r = httpx.post(url, json={"chat_id": chat_id, "text": text}, timeout=10.0)
        if r.status_code >= 400:
            log.warning("telegram send %s -> %s: %s", chat_id, r.status_code, r.text)
    except httpx.HTTPError as e:
        log.warning("telegram send %s failed: %s", chat_id, e)
