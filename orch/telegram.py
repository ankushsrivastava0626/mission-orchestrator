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


def send_via(
    token: str, chat_id: str, text: str, message_thread_id: int | None = None
) -> int | None:
    """Send via a specific bot token. Returns the telegram message_id or None.

    If message_thread_id is given, the message is posted into that forum topic.
    """
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    body: dict = {"chat_id": chat_id, "text": text}
    if message_thread_id is not None:
        body["message_thread_id"] = int(message_thread_id)
    try:
        r = httpx.post(url, json=body, timeout=10.0)
        if r.status_code >= 400:
            log.warning("telegram send %s -> %s: %s", chat_id, r.status_code, r.text)
            return None
        data = r.json()
        if data.get("ok"):
            return data["result"]["message_id"]
    except httpx.HTTPError as e:
        log.warning("telegram send %s failed: %s", chat_id, e)
    return None


def create_forum_topic(token: str, chat_id: str, name: str) -> int | None:
    """Create a forum topic in a supergroup. Returns its message_thread_id."""
    url = f"https://api.telegram.org/bot{token}/createForumTopic"
    try:
        r = httpx.post(url, json={"chat_id": chat_id, "name": name[:128]}, timeout=10.0)
        data = r.json()
        if data.get("ok"):
            return data["result"]["message_thread_id"]
        log.warning("createForumTopic %s -> %s: %s", chat_id, r.status_code, r.text)
    except httpx.HTTPError as e:
        log.warning("createForumTopic %s failed: %s", chat_id, e)
    return None


def send(chat_id: str, text: str) -> None:
    """Send via the notification bot (bot #1). Legacy / fallback path."""
    token = _token()
    if not token:
        log.warning("telegram: %s not set; skipping send to %s", config.ENV_TELEGRAM_TOKEN, chat_id)
        return
    send_via(token, chat_id, text)


_IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".webp"}


def send_file_via(
    token: str, chat_id: str, file_path: str, caption: str | None = None,
    message_thread_id: int | None = None,
) -> int | None:
    """Upload a local file to Telegram. Images go via sendPhoto (inline preview);
    everything else via sendDocument (any type, as-is). Returns message_id.

    Bot API upload cap is 50 MB. Caller should validate size first."""
    import os
    ext = os.path.splitext(file_path)[1].lower()
    is_img = ext in _IMAGE_EXTS
    method = "sendPhoto" if is_img else "sendDocument"
    field = "photo" if is_img else "document"
    data: dict = {"chat_id": str(chat_id)}
    if message_thread_id is not None:
        data["message_thread_id"] = str(int(message_thread_id))
    if caption:
        data["caption"] = caption[:1024]
    try:
        with open(file_path, "rb") as fh:
            files = {field: (os.path.basename(file_path) or "file", fh)}
            r = httpx.post(
                f"https://api.telegram.org/bot{token}/{method}",
                data=data, files=files, timeout=120.0,
            )
        if r.status_code >= 400:
            # sendPhoto can reject odd images; retry as a plain document.
            if is_img:
                return send_document_fallback(token, chat_id, file_path, caption, message_thread_id)
            log.warning("telegram %s %s -> %s: %s", method, chat_id, r.status_code, r.text)
            return None
        d = r.json()
        if d.get("ok"):
            return d["result"]["message_id"]
    except (httpx.HTTPError, OSError) as e:
        log.warning("telegram send_file %s failed: %s", chat_id, e)
    return None


def send_document_fallback(token, chat_id, file_path, caption, message_thread_id):
    import os
    data: dict = {"chat_id": str(chat_id)}
    if message_thread_id is not None:
        data["message_thread_id"] = str(int(message_thread_id))
    if caption:
        data["caption"] = caption[:1024]
    try:
        with open(file_path, "rb") as fh:
            r = httpx.post(
                f"https://api.telegram.org/bot{token}/sendDocument",
                data=data, files={"document": (os.path.basename(file_path) or "file", fh)},
                timeout=120.0,
            )
        d = r.json()
        if d.get("ok"):
            return d["result"]["message_id"]
        log.warning("telegram sendDocument fallback %s: %s", chat_id, r.text)
    except (httpx.HTTPError, OSError) as e:
        log.warning("telegram sendDocument fallback %s failed: %s", chat_id, e)
    return None


def download_file_via(token: str, file_id: str, dest_dir: str) -> dict | None:
    """Download a Telegram file (by file_id) into dest_dir. Returns
    {path, name, size} or None. Bot API download cap is 20 MB."""
    import os
    try:
        r = httpx.get(
            f"https://api.telegram.org/bot{token}/getFile",
            params={"file_id": file_id}, timeout=30.0,
        )
        d = r.json()
        if not d.get("ok"):
            log.warning("getFile %s: %s", file_id, r.text)
            return None
        file_path = d["result"]["file_path"]
        name = os.path.basename(file_path) or "file"
        os.makedirs(dest_dir, exist_ok=True)
        dest = os.path.join(dest_dir, name)
        with httpx.stream(
            "GET", f"https://api.telegram.org/file/bot{token}/{file_path}", timeout=120.0
        ) as resp:
            if resp.status_code >= 400:
                log.warning("download %s -> %s", file_path, resp.status_code)
                return None
            with open(dest, "wb") as out:
                for chunk in resp.iter_bytes():
                    out.write(chunk)
        return {"path": dest, "name": name, "size": os.path.getsize(dest)}
    except (httpx.HTTPError, OSError) as e:
        log.warning("download_file %s failed: %s", file_id, e)
    return None
