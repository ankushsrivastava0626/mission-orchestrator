"""Interactive first-run setup - writes ~/.orch/orchd.env.

Run explicitly with `orchd setup`, or it is offered automatically the first
time `orchd start` runs on a machine with no config file.
"""

from __future__ import annotations

import json
import os
import secrets
import shutil
import subprocess
import sys
import time
from pathlib import Path

import httpx

from . import config

ENV_PATH = config.ORCH_DIR / "orchd.env"

AGENTS = {
    "1": ("claude", "Claude Code CLI (needs `claude` installed & logged in)"),
    "2": ("codex", "OpenAI Codex CLI (needs `codex` installed & logged in)"),
    "3": ("gemini", "Gemini CLI (needs `gemini` installed & logged in)"),
    "4": ("api", "No CLI - raw API key (Anthropic or any OpenAI-compatible)"),
    "5": ("custom", "Custom - any agent CLI via command templates"),
}


def _ask(prompt: str, default: str = "") -> str:
    tag = f" [{default}]" if default else ""
    try:
        v = input(f"  {prompt}{tag}: ").strip()
    except EOFError:
        v = ""
    return v or default


def _yes(prompt: str, default: bool = True) -> bool:
    d = "Y/n" if default else "y/N"
    v = _ask(f"{prompt} ({d})", "")
    if not v:
        return default
    return v.lower().startswith("y")


def _detect_chat_id(token: str, *, want_group: bool) -> str:
    """Ask the user to message the bot, then read the chat id off getUpdates."""
    kind = "the GROUP you added the bot to" if want_group else "your bot (a DM)"
    print(f"    → In Telegram, send any message to {kind} now. Waiting 60s…")
    deadline = time.time() + 60
    offset = 0
    while time.time() < deadline:
        try:
            r = httpx.get(
                f"https://api.telegram.org/bot{token}/getUpdates",
                params={"timeout": 10, "offset": offset}, timeout=15,
            ).json()
        except httpx.HTTPError:
            time.sleep(2)
            continue
        for u in r.get("result", []):
            offset = max(offset, u["update_id"] + 1)
            msg = u.get("message") or u.get("edited_message") or {}
            chat = msg.get("chat") or {}
            cid = chat.get("id")
            if cid is None:
                continue
            is_group = chat.get("type") in ("group", "supergroup")
            if is_group == want_group:
                who = chat.get("title") or chat.get("username") or cid
                print(f"    ✓ detected chat: {who} ({cid})")
                return str(cid)
    print("    (nothing received - you can fill it in later in orchd.env)")
    return ""


def run_wizard() -> None:
    print("\n╭──────────────────────────────────────────────╮")
    print("│  orch setup - mission orchestrator            │")
    print("╰──────────────────────────────────────────────╯")
    if ENV_PATH.exists():
        if not _yes(f"{ENV_PATH} already exists - overwrite?", default=False):
            print("keeping existing config; nothing changed.")
            return

    env: dict[str, str] = {}

    # 1. Agent backend ------------------------------------------------------
    print("\n[1/4] Which agent runs your workers?")
    for k, (name, desc) in AGENTS.items():
        print(f"    {k}) {name:7} - {desc}")
    choice = _ask("choose", "1")
    agent = AGENTS.get(choice, AGENTS["1"])[0]
    env["ORCH_AGENT"] = agent

    if agent == "api":
        prov = _ask("provider (anthropic/openai-compatible)", "anthropic")
        env["ORCH_API_PROVIDER"] = "anthropic" if prov.startswith("a") else "openai"
        env["ORCH_API_KEY"] = _ask("API key")
        default_model = "claude-sonnet-5" if env["ORCH_API_PROVIDER"] == "anthropic" else ""
        env["ORCH_API_MODEL"] = _ask("model id", default_model)
        base = _ask("base URL (blank = provider default)", "")
        if base:
            env["ORCH_API_BASE_URL"] = base
    elif agent == "custom":
        print("    Templates: {mission_id} and {directive} are substituted.")
        env["ORCH_CUSTOM_FIRST_CMD"] = _ask("first-launch command template")
        env["ORCH_CUSTOM_RESUME_CMD"] = _ask("resume command template",
                                             env["ORCH_CUSTOM_FIRST_CMD"])
    else:
        binmap = {"claude": "claude", "codex": "codex", "gemini": "gemini"}
        if shutil.which(binmap[agent]) is None:
            print(f"    ⚠ `{binmap[agent]}` not found on PATH - install it before starting missions.")

    # 2. Telegram -----------------------------------------------------------
    print("\n[2/4] Telegram control (talk to your agents from your phone).")
    print("    Create a bot with @BotFather (/newbot) and paste its token.")
    tok = _ask("command bot token (blank = skip Telegram)", "")
    if tok:
        env["ORCH_HOST_BOT_TOKEN"] = tok
        cid = _detect_chat_id(tok, want_group=False)
        if cid:
            env["ORCH_HOST_ALLOWED_CHAT_IDS"] = cid
            env["ORCH_DEFAULT_CHAT_ID"] = cid
        else:
            env["ORCH_HOST_ALLOWED_CHAT_IDS"] = _ask("your telegram chat id (numeric)")
            env["ORCH_DEFAULT_CHAT_ID"] = env["ORCH_HOST_ALLOWED_CHAT_IDS"]
        print("\n    Optional: a forum group gives each mission its own Topic (thread).")
        print("    Create a group → enable Topics → add the bot as ADMIN with Manage Topics.")
        if _yes("set up a topics group now?", default=False):
            gid = _detect_chat_id(tok, want_group=True)
            if gid:
                env["ORCH_TOPICS_CHAT_ID"] = gid

    # 3. Secrets vault ------------------------------------------------------
    print("\n[3/4] Secrets vault (pass/GPG). Workers fetch mission secrets with `msec`.")
    env["ORCH_MASTER_PASSPHRASE"] = _ask(
        "master passphrase (blank = generate one)", "") or secrets.token_urlsafe(24)

    # 4. Write config + optional service ------------------------------------
    config.ORCH_DIR.mkdir(parents=True, exist_ok=True)
    lines = [f"{k}={v}" for k, v in env.items() if v]
    ENV_PATH.write_text("\n".join(lines) + "\n")
    os.chmod(ENV_PATH, 0o600)
    print(f"\n[4/4] wrote {ENV_PATH}")

    if shutil.which("systemctl") and _yes("install a systemd service so orchd survives reboots?"):
        _install_systemd()

    print("\ndone. next steps:")
    print("  orchd start            # run the daemon (or use the systemd service)")
    print("  orchctl create --name my-first-mission")
    print("  …or just message your Telegram bot: /help\n")


def _install_systemd() -> None:
    orchd_bin = shutil.which("orchd") or sys.argv[0]
    is_root = os.geteuid() == 0
    unit = f"""[Unit]
Description=Mission Orchestrator daemon
After=network-online.target

[Service]
Type=simple
Environment=ORCH_ENV_FILE={ENV_PATH}
ExecStart={orchd_bin} start
Restart=on-failure
RestartSec=5s

[Install]
WantedBy={'multi-user.target' if is_root else 'default.target'}
"""
    if is_root:
        path = Path("/etc/systemd/system/orchd.service")
        scope: list[str] = []
    else:
        path = Path.home() / ".config/systemd/user/orchd.service"
        path.parent.mkdir(parents=True, exist_ok=True)
        scope = ["--user"]
    path.write_text(unit)
    subprocess.run(["systemctl", *scope, "daemon-reload"], check=False)
    subprocess.run(["systemctl", *scope, "enable", "--now", "orchd"], check=False)
    print(f"    ✓ installed + started {path}")
    if not is_root:
        print("    (user service - run `loginctl enable-linger $USER` so it survives logout)")
