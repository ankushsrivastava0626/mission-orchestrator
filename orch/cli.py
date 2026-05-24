"""CLI entry points: orchd (daemon), orchctl (admin), msec (worker secret reader)."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys

import click

from . import client, config, daemon


# ---------- orchd ----------


@click.group()
def daemon_cli() -> None:
    """Orchestrator daemon."""


@daemon_cli.command("start")
def daemon_start() -> None:
    """Start the daemon in the foreground."""
    if not os.environ.get(config.ENV_MASTER_PASSPHRASE):
        click.echo(
            f"warning: {config.ENV_MASTER_PASSPHRASE} not set; vault operations will fail",
            err=True,
        )
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        asyncio.run(daemon.run())
    except KeyboardInterrupt:
        pass


@daemon_cli.command("status")
def daemon_status() -> None:
    """Check daemon liveness via socket."""
    try:
        with client.DaemonClient() as c:
            missions = c.call("mission.list", {})
        click.echo(f"ok; {len(missions)} mission(s)")
    except (ConnectionRefusedError, FileNotFoundError):
        click.echo("daemon not running", err=True)
        sys.exit(1)
    except client.DaemonError as e:
        click.echo(f"error: {e}", err=True)
        sys.exit(1)


def daemon_main() -> None:
    daemon_cli()


# ---------- orchctl ----------


@click.group()
def ctl_cli() -> None:
    """Orchestrator control / admin CLI."""


@ctl_cli.command("missions")
def ctl_missions() -> None:
    """List missions."""
    with client.DaemonClient() as c:
        out = c.call("mission.list", {})
    click.echo(json.dumps(out, indent=2, default=str))


@ctl_cli.command("mission-get")
@click.argument("mission_id")
def ctl_mission_get(mission_id: str) -> None:
    with client.DaemonClient() as c:
        out = c.call("mission.get", {"mission_id": mission_id})
    click.echo(json.dumps(out, indent=2, default=str))


@ctl_cli.command("create")
@click.option("--name", required=True)
@click.option("--chat-id", default=None, help="Telegram chat id. Falls back to daemon's ORCH_DEFAULT_CHAT_ID.")
@click.option("--heartbeat", type=int, default=None)
def ctl_create(name: str, chat_id: str | None, heartbeat: int | None) -> None:
    params: dict = {"name": name}
    if chat_id is not None:
        params["telegram_chat_id"] = chat_id
    if heartbeat is not None:
        params["heartbeat_interval_s"] = heartbeat
    with client.DaemonClient() as c:
        out = c.call("mission.create", params)
    click.echo(json.dumps(out, indent=2))


@ctl_cli.command("step-add")
@click.option("--mission-id", required=True)
@click.option("--directive", required=True)
@click.option(
    "--cue",
    required=True,
    help="JSON cue, e.g. '{\"type\":\"on_prev_complete\"}'",
)
def ctl_step_add(mission_id: str, directive: str, cue: str) -> None:
    with client.DaemonClient() as c:
        out = c.call(
            "step.add",
            {
                "mission_id": mission_id,
                "directive": directive,
                "cue": json.loads(cue),
            },
        )
    click.echo(json.dumps(out, indent=2))


@ctl_cli.command("cancel")
@click.argument("mission_id")
def ctl_cancel(mission_id: str) -> None:
    with client.DaemonClient() as c:
        out = c.call("mission.cancel", {"mission_id": mission_id})
    click.echo(json.dumps(out, indent=2))


def ctl_main() -> None:
    ctl_cli()


# ---------- msec ----------


def _msec_mission_id() -> str:
    mid = os.environ.get(config.ENV_MISSION_ID)
    if not mid:
        click.echo(f"{config.ENV_MISSION_ID} not set", err=True)
        sys.exit(2)
    return mid


@click.group()
def msec_cli() -> None:
    """Mission secret/cookie accessor (worker-side)."""


@msec_cli.command("get")
@click.argument("name")
def msec_get(name: str) -> None:
    mid = _msec_mission_id()
    try:
        with client.DaemonClient() as c:
            res = c.call(
                "secret.get",
                {"mission_id": mid, "name": name, "caller": "msec"},
            )
    except client.DaemonError as e:
        click.echo(f"error: {e}", err=True)
        sys.exit(1)
    click.echo(res["value"])


@msec_cli.command("cookies")
@click.argument("name")
def msec_cookies(name: str) -> None:
    mid = _msec_mission_id()
    try:
        with client.DaemonClient() as c:
            res = c.call(
                "cookies.materialize",
                {"mission_id": mid, "name": name, "caller": "msec"},
            )
    except client.DaemonError as e:
        click.echo(f"error: {e}", err=True)
        sys.exit(1)
    click.echo(res["path"])


@msec_cli.command("list")
def msec_list() -> None:
    mid = _msec_mission_id()
    with client.DaemonClient() as c:
        secrets = c.call("secret.list", {"mission_id": mid})
        cookies = c.call("cookies.list", {"mission_id": mid})
    click.echo(json.dumps({"secrets": secrets, "cookies": cookies}, indent=2))


def msec_main() -> None:
    msec_cli()


# ---------- owatch (scripted-ping callbacks, used by watcher scripts) ----------


@click.group()
def owatch_cli() -> None:
    """Scripted-ping callback tool, called by autonomous watcher scripts."""


@owatch_cli.command("alive")
@click.argument("scripted_ping_id")
def owatch_alive(scripted_ping_id: str) -> None:
    """Heartbeat - prove the watcher script is still running."""
    try:
        with client.DaemonClient() as c:
            c.call("scripted_ping.alive", {"scripted_ping_id": scripted_ping_id, "caller": "owatch"})
    except client.DaemonError as e:
        click.echo(f"error: {e}", err=True)
        sys.exit(1)
    click.echo("ok")


@owatch_cli.command("fire")
@click.argument("scripted_ping_id")
@click.argument("context", required=False, default="")
def owatch_fire(scripted_ping_id: str, context: str) -> None:
    """Condition met - wake the worker to send the notify."""
    try:
        with client.DaemonClient() as c:
            c.call("scripted_ping.fire", {
                "scripted_ping_id": scripted_ping_id, "context": context, "caller": "owatch",
            })
    except client.DaemonError as e:
        click.echo(f"error: {e}", err=True)
        sys.exit(1)
    click.echo("fired")


@owatch_cli.command("ready")
@click.argument("scripted_ping_id")
@click.argument("script_path", required=False, default=None)
def owatch_ready(scripted_ping_id: str, script_path: str | None) -> None:
    """Register the script as tested and running (activates the watchdog)."""
    try:
        with client.DaemonClient() as c:
            c.call("scripted_ping.ready", {
                "scripted_ping_id": scripted_ping_id, "script_path": script_path,
                "caller": "owatch",
            })
    except client.DaemonError as e:
        click.echo(f"error: {e}", err=True)
        sys.exit(1)
    click.echo("ready")


def owatch_main() -> None:
    owatch_cli()


if __name__ == "__main__":  # pragma: no cover
    daemon_main()
