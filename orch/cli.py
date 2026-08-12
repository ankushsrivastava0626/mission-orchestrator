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


@daemon_cli.command("setup")
def daemon_setup() -> None:
    """Interactive first-run setup (agent backend, Telegram, vault, service)."""
    from . import setup_wizard
    setup_wizard.run_wizard()


@daemon_cli.command("start")
def daemon_start() -> None:
    """Start the daemon in the foreground."""
    from . import setup_wizard
    # First start on a fresh machine: offer the wizard (interactive TTY only).
    if (not setup_wizard.ENV_PATH.exists()
            and not os.path.exists("/etc/orchd.env")
            and sys.stdin.isatty()):
        click.echo("no config found - running first-time setup (orchd setup).")
        setup_wizard.run_wizard()
        config._load_env_file()
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


@ctl_cli.group("agent")
def ctl_agent() -> None:
    """Show or switch the worker agent backend (claude/codex/gemini/api/custom)."""


@ctl_agent.command("show")
def ctl_agent_show() -> None:
    """Current backend, last-known-good, and what's usable on this machine."""
    with client.DaemonClient() as c:
        st = c.call("agent.get", {})
    click.echo(f"active     : {st['active']}")
    click.echo(f"last good  : {st['last_good'] or '(none recorded yet)'}")
    click.echo(f"fail streak: {st['consecutive_failures']}")
    click.echo("backends:")
    for name, b in st["backends"].items():
        mark = "✓" if b["available"] else "✗"
        click.echo(f"  {mark} {name:7} {'' if b['available'] else '- ' + b['reason']}")


@ctl_agent.command("set")
@click.argument("name")
@click.option("--force", is_flag=True, help="switch even if the backend fails its availability check")
def ctl_agent_set(name: str, force: bool) -> None:
    """Switch the backend live, e.g. `orchctl agent set gemini`.

    Persists to the config file; each running mission migrates on its next
    wake - new session on the new agent, seeded with a handoff summary of its
    old one. If the new agent turns out dead, orch auto-falls back to the
    last backend that worked."""
    try:
        with client.DaemonClient() as c:
            res = c.call("agent.set", {"agent": name, "force": force, "by": "cli"})
    except client.DaemonError as e:
        click.echo(f"error: {e}", err=True)
        sys.exit(1)
    click.echo(f"switched {res['from']} -> {res['to']} (saved to {res['persisted_to']})")
    click.echo(res["note"])


@ctl_cli.group("config")
def ctl_config() -> None:
    """View or change orch settings - live, no restart needed."""


@ctl_config.command("list")
@click.option("--all", "show_all", is_flag=True, help="include unmodified settings")
def ctl_config_list(show_all: bool) -> None:
    """All settings with current values (secrets masked). Modified ones marked *."""
    with client.DaemonClient() as c:
        st = c.call("config.list", {})
    width = max(len(k) for k in st)
    for key, s in st.items():
        mark = "*" if s["modified"] else " "
        click.echo(f"{mark} {key:<{width}}  = {s['value']:<24}  {s['description']}")
    click.echo("\n(* = changed from default; set with `orchctl config set <key> <value>`)")


@ctl_config.command("get")
@click.argument("key")
def ctl_config_get(key: str) -> None:
    with client.DaemonClient() as c:
        st = c.call("config.list", {})
    s = st.get(key.lower())
    if not s:
        click.echo(f"unknown key {key!r}", err=True)
        sys.exit(1)
    click.echo(f"{key} = {s['value']}   (default {s['default']}, env {s['env']})")
    click.echo(s["description"])


@ctl_config.command("set")
@click.argument("key")
@click.argument("value")
def ctl_config_set(key: str, value: str) -> None:
    """Set a value - validated, persisted, applied to the running daemon."""
    try:
        with client.DaemonClient() as c:
            res = c.call("config.set", {"key": key, "value": value})
    except client.DaemonError as e:
        click.echo(f"error: {e}", err=True)
        sys.exit(1)
    click.echo(f"{res['key']} = {res['value']}  (saved to {res['persisted_to']}, live now)")


@ctl_config.command("reset")
@click.argument("key")
def ctl_config_reset(key: str) -> None:
    """Reset a key back to its default."""
    try:
        with client.DaemonClient() as c:
            res = c.call("config.set", {"key": key, "value": ""})
    except client.DaemonError as e:
        click.echo(f"error: {e}", err=True)
        sys.exit(1)
    click.echo(f"{res['key']} reset to default: {res['value']}")


@ctl_agent.command("pin")
@click.argument("mission_id")
@click.argument("name")
def ctl_agent_pin(mission_id: str, name: str) -> None:
    """Pin ONE mission to a backend (others keep the global agent).
    Use name '-' to clear the pin."""
    try:
        with client.DaemonClient() as c:
            res = c.call("mission.set_agent",
                         {"mission_id": mission_id,
                          "agent": None if name == "-" else name})
    except client.DaemonError as e:
        click.echo(f"error: {e}", err=True)
        sys.exit(1)
    click.echo(f"pinned: {res['pinned'] or '(cleared - follows global)'}")
    click.echo(res["note"])


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


# ---------- oworker (worker toolkit over the daemon socket) ----------
#
# Shell bridge for agent backends that can't load MCP servers (Antigravity's
# agy, custom CLIs): every worker tool as a command. Mission identity comes
# from ORCH_MISSION_ID in the worker's environment.


@click.group()
def oworker_cli() -> None:
    """Worker toolkit - notify the user, send files, queue future steps."""


def _oworker_call(method: str, params: dict) -> object:
    mid = _msec_mission_id()
    with client.DaemonClient() as c:
        return c.call(method, {"mission_id": mid, **params})


@oworker_cli.command("notify")
@click.argument("text")
def oworker_notify(text: str) -> None:
    """Send a Telegram message to the user."""
    _oworker_call("notify", {"text": text})
    click.echo("sent")


@oworker_cli.command("send-file")
@click.argument("path")
@click.option("--caption", default="")
def oworker_send_file(path: str, caption: str) -> None:
    """Send a file (any type; images show inline) to the user."""
    _oworker_call("notify_file", {"path": path, "caption": caption})
    click.echo("sent")


@oworker_cli.command("message-host")
@click.argument("text")
@click.option("--file", "files", multiple=True, help="attach a file (repeatable)")
def oworker_message_host(text: str, files: tuple[str, ...]) -> None:
    """Message the orchestrating host's mailbox (not the human)."""
    res = _oworker_call("host.message", {"text": text, "files": list(files)})
    click.echo(json.dumps(res))


@oworker_cli.command("queue-add")
@click.argument("directive")
@click.option("--at", default=None, help='absolute time "YYYY-MM-DD HH:MM" (local)')
@click.option("--in", "in_s", type=int, default=None, help="seconds after the current step started")
@click.option("--next", "run_next", is_flag=True, help="run right after the current step")
def oworker_queue_add(directive: str, at: str | None, in_s: int | None, run_next: bool) -> None:
    """Queue a future step for yourself (this is how you self-schedule)."""
    if at:
        cue: dict = {"type": "at_time", "at": at}
    elif in_s:
        cue = {"type": "on_timeout", "seconds": in_s}
    elif run_next:
        cue = {"type": "on_current_complete"}
    else:
        cue = {"type": "on_current_complete"}
    res = _oworker_call("step.add", {"directive": directive, "cue": cue,
                                     "created_by": "worker"})
    click.echo(json.dumps(res))


@oworker_cli.command("queue-list")
def oworker_queue_list() -> None:
    """List this mission's steps."""
    click.echo(json.dumps(_oworker_call("step.list", {}), default=str))


@oworker_cli.command("status")
def oworker_status() -> None:
    """This mission's full state."""
    click.echo(json.dumps(_oworker_call("mission.get", {}), default=str))


@oworker_cli.command("heartbeat-set")
@click.argument("interval_s", type=int)
def oworker_heartbeat_set(interval_s: int) -> None:
    """Set the heartbeat interval in seconds."""
    click.echo(json.dumps(_oworker_call("heartbeat.set", {"interval_s": interval_s})))


@oworker_cli.command("location")
def oworker_location() -> None:
    """The user's latest shared location, if any."""
    click.echo(json.dumps(_oworker_call("location.get", {})))


def oworker_main() -> None:
    oworker_cli()


if __name__ == "__main__":  # pragma: no cover
    daemon_main()


# ---------- orch-fleet (Docker multi-instance manager) ----------


@click.group()
def fleet_cli() -> None:
    """Run many isolated orch instances as Docker containers, one bot each."""


@fleet_cli.command("build")
@click.option("--image", default=None, help="image tag (default orch:latest)")
def fleet_build(image: str | None) -> None:
    """Build the orch container image (stages agent CLIs from this host)."""
    import os as _os
    import subprocess as _sp
    from . import fleet as _f
    here = _os.path.join(_os.path.dirname(_os.path.dirname(__file__)), "docker", "build.sh")
    env = {**_os.environ}
    if image:
        env["ORCH_IMAGE"] = image
    sys.exit(_sp.run(["bash", here], env=env).returncode)


@fleet_cli.command("create")
@click.argument("name")
@click.option("--bot-token", required=True, help="Telegram command bot token for this instance")
@click.option("--chat-id", default=None, help="your Telegram chat id (allowlist + default)")
@click.option("--agent", default=None, help="override the default agent backend for this instance")
@click.option("--model", default=None, help="opencode model, e.g. openrouter/deepseek/deepseek-chat")
@click.option("--topics-chat", default=None, help="forum supergroup id for per-mission topics")
@click.option("--image", default="orch:latest")
@click.option("--no-seed", is_flag=True, help="do not copy host agent logins into the instance")
def fleet_create(name, bot_token, chat_id, agent, model, topics_chat, image, no_seed) -> None:
    """Create and start a new isolated instance."""
    from . import fleet as _f
    try:
        _f.create(name, bot_token=bot_token, chat_id=chat_id, agent=agent,
                  model=model, topics_chat=topics_chat, image=image, no_seed=no_seed)
    except _f.FleetError as e:
        click.echo(f"error: {e}", err=True); sys.exit(1)


@fleet_cli.command("list")
def fleet_list() -> None:
    """List all instances and their state."""
    from . import fleet as _f
    try:
        rows = _f.ls()
    except _f.FleetError as e:
        click.echo(f"error: {e}", err=True); sys.exit(1)
    if not rows:
        click.echo("no instances yet. Create one: orch-fleet create <name> --bot-token <tok>")
        return
    for r in rows:
        mark = {"running": "▶", "exited": "■", "restarting": "…"}.get(r["state"], "?")
        line = f"{mark} {r['name']:<20} {r['state']:<11} {r['status']}"
        if r["missions"]:
            line += f"   [{r['missions']}]"
        click.echo(line)


@fleet_cli.command("start")
@click.argument("name")
def fleet_start(name) -> None:
    from . import fleet as _f
    try: _f.start(name)
    except _f.FleetError as e: click.echo(f"error: {e}", err=True); sys.exit(1)


@fleet_cli.command("stop")
@click.argument("name")
def fleet_stop(name) -> None:
    from . import fleet as _f
    try: _f.stop(name)
    except _f.FleetError as e: click.echo(f"error: {e}", err=True); sys.exit(1)


@fleet_cli.command("restart")
@click.argument("name")
def fleet_restart(name) -> None:
    from . import fleet as _f
    try: _f.restart(name)
    except _f.FleetError as e: click.echo(f"error: {e}", err=True); sys.exit(1)


@fleet_cli.command("rm")
@click.argument("name")
@click.option("--purge", is_flag=True, help="also delete the volume (ALL data for this instance)")
def fleet_rm(name, purge) -> None:
    """Remove an instance. The volume is kept unless --purge."""
    from . import fleet as _f
    try: _f.remove(name, purge=purge)
    except _f.FleetError as e: click.echo(f"error: {e}", err=True); sys.exit(1)


@fleet_cli.command("logs")
@click.argument("name")
@click.option("-f", "--follow", is_flag=True)
@click.option("--tail", default=200)
def fleet_logs(name, follow, tail) -> None:
    from . import fleet as _f
    try: _f.logs(name, follow=follow, tail=tail)
    except _f.FleetError as e: click.echo(f"error: {e}", err=True); sys.exit(1)


@fleet_cli.command("exec", context_settings={"ignore_unknown_options": True})
@click.argument("name")
@click.argument("cmd", nargs=-1)
def fleet_exec(name, cmd) -> None:
    """Run a command inside an instance (default: bash). e.g. orch-fleet exec coach orchctl missions"""
    from . import fleet as _f
    try: sys.exit(_f.exec_in(name, list(cmd)))
    except _f.FleetError as e: click.echo(f"error: {e}", err=True); sys.exit(1)


def fleet_main() -> None:
    fleet_cli()
