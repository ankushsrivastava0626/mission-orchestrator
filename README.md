# mission-orchestrator

A Python daemon + MCP server that lets a "host" Claude Code delegate long-running tasks to a "worker" Claude Code on this machine running inside a tmux session.

## Pieces

- `orchd` - long-running daemon. Owns SQLite at `~/.orch/orch.db`, manages tmux sessions, spawns `claude --resume` per Step, runs the cue engine and scheduler. RPC over `~/.orch/orchd.sock`.
- `orch-mcp` - stdio MCP server. Two modes:
  - `orch-mcp --mode host` - exposes the full tool surface (`mission.*`, `step.*`, `ping.*`, `heartbeat.*`, `secret.*`, `cookies.*`).
  - `orch-mcp --mode worker --mission-id <id>` - scoped tool surface for the worker Claude inside the mission's tmux.
- `orchctl` - admin CLI talking to the daemon socket (missions, step-add, cancel, etc.).
- `msec` - used by the worker Claude inside the mission tmux to read secrets and materialize cookie files.

## Install

```
pip install -e /root/mission-orchestrator
```

Required system packages: `tmux`, `gpg`, `pass`, `claude`.

## Run

```
export ORCH_MASTER_PASSPHRASE=<a-strong-passphrase>
export ORCH_TELEGRAM_BOT_TOKEN=<notification-bot-token>      # outbound: worker.notify -> Telegram
export ORCH_DEFAULT_CHAT_ID=<your-chat-id>                   # default chat for mission.create
# Optional: inbound /command interface on a SEPARATE bot
export ORCH_HOST_BOT_TOKEN=<command-bot-token>               # must be a different bot from above
export ORCH_HOST_ALLOWED_CHAT_IDS=<comma-separated-chat-ids> # who's allowed to send /commands
orchd start
```

If `ORCH_HOST_BOT_TOKEN` and `ORCH_HOST_ALLOWED_CHAT_IDS` are set, orchd will long-poll that bot and accept commands like `/missions`, `/m <id>`, `/step <id> <directive>`, `/cancel <id>`, `/pane <id>`, `/events <id>`, `/secret <id> <name> <value>`, `/heartbeat <id> <s>`, `/delete <id>`, `/help`. Send `/help` in the bot DM for the full list. The bot used here MUST be different from the notification bot - Telegram only allows one polling consumer per bot.

On first start, `orchd` generates a GPG key (`orch-vault`, `orch@localhost`) and initializes the `pass` store under `~/.password-store/` if not already initialized.

## MCP wiring (host Claude)

Add to your host's `.mcp.json`:

```json
{
  "mcpServers": {
    "orch": {
      "command": "orch-mcp",
      "args": ["--mode", "host"]
    }
  }
}
```

The daemon writes a per-mission `.mcp.json` at `/tmp/orch-<mission_id>/.mcp.json` for the worker Claude; the daemon launches each step with `claude --mcp-config <that-path>`.

## Vocabulary

- **Mission** - one Claude session in one tmux. `mission_id == claude session UUID`. Tmux session name: `mission-<id>`.
- **Step** - a directive (prompt) sent to the worker with an entry **Cue**.
- **Cue** types: `immediate` (first step only), `on_prev_complete`, `on_prev_complete_or_timeout` (needs `seconds`), `on_timeout` (delay since prev started, needs `seconds`).
- **Heartbeat** - mandatory, exactly one per Mission, default 86400s, max 86400s. Cannot be deleted or silenced by the worker (worker MCP exposes `heartbeat.get` only).
- **Status Ping** - host-defined, may be multiple. Modes: `on_step_complete` and `on_schedule` (with `seconds`).
- **Vault** - per-Mission `pass` namespace under `mission-<id>/secrets/<name>` and `mission-<id>/cookies/<name>`.

## See also

- `examples/hello_mission.md` - a 60-second walk-through.

## Implementation notes / TODOs

- Crash recovery is best-effort: if a `running` mission's tmux session is gone at daemon startup, the current step is relaunched in a new tmux with `restart_count` bumped and Telegram notified. Full DAG resumption is TODO.
- Worker MCP surface implements `queue.*`, `pings.*`, `heartbeat.get`, `mission.status`, `secrets.list`, `cookies.list`. Wider surface can be added without daemon changes.
- The `events` table is populated but no audit-log MCP tool is exposed yet.
