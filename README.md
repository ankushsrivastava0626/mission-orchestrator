# orch - mission orchestrator for AI agents

Delegate long-running, stateful jobs ("missions") to AI worker agents that run
unattended on your machine - each in its own tmux session with persistent
memory - and control the whole fleet from Telegram: every mission gets its own
chat topic, agents message you, you message them back, they phone you (voice
extra) when something truly needs you.

Works with **any coding agent**:

| backend  | runs on                              | resume | compact | notes |
|----------|--------------------------------------|--------|---------|-------|
| `claude` | Claude Code CLI                      | ✓      | ✓       | richest support (default) |
| `codex`  | OpenAI Codex CLI                     | ✓      | ✓       | per-mission CODEX_HOME isolation |
| `antigravity` | Google Antigravity (`agy`)      | ✓      | -       | tools via the `oworker` shell bridge |
| `opencode` | OpenCode (open source)             | ✓      | -       | any model via `-m provider/model` (OpenRouter, local, …) |
| `gemini` | Gemini CLI                           | ✓      | -       | per-mission workdir isolation |
| `api`    | **no CLI - just an API key**         | ✓      | ✓       | Anthropic or any OpenAI-compatible endpoint (OpenAI, OpenRouter, Ollama, …) |
| `custom` | any agent CLI via command templates  | you    | -       | plug in anything |

**Switch backends live** - `orchctl agent set gemini` (or `/agent gemini` in
Telegram, or the `agent.set` host-MCP tool). The switch persists, and each
running mission *migrates on its next wake*: it starts a fresh session on the
new agent seeded with an auto-generated handoff (mission state, step history,
active watchers, and a tail of the old conversation) - no work lost, no tokens
spent building it. `orchctl agent show` lists what's usable on the machine.
**Sticky by default:** whatever agent/model you set STAYS set - orch never
switches behind your back; a broken backend fails loudly in the logs instead.
Optional safety net: `orchctl config set agent_auto_fallback on` lets orch
auto-revert to the last working backend (and unpin dead pinned ones) after
repeated dead turns.

**Per-mission override** - pin a single mission to a different backend while
everything else stays on the global one: the 🤖 button on the mission's
Telegram card, or `orchctl agent pin <mission> <backend>` (`-` clears the
pin), or the `mission.set_agent` RPC. Pinned missions migrate with the same
handoff mechanism; if a pinned backend dies, the mission auto-unpins back to
the global agent rather than stalling.

## Install

```bash
git clone <this repo> orch && cd orch
./install.sh          # checks deps, installs CLIs, runs the setup wizard
```

Requirements: Linux or macOS (Windows → WSL), Python ≥ 3.11, `tmux`.
Optional: `pass`+GPG (secrets vault), systemd (run-at-boot service).

The wizard (`orchd setup`, also auto-offered on first `orchd start`) asks for:
your agent backend, a Telegram bot token (it auto-detects your chat id when
you message the bot), an optional Topics group, and a vault passphrase - then
writes `~/.orch/orchd.env` and can install the systemd service.

## Concepts (90 seconds)

- **Mission** - one persistent worker-agent session in one tmux. Survives
  reboots; resumes with full context on every wake.
- **Step** - a directive in the mission's linear queue, gated by a **cue**:
  `immediate`, `on_current_complete` (jump the queue), `on_timeout`,
  `at_time` (absolute wall-clock - workers self-schedule future work).
- **Heartbeat** - periodic "report status to the user" nudge (default daily).
- **Scripted ping** - the worker writes+tests a tiny watcher script that polls
  some condition with **zero tokens** and only wakes the agent when it fires
  (a watchdog re-tasks the worker if the script dies).
- **Auto-compaction** - when an idle worker's context crosses 200k tokens the
  daemon compacts the session so future wakes stay cheap.
- **Vault** - per-mission secrets in `pass`; workers read them with `msec`.

## Telegram control

One @BotFather bot gives you:

- `/create <name>`, `/missions`, `/m <id>`, `/context` (live token sizes),
  `/pane`, `/events`, cancel/delete - all with inline-keyboard menus.
- **Topics mode**: point orch at a forum supergroup and every mission gets its
  own topic. Messages you type in a topic go straight to that worker; its
  replies come back in-thread. **Creating a topic creates a mission.**
- Attachments both ways (images inline, any file type), typing indicators,
  reply coalescing (rapid messages within 5 s arrive as one directive),
  per-mission "calling name", live-location capture for `get_user_location`.

## Host control (drive it from another AI)

Any MCP-capable assistant can be the "host" that creates missions and reads
the worker→host mailbox. Point it at `orch-mcp --mode host` (see
`host-mcp-config.json`; works over SSH for remote control). Workers get their
own scoped MCP surface automatically: `notify`, `send_file`, `talk_to_user`
(voice extra), `message_host`, `queue.*`, `pings.*`, `heartbeat.*`,
`get_user_location`, `mission.status`.

## CLIs

| command   | what |
|-----------|------|
| `orchd`   | the daemon (`setup`, `start`, `status`) |
| `orchctl` | admin from the shell (create, step-add, cancel, …) |
| `orch-mcp`| MCP server, host mode & worker mode |
| `owatch`  | scripted-ping heartbeat/fire/ready (used by watcher scripts) |
| `msec`    | worker-side secret/cookie reader |
| `oworker` | worker toolkit as shell commands (for MCP-less backends) |

## Extras

- **`extras/jami-voice/`** - J-dawg: real ringing voice calls over Jami when a
  worker needs a decision; reads the mission's context, relays your spoken
  answer back as a new step. See its README.

## Files & dirs

`~/.orch/` - db, socket, config (`orchd.env`), mailbox, incoming files,
api-backend sessions, compact logs. Worker scratch: `/tmp/orch-<mission>/`.

## Security notes

Workers run with full permissions on this machine by design - they are *your*
agents doing real work. Isolation between missions is logical (scoped tools,
separate sessions/topics), not an OS sandbox. Don't run untrusted directives.
