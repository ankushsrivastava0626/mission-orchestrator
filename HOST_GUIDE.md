# Host MCP Guide

This is the reference for the **host** side of mission-orchestrator. The host Claude (your interactive session) uses these tools to delegate long-running, stateful work to a separate **worker** Claude running on the daemon machine.

The same guide is shipped to the host Claude at MCP-server init time via the `instructions` field, so it should already see this content. This file is for humans.

## Mental model

```
┌─────────────────────────┐         ┌──────────────────────────────┐
│  host Claude (you)      │         │  daemon machine              │
│                         │         │                              │
│  via SSH-tunnelled      │  RPC    │   orchd (systemd)            │
│  stdio MCP              │ ──────► │   ├─ SQLite (state)          │
│                         │         │   ├─ pass vault (secrets)    │
│  Tools: mission.*,      │         │   └─ tmux: mission-<id>      │
│  step.*, ping.*,        │         │       └─ claude --resume     │
│  heartbeat.*,           │         │           (worker Claude)    │
│  secret.*, cookies.*    │         │                              │
└─────────────────────────┘         └──────────────────────────────┘
                                                  │
                                                  │ worker's notify tool
                                                  ▼
                                            ┌──────────────┐
                                            │  Telegram    │
                                            │  (the user)  │
                                            └──────────────┘
```

**Key invariant**: every Telegram message the user sees is composed by the worker Claude, not the daemon, not the host. The daemon's job is plumbing; the host's job is orchestration; the worker's job is to do the work *and* report on it.

## Vocabulary

| Term | Definition |
|---|---|
| **Mission** | One worker Claude session in one tmux. `mission_id` = Claude session UUID. |
| **Step** | One directive (prompt) sent to the worker, with an entry **Cue**. Linear queue. |
| **Cue** | When a step starts: `immediate` (first only), `on_prev_complete`, `on_prev_complete_or_timeout`, `on_timeout`. |
| **Heartbeat** | Mandatory per-mission timer (24h default, max 24h). Fires → worker nudged to `notify` a status update. |
| **Status Ping** | Optional host-defined nudge. Modes: `on_step_complete` or `on_schedule` (with `seconds`). |
| **Notify** | Worker tool - the ONLY way Telegram messages reach the user. |
| **Vault** | Per-mission `pass` namespace for secrets (`mission-<id>/secrets/<name>`) and cookies (`mission-<id>/cookies/<name>`). |
| **OOB** | "Out-of-band" - directives that bypass the step queue (heartbeat, ping, wrap-up, resume-notice). |

## Typical workflow

1. **Create the mission.** Pick a name; chat id falls back to daemon default.
   ```
   mission.create({name: "reddit-watch"}) → {mission_id}
   ```

2. **Stage any credentials.** If the worker needs API keys, cookies, etc.:
   ```
   secret.put({mission_id, name: "reddit_token", value: "..."})
   cookies.put({mission_id, name: "reddit", content: "<netscape jar>"})
   ```

3. **Queue the first step.** Cue must be `immediate`:
   ```
   step.add({
     mission_id,
     directive: "Use `msec get reddit_token` for the token. Fetch /r/python top 10 today, save to /tmp/posts.json. Then call notify with a 2-sentence summary.",
     cue: {type: "immediate"}
   })
   ```

4. **Queue follow-ups.** Use `on_prev_complete`:
   ```
   step.add({
     mission_id,
     directive: "Re-read /tmp/posts.json, rank by upvotes, post the top 3 titles to Telegram via notify.",
     cue: {type: "on_prev_complete"}
   })
   ```

5. **(Optional) Add recurring pings.** E.g. every 30 min, ask the worker to report progress:
   ```
   ping.add({
     mission_id,
     command: "Briefly say what you're currently working on.",
     mode: {type: "on_schedule", seconds: 1800}
   })
   ```

6. **Monitor.** `mission.get(mission_id)` returns mission row + steps + pings. State transitions visible in the steps.

7. **Auto-completion.** Only fires when *all* of: no pending steps, no running step, no pings configured, and the worker is idle. The engine injects a wrap-up directive ("summarize what you accomplished via notify"), waits, then tears down the tmux. Mission state becomes `completed`. **The vault is preserved** so the mission can be reopened.
   - **If you've configured pings**, the mission stays `running` indefinitely - pings keep firing forever. To end it: `ping.delete` them all (auto-complete kicks in), or `mission.cancel` (soft-cancel with goodbye).

8. **(Optional) Reopen.** Call `step.add` on a `completed` mission and it transitions back to `running` automatically: tmux is recreated, the worker Claude resumes via `claude --resume` (full prior context), the vault is still there. The new step must use `cue: on_prev_complete` (not `immediate`, since position > 0). `mission.pane_snapshot` returns the archived final pane up until the moment of reopen.

9. **mission.delete** once truly done. This is the only path that purges the vault and removes the mission row.

8. **(Optional) Delete.** Once terminal:
   ```
   mission.delete({mission_id})
   ```
   Removes the row, cascades steps + pings, best-effort cleans up tmux + vault.

## Cue types in detail

| Cue | When the step starts |
|---|---|
| `{type: "immediate"}` | As soon as queued. **Only valid for the first step**. |
| `{type: "on_prev_complete"}` | After the previous step finishes (any terminal state). |
| `{type: "on_prev_complete_or_timeout", seconds: N}` | Whichever comes first: previous completes, or N seconds elapse since it started. Previous step is marked `timed_out` if the timer wins. |
| `{type: "on_timeout", seconds: N}` | Always wait N seconds after the previous step *started*, then fire - regardless of whether previous completed. |

## What the worker can do

Inside the tmux, the worker Claude has access to the **worker MCP server** (scoped to its own mission only):

```
notify(text)                - send Telegram message (the ONLY user-facing channel)
queue.list/add/update/delete - recursively manage own pending steps
pings.list/add/update/delete - manage own pings
heartbeat.get               - read-only (worker cannot silence its heartbeat)
mission.status              - read own mission row
secrets.list / cookies.list - names only; values via `msec` CLI
```

Plus all of Claude Code's normal capabilities (Bash, Read, Write, web tools, etc.) - runs with `--dangerously-skip-permissions`, full system access.

## What the host CANNOT do (today)

These are known gaps tracked in the project audit. Listed here so you don't waste time looking for them:

- Read the worker's live tmux pane contents
- Read the structured events audit log
- Send Telegram messages directly (by design - only the worker composes)
- Read a step's full transcript (Claude's actual responses)
- Soft-cancel with a Claude-composed goodbye (cancel is hard kill today)

If you need real-time visibility, `mission.attach_info` returns the `tmux attach` command - the user can SSH into the daemon machine and watch the pane directly.

## Error handling

| Error code | Meaning | Fix |
|---|---|---|
| `missing_chat_id` | `mission.create` got no chat id and the daemon has no default. | Pass `telegram_chat_id` explicitly or set `ORCH_DEFAULT_CHAT_ID` in `/etc/orchd.env`. |
| `not_found` | mission_id / step_id / ping_id doesn't exist. | Check it (typo? deleted?). |
| `not_pending` | Trying to update/delete a step that's already running or terminal. | Use `step.cancel_current` to stop a running step. |
| `not_terminal` | Trying to delete a mission that's still running. | Call `mission.cancel` first, then `mission.delete`. |
| `bad_interval` | Heartbeat interval out of range (1..86400). | Pick a value in range. |
| `tmux_failed` | Couldn't create the tmux session. | Check tmux is installed on the daemon machine. |

## A worked example

User says: "watch r/python every hour, post top-3 to Telegram each time, and tell me when something hits >1000 upvotes."

```
1. mid = mission.create({name: "rpython-watch"}).mission_id

2. secret.put({mission_id: mid, name: "reddit_token", value: "..."})

3. step.add({
     mission_id: mid,
     directive: "You are watching r/python. Don't fetch yet; just confirm setup via notify ('Watching r/python, polling hourly').",
     cue: {type: "immediate"}
   })

4. ping.add({
     mission_id: mid,
     command: "Use `msec get reddit_token`, fetch /r/python top 10 last hour. Pick top 3 by score, notify them. If any score >1000, send a separate notify flagging it.",
     mode: {type: "on_schedule", seconds: 3600}
   })

5. heartbeat.set({mission_id: mid, interval_s: 43200})  # 12h sanity
```

That's it. The host hands off and the worker handles everything from there, sending Telegram updates per ping.

## Operational notes

- Mission state is fully recoverable across orchd restarts. The systemd unit auto-restarts on failure (`/etc/systemd/system/orchd.service`).
- The daemon socket is at `/root/.orch/orchd.sock` (mode 0600, root).
- The pass vault lives under `~/.password-store/mission-<id>/`.
- Worker MCP config per mission: `/tmp/orch-<id>/.mcp.json`.
- Logs: `journalctl -u orchd -f`.
