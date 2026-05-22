# Hello mission

A 60-second walkthrough creating a mission, adding two steps, watching them run.

## 1. Start the daemon

```
export ORCH_MASTER_PASSPHRASE=test-passphrase
export ORCH_TELEGRAM_BOT_TOKEN=123:ABC      # optional; messages are skipped if unset
orchd start
```

## 2. Create a mission

From a second shell:

```
orchctl create --name "hello" --chat-id 12345
# => {"mission_id": "..."}
MID=<paste-uuid>
```

This spawns a tmux session `mission-<MID>` and writes
`/tmp/orch-<MID>/.mcp.json` for the worker Claude.

You can attach to watch:

```
tmux attach -t mission-$MID
```

## 3. Add two steps

```
orchctl step-add --mission-id $MID \
  --directive "Print 'hello world' and exit." \
  --cue '{"type":"immediate"}'

orchctl step-add --mission-id $MID \
  --directive "Now print the current date." \
  --cue '{"type":"on_prev_complete"}'
```

The daemon's cue engine launches step 1 immediately, watches for `claude` to exit, marks the step complete, then launches step 2.

## 4. Store a secret

```
orchctl missions          # confirm mission state == running
# Use the MCP host tools (or write a small client) for secret.put:
python - <<'EOF'
from orch.client import DaemonClient
with DaemonClient() as c:
    print(c.call("secret.put", {
        "mission_id": "$MID",
        "name": "GITHUB_TOKEN",
        "value": "ghp_xxxxxxxxxxxxxx",
    }))
EOF
```

Inside the worker tmux:

```
msec get GITHUB_TOKEN   # prints the value
msec list               # lists all secret/cookie names
```

## 5. Cancel

```
orchctl cancel $MID
```

Kills the tmux session, marks pending steps `cancelled`, purges the vault namespace, sends a Telegram notice.
