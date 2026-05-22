# Wiring a remote Claude Code to this daemon

The daemon runs on this machine (`107.170.18.216`). A "host" Claude Code on any other machine can drive it over SSH - the remote machine doesn't need anything installed except an SSH client and key access to this box.

## How it works

```
[remote machine: Claude Code]
        │
        │ stdin/stdout over SSH
        ▼
[ssh root@107.170.18.216 /usr/local/bin/orch-mcp --mode host]
        │
        │ Unix-socket JSON-RPC
        ▼
[orchd (systemd) at /root/.orch/orchd.sock]
```

The remote `claude` spawns `ssh ...` as the MCP server command; `orch-mcp` runs on this machine and talks to the local daemon socket. Standard MCP stdio is tunnelled inside the SSH connection.

## Prerequisites on the remote machine

1. SSH client (already present on macOS / Linux / WSL / Windows OpenSSH).
2. SSH key authentication to this machine - i.e. `ssh root@107.170.18.216 echo ok` returns `ok` with no password prompt. Use `ssh-copy-id` if you haven't set this up yet.
3. Claude Code installed.

## Wiring

Pick one of three scopes for the MCP config:

### A. User scope (available in every Claude Code session - recommended)

```bash
claude mcp add --scope user orch -- \
  ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
      -o ServerAliveInterval=30 -o ServerAliveCountMax=3 -o ConnectTimeout=10 \
  root@107.170.18.216 /usr/local/bin/orch-mcp --mode host
```

### B. Project scope (committed to a project)

Copy `host-mcp-config.json` from this repo to `.mcp.json` in the project root. Anyone who opens that project in Claude Code will get the `orch` server (after approving the workspace-trust prompt).

```bash
scp root@107.170.18.216:/root/mission-orchestrator/host-mcp-config.json ./.mcp.json
```

### C. Local scope (one machine, one user)

```bash
claude mcp add orch -- ssh root@107.170.18.216 /usr/local/bin/orch-mcp --mode host
```

## Verify

```bash
claude mcp list | grep orch
# should show: orch: ssh root@107.170.18.216 ... - ✓ Connected
```

In a fresh Claude Code session, the host tools appear as `mcp__orch__*` - `mission.create`, `step.add`, `ping.add`, `secret.put`, `cookies.put`, etc.

## Notes

- **Change the IP/hostname.** `107.170.18.216` is this droplet's current public IPv4. If you have DNS for it, use the name instead so it survives an IP change.
- **Non-root SSH user.** If you don't want to log in as root, create a user on this machine, add it to a group that can read `/root/.orch/orchd.sock`, and update the SSH command. Easiest path: keep root for now, harden later.
- **Latency.** Every MCP tool call round-trips over SSH. For local-LAN this is unnoticeable; over WAN there's an extra ~50-200ms per call. Fine for an orchestration tool, not great for tight loops.
- **`BatchMode=yes`** ensures the SSH command fails fast if key auth isn't set up, instead of hanging waiting for a password prompt that Claude Code can't respond to.
