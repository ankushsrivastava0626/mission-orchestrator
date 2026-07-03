#!/usr/bin/env bash
# orch installer - Linux/macOS (Windows: use WSL). Installs the package and
# its CLIs (orchd, orchctl, orch-mcp, owatch, msec), then runs `orchd setup`.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
echo "── orch installer ──────────────────────────────"

# 1. prerequisites
py="$(command -v python3 || true)"
[ -n "$py" ] || { echo "✗ python3 not found (need >= 3.11)"; exit 1; }
"$py" - <<'EOF' || { echo "✗ python >= 3.11 required"; exit 1; }
import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)
EOF
command -v tmux >/dev/null || {
  echo "✗ tmux is required (workers live in tmux sessions)."
  echo "  install:  apt install tmux   |   brew install tmux"
  exit 1
}
command -v pass >/dev/null || echo "⚠ 'pass' not found - secrets vault disabled until installed (apt/brew install pass)"

# 2. install the package (pipx if present, else pip --user, else venv)
if command -v pipx >/dev/null; then
  echo "→ installing with pipx"
  pipx install --force "$here"
elif "$py" -m pip install --user "$here" 2>/dev/null; then
  echo "→ installed with pip --user (ensure ~/.local/bin is on PATH)"
else
  echo "→ installing into venv ~/.orch/venv"
  "$py" -m venv "$HOME/.orch/venv"
  "$HOME/.orch/venv/bin/pip" install -q "$here"
  mkdir -p "$HOME/.local/bin"
  for cmd in orchd orchctl orch-mcp owatch msec; do
    ln -sf "$HOME/.orch/venv/bin/$cmd" "$HOME/.local/bin/$cmd"
  done
  echo "  linked CLIs into ~/.local/bin (add it to PATH if needed)"
fi

# 3. first-run wizard (agent backend, telegram, vault, systemd service)
echo
orchd setup || "$HOME/.local/bin/orchd" setup

echo
echo "── done ────────────────────────────────────────"
echo "voice extras (Jami calls): see extras/jami-voice/README.md"
