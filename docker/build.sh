#!/usr/bin/env bash
# Build the orch container image.
#
# Stages the self-contained agent CLI binaries from THIS host into the build
# context (docker/cli-bundle/), then builds. Staging from a working host is the
# reliable path: the binaries are large single executables and the vendor
# installers drift. On a host without the CLIs, install them first (see the
# installer notes in the Dockerfile) or drop the binaries into cli-bundle/
# yourself.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
repo="$(cd "$here/.." && pwd)"
bundle="$here/cli-bundle"
image="${ORCH_IMAGE:-orch:latest}"

echo "== staging agent CLIs into $bundle =="
mkdir -p "$bundle"

stage() {  # name  candidate-glob...
    local name="$1"; shift
    for cand in "$@"; do
        for f in $cand; do
            if [ -x "$f" ]; then
                cp -f "$f" "$bundle/$name"
                echo "   $name  <-  $f  ($(du -h "$f" | cut -f1))"
                return 0
            fi
        done
    done
    echo "   WARNING: $name not found on host; creating a stub (backend will show unavailable)"
    printf '#!/bin/sh\necho "%s not bundled in this image" >&2; exit 127\n' "$name" > "$bundle/$name"
}

stage claude   "$HOME/.local/share/claude/versions/"* "$(command -v claude || true)"
stage codex    "$HOME/.codex/packages/standalone/releases/"*/bin/codex "$(command -v codex || true)"
stage agy      "$HOME/.local/bin/agy" "$(command -v agy || true)"
stage opencode "$HOME/.opencode/bin/opencode" "$(command -v opencode || true)"
chmod +x "$bundle"/*

echo "== docker build ($image) =="
docker build -f "$here/Dockerfile" -t "$image" "$repo"

echo "== done: $image =="
docker image ls "$image" --format '   {{.Repository}}:{{.Tag}}  {{.Size}}'
