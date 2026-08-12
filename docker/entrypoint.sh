#!/usr/bin/env bash
# Container entrypoint. The persistent volume is mounted at /root, so anything
# needed at runtime lives either here (created on demand) or in /opt (image).
set -e

export HOME=/root
mkdir -p /root/.orch

# Seed a persistent env file from the container environment on first boot, so
# live config changes survive restarts and orchd finds a config.
if [ ! -f /root/.orch/orchd.env ]; then
    : > /root/.orch/orchd.env
    chmod 600 /root/.orch/orchd.env
    for k in ORCH_AGENT ORCH_HOST_BOT_TOKEN ORCH_HOST_ALLOWED_CHAT_IDS \
             ORCH_DEFAULT_CHAT_ID ORCH_TOPICS_CHAT_ID ORCH_MASTER_PASSPHRASE \
             ORCH_API_PROVIDER ORCH_API_KEY ORCH_API_MODEL ORCH_API_BASE_URL \
             ORCH_OPENCODE_MODEL OPENROUTER_API_KEY ANTHROPIC_API_KEY \
             OPENAI_API_KEY GEMINI_API_KEY ORCH_INSTANCE_NAME; do
        v="${!k}"
        [ -n "$v" ] && echo "$k=$v" >> /root/.orch/orchd.env
    done
fi

# GPG needs a home for the pass vault; keep it quiet in a headless container.
export GNUPGHOME="${GNUPGHOME:-/root/.gnupg}"
mkdir -p "$GNUPGHOME" && chmod 700 "$GNUPGHOME"

echo "orch container starting: instance=${ORCH_INSTANCE_NAME:-unnamed} agent=${ORCH_AGENT:-claude}"
exec orchd start
