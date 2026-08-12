# orch container image: one full orch stack (daemon + tmux + workers) per container.
#
# Design: agent CLI binaries live in /opt (baked into the image, shared across
# every container). ALL state and credentials live under /root, which the fleet
# manager mounts as a per-instance named volume. So a container is disposable
# and the volume is the memory: kill a container, start a new one on the same
# volume, and orchd's crash recovery resumes every in-flight mission.
#
# The self-contained agent binaries are staged from a working host by
# docker/build.sh into docker/cli-bundle/ before the build. The equivalent
# public installers are noted next to each COPY for anyone building fresh.

FROM python:3.12-slim-bookworm

# Runtime dependencies:
#   tmux      workers live in tmux sessions
#   pass gnupg  the secrets vault
#   git curl ca-certificates  general + installers
#   procps    pgrep, used by busy-detection
#   nodejs    the gemini CLI is a node package
RUN apt-get update && apt-get install -y --no-install-recommends \
        tmux git curl ca-certificates procps pass gnupg xz-utils \
    && curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*

# Agent CLIs.
# Bundled self-contained binaries (staged by docker/build.sh). Public installers:
#   claude   : curl -fsSL https://claude.ai/install.sh | bash
#   codex    : npm install -g @openai/codex
#   agy      : Google Antigravity installer
#   opencode : curl -fsSL https://opencode.ai/install | bash
COPY docker/cli-bundle/claude   /opt/agents/claude
COPY docker/cli-bundle/codex    /opt/agents/codex
COPY docker/cli-bundle/agy      /opt/agents/agy
COPY docker/cli-bundle/opencode /opt/agents/opencode
RUN chmod +x /opt/agents/* \
    && ln -sf /opt/agents/claude   /usr/local/bin/claude \
    && ln -sf /opt/agents/codex    /usr/local/bin/codex \
    && ln -sf /opt/agents/agy      /usr/local/bin/agy \
    && ln -sf /opt/agents/opencode /usr/local/bin/opencode

# gemini CLI (node package, installs outside /root so the volume never hides it)
RUN npm install -g @google/gemini-cli || echo "gemini install skipped"

# orch itself (the api and custom backends need no CLI at all). Only the
# package files, so the bundled binaries are not duplicated into this layer.
COPY orch /opt/orch/orch
COPY pyproject.toml README.md /opt/orch/
RUN pip install --no-cache-dir /opt/orch

# Live config changes (orchctl config set, agent switch) persist to the volume.
ENV ORCH_ENV_FILE=/root/.orch/orchd.env
ENV PATH="/usr/local/bin:/opt/agents:${PATH}"

COPY docker/entrypoint.sh /usr/local/bin/orch-entrypoint
RUN chmod +x /usr/local/bin/orch-entrypoint

ENTRYPOINT ["orch-entrypoint"]
