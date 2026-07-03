"""Adapter interface every worker backend implements."""

from __future__ import annotations

import shlex


class Adapter:
    """One coding-agent backend (Claude Code, Codex, Gemini, raw API, …).

    The contract with the core:
      - prepare() is called before every launch; set up per-mission config
        (MCP wiring, session home, workdir) there.
      - step_cmd() returns ONE shell command line; the runner types it into the
        mission's tmux pane. It must run the agent non-interactively on the
        given directive and exit when the turn is done.
      - The command line MUST contain the mission id somewhere pgrep can see
        (all built-ins do) so step_running() can detect a live turn.
    """

    name = "base"
    # Can the backend shrink a long-lived session to a summary?
    supports_compact = False

    # ---- lifecycle -------------------------------------------------------

    def prepare(self, mission_id: str) -> None:
        """Create per-mission config (MCP files, session homes). Idempotent."""

    def step_cmd(self, mission_id: str, directive: str, first: bool) -> str:
        raise NotImplementedError

    def cleanup(self, mission_id: str) -> None:
        """Remove per-mission scratch state (called at mission teardown)."""

    # ---- observability ---------------------------------------------------

    def is_running_line(self, line: str, mission_id: str) -> bool:
        """Does this pgrep -af line represent a live worker turn for the
        mission? Default: the line mentions the mission id and the agent."""
        return mission_id in line

    def context_tokens(self, mission_id: str) -> tuple[int, int] | None:
        """(approx tokens re-read on next resume, turn count), or None if the
        backend can't measure it."""
        return None

    def session_path(self, mission_id: str) -> str | None:
        """Path of the persisted session/transcript file, if any."""
        return None

    def compact(self, mission_id: str) -> bool:
        """Kick off a session compaction. Returns True if one was started."""
        return False

    # ---- helpers ---------------------------------------------------------

    @staticmethod
    def q(s: str) -> str:
        return shlex.quote(s)
