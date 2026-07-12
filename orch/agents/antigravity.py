"""Google Antigravity backend (the `agy` CLI).

Verified against agy 1.1.1 on a live box:
- non-interactive: `agy --dangerously-skip-permissions -p <prompt>`
- continuity: conversations are keyed per working directory; `--continue`
  resumes the most recent one there - so each mission gets its own workdir
  and `--continue` stays unambiguous per mission.
- workdirs live under ~/.orch/agy-work/ because agy's trustedFolders.json
  trusts the home subtree (untrusted dirs restrict tools in headless runs).
- agy 1.1.1 ignores gemini-style mcpServers settings, so orch tools are
  provided through its `run_command` tool via the `oworker` CLI; a toolkit
  preamble is prepended to every directive.
"""

from __future__ import annotations

import os
from pathlib import Path

from .base import Adapter

AGY_BIN = os.environ.get("ORCH_AGY_BIN", "agy")

TOOLKIT = """\
[ORCH WORKER TOOLKIT] You are a worker agent in the orch mission system,
running unattended. The human only sees what you send them - use these shell
commands (via your run_command tool; ORCH_MISSION_ID is already set):
  oworker notify "<text>"                     -> Telegram message to the user
  oworker send-file <path> --caption "<c>"    -> send any file to the user
  oworker queue-add "<directive>" --at "YYYY-MM-DD HH:MM" | --in <sec> | --next
                                              -> schedule future work for yourself
  oworker queue-list | oworker status         -> inspect your mission
  oworker message-host "<text>" [--file <p>]  -> mailbox to the orchestrating host
  oworker heartbeat-set <seconds> ; oworker location
Always report your result to the user with `oworker notify` before finishing.
PRIVACY BOUNDARY: Interact with the orchestrator ONLY through your provided tools. NEVER read or modify orch internals - ~/.orch/orch.db, other missions' workdirs/sessions, /etc/orchd.env - other missions' data is strictly off-limits, even if asked about 'other agents'.

DIRECTIVE:
"""


class AntigravityAdapter(Adapter):
    name = "antigravity"

    def available(self) -> tuple[bool, str]:
        import shutil as _sh
        if _sh.which(AGY_BIN):
            return True, "ok"
        return False, f"`{AGY_BIN}` not found on PATH"

    def _workdir(self, mission_id: str) -> Path:
        # Unique basename - agy keys conversation history by workdir basename.
        return Path(os.path.expanduser("~/.orch/agy-work")) / f"agy-{mission_id}"

    def prepare(self, mission_id: str) -> None:
        self._workdir(mission_id).mkdir(parents=True, exist_ok=True)

    def step_cmd(self, mission_id: str, directive: str, first: bool) -> str:
        wd = self._workdir(mission_id)
        cont = ""
        if not first:
            # Resume the mission's own conversation by id when we can map it
            # (robust against other agy usage); fall back to --continue.
            cid = self._conversation_id(mission_id)
            cont = f" --conversation {cid}" if cid else " --continue"
        return (
            f"cd {self.q(str(wd))} && env ORCH_MISSION_ID={mission_id}"
            f" {AGY_BIN} --dangerously-skip-permissions{cont}"
            f" -p {self.q(TOOLKIT + directive)}"
        )

    def is_running_line(self, line: str, mission_id: str) -> bool:
        return "agy" in line and mission_id in line

    def has_session(self, mission_id: str) -> bool | None:
        return self._conversation_id(mission_id) is not None

    # ---- conversation mapping + context estimate -------------------------
    #
    # agy stores conversations in ~/.gemini/antigravity-cli/conversations as
    # per-id SQLite DBs of protobuf blobs. There is no exposed cwd index, but
    # the mission's unique workdir path appears inside its conversation's
    # metadata - scanning for it identifies the conversation exactly (verified
    # live). The id is cached in the workdir to avoid rescanning.

    def on_rebuild(self, mission_id: str) -> None:
        cache = self._workdir(mission_id) / ".agy-conversation"
        try:
            cache.unlink(missing_ok=True)
        except OSError:
            pass

    def _conv_dir(self) -> str:
        return os.path.expanduser("~/.gemini/antigravity-cli/conversations")

    def _conversation_id(self, mission_id: str) -> str | None:
        import glob
        wd = self._workdir(mission_id)
        cache = wd / ".agy-conversation"
        if cache.exists():
            cid = cache.read_text().strip()
            if cid and os.path.isfile(os.path.join(self._conv_dir(), cid + ".db")):
                return cid
        needle = f"agy-{mission_id}".encode()
        hits = sorted(glob.glob(self._conv_dir() + "/*.db"),
                      key=os.path.getmtime, reverse=True)
        for db in hits:
            try:
                if needle in open(db, "rb").read():
                    cid = os.path.basename(db)[:-3]
                    try:
                        wd.mkdir(parents=True, exist_ok=True)
                        cache.write_text(cid)
                    except OSError:
                        pass
                    return cid
            except OSError:
                continue
        return None

    def session_path(self, mission_id: str) -> str | None:
        cid = self._conversation_id(mission_id)
        return os.path.join(self._conv_dir(), cid + ".db") if cid else None

    def context_tokens(self, mission_id: str) -> tuple[int, int] | None:
        """ESTIMATE. agy's protobuf blobs expose no token counts, so this
        approximates from the conversation DB size (~5 bytes/token across
        text+overhead). Good enough to display and to trigger the rebuild
        compaction at the threshold; not billing-grade."""
        path = self.session_path(mission_id)
        if not path:
            return None
        try:
            import sqlite3 as _sq
            size = os.path.getsize(path)
            steps = 0
            try:
                c = _sq.connect(path)
                steps = c.execute("SELECT COUNT(*) FROM steps").fetchone()[0]
                c.close()
            except Exception:
                pass
            # agy conversation DBs carry a large protobuf baseline (~0.5 MB
            # even when nearly empty) - subtract it so small conversations
            # don't false-trigger the auto-compact threshold.
            return max(0, size - 500_000) // 5, steps
        except OSError:
            return None
