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
        cont = "" if first else " --continue"
        return (
            f"cd {self.q(str(wd))} && env ORCH_MISSION_ID={mission_id}"
            f" {AGY_BIN} --dangerously-skip-permissions{cont}"
            f" -p {self.q(TOOLKIT + directive)}"
        )

    def is_running_line(self, line: str, mission_id: str) -> bool:
        return "agy" in line and mission_id in line

    def has_session(self, mission_id: str) -> bool | None:
        # agy stores conversations in ~/.gemini/antigravity-cli/conversations
        # as opaque per-id SQLite DBs with no exposed cwd mapping - we can't
        # cheaply tell which belongs to this mission. Return None so the
        # engine falls back to DB history (first launch = create, then
        # --continue), which is correct for agy's per-workdir continuity.
        return None
