"""Template-driven backend - plug in ANY agent CLI without writing code.

Set two command templates (shell lines; {mission_id} and {directive} are
substituted, directive is shell-quoted for you):

  ORCH_CUSTOM_FIRST_CMD   e.g.  myagent run --new {mission_id} -p {directive}
  ORCH_CUSTOM_RESUME_CMD  e.g.  myagent run --continue {mission_id} -p {directive}

Requirements for the target CLI:
  - non-interactive: runs the directive and exits when the turn is done
  - the command line contains {mission_id} (needed for busy-detection)
  - to give the agent orch tools, point it at the `orch-mcp` MCP server
    (--mode worker --mission-id {mission_id}) in its own config format.
"""

from __future__ import annotations

import os

from .base import Adapter


class CustomAdapter(Adapter):
    name = "custom"

    def __init__(self) -> None:
        self.first_tmpl = os.environ.get("ORCH_CUSTOM_FIRST_CMD", "")
        self.resume_tmpl = os.environ.get("ORCH_CUSTOM_RESUME_CMD", "") or self.first_tmpl
        if not self.first_tmpl:
            raise RuntimeError(
                "ORCH_AGENT=custom requires ORCH_CUSTOM_FIRST_CMD "
                "(and optionally ORCH_CUSTOM_RESUME_CMD)"
            )

    def step_cmd(self, mission_id: str, directive: str, first: bool) -> str:
        tmpl = self.first_tmpl if first else self.resume_tmpl
        return tmpl.replace("{mission_id}", mission_id).replace(
            "{directive}", self.q(directive)
        )
