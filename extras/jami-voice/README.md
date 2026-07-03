# jami-voice - phone-call escalation for orch (J-dawg)

Lets workers place REAL ringing voice calls to you over [Jami](https://jami.net)
(p2p, no phone number needed) when a text notify isn't enough: the voice agent
reads the mission's context, asks the question, and relays your answer back to
the worker as a new step.

## Pieces
- `agent_jami_orch.py`  - the voice agent: watches `~/.jdawg/requests/`,
  places/answers Jami calls, streams audio to/from the speech model, and
  reports the outcome back to orchd (`mission.hold`, `step.add`).
- `agent_matrix_orch.py`, `matrix_lk.py` - the shared "brain" (LLM turn logic).
- `jdawg_mcp.py`        - the `talk_to_user` MCP server exposed to workers.
  Wire it in via ORCH_EXTRA_WORKER_MCPS (see worker_mcp.json example below).
- `jami-orch-agent.service` - systemd unit template.

## Requirements
- `jamid` (Jami daemon) running headless with a registered account.
- PulseAudio/pipewire null sinks for audio injection/capture:
  a `virtmic` sink (worker → call) and `dummyout.monitor` (call → worker).
- A speech-to-speech or STT+TTS backend (the reference setup uses Kyutai
  Moshi on a Modal GPU; anything the brain module can reach over HTTP works).
- `pip install -r requirements.txt` into the venv the service uses.

## Worker wiring (any agent backend)
`/etc/orch/worker_mcp.json`:
```json
{
  "mcpServers": {
    "jdawg": {
      "command": "python3",
      "args": ["/opt/orch/extras/jami-voice/jdawg_mcp.py",
               "--mission-id", "{mission_id}"]
    }
  }
}
```
Workers then get a `talk_to_user(summary, question)` tool; calling it drops a
request file, holds the mission open, and J-dawg phones you with that
mission's calling name as the caller ID.

This extra is a reference implementation extracted from a working deployment -
expect to adapt paths, account ids, and the audio routing to your machine.
