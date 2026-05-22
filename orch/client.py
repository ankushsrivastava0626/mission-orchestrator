"""Unix-socket JSON-line RPC client. Used by MCP server and CLIs."""

from __future__ import annotations

import json
import socket
from pathlib import Path
from typing import Any

from . import config


class DaemonError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


class DaemonClient:
    def __init__(self, sock_path: Path | None = None) -> None:
        self.sock_path = sock_path or config.socket_path()
        self._sock: socket.socket | None = None
        self._buf = b""
        self._req_id = 0

    def _connect(self) -> None:
        if self._sock is not None:
            return
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.connect(str(self.sock_path))
        self._sock = s

    def close(self) -> None:
        if self._sock is not None:
            try:
                self._sock.close()
            finally:
                self._sock = None

    def __enter__(self) -> "DaemonClient":
        self._connect()
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def _readline(self) -> bytes:
        assert self._sock is not None
        while b"\n" not in self._buf:
            chunk = self._sock.recv(65536)
            if not chunk:
                raise DaemonError("eof", "daemon closed connection")
            self._buf += chunk
        line, self._buf = self._buf.split(b"\n", 1)
        return line

    def call(self, method: str, params: dict[str, Any] | None = None) -> Any:
        self._connect()
        assert self._sock is not None
        self._req_id += 1
        req = {"id": self._req_id, "method": method, "params": params or {}}
        self._sock.sendall((json.dumps(req) + "\n").encode("utf-8"))
        line = self._readline()
        resp = json.loads(line.decode("utf-8"))
        if "error" in resp and resp["error"] is not None:
            err = resp["error"]
            raise DaemonError(err.get("code", "unknown"), err.get("message", ""))
        return resp.get("result")
