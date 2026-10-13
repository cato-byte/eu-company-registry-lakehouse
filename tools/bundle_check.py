"""Validate the Databricks Asset Bundle without a workspace (CI on every pull request).

`databricks bundle validate` needs a workspace only to resolve the current user and check the
deploy folder. This script answers those two calls from a local stub, then runs a strict
validation of every target. That catches schema errors, bad variable interpolation and target
rules (e.g. production root_path) before anything is deployed.

    python tools/bundle_check.py [path/to/databricks]
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

FAKE_SP = "00000000-0000-0000-0000-000000000000"


class Stub(BaseHTTPRequestHandler):
    def _send(self, body: dict) -> None:
        data = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        if "scim/v2/Me" in self.path:
            self._send({"userName": "ci@example.com", "id": "1"})
        elif "workspace/get-status" in self.path:
            self._send({"object_type": "DIRECTORY", "path": "/", "object_id": 1})
        else:
            self._send({})

    do_POST = do_GET

    def log_message(self, *args: object) -> None:
        pass


def main(argv: list[str]) -> int:
    cli = argv[0] if argv else "databricks"
    server = ThreadingHTTPServer(("127.0.0.1", 0), Stub)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    env = {
        **os.environ,
        "DATABRICKS_HOST": f"http://127.0.0.1:{server.server_port}",
        "DATABRICKS_TOKEN": "stub",
        "BUNDLE_VAR_alert_email": "alerts@example.com",
        "BUNDLE_VAR_service_principal_id": FAKE_SP,
    }
    for var in ("DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET", "DATABRICKS_CONFIG_PROFILE"):
        env.pop(var, None)
    failed = False
    for target in ("dev", "prod"):
        res = subprocess.run([cli, "bundle", "validate", "-t", target, "--strict"], env=env,
                             capture_output=True, text=True, check=False)
        out = "\n".join(line for line in (res.stdout + res.stderr).splitlines() if "oidc_endpoint" not in line)
        print(f"== {target}\n{out}")
        failed |= res.returncode != 0
    server.shutdown()
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
