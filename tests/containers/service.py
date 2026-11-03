import hashlib
import json
import random
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

TOKEN = "Bearer test-token"
ADDRESS = ("127.0.0.1", 8080)
EVENT_COUNT = 5
EVENT_GAP_SECONDS = 0.5
JSON = "application/json"


def deterministic_bytes(size: int) -> bytes:
    return random.Random(size).randbytes(size)


class ServiceHandler(BaseHTTPRequestHandler):
    def _reply(self, status: int, body: bytes, headers: dict[str, str]) -> None:
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        for name, value in headers.items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(body)

    def _is_authorized(self) -> bool:
        if self.headers.get("Authorization") == TOKEN:
            return True
        self._reply(401, b'{"error":"no bearer token"}', {"Content-Type": JSON})
        return False

    def _read_body(self) -> bytes:
        length = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(length)

    def do_GET(self) -> None:
        if not self._is_authorized():
            return
        if not self.path.startswith("/big?bytes="):
            self._reply(404, b"{}", {"Content-Type": JSON})
            return
        size = int(self.path.split("=", 1)[1])
        data = deterministic_bytes(size)
        headers = {
            "Content-Type": "application/octet-stream",
            "X-Sha256": hashlib.sha256(data).hexdigest(),
        }
        self._reply(200, data, headers)

    def do_POST(self) -> None:
        if not self._is_authorized():
            return
        body = self._read_body()
        if self.path == "/upload":
            answer = {"bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}
            self._reply(200, json.dumps(answer).encode(), {"Content-Type": JSON})
            return
        if self.path != "/mcp":
            self._reply(404, b"{}", {"Content-Type": JSON})
            return
        request = json.loads(body)
        if "text/event-stream" not in self.headers.get("Accept", ""):
            result = {"echo": request.get("params")}
            answer = {"jsonrpc": "2.0", "id": request.get("id"), "result": result}
            self._reply(200, json.dumps(answer).encode(), {"Content-Type": JSON})
            return
        self._stream_events(request.get("id"))

    def _stream_events(self, request_id: object) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        for seq in range(EVENT_COUNT):
            event = {"jsonrpc": "2.0", "id": request_id, "seq": seq}
            self.wfile.write(f"event: message\ndata: {json.dumps(event)}\n\n".encode())
            self.wfile.flush()
            time.sleep(EVENT_GAP_SECONDS)


if __name__ == "__main__":
    ThreadingHTTPServer(ADDRESS, ServiceHandler).serve_forever()
