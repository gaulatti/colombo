import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

STATE = {"callbacks": [], "objects": []}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, _format, *_args):
        return

    def _json(self, status, body):
        encoded = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _body(self):
        return self.rfile.read(int(self.headers.get("Content-Length", "0")))

    def do_GET(self):
        if self.path == "/health":
            return self._json(200, {"status": "UP"})
        if self.path == "/state":
            return self._json(200, STATE)
        self._json(404, {})

    def do_POST(self):
        body = json.loads(self._body() or b"{}")
        if self.path == "/validate":
            if self.headers.get("X-Colombo-API-Key") != "tenant-api-key" or body.get("key") not in ("secret", "naming"):
                return self._json(401, {})
            upload = {
                "accessKeyId": "test-access",
                "secretAccessKey": "test-secret",
                "sessionToken": "test-session",
                "region": "us-east-1",
                "bucket": "uploads",
                "keyPrefix": "assignment-123",
                "expiresAt": "2099-01-01T00:00:00Z",
            }
            if body["key"] == "naming":
                upload.update({
                    "sequenceEndpoint": "/sequence",
                    "namingPolicy": {
                        "version": 1,
                        "assignmentSlug": "demo",
                        "path": [{"type": "placeholder", "name": "assignmentSlug"}],
                        "filename": [
                            {"type": "placeholder", "name": "originalStem"},
                            {"type": "literal", "value": "-"},
                            {"type": "placeholder", "name": "sequence", "width": 4},
                            {"type": "literal", "value": "."},
                            {"type": "placeholder", "name": "originalExtension"},
                        ],
                        "timezone": "UTC",
                        "captureTimeFallback": "uploadedTime",
                        "case": "lowercase",
                    },
                })
            return self._json(200, {"assignmentId": "assignment-123", "upload": upload})
        if self.path == "/sequence":
            return self._json(200, {"sequence": 7})
        if self.path == "/photo":
            STATE["callbacks"].append(body)
            return self._json(204, {})
        self._json(404, {})

    def do_PUT(self):
        self._body()
        STATE["objects"].append(self.path)
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()


def serve(port):
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()


for selected_port in (18080, 19000):
    threading.Thread(target=serve, args=(selected_port,), daemon=True).start()
threading.Event().wait()
