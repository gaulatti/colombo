import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

STATE = {
    "callback_attempts": [],
    "callbacks": [],
    "hold_s3": False,
    "s3_failures": 0,
    "s3_expired": 0,
    "s3_requests": [],
    "objects": [],
    "callback_failures": 0,
    "cms_unavailable": False,
    "wanderers_unavailable": False,
    "wanderers_ambiguous": False,
    "outage_requests": [],
    "wanderer_registration_attempts": [],
    "sequence_reason": None,
    "callback_reason": None,
    "credentials_requests": [],
    "sequence_requests": [],
    "wanderers": {},
    "wanderer_deliveries": [],
}
STATE_CHANGED = threading.Condition()


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
            with STATE_CHANGED:
                return self._json(200, dict(STATE))
        self._json(404, {})

    def do_POST(self):
        body = json.loads(self._body() or b"{}")
        if self.path == "/control":
            with STATE_CHANGED:
                for key in ("hold_s3", "s3_failures", "s3_expired", "callback_failures", "cms_unavailable", "wanderers_unavailable", "wanderers_ambiguous", "sequence_reason", "callback_reason"):
                    if key in body:
                        STATE[key] = body[key]
                STATE_CHANGED.notify_all()
            return self._json(200, {"status": "updated"})
        if STATE["cms_unavailable"] and self.path in ("/credentials", "/sequence", "/photo", "/wanderers"):
            STATE["outage_requests"].append(self.path)
            return self._json(503, {})
        if STATE["wanderers_unavailable"] and self.path.startswith("/wanderers"):
            return self._json(503, {})
        if self.path == "/validate":
            if self.headers.get("X-Colombo-API-Key") != "tenant-api-key" or body.get(
                "key"
            ) not in ("secret", "naming", "other-assignment", "late", "late-naming"):
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
            if body["key"] in ("late", "late-naming"):
                upload.update({"credentialsEndpoint": "/credentials", "wanderersEndpoint": "/wanderers"})
            if body["key"] in ("naming", "late-naming"):
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
            assignment_id = (
                "assignment-other"
                if body["key"] == "other-assignment"
                else "assignment-123"
            )
            response = {"assignmentId": assignment_id, "upload": upload}
            if body["key"] in ("late", "late-naming"):
                response["deviceId"] = "17"
            return self._json(200, response)
        if self.path == "/credentials":
            with STATE_CHANGED:
                STATE["credentials_requests"].append(body)
            return self._json(200, {"upload": {
                "accessKeyId": "test-access", "secretAccessKey": "test-secret",
                "sessionToken": "test-session", "region": "us-east-1", "bucket": "uploads",
                "keyPrefix": "assignment-123", "expiresAt": "2099-01-01T00:00:00Z",
                "credentialsEndpoint": "/credentials", "wanderersEndpoint": "/wanderers",
            }})
        if self.path == "/sequence":
            with STATE_CHANGED:
                STATE["sequence_requests"].append(body)
                reason = STATE["sequence_reason"]
            if reason:
                return self._json(400, {"reason": reason})
            return self._json(200, {"sequence": 7})
        if self.path == "/photo":
            with STATE_CHANGED:
                STATE["callback_attempts"].append(body)
                if STATE["callback_reason"]:
                    return self._json(400, {"reason": STATE["callback_reason"]})
                if STATE["callback_failures"] > 0:
                    STATE["callback_failures"] -= 1
                    return self._json(503, {})
                STATE["callbacks"].append(body)
            return self._json(204, {})
        if self.path == "/wanderers":
            operation_id = body["operation_id"]
            with STATE_CHANGED:
                STATE["wanderer_registration_attempts"].append(operation_id)
                if operation_id not in STATE["wanderers"]:
                    STATE["wanderers"][operation_id] = body
                registered = STATE["wanderers"][operation_id]
            if STATE["wanderers_ambiguous"]:
                return self._json(503, {})
            if registered.get("s3_url"):
                return self._json(200, {"status": "pending_review"})
            return self._json(200, {"status": "registered", "upload": {
                "accessKeyId": "test-access", "secretAccessKey": "test-secret",
                "sessionToken": "test-session", "region": "us-east-1", "bucket": "uploads",
                "keyPrefix": f"wanderers/{operation_id}/", "expiresAt": "2099-01-01T00:00:00Z",
            }})
        if self.path.startswith("/wanderers/") and self.path.endswith("/delivered"):
            with STATE_CHANGED:
                STATE["wanderer_deliveries"].append(body)
            return self._json(200, {"status": "pending_review"})
        self._json(404, {})

    def do_PUT(self):
        self._body()
        with STATE_CHANGED:
            STATE["s3_requests"].append(self.path)
            while STATE["hold_s3"]:
                STATE_CHANGED.wait()
            if STATE["s3_expired"] > 0:
                STATE["s3_expired"] -= 1
                response = b"<Error><Code>ExpiredToken</Code></Error>"
                self.send_response(403)
                self.send_header("Content-Type", "application/xml")
                self.send_header("Content-Length", str(len(response)))
                self.end_headers()
                self.wfile.write(response)
                return
            if STATE["s3_failures"] > 0:
                STATE["s3_failures"] -= 1
                self.send_response(503)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            if self.path not in STATE["objects"]:
                STATE["objects"].append(self.path)
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()


def serve(port):
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()


for selected_port in (18080, 19000):
    threading.Thread(target=serve, args=(selected_port,), daemon=True).start()
threading.Event().wait()
