#!/usr/bin/env bash
set -Eeuo pipefail
trap 'echo "integration assertion failed at line $LINENO (${BASH_COMMAND%% *})" >&2' ERR

repo_dir="$(cd "$(dirname "$0")/.." && pwd)"
cert_dir="$(mktemp -d "$repo_dir/.integration.XXXXXX")"
cleanup() {
  docker compose -f "$repo_dir/compose.yaml" down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$cert_dir"
}
trap cleanup EXIT

validate_metrics() {
  local metrics_file="$cert_dir/metrics.txt"
  local colombo_metrics_file="$cert_dir/colombo-metrics.txt"
  curl -fsS -H 'Authorization: Bearer local-metrics-token' \
    http://127.0.0.1:18081/actuator/prometheus > "$metrics_file"
  for family in \
    colombo_build_identity \
    colombo_ftp_sessions_active \
    colombo_upload_queue_depth \
    colombo_upload_queue_active_threads \
    colombo_authentication_attempts_total \
    colombo_ftp_connection_events_total \
    colombo_upload_events_total \
    colombo_dependency_request_duration_seconds \
    colombo_retry_attempts_total \
    colombo_upload_spool_operations \
    colombo_upload_spool_oldest_age_seconds \
    colombo_upload_spool_oldest_wandering_age_seconds \
    colombo_upload_spool_outcomes_total; do
    grep -q "^# HELP $family" "$metrics_file"
  done
  grep -q 'colombo_build_identity{service="colombo",version="development"}' "$metrics_file"
  if [[ "${1:-true}" == true ]]; then
    grep -q 'source="http_upload"' "$metrics_file"
  fi
  grep -q 'queue="s3_upload"' "$metrics_file"
  grep -q 'queue="cms_callback"' "$metrics_file"
  ! grep '^colombo_' "$metrics_file" | grep -Eqi \
    '(device_id|username|assignment_id|filename|bucket|url|exception)="'
  grep -E '^(# (HELP|TYPE) colombo_|colombo_)' "$metrics_file" > "$colombo_metrics_file"
  docker run --rm --entrypoint /bin/promtool -i prom/prometheus:v3.5.0 \
    check metrics < "$colombo_metrics_file"
}

mock_control() {
  docker compose -f "$repo_dir/compose.yaml" exec -T mocks \
    python -c 'import sys, urllib.request; request = urllib.request.Request("http://127.0.0.1:18080/control", data=sys.argv[1].encode(), headers={"Content-Type": "application/json"}); urllib.request.urlopen(request).read()' "$1"
}

mock_state() {
  docker compose -f "$repo_dir/compose.yaml" exec -T mocks \
    wget -q -O - http://127.0.0.1:18080/state
}

rewind_record_and_restart() {
  local operation_id="$1" age_minutes="$2" container
  docker compose -f "$repo_dir/compose.yaml" stop colombo
  container="$(docker compose -f "$repo_dir/compose.yaml" ps -aq colombo)"
  docker cp "$container:/var/lib/colombo/spool/operations/$operation_id/record.json" "$cert_dir/record.json"
  python3 - "$cert_dir/record.json" "$age_minutes" <<'PY'
import datetime
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
record = json.loads(path.read_text())
if int(sys.argv[2]):
    record["accepted_at"] = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=int(sys.argv[2]))).isoformat()
record["next_attempt_at"] = "2000-01-01T00:00:00Z"
path.write_text(json.dumps(record))
PY
  docker cp "$cert_dir/record.json" "$container:/var/lib/colombo/spool/operations/$operation_id/record.json"
  if [[ $# -ge 3 ]]; then mock_control "$3"; fi
  docker compose -f "$repo_dir/compose.yaml" start colombo
  for _ in $(seq 1 80); do
    if curl -fsS http://127.0.0.1:18081/actuator/health >/dev/null; then return; fi
    sleep 0.25
  done
  echo "Colombo did not restart" >&2
  exit 1
}

openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj "/CN=localhost" \
  -keyout "$cert_dir/key.pem" -out "$cert_dir/cert.pem" >/dev/null 2>&1
export COLOMBO_TEST_CERT_DIR="$cert_dir"
export COLOMBO_FTPS_CERTIFICATE_PATH=/certs/cert.pem
export COLOMBO_FTPS_PRIVATE_KEY_PATH=/certs/key.pem
export COLOMBO_HTTP_HOST_PORT=18081
export COLOMBO_FTP_HOST_PORT=12121

docker compose -f "$repo_dir/compose.yaml" up --build --wait
docker compose -f "$repo_dir/compose.yaml" exec -T postgres psql -U colombo -d colombo -v ON_ERROR_STOP=1 -c \
  "INSERT INTO tenants (name, ftp_username, api_key, validation_endpoint, photo_endpoint) VALUES ('Test tenant', 'photographer', 'tenant-api-key', 'http://mocks:18080/validate', 'http://mocks:18080/photo')"
tenant_list="$(printf '1\n\n7\n' | docker compose -f "$repo_dir/compose.yaml" exec -T colombo tenants-cli)"
echo "$tenant_list" | grep -q photographer
tenant_view="$(printf '2\n1\n\n7\n' | docker compose -f "$repo_dir/compose.yaml" exec -T colombo tenants-cli)"
echo "$tenant_view" | grep -q '\[configured\]'
! echo "$tenant_view" | grep -q 'tenant-api-key'

test "$(curl -fsS http://127.0.0.1:18081/actuator/health)" = '{"status":"UP"}'
test "$(curl -sS -o /dev/null -w '%{http_code}' http://127.0.0.1:18081/)" = 302
test "$(curl -sS -o /dev/null -w '%{http_code}' http://127.0.0.1:18081/actuator/prometheus)" = 401
test "$(curl -sS -o /dev/null -w '%{http_code}' http://127.0.0.1:18081/private)" = 401
test "$(curl -sS -o /dev/null -w '%{http_code}' -X POST -H 'X-Colombo-Username: unknown' -H 'X-Colombo-Password: secret' -F file=@README.md http://127.0.0.1:18081/upload)" = 404
test "$(curl -sS -o /dev/null -w '%{http_code}' -X POST -H 'X-Colombo-Username: photographer' -H 'X-Colombo-Password: wrong' -F file=@README.md http://127.0.0.1:18081/upload)" = 401

mock_control '{"hold_s3": true}'
receipt="$(curl -fsS -X POST -H 'X-Colombo-Username: photographer' -H 'X-Colombo-Password: naming' -F file=@README.md http://127.0.0.1:18081/upload)"
operation_id="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["operation_id"])' <<<"$receipt")"
test "$(python3 -c 'import json,sys; value=json.load(sys.stdin); print(value["status"], value["assignment_id"])' <<<"$receipt")" = 'accepted assignment-123'
receipt_state="$(curl -fsS -H 'X-Colombo-Username: photographer' -H 'X-Colombo-Password: naming' "http://127.0.0.1:18081/uploads/$operation_id")"
python3 -c 'import json,sys; value=json.load(sys.stdin); assert value["state"] in ("accepted", "uploading"); assert value["content_length"] > 0; assert len(value["checksum_sha256"]) == 64' <<<"$receipt_state"

printf 'ftp-restart-body' > "$cert_dir/ftp-restart.txt"
curl --silent --show-error --user photographer:secret \
  -T "$cert_dir/ftp-restart.txt" ftp://127.0.0.1:12121/ftp-restart.txt

completed=0
for _ in $(seq 1 40); do
  state="$(mock_state)"
  if STATE_JSON="$state" python3 -c 'import json,os; value=json.loads(os.environ["STATE_JSON"]); raise SystemExit(0 if len(value["s3_requests"]) >= 2 else 1)'; then
    break
  fi
  sleep 0.25
done
STATE_JSON="$state" python3 -c 'import json,os; value=json.loads(os.environ["STATE_JSON"]); assert len(value["s3_requests"]) >= 2'
docker compose -f "$repo_dir/compose.yaml" kill -s SIGKILL colombo
state="$(mock_state)"
STATE_JSON="$state" python3 -c 'import json,os; value=json.loads(os.environ["STATE_JSON"]); assert value["objects"] == []; assert value["callbacks"] == []'
docker compose -f "$repo_dir/compose.yaml" run --rm --no-deps --entrypoint sh colombo -c \
  'test "$(find /var/lib/colombo/spool/operations -mindepth 1 -maxdepth 1 -type d | wc -l)" -ge 2 && test "$(find /var/lib/colombo/spool/operations -name record.json | wc -l)" -ge 2 && test "$(find /var/lib/colombo/spool/operations -name content | wc -l)" -ge 2 && test -z "$(find /var/lib/colombo/spool/ftp-incoming -type f -print -quit)"'

# Convert one accepted operation to the pre-change JSON shape while Colombo is
# stopped, then prove the existing recovery path delivers it after restart.
legacy_container="$(docker compose -f "$repo_dir/compose.yaml" ps -aq colombo)"
for filename in record.json private.json; do
  docker cp "$legacy_container:/var/lib/colombo/spool/operations/$operation_id/$filename" "$cert_dir/$filename"
done
python3 - "$cert_dir" <<'PY'
import json
import pathlib
import sys

directory = pathlib.Path(sys.argv[1])
record_path = directory / "record.json"
record = json.loads(record_path.read_text())
record.pop("device_id", None)
record.pop("wanderer_reason", None)
record_path.write_text(json.dumps(record))
private_path = directory / "private.json"
private = json.loads(private_path.read_text())
private["upload"].pop("credentialsEndpoint", None)
private["upload"].pop("wanderersEndpoint", None)
private_path.write_text(json.dumps(private))
PY
for filename in record.json private.json; do
  docker cp "$cert_dir/$filename" "$legacy_container:/var/lib/colombo/spool/operations/$operation_id/$filename"
done

mock_control '{"hold_s3": false, "s3_failures": 1, "callback_failures": 1}'
docker compose -f "$repo_dir/compose.yaml" up -d --wait colombo
for _ in $(seq 1 80); do
  receipt_state="$(curl -fsS -H 'X-Colombo-Username: photographer' -H 'X-Colombo-Password: naming' "http://127.0.0.1:18081/uploads/$operation_id")"
  if python3 -c 'import json,sys; raise SystemExit(0 if json.load(sys.stdin)["state"] == "callback-confirmed" else 1)' <<<"$receipt_state"; then
    break
  fi
  sleep 0.25
done
python3 -c 'import json,sys; value=json.load(sys.stdin); assert value["state"] == "callback-confirmed"; assert value["upload_attempts"] >= 1; assert value["callback_attempts"] >= 1' <<<"$receipt_state"
test "$(curl -sS -o /dev/null -w '%{http_code}' -H 'X-Colombo-Username: photographer' -H 'X-Colombo-Password: wrong' "http://127.0.0.1:18081/uploads/$operation_id")" = 401
test "$(curl -sS -o /dev/null -w '%{http_code}' -H 'X-Colombo-Username: photographer' -H 'X-Colombo-Password: other-assignment' "http://127.0.0.1:18081/uploads/$operation_id")" = 404
test "$(curl -sS -o /dev/null -w '%{http_code}' -H 'X-Colombo-Username: photographer' -H 'X-Colombo-Password: secret' "http://127.0.0.1:18081/uploads/00000000-0000-4000-8000-000000000000")" = 404
printf 'http-after-restart' > "$cert_dir/http-after-restart.txt"
curl -fsS -X POST -H 'X-Colombo-Username: photographer' -H 'X-Colombo-Password: secret' \
  -F file=@"$cert_dir/http-after-restart.txt" http://127.0.0.1:18081/upload >/dev/null

printf 'ftp-body' > "$cert_dir/ftp.txt"
curl --silent --show-error --ftp-ssl --insecure --user photographer:secret \
  -T "$cert_dir/ftp.txt" ftp://127.0.0.1:12121/ftp.txt
printf 'plain-body' > "$cert_dir/plain.txt"
curl --silent --show-error --user photographer:secret \
  -T "$cert_dir/plain.txt" ftp://127.0.0.1:12121/plain.txt

# A new PASV command must retire the prior data endpoint, matching v1 and
# preventing persistent camera clients from leaving orphaned data channels.
python3 "$repo_dir/tests/ftp_pasv_state.py" "$COLOMBO_FTP_HOST_PORT"

# Two live sessions for the same username must retain independent connection state.
printf 'concurrent-a' > "$cert_dir/concurrent-a.txt"
printf 'concurrent-b' > "$cert_dir/concurrent-b.txt"
curl --silent --show-error --user photographer:secret -T "$cert_dir/concurrent-a.txt" ftp://127.0.0.1:12121/concurrent-a.txt &
first_pid=$!
curl --silent --show-error --user photographer:secret -T "$cert_dir/concurrent-b.txt" ftp://127.0.0.1:12121/concurrent-b.txt &
second_pid=$!
wait "$first_pid" "$second_pid"

completed=0
for _ in $(seq 1 40); do
  state="$(mock_state)"
  if echo "$state" | grep -q 'assignment-123/demo/readme-0007.md' \
    && echo "$state" | grep -q 'assignment-123/http-after-restart.txt' \
    && echo "$state" | grep -q 'assignment-123/ftp.txt' \
    && echo "$state" | grep -q 'assignment-123/plain.txt' \
    && echo "$state" | grep -q 'assignment-123/camera-pasv-regression.jpg' \
    && echo "$state" | grep -q 'assignment-123/concurrent-a.txt' \
    && echo "$state" | grep -q 'assignment-123/concurrent-b.txt'; then
    echo "$state" | grep -q '"original_filename": "README.md"'
    echo "$state" | grep -q '"target_filename": "demo/readme-0007.md"'
    echo "$state" | grep -q '"original_filename": "ftp.txt"'
    STATE_JSON="$state" python3 -c 'import collections,json,os; value=json.loads(os.environ["STATE_JSON"]); successes=collections.Counter(value["objects"]); assert all(count == 1 for count in successes.values()); requests=collections.Counter(value["s3_requests"]); assert any(requests[path] > successes[path] for path in successes); attempts=collections.Counter(item["s3_url"] for item in value["callback_attempts"]); assert any(count > 1 for count in attempts.values())'
    validate_metrics
    completed=1
    break
  fi
  sleep 0.25
done

if [[ "$completed" != 1 ]]; then
  echo "background uploads did not complete" >&2
  docker compose -f "$repo_dir/compose.yaml" logs colombo >&2
  exit 1
fi

# Expired S3 credentials use historical refresh without sending the validation key.
mock_control '{"s3_expired":1}'
printf 'refresh-body' > "$cert_dir/refreshed.jpg"
refresh_receipt="$(curl -fsS -X POST -H 'X-Colombo-Username: photographer' -H 'X-Colombo-Password: late' -F file=@"$cert_dir/refreshed.jpg" http://127.0.0.1:18081/upload)"
refresh_id="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["operation_id"])' <<<"$refresh_receipt")"
for _ in $(seq 1 80); do
  refresh_state="$(curl -fsS -H 'X-Colombo-Username: photographer' -H 'X-Colombo-Password: late' "http://127.0.0.1:18081/uploads/$refresh_id")"
  if python3 -c 'import json,sys; raise SystemExit(json.load(sys.stdin)["state"] != "callback-confirmed")' <<<"$refresh_state"; then break; fi
  sleep 0.25
done
python3 -c 'import json,sys; assert json.load(sys.stdin)["state"] == "callback-confirmed"' <<<"$refresh_state"
state="$(mock_state)"
STATE_JSON="$state" python3 -c 'import json,os; s=json.loads(os.environ["STATE_JSON"]); assert any(r.get("assignment_id") == "assignment-123" and r.get("accepted_at") and "key" not in r for r in s["credentials_requests"]); assert any(c.get("device_id") == "17" and c.get("accepted_at") for c in s["callbacks"]); assert any("device_id" not in c for c in s["callbacks"] if c.get("original_filename") == "README.md")'

# Callback denial after S3 delivery must register the existing object only once.
mock_control '{"callback_reason":"assignment_inactive_at_acceptance"}'
printf 'held-after-callback' > "$cert_dir/held-callback.jpg"
callback_receipt="$(curl -fsS -X POST -H 'X-Colombo-Username: photographer' -H 'X-Colombo-Password: late' -F file=@"$cert_dir/held-callback.jpg" http://127.0.0.1:18081/upload)"
callback_id="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["operation_id"])' <<<"$callback_receipt")"
for _ in $(seq 1 80); do
  state="$(curl -fsS -H 'X-Colombo-Username: photographer' -H 'X-Colombo-Password: late' "http://127.0.0.1:18081/uploads/$callback_id")"
  if python3 -c 'import json,sys; raise SystemExit(json.load(sys.stdin)["state"] != "held")' <<<"$state"; then break; fi
  sleep 0.25
done
python3 -c 'import json,sys; d=json.load(sys.stdin); assert d["state"] == "held"; assert not any(k in d for k in ("device_id","s3_url","object_bucket","object_key"))' <<<"$state"
state="$(mock_state)"
STATE_JSON="$state" CALLBACK_ID="$callback_id" python3 -c 'import json,os; s=json.loads(os.environ["STATE_JSON"]); w=s["wanderers"][os.environ["CALLBACK_ID"]]; assert w["reason"] == "assignment_inactive_at_acceptance" and w["s3_url"].startswith("s3://"); assert not any("wanderers/" in path and os.environ["CALLBACK_ID"] in path for path in s["objects"]); assert any(c.get("device_id") == "17" for c in s["callbacks"] + s["callback_attempts"])'

# Sequence denial occurs before any assignment S3 PUT, then bytes go to holding.
mock_control '{"callback_reason":null,"sequence_reason":"assignment_missing"}'
printf 'held-before-put' > "$cert_dir/held-sequence.jpg"
sequence_receipt="$(curl -fsS -X POST -H 'X-Colombo-Username: photographer' -H 'X-Colombo-Password: late-naming' -F file=@"$cert_dir/held-sequence.jpg" http://127.0.0.1:18081/upload)"
sequence_id="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["operation_id"])' <<<"$sequence_receipt")"
for _ in $(seq 1 80); do
  state="$(curl -fsS -H 'X-Colombo-Username: photographer' -H 'X-Colombo-Password: late-naming' "http://127.0.0.1:18081/uploads/$sequence_id")"
  if python3 -c 'import json,sys; raise SystemExit(json.load(sys.stdin)["state"] != "held")' <<<"$state"; then break; fi
  sleep 0.25
done
python3 -c 'import json,sys; assert json.load(sys.stdin)["state"] == "held"' <<<"$state"
state="$(mock_state)"
STATE_JSON="$state" SEQUENCE_ID="$sequence_id" python3 -c 'import json,os; s=json.loads(os.environ["STATE_JSON"]); i=os.environ["SEQUENCE_ID"]; assert s["wanderers"][i]["reason"] == "assignment_missing"; assert any("wanderers/"+i+"/held-sequence.jpg" in p for p in s["objects"]); assert any("wanderers/"+i+"/held-sequence.jpg" in d["s3_url"] for d in s["wanderer_deliveries"]); assert any(r.get("accepted_at") for r in s["sequence_requests"])'
validate_metrics

# The persisted acceptance time governs retries across a restart. This fixture
# advances the clock represented in the record without waiting half an hour.
mock_control '{"sequence_reason":null,"cms_unavailable":true}'
printf 'outage-recovery' > "$cert_dir/outage.jpg"
outage_receipt="$(curl -fsS -X POST -H 'X-Colombo-Username: photographer' -H 'X-Colombo-Password: late-naming' -F file=@"$cert_dir/outage.jpg" http://127.0.0.1:18081/upload)"
outage_id="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["operation_id"])' <<<"$outage_receipt")"
for _ in $(seq 1 80); do
  state="$(mock_state)"
  if STATE_JSON="$state" python3 -c 'import json,os; raise SystemExit("/sequence" not in json.loads(os.environ["STATE_JSON"])["outage_requests"])'; then break; fi
  sleep 0.25
done
for _ in $(seq 1 80); do
  outage_state="$(curl -fsS -H 'X-Colombo-Username: photographer' -H 'X-Colombo-Password: late-naming' "http://127.0.0.1:18081/uploads/$outage_id")"
  if python3 -c 'import json,sys; d=json.load(sys.stdin); raise SystemExit(d["upload_attempts"] != 0 or d["state"] != "accepted")' <<<"$outage_state"; then break; fi
  sleep 0.25
done
python3 -c 'import json,sys; d=json.load(sys.stdin); assert d["upload_attempts"] == 0 and d["state"] == "accepted"' <<<"$outage_state"
rewind_record_and_restart "$outage_id" 30 '{"cms_unavailable":false}'
for _ in $(seq 1 80); do
  outage_state="$(curl -fsS -H 'X-Colombo-Username: photographer' -H 'X-Colombo-Password: late-naming' "http://127.0.0.1:18081/uploads/$outage_id")"
  if python3 -c 'import json,sys; raise SystemExit(json.load(sys.stdin)["state"] != "callback-confirmed")' <<<"$outage_state"; then break; fi
  sleep 0.25
done
python3 -c 'import json,sys; d=json.load(sys.stdin); assert d["state"] == "callback-confirmed" and d["upload_attempts"] == 1' <<<"$outage_state"

# Past the 48-hour deadline, the accepted bytes become a wanderer.
mock_control '{"cms_unavailable":true}'
printf 'deadline-recovery' > "$cert_dir/deadline.jpg"
deadline_receipt="$(curl -fsS -X POST -H 'X-Colombo-Username: photographer' -H 'X-Colombo-Password: late-naming' -F file=@"$cert_dir/deadline.jpg" http://127.0.0.1:18081/upload)"
deadline_id="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["operation_id"])' <<<"$deadline_receipt")"
rewind_record_and_restart "$deadline_id" 2940 '{"cms_unavailable":false}'
for _ in $(seq 1 80); do
  deadline_state="$(curl -fsS -H 'X-Colombo-Username: photographer' -H 'X-Colombo-Password: late-naming' "http://127.0.0.1:18081/uploads/$deadline_id")"
  if python3 -c 'import json,sys; raise SystemExit(json.load(sys.stdin)["state"] != "held")' <<<"$deadline_state"; then break; fi
  sleep 0.25
done
python3 -c 'import json,sys; assert json.load(sys.stdin)["state"] == "held"' <<<"$deadline_state"
state="$(mock_state)"
STATE_JSON="$state" DEADLINE_ID="$deadline_id" python3 -c 'import json,os; s=json.loads(os.environ["STATE_JSON"]); assert s["wanderers"][os.environ["DEADLINE_ID"]]["reason"] == "deadline_exceeded"'

# Ambiguous registration can be retried after restart with one CMS row.
mock_control '{"sequence_reason":"assignment_missing","wanderers_ambiguous":true}'
printf 'restart-wandering' > "$cert_dir/restart-wandering.jpg"
restart_receipt="$(curl -fsS -X POST -H 'X-Colombo-Username: photographer' -H 'X-Colombo-Password: late-naming' -F file=@"$cert_dir/restart-wandering.jpg" http://127.0.0.1:18081/upload)"
restart_id="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["operation_id"])' <<<"$restart_receipt")"
for _ in $(seq 1 80); do
  state="$(mock_state)"
  if STATE_JSON="$state" RESTART_ID="$restart_id" python3 -c 'import json,os; s=json.loads(os.environ["STATE_JSON"]); raise SystemExit(os.environ["RESTART_ID"] not in s["wanderers"])'; then break; fi
  sleep 0.25
done
restart_state="$(curl -fsS -H 'X-Colombo-Username: photographer' -H 'X-Colombo-Password: late-naming' "http://127.0.0.1:18081/uploads/$restart_id")"
python3 -c 'import json,sys; assert json.load(sys.stdin)["state"] == "wandering"' <<<"$restart_state"
rewind_record_and_restart "$restart_id" 0 '{"wanderers_ambiguous":false,"sequence_reason":null}'
for _ in $(seq 1 80); do
  restart_state="$(curl -fsS -H 'X-Colombo-Username: photographer' -H 'X-Colombo-Password: late-naming' "http://127.0.0.1:18081/uploads/$restart_id")"
  if python3 -c 'import json,sys; raise SystemExit(json.load(sys.stdin)["state"] != "held")' <<<"$restart_state"; then break; fi
  sleep 0.25
done
python3 -c 'import json,sys; assert json.load(sys.stdin)["state"] == "held"' <<<"$restart_state"
state="$(mock_state)"
STATE_JSON="$state" RESTART_ID="$restart_id" python3 -c 'import json,os; s=json.loads(os.environ["STATE_JSON"]); i=os.environ["RESTART_ID"]; assert i in s["wanderers"]; assert s["wanderer_registration_attempts"].count(i) >= 2'
validate_metrics false
