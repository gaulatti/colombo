#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "$0")/.." && pwd)"
cert_dir="$(mktemp -d "$repo_dir/.integration-certs.XXXXXX")"
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
    colombo_upload_spool_outcomes_total; do
    grep -q "^# HELP $family" "$metrics_file"
  done
  grep -q 'colombo_build_identity{service="colombo",version="development"}' "$metrics_file"
  grep -q 'source="http_upload"' "$metrics_file"
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

validation_count() {
  mock_state | python3 -c 'import json,sys; print(json.load(sys.stdin)["validation_attempts"])'
}

verify_login_throttle() {
  local before after status
  before="$(validation_count)"
  for _ in $(seq 1 12); do
    test "$(curl -sS -o /dev/null -w '%{http_code}' -H 'X-Colombo-Username: photographer' -H 'X-Colombo-Password: wrong' "http://127.0.0.1:18081/uploads/$operation_id")" = 401
  done
  after="$(validation_count)"
  test "$((after - before))" = 12

  docker compose -f "$repo_dir/compose.yaml" exec -T postgres psql -U colombo -d colombo -v ON_ERROR_STOP=1 -c \
    'UPDATE tenants SET login_failures_per_minute = 10 WHERE ftp_username = '\''photographer'\'';'
  before="$(validation_count)"
  for _ in $(seq 1 10); do
    test "$(curl -sS -o /dev/null -w '%{http_code}' -H 'X-Colombo-Username: photographer' -H 'X-Colombo-Password: wrong' "http://127.0.0.1:18081/uploads/$operation_id")" = 401
  done
  status="$(curl -sS -D "$cert_dir/throttle-headers" -o /dev/null -w '%{http_code}' -H 'X-Colombo-Username: photographer' -H 'X-Colombo-Password: wrong' "http://127.0.0.1:18081/uploads/$operation_id")"
  test "$status" = 429
  grep -qi '^retry-after: [1-9][0-9]*' "$cert_dir/throttle-headers"
  after="$(validation_count)"
  test "$((after - before))" = 10
  curl -fsS -H 'Authorization: Bearer local-metrics-token' http://127.0.0.1:18081/actuator/prometheus | grep -q 'colombo_authentication_attempts_total{result="throttled",source="http_status"} 1'

  # Removing the optional setting disables throttling immediately.
  docker compose -f "$repo_dir/compose.yaml" exec -T postgres psql -U colombo -d colombo -v ON_ERROR_STOP=1 -c \
    'UPDATE tenants SET login_failures_per_minute = NULL WHERE ftp_username = '\''photographer'\'';'
  test "$(curl -sS -o /dev/null -w '%{http_code}' -H 'X-Colombo-Username: photographer' -H 'X-Colombo-Password: wrong' "http://127.0.0.1:18081/uploads/$operation_id")" = 401
  test "$(curl -sS -o /dev/null -w '%{http_code}' -H 'X-Colombo-Username: photographer' -H 'X-Colombo-Password: secret' "http://127.0.0.1:18081/uploads/$operation_id")" = 200

  docker compose -f "$repo_dir/compose.yaml" exec -T postgres psql -U colombo -d colombo -v ON_ERROR_STOP=1 -c \
    'UPDATE tenants SET login_failures_per_minute = 10 WHERE ftp_username = '\''photographer'\'';'
  for _ in $(seq 1 5); do
    test "$(curl -sS -o /dev/null -w '%{http_code}' -H 'X-Colombo-Username: photographer' -H 'X-Colombo-Password: wrong' "http://127.0.0.1:18081/uploads/$operation_id")" = 401
  done
  test "$(curl -sS -o /dev/null -w '%{http_code}' -H 'X-Colombo-Username: photographer' -H 'X-Colombo-Password: secret' "http://127.0.0.1:18081/uploads/$operation_id")" = 200
  for _ in $(seq 1 6); do
    test "$(curl -sS -o /dev/null -w '%{http_code}' -H 'X-Colombo-Username: photographer' -H 'X-Colombo-Password: wrong' "http://127.0.0.1:18081/uploads/$operation_id")" = 401
  done
  test "$(curl -sS -o /dev/null -w '%{http_code}' -H 'X-Colombo-Username: photographer' -H 'X-Colombo-Password: secret' "http://127.0.0.1:18081/uploads/$operation_id")" = 200

  docker compose -f "$repo_dir/compose.yaml" exec -T postgres psql -U colombo -d colombo -v ON_ERROR_STOP=1 -c \
    'UPDATE tenants SET login_failures_per_minute = 1 WHERE ftp_username = '\''photographer'\'';'
  before="$(validation_count)"
  python3 - <<'PY'
from ftplib import FTP, error_perm
for _ in range(2):
    with FTP() as ftp:
        ftp.connect('127.0.0.1', 12121)
        try:
            ftp.login('photographer', 'wrong')
        except error_perm as exc:
            assert str(exc).startswith('530')
        else:
            raise AssertionError('invalid FTP login succeeded')
PY
  after="$(validation_count)"
  test "$((after - before))" = 1
  curl -fsS -H 'Authorization: Bearer local-metrics-token' http://127.0.0.1:18081/actuator/prometheus | grep -q 'colombo_authentication_attempts_total{result="throttled",source="ftp"} 1'
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
echo "$tenant_view" | grep -q 'login_failures_per_minute'
tenant_update="$(printf '4\n1\n\n\n\n\n10\n\n7\n' | docker compose -f "$repo_dir/compose.yaml" exec -T colombo tenants-cli)"
echo "$tenant_update" | grep -q 'Tenant updated.'
test "$(docker compose -f "$repo_dir/compose.yaml" exec -T postgres psql -U colombo -d colombo -t -A -c "SELECT login_failures_per_minute FROM tenants WHERE id = 1")" = 10
tenant_view="$(printf '2\n1\n\n7\n' | docker compose -f "$repo_dir/compose.yaml" exec -T colombo tenants-cli)"
echo "$tenant_view" | grep -q '10'
tenant_update="$(printf '4\n1\n\n\n\n\noff\n\n7\n' | docker compose -f "$repo_dir/compose.yaml" exec -T colombo tenants-cli)"
echo "$tenant_update" | grep -q 'Tenant updated.'
test "$(docker compose -f "$repo_dir/compose.yaml" exec -T postgres psql -U colombo -d colombo -t -A -c "SELECT login_failures_per_minute IS NULL FROM tenants WHERE id = 1")" = t

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
    verify_login_throttle
    exit 0
  fi
  sleep 0.25
done

echo "background uploads did not complete" >&2
docker compose -f "$repo_dir/compose.yaml" logs colombo >&2
exit 1
