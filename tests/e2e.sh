#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "$0")/.." && pwd)"
cert_dir="$(mktemp -d)"
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
    colombo_retry_attempts_total; do
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

receipt="$(curl -fsS -X POST -H 'X-Colombo-Username: photographer' -H 'X-Colombo-Password: naming' -F file=@README.md http://127.0.0.1:18081/upload)"
test "$receipt" = '{"status":"accepted","assignment_id":"assignment-123"}'

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
  state="$(docker compose -f "$repo_dir/compose.yaml" exec -T mocks wget -q -O - http://127.0.0.1:18080/state)"
  if echo "$state" | grep -q 'assignment-123/demo/readme-0007.md' \
    && echo "$state" | grep -q 'assignment-123/ftp.txt' \
    && echo "$state" | grep -q 'assignment-123/plain.txt' \
    && echo "$state" | grep -q 'assignment-123/camera-pasv-regression.jpg' \
    && echo "$state" | grep -q 'assignment-123/concurrent-a.txt' \
    && echo "$state" | grep -q 'assignment-123/concurrent-b.txt'; then
    echo "$state" | grep -q '"original_filename": "README.md"'
    echo "$state" | grep -q '"target_filename": "demo/readme-0007.md"'
    echo "$state" | grep -q '"original_filename": "ftp.txt"'
    validate_metrics
    exit 0
  fi
  sleep 0.25
done

echo "background uploads did not complete" >&2
docker compose -f "$repo_dir/compose.yaml" logs colombo >&2
exit 1
