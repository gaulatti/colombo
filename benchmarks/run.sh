#!/usr/bin/env bash
set -euo pipefail

java_image="${JAVA_IMAGE:-colombo-java:bench}"
rust_image="${RUST_IMAGE:-colombo-rust:bench}"
network="colombo-benchmark"
database="colombo-benchmark-postgres"

cleanup() {
  docker rm -f colombo-benchmark-app "$database" >/dev/null 2>&1 || true
  docker network rm "$network" >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup
docker network create "$network" >/dev/null
docker run -d --name "$database" --network "$network" \
  -e POSTGRES_DB=colombo -e POSTGRES_USER=colombo -e POSTGRES_PASSWORD=colombo \
  postgres:17-alpine >/dev/null
until docker exec "$database" pg_isready -U colombo -d colombo >/dev/null 2>&1; do sleep 0.2; done
# The official image briefly accepts connections on its initialization server before restarting it.
sleep 1
until docker exec "$database" psql -U colombo -d colombo -c 'SELECT 1' >/dev/null 2>&1; do sleep 0.2; done

printf 'runtime,image_bytes,startup_ms,memory,idle_cpu_percent,root_requests_per_second,ftp_connections_per_second\n'
for runtime in java rust; do
  if [ "$runtime" = java ]; then image="$java_image"; else image="$rust_image"; fi
  docker exec "$database" psql -U colombo -d colombo -v ON_ERROR_STOP=1 -c \
    'DROP SCHEMA public CASCADE; CREATE SCHEMA public;' >/dev/null
  started="$(python3 -c 'import time; print(time.time_ns())')"
  docker run -d --name colombo-benchmark-app --network "$network" --cpus 1 --memory 512m \
    -p 18082:8080 -p 12122:2121 \
    -e PORT=8080 -e COLOMBO_FTP_PORT=2121 -e COLOMBO_FTP_PASSIVE_PORTS=60000-60010 \
    -e COLOMBO_FTP_PASSIVE_EXTERNAL_ADDRESS=127.0.0.1 \
    -e DATABASE_URL=jdbc:postgresql://colombo-benchmark-postgres:5432/colombo \
    -e DATABASE_USER=colombo -e DATABASE_PASSWORD=colombo -e FLYWAY_ENABLED=true \
    -e COLOMBO_METRICS_TOKEN=benchmark-metrics-token \
    "$image" >/dev/null
  ready=false
  for _ in $(seq 1 1200); do
    if curl -fsS http://127.0.0.1:18082/actuator/health >/dev/null 2>&1; then
      ready=true
      break
    fi
    if [ "$(docker inspect -f '{{.State.Running}}' colombo-benchmark-app)" != true ]; then
      docker logs colombo-benchmark-app >&2
      exit 1
    fi
    sleep 0.1
  done
  if [ "$ready" != true ]; then
    echo "$runtime did not become healthy within 120 seconds" >&2
    docker logs colombo-benchmark-app >&2
    exit 1
  fi
  finished="$(python3 -c 'import time; print(time.time_ns())')"
  startup_ms="$(((finished - started) / 1000000))"
  sleep 5
  stats="$(docker stats --no-stream --format '{{.MemUsage}}|{{.CPUPerc}}' colombo-benchmark-app)"
  memory="$(printf '%s' "$stats" | awk -F'|' '{print $1}' | awk '{print $1}')"
  idle_cpu="$(printf '%s' "$stats" | awk -F'|' '{print $2}' | tr -d '%')"
  root_rps="$(ab -q -n 5000 -c 50 http://127.0.0.1:18082/ 2>/dev/null | awk -F: '/Requests per second/{gsub(/^ +| +$/,"",$2); split($2,a," "); print a[1]}')"
  ftp_rps="$(python3 benchmarks/ftp_churn.py 127.0.0.1 12122 1000 50)"
  image_bytes="$(docker image inspect "$image" --format '{{.Size}}')"
  printf '%s,%s,%s,%s,%s,%s,%s\n' "$runtime" "$image_bytes" "$startup_ms" "$memory" "$idle_cpu" "$root_rps" "$ftp_rps"
  docker rm -f colombo-benchmark-app >/dev/null
done
