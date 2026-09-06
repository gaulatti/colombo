# Colombo

[![codecov](https://codecov.io/gh/gaulatti/colombo/branch/main/graph/badge.svg)](https://codecov.io/gh/gaulatti/colombo)

Colombo is a single-process Rust upload gateway. It accepts FTP/explicit FTPS and HTTP uploads, validates tenant credentials with the tenant CMS, delivers accepted files to Amazon S3, and notifies the CMS asynchronously.

The previous Java implementation is preserved on the `archive/v1-java` branch.

## Runtime contract

1. Resolve the FTP username in PostgreSQL.
2. POST the supplied credential to that tenant's validation endpoint.
3. Receive an assignment ID and short-lived S3 credentials.
4. Hash and fsync the file and private operation context into an atomic durable spool entry.
5. Return FTP `226` or HTTP `202` only after that entry is recoverable after a restart.
6. Upload to a stable S3 key and persist delivery before independently retrying the CMS callback.

Eight fixed workers recover accepted or in-flight entries at startup. Per-operation file locks prevent concurrent processes from executing the same entry. S3 and callback attempts each have a five-attempt retry budget; the persisted S3 key and callback payload remain stable across attempts. Each FTP connection still has isolated session credentials, including when one username is connected concurrently.

## Local start

Rust 1.94.1 or newer and PostgreSQL are required for host development. Docker is the reproducible integration path.

```bash
cp .env.example .env
cargo run
```

Or start the complete disposable local stack:

```bash
docker compose up --build --wait
docker compose exec colombo tenants-cli
```

The default ports are HTTP `8080`, FTP/explicit FTPS `2121`, and passive FTP `60000-60010` in Compose. PostgreSQL and mock CMS/S3 dependencies are local-only; the deployable application remains one Colombo container.

Passive data ports are prebound by the Colombo process. When an FTP client sends a new `PASV` command, the previously reserved endpoint is retired before the replacement is issued. This preserves the v1 behavior required by persistent camera clients and prevents stale data channels during upload bursts.

## HTTP upload

```bash
curl -X POST http://localhost:8080/upload \
  -H 'X-Colombo-Username: photographer' \
  -H 'X-Colombo-Password: cms-credential' \
  -F file=@photo.jpg
```

Successful local acceptance returns:

```json
{"status":"accepted","assignment_id":"...","operation_id":"..."}
```

The same operation ID is written to Colombo's structured log before an FTP upload receives `226`. The multipart request limit is 100 MiB. The FTP-only master-password support bypass is never accepted by this endpoint.

Use the same tenant headers to read the durable receipt:

```bash
curl http://localhost:8080/uploads/OPERATION_ID \
  -H 'X-Colombo-Username: photographer' \
  -H 'X-Colombo-Password: cms-credential'
```

Receipt states are `accepted`, `uploading`, `delivered`, `callback-confirmed`, `failed`, and `expired`. Receipts include the assignment, protocol, byte count, SHA-256 checksum, timestamps, and bounded attempt/failure fields; they never include local paths, credentials, bucket names, object keys, or raw errors. Unknown or tenant/assignment-mismatched IDs return `404`, bad credentials return `401`, and retained expired receipts return `410`.

## FTPS

Plain FTP remains available. To additionally enable explicit FTPS on the same control port, mount a PEM certificate chain and private key read-only and set:

```dotenv
COLOMBO_FTPS_CERTIFICATE_PATH=/etc/letsencrypt/live/colombo.gaulatti.com/fullchain.pem
COLOMBO_FTPS_PRIVATE_KEY_PATH=/etc/letsencrypt/live/colombo.gaulatti.com/privkey.pem
```

For `colombo.gaulatti.com`, obtain and renew the certificate on the host with Let's Encrypt DNS-01, then mount `/etc/letsencrypt` read-only into the container. Setting only one path fails startup.

## Operations

- `GET /actuator/health` is public and checks PostgreSQL.
- `GET /actuator/prometheus` requires `Authorization: Bearer $COLOMBO_METRICS_TOKEN`; without a configured token it remains inaccessible.
- A configured `COLOMBO_METRICS_TOKEN` must be at least 16 characters.
- `COLOMBO_SPOOL_PATH` must be persistent across process, container, and host-service restarts. Compose and the deploy workflow mount the `colombo-spool` Docker volume at `/var/lib/colombo/spool`.
- Confirmed receipts are retained for seven days; terminal failures and quarantined bytes are retained for 30 days; expired receipt tombstones remain for another 30 days. Operators may remove an expired operation directory and its matching quarantine file earlier when policy requires it, but must not edit live entries.
- The scrape preserves the bounded v1 domain families and adds durable-operation count, oldest-age, and outcome families. Labels never contain usernames, assignments, filenames, buckets, URLs, object keys, or exception text.
- Other routes retain the v1 protected response behavior (`401`).
- `tenants-cli` remains installed in the runtime container.
- Tenant inspection reports whether an API key is configured without printing
  the key. Creation and explicit rotation are the only commands that reveal a
  newly generated key.

Useful commands:

| Command | Purpose |
| --- | --- |
| `make run` | Run the Rust process |
| `make test` | Run unit tests |
| `make verify` | Check formatting, tests, and strict Clippy |
| `make integration` | Exercise PostgreSQL, HTTP, FTP, FTPS, S3, and callbacks through Docker |
| `make package` | Build the release binary |
| `make tenants` | Open the tenant CRUD CLI |

Full architecture, configuration, CMS contracts, deployment, and tenant administration documentation is maintained in the [repository wiki](https://github.com/gaulatti/colombo/wiki).

## Java versus Rust benchmark

Under identical Docker limits (one CPU, 512 MiB), the three-run medians were an 18.5 MB Rust image versus 148.5 MB for Java, 255 ms versus 9.94 s to become healthy, 1.281 MiB versus 235.6 MiB idle memory, 18.41x the HTTP root throughput, and 4.23x the FTP connection churn. See [the benchmark method and complete results](benchmarks/RESULTS.md); these listener measurements intentionally exclude CMS and S3 latency.
