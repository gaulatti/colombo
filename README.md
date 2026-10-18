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

FTP transfers do not hold the shared session mutex while waiting on the camera's data socket. If a camera drops its control connection during a stalled transfer, cleanup for that session cannot block the pooled listener from greeting other connections.

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

The same operation ID is written to Colombo's structured log before an FTP upload receives `226`. The multipart request limit is 100 MiB, including multipart headers and boundaries. Oversized requests return `413` with `{"error":"Upload exceeds the size limit"}` and create no accepted operation. Malformed multipart requests return `400`; temporary storage failures return `500`. The FTP-only master-password support bypass is never accepted by this endpoint.

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

`make test` and `make verify` start a disposable PostgreSQL test service on
loopback port `55436`. Tests use separate schemas, fictional credentials, and
local CMS/S3 servers. The local test database uses memory-backed storage. Both
commands remove it on exit; `make test-db-stop` removes a manually started service.
CI provides the same PostgreSQL service. For a separately managed disposable
database, run Cargo directly with `COLOMBO_TEST_DATABASE_URL` set explicitly.

The tests exercise CMS failures, FTP session isolation, multipart streaming and
disk failures, naming and EXIF metadata, durable recovery, bounded S3/callback
retries, receipt isolation, and process startup/shutdown. HTTP temporary files
remain owned until durable acceptance or rejection, including malformed trailing
multipart input. Both HTTP and FTP listeners shut down on Ctrl+C.

Full architecture, configuration, CMS contracts, deployment, and tenant administration documentation is maintained in the [repository wiki](https://github.com/gaulatti/colombo/wiki).

## Pull requests

The Backend CI workflow uses the public [Clouseau](https://github.com/gaulatti/clouseau)
action, pinned to a reviewed commit. It requires all tests and exactly 100% line and branch
coverage of Colombo's application sources. The required native approval job arms auto-merge and approves only
the verified PR revision. DeepSeek Flash assesses repository purpose, supported
intent, and meaningful test evidence using the contract, tickets, and precedent.
Coverage improvements and in-scope maintenance have no file/line cutoff. Product
drift, unclear requirements, and changes to review controls need the owner's
second opinion. Detected fake tests or bypassed behavior block approval even
after intent confirmation; all tests and exact coverage remain mandatory. External contributions follow the same policy, with
fork code isolated from approval credentials and secrets.

Each repository keeps its [review contract](.github/clouseau.md) and
[configuration](.github/clouseau.json); shared review rules live in Clouseau.
Private bastille access is unnecessary for contributors or CI.
Missing or token-truncated AI responses retry automatically within the read-only
assessment, up to three attempts with the same evidence. Token truncation increases
the output budget from 4,096 to at most 16,384 tokens. Complete verdicts stop retries;
exhausted attempts and other provider failures still require a second opinion.
Only bounded attempt/outcome/token-budget details appear in diagnostic logs. Colombo also tests the exact pinned reviewer checkout directly in its mandatory CI suite: recovery, retry exhaustion, and stopping at complete negative or invalid responses.

See the [shared review policy](https://github.com/gaulatti/clouseau/blob/bc4f5d2d1c220da9dd719178e3eb95f2ed56721c/policy/intent-review.md) and
[CI operations guide](https://github.com/gaulatti/colombo/wiki/CI-and-Approval).

CI builds the normal container once using persistent Docker-layer and Cargo
cache mounts. The integration suite exercises that image; CD pushes the tested
image without rebuilding it. Intent comments reuse the existing verification.

## Java versus Rust benchmark

Under identical Docker limits (one CPU, 512 MiB), the three-run medians were an 18.5 MB Rust image versus 148.5 MB for Java, 255 ms versus 9.94 s to become healthy, 1.281 MiB versus 235.6 MiB idle memory, 18.41x the HTTP root throughput, and 4.23x the FTP connection churn. See [the benchmark method and complete results](benchmarks/RESULTS.md); these listener measurements intentionally exclude CMS and S3 latency.


## Wiki maintenance after deployment

After the existing `Backend Deploy` workflow's `deploy` job succeeds on `main`,
DeepSeek prepares a coordinated update to the repository wiki. Clouseau reads the
exact deployed source revision and current wiki, plans every affected page, then
generates and validates the complete batch, including navigation and page links.
A separate DeepSeek call fact-checks the full update against production source;
it must support every updated page with an exact source quotation and no findings.
Unsupported claims, invented quotations, or missing page evidence stop publication.
The source allowlist is `.github/clouseau-wiki.json`; agent instructions and real
environment files are not documentation sources.

A separate job, without the provider key or deployment credentials, independently
checks the deployment result and publishes one atomic wiki commit. Human edits
made during generation stop publication rather than being overwritten. The wiki
receipt records the source SHA and deployment run; rerunning an already documented
revision does not create another commit. Deployment and documentation are serialized.
A documentation failure leaves the completed deployment intact and makes the
workflow visibly fail; it does not roll back or claim external service health.

For a safe preflight, manually run `Backend Deploy` with `validate_wiki_only: true`.
It verifies the live factual gate against supported and deliberately false
authentication claims, generates a real DeepSeek draft artifact, and checks wiki
Git write access with
`git push --dry-run`; it skips image builds, deployment, and wiki publication.
Normal post-deployment runs use the built-in GitHub token for wiki publication and
`DEEPSEEK_API_KEY` only in the read-only generation job. No additional wiki secret
is required when the repository token has wiki write access.

Wiki calls use DeepSeek Flash in non-thinking JSON mode so their bounded response
budget is available for the documentation and source citations. PR assessment
keeps its existing reasoning mode. The independent factual gate still applies to
every proposed wiki update before publication.
