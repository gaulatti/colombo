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

## Credential refresh and authentication failures

The durable upload worker handles both HTTP- and FTP-accepted operations. Expired
S3 credentials can trigger CMS revalidation using the persisted validation key,
followed by another S3 attempt within the five-attempt upload budget. Refresh
unavailability is retryable. A denied refresh or changed assignment is terminal;
only a terminal dependency denial invalidates an associated live FTP session.
HTTP operations have no persistent FTP session to invalidate.

CMS validation distinguishes denied credentials from upstream unavailability.
Client-error statuses and valid JSON with missing/invalid required fields are
`Denied`; transport failures, other unsuccessful statuses, and unreadable JSON
are `Unavailable`. The HTTP upload endpoint currently maps both to `401`.
Receipt lookup maps denial to `401` and CMS unavailability to `503`.
A callback 4xx is a terminal dependency denial: the receipt records
`dependency_denied`, accepted bytes are quarantined, and an associated live FTP
session is invalidated. Callback 5xx/network failures schedule bounded retries
after delivery; they become terminal only when the callback budget is exhausted.

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

Agents must run the current shared Clouseau skill locally before creating a PR,
fix the reported findings, and review the final committed change again after
repairs. Publish the passing signed receipt and include its marker in the initial
ready PR body. `AGENTS.md` is authoritative; `CLAUDE.md` points Claude Code to the
same requirement. Never push directly to `main`.

Local review uses the authenticated coding harness and `.clouseau-local.json`,
independently of CI provider keys. Install the documented Rust coverage tooling
and start Docker first. `make coverage` runs the instrumented targets with a
disposable PostgreSQL database and writes `coverage.json`; the shared runner
checks exact native line and branch counters.

CI authenticates signed local reviews without another AI request and still runs
all tests, exact coverage and real container checks. An invalid, stale or altered
stamp blocks approval without spending provider tokens. Unstamped manual and
dependency-bot PRs use the separately installed CI AI reviewer.

The Backend CI workflow uses the public [Clouseau](https://github.com/gaulatti/clouseau)
action at an immutable reviewed revision. All tests and exact 100% application
line and branch coverage are mandatory. Independent DeepSeek Flash and Pro
inspectors read source, tests, requirements, and precedent; a separate atomic
grader selects evidence-backed semantic levels. Code computes each material
unit's fit, need, and proportionality (each must reach 80), and each causal
technical finding's nine-factor score (70 blocks). Tests also need meaningful
assertions and failure cases; percentages alone cannot establish integrity.

The bot retains a source-linked findings ledger and complete assessment artifact.
It approves only the verified revision and arms native protected auto-merge.
Product direction, authority, and protected review-control changes require the
owner's decision. Technical uncertainty holds for automated investigation;
intent confirmation cannot waive tests, integrity, or required evidence. A
bounded repair may change the current internal PR's existing changed paths, at
most twice, using an author credential independent of the approval bot. Every
new revision requires fresh CI and independent review. Forks use the same merge
criteria. The repair job requires a matching head repository before loading its
author credential, and the writer independently rejects forks.

Each repository keeps its [review contract](.github/clouseau.md) and
[configuration](.github/clouseau.json); [shared rules](https://github.com/gaulatti/clouseau/blob/b60ac157aaf4989e984e9f0694374212babb2c60/policy/change-acceptance.md)
live in Clouseau. Private bastille access is unnecessary. Jev grades repository
domain fit, unnecessary abstraction, and excess authority. DeepSeek grades the
remaining factors and unresolved Jev answers in one batch per investigation;
settled Jev answers are retained. The read-only `check_provider` diagnostic
qualifies this combined route on six fixed executed fixtures and retains
decisions, usage, and whole-review cost. This qualification is separate from a
real PR merge. Provider truncation retries at most three times with
identical evidence and 8,192/16,384/32,768 output-token limits; completed negative
or malformed answers are never resampled for approval. See the
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
Flash plans and writes the pages. Each page gets an independent DeepSeek V4 Pro
audit against production source, with the other proposed pages available for
consistency;
it must support every updated page with a reference to an exact source line and no findings.
Unsupported claims, invented source references, or missing page evidence stop publication.
The wiki workflow freezes its own run identity: an isolated `resolve-clouseau`
job resolves protected Clouseau `main` once, and generation and publication check
out that exact SHA, verify it, and record it in the plan artifact. It follows
central releases without update PRs and is independent of the deployment jobs.
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
authentication and protocol-refresh claims, generates a real DeepSeek draft
artifact, and checks wiki
Git write access with
`git push --dry-run`; it skips image builds, deployment, and wiki publication.
Normal post-deployment runs use the built-in GitHub token for wiki publication and
`DEEPSEEK_API_KEY` only in the read-only generation job. No additional wiki secret
is required when the repository token has wiki write access.

Wiki planning and writing use DeepSeek Flash without reasoning. The independent
V4 Pro factual checker uses low reasoning effort to trace shared production paths
and challenge retained claims. PR assessment keeps its existing reasoning mode.
Every proposed page must pass factual review before publication.

If factual review rejects a draft with actionable findings, the generator may
produce up to three corrected batches. Each must change the rejected text and pass a fresh
full review. Repeated drafts and an exhausted correction budget stop publication. Drafts and factual findings
are retained in the workflow artifact for diagnosis, including failed runs.
The validation-only false-claim diagnostic disables correction so rejection is
verified directly.

Factual citations select preassigned `path:Lnumber` references to exact source
lines. The model cannot supply its own quotation text. Missing, invented, blank,
or incomplete page references fail validation; the workflow diagnostic includes
the source-line map for inspection. Every request keeps the complete evidence
within its explicit context limit or fails without publication.

Planning, page generation, and factual review each have a native response schema
using DeepSeek's beta strict return format, in addition to all local checks. No model-selected function is executed. PR review stays on
its existing API and reasoning policy. This keeps response shape enforcement
separate from the substantive correctness and publication decisions.

The application coverage checker comes from the CI-pinned Clouseau checkout at
`.clouseau-ci`. After producing the instrumented `coverage.json` report, the
local equivalent of the enforcement step is:

```sh
python3 .clouseau-ci/src/check_coverage.py coverage.json src
```

Use the exact Clouseau revision checked out by `backend-ci.yml`; the same checkout
is required by the workflow integration tests in `.github/tests`.

Factual review emits source evidence and unresolved findings before the final
supporting verdict. Supported claims belong in evidence; a false verdict, any
unresolved findings, or invalid source references still blocks publication.

The factual report is a fixed checklist for the planned pages: one exact source
reference, bounded unresolved findings, and a verdict for each page. Every page
must pass, with no contradictions between its findings and verdict. Correction
regenerates only flagged pages and preserves the other page bytes exactly.

Planning and writing use DeepSeek Flash. The independent factual reviewer uses
DeepSeek V4 Pro with the same configured provider key; no new secret is required.
The live diagnostic exercises the actual factual reviewer on both correct and
deliberately false documentation before validation-only draft generation.

See [automatic Clouseau CI upgrades](docs/automatic-ci-upgrades.md) for the trusted
central source, frozen engine identity, receipt compatibility and diagnostics.
