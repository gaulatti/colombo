# Colombo

[![codecov](https://codecov.io/gh/gaulatti/colombo/branch/main/graph/badge.svg)](https://codecov.io/gh/gaulatti/colombo)

A **multi-tenant FTP server** that authenticates users via CMS-issued credentials, uploads files to Amazon S3, and notifies the CMS via a photo-callback webhook. No long-lived AWS keys are stored — every session is credential-less until the CMS grants it.

Built with Spring Boot 4 · Apache FTP Server · AWS SDK for Java v2 · PostgreSQL + Flyway.

---

## How it works

1. A tenant FTP client connects and sends its username and password.
2. Colombo posts the password to the tenant's CMS **validation endpoint**.
3. The CMS returns a short-lived STS credential set + an assignment ID.
4. The client uploads a file via `STOR`.
5. Colombo writes the file to the CMS-specified S3 bucket/prefix.
6. Colombo calls the CMS **photo-callback endpoint** with the S3 URL.

---

## Quick Start

```bash
# Clone and enter the project
git clone <repo-url>
cd colombo

# Set up environment
cp .env.example .env   # fill in DATABASE_URL, COLOMBO_MASTER_PASSWORD, etc.

# Start the dev session (requires tmux)
make dev

# Or run directly
make run
```

FTP server starts on **port 2121** in development. HTTP/actuator on **port 8080**.

For Docker runtime tenant management (no `make` inside container), run:

```bash
docker exec -it colombo-backend tenants-cli
```

---

## Make Targets

| Target              | Description                          |
| ------------------- | ------------------------------------ |
| `make dev`          | Start tmux dev session               |
| `make run`          | Run app in current terminal          |
| `make test`         | Run test suite                       |
| `make verify`       | Run tests + coverage checks          |
| `make coverage`     | Generate JaCoCo HTML/XML coverage    |
| `make build`        | Compile (skip tests)                 |
| `make package`      | Build runnable JAR                   |
| `make tenants`      | Interactive tenant CRUD CLI          |
| `make docker-build` | Build Docker image (`colombo:local`) |
| `make docker-run`   | Run Docker image with `.env`         |

---

## Documentation

Full documentation is in the [wiki](wiki/Home.md):

| Page                                           | Contents                                                      |
| ---------------------------------------------- | ------------------------------------------------------------- |
| [Architecture](wiki/Architecture.md)           | System overview, component descriptions, upload flow          |
| [Configuration](wiki/Configuration.md)         | All environment variables and `application.properties`        |
| [Development](wiki/Development.md)             | Local setup, dev workflow, build commands                     |
| [Deployment](wiki/Deployment.md)               | Docker, GitHub Actions CI/CD, production checklist            |
| [Tenant Management](wiki/Tenant-Management.md) | Schema, CLI tool, API key rotation                            |
| [CMS Integration](wiki/CMS-Integration.md)     | Validation endpoint, photo-callback contract, master password |

---

## Coverage

- Codecov uploads from GitHub Actions on `main`
- Branch used for badge/report: `main`
- Local coverage report: `coverage/index.html` (after `make coverage`)
- Raw JaCoCo output: `target/site/jacoco/index.html` (after `./mvnw verify`)

---

## Requirements

- Java 21+
- PostgreSQL 14+
- tmux (for `make dev`)
- Docker (optional)
