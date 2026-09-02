# Java v1 versus Rust v2 benchmark

Measured on 2026-09-02 with Docker Desktop 29.6.1 on Apple silicon (`arm64`). Each runtime received one CPU and 512 MiB of memory, used the same PostgreSQL 17 container, migrated an empty schema, idled for five seconds, then served 5,000 HTTP root requests at concurrency 50 and 1,000 FTP banner/QUIT connections at concurrency 50.

## Raw runs

| Run | Runtime | Image bytes | Startup to healthy | Idle memory | Idle CPU | HTTP requests/s | FTP connections/s |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | Java v1 | 148,526,310 | 9,755 ms | 266 MiB | 0.17% | 405.66 | 511.80 |
| 1 | Rust v2 | 18,504,719 | 204 ms | 1.230 MiB | 0.00% | 6,172.88 | 2,877.83 |
| 2 | Java v1 | 148,526,310 | 9,938 ms | 233.6 MiB | 1.85% | 433.36 | 679.84 |
| 2 | Rust v2 | 18,504,719 | 264 ms | 1.969 MiB | 0.00% | 9,935.79 | 2,227.32 |
| 3 | Java v1 | 148,526,310 | 10,423 ms | 235.6 MiB | 0.32% | 615.10 | 754.11 |
| 3 | Rust v2 | 18,504,719 | 255 ms | 1.281 MiB | 0.00% | 7,976.62 | 3,393.64 |

## Median comparison

| Metric | Java v1 | Rust v2 | Rust result |
| --- | ---: | ---: | ---: |
| Runtime image | 148,526,310 bytes | 18,504,719 bytes | 8.03x smaller |
| Startup to healthy | 9,938 ms | 255 ms | 38.97x faster |
| Idle memory | 235.6 MiB | 1.281 MiB | 183.92x lower |
| Idle CPU | 0.32% | 0.00% | Below `docker stats` reporting resolution |
| HTTP root throughput | 433.36 requests/s | 7,976.62 requests/s | 18.41x higher |
| FTP connection churn | 679.84 connections/s | 2,877.83 connections/s | 4.23x higher |

These numbers measure runtime overhead and connection handling, not end-to-end S3 throughput. Production upload latency also depends on the CMS, local disk, network, and Amazon S3. The earlier 400% CPU observation was not reproduced under this one-CPU benchmark: Java's median idle sample was 0.32%, while Rust was below the reporting resolution in all three samples. Run `./benchmarks/run.sh` on the deployment host for hardware-specific capacity planning.
