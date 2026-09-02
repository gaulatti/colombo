# Java v1 versus Rust v2 benchmark

Measured on 2026-09-02 with Docker Desktop 29.6.1 on Apple silicon (`arm64`). Each runtime received one CPU and 512 MiB of memory, used the same PostgreSQL 17 container, migrated an empty schema, idled for five seconds, then served 5,000 HTTP root requests at concurrency 50 and 1,000 FTP banner/QUIT connections at concurrency 50.

## Raw runs

| Run | Runtime | Image bytes | Startup to healthy | Idle memory | Idle CPU | HTTP requests/s | FTP connections/s |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | Java v1 | 148,519,531 | 10,306 ms | 232.8 MiB | 0.31% | 377.84 | 750.45 |
| 1 | Rust v2 | 18,500,106 | 243 ms | 1.230 MiB | 0.00% | 9,390.22 | 3,658.84 |
| 2 | Java v1 | 148,519,531 | 10,049 ms | 229.5 MiB | 1.25% | 412.57 | 569.14 |
| 2 | Rust v2 | 18,500,106 | 202 ms | 1.227 MiB | 0.00% | 10,999.45 | 3,391.03 |
| 3 | Java v1 | 148,519,531 | 10,533 ms | 233.2 MiB | 0.70% | 415.39 | 640.23 |
| 3 | Rust v2 | 18,500,106 | 203 ms | 1.438 MiB | 0.00% | 11,384.05 | 2,174.39 |

## Median comparison

| Metric | Java v1 | Rust v2 | Rust result |
| --- | ---: | ---: | ---: |
| Runtime image | 148,519,531 bytes | 18,500,106 bytes | 8.03x smaller |
| Startup to healthy | 10,306 ms | 203 ms | 50.77x faster |
| Idle memory | 232.8 MiB | 1.230 MiB | 189.27x lower |
| Idle CPU | 0.70% | 0.00% | Below `docker stats` reporting resolution |
| HTTP root throughput | 412.57 requests/s | 10,999.45 requests/s | 26.66x higher |
| FTP connection churn | 640.23 connections/s | 3,391.03 connections/s | 5.30x higher |

These numbers measure runtime overhead and connection handling, not end-to-end S3 throughput. Production upload latency also depends on the CMS, local disk, network, and Amazon S3. The earlier 400% CPU observation was not reproduced under this one-CPU benchmark: Java's median idle sample was 0.70%, while Rust was below the reporting resolution in all three samples. Run `./benchmarks/run.sh` on the deployment host for hardware-specific capacity planning.
