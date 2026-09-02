# Java v1 vs Rust v2 benchmark

`run.sh` compares the archived Java image and current Rust image under the same Docker limits: one CPU, 512 MiB memory, the same PostgreSQL container, equivalent HTTP/FTP ports, five seconds of warm idle time, 5,000 HTTP redirect requests at concurrency 50, and 1,000 FTP connect/banner/QUIT cycles at concurrency 50.

The benchmark intentionally measures the stable listener/runtime overhead rather than S3 or CMS latency, which would mostly measure external dependencies. Build both images for the same machine architecture and run:

```bash
JAVA_IMAGE=colombo-java:bench RUST_IMAGE=colombo-rust:bench ./benchmarks/run.sh
```

Results recorded for the completed migration are in `RESULTS.md`.
