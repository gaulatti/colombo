FROM rust:1.98-alpine AS builder
RUN apk add --no-cache clang cmake make musl-dev perl
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY migrations ./migrations
COPY vendor ./vendor
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build --locked --release && cp /build/target/release/colombo /tmp/colombo

FROM alpine:3.22
RUN apk add --no-cache bash ca-certificates curl postgresql-client
COPY --from=builder /tmp/colombo /usr/local/bin/colombo
COPY scripts/tenants-cli.sh /usr/local/bin/tenants-cli
RUN chmod +x /usr/local/bin/tenants-cli
EXPOSE 8080 2121 60000-60100
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 CMD curl --fail --silent http://localhost:${PORT:-8080}/actuator/health || exit 1
ENTRYPOINT ["/usr/local/bin/colombo"]
