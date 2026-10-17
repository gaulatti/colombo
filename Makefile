SHELL := /bin/bash

.DEFAULT_GOAL := help

SESSION_NAME ?= colombo-dev
CARGO := cargo
# Keep database-backed fixtures bounded on smaller development and CI machines.
export RUST_TEST_THREADS ?= 4

.PHONY: help dev dev-attach dev-stop tenants run build test test-db test-db-stop verify integration clean package docker-build docker-run

help:
	@echo "Colombo CLI"
	@echo ""
	@echo "Usage: make <target>"
	@echo ""
	@echo "Targets:"
	@echo "  make dev          Start local dev tmux session (delegates to ./dev.sh)"
	@echo "  make dev-attach   Attach to existing dev tmux session"
	@echo "  make dev-stop     Stop dev tmux session"
	@echo "  make tenants      Interactive tenant CRUD CLI"
	@echo "  make run          Run app in current terminal"
	@echo "  make build        Compile project (skip tests)"
	@echo "  make test         Run tests"
	@echo "  make verify       Run format, tests, and Clippy"
	@echo "  make integration  Exercise the Docker/FTP/HTTP boundary"
	@echo "  make package      Build optimized native binary"
	@echo "  make clean        Clean build outputs"
	@echo "  make docker-build Build Docker image (tag: colombo:local)"
	@echo "  make docker-run   Run Docker image (port 8080)"

dev:
	@./dev.sh

dev-attach:
	@tmux attach-session -t "$(SESSION_NAME)"

dev-stop:
	@tmux kill-session -t "$(SESSION_NAME)"

tenants:
	@./scripts/tenants-cli.sh

run:
	@$(CARGO) run

build:
	@$(CARGO) build

test-db:
	@docker compose -p colombo-tests -f compose.test.yaml up -d --wait

test-db-stop:
	@docker compose -p colombo-tests -f compose.test.yaml down -v

test: test-db
	@trap '$(MAKE) test-db-stop' EXIT; $(CARGO) test --locked

verify: test-db
	@trap '$(MAKE) test-db-stop' EXIT; \
	  $(CARGO) fmt --all -- --check && \
	  $(CARGO) test --locked && \
	  $(CARGO) clippy --all-targets --all-features -- -D warnings

integration:
	@./tests/e2e.sh

package:
	@$(CARGO) build --locked --release

clean:
	@$(CARGO) clean

docker-build:
	@docker build -t colombo:local .

docker-run:
	@docker run --rm -p 8080:8080 -p 2121:2121 -p 60000-60100:60000-60100 --env-file .env -e COLOMBO_SPOOL_PATH=/var/lib/colombo/spool -v colombo-spool:/var/lib/colombo/spool colombo:local
