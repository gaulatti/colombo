SHELL := /bin/bash

.DEFAULT_GOAL := help

SESSION_NAME ?= colombo-dev
MVNW := ./mvnw

.PHONY: help dev dev-attach dev-stop tenants run build test verify coverage clean package docker-build docker-run

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
	@echo "  make verify       Run tests + coverage checks"
	@echo "  make coverage     Generate HTML/XML coverage reports"
	@echo "  make package      Build jar (skip tests)"
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
	@$(MVNW) spring-boot:run

build:
	@$(MVNW) -DskipTests compile

test:
	@$(MVNW) test

verify:
	@$(MVNW) verify

coverage:
	@$(MVNW) clean verify
	@rm -rf coverage
	@mkdir -p coverage
	@cp -R target/site/jacoco/. coverage/
	@echo "Coverage HTML: coverage/index.html"

package:
	@$(MVNW) -DskipTests package

clean:
	@$(MVNW) clean

docker-build:
	@docker build -t colombo:local .

docker-run:
	@docker run --rm -p 8080:8080 --env-file .env colombo:local
