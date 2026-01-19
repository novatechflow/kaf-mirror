.PHONY: all build run start stop restart clean test docker docker-build docker-run docs

BIN_DIR=bin
KAF_MIRROR_BINARY=$(BIN_DIR)/kaf-mirror
ADMIN_CLI_BINARY=$(BIN_DIR)/admin-cli
MIRROR_CLI_BINARY=$(BIN_DIR)/mirror-cli
PID_FILE=/tmp/kaf-mirror.pid

all: build

# The 'docs' target generates API documentation using Swagger.
# It is intentionally kept separate from the main 'build' target
# to avoid issues with the swag tool parsing non-application Go files.
docs:
	@echo "Generating Swagger docs..."
	@GOPATH=$(shell go env GOPATH); \
	if [ -z "$$GOPATH" ]; then \
		echo "GOPATH is not set. Please set it and try again."; \
		exit 1; \
	fi; \
	go install github.com/swaggo/swag/cmd/swag@latest; \
	$$GOPATH/bin/swag init -g cmd/kaf-mirror/main.go --output web/docu/swagger --exclude scripts

build: docs docs-cli docs-html
	@echo "Building..."
	@mkdir -p $(BIN_DIR)
	$(eval VERSION := $(shell git describe --tags --abbrev=0))
	@go build -ldflags="-s -w -X main.Version=$(VERSION)" -o $(KAF_MIRROR_BINARY) cmd/kaf-mirror/main.go
	@go build -ldflags="-s -w" -o $(ADMIN_CLI_BINARY) cmd/admin-cli/main.go
	@go build -ldflags="-s -w -X main.Version=$(VERSION)" -o $(MIRROR_CLI_BINARY) cmd/mirror-cli/main.go

build-linux:
	@echo "Linux binaries are downloaded from GitHub releases during deployment."
	@echo "This target is a placeholder."

run: build
	@echo "Running..."
	@./$(KAF_MIRROR_BINARY)

start: build
	@echo "Starting..."
	@./$(KAF_MIRROR_BINARY) & echo $$! > $(PID_FILE)
	@echo "PID: `cat $(PID_FILE)`"

stop:
	@echo "Stopping..."
	@pkill -f kaf-mirror || echo "Process not found."
	@rm -f $(PID_FILE)

restart: stop start

update: build restart

clean:
	@echo "Cleaning..."
	@rm -rf $(BIN_DIR)
	@rm -f $(PID_FILE)

test:
	@echo "Running tests..."
	@go test ./tests/...
	@echo "Running go vet..."
	@go vet ./...
	@echo "Running race tests..."
	@go test -race ./...

docker-build:
	@echo "Building Docker image..."
	@docker build -t kaf-mirror:latest .

docker-run: docker-build
	@echo "Running Docker container..."
	@docker run -p 8080:8080 --name kaf-mirror kaf-mirror:latest

docs-html:
	@echo "Generating HTML documentation..."
	@go run scripts/generate-docs.go
	@go run scripts/generate-rbac-html.go

docs-cli:
	@echo "Generating CLI documentation (Markdown)..."
	@go run cmd/mirror-cli/main.go docs generate

# Detect docker compose command (plugin or standalone)
COMPOSE ?= $(shell if docker compose version >/dev/null 2>&1; then echo "docker compose"; elif command -v docker-compose >/dev/null 2>&1; then echo "docker-compose"; else echo ""; fi)
PROJECT_NAME ?= kaf-mirror

dev-up:
	@if [ -z "$(COMPOSE)" ]; then \
		echo "docker compose not found. Install Docker Desktop/Compose or ensure compose plugin is available."; \
		exit 1; \
	fi; \
	$(COMPOSE) -p $(PROJECT_NAME) up -d --build

dev-down:
	@if [ -z "$(COMPOSE)" ]; then \
		echo "docker compose not found. Install Docker Desktop/Compose or ensure compose plugin is available."; \
		exit 1; \
	fi; \
	$(COMPOSE) -p $(PROJECT_NAME) down -v --remove-orphans || true; \
	docker network rm $(PROJECT_NAME)_kaf-mirror 2>/dev/null || true
