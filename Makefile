.PHONY: help build test tidy up down logs bench loadgen fmt ui

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

build: ## Build all binaries
	go build ./...

test: ## Run tests
	go test ./...

tidy: ## Sync go.mod/go.sum
	go mod tidy

fmt: ## Format code
	gofmt -w .

up: ## Start the full stack (Kafka, ClickHouse, services)
	docker compose -f deploy/docker-compose.yml up --build -d

down: ## Stop the stack
	docker compose -f deploy/docker-compose.yml down

logs: ## Tail service logs
	docker compose -f deploy/docker-compose.yml logs -f

ui: ## Print the web UI URL (served by the query service)
	@echo "TraceForge UI: http://localhost:8080"

loadgen: ## Run the OTLP load generator against localhost collector
	go run ./cmd/loadgen --endpoint localhost:4317 --rps 2000 --duration 60s

bench: ## Run loadgen at increasing rates (fills the resume [X] numbers)
	@for rps in 1000 5000 10000 20000; do \
		echo "=== $$rps rps ==="; \
		go run ./cmd/loadgen --endpoint localhost:4317 --rps $$rps --duration 30s; \
	done
