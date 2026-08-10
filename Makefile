DATABASE_URL ?= postgres://postgres:postgres@127.0.0.1:5432/pmr?sslmode=disable
REDIS_URL    ?= redis://127.0.0.1:6379
export DATABASE_URL
export REDIS_URL

.PHONY: help db demo chaos chaos-clean chaos-hard crash-test serve verify test build fmt vet clean

help: ## Show this help
	@grep -hE '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  \033[1m%-14s\033[0m %s\n", $$1, $$2}'

db: ## Start Postgres and Redis in Docker
	docker compose up -d
	@printf 'waiting for postgres'
	@until docker compose exec -T postgres pg_isready -q -U postgres 2>/dev/null; do printf .; sleep 1; done; echo ' ready'

demo: ## Narrated walkthrough of each failure mode (start here)
	go run ./cmd/pmr demo

chaos: ## The main event: 2000 random ops, faults on, hard restart every 25
	go run ./cmd/pmr chaos -iterations 2000 -crash-every 25

chaos-clean: ## Control group: same workload, no injected faults
	go run ./cmd/pmr chaos -iterations 800 -clean -crash-every 0

chaos-hard: ## 20 seeds back to back; this is the number quoted in the README
	@for s in $$(seq 1 20); do \
	  printf 'seed %-3s ' $$s; \
	  go run ./cmd/pmr chaos -iterations 2000 -seed $$s -crash-every 25 -verify-every 100 2>/dev/null \
	    | grep -E '^RESULT' || exit 1; \
	done

crash-test: ## Send a real SIGKILL to a real process, six times, mid-traffic
	./scripts/crash_test.sh

serve: ## HTTP API and operator panel on :8080
	go run ./cmd/pmr serve -fresh

verify: ## Check the invariants against whatever is in the database
	go run ./cmd/pmr verify

test: ## go test (the chaos harness, wrapped)
	go test ./... -count=1

build: ## Build the binary
	go build -o bin/pmr ./cmd/pmr

fmt: ; gofmt -l -w .
vet: ; go vet ./...

clean: ## Tear down containers and scratch files
	docker compose down -v || true
	rm -rf bin /tmp/pmr-*.json /tmp/pmr-chain*.json
