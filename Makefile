.PHONY: all test test-unit test-integration lint pre-commit \
        docker-up docker-down coverage-check

# ── Config ────────────────────────────────────────────────────────────────────
DOCKER_COMPOSE_UNIT     := docker-compose.unit-test.yml
DOCKER_COMPOSE_INTEG    := ./integration_tests/docker-compose.yml
PYTEST_DIR              := ./integration_tests

# ── Top-level targets ─────────────────────────────────────────────────────────
all: lint test

build:
	go build ./..

setup:
	cp scripts/hooks/pre-commit .git/hooks/pre-commit
	chmod +x .git/hooks/pre-commit
	go install golang.org/x/tools/cmd/goimports@latest
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	@echo "✅ Dev environment ready. Run 'make all' to verify.".

# ── Docker helpers ────────────────────────────────────────────────────────────
docker-up:
	docker compose -f $(DOCKER_COMPOSE_UNIT) up -d --wait

docker-down:
	docker compose -f $(DOCKER_COMPOSE_UNIT) down

# ── Go tests (with docker lifecycle) ─────────────────────────────────────────

COVERAGE_OUT := coverage.out

test-unit: docker-up
	go test -coverprofile=$(COVERAGE_OUT) ./... ; \
	EXIT_CODE=$$? ; \
	$(MAKE) docker-down ; \
	exit $$EXIT_CODE

coverage-check: test-unit
	@go tool cover -func=$(COVERAGE_OUT) | grep "total:" | \
		awk '{print $$3}' | tr -d '%' | \
		awk '{if ($$1 < 50) {print "Coverage " $$1 "% is below threshold of 50%"; exit 1}}'

# ── Pytest integration tests ──────────────────────────────────────────────────
test-integration:
	docker compose -f $(DOCKER_COMPOSE_INTEG) up --build --abort-on-container-exit --exit-code-from test-runner ; \
	EXIT_CODE=$$? ; \
	docker compose -f $(DOCKER_COMPOSE_INTEG) down --volumes ; \
	exit $$EXIT_CODE


test: test-unit test-integration

# ── Lint / format / analysis ──────────────────────────────────────────────────
lint-fix:
	golangci-lint run --fix ./...

lint:
	golangci-lint run ./...

# ── Pre-commit (run everything) ───────────────────────────────────────────────
# todo: replace test-unit with coverage-check when test coverage is adequate
pre-commit: lint test-unit


