# Pamawas Integration Tests

Cross-component end-to-end integration tests for the Pamawas AI Infrastructure Incident Investigator.

## Overview

This repository contains integration tests that exercise the complete Pamawas pipeline:

```
Webhook (Ingest) → Event → Correlation (Incident + Outbox) → Investigation (Evidence) → Report → Delivery
```

## Components Tested

- **pamawas-ingest**: Generic/Grafana webhook handlers with deduplication
- **pamawas-correlator**: Correlation engine with investigation outbox worker
- **pamawas-investigator**: Bounded LLM investigation with real adapters
- **pamawas-reporter**: Report generation with fake delivery transports
- **pamawas-scheduler**: Daily/high-severity report triggers
- **pamawas-schema**: Database migrations and domain models

## Quick Start

```bash
# Prerequisites
# - Go 1.26+
# - Python 3.14+
# - Docker (for PostgreSQL testcontainer)

# Run integration tests
cd integration-tests
go test -v -race -count=1 ./...
```

## CI/CD

The `.github/workflows/integration.yml` runs on every push to `dev` and `main` branches:

1. Checks out all 6 component repositories
2. Creates a Go workspace with `go work init`
3. Starts PostgreSQL test container
2. Runs database migrations
3. Runs full cross-component E2E tests with race detector

## Test Coverage

- **Reporter Delivery Pipeline**: Verifies fake Discord/Telegram/Email delivery capture
- **Full Pipeline E2E**: Webhook → Ingest → Correlate → Investigate → Report → Deliver
- **Retry/Restart Deduplication**: Verifies no duplicate deliveries on restart

## Architecture

```
integration-tests/
├── go.mod                    # Go module with replace directives
├── go.sum
├── reporter_delivery_e2e_test.go    # Reporter delivery pipeline tests
├── full_pipeline_e2e_test.go        # Full cross-component E2E test (TODO)
├── .github/workflows/
│   └── integration.yml      # GitHub Actions workflow
└── README.md
```

## Dependencies

- **Go**: 1.26+
- **Python**: 3.14+ (for investigator)
- **PostgreSQL**: 16+ (via testcontainers)
- **Component repos**: All 6 Pamawas component repos checked out in CI

## Local Development

```bash
# Start PostgreSQL
docker run -d --name pamawas-test-pg \
  -e POSTGRES_USER=pamawas \
  -e POSTGRES_PASSWORD=pamawas \
  -e POSTGRES_DB=pamawas \
  -p 5432:5432 \
  postgres:16-alpine

# Run migrations
export DATABASE_URL="postgres://pamawas:pamawas@localhost:5432/pamawas?sslmode=disable"
go run ../pamawas-schema/main.go migrate

# Run tests
export DATABASE_URL="postgres://pamawas:pamawas@localhost:5432/pamawas?sslmode=disable"
go test -v -race -count=1 ./...
```# Trigger fresh CI run
# Trigger with fixed pamawas-schema
# Trigger with fixed pamawas-schema
# Trigger with self-hosted runner
# Trigger fresh run
# Trigger with clean state
