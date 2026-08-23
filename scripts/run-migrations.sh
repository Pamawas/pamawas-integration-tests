#!/usr/bin/env bash
# Wait for PostgreSQL to be ready

set -euo pipefail

echo "Waiting for PostgreSQL..."

# Use pg_isready which is available in the GitHub Actions runner
for i in {1..30}; do
  if pg_isready -h localhost -p 5432 -U pamawas -d pamawas 2>/dev/null; then
    echo "PostgreSQL is ready"
    break
  fi
  echo "Waiting for PostgreSQL... ($i/30)"
  sleep 1
done

# Download dependencies
go mod download

# Run migrations with migrate-only mode
echo "Starting migration runner..."
go run ./main.go