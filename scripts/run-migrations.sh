#!/usr/bin/env bash
# Migration runner for CI - runs embedded migrations from pamawas-schema and exits

set -euo pipefail

# This runs the main.go with env vars that make it only run migrations and exit
cd /opt/data/workspace/pamawas/pamawas-schema
DATABASE_URL="${PAMAWAS_SCHEMA_DATABASE_URL}" \
PAMAWAS_SCHEMA_USE_EMBEDDED_MIGRATIONS=true \
PAMAWAS_SCHEMA_MIGRATE_ONLY=true \
go run ./main.go