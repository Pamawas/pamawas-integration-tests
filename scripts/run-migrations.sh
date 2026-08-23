#!/usr/bin/env bash
# Wait for PostgreSQL to be ready using a simple Go program

set -euo pipefail

echo "Waiting for PostgreSQL..."

# Create a temporary Go file for checking PostgreSQL
cat > /tmp/wait_pg.go << 'GOEOF'
package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/lib/pq"
)

func main() {
	db, err := sql.Open("postgres", os.Getenv("DATABASE_URL"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("PostgreSQL is ready")
	os.Exit(0)
}
GOEOF

for i in {1..30}; do
  if go run /tmp/wait_pg.go; then
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