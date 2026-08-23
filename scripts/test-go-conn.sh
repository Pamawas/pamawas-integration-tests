#!/usr/bin/env bash
# Test Go database connection

set -euo pipefail

cat > /tmp/test_conn.go << 'GOEOF'
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
		fmt.Fprintf(os.Stderr, "Error opening DB: %v\n", err)
		os.Exit(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Error pinging DB: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Go database connection successful")
	os.Exit(0)
}
GOEOF

# Use the DATABASE_URL from environment (passed from workflow)
go run /tmp/test_conn.go