package integration

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	correlatorservice "github.com/Pamawas/pamawas-correlator/service"
	correlatormetrics "github.com/Pamawas/pamawas-correlator/metrics"
	ingestconfig "github.com/Pamawas/pamawas-ingest/config"
	ingesthandlers "github.com/Pamawas/pamawas-ingest/handlers"
	ingestmetrics "github.com/Pamawas/pamawas-ingest/metrics"
	ingestmodel "github.com/Pamawas/pamawas-ingest/models"
	"github.com/Pamawas/pamawas-reporter/fakes"
	reporterservice "github.com/Pamawas/pamawas-reporter/service"
	reportermodel "github.com/Pamawas/pamawas-reporter/models"
	reportermtrics "github.com/Pamawas/pamawas-reporter/metrics"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

// TestFullPipelineE2E runs the complete webhook-to-delivery pipeline
// against a real PostgreSQL database (via docker-compose) and fake delivery
// transports for external providers (Discord, Telegram).
//
// The journey exercised:
//   ingest (webhook -> event) -> correlate (event -> incident -> outbox)
//   -> investigate (outbox -> evidence) -> report (report request -> report + delivery attempts)
//   -> deliver (send to fake Discord/Telegram servers)
//
// Per the MVP verification scope, this test uses deterministic fake transports
// for all external providers. Real PostgreSQL is required for transaction,
// constraint, and lifecycle semantics.
func TestFullPipelineE2E(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	// 1. Get database connection from environment (set by docker-compose or CI).
	connStr := getEnvOrDefault("DATABASE_URL", "postgres://pamawas:pamawas@localhost:5432/pamawas?sslmode=disable")

	// 2. Connect to database.
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Wait for database to be ready.
	if err := waitForDB(ctx, db); err != nil {
		t.Fatalf("Database not ready: %v", err)
	}

	// 3. Run migrations using the actual pamawas-schema migrations.
	if err := runMigrations(db); err != nil {
		t.Fatalf("Failed to run migrations: %v", err)
	}

	// Clean up any existing test data (migrations create tables with IF NOT EXISTS,
	// but we need a clean slate for deterministic test runs).
	if err := cleanupTestData(db); err != nil {
		t.Fatalf("Failed to clean up test data: %v", err)
	}

	// 4. Setup fake delivery transport for reporter.
	deliveryTransport := fakes.NewFakeDeliveryTransport()
	defer deliveryTransport.Close()

	discordServer := deliveryTransport.StartDiscordServer()
	defer discordServer.Close()
	telegramServer := deliveryTransport.StartTelegramServer()
	defer telegramServer.Close()

	// 5. Setup Ingest handler.
	ingestCfg := ingestconfig.Config{
		DatabaseURL:  connStr,
		WebhookToken: "test-token",
		TestMode:     true,
		MaxBodyBytes: 1024 * 1024,
		Port:         "8080",
		LogLevel:     "info",
		Environment:  "test",
	}
	ingestMetrics := ingestmetrics.NewMetrics()
	ingestHandler := ingesthandlers.NewHandler(db, ingestCfg, ingestMetrics)

	// 6. Setup Correlator.
	correlator := correlatorservice.NewCorrelator(
		db,
		10*time.Minute, // timeWindow
		1*time.Minute,  // interval
		"auto",         // mode
		correlatormetrics.NewMetrics(),
		getEnvOrDefault("INVESTIGATOR_URL", "http://localhost:8082/v1/investigations"),
	)

	// 7. Setup Reporter with fake delivery URLs.
	// TelegramAPIBaseURL redirects sendToTelegram to the fake server instead of api.telegram.org.
	reporterCfg := reporterservice.ReporterConfig{
		DatabaseURL:        connStr,
		DiscordWebhookURL:  discordServer.URL,
		TelegramBotToken:   "fake-token",
		TelegramChatID:     "fake-chat",
		TelegramAPIBaseURL: telegramServer.URL,
		Mode:               "test",
	}
	reporterMetrics := reportermtrics.NewMetrics()
	reporter := reporterservice.NewReporter(db, reporterCfg, reporterMetrics)

	t.Run("Full pipeline: webhook to delivery", func(t *testing.T) {
		// Step 1: Submit Grafana webhook.
		webhookPayload := `{
			"ruleName": "High CPU on api-service",
			"timestamp": "2026-08-22T10:30:00Z",
			"evalMatches": [{
				"time": "2026-08-22T10:30:00Z",
				"value": "95.5",
				"metric": {
					"service": "api-service",
					"instance": "api-1"
				}
			}],
			"tags": {
				"service": "api-service",
				"severity": "high"
			}
		}`

		req := httptest.NewRequest("POST", "/webhook/grafana", strings.NewReader(webhookPayload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer test-token")
		req.Header.Set("X-Idempotency-Key", "test-key-123")

		webhookResp := httptest.NewRecorder()
		ingestHandler.GrafanaWebhook(webhookResp, req)

		if webhookResp.Code != http.StatusAccepted {
			t.Fatalf("Expected %d Accepted, got %d. Body: %s", http.StatusAccepted, webhookResp.Code, webhookResp.Body.String())
		}

		var webhookRespBody ingestmodel.WebhookResponse
		if err := json.Unmarshal(webhookResp.Body.Bytes(), &webhookRespBody); err != nil {
			t.Fatalf("Failed to unmarshal webhook response: %v", err)
		}
		eventID := webhookRespBody.Data.EventID
		if eventID == "" {
			t.Fatal("No event ID returned")
		}
		t.Logf("Created event: %s", eventID)

		// Step 2: Correlator processes event -> creates incident.
		// Run the correlator once to process pending events.
		if err := correlator.Correlate(); err != nil {
			t.Fatalf("Correlator run failed: %v", err)
		}

		// Query for the created incident.
		var incidentID string
		err := db.QueryRowContext(ctx, `SELECT id FROM incidents WHERE status IN ('open', 'investigating') ORDER BY started_at DESC LIMIT 1`).Scan(&incidentID)
		if err != nil {
			t.Fatalf("Failed to find created incident: %v", err)
		}
		t.Logf("Created incident: %s", incidentID)

		// Step 3: Create terminal investigation evidence for the incident.
		// This simulates the investigator having completed its investigation
		// and stored evidence in the database. We create the investigation run
		// and evidence directly since the investigator service is not running.
		runID := uuid.NewString()
		_, err = db.ExecContext(ctx, `
			INSERT INTO investigation_runs (id, incident_id, request_key_hash, status,
				model_provider, model_name, prompt_version, tool_contract, max_tool_calls,
				started_at, completed_at)
			VALUES ($1, $2, $3, 'completed',
				'test-provider', 'test-model', 'v1', 1, 10,
				now() - interval '5 minutes', now())
		`, runID, incidentID, uuid.NewString())
		if err != nil {
			t.Fatalf("Failed to create investigation run: %v", err)
		}

		_, err = db.ExecContext(ctx, `
			INSERT INTO evidence (id, incident_id, run_id, type, content, source, confidence, supports_evidence, contradicts_evidence, ordinal)
			VALUES ($1, $2, $3, 'likely_cause', 'Database connection pool exhausted', 'prometheus', 0.82, '{}', '{}', 1)
		`, uuid.NewString(), incidentID, runID)
		if err != nil {
			t.Fatalf("Failed to create evidence: %v", err)
		}

		// Step 5: Create a pending report_requests row (simulating scheduler output).
		requestID := uuid.NewString()
		periodStart := time.Now().Add(-1 * time.Hour).UTC().Format(time.RFC3339)
		periodEnd := time.Now().UTC().Format(time.RFC3339)
		requestHash := sha256.Sum256([]byte(requestID + incidentID))
		idempotencyHash := hex.EncodeToString(requestHash[:])

		_, err = db.ExecContext(ctx, `
			INSERT INTO report_requests (id, request_type, period_start, period_end, timezone,
				idempotency_hash, status, attempts, lease_expires_at)
			VALUES ($1, 'high_severity', $2, $3, 'Asia/Jakarta', $4, 'pending', 0, NULL)
		`, requestID, periodStart, periodEnd, idempotencyHash)
		if err != nil {
			t.Fatalf("Failed to create report request: %v", err)
		}

		// Step 6: Reporter generates report.
		reportReq := reportermodel.ReportPayload{
			ContractVersion: 1,
			RequestID:       requestID,
			ReportType:      "high_severity",
			PeriodStart:     periodStart,
			PeriodEnd:       periodEnd,
			Timezone:        "Asia/Jakarta",
			IncidentIDs:     []string{incidentID},
		}
		reportResp, err := reporter.ProcessReportRequest(ctx, reportReq)
		if err != nil {
			t.Fatalf("Report generation failed: %v", err)
		}
		reportID := reportResp.ReportID
		if reportID == "" {
			t.Fatal("No report ID returned")
		}
		t.Logf("Generated report: %s", reportID)

		// Step 7: Delivery is triggered (async).
		// Poll the fake transport until deliveries appear or timeout,
		// then wait a bit more for the async updateReportDeliveryStatus DB calls.
		waitForAsyncDelivery(t, deliveryTransport)

		// Step 8: Verify delivery was captured.
		discordDeliveries := deliveryTransport.GetDeliveriesByChannel("discord")
		telegramDeliveries := deliveryTransport.GetDeliveriesByChannel("telegram")

		if len(discordDeliveries) == 0 {
			t.Errorf("Expected at least 1 Discord delivery, got 0")
		} else {
			t.Logf("Discord deliveries captured: %d", len(discordDeliveries))
		}

		if len(telegramDeliveries) == 0 {
			t.Errorf("Expected at least 1 Telegram delivery, got 0")
		} else {
			t.Logf("Telegram deliveries captured: %d", len(telegramDeliveries))
		}

		// Verify content references the incident (evidence-based, honest reporting).
		if len(discordDeliveries) > 0 {
			content := discordDeliveries[0].Content
			t.Logf("Discord delivery content:\n%s", content)

			// The report content should reference the incident ID (truncated).
			if !strings.Contains(content, incidentID[:min(8, len(incidentID))]) {
				t.Errorf("Discord delivery content should contain incident ID prefix %q", incidentID[:min(8, len(incidentID))])
			}

			// The report should contain evidence (not "investigation unavailable").
			if strings.Contains(content, "investigation unavailable") {
				t.Errorf("Report should contain evidence findings, not 'investigation unavailable'")
			}

			// The report should NOT fabricate availability (no percentage claiming availability).
			// "unavailable" is an honest status; "82%" is evidence confidence, not availability.
			if strings.Contains(content, "Availability:") && strings.Contains(content, "available") && strings.Contains(content, "%") && !strings.Contains(content, "unavailable") {
				t.Errorf("Report should not fabricate availability percentage")
			}

			// The report should not claim a root cause without evidence.
			// Evidence content "Database connection pool exhausted" should appear.
			if !strings.Contains(content, "Database connection pool exhausted") {
				t.Errorf("Report content should contain evidence: 'Database connection pool exhausted'")
			}
		}

		// Verify report status was updated to "delivered" in the database.
		var reportStatus string
		err = db.QueryRowContext(ctx, `SELECT status FROM reports WHERE id = $1`, reportID).Scan(&reportStatus)
		if err != nil {
			t.Fatalf("Failed to query report status: %v", err)
		}
		if reportStatus != "delivered" {
			t.Errorf("Expected report status 'delivered', got '%s'", reportStatus)
		}

		// Verify delivery attempts are recorded with "sent" status.
		rows, err := db.QueryContext(ctx, `SELECT channel, status FROM delivery_attempts WHERE report_id = $1`, reportID)
		if err != nil {
			t.Fatalf("Failed to query delivery attempts: %v", err)
		}
		defer rows.Close()
		var sentChannels []string
		for rows.Next() {
			var channel, status string
			if err := rows.Scan(&channel, &status); err != nil {
				t.Fatalf("Failed to scan delivery attempt: %v", err)
			}
			if status == "sent" {
				sentChannels = append(sentChannels, channel)
			}
		}
		if len(sentChannels) != 2 {
			t.Errorf("Expected 2 sent channels (discord, telegram), got %d: %v", len(sentChannels), sentChannels)
		}

		// Step 9: Test retry/restart deduplication.
		t.Run("Retry/restart deduplication", func(t *testing.T) {
			// Attempt to re-process the same request.
			// The claim should fail because the request is already "generated".
			_, err := reporter.ProcessReportRequest(ctx, reportReq)
			if err == nil {
				t.Fatal("Expected error on duplicate report request, got nil")
			}
			t.Logf("Duplicate request correctly rejected: %v", err)

			// Verify no additional deliveries were created.
			time.Sleep(500 * time.Millisecond) // Allow any async work to settle
			discordCount := deliveryTransport.GetDeliveryCount("discord")
			telegramCount := deliveryTransport.GetDeliveryCount("telegram")
			if discordCount != 1 {
				t.Errorf("Expected 1 Discord delivery after dedup, got %d", discordCount)
			}
			if telegramCount != 1 {
				t.Errorf("Expected 1 Telegram delivery after dedup, got %d", telegramCount)
			}
		})
	})
}

// waitForAsyncDelivery polls the fake transport until both Discord and Telegram
// deliveries are captured, then waits an additional period to allow the async
// updateReportDeliveryStatus DB calls to complete.
func waitForAsyncDelivery(t *testing.T, transport *fakes.FakeDeliveryTransport) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		discordDeliveries := transport.GetDeliveriesByChannel("discord")
		telegramDeliveries := transport.GetDeliveriesByChannel("telegram")
		if len(discordDeliveries) > 0 && len(telegramDeliveries) > 0 {
			// Both deliveries happened. Wait a bit more for the async
			// updateReportDeliveryStatus DB calls to complete.
			time.Sleep(500 * time.Millisecond)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("Timed out waiting for async delivery to complete")
}

// waitForDB waits for the database to be ready.
func waitForDB(ctx context.Context, db *sql.DB) error {
	for i := 0; i < 30; i++ {
		if err := db.PingContext(ctx); err == nil {
			return nil
		}
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("database not ready after 30 seconds")
}

// runMigrations runs the actual schema migrations from pamawas-schema.
// It embeds the migration SQL inline to avoid filesystem path dependencies in CI.
func runMigrations(db *sql.DB) error {
	queries := []string{
		// Schema migrations tracking
		`CREATE TABLE IF NOT EXISTS schema_migrations (
			version TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,

		// Events table
		`CREATE TABLE IF NOT EXISTS events (
			id TEXT PRIMARY KEY,
			source TEXT NOT NULL,
			source_event_id TEXT,
			fingerprint TEXT,
			type TEXT NOT NULL,
			occurred_at TIMESTAMPTZ NOT NULL,
			received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			service TEXT,
			environment TEXT NOT NULL,
			severity TEXT NOT NULL CHECK (severity IN ('debug', 'info', 'warning', 'high', 'critical')),
			title TEXT NOT NULL,
			status TEXT NOT NULL CHECK (status IN ('firing', 'resolved', 'informational')),
			labels JSONB NOT NULL DEFAULT '{}'::jsonb,
			raw_payload JSONB,
			schema_version INTEGER NOT NULL DEFAULT 1 CHECK (schema_version > 0),
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS events_source_event_id_key
			ON events (source, source_event_id) WHERE source_event_id IS NOT NULL`,
		`CREATE UNIQUE INDEX IF NOT EXISTS events_source_fingerprint_occurred_at_key
			ON events (source, fingerprint, occurred_at)
			WHERE source_event_id IS NULL AND fingerprint IS NOT NULL`,
		`CREATE INDEX IF NOT EXISTS events_occurred_at_idx ON events (occurred_at)`,
		`CREATE INDEX IF NOT EXISTS events_service_time_idx ON events (environment, service, occurred_at)`,
		`CREATE INDEX IF NOT EXISTS events_status_time_idx ON events (status, occurred_at)`,

		// Incidents table
		`CREATE TABLE IF NOT EXISTS incidents (
			id TEXT PRIMARY KEY,
			title TEXT NOT NULL,
			status TEXT NOT NULL CHECK (status IN ('open', 'investigating', 'resolved', 'suppressed')),
			started_at TIMESTAMPTZ NOT NULL,
			last_event_at TIMESTAMPTZ NOT NULL,
			resolved_at TIMESTAMPTZ,
			severity TEXT NOT NULL,
			environment TEXT NOT NULL,
			affected_services TEXT[] NOT NULL DEFAULT '{}',
			correlation_policy TEXT NOT NULL,
			correlation_version INTEGER NOT NULL CHECK (correlation_version > 0),
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			CHECK ((status = 'resolved') = (resolved_at IS NOT NULL)),
			CHECK (last_event_at >= started_at)
		)`,

		// Incident events junction table
		`CREATE TABLE IF NOT EXISTS incident_events (
			incident_id TEXT NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
			event_id TEXT NOT NULL REFERENCES events(id) ON DELETE RESTRICT,
			correlation_reason TEXT NOT NULL,
			policy_version INTEGER NOT NULL CHECK (policy_version > 0),
			attached_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			PRIMARY KEY (incident_id, event_id),
			UNIQUE (event_id, policy_version)
		)`,

		// Investigation runs
		`CREATE TABLE IF NOT EXISTS investigation_runs (
			id TEXT PRIMARY KEY,
			incident_id TEXT NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
			request_key_hash TEXT NOT NULL,
			status TEXT NOT NULL CHECK (status IN (
				'queued', 'running', 'completed', 'unknown',
				'failed_retryable', 'failed_terminal'
			)),
			model_provider TEXT NOT NULL,
			model_name TEXT NOT NULL,
			prompt_version TEXT NOT NULL,
			tool_contract INTEGER NOT NULL CHECK (tool_contract > 0),
			max_tool_calls INTEGER NOT NULL CHECK (max_tool_calls >= 0),
			started_at TIMESTAMPTZ,
			completed_at TIMESTAMPTZ,
			safe_error_code TEXT,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			UNIQUE (incident_id, request_key_hash),
			CHECK (completed_at IS NULL OR started_at IS NULL OR completed_at >= started_at)
		)`,

		// Tool executions
		`CREATE TABLE IF NOT EXISTS tool_executions (
			id TEXT PRIMARY KEY,
			run_id TEXT NOT NULL REFERENCES investigation_runs(id) ON DELETE CASCADE,
			sequence_no INTEGER NOT NULL CHECK (sequence_no >= 0),
			tool_name TEXT NOT NULL,
			arguments_redacted JSONB NOT NULL DEFAULT '{}'::jsonb,
			result_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
			result_hash TEXT,
			status TEXT NOT NULL,
			duration_ms INTEGER NOT NULL CHECK (duration_ms >= 0),
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			UNIQUE (run_id, sequence_no)
		)`,

		// Evidence
		`CREATE TABLE IF NOT EXISTS evidence (
			id TEXT PRIMARY KEY,
			incident_id TEXT NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
			run_id TEXT NOT NULL REFERENCES investigation_runs(id) ON DELETE CASCADE,
			type TEXT NOT NULL CHECK (type IN ('fact', 'likely_cause', 'hypothesis', 'unknown')),
			content TEXT NOT NULL,
			source TEXT NOT NULL,
			confidence DOUBLE PRECISION NOT NULL CHECK (confidence >= 0.0 AND confidence <= 1.0),
			supports_evidence TEXT[] NOT NULL DEFAULT '{}',
			contradicts_evidence TEXT[] NOT NULL DEFAULT '{}',
			ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			UNIQUE (run_id, ordinal)
		)`,

		// Investigation outbox
		`CREATE TABLE IF NOT EXISTS investigation_outbox (
			id TEXT PRIMARY KEY,
			incident_id TEXT NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
			contract_version INTEGER NOT NULL CHECK (contract_version > 0),
			request_key_hash TEXT NOT NULL UNIQUE,
			status TEXT NOT NULL CHECK (status IN ('pending', 'leased', 'delivered', 'retryable', 'failed_terminal')),
			attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
			lease_expires_at TIMESTAMPTZ,
			next_attempt_at TIMESTAMPTZ,
			safe_error_code TEXT,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,

		// Idempotency records
		`CREATE TABLE IF NOT EXISTS idempotency_records (
			audience TEXT NOT NULL,
			caller TEXT NOT NULL,
			key_hash TEXT NOT NULL,
			request_hash TEXT NOT NULL,
			status TEXT NOT NULL CHECK (status IN ('processing', 'completed', 'conflict')),
			result_reference TEXT,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			expires_at TIMESTAMPTZ NOT NULL,
			PRIMARY KEY (audience, caller, key_hash)
		)`,
		`CREATE INDEX IF NOT EXISTS idempotency_records_expires_at_idx ON idempotency_records (expires_at)`,

		// Report requests
		`CREATE TABLE IF NOT EXISTS report_requests (
			id TEXT PRIMARY KEY,
			request_type TEXT NOT NULL CHECK (request_type IN ('daily', 'high_severity')),
			period_start TIMESTAMPTZ NOT NULL,
			period_end TIMESTAMPTZ NOT NULL,
			timezone TEXT NOT NULL,
			idempotency_hash TEXT NOT NULL UNIQUE,
			status TEXT NOT NULL CHECK (status IN (
				'pending', 'generating', 'generated',
				'failed_retryable', 'failed_terminal'
			)),
			attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
			next_attempt_at TIMESTAMPTZ,
			lease_expires_at TIMESTAMPTZ,
			safe_error_code TEXT,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			CHECK (period_end > period_start)
		)`,

		// Reports
		`CREATE TABLE IF NOT EXISTS reports (
			id TEXT PRIMARY KEY,
			request_id TEXT NOT NULL UNIQUE REFERENCES report_requests(id),
			report_type TEXT NOT NULL CHECK (report_type IN ('daily', 'high_severity')),
			period_start TIMESTAMPTZ NOT NULL,
			period_end TIMESTAMPTZ NOT NULL,
			timezone TEXT NOT NULL,
			template_version TEXT NOT NULL,
			content TEXT NOT NULL,
			generated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			status TEXT NOT NULL CHECK (status IN (
				'generated', 'partially_delivered', 'delivered', 'delivery_failed'
			)),
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,

		// Report incidents
		`CREATE TABLE IF NOT EXISTS report_incidents (
			report_id TEXT NOT NULL REFERENCES reports(id) ON DELETE CASCADE,
			incident_id TEXT NOT NULL REFERENCES incidents(id) ON DELETE RESTRICT,
			inclusion_reason TEXT NOT NULL,
			PRIMARY KEY (report_id, incident_id)
		)`,

		// Delivery attempts
		`CREATE TABLE IF NOT EXISTS delivery_attempts (
			id TEXT PRIMARY KEY,
			report_id TEXT NOT NULL REFERENCES reports(id) ON DELETE CASCADE,
			channel TEXT NOT NULL CHECK (channel IN ('discord', 'telegram', 'email')),
			destination_key TEXT NOT NULL,
			status TEXT NOT NULL CHECK (status IN (
				'pending', 'sending', 'sent', 'retryable', 'failed_terminal'
			)),
			attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
			lease_expires_at TIMESTAMPTZ,
			next_attempt_at TIMESTAMPTZ,
			provider_message_id TEXT,
			safe_error_code TEXT,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			UNIQUE (report_id, channel, destination_key)
		)`,

		// Feedback and reviews
		`CREATE TABLE IF NOT EXISTS investigation_reviews (
			id TEXT PRIMARY KEY,
			incident_id TEXT NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
			run_id TEXT NOT NULL REFERENCES investigation_runs(id) ON DELETE CASCADE,
			evidence_id TEXT REFERENCES evidence(id) ON DELETE SET NULL,
			verdict TEXT NOT NULL CHECK (verdict IN ('correct', 'incorrect', 'partially_correct', 'unknown')),
			corrected_cause TEXT,
			notes TEXT,
			reviewer TEXT NOT NULL,
			reviewed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`CREATE INDEX IF NOT EXISTS investigation_reviews_incident_id_idx ON investigation_reviews (incident_id)`,
		`CREATE INDEX IF NOT EXISTS investigation_reviews_run_id_idx ON investigation_reviews (run_id)`,
		`CREATE INDEX IF NOT EXISTS investigation_reviews_evidence_id_idx ON investigation_reviews (evidence_id)`,

		`CREATE TABLE IF NOT EXISTS report_feedback (
			id TEXT PRIMARY KEY,
			report_id TEXT NOT NULL REFERENCES reports(id) ON DELETE CASCADE,
			rating TEXT NOT NULL CHECK (rating IN ('useful', 'not_useful', 'wrong', 'missing_context')),
			comments TEXT,
			reviewer TEXT NOT NULL,
			reviewed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`CREATE INDEX IF NOT EXISTS report_feedback_report_id_idx ON report_feedback (report_id)`,
	}

	for _, q := range queries {
		if _, err := db.Exec(q); err != nil {
			return fmt.Errorf("failed to execute migration: %w\nquery: %s", err, q)
		}
	}
	return nil
}

// cleanupTestData removes all rows from test tables to ensure a clean slate.
func cleanupTestData(db *sql.DB) error {
	tables := []string{
		"report_feedback", "investigation_reviews", "delivery_attempts",
		"reports", "report_requests", "evidence", "tool_executions",
		"investigation_runs", "investigation_outbox", "incident_events",
		"incidents", "events", "idempotency_records", "schema_migrations",
	}
	for _, table := range tables {
		if _, err := db.Exec(fmt.Sprintf("DELETE FROM %s", table)); err != nil {
			return fmt.Errorf("failed to clean table %s: %w", table, err)
		}
	}
	return nil
}

// getEnvOrDefault returns env var value or default.
func getEnvOrDefault(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}

// min returns the smaller of two integers.
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
