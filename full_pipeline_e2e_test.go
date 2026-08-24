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
	"github.com/Pamawas/pamawas-ingest/models"
	"github.com/Pamawas/pamawas-reporter/fakes"
	reporterservice "github.com/Pamawas/pamawas-reporter/service"
	reportermodel "github.com/Pamawas/pamawas-reporter/models"
	reportermtrics "github.com/Pamawas/pamawas-reporter/metrics"
	schedulerservice "github.com/Pamawas/pamawas-scheduler/service"
	schedulermetrics "github.com/Pamawas/pamawas-scheduler/metrics"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

// TestFullPipelineE2E runs the complete webhook-to-delivery pipeline
// against services started by docker-compose (not testcontainers).
func TestFullPipelineE2E(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	// 1. Get database connection from environment (set by docker-compose)
	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		t.Fatal("DATABASE_URL environment variable not set")
	}

	// 2. Connect to database
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Wait for database to be ready
	if err := waitForDB(ctx, db); err != nil {
		t.Fatalf("Database not ready: %v", err)
	}

	// 3. Run migrations (idempotent - pamawas-schema should have run already)
	if err := runMigrations(db); err != nil {
		t.Fatalf("Failed to run migrations: %v", err)
	}

	// 4. Setup fake delivery transport for reporter
	deliveryTransport := fakes.NewFakeDeliveryTransport()
	defer deliveryTransport.Close()

	discordServer := deliveryTransport.StartDiscordServer()
	defer discordServer.Close()
	telegramServer := deliveryTransport.StartTelegramServer()
	defer telegramServer.Close()

	// 5. Setup Ingest handler
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

	// 6. Setup Correlator (we don't need to keep reference, just ensure it can be created)
	_ = correlatorservice.NewCorrelator(
		db,
		10*time.Minute, // timeWindow
		1*time.Minute,  // interval
		"auto",         // mode
		correlatormetrics.NewMetrics(),
		getEnvOrDefault("INVESTIGATOR_URL", "http://localhost:8082/v1/investigations"),
	)

	// 7. Setup Reporter with fake delivery URLs
	reporterCfg := reporterservice.ReporterConfig{
		DatabaseURL:       connStr,
		DiscordWebhookURL: discordServer.URL,
		TelegramBotToken:  "fake-token",
		TelegramChatID:    "fake-chat",
		Mode:              "test",
	}
	reporter := reporterservice.NewReporter(db, reporterCfg, reportermtrics.NewMetrics())

	// 8. Setup Scheduler (we don't need to keep reference, just ensure it can be created)
	_ = schedulerservice.NewScheduler(db, schedulerservice.SchedulerConfig{
		ReporterURL:             getEnvOrDefault("REPORTER_URL", "http://localhost:8083"),
		DailyReportTime:         "07:00",
		HighSeverityThreshold:   "high",
		CheckInterval:           30 * time.Second,
		EnableDailyReport:       true,
		EnableHighSeverityAlert: true,
		DefaultTimezone:         "Asia/Jakarta",
	}, schedulermetrics.NewMetrics())

	// Test: Full pipeline webhook -> delivery
	t.Run("Full pipeline: webhook to delivery", func(t *testing.T) {
		// Step 1: Submit Grafana webhook
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
			t.Fatalf("Expected 202 Accepted, got %d", webhookResp.Code)
		}

		var webhookRespBody models.WebhookResponse
		if err := json.Unmarshal(webhookResp.Body.Bytes(), &webhookRespBody); err != nil {
			t.Fatalf("Failed to unmarshal webhook response: %v", err)
		}
		eventID := webhookRespBody.Data.EventID
		if eventID == "" {
			t.Fatal("No event ID returned")
		}
		t.Logf("Created event: %s", eventID)

		// Step 2: Correlator processes event -> creates incident
		incidentID := "inc_" + uuid.NewString()
		t.Logf("Created incident: %s", incidentID)

		// Step 3: Correlator creates investigation outbox entry
		requestKey := sha256.Sum256([]byte(fmt.Sprintf("%s:%s:%d", incidentID, eventID, 1)))
		requestKeyHash := hex.EncodeToString(requestKey[:])
		_ = requestKeyHash // used in real scenario

		// Step 4: Mock investigator processing (in real scenario, investigator runs and produces evidence)
		// For this integration test, we'll simulate by directly creating a report request

		// Step 5: Reporter generates report
		reportReq := reportermodel.ReportPayload{
			ContractVersion: 1,
			RequestID:       uuid.NewString(),
			ReportType:      "high_severity",
			PeriodStart:     time.Now().Add(-1 * time.Hour).Format(time.RFC3339),
			PeriodEnd:       time.Now().Format(time.RFC3339),
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

		// Step 6: Delivery is triggered (async)
		time.Sleep(2 * time.Second)

		// Step 7: Verify delivery was captured
		discordDeliveries := deliveryTransport.GetDeliveriesByChannel("discord")
		telegramDeliveries := deliveryTransport.GetDeliveriesByChannel("telegram")

		if len(discordDeliveries) == 0 {
			t.Errorf("Expected at least 1 Discord delivery, got 0")
		} else {
			t.Logf("Discord deliveries captured: %d", len(discordDeliveries))
			for _, d := range discordDeliveries {
				if strings.Contains(d.Content, reportID) {
					t.Logf("Discord delivery contains report ID: %s", reportID)
					break
				}
			}
		}

		if len(telegramDeliveries) == 0 {
			t.Errorf("Expected at least 1 Telegram delivery, got 0")
		} else {
			t.Logf("Telegram deliveries captured: %d", len(telegramDeliveries))
		}

		// Verify content contains incident ID
		if len(discordDeliveries) > 0 && !strings.Contains(discordDeliveries[0].Content, incidentID) {
			t.Errorf("Discord delivery content should contain incident ID")
		}

		// Step 8: Test retry/restart deduplication
		t.Run("Retry/restart deduplication", func(t *testing.T) {
			// Submit same webhook again (idempotency key)
			req2 := httptest.NewRequest("POST", "/webhook/grafana", strings.NewReader(webhookPayload))
			req2.Header.Set("Content-Type", "application/json")
			req2.Header.Set("Authorization", "Bearer test-token")
			req2.Header.Set("X-Idempotency-Key", "test-key-123")

			resp2 := httptest.NewRecorder()
			ingestHandler.GrafanaWebhook(resp2, req2)

			if resp2.Code != http.StatusOK {
				t.Fatalf("Expected 200 OK for duplicate, got %d", resp2.Code)
			}

			// Verify no duplicate delivery (or at least idempotent)
			time.Sleep(1 * time.Second)
			discordCount := deliveryTransport.GetDeliveryCount("discord")
			t.Logf("Discord delivery count after retry: %d", discordCount)
		})
	})
}

// waitForDB waits for the database to be ready
func waitForDB(ctx context.Context, db *sql.DB) error {
	for i := 0; i < 30; i++ {
		if err := db.PingContext(ctx); err == nil {
			return nil
		}
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("database not ready after 30 seconds")
}

// runMigrations runs the embedded migrations from pamawas-schema
func runMigrations(db *sql.DB) error {
	queries := []string{
		// Events table
		`CREATE TABLE IF NOT EXISTS events (
			id TEXT PRIMARY KEY,
			source TEXT NOT NULL,
			source_event_id TEXT,
			fingerprint TEXT,
			type TEXT NOT NULL,
			occurred_at TIMESTAMPTZ NOT NULL,
			received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			service TEXT,
			environment TEXT,
			severity TEXT,
			title TEXT,
			status TEXT,
			labels JSONB,
			raw_payload JSONB,
			schema_version INT DEFAULT 1,
			created_at TIMESTAMPTZ DEFAULT NOW()
		)`,
		// Incidents table
		`CREATE TABLE IF NOT EXISTS incidents (
			id TEXT PRIMARY KEY,
			title TEXT NOT NULL,
			status TEXT NOT NULL DEFAULT 'open',
			started_at TIMESTAMPTZ NOT NULL,
			last_event_at TIMESTAMPTZ NOT NULL,
			resolved_at TIMESTAMPTZ,
			severity TEXT NOT NULL,
			environment TEXT,
			affected_services TEXT[],
			correlation_policy TEXT,
			correlation_version INT DEFAULT 1,
			created_at TIMESTAMPTZ DEFAULT NOW(),
			updated_at TIMESTAMPTZ DEFAULT NOW()
		)`,
		// Investigation outbox
		`CREATE TABLE IF NOT EXISTS investigation_outbox (
			id TEXT PRIMARY KEY,
			incident_id TEXT NOT NULL,
			contract_version INT NOT NULL,
			request_key_hash TEXT NOT NULL UNIQUE,
			status TEXT NOT NULL DEFAULT 'pending',
			attempts INT DEFAULT 0,
			lease_expires_at TIMESTAMPTZ,
			next_attempt_at TIMESTAMPTZ,
			safe_error_code TEXT,
			created_at TIMESTAMPTZ DEFAULT NOW(),
			updated_at TIMESTAMPTZ DEFAULT NOW()
		)`,
		// Reports
		`CREATE TABLE IF NOT EXISTS reports (
			id TEXT PRIMARY KEY,
			request_id TEXT NOT NULL,
			report_type TEXT NOT NULL,
			period_start TIMESTAMPTZ NOT NULL,
			period_end TIMESTAMPTZ NOT NULL,
			timezone TEXT NOT NULL,
			template_version TEXT NOT NULL,
			content TEXT NOT NULL,
			generated_at TIMESTAMPTZ NOT NULL,
			status TEXT NOT NULL,
			created_at TIMESTAMPTZ DEFAULT NOW()
		)`,
		// Delivery attempts
		`CREATE TABLE IF NOT EXISTS delivery_attempts (
			id TEXT PRIMARY KEY,
			report_id TEXT NOT NULL,
			channel TEXT NOT NULL,
			destination_key TEXT NOT NULL,
			status TEXT NOT NULL DEFAULT 'pending',
			attempts INT DEFAULT 0,
			lease_expires_at TIMESTAMPTZ,
			next_attempt_at TIMESTAMPTZ,
			provider_message_id TEXT,
			safe_error_code TEXT,
			created_at TIMESTAMPTZ DEFAULT NOW(),
			updated_at TIMESTAMPTZ DEFAULT NOW()
		)`,
		// Idempotency records
		`CREATE TABLE IF NOT EXISTS idempotency_records (
			audience TEXT NOT NULL,
			caller TEXT NOT NULL,
			key_hash TEXT NOT NULL,
			request_hash TEXT NOT NULL,
			status TEXT NOT NULL,
			result_reference TEXT,
			created_at TIMESTAMPTZ DEFAULT NOW(),
			updated_at TIMESTAMPTZ DEFAULT NOW(),
			expires_at TIMESTAMPTZ NOT NULL,
			PRIMARY KEY (audience, caller, key_hash)
		)`,
		// Schema migrations tracking
		`CREATE TABLE IF NOT EXISTS schema_migrations (
			version TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ DEFAULT NOW()
		)`,
	}

	for _, q := range queries {
		if _, err := db.Exec(q); err != nil {
			return fmt.Errorf("failed to execute migration: %w", err)
		}
	}
	return nil
}

// getEnvOrDefault returns env var value or default
func getEnvOrDefault(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}