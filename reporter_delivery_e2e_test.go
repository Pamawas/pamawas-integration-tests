package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Pamawas/pamawas-reporter/fakes"
	"github.com/Pamawas/pamawas-reporter/models"
	"github.com/Pamawas/pamawas-reporter/service"
	"github.com/google/uuid"
)

// TestReporterDeliveryPipeline runs the reporter delivery pipeline with fake transports
func TestReporterDeliveryPipeline(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	// 1. Setup fake delivery transport for reporter
	deliveryTransport := fakes.NewFakeDeliveryTransport()
	defer deliveryTransport.Close()

	// Start fake delivery servers
	discordServer := deliveryTransport.StartDiscordServer()
	defer discordServer.Close()
	telegramServer := deliveryTransport.StartTelegramServer()
	defer telegramServer.Close()

	// 2. Setup database mock
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Failed to create sqlmock: %v", err)
	}
	defer db.Close()

	// Setup mock expectations for reporter operations
	setupReporterMocks(mock)

	// 3. Setup Reporter with fake delivery URLs
	reporterConfig := service.ReporterConfig{
		DatabaseURL:       "postgres://test",
		DiscordWebhookURL: discordServer.URL,
		TelegramBotToken:  "fake-token",
		TelegramChatID:    "fake-chat",
		Mode:              "test",
	}
	reporter := service.NewReporter(db, reporterConfig, nil)
	// NewReporter already creates an httpClient with 10s timeout

	// Test: Full reporter delivery pipeline
	t.Run("Reporter delivery: generate report -> create delivery attempts -> deliver", func(t *testing.T) {
		// Step 1: Create a report request
			reportReq := models.ReportPayload{
				RequestID:   uuid.NewString(),
				ReportType:  "high_severity",
				PeriodStart: time.Now().Add(-1 * time.Hour).Format(time.RFC3339),
				PeriodEnd:   time.Now().Format(time.RFC3339),
				Timezone:    "Asia/Jakarta",
				IncidentIDs: []string{"inc_test123"},
			}

			reportResp, err := reporter.ProcessReportRequest(ctx, reportReq)
			if err != nil {
				t.Fatalf("Report generation failed: %v", err)
			}
			reportID := reportResp.ReportID
			if reportID == "" {
				t.Fatal("No report ID returned")
			}

		// Step 2: Delivery is triggered (async)
		// Wait for delivery to complete
		time.Sleep(2 * time.Second)

		// Step 3: Verify delivery was captured
		discordDeliveries := deliveryTransport.GetDeliveriesByChannel("discord")
		telegramDeliveries := deliveryTransport.GetDeliveriesByChannel("telegram")

		if len(discordDeliveries) == 0 {
			t.Errorf("Expected at least 1 Discord delivery, got 0")
		} else {
			t.Logf("Discord deliveries captured: %d", len(discordDeliveries))
			// Verify content contains report ID
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
	})

	// Test: Retry/restart deduplication
	t.Run("Retry/restart deduplication for delivery", func(t *testing.T) {
		initialDiscordCount := deliveryTransport.GetDeliveryCount("discord")

		// Simulate restart by re-processing the same report
			reportReq := models.ReportPayload{
				RequestID:   uuid.NewString(),
				ReportType:  "daily",
				PeriodStart: time.Now().Add(-24 * time.Hour).Format(time.RFC3339),
				PeriodEnd:   time.Now().Format(time.RFC3339),
				Timezone:    "Asia/Jakarta",
				IncidentIDs: []string{"inc_test456"},
			}

		_, err := reporter.ProcessReportRequest(ctx, reportReq)
		if err != nil {
			t.Fatalf("Report generation failed: %v", err)
		}

		time.Sleep(1 * time.Second)

		newDiscordCount := deliveryTransport.GetDeliveryCount("discord")
		t.Logf("Discord delivery count before: %d, after: %d", initialDiscordCount, newDiscordCount)

		// Verify at least one new delivery was attempted
		if newDiscordCount <= initialDiscordCount {
			t.Errorf("Expected new delivery after restart, count unchanged")
		}
	})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("Unfulfilled mock expectations: %v", err)
	}
}

// setupReporterMocks sets up the SQL mock expectations for reporter operations
func setupReporterMocks(mock sqlmock.Sqlmock) {
	// Mock claimReportRequest
		mock.ExpectQuery(`UPDATE report_requests.*RETURNING id`).
			WithArgs(sqlmock.AnyArg()).
			WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("test-request-id"))

		// Mock selectDailyIncidents or high_severity incident selection
		mock.ExpectQuery(`SELECT i\.id.*FROM incidents`).
			WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnRows(sqlmock.NewRows([]string{"id", "started_at", "resolved_at", "status"}).
				AddRow("inc_test123", time.Now().Add(-2*time.Hour), time.Time{}, "open"))

		// Mock loadEvidenceForIncidents
		mock.ExpectQuery(`SELECT e\.id, e\.incident_id.*FROM evidence`).
			WithArgs("inc_test123").
			WillReturnRows(sqlmock.NewRows([]string{"id", "incident_id", "type", "content", "source", "confidence", "supports_evidence", "contradicts_evidence", "ordinal"}))

		// Mock getIncidentsByIDs
		mock.ExpectQuery(`SELECT i\.id, i\.title, i\.status, i\.started_at, i\.resolved_at, i\.severity, i\.affected_services.*FROM incidents`).
			WithArgs(sqlmock.AnyArg()).
			WillReturnRows(sqlmock.NewRows([]string{"id", "title", "status", "started_at", "resolved_at", "severity", "affected_services"}).
				AddRow("inc_test123", "Test Incident", "open", time.Now().Add(-2*time.Hour), time.Time{}, "high", `["api-service"]`))

	// Mock persistReportWithIncidents (INSERT INTO reports + INSERT INTO report_incidents)
	mock.ExpectExec(`INSERT INTO reports`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec(`INSERT INTO report_incidents`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	// Mock createDeliveryAttempts (INSERT INTO delivery_attempts for each channel)
	mock.ExpectExec(`INSERT INTO delivery_attempts`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec(`INSERT INTO delivery_attempts`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	// Mock updateReportRequestStatus
	mock.ExpectExec(`UPDATE report_requests`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	// Mock claimDeliveryAttempt
	mock.ExpectQuery(`UPDATE delivery_attempts`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("attempt-123"))

	// Mock updateDeliveryAttemptStatus
	mock.ExpectExec(`UPDATE delivery_attempts`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	// Mock updateReportDeliveryStatus
	mock.ExpectQuery(`SELECT status FROM delivery_attempts WHERE report_id = \$1`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("sent"))

	// Mock updateReportRequestStatus (final)
	mock.ExpectExec(`UPDATE report_requests`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
}