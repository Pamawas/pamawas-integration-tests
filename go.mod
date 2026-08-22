module github.com/Pamawas/integration-tests

go 1.26

require (
	github.com/DATA-DOG/go-sqlmock v1.5.2
	github.com/Pamawas/pamawas-reporter v0.0.0
	github.com/google/uuid v1.6.0
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/prometheus/client_golang v1.24.1 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.70.1 // indirect
	github.com/prometheus/procfs v0.21.1 // indirect
	github.com/rs/zerolog v1.35.1 // indirect
	github.com/wneessen/go-mail v0.8.1 // indirect
	golang.org/x/crypto v0.54.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.40.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

replace (
	github.com/Pamawas/pamawas-correlator => /opt/data/workspace/pamawas/pamawas-correlator
	github.com/Pamawas/pamawas-ingest => /opt/data/workspace/pamawas/pamawas-ingest
	github.com/Pamawas/pamawas-reporter => /opt/data/workspace/pamawas/pamawas-reporter
	github.com/Pamawas/pamawas-scheduler => /opt/data/workspace/pamawas/pamawas-scheduler
)
