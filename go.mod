module github.com/Pamawas/pamawas-integration-tests

go 1.26

require (
	github.com/Pamawas/pamawas-correlator v0.0.0
	github.com/Pamawas/pamawas-ingest v0.0.0
	github.com/Pamawas/pamawas-reporter v0.0.0
	github.com/Pamawas/pamawas-scheduler v0.0.0
	github.com/Pamawas/pamawas-schema v0.0.0
	github.com/google/uuid v1.6.0
	github.com/testcontainers/testcontainers-go v0.27.0
	github.com/testcontainers/testcontainers-go/modules/postgres v0.27.0
	github.com/testcontainers/testcontainers-go/wait v0.27.0
)

replace (
	github.com/Pamawas/pamawas-correlator => /opt/data/workspace/pamawas/pamawas-correlator
	github.com/Pamawas/pamawas-ingest => /opt/data/workspace/pamawas/pamawas-ingest
	github.com/Pamawas/pamawas-reporter => /opt/data/workspace/pamawas/pamawas-reporter
	github.com/Pamawas/pamawas-scheduler => /opt/data/workspace/pamawas/pamawas-scheduler
	github.com/Pamawas/pamawas-schema => /opt/data/workspace/pamawas/pamawas-schema
)