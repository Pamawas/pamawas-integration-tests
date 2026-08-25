module github.com/Pamawas/pamawas-integration-tests

go 1.26

require (
	github.com/Pamawas/pamawas-correlator v0.0.0
	github.com/Pamawas/pamawas-ingest v0.0.0
	github.com/Pamawas/pamawas-reporter v0.0.0
	github.com/Pamawas/pamawas-scheduler v0.0.0
	github.com/Pamawas/pamawas-schema v0.0.0
	github.com/google/uuid v1.6.0
	github.com/lib/pq v1.10.9
)

replace (
	github.com/Pamawas/pamawas-correlator => ../pamawas-correlator
	github.com/Pamawas/pamawas-ingest => ../pamawas-ingest
	github.com/Pamawas/pamawas-reporter => ../pamawas-reporter
	github.com/Pamawas/pamawas-scheduler => ../pamawas-scheduler
	github.com/Pamawas/pamawas-schema => ../pamawas-schema
)