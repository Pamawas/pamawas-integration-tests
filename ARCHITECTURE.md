# Clean Integration Test Architecture

## Current Problems
1. Docker build context complexity with component sources
2. Multi-stage Dockerfile that's hard to debug
3. Workspace/checkout structure confusion
4. Test runs inside Docker, making debugging hard

## New Architecture

### CI Workflow (GitHub Actions)
1. Checkout all repos to workspace root
2. Build test binary: `go test -c -o integration-test .`
3. Start docker-compose services (postgres + 6 pamawas services)
4. Run test binary on runner (connects to localhost:5432, :8080-:8084)
5. Upload test results

### Docker Compose
- Only the 7 services (postgres + 6 pamawas services)
- NO test service
- Ports exposed to host

### Test Binary
- Runs on host (or simple container)
- Connects to services via localhost:PORT
- Uses environment variables for service URLs

This eliminates all Docker build context issues and makes tests debuggable.