# Distributed Task Scheduler

Production-ready distributed task scheduler in Go with persistence, retry logic, and timeout handling.

## Features

✅ Priority-based job scheduling (1-10)

✅ Multiple job types (HTTP, Sleep, File)

✅ Automatic retry on failure

✅ Configurable timeout detection

✅ Distributed workers via HTTP API

✅ SQLite persistence

✅ Complete test coverage

✅ Docker support

## Architecture

**Workers communicate ONLY via HTTP API** - no direct database access.

```
Workers → HTTP API → Scheduler → SQLite Database
```

## Quick Start

### Standalone Mode (Easiest)
```bash
go mod download
go build -o scheduler .
./scheduler -mode standalone -port 8080
```

### Distributed Mode
```bash
# Terminal 1: Scheduler + API
./scheduler -mode scheduler -port 8080 -db scheduler.db

# Terminal 2: Worker 1
./scheduler -mode worker -worker-id worker-1 -api-url http://localhost:8080

# Terminal 3: Worker 2 (can be on different machine)
./scheduler -mode worker -worker-id worker-2 -api-url http://localhost:8080

# Optional Terminal 4: Scheduler + API
./scheduler -mode scheduler -port 8081 -db scheduler.db

# Optional Terminal 5: Worker 3 (can be on different machine)
./scheduler -mode worker -worker-id worker-3 -api-url http://localhost:8081

# Optional Terminal 6: Worker 4 (can be on different machine)
./scheduler -mode worker -worker-id worker-4 -api-url http://localhost:8081
```

### Docker
```bash
docker-compose up --build
```

## Usage

### Create a Job
```bash
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "type": "sleep",
    "payload": {"duration": 5},
    "priority": 7
  }'
```

### Check Job Status
```bash
curl http://localhost:8080/jobs/{job-id}
```

## API Endpoints

- `POST /jobs` - Create job
- `GET /jobs/{id}` - Get job status
- `GET /next-job` - Worker: get next available job
- `POST /jobs/{id}/complete` - Worker: report completion
- `GET /health` - Health check

## Job Types

### 1. Sleep Job
```json
{
  "type": "sleep",
  "payload": {"duration": 5},
  "priority": 7
}
```

## Testing

```bash
# Run all tests
go test ./... -v

# Test with coverage
make test-coverage
```

## Command-Line Flags

- `-mode` - Operation mode (scheduler, worker, standalone)
- `-port` - HTTP port (default: 8080)
- `-db` - Database path (default: scheduler.db)
- `-worker-id` - Worker identifier (required for worker mode)
- `-api-url` - Scheduler API URL (default: http://localhost:8080)

## Key Design Decisions

1. **API-First Architecture**: Workers only communicate via HTTP API
2. **No Shared Database**: Only scheduler accesses database
3. **Polling Model**: Workers poll every 2 seconds for jobs and processes concurrently
4. **Priority Scheduling**: Higher priority jobs execute first but not guaranteed
5. **Automatic Retry**: Failed jobs retry after configured time-out duration
6. **Timeout Detection**: Configured timeout with scheduler monitoring
7. **Worker-Backoff** Workers back off after any http error and the next time.ticker triggers polling again

See ARCHITECTURE.md for detailed design rationale.

## Production Deployment

For production:
- Use PostgreSQL instead of SQLite
- Add TLS/HTTPS for API
- Implement authentication for workers
- Add Prometheus metrics
- Use structured logging

## License

MIT
