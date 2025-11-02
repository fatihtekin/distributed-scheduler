# Architecture Documentation

## System Overview

Distributed task scheduler with API-based worker coordination.

## Key Design: Workers Use API Only

**CRITICAL**: Workers do NOT access the database directly.

```
┌─────────┐                    ┌─────────────┐
│ Worker 1│──GET /next-job────▶│             │
│         │◀────JSON Job───────│  Scheduler  │
│         │                    │      +      │──┐
│         │──POST /complete───▶│   API       │  │ SQLite
└─────────┘                    │   Server    │◀─┘ Database
                               │             │
┌─────────┐                    │   (Only     │
│ Worker 2│──HTTP API─────────▶│   scheduler │
└─────────┘                    │   accesses  │
                               │   database) │
                               └─────────────┘
```

## Benefits

**Scalability**: Add workers/servers as many as needed on any machine/datacenter

**Network Distribution**: Workers run anywhere with HTTP access

**Clean Separation**: Scheduler owns scheduling, Server is responsible for task dist and workers are only responsible executing the jobs and reporting back

**Concurrency**: Workers can process in parallel using go routines with a limit

## Components

### Scheduler & Server
- Manages job queue
- Detects timeouts
- Handles retries
- Hosts HTTP API
- ONLY component that accesses database

### Workers
- Poll `/next-job` every 2 seconds
- Execute jobs (HTTP, sleep, file operations)
- Report results via `/jobs/{id}/complete`
- Pure HTTP clients - no database code

### Database
- SQLite (or PostgreSQL for production)
- Accessed ONLY by scheduler/server
- Uses optimistic locking for job update conflicts

## Testing
- Integration tests: Full API endpoints
- No mocks needed - real SQLite database

## TODOs

1. Replace SQLite with PostgreSQL
2. Load balance API servers
3. Add API authentication/SSL support
4. Implement metrics/monitoring
5. Use structured logging
6. Use circuit breaker with exponential backoff mechanism
7. Unit-testing to cover exceptional error scenarios
8. Chaos testing
9. Load Testing
10. More int-tests to cover different concurrency scenarios for the worker
11. Split worker and server into different projects
12. Improvement to ***GetNextJob*** to support batching
13. Use swagger style api auto client generation 