.PHONY: build test clean run-standalone run-scheduler run-worker help

build:
	go build -o scheduler .

test:
	go test ./... -v

test-coverage:
	go test ./... -cover -coverprofile=coverage.out
	go tool cover -html=coverage.out -o coverage.html

clean:
	rm -f scheduler *.db test_*.db coverage.out coverage.html

run-scheduler: build
	./scheduler -mode scheduler -port 8080 -db scheduler.db

run-worker: build
	@if [ -z "$(WORKER_ID)" ]; then \
		echo "Error: WORKER_ID required. Usage: make run-worker WORKER_ID=worker-1"; \
		exit 1; \
	fi
	./scheduler -mode worker -worker-id $(WORKER_ID) -api-url http://localhost:8080

deps:
	go mod download
	go mod tidy

fmt:
	go fmt ./...

help:
	@echo "Available targets:"
	@echo "  build           - Build the application"
	@echo "  test            - Run tests"
	@echo "  test-coverage   - Run tests with coverage"
	@echo "  clean           - Remove build artifacts"
	@echo "  run-standalone  - Run in standalone mode"
	@echo "  run-scheduler   - Run scheduler only"
	@echo "  run-worker      - Run worker (requires WORKER_ID=<id>)"
	@echo "  deps            - Install dependencies"
	@echo "  fmt             - Format code"
