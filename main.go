package main

import (
	"context"
	"distributed-scheduler/internal/scheduler"
	"distributed-scheduler/worker"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"distributed-scheduler/internal/api"
	"distributed-scheduler/internal/db"

	"github.com/oklog/run"
)

func main() {
	mode := flag.String("mode", "scheduler", "Mode: scheduler, worker, or standalone")
	port := flag.Int("port", 8080, "HTTP server port")
	dbPath := flag.String("db", "scheduler.db", "Database path")
	workerID := flag.String("worker-id", "", "Worker ID (required for worker mode)")
	apiURL := flag.String("api-url", "http://localhost:8080", "API URL for workers")
	jobTimeOutDuration := flag.Duration("job-timeout", 1*time.Minute, "Job timeout duration")
	retryCount := flag.Int("retry-count", 1, "Number of retry attempts")
	checkInterval := flag.Duration("check-interval", 1*time.Minute, "Check interval")
	maxConcurrentJobs := flag.Int("max-concurrent-jobs", 5, "Max concurrent jobs")
	pollInterval := flag.Duration("poll-interval", 1*time.Second, "Poll interval")

	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	switch *mode {
	case "scheduler":
		schedulerConfig := scheduler.NewSchedulerConfig(*retryCount, *checkInterval)
		serverConfig := api.NewServerConfig(*port, *jobTimeOutDuration)
		runScheduler(ctx, *dbPath, schedulerConfig, serverConfig)
	case "worker":
		if *workerID == "" {
			log.Fatal("Worker ID is required in worker mode (use -worker-id flag)")
		}
		workerConfig := worker.NewWorkerConfig(*workerID, *maxConcurrentJobs, *pollInterval)
		runWorker(ctx, *apiURL, workerConfig)
	default:
		log.Fatalf("Invalid mode: %s", *mode)
	}
}

func runScheduler(ctx context.Context, dbPath string, schedulerConfig scheduler.SchedulerConfig, serverConfig api.ServerConfig) {
	log.Println("Starting scheduler mode...")

	database, err := db.NewDatabase(ctx, dbPath)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer database.Close()

	scheduler := scheduler.NewScheduler(database, schedulerConfig)
	server := api.NewServer(database, serverConfig)

	var g run.Group
	g.Add(
		func() error {
			scheduler.Start(ctx)
			return nil
		},
		func(error) { scheduler.Shutdown() },
	)
	g.Add(
		func() error {
			if err := server.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Printf("Failed to start API server: %v", err)
				return err
			}
			log.Printf("API server stopped")
			return nil
		},
		func(error) { server.Shutdown() },
	)

	if err := g.Run(); err != nil && err.Error() != "interrupt" {
		log.Fatalf("Group exited with unexpected error: %v", err)
	}

	log.Println("Graceful shutdown complete. Exiting.")
}

func runWorker(ctx context.Context, apiURL string, workerConfig worker.WorkerConfig) {
	log.Printf("Starting worker mode API: %s", apiURL)
	w := worker.NewWorker(api.NewDefaultClient(apiURL), workerConfig)
	w.Start(ctx)
	log.Println("Shutting down worker...")
	w.Shutdown()
	log.Printf("Worker has fully terminated.")
}
