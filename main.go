package main

import (
	"context"
	"distributed-scheduler/worker"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"distributed-scheduler/internal/api"
	"distributed-scheduler/internal/db"
	"distributed-scheduler/internal/scheduler"
)

func main() {
	mode := flag.String("mode", "scheduler", "Mode: scheduler, worker, or standalone")
	port := flag.Int("port", 8080, "HTTP server port")
	dbPath := flag.String("db", "scheduler.db", "Database path")
	workerID := flag.String("worker-id", "", "Worker ID (required for worker mode)")
	apiURL := flag.String("api-url", "http://localhost:8080", "API URL for workers")
	jobTimeOutDuration := flag.Duration("job-timeout", 1*time.Minute, "Job timeout duration")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	switch *mode {
	case "scheduler":
		runScheduler(ctx, *dbPath, *port, *jobTimeOutDuration, sigChan)
	case "worker":
		if *workerID == "" {
			log.Fatal("Worker ID is required in worker mode (use -worker-id flag)")
		}
		runWorker(ctx, *workerID, *apiURL, sigChan)
	default:
		log.Fatalf("Invalid mode: %s", *mode)
	}
}

func runScheduler(ctx context.Context, dbPath string, port int, jobTimeOutDuration time.Duration, sigChan chan os.Signal) {
	log.Println("Starting scheduler mode...")
	database, err := db.NewDatabase(dbPath)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer database.Close()

	sched := scheduler.NewScheduler(database, 5*time.Second)
	go sched.Start(ctx)

	server := api.NewServer(database, port, jobTimeOutDuration)
	go func() {
		if err := server.Start(); err != nil {
			log.Printf("Server error: %v", err)
			sigChan <- syscall.SIGTERM
		}
	}()

	log.Printf("Scheduler running on port %d", port)
	<-sigChan
	log.Println("Shutting down scheduler...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	server.Shutdown(shutdownCtx)
	log.Printf("Server has fully terminated.")
}

func runWorker(ctx context.Context, workerID, apiURL string, sigChan chan os.Signal) {
	log.Printf("Starting worker mode (ID: %s, API: %s)...", workerID, apiURL)

	w := worker.NewWorker(workerID, api.NewDefaultClient(apiURL))

	workerCtx, cancelWorker := context.WithCancel(ctx)
	go w.Start(workerCtx)
	log.Printf("Worker %s running", workerID)

	<-sigChan
	log.Println("Shutting down worker...")
	cancelWorker()

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()

	w.Shutdown(shutdownCtx)
	log.Printf("Worker %s has fully terminated.", workerID)
}
