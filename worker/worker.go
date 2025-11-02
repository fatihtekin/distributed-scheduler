package worker

import (
	"context"
	"distributed-scheduler/internal/api"
	"distributed-scheduler/worker/executors"
	"log"
	"sync"
	"time"

	"distributed-scheduler/internal/models"
)

type Worker struct {
	id                  string
	defaultPollInterval time.Duration
	executors           map[models.JobType]executors.Executor
	jobConcurrencyGate  chan struct{}
	client              *api.Client
	runningJobs         *sync.WaitGroup // Track currently running jobs for graceful shutdown
}

const MaxConcurrentJobs = 5

type WorkerConfig struct {
	maxConcurrentJobs int
	pollInterval      time.Duration
	id                string
}

func NewWorkerConfig(id string, maxConcurrentJobs int, pollInterval time.Duration) WorkerConfig {
	return WorkerConfig{
		id:                id,
		maxConcurrentJobs: maxConcurrentJobs,
		pollInterval:      pollInterval,
	}
}

func NewWorker(client *api.Client, config WorkerConfig) *Worker {
	return &Worker{
		id:                  config.id,
		defaultPollInterval: config.pollInterval,
		jobConcurrencyGate:  make(chan struct{}, config.maxConcurrentJobs),
		client:              client,
		runningJobs:         &sync.WaitGroup{},
		executors: map[models.JobType]executors.Executor{
			models.JobTypeSleep: executors.NewSleepExecutor(),
		},
	}
}

func (w *Worker) Start(ctx context.Context) {
	log.Printf("Worker %s started with max concurrency %d, polling API", w.id, MaxConcurrentJobs)

	ticker := time.NewTicker(w.defaultPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("Worker %s context cancelled", w.id)
			return
		case <-ticker.C:
			log.Printf("Worker %s polling API", w.id)
			w.pollForJobs(ctx)
		}
	}
}

// pollForJobs runs a hot loop to acquire slots and fetch jobs until the
// concurrency limit is reached or the queue is empty.
func (w *Worker) pollForJobs(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Printf("Worker %s: Context cancelled. Shutting down poll loop.", w.id)
			return
		case w.jobConcurrencyGate <- struct{}{}:
			job, err := w.client.GetNextJob(ctx, w.id)
			if err != nil {
				log.Printf("Worker %s: Failed to get next job: %v", w.id, err)
				<-w.jobConcurrencyGate
				// rate-limit or any other server errors, just exit and let the next tick call again.
				// this is a very simplistic circuit breaker approach.
				return
			}
			if job == nil {
				log.Printf("Worker %s: No more jobs available.", w.id)
				<-w.jobConcurrencyGate
				return
			}

			log.Printf("Worker %s: Claimed job %s (type: %s).", w.id, job.ID, job.Type)
			go func(job *models.Job) {
				defer func() { <-w.jobConcurrencyGate }()
				w.run(ctx, job)
			}(job)
		}
	}
}

func (w *Worker) run(ctx context.Context, job *models.Job) {
	w.runningJobs.Add(1)
	defer w.runningJobs.Done()

	log.Printf("Worker %s: Processing job %s (type: %s, priority: %d)", w.id, job.ID, job.Type, job.Priority)

	executor, ok := w.executors[job.Type]
	if !ok {
		if err := w.client.CompleteJob(job.ID, models.StatusFailed); err != nil {
			log.Printf("Worker %s: Failed to run unknown job execution type: %v", w.id, err)
		}
		return
	}

	status := models.StatusCompleted
	err := executor.Execute(ctx, job)
	if err != nil {
		log.Printf("Worker %s: Job %s FAILED: %v", w.id, job.ID, err)
		status = models.StatusFailed
	}

	if err := w.client.CompleteJob(job.ID, status); err != nil {
		log.Printf("Worker %s: Failed to complete job: %v", w.id, err)
	}
}

func (w *Worker) Shutdown() {
	log.Printf("Worker %s: Initiating graceful shutdown. Waiting for active jobs...", w.id)
	ctx, cancelShutdown := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelShutdown()

	// We use a separate goroutine to wait on the WaitGroup,
	// allowing us to use the context timeout for the shutdown process.
	done := make(chan struct{})
	go func() {
		w.runningJobs.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Printf("Worker %s: All running jobs completed gracefully.", w.id)
	case <-ctx.Done():
		log.Printf("Worker %s: Timeout waiting for running jobs to complete. Forcing shutdown.", w.id)
	}
}
