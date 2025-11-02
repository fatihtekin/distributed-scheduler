package scheduler

import (
	"context"
	"log"
	"time"

	"distributed-scheduler/internal/db"
)

type Scheduler struct {
	db            db.DBClient
	checkInterval time.Duration
	retryCount    int
	stopCh        chan struct{}
}

type SchedulerConfig struct {
	retryCount    int
	checkInterval time.Duration
}

func NewSchedulerConfig(retryCount int, checkInterval time.Duration) SchedulerConfig {
	return SchedulerConfig{
		retryCount:    retryCount,
		checkInterval: checkInterval,
	}
}

func NewScheduler(database db.DBClient, config SchedulerConfig) *Scheduler {
	return &Scheduler{
		db:            database,
		checkInterval: config.checkInterval,
		retryCount:    config.retryCount,
		stopCh:        make(chan struct{}),
	}
}

func (s *Scheduler) Start(ctx context.Context) {
	log.Println("Scheduler started")
	ticker := time.NewTicker(s.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("Scheduler shutting down")
			return
		case <-s.stopCh:
			log.Println("Scheduler stop requested")
			return
		case <-ticker.C:
			s.checkTimeouts(ctx)
		}
	}
}

func (s *Scheduler) checkTimeouts(ctx context.Context) {
	jobs, err := s.db.GetTimedOutJobs(ctx)
	if err != nil {
		log.Printf("Failed to get timed out jobs: %v", err)
		return
	}
	for _, job := range jobs {
		select {
		case <-ctx.Done():
			log.Printf("INFO: Stopping job timeout check loop early due to context cancellation: %v", ctx.Err())
			return
		case <-s.stopCh:
			log.Println("Scheduler stop requested")
			return
		default:
		}
		log.Printf("Job %s timed out (worker: %s) retryCount: %d", job.ID, job.WorkerID, s.retryCount)
		if job.RetryCount < s.retryCount {
			if err := s.db.ResetJobToPending(ctx, job); err != nil {
				log.Printf("Failed to retry job %s: %v", job.ID, err)
			} else {
				log.Printf("Job %s reset to PENDING for retry", job.ID)
			}
		} else {
			if err := s.db.FailTimedOutJob(ctx, job); err != nil {
				log.Printf("Failed to mark job %s as failed: %v", job.ID, err)
			} else {
				log.Printf("Job %s marked as FAILED after timeout retry", job.ID)
			}
		}
	}
}

func (w *Scheduler) Shutdown() {
	log.Printf("Scheduler initiating graceful shutdown.")
	close(w.stopCh)
}
