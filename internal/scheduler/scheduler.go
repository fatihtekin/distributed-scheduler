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
}

func NewScheduler(database db.DBClient, checkInterval time.Duration) *Scheduler {
	return &Scheduler{
		db:            database,
		checkInterval: checkInterval,
		retryCount:    1, // Default would be higher
	}
}

func (s *Scheduler) Start(ctx context.Context) {
	log.Println("Scheduler started")
	ticker := time.NewTicker(s.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Scheduler stopped")
			return
		case <-ticker.C:
			s.checkTimeouts()
		}
	}
}

func (s *Scheduler) checkTimeouts() {
	jobs, err := s.db.GetTimedOutJobs()
	if err != nil {
		log.Printf("Failed to get timed out jobs: %v", err)
		return
	}
	for _, job := range jobs {
		log.Printf("Job %s timed out (worker: %s) retryCount: %d", job.ID, job.WorkerID, s.retryCount)
		if job.RetryCount < s.retryCount {
			if err := s.db.ResetJobToPending(job); err != nil {
				log.Printf("Failed to retry job %s: %v", job.ID, err)
			} else {
				log.Printf("Job %s reset to PENDING for retry", job.ID)
			}
		} else {
			if err := s.db.FailTimedOutJob(job); err != nil {
				log.Printf("Failed to mark job %s as failed: %v", job.ID, err)
			} else {
				log.Printf("Job %s marked as FAILED after timeout retry", job.ID)
			}
		}
	}
}
