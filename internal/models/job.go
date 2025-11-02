package models

import (
	"encoding/json"
	"time"
)

type JobStatus string

const (
	StatusPending   JobStatus = "PENDING"
	StatusRunning   JobStatus = "RUNNING"
	StatusCompleted JobStatus = "COMPLETED"
	StatusFailed    JobStatus = "FAILED"
)

type JobType string

const (
	JobTypeSleep JobType = "sleep"
)

// Job TODO add fields like result to store the job execution error or the outcome of the job
type Job struct {
	ID          string          `json:"id"`
	Type        JobType         `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	Priority    int             `json:"priority"`
	Status      JobStatus       `json:"status"`
	RetryCount  int             `json:"retry_count"`
	WorkerID    string          `json:"worker_id,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	StartedAt   *time.Time      `json:"started_at,omitempty"`
	CompletedAt *time.Time      `json:"completed_at,omitempty"`
	TimeoutAt   *time.Time      `json:"timeout_at,omitempty"`
	Version     int             `json:"version"`
}

type SleepJobPayload struct {
	Duration int `json:"duration"`
}

type CreateJobRequest struct {
	Type     JobType         `json:"type"`
	Payload  json.RawMessage `json:"payload"`
	Priority int             `json:"priority"`
}

type CompleteJobRequest struct {
	Status JobStatus `json:"status,omitempty"`
}
