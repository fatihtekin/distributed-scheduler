package executors

import (
	"context"
	"distributed-scheduler/internal/models"
	"encoding/json"
	"fmt"
	"time"
)

type SleepExecutor struct{}

func NewSleepExecutor() *SleepExecutor { return &SleepExecutor{} }

func (e *SleepExecutor) Execute(ctx context.Context, job *models.Job) error {
	var payload models.SleepJobPayload
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		return fmt.Errorf("invalid sleep payload: %w", err)
	}

	if payload.Duration <= 0 || payload.Duration > 25 {
		return fmt.Errorf("invalid duration: must be between 1 and 25 seconds not %d ", payload.Duration)
	}

	time.Sleep(time.Duration(payload.Duration) * time.Second)
	return nil
}
