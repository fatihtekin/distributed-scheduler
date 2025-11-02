package executors

import (
	"context"
	"distributed-scheduler/internal/models"
)

type Executor interface {
	Execute(ctx context.Context, job *models.Job) error
}
