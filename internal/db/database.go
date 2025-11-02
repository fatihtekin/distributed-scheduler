package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"distributed-scheduler/internal/models"

	_ "github.com/mattn/go-sqlite3"
)

type DBClient interface {
	CreateJob(ctx context.Context, job *models.Job) error
	GetJob(ctx context.Context, id string) (*models.Job, error)
	GetNextJob(ctx context.Context, workerID string, jobTimeOutDuration time.Duration) (*models.Job, error)
	CompleteJob(ctx context.Context, id string, status models.JobStatus) error
	FailTimedOutJob(ctx context.Context, job *models.Job) error
	ResetJobToPending(ctx context.Context, job *models.Job) error
	GetTimedOutJobs(ctx context.Context) ([]*models.Job, error)
	Close() error
}

func NewDatabase(ctx context.Context, path string) (DBClient, error) {
	return newSqliteDatabase(ctx, path)
}

type database struct {
	db *sql.DB
}

func newSqliteDatabase(ctx context.Context, path string) (*database, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(1 * time.Minute)

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	d := &database{db: db}
	if err := d.initSQLiteSchema(ctx); err != nil {
		return nil, err
	}

	return d, nil
}

func (d *database) initSQLiteSchema(ctx context.Context) error {
	schema := `
    CREATE TABLE IF NOT EXISTS jobs (
       id TEXT PRIMARY KEY,
       type TEXT NOT NULL,
       payload TEXT NOT NULL,
       priority INTEGER NOT NULL DEFAULT 5,
       status TEXT NOT NULL DEFAULT 'PENDING',
       retry_count INTEGER NOT NULL DEFAULT 0,
       worker_id TEXT,
       created_at TIMESTAMP NOT NULL,
       started_at TIMESTAMP,
       completed_at TIMESTAMP,
       timeout_at TIMESTAMP,
       version INTEGER NOT NULL DEFAULT 0
    );
    CREATE INDEX IF NOT EXISTS idx_status_priority ON jobs(status, priority DESC, created_at);
    CREATE INDEX IF NOT EXISTS idx_timeout ON jobs(timeout_at) WHERE timeout_at IS NOT NULL;
    `
	_, err := d.db.ExecContext(ctx, schema)
	return err
}

func (d *database) CreateJob(ctx context.Context, job *models.Job) error {
	payload, err := json.Marshal(job.Payload)
	if err != nil {
		return err
	}

	_, err = d.db.ExecContext(ctx, `
       INSERT INTO jobs (id, type, payload, priority, status, retry_count, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?)
    `, job.ID, job.Type, payload, job.Priority, job.Status, job.RetryCount, job.CreatedAt)
	if err != nil {
		return fmt.Errorf("failed to insert job %s: %v", job.ID, err)
	}
	return nil
}

func (d *database) GetJob(ctx context.Context, id string) (*models.Job, error) {
	row := d.db.QueryRowContext(ctx, `
       SELECT id, type, payload, priority, status, retry_count, version,
              worker_id, created_at, started_at, completed_at, timeout_at
       FROM jobs WHERE id = ?
    `, id)

	job, err := scanJobRow(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return job, err
}

const batchSize = 10
const maxBatchRetries = 3

func (d *database) GetNextJob(ctx context.Context, workerID string, jobTimeOutDuration time.Duration) (*models.Job, error) {
	for attempt := 0; attempt < maxBatchRetries; attempt++ {
		candidates, err := d.selectJobCandidates(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to select candidates: %w", err)
		}
		if len(candidates) == 0 {
			return nil, nil
		}
		for _, candidate := range candidates {
			if claimed, err := d.tryClaimJob(ctx, workerID, candidate, jobTimeOutDuration); err != nil {
				return nil, fmt.Errorf("failed to claim job %s: %w", candidate.ID, err)
			} else if claimed {
				return candidate, nil
			}
		}
	}
	return nil, nil
}

func (d *database) selectJobCandidates(ctx context.Context) ([]*models.Job, error) {
	rows, err := d.db.QueryContext(ctx, `
       SELECT id, type, payload, priority, status, retry_count, version,
              worker_id, created_at, started_at, completed_at, timeout_at
       FROM jobs
       WHERE status = ?
       ORDER BY priority ASC, created_at ASC
       LIMIT ?
    `, models.StatusPending, batchSize)
	if err != nil {
		return nil, fmt.Errorf("failed to select job batch: %w", err)
	}
	defer rows.Close()

	return scanJobBatch(rows)
}

func (d *database) tryClaimJob(ctx context.Context, workerID string, candidate *models.Job, timeOutAfter time.Duration) (bool, error) {
	timeoutAt := time.Now().Add(timeOutAfter)
	result, err := d.db.ExecContext(ctx, `
        UPDATE jobs
        SET status = ?, worker_id = ?, started_at = ?, timeout_at = ?, version = version + 1
        WHERE id = ? AND version = ? AND status = ?
    `,
		models.StatusRunning,
		workerID,
		time.Now(),
		timeoutAt,
		candidate.ID,
		candidate.Version,
		models.StatusPending,
	)
	if err != nil {
		return false, err
	}
	rowsAffected, _ := result.RowsAffected()
	return rowsAffected == 1, nil
}

func (d *database) CompleteJob(ctx context.Context, id string, status models.JobStatus) error {
	_, err := d.db.ExecContext(ctx, `
       UPDATE jobs 
       SET status = ?,  completed_at = ?, timeout_at = NULL, version = version + 1
       WHERE id = ?
    `, status, time.Now(), id)
	if err != nil {
		return fmt.Errorf("failed to complete job %s: %w", id, err)
	}
	return nil
}

func (d *database) FailTimedOutJob(ctx context.Context, job *models.Job) error {
	_, err := d.db.ExecContext(ctx, `
       UPDATE jobs 
       SET status = ?,  completed_at = ?, timeout_at = NULL, version = version + 1
       WHERE id = ? and status = ? and version = ?
    `, models.StatusFailed, time.Now(), job.ID, models.StatusRunning, job.Version)
	if err != nil {
		return fmt.Errorf("failed to complete job %s: %w", job.ID, err)
	}
	return nil
}

func (d *database) ResetJobToPending(ctx context.Context, job *models.Job) error {
	res, err := d.db.ExecContext(ctx, `
       UPDATE jobs 
       SET retry_count = retry_count + 1, status = ?, started_at = NULL, timeout_at = NULL, worker_id = NULL, version = version + 1
       WHERE id = ? AND status = ? AND version = ?
    `, models.StatusPending, job.ID, models.StatusRunning, job.Version)
	if err != nil {
		return fmt.Errorf("ResetJobToPending job %s: %w", job.ID, err)
	}
	if rows, _ := res.RowsAffected(); rows == 0 {
		log.Printf("Job %s is already in pending state", job.ID)
	}

	return nil
}

func (d *database) GetTimedOutJobs(ctx context.Context) ([]*models.Job, error) {
	rows, err := d.db.QueryContext(ctx, `
		SELECT id, version, worker_id, retry_count
		FROM jobs
		WHERE status = ? AND timeout_at IS NOT NULL AND timeout_at < ?
		limit ?
	`, models.StatusRunning, time.Now(), 100)
	if err != nil {
		return nil, fmt.Errorf("query timed out jobs: %w", err)
	}
	defer rows.Close()

	var jobs []*models.Job
	for rows.Next() {
		var job models.Job
		if err := rows.Scan(&job.ID, &job.Version, &job.WorkerID, &job.RetryCount); err != nil {
			return nil, fmt.Errorf("scan timed out job: %w", err)
		}
		jobs = append(jobs, &job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate timed out jobs: %w", err)
	}
	return jobs, nil
}

func scanJobRow(scanner interface{ Scan(dest ...any) error }) (*models.Job, error) {
	var job models.Job
	var payloadStr string
	var startedAt, completedAt, timeoutAt sql.NullTime
	var workerID sql.NullString

	err := scanner.Scan(
		&job.ID, &job.Type, &payloadStr, &job.Priority, &job.Status, &job.RetryCount,
		&job.Version, &workerID, &job.CreatedAt, &startedAt, &completedAt, &timeoutAt,
	)
	if err != nil {
		return nil, err
	}

	job.Payload = json.RawMessage(payloadStr)
	if workerID.Valid {
		job.WorkerID = workerID.String
	}
	if startedAt.Valid {
		job.StartedAt = &startedAt.Time
	}
	if completedAt.Valid {
		job.CompletedAt = &completedAt.Time
	}
	if timeoutAt.Valid {
		job.TimeoutAt = &timeoutAt.Time
	}

	return &job, nil
}

// scanJobBatch scans multiple job rows into Job slices
func scanJobBatch(rows *sql.Rows) ([]*models.Job, error) {
	var jobs []*models.Job
	for rows.Next() {
		job, err := scanJobRow(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan job: %w", err)
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return jobs, nil
}

func (d *database) Close() error {
	return d.db.Close()
}
