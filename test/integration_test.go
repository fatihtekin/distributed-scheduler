package test

import (
	"context"
	"distributed-scheduler/internal/api"
	"distributed-scheduler/internal/db"
	"distributed-scheduler/internal/models"
	"distributed-scheduler/internal/scheduler"
	"distributed-scheduler/worker"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

// BDDContext is the wrapper that holds all live components (DB, Server, Scheduler, Worker)
// and provides the Given/When/Then methods for test clarity.
type BDDContext struct {
	t           *testing.T
	ctx         context.Context
	cancel      context.CancelFunc
	dbPath      string
	database    db.DBClient
	apiServer   *api.Server
	apiClient   *api.Client
	sched       *scheduler.Scheduler
	worker      *worker.Worker
	workerID    string
	port        int
	runningJobs []*models.Job
	wg          sync.WaitGroup // To wait for goroutines (server, scheduler, worker)
}

// NewBDDContext sets up the temporary database and basic context for a test run.
func NewBDDContext(t *testing.T) *BDDContext {
	// 1. Setup DB
	dbPath := fmt.Sprintf("./test-%s.db", uuid.New().String())
	ctx, cancel := context.WithCancel(context.Background())
	database, err := db.NewDatabase(ctx, dbPath)
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}

	// 3. Find Free Port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to find free port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()

	// 4. Setup Server and Client
	server := api.NewServer(database, api.NewServerConfig(port, 10*time.Second))
	apiURL := fmt.Sprintf("http://127.0.0.1:%d", port)
	client := newTestClient(apiURL)

	return &BDDContext{
		t:         t,
		ctx:       ctx,
		cancel:    cancel,
		dbPath:    dbPath,
		database:  database,
		apiServer: server,
		apiClient: client,
		port:      port,
		workerID:  "1",
	}
}

// GivenTheSystemIsRunning performs the "GIVEN" steps: starts the server, scheduler, and worker.
func (c *BDDContext) GivenTheSystemIsRunning() *BDDContext {
	c.t.Helper()
	c.startAPIServer()
	c.startScheduler()
	c.startWorker()
	return c
}

// Shutdown gracefully cleans up all resources.
func (c *BDDContext) Shutdown() {
	// Cancel the context to stop all loops (worker, scheduler)
	c.cancel()

	// Shut down API server gracefully
	c.apiServer.Shutdown()

	// Wait for background goroutines (scheduler, server) to stop
	c.wg.Wait()

	// Wait for the worker's active jobs to finish (if started)
	if c.worker != nil {
		c.worker.Shutdown()
	}

	// Close DB and delete the temporary file
	c.database.Close()
	os.Remove(c.dbPath)
}

// ======================================================================
// GIVEN/SETUP HELPERS (INTERNAL)
// ======================================================================

func (c *BDDContext) startAPIServer() {
	c.t.Helper()
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		if err := c.apiServer.Start(); err != http.ErrServerClosed {
			c.t.Logf("API Server stopped with error: %v", err)
		}
	}()

	// Wait for the server to be available
	for i := 0; i < 20; i++ {
		req, _ := http.NewRequestWithContext(c.ctx, "GET", fmt.Sprintf("http://127.0.0.1:%d/health", c.port), nil)
		resp, err := http.DefaultClient.Do(req)
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}

	c.t.Fatal("API Server failed to start up within timeout")
}

func (c *BDDContext) startScheduler() {
	c.t.Helper()
	c.sched = scheduler.NewScheduler(c.database, scheduler.NewSchedulerConfig(1, 100*time.Millisecond))
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.sched.Start(c.ctx)
	}()
}

func (c *BDDContext) startWorker() {
	c.t.Helper()
	c.worker = worker.NewWorker(c.apiClient, worker.NewWorkerConfig(c.workerID, 5, time.Second))
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.worker.Start(c.ctx)
	}()
}

// ======================================================================
// WHEN/ACTION HELPERS (BDD)
// ======================================================================

// WhenIQueueAJob acts as the system input, submitting a job via the API.
func (c *BDDContext) WhenIQueueAJob(jobType models.JobType, priority int, payload string) (*models.Job, *BDDContext) {
	c.t.Helper()
	req := models.CreateJobRequest{
		Type:     jobType,
		Priority: priority,
		Payload:  json.RawMessage(payload),
	}
	job, err := c.apiClient.CreateJob(req)
	if err != nil {
		c.t.Fatalf("Failed to create job: %v", err)
	}
	c.runningJobs = append(c.runningJobs, job)
	return job, c
}

// WhenIWaitForJobToComplete polls the DB until the job reaches a terminal state (COMPLETED or FAILED).
func (c *BDDContext) WhenIWaitForJobToComplete(jobID string, timeout time.Duration) *BDDContext {
	c.t.Helper()
	start := time.Now()
	for time.Since(start) < timeout {
		job, err := c.GetJob(jobID)
		if err != nil {
			c.t.Fatalf("Error retrieving job %s: %v", jobID, err)
		}
		if job == nil {
			c.t.Fatalf("Job %s not found in DB", jobID)
		}

		if job.Status == models.StatusCompleted || job.Status == models.StatusFailed {
			return c
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatalf("Timeout waiting for job %s to complete. Final status: %s", jobID, c.MustGetJob(jobID).Status)
	return c
}

// WhenIWaitForJobToRunning polls the DB until the job reaches RUNNING state.
func (c *BDDContext) WhenIWaitForJobToRunning(jobID string, timeout time.Duration) *BDDContext {
	c.t.Helper()
	start := time.Now()
	for time.Since(start) < timeout {
		job, err := c.GetJob(jobID)
		if err != nil {
			c.t.Fatalf("Error retrieving job %s: %v", jobID, err)
		}
		if job == nil {
			c.t.Fatalf("Job %s not found in DB", jobID)
		}

		if job.Status == models.StatusRunning {
			return c
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatalf("Timeout waiting for job %s to complete. Final status: %s", jobID, c.MustGetJob(jobID).Status)
	return c
}

// ======================================================================
// THEN/ASSERTION HELPERS (BDD)
// ======================================================================

// getJobViaAPI fetches a job from the scheduler's /jobs/{id} REST endpoint.
func (c *BDDContext) GetJob(jobID string) (*models.Job, error) {
	url := fmt.Sprintf("http://127.0.0.1:%d/jobs/%s", c.port, jobID)
	req, err := http.NewRequestWithContext(c.ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("received non-OK status: %d", resp.StatusCode)
	}

	var job models.Job
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}
	return &job, nil
}

// MustGetJob retrieves the job from the REST API, failing the test if it cannot be found.
func (c *BDDContext) MustGetJob(jobID string) *models.Job {
	c.t.Helper()
	// CHANGE: Use API instead of DB
	job, err := c.GetJob(jobID)
	if err != nil {
		c.t.Fatalf("Failed to retrieve job %s via API: %v", jobID, err)
	}
	if job == nil {
		c.t.Fatalf("Job %s not found via API", jobID)
	}
	return job
}

// ThenTheJobShouldBe asserts the final job status.
func (c *BDDContext) ThenTheJobShouldBe(jobID string, status models.JobStatus) *BDDContext {
	c.t.Helper()
	job := c.MustGetJob(jobID)
	if job.Status != status {
		c.t.Errorf("Job %s status is %s, expected %s", jobID, job.Status, status)
	}
	return c
}

// ThenTheJobCompletesWithin combines waiting for completion and asserting success.
func (c *BDDContext) ThenTheJobCompletesWithin(jobID string, timeout time.Duration) *BDDContext {
	c.t.Helper()
	return c.
		WhenIWaitForJobToComplete(jobID, timeout).
		ThenTheJobShouldBe(jobID, models.StatusCompleted)
}

// ThenTheJobRetryCountShouldBe asserts the final retry count.
func (c *BDDContext) ThenTheJobRetryCountShouldBe(jobID string, count int) *BDDContext {
	c.t.Helper()
	job := c.MustGetJob(jobID)
	if job.RetryCount != count {
		c.t.Errorf("Job %s retry count is %d, expected %d", jobID, job.RetryCount, count)
	}
	return c
}

// ======================================================================
// TEST UTILITIES (NOT BDD STEPS)
// ======================================================================

// newTestClient is a simplified client for submitting jobs during tests.
func newTestClient(baseURL string) *api.Client {
	return api.NewClient(baseURL, &http.Client{Timeout: 5 * time.Second})
}

// ======================================================================
// TEST SCENARIOS (EACH TEST IS A SELF-CONTAINED SCENARIO)
// ======================================================================

func TestIntegration_SuccessfulJobLifecycle(t *testing.T) {
	// GIVEN a fully operational distributed system (API, Scheduler, Worker)
	ctx := NewBDDContext(t).GivenTheSystemIsRunning()
	defer ctx.Shutdown()

	const shortDuration = 1
	job, _ := ctx.
		// WHEN: I queue a short sleep job (1s) with normal priority.
		WhenIQueueAJob(models.JobTypeSleep, 10, fmt.Sprintf(`{"duration": %d}`, shortDuration))

	// THEN: The system should process it and it should successfully complete.
	ctx.ThenTheJobCompletesWithin(job.ID, 10*time.Second).
		ThenTheJobRetryCountShouldBe(job.ID, 0)
}

func TestIntegration_HighPriorityJob_PreemptsLowPriorityJob(t *testing.T) {
	// GIVEN a fully operational distributed system
	ctx := NewBDDContext(t).GivenTheSystemIsRunning()
	defer ctx.Shutdown()

	// WHEN: I queue a low priority job, followed immediately by a high priority job.
	jobLow, _ := ctx.WhenIQueueAJob(models.JobTypeSleep, 9, `{"duration": 1}`)  // Priority 9 (Low)
	jobHigh, _ := ctx.WhenIQueueAJob(models.JobTypeSleep, 1, `{"duration": 1}`) // Priority 1 (High)

	// THEN: We assert that the High Priority job is picked up and completed first.
	// Since the worker is constantly polling, the high priority job should be claimed first.
	ctx.ThenTheJobCompletesWithin(jobHigh.ID, 5*time.Second)

	// THEN: The low priority job should complete shortly after.
	ctx.ThenTheJobCompletesWithin(jobLow.ID, 8*time.Second)
}

func TestIntegration_ImmediateFailureOnUnknownExecutor(t *testing.T) {
	// GIVEN a fully operational distributed system
	ctx := NewBDDContext(t).GivenTheSystemIsRunning()
	defer ctx.Shutdown()

	const unknownJobType models.JobType = "unknown_type"
	job, _ := ctx.
		// WHEN: I queue an unknown job type that has no registered executor on the worker.
		WhenIQueueAJob(unknownJobType, 50, `{"data": "test"}`)

	// THEN: The worker should claim the job, immediately recognize the missing executor, and mark it as FAILED.
	ctx.WhenIWaitForJobToComplete(job.ID, 5*time.Second).
		ThenTheJobShouldBe(job.ID, models.StatusFailed).
		ThenTheJobRetryCountShouldBe(job.ID, 0) // Should not be retried since it failed immediately on dispatch
}

func TestIntegration_JobTimeoutAndRetry(t *testing.T) {
	// GIVEN a fully operational distributed system
	ctx := NewBDDContext(t).GivenTheSystemIsRunning()
	defer ctx.Shutdown()

	// The job is set to timeout in 10s in the database implementation.
	// We set the executor duration to be longer (20s) to guarantee a timeout.
	// The scheduler's retry count is set to 1 (meaning 2 total attempts).
	const longDuration = 25

	job, _ := ctx.
		// WHEN: I queue a job designed to take 20s (guaranteed to timeout after 10s).
		WhenIQueueAJob(models.JobTypeSleep, 10, fmt.Sprintf(`{"duration": %d}`, longDuration))

	// THEN: Wait long enough for the job to run twice and fail.
	// Wait Time Calculation: ~10s (1st run timeout) + 1s (scheduler check buffer) + ~10s (2nd run timeout) + 4s (buffer) = 25s
	ctx.WhenIWaitForJobToComplete(job.ID, 25*time.Second).
		// THEN: The job should be FAILED after reaching the maximum retry limit (1 retry).
		ThenTheJobShouldBe(job.ID, models.StatusFailed).
		ThenTheJobRetryCountShouldBe(job.ID, 1)
}

func TestIntegration_WorkerGracefulShutdown(t *testing.T) {
	// GIVEN a fully operational distributed system
	ctx := NewBDDContext(t).GivenTheSystemIsRunning()
	// NOTE: We do NOT defer ctx.Shutdown() here, as we want to control the shutdown sequence

	// WHEN: I queue a job designed to take a long time (5s).
	const longDuration = 5
	job, _ := ctx.WhenIQueueAJob(models.JobTypeSleep, 10, fmt.Sprintf(`{"duration": %d}`, longDuration))

	// Wait for the job to start running
	ctx.WhenIWaitForJobToRunning(job.ID, 3*time.Second)
	ctx.ThenTheJobShouldBe(job.ID, models.StatusRunning)

	workerDone := make(chan struct{})
	go func() {
		ctx.worker.Shutdown()
		close(workerDone)
	}()

	// THEN: The shutdown should block until the long-running job completes.
	// We wait for the job to complete first.
	ctx.WhenIWaitForJobToComplete(job.ID, 6*time.Second)

	// THEN: The job should be marked as COMPLETED.
	ctx.ThenTheJobShouldBe(job.ID, models.StatusCompleted)

	// THEN: The worker's Shutdown method should return now that the job is done.
	select {
	case <-workerDone:
		// Success: Shutdown completed after the job was finished.
	case <-time.After(2 * time.Second):
		t.Fatal("Worker did not shut down within the expected time after the job completed.")
	}

	// The rest of the system (API/Scheduler) is still running, shut it down now.
	ctx.worker = nil
	ctx.Shutdown()
}

func TestIntegration_WorkerConcurrencyLimit(t *testing.T) {
	// GIVEN a fully operational distributed system
	ctx := NewBDDContext(t).GivenTheSystemIsRunning()
	defer ctx.Shutdown()

	// The SleepExecutor will run for 3 seconds
	const jobDuration = 3
	// Queue 7 jobs (5 running, 2 pending, assuming MaxConcurrentJobs = 5)
	const totalJobs = worker.MaxConcurrentJobs + 2

	var jobs []*models.Job
	for i := 0; i < totalJobs; i++ {
		// WHEN: I queue more jobs (7) than the worker's max concurrency (5).
		job, _ := ctx.WhenIQueueAJob(models.JobTypeSleep, 1, fmt.Sprintf(`{"duration": %d}`, jobDuration))
		jobs = append(jobs, job)
	}

	// THEN: Wait for only the concurrent limit (5 jobs) to transition to RUNNING.
	// We wait slightly less than the job duration to ensure they don't complete.
	// This relies on the worker successfully claiming the jobs.
	// The maximum duration for a job is 25s, so a 2s wait is safe.
	ctx.WhenIWaitForJobToRunning(jobs[worker.MaxConcurrentJobs-1].ID, 3*time.Second)

	// THEN: The first 5 jobs should be RUNNING.
	for i := 0; i < worker.MaxConcurrentJobs; i++ {
		ctx.ThenTheJobShouldBe(jobs[i].ID, models.StatusRunning)
	}

	// THEN: The overflow jobs (6th and 7th) should still be PENDING.
	for i := worker.MaxConcurrentJobs; i < totalJobs; i++ {
		ctx.ThenTheJobShouldBe(jobs[i].ID, models.StatusPending)
	}

	// WHEN: I wait for all jobs to complete (the total time should be close to 'jobDuration' x 2 batches).
	// Wait time: 3s (Batch 1) + 3s (Batch 2) + buffer
	ctx.WhenIWaitForJobToComplete(jobs[totalJobs-1].ID, (jobDuration*2+2)*time.Second)

	// THEN: All jobs should be COMPLETED.
	for _, job := range jobs {
		ctx.ThenTheJobShouldBe(job.ID, models.StatusCompleted)
	}
}
