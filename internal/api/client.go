package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"distributed-scheduler/internal/models"
)

// APIError wraps HTTP response status codes so we can apply backpressure policies.
type APIError struct {
	StatusCode int
	Message    string
}

func (e APIError) Error() string {
	return fmt.Sprintf("status %d: %s", e.StatusCode, e.Message)
}

// Client defines the API interface the worker uses.
type Client struct {
	baseURL    string
	HTTPClient *http.Client
}

func NewClient(baseURL string, httpClient *http.Client) *Client {
	return &Client{
		baseURL:    baseURL,
		HTTPClient: httpClient,
	}
}

var defaultHTTPClient = &http.Client{
	Timeout: 5 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:    10,
		IdleConnTimeout: 2 * time.Second,
	},
}

func NewDefaultClient(baseURL string) *Client {
	return &Client{
		baseURL:    baseURL,
		HTTPClient: defaultHTTPClient,
	}
}

func (c *Client) GetNextJob(workerID string) (*models.Job, error) {
	req, err := http.NewRequest("GET", c.baseURL+"/next-job", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("X-Worker-ID", workerID)

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("API request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	msg := strings.TrimSpace(string(body))

	// TODO handle rate limit errors 429 and use circuit breaker with exponential backoff mechanism
	switch resp.StatusCode {
	case http.StatusOK:
		var job models.Job
		if err := json.Unmarshal(body, &job); err != nil {
			return nil, fmt.Errorf("failed to parse job JSON: %s", msg)
		}
		return &job, nil

	case http.StatusNotFound:
		return nil, nil
	default:
		return nil, APIError{StatusCode: resp.StatusCode, Message: msg}
	}
}

func (c *Client) CompleteJob(jobID string, status models.JobStatus) error {
	reqBody, err := json.Marshal(models.CompleteJobRequest{Status: status})
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	resp, err := c.HTTPClient.Post(
		fmt.Sprintf("%s/jobs/%s/complete", c.baseURL, jobID),
		"application/json",
		bytes.NewBuffer(reqBody),
	)
	if err != nil {
		return fmt.Errorf("API request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return APIError{StatusCode: resp.StatusCode, Message: fmt.Sprintf("unexpected status: %d", resp.StatusCode)}
	}

	return nil
}

func (c *Client) CreateJob(req models.CreateJobRequest) (*models.Job, error) {
	reqBody, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	resp, err := c.HTTPClient.Post(
		fmt.Sprintf("%s/jobs", c.baseURL),
		"application/json",
		bytes.NewBuffer(reqBody),
	)
	if err != nil {
		return nil, fmt.Errorf("API request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("unexpected status: %d, response: %s", resp.StatusCode, string(body))
	}

	var job models.Job
	if err := json.Unmarshal(body, &job); err != nil {
		return nil, fmt.Errorf("failed to parse job JSON: %w", err)
	}

	return &job, nil
}
