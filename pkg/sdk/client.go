package sdk

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/checkpoint"
)

// Client provides API access to a LangGraph-compatible server.
type Client struct {
	baseURL    *url.URL
	httpClient *http.Client
	headers    map[string]string

	Threads    *ThreadsClient
	Runs       *RunsClient
	Assistants *AssistantsClient
	Cron       *CronClient
	Store      *StoreClient
}

// ThreadsClient manages thread APIs.
type ThreadsClient struct {
	client *Client
}

// RunsClient manages run APIs.
type RunsClient struct {
	client *Client
}

// AssistantsClient manages assistant APIs.
type AssistantsClient struct {
	client *Client
}

// CronClient manages scheduled run APIs.
type CronClient struct {
	client *Client
}

// StoreClient manages global store APIs.
type StoreClient struct {
	client *Client
}

// Config configures the SDK client.
type Config struct {
	HTTPClient *http.Client
	Headers    map[string]string
	BaseURL    string
	APIKey     string
}

// New creates a new SDK client.
func New(cfg Config) (*Client, error) {
	if cfg.BaseURL == "" {
		return nil, fmt.Errorf("sdk: BaseURL must not be empty")
	}
	u, err := url.Parse(strings.TrimRight(cfg.BaseURL, "/"))
	if err != nil {
		return nil, fmt.Errorf("sdk: parse BaseURL: %w", err)
	}
	hc := cfg.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: 30 * time.Second}
	}
	apiKey := strings.TrimSpace(cfg.APIKey)
	if apiKey == "" {
		apiKey = strings.TrimSpace(os.Getenv("GOGRAPHGO_API_KEY"))
	}
	headers := make(map[string]string, len(cfg.Headers)+1)
	for key, value := range cfg.Headers {
		headers[key] = value
	}
	if apiKey != "" {
		if _, ok := headers["Authorization"]; !ok {
			headers["Authorization"] = "Bearer " + apiKey
		}
	}

	client := &Client{baseURL: u, httpClient: hc, headers: headers}
	client.Threads = &ThreadsClient{client: client}
	client.Runs = &RunsClient{client: client}
	client.Assistants = &AssistantsClient{client: client}
	client.Cron = &CronClient{client: client}
	client.Store = &StoreClient{client: client}
	return client, nil
}

// CreateThread creates a thread.
func (c *Client) CreateThread(ctx context.Context, threadID string, metadata map[string]any) (*Thread, error) {
	return c.Threads.Create(ctx, ThreadCreateRequest{ThreadID: threadID, Metadata: metadata})
}

// GetThread gets a thread by ID.
func (c *Client) GetThread(ctx context.Context, threadID string) (*Thread, error) {
	return c.Threads.Get(ctx, threadID)
}

// GetInfo returns server capability metadata.
func (c *Client) GetInfo(ctx context.Context) (*Info, error) {
	var out Info
	if err := c.doJSON(ctx, http.MethodGet, "/v1/info", nil, &out, http.StatusOK); err != nil {
		return nil, err
	}
	return &out, nil
}

// CreateRun creates and starts a run.
func (c *Client) CreateRun(
	ctx context.Context,
	threadID string,
	assistantID string,
	input map[string]any,
	metadata map[string]any,
) (*Run, error) {
	return c.Runs.Create(ctx, threadID, RunCreateRequest{AssistantID: assistantID, Input: input, Metadata: metadata})
}

// CreateRunWithOptions creates and starts a run with full run options.
func (c *Client) CreateRunWithOptions(ctx context.Context, threadID string, req RunCreateRequest) (*Run, error) {
	return c.Runs.Create(ctx, threadID, req)
}

// GetRun gets a run by ID.
func (c *Client) GetRun(ctx context.Context, threadID, runID string) (*Run, error) {
	return c.Runs.Get(ctx, threadID, runID)
}

// WaitRun polls until a run reaches a terminal status.
func (c *Client) WaitRun(ctx context.Context, threadID, runID string, interval time.Duration) (*Run, error) {
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		run, err := c.GetRun(ctx, threadID, runID)
		if err != nil {
			return nil, err
		}
		if isTerminalRunStatus(run.Status) {
			return run, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

func isTerminalRunStatus(status RunStatus) bool {
	switch status {
	case RunStatusSuccess, RunStatusError, RunStatusTimeout, RunStatusInterrupted:
		return true
	}
	legacy := string(status)
	return legacy == "completed" || legacy == "failed"
}

// StreamRunEvents streams server-sent run events.
func (c *Client) StreamRunEvents(ctx context.Context, threadID, runID string) (<-chan RunEvent, <-chan error) {
	return c.Runs.Events(ctx, threadID, runID)
}

// StreamRunWithOptions creates and streams run parts with full run options.
func (c *Client) StreamRunWithOptions(
	ctx context.Context,
	threadID string,
	req RunStreamRequest,
) (<-chan StreamPart, <-chan error) {
	return c.Runs.Stream(ctx, threadID, req)
}

// StreamRunWithOptionsTyped creates and streams typed run parts with full options.
func (c *Client) StreamRunWithOptionsTyped(
	ctx context.Context,
	threadID string,
	req RunStreamRequest,
) (<-chan StreamPartV2, <-chan error) {
	return c.Runs.StreamTyped(ctx, threadID, req)
}

// GetThreadState retrieves the latest thread state.
func (c *Client) GetThreadState(ctx context.Context, threadID string) (*ThreadState, error) {
	return c.Threads.GetState(ctx, threadID)
}

// ListThreadHistory returns checkpoint history for a thread.
func (c *Client) ListThreadHistory(ctx context.Context, threadID string) ([]checkpoint.CheckpointTuple, error) {
	return c.Threads.ListHistory(ctx, threadID)
}

// PutStoreValue writes a thread-scoped store value.
func (c *Client) PutStoreValue(ctx context.Context, threadID, namespace, key string, value map[string]any) (*StoreValue, error) {
	var out StoreValue
	body := map[string]any{"value": value}
	p := "/v1/threads/" + threadID + "/store/" + namespace + "/" + key
	if err := c.doJSON(ctx, http.MethodPut, p, body, &out, http.StatusOK); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetStoreValue reads a thread-scoped store value.
func (c *Client) GetStoreValue(ctx context.Context, threadID, namespace, key string) (*StoreValue, error) {
	var out StoreValue
	p := "/v1/threads/" + threadID + "/store/" + namespace + "/" + key
	if err := c.doJSON(ctx, http.MethodGet, p, nil, &out, http.StatusOK); err != nil {
		return nil, err
	}
	return &out, nil
}
