package server

import (
	"context"
	"time"

	graphpkg "github.com/SkinnyPeteTheGiraffe/gographgo/pkg/graph"
)

// Thread represents a conversation/session container.
type Thread struct {
	ID        string         `json:"thread_id"`
	CreatedAt time.Time      `json:"created_at"`
	Metadata  map[string]any `json:"metadata,omitempty"`
}

// RunStatus is the status of a run lifecycle.
type RunStatus string

const (
	RunStatusPending     RunStatus = "pending"
	RunStatusRunning     RunStatus = "running"
	RunStatusSuccess     RunStatus = "success"
	RunStatusError       RunStatus = "error"
	RunStatusTimeout     RunStatus = "timeout"
	RunStatusInterrupted RunStatus = "interrupted"
)

// Run is an execution record within a thread.
type Run struct {
	ID          string         `json:"run_id"`
	ThreadID    string         `json:"thread_id"`
	AssistantID string         `json:"assistant_id,omitempty"`
	Status      RunStatus      `json:"status"`
	Input       map[string]any `json:"input,omitempty"`
	Output      map[string]any `json:"output,omitempty"`
	Error       string         `json:"error,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty"`
	CreatedAt   time.Time      `json:"created_at"`
	StartedAt   *time.Time     `json:"started_at,omitempty"`
	EndedAt     *time.Time     `json:"ended_at,omitempty"`
}

// RunEvent represents a streamable run event.
type RunEvent struct {
	Type      string         `json:"type"`
	ThreadID  string         `json:"thread_id"`
	RunID     string         `json:"run_id"`
	Timestamp time.Time      `json:"timestamp"`
	Payload   map[string]any `json:"payload,omitempty"`
}

// CreateThreadRequest creates a thread.
type CreateThreadRequest struct {
	ThreadID string         `json:"thread_id,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

// CreateRunRequest creates a run for a thread.
type CreateRunRequest struct {
	AssistantID       string         `json:"assistant_id,omitempty"`
	Input             map[string]any `json:"input,omitempty"`
	Command           map[string]any `json:"command,omitempty"`
	Metadata          map[string]any `json:"metadata,omitempty"`
	Config            map[string]any `json:"config,omitempty"`
	Context           map[string]any `json:"context,omitempty"`
	StreamMode        []string       `json:"stream_mode,omitempty"`
	StreamSubgraphs   bool           `json:"stream_subgraphs,omitempty"`
	StreamResumable   bool           `json:"stream_resumable,omitempty"`
	InterruptBefore   any            `json:"interrupt_before,omitempty"`
	InterruptAfter    any            `json:"interrupt_after,omitempty"`
	Webhook           string         `json:"webhook,omitempty"`
	MultitaskStrategy string         `json:"multitask_strategy,omitempty"`
	IfNotExists       string         `json:"if_not_exists,omitempty"`
	AfterSeconds      int            `json:"after_seconds,omitempty"`
	Durability        string         `json:"durability,omitempty"`
	Checkpoint        map[string]any `json:"checkpoint,omitempty"`
	CheckpointID      string         `json:"checkpoint_id,omitempty"`
}

// RunRequest is sent to the graph/runtime runner.
type RunRequest struct {
	ThreadID    string
	RunID       string
	AssistantID string
	Input       map[string]any
	Metadata    map[string]any
}

// RunResult is returned by the graph/runtime runner.
type RunResult struct {
	Output map[string]any
	State  map[string]any
}

// GraphRunner executes a run request.
type GraphRunner interface {
	Run(ctx context.Context, req RunRequest) (RunResult, error)
}

// GraphIntrospector resolves graph introspection data for assistant graph IDs.
type GraphIntrospector interface {
	GraphInfo(ctx context.Context, graphID string) (graphpkg.GraphInfo, error)
}

// StoreValue represents a value in the thread-level key-value store.
type StoreValue struct {
	Namespace string         `json:"namespace"`
	Key       string         `json:"key"`
	Value     map[string]any `json:"value"`
}

// ThreadState is the current state view for a thread.
type ThreadState struct {
	ThreadID     string         `json:"thread_id"`
	CheckpointID string         `json:"checkpoint_id,omitempty"`
	Values       map[string]any `json:"values,omitempty"`
	Metadata     map[string]any `json:"metadata,omitempty"`
	CreatedAt    *time.Time     `json:"created_at,omitempty"`
	ParentConfig map[string]any `json:"parent_checkpoint,omitempty"`
}

// Assistant represents an assistant resource.
type Assistant struct {
	ID          string         `json:"assistant_id"`
	GraphID     string         `json:"graph_id,omitempty"`
	Name        string         `json:"name,omitempty"`
	Description string         `json:"description,omitempty"`
	Config      map[string]any `json:"config,omitempty"`
	Context     map[string]any `json:"context,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty"`
	CreatedAt   time.Time      `json:"created_at"`
	UpdatedAt   time.Time      `json:"updated_at"`
}

// AssistantVersion describes a stored assistant version.
type AssistantVersion struct {
	Version   int            `json:"version"`
	Metadata  map[string]any `json:"metadata,omitempty"`
	CreatedAt time.Time      `json:"created_at"`
}

// Cron represents a scheduled run.
type Cron struct {
	ID          string         `json:"cron_id"`
	AssistantID string         `json:"assistant_id,omitempty"`
	ThreadID    string         `json:"thread_id,omitempty"`
	Schedule    string         `json:"schedule,omitempty"`
	Payload     map[string]any `json:"payload,omitempty"`
	Enabled     bool           `json:"enabled,omitempty"`
	NextRunDate *time.Time     `json:"next_run_date,omitempty"`
	EndTime     *time.Time     `json:"end_time,omitempty"`
	CreatedAt   time.Time      `json:"created_at"`
	UpdatedAt   time.Time      `json:"updated_at"`
}

// StoreItem is a global namespaced store value.
type StoreItem struct {
	Namespace []string       `json:"namespace"`
	Key       string         `json:"key"`
	Value     map[string]any `json:"value"`
	Score     *float64       `json:"score,omitempty"`
	CreatedAt *time.Time     `json:"created_at,omitempty"`
	UpdatedAt *time.Time     `json:"updated_at,omitempty"`
}

// Info exposes server capability metadata.
type Info struct {
	Version      string         `json:"version"`
	Capabilities map[string]any `json:"capabilities"`
}

// AssistantGraphResponse is the server response for assistant graph introspection.
type AssistantGraphResponse struct {
	AssistantID string             `json:"assistant_id"`
	GraphID     string             `json:"graph_id"`
	Graph       graphpkg.GraphInfo `json:"graph"`
	Mermaid     string             `json:"mermaid,omitempty"`
}

// AssistantSchemasResponse is the server response for assistant schema introspection.
type AssistantSchemasResponse struct {
	AssistantID string         `json:"assistant_id"`
	GraphID     string         `json:"graph_id"`
	Input       map[string]any `json:"input"`
	Output      map[string]any `json:"output"`
	Context     map[string]any `json:"context"`
}

// AssistantSubgraph describes a subgraph namespace exposed by an assistant graph.
type AssistantSubgraph struct {
	Name       string `json:"name"`
	ParentNode string `json:"parent_node"`
	Namespace  string `json:"namespace"`
}

// AssistantSubgraphsResponse is the server response for assistant subgraph introspection.
type AssistantSubgraphsResponse struct {
	AssistantID string              `json:"assistant_id"`
	GraphID     string              `json:"graph_id"`
	Subgraphs   []AssistantSubgraph `json:"subgraphs"`
}
