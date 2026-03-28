package server

import (
	"context"
	"time"

	graphpkg "github.com/SkinnyPeteTheGiraffe/gographgo/pkg/graph"
)

// Thread represents a conversation/session container.
type Thread struct {
	CreatedAt time.Time      `json:"created_at"`
	Metadata  map[string]any `json:"metadata,omitempty"`
	ID        string         `json:"thread_id"`
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
	CreatedAt   time.Time      `json:"created_at"`
	Input       map[string]any `json:"input,omitempty"`
	Output      map[string]any `json:"output,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty"`
	StartedAt   *time.Time     `json:"started_at,omitempty"`
	EndedAt     *time.Time     `json:"ended_at,omitempty"`
	ID          string         `json:"run_id"`
	ThreadID    string         `json:"thread_id"`
	AssistantID string         `json:"assistant_id,omitempty"`
	Status      RunStatus      `json:"status"`
	Error       string         `json:"error,omitempty"`
}

// RunEvent represents a streamable run event.
type RunEvent struct {
	Timestamp time.Time      `json:"timestamp"`
	Payload   map[string]any `json:"payload,omitempty"`
	Type      string         `json:"type"`
	ThreadID  string         `json:"thread_id"`
	RunID     string         `json:"run_id"`
}

// CreateThreadRequest creates a thread.
type CreateThreadRequest struct {
	Metadata map[string]any `json:"metadata,omitempty"`
	ThreadID string         `json:"thread_id,omitempty"`
}

// CreateRunRequest creates a run for a thread.
type CreateRunRequest struct {
	InterruptBefore   any            `json:"interrupt_before,omitempty"`
	InterruptAfter    any            `json:"interrupt_after,omitempty"`
	Input             map[string]any `json:"input,omitempty"`
	Command           map[string]any `json:"command,omitempty"`
	Metadata          map[string]any `json:"metadata,omitempty"`
	Config            map[string]any `json:"config,omitempty"`
	Context           map[string]any `json:"context,omitempty"`
	Checkpoint        map[string]any `json:"checkpoint,omitempty"`
	MultitaskStrategy string         `json:"multitask_strategy,omitempty"`
	Webhook           string         `json:"webhook,omitempty"`
	AssistantID       string         `json:"assistant_id,omitempty"`
	IfNotExists       string         `json:"if_not_exists,omitempty"`
	Durability        string         `json:"durability,omitempty"`
	CheckpointID      string         `json:"checkpoint_id,omitempty"`
	StreamMode        []string       `json:"stream_mode,omitempty"`
	AfterSeconds      int            `json:"after_seconds,omitempty"`
	StreamResumable   bool           `json:"stream_resumable,omitempty"`
	StreamSubgraphs   bool           `json:"stream_subgraphs,omitempty"`
}

// RunRequest is sent to the graph/runtime runner.
type RunRequest struct {
	Input       map[string]any
	Metadata    map[string]any
	ThreadID    string
	RunID       string
	AssistantID string
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
	Value     map[string]any `json:"value"`
	Namespace string         `json:"namespace"`
	Key       string         `json:"key"`
}

// ThreadState is the current state view for a thread.
type ThreadState struct {
	Values       map[string]any `json:"values,omitempty"`
	Metadata     map[string]any `json:"metadata,omitempty"`
	CreatedAt    *time.Time     `json:"created_at,omitempty"`
	ParentConfig map[string]any `json:"parent_checkpoint,omitempty"`
	ThreadID     string         `json:"thread_id"`
	CheckpointID string         `json:"checkpoint_id,omitempty"`
}

// Assistant represents an assistant resource.
type Assistant struct {
	CreatedAt   time.Time      `json:"created_at"`
	UpdatedAt   time.Time      `json:"updated_at"`
	Config      map[string]any `json:"config,omitempty"`
	Context     map[string]any `json:"context,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty"`
	ID          string         `json:"assistant_id"`
	GraphID     string         `json:"graph_id,omitempty"`
	Name        string         `json:"name,omitempty"`
	Description string         `json:"description,omitempty"`
}

// AssistantVersion describes a stored assistant version.
type AssistantVersion struct {
	CreatedAt time.Time      `json:"created_at"`
	Metadata  map[string]any `json:"metadata,omitempty"`
	Version   int            `json:"version"`
}

// Cron represents a scheduled run.
type Cron struct {
	CreatedAt   time.Time      `json:"created_at"`
	UpdatedAt   time.Time      `json:"updated_at"`
	Payload     map[string]any `json:"payload,omitempty"`
	NextRunDate *time.Time     `json:"next_run_date,omitempty"`
	EndTime     *time.Time     `json:"end_time,omitempty"`
	ID          string         `json:"cron_id"`
	AssistantID string         `json:"assistant_id,omitempty"`
	ThreadID    string         `json:"thread_id,omitempty"`
	Schedule    string         `json:"schedule,omitempty"`
	Enabled     bool           `json:"enabled,omitempty"`
}

// StoreItem is a global namespaced store value.
type StoreItem struct {
	Value     map[string]any `json:"value"`
	Score     *float64       `json:"score,omitempty"`
	CreatedAt *time.Time     `json:"created_at,omitempty"`
	UpdatedAt *time.Time     `json:"updated_at,omitempty"`
	Key       string         `json:"key"`
	Namespace []string       `json:"namespace"`
}

// Info exposes server capability metadata.
type Info struct {
	Capabilities map[string]any `json:"capabilities"`
	Version      string         `json:"version"`
}

// AssistantGraphResponse is the server response for assistant graph introspection.
type AssistantGraphResponse struct {
	AssistantID string             `json:"assistant_id"`
	GraphID     string             `json:"graph_id"`
	Mermaid     string             `json:"mermaid,omitempty"`
	Graph       graphpkg.GraphInfo `json:"graph"`
}

// AssistantSchemasResponse is the server response for assistant schema introspection.
type AssistantSchemasResponse struct {
	Input       map[string]any `json:"input"`
	Output      map[string]any `json:"output"`
	Context     map[string]any `json:"context"`
	AssistantID string         `json:"assistant_id"`
	GraphID     string         `json:"graph_id"`
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
