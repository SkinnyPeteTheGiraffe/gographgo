// Package sdk provides the official Go client for LangGraph-compatible server APIs.
package sdk

import (
	"encoding/json"
	"time"
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

// ThreadState is the current state view for a thread.
type ThreadState struct {
	ThreadID     string         `json:"thread_id"`
	CheckpointID string         `json:"checkpoint_id,omitempty"`
	Values       map[string]any `json:"values,omitempty"`
	Metadata     map[string]any `json:"metadata,omitempty"`
	CreatedAt    *time.Time     `json:"created_at,omitempty"`
	ParentConfig map[string]any `json:"parent_checkpoint,omitempty"`
}

// Info exposes server capability metadata.
type Info struct {
	Version      string         `json:"version"`
	Capabilities map[string]any `json:"capabilities"`
}

// StoreValue represents a value in the thread-level key-value store.
type StoreValue struct {
	Namespace string         `json:"namespace"`
	Key       string         `json:"key"`
	Value     map[string]any `json:"value"`
}

// StreamMode defines stream-mode names used by run/thread streaming APIs.
type StreamMode string

const (
	StreamModeValues      StreamMode = "values"
	StreamModeUpdates     StreamMode = "updates"
	StreamModeMessages    StreamMode = "messages"
	StreamModeCustom      StreamMode = "custom"
	StreamModeCheckpoints StreamMode = "checkpoints"
	StreamModeTasks       StreamMode = "tasks"
	StreamModeDebug       StreamMode = "debug"
	StreamModeEvents      StreamMode = "events"
)

// StreamPart is one event in an SSE stream.
type StreamPart struct {
	Event string          `json:"event"`
	ID    string          `json:"id,omitempty"`
	Data  json.RawMessage `json:"data"`
}

// StreamPartV2 is a typed stream part for v2 stream semantics.
//
// Concrete implementations include ValuesStreamPart, UpdatesStreamPart,
// MessagesStreamPart, CustomStreamPart, CheckpointStreamPart,
// TasksStreamPart, DebugStreamPart, MetadataStreamPart, and UnknownStreamPart.
type StreamPartV2 interface {
	streamPartV2()
}

// ValuesStreamPart is emitted for stream mode `values`.
type ValuesStreamPart struct {
	Type       string           `json:"type"`
	NS         []string         `json:"ns"`
	Data       map[string]any   `json:"data"`
	Interrupts []map[string]any `json:"interrupts"`
	Event      string           `json:"event,omitempty"`
	ID         string           `json:"id,omitempty"`
}

func (ValuesStreamPart) streamPartV2() {}

// UpdatesStreamPart is emitted for stream mode `updates`.
type UpdatesStreamPart struct {
	Type       string           `json:"type"`
	NS         []string         `json:"ns"`
	Data       map[string]any   `json:"data"`
	Interrupts []map[string]any `json:"interrupts"`
	Event      string           `json:"event,omitempty"`
	ID         string           `json:"id,omitempty"`
}

func (UpdatesStreamPart) streamPartV2() {}

// MessagesStreamPart is emitted for stream mode `messages`.
type MessagesStreamPart struct {
	Type       string           `json:"type"`
	NS         []string         `json:"ns"`
	Data       any              `json:"data"`
	Interrupts []map[string]any `json:"interrupts"`
	Event      string           `json:"event,omitempty"`
	ID         string           `json:"id,omitempty"`
}

func (MessagesStreamPart) streamPartV2() {}

// CustomStreamPart is emitted for stream mode `custom`.
type CustomStreamPart struct {
	Type       string           `json:"type"`
	NS         []string         `json:"ns"`
	Data       any              `json:"data"`
	Interrupts []map[string]any `json:"interrupts"`
	Event      string           `json:"event,omitempty"`
	ID         string           `json:"id,omitempty"`
}

func (CustomStreamPart) streamPartV2() {}

// CheckpointStreamPart is emitted for stream mode `checkpoints`.
type CheckpointStreamPart struct {
	Type       string           `json:"type"`
	NS         []string         `json:"ns"`
	Data       map[string]any   `json:"data"`
	Interrupts []map[string]any `json:"interrupts"`
	Event      string           `json:"event,omitempty"`
	ID         string           `json:"id,omitempty"`
}

func (CheckpointStreamPart) streamPartV2() {}

// TasksStreamPart is emitted for stream mode `tasks`.
type TasksStreamPart struct {
	Type       string           `json:"type"`
	NS         []string         `json:"ns"`
	Data       map[string]any   `json:"data"`
	Interrupts []map[string]any `json:"interrupts"`
	Event      string           `json:"event,omitempty"`
	ID         string           `json:"id,omitempty"`
}

func (TasksStreamPart) streamPartV2() {}

// DebugStreamPart is emitted for stream mode `debug`.
type DebugStreamPart struct {
	Type       string           `json:"type"`
	NS         []string         `json:"ns"`
	Data       map[string]any   `json:"data"`
	Interrupts []map[string]any `json:"interrupts"`
	Event      string           `json:"event,omitempty"`
	ID         string           `json:"id,omitempty"`
}

func (DebugStreamPart) streamPartV2() {}

// MetadataStreamPart is emitted for stream control metadata events.
type MetadataStreamPart struct {
	Type       string           `json:"type"`
	NS         []string         `json:"ns"`
	Data       map[string]any   `json:"data"`
	Interrupts []map[string]any `json:"interrupts"`
	Event      string           `json:"event,omitempty"`
	ID         string           `json:"id,omitempty"`
}

func (MetadataStreamPart) streamPartV2() {}

// UnknownStreamPart preserves unrecognized stream part payloads.
type UnknownStreamPart struct {
	Type       string           `json:"type"`
	NS         []string         `json:"ns"`
	Data       any              `json:"data"`
	Interrupts []map[string]any `json:"interrupts"`
	Event      string           `json:"event,omitempty"`
	ID         string           `json:"id,omitempty"`
}

func (UnknownStreamPart) streamPartV2() {}

// ThreadStatus is the current runtime status for a thread.
type ThreadStatus string

const (
	ThreadStatusIdle        ThreadStatus = "idle"
	ThreadStatusBusy        ThreadStatus = "busy"
	ThreadStatusInterrupted ThreadStatus = "interrupted"
	ThreadStatusError       ThreadStatus = "error"
)

// ThreadCreateRequest creates a thread.
type ThreadCreateRequest struct {
	ThreadID   string         `json:"thread_id,omitempty"`
	Metadata   map[string]any `json:"metadata,omitempty"`
	IfExists   string         `json:"if_exists,omitempty"`
	Supersteps []any          `json:"supersteps,omitempty"`
	TTL        any            `json:"ttl,omitempty"`
}

// ThreadUpdateRequest updates a thread.
type ThreadUpdateRequest struct {
	Metadata map[string]any `json:"metadata,omitempty"`
	TTL      any            `json:"ttl,omitempty"`
}

// ThreadSearchRequest searches for threads.
type ThreadSearchRequest struct {
	Metadata  map[string]any `json:"metadata,omitempty"`
	Values    map[string]any `json:"values,omitempty"`
	IDs       []string       `json:"ids,omitempty"`
	Status    ThreadStatus   `json:"status,omitempty"`
	Limit     int            `json:"limit,omitempty"`
	Offset    int            `json:"offset,omitempty"`
	SortBy    string         `json:"sort_by,omitempty"`
	SortOrder string         `json:"sort_order,omitempty"`
	Select    []string       `json:"select,omitempty"`
	Extract   map[string]any `json:"extract,omitempty"`
}

// ThreadCountRequest counts threads by filter criteria.
type ThreadCountRequest struct {
	Metadata map[string]any `json:"metadata,omitempty"`
	Values   map[string]any `json:"values,omitempty"`
	Status   ThreadStatus   `json:"status,omitempty"`
}

// ThreadPruneRequest prunes thread history.
type ThreadPruneRequest struct {
	ThreadIDs []string `json:"thread_ids"`
	Strategy  string   `json:"strategy,omitempty"`
}

// ThreadPruneResponse is the response from thread prune.
type ThreadPruneResponse struct {
	PrunedCount int `json:"pruned_count"`
}

// ThreadUpdateStateRequest updates thread state.
type ThreadUpdateStateRequest struct {
	Values       any            `json:"values"`
	AsNode       string         `json:"as_node,omitempty"`
	Checkpoint   map[string]any `json:"checkpoint,omitempty"`
	CheckpointID string         `json:"checkpoint_id,omitempty"`
}

// ThreadUpdateStateResponse is the response from state updates.
type ThreadUpdateStateResponse struct {
	Checkpoint map[string]any `json:"checkpoint,omitempty"`
}

// ThreadHistoryRequest queries thread history.
type ThreadHistoryRequest struct {
	Limit      int            `json:"limit,omitempty"`
	Before     any            `json:"before,omitempty"`
	Metadata   map[string]any `json:"metadata,omitempty"`
	Checkpoint map[string]any `json:"checkpoint,omitempty"`
}

// ThreadJoinStreamRequest configures thread stream joins.
type ThreadJoinStreamRequest struct {
	StreamMode  []string `json:"stream_mode,omitempty"`
	LastEventID string   `json:"last_event_id,omitempty"`
}

// RunCreateRequest creates a run.
type RunCreateRequest struct {
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

// RunStreamRequest creates and streams a run.
type RunStreamRequest = RunCreateRequest

// RunCancelOptions configures run cancellation.
type RunCancelOptions struct {
	Wait   bool
	Action string
}

// RunCancelManyRequest configures bulk run cancellation.
type RunCancelManyRequest struct {
	ThreadID string
	RunIDs   []string
	Status   string
	Action   string
}

// RunJoinStreamRequest configures join stream behavior.
type RunJoinStreamRequest struct {
	CancelOnDisconnect bool
	StreamMode         []string
	LastEventID        string
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

// AssistantCreateRequest creates an assistant.
type AssistantCreateRequest struct {
	GraphID     string         `json:"graph_id,omitempty"`
	Config      map[string]any `json:"config,omitempty"`
	Context     map[string]any `json:"context,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty"`
	AssistantID string         `json:"assistant_id,omitempty"`
	IfExists    string         `json:"if_exists,omitempty"`
	Name        string         `json:"name,omitempty"`
	Description string         `json:"description,omitempty"`
}

// AssistantUpdateRequest updates an assistant.
type AssistantUpdateRequest struct {
	GraphID     string         `json:"graph_id,omitempty"`
	Config      map[string]any `json:"config,omitempty"`
	Context     map[string]any `json:"context,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty"`
	Name        string         `json:"name,omitempty"`
	Description string         `json:"description,omitempty"`
}

// AssistantSearchRequest searches assistants.
type AssistantSearchRequest struct {
	Metadata  map[string]any `json:"metadata,omitempty"`
	GraphID   string         `json:"graph_id,omitempty"`
	Name      string         `json:"name,omitempty"`
	Limit     int            `json:"limit,omitempty"`
	Offset    int            `json:"offset,omitempty"`
	SortBy    string         `json:"sort_by,omitempty"`
	SortOrder string         `json:"sort_order,omitempty"`
	Select    []string       `json:"select,omitempty"`
}

type AssistantSearchResponse struct {
	Assistants []Assistant `json:"assistants"`
	Next       string      `json:"next,omitempty"`
}

// AssistantCountRequest counts assistants.
type AssistantCountRequest struct {
	Metadata map[string]any `json:"metadata,omitempty"`
	GraphID  string         `json:"graph_id,omitempty"`
	Name     string         `json:"name,omitempty"`
}

// AssistantVersion describes an assistant version.
type AssistantVersion struct {
	Version   int            `json:"version"`
	Metadata  map[string]any `json:"metadata,omitempty"`
	CreatedAt time.Time      `json:"created_at"`
}

// AssistantVersionRequest queries assistant versions.
type AssistantVersionRequest struct {
	Metadata map[string]any `json:"metadata,omitempty"`
	Limit    int            `json:"limit,omitempty"`
	Offset   int            `json:"offset,omitempty"`
}

// Cron represents a cron schedule.
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

// CronCreateRequest creates a cron schedule.
type CronCreateRequest struct {
	AssistantID       string         `json:"assistant_id,omitempty"`
	Schedule          string         `json:"schedule,omitempty"`
	Input             map[string]any `json:"input,omitempty"`
	Metadata          map[string]any `json:"metadata,omitempty"`
	Config            map[string]any `json:"config,omitempty"`
	Context           map[string]any `json:"context,omitempty"`
	InterruptBefore   any            `json:"interrupt_before,omitempty"`
	InterruptAfter    any            `json:"interrupt_after,omitempty"`
	Webhook           string         `json:"webhook,omitempty"`
	OnRunCompleted    string         `json:"on_run_completed,omitempty"`
	MultitaskStrategy string         `json:"multitask_strategy,omitempty"`
	EndTime           *time.Time     `json:"end_time,omitempty"`
	Enabled           *bool          `json:"enabled,omitempty"`
	Timezone          string         `json:"timezone,omitempty"`
	StreamMode        []string       `json:"stream_mode,omitempty"`
	StreamSubgraphs   *bool          `json:"stream_subgraphs,omitempty"`
	StreamResumable   *bool          `json:"stream_resumable,omitempty"`
	Durability        string         `json:"durability,omitempty"`
}

// CronUpdateRequest updates a cron schedule.
type CronUpdateRequest = CronCreateRequest

// CronSearchRequest searches cron schedules.
type CronSearchRequest struct {
	AssistantID string   `json:"assistant_id,omitempty"`
	ThreadID    string   `json:"thread_id,omitempty"`
	Enabled     *bool    `json:"enabled,omitempty"`
	Limit       int      `json:"limit,omitempty"`
	Offset      int      `json:"offset,omitempty"`
	SortBy      string   `json:"sort_by,omitempty"`
	SortOrder   string   `json:"sort_order,omitempty"`
	Select      []string `json:"select,omitempty"`
}

// CronCountRequest counts cron schedules.
type CronCountRequest struct {
	AssistantID string `json:"assistant_id,omitempty"`
	ThreadID    string `json:"thread_id,omitempty"`
}

// StoreItem is a global store item.
type StoreItem struct {
	Namespace []string       `json:"namespace"`
	Key       string         `json:"key"`
	Value     map[string]any `json:"value"`
	Score     *float64       `json:"score,omitempty"`
	CreatedAt *time.Time     `json:"created_at,omitempty"`
	UpdatedAt *time.Time     `json:"updated_at,omitempty"`
}

// StoreItemPutRequest creates or updates a global store item.
type StoreItemPutRequest struct {
	Namespace []string       `json:"namespace"`
	Key       string         `json:"key"`
	Value     map[string]any `json:"value"`
	Index     any            `json:"index,omitempty"`
	TTL       *int           `json:"ttl,omitempty"`
}

// StoreSearchRequest searches the global store.
type StoreSearchRequest struct {
	NamespacePrefix []string       `json:"namespace_prefix"`
	Filter          map[string]any `json:"filter,omitempty"`
	Limit           int            `json:"limit,omitempty"`
	Offset          int            `json:"offset,omitempty"`
	Query           string         `json:"query,omitempty"`
	RefreshTTL      *bool          `json:"refresh_ttl,omitempty"`
}

// StoreSearchResponse is a global store search response.
type StoreSearchResponse struct {
	Items []StoreItem `json:"items"`
}

// StoreNamespaceListRequest lists store namespaces.
type StoreNamespaceListRequest struct {
	Prefix   []string `json:"prefix,omitempty"`
	Suffix   []string `json:"suffix,omitempty"`
	MaxDepth *int     `json:"max_depth,omitempty"`
	Limit    int      `json:"limit,omitempty"`
	Offset   int      `json:"offset,omitempty"`
}

// StoreNamespaceListResponse is a global store namespaces response.
type StoreNamespaceListResponse struct {
	Namespaces [][]string `json:"namespaces"`
}
