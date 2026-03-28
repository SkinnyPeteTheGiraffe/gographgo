package graph

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"
)

// RetryPolicy configures retry behavior for nodes.
type RetryPolicy struct {
	// RetryOn specifies which errors should trigger a retry.
	// Can be a list of error types or a custom function.
	RetryOn func(error) bool

	// InitialInterval is the time to wait before the first retry.
	InitialInterval time.Duration

	// BackoffFactor is the multiplier for the interval after each retry.
	BackoffFactor float64

	// MaxInterval is the maximum time between retries.
	MaxInterval time.Duration

	// MaxAttempts is the maximum number of attempts (including first).
	MaxAttempts int

	// Jitter enables random jitter between retries.
	Jitter bool
}

// DefaultRetryPolicy returns a default retry policy.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		InitialInterval: 500 * time.Millisecond,
		BackoffFactor:   2.0,
		MaxInterval:     128 * time.Second,
		MaxAttempts:     3,
		Jitter:          true,
		RetryOn:         DefaultRetryOn,
	}
}

// DefaultRetryOn returns true for errors that should trigger a retry.
func DefaultRetryOn(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	var recursionErr *GraphRecursionError
	if errors.As(err, &recursionErr) {
		return false
	}
	var invalidUpdateErr *InvalidUpdateError
	if errors.As(err, &invalidUpdateErr) {
		return false
	}
	var emptyInputErr *EmptyInputError
	if errors.As(err, &emptyInputErr) {
		return false
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	return true
}

// CachePolicy configures caching behavior for nodes.
type CachePolicy struct {
	// KeyFunc generates a cache key from the node's input.
	KeyFunc func(any) string

	// TTL is the time-to-live for cache entries.
	// If nil, entries never expire.
	TTL *time.Duration
}

// DefaultCacheKey generates a default cache key from the input's string representation.
// Callers that need collision-resistant keys should provide their own KeyFunc.
func DefaultCacheKey(input any) string {
	return fmt.Sprintf("%v", input)
}

// Interrupt represents a pause in graph execution for human input.
type Interrupt struct {
	// Value is the value associated with the interrupt.
	Value Dynamic

	// ID is the unique identifier for this interrupt.
	ID string
}

// NewInterrupt creates a new Interrupt with the given value.
func NewInterrupt(value Dynamic, id string) Interrupt {
	return Interrupt{
		Value: value,
		ID:    id,
	}
}

// Command represents an update to the graph's state and routing.
type Command struct {
	// Graph is the target graph for this command.
	// Use CommandParent for parent graph.
	Graph *string

	// Update is the state update to apply.
	Update map[string]Dynamic

	// Resume provides interrupt resume input.
	//
	// Supported values:
	//   - map[string]Dynamic or map[string]any: interrupt-id keyed resume values
	//   - []Dynamic or []any: ordered resume values for one interrupted task
	//   - any single value (including Dynamic): resume the next interrupt
	Resume any

	// Goto specifies the next route(s) to navigate to.
	Goto Route
}

// CommandParent is a sentinel value indicating the parent graph.
const CommandParent = "__parent__"

// Send represents a message to send to a specific node.
type Send struct {
	// Arg is the state or message to send.
	Arg Dynamic

	// Node is the name of the target node.
	Node string
}

// Call is a functional PUSH task packet dispatched through the Pregel TASKS
// channel. Unlike Send, Call executes the provided function directly instead of
// routing to a named graph node.
type Call struct {
	// Fn is the function executed for the call task.
	Fn func(ctx context.Context, input Dynamic) (NodeResult, error)

	// CachePolicy configures call-level caching.
	CachePolicy *CachePolicy

	// Arg is the call input payload.
	Arg Dynamic

	// Name identifies this call in task metadata and stream events.
	Name string

	// RetryPolicy configures call-level retries.
	RetryPolicy []RetryPolicy
}

// Route is a typed routing result used by conditional edges.
//
// Nodes are routed as PULL tasks. Sends are routed as PUSH tasks.
type Route struct {
	Nodes []string
	Sends []Send
}

// RouteTo returns a Route that targets a single node.
func RouteTo(node string) Route {
	return Route{Nodes: []string{node}}
}

// RouteToMany returns a Route that targets multiple nodes.
func RouteToMany(nodes ...string) Route {
	return Route{Nodes: append([]string(nil), nodes...)}
}

// RouteWithSends returns a Route that dispatches Send-based PUSH tasks.
func RouteWithSends(sends ...Send) Route {
	return Route{Sends: append([]Send(nil), sends...)}
}

// CoerceRoute converts common routing forms into a typed Route.
//
// Supported values:
//   - Route or *Route
//   - string or []string
//   - Send or []Send
//   - []any with string/Send/Route entries
func CoerceRoute(value any) (Route, error) {
	switch v := value.(type) {
	case nil:
		return Route{}, nil
	case Route:
		return v, nil
	case *Route:
		if v == nil {
			return Route{}, nil
		}
		return *v, nil
	case string:
		if v == "" {
			return Route{}, nil
		}
		return RouteTo(v), nil
	case []string:
		return RouteToMany(v...), nil
	case Send:
		return RouteWithSends(v), nil
	case []Send:
		return RouteWithSends(v...), nil
	case []any:
		var out Route
		for i, item := range v {
			itemRoute, err := CoerceRoute(item)
			if err != nil {
				return Route{}, fmt.Errorf("route entry %d: %w", i, err)
			}
			out.Nodes = append(out.Nodes, itemRoute.Nodes...)
			out.Sends = append(out.Sends, itemRoute.Sends...)
		}
		return out, nil
	default:
		return Route{}, fmt.Errorf("unsupported route type %T", value)
	}
}

// SubgraphPersistenceMode controls checkpoint namespace behavior when a
// compiled graph is used as a subgraph node.
type SubgraphPersistenceMode string

const (
	// SubgraphPersistenceTaskScope isolates each subgraph call by task id.
	SubgraphPersistenceTaskScope SubgraphPersistenceMode = "task"

	// SubgraphPersistenceNodeScope reuses one namespace per node.
	SubgraphPersistenceNodeScope SubgraphPersistenceMode = "node"
)

// SubgraphNodeOptions configures how a compiled graph behaves as a subgraph node.
type SubgraphNodeOptions struct {
	// Persistence controls namespace scoping for child checkpoints.
	Persistence SubgraphPersistenceMode

	// DisableCheckpointer disables checkpoint usage in the child invocation even
	// when the parent run carries a checkpointer.
	DisableCheckpointer bool
}

// StateSnapshot represents the state of the graph at a point in time.
type StateSnapshot struct {
	// Values contains current channel values.
	Values any

	// Next contains the names of nodes scheduled to execute.
	Next []string

	// Config is the configuration used to fetch this snapshot.
	Config *Config

	// Metadata is associated metadata.
	Metadata map[string]any

	// CreatedAt is the timestamp of snapshot creation.
	CreatedAt *time.Time

	// ParentConfig is the config of the parent checkpoint.
	ParentConfig *Config

	// Tasks are tasks to execute in this step.
	Tasks []PregelTask

	// Interrupts are pending interrupts.
	Interrupts []Interrupt
}

// PregelTask represents a task in the Pregel execution model.
type PregelTask struct {
	Error      error
	State      any
	Result     any
	ID         string
	Name       string
	Path       []any
	Interrupts []Interrupt
}

// PregelExecutableTask represents an executable task.
type PregelExecutableTask struct {
	// Name is the node name.
	Name string

	// Input is the task input.
	Input any

	// Writes contains pending writes.
	Writes []any

	// Config is the runtime configuration.
	Config any

	// Triggers are the channel names that triggered this task.
	Triggers []string

	// RetryPolicy is the retry configuration.
	RetryPolicy []RetryPolicy

	// CacheKey is the cache key for this task.
	CacheKey *CacheKey

	// ID is the unique task identifier.
	ID string

	// Path is the execution path.
	Path []any
}

// CacheKey represents a cache key.
type CacheKey struct {
	// TTL is the time-to-live in seconds.
	TTL *int

	// Key for the cache entry.
	Key string

	// Namespace for the cache entry.
	NS []string
}

// StreamMode determines how the stream method emits outputs.
type StreamMode string

const (
	// StreamModeValues emits all values in the state after each step.
	StreamModeValues StreamMode = "values"

	// StreamModeUpdates emits only node/task names and updates.
	StreamModeUpdates StreamMode = "updates"

	// StreamModeCustom emits custom data using StreamWriter.
	StreamModeCustom StreamMode = "custom"

	// StreamModeMessages emits LLM messages token-by-token.
	StreamModeMessages StreamMode = "messages"

	// StreamModeCheckpoints emits checkpoint creation events.
	StreamModeCheckpoints StreamMode = "checkpoints"

	// StreamModeTasks emits task start/finish events.
	StreamModeTasks StreamMode = "tasks"

	// StreamModeDebug emits debug information.
	StreamModeDebug StreamMode = "debug"
)

// StreamPart represents a single part of a stream.
// If Err is non-nil, this part signals a terminal error and no further parts
// will be emitted. All other fields are zero in that case.
type StreamPart struct {
	// Data is the stream data.
	Data any

	// Err is set on terminal stream errors. Callers must check this field.
	Err error

	// Type is the stream part type.
	Type StreamMode

	// Namespace is the graph namespace.
	Namespace []string

	// Interrupts contains any interrupts.
	Interrupts []Interrupt
}

// StreamEmit is an optional payload wrapper for StreamWriter calls.
//
// If a node writes StreamEmit via StreamWriter, the runtime emits Data as a
// stream part with Type set to Mode.
type StreamEmit struct {
	Mode StreamMode
	Data any

	// Namespace scopes this event to a checkpoint namespace path.
	Namespace []string
}

// MessageChunk is a messages-mode payload carrying incremental model output.
type MessageChunk struct {
	Chunk    any            `json:"chunk"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

// StreamWriter writes data to the output stream.
type StreamWriter func(any)

// StateUpdate represents an update to the state.
type StateUpdate struct {
	// Values are the updated values.
	Values map[string]any

	// AsNode is the node that made this update.
	AsNode *string

	// TaskID is the task that made this update.
	TaskID *string
}

// GraphOutput is the result of invoking a graph.
type GraphOutput struct {
	// Value is the final output.
	Value any

	// Interrupts are any interrupts that occurred.
	Interrupts []Interrupt
}

// DurabilityMode controls checkpoint persistence timing.
type DurabilityMode string

const (
	// DurabilitySync persists before next step starts.
	DurabilitySync DurabilityMode = "sync"

	// DurabilityAsync persists asynchronously.
	DurabilityAsync DurabilityMode = "async"

	// DurabilityExit persists only when graph exits.
	DurabilityExit DurabilityMode = "exit"
)

// Checkpointer is an alias for checkpoint.Saver kept here for backward
// compatibility. New code should reference checkpoint.Saver directly.
//
// See pkg/checkpoint for the full interface and InMemorySaver implementation.

// All is a sentinel value indicating all nodes.
const All = "*"

// Overwrite bypasses a reducer and writes the wrapped value directly to a
// BinaryOperatorAggregate channel, replacing its accumulated state.
//
// Receiving multiple Overwrite values for the same channel in a single
// superstep raises InvalidUpdateError.
//
// Mirrors Python's langgraph.types.Overwrite.
type Overwrite struct {
	// Value is the replacement value to write directly to the channel.
	Value any
}
