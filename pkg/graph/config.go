package graph

import (
	"context"
	"time"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/checkpoint"
)

// Config contains runtime configuration for graph execution.
//
// Thread-scoped fields (ThreadID, CheckpointID, CheckpointNS) are used by
// checkpoint-aware graphs. At minimum, set ThreadID to enable persistence.
type Config struct {
	Checkpointer   checkpoint.Saver
	Store          any
	Cache          any
	Metadata       map[string]any
	ThreadID       string
	CheckpointID   string
	CheckpointNS   string
	Durability     DurabilityMode
	RecursionLimit int
	StepTimeout    time.Duration
	MaxConcurrency int
	Debug          bool
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		RecursionLimit: 25,
		Debug:          false,
		Durability:     DurabilityAsync,
	}
}

// CheckpointConfig returns a checkpoint.Config derived from this Config.
// Returns nil when no ThreadID is set.
func (c *Config) CheckpointConfig() *checkpoint.Config {
	if c == nil || c.ThreadID == "" {
		return nil
	}
	return &checkpoint.Config{
		ThreadID:     c.ThreadID,
		CheckpointID: c.CheckpointID,
		CheckpointNS: c.CheckpointNS,
	}
}

// ConfigKey is used to store/retrieve Config from context.
type ConfigKey struct{}

// WithConfig returns a context carrying the given config.
func WithConfig(ctx context.Context, config Config) context.Context {
	return context.WithValue(ctx, ConfigKey{}, config)
}

// GetConfig retrieves the Config from context.
// Returns DefaultConfig() if none has been set.
func GetConfig(ctx context.Context) Config {
	if cfg, ok := ctx.Value(ConfigKey{}).(Config); ok {
		return cfg
	}
	return DefaultConfig()
}

// StoreKey is used to store/retrieve Store from context.
type StoreKey struct{}

// WithStore returns a context carrying the given store.
func WithStore(ctx context.Context, store Store) context.Context {
	return context.WithValue(ctx, StoreKey{}, store)
}

// GetStore retrieves the Store from context.
// Returns nil if none has been set.
func GetStore(ctx context.Context) Store {
	if store, ok := ctx.Value(StoreKey{}).(Store); ok {
		return store
	}
	return nil
}

// PregelScratchpadKey is used to store/retrieve PregelScratchpad from context.
type PregelScratchpadKey struct{}

// WithPregelScratchpad returns a context carrying the given scratchpad.
func WithPregelScratchpad(ctx context.Context, scratchpad *PregelScratchpad) context.Context {
	return context.WithValue(ctx, PregelScratchpadKey{}, scratchpad)
}

// GetPregelScratchpad retrieves the PregelScratchpad from context.
// Returns nil if none has been set.
func GetPregelScratchpad(ctx context.Context) *PregelScratchpad {
	if sp, ok := ctx.Value(PregelScratchpadKey{}).(*PregelScratchpad); ok {
		return sp
	}
	return nil
}

// RuntimeKey is used to store/retrieve Runtime from context.
type RuntimeKey struct{}

// WithRuntime returns a context carrying the given runtime.
func WithRuntime(ctx context.Context, runtime *Runtime) context.Context {
	return context.WithValue(ctx, RuntimeKey{}, runtime)
}

// GetRuntime retrieves the Runtime from context.
// Returns a default runtime if none has been set.
func GetRuntime(ctx context.Context) *Runtime {
	if rt, ok := ctx.Value(RuntimeKey{}).(*Runtime); ok {
		return rt
	}
	return &Runtime{}
}

// Runtime contains run-scoped context and utilities injected into nodes.
type Runtime struct {
	// Context is the static context for this graph run (e.g., user_id, db_conn).
	Context any

	// Store is the store for cross-thread persistence.
	Store Store

	// StreamWriter is the function to write to the custom stream.
	StreamWriter func(any)

	// Previous is the previous return value for the given thread.
	Previous any

	// ExecutionInfo contains read-only execution metadata.
	ExecutionInfo *ExecutionInfo
}

// ExecutionInfo contains read-only execution metadata.
type ExecutionInfo struct {
	// NodeAttempt is the current node execution attempt number (1-indexed).
	NodeAttempt int

	// NodeFirstAttemptTime is the Unix timestamp for when the first attempt started.
	NodeFirstAttemptTime int64

	// IsReplaying is true when this node runs while replaying from a specific
	// checkpoint ID.
	IsReplaying bool
}
