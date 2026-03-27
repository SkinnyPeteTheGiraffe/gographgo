// Package checkpoint provides checkpoint interfaces and in-memory/persistent
// implementations for gographgo.
//
// Checkpointers allow graphs to persist execution state across invocations,
// enabling interrupt/resume (human-in-the-loop) and time-travel debugging.
//
// This package mirrors Python LangGraph's langgraph/checkpoint/ hierarchy.
package checkpoint

import (
	"context"
	"fmt"
	"time"
)

// Version is a channel version token persisted in checkpoints.
//
// LangGraph supports string and numeric version representations.
type Version any

// Checkpoint is a serialized snapshot of the graph's channel state at a given
// point during execution.
//
// Mirrors Python's langgraph.checkpoint.base.Checkpoint.
type Checkpoint struct {
	ChannelValues   map[string]any
	ChannelVersions map[string]Version
	VersionsSeen    map[string]map[string]Version
	ID              string
	TS              string
	UpdatedChannels []string `json:",omitempty"`
	PendingSends    []any
	V               int
}

// CheckpointMetadata carries additional context recorded alongside a Checkpoint.
//
// Mirrors Python's langgraph.checkpoint.base.CheckpointMetadata.
type CheckpointMetadata struct {
	Parents map[string]string
	Extra   map[string]any `json:"-"`
	Source  string
	RunID   string
	Step    int
}

// PendingWrite is a task-scoped write that has been recorded but not yet
// applied to the channel state.
//
// Mirrors Python's PendingWrite = tuple[task_id, channel, value].
type PendingWrite struct {
	Value   any
	TaskID  string
	Channel string
}

// CheckpointTuple bundles a Checkpoint with its config and related data.
// It is the primary return type of Saver.GetTuple.
//
// Mirrors Python's langgraph.checkpoint.base.CheckpointTuple.
type CheckpointTuple struct {
	// Config is the configuration that was used to fetch this checkpoint.
	Config *Config

	// Checkpoint is the stored state snapshot.
	Checkpoint *Checkpoint

	// Metadata is the metadata associated with this checkpoint.
	Metadata *CheckpointMetadata

	// ParentConfig points to the configuration of the prior checkpoint.
	ParentConfig *Config

	// PendingWrites are writes recorded after the checkpoint was created
	// but before the next superstep completed.
	PendingWrites []PendingWrite
}

// Config carries the runtime configuration for a checkpoint-aware graph
// execution. It is embedded in the regular graph Config for compatibility
// with the checkpoint package.
//
// This is kept separate from pkg/graph.Config to avoid circular imports.
// Use graph.Config for the full runtime config.
type Config struct {
	// ThreadID is the primary key used to store and retrieve checkpoints.
	// Without it, checkpoints cannot be saved or resumed.
	ThreadID string

	// CheckpointID selects a specific checkpoint to resume from.
	// When empty, the most recent checkpoint for ThreadID is used.
	CheckpointID string

	// CheckpointNS is the checkpoint namespace for subgraph isolation.
	// Empty string ("") is used for the root graph.
	CheckpointNS string
}

// ListOptions controls pagination and filtering in Saver.List.
type ListOptions struct {
	// Filter applies additional key-value filtering to checkpoint metadata.
	Filter map[string]any

	// Before lists only checkpoints created before this config.
	Before *Config

	// Limit caps the number of results returned. 0 means no limit.
	Limit int
}

// PruneStrategy controls saver pruning behavior.
type PruneStrategy string

const (
	// PruneStrategyKeepLatest keeps only the latest checkpoint per namespace.
	PruneStrategyKeepLatest PruneStrategy = "keep_latest"
	// PruneStrategyDelete removes all checkpoints for matching threads.
	PruneStrategyDelete PruneStrategy = "delete"
)

// Saver is the interface that checkpoint backends must implement.
//
// Mirrors Python's langgraph.checkpoint.base.BaseCheckpointSaver.
type Saver interface {
	// GetTuple fetches the most recent checkpoint matching config.
	// Returns (nil, nil) when no checkpoint exists for the given config.
	GetTuple(ctx context.Context, config *Config) (*CheckpointTuple, error)

	// Put stores a checkpoint and returns the updated config (with checkpoint_id
	// set to the stored checkpoint's ID).
	Put(ctx context.Context, config *Config, cp *Checkpoint, meta *CheckpointMetadata) (*Config, error)

	// PutWrites records intermediate task writes linked to a checkpoint.
	PutWrites(ctx context.Context, config *Config, writes []PendingWrite, taskID string) error

	// List returns all checkpoints matching config, applying opts.
	// The returned slice is ordered newest-first.
	//
	// When config is nil, implementations perform a global search across threads.
	List(ctx context.Context, config *Config, opts ListOptions) ([]*CheckpointTuple, error)

	// DeleteThread removes all checkpoints and pending writes for threadID.
	DeleteThread(ctx context.Context, threadID string) error

	// DeleteForRuns removes all checkpoints (and pending writes) for run IDs.
	DeleteForRuns(ctx context.Context, runIDs []string) error

	// CopyThread copies all checkpoints and pending writes from source to target.
	CopyThread(ctx context.Context, sourceThreadID, targetThreadID string) error

	// Prune removes checkpoints for matching threads using the provided strategy.
	Prune(ctx context.Context, threadIDs []string, strategy PruneStrategy) error
}

// EmptyCheckpoint returns a fresh Checkpoint with the given ID and no channel data.
func EmptyCheckpoint(id string) *Checkpoint {
	return &Checkpoint{
		V:               1,
		ID:              id,
		TS:              time.Now().UTC().Format(time.RFC3339Nano),
		ChannelValues:   map[string]any{},
		ChannelVersions: map[string]Version{},
		VersionsSeen:    map[string]map[string]Version{},
	}
}

// CopyCheckpoint returns a deep copy of cp with independent maps.
func CopyCheckpoint(cp *Checkpoint) *Checkpoint {
	cv := make(map[string]any, len(cp.ChannelValues))
	for k, v := range cp.ChannelValues {
		cv[k] = v
	}
	chv := make(map[string]Version, len(cp.ChannelVersions))
	for k, v := range cp.ChannelVersions {
		chv[k] = v
	}
	vs := make(map[string]map[string]Version, len(cp.VersionsSeen))
	for k, inner := range cp.VersionsSeen {
		cp2 := make(map[string]Version, len(inner))
		for ik, iv := range inner {
			cp2[ik] = iv
		}
		vs[k] = cp2
	}
	return &Checkpoint{
		V:               cp.V,
		ID:              cp.ID,
		TS:              cp.TS,
		ChannelValues:   cv,
		ChannelVersions: chv,
		VersionsSeen:    vs,
		UpdatedChannels: append([]string(nil), cp.UpdatedChannels...),
		PendingSends:    append([]any(nil), cp.PendingSends...),
	}
}

// EnsureTimestamp normalizes checkpoint TS to an RFC3339 string.
func EnsureTimestamp(ts string) string {
	if ts == "" {
		return time.Now().UTC().Format(time.RFC3339Nano)
	}
	parsed, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		return ts
	}
	return parsed.UTC().Format(time.RFC3339Nano)
}

// VersionAsInt64 attempts to parse a version token into int64.
func VersionAsInt64(v Version) (int64, error) {
	switch x := any(v).(type) {
	case int:
		return int64(x), nil
	case int8:
		return int64(x), nil
	case int16:
		return int64(x), nil
	case int32:
		return int64(x), nil
	case int64:
		return x, nil
	case uint:
		return int64(x), nil
	case uint8:
		return int64(x), nil
	case uint16:
		return int64(x), nil
	case uint32:
		return int64(x), nil
	case uint64:
		return int64(x), nil
	case float32:
		return int64(x), nil
	case float64:
		return int64(x), nil
	case string:
		var out int64
		_, err := fmt.Sscanf(x, "%d", &out)
		if err != nil {
			return 0, err
		}
		return out, nil
	default:
		return 0, fmt.Errorf("unsupported version type %T", v)
	}
}
