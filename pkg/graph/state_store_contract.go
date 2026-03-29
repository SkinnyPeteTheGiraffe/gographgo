package graph

import (
	"context"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/checkpoint"
)

// StateStoreMode controls where business/domain state is authoritative.
type StateStoreMode string

const (
	// StateStoreModeCheckpointAuthoritative keeps checkpoint state as the primary
	// source of truth. This is the default mode.
	StateStoreModeCheckpointAuthoritative StateStoreMode = "checkpoint_authoritative"

	// StateStoreModeAuthoritativeExternalState makes external state authoritative.
	// Checkpoints remain orchestration metadata and pending writes.
	StateStoreModeAuthoritativeExternalState StateStoreMode = "authoritative_external_state"
)

// ExternalStateVersionMetadataKey is persisted in checkpoint metadata `extra`
// and is used to detect stale resume/replay attempts in external-authoritative
// mode.
const ExternalStateVersionMetadataKey = "external_state_version"

// StateStoreReadRequest identifies the authoritative state snapshot to load.
type StateStoreReadRequest struct {
	ThreadID     string
	CheckpointID string
	CheckpointNS string
}

// AuthoritativeStateSnapshot is the externally authoritative state payload.
//
// Version must be a monotonic token (for example a DB row version, event
// sequence, or commit counter) and is required in
// `authoritative_external_state` mode.
type AuthoritativeStateSnapshot struct {
	Values  map[string]any
	Version string
}

// StateStore loads externally authoritative business state.
type StateStore interface {
	// Mode returns the execution mode supported by this state store.
	Mode() StateStoreMode

	// Read returns the authoritative state snapshot for a thread/namespace.
	Read(ctx context.Context, req StateStoreReadRequest) (*AuthoritativeStateSnapshot, error)
}

// CheckpointStore binds a checkpoint saver with explicit state authority mode
// and optional external state adapter.
type CheckpointStore interface {
	Mode() StateStoreMode
	Checkpointer() checkpoint.Saver
	StateStore() StateStore
}

// BasicCheckpointStore is a small helper that satisfies CheckpointStore.
type BasicCheckpointStore struct {
	Saver         checkpoint.Saver
	ExternalState StateStore
	ExecutionMode StateStoreMode
}

// Mode reports the configured execution mode.
func (s *BasicCheckpointStore) Mode() StateStoreMode {
	if s == nil {
		return ""
	}
	return s.ExecutionMode
}

// Checkpointer returns the configured checkpoint saver.
func (s *BasicCheckpointStore) Checkpointer() checkpoint.Saver {
	if s == nil {
		return nil
	}
	return s.Saver
}

// StateStore returns the configured external state adapter.
func (s *BasicCheckpointStore) StateStore() StateStore {
	if s == nil {
		return nil
	}
	return s.ExternalState
}

func effectiveStateMode(config Config) StateStoreMode {
	if config.StateMode != "" {
		return config.StateMode
	}
	if config.CheckpointStore != nil && config.CheckpointStore.Mode() != "" {
		return config.CheckpointStore.Mode()
	}
	if config.StateStore != nil && config.StateStore.Mode() != "" {
		return config.StateStore.Mode()
	}
	return StateStoreModeCheckpointAuthoritative
}

func isAuthoritativeExternalStateMode(mode StateStoreMode) bool {
	return mode == StateStoreModeAuthoritativeExternalState
}
