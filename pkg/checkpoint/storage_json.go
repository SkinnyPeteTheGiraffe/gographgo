package checkpoint

import (
	"encoding/json"
	"fmt"
	"reflect"
)

type checkpointWire struct {
	ChannelValues   map[string]any                `json:"channel_values"`
	ChannelVersions map[string]Version            `json:"channel_versions"`
	VersionsSeen    map[string]map[string]Version `json:"versions_seen"`
	ID              string                        `json:"id"`
	TS              string                        `json:"ts"`
	UpdatedChannels []string                      `json:"updated_channels,omitempty"`
	PendingSends    []any                         `json:"pending_sends"`
	V               int                           `json:"v"`
}

type checkpointMetadataWire struct {
	Parents map[string]string `json:"parents,omitempty"`
	Source  string            `json:"source,omitempty"`
	RunID   string            `json:"run_id,omitempty"`
	Step    int               `json:"step"`
}

// MarshalCheckpointForStorage serializes a checkpoint with a stable snake_case
// schema used by persistent savers.
func MarshalCheckpointForStorage(cp *Checkpoint) ([]byte, error) {
	if cp == nil {
		return nil, fmt.Errorf("checkpoint: checkpoint must not be nil")
	}
	wire := checkpointWire{
		V:               cp.V,
		ID:              cp.ID,
		TS:              cp.TS,
		ChannelValues:   cp.ChannelValues,
		ChannelVersions: cp.ChannelVersions,
		VersionsSeen:    cp.VersionsSeen,
		UpdatedChannels: cp.UpdatedChannels,
		PendingSends:    cp.PendingSends,
	}
	return json.Marshal(wire)
}

// UnmarshalCheckpointFromStorage deserializes a persisted checkpoint from
// either snake_case or legacy CamelCase storage shapes.
func UnmarshalCheckpointFromStorage(payload []byte) (*Checkpoint, bool, error) {
	if len(payload) == 0 {
		return nil, false, fmt.Errorf("checkpoint: checkpoint payload is empty")
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(payload, &raw); err != nil {
		return nil, false, err
	}

	cp := &Checkpoint{}
	if err := unmarshalRawField(raw, &cp.V, "v", "V"); err != nil {
		return nil, false, err
	}
	if err := unmarshalRawField(raw, &cp.ID, "id", "ID"); err != nil {
		return nil, false, err
	}
	if err := unmarshalRawField(raw, &cp.TS, "ts", "TS"); err != nil {
		return nil, false, err
	}
	if err := unmarshalRawField(raw, &cp.ChannelValues, "channel_values", "ChannelValues"); err != nil {
		return nil, false, err
	}
	if err := unmarshalRawField(raw, &cp.ChannelVersions, "channel_versions", "ChannelVersions"); err != nil {
		return nil, false, err
	}
	if err := unmarshalRawField(raw, &cp.VersionsSeen, "versions_seen", "VersionsSeen"); err != nil {
		return nil, false, err
	}
	if err := unmarshalRawField(raw, &cp.UpdatedChannels, "updated_channels", "UpdatedChannels"); err != nil {
		return nil, false, err
	}

	hasPendingSends := hasRawField(raw, "pending_sends", "PendingSends")
	if err := unmarshalRawField(raw, &cp.PendingSends, "pending_sends", "PendingSends"); err != nil {
		return nil, false, err
	}

	return cp, hasPendingSends, nil
}

// MarshalMetadataForStorage serializes checkpoint metadata with stable
// snake_case keys while preserving dynamic metadata fields.
func MarshalMetadataForStorage(meta *CheckpointMetadata) ([]byte, error) {
	if meta == nil {
		return []byte("{}"), nil
	}

	wire := checkpointMetadataWire{
		Source: meta.Source,
		Step:   meta.Step,
		RunID:  meta.RunID,
	}
	if len(meta.Parents) > 0 {
		wire.Parents = make(map[string]string, len(meta.Parents))
		for k, v := range meta.Parents {
			wire.Parents[k] = v
		}
	}

	base, err := json.Marshal(wire)
	if err != nil {
		return nil, err
	}
	if len(meta.Extra) == 0 {
		return base, nil
	}

	var out map[string]any
	if err := json.Unmarshal(base, &out); err != nil {
		return nil, err
	}
	for k, v := range meta.Extra {
		if _, exists := out[k]; exists {
			continue
		}
		out[k] = v
	}
	return json.Marshal(out)
}

// UnmarshalMetadataFromStorage deserializes persisted metadata and preserves
// non-core dynamic keys in CheckpointMetadata.Extra.
func UnmarshalMetadataFromStorage(payload []byte) (*CheckpointMetadata, error) {
	if len(payload) == 0 {
		return nil, nil
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(payload, &raw); err != nil {
		return nil, err
	}

	meta := &CheckpointMetadata{}
	if err := unmarshalRawField(raw, &meta.Source, "source", "Source"); err != nil {
		return nil, err
	}
	if err := unmarshalRawField(raw, &meta.Step, "step", "Step"); err != nil {
		return nil, err
	}
	if err := unmarshalRawField(raw, &meta.Parents, "parents", "Parents"); err != nil {
		return nil, err
	}
	if err := unmarshalRawField(raw, &meta.RunID, "run_id", "RunID"); err != nil {
		return nil, err
	}

	extra := make(map[string]any)
	known := map[string]struct{}{
		"source": {}, "Source": {},
		"step": {}, "Step": {},
		"parents": {}, "Parents": {},
		"run_id": {}, "RunID": {},
	}
	for k, v := range raw {
		if _, ok := known[k]; ok {
			continue
		}
		var decoded any
		if err := json.Unmarshal(v, &decoded); err != nil {
			return nil, err
		}
		extra[k] = decoded
	}
	if len(extra) > 0 {
		meta.Extra = extra
	}

	if meta.Source == "" && meta.Step == 0 && len(meta.Parents) == 0 && meta.RunID == "" && len(meta.Extra) == 0 {
		return nil, nil
	}
	return meta, nil
}

// MetadataMatchesFilter returns true when all filter key/value pairs match
// metadata values, including dynamic metadata fields.
func MetadataMatchesFilter(meta *CheckpointMetadata, filter map[string]any) bool {
	if len(filter) == 0 {
		return true
	}
	for key, want := range filter {
		got, ok := metadataValue(meta, key)
		if !ok || !reflect.DeepEqual(got, want) {
			return false
		}
	}
	return true
}

func metadataValue(meta *CheckpointMetadata, key string) (any, bool) {
	if meta == nil {
		return nil, false
	}

	switch key {
	case "source", "Source":
		if meta.Source == "" {
			return nil, false
		}
		return meta.Source, true
	case "step", "Step":
		return meta.Step, true
	case "parents", "Parents":
		if meta.Parents == nil {
			return nil, false
		}
		return meta.Parents, true
	case "run_id", "RunID":
		if meta.RunID == "" {
			return nil, false
		}
		return meta.RunID, true
	}

	if len(meta.Extra) == 0 {
		return nil, false
	}
	if v, ok := meta.Extra[key]; ok {
		return v, true
	}
	if alias, ok := metadataAliasKey(key); ok {
		if v, exists := meta.Extra[alias]; exists {
			return v, true
		}
	}
	return nil, false
}

func metadataAliasKey(key string) (string, bool) {
	switch key {
	case "runId":
		return "run_id", true
	case "run_id":
		return "runId", true
	default:
		return "", false
	}
}

func hasRawField(raw map[string]json.RawMessage, keys ...string) bool {
	for _, k := range keys {
		if _, ok := raw[k]; ok {
			return true
		}
	}
	return false
}

func unmarshalRawField(raw map[string]json.RawMessage, out any, keys ...string) error {
	for _, k := range keys {
		v, ok := raw[k]
		if !ok {
			continue
		}
		if err := json.Unmarshal(v, out); err != nil {
			return fmt.Errorf("checkpoint: decode field %q: %w", k, err)
		}
		return nil
	}
	return nil
}
