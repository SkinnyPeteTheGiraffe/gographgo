package runtime

import (
	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/graph"
)

// ChannelsFromCheckpoint initialises a channel map from a persisted snapshot.
//
// For each key in snap, the corresponding channel from schema is asked to
// restore itself via FromCheckpoint. Keys in snap that have no schema entry
// get a fresh LastValue channel seeded with the raw checkpoint value.
//
// Mirrors Python LangGraph's pregel/_checkpoint.py:channels_from_checkpoint.
func ChannelsFromCheckpoint(
	schema map[string]graph.Channel,
	snap map[string]any,
) (map[string]graph.Channel, error) {
	result := make(map[string]graph.Channel, len(schema))

	// Start from schema copies.
	for k, ch := range schema {
		result[k] = ch.Copy()
	}

	// Restore from checkpoint snapshot.
	for k, raw := range snap {
		base, ok := result[k]
		if !ok {
			// No schema entry — default to LastValue.
			base = graph.NewLastValue()
		}
		restored, err := base.FromCheckpoint(graph.Dyn(raw))
		if err != nil {
			return nil, err
		}
		result[k] = restored
	}

	return result, nil
}

// CreateCheckpoint serialises the current channel map into a snapshot map
// suitable for persistence. Only channels whose Checkpoint method returns
// (_, true) are included.
//
// Mirrors Python LangGraph's pregel/_checkpoint.py:create_checkpoint.
func CreateCheckpoint(channels map[string]graph.Channel) map[string]any {
	snap := make(map[string]any, len(channels))
	for k, ch := range channels {
		val, ok := ch.Checkpoint()
		if ok {
			snap[k] = val.Value()
		}
	}
	return snap
}

// EmptyCheckpoint returns an empty snapshot map. Used as the initial
// checkpoint when no prior state exists.
//
// Mirrors Python LangGraph's pregel/_checkpoint.py:empty_checkpoint.
func EmptyCheckpoint() map[string]any {
	return map[string]any{}
}
