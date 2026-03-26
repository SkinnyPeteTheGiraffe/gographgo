package runtime

import (
	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/graph"
)

// MapInput converts an input value to a map of channel writes.
//
// For map[string]any inputs each key-value pair becomes a channel write.
// For any other type the whole value is written under the __input__ key.
//
// Mirrors Python LangGraph's pregel/_io.py:map_input.
func MapInput(input any, _ map[string]graph.Channel) map[string][]any {
	writes := make(map[string][]any)

	if inputMap, ok := input.(map[string]any); ok {
		for k, v := range inputMap {
			writes[k] = append(writes[k], v)
		}
		return writes
	}

	// Non-map input: write to the __input__ channel.
	writes["__input__"] = []any{input}
	return writes
}

// ReadChannels reads all available channel values into a map[string]any.
// Channels that have no value (EmptyChannelError) are omitted.
//
// Mirrors Python LangGraph's pregel/_io.py:read_channels.
func ReadChannels(channels map[string]graph.Channel) map[string]any {
	out := make(map[string]any, len(channels))
	for k, ch := range channels {
		val, err := ch.Get()
		if err != nil {
			continue // EmptyChannelError — skip
		}
		out[k] = val
	}
	return out
}

// MapOutputValues reads the graph's output channels into a map[string]any,
// filtering out internal pregel protocol channels.
//
// Mirrors Python LangGraph's pregel/_io.py:map_output_values.
func MapOutputValues(channels map[string]graph.Channel) map[string]any {
	internal := map[string]bool{
		"__input__":      true,
		"__interrupt__":  true,
		"__resume__":     true,
		"__error__":      true,
		"__pregel_push":  true,
		"__pregel_tasks": true,
	}

	out := make(map[string]any, len(channels))
	for k, ch := range channels {
		if internal[k] {
			continue
		}
		val, err := ch.Get()
		if err != nil {
			continue
		}
		out[k] = val
	}
	return out
}

// MapOutputUpdates converts task writes to an update map keyed by node name.
//
// Mirrors Python LangGraph's pregel/_io.py:map_output_updates.
func MapOutputUpdates(nodeUpdates map[string]map[string]any) map[string]any {
	out := make(map[string]any, len(nodeUpdates))
	for node, updates := range nodeUpdates {
		out[node] = updates
	}
	return out
}
