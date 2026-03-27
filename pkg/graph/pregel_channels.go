package graph

import "reflect"

// pregelChannelMap manages the Channel instances for a single Pregel execution.
// It mirrors Python LangGraph's channels_from_checkpoint / read_channels /
// apply_writes helpers spread across pregel/_checkpoint.py and pregel/_io.py.
type pregelChannelMap struct {
	channels map[string]Channel
}

// newPregelChannelMap creates a fresh channel map seeded from a schema.
// Each channel in the schema is copied so that this run's mutations are isolated.
func newPregelChannelMap(schema map[string]Channel) *pregelChannelMap {
	ch := make(map[string]Channel, len(schema))
	for k, v := range schema {
		ch[k] = v.Copy()
	}
	return &pregelChannelMap{channels: ch}
}

// getOrCreate returns the channel for a key, auto-creating a LastValue channel
// if no channel exists for that key. This matches LangGraph's behavior of
// defaulting untyped keys to LastValue.
func (m *pregelChannelMap) getOrCreate(key string) Channel {
	if ch, ok := m.channels[key]; ok {
		return ch
	}
	ch := NewLastValue()
	m.channels[key] = ch
	return ch
}

// writeToChannel appends value to the pending writes list for a channel key and
// applies the batch immediately. Returns (changed, error).
//
// In a real superstep all writes for a channel would be batched before calling
// Update; here we expose a convenience method for single writes used in
// mapInput.
func (m *pregelChannelMap) writeSingle(key string, value any) (bool, error) {
	ch := m.getOrCreate(key)
	return ch.Update([]Dynamic{Dyn(value)})
}

// applyBatch calls Update on each channel with all values collected for it
// during a superstep. Returns the set of channels that changed.
func (m *pregelChannelMap) applyBatch(writes []pregelWrite) (map[string]bool, error) {
	// Group writes by channel.
	grouped := make(map[string][]Dynamic)
	for _, w := range writes {
		grouped[w.channel] = append(grouped[w.channel], Dyn(w.value))
	}

	changed := make(map[string]bool, len(grouped))
	for key, values := range grouped {
		ch := m.getOrCreate(key)
		ok, err := ch.Update(values)
		if err != nil {
			return changed, err
		}
		if ok {
			changed[key] = true
		}
	}
	return changed, nil
}

// consumeChannels calls Consume on the selected channels and returns the subset
// whose Consume lifecycle changed state.
func (m *pregelChannelMap) consumeChannels(keys []string) map[string]bool {
	changed := make(map[string]bool)
	for _, key := range keys {
		ch, ok := m.channels[key]
		if !ok {
			continue
		}
		if ch.Consume() {
			changed[key] = true
		}
	}
	return changed
}

// finishAll calls Finish on every channel and returns true if any changed.
func (m *pregelChannelMap) finishAll() bool {
	changed := false
	for _, ch := range m.channels {
		if ch.Finish() {
			changed = true
		}
	}
	return changed
}

// snapshot returns a serialisable point-in-time snapshot of all channels that
// have data (Checkpoint returns true). Used to persist to a Checkpointer.
func (m *pregelChannelMap) snapshot() map[string]any {
	out := make(map[string]any)
	for k, ch := range m.channels {
		if val, ok := ch.Checkpoint(); ok {
			out[k] = val.Value()
		}
	}
	return out
}

// restoreFromCheckpoint initializes a channel map from a previously saved
// snapshot, delegating per-channel restoration to FromCheckpoint.
func restoreFromCheckpoint(schema map[string]Channel, snap map[string]any) (*pregelChannelMap, error) {
	m := newPregelChannelMap(schema) // copies schema channels as baseline
	for k, raw := range snap {
		ch := m.getOrCreate(k)
		restored, err := ch.FromCheckpoint(Dyn(raw))
		if err != nil {
			return nil, err
		}
		m.channels[k] = restored
	}
	return m, nil
}

// toStateMap reads every available channel into a map[string]any. Keys whose
// channels have no value are omitted. This is the Go equivalent of Python's
// read_channels / map_output_values.
func (m *pregelChannelMap) toStateMap() map[string]any {
	out := make(map[string]any, len(m.channels))
	for k, ch := range m.channels {
		val, err := ch.Get()
		if err != nil {
			// EmptyChannelError — skip
			continue
		}
		out[k] = val.Value()
	}
	return out
}

// mapInputToChannels writes an input value into the channel map.
//
// Resolution order:
//  1. map[string]any — each key-value pair is written to its named channel.
//  2. Exported struct fields — each exported field is written to a channel
//     whose name matches the field name. This enables typed-State graphs
//     (e.g., MessagesState) to propagate individual fields to their reducers.
//  3. Anything else — the whole value is written to the special __input__
//     channel, matching LangGraph's map_input fall-through behavior.
func mapInputToChannels(m *pregelChannelMap, input any) error {
	if inputMap, ok := input.(map[string]any); ok {
		for k, v := range inputMap {
			if _, err := m.writeSingle(k, v); err != nil {
				return err
			}
		}
		return nil
	}

	// Struct reflection: write each exported field to its named channel.
	if input != nil {
		rt := reflect.TypeOf(input)
		rv := reflect.ValueOf(input)
		if rt.Kind() == reflect.Struct {
			for i := 0; i < rt.NumField(); i++ {
				f := rt.Field(i)
				if !f.IsExported() {
					continue
				}
				if _, err := m.writeSingle(f.Name, rv.Field(i).Interface()); err != nil {
					return err
				}
			}
			return nil
		}
	}

	_, err := m.writeSingle(pregelInput, input)
	return err
}
