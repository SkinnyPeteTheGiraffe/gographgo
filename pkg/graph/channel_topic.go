package graph

import "reflect"

// Topic is a configurable pub/sub channel that accumulates a sequence of values.
//
// When accumulate is false (the default), the channel clears its values at the
// start of each superstep and only exposes the writes from that step.
// When accumulate is true, values persist across supersteps.
//
// Writes may be single values or slices (which are flattened into the list).
//
// Mirrors Python's langgraph.channels.topic.Topic.
type Topic struct {
	values     []Dynamic
	accumulate bool
}

// NewTopic creates a new Topic channel.
// If accumulate is true, values persist across supersteps.
func NewTopic(accumulate bool) *Topic {
	return &Topic{accumulate: accumulate}
}

// Get returns the current list of values, or EmptyChannelError if the list is empty.
func (c *Topic) Get() (Dynamic, error) {
	if len(c.values) == 0 {
		return Dynamic{}, &EmptyChannelError{}
	}
	// Return a copy to prevent external mutation.
	out := make([]Dynamic, len(c.values))
	copy(out, c.values)
	return Dyn(out), nil
}

// Update applies incoming writes to the topic.
// If not accumulating, the channel is cleared before applying writes.
// Each incoming value is either a single item or a []any slice that is flattened.
func (c *Topic) Update(values []Dynamic) (bool, error) {
	updated := false

	if !c.accumulate {
		updated = len(c.values) > 0
		c.values = c.values[:0]
	}

	for _, v := range values {
		switch s := v.Value().(type) {
		case []Dynamic:
			c.values = append(c.values, s...)
		default:
			c.values = append(c.values, Dyn(s))
		}
	}

	if len(values) > 0 {
		updated = true
	}

	return updated, nil
}

// Consume is a no-op for Topic.
// Topic lifecycle is advanced via Update([]) at step boundaries.
func (c *Topic) Consume() bool { return false }

// Finish is a no-op for Topic.
func (c *Topic) Finish() bool { return false }

// IsAvailable returns true if the topic has at least one value.
func (c *Topic) IsAvailable() bool { return len(c.values) > 0 }

// Checkpoint returns a copy of the current values list and true.
// Returns nil and false if the topic is empty.
func (c *Topic) Checkpoint() (Dynamic, bool) {
	if len(c.values) == 0 {
		return Dynamic{}, false
	}
	snapshot := make([]Dynamic, len(c.values))
	copy(snapshot, c.values)
	return Dyn(snapshot), true
}

// FromCheckpoint returns a new Topic initialized from a checkpointed values list.
func (c *Topic) FromCheckpoint(checkpoint Dynamic) (Channel, error) {
	n := &Topic{accumulate: c.accumulate}
	if checkpoint != (Dynamic{}) {
		if vals, ok := checkpoint.Value().([]Dynamic); ok {
			n.values = make([]Dynamic, len(vals))
			copy(n.values, vals)
		} else if legacy, ok := decodeLegacyTopicCheckpoint(checkpoint.Value()); ok {
			n.values = legacy
		}
	}
	return n, nil
}

func decodeLegacyTopicCheckpoint(checkpoint any) ([]Dynamic, bool) {
	values, ok := checkpoint.([]any)
	if !ok || len(values) != 2 {
		return nil, false
	}
	legacyValues, ok := values[1].([]any)
	if !ok {
		return nil, false
	}
	out := make([]Dynamic, len(legacyValues))
	for i, v := range legacyValues {
		if d, isDynamic := v.(Dynamic); isDynamic {
			out[i] = d
			continue
		}
		out[i] = Dyn(v)
	}
	return out, true
}

// Copy returns an independent copy of this channel.
func (c *Topic) Copy() Channel {
	n := &Topic{accumulate: c.accumulate}
	if len(c.values) > 0 {
		n.values = make([]Dynamic, len(c.values))
		copy(n.values, c.values)
	}
	return n
}

// ValueType returns the topic read value type metadata.
func (c *Topic) ValueType() reflect.Type { return reflect.TypeOf([]Dynamic{}) }

// UpdateType returns the topic update type metadata.
func (c *Topic) UpdateType() reflect.Type { return anyReflectType }

// Equal reports whether channels have identical configuration and values.
func (c *Topic) Equal(other Channel) bool {
	o, ok := other.(*Topic)
	if !ok {
		return false
	}
	return c.accumulate == o.accumulate && reflect.DeepEqual(c.values, o.values)
}
