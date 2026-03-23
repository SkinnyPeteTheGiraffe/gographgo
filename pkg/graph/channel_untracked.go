package graph

import "reflect"

// UntrackedValue is a channel that stores the last value received but is never
// included in checkpoints. State held in an UntrackedValue is lost across
// interrupts and replays.
//
// When guard is true (the default), multiple concurrent writes per superstep
// raise InvalidUpdateError. When guard is false, the last write wins silently.
//
// Mirrors Python's langgraph.channels.untracked_value.UntrackedValue.
type UntrackedValue struct {
	val      Dynamic
	hasValue bool
	guard    bool
}

// NewUntrackedValue creates a new UntrackedValue channel.
// When guard is true, concurrent writes in a superstep raise an error.
func NewUntrackedValue(guard bool) *UntrackedValue {
	return &UntrackedValue{guard: guard}
}

// Get returns the current value or EmptyChannelError if no value is set.
func (c *UntrackedValue) Get() (Dynamic, error) {
	if !c.hasValue {
		return Dynamic{}, &EmptyChannelError{}
	}
	return c.val, nil
}

// Update sets the channel value.
// With guard enabled, more than one write per superstep raises InvalidUpdateError.
func (c *UntrackedValue) Update(values []Dynamic) (bool, error) {
	if len(values) == 0 {
		return false, nil
	}
	if c.guard && len(values) != 1 {
		return false, &InvalidUpdateError{
			Message: "UntrackedValue(guard=true) received multiple writes in one superstep; " +
				"use guard=false to accept the last value silently",
		}
	}
	c.val = values[len(values)-1]
	c.hasValue = true
	return true, nil
}

// Consume is a no-op for UntrackedValue.
func (c *UntrackedValue) Consume() bool { return false }

// Finish is a no-op for UntrackedValue.
func (c *UntrackedValue) Finish() bool { return false }

// IsAvailable returns true if a value has been set.
func (c *UntrackedValue) IsAvailable() bool { return c.hasValue }

// Checkpoint always returns (nil, false) — UntrackedValue is never persisted.
func (c *UntrackedValue) Checkpoint() (Dynamic, bool) { return Dynamic{}, false }

// FromCheckpoint returns a new empty UntrackedValue, ignoring the checkpoint.
// This channel's value is intentionally not restored from checkpoints.
func (c *UntrackedValue) FromCheckpoint(_ Dynamic) (Channel, error) {
	return &UntrackedValue{guard: c.guard}, nil
}

// Copy returns an independent copy of this channel.
func (c *UntrackedValue) Copy() Channel {
	return &UntrackedValue{val: c.val, hasValue: c.hasValue, guard: c.guard}
}

// ValueType returns the runtime value type metadata.
func (c *UntrackedValue) ValueType() reflect.Type { return anyReflectType }

// UpdateType returns the runtime update type metadata.
func (c *UntrackedValue) UpdateType() reflect.Type { return anyReflectType }

// Equal reports whether channels have identical configuration and state.
func (c *UntrackedValue) Equal(other Channel) bool {
	o, ok := other.(*UntrackedValue)
	if !ok {
		return false
	}
	return c.guard == o.guard && c.hasValue == o.hasValue && reflect.DeepEqual(c.val, o.val)
}
