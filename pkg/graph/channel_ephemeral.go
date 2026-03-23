package graph

import "reflect"

// EphemeralValue is a channel that stores a value for exactly one superstep.
// An empty update clears any previously held value.
//
// When guard is true (the default), multiple writes per superstep raise
// InvalidUpdateError. When guard is false, the last write is used silently.
//
// Mirrors Python's langgraph.channels.ephemeral_value.EphemeralValue.
type EphemeralValue struct {
	val      Dynamic
	hasValue bool
	guard    bool
}

// NewEphemeralValue creates a new EphemeralValue channel.
// When guard is true, concurrent writes in a superstep raise an error.
func NewEphemeralValue(guard bool) *EphemeralValue {
	return &EphemeralValue{guard: guard}
}

// Get returns the current value or EmptyChannelError if no value is set.
func (c *EphemeralValue) Get() (Dynamic, error) {
	if !c.hasValue {
		return Dynamic{}, &EmptyChannelError{}
	}
	return c.val, nil
}

// Update sets the channel value for this superstep.
// An empty batch clears any previously held value.
// With guard enabled, more than one write per superstep raises InvalidUpdateError.
func (c *EphemeralValue) Update(values []Dynamic) (bool, error) {
	if len(values) == 0 {
		if !c.hasValue {
			return false, nil
		}
		c.val = Dynamic{}
		c.hasValue = false
		return true, nil
	}

	if c.guard && len(values) != 1 {
		return false, &InvalidUpdateError{
			Message: "EphemeralValue(guard=true) received multiple writes in one superstep; " +
				"use guard=false to accept the last value silently",
		}
	}

	c.val = values[len(values)-1]
	c.hasValue = true
	return true, nil
}

// Consume is a no-op for EphemeralValue.
func (c *EphemeralValue) Consume() bool { return false }

// Finish is a no-op for EphemeralValue.
func (c *EphemeralValue) Finish() bool { return false }

// IsAvailable returns true if a value has been set.
func (c *EphemeralValue) IsAvailable() bool { return c.hasValue }

// Checkpoint returns the stored value and true, or nil and false if empty.
func (c *EphemeralValue) Checkpoint() (Dynamic, bool) {
	if !c.hasValue {
		return Dynamic{}, false
	}
	return c.val, true
}

// FromCheckpoint returns a new EphemeralValue initialized from a checkpoint.
func (c *EphemeralValue) FromCheckpoint(checkpoint Dynamic) (Channel, error) {
	return &EphemeralValue{val: checkpoint, hasValue: true, guard: c.guard}, nil
}

// Copy returns an independent copy of this channel.
func (c *EphemeralValue) Copy() Channel {
	return &EphemeralValue{val: c.val, hasValue: c.hasValue, guard: c.guard}
}

// ValueType returns the runtime value type metadata.
func (c *EphemeralValue) ValueType() reflect.Type { return anyReflectType }

// UpdateType returns the runtime update type metadata.
func (c *EphemeralValue) UpdateType() reflect.Type { return anyReflectType }

// Equal reports whether channels have identical configuration and state.
func (c *EphemeralValue) Equal(other Channel) bool {
	o, ok := other.(*EphemeralValue)
	if !ok {
		return false
	}
	return c.guard == o.guard && c.hasValue == o.hasValue && reflect.DeepEqual(c.val, o.val)
}
