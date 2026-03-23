package graph

import (
	"fmt"
	"reflect"
)

// Channel is the core interface for all state channels in the Pregel execution model.
//
// Channels handle how values are written and merged within and across supersteps.
// Each key in a graph's state schema maps to one Channel instance. The Pregel
// runtime calls Update once per superstep with every write collected for that
// channel, then calls Consume and Finish at the appropriate lifecycle points.
//
// This interface mirrors Python LangGraph's BaseChannel contract.
type Channel interface {
	// Get returns the current value.
	// Returns EmptyChannelError if the channel has not been set.
	Get() (Dynamic, error)

	// Update applies a batch of values for the current superstep.
	// values contains every write collected for this channel in this step.
	// Returns true if the channel value changed, false if it is unchanged.
	// Returns InvalidUpdateError if the write sequence violates channel constraints
	// (e.g. LastValue receiving more than one write).
	Update(values []Dynamic) (bool, error)

	// Consume is called by the Pregel runtime after a subscribed task runs.
	// Barrier-style channels use this to reset after all expected writes arrive.
	// Returns true if the channel state changed.
	Consume() bool

	// Finish is called when the Pregel run is ending.
	// Returns true if the channel state changed.
	Finish() bool

	// IsAvailable returns true if Get() would succeed.
	IsAvailable() bool

	// Checkpoint returns a serializable snapshot of the channel state and
	// whether the channel has data to persist. The runtime does not persist
	// channels for which the second return value is false.
	Checkpoint() (Dynamic, bool)

	// FromCheckpoint returns a new channel initialized from a previously
	// checkpointed value. Called only when Checkpoint returned (_, true).
	FromCheckpoint(checkpoint Dynamic) (Channel, error)

	// Copy returns an independent clone of this channel.
	Copy() Channel
}

// LastValue is a channel that stores only the most recently received value.
// It enforces that at most one write is received per superstep; multiple
// concurrent writes raise InvalidUpdateError.
//
// Mirrors Python's langgraph.channels.last_value.LastValue.
type LastValue struct {
	val      Dynamic
	hasValue bool
}

// NewLastValue creates a new empty LastValue channel.
func NewLastValue() *LastValue {
	return &LastValue{}
}

// Get returns the current value or EmptyChannelError if no value has been set.
func (c *LastValue) Get() (Dynamic, error) {
	if !c.hasValue {
		return Dynamic{}, &EmptyChannelError{}
	}
	return c.val, nil
}

// Update sets the channel value from a batch of superstep writes.
// Returns InvalidUpdateError if more than one write is present.
func (c *LastValue) Update(values []Dynamic) (bool, error) {
	if len(values) == 0 {
		return false, nil
	}
	if len(values) != 1 {
		return false, &InvalidUpdateError{
			Message: CreateErrorMessage(
				fmt.Sprintf(
					"LastValue channel received %d writes in one superstep; expected exactly 1. "+
						"Use a reducer annotation (e.g. BinaryOperatorAggregate) to handle multiple writes.",
					len(values),
				),
				ErrorCodeInvalidConcurrentGraphUpdate,
			),
		}
	}
	c.val = values[0]
	c.hasValue = true
	return true, nil
}

// Consume is a no-op for LastValue.
func (c *LastValue) Consume() bool { return false }

// Finish is a no-op for LastValue.
func (c *LastValue) Finish() bool { return false }

// IsAvailable returns true if a value has been set.
func (c *LastValue) IsAvailable() bool { return c.hasValue }

// Checkpoint returns the stored value and true, or nil and false if empty.
func (c *LastValue) Checkpoint() (Dynamic, bool) {
	if !c.hasValue {
		return Dynamic{}, false
	}
	return c.val, true
}

// FromCheckpoint returns a new LastValue initialized from a checkpoint value.
func (c *LastValue) FromCheckpoint(checkpoint Dynamic) (Channel, error) {
	return &LastValue{val: checkpoint, hasValue: true}, nil
}

// Copy returns an independent copy of this channel.
func (c *LastValue) Copy() Channel {
	return &LastValue{val: c.val, hasValue: c.hasValue}
}

// ValueType returns the runtime value type metadata.
func (c *LastValue) ValueType() reflect.Type { return anyReflectType }

// UpdateType returns the runtime update type metadata.
func (c *LastValue) UpdateType() reflect.Type { return anyReflectType }

// Equal reports whether channels have identical state.
func (c *LastValue) Equal(other Channel) bool {
	o, ok := other.(*LastValue)
	if !ok {
		return false
	}
	return c.hasValue == o.hasValue && reflect.DeepEqual(c.val, o.val)
}

// LastValueAfterFinish is a single-value channel that only becomes readable
// after Finish is called. This is used for values that should be emitted once
// per run at graph finalization time.
//
// Mirrors Python's langgraph.channels.last_value.LastValueAfterFinish.
type LastValueAfterFinish struct {
	val      Dynamic
	hasValue bool
	finished bool
}

type lastValueAfterFinishCheckpoint struct {
	Value    Dynamic
	Finished bool
}

// NewLastValueAfterFinish creates a new empty LastValueAfterFinish channel.
func NewLastValueAfterFinish() *LastValueAfterFinish {
	return &LastValueAfterFinish{}
}

// Get returns the value only after Finish has been called.
func (c *LastValueAfterFinish) Get() (Dynamic, error) {
	if !c.hasValue || !c.finished {
		return Dynamic{}, &EmptyChannelError{}
	}
	return c.val, nil
}

// Update stores exactly one value for this step and clears the finished flag.
func (c *LastValueAfterFinish) Update(values []Dynamic) (bool, error) {
	if len(values) == 0 {
		return false, nil
	}
	if len(values) != 1 {
		return false, &InvalidUpdateError{
			Message: CreateErrorMessage(
				fmt.Sprintf(
					"LastValueAfterFinish channel received %d writes in one superstep; expected exactly 1. "+
						"Use a reducer annotation (e.g. BinaryOperatorAggregate) to handle multiple writes.",
					len(values),
				),
				ErrorCodeInvalidConcurrentGraphUpdate,
			),
		}
	}
	c.val = values[0]
	c.hasValue = true
	c.finished = false
	return true, nil
}

// Consume clears a finished value after it has been read/advanced.
func (c *LastValueAfterFinish) Consume() bool {
	if c.finished && c.hasValue {
		c.val = Dynamic{}
		c.hasValue = false
		c.finished = false
		return true
	}
	return false
}

// Finish marks the current value as readable.
func (c *LastValueAfterFinish) Finish() bool {
	if c.hasValue && !c.finished {
		c.finished = true
		return true
	}
	return false
}

// IsAvailable returns true when a value exists and Finish has been called.
func (c *LastValueAfterFinish) IsAvailable() bool {
	return c.hasValue && c.finished
}

// Checkpoint returns a snapshot with value/finished state when a value exists.
func (c *LastValueAfterFinish) Checkpoint() (Dynamic, bool) {
	if !c.hasValue {
		return Dynamic{}, false
	}
	return Dyn(lastValueAfterFinishCheckpoint{Value: c.val, Finished: c.finished}), true
}

// FromCheckpoint restores a LastValueAfterFinish from a prior snapshot.
func (c *LastValueAfterFinish) FromCheckpoint(snapshot Dynamic) (Channel, error) {
	out := NewLastValueAfterFinish()
	cp, ok := snapshot.Value().(lastValueAfterFinishCheckpoint)
	if !ok {
		return nil, &InvalidUpdateError{Message: "LastValueAfterFinish checkpoint must be lastValueAfterFinishCheckpoint"}
	}
	out.val = cp.Value
	out.hasValue = true
	out.finished = cp.Finished
	if out.val == (Dynamic{}) {
		out.hasValue = false
	}
	return out, nil
}

// Copy returns an independent copy of this channel.
func (c *LastValueAfterFinish) Copy() Channel {
	return &LastValueAfterFinish{
		val:      c.val,
		hasValue: c.hasValue,
		finished: c.finished,
	}
}

// ValueType returns the runtime value type metadata.
func (c *LastValueAfterFinish) ValueType() reflect.Type { return anyReflectType }

// UpdateType returns the runtime update type metadata.
func (c *LastValueAfterFinish) UpdateType() reflect.Type { return anyReflectType }

// Equal reports whether channels have identical state.
func (c *LastValueAfterFinish) Equal(other Channel) bool {
	o, ok := other.(*LastValueAfterFinish)
	if !ok {
		return false
	}
	return c.hasValue == o.hasValue && c.finished == o.finished && reflect.DeepEqual(c.val, o.val)
}

// AnyValue is a channel that stores only the most recently received value,
// accepting multiple writes per superstep without error (it takes the last one).
// An empty update clears the previously held value.
//
// Mirrors Python's langgraph.channels.any_value.AnyValue.
type AnyValue struct {
	val      Dynamic
	hasValue bool
}

// NewAnyValue creates a new empty AnyValue channel.
func NewAnyValue() *AnyValue {
	return &AnyValue{}
}

// Get returns the current value or EmptyChannelError if no value has been set.
func (c *AnyValue) Get() (Dynamic, error) {
	if !c.hasValue {
		return Dynamic{}, &EmptyChannelError{}
	}
	return c.val, nil
}

// Update sets the channel to the last value in the batch.
// An empty batch clears the previously held value and returns true if one was set.
func (c *AnyValue) Update(values []Dynamic) (bool, error) {
	if len(values) == 0 {
		if !c.hasValue {
			return false, nil
		}
		c.val = Dynamic{}
		c.hasValue = false
		return true, nil
	}
	c.val = values[len(values)-1]
	c.hasValue = true
	return true, nil
}

// Consume is a no-op for AnyValue.
func (c *AnyValue) Consume() bool { return false }

// Finish is a no-op for AnyValue.
func (c *AnyValue) Finish() bool { return false }

// IsAvailable returns true if a value has been set.
func (c *AnyValue) IsAvailable() bool { return c.hasValue }

// Checkpoint returns the stored value and true, or nil and false if empty.
func (c *AnyValue) Checkpoint() (Dynamic, bool) {
	if !c.hasValue {
		return Dynamic{}, false
	}
	return c.val, true
}

// FromCheckpoint returns a new AnyValue initialized from a checkpoint value.
func (c *AnyValue) FromCheckpoint(checkpoint Dynamic) (Channel, error) {
	return &AnyValue{val: checkpoint, hasValue: true}, nil
}

// Copy returns an independent copy of this channel.
func (c *AnyValue) Copy() Channel {
	return &AnyValue{val: c.val, hasValue: c.hasValue}
}

// ValueType returns the runtime value type metadata.
func (c *AnyValue) ValueType() reflect.Type { return anyReflectType }

// UpdateType returns the runtime update type metadata.
func (c *AnyValue) UpdateType() reflect.Type { return anyReflectType }

// Equal reports whether channels have identical state.
func (c *AnyValue) Equal(other Channel) bool {
	o, ok := other.(*AnyValue)
	if !ok {
		return false
	}
	return c.hasValue == o.hasValue && reflect.DeepEqual(c.val, o.val)
}
