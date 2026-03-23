package graph

import "reflect"

// NamedBarrierValue is a channel that waits until writes have been received from
// all expected sources before making a value available.
//
// When all sources have written, IsAvailable returns true and Get returns nil
// (the barrier value carries no payload — its presence signals completion).
// Consume resets the barrier, allowing it to be triggered again in future steps.
//
// Mirrors Python's langgraph.channels.named_barrier_value.NamedBarrierValue.
type NamedBarrierValue[T comparable] struct {
	names map[T]struct{}
	seen  map[T]struct{}
}

// NewNamedBarrierValue creates a new NamedBarrierValue that waits for all of
// the provided values to write before becoming available.
func NewNamedBarrierValue[T comparable](names []T) *NamedBarrierValue[T] {
	ns := make(map[T]struct{}, len(names))
	for _, n := range names {
		ns[n] = struct{}{}
	}
	return &NamedBarrierValue[T]{
		names: ns,
		seen:  make(map[T]struct{}),
	}
}

// Get returns nil when all expected names have been seen, or EmptyChannelError
// if the barrier has not yet been satisfied.
func (c *NamedBarrierValue[T]) Get() (Dynamic, error) {
	if len(c.seen) != len(c.names) {
		return Dynamic{}, &EmptyChannelError{}
	}
	return Dynamic{}, nil
}

// Update records each write and validates it is from a known source.
func (c *NamedBarrierValue[T]) Update(values []Dynamic) (bool, error) {
	updated := false
	for _, v := range values {
		name, ok := v.Value().(T)
		if !ok {
			return false, &InvalidUpdateError{
				Message: "NamedBarrierValue received value with unexpected type",
			}
		}
		if _, known := c.names[name]; !known {
			return false, &InvalidUpdateError{Message: "NamedBarrierValue received unexpected source value"}
		}
		if _, already := c.seen[name]; !already {
			c.seen[name] = struct{}{}
			updated = true
		}
	}
	return updated, nil
}

// Consume resets the barrier after it has been satisfied, allowing it to fire
// again in future supersteps. Returns true if the barrier was satisfied.
func (c *NamedBarrierValue[T]) Consume() bool {
	if len(c.seen) == len(c.names) {
		c.seen = make(map[T]struct{})
		return true
	}
	return false
}

// Finish is a no-op for NamedBarrierValue.
func (c *NamedBarrierValue[T]) Finish() bool { return false }

// IsAvailable returns true when all expected sources have written.
func (c *NamedBarrierValue[T]) IsAvailable() bool {
	return len(c.seen) == len(c.names)
}

// Checkpoint returns the set of seen names and true, or nil and false if empty.
func (c *NamedBarrierValue[T]) Checkpoint() (Dynamic, bool) {
	if len(c.seen) == 0 {
		return Dynamic{}, false
	}
	snapshot := make([]T, 0, len(c.seen))
	for k := range c.seen {
		snapshot = append(snapshot, k)
	}
	return Dyn(snapshot), true
}

// FromCheckpoint returns a new NamedBarrierValue restored from a checkpoint.
func (c *NamedBarrierValue[T]) FromCheckpoint(checkpoint Dynamic) (Channel, error) {
	n := NewNamedBarrierValue([]T(nil))
	n.names = c.names // share the immutable names set
	if names, ok := checkpoint.Value().([]T); ok {
		for _, name := range names {
			n.seen[name] = struct{}{}
		}
	}
	return n, nil
}

// Copy returns an independent copy of this channel.
func (c *NamedBarrierValue[T]) Copy() Channel {
	n := &NamedBarrierValue[T]{
		names: c.names, // immutable, safe to share
		seen:  make(map[T]struct{}, len(c.seen)),
	}
	for k := range c.seen {
		n.seen[k] = struct{}{}
	}
	return n
}

// ValueType returns the expected barrier value type.
func (c *NamedBarrierValue[T]) ValueType() reflect.Type { return reflect.TypeFor[T]() }

// UpdateType returns the expected barrier update type.
func (c *NamedBarrierValue[T]) UpdateType() reflect.Type { return reflect.TypeFor[T]() }

// Equal reports whether channels have identical configuration and seen state.
func (c *NamedBarrierValue[T]) Equal(other Channel) bool {
	o, ok := other.(*NamedBarrierValue[T])
	if !ok {
		return false
	}
	return reflect.DeepEqual(c.names, o.names) && reflect.DeepEqual(c.seen, o.seen)
}

// NamedBarrierValueAfterFinish is a barrier channel that only becomes available
// after Finish() is called. Once consumed, it resets for the next cycle.
//
// Mirrors Python's langgraph.channels.named_barrier_value.NamedBarrierValueAfterFinish.
type NamedBarrierValueAfterFinish[T comparable] struct {
	names    map[T]struct{}
	seen     map[T]struct{}
	finished bool
}

// NewNamedBarrierValueAfterFinish creates a barrier that requires all expected
// values to write AND a call to Finish before becoming available.
func NewNamedBarrierValueAfterFinish[T comparable](names []T) *NamedBarrierValueAfterFinish[T] {
	ns := make(map[T]struct{}, len(names))
	for _, n := range names {
		ns[n] = struct{}{}
	}
	return &NamedBarrierValueAfterFinish[T]{
		names: ns,
		seen:  make(map[T]struct{}),
	}
}

// Get returns nil when all sources have written and Finish has been called.
func (c *NamedBarrierValueAfterFinish[T]) Get() (Dynamic, error) {
	if !c.finished || len(c.seen) != len(c.names) {
		return Dynamic{}, &EmptyChannelError{}
	}
	return Dynamic{}, nil
}

// Update records each write and validates it is from a known source.
func (c *NamedBarrierValueAfterFinish[T]) Update(values []Dynamic) (bool, error) {
	updated := false
	for _, v := range values {
		name, ok := v.Value().(T)
		if !ok {
			return false, &InvalidUpdateError{
				Message: "NamedBarrierValueAfterFinish received value with unexpected type",
			}
		}
		if _, known := c.names[name]; !known {
			return false, &InvalidUpdateError{Message: "NamedBarrierValueAfterFinish received unexpected source value"}
		}
		if _, already := c.seen[name]; !already {
			c.seen[name] = struct{}{}
			updated = true
		}
	}
	return updated, nil
}

// Consume resets the barrier after it has been finished and satisfied.
func (c *NamedBarrierValueAfterFinish[T]) Consume() bool {
	if c.finished && len(c.seen) == len(c.names) {
		c.finished = false
		c.seen = make(map[T]struct{})
		return true
	}
	return false
}

// Finish marks the barrier as finishing. Returns true if it transitions to
// available (all sources seen and not already finished).
func (c *NamedBarrierValueAfterFinish[T]) Finish() bool {
	if !c.finished && len(c.seen) == len(c.names) {
		c.finished = true
		return true
	}
	return false
}

// IsAvailable returns true when all sources have written and Finish has been called.
func (c *NamedBarrierValueAfterFinish[T]) IsAvailable() bool {
	return c.finished && len(c.seen) == len(c.names)
}

// Checkpoint returns the seen set, finished flag, and true, or nil and false if empty.
func (c *NamedBarrierValueAfterFinish[T]) Checkpoint() (Dynamic, bool) {
	if len(c.seen) == 0 && !c.finished {
		return Dynamic{}, false
	}
	seen := make([]T, 0, len(c.seen))
	for k := range c.seen {
		seen = append(seen, k)
	}
	return Dyn(namedBarrierAfterFinishCheckpoint[T]{Seen: seen, Finished: c.finished}), true
}

type namedBarrierAfterFinishCheckpoint[T comparable] struct {
	Seen     []T
	Finished bool
}

// FromCheckpoint returns a new NamedBarrierValueAfterFinish restored from a checkpoint.
func (c *NamedBarrierValueAfterFinish[T]) FromCheckpoint(checkpoint Dynamic) (Channel, error) {
	n := &NamedBarrierValueAfterFinish[T]{
		names: c.names,
		seen:  make(map[T]struct{}),
	}
	if cp, ok := checkpoint.Value().(namedBarrierAfterFinishCheckpoint[T]); ok {
		for _, name := range cp.Seen {
			n.seen[name] = struct{}{}
		}
		n.finished = cp.Finished
	}
	return n, nil
}

// Copy returns an independent copy of this channel.
func (c *NamedBarrierValueAfterFinish[T]) Copy() Channel {
	n := &NamedBarrierValueAfterFinish[T]{
		names:    c.names,
		seen:     make(map[T]struct{}, len(c.seen)),
		finished: c.finished,
	}
	for k := range c.seen {
		n.seen[k] = struct{}{}
	}
	return n
}

// ValueType returns the expected barrier value type.
func (c *NamedBarrierValueAfterFinish[T]) ValueType() reflect.Type { return reflect.TypeFor[T]() }

// UpdateType returns the expected barrier update type.
func (c *NamedBarrierValueAfterFinish[T]) UpdateType() reflect.Type { return reflect.TypeFor[T]() }

// Equal reports whether channels have identical configuration and state.
func (c *NamedBarrierValueAfterFinish[T]) Equal(other Channel) bool {
	o, ok := other.(*NamedBarrierValueAfterFinish[T])
	if !ok {
		return false
	}
	return c.finished == o.finished && reflect.DeepEqual(c.names, o.names) && reflect.DeepEqual(c.seen, o.seen)
}
