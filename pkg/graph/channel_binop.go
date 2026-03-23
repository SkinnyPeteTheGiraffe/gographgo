package graph

import (
	"fmt"
	"reflect"
	"runtime"
	"strings"
)

const overwriteKey = "__overwrite__"
const legacyOverwriteKey = "__OVERWRITE__"

// BinaryOperatorAggregate is a channel that accumulates values by applying a
// binary operator to the running total and each new write.
//
// Example: using operator.add semantics to accumulate a list across nodes.
//
// If the channel is empty and receives multiple writes in one superstep, the
// first write becomes the initial value and the operator is applied to each
// subsequent write.
//
// A write wrapped in Overwrite bypasses the operator and replaces the stored
// value directly. At most one Overwrite may be received per superstep.
//
// Mirrors Python's langgraph.channels.binop.BinaryOperatorAggregate.
type BinaryOperatorAggregate struct {
	val      Dynamic
	hasValue bool
	operator func(a, b any) any
	valueTyp reflect.Type
}

// NewBinaryOperatorAggregate creates a new BinaryOperatorAggregate channel
// with the given binary operator.
func NewBinaryOperatorAggregate(operator func(a, b any) any) *BinaryOperatorAggregate {
	return &BinaryOperatorAggregate{operator: operator}
}

// NewBinaryOperatorAggregateWithType creates a BinaryOperatorAggregate with
// explicit value type metadata. This enables zero-value initialization for
// typed reducer channels and normalizes collection/interface aliases to
// concrete runtime values where applicable.
func NewBinaryOperatorAggregateWithType(valueType reflect.Type, operator func(a, b any) any) *BinaryOperatorAggregate {
	normalized := normalizeAggregateType(valueType)
	c := &BinaryOperatorAggregate{operator: operator, valueTyp: normalized}
	if init, ok := instantiateAggregateValue(normalized); ok {
		c.val = Dyn(init)
		c.hasValue = true
	}
	return c
}

func normalizeAggregateType(t reflect.Type) reflect.Type {
	if t == nil {
		return nil
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t
}

func instantiateAggregateValue(t reflect.Type) (any, bool) {
	if t == nil {
		return nil, false
	}
	defer func() {
		_ = recover()
	}()
	if t.Kind() == reflect.Interface {
		return nil, false
	}
	v := reflect.New(t).Elem()
	return v.Interface(), true
}

func overwriteValue(value any) (any, bool) {
	switch v := value.(type) {
	case Overwrite:
		return v.Value, true
	case *Overwrite:
		if v == nil {
			return nil, true
		}
		return v.Value, true
	case map[string]any:
		if len(v) == 1 {
			if ov, ok := v[overwriteKey]; ok {
				if d, isDyn := ov.(Dynamic); isDyn {
					return d.Value(), true
				}
				return ov, true
			}
			if ov, ok := v[legacyOverwriteKey]; ok {
				if d, isDyn := ov.(Dynamic); isDyn {
					return d.Value(), true
				}
				return ov, true
			}
		}
	case map[string]Dynamic:
		if len(v) == 1 {
			if ov, ok := v[overwriteKey]; ok {
				return ov.Value(), true
			}
			if ov, ok := v[legacyOverwriteKey]; ok {
				return ov.Value(), true
			}
		}
	}

	rv := reflect.ValueOf(value)
	if !rv.IsValid() || rv.Kind() != reflect.Map || rv.Len() != 1 {
		return nil, false
	}
	iter := rv.MapRange()
	if !iter.Next() {
		return nil, false
	}
	key, ok := iter.Key().Interface().(string)
	if !ok || (key != overwriteKey && key != legacyOverwriteKey) {
		return nil, false
	}
	mapValue := iter.Value().Interface()
	if d, isDyn := mapValue.(Dynamic); isDyn {
		return d.Value(), true
	}
	return mapValue, true
}

func isAnonymousOperator(op func(a, b any) any) bool {
	if op == nil {
		return false
	}
	fn := runtime.FuncForPC(reflect.ValueOf(op).Pointer())
	if fn == nil {
		return true
	}
	name := fn.Name()
	return strings.Contains(name, ".func") || strings.HasSuffix(name, "-fm")
}

// Get returns the accumulated value or EmptyChannelError if no value has been set.
func (c *BinaryOperatorAggregate) Get() (Dynamic, error) {
	if !c.hasValue {
		return Dynamic{}, &EmptyChannelError{}
	}
	return c.val, nil
}

// Update applies each write through the binary operator.
// If the channel has no value, the first write becomes the initial value.
// An Overwrite value replaces the accumulated result directly; at most one
// Overwrite may be present per superstep.
func (c *BinaryOperatorAggregate) Update(values []Dynamic) (bool, error) {
	if len(values) == 0 {
		return false, nil
	}

	seenOverwrite := false

	for i, v := range values {
		// Overwrite bypasses the operator.
		if overwrite, ok := overwriteValue(v.Value()); ok {
			if seenOverwrite {
				return false, &InvalidUpdateError{
					Message: CreateErrorMessage(
						"BinaryOperatorAggregate channel received more than one Overwrite in one superstep",
						ErrorCodeInvalidConcurrentGraphUpdate,
					),
				}
			}
			c.val = Dyn(overwrite)
			c.hasValue = true
			seenOverwrite = true
			continue
		}

		if seenOverwrite {
			// Once an overwrite is seen in a step, later writes are ignored.
			continue
		}

		if !c.hasValue && i == 0 {
			// Seed the accumulator with the first value.
			c.val = v
			c.hasValue = true
			continue
		}

		c.val = Dyn(c.operator(c.val.Value(), v.Value()))
	}
	return true, nil
}

// ValueType returns the configured runtime value type when known.
func (c *BinaryOperatorAggregate) ValueType() reflect.Type {
	if c.valueTyp != nil {
		return c.valueTyp
	}
	return anyReflectType
}

// UpdateType returns the expected runtime update type.
func (c *BinaryOperatorAggregate) UpdateType() reflect.Type {
	if c.valueTyp != nil {
		return c.valueTyp
	}
	return anyReflectType
}

// Equal reports whether the channel has the same reducer configuration and state.
func (c *BinaryOperatorAggregate) Equal(other Channel) bool {
	o, ok := other.(*BinaryOperatorAggregate)
	if !ok {
		return false
	}
	if c.hasValue != o.hasValue || !reflect.DeepEqual(c.val, o.val) || c.valueTyp != o.valueTyp {
		return false
	}
	if c.operator == nil || o.operator == nil {
		return c.operator == nil && o.operator == nil
	}
	if isAnonymousOperator(c.operator) || isAnonymousOperator(o.operator) {
		return true
	}
	return reflect.ValueOf(c.operator).Pointer() == reflect.ValueOf(o.operator).Pointer()
}

// Consume is a no-op for BinaryOperatorAggregate.
func (c *BinaryOperatorAggregate) Consume() bool { return false }

// Finish is a no-op for BinaryOperatorAggregate.
func (c *BinaryOperatorAggregate) Finish() bool { return false }

// IsAvailable returns true if a value has been set.
func (c *BinaryOperatorAggregate) IsAvailable() bool { return c.hasValue }

// Checkpoint returns the accumulated value and true, or nil and false if empty.
func (c *BinaryOperatorAggregate) Checkpoint() (Dynamic, bool) {
	if !c.hasValue {
		return Dynamic{}, false
	}
	return c.val, true
}

// FromCheckpoint returns a new BinaryOperatorAggregate initialized from a checkpoint value.
func (c *BinaryOperatorAggregate) FromCheckpoint(checkpoint Dynamic) (Channel, error) {
	return &BinaryOperatorAggregate{
		val:      checkpoint,
		hasValue: true,
		operator: c.operator,
		valueTyp: c.valueTyp,
	}, nil
}

// Copy returns an independent copy of this channel.
func (c *BinaryOperatorAggregate) Copy() Channel {
	return &BinaryOperatorAggregate{
		val:      c.val,
		hasValue: c.hasValue,
		operator: c.operator,
		valueTyp: c.valueTyp,
	}
}

// String returns a human-readable description.
func (c *BinaryOperatorAggregate) String() string {
	if !c.hasValue {
		return fmt.Sprintf("BinaryOperatorAggregate(empty)")
	}
	return fmt.Sprintf("BinaryOperatorAggregate(%v)", c.val.Value())
}
