package graph

import "reflect"

var anyReflectType = reflect.TypeOf((*any)(nil)).Elem()

// ChannelTypeInfo exposes optional runtime type metadata for channel values.
//
// This mirrors LangGraph channel `ValueType`/`UpdateType` properties while
// keeping the core Channel interface minimal.
type ChannelTypeInfo interface {
	ValueType() reflect.Type
	UpdateType() reflect.Type
}

// ChannelEqualer exposes optional channel equality checks.
//
// When implemented, Equals compares channel configuration and current state.
type ChannelEqualer interface {
	Equal(other Channel) bool
}

// ChannelValueType returns a channel value type when available.
func ChannelValueType(ch Channel) reflect.Type {
	if typed, ok := ch.(ChannelTypeInfo); ok {
		return typed.ValueType()
	}
	return anyReflectType
}

// ChannelUpdateType returns a channel update type when available.
func ChannelUpdateType(ch Channel) reflect.Type {
	if typed, ok := ch.(ChannelTypeInfo); ok {
		return typed.UpdateType()
	}
	return anyReflectType
}

// ChannelsEqual compares two channels for semantic equality.
func ChannelsEqual(a, b Channel) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if eq, ok := a.(ChannelEqualer); ok {
		return eq.Equal(b)
	}
	return reflect.DeepEqual(a, b)
}
