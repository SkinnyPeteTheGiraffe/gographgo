package checkpoint

import (
	"fmt"
	"reflect"
	"time"

	"github.com/google/uuid"
)

// JSONPlusSerializer is a Go-native approximation of LangGraph's
// JSONPlus serializer. It uses typed envelopes and MessagePack for general
// payloads while providing explicit handling for time and UUID values.
type JSONPlusSerializer struct {
	msgpack *MsgpackSerializer
}

// NewJSONPlusSerializer creates a JSONPlusSerializer.
func NewJSONPlusSerializer(opts ...MsgpackOption) *JSONPlusSerializer {
	return &JSONPlusSerializer{msgpack: NewMsgpackSerializer(opts...)}
}

// Serialize serializes value into a storage-friendly payload.
func (s *JSONPlusSerializer) Serialize(value any) (any, error) {
	typeName, payload, err := s.DumpsTyped(value)
	if err != nil {
		return nil, err
	}
	return SerializedValue{Type: typeName, Data: payload}, nil
}

// Deserialize deserializes a storage payload into a Go value.
func (s *JSONPlusSerializer) Deserialize(value any) (any, error) {
	env, ok, err := decodeSerializedEnvelope(value)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("checkpoint.JSONPlusSerializer: expected serialized envelope, got %T", value)
	}
	return s.LoadsTyped(env.Type, env.Data)
}

// DumpsTyped serializes value into a `(type, bytes)` tuple.
func (s *JSONPlusSerializer) DumpsTyped(value any) (string, []byte, error) {
	if s == nil {
		s = NewJSONPlusSerializer()
	}
	switch v := value.(type) {
	case nil:
		return "null", nil, nil
	case []byte:
		return "bytes", append([]byte(nil), v...), nil
	case time.Time:
		return "time", []byte(v.UTC().Format(time.RFC3339Nano)), nil
	case uuid.UUID:
		return "uuid", []byte(v.String()), nil
	default:
		return s.msgpack.DumpsTyped(v)
	}
}

// LoadsTyped deserializes a `(type, bytes)` tuple.
func (s *JSONPlusSerializer) LoadsTyped(typeName string, payload []byte) (any, error) {
	if s == nil {
		s = NewJSONPlusSerializer()
	}
	switch typeName {
	case "null":
		return nil, nil
	case "bytes":
		return append([]byte(nil), payload...), nil
	case "time":
		out, err := time.Parse(time.RFC3339Nano, string(payload))
		if err != nil {
			return nil, err
		}
		return out, nil
	case "uuid":
		return uuid.Parse(string(payload))
	default:
		return s.msgpack.LoadsTyped(typeName, payload)
	}
}

// WithMsgpackAllowlist returns a cloned serializer with an updated msgpack
// allowlist, mirroring LangGraph's `with_msgpack_allowlist` behavior.
func (s *JSONPlusSerializer) WithMsgpackAllowlist(extraAllowlist ...any) *JSONPlusSerializer {
	if s == nil {
		s = NewJSONPlusSerializer()
	}
	if len(extraAllowlist) == 0 {
		return s
	}
	clone := *s
	base := &MsgpackSerializer{}
	if s.msgpack != nil {
		base = s.msgpack
	}
	clone.msgpack = &MsgpackSerializer{
		allowlist: make(map[reflect.Type]struct{}, len(base.allowlist)+len(extraAllowlist)),
		strict:    true,
	}
	for k := range base.allowlist {
		clone.msgpack.allowlist[k] = struct{}{}
	}
	for _, v := range extraAllowlist {
		if v == nil {
			continue
		}
		clone.msgpack.allowlist[reflect.TypeOf(v)] = struct{}{}
	}
	return &clone
}
