package checkpoint

import (
	"fmt"
	"reflect"
	"time"

	"github.com/google/uuid"
)

// JsonPlusSerializer is a Go-native approximation of LangGraph's
// JsonPlus serializer. It uses typed envelopes and MessagePack for general
// payloads while providing explicit handling for time and UUID values.
type JsonPlusSerializer struct {
	msgpack *MsgpackSerializer
}

// NewJsonPlusSerializer creates a JsonPlusSerializer.
func NewJsonPlusSerializer(opts ...MsgpackOption) *JsonPlusSerializer {
	return &JsonPlusSerializer{msgpack: NewMsgpackSerializer(opts...)}
}

// Serialize serializes value into a storage-friendly payload.
func (s *JsonPlusSerializer) Serialize(value any) (any, error) {
	typeName, payload, err := s.DumpsTyped(value)
	if err != nil {
		return nil, err
	}
	return SerializedValue{Type: typeName, Data: payload}, nil
}

// Deserialize deserializes a storage payload into a Go value.
func (s *JsonPlusSerializer) Deserialize(value any) (any, error) {
	env, ok, err := decodeSerializedEnvelope(value)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("checkpoint.JsonPlusSerializer: expected serialized envelope, got %T", value)
	}
	return s.LoadsTyped(env.Type, env.Data)
}

// DumpsTyped serializes value into a `(type, bytes)` tuple.
func (s *JsonPlusSerializer) DumpsTyped(value any) (string, []byte, error) {
	if s == nil {
		s = NewJsonPlusSerializer()
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
func (s *JsonPlusSerializer) LoadsTyped(typeName string, payload []byte) (any, error) {
	if s == nil {
		s = NewJsonPlusSerializer()
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
func (s *JsonPlusSerializer) WithMsgpackAllowlist(extraAllowlist ...any) *JsonPlusSerializer {
	if s == nil {
		s = NewJsonPlusSerializer()
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
