package checkpoint

import (
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/vmihailenco/msgpack/v5"
)

// MsgpackSerializer serializes values as MessagePack.
type MsgpackSerializer struct {
	allowlist map[reflect.Type]struct{}
	strict    bool
}

// MsgpackOption configures MsgpackSerializer.
type MsgpackOption func(*MsgpackSerializer)

// WithAllowlist enables strict mode and allows listed concrete types.
func WithAllowlist(values ...any) MsgpackOption {
	return func(s *MsgpackSerializer) {
		if s.allowlist == nil {
			s.allowlist = make(map[reflect.Type]struct{}, len(values))
		}
		s.strict = true
		for _, v := range values {
			if v == nil {
				continue
			}
			s.allowlist[reflect.TypeOf(v)] = struct{}{}
		}
	}
}

// NewMsgpackSerializer creates a MessagePack serializer.
func NewMsgpackSerializer(opts ...MsgpackOption) *MsgpackSerializer {
	s := &MsgpackSerializer{}
	if strictMsgpackEnabled() {
		s.strict = true
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Serialize marshals value into MessagePack bytes.
func (s *MsgpackSerializer) Serialize(value any) (any, error) {
	typeName, payload, err := s.DumpsTyped(value)
	if err != nil {
		return nil, err
	}
	if typeName != "msgpack" {
		return SerializedValue{Type: typeName, Data: payload}, nil
	}
	return payload, nil
}

// DumpsTyped marshals value into a `(type, bytes)` tuple.
func (s *MsgpackSerializer) DumpsTyped(value any) (typeName string, payload []byte, err error) {
	if s == nil {
		s = &MsgpackSerializer{}
	}
	switch v := value.(type) {
	case nil:
		return "null", nil, nil
	case []byte:
		return "bytes", append([]byte(nil), v...), nil
	}
	if s.strict {
		if err := s.validateAllowlist(value); err != nil {
			return "", nil, err
		}
	}
	b, err := msgpack.Marshal(value)
	if err != nil {
		return "", nil, err
	}
	return "msgpack", b, nil
}

// Deserialize unmarshals MessagePack bytes into `any`.
func (s *MsgpackSerializer) Deserialize(value any) (any, error) {
	env, ok, err := decodeSerializedEnvelope(value)
	if err != nil {
		return nil, err
	}
	if ok {
		return s.LoadsTyped(env.Type, env.Data)
	}

	b, ok := value.([]byte)
	if !ok {
		return nil, fmt.Errorf("checkpoint.MsgpackSerializer: expected []byte, got %T", value)
	}
	return s.LoadsTyped("msgpack", b)
}

// LoadsTyped unmarshals a `(type, bytes)` tuple into `any`.
func (s *MsgpackSerializer) LoadsTyped(typeName string, payload []byte) (any, error) {
	switch typeName {
	case "null":
		return nil, nil
	case "bytes":
		return append([]byte(nil), payload...), nil
	case "msgpack":
		var out any
		if err := msgpack.Unmarshal(payload, &out); err != nil {
			return nil, err
		}
		return out, nil
	default:
		return nil, fmt.Errorf("checkpoint.MsgpackSerializer: unknown serialization type %q", typeName)
	}
}

func (s *MsgpackSerializer) validateAllowlist(value any) error {
	if value == nil {
		return nil
	}
	t := reflect.TypeOf(value)
	if s.isAllowedType(t) {
		return nil
	}
	if isBuiltinMsgpackType(t) {
		return nil
	}
	return fmt.Errorf("checkpoint.MsgpackSerializer: type %s is not in allowlist", t.String())
}

func (s *MsgpackSerializer) isAllowedType(t reflect.Type) bool {
	if t == nil || s.allowlist == nil {
		return false
	}
	if _, ok := s.allowlist[t]; ok {
		return true
	}
	if t.Kind() == reflect.Ptr {
		if _, ok := s.allowlist[t.Elem()]; ok {
			return true
		}
	}
	return false
}

func isBuiltinMsgpackType(t reflect.Type) bool {
	if t == nil {
		return true
	}
	switch t.Kind() {
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64,
		reflect.String:
		return true
	case reflect.Slice, reflect.Array:
		return isBuiltinMsgpackType(t.Elem())
	case reflect.Map:
		return t.Key().Kind() == reflect.String
	case reflect.Interface:
		return true
	default:
		return false
	}
}

func strictMsgpackEnabled() bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv("LANGGRAPH_STRICT_MSGPACK")))
	return v == "1" || v == "true" || v == "yes"
}
