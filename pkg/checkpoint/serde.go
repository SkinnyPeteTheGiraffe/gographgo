package checkpoint

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
)

// Serializer defines how checkpoint values are encoded for storage and decoded
// when read back.
//
// It mirrors Python LangGraph's pluggable serializer protocol in spirit while
// staying idiomatic for Go.
type Serializer interface {
	Serialize(value any) (any, error)
	Deserialize(value any) (any, error)
}

// TypedSerializer defines LangGraph-style typed serialization where values are
// encoded as `(type, bytes)` tuples.
type TypedSerializer interface {
	DumpsTyped(value any) (string, []byte, error)
	LoadsTyped(typeName string, payload []byte) (any, error)
}

// SerializedValue is the on-disk envelope used by typed serializers.
type SerializedValue struct {
	Type string `json:"type"`
	Data []byte `json:"data"`
}

// IdentitySerializer stores values as-is without transformation.
//
// This preserves existing behaviour for in-memory checkpoints while still
// allowing callers to inject custom serialization.
type IdentitySerializer struct{}

// Serialize returns value unchanged.
func (IdentitySerializer) Serialize(value any) (any, error) {
	return value, nil
}

// Deserialize returns value unchanged.
func (IdentitySerializer) Deserialize(value any) (any, error) {
	return value, nil
}

// DumpsTyped serializes value using JSON-compatible typed envelopes.
func (IdentitySerializer) DumpsTyped(value any) (string, []byte, error) {
	switch v := value.(type) {
	case nil:
		return "null", nil, nil
	case []byte:
		return "bytes", append([]byte(nil), v...), nil
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return "", nil, err
		}
		return "json", b, nil
	}
}

// LoadsTyped deserializes a typed envelope.
func (IdentitySerializer) LoadsTyped(typeName string, payload []byte) (any, error) {
	switch typeName {
	case "null":
		return nil, nil
	case "bytes":
		return append([]byte(nil), payload...), nil
	case "json":
		var out any
		if err := json.Unmarshal(payload, &out); err != nil {
			return nil, err
		}
		return out, nil
	default:
		return nil, fmt.Errorf("checkpoint.IdentitySerializer: unknown serialization type %q", typeName)
	}
}

// JSONSerializer encodes values as JSON bytes and decodes them into `any`.
//
// Decoded numeric values follow encoding/json defaults (float64).
type JSONSerializer struct{}

// Serialize marshals value into JSON bytes.
func (JSONSerializer) Serialize(value any) (any, error) {
	b, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// Deserialize unmarshals JSON bytes into `any`.
func (JSONSerializer) Deserialize(value any) (any, error) {
	b, ok := value.([]byte)
	if !ok {
		return nil, fmt.Errorf("checkpoint.JSONSerializer: expected []byte, got %T", value)
	}
	var out any
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// DumpsTyped serializes value as a `(type, bytes)` tuple.
func (JSONSerializer) DumpsTyped(value any) (string, []byte, error) {
	switch v := value.(type) {
	case nil:
		return "null", nil, nil
	case []byte:
		return "bytes", append([]byte(nil), v...), nil
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return "", nil, err
		}
		return "json", b, nil
	}
}

// LoadsTyped deserializes a `(type, bytes)` tuple.
func (JSONSerializer) LoadsTyped(typeName string, payload []byte) (any, error) {
	switch typeName {
	case "null":
		return nil, nil
	case "bytes":
		return append([]byte(nil), payload...), nil
	case "json":
		var out any
		if err := json.Unmarshal(payload, &out); err != nil {
			return nil, err
		}
		return out, nil
	default:
		return nil, fmt.Errorf("checkpoint.JSONSerializer: unknown serialization type %q", typeName)
	}
}

// SerializeForStorage serializes a value for persistent checkpoint storage.
func SerializeForStorage(serializer Serializer, value any) (any, error) {
	ts := ensureTypedSerializer(serializer)
	typeName, payload, err := ts.DumpsTyped(value)
	if err != nil {
		return nil, err
	}
	return SerializedValue{Type: typeName, Data: payload}, nil
}

// DeserializeFromStorage deserializes a value loaded from persistent storage.
func DeserializeFromStorage(serializer Serializer, value any) (any, error) {
	ts := ensureTypedSerializer(serializer)
	env, ok, err := decodeSerializedEnvelope(value)
	if err != nil {
		return nil, err
	}
	if ok {
		return ts.LoadsTyped(env.Type, env.Data)
	}

	// Backward compatibility for legacy persisted payloads.
	dv, err := serializer.Deserialize(value)
	if err == nil {
		return dv, nil
	}
	encoded, isString := value.(string)
	if !isString {
		return nil, err
	}
	b, decErr := base64.StdEncoding.DecodeString(encoded)
	if decErr != nil {
		return nil, err
	}
	dv, secondErr := serializer.Deserialize(b)
	if secondErr != nil {
		return nil, err
	}
	return dv, nil
}

func ensureTypedSerializer(serializer Serializer) TypedSerializer {
	if ts, ok := serializer.(TypedSerializer); ok {
		return ts
	}
	return serializerCompat{legacy: serializer}
}

func decodeSerializedEnvelope(value any) (SerializedValue, bool, error) {
	switch v := value.(type) {
	case SerializedValue:
		return v, true, nil
	case *SerializedValue:
		if v == nil {
			return SerializedValue{}, false, nil
		}
		return *v, true, nil
	case map[string]any:
		typeName, ok := v["type"].(string)
		if !ok || typeName == "" {
			return SerializedValue{}, false, nil
		}
		rawData, ok := v["data"]
		if !ok {
			return SerializedValue{}, false, nil
		}
		switch data := rawData.(type) {
		case string:
			decoded, err := base64.StdEncoding.DecodeString(data)
			if err != nil {
				return SerializedValue{}, false, err
			}
			return SerializedValue{Type: typeName, Data: decoded}, true, nil
		case []byte:
			return SerializedValue{Type: typeName, Data: append([]byte(nil), data...)}, true, nil
		default:
			return SerializedValue{}, false, fmt.Errorf("checkpoint: invalid serialized envelope data type %T", rawData)
		}
	default:
		return SerializedValue{}, false, nil
	}
}

type serializerCompat struct {
	legacy Serializer
}

func (s serializerCompat) DumpsTyped(value any) (string, []byte, error) {
	if value == nil {
		return "null", nil, nil
	}
	raw, err := s.legacy.Serialize(value)
	if err != nil {
		return "", nil, err
	}
	if b, ok := raw.([]byte); ok {
		return "legacy-bytes", append([]byte(nil), b...), nil
	}
	b, err := json.Marshal(raw)
	if err != nil {
		return "", nil, err
	}
	return "legacy-json", b, nil
}

func (s serializerCompat) LoadsTyped(typeName string, payload []byte) (any, error) {
	switch typeName {
	case "null":
		return nil, nil
	case "legacy-bytes":
		return s.legacy.Deserialize(payload)
	case "legacy-json", "json":
		var raw any
		if err := json.Unmarshal(payload, &raw); err != nil {
			return nil, err
		}
		return s.legacy.Deserialize(raw)
	case "bytes":
		return s.legacy.Deserialize(payload)
	default:
		var raw any
		if len(payload) > 0 {
			if err := json.Unmarshal(payload, &raw); err == nil {
				if out, err := s.legacy.Deserialize(raw); err == nil {
					return out, nil
				}
			}
		}
		return s.legacy.Deserialize(payload)
	}
}
