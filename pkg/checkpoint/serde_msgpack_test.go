package checkpoint

import "testing"

type allowedPayload struct {
	Name string
}

func TestMsgpackSerializer_RoundTrip(t *testing.T) {
	ser := NewMsgpackSerializer()
	encoded, err := ser.Serialize(map[string]any{"k": "v", "n": 3})
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	decoded, err := ser.Deserialize(encoded)
	if err != nil {
		t.Fatalf("deserialize: %v", err)
	}
	decodedMap, ok := decoded.(map[string]any)
	if !ok {
		t.Fatalf("decoded type: want map[string]any, got %T", decoded)
	}
	if decodedMap["k"] != "v" {
		t.Fatalf("expected key k=v, got %v", decodedMap["k"])
	}
}

func TestMsgpackSerializer_WithAllowlist(t *testing.T) {
	ser := NewMsgpackSerializer(WithAllowlist(allowedPayload{}))
	if _, err := ser.Serialize(allowedPayload{Name: "ok"}); err != nil {
		t.Fatalf("allowed type serialize failed: %v", err)
	}
	if _, err := ser.Serialize(struct{ X int }{X: 1}); err == nil {
		t.Fatal("expected disallowed type to fail in strict mode")
	}
}
