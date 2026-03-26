package checkpoint

import (
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestSerializeForStorage_JSONSerializer(t *testing.T) {
	ser := JSONSerializer{}

	encoded, err := SerializeForStorage(ser, map[string]any{"k": "v", "n": 3})
	if err != nil {
		t.Fatalf("SerializeForStorage: %v", err)
	}

	decoded, err := DeserializeFromStorage(ser, encoded)
	if err != nil {
		t.Fatalf("DeserializeFromStorage: %v", err)
	}

	got, ok := decoded.(map[string]any)
	if !ok {
		t.Fatalf("decoded type = %T, want map[string]any", decoded)
	}
	if got["k"] != "v" {
		t.Fatalf("decoded[k] = %v, want v", got["k"])
	}
}

func TestDeserializeFromStorage_LegacyBase64Fallback(t *testing.T) {
	ser := JSONSerializer{}
	raw, err := ser.Serialize(map[string]any{"legacy": true})
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	b, ok := raw.([]byte)
	if !ok {
		t.Fatalf("serialized type = %T, want []byte", raw)
	}
	legacy := base64.StdEncoding.EncodeToString(b)

	decoded, err := DeserializeFromStorage(ser, legacy)
	if err != nil {
		t.Fatalf("DeserializeFromStorage: %v", err)
	}
	got, ok := decoded.(map[string]any)
	if !ok {
		t.Fatalf("decoded type = %T, want map[string]any", decoded)
	}
	if got["legacy"] != true {
		t.Fatalf("decoded[legacy] = %v, want true", got["legacy"])
	}
}

func TestJSONPlusSerializer_TimeAndUUID(t *testing.T) {
	ser := NewJSONPlusSerializer()

	now := time.Date(2026, 3, 24, 1, 2, 3, 456789000, time.UTC)
	encodedTime, err := ser.Serialize(now)
	if err != nil {
		t.Fatalf("Serialize time: %v", err)
	}
	decodedTimeAny, err := ser.Deserialize(encodedTime)
	if err != nil {
		t.Fatalf("Deserialize time: %v", err)
	}
	decodedTime, ok := decodedTimeAny.(time.Time)
	if !ok {
		t.Fatalf("decoded time type = %T, want time.Time", decodedTimeAny)
	}
	if !decodedTime.Equal(now) {
		t.Fatalf("decoded time = %v, want %v", decodedTime, now)
	}

	id := uuid.MustParse("123e4567-e89b-12d3-a456-426614174000")
	encodedUUID, err := ser.Serialize(id)
	if err != nil {
		t.Fatalf("Serialize uuid: %v", err)
	}
	decodedUUIDAny, err := ser.Deserialize(encodedUUID)
	if err != nil {
		t.Fatalf("Deserialize uuid: %v", err)
	}
	decodedUUID, ok := decodedUUIDAny.(uuid.UUID)
	if !ok {
		t.Fatalf("decoded uuid type = %T, want uuid.UUID", decodedUUIDAny)
	}
	if decodedUUID != id {
		t.Fatalf("decoded uuid = %v, want %v", decodedUUID, id)
	}
}

func TestJSONPlusSerializer_WithMsgpackAllowlist(t *testing.T) {
	type disallowed struct{ X int }
	type allowed struct{ Name string }

	base := NewJSONPlusSerializer(WithAllowlist(allowed{}))
	if _, _, err := base.DumpsTyped(allowed{Name: "ok"}); err != nil {
		t.Fatalf("allowed dumps: %v", err)
	}
	if _, _, err := base.DumpsTyped(disallowed{X: 1}); err == nil {
		t.Fatal("expected disallowed type to fail")
	}

	extended := base.WithMsgpackAllowlist(disallowed{})
	if extended == base {
		t.Fatal("expected cloned serializer when allowlist changes")
	}
	if _, _, err := extended.DumpsTyped(disallowed{X: 1}); err != nil {
		t.Fatalf("extended allowlist should permit type: %v", err)
	}
	if _, _, err := base.DumpsTyped(disallowed{X: 1}); err == nil {
		t.Fatal("original serializer should remain unchanged")
	}
}

type passthroughCipher struct{}

func (passthroughCipher) Encrypt(plaintext []byte) (string, []byte, error) {
	return "passthrough", append([]byte(nil), plaintext...), nil
}

func (passthroughCipher) Decrypt(cipherName string, ciphertext []byte) ([]byte, error) {
	if cipherName != "passthrough" {
		return nil, nil
	}
	return append([]byte(nil), ciphertext...), nil
}

func TestEncryptedSerializer_RoundTripAndUnencryptedFallback(t *testing.T) {
	inner := NewJSONPlusSerializer()
	enc := NewEncryptedSerializer(passthroughCipher{}, inner)

	obj := map[string]any{"k": "v", "n": 1}
	typeName, payload, err := enc.DumpsTyped(obj)
	if err != nil {
		t.Fatalf("DumpsTyped: %v", err)
	}
	if !strings.Contains(typeName, "+passthrough") {
		t.Fatalf("typeName = %q, want +passthrough suffix", typeName)
	}

	decoded, err := enc.LoadsTyped(typeName, payload)
	if err != nil {
		t.Fatalf("LoadsTyped: %v", err)
	}
	decodedMap, ok := decoded.(map[string]any)
	if !ok {
		t.Fatalf("decoded type = %T, want map[string]any", decoded)
	}
	if decodedMap["k"] != "v" {
		t.Fatalf("decoded[k] = %v, want v", decodedMap["k"])
	}
	if decodedMap["n"] != int8(1) && decodedMap["n"] != int64(1) && decodedMap["n"] != float64(1) {
		t.Fatalf("decoded[n] = %v, want numeric 1", decodedMap["n"])
	}

	// Backward compatibility: encrypted serializer can deserialize unencrypted
	// payloads emitted by its inner serializer.
	plainType, plainPayload, err := inner.DumpsTyped(obj)
	if err != nil {
		t.Fatalf("inner DumpsTyped: %v", err)
	}
	plainDecoded, err := enc.LoadsTyped(plainType, plainPayload)
	if err != nil {
		t.Fatalf("LoadsTyped unencrypted: %v", err)
	}
	plainMap, ok := plainDecoded.(map[string]any)
	if !ok {
		t.Fatalf("plainDecoded type = %T, want map[string]any", plainDecoded)
	}
	if plainMap["k"] != "v" {
		t.Fatalf("plainDecoded[k] = %v, want v", plainMap["k"])
	}
}

func TestAESGCMCipher(t *testing.T) {
	c, err := NewAESGCMCipher([]byte("1234567890123456"))
	if err != nil {
		t.Fatalf("NewAESGCMCipher: %v", err)
	}

	typeName, ciphertext, err := c.Encrypt([]byte("hello"))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if typeName != "aes" {
		t.Fatalf("cipher name = %q, want aes", typeName)
	}

	plaintext, err := c.Decrypt(typeName, ciphertext)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if string(plaintext) != "hello" {
		t.Fatalf("plaintext = %q, want hello", string(plaintext))
	}
}

func TestAESGCMCipher_DecryptLegacyCipherName(t *testing.T) {
	c, err := NewAESGCMCipher([]byte("1234567890123456"))
	if err != nil {
		t.Fatalf("NewAESGCMCipher: %v", err)
	}

	_, ciphertext, err := c.Encrypt([]byte("hello"))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	plaintext, err := c.Decrypt("aes-gcm", ciphertext)
	if err != nil {
		t.Fatalf("Decrypt legacy cipher name: %v", err)
	}
	if string(plaintext) != "hello" {
		t.Fatalf("plaintext = %q, want hello", string(plaintext))
	}
}
