package checkpoint

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"strings"
)

// Cipher encrypts and decrypts serialized payload bytes.
type Cipher interface {
	Encrypt(plaintext []byte) (cipherName string, ciphertext []byte, err error)
	Decrypt(cipherName string, ciphertext []byte) ([]byte, error)
}

// EncryptedSerializer wraps another serializer and encrypts typed payload bytes.
type EncryptedSerializer struct {
	Cipher Cipher
	Serde  Serializer
}

// NewEncryptedSerializer constructs an encrypted serializer.
func NewEncryptedSerializer(c Cipher, serde Serializer) *EncryptedSerializer {
	if serde == nil {
		serde = NewJSONPlusSerializer()
	}
	return &EncryptedSerializer{Cipher: c, Serde: serde}
}

// Serialize serializes and encrypts a value into a typed envelope.
func (s *EncryptedSerializer) Serialize(value any) (any, error) {
	typeName, payload, err := s.DumpsTyped(value)
	if err != nil {
		return nil, err
	}
	return SerializedValue{Type: typeName, Data: payload}, nil
}

// Deserialize decrypts and deserializes a typed envelope.
func (s *EncryptedSerializer) Deserialize(value any) (any, error) {
	env, ok, err := decodeSerializedEnvelope(value)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("checkpoint.EncryptedSerializer: expected serialized envelope, got %T", value)
	}
	return s.LoadsTyped(env.Type, env.Data)
}

// DumpsTyped serializes a value and encrypts the serialized bytes.
func (s *EncryptedSerializer) DumpsTyped(value any) (string, []byte, error) {
	if s == nil || s.Cipher == nil {
		return "", nil, fmt.Errorf("checkpoint.EncryptedSerializer: cipher must not be nil")
	}
	ts := ensureTypedSerializer(s.innerSerializer())
	typeName, payload, err := ts.DumpsTyped(value)
	if err != nil {
		return "", nil, err
	}
	cipherName, ciphertext, err := s.Cipher.Encrypt(payload)
	if err != nil {
		return "", nil, err
	}
	return typeName + "+" + cipherName, ciphertext, nil
}

// LoadsTyped decrypts and deserializes typed payload bytes.
func (s *EncryptedSerializer) LoadsTyped(typeName string, payload []byte) (any, error) {
	ts := ensureTypedSerializer(s.innerSerializer())
	typ := typeName
	cipherName := ""
	if cut := strings.Index(typeName, "+"); cut >= 0 {
		typ = typeName[:cut]
		cipherName = typeName[cut+1:]
	}
	if cipherName == "" {
		return ts.LoadsTyped(typ, payload)
	}
	if s == nil || s.Cipher == nil {
		return nil, fmt.Errorf("checkpoint.EncryptedSerializer: cipher must not be nil")
	}
	decrypted, err := s.Cipher.Decrypt(cipherName, payload)
	if err != nil {
		return nil, err
	}
	return ts.LoadsTyped(typ, decrypted)
}

func (s *EncryptedSerializer) innerSerializer() Serializer {
	if s != nil && s.Serde != nil {
		return s.Serde
	}
	return NewJSONPlusSerializer()
}

// AESGCMCipher implements AES-GCM authenticated encryption.
type AESGCMCipher struct {
	key []byte
}

// NewAESGCMCipher creates an AES-GCM cipher with a 16/24/32-byte key.
func NewAESGCMCipher(key []byte) (*AESGCMCipher, error) {
	if len(key) != 16 && len(key) != 24 && len(key) != 32 {
		return nil, fmt.Errorf("checkpoint: AES key must be 16, 24, or 32 bytes")
	}
	k := append([]byte(nil), key...)
	return &AESGCMCipher{key: k}, nil
}

// NewAESGCMCipherFromEnv loads key material from LANGGRAPH_AES_KEY.
//
// Accepted formats:
// - raw key text (length 16/24/32 bytes)
// - base64-encoded key bytes
func NewAESGCMCipherFromEnv() (*AESGCMCipher, error) {
	raw := os.Getenv("LANGGRAPH_AES_KEY")
	if raw == "" {
		return nil, fmt.Errorf("checkpoint: LANGGRAPH_AES_KEY is not set")
	}
	if aesgcmCipher, err := NewAESGCMCipher([]byte(raw)); err == nil {
		return aesgcmCipher, nil
	}
	decoded, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return nil, fmt.Errorf("checkpoint: LANGGRAPH_AES_KEY invalid (expected raw or base64 key): %w", err)
	}
	return NewAESGCMCipher(decoded)
}

// Encrypt encrypts plaintext and prefixes nonce in ciphertext.
func (c *AESGCMCipher) Encrypt(plaintext []byte) (string, []byte, error) {
	block, err := aes.NewCipher(c.key)
	if err != nil {
		return "", nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", nil, err
	}
	sealed := gcm.Seal(nil, nonce, plaintext, nil)
	out := make([]byte, 0, len(nonce)+len(sealed))
	out = append(out, nonce...)
	out = append(out, sealed...)
	return "aes", out, nil
}

// Decrypt decrypts AES-GCM ciphertext that includes nonce prefix.
func (c *AESGCMCipher) Decrypt(cipherName string, ciphertext []byte) ([]byte, error) {
	if cipherName != "aes" && cipherName != "aes-gcm" {
		return nil, fmt.Errorf("checkpoint: unsupported cipher %q", cipherName)
	}
	block, err := aes.NewCipher(c.key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if len(ciphertext) < gcm.NonceSize() {
		return nil, fmt.Errorf("checkpoint: ciphertext too short")
	}
	nonce := ciphertext[:gcm.NonceSize()]
	body := ciphertext[gcm.NonceSize():]
	return gcm.Open(nil, nonce, body, nil)
}
