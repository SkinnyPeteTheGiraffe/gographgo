package postgres

import (
	"strings"
	"testing"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/checkpoint"
)

func TestSaver_LoadsTyped_RejectsUnsupportedSerializedType(t *testing.T) {
	s := &Saver{serializer: checkpoint.IdentitySerializer{}}

	_, err := s.loadsTyped("msgpack", []byte{0x01})
	if err == nil {
		t.Fatal("expected unsupported format error")
	}
	if !checkpoint.IsUnsupportedPersistenceFormat(err) {
		t.Fatalf("expected unsupported format error, got %v", err)
	}
	if !strings.Contains(err.Error(), `deserialize typed value "msgpack"`) {
		t.Fatalf("error = %v, want typed value detail", err)
	}
}

func TestSaver_LoadsTyped_JSONValueRoundTrip(t *testing.T) {
	s := &Saver{serializer: checkpoint.IdentitySerializer{}}

	got, err := s.loadsTyped("json", []byte(`{"ok":true}`))
	if err != nil {
		t.Fatalf("loadsTyped json: %v", err)
	}
	m, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("type = %T, want map[string]any", got)
	}
	if m["ok"] != true {
		t.Fatalf("ok = %v, want true", m["ok"])
	}
}
