package checkpoint

import (
	"errors"
	"strings"
	"testing"
)

func TestPersistenceCompatibilityPolicy(t *testing.T) {
	if PersistenceCompatibilityPolicy != "go-runtime-only" {
		t.Fatalf("policy = %q, want go-runtime-only", PersistenceCompatibilityPolicy)
	}
}

func TestUnsupportedPersistenceFormatError(t *testing.T) {
	base := errors.New("bad payload")
	err := NewUnsupportedPersistenceFormatError("checkpoint/sqlite", "decode checkpoint payload", base)
	if !IsUnsupportedPersistenceFormat(err) {
		t.Fatal("expected IsUnsupportedPersistenceFormat to be true")
	}
	if !errors.Is(err, base) {
		t.Fatal("expected wrapped base error")
	}
	if !strings.Contains(err.Error(), "decode checkpoint payload") {
		t.Fatalf("error = %q, want detail", err.Error())
	}
}
