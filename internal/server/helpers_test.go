package server

import (
	"testing"
	"time"
)

func TestCloneStoreItem_DeepCopiesMutableFields(t *testing.T) {
	now := time.Date(2026, 3, 26, 2, 3, 4, 0, time.UTC)
	score := 0.9
	original := &StoreItem{
		Namespace: []string{"profiles", "user"},
		Key:       "bio",
		Value:     map[string]any{"name": "Ada"},
		Score:     &score,
		CreatedAt: &now,
		UpdatedAt: &now,
	}

	cloned := cloneStoreItem(original)
	if cloned == nil {
		t.Fatal("cloneStoreItem returned nil")
	}
	cloned.Namespace[0] = "changed"
	cloned.Value["name"] = "Grace"

	if original.Namespace[0] != "profiles" {
		t.Fatalf("original namespace mutated: %#v", original.Namespace)
	}
	if got, _ := original.Value["name"].(string); got != "Ada" {
		t.Fatalf("original value mutated: %#v", original.Value)
	}
}
