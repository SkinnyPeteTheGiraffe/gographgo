package checkpoint

import (
	"encoding/json"
	"testing"
)

func TestUnmarshalCheckpointFromStorage_AcceptsSnakeAndCamel(t *testing.T) {
	t.Parallel()

	snake := map[string]any{
		"v":                1,
		"id":               "cp-1",
		"ts":               "2026-03-24T00:00:00Z",
		"channel_values":   map[string]any{"a": 1},
		"channel_versions": map[string]any{"a": 2},
		"versions_seen":    map[string]any{"n": map[string]any{"a": 2}},
		"pending_sends":    []any{"s1"},
	}
	camel := map[string]any{
		"V":               1,
		"ID":              "cp-2",
		"TS":              "2026-03-24T00:00:01Z",
		"ChannelValues":   map[string]any{"b": 1},
		"ChannelVersions": map[string]any{"b": 2},
		"VersionsSeen":    map[string]any{"n": map[string]any{"b": 2}},
		"PendingSends":    []any{"s2"},
	}

	for name, payloadMap := range map[string]map[string]any{"snake": snake, "camel": camel} {
		name := name
		payloadMap := payloadMap
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			payload, err := json.Marshal(payloadMap)
			if err != nil {
				t.Fatalf("Marshal payload: %v", err)
			}
			cp, hasPendingSends, err := UnmarshalCheckpointFromStorage(payload)
			if err != nil {
				t.Fatalf("UnmarshalCheckpointFromStorage: %v", err)
			}
			if cp == nil {
				t.Fatal("expected checkpoint")
			}
			if !hasPendingSends {
				t.Fatal("expected pending_sends field to be detected")
			}
			if cp.ID == "" {
				t.Fatal("expected checkpoint ID")
			}
			if len(cp.ChannelVersions) != 1 {
				t.Fatalf("ChannelVersions len = %d, want 1", len(cp.ChannelVersions))
			}
			if len(cp.PendingSends) != 1 {
				t.Fatalf("PendingSends len = %d, want 1", len(cp.PendingSends))
			}
		})
	}
}

func TestMetadataStorage_PreservesExtraFieldsAndFilter(t *testing.T) {
	t.Parallel()

	meta := &CheckpointMetadata{
		Source: "loop",
		Step:   2,
		RunID:  "run-1",
		Extra: map[string]any{
			"score": 99.0,
			"flags": map[string]any{"ok": true},
		},
	}
	payload, err := MarshalMetadataForStorage(meta)
	if err != nil {
		t.Fatalf("MarshalMetadataForStorage: %v", err)
	}

	decoded, err := UnmarshalMetadataFromStorage(payload)
	if err != nil {
		t.Fatalf("UnmarshalMetadataFromStorage: %v", err)
	}
	if decoded == nil {
		t.Fatal("expected metadata")
	}
	if decoded.Extra["score"] != 99.0 {
		t.Fatalf("score = %v, want 99", decoded.Extra["score"])
	}
	if !MetadataMatchesFilter(decoded, map[string]any{"score": 99.0}) {
		t.Fatal("expected score filter match")
	}
	if !MetadataMatchesFilter(decoded, map[string]any{"flags": map[string]any{"ok": true}}) {
		t.Fatal("expected nested extra filter match")
	}
	if MetadataMatchesFilter(decoded, map[string]any{"score": 100.0}) {
		t.Fatal("unexpected match for non-equal score")
	}
}
