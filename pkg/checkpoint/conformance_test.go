package checkpoint_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/checkpoint"
)

func TestCheckpointSnapshotGolden(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := checkpoint.NewInMemorySaverWithSerializer(checkpoint.JSONSerializer{})

	cfg := &checkpoint.Config{ThreadID: "thread-golden"}

	cp1 := &checkpoint.Checkpoint{
		V:  1,
		ID: "cp-001",
		TS: time.Date(2026, 3, 22, 0, 0, 1, 0, time.UTC).Format(time.RFC3339Nano),
		ChannelValues: map[string]any{
			"counter":  1,
			"messages": []any{"hello"},
		},
		ChannelVersions: map[string]checkpoint.Version{"counter": 1, "messages": 1},
		VersionsSeen:    map[string]map[string]checkpoint.Version{"node-a": {"counter": 1, "messages": 1}},
		PendingSends:    []any{"send-1"},
	}

	cp2 := &checkpoint.Checkpoint{
		V:  1,
		ID: "cp-002",
		TS: time.Date(2026, 3, 22, 0, 0, 2, 0, time.UTC).Format(time.RFC3339Nano),
		ChannelValues: map[string]any{
			"counter":  2,
			"messages": []any{"hello", "world"},
		},
		ChannelVersions: map[string]checkpoint.Version{"counter": 2, "messages": 2},
		VersionsSeen:    map[string]map[string]checkpoint.Version{"node-a": {"counter": 2, "messages": 2}},
		PendingSends:    []any{"send-2"},
	}

	cp3 := &checkpoint.Checkpoint{
		V:  1,
		ID: "cp-003",
		TS: time.Date(2026, 3, 22, 0, 0, 3, 0, time.UTC).Format(time.RFC3339Nano),
		ChannelValues: map[string]any{
			"counter":  3,
			"messages": []any{"hello", "world", "!"},
		},
		ChannelVersions: map[string]checkpoint.Version{"counter": 3, "messages": 3},
		VersionsSeen:    map[string]map[string]checkpoint.Version{"node-a": {"counter": 3, "messages": 3}},
		PendingSends:    []any{"send-3"},
	}

	stored1, err := s.Put(ctx, cfg, cp1, &checkpoint.CheckpointMetadata{Source: "input", Step: -1, RunID: "run-123"})
	if err != nil {
		t.Fatalf("Put cp1: %v", err)
	}
	stored2, err := s.Put(ctx, stored1, cp2, &checkpoint.CheckpointMetadata{Source: "loop", Step: 0, RunID: "run-123"})
	if err != nil {
		t.Fatalf("Put cp2: %v", err)
	}
	stored3, err := s.Put(ctx, stored2, cp3, &checkpoint.CheckpointMetadata{Source: "loop", Step: 1, RunID: "run-123"})
	if err != nil {
		t.Fatalf("Put cp3: %v", err)
	}

	err = s.PutWrites(ctx, stored3, []checkpoint.PendingWrite{{Channel: "messages", Value: map[string]any{"delta": "ok"}}}, "task-1")
	if err != nil {
		t.Fatalf("PutWrites: %v", err)
	}

	tuple, err := s.GetTuple(ctx, &checkpoint.Config{ThreadID: "thread-golden", CheckpointID: "cp-003"})
	if err != nil {
		t.Fatalf("GetTuple: %v", err)
	}
	if tuple == nil {
		t.Fatal("expected tuple")
	}

	assertJSONGolden(t, "checkpoint_tuple.json", tuple)
}

func TestCheckpointTimeTravelTraceGolden(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := checkpoint.NewInMemorySaver()

	cfg := &checkpoint.Config{ThreadID: "thread-trace"}
	for _, id := range []string{"cp-001", "cp-002", "cp-003", "cp-004"} {
		_, err := s.Put(ctx, cfg, &checkpoint.Checkpoint{
			V:               1,
			ID:              id,
			TS:              time.Date(2026, 3, 22, 0, 1, 0, 0, time.UTC).Format(time.RFC3339Nano),
			ChannelValues:   map[string]any{"id": id},
			ChannelVersions: map[string]checkpoint.Version{},
			VersionsSeen:    map[string]map[string]checkpoint.Version{},
		}, &checkpoint.CheckpointMetadata{Source: "loop", Step: 1})
		if err != nil {
			t.Fatalf("Put %s: %v", id, err)
		}
	}

	latest, err := s.GetTuple(ctx, &checkpoint.Config{ThreadID: "thread-trace"})
	if err != nil {
		t.Fatalf("GetTuple latest: %v", err)
	}
	if latest == nil {
		t.Fatal("expected latest tuple")
	}

	before, err := s.List(ctx, &checkpoint.Config{ThreadID: "thread-trace"}, checkpoint.ListOptions{Before: &checkpoint.Config{CheckpointID: "cp-004"}})
	if err != nil {
		t.Fatalf("List before: %v", err)
	}
	ids := make([]string, 0, len(before))
	for _, t := range before {
		ids = append(ids, t.Checkpoint.ID)
	}

	trace := map[string]any{
		"latest":            latest.Checkpoint.ID,
		"before_cp_004":     ids,
		"resume_checkpoint": "cp-002",
	}
	assertJSONGolden(t, "time_travel_trace.json", trace)
}

func assertJSONGolden(t *testing.T, filename string, got any) {
	t.Helper()
	b, err := json.MarshalIndent(got, "", "  ")
	if err != nil {
		t.Fatalf("json.MarshalIndent() error = %v", err)
	}
	path := filepath.Join("testdata", "conformance", filename)
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("os.ReadFile(%q) error = %v", path, err)
	}
	wantNormalized := normalizeGoldenNewlines(string(want))
	gotNormalized := normalizeGoldenNewlines(string(b) + "\n")
	if wantNormalized != gotNormalized {
		t.Fatalf("golden mismatch for %s\nwant:\n%s\n\ngot:\n%s\n", path, string(want), string(b))
	}
}

func normalizeGoldenNewlines(s string) string {
	return strings.ReplaceAll(s, "\r\n", "\n")
}
