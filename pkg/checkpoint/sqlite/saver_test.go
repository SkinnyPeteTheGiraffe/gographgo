package sqlite_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/checkpoint"
	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/checkpoint/sqlite"
)

func mkCP(id string, values map[string]any) *checkpoint.Checkpoint {
	if values == nil {
		values = map[string]any{}
	}
	return &checkpoint.Checkpoint{
		V:               1,
		ID:              id,
		TS:              time.Now().UTC().Format(time.RFC3339Nano),
		ChannelValues:   values,
		ChannelVersions: map[string]checkpoint.Version{},
		VersionsSeen:    map[string]map[string]checkpoint.Version{},
	}
}

func mkMeta(source string, step int) *checkpoint.CheckpointMetadata {
	return &checkpoint.CheckpointMetadata{Source: source, Step: step}
}

func newSaver(t *testing.T) *sqlite.Saver {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })

	s, err := sqlite.New(db, checkpoint.JSONSerializer{})
	if err != nil {
		t.Fatalf("new saver: %v", err)
	}
	return s
}

func TestSaver_New_AllowsTypedRuntimeSchemaCompatibility(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	schemaSQL, err := os.ReadFile(filepath.Join("testdata", "incompatible_runtime_schema.sql"))
	if err != nil {
		t.Fatalf("read schema fixture: %v", err)
	}
	if _, err := db.ExecContext(ctx, string(schemaSQL)); err != nil {
		t.Fatalf("seed incompatible schema: %v", err)
	}

	s, err := sqlite.New(db, checkpoint.JSONSerializer{})
	if err != nil {
		t.Fatalf("new saver with typed schema: %v", err)
	}

	stored, err := s.Put(ctx, &checkpoint.Config{ThreadID: "typed-schema"}, mkCP("cp-typed-001", map[string]any{"counter": 1}), mkMeta("loop", 0))
	if err != nil {
		t.Fatalf("Put typed schema: %v", err)
	}
	got, err := s.GetTuple(ctx, stored)
	if err != nil {
		t.Fatalf("GetTuple typed schema: %v", err)
	}
	if got == nil {
		t.Fatal("expected tuple")
	}
}

func TestSaver_GetTuple_LoadsTypedCheckpointAndWritesFromSchema(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	schemaSQL, err := os.ReadFile(filepath.Join("testdata", "incompatible_runtime_schema.sql"))
	if err != nil {
		t.Fatalf("read schema fixture: %v", err)
	}
	if _, err := db.ExecContext(ctx, string(schemaSQL)); err != nil {
		t.Fatalf("seed typed schema: %v", err)
	}

	s, err := sqlite.New(db, checkpoint.JSONSerializer{})
	if err != nil {
		t.Fatalf("new saver: %v", err)
	}

	cpPayload, err := checkpoint.MarshalCheckpointForStorage(mkCP("cp-typed-row-001", map[string]any{"counter": 7}))
	if err != nil {
		t.Fatalf("marshal checkpoint payload: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
INSERT INTO checkpoints (thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, type, checkpoint, metadata)
VALUES (?, ?, ?, ?, ?, ?, ?)
`, "thread-typed-row", "", "cp-typed-row-001", nil, "json", cpPayload, []byte(`{"source":"loop","step":0}`)); err != nil {
		t.Fatalf("insert typed checkpoint row: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
INSERT INTO writes (thread_id, checkpoint_ns, checkpoint_id, task_id, idx, channel, type, value)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
`, "thread-typed-row", "", "cp-typed-row-001", "task-1", 0, "events", "json", []byte(`"hello"`)); err != nil {
		t.Fatalf("insert typed write row: %v", err)
	}

	got, err := s.GetTuple(ctx, &checkpoint.Config{ThreadID: "thread-typed-row", CheckpointNS: "", CheckpointID: "cp-typed-row-001"})
	if err != nil {
		t.Fatalf("GetTuple: %v", err)
	}
	if got == nil {
		t.Fatal("expected tuple")
	}
	if got.Checkpoint.ChannelValues["counter"] != float64(7) {
		t.Fatalf("counter = %v, want 7", got.Checkpoint.ChannelValues["counter"])
	}
	if len(got.PendingWrites) != 1 || got.PendingWrites[0].Value != "hello" {
		t.Fatalf("pending writes = %+v, want one typed write with value hello", got.PendingWrites)
	}
}

func TestSaver_GetTuple_RejectsUnsupportedSerializedValues(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })

	s, err := sqlite.New(db, checkpoint.IdentitySerializer{})
	if err != nil {
		t.Fatalf("new saver: %v", err)
	}

	payloadMap := map[string]any{
		"v":  1,
		"id": "cp-unsupported-001",
		"ts": time.Now().UTC().Format(time.RFC3339Nano),
		"channel_values": map[string]any{
			"x": map[string]any{"type": "msgpack", "data": "AQ=="},
		},
		"channel_versions": map[string]any{"x": "1"},
		"versions_seen":    map[string]any{},
		"pending_sends":    []any{},
	}
	payload, err := json.Marshal(payloadMap)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
INSERT INTO checkpoints (thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, checkpoint, metadata)
VALUES (?, ?, ?, ?, ?, ?)
`, "thread-unsupported", "", "cp-unsupported-001", nil, payload, []byte("{}")); err != nil {
		t.Fatalf("insert checkpoint: %v", err)
	}

	_, err = s.GetTuple(ctx, &checkpoint.Config{ThreadID: "thread-unsupported", CheckpointNS: "", CheckpointID: "cp-unsupported-001"})
	if err == nil {
		t.Fatal("expected unsupported format error")
	}
	if !checkpoint.IsUnsupportedPersistenceFormat(err) {
		t.Fatalf("expected unsupported format error, got %v", err)
	}
	if !strings.Contains(err.Error(), "deserialize checkpoint value") {
		t.Fatalf("error = %v, want deserialize checkpoint value detail", err)
	}
}

func TestSaver_PutGetTuple(t *testing.T) {
	ctx := context.Background()
	s := newSaver(t)

	cfg := &checkpoint.Config{ThreadID: "thread-1"}
	stored, err := s.Put(ctx, cfg, mkCP("cp-001", map[string]any{"counter": 1}), mkMeta("loop", 0))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if stored.CheckpointID != "cp-001" {
		t.Fatalf("CheckpointID = %q, want cp-001", stored.CheckpointID)
	}

	got, err := s.GetTuple(ctx, &checkpoint.Config{ThreadID: "thread-1"})
	if err != nil {
		t.Fatalf("GetTuple: %v", err)
	}
	if got == nil {
		t.Fatal("expected tuple")
	}
	if got.Checkpoint.ChannelValues["counter"] != float64(1) {
		t.Fatalf("counter = %v, want 1", got.Checkpoint.ChannelValues["counter"])
	}
}

func TestSaver_PutWrites_RegularAndSpecial(t *testing.T) {
	ctx := context.Background()
	s := newSaver(t)

	stored, err := s.Put(ctx, &checkpoint.Config{ThreadID: "thread-2"}, mkCP("cp-001", nil), mkMeta("loop", 0))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	if err := s.PutWrites(ctx, stored, []checkpoint.PendingWrite{{Channel: "a", Value: "x"}}, "task-1"); err != nil {
		t.Fatalf("PutWrites first regular: %v", err)
	}
	if err := s.PutWrites(ctx, stored, []checkpoint.PendingWrite{{Channel: "a", Value: "x"}}, "task-1"); err != nil {
		t.Fatalf("PutWrites second regular: %v", err)
	}

	if err := s.PutWrites(ctx, stored, []checkpoint.PendingWrite{{Channel: "__error__", Value: "one"}}, "task-1"); err != nil {
		t.Fatalf("PutWrites first special: %v", err)
	}
	if err := s.PutWrites(ctx, stored, []checkpoint.PendingWrite{{Channel: "__error__", Value: "two"}}, "task-1"); err != nil {
		t.Fatalf("PutWrites second special: %v", err)
	}

	got, err := s.GetTuple(ctx, &checkpoint.Config{ThreadID: "thread-2", CheckpointID: "cp-001"})
	if err != nil {
		t.Fatalf("GetTuple: %v", err)
	}
	if got == nil {
		t.Fatal("expected tuple")
	}
	if len(got.PendingWrites) != 2 {
		t.Fatalf("PendingWrites len = %d, want 2", len(got.PendingWrites))
	}

	var sawRegular, sawSpecial bool
	for _, w := range got.PendingWrites {
		if w.Channel == "a" {
			sawRegular = true
		}
		if w.Channel == "__error__" {
			sawSpecial = true
			if w.Value != "two" {
				t.Fatalf("special write value = %v, want two", w.Value)
			}
		}
	}
	if !sawRegular || !sawSpecial {
		t.Fatalf("missing expected writes, regular=%v special=%v", sawRegular, sawSpecial)
	}
}

func TestSaver_PutWrites_SpecialChannels(t *testing.T) {
	ctx := context.Background()
	s := newSaver(t)

	stored, err := s.Put(ctx, &checkpoint.Config{ThreadID: "thread-special-channels"}, mkCP("cp-001", nil), mkMeta("loop", 0))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.PutWrites(ctx, stored, []checkpoint.PendingWrite{{Channel: "__error__", Value: "boom"}, {Channel: "__interrupt__", Value: map[string]any{"reason": "human"}}}, "task-1"); err != nil {
		t.Fatalf("PutWrites: %v", err)
	}

	got, err := s.GetTuple(ctx, &checkpoint.Config{ThreadID: "thread-special-channels", CheckpointID: "cp-001"})
	if err != nil {
		t.Fatalf("GetTuple: %v", err)
	}
	if got == nil {
		t.Fatal("expected tuple")
	}
	if len(got.PendingWrites) != 2 {
		t.Fatalf("PendingWrites len = %d, want 2", len(got.PendingWrites))
	}

	sawError := false
	sawInterrupt := false
	for _, w := range got.PendingWrites {
		switch w.Channel {
		case "__error__":
			sawError = w.Value == "boom"
		case "__interrupt__":
			m, ok := w.Value.(map[string]any)
			if ok && m["reason"] == "human" {
				sawInterrupt = true
			}
		}
	}
	if !sawError || !sawInterrupt {
		t.Fatalf("missing expected special writes, sawError=%v sawInterrupt=%v writes=%+v", sawError, sawInterrupt, got.PendingWrites)
	}
}

func TestSaver_List_FilterBeforeLimit(t *testing.T) {
	ctx := context.Background()
	s := newSaver(t)

	cfg := &checkpoint.Config{ThreadID: "thread-3"}
	_, _ = s.Put(ctx, cfg, mkCP("cp-001", nil), mkMeta("input", -1))
	_, _ = s.Put(ctx, cfg, mkCP("cp-002", nil), mkMeta("loop", 0))
	_, _ = s.Put(ctx, cfg, mkCP("cp-003", nil), mkMeta("loop", 1))

	got, err := s.List(ctx, cfg, checkpoint.ListOptions{
		Filter: map[string]any{"source": "loop"},
		Before: &checkpoint.Config{CheckpointID: "cp-003"},
		Limit:  1,
	})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1", len(got))
	}
	if got[0].Checkpoint.ID != "cp-002" {
		t.Fatalf("checkpoint id = %q, want cp-002", got[0].Checkpoint.ID)
	}
}

func TestSaver_List_FilterExtraMetadata(t *testing.T) {
	ctx := context.Background()
	s := newSaver(t)

	cfg := &checkpoint.Config{ThreadID: "thread-extra-filter"}
	_, _ = s.Put(ctx, cfg, mkCP("cp-001", nil), &checkpoint.CheckpointMetadata{Source: "loop", Step: 0, Extra: map[string]any{"score": 10.0}})
	_, _ = s.Put(ctx, cfg, mkCP("cp-002", nil), &checkpoint.CheckpointMetadata{Source: "loop", Step: 1, Extra: map[string]any{"score": 20.0, "flags": map[string]any{"ok": true}}})

	got, err := s.List(ctx, cfg, checkpoint.ListOptions{Filter: map[string]any{"score": 20.0, "flags": map[string]any{"ok": true}}})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1", len(got))
	}
	if got[0].Checkpoint.ID != "cp-002" {
		t.Fatalf("checkpoint id = %q, want cp-002", got[0].Checkpoint.ID)
	}
}

func TestSaver_List_GlobalSearch(t *testing.T) {
	ctx := context.Background()
	s := newSaver(t)

	marker := "run-global-search"
	if _, err := s.Put(ctx, &checkpoint.Config{ThreadID: "thread-global-a", CheckpointNS: ""}, mkCP("cp-001", nil), &checkpoint.CheckpointMetadata{Source: "input", Step: 0, RunID: marker}); err != nil {
		t.Fatalf("Put thread-global-a: %v", err)
	}
	if _, err := s.Put(ctx, &checkpoint.Config{ThreadID: "thread-global-b", CheckpointNS: ""}, mkCP("cp-002", nil), &checkpoint.CheckpointMetadata{Source: "loop", Step: 1, RunID: marker}); err != nil {
		t.Fatalf("Put thread-global-b: %v", err)
	}

	inputOnly, err := s.List(ctx, nil, checkpoint.ListOptions{Filter: map[string]any{"source": "input", "run_id": marker}})
	if err != nil {
		t.Fatalf("List input-only global search: %v", err)
	}
	if len(inputOnly) != 1 {
		t.Fatalf("input-only global search len = %d, want 1", len(inputOnly))
	}
	if inputOnly[0].Config.ThreadID != "thread-global-a" {
		t.Fatalf("input-only global search thread = %q, want %q", inputOnly[0].Config.ThreadID, "thread-global-a")
	}

	both, err := s.List(ctx, nil, checkpoint.ListOptions{Filter: map[string]any{"run_id": marker}})
	if err != nil {
		t.Fatalf("List global search all: %v", err)
	}
	if len(both) != 2 {
		t.Fatalf("global search len = %d, want 2", len(both))
	}
}

func TestSaver_ClosedDBReturnsErrors(t *testing.T) {
	ctx := context.Background()
	s := newSaver(t)
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	_, err := s.Put(ctx, &checkpoint.Config{ThreadID: "thread-closed"}, mkCP("cp-001", nil), mkMeta("loop", 0))
	if err == nil {
		t.Fatal("expected Put error after close")
	}
	if !strings.Contains(err.Error(), "closed") {
		t.Fatalf("Put error = %v, want closed-db error", err)
	}

	_, err = s.GetTuple(ctx, &checkpoint.Config{ThreadID: "thread-closed"})
	if err == nil {
		t.Fatal("expected GetTuple error after close")
	}
}

func TestSaver_DeleteThread(t *testing.T) {
	ctx := context.Background()
	s := newSaver(t)

	stored, err := s.Put(ctx, &checkpoint.Config{ThreadID: "thread-delete", CheckpointNS: ""}, mkCP("cp-001", nil), mkMeta("loop", 0))
	if err != nil {
		t.Fatalf("Put root: %v", err)
	}
	if _, err := s.Put(ctx, &checkpoint.Config{ThreadID: "thread-delete", CheckpointNS: "child:1"}, mkCP("cp-002", nil), mkMeta("loop", 0)); err != nil {
		t.Fatalf("Put child: %v", err)
	}
	if _, err := s.Put(ctx, &checkpoint.Config{ThreadID: "thread-keep", CheckpointNS: ""}, mkCP("cp-003", nil), mkMeta("loop", 0)); err != nil {
		t.Fatalf("Put keep: %v", err)
	}
	if err := s.PutWrites(ctx, stored, []checkpoint.PendingWrite{{Channel: "a", Value: "x"}}, "task-1"); err != nil {
		t.Fatalf("PutWrites: %v", err)
	}

	if err := s.DeleteThread(ctx, "thread-delete"); err != nil {
		t.Fatalf("DeleteThread: %v", err)
	}

	got, _ := s.GetTuple(ctx, &checkpoint.Config{ThreadID: "thread-delete", CheckpointNS: ""})
	if got != nil {
		t.Fatalf("deleted root tuple = %+v, want nil", got)
	}
	got, _ = s.GetTuple(ctx, &checkpoint.Config{ThreadID: "thread-delete", CheckpointNS: "child:1"})
	if got != nil {
		t.Fatalf("deleted child tuple = %+v, want nil", got)
	}
	got, _ = s.GetTuple(ctx, &checkpoint.Config{ThreadID: "thread-keep", CheckpointNS: ""})
	if got == nil {
		t.Fatal("expected other thread to remain")
	}
}

func TestSaver_DeleteForRuns(t *testing.T) {
	ctx := context.Background()
	s := newSaver(t)

	threadID := "thread-runs"
	storedA, err := s.Put(ctx, &checkpoint.Config{ThreadID: threadID, CheckpointNS: ""}, mkCP("cp-001", nil), &checkpoint.CheckpointMetadata{Source: "loop", Step: 0, RunID: "run-a"})
	if err != nil {
		t.Fatalf("Put run-a root: %v", err)
	}
	storedB, err := s.Put(ctx, &checkpoint.Config{ThreadID: threadID, CheckpointNS: ""}, mkCP("cp-002", nil), &checkpoint.CheckpointMetadata{Source: "loop", Step: 1, RunID: "run-b"})
	if err != nil {
		t.Fatalf("Put run-b root: %v", err)
	}
	storedC, err := s.Put(ctx, &checkpoint.Config{ThreadID: threadID, CheckpointNS: "child:1"}, mkCP("cp-003", nil), &checkpoint.CheckpointMetadata{Source: "loop", Step: 1, RunID: "run-a"})
	if err != nil {
		t.Fatalf("Put run-a child: %v", err)
	}
	if err := s.PutWrites(ctx, storedA, []checkpoint.PendingWrite{{Channel: "ch", Value: "root"}}, "task-1"); err != nil {
		t.Fatalf("PutWrites root: %v", err)
	}
	if err := s.PutWrites(ctx, storedC, []checkpoint.PendingWrite{{Channel: "ch", Value: "child"}}, "task-2"); err != nil {
		t.Fatalf("PutWrites child: %v", err)
	}

	if err := s.DeleteForRuns(ctx, []string{"run-a"}); err != nil {
		t.Fatalf("DeleteForRuns: %v", err)
	}

	got, _ := s.GetTuple(ctx, &checkpoint.Config{ThreadID: threadID, CheckpointNS: "", CheckpointID: "cp-001"})
	if got != nil {
		t.Fatalf("cp-001 should be removed, got %+v", got)
	}
	got, _ = s.GetTuple(ctx, &checkpoint.Config{ThreadID: threadID, CheckpointNS: "child:1", CheckpointID: "cp-003"})
	if got != nil {
		t.Fatalf("cp-003 should be removed, got %+v", got)
	}
	got, _ = s.GetTuple(ctx, &checkpoint.Config{ThreadID: threadID, CheckpointNS: "", CheckpointID: storedB.CheckpointID})
	if got == nil {
		t.Fatal("cp-002 should remain")
	}
}

func TestSaver_CopyThread(t *testing.T) {
	ctx := context.Background()
	s := newSaver(t)

	storedRoot, err := s.Put(ctx, &checkpoint.Config{ThreadID: "thread-src", CheckpointNS: ""}, mkCP("cp-001", map[string]any{"v": 1}), &checkpoint.CheckpointMetadata{Source: "loop", Step: 0, RunID: "run-1"})
	if err != nil {
		t.Fatalf("Put root: %v", err)
	}
	storedChild, err := s.Put(ctx, &checkpoint.Config{ThreadID: "thread-src", CheckpointNS: "child:1"}, mkCP("cp-002", map[string]any{"v": 2}), &checkpoint.CheckpointMetadata{Source: "loop", Step: 1, RunID: "run-1"})
	if err != nil {
		t.Fatalf("Put child: %v", err)
	}
	if err := s.PutWrites(ctx, storedRoot, []checkpoint.PendingWrite{{Channel: "ch", Value: "root"}}, "task-1"); err != nil {
		t.Fatalf("PutWrites root: %v", err)
	}
	if err := s.PutWrites(ctx, storedChild, []checkpoint.PendingWrite{{Channel: "ch", Value: "child"}}, "task-2"); err != nil {
		t.Fatalf("PutWrites child: %v", err)
	}

	if err := s.CopyThread(ctx, "thread-src", "thread-dst"); err != nil {
		t.Fatalf("CopyThread: %v", err)
	}

	gotRoot, _ := s.GetTuple(ctx, &checkpoint.Config{ThreadID: "thread-dst", CheckpointNS: "", CheckpointID: storedRoot.CheckpointID})
	if gotRoot == nil {
		t.Fatal("expected copied root tuple")
	}
	if gotRoot.Checkpoint.ChannelValues["v"] != float64(1) {
		t.Fatalf("copied root value = %v, want 1", gotRoot.Checkpoint.ChannelValues["v"])
	}
	if len(gotRoot.PendingWrites) != 1 || gotRoot.PendingWrites[0].Value != "root" {
		t.Fatalf("copied root writes = %+v, want root write", gotRoot.PendingWrites)
	}
	gotChild, _ := s.GetTuple(ctx, &checkpoint.Config{ThreadID: "thread-dst", CheckpointNS: "child:1", CheckpointID: storedChild.CheckpointID})
	if gotChild == nil {
		t.Fatal("expected copied child tuple")
	}
	if len(gotChild.PendingWrites) != 1 || gotChild.PendingWrites[0].Value != "child" {
		t.Fatalf("copied child writes = %+v, want child write", gotChild.PendingWrites)
	}
}

func TestSaver_Prune(t *testing.T) {
	ctx := context.Background()
	s := newSaver(t)

	cfg := &checkpoint.Config{ThreadID: "thread-prune", CheckpointNS: ""}
	if _, err := s.Put(ctx, cfg, mkCP("cp-001", nil), mkMeta("loop", 0)); err != nil {
		t.Fatalf("Put cp-001: %v", err)
	}
	if _, err := s.Put(ctx, cfg, mkCP("cp-002", nil), mkMeta("loop", 1)); err != nil {
		t.Fatalf("Put cp-002: %v", err)
	}
	stored, err := s.Put(ctx, cfg, mkCP("cp-003", nil), mkMeta("loop", 2))
	if err != nil {
		t.Fatalf("Put cp-003: %v", err)
	}
	if err := s.PutWrites(ctx, stored, []checkpoint.PendingWrite{{Channel: "ch", Value: "latest"}}, "task-1"); err != nil {
		t.Fatalf("PutWrites latest: %v", err)
	}

	if err := s.Prune(ctx, []string{"thread-prune"}, checkpoint.PruneStrategyKeepLatest); err != nil {
		t.Fatalf("Prune keep_latest: %v", err)
	}
	list, err := s.List(ctx, cfg, checkpoint.ListOptions{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 1 || list[0].Checkpoint.ID != "cp-003" {
		t.Fatalf("post-prune list = %+v, want only cp-003", list)
	}
	if len(list[0].PendingWrites) != 1 || list[0].PendingWrites[0].Value != "latest" {
		t.Fatalf("post-prune writes = %+v, want latest write", list[0].PendingWrites)
	}

	if err := s.Prune(ctx, []string{"thread-prune"}, checkpoint.PruneStrategyDelete); err != nil {
		t.Fatalf("Prune delete: %v", err)
	}
	got, _ := s.GetTuple(ctx, cfg)
	if got != nil {
		t.Fatalf("thread should be deleted, got %+v", got)
	}
}

func TestSaver_Prune_InvalidStrategy(t *testing.T) {
	ctx := context.Background()
	s := newSaver(t)
	if err := s.Prune(ctx, []string{"thread-prune-invalid"}, checkpoint.PruneStrategy("bad")); err == nil {
		t.Fatal("expected invalid strategy error")
	}
}
