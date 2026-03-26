package checkpoint_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/checkpoint"
)

func mkCfg(threadID, cpID string) *checkpoint.Config {
	return &checkpoint.Config{ThreadID: threadID, CheckpointID: cpID}
}

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

func TestInMemorySaver_PutAndGetTuple(t *testing.T) {
	ctx := context.Background()
	s := checkpoint.NewInMemorySaver()

	cfg := mkCfg("thread-1", "")
	cp := mkCP("cp-001", map[string]any{"counter": 1})

	stored, err := s.Put(ctx, cfg, cp, mkMeta("loop", 0))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if stored.CheckpointID != "cp-001" {
		t.Errorf("returned config CheckpointID = %q, want %q", stored.CheckpointID, "cp-001")
	}

	got, err := s.GetTuple(ctx, mkCfg("thread-1", ""))
	if err != nil {
		t.Fatalf("GetTuple: %v", err)
	}
	if got == nil {
		t.Fatal("GetTuple returned nil, want tuple")
	}
	if got.Checkpoint.ChannelValues["counter"] != 1 {
		t.Errorf("counter = %v, want 1", got.Checkpoint.ChannelValues["counter"])
	}
}

func TestInMemorySaver_GetLatestCheckpoint(t *testing.T) {
	ctx := context.Background()
	s := checkpoint.NewInMemorySaver()
	cfg := mkCfg("thread-2", "")

	// Store three checkpoints; IDs are lexicographically ordered strings.
	for _, id := range []string{"cp-001", "cp-002", "cp-003"} {
		_, err := s.Put(ctx, cfg, mkCP(id, map[string]any{"id": id}), mkMeta("loop", 0))
		if err != nil {
			t.Fatalf("Put %s: %v", id, err)
		}
		// Slight delay so IDs differ.
		time.Sleep(time.Millisecond)
	}

	got, err := s.GetTuple(ctx, mkCfg("thread-2", ""))
	if err != nil {
		t.Fatalf("GetTuple: %v", err)
	}
	if got == nil {
		t.Fatal("GetTuple returned nil")
	}
	if got.Checkpoint.ID != "cp-003" {
		t.Errorf("latest ID = %q, want %q", got.Checkpoint.ID, "cp-003")
	}
}

func TestInMemorySaver_GetLatestCheckpoint_IsNamespaceScoped(t *testing.T) {
	ctx := context.Background()
	s := checkpoint.NewInMemorySaver()

	_, err := s.Put(ctx, &checkpoint.Config{ThreadID: "thread-ns", CheckpointNS: ""}, mkCP("cp-001", map[string]any{"scope": "root"}), mkMeta("loop", 0))
	if err != nil {
		t.Fatalf("Put root cp-001: %v", err)
	}
	_, err = s.Put(ctx, &checkpoint.Config{ThreadID: "thread-ns", CheckpointNS: "child"}, mkCP("cp-100", map[string]any{"scope": "child"}), mkMeta("loop", 0))
	if err != nil {
		t.Fatalf("Put child cp-100: %v", err)
	}
	_, err = s.Put(ctx, &checkpoint.Config{ThreadID: "thread-ns", CheckpointNS: ""}, mkCP("cp-002", map[string]any{"scope": "root-latest"}), mkMeta("loop", 1))
	if err != nil {
		t.Fatalf("Put root cp-002: %v", err)
	}

	gotRoot, err := s.GetTuple(ctx, &checkpoint.Config{ThreadID: "thread-ns", CheckpointNS: ""})
	if err != nil {
		t.Fatalf("GetTuple root: %v", err)
	}
	if gotRoot == nil || gotRoot.Checkpoint.ID != "cp-002" {
		t.Fatalf("root latest checkpoint = %#v, want cp-002", gotRoot)
	}

	gotChild, err := s.GetTuple(ctx, &checkpoint.Config{ThreadID: "thread-ns", CheckpointNS: "child"})
	if err != nil {
		t.Fatalf("GetTuple child: %v", err)
	}
	if gotChild == nil || gotChild.Checkpoint.ID != "cp-100" {
		t.Fatalf("child latest checkpoint = %#v, want cp-100", gotChild)
	}
}

func TestInMemorySaver_GetSpecificCheckpoint(t *testing.T) {
	ctx := context.Background()
	s := checkpoint.NewInMemorySaver()
	cfg := mkCfg("thread-3", "")

	for _, id := range []string{"cp-001", "cp-002"} {
		_, _ = s.Put(ctx, cfg, mkCP(id, map[string]any{"id": id}), mkMeta("loop", 0))
	}

	// Fetch specific older checkpoint.
	got, err := s.GetTuple(ctx, mkCfg("thread-3", "cp-001"))
	if err != nil {
		t.Fatalf("GetTuple: %v", err)
	}
	if got == nil {
		t.Fatal("GetTuple returned nil")
	}
	if got.Checkpoint.ID != "cp-001" {
		t.Errorf("ID = %q, want %q", got.Checkpoint.ID, "cp-001")
	}
}

func TestInMemorySaver_ParentConfig(t *testing.T) {
	ctx := context.Background()
	s := checkpoint.NewInMemorySaver()
	cfg := mkCfg("thread-4", "")

	stored1, _ := s.Put(ctx, cfg, mkCP("cp-001", nil), mkMeta("loop", 0))
	_, _ = s.Put(ctx, stored1, mkCP("cp-002", nil), mkMeta("loop", 1))

	got, _ := s.GetTuple(ctx, mkCfg("thread-4", "cp-002"))
	if got == nil || got.ParentConfig == nil {
		t.Fatal("expected ParentConfig to be set")
	}
	if got.ParentConfig.CheckpointID != "cp-001" {
		t.Errorf("ParentConfig.CheckpointID = %q, want %q", got.ParentConfig.CheckpointID, "cp-001")
	}
}

func TestInMemorySaver_ParentConfig_UsesProvidedCheckpointID(t *testing.T) {
	ctx := context.Background()
	s := checkpoint.NewInMemorySaver()

	_, _ = s.Put(ctx, mkCfg("thread-parent", ""), mkCP("cp-001", nil), mkMeta("loop", 0))
	_, _ = s.Put(ctx, mkCfg("thread-parent", ""), mkCP("cp-002", nil), mkMeta("loop", 1))
	_, _ = s.Put(ctx, &checkpoint.Config{ThreadID: "thread-parent", CheckpointID: "cp-001"}, mkCP("cp-003", nil), mkMeta("fork", 2))

	got, _ := s.GetTuple(ctx, mkCfg("thread-parent", "cp-003"))
	if got == nil || got.ParentConfig == nil {
		t.Fatal("expected ParentConfig to be set")
	}
	if got.ParentConfig.CheckpointID != "cp-001" {
		t.Fatalf("ParentConfig.CheckpointID = %q, want %q", got.ParentConfig.CheckpointID, "cp-001")
	}
}

func TestInMemorySaver_GetTuple_Missing(t *testing.T) {
	ctx := context.Background()
	s := checkpoint.NewInMemorySaver()

	got, err := s.GetTuple(ctx, mkCfg("no-such-thread", ""))
	if err != nil {
		t.Fatalf("GetTuple error: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil, got %+v", got)
	}
}

func TestInMemorySaver_PutWrites(t *testing.T) {
	ctx := context.Background()
	s := checkpoint.NewInMemorySaver()

	cfg := mkCfg("thread-5", "")
	stored, _ := s.Put(ctx, cfg, mkCP("cp-001", nil), mkMeta("loop", 0))

	writes := []checkpoint.PendingWrite{
		{TaskID: "task-1", Channel: "messages", Value: "hello"},
	}
	if err := s.PutWrites(ctx, stored, writes, "task-1"); err != nil {
		t.Fatalf("PutWrites: %v", err)
	}

	got, _ := s.GetTuple(ctx, mkCfg("thread-5", "cp-001"))
	if len(got.PendingWrites) != 1 {
		t.Errorf("PendingWrites len = %d, want 1", len(got.PendingWrites))
	}
	if got.PendingWrites[0].Value != "hello" {
		t.Errorf("PendingWrites[0].Value = %v, want %q", got.PendingWrites[0].Value, "hello")
	}
}

func TestInMemorySaver_List(t *testing.T) {
	ctx := context.Background()
	s := checkpoint.NewInMemorySaver()
	cfg := mkCfg("thread-6", "")

	for _, id := range []string{"cp-001", "cp-002", "cp-003"} {
		_, _ = s.Put(ctx, cfg, mkCP(id, nil), mkMeta("loop", 0))
	}

	all, err := s.List(ctx, cfg, checkpoint.ListOptions{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(all) != 3 {
		t.Errorf("List len = %d, want 3", len(all))
	}
	// Newest first.
	if all[0].Checkpoint.ID != "cp-003" {
		t.Errorf("all[0].ID = %q, want %q", all[0].Checkpoint.ID, "cp-003")
	}
}

func TestInMemorySaver_List_Limit(t *testing.T) {
	ctx := context.Background()
	s := checkpoint.NewInMemorySaver()
	cfg := mkCfg("thread-7", "")

	for _, id := range []string{"cp-001", "cp-002", "cp-003"} {
		_, _ = s.Put(ctx, cfg, mkCP(id, nil), mkMeta("loop", 0))
	}

	got, _ := s.List(ctx, cfg, checkpoint.ListOptions{Limit: 2})
	if len(got) != 2 {
		t.Errorf("List limit=2 len = %d, want 2", len(got))
	}
}

func TestInMemorySaver_List_Before(t *testing.T) {
	ctx := context.Background()
	s := checkpoint.NewInMemorySaver()
	cfg := mkCfg("thread-8", "")

	for _, id := range []string{"cp-001", "cp-002", "cp-003"} {
		_, _ = s.Put(ctx, cfg, mkCP(id, nil), mkMeta("loop", 0))
	}

	// Before cp-003 → should return cp-001, cp-002.
	got, _ := s.List(ctx, cfg, checkpoint.ListOptions{
		Before: &checkpoint.Config{CheckpointID: "cp-003"},
	})
	if len(got) != 2 {
		t.Errorf("List before=cp-003 len = %d, want 2", len(got))
	}
}

func TestInMemorySaver_List_Filter(t *testing.T) {
	ctx := context.Background()
	s := checkpoint.NewInMemorySaver()
	cfg := mkCfg("thread-filter", "")

	_, _ = s.Put(ctx, cfg, mkCP("cp-001", nil), mkMeta("input", -1))
	_, _ = s.Put(ctx, cfg, mkCP("cp-002", nil), mkMeta("loop", 0))

	got, err := s.List(ctx, cfg, checkpoint.ListOptions{Filter: map[string]any{"source": "loop"}})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("List filtered len = %d, want 1", len(got))
	}
	if got[0].Checkpoint.ID != "cp-002" {
		t.Fatalf("filtered checkpoint ID = %q, want %q", got[0].Checkpoint.ID, "cp-002")
	}
}

func TestInMemorySaver_List_FilterExtraMetadata(t *testing.T) {
	ctx := context.Background()
	s := checkpoint.NewInMemorySaver()
	cfg := mkCfg("thread-filter-extra", "")

	_, _ = s.Put(ctx, cfg, mkCP("cp-001", nil), &checkpoint.CheckpointMetadata{Source: "loop", Step: 0, Extra: map[string]any{"score": 10.0}})
	_, _ = s.Put(ctx, cfg, mkCP("cp-002", nil), &checkpoint.CheckpointMetadata{Source: "loop", Step: 1, Extra: map[string]any{"score": 20.0, "flags": map[string]any{"ok": true}}})

	got, err := s.List(ctx, cfg, checkpoint.ListOptions{Filter: map[string]any{"score": 20.0, "flags": map[string]any{"ok": true}}})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("List filtered len = %d, want 1", len(got))
	}
	if got[0].Checkpoint.ID != "cp-002" {
		t.Fatalf("filtered checkpoint ID = %q, want %q", got[0].Checkpoint.ID, "cp-002")
	}
}

func TestInMemorySaver_List_GlobalSearch(t *testing.T) {
	ctx := context.Background()
	s := checkpoint.NewInMemorySaver()

	marker := "run-global-search"
	_, _ = s.Put(ctx, &checkpoint.Config{ThreadID: "thread-global-a"}, mkCP("cp-001", nil), &checkpoint.CheckpointMetadata{Source: "input", Step: 0, RunID: marker})
	_, _ = s.Put(ctx, &checkpoint.Config{ThreadID: "thread-global-b"}, mkCP("cp-002", nil), &checkpoint.CheckpointMetadata{Source: "loop", Step: 1, RunID: marker})

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

func TestInMemorySaver_GetTuple_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s := checkpoint.NewInMemorySaver()
	_, err := s.GetTuple(ctx, mkCfg("thread-x", ""))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestInMemorySaver_Put_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s := checkpoint.NewInMemorySaver()
	_, err := s.Put(ctx, mkCfg("thread-canceled-put", ""), mkCP("cp-001", nil), mkMeta("loop", 0))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestInMemorySaver_List_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s := checkpoint.NewInMemorySaver()
	_, err := s.List(ctx, mkCfg("thread-canceled-list", ""), checkpoint.ListOptions{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestInMemorySaver_Put_RequiresThreadID(t *testing.T) {
	ctx := context.Background()
	s := checkpoint.NewInMemorySaver()
	_, err := s.Put(ctx, &checkpoint.Config{}, mkCP("cp-001", nil), mkMeta("loop", 0))
	if err == nil {
		t.Error("expected error for empty ThreadID, got nil")
	}
}

func TestInMemorySaver_NamespaceIsolation(t *testing.T) {
	ctx := context.Background()
	s := checkpoint.NewInMemorySaver()

	cfgRoot := &checkpoint.Config{ThreadID: "thread-9", CheckpointNS: ""}
	cfgSub := &checkpoint.Config{ThreadID: "thread-9", CheckpointNS: "subgraph"}

	_, _ = s.Put(ctx, cfgRoot, mkCP("cp-001", map[string]any{"ns": "root"}), mkMeta("loop", 0))
	_, _ = s.Put(ctx, cfgSub, mkCP("cp-002", map[string]any{"ns": "sub"}), mkMeta("loop", 0))

	gotRoot, _ := s.GetTuple(ctx, &checkpoint.Config{ThreadID: "thread-9", CheckpointNS: ""})
	gotSub, _ := s.GetTuple(ctx, &checkpoint.Config{ThreadID: "thread-9", CheckpointNS: "subgraph"})

	if gotRoot == nil || gotRoot.Checkpoint.ChannelValues["ns"] != "root" {
		t.Error("root namespace isolation failed")
	}
	if gotSub == nil || gotSub.Checkpoint.ChannelValues["ns"] != "sub" {
		t.Error("sub namespace isolation failed")
	}
}

func TestInMemorySaver_WithJSONSerializer(t *testing.T) {
	ctx := context.Background()
	s := checkpoint.NewInMemorySaverWithSerializer(checkpoint.JSONSerializer{})

	cfg := &checkpoint.Config{ThreadID: "thread-json"}
	cp := mkCP("cp-json", map[string]any{"msg": "hello"})
	if _, err := s.Put(ctx, cfg, cp, mkMeta("loop", 0)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := s.GetTuple(ctx, cfg)
	if err != nil {
		t.Fatalf("GetTuple: %v", err)
	}
	if got == nil {
		t.Fatal("expected tuple")
	}
	if got.Checkpoint.ChannelValues["msg"] != "hello" {
		t.Fatalf("msg = %v, want hello", got.Checkpoint.ChannelValues["msg"])
	}
}

func TestInMemorySaver_ReconstructsChannelValuesFromVersionedBlobs(t *testing.T) {
	ctx := context.Background()
	s := checkpoint.NewInMemorySaverWithSerializer(checkpoint.JSONSerializer{})

	rootCfg := &checkpoint.Config{ThreadID: "thread-blob", CheckpointNS: ""}
	if _, err := s.Put(ctx, rootCfg, &checkpoint.Checkpoint{
		V:               1,
		ID:              "cp-001",
		TS:              time.Now().UTC().Format(time.RFC3339Nano),
		ChannelValues:   map[string]any{"a": "seed"},
		ChannelVersions: map[string]checkpoint.Version{"a": "1"},
		VersionsSeen:    map[string]map[string]checkpoint.Version{},
	}, mkMeta("loop", 0)); err != nil {
		t.Fatalf("Put cp-001: %v", err)
	}

	if _, err := s.Put(ctx, &checkpoint.Config{ThreadID: "thread-blob", CheckpointNS: "", CheckpointID: "cp-001"}, &checkpoint.Checkpoint{
		V:               1,
		ID:              "cp-002",
		TS:              time.Now().UTC().Format(time.RFC3339Nano),
		ChannelValues:   map[string]any{"b": "next"},
		ChannelVersions: map[string]checkpoint.Version{"a": "1", "b": "1"},
		VersionsSeen:    map[string]map[string]checkpoint.Version{},
	}, mkMeta("loop", 1)); err != nil {
		t.Fatalf("Put cp-002: %v", err)
	}

	got, err := s.GetTuple(ctx, &checkpoint.Config{ThreadID: "thread-blob", CheckpointNS: "", CheckpointID: "cp-002"})
	if err != nil {
		t.Fatalf("GetTuple cp-002: %v", err)
	}
	if got == nil {
		t.Fatal("expected tuple")
	}
	if got.Checkpoint.ChannelValues["a"] != "seed" {
		t.Fatalf("channel a = %v, want seed", got.Checkpoint.ChannelValues["a"])
	}
	if got.Checkpoint.ChannelValues["b"] != "next" {
		t.Fatalf("channel b = %v, want next", got.Checkpoint.ChannelValues["b"])
	}
}

func TestInMemorySaver_PutWrites_UsesTaskIDParam(t *testing.T) {
	ctx := context.Background()
	s := checkpoint.NewInMemorySaver()

	stored, _ := s.Put(ctx, &checkpoint.Config{ThreadID: "thread-task"}, mkCP("cp-001", nil), mkMeta("loop", 0))
	err := s.PutWrites(ctx, stored, []checkpoint.PendingWrite{{TaskID: "ignored", Channel: "messages", Value: "x"}}, "task-from-param")
	if err != nil {
		t.Fatalf("PutWrites: %v", err)
	}

	got, _ := s.GetTuple(ctx, &checkpoint.Config{ThreadID: "thread-task", CheckpointID: "cp-001"})
	if len(got.PendingWrites) != 1 {
		t.Fatalf("PendingWrites len = %d, want 1", len(got.PendingWrites))
	}
	if got.PendingWrites[0].TaskID != "task-from-param" {
		t.Fatalf("TaskID = %q, want %q", got.PendingWrites[0].TaskID, "task-from-param")
	}
}

func TestInMemorySaver_PutWrites_IdempotentRegularWrites(t *testing.T) {
	ctx := context.Background()
	s := checkpoint.NewInMemorySaver()

	stored, _ := s.Put(ctx, &checkpoint.Config{ThreadID: "thread-idempotent"}, mkCP("cp-001", nil), mkMeta("loop", 0))
	writes := []checkpoint.PendingWrite{{Channel: "a", Value: 1}, {Channel: "b", Value: 2}}
	if err := s.PutWrites(ctx, stored, writes, "task-1"); err != nil {
		t.Fatalf("PutWrites first: %v", err)
	}
	if err := s.PutWrites(ctx, stored, writes, "task-1"); err != nil {
		t.Fatalf("PutWrites second: %v", err)
	}

	got, _ := s.GetTuple(ctx, &checkpoint.Config{ThreadID: "thread-idempotent", CheckpointID: "cp-001"})
	if len(got.PendingWrites) != 2 {
		t.Fatalf("PendingWrites len = %d, want 2", len(got.PendingWrites))
	}
}

func TestInMemorySaver_PutWrites_SpecialWriteReplacesPriorValue(t *testing.T) {
	ctx := context.Background()
	s := checkpoint.NewInMemorySaver()

	stored, _ := s.Put(ctx, &checkpoint.Config{ThreadID: "thread-special"}, mkCP("cp-001", nil), mkMeta("loop", 0))
	if err := s.PutWrites(ctx, stored, []checkpoint.PendingWrite{{Channel: "__error__", Value: "first"}}, "task-1"); err != nil {
		t.Fatalf("PutWrites first: %v", err)
	}
	if err := s.PutWrites(ctx, stored, []checkpoint.PendingWrite{{Channel: "__error__", Value: "second"}}, "task-1"); err != nil {
		t.Fatalf("PutWrites second: %v", err)
	}

	got, _ := s.GetTuple(ctx, &checkpoint.Config{ThreadID: "thread-special", CheckpointID: "cp-001"})
	if len(got.PendingWrites) != 1 {
		t.Fatalf("PendingWrites len = %d, want 1", len(got.PendingWrites))
	}
	if got.PendingWrites[0].Value != "second" {
		t.Fatalf("Value = %v, want %q", got.PendingWrites[0].Value, "second")
	}
}

func TestInMemorySaver_PutWrites_SpecialChannels(t *testing.T) {
	ctx := context.Background()
	s := checkpoint.NewInMemorySaver()

	stored, _ := s.Put(ctx, &checkpoint.Config{ThreadID: "thread-special-channels"}, mkCP("cp-001", nil), mkMeta("loop", 0))
	if err := s.PutWrites(ctx, stored, []checkpoint.PendingWrite{{Channel: "__error__", Value: "boom"}, {Channel: "__interrupt__", Value: map[string]any{"reason": "human"}}}, "task-1"); err != nil {
		t.Fatalf("PutWrites: %v", err)
	}

	got, _ := s.GetTuple(ctx, &checkpoint.Config{ThreadID: "thread-special-channels", CheckpointID: "cp-001"})
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

func TestInMemorySaver_DeleteThread(t *testing.T) {
	ctx := context.Background()
	s := checkpoint.NewInMemorySaver()

	rootCfg := &checkpoint.Config{ThreadID: "thread-delete", CheckpointNS: ""}
	childCfg := &checkpoint.Config{ThreadID: "thread-delete", CheckpointNS: "child:1"}
	otherCfg := &checkpoint.Config{ThreadID: "thread-keep", CheckpointNS: ""}

	rootStored, _ := s.Put(ctx, rootCfg, mkCP("cp-001", nil), mkMeta("loop", 0))
	_, _ = s.Put(ctx, childCfg, mkCP("cp-002", nil), mkMeta("loop", 0))
	_, _ = s.Put(ctx, otherCfg, mkCP("cp-003", nil), mkMeta("loop", 0))
	if err := s.PutWrites(ctx, rootStored, []checkpoint.PendingWrite{{Channel: "a", Value: "x"}}, "task-1"); err != nil {
		t.Fatalf("PutWrites: %v", err)
	}

	if err := s.DeleteThread(ctx, "thread-delete"); err != nil {
		t.Fatalf("DeleteThread: %v", err)
	}

	gotRoot, _ := s.GetTuple(ctx, &checkpoint.Config{ThreadID: "thread-delete", CheckpointNS: ""})
	if gotRoot != nil {
		t.Fatalf("root thread tuple = %+v, want nil", gotRoot)
	}
	gotChild, _ := s.GetTuple(ctx, &checkpoint.Config{ThreadID: "thread-delete", CheckpointNS: "child:1"})
	if gotChild != nil {
		t.Fatalf("child thread tuple = %+v, want nil", gotChild)
	}
	gotOther, _ := s.GetTuple(ctx, &checkpoint.Config{ThreadID: "thread-keep", CheckpointNS: ""})
	if gotOther == nil {
		t.Fatal("expected other thread to remain")
	}
}

func TestInMemorySaver_DeleteForRuns(t *testing.T) {
	ctx := context.Background()
	s := checkpoint.NewInMemorySaver()

	threadID := "thread-runs"
	storedA, _ := s.Put(ctx, &checkpoint.Config{ThreadID: threadID, CheckpointNS: ""}, mkCP("cp-001", nil), &checkpoint.CheckpointMetadata{Source: "loop", Step: 0, RunID: "run-a"})
	storedB, _ := s.Put(ctx, storedA, mkCP("cp-002", nil), &checkpoint.CheckpointMetadata{Source: "loop", Step: 1, RunID: "run-b"})
	storedC, _ := s.Put(ctx, &checkpoint.Config{ThreadID: threadID, CheckpointNS: "child:1"}, mkCP("cp-003", nil), &checkpoint.CheckpointMetadata{Source: "loop", Step: 1, RunID: "run-a"})

	if err := s.PutWrites(ctx, storedA, []checkpoint.PendingWrite{{Channel: "a", Value: 1}}, "task-1"); err != nil {
		t.Fatalf("PutWrites A: %v", err)
	}
	if err := s.PutWrites(ctx, storedC, []checkpoint.PendingWrite{{Channel: "a", Value: 2}}, "task-2"); err != nil {
		t.Fatalf("PutWrites C: %v", err)
	}

	if err := s.DeleteForRuns(ctx, []string{"run-a"}); err != nil {
		t.Fatalf("DeleteForRuns: %v", err)
	}

	gotA, _ := s.GetTuple(ctx, &checkpoint.Config{ThreadID: threadID, CheckpointNS: "", CheckpointID: "cp-001"})
	if gotA != nil {
		t.Fatalf("cp-001 should be removed, got %+v", gotA)
	}
	gotC, _ := s.GetTuple(ctx, &checkpoint.Config{ThreadID: threadID, CheckpointNS: "child:1", CheckpointID: "cp-003"})
	if gotC != nil {
		t.Fatalf("cp-003 should be removed, got %+v", gotC)
	}
	gotB, _ := s.GetTuple(ctx, &checkpoint.Config{ThreadID: threadID, CheckpointNS: "", CheckpointID: storedB.CheckpointID})
	if gotB == nil {
		t.Fatal("cp-002 should remain")
	}
}

func TestInMemorySaver_CopyThread(t *testing.T) {
	ctx := context.Background()
	s := checkpoint.NewInMemorySaver()

	storedRoot, _ := s.Put(ctx, &checkpoint.Config{ThreadID: "thread-src", CheckpointNS: ""}, mkCP("cp-001", map[string]any{"v": 1}), &checkpoint.CheckpointMetadata{Source: "loop", Step: 0, RunID: "run-1"})
	storedChild, _ := s.Put(ctx, &checkpoint.Config{ThreadID: "thread-src", CheckpointNS: "child:1"}, mkCP("cp-002", map[string]any{"v": 2}), &checkpoint.CheckpointMetadata{Source: "loop", Step: 1, RunID: "run-1"})
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
		t.Fatal("expected copied root checkpoint")
	}
	if gotRoot.Checkpoint.ChannelValues["v"] != 1 {
		t.Fatalf("copied root value = %v, want 1", gotRoot.Checkpoint.ChannelValues["v"])
	}
	if len(gotRoot.PendingWrites) != 1 || gotRoot.PendingWrites[0].Value != "root" {
		t.Fatalf("copied root writes = %+v, want one root write", gotRoot.PendingWrites)
	}
	gotChild, _ := s.GetTuple(ctx, &checkpoint.Config{ThreadID: "thread-dst", CheckpointNS: "child:1", CheckpointID: storedChild.CheckpointID})
	if gotChild == nil {
		t.Fatal("expected copied child checkpoint")
	}
	if len(gotChild.PendingWrites) != 1 || gotChild.PendingWrites[0].Value != "child" {
		t.Fatalf("copied child writes = %+v, want one child write", gotChild.PendingWrites)
	}
}

func TestInMemorySaver_Prune(t *testing.T) {
	ctx := context.Background()
	s := checkpoint.NewInMemorySaver()

	cfg := &checkpoint.Config{ThreadID: "thread-prune", CheckpointNS: ""}
	first, _ := s.Put(ctx, cfg, mkCP("cp-001", nil), mkMeta("loop", 0))
	second, _ := s.Put(ctx, first, mkCP("cp-002", nil), mkMeta("loop", 1))
	third, _ := s.Put(ctx, second, mkCP("cp-003", nil), mkMeta("loop", 2))
	if err := s.PutWrites(ctx, third, []checkpoint.PendingWrite{{Channel: "ch", Value: "latest"}}, "task-1"); err != nil {
		t.Fatalf("PutWrites latest: %v", err)
	}

	if err := s.Prune(ctx, []string{"thread-prune"}, checkpoint.PruneStrategyKeepLatest); err != nil {
		t.Fatalf("Prune keep_latest: %v", err)
	}

	listed, err := s.List(ctx, cfg, checkpoint.ListOptions{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(listed) != 1 || listed[0].Checkpoint.ID != "cp-003" {
		t.Fatalf("post-prune list = %+v, want only cp-003", listed)
	}
	if len(listed[0].PendingWrites) != 1 || listed[0].PendingWrites[0].Value != "latest" {
		t.Fatalf("post-prune writes = %+v, want latest write", listed[0].PendingWrites)
	}

	if err := s.Prune(ctx, []string{"thread-prune"}, checkpoint.PruneStrategyDelete); err != nil {
		t.Fatalf("Prune delete: %v", err)
	}
	gone, _ := s.GetTuple(ctx, cfg)
	if gone != nil {
		t.Fatalf("expected thread deleted, got %+v", gone)
	}
}

func TestInMemorySaver_Prune_InvalidStrategy(t *testing.T) {
	ctx := context.Background()
	s := checkpoint.NewInMemorySaver()
	if err := s.Prune(ctx, []string{"thread-prune-invalid"}, checkpoint.PruneStrategy("bad")); err == nil {
		t.Fatal("expected invalid strategy error")
	}
}

func TestInMemorySaver_PutWrites_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s := checkpoint.NewInMemorySaver()
	stored, _ := s.Put(context.Background(), &checkpoint.Config{ThreadID: "thread-cancel"}, mkCP("cp-001", nil), mkMeta("loop", 0))
	err := s.PutWrites(ctx, stored, []checkpoint.PendingWrite{{Channel: "a", Value: 1}}, "task-1")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}
