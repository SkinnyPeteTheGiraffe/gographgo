package graph

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/checkpoint"
)

type testAuthoritativeStateStore struct {
	mu                     sync.Mutex
	mode                   StateStoreMode
	values                 map[string]any
	version                string
	bumpVersionAfterReplay bool
	replayReads            int
}

func (s *testAuthoritativeStateStore) Mode() StateStoreMode {
	if s.mode == "" {
		return StateStoreModeAuthoritativeExternalState
	}
	return s.mode
}

func (s *testAuthoritativeStateStore) Read(_ context.Context, req StateStoreReadRequest) (*AuthoritativeStateSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	values := make(map[string]any, len(s.values))
	for k, v := range s.values {
		values[k] = v
	}
	version := s.version
	if s.bumpVersionAfterReplay && req.CheckpointID != "" {
		s.replayReads++
		if s.replayReads >= 1 {
			s.version = "v2"
		}
	}
	return &AuthoritativeStateSnapshot{Values: values, Version: version}, nil
}

func TestAuthoritativeExternalStateMode_RequiresStateStore(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNode("noop", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NoNodeResult(), nil
	})
	g.AddEdge(Start, "noop")
	g.AddEdge("noop", End)

	saver := checkpoint.NewInMemorySaver()
	compiled, err := g.Compile(CompileOptions{
		Checkpointer: saver,
		StateMode:    StateStoreModeAuthoritativeExternalState,
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ctx := WithConfig(context.Background(), Config{ThreadID: "missing-state-store"})
	_, err = compiled.Invoke(ctx, map[string]any{"value": 1})
	if err == nil {
		t.Fatal("expected startup error for missing state store")
	}
	if got := err.Error(); got == "" || !strings.Contains(got, "authoritative_external_state") {
		t.Fatalf("err = %v, want authoritative_external_state guidance", err)
	}
}

func TestAuthoritativeExternalStateMode_RequiresCheckpointer(t *testing.T) {
	store := &testAuthoritativeStateStore{
		mode:    StateStoreModeAuthoritativeExternalState,
		values:  map[string]any{"counter": 1},
		version: "v1",
	}

	g := NewStateGraph[map[string]any]()
	g.AddNode("noop", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NoNodeResult(), nil
	})
	g.AddEdge(Start, "noop")
	g.AddEdge("noop", End)

	compiled, err := g.Compile(CompileOptions{StateStore: store, StateMode: StateStoreModeAuthoritativeExternalState})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	_, err = compiled.Invoke(WithConfig(context.Background(), Config{ThreadID: "missing-checkpointer"}), map[string]any{})
	if err == nil {
		t.Fatal("expected startup error for missing checkpointer")
	}
	if !strings.Contains(err.Error(), "requires a checkpointer") {
		t.Fatalf("err = %v, want missing checkpointer guidance", err)
	}
}

func TestAuthoritativeExternalStateMode_HappyPathAndMetadataVersion(t *testing.T) {
	store := &testAuthoritativeStateStore{
		mode:    StateStoreModeAuthoritativeExternalState,
		values:  map[string]any{"counter": 41},
		version: "v1",
	}

	g := NewStateGraph[map[string]any]()
	g.AddNode("read", func(_ context.Context, state map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"seen": state["counter"]})), nil
	})
	g.AddEdge(Start, "read")
	g.AddEdge("read", End)

	saver := checkpoint.NewInMemorySaver()
	compiled, err := g.Compile(CompileOptions{
		Checkpointer: saver,
		StateStore:   store,
		StateMode:    StateStoreModeAuthoritativeExternalState,
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ctx := WithConfig(context.Background(), Config{ThreadID: "external-happy"})
	out, err := compiled.Invoke(ctx, map[string]any{"counter": 0})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	result := out.Value.(map[string]any)
	if result["seen"] != 41 {
		t.Fatalf("seen = %v, want 41 from authoritative state", result["seen"])
	}

	tuple, err := saver.GetTuple(context.Background(), &checkpoint.Config{ThreadID: "external-happy"})
	if err != nil {
		t.Fatalf("get tuple: %v", err)
	}
	if tuple == nil || tuple.Metadata == nil || tuple.Metadata.Extra == nil {
		t.Fatalf("tuple metadata extra missing: %#v", tuple)
	}
	if tuple.Metadata.Extra[ExternalStateVersionMetadataKey] != "v1" {
		t.Fatalf("metadata external version = %v, want v1", tuple.Metadata.Extra[ExternalStateVersionMetadataKey])
	}
}

func TestAuthoritativeExternalStateMode_ReplayConflict(t *testing.T) {
	store := &testAuthoritativeStateStore{
		mode:    StateStoreModeAuthoritativeExternalState,
		values:  map[string]any{"counter": 1},
		version: "v1",
	}

	g := NewStateGraph[map[string]any]()
	g.AddNode("noop", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NoNodeResult(), nil
	})
	g.AddEdge(Start, "noop")
	g.AddEdge("noop", End)

	saver := checkpoint.NewInMemorySaver()
	compiled, err := g.Compile(CompileOptions{Checkpointer: saver, StateStore: store, StateMode: StateStoreModeAuthoritativeExternalState})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	baseCfg := Config{ThreadID: "external-replay"}
	ctx := WithConfig(context.Background(), baseCfg)
	if _, err := compiled.Invoke(ctx, map[string]any{}); err != nil {
		t.Fatalf("first invoke: %v", err)
	}
	tuple, err := saver.GetTuple(context.Background(), &checkpoint.Config{ThreadID: "external-replay"})
	if err != nil {
		t.Fatalf("get tuple: %v", err)
	}

	store.mu.Lock()
	store.version = "v2"
	store.mu.Unlock()

	replayCfg := baseCfg
	replayCfg.CheckpointID = tuple.Checkpoint.ID
	_, err = compiled.Invoke(WithConfig(context.Background(), replayCfg), map[string]any{})
	if err == nil {
		t.Fatal("expected replay conflict error")
	}
	var conflict *ExternalStateConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("err type = %T, want *ExternalStateConflictError (err=%v)", err, err)
	}
}

func TestAuthoritativeExternalStateMode_ResumeConflictWhenExternalAdvanced(t *testing.T) {
	store := &testAuthoritativeStateStore{
		mode:    StateStoreModeAuthoritativeExternalState,
		values:  map[string]any{"counter": 1},
		version: "v1",
	}

	g := NewStateGraph[map[string]any]()
	g.AddNode("approve", func(ctx context.Context, _ map[string]any) (NodeResult, error) {
		_ = NodeInterrupt(ctx, Dyn("approve"))
		return NodeWrites(DynMap(map[string]any{"done": true})), nil
	})
	g.AddEdge(Start, "approve")
	g.AddEdge("approve", End)

	saver := checkpoint.NewInMemorySaver()
	compiled, err := g.Compile(CompileOptions{Checkpointer: saver, StateStore: store, StateMode: StateStoreModeAuthoritativeExternalState})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cfg := Config{ThreadID: "external-resume"}
	first, err := compiled.Invoke(WithConfig(context.Background(), cfg), map[string]any{})
	if err != nil {
		t.Fatalf("first invoke: %v", err)
	}
	if len(first.Interrupts) != 1 {
		t.Fatalf("interrupts = %d, want 1", len(first.Interrupts))
	}

	tuple, err := saver.GetTuple(context.Background(), &checkpoint.Config{ThreadID: "external-resume"})
	if err != nil {
		t.Fatalf("get tuple: %v", err)
	}

	store.mu.Lock()
	store.version = "v2"
	store.mu.Unlock()

	resumeCfg := cfg
	resumeCfg.CheckpointID = tuple.Checkpoint.ID
	resumeCfg.Metadata = map[string]any{ConfigKeyResume: "yes"}
	_, err = compiled.Invoke(WithConfig(context.Background(), resumeCfg), map[string]any{})
	if err == nil {
		t.Fatal("expected resume conflict error")
	}
	var conflict *ExternalStateConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("err type = %T, want *ExternalStateConflictError (err=%v)", err, err)
	}
}

func TestAuthoritativeExternalStateMode_ConcurrentDuplicateResumeConflict(t *testing.T) {
	store := &testAuthoritativeStateStore{
		mode:                   StateStoreModeAuthoritativeExternalState,
		values:                 map[string]any{"counter": 1},
		version:                "v1",
		bumpVersionAfterReplay: true,
	}

	g := NewStateGraph[map[string]any]()
	g.AddNode("approve", func(ctx context.Context, _ map[string]any) (NodeResult, error) {
		_ = NodeInterrupt(ctx, Dyn("approve"))
		return NodeWrites(DynMap(map[string]any{"done": true})), nil
	})
	g.AddEdge(Start, "approve")
	g.AddEdge("approve", End)

	saver := checkpoint.NewInMemorySaver()
	compiled, err := g.Compile(CompileOptions{Checkpointer: saver, StateStore: store, StateMode: StateStoreModeAuthoritativeExternalState})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	baseCfg := Config{ThreadID: "external-duplicate-resume"}
	first, err := compiled.Invoke(WithConfig(context.Background(), baseCfg), map[string]any{})
	if err != nil {
		t.Fatalf("first invoke: %v", err)
	}
	if len(first.Interrupts) != 1 {
		t.Fatalf("interrupts = %d, want 1", len(first.Interrupts))
	}

	tuple, err := saver.GetTuple(context.Background(), &checkpoint.Config{ThreadID: "external-duplicate-resume"})
	if err != nil {
		t.Fatalf("get tuple: %v", err)
	}

	resumeCfg := baseCfg
	resumeCfg.CheckpointID = tuple.Checkpoint.ID
	resumeCfg.Metadata = map[string]any{ConfigKeyResume: "ok"}

	errCh := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			_, invokeErr := compiled.Invoke(WithConfig(context.Background(), resumeCfg), map[string]any{})
			errCh <- invokeErr
		}()
	}

	var successCount, conflictCount int
	for i := 0; i < 2; i++ {
		err := <-errCh
		if err == nil {
			successCount++
			continue
		}
		var conflict *ExternalStateConflictError
		if errors.As(err, &conflict) {
			conflictCount++
			continue
		}
		t.Fatalf("unexpected resume error: %v", err)
	}
	if successCount != 1 || conflictCount != 1 {
		t.Fatalf("success=%d conflict=%d, want 1 success and 1 conflict", successCount, conflictCount)
	}
}
