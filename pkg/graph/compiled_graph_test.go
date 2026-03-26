package graph

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/checkpoint"
)

type countingSaver struct {
	inner checkpoint.Saver

	mu         sync.Mutex
	putConfigs []*checkpoint.Config
}

func newCountingSaver(inner checkpoint.Saver) *countingSaver {
	return &countingSaver{inner: inner}
}

func (s *countingSaver) GetTuple(ctx context.Context, config *checkpoint.Config) (*checkpoint.CheckpointTuple, error) {
	return s.inner.GetTuple(ctx, config)
}

func (s *countingSaver) Put(ctx context.Context, config *checkpoint.Config, cp *checkpoint.Checkpoint, meta *checkpoint.CheckpointMetadata) (*checkpoint.Config, error) {
	s.mu.Lock()
	if config != nil {
		cfgCopy := *config
		s.putConfigs = append(s.putConfigs, &cfgCopy)
	}
	s.mu.Unlock()
	return s.inner.Put(ctx, config, cp, meta)
}

func (s *countingSaver) PutWrites(ctx context.Context, config *checkpoint.Config, writes []checkpoint.PendingWrite, taskID string) error {
	return s.inner.PutWrites(ctx, config, writes, taskID)
}

func (s *countingSaver) List(ctx context.Context, config *checkpoint.Config, opts checkpoint.ListOptions) ([]*checkpoint.CheckpointTuple, error) {
	return s.inner.List(ctx, config, opts)
}

func (s *countingSaver) DeleteThread(ctx context.Context, threadID string) error {
	return s.inner.DeleteThread(ctx, threadID)
}

func (s *countingSaver) DeleteForRuns(ctx context.Context, runIDs []string) error {
	return s.inner.DeleteForRuns(ctx, runIDs)
}

func (s *countingSaver) CopyThread(ctx context.Context, sourceThreadID, targetThreadID string) error {
	return s.inner.CopyThread(ctx, sourceThreadID, targetThreadID)
}

func (s *countingSaver) Prune(ctx context.Context, threadIDs []string, strategy checkpoint.PruneStrategy) error {
	return s.inner.Prune(ctx, threadIDs, strategy)
}

func (s *countingSaver) putCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.putConfigs)
}

func (s *countingSaver) checkpointNamespaces() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.putConfigs))
	for _, cfg := range s.putConfigs {
		if cfg == nil {
			continue
		}
		out = append(out, cfg.CheckpointNS)
	}
	return out
}

func Test_DurabilityModes(t *testing.T) {
	makeGraph := func() *CompiledStateGraph[map[string]any, any, map[string]any, map[string]any] {
		g := NewStateGraph[map[string]any]()
		g.AddNode("a", func(_ context.Context, _ map[string]any) (NodeResult, error) {
			return NodeWrites(map[string]Dynamic{"a": Dyn(1)}), nil
		})
		g.AddNode("b", func(_ context.Context, _ map[string]any) (NodeResult, error) {
			return NodeWrites(map[string]Dynamic{"b": Dyn(2)}), nil
		})
		g.AddEdge(Start, "a")
		g.AddEdge("a", "b")
		g.AddEdge("b", End)
		compiled, err := g.Compile()
		if err != nil {
			t.Fatalf("compile: %v", err)
		}
		return compiled
	}

	ctx := context.Background()

	for _, tc := range []struct {
		name       string
		durability DurabilityMode
		wantOne    bool
	}{
		{name: "exit", durability: DurabilityExit, wantOne: true},
		{name: "sync", durability: DurabilitySync, wantOne: false},
		{name: "async", durability: DurabilityAsync, wantOne: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			saver := newCountingSaver(checkpoint.NewInMemorySaver())
			compiled := makeGraph()
			runCtx := WithConfig(ctx, Config{ThreadID: fmt.Sprintf("durability-%s", tc.name), Checkpointer: saver, Durability: tc.durability})

			if _, err := compiled.Invoke(runCtx, map[string]any{}); err != nil {
				t.Fatalf("invoke: %v", err)
			}

			got := saver.putCount()
			if tc.wantOne && got != 1 {
				t.Fatalf("durability=%s put count=%d, want 1", tc.durability, got)
			}
			if !tc.wantOne && got <= 1 {
				t.Fatalf("durability=%s put count=%d, want >1", tc.durability, got)
			}
		})
	}
}

func Test_BulkUpdateState(t *testing.T) {
	saver := checkpoint.NewInMemorySaver()

	g := NewStateGraph[map[string]any]()
	g.AddNode("noop", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NoNodeResult(), nil
	})
	g.AddEdge(Start, "noop")
	g.AddEdge("noop", End)

	compiled, err := g.Compile(CompileOptions{Checkpointer: saver})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cfg := &Config{ThreadID: "bulk-thread"}
	_, err = compiled.BulkUpdateState(context.Background(), cfg, [][]StateUpdate{
		{{Values: map[string]any{"x": 1}}},
		{{Values: map[string]any{"y": 2}}, {Values: map[string]any{"x": 3}}},
	})
	if err != nil {
		t.Fatalf("bulk update: %v", err)
	}

	state, err := compiled.GetState(context.Background(), cfg)
	if err != nil {
		t.Fatalf("get state: %v", err)
	}
	values, ok := state.Values.(map[string]any)
	if !ok {
		t.Fatalf("state values type = %T, want map[string]any", state.Values)
	}
	if values["x"] != 3 {
		t.Fatalf("x=%v, want 3", values["x"])
	}
	if values["y"] != 2 {
		t.Fatalf("y=%v, want 2", values["y"])
	}
}

func TestMergeState_MapShallowMergeCopiesInput(t *testing.T) {
	current := map[string]any{"a": 1, "nested": map[string]any{"x": 1}}
	update := map[string]any{"b": 2, "a": 3}

	merged := mergeState(current, update)
	if merged["a"] != 3 {
		t.Fatalf("merged[a] = %v, want 3", merged["a"])
	}
	if merged["b"] != 2 {
		t.Fatalf("merged[b] = %v, want 2", merged["b"])
	}
	if current["a"] != 1 {
		t.Fatalf("current[a] mutated to %v, want 1", current["a"])
	}
}

func TestMergeState_TypedStateReplacement(t *testing.T) {
	type simpleState struct {
		Count int
	}

	current := simpleState{Count: 1}
	updated := mergeState(current, simpleState{Count: 4})
	if updated.Count != 4 {
		t.Fatalf("updated.Count = %d, want 4", updated.Count)
	}

	notReplaced := mergeState(current, map[string]any{"Count": 9})
	if notReplaced.Count != 1 {
		t.Fatalf("notReplaced.Count = %d, want 1", notReplaced.Count)
	}
}

func Test_GetStateHistory(t *testing.T) {
	saver := checkpoint.NewInMemorySaver()

	g := NewStateGraph[map[string]any]()
	g.AddNode("noop", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NoNodeResult(), nil
	})
	g.AddEdge(Start, "noop")
	g.AddEdge("noop", End)

	compiled, err := g.Compile(CompileOptions{Checkpointer: saver})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cfg := &Config{ThreadID: "history-thread"}
	if err := compiled.UpdateState(context.Background(), cfg, map[string]any{"count": 1}); err != nil {
		t.Fatalf("first update: %v", err)
	}
	if err := compiled.UpdateState(context.Background(), cfg, map[string]any{"count": 2}); err != nil {
		t.Fatalf("second update: %v", err)
	}

	history, err := compiled.GetStateHistory(context.Background(), cfg)
	if err != nil {
		t.Fatalf("GetStateHistory: %v", err)
	}
	if len(history) != 2 {
		t.Fatalf("history len = %d, want 2", len(history))
	}

	latestValues, ok := history[0].Values.(map[string]any)
	if !ok {
		t.Fatalf("latest values type = %T, want map[string]any", history[0].Values)
	}
	if latestValues["count"] != 2 {
		t.Fatalf("latest count = %v, want 2", latestValues["count"])
	}
	if history[0].Config == nil {
		t.Fatal("latest snapshot config is nil")
	}
	if history[0].Config.ThreadID != "history-thread" {
		t.Fatalf("latest thread_id = %q, want history-thread", history[0].Config.ThreadID)
	}
	if history[0].Config.CheckpointID == "" {
		t.Fatal("latest snapshot checkpoint_id is empty")
	}
	if history[0].Metadata == nil {
		t.Fatal("latest metadata is nil")
	}
	if history[0].Metadata["source"] != "update" {
		t.Fatalf("latest metadata source = %v, want update", history[0].Metadata["source"])
	}
	if history[0].CreatedAt == nil {
		t.Fatal("latest created_at is nil")
	}

	previousValues, ok := history[1].Values.(map[string]any)
	if !ok {
		t.Fatalf("previous values type = %T, want map[string]any", history[1].Values)
	}
	if previousValues["count"] != 1 {
		t.Fatalf("previous count = %v, want 1", previousValues["count"])
	}

	historyWithNilConfig, err := compiled.GetStateHistory(context.Background(), nil)
	if err != nil {
		t.Fatalf("GetStateHistory(nil): %v", err)
	}
	if len(historyWithNilConfig) != len(history) {
		t.Fatalf("nil config history len = %d, want %d", len(historyWithNilConfig), len(history))
	}
}

func Test_GetStateHistory_NoCheckpointer(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNode("noop", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NoNodeResult(), nil
	})
	g.AddEdge(Start, "noop")
	g.AddEdge("noop", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	_, err = compiled.GetStateHistory(context.Background(), &Config{ThreadID: "no-checkpointer-history"})
	if err == nil {
		t.Fatal("expected error when no checkpointer is configured")
	}
	if !strings.Contains(err.Error(), "GetStateHistory requires a Checkpointer") {
		t.Fatalf("error = %v, want checkpointer guidance", err)
	}
}

func Test_GetStateHistory_Empty(t *testing.T) {
	saver := checkpoint.NewInMemorySaver()

	g := NewStateGraph[map[string]any]()
	g.AddNode("noop", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NoNodeResult(), nil
	})
	g.AddEdge(Start, "noop")
	g.AddEdge("noop", End)

	compiled, err := g.Compile(CompileOptions{Checkpointer: saver})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	history, err := compiled.GetStateHistory(context.Background(), &Config{ThreadID: "empty-history-thread"})
	if err != nil {
		t.Fatalf("GetStateHistory: %v", err)
	}
	if len(history) != 0 {
		t.Fatalf("history len = %d, want 0", len(history))
	}
}

func Test_ReplayFromCheckpointIgnoresInput(t *testing.T) {
	saver := checkpoint.NewInMemorySaver()

	g := NewStateGraph[map[string]any]()
	g.AddNode("pass", func(_ context.Context, state map[string]any) (NodeResult, error) {
		return NodeState(Dyn(state)), nil
	})
	g.AddEdge(Start, "pass")
	g.AddEdge("pass", End)

	compiled, err := g.Compile(CompileOptions{Checkpointer: saver})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ctx := WithConfig(context.Background(), Config{ThreadID: "replay-thread"})
	if _, err := compiled.Invoke(ctx, map[string]any{"value": "first"}); err != nil {
		t.Fatalf("first invoke: %v", err)
	}

	tuples, err := saver.List(context.Background(), (&Config{ThreadID: "replay-thread"}).CheckpointConfig(), checkpoint.ListOptions{Limit: 1})
	if err != nil {
		t.Fatalf("list checkpoints: %v", err)
	}
	if len(tuples) == 0 || tuples[0] == nil || tuples[0].Checkpoint == nil {
		t.Fatal("expected at least one checkpoint")
	}
	cpID := tuples[0].Checkpoint.ID

	replayCtx := WithConfig(context.Background(), Config{ThreadID: "replay-thread", CheckpointID: cpID})
	out, err := compiled.Invoke(replayCtx, map[string]any{"value": "second"})
	if err != nil {
		t.Fatalf("replay invoke: %v", err)
	}

	got, ok := out.Value.(map[string]any)
	if !ok {
		t.Fatalf("output type = %T, want map[string]any", out.Value)
	}
	if got["value"] != "first" {
		t.Fatalf("replayed value = %v, want first", got["value"])
	}
}

func Test_SubgraphNodeNamespaces(t *testing.T) {
	baseSaver := checkpoint.NewInMemorySaver()
	saver := newCountingSaver(baseSaver)

	childBuilder := NewStateGraph[map[string]any]()
	childBuilder.AddNode("child_work", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeWrites(map[string]Dynamic{"child": Dyn("ok")}), nil
	})
	childBuilder.AddEdge(Start, "child_work")
	childBuilder.AddEdge("child_work", End)
	child, err := childBuilder.Compile(CompileOptions{Checkpointer: saver})
	if err != nil {
		t.Fatalf("compile child: %v", err)
	}

	parentBuilder := NewStateGraph[map[string]any]()
	parentBuilder.AddNodeFunc("child_node", child.AsSubgraphNode("child_node"))
	parentBuilder.AddEdge(Start, "child_node")
	parentBuilder.AddEdge("child_node", End)
	parent, err := parentBuilder.Compile(CompileOptions{Checkpointer: saver})
	if err != nil {
		t.Fatalf("compile parent: %v", err)
	}

	out, err := parent.Invoke(WithConfig(context.Background(), Config{ThreadID: "subgraph-thread"}), map[string]any{})
	if err != nil {
		t.Fatalf("invoke parent: %v", err)
	}
	vals, ok := out.Value.(map[string]any)
	if !ok {
		t.Fatalf("output type = %T, want map[string]any", out.Value)
	}
	if vals["child"] != "ok" {
		t.Fatalf("child output = %v, want ok", vals["child"])
	}

	nss := saver.checkpointNamespaces()
	foundSubgraphNS := false
	for _, ns := range nss {
		if strings.Contains(ns, "child_node:") {
			foundSubgraphNS = true
			break
		}
	}
	if !foundSubgraphNS {
		t.Fatalf("expected subgraph checkpoint namespace in put configs, got %v", nss)
	}
}

func Test_SubgraphNodeNamespaces_StatefulModeUsesNodeScope(t *testing.T) {
	baseSaver := checkpoint.NewInMemorySaver()
	saver := newCountingSaver(baseSaver)

	childBuilder := NewStateGraph[map[string]any]()
	childBuilder.AddNode("child_work", func(_ context.Context, state map[string]any) (NodeResult, error) {
		count := 0
		switch v := state["sub_count"].(type) {
		case int:
			count = v
		case int64:
			count = int(v)
		case float64:
			count = int(v)
		}
		return NodeWrites(map[string]Dynamic{"sub_count": Dyn(count + 1)}), nil
	})
	childBuilder.AddEdge(Start, "child_work")
	childBuilder.AddEdge("child_work", End)
	child, err := childBuilder.Compile(CompileOptions{Checkpointer: saver})
	if err != nil {
		t.Fatalf("compile child: %v", err)
	}

	parentBuilder := NewStateGraph[map[string]any]()
	statefulChild := child.AsStatefulSubgraphNode("child_node")
	parentBuilder.AddNode("child_node", func(ctx context.Context, _ map[string]any) (NodeResult, error) {
		return statefulChild(ctx, map[string]any{"seed": true})
	})
	parentBuilder.AddEdge(Start, "child_node")
	parentBuilder.AddEdge("child_node", End)
	parent, err := parentBuilder.Compile(CompileOptions{Checkpointer: saver})
	if err != nil {
		t.Fatalf("compile parent: %v", err)
	}

	ctx := WithConfig(context.Background(), Config{ThreadID: "subgraph-stateful-thread"})
	if _, err := parent.Invoke(ctx, map[string]any{}); err != nil {
		t.Fatalf("first invoke: %v", err)
	}
	if _, err := parent.Invoke(ctx, map[string]any{}); err != nil {
		t.Fatalf("second invoke: %v", err)
	}

	nss := saver.checkpointNamespaces()
	foundNodeScope := false
	foundTaskScope := false
	for _, ns := range nss {
		if strings.HasSuffix(ns, "|child_node") || ns == "child_node" {
			foundNodeScope = true
		}
		if strings.Contains(ns, "child_node:") {
			foundTaskScope = true
		}
	}
	if !foundNodeScope {
		t.Fatalf("expected stateful subgraph namespace ending in child_node, got %v", nss)
	}
	if foundTaskScope {
		t.Fatalf("stateful mode should not use task-scoped namespaces, got %v", nss)
	}
}

func Test_SubgraphNodeOptions_DisableCheckpointer(t *testing.T) {
	baseSaver := checkpoint.NewInMemorySaver()
	saver := newCountingSaver(baseSaver)

	childBuilder := NewStateGraph[map[string]any]()
	childBuilder.AddNode("child_work", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeWrites(map[string]Dynamic{"child": Dyn("ok")}), nil
	})
	childBuilder.AddEdge(Start, "child_work")
	childBuilder.AddEdge("child_work", End)
	child, err := childBuilder.Compile(CompileOptions{Checkpointer: saver})
	if err != nil {
		t.Fatalf("compile child: %v", err)
	}

	parentBuilder := NewStateGraph[map[string]any]()
	parentBuilder.AddNodeFunc("child_node", child.AsSubgraphNodeWithOptions("child_node", SubgraphNodeOptions{
		Persistence:         SubgraphPersistenceTaskScope,
		DisableCheckpointer: true,
	}))
	parentBuilder.AddEdge(Start, "child_node")
	parentBuilder.AddEdge("child_node", End)
	parent, err := parentBuilder.Compile(CompileOptions{Checkpointer: saver})
	if err != nil {
		t.Fatalf("compile parent: %v", err)
	}

	if _, err := parent.Invoke(WithConfig(context.Background(), Config{ThreadID: "subgraph-no-checkpoint"}), map[string]any{}); err != nil {
		t.Fatalf("invoke parent: %v", err)
	}

	nss := saver.checkpointNamespaces()
	for _, ns := range nss {
		if strings.Contains(ns, "child_node:") {
			t.Fatalf("unexpected child namespace writes when checkpointer disabled: %v", nss)
		}
	}
}

// TestGetState_FullSnapshot verifies that GetState populates all fields in the
// StateSnapshot after a normal graph execution.
func TestGetState_FullSnapshot(t *testing.T) {
	saver := checkpoint.NewInMemorySaver()

	g := NewStateGraph[map[string]any]()
	g.AddNode("work", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"done": true})), nil
	})
	g.AddEdge(Start, "work")
	g.AddEdge("work", End)

	compiled, err := g.Compile(CompileOptions{Checkpointer: saver})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cfg := &Config{ThreadID: "full-snapshot-thread"}
	if _, err := compiled.Invoke(WithConfig(context.Background(), *cfg), map[string]any{}); err != nil {
		t.Fatalf("invoke: %v", err)
	}

	snap, err := compiled.GetState(context.Background(), cfg)
	if err != nil {
		t.Fatalf("GetState: %v", err)
	}

	// Values must be populated.
	vals, ok := snap.Values.(map[string]any)
	if !ok {
		t.Fatalf("Values type = %T, want map[string]any", snap.Values)
	}
	if vals["done"] != true {
		t.Fatalf("Values[done] = %v, want true", vals["done"])
	}

	// Metadata must be present with step and source.
	if snap.Metadata == nil {
		t.Fatal("Metadata is nil")
	}
	if _, ok := snap.Metadata["step"]; !ok {
		t.Fatalf("Metadata missing 'step', got %v", snap.Metadata)
	}

	// CreatedAt must be set.
	if snap.CreatedAt == nil {
		t.Fatal("CreatedAt is nil")
	}

	// Config must be set and carry the thread ID.
	if snap.Config == nil {
		t.Fatal("Config is nil")
	}
	if snap.Config.ThreadID != cfg.ThreadID {
		t.Fatalf("Config.ThreadID = %q, want %q", snap.Config.ThreadID, cfg.ThreadID)
	}

	// Completed graph: no nodes left to run.
	if len(snap.Next) != 0 {
		t.Fatalf("Next = %v, want empty for completed graph", snap.Next)
	}
	if len(snap.Tasks) != 0 {
		t.Fatalf("Tasks len = %d, want 0 for completed graph", len(snap.Tasks))
	}
}

// TestGetState_InterruptBefore verifies that GetState correctly reports the
// interrupted node in Next and Tasks after an interruptBefore trigger.
func TestGetState_InterruptBefore(t *testing.T) {
	saver := checkpoint.NewInMemorySaver()

	g := NewStateGraph[map[string]any]()
	g.AddNode("a", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"ran": "a"})), nil
	})
	g.AddNode("b", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"ran": "b"})), nil
	})
	g.AddEdge(Start, "a")
	g.AddEdge("a", "b")
	g.AddEdge("b", End)

	compiled, err := g.Compile(CompileOptions{
		Checkpointer:    saver,
		InterruptBefore: []string{"b"},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cfg := &Config{ThreadID: "interrupt-before-snapshot"}
	invOut, err := compiled.Invoke(WithConfig(context.Background(), *cfg), map[string]any{})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if len(invOut.Interrupts) == 0 {
		t.Fatal("expected interrupts from invoke")
	}

	snap, err := compiled.GetState(context.Background(), cfg)
	if err != nil {
		t.Fatalf("GetState: %v", err)
	}

	// Node "a" ran before the interrupt; node "b" was paused.
	vals, _ := snap.Values.(map[string]any)
	if vals["ran"] != "a" {
		t.Fatalf("Values[ran] = %v, want a", vals["ran"])
	}

	// Next must list the interrupted node.
	if len(snap.Next) != 1 || snap.Next[0] != "b" {
		t.Fatalf("Next = %v, want [b]", snap.Next)
	}

	// Tasks must carry the interrupt details.
	if len(snap.Tasks) != 1 {
		t.Fatalf("Tasks len = %d, want 1", len(snap.Tasks))
	}
	task := snap.Tasks[0]
	if task.Name != "b" {
		t.Fatalf("Tasks[0].Name = %q, want b", task.Name)
	}
	if len(task.Interrupts) == 0 {
		t.Fatal("Tasks[0].Interrupts is empty, want at least one interrupt")
	}

	// Top-level Interrupts must mirror task interrupts.
	if len(snap.Interrupts) == 0 {
		t.Fatal("Interrupts is empty, want at least one")
	}

	// Metadata and CreatedAt must be set.
	if snap.Metadata == nil {
		t.Fatal("Metadata is nil")
	}
	if snap.CreatedAt == nil {
		t.Fatal("CreatedAt is nil")
	}
}

// TestGetState_UserInterrupt verifies that GetState correctly reports a node
// that called Interrupt() (user-triggered pause) in Next and Tasks.
func TestGetState_UserInterrupt(t *testing.T) {
	saver := checkpoint.NewInMemorySaver()

	g := NewStateGraph[map[string]any]()
	g.AddNode("ask", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		iv := NewInterrupt(Dyn("need input"), "ask-interrupt")
		panic(nodeInterruptSignal{interrupt: iv})
	})
	g.AddEdge(Start, "ask")
	g.AddEdge("ask", End)

	compiled, err := g.Compile(CompileOptions{Checkpointer: saver})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cfg := &Config{ThreadID: "user-interrupt-snapshot"}
	invOut, err := compiled.Invoke(WithConfig(context.Background(), *cfg), map[string]any{})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if len(invOut.Interrupts) == 0 {
		t.Fatal("expected interrupts from invoke")
	}

	snap, err := compiled.GetState(context.Background(), cfg)
	if err != nil {
		t.Fatalf("GetState: %v", err)
	}

	// The interrupted node must appear in Next and Tasks.
	if len(snap.Next) != 1 || snap.Next[0] != "ask" {
		t.Fatalf("Next = %v, want [ask]", snap.Next)
	}
	if len(snap.Tasks) != 1 || snap.Tasks[0].Name != "ask" {
		t.Fatalf("Tasks = %v, want [{Name:ask ...}]", snap.Tasks)
	}
	if len(snap.Tasks[0].Interrupts) == 0 {
		t.Fatal("Tasks[0].Interrupts is empty")
	}
	if len(snap.Interrupts) == 0 {
		t.Fatal("Interrupts is empty")
	}
}

// TestGetState_ParentConfig verifies that the ParentConfig field is correctly
// set when multiple checkpoints exist for a thread.
func TestGetState_ParentConfig(t *testing.T) {
	saver := checkpoint.NewInMemorySaver()

	g := NewStateGraph[map[string]any]()
	g.AddNode("step", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NoNodeResult(), nil
	})
	g.AddEdge(Start, "step")
	g.AddEdge("step", End)

	compiled, err := g.Compile(CompileOptions{Checkpointer: saver})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	// Run twice to create multiple checkpoints (each run produces a new one).
	cfg := &Config{ThreadID: "parent-config-thread"}
	for i := 0; i < 2; i++ {
		if _, err := compiled.Invoke(WithConfig(context.Background(), *cfg), map[string]any{}); err != nil {
			t.Fatalf("invoke %d: %v", i, err)
		}
	}

	snap, err := compiled.GetState(context.Background(), cfg)
	if err != nil {
		t.Fatalf("GetState: %v", err)
	}

	// After two runs the latest checkpoint should have a parent.
	if snap.ParentConfig == nil {
		t.Fatal("ParentConfig is nil; expected a parent checkpoint reference after multiple runs")
	}
}
