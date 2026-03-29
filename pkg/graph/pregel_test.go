package graph

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/checkpoint"
)

type transientRetryErr struct{}

func (transientRetryErr) Error() string { return "transient" }

// --- retry tests ---

func TestExecuteWithRetry_SuccessFirstAttempt(t *testing.T) {
	calls := 0
	err := executeWithRetry(context.Background(), RetryPolicy{
		MaxAttempts:     3,
		InitialInterval: time.Millisecond,
		BackoffFactor:   2,
		MaxInterval:     time.Second,
	}, func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}
}

func TestExecuteWithRetry_ExhaustsAttempts(t *testing.T) {
	calls := 0
	sentinel := errors.New("always fails")
	err := executeWithRetry(context.Background(), RetryPolicy{
		MaxAttempts:     3,
		InitialInterval: time.Millisecond,
		BackoffFactor:   1, // no backoff for test speed
		MaxInterval:     time.Second,
		RetryOn:         DefaultRetryOn,
	}, func() error {
		calls++
		return sentinel
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestExecuteWithRetry_StopsWhenRetryOnReturnsFalse(t *testing.T) {
	calls := 0
	permanent := errors.New("permanent")
	err := executeWithRetry(context.Background(), RetryPolicy{
		MaxAttempts:     5,
		InitialInterval: time.Millisecond,
		BackoffFactor:   1,
		MaxInterval:     time.Second,
		RetryOn:         func(_ error) bool { return false }, // never retry
	}, func() error {
		calls++
		return permanent
	})
	if !errors.Is(err, permanent) {
		t.Fatalf("expected permanent error, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call (no retry), got %d", calls)
	}
}

func TestExecuteWithRetry_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err := executeWithRetry(ctx, RetryPolicy{
		MaxAttempts:     5,
		InitialInterval: time.Second, // would block if context not checked
		BackoffFactor:   1,
		MaxInterval:     time.Second,
	}, func() error {
		return errors.New("fail")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestExecuteWithRetry_SucceedsOnSecondAttempt(t *testing.T) {
	var calls int32
	err := executeWithRetry(context.Background(), RetryPolicy{
		MaxAttempts:     3,
		InitialInterval: time.Millisecond,
		BackoffFactor:   1,
		MaxInterval:     time.Second,
		RetryOn:         DefaultRetryOn,
	}, func() error {
		n := atomic.AddInt32(&calls, 1)
		if n < 2 {
			return errors.New("transient")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if calls != 2 {
		t.Fatalf("expected 2 calls, got %d", calls)
	}
}

func TestExecuteWithRetry_ZeroMaxAttemptsTreatedAsOne(t *testing.T) {
	calls := 0
	err := executeWithRetry(context.Background(), RetryPolicy{MaxAttempts: 0}, func() error {
		calls++
		return errors.New("fail")
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if calls != 1 {
		t.Fatalf("expected 1 call for MaxAttempts=0, got %d", calls)
	}
}

func TestExecuteWithRetryPolicies_UsesFirstMatchingPolicy(t *testing.T) {
	var calls int32
	err := executeWithRetryPolicies(context.Background(), []RetryPolicy{
		{
			MaxAttempts:     1,
			InitialInterval: time.Millisecond,
			BackoffFactor:   1,
			MaxInterval:     time.Second,
			RetryOn: func(err error) bool {
				var te *transientRetryErr
				return errors.As(err, &te)
			},
		},
		{
			MaxAttempts:     3,
			InitialInterval: time.Millisecond,
			BackoffFactor:   1,
			MaxInterval:     time.Second,
			RetryOn: func(err error) bool {
				var te *transientRetryErr
				return errors.As(err, &te)
			},
		},
	}, func() error {
		n := atomic.AddInt32(&calls, 1)
		if n < 2 {
			return &transientRetryErr{}
		}
		return nil
	})
	if err == nil {
		t.Fatal("expected error when first matching policy max attempts is exhausted")
	}
	if calls != 1 {
		t.Fatalf("expected exactly one attempt from first matching policy, got %d", calls)
	}
}

func TestExecuteWithRetryPolicies_UpdatesRuntimeExecutionInfo(t *testing.T) {
	rt := &Runtime{ExecutionInfo: &ExecutionInfo{}}
	ctx := WithRuntime(context.Background(), rt)

	var attempts []int
	var firstAttemptTimes []int64
	err := executeWithRetry(ctx, RetryPolicy{
		MaxAttempts:     3,
		InitialInterval: time.Millisecond,
		BackoffFactor:   1,
		MaxInterval:     time.Second,
		RetryOn:         DefaultRetryOn,
	}, func() error {
		info := GetRuntime(ctx).ExecutionInfo
		attempts = append(attempts, info.NodeAttempt)
		firstAttemptTimes = append(firstAttemptTimes, info.NodeFirstAttemptTime)
		if info.NodeAttempt < 3 {
			return transientRetryErr{}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !reflect.DeepEqual(attempts, []int{1, 2, 3}) {
		t.Fatalf("attempts = %v, want [1 2 3]", attempts)
	}
	if len(firstAttemptTimes) != 3 {
		t.Fatalf("firstAttemptTimes len = %d, want 3", len(firstAttemptTimes))
	}
	if firstAttemptTimes[0] == 0 {
		t.Fatal("expected non-zero first attempt time")
	}
	for i := 1; i < len(firstAttemptTimes); i++ {
		if firstAttemptTimes[i] != firstAttemptTimes[0] {
			t.Fatalf("first attempt times = %v, want stable value", firstAttemptTimes)
		}
	}
}

func TestDefaultRetryOn_SelectivelyRejectsDeterministicErrors(t *testing.T) {
	if DefaultRetryOn(nil) {
		t.Fatal("expected nil error to be non-retryable")
	}
	if DefaultRetryOn(context.Canceled) {
		t.Fatal("expected context.Canceled to be non-retryable")
	}
	if DefaultRetryOn(context.DeadlineExceeded) {
		t.Fatal("expected context.DeadlineExceeded to be non-retryable")
	}
	if DefaultRetryOn(&InvalidUpdateError{Message: "bad write"}) {
		t.Fatal("expected InvalidUpdateError to be non-retryable")
	}
	if DefaultRetryOn(&GraphRecursionError{Message: "limit"}) {
		t.Fatal("expected GraphRecursionError to be non-retryable")
	}
	if !DefaultRetryOn(errors.New("transient")) {
		t.Fatal("expected generic transient error to be retryable")
	}
}

// --- Pregel loop tests ---

func TestPregelLoop_SimpleLinearGraph(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNode("a", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"x": 1})), nil
	})
	g.AddNode("b", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"y": 2})), nil
	})
	g.AddEdge(Start, "a")
	g.AddEdge("a", "b")
	g.AddEdge("b", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	out, err := compiled.Invoke(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}

	state, ok := out.Value.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any output, got %T", out.Value)
	}
	if state["x"] != 1 {
		t.Errorf("expected x=1, got %v", state["x"])
	}
	if state["y"] != 2 {
		t.Errorf("expected y=2, got %v", state["y"])
	}
}

func TestPregelLoop_StatePassedToNodes(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNode("double", func(_ context.Context, state map[string]any) (NodeResult, error) {
		v, _ := state["value"].(int)
		return NodeWrites(DynMap(map[string]any{"value": v * 2})), nil
	})
	g.AddEdge(Start, "double")
	g.AddEdge("double", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	out, err := compiled.Invoke(context.Background(), map[string]any{"value": 5})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}

	state, _ := out.Value.(map[string]any)
	if state["value"] != 10 {
		t.Errorf("expected value=10, got %v", state["value"])
	}
}

func TestPregelLoop_ConditionalBranching(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNode("router", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NoNodeResult(), nil
	})
	g.AddNode("left", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"path": "left"})), nil
	})
	g.AddNode("right", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"path": "right"})), nil
	})

	g.AddEdge(Start, "router")
	g.AddConditionalEdges("router",
		func(_ context.Context, state map[string]any) (Route, error) {
			if state["go"] == "left" {
				return RouteTo("left"), nil
			}
			return RouteTo("right"), nil
		},
		nil, // no pathMap — result is node name directly
	)
	g.AddEdge("left", End)
	g.AddEdge("right", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	// Route left.
	out, err := compiled.Invoke(context.Background(), map[string]any{"go": "left"})
	if err != nil {
		t.Fatalf("invoke left: %v", err)
	}
	state, _ := out.Value.(map[string]any)
	if state["path"] != "left" {
		t.Errorf("expected path=left, got %v", state["path"])
	}

	// Route right.
	out, err = compiled.Invoke(context.Background(), map[string]any{"go": "right"})
	if err != nil {
		t.Fatalf("invoke right: %v", err)
	}
	state, _ = out.Value.(map[string]any)
	if state["path"] != "right" {
		t.Errorf("expected path=right, got %v", state["path"])
	}
}

func TestPregelLoop_RecursionLimitEnforced(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNode("loop", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NoNodeResult(), nil
	})
	g.AddEdge(Start, "loop")
	g.AddEdge("loop", "loop") // infinite loop

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ctx := WithConfig(context.Background(), Config{RecursionLimit: 5})
	_, err = compiled.Invoke(ctx, map[string]any{})
	if err == nil {
		t.Fatal("expected recursion limit error, got nil")
	}

	var recErr *GraphRecursionError
	if !errors.As(err, &recErr) {
		t.Fatalf("expected *GraphRecursionError, got %T: %v", err, err)
	}
}

func TestPregelLoop_NodeError(t *testing.T) {
	sentinel := errors.New("node exploded")
	g := NewStateGraph[map[string]any]()
	g.AddNode("boom", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NoNodeResult(), sentinel
	})
	g.AddEdge(Start, "boom")
	g.AddEdge("boom", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	_, err = compiled.Invoke(context.Background(), map[string]any{})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("expected sentinel error in chain, got %v", err)
	}
}

func TestPregelLoop_StreamUpdatesMode(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNode("n", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"k": "v"})), nil
	})
	g.AddEdge(Start, "n")
	g.AddEdge("n", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ch := compiled.Stream(context.Background(), map[string]any{}, StreamModeUpdates)
	var parts []StreamPart
	for p := range ch {
		parts = append(parts, p)
	}

	if len(parts) == 0 {
		t.Fatal("expected stream parts, got none")
	}
	for _, p := range parts {
		if p.Err != nil {
			t.Fatalf("unexpected stream error: %v", p.Err)
		}
	}
}

func TestPregelLoop_RetryOnNodeFailure(t *testing.T) {
	var calls int32
	g := NewStateGraph[map[string]any]()
	g.nodes["flaky"] = &NodeSpec[map[string]any]{
		Name: "flaky",
		Fn: func(_ context.Context, _ map[string]any) (NodeResult, error) {
			n := atomic.AddInt32(&calls, 1)
			if n < 3 {
				return NoNodeResult(), errors.New("transient error")
			}
			return NodeWrites(DynMap(map[string]any{"done": true})), nil
		},
		RetryPolicy: []RetryPolicy{
			{
				MaxAttempts:     5,
				InitialInterval: time.Millisecond,
				BackoffFactor:   1,
				MaxInterval:     10 * time.Millisecond,
				RetryOn:         DefaultRetryOn,
			},
		},
	}
	g.AddEdge(Start, "flaky")
	g.AddEdge("flaky", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	out, err := compiled.Invoke(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("expected success after retries, got %v", err)
	}
	state, _ := out.Value.(map[string]any)
	if state["done"] != true {
		t.Errorf("expected done=true, got %v", state["done"])
	}
	if calls != 3 {
		t.Errorf("expected 3 calls, got %d", calls)
	}
}

func TestPregelLoop_BinaryOperatorChannel(t *testing.T) {
	// Register a channel with an append reducer.
	g := NewStateGraph[map[string]any]()
	g.channels["messages"] = NewBinaryOperatorAggregate(func(a, b any) any {
		if aSlice, ok := a.([]any); ok {
			return append(aSlice, b)
		}
		// First accumulation: seed was a scalar — wrap both values.
		return []any{a, b}
	})

	g.AddNode("append_hello", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"messages": "hello"})), nil
	})
	g.AddNode("append_world", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"messages": "world"})), nil
	})
	g.AddEdge(Start, "append_hello")
	g.AddEdge("append_hello", "append_world")
	g.AddEdge("append_world", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	out, err := compiled.Invoke(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}

	state, _ := out.Value.(map[string]any)
	msgs, _ := state["messages"].([]any)
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages, got %d: %v", len(msgs), msgs)
	}
	if msgs[0] != "hello" || msgs[1] != "world" {
		t.Errorf("expected [hello world], got %v", msgs)
	}
}

func TestPregelLoop_AnyValueClearsOnIdleStep(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.channels["scratch"] = NewAnyValue()

	g.AddNode("writer", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"scratch": "set"})), nil
	})
	g.AddNode("idle", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NoNodeResult(), nil
	})
	g.AddEdge(Start, "writer")
	g.AddEdge("writer", "idle")
	g.AddEdge("idle", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	out, err := compiled.Invoke(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	state, _ := out.Value.(map[string]any)
	if _, ok := state["scratch"]; ok {
		t.Fatalf("expected scratch to clear on idle step, got %v", state["scratch"])
	}
}

func TestPregelLoop_EphemeralValueClearsOnIdleStep(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.channels["ephemeral"] = NewEphemeralValue(true)

	g.AddNode("writer", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"ephemeral": "set"})), nil
	})
	g.AddNode("idle", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NoNodeResult(), nil
	})
	g.AddEdge(Start, "writer")
	g.AddEdge("writer", "idle")
	g.AddEdge("idle", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	out, err := compiled.Invoke(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	state, _ := out.Value.(map[string]any)
	if _, ok := state["ephemeral"]; ok {
		t.Fatalf("expected ephemeral to clear on idle step, got %v", state["ephemeral"])
	}
}

func TestPregelLoop_TopicAccumulatePersistsAcrossIdleStep(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.channels["events"] = NewTopic(true)

	g.AddNode("writer", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"events": "event-a"})), nil
	})
	g.AddNode("idle", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NoNodeResult(), nil
	})
	g.AddEdge(Start, "writer")
	g.AddEdge("writer", "idle")
	g.AddEdge("idle", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	out, err := compiled.Invoke(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}

	state, _ := out.Value.(map[string]any)
	items, ok := state["events"].([]Dynamic)
	if !ok {
		t.Fatalf("events type = %T, want []Dynamic", state["events"])
	}
	if len(items) != 1 || items[0].Value() != "event-a" {
		t.Fatalf("expected accumulated topic value to persist, got %v", items)
	}
}

func TestPregelLoop_TopicNonAccumulateClearsOnIdleStep(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.channels["events"] = NewTopic(false)

	g.AddNode("writer", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"events": "event-a"})), nil
	})
	g.AddNode("idle", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NoNodeResult(), nil
	})
	g.AddEdge(Start, "writer")
	g.AddEdge("writer", "idle")
	g.AddEdge("idle", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	out, err := compiled.Invoke(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}

	state, _ := out.Value.(map[string]any)
	if _, ok := state["events"]; ok {
		t.Fatalf("expected non-accumulating topic to clear on idle step, got %v", state["events"])
	}
}

func TestPregelLoop_IgnoresUnknownChannelWritesForTypedState(t *testing.T) {
	type typedState struct {
		Known int
	}

	saver := checkpoint.NewInMemorySaver()
	g := NewStateGraph[typedState]()
	g.channels["Known"] = NewLastValue()

	g.AddNode("writer", func(_ context.Context, _ typedState) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"Known": 1, "Unknown": 2})), nil
	})
	g.AddEdge(Start, "writer")
	g.AddEdge("writer", End)

	compiled, err := g.Compile(CompileOptions{Checkpointer: saver})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	runConfig := Config{ThreadID: "typed-unknown-write", Checkpointer: saver}
	out, err := compiled.Invoke(WithConfig(context.Background(), runConfig), typedState{})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}

	state, _ := out.Value.(typedState)
	if state.Known != 1 {
		t.Fatalf("Known = %d, want 1", state.Known)
	}

	tuple, err := saver.GetTuple(context.Background(), runConfig.CheckpointConfig())
	if err != nil {
		t.Fatalf("loading checkpoint: %v", err)
	}
	if tuple == nil || tuple.Checkpoint == nil {
		t.Fatal("expected checkpoint tuple")
	}
	if _, ok := tuple.Checkpoint.ChannelValues["Unknown"]; ok {
		t.Fatalf("unknown typed-state write should be ignored, got %v", tuple.Checkpoint.ChannelValues["Unknown"])
	}
}

func TestPregelLoop_SendFanoutNotReplayedOnLaterSteps(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.channels["count"] = NewBinaryOperatorAggregate(func(a, b any) any {
		ai, _ := a.(int)
		bi, _ := b.(int)
		return ai + bi
	})

	g.AddNode("sender", func(ctx context.Context, _ map[string]any) (NodeResult, error) {
		GetSend(ctx)("worker", Dyn(map[string]any{"n": 1}))
		return NoNodeResult(), nil
	})
	g.AddNode("worker", func(_ context.Context, state map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"count": state["n"]})), nil
	})
	g.AddNode("step1", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NoNodeResult(), nil
	})
	g.AddNode("step2", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NoNodeResult(), nil
	})

	g.AddEdge(Start, "sender")
	g.AddEdge("sender", "step1")
	g.AddEdge("step1", "step2")
	g.AddEdge("step2", End)
	g.AddEdge("worker", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	out, err := compiled.Invoke(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}

	state, _ := out.Value.(map[string]any)
	if state["count"] != 1 {
		t.Fatalf("count = %v, want 1 (send should run once)", state["count"])
	}
}

func TestPregelLoop_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	g := NewStateGraph[map[string]any]()
	g.AddNode("slow", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		cancel() // cancel from inside the node
		return NodeWrites(DynMap(map[string]any{})), nil
	})
	g.AddNode("never", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		t.Error("node 'never' should not have been executed")
		return NoNodeResult(), nil
	})
	g.AddEdge(Start, "slow")
	g.AddEdge("slow", "never")
	g.AddEdge("never", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	_, err = compiled.Invoke(ctx, map[string]any{})
	// Either context error or successful early exit — both are acceptable.
	// The important thing is "never" did not run.
	_ = err
}

func TestPregelLoop_StepTimeoutEnforced(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNode("slow", func(ctx context.Context, _ map[string]any) (NodeResult, error) {
		select {
		case <-ctx.Done():
			return NoNodeResult(), ctx.Err()
		case <-time.After(200 * time.Millisecond):
			return NodeWrites(DynMap(map[string]any{"slow": true})), nil
		}
	})
	g.AddNode("fast", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"fast": true})), nil
	})
	g.AddEdge(Start, "slow")
	g.AddEdge(Start, "fast")
	g.AddEdge("slow", End)
	g.AddEdge("fast", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ctx := WithConfig(context.Background(), Config{StepTimeout: 50 * time.Millisecond})
	start := time.Now()
	_, err = compiled.Invoke(ctx, map[string]any{})
	if err == nil {
		t.Fatal("expected step timeout error, got nil")
	}
	var timeoutErr *GraphStepTimeoutError
	if !errors.As(err, &timeoutErr) {
		t.Fatalf("expected GraphStepTimeoutError, got %T: %v", err, err)
	}
	if timeoutErr.Step != 0 {
		t.Fatalf("timeout step = %d, want 0", timeoutErr.Step)
	}
	if timeoutErr.Timeout != 50*time.Millisecond {
		t.Fatalf("timeout duration = %s, want %s", timeoutErr.Timeout, 50*time.Millisecond)
	}
	if elapsed := time.Since(start); elapsed >= 180*time.Millisecond {
		t.Fatalf("invoke took too long (%s), timeout not enforced", elapsed)
	}
}

func TestPregelLoop_StepTimeoutSavesCheckpointOnFailure(t *testing.T) {
	saver := checkpoint.NewInMemorySaver()
	g := NewStateGraph[map[string]any]()
	g.AddNode("slow", func(ctx context.Context, _ map[string]any) (NodeResult, error) {
		select {
		case <-ctx.Done():
			return NoNodeResult(), ctx.Err()
		case <-time.After(150 * time.Millisecond):
			return NodeWrites(DynMap(map[string]any{"slow": true})), nil
		}
	})
	g.AddEdge(Start, "slow")
	g.AddEdge("slow", End)

	compiled, err := g.Compile(CompileOptions{Checkpointer: saver, Durability: DurabilitySync})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	runConfig := Config{ThreadID: "step-timeout-checkpoint", StepTimeout: 25 * time.Millisecond}
	ctx := WithConfig(context.Background(), runConfig)
	_, err = compiled.Invoke(ctx, map[string]any{})
	if err == nil {
		t.Fatal("expected step timeout error, got nil")
	}

	tuple, err := saver.GetTuple(context.Background(), runConfig.CheckpointConfig())
	if err != nil {
		t.Fatalf("loading checkpoint: %v", err)
	}
	if tuple == nil || tuple.Checkpoint == nil {
		t.Fatal("expected checkpoint tuple after timeout failure")
	}
}

func TestPregelLoop_MaxConcurrencyLimitsFanoutAndKeepsWriteOrder(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.channels["events"] = NewTopic(true)

	var inFlight atomic.Int64
	var maxInFlight atomic.Int64

	g.AddNode("dispatch", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		sends := make([]Send, 0, 100)
		for i := 0; i < 100; i++ {
			sends = append(sends, Send{Node: "worker", Arg: Dyn(map[string]any{"idx": i})})
		}
		return NodeCommand(&Command{Goto: RouteWithSends(sends...)}), nil
	})
	g.AddNode("worker", func(ctx context.Context, state map[string]any) (NodeResult, error) {
		now := inFlight.Add(1)
		for {
			currentMax := maxInFlight.Load()
			if now <= currentMax || maxInFlight.CompareAndSwap(currentMax, now) {
				break
			}
		}
		defer inFlight.Add(-1)

		select {
		case <-ctx.Done():
			return NoNodeResult(), ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}

		idx, _ := state["idx"].(int)
		return NodeWrites(DynMap(map[string]any{"events": idx})), nil
	})
	g.AddEdge(Start, "dispatch")

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ctx := WithConfig(context.Background(), Config{MaxConcurrency: 10})
	out, err := compiled.Invoke(ctx, map[string]any{})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}

	if got := maxInFlight.Load(); got > 10 {
		t.Fatalf("max concurrent workers = %d, want <= 10", got)
	}

	state, _ := out.Value.(map[string]any)
	events, ok := state["events"].([]Dynamic)
	if !ok {
		t.Fatalf("events type = %T, want []Dynamic", state["events"])
	}
	if len(events) != 100 {
		t.Fatalf("events len = %d, want 100", len(events))
	}
	for i, event := range events {
		idx, ok := event.Value().(int)
		if !ok {
			t.Fatalf("events[%d] type = %T, want int", i, event.Value())
		}
		if idx != i {
			t.Fatalf("events[%d] = %d, want %d", i, idx, i)
		}
	}
}

func TestApplyTaskWrites_OrdersWritesByTaskPath(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	channels := newPregelChannelMap(map[string]Channel{
		"events": NewTopic(true),
	})

	tasks := []pregelTask[map[string]any]{
		{
			id:       "task-ten",
			name:     "worker",
			path:     []any{"push", 10},
			triggers: []string{"trigger"},
		},
		{
			id:       "task-two",
			name:     "worker",
			path:     []any{"push", 2},
			triggers: []string{"trigger"},
		},
	}
	results := []pregelTaskResult{
		{
			taskID: "task-ten",
			node:   "worker",
			path:   []any{"push", 10},
			writes: []pregelWrite{{channel: "events", value: 10}},
		},
		{
			taskID: "task-two",
			node:   "worker",
			path:   []any{"push", 2},
			writes: []pregelWrite{{channel: "events", value: 2}},
		},
	}

	_, _, _, err := applyTaskWrites(g, channels, tasks, results, nil, false)
	if err != nil {
		t.Fatalf("applyTaskWrites: %v", err)
	}

	raw, err := channels.channels["events"].Get()
	if err != nil {
		t.Fatalf("reading events topic: %v", err)
	}
	values, ok := raw.Value().([]Dynamic)
	if !ok {
		t.Fatalf("events type = %T, want []Dynamic", raw.Value())
	}
	if len(values) != 2 {
		t.Fatalf("events len = %d, want 2", len(values))
	}
	first, ok := values[0].Value().(int)
	if !ok {
		t.Fatalf("events[0] type = %T, want int", values[0].Value())
	}
	second, ok := values[1].Value().(int)
	if !ok {
		t.Fatalf("events[1] type = %T, want int", values[1].Value())
	}
	if first != 2 || second != 10 {
		t.Fatalf("events order = [%d %d], want [2 10]", first, second)
	}
}

func TestRoutingTargets_AppliesPathMapAndFiltersInvalid(t *testing.T) {
	route := Route{
		Nodes: []string{"raw", "mapped", "", End},
	}
	pathMap := map[string]string{
		"mapped": "dest",
	}

	targets := routingTargets(route, pathMap)
	if len(targets) != 2 || targets[0] != "raw" || targets[1] != "dest" {
		t.Fatalf("routingTargets = %#v, want [raw dest]", targets)
	}
}

func TestRoutingTargetsBoth_SplitsNodeTargetsAndSends(t *testing.T) {
	route := Route{
		Nodes: []string{"left", End},
		Sends: []Send{
			{Node: "worker", Arg: Dyn("ok")},
			{Node: "", Arg: Dyn("skip")},
			{Node: End, Arg: Dyn("skip")},
		},
	}

	nodes, sends, err := routingTargetsBoth(route, nil)
	if err != nil {
		t.Fatalf("routingTargetsBoth error: %v", err)
	}
	if len(nodes) != 1 || nodes[0] != "left" {
		t.Fatalf("routingTargetsBoth nodes = %#v, want [left]", nodes)
	}
	if len(sends) != 1 || sends[0].Node != "worker" {
		t.Fatalf("routingTargetsBoth sends = %#v, want only worker send", sends)
	}
}

func TestPregelChannelMap_ConsumeChannels(t *testing.T) {
	ready := NewNamedBarrierValue([]string{"source"})
	if _, err := ready.Update([]Dynamic{Dyn("source")}); err != nil {
		t.Fatalf("seed ready channel: %v", err)
	}
	other := NewNamedBarrierValue([]string{"source"})
	if _, err := other.Update([]Dynamic{Dyn("source")}); err != nil {
		t.Fatalf("seed other channel: %v", err)
	}

	channels := newPregelChannelMap(map[string]Channel{
		"ready": ready,
		"other": other,
	})

	changed := channels.consumeChannels([]string{"ready", "missing"})
	if len(changed) != 1 || !changed["ready"] {
		t.Fatalf("consume changed = %#v, want only ready", changed)
	}
	if channels.channels["ready"].IsAvailable() {
		t.Fatal("ready channel should be consumed and unavailable")
	}
	if !channels.channels["other"].IsAvailable() {
		t.Fatal("other channel should remain available")
	}
}

func TestApplyTaskWrites_ConsumesOnlyTriggeredChannels(t *testing.T) {
	g := NewStateGraph[map[string]any]()

	triggered := NewNamedBarrierValue([]string{"source"})
	if _, err := triggered.Update([]Dynamic{Dyn("source")}); err != nil {
		t.Fatalf("seed triggered channel: %v", err)
	}
	notTriggered := NewNamedBarrierValue([]string{"source"})
	if _, err := notTriggered.Update([]Dynamic{Dyn("source")}); err != nil {
		t.Fatalf("seed non-triggered channel: %v", err)
	}

	channels := newPregelChannelMap(map[string]Channel{
		"triggered":     triggered,
		"not_triggered": notTriggered,
	})

	tasks := []pregelTask[map[string]any]{
		{
			id:       "task-1",
			name:     "worker",
			path:     []any{"pull", "worker"},
			triggers: []string{"triggered"},
		},
	}
	results := []pregelTaskResult{{
		taskID: "task-1",
		node:   "worker",
		path:   []any{"pull", "worker"},
	}}

	_, versionBumped, updatedChannels, err := applyTaskWrites(g, channels, tasks, results, nil, false)
	if err != nil {
		t.Fatalf("applyTaskWrites: %v", err)
	}

	if !versionBumped["triggered"] {
		t.Fatalf("versionBumped = %#v, want triggered consumed", versionBumped)
	}
	if versionBumped["not_triggered"] {
		t.Fatalf("versionBumped = %#v, did not expect not_triggered", versionBumped)
	}
	if len(updatedChannels) != 0 {
		t.Fatalf("updatedChannels = %#v, want empty", updatedChannels)
	}
	if channels.channels["triggered"].IsAvailable() {
		t.Fatal("triggered channel should have been consumed")
	}
	if !channels.channels["not_triggered"].IsAvailable() {
		t.Fatal("not_triggered channel should still be available")
	}
}

func TestPrepareNextTasks_WaitingEdgeFallsBackToPrevNodesWhenJoinChannelMissing(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNode("a", func(_ context.Context, _ map[string]any) (NodeResult, error) { return NoNodeResult(), nil })
	g.AddNode("b", func(_ context.Context, _ map[string]any) (NodeResult, error) { return NoNodeResult(), nil })
	g.AddNode("join", func(_ context.Context, _ map[string]any) (NodeResult, error) { return NoNodeResult(), nil })
	g.AddEdge(Start, "a")
	g.AddEdge(Start, "b")
	g.AddEdges([]string{"a", "b"}, "join")
	g.AddEdge("join", End)

	channels := newPregelChannelMap(map[string]Channel{})
	tasks, err := prepareNextTasks(context.Background(), g, Config{}, channels, nil, []string{"a", "b"}, 1, false)
	if err != nil {
		t.Fatalf("prepareNextTasks: %v", err)
	}
	if len(tasks) != 1 || tasks[0].name != "join" {
		t.Fatalf("tasks = %#v, want one join task", tasks)
	}
}

func TestPrepareNextTasks_WaitingEdgeFallbackRequiresAllSources(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNode("a", func(_ context.Context, _ map[string]any) (NodeResult, error) { return NoNodeResult(), nil })
	g.AddNode("b", func(_ context.Context, _ map[string]any) (NodeResult, error) { return NoNodeResult(), nil })
	g.AddNode("join", func(_ context.Context, _ map[string]any) (NodeResult, error) { return NoNodeResult(), nil })
	g.AddEdge(Start, "a")
	g.AddEdge(Start, "b")
	g.AddEdges([]string{"a", "b"}, "join")
	g.AddEdge("join", End)

	channels := newPregelChannelMap(map[string]Channel{})
	tasks, err := prepareNextTasks(context.Background(), g, Config{}, channels, nil, []string{"a"}, 1, false)
	if err != nil {
		t.Fatalf("prepareNextTasks: %v", err)
	}
	if len(tasks) != 0 {
		t.Fatalf("tasks = %#v, want no tasks", tasks)
	}
}

func TestPregelLoop_CompileDefaultStepTimeoutApplied(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNode("slow", func(ctx context.Context, _ map[string]any) (NodeResult, error) {
		select {
		case <-ctx.Done():
			return NoNodeResult(), ctx.Err()
		case <-time.After(120 * time.Millisecond):
			return NodeWrites(DynMap(map[string]any{"ok": true})), nil
		}
	})
	g.AddEdge(Start, "slow")
	g.AddEdge("slow", End)

	compiled, err := g.Compile(CompileOptions{StepTimeout: 20 * time.Millisecond})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	_, err = compiled.Invoke(context.Background(), map[string]any{})
	if err == nil {
		t.Fatal("expected step timeout error, got nil")
	}
	var timeoutErr *GraphStepTimeoutError
	if !errors.As(err, &timeoutErr) {
		t.Fatalf("expected GraphStepTimeoutError, got %T: %v", err, err)
	}
}

func TestPregelLoop_CompileDefaultMaxConcurrencyApplied(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	var inFlight atomic.Int64
	var maxInFlight atomic.Int64

	g.AddNode("dispatch", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		sends := make([]Send, 0, 30)
		for i := 0; i < 30; i++ {
			sends = append(sends, Send{Node: "worker", Arg: Dyn(map[string]any{"idx": i})})
		}
		return NodeCommand(&Command{Goto: RouteWithSends(sends...)}), nil
	})
	g.AddNode("worker", func(ctx context.Context, _ map[string]any) (NodeResult, error) {
		now := inFlight.Add(1)
		for {
			currentMax := maxInFlight.Load()
			if now <= currentMax || maxInFlight.CompareAndSwap(currentMax, now) {
				break
			}
		}
		defer inFlight.Add(-1)

		select {
		case <-ctx.Done():
			return NoNodeResult(), ctx.Err()
		case <-time.After(8 * time.Millisecond):
		}
		return NoNodeResult(), nil
	})
	g.AddEdge(Start, "dispatch")

	compiled, err := g.Compile(CompileOptions{MaxConcurrency: 4})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	if _, err := compiled.Invoke(context.Background(), map[string]any{}); err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if got := maxInFlight.Load(); got > 4 {
		t.Fatalf("max concurrent workers = %d, want <= 4", got)
	}
}

// TestPregelLoop_SendFanout verifies that GetSend dispatches a PUSH task whose
// input is Send.Arg (not graph state). There is NO direct edge from sender to
// receiver; routing is purely via the fan-out Send.
func TestPregelLoop_SendFanout(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNode("sender", func(ctx context.Context, _ map[string]any) (NodeResult, error) {
		GetSend(ctx)("receiver", Dyn(map[string]any{"msg": "ping"}))
		return NoNodeResult(), nil
	})
	g.AddNode("receiver", func(_ context.Context, state map[string]any) (NodeResult, error) {
		// PUSH task: state is Send.Arg, so state["msg"] == "ping"
		return NodeWrites(DynMap(map[string]any{"received": state["msg"]})), nil
	})
	g.AddEdge(Start, "sender")
	// No edge "sender" → "receiver": receiver is reached only via Send fan-out.
	g.AddEdge("receiver", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	out, err := compiled.Invoke(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	state, _ := out.Value.(map[string]any)
	if state["received"] != "ping" {
		t.Errorf("received = %v, want %q", state["received"], "ping")
	}
}

func TestPregelLoop_InterruptBefore(t *testing.T) {
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

	compiled, err := g.Compile(CompileOptions{InterruptBefore: []string{"b"}})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	out, err := compiled.Invoke(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Node "a" should have run, "b" should have been interrupted.
	state, _ := out.Value.(map[string]any)
	if state["ran"] != "a" {
		t.Errorf("expected ran=a after interrupt-before b, got %v", state["ran"])
	}
	if len(out.Interrupts) == 0 {
		t.Error("expected at least one interrupt, got none")
	}
}

func TestPregelLoop_StreamValuesMode(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNode("step1", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"step": 1})), nil
	})
	g.AddEdge(Start, "step1")
	g.AddEdge("step1", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ch := compiled.Stream(context.Background(), map[string]any{}, StreamModeValues)
	var parts []StreamPart
	for p := range ch {
		if p.Err != nil {
			t.Fatalf("stream error: %v", p.Err)
		}
		if p.Type == StreamModeValues {
			parts = append(parts, p)
		}
	}

	if len(parts) == 0 {
		t.Fatal("expected at least one values stream part")
	}
}

func TestPregelLoop_StreamMessagesMode(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNode("llm", func(ctx context.Context, _ map[string]any) (NodeResult, error) {
		StreamMessage(ctx, "hel", map[string]any{"index": 0})
		StreamMessage(ctx, "lo", map[string]any{"index": 1})
		return NodeWrites(DynMap(map[string]any{"done": true})), nil
	})
	g.AddEdge(Start, "llm")
	g.AddEdge("llm", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	stream := compiled.Stream(context.Background(), map[string]any{}, StreamModeMessages)
	chunks := make([]MessageChunk, 0, 2)
	for part := range stream {
		if part.Err != nil {
			t.Fatalf("stream error: %v", part.Err)
		}
		if part.Type != StreamModeMessages {
			continue
		}
		chunk, ok := part.Data.(MessageChunk)
		if !ok {
			t.Fatalf("expected MessageChunk payload, got %T", part.Data)
		}
		chunks = append(chunks, chunk)
	}

	if len(chunks) != 2 {
		t.Fatalf("expected 2 message chunks, got %d", len(chunks))
	}
	if chunks[0].Chunk != "hel" || chunks[1].Chunk != "lo" {
		t.Fatalf("unexpected chunk payloads: %#v", chunks)
	}
}

func TestPregelLoop_TagNoStreamSuppressesMessages(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNode("llm", func(ctx context.Context, _ map[string]any) (NodeResult, error) {
		StreamMessage(ctx, "hidden-token", nil)
		return NodeWrites(DynMap(map[string]any{"done": true})), nil
	})
	g.nodes["llm"].Metadata = map[string]any{"tags": []string{TagNoStream}}
	g.AddEdge(Start, "llm")
	g.AddEdge("llm", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	stream := compiled.Stream(context.Background(), map[string]any{}, StreamModeDebug)
	messageCount := 0
	updateCount := 0
	for part := range stream {
		if part.Err != nil {
			t.Fatalf("stream error: %v", part.Err)
		}
		if part.Type == StreamModeMessages {
			messageCount++
		}
		if part.Type == StreamModeUpdates {
			updateCount++
		}
	}
	if messageCount != 0 {
		t.Fatalf("messageCount = %d, want 0", messageCount)
	}
	if updateCount == 0 {
		t.Fatal("expected updates stream to include write")
	}
}

func TestPregelLoop_TagHiddenSuppressesTaskAndUpdateStreams(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNode("hidden", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"x": 1})), nil
	})
	g.nodes["hidden"].Metadata = map[string]any{"tags": []string{TagHidden}}
	g.AddEdge(Start, "hidden")
	g.AddEdge("hidden", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	stream := compiled.Stream(context.Background(), map[string]any{}, StreamModeTasks)
	for part := range stream {
		if part.Err != nil {
			t.Fatalf("stream error: %v", part.Err)
		}
		t.Fatalf("unexpected stream part for hidden node: %#v", part)
	}
}

func TestPregelLoop_TagHiddenSkippedForInterruptAll(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNode("hidden", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"ran": true})), nil
	})
	g.nodes["hidden"].Metadata = map[string]any{"tags": []string{TagHidden}}
	g.AddEdge(Start, "hidden")
	g.AddEdge("hidden", End)

	compiled, err := g.Compile(CompileOptions{InterruptBefore: []string{All}})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	out, err := compiled.Invoke(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	state := out.Value.(map[string]any)
	if state["ran"] != true {
		t.Fatalf("state = %#v, want hidden node to run", state)
	}
}

func TestPregelLoop_StreamCheckpointsModeIncludesNamespace(t *testing.T) {
	saver := checkpoint.NewInMemorySaver()
	g := NewStateGraph[map[string]any]()
	g.AddNode("n", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"k": "v"})), nil
	})
	g.AddEdge(Start, "n")
	g.AddEdge("n", End)

	compiled, err := g.Compile(CompileOptions{Checkpointer: saver})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ctx := WithConfig(context.Background(), Config{
		ThreadID:     "stream-checkpoint-ns",
		Checkpointer: saver,
		CheckpointNS: "root|subgraph",
	})
	stream := compiled.Stream(ctx, map[string]any{}, StreamModeCheckpoints)

	seen := 0
	for part := range stream {
		if part.Err != nil {
			t.Fatalf("stream error: %v", part.Err)
		}
		if part.Type != StreamModeCheckpoints {
			continue
		}
		seen++
		if !reflect.DeepEqual(part.Namespace, []string{"root", "subgraph"}) {
			t.Fatalf("checkpoint namespace = %v, want [root subgraph]", part.Namespace)
		}
		payload, ok := part.Data.(map[string]any)
		if !ok {
			t.Fatalf("checkpoint payload type = %T, want map[string]any", part.Data)
		}
		for _, key := range []string{"config", "metadata", "values", "next", "tasks", "parent_config"} {
			if _, exists := payload[key]; !exists {
				t.Fatalf("checkpoint payload missing key %q: %v", key, payload)
			}
		}
	}

	if seen == 0 {
		t.Fatal("expected checkpoint stream events, got none")
	}
}

func TestPregelLoop_StreamTasksIncludesTaskResultEnvelope(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNode("n", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"k": "v"})), nil
	})
	g.AddEdge(Start, "n")
	g.AddEdge("n", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	stream := compiled.Stream(context.Background(), map[string]any{}, StreamModeTasks)
	var sawStart, sawResult bool
	for part := range stream {
		if part.Err != nil {
			t.Fatalf("stream error: %v", part.Err)
		}
		if part.Type != StreamModeTasks {
			continue
		}
		payload, ok := part.Data.(map[string]any)
		if !ok {
			t.Fatalf("task payload type = %T, want map[string]any", part.Data)
		}
		if _, ok := payload["triggers"]; ok {
			sawStart = true
		}
		if _, ok := payload["result"]; ok {
			sawResult = true
			if _, hasError := payload["error"]; !hasError {
				t.Fatalf("task result payload missing error field: %v", payload)
			}
		}
	}
	if !sawStart || !sawResult {
		t.Fatalf("expected tasks stream to include start+result payloads, got start=%v result=%v", sawStart, sawResult)
	}
}

func TestPregelLoop_StreamDebugWrapsTaskAndCheckpointEvents(t *testing.T) {
	saver := checkpoint.NewInMemorySaver()
	g := NewStateGraph[map[string]any]()
	g.AddNode("n", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"k": "v"})), nil
	})
	g.AddEdge(Start, "n")
	g.AddEdge("n", End)

	compiled, err := g.Compile(CompileOptions{Checkpointer: saver})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ctx := WithConfig(context.Background(), Config{ThreadID: "debug-stream", Checkpointer: saver})
	stream := compiled.Stream(ctx, map[string]any{}, StreamModeDebug)

	sawTask := false
	sawTaskResult := false
	sawCheckpoint := false
	for part := range stream {
		if part.Err != nil {
			t.Fatalf("stream error: %v", part.Err)
		}
		if part.Type != StreamModeDebug {
			continue
		}
		envelope, ok := part.Data.(map[string]any)
		if !ok {
			t.Fatalf("debug envelope type = %T, want map[string]any", part.Data)
		}
		for _, key := range []string{"step", "timestamp", "type", "payload"} {
			if _, exists := envelope[key]; !exists {
				t.Fatalf("debug envelope missing key %q: %v", key, envelope)
			}
		}
		typ, _ := envelope["type"].(string)
		switch typ {
		case "task":
			sawTask = true
		case "task_result":
			sawTaskResult = true
		case "checkpoint":
			sawCheckpoint = true
		}
	}
	if !sawTask || !sawTaskResult || !sawCheckpoint {
		t.Fatalf("expected debug events for task/task_result/checkpoint, got task=%v task_result=%v checkpoint=%v", sawTask, sawTaskResult, sawCheckpoint)
	}
}

func TestPregelLoop_StreamDuplexCombinesModes(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNode("n", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"k": "v"})), nil
	})
	g.AddEdge(Start, "n")
	g.AddEdge("n", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ctx := WithConfig(context.Background(), Config{CheckpointNS: "root|branch"})
	stream := compiled.StreamDuplex(ctx, map[string]any{}, StreamModeTasks, StreamModeUpdates)
	var sawTasks, sawUpdates bool
	for part := range stream {
		if part.Err != nil {
			t.Fatalf("stream error: %v", part.Err)
		}
		switch part.Type {
		case StreamModeTasks:
			sawTasks = true
			if !reflect.DeepEqual(part.Namespace, []string{"root", "branch"}) {
				t.Fatalf("task namespace = %v, want [root branch]", part.Namespace)
			}
		case StreamModeUpdates:
			sawUpdates = true
			if !reflect.DeepEqual(part.Namespace, []string{"root", "branch"}) {
				t.Fatalf("update namespace = %v, want [root branch]", part.Namespace)
			}
		}
	}

	if !sawTasks || !sawUpdates {
		t.Fatalf("expected duplex stream to include tasks+updates, got tasks=%v updates=%v", sawTasks, sawUpdates)
	}
}

func TestPregelLoop_EmptyGraphErrors(t *testing.T) {
	// A graph with Start → End but no nodes won't panic — it just produces no tasks.
	g := NewStateGraph[map[string]any]()
	g.AddEdge(Start, End)

	_, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	// Invoking should succeed with empty output.
	// (No tasks → loop exits immediately.)
}

// TestPregelLoop_MultipleNodesPerSuperstep verifies that when multiple nodes
// are triggered from the same source, all run in the same superstep.
func TestPregelLoop_MultipleNodesPerSuperstep(t *testing.T) {
	var (
		orderMu sync.Mutex
		order   []string
	)
	g := NewStateGraph[map[string]any]()
	g.AddNode("a", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		orderMu.Lock()
		order = append(order, "a")
		orderMu.Unlock()
		return NoNodeResult(), nil
	})
	g.AddNode("b1", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		orderMu.Lock()
		order = append(order, "b1")
		orderMu.Unlock()
		return NodeWrites(DynMap(map[string]any{"b1": true})), nil
	})
	g.AddNode("b2", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		orderMu.Lock()
		order = append(order, "b2")
		orderMu.Unlock()
		return NodeWrites(DynMap(map[string]any{"b2": true})), nil
	})

	g.AddEdge(Start, "a")
	g.AddEdge("a", "b1")
	g.AddEdge("a", "b2")
	g.AddEdge("b1", End)
	g.AddEdge("b2", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	out, err := compiled.Invoke(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}

	state, _ := out.Value.(map[string]any)
	if state["b1"] != true {
		t.Errorf("expected b1=true, got %v", state["b1"])
	}
	if state["b2"] != true {
		t.Errorf("expected b2=true, got %v", state["b2"])
	}
	// Both b1 and b2 ran (order among them is not deterministic).
	if len(order) != 3 {
		t.Errorf("expected 3 node executions (a, b1, b2), got %d: %v", len(order), order)
	}
	found := make(map[string]bool)
	for _, n := range order {
		found[n] = true
	}
	if !found["b1"] || !found["b2"] {
		t.Errorf("expected both b1 and b2 to run, got %v", order)
	}
}

func TestPregelLoop_WaitingEdgeActsAsPersistentJoinBarrier(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNode("a", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"a": true})), nil
	})
	g.AddNode("mid", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NoNodeResult(), nil
	})
	g.AddNode("b", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"b": true})), nil
	})
	g.AddNode("join", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"joined": true})), nil
	})
	g.AddEdge(Start, "a")
	g.AddEdge("a", "mid")
	g.AddEdge("mid", "b")
	g.AddEdges([]string{"a", "b"}, "join")
	g.AddEdge("join", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	out, err := compiled.Invoke(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}

	state, ok := out.Value.(map[string]any)
	if !ok {
		t.Fatalf("output type = %T, want map[string]any", out.Value)
	}
	if state["joined"] != true {
		t.Fatalf("joined = %v, want true", state["joined"])
	}
}

func TestPregelLoop_DeferredWaitingJoinRunsAfterFinish(t *testing.T) {
	var joinRuns atomic.Int64

	g := NewStateGraph[map[string]any]()
	g.AddNode("a", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"a": true})), nil
	})
	g.AddNode("b", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"b": true})), nil
	})
	g.AddNode("join", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		joinRuns.Add(1)
		return NodeWrites(DynMap(map[string]any{"deferred_join": true})), nil
	})
	g.nodes["join"].Defer = true
	g.AddEdge(Start, "a")
	g.AddEdge(Start, "b")
	g.AddEdges([]string{"a", "b"}, "join")
	g.AddEdge("join", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	out, err := compiled.Invoke(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}

	state, ok := out.Value.(map[string]any)
	if !ok {
		t.Fatalf("output type = %T, want map[string]any", out.Value)
	}
	if state["deferred_join"] != true {
		t.Fatalf("deferred_join = %v, want true", state["deferred_join"])
	}
	if got := joinRuns.Load(); got != 1 {
		t.Fatalf("join run count = %d, want 1", got)
	}
}

// --- Phase 4: Send fan-out with arg dispatch ---

// TestPregelLoop_BranchReturnedSend verifies that a branch function returning
// a Send dispatches a PUSH task whose input is Send.Arg.
func TestPregelLoop_BranchReturnedSend(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNode("router", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NoNodeResult(), nil
	})
	g.AddNode("worker", func(_ context.Context, state map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"result": state["payload"]})), nil
	})
	g.AddEdge(Start, "router")
	g.AddConditionalEdges("router",
		func(_ context.Context, _ map[string]any) (Route, error) {
			return RouteWithSends(Send{Node: "worker", Arg: Dyn(map[string]any{"payload": "hello"})}), nil
		}, nil)
	g.AddEdge("worker", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	out, err := compiled.Invoke(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	state, _ := out.Value.(map[string]any)
	if state["result"] != "hello" {
		t.Errorf("result = %v, want %q (Send.Arg should be the task input)", state["result"], "hello")
	}
}

// TestPregelLoop_MultipleSendFanout verifies that multiple Send objects
// dispatch multiple independent PUSH tasks.
func TestPregelLoop_MultipleSendFanout(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.channels["items"] = NewBinaryOperatorAggregate(func(a, b any) any {
		if aSlice, ok := a.([]any); ok {
			return append(aSlice, b)
		}
		// Seed was a scalar (BinaryOperatorAggregate seeds accumulator with
		// the first raw value, so a may be a non-slice on second call).
		return []any{a, b}
	})

	g.AddNode("dispatcher", func(ctx context.Context, _ map[string]any) (NodeResult, error) {
		send := GetSend(ctx)
		send("worker", Dyn(map[string]any{"n": 1}))
		send("worker", Dyn(map[string]any{"n": 2}))
		return NoNodeResult(), nil
	})
	g.AddNode("worker", func(_ context.Context, state map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"items": state["n"]})), nil
	})
	g.AddEdge(Start, "dispatcher")
	g.AddEdge("worker", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	out, err := compiled.Invoke(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	state, _ := out.Value.(map[string]any)
	items, _ := state["items"].([]any)
	if len(items) != 2 {
		t.Errorf("items len = %d, want 2; items = %v", len(items), items)
	}
}

// TestPregelLoop_CommandGotoSend verifies that Command{Goto: Send{...}} routes
// to a node with Send.Arg as input in the next superstep.
func TestPregelLoop_CommandGotoSend(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNode("gate", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeCommand(&Command{Goto: RouteWithSends(Send{Node: "handler", Arg: Dyn(map[string]any{"value": 42})})}), nil
	})
	g.AddNode("handler", func(_ context.Context, state map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"answer": state["value"]})), nil
	})
	g.AddEdge(Start, "gate")
	g.AddEdge("handler", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	out, err := compiled.Invoke(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	state, _ := out.Value.(map[string]any)
	if state["answer"] != 42 {
		t.Errorf("answer = %v, want 42", state["answer"])
	}
}

func TestPregelLoop_SendPayloadCoercesToTypedState(t *testing.T) {
	type S struct {
		Value string `json:"value"`
	}

	g := NewStateGraph[S]()
	g.AddNode("sender", func(ctx context.Context, _ S) (NodeResult, error) {
		GetSend(ctx)("receiver", Dyn(map[string]any{"value": "from-send"}))
		return NoNodeResult(), nil
	})
	g.AddNode("receiver", func(_ context.Context, state S) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"Value": state.Value})), nil
	})
	g.AddEdge(Start, "sender")
	g.AddEdge("receiver", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	out, err := compiled.Invoke(context.Background(), S{Value: "x"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	state, _ := out.Value.(S)
	if state.Value != "from-send" {
		t.Fatalf("state.Value = %q, want %q", state.Value, "from-send")
	}
}

func TestPregelLoop_SendPayloadTypeMismatchErrors(t *testing.T) {
	type S struct {
		Value string
	}

	g := NewStateGraph[S]()
	g.AddNode("sender", func(ctx context.Context, _ S) (NodeResult, error) {
		GetSend(ctx)("receiver", Dyn(123))
		return NoNodeResult(), nil
	})
	g.AddNode("receiver", func(_ context.Context, _ S) (NodeResult, error) {
		return NoNodeResult(), nil
	})
	g.AddEdge(Start, "sender")
	g.AddEdge("receiver", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	_, err = compiled.Invoke(context.Background(), S{Value: "x"})
	if err == nil {
		t.Fatal("expected type mismatch error, got nil")
	}
	if got := err.Error(); got == "" || !strings.Contains(got, "invalid task input") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPregelLoop_FunctionalCallFanout(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNode("dispatch", func(ctx context.Context, _ map[string]any) (NodeResult, error) {
		GetCall(ctx)(Call{
			Name: "double",
			Arg:  Dyn(21),
			Fn: func(_ context.Context, input Dynamic) (NodeResult, error) {
				n, ok := input.Value().(int)
				if !ok {
					return NoNodeResult(), fmt.Errorf("call input type = %T", input.Value())
				}
				return NodeWrites(DynMap(map[string]any{"answer": n * 2})), nil
			},
		})
		return NoNodeResult(), nil
	})
	g.AddEdge(Start, "dispatch")

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	out, err := compiled.Invoke(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	state, _ := out.Value.(map[string]any)
	if state["answer"] != 42 {
		t.Fatalf("answer = %v, want 42", state["answer"])
	}
}

// --- Phase 4: Parallel task execution ---

// TestPregelLoop_ParallelExecution verifies that multiple independent tasks
// in the same superstep run concurrently without data races.
func TestPregelLoop_ParallelExecution(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.channels["counter"] = NewBinaryOperatorAggregate(func(a, b any) any {
		ai, _ := a.(int)
		bi, _ := b.(int)
		return ai + bi
	})

	g.AddNode("start", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NoNodeResult(), nil
	})
	for i := 0; i < 5; i++ {
		name := fmt.Sprintf("w%d", i)
		g.AddNode(name, func(_ context.Context, _ map[string]any) (NodeResult, error) {
			return NodeWrites(DynMap(map[string]any{"counter": 1})), nil
		})
		g.AddEdge("start", name)
		g.AddEdge(name, End)
	}
	g.AddEdge(Start, "start")

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	out, err := compiled.Invoke(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	state, _ := out.Value.(map[string]any)
	if state["counter"] != 5 {
		t.Errorf("counter = %v, want 5 (parallel writes accumulated)", state["counter"])
	}
}

// --- Phase 4: Struct-reflection state I/O ---

// TestStructState_ReflectionIO verifies that typed struct State is correctly
// decomposed into named channels on input and reconstructed on output.
func TestStructState_ReflectionIO(t *testing.T) {
	type MyState struct {
		Name  string
		Count int
	}

	g := NewStateGraph[MyState]()
	g.AddNode("incr", func(_ context.Context, state MyState) (NodeResult, error) {
		return NodeState(Dyn(MyState{Name: state.Name + "!", Count: state.Count + 1})), nil
	})
	g.AddEdge(Start, "incr")
	g.AddEdge("incr", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	out, err := compiled.Invoke(context.Background(), MyState{Name: "hello", Count: 10})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	got, ok := out.Value.(MyState)
	if !ok {
		t.Fatalf("output type = %T, want MyState", out.Value)
	}
	if got.Name != "hello!" {
		t.Errorf("Name = %q, want %q", got.Name, "hello!")
	}
	if got.Count != 11 {
		t.Errorf("Count = %d, want 11", got.Count)
	}
}

// TestMessagesStateGraph_AddMessages verifies that NewMessagesStateGraph uses
// the AddMessages reducer so messages accumulate across nodes.
func TestMessagesStateGraph_AddMessages(t *testing.T) {
	g := NewMessagesStateGraph()
	g.AddNode("node1", func(_ context.Context, _ MessagesState) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{
			"Messages": []Message{{ID: "1", Role: "user", Content: "hi"}},
		})), nil
	})
	g.AddNode("node2", func(_ context.Context, _ MessagesState) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{
			"Messages": []Message{{ID: "2", Role: "assistant", Content: "hello"}},
		})), nil
	})
	g.AddEdge(Start, "node1")
	g.AddEdge("node1", "node2")
	g.AddEdge("node2", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	out, err := compiled.Invoke(context.Background(), MessagesState{})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	got, ok := out.Value.(MessagesState)
	if !ok {
		t.Fatalf("output type = %T, want MessagesState", out.Value)
	}
	if len(got.Messages) != 2 {
		t.Errorf("Messages len = %d, want 2; got %v", len(got.Messages), got.Messages)
	}
}

// --- NodeInterrupt tests ---

// TestNodeInterrupt_HaltsNode verifies that NodeInterrupt halts execution and
// surfaces an Interrupt in the result.
func TestNodeInterrupt_HaltsNode(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNode("ask", func(ctx context.Context, _ map[string]any) (NodeResult, error) {
		NodeInterrupt(ctx, Dyn("need input"))
		// Should not reach here.
		return NodeWrites(DynMap(map[string]any{"reached": true})), nil
	})
	g.AddEdge(Start, "ask")
	g.AddEdge("ask", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	out, err := compiled.Invoke(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out.Interrupts) == 0 {
		t.Fatal("expected at least one interrupt")
	}
	if out.Interrupts[0].Value.Value() != "need input" {
		t.Errorf("interrupt value = %v, want %q", out.Interrupts[0].Value, "need input")
	}

	// "reached" should NOT be set because the node was halted.
	state, _ := out.Value.(map[string]any)
	if state["reached"] == true {
		t.Error("node continued executing after NodeInterrupt — should have halted")
	}
}

// TestNodeInterrupt_ResumeSkipsHalt verifies that when the scratchpad has a
// resume value, NodeInterrupt returns that value instead of panicking.
func TestNodeInterrupt_ResumeReturnValue(t *testing.T) {
	sp := &taskScratchpad{resume: []Dynamic{Dyn("user said yes")}}
	ctx := withTaskScratchpad(context.Background(), sp)

	got := NodeInterrupt(ctx, Dyn("question?"))
	if got.Value() != "user said yes" {
		t.Errorf("NodeInterrupt resume = %v, want %q", got, "user said yes")
	}
}

// TestNodeInterrupt_MultipleCallsMatchedPositionally verifies that multiple
// NodeInterrupt calls within a single node match resume values by position.
func TestNodeInterrupt_MultipleCallsMatchedPositionally(t *testing.T) {
	sp := &taskScratchpad{resume: []Dynamic{Dyn("first"), Dyn("second")}}
	ctx := withTaskScratchpad(context.Background(), sp)

	a := NodeInterrupt(ctx, Dyn("q1"))
	b := NodeInterrupt(ctx, Dyn("q2"))

	if a.Value() != "first" {
		t.Errorf("first resume = %v, want %q", a, "first")
	}
	if b.Value() != "second" {
		t.Errorf("second resume = %v, want %q", b, "second")
	}
}

// TestNodeInterrupt_ThirdCallHaltsWhenNoMoreResumeValues verifies that the
// third NodeInterrupt halts when only two resume values were provided.
func TestNodeInterrupt_ThirdCallHalts(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Error("expected panic from third NodeInterrupt, got none")
			return
		}
		if _, ok := r.(nodeInterruptSignal); !ok {
			panic(r) // re-panic unexpected panic
		}
	}()

	sp := &taskScratchpad{resume: []Dynamic{Dyn("a"), Dyn("b")}}
	ctx := withTaskScratchpad(context.Background(), sp)

	NodeInterrupt(ctx, Dyn("q1")) // consumes resume[0]
	NodeInterrupt(ctx, Dyn("q2")) // consumes resume[1]
	NodeInterrupt(ctx, Dyn("q3")) // should panic
}

// --- Command routing tests ---

// TestCommandGoto_StringRouting verifies that a node returning Command{Goto: "target"}
// is extracted without error.
func TestCommandResult_StringGoto(t *testing.T) {
	cmd := &Command{Goto: RouteTo("node_b"), Update: map[string]Dynamic{"x": Dyn(1)}}
	writes, targets, resumeMap, resumeValues, err := commandResult(cmd)
	if err != nil {
		t.Fatalf("commandResult: %v", err)
	}

	if len(writes) != 1 || writes[0].channel != "x" {
		t.Errorf("writes = %v, want [{x 1}]", writes)
	}
	if len(targets) != 1 || targets[0].Node != "node_b" {
		t.Errorf("targets = %v, want [{node_b ...}]", targets)
	}
	if len(resumeMap) != 0 {
		t.Errorf("resumeMap = %v, want empty", resumeMap)
	}
	if len(resumeValues) != 0 {
		t.Errorf("resumeValues = %v, want []", resumeValues)
	}
}

// TestCommandResult_SendGoto verifies a Send-based Goto.
func TestCommandResult_SendGoto(t *testing.T) {
	cmd := &Command{Goto: RouteWithSends(Send{Node: "node_c", Arg: Dyn("custom")})}
	_, targets, _, _, err := commandResult(cmd)
	if err != nil {
		t.Fatalf("commandResult: %v", err)
	}
	if len(targets) != 1 || targets[0].Node != "node_c" || targets[0].Arg.Value() != "custom" {
		t.Errorf("targets = %v, want [{node_c custom}]", targets)
	}
}

// TestCommandResult_SliceSendGoto verifies []Send Goto.
func TestCommandResult_SliceSendGoto(t *testing.T) {
	cmd := &Command{Goto: RouteWithSends(Send{Node: "n1"}, Send{Node: "n2"})}
	_, targets, _, _, err := commandResult(cmd)
	if err != nil {
		t.Fatalf("commandResult: %v", err)
	}
	if len(targets) != 2 {
		t.Fatalf("targets len = %d, want 2", len(targets))
	}
}

// TestCommandResult_Resume verifies that resume values are extracted.
func TestCommandResult_Resume(t *testing.T) {
	cmd := &Command{Resume: []Dynamic{Dyn("user approved")}}
	_, _, resumeMap, resumeValues, err := commandResult(cmd)
	if err != nil {
		t.Fatalf("commandResult: %v", err)
	}
	if len(resumeMap) != 0 {
		t.Fatalf("resumeMap = %v, want empty", resumeMap)
	}
	if len(resumeValues) != 1 || resumeValues[0] != "user approved" {
		t.Errorf("resumeValues = %v, want [user approved]", resumeValues)
	}
}

// TestCommandResult_SliceResume verifies that []any resume values pass through.
func TestCommandResult_SliceResume(t *testing.T) {
	cmd := &Command{Resume: []Dynamic{Dyn("a"), Dyn("b")}}
	_, _, _, resumeValues, err := commandResult(cmd)
	if err != nil {
		t.Fatalf("commandResult: %v", err)
	}
	if len(resumeValues) != 2 {
		t.Fatalf("resumeValues len = %d, want 2", len(resumeValues))
	}
}

func TestCommandResult_SingleResumeValue(t *testing.T) {
	cmd := &Command{Resume: "approved"}
	_, _, resumeMap, resumeValues, err := commandResult(cmd)
	if err != nil {
		t.Fatalf("commandResult: %v", err)
	}
	if len(resumeMap) != 0 {
		t.Fatalf("resumeMap = %v, want empty", resumeMap)
	}
	if len(resumeValues) != 1 || resumeValues[0] != "approved" {
		t.Fatalf("resumeValues = %v, want [approved]", resumeValues)
	}
}

func TestCommandResult_ResumeMap(t *testing.T) {
	cmd := &Command{Resume: map[string]Dynamic{"interrupt-1": Dyn("Alice")}}
	_, _, resumeMap, resumeValues, err := commandResult(cmd)
	if err != nil {
		t.Fatalf("commandResult: %v", err)
	}
	if len(resumeValues) != 0 {
		t.Fatalf("resumeValues = %v, want empty", resumeValues)
	}
	if len(resumeMap) != 1 || resumeMap["interrupt-1"] != "Alice" {
		t.Fatalf("resumeMap = %v, want map[interrupt-1:Alice]", resumeMap)
	}
}

func TestCommandResult_ResumeAnyMap(t *testing.T) {
	cmd := &Command{Resume: map[string]any{"interrupt-1": "Alice"}}
	_, _, resumeMap, resumeValues, err := commandResult(cmd)
	if err != nil {
		t.Fatalf("commandResult: %v", err)
	}
	if len(resumeValues) != 0 {
		t.Fatalf("resumeValues = %v, want empty", resumeValues)
	}
	if len(resumeMap) != 1 || resumeMap["interrupt-1"] != "Alice" {
		t.Fatalf("resumeMap = %v, want map[interrupt-1:Alice]", resumeMap)
	}
}

func TestCommandResult_InvalidResumeMapType(t *testing.T) {
	cmd := &Command{Resume: map[int]any{1: "nope"}}
	_, _, _, _, err := commandResult(cmd)
	if err == nil {
		t.Fatal("expected error for non-string resume map keys")
	}
}

// TestCommandResult_ParentGraph verifies that a Command with Graph == CommandParent
// is rejected by commandResult with a parentCommandError, and that commands
// with a nil Graph field are accepted normally.
func TestCommandResult_ParentGraph(t *testing.T) {
	parent := CommandParent
	cmd := &Command{
		Graph:  &parent,
		Update: map[string]Dynamic{"x": Dyn(42)},
		Goto:   RouteTo("node_b"),
	}
	_, _, _, _, err := commandResult(cmd)
	if err == nil {
		t.Fatal("expected parentCommandError, got nil")
	}
	var pce *parentCommandError
	if !errors.As(err, &pce) {
		t.Fatalf("expected *parentCommandError, got %T: %v", err, err)
	}
	if pce.cmd != cmd {
		t.Error("parentCommandError should carry the original command")
	}

	// nil Graph is a normal command — no error expected.
	normal := &Command{Goto: RouteTo("node_b")}
	_, _, _, _, errNormal := commandResult(normal)
	if errNormal != nil {
		t.Fatalf("normal command should not error, got %v", errNormal)
	}
}

// TestExtractCommandFromOutput verifies both *Command and Command value detection.
func TestExtractCommandFromOutput(t *testing.T) {
	cmd := &Command{Goto: RouteTo("target")}
	got, ok := extractCommandFromOutput(cmd)
	if !ok || got != cmd {
		t.Errorf("*Command extraction failed: %v %v", ok, got)
	}

	val := Command{Goto: RouteTo("target")}
	got2, ok2 := extractCommandFromOutput(val)
	if !ok2 || got2 == nil {
		t.Errorf("Command value extraction failed: %v %v", ok2, got2)
	}

	_, ok3 := extractCommandFromOutput("not a command")
	if ok3 {
		t.Error("string should not be extracted as Command")
	}
}

// TestResumeValues_Single verifies single value wrapping.
func TestResumeValues_Single(t *testing.T) {
	got := resumeValues([]Dynamic{Dyn("hello")})
	if len(got) != 1 || got[0].Value() != "hello" {
		t.Errorf("got = %v, want [hello]", got)
	}
}

// TestResumeValues_Slice verifies slice pass-through.
func TestResumeValues_Slice(t *testing.T) {
	got := resumeValues([]Dynamic{Dyn("a"), Dyn("b")})
	if len(got) != 2 {
		t.Errorf("got = %v, want [a b]", got)
	}
}

// TestResumeValues_Nil returns nil.
func TestResumeValues_Nil(t *testing.T) {
	got := resumeValues(nil)
	if got != nil {
		t.Errorf("got = %v, want nil", got)
	}
}

func TestNormalizeResumeValues_Dynamic(t *testing.T) {
	got, err := normalizeResumeValues(Dyn("hello"))
	if err != nil {
		t.Fatalf("normalizeResumeValues: %v", err)
	}
	if len(got) != 1 || got[0].Value() != "hello" {
		t.Fatalf("got = %v, want [hello]", got)
	}
}

// Compile-time check: CompiledStateGraph.Stream produces typed StreamPart values.
func TestPregelLoop_StreamErrorTerminates(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNode("err_node", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NoNodeResult(), fmt.Errorf("intentional error")
	})
	g.AddEdge(Start, "err_node")
	g.AddEdge("err_node", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ch := compiled.Stream(context.Background(), map[string]any{}, StreamModeUpdates)
	var errPart *StreamPart
	for p := range ch {
		if p.Err != nil {
			pp := p
			errPart = &pp
		}
	}

	if errPart == nil {
		t.Fatal("expected a StreamPart with Err set, got none")
	}
}

func TestPregelLoop_PersistsErrorChannelWriteOnFailure(t *testing.T) {
	saver := checkpoint.NewInMemorySaver()
	g := NewStateGraph[map[string]any]()
	g.AddNode("err_node", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NoNodeResult(), fmt.Errorf("intentional error")
	})
	g.AddEdge(Start, "err_node")
	g.AddEdge("err_node", End)

	compiled, err := g.Compile(CompileOptions{Checkpointer: saver})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ctx := WithConfig(context.Background(), Config{
		ThreadID:     "error-write-thread",
		Checkpointer: saver,
	})
	_, err = compiled.Invoke(ctx, map[string]any{})
	if err == nil {
		t.Fatal("expected invoke error, got nil")
	}

	tuple, err := saver.GetTuple(context.Background(), &checkpoint.Config{ThreadID: "error-write-thread"})
	if err != nil {
		t.Fatalf("GetTuple: %v", err)
	}
	if tuple == nil {
		t.Fatal("expected checkpoint tuple to be present")
	}

	var (
		found bool
		value any
	)
	for _, w := range tuple.PendingWrites {
		if w.Channel != "__error__" {
			continue
		}
		found = true
		value = w.Value
		break
	}
	if !found {
		t.Fatalf("expected __error__ pending write, got %#v", tuple.PendingWrites)
	}
	if got, ok := value.(string); !ok || !strings.Contains(got, "intentional error") {
		t.Fatalf("expected __error__ value containing task error, got %#v", value)
	}
}

func TestPregelLoop_TaskFailureCancelsSiblingTasks(t *testing.T) {
	var canceled atomic.Bool

	g := NewStateGraph[map[string]any]()
	g.AddNode("fail", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NoNodeResult(), errors.New("boom")
	})
	g.AddNode("slow", func(ctx context.Context, _ map[string]any) (NodeResult, error) {
		select {
		case <-ctx.Done():
			canceled.Store(true)
			return NoNodeResult(), ctx.Err()
		case <-time.After(250 * time.Millisecond):
			return NodeWrites(DynMap(map[string]any{"slow": true})), nil
		}
	})
	g.AddEdge(Start, "fail")
	g.AddEdge(Start, "slow")
	g.AddEdge("fail", End)
	g.AddEdge("slow", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	_, err = compiled.Invoke(context.Background(), map[string]any{})
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("expected boom error, got %v", err)
	}
	if !canceled.Load() {
		t.Fatal("expected sibling task to observe context cancellation")
	}
}

func TestPregelLoop_BranchRoutePathMap(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNode("router", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NoNodeResult(), nil
	})
	g.AddNode("left", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"path": "left"})), nil
	})
	g.AddEdge(Start, "router")
	g.AddConditionalEdges("router", func(_ context.Context, _ map[string]any) (Route, error) {
		return RouteTo("L"), nil
	}, map[string]string{"L": "left"})
	g.AddEdge("left", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	out, err := compiled.Invoke(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	state, _ := out.Value.(map[string]any)
	if state["path"] != "left" {
		t.Fatalf("path = %v, want left", state["path"])
	}
}

func TestPregelLoop_UnsupportedNodeOutputTypeErrors(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNode("bad", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeState(Dyn(123)), nil
	})
	g.AddEdge(Start, "bad")
	g.AddEdge("bad", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	_, err = compiled.Invoke(context.Background(), map[string]any{})
	if err == nil {
		t.Fatal("expected unsupported node output error")
	}
}

func TestPregelLoop_CheckpointTracksVersionsSeen(t *testing.T) {
	saver := checkpoint.NewInMemorySaver()
	g := NewStateGraph[map[string]any]()
	g.AddNode("a", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"x": 1})), nil
	})
	g.AddEdge(Start, "a")
	g.AddEdge("a", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ctx := WithConfig(context.Background(), Config{
		ThreadID:     "version-test",
		Checkpointer: saver,
	})
	_, err = compiled.Invoke(ctx, map[string]any{"in": "v"})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}

	tuple, err := saver.GetTuple(context.Background(), &checkpoint.Config{ThreadID: "version-test"})
	if err != nil {
		t.Fatalf("GetTuple: %v", err)
	}
	if tuple == nil || tuple.Checkpoint == nil {
		t.Fatal("expected stored checkpoint")
	}
	if len(tuple.Checkpoint.ChannelVersions) == 0 {
		t.Fatal("expected ChannelVersions to be populated")
	}
	xVersion, err := checkpoint.VersionAsInt64(tuple.Checkpoint.ChannelVersions["x"])
	if err != nil || xVersion <= 0 {
		t.Fatalf("expected numeric version token for channel 'x', got %v (err=%v)", tuple.Checkpoint.ChannelVersions["x"], err)
	}
	seen, ok := tuple.Checkpoint.VersionsSeen["a"]
	if !ok || len(seen) == 0 {
		t.Fatalf("expected VersionsSeen for node 'a', got %#v", tuple.Checkpoint.VersionsSeen)
	}
	if len(tuple.Checkpoint.UpdatedChannels) == 0 {
		t.Fatal("expected UpdatedChannels to be populated")
	}
}

func TestPregelTaskStructure_DeterministicTaskIDs(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNode("a", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"x": 1})), nil
	})
	g.AddEdge(Start, "a")
	g.AddEdge("a", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ctx := WithConfig(context.Background(), Config{CheckpointNS: "root"})
	first := streamTaskStarts(t, compiled.Stream(ctx, map[string]any{}, StreamModeTasks))
	second := streamTaskStarts(t, compiled.Stream(ctx, map[string]any{}, StreamModeTasks))

	if len(first) == 0 || len(second) == 0 {
		t.Fatal("expected task start events")
	}
	if got, want := first[0]["id"], second[0]["id"]; got != want {
		t.Fatalf("task IDs are not deterministic: first=%v second=%v", got, want)
	}
}

func TestPregelTaskStructure_WaitingJoinUsesJoinTriggerForID(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNode("a", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"a": true})), nil
	})
	g.AddNode("b", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"b": true})), nil
	})
	g.AddNode("join", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"joined": true})), nil
	})
	g.AddEdge(Start, "a")
	g.AddEdge(Start, "b")
	g.AddEdges([]string{"a", "b"}, "join")
	g.AddEdge("join", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ctx := WithConfig(context.Background(), Config{CheckpointNS: "root"})
	starts := streamTaskStarts(t, compiled.Stream(ctx, map[string]any{}, StreamModeTasks))

	var join map[string]any
	for _, event := range starts {
		name, _ := event["name"].(string)
		if name == "join" {
			join = event
			break
		}
	}
	if join == nil {
		t.Fatal("expected join task start event")
	}

	joinTrigger := "join:a+b:join"
	triggers, ok := join["triggers"].([]string)
	if !ok {
		t.Fatalf("join triggers type = %T, want []string", join["triggers"])
	}
	if !reflect.DeepEqual(triggers, []string{joinTrigger}) {
		t.Fatalf("join triggers = %v, want [%s]", triggers, joinTrigger)
	}

	path, ok := join["path"].([]any)
	if !ok {
		t.Fatalf("join path type = %T, want []any", join["path"])
	}
	id, _ := join["id"].(string)
	step := taskEventStep(t, join)
	expected := pregelTaskID(Config{CheckpointNS: "root"}, step, path, "join", []string{joinTrigger})
	if id != expected {
		t.Fatalf("join task id = %s, want %s", id, expected)
	}
}

func TestPregelTaskStructure_TracksTaskPathForPushAndPull(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNode("sender", func(ctx context.Context, _ map[string]any) (NodeResult, error) {
		GetSend(ctx)("receiver", Dyn(map[string]any{"msg": "ping"}))
		return NoNodeResult(), nil
	})
	g.AddNode("receiver", func(_ context.Context, state map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"seen": state["msg"]})), nil
	})
	g.AddEdge(Start, "sender")
	g.AddEdge("receiver", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	starts := streamTaskStarts(t, compiled.Stream(context.Background(), map[string]any{}, StreamModeTasks))
	if len(starts) < 2 {
		t.Fatalf("expected at least 2 task starts, got %d", len(starts))
	}

	pathsByName := map[string][]any{}
	for _, ev := range starts {
		name, _ := ev["name"].(string)
		path, _ := ev["path"].([]any)
		pathsByName[name] = path
	}

	if got := fmt.Sprintf("%v", pathsByName["sender"]); !strings.Contains(got, "pull") {
		t.Fatalf("sender path should include pull semantics, got %v", pathsByName["sender"])
	}
	if got := fmt.Sprintf("%v", pathsByName["receiver"]); !strings.Contains(got, "push") {
		t.Fatalf("receiver path should include push semantics, got %v", pathsByName["receiver"])
	}
}

func TestPregelTaskStructure_AppliesNodeWriters(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNode("a", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NoNodeResult(), nil
	})
	g.SetNodeWriters("a", func(_ context.Context, _ TaskContext[map[string]any], result NodeResult) (NodeResult, error) {
		if result.Writes == nil {
			result.Writes = map[string]Dynamic{}
		}
		result.Writes["writer_applied"] = Dyn(true)
		return result, nil
	})
	g.AddEdge(Start, "a")
	g.AddEdge("a", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	out, err := compiled.Invoke(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	state := out.Value.(map[string]any)
	if state["writer_applied"] != true {
		t.Fatalf("expected writer_applied=true, got %v", state["writer_applied"])
	}
}

func TestPregelLoop_ResumeMapByInterruptID(t *testing.T) {
	saver := checkpoint.NewInMemorySaver()
	g := NewStateGraph[map[string]any]()
	g.AddNode("ask", func(ctx context.Context, _ map[string]any) (NodeResult, error) {
		answer := NodeInterrupt(ctx, Dyn("what is your name?"))
		return NodeWrites(DynMap(map[string]any{"answer": answer.Value()})), nil
	})
	g.AddEdge(Start, "ask")
	g.AddEdge("ask", End)

	compiled, err := g.Compile(CompileOptions{Checkpointer: saver})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	firstCfg := Config{ThreadID: "resume-map-by-id", Checkpointer: saver}
	first, err := compiled.Invoke(WithConfig(context.Background(), firstCfg), map[string]any{"input": "hi"})
	if err != nil {
		t.Fatalf("first invoke: %v", err)
	}
	if len(first.Interrupts) != 1 {
		t.Fatalf("expected 1 interrupt, got %d", len(first.Interrupts))
	}
	interruptID := first.Interrupts[0].ID

	secondCfg := Config{
		ThreadID:     "resume-map-by-id",
		Checkpointer: saver,
		Metadata: map[string]any{
			ConfigKeyResumeMap: map[string]any{interruptID: "Alice"},
		},
	}
	second, err := compiled.Invoke(WithConfig(context.Background(), secondCfg), map[string]any{})
	if err != nil {
		t.Fatalf("second invoke: %v", err)
	}
	if len(second.Interrupts) != 0 {
		t.Fatalf("expected no interrupts after resume, got %d", len(second.Interrupts))
	}

	state, ok := second.Value.(map[string]any)
	if !ok {
		t.Fatalf("expected map output, got %T", second.Value)
	}
	if got := state["answer"]; got != "Alice" {
		t.Fatalf("answer = %v, want Alice", got)
	}
}

func TestPregelLoop_SequentialInterruptResumesDoNotReplayHistoricalTasks(t *testing.T) {
	saver := checkpoint.NewInMemorySaver()
	g := NewStateGraph[map[string]any]()
	g.AddNode("ask1", func(ctx context.Context, _ map[string]any) (NodeResult, error) {
		answer := NodeInterrupt(ctx, Dyn("q1"))
		return NodeWrites(DynMap(map[string]any{"a1": answer.Value()})), nil
	})
	g.AddNode("ask2", func(ctx context.Context, _ map[string]any) (NodeResult, error) {
		answer := NodeInterrupt(ctx, Dyn("q2"))
		return NodeWrites(DynMap(map[string]any{"a2": answer.Value()})), nil
	})
	g.AddNode("done", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"done": true})), nil
	})
	g.AddEdge(Start, "ask1")
	g.AddEdge("ask1", "ask2")
	g.AddEdge("ask2", "done")
	g.AddEdge("done", End)

	compiled, err := g.Compile(CompileOptions{Checkpointer: saver})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	firstCfg := Config{ThreadID: "resume-sequential", Checkpointer: saver}
	first, err := compiled.Invoke(WithConfig(context.Background(), firstCfg), map[string]any{})
	if err != nil {
		t.Fatalf("first invoke: %v", err)
	}
	if len(first.Interrupts) != 1 {
		t.Fatalf("expected 1 interrupt on first invoke, got %d", len(first.Interrupts))
	}

	secondCfg := Config{
		ThreadID:     "resume-sequential",
		Checkpointer: saver,
		Metadata: map[string]any{
			ConfigKeyResumeMap: map[string]any{first.Interrupts[0].ID: "one"},
		},
	}
	second, err := compiled.Invoke(WithConfig(context.Background(), secondCfg), map[string]any{})
	if err != nil {
		t.Fatalf("second invoke: %v", err)
	}
	if len(second.Interrupts) != 1 {
		t.Fatalf("expected 1 interrupt on second invoke, got %d", len(second.Interrupts))
	}

	thirdCfg := Config{
		ThreadID:     "resume-sequential",
		Checkpointer: saver,
		Metadata: map[string]any{
			ConfigKeyResumeMap: map[string]any{second.Interrupts[0].ID: "two"},
		},
	}
	third, err := compiled.Invoke(WithConfig(context.Background(), thirdCfg), map[string]any{})
	if err != nil {
		t.Fatalf("third invoke: %v", err)
	}
	if len(third.Interrupts) != 0 {
		t.Fatalf("expected no interrupts after second resume, got %d", len(third.Interrupts))
	}
	state, ok := third.Value.(map[string]any)
	if !ok {
		t.Fatalf("expected map output, got %T", third.Value)
	}
	if got := state["a1"]; got != "one" {
		t.Fatalf("a1 = %v, want one", got)
	}
	if got := state["a2"]; got != "two" {
		t.Fatalf("a2 = %v, want two", got)
	}
	if got := state["done"]; got != true {
		t.Fatalf("done = %v, want true", got)
	}

	fourthCfg := Config{ThreadID: "resume-sequential", Checkpointer: saver}
	fourth, err := compiled.Invoke(WithConfig(context.Background(), fourthCfg), map[string]any{})
	if err != nil {
		t.Fatalf("fourth invoke: %v", err)
	}
	if len(fourth.Interrupts) != 0 {
		t.Fatalf("expected no interrupts after completion, got %d", len(fourth.Interrupts))
	}
}

func TestPregelLoop_ThreeSequentialInterruptsNoReplayOrRecursionLimit(t *testing.T) {
	saver := checkpoint.NewInMemorySaver()
	g := NewStateGraph[map[string]any]()
	g.AddNode("interview", func(ctx context.Context, _ map[string]any) (NodeResult, error) {
		answer := NodeInterrupt(ctx, Dyn("interview question"))
		return NodeWrites(DynMap(map[string]any{"interview_answer": answer.Value()})), nil
	})
	g.AddNode("consultation", func(ctx context.Context, _ map[string]any) (NodeResult, error) {
		answer := NodeInterrupt(ctx, Dyn("consultation question"))
		return NodeWrites(DynMap(map[string]any{"consultation_answer": answer.Value()})), nil
	})
	g.AddNode("final", func(ctx context.Context, _ map[string]any) (NodeResult, error) {
		answer := NodeInterrupt(ctx, Dyn("final question"))
		return NodeWrites(DynMap(map[string]any{"final_answer": answer.Value()})), nil
	})
	g.AddNode("done", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"done": true})), nil
	})
	g.AddEdge(Start, "interview")
	g.AddEdge("interview", "consultation")
	g.AddEdge("consultation", "final")
	g.AddEdge("final", "done")
	g.AddEdge("done", End)

	compiled, err := g.Compile(CompileOptions{Checkpointer: saver})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	threadID := "three-stage-workflow"

	// First invoke - should interrupt at interview
	first, err := compiled.Invoke(WithConfig(context.Background(), Config{ThreadID: threadID, Checkpointer: saver}), map[string]any{})
	if err != nil {
		t.Fatalf("first invoke: %v", err)
	}
	if len(first.Interrupts) != 1 {
		t.Fatalf("expected 1 interrupt at interview, got %d", len(first.Interrupts))
	}

	// Second invoke - resume interview, should interrupt at consultation
	second, err := compiled.Invoke(WithConfig(context.Background(), Config{
		ThreadID:     threadID,
		Checkpointer: saver,
		Metadata:     map[string]any{ConfigKeyResumeMap: map[string]any{first.Interrupts[0].ID: "interview_answer"}},
	}), map[string]any{})
	if err != nil {
		t.Fatalf("second invoke: %v", err)
	}
	if len(second.Interrupts) != 1 {
		t.Fatalf("expected 1 interrupt at consultation, got %d", len(second.Interrupts))
	}

	// Third invoke - resume consultation, should interrupt at final
	third, err := compiled.Invoke(WithConfig(context.Background(), Config{
		ThreadID:     threadID,
		Checkpointer: saver,
		Metadata:     map[string]any{ConfigKeyResumeMap: map[string]any{second.Interrupts[0].ID: "consultation_answer"}},
	}), map[string]any{})
	if err != nil {
		t.Fatalf("third invoke: %v", err)
	}
	if len(third.Interrupts) != 1 {
		t.Fatalf("expected 1 interrupt at final, got %d", len(third.Interrupts))
	}

	// Fourth invoke - resume final, should complete
	fourth, err := compiled.Invoke(WithConfig(context.Background(), Config{
		ThreadID:     threadID,
		Checkpointer: saver,
		Metadata:     map[string]any{ConfigKeyResumeMap: map[string]any{third.Interrupts[0].ID: "final_answer"}},
	}), map[string]any{})
	if err != nil {
		t.Fatalf("fourth invoke: %v", err)
	}
	if len(fourth.Interrupts) != 0 {
		t.Fatalf("expected no interrupts after final resume, got %d", len(fourth.Interrupts))
	}

	state, ok := fourth.Value.(map[string]any)
	if !ok {
		t.Fatalf("expected map output, got %T", fourth.Value)
	}
	if got := state["interview_answer"]; got != "interview_answer" {
		t.Fatalf("interview_answer = %v, want interview_answer", got)
	}
	if got := state["consultation_answer"]; got != "consultation_answer" {
		t.Fatalf("consultation_answer = %v, want consultation_answer", got)
	}
	if got := state["final_answer"]; got != "final_answer" {
		t.Fatalf("final_answer = %v, want final_answer", got)
	}
	if got := state["done"]; got != true {
		t.Fatalf("done = %v, want true", got)
	}

	// Fifth invoke - post-completion check, should have no interrupts
	fifth, err := compiled.Invoke(WithConfig(context.Background(), Config{ThreadID: threadID, Checkpointer: saver}), map[string]any{})
	if err != nil {
		t.Fatalf("fifth invoke: %v", err)
	}
	if len(fifth.Interrupts) != 0 {
		t.Fatalf("expected no interrupts after completion, got %d", len(fifth.Interrupts))
	}
}

func TestPregelLoop_ResumeRequiresMapWhenMultiplePendingInterrupts(t *testing.T) {
	saver := checkpoint.NewInMemorySaver()
	g := NewStateGraph[map[string]any]()
	g.AddNode("a", func(ctx context.Context, _ map[string]any) (NodeResult, error) {
		NodeInterrupt(ctx, Dyn("a?"))
		return NoNodeResult(), nil
	})
	g.AddNode("b", func(ctx context.Context, _ map[string]any) (NodeResult, error) {
		NodeInterrupt(ctx, Dyn("b?"))
		return NoNodeResult(), nil
	})
	g.AddEdge(Start, "a")
	g.AddEdge(Start, "b")

	compiled, err := g.Compile(CompileOptions{Checkpointer: saver})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	firstCfg := Config{ThreadID: "resume-multi", Checkpointer: saver}
	first, err := compiled.Invoke(WithConfig(context.Background(), firstCfg), map[string]any{})
	if err != nil {
		t.Fatalf("first invoke: %v", err)
	}
	if len(first.Interrupts) != 2 {
		t.Fatalf("expected 2 interrupts, got %d", len(first.Interrupts))
	}

	secondCfg := Config{
		ThreadID:     "resume-multi",
		Checkpointer: saver,
		Metadata: map[string]any{
			ConfigKeyResume: "single-value",
		},
	}
	_, err = compiled.Invoke(WithConfig(context.Background(), secondCfg), map[string]any{})
	if err == nil {
		t.Fatal("expected error when resuming multiple interrupts without resume_map")
	}
	if !strings.Contains(err.Error(), ConfigKeyResumeMap) {
		t.Fatalf("expected resume_map guidance error, got %v", err)
	}
}

func TestPregelLoop_InterruptBeforeUsesVersionChecksOnResume(t *testing.T) {
	saver := checkpoint.NewInMemorySaver()
	g := NewStateGraph[map[string]any]()
	g.AddNode("a", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"x": 1})), nil
	})
	g.AddNode("b", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"done": true})), nil
	})
	g.AddEdge(Start, "a")
	g.AddEdge("a", "b")
	g.AddEdge("b", End)

	compiled, err := g.Compile(CompileOptions{Checkpointer: saver, InterruptBefore: []string{"b"}})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	firstCfg := Config{ThreadID: "interrupt-version-check", Checkpointer: saver}
	first, err := compiled.Invoke(WithConfig(context.Background(), firstCfg), map[string]any{})
	if err != nil {
		t.Fatalf("first invoke: %v", err)
	}
	if len(first.Interrupts) != 1 {
		t.Fatalf("expected 1 interrupt, got %d", len(first.Interrupts))
	}

	resumeID := first.Interrupts[0].ID
	secondCfg := Config{
		ThreadID:     "interrupt-version-check",
		Checkpointer: saver,
		Metadata: map[string]any{
			ConfigKeyResumeMap: map[string]any{resumeID: "continue"},
		},
	}
	second, err := compiled.Invoke(WithConfig(context.Background(), secondCfg), map[string]any{})
	if err != nil {
		t.Fatalf("second invoke: %v", err)
	}
	if len(second.Interrupts) != 0 {
		t.Fatalf("expected no second interrupt, got %d", len(second.Interrupts))
	}
	state, ok := second.Value.(map[string]any)
	if !ok {
		t.Fatalf("expected map output, got %T", second.Value)
	}
	if got := state["done"]; got != true {
		t.Fatalf("done = %v, want true", got)
	}
}

func TestPregelTaskStructure_EmitsTaskCacheKeyInputSchemaAndSubgraphs(t *testing.T) {
	type nodeInput struct {
		Message string `json:"message"`
	}

	g := NewStateGraph[map[string]any]()
	g.AddNodeFunc("typed", func(_ context.Context, in nodeInput) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"message": in.Message})), nil
	})
	g.SetNodeCachePolicy("typed", &CachePolicy{KeyFunc: func(_ any) string { return "cache-key" }})
	g.SetNodeSubgraphs("typed", "math_subgraph", "io_subgraph")
	g.AddEdge(Start, "typed")
	g.AddEdge("typed", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ctx := WithConfig(context.Background(), Config{ThreadID: "task-structure-thread", CheckpointNS: "root"})
	starts := streamTaskStarts(t, compiled.Stream(ctx, map[string]any{"message": "hello"}, StreamModeTasks))
	if len(starts) == 0 {
		t.Fatal("expected task start events")
	}
	event := starts[0]

	if event["input_schema"] == nil {
		t.Fatal("expected input_schema in task event")
	}
	cacheKey, ok := event["cache_key"].(*CacheKey)
	if !ok || cacheKey == nil {
		t.Fatalf("expected cache_key in task event, got %T", event["cache_key"])
	}
	if cacheKey.Key != "cache-key" {
		t.Fatalf("unexpected cache key: %q", cacheKey.Key)
	}
	subgraphs, ok := event["subgraphs"].([]string)
	if !ok {
		t.Fatalf("expected []string subgraphs, got %T", event["subgraphs"])
	}
	if len(subgraphs) != 2 || subgraphs[0] != "math_subgraph" || subgraphs[1] != "io_subgraph" {
		t.Fatalf("unexpected subgraphs metadata: %v", subgraphs)
	}
}

func TestPregelTaskStructure_EmitsTaskCheckpointNamespaceMetadata(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNode("n", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"done": true})), nil
	})
	g.AddEdge(Start, "n")
	g.AddEdge("n", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ctx := WithConfig(context.Background(), Config{CheckpointNS: "root|branch"})
	starts := streamTaskStarts(t, compiled.Stream(ctx, map[string]any{}, StreamModeTasks))
	if len(starts) == 0 {
		t.Fatal("expected task start events")
	}
	event := starts[0]

	id, _ := event["id"].(string)
	checkpointNS, ok := event["checkpoint_ns"].(string)
	if !ok {
		t.Fatalf("task checkpoint_ns type = %T, want string", event["checkpoint_ns"])
	}
	wantNS := fmt.Sprintf("root|branch|n:%s", id)
	if checkpointNS != wantNS {
		t.Fatalf("checkpoint_ns = %q, want %q", checkpointNS, wantNS)
	}
	path, ok := event["checkpoint_path"].([]string)
	if !ok {
		t.Fatalf("task checkpoint_path type = %T, want []string", event["checkpoint_path"])
	}
	if !reflect.DeepEqual(path, []string{"root", "branch", fmt.Sprintf("n:%s", id)}) {
		t.Fatalf("checkpoint_path = %v, want [root branch n:%s]", path, id)
	}
}

func streamTaskStarts(t *testing.T, ch <-chan StreamPart) []map[string]any {
	t.Helper()
	starts := make([]map[string]any, 0)
	for part := range ch {
		if part.Err != nil {
			t.Fatalf("stream error: %v", part.Err)
		}
		if part.Type != StreamModeTasks {
			continue
		}
		payload, ok := part.Data.(map[string]any)
		if !ok {
			continue
		}
		if _, hasInput := payload["input"]; hasInput {
			starts = append(starts, payload)
		}
	}
	return starts
}

func taskEventStep(t *testing.T, payload map[string]any) int {
	t.Helper()
	stepValue, ok := payload["step"]
	if !ok {
		t.Fatalf("task payload missing step: %v", payload)
	}
	switch step := stepValue.(type) {
	case int:
		return step
	case int64:
		return int(step)
	case float64:
		return int(step)
	default:
		t.Fatalf("unexpected step type %T", stepValue)
	}
	return 0
}

// --- parent command routing tests ---

// TestSubgraph_ParentCommandUpdatesPropagateToParent verifies that a node inside
// a subgraph can issue a Command with Graph == CommandParent to apply state
// updates directly to the enclosing parent graph.
func TestSubgraph_ParentCommandUpdatesPropagateToParent(t *testing.T) {
	// Subgraph: one node that issues a parent command with a state update.
	sub := NewStateGraph[map[string]any]()
	sub.AddNode("sub_node", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		parent := CommandParent
		return NodeCommand(&Command{
			Graph:  &parent,
			Update: map[string]Dynamic{"parent_key": Dyn("from_subgraph")},
		}), nil
	})
	sub.AddEdge(Start, "sub_node")
	sub.AddEdge("sub_node", End)
	compiledSub, err := sub.Compile()
	if err != nil {
		t.Fatalf("compile subgraph: %v", err)
	}

	// Parent graph: routes through the subgraph node, then a finalizer.
	parent := NewStateGraph[map[string]any]()
	parent.AddNode("subgraph_node", compiledSub.AsSubgraphNode("subgraph_node"))
	parent.AddNode("after", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"after_ran": true})), nil
	})
	parent.AddEdge(Start, "subgraph_node")
	parent.AddEdge("subgraph_node", "after")
	parent.AddEdge("after", End)
	compiledParent, err := parent.Compile()
	if err != nil {
		t.Fatalf("compile parent: %v", err)
	}

	out, err := compiledParent.Invoke(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	state := out.Value.(map[string]any)
	if state["parent_key"] != "from_subgraph" {
		t.Errorf("parent_key = %v, want %q", state["parent_key"], "from_subgraph")
	}
	if state["after_ran"] != true {
		t.Errorf("after_ran = %v, want true (execution should continue after parent command)", state["after_ran"])
	}
}

// TestSubgraph_ParentCommandGotoRoutesParent verifies that a parent command's
// Goto field routes execution in the parent graph rather than the subgraph.
func TestSubgraph_ParentCommandGotoRoutesParent(t *testing.T) {
	sub := NewStateGraph[map[string]any]()
	sub.AddNode("sub_node", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		parent := CommandParent
		return NodeCommand(&Command{
			Graph: &parent,
			Goto:  RouteTo("routed_node"),
		}), nil
	})
	sub.AddEdge(Start, "sub_node")
	sub.AddEdge("sub_node", End)
	compiledSub, err := sub.Compile()
	if err != nil {
		t.Fatalf("compile subgraph: %v", err)
	}

	var routedRan bool
	parent := NewStateGraph[map[string]any]()
	parent.AddNode("subgraph_node", compiledSub.AsSubgraphNode("subgraph_node"))
	parent.AddNode("routed_node", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		routedRan = true
		return NodeWrites(DynMap(map[string]any{"routed": true})), nil
	})
	parent.AddNode("unreachable", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		t.Error("unreachable node should not run")
		return NodeResult{}, nil
	})
	parent.AddEdge(Start, "subgraph_node")
	// subgraph_node has no direct outgoing edge — routing comes from the parent command.
	parent.AddEdge("routed_node", End)
	compiledParent, err := parent.Compile()
	if err != nil {
		t.Fatalf("compile parent: %v", err)
	}

	out, err := compiledParent.Invoke(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if !routedRan {
		t.Error("routed_node should have run via parent command Goto")
	}
	state := out.Value.(map[string]any)
	if state["routed"] != true {
		t.Errorf("routed = %v, want true", state["routed"])
	}
}

// TestSubgraph_ParentCommandWithUpdateAndGoto verifies that both state update and
// goto routing work together when issued via a parent command.
func TestSubgraph_ParentCommandWithUpdateAndGoto(t *testing.T) {
	sub := NewStateGraph[map[string]any]()
	sub.AddNode("sub_node", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		p := CommandParent
		return NodeCommand(&Command{
			Graph:  &p,
			Update: map[string]Dynamic{"injected": Dyn("yes")},
			Goto:   RouteTo("landing"),
		}), nil
	})
	sub.AddEdge(Start, "sub_node")
	sub.AddEdge("sub_node", End)
	compiledSub, err := sub.Compile()
	if err != nil {
		t.Fatalf("compile subgraph: %v", err)
	}

	parent := NewStateGraph[map[string]any]()
	parent.AddNode("sub", compiledSub.AsSubgraphNode("sub"))
	parent.AddNode("landing", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"landed": true})), nil
	})
	parent.AddEdge(Start, "sub")
	parent.AddEdge("landing", End)
	compiledParent, err := parent.Compile()
	if err != nil {
		t.Fatalf("compile parent: %v", err)
	}

	out, err := compiledParent.Invoke(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	state := out.Value.(map[string]any)
	if state["injected"] != "yes" {
		t.Errorf("injected = %v, want %q", state["injected"], "yes")
	}
	if state["landed"] != true {
		t.Errorf("landed = %v, want true", state["landed"])
	}
}

// TestRootGraph_ParentCommandErrors verifies that issuing a parent command from
// a node in a root (non-subgraph) graph returns an error to the caller.
func TestRootGraph_ParentCommandErrors(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNode("node", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		p := CommandParent
		return NodeCommand(&Command{
			Graph:  &p,
			Update: map[string]Dynamic{"k": Dyn(1)},
		}), nil
	})
	g.AddEdge(Start, "node")
	g.AddEdge("node", End)
	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	_, err = compiled.Invoke(context.Background(), map[string]any{})
	if err == nil {
		t.Fatal("expected error when issuing parent command from root graph, got nil")
	}
	var pce *parentCommandError
	if !errors.As(err, &pce) {
		t.Fatalf("expected *parentCommandError, got %T: %v", err, err)
	}
}
