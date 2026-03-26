package graph

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestStateGraph_ManagedValuesInjected(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddManagedValue("remaining", RemainingStepsManager{})
	g.AddNode("check", func(_ context.Context, state map[string]any) (NodeResult, error) {
		_, ok := state["remaining"]
		return NodeWrites(DynMap(map[string]any{"has_remaining": ok})), nil
	})
	g.AddEdge(Start, "check")
	g.AddEdge("check", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	out, err := compiled.Invoke(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	state := out.Value.(map[string]any)
	if state["has_remaining"] != true {
		t.Fatalf("expected managed value in state, got %v", state["has_remaining"])
	}
}

func TestStateGraph_RuntimeContextAndStoreInjected(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNode("n", func(ctx context.Context, _ map[string]any) (NodeResult, error) {
		rt := GetRuntime(ctx)
		_, hasCtx := rt.Context.(map[string]any)
		hasStore := GetStore(ctx) != nil
		return NodeWrites(DynMap(map[string]any{"ctx": hasCtx, "store": hasStore})), nil
	})
	g.AddEdge(Start, "n")
	g.AddEdge("n", End)

	compiled, err := g.Compile(CompileOptions{Store: NewInMemoryStore(), Context: map[string]any{"user_id": "u1"}})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	out, err := compiled.Invoke(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	state := out.Value.(map[string]any)
	if state["ctx"] != true || state["store"] != true {
		t.Fatalf("expected runtime context and store injection, got %#v", state)
	}
}

func TestStateGraph_NodeCachePolicy(t *testing.T) {
	var calls int32
	g := NewStateGraph[map[string]any]()
	g.AddNode("cached", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		atomic.AddInt32(&calls, 1)
		return NodeWrites(DynMap(map[string]any{"ok": true})), nil
	})
	ttl := time.Minute
	g.SetNodeCachePolicy("cached", &CachePolicy{
		TTL: &ttl,
		KeyFunc: func(input any) string {
			m := input.(map[string]any)
			return "x=" + DefaultCacheKey(m["x"])
		},
	})
	g.AddEdge(Start, "cached")
	g.AddEdge("cached", End)

	compiled, err := g.Compile(CompileOptions{Cache: NewInMemoryCache()})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if _, err := compiled.Invoke(context.Background(), map[string]any{"x": 1}); err != nil {
		t.Fatalf("first invoke: %v", err)
	}
	if _, err := compiled.Invoke(context.Background(), map[string]any{"x": 1}); err != nil {
		t.Fatalf("second invoke: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("expected cached node to run once, got %d", got)
	}
}

func TestStateGraph_SchemaAccessors(t *testing.T) {
	type In struct {
		Name string `json:"name"`
	}
	type Out struct {
		Count int `json:"count"`
	}
	type Ctx struct {
		UserID string `json:"user_id"`
	}

	g := NewStateGraphWithTypes[map[string]any, Ctx, In, Out]()
	g.SetInputSchema(In{}).SetOutputSchema(Out{}).SetContextSchema(Ctx{})

	in := g.GetInputJSONSchema()
	out := g.GetOutputJSONSchema()
	c := g.GetContextJSONSchema()

	if in["type"] != "object" || out["type"] != "object" || c["type"] != "object" {
		t.Fatalf("expected object schemas, got in=%v out=%v ctx=%v", in["type"], out["type"], c["type"])
	}
}

// TestStateGraph_InputDistinctFromState verifies that a graph where Input is a
// strict subset of State correctly maps only the Input fields into state
// channels without panicking. Previously any(input).(State) caused a runtime
// panic when Input != State.
func TestStateGraph_InputDistinctFromState(t *testing.T) {
	type State struct {
		Query  string
		Answer string
		Step   int
	}
	type Input struct {
		Query string
	}

	g := NewStateGraphWithTypes[State, any, Input, State]()
	g.AddNode("answer", func(_ context.Context, _ State) (NodeResult, error) {
		return NodeWrites(map[string]Dynamic{
			"Answer": Dyn("42"),
			"Step":   Dyn(1),
		}), nil
	})
	g.AddEdge(Start, "answer")
	g.AddEdge("answer", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	out, err := compiled.Invoke(context.Background(), Input{Query: "what is the answer"})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}

	result, ok := out.Value.(State)
	if !ok {
		t.Fatalf("expected State output, got %T", out.Value)
	}
	if result.Query != "what is the answer" {
		t.Fatalf("expected Query to be propagated from input, got %q", result.Query)
	}
	if result.Answer != "42" {
		t.Fatalf("expected Answer set by node, got %q", result.Answer)
	}
	if result.Step != 1 {
		t.Fatalf("expected Step set by node, got %d", result.Step)
	}
}

// TestStateGraph_OutputDistinctFromState verifies that when Output is a subset
// of State, Invoke returns an Output value built from the matching state
// channels rather than panicking on a type assertion.
func TestStateGraph_OutputDistinctFromState(t *testing.T) {
	type State struct {
		Value    string
		Internal string
	}
	type Output struct {
		Value string
	}

	g := NewStateGraphWithTypes[State, any, State, Output]()
	g.AddNode("set", func(_ context.Context, _ State) (NodeResult, error) {
		return NodeWrites(map[string]Dynamic{
			"Value":    Dyn("result"),
			"Internal": Dyn("hidden"),
		}), nil
	})
	g.AddEdge(Start, "set")
	g.AddEdge("set", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	out, err := compiled.Invoke(context.Background(), State{})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}

	result, ok := out.Value.(Output)
	if !ok {
		t.Fatalf("expected Output type, got %T", out.Value)
	}
	if result.Value != "result" {
		t.Fatalf("expected Value=%q, got %q", "result", result.Value)
	}
}

// TestStateGraph_InputAndOutputBothDistinctFromState verifies that a graph
// with all three types differing (State, Input, Output) maps input fields in
// on entry and output fields out on return without panicking.
func TestStateGraph_InputAndOutputBothDistinctFromState(t *testing.T) {
	type State struct {
		UserQuery string
		Response  string
		Tokens    int
	}
	type Input struct {
		UserQuery string
	}
	type Output struct {
		Response string
	}

	g := NewStateGraphWithTypes[State, any, Input, Output]()
	g.AddNode("process", func(_ context.Context, s State) (NodeResult, error) {
		return NodeWrites(map[string]Dynamic{
			"Response": Dyn("processed: " + s.UserQuery),
			"Tokens":   Dyn(10),
		}), nil
	})
	g.AddEdge(Start, "process")
	g.AddEdge("process", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	out, err := compiled.Invoke(context.Background(), Input{UserQuery: "hello"})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}

	result, ok := out.Value.(Output)
	if !ok {
		t.Fatalf("expected Output type, got %T", out.Value)
	}
	if result.Response != "processed: hello" {
		t.Fatalf("expected Response=%q, got %q", "processed: hello", result.Response)
	}
}
