package graph

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

func TestStateGraph_ReducerAnnotationTag(t *testing.T) {
	type State struct {
		Items []string `graph:"reducer=append"`
	}

	g := NewStateGraph[State]()
	g.AddNode("a", func(_ context.Context, _ State) (NodeResult, error) {
		return NodeWrites(map[string]Dynamic{"Items": Dyn("first")}), nil
	})
	g.AddNode("b", func(_ context.Context, _ State) (NodeResult, error) {
		return NodeWrites(map[string]Dynamic{"Items": Dyn("second")}), nil
	})
	g.AddEdge(Start, "a")
	g.AddEdge("a", "b")
	g.AddEdge("b", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	out, err := compiled.Invoke(context.Background(), State{})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	result := out.Value.(State)
	if !reflect.DeepEqual(result.Items, []string{"first", "second"}) {
		t.Fatalf("expected append reducer to accumulate values, got %#v", result.Items)
	}
}

func TestStateGraph_AddNodeFuncInfersInputSchema(t *testing.T) {
	type NodeInput struct {
		Count int `json:"count"`
	}

	g := NewStateGraph[map[string]any]()
	g.AddNodeFunc("reader", func(_ context.Context, in NodeInput) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"seen": in.Count})), nil
	})
	g.AddEdge(Start, "reader")
	g.AddEdge("reader", End)

	schema, ok := g.NodeInputSchema("reader")
	if !ok {
		t.Fatal("expected node input schema to be present")
	}
	if reflect.TypeOf(schema) != reflect.TypeOf(NodeInput{}) {
		t.Fatalf("expected inferred input schema %T, got %T", NodeInput{}, schema)
	}

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	out, err := compiled.Invoke(context.Background(), map[string]any{"count": 7})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	state := out.Value.(map[string]any)
	if state["seen"] != 7 {
		t.Fatalf("expected inferred input conversion to set seen=7, got %#v", state["seen"])
	}
}

func TestStateGraph_AddNodeFuncDetectsCommandReturn(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNodeFunc("router", func(_ context.Context, _ map[string]any) (Command, error) {
		return Command{Goto: RouteTo("next"), Update: map[string]Dynamic{"routed": Dyn(true)}}, nil
	})
	g.AddNode("next", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"done": true})), nil
	})
	g.AddEdge(Start, "router")
	g.AddEdge("next", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	out, err := compiled.Invoke(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	state := out.Value.(map[string]any)
	if state["routed"] != true || state["done"] != true {
		t.Fatalf("expected command update and route execution, got %#v", state)
	}
}

func TestStateGraph_CommandDestinationValidation(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNode("router", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeCommand(&Command{Goto: RouteTo("other")}), nil
	})
	g.AddNode("allowed", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NoNodeResult(), nil
	})
	g.AddNode("other", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NoNodeResult(), nil
	})
	g.AddEdge(Start, "router")
	g.AddEdge("router", "allowed")
	g.AddEdge("allowed", End)
	g.AddEdge("other", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	_, err = compiled.Invoke(context.Background(), map[string]any{})
	if err == nil {
		t.Fatal("expected destination validation error")
	}
	if !strings.Contains(err.Error(), "not declared in destinations") {
		t.Fatalf("expected destination validation error, got %v", err)
	}
}

func TestStateGraph_AllEdgesIncludesWaitingEdges(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNode("a", func(_ context.Context, _ map[string]any) (NodeResult, error) { return NoNodeResult(), nil })
	g.AddNode("b", func(_ context.Context, _ map[string]any) (NodeResult, error) { return NoNodeResult(), nil })
	g.AddNode("join", func(_ context.Context, _ map[string]any) (NodeResult, error) { return NoNodeResult(), nil })
	g.AddEdge(Start, "a")
	g.AddEdge(Start, "b")
	g.AddEdges([]string{"a", "b"}, "join")

	all := g.AllEdges()
	if len(all) != 4 {
		t.Fatalf("expected 4 total edges (2 direct + 2 waiting), got %d", len(all))
	}
}

func TestStateGraph_AddNodeFuncInjectsRuntimeParameters(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNodeFunc("n", func(
		_ context.Context,
		_ map[string]any,
		cfg Config,
		writer StreamWriter,
		store Store,
		runtime *Runtime,
	) (NodeResult, error) {
		writer("custom-event")
		return NodeWrites(DynMap(map[string]any{
			"has_config":  cfg.RecursionLimit > 0,
			"has_store":   store != nil,
			"has_runtime": runtime != nil && runtime.Context != nil,
		})), nil
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
	if state["has_config"] != true || state["has_store"] != true || state["has_runtime"] != true {
		t.Fatalf("expected injected config/store/runtime, got %#v", state)
	}

	stream := compiled.Stream(context.Background(), map[string]any{}, StreamModeCustom)
	foundCustom := false
	for part := range stream {
		if part.Err != nil {
			t.Fatalf("stream error: %v", part.Err)
		}
		if part.Type == StreamModeCustom && part.Data == "custom-event" {
			foundCustom = true
		}
	}
	if !foundCustom {
		t.Fatal("expected custom stream event from injected writer")
	}
}

func TestStateGraph_AddNodeFuncInjectsConfigPointer(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNodeFunc("n", func(_ context.Context, _ map[string]any, cfg *Config) (NodeResult, error) {
		if cfg == nil {
			return NodeWrites(DynMap(map[string]any{"cfg_ptr": false})), nil
		}
		return NodeWrites(DynMap(map[string]any{"cfg_ptr": cfg.RecursionLimit > 0})), nil
	})
	g.AddEdge(Start, "n")
	g.AddEdge("n", End)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	out, err := compiled.Invoke(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	state := out.Value.(map[string]any)
	if state["cfg_ptr"] != true {
		t.Fatalf("expected non-nil injected config pointer, got %#v", state)
	}
}

func TestStateGraph_AddConditionalEdgesDynamicSupportsStringAndSlicePathMap(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNode("router", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NoNodeResult(), nil
	})
	g.AddNode("left", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"branch": "left"})), nil
	})
	g.AddNode("right", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NodeWrites(DynMap(map[string]any{"branch": "right"})), nil
	})
	g.AddEdge(Start, "router")
	g.AddEdge("left", End)
	g.AddEdge("right", End)

	g.AddConditionalEdgesDynamic("router", func(_ context.Context, _ map[string]any) (any, error) {
		return "left", nil
	}, []string{"left", "right"})

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	out, err := compiled.Invoke(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	state := out.Value.(map[string]any)
	if state["branch"] != "left" {
		t.Fatalf("branch=%v, want left", state["branch"])
	}
}

func TestCoerceRouteSupportsMixedRouteEntries(t *testing.T) {
	route, err := CoerceRoute([]any{
		"node_a",
		Send{Node: "node_b", Arg: Dyn(map[string]any{"x": 1})},
		RouteToMany("node_c", "node_d"),
	})
	if err != nil {
		t.Fatalf("CoerceRoute error: %v", err)
	}
	if !reflect.DeepEqual(route.Nodes, []string{"node_a", "node_c", "node_d"}) {
		t.Fatalf("route nodes=%v", route.Nodes)
	}
	if len(route.Sends) != 1 || route.Sends[0].Node != "node_b" {
		t.Fatalf("route sends=%v", route.Sends)
	}
}
