package graph

import (
	"context"
	"testing"
)

// TestStateGraphBasic tests basic graph execution with map[string]any state.
func TestStateGraphBasic(t *testing.T) {
	// Create a simple graph using map[string]any for state
	builder := NewStateGraph[map[string]any]()

	// Add nodes
	builder.AddNode("increment", func(_ context.Context, state map[string]any) (NodeResult, error) {
		count := 0
		if c, ok := state["count"]; ok {
			count = c.(int)
		}
		return NodeWrites(DynMap(map[string]any{"count": count + 1})), nil
	})

	builder.AddNode("append_name", func(_ context.Context, state map[string]any) (NodeResult, error) {
		name := ""
		if n, ok := state["name"]; ok {
			name = n.(string)
		}
		return NodeWrites(DynMap(map[string]any{"name": name + "_modified"})), nil
	})

	// Add edges
	builder.AddEdge(Start, "increment")
	builder.AddEdge("increment", "append_name")
	builder.AddEdge("append_name", End)

	// Compile
	graph, err := builder.Compile()
	if err != nil {
		t.Fatalf("Failed to compile graph: %v", err)
	}

	// Invoke
	ctx := context.Background()
	initialState := map[string]any{
		"count": 0,
		"name":  "test",
	}
	result, err := graph.Invoke(ctx, initialState)
	if err != nil {
		t.Fatalf("Failed to invoke graph: %v", err)
	}

	// Verify result
	finalState, ok := result.Value.(map[string]any)
	if !ok {
		t.Fatalf("Expected map[string]any, got %T", result.Value)
	}

	if finalState["count"] != 1 {
		t.Errorf("Expected count=1, got %v", finalState["count"])
	}

	if finalState["name"] != "test_modified" {
		t.Errorf("Expected name='test_modified', got '%v'", finalState["name"])
	}
}

func TestStateGraphValidation(t *testing.T) {
	// Test missing entry point
	builder := NewStateGraph[map[string]any]()
	builder.AddNode("node1", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NoNodeResult(), nil
	})
	builder.AddEdge("node1", End)

	err := builder.Validate(nil)
	if err == nil {
		t.Error("Expected error for missing entry point, got nil")
	}

	// Test unknown node in edge
	builder2 := NewStateGraph[map[string]any]()
	builder2.AddNode("node1", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NoNodeResult(), nil
	})
	builder2.AddEdge(Start, "unknown")

	err = builder2.Validate(nil)
	if err == nil {
		t.Error("Expected error for unknown node, got nil")
	}
}

func TestStateGraphStream(t *testing.T) {
	builder := NewStateGraph[map[string]any]()

	builder.AddNode("double", func(_ context.Context, state map[string]any) (NodeResult, error) {
		count := 0
		if c, ok := state["count"]; ok {
			count = c.(int)
		}
		return NodeWrites(DynMap(map[string]any{"count": count * 2})), nil
	})

	builder.AddEdge(Start, "double")
	builder.AddEdge("double", End)

	graph, err := builder.Compile()
	if err != nil {
		t.Fatalf("Failed to compile graph: %v", err)
	}

	ctx := context.Background()
	stream := graph.Stream(ctx, map[string]any{"count": 5}, StreamModeUpdates)

	updateCount := 0
	for part := range stream {
		if part.Type == StreamModeUpdates {
			updateCount++
		}
	}

	if updateCount == 0 {
		t.Error("Expected stream updates, got none")
	}
}

func TestContains(t *testing.T) {
	slice := []string{"a", "b", "c"}

	if !contains(slice, "a") {
		t.Error("Expected contains(slice, 'a') to be true")
	}

	if contains(slice, "d") {
		t.Error("Expected contains(slice, 'd') to be false")
	}
}
