package graph

import (
	"context"
	"strings"
	"testing"
)

func TestStateGraph_AddNodeRejectsReservedAndInvalidNames(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	nodeFn := func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NoNodeResult(), nil
	}

	assertPanicsWith(t, "reserved", func() {
		g.AddNode(START, nodeFn)
	})
	assertPanicsWith(t, "reserved character", func() {
		g.AddNode("bad|node", nodeFn)
	})
	assertPanicsWith(t, "must not be empty", func() {
		g.AddNode("", nodeFn)
	})
	assertPanicsWith(t, "must not be nil", func() {
		g.AddNode("nil_fn", nil)
	})
}

func TestStateGraph_AddManagedValueRejectsReservedNames(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	assertPanicsWith(t, "reserved", func() {
		g.AddManagedValue(pregelInterrupt, RemainingStepsManager{})
	})
	assertPanicsWith(t, "reserved character", func() {
		g.AddManagedValue("bad:name", RemainingStepsManager{})
	})
}

func TestStateGraph_AddConditionalEdgesValidatesPathAndDuplicateNames(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNode("router", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NoNodeResult(), nil
	})

	assertPanicsWith(t, "must not be nil", func() {
		g.AddConditionalEdges("router", nil, nil)
	})

	routeLeft := func(_ context.Context, _ map[string]any) (Route, error) {
		return RouteTo("left"), nil
	}
	g.AddConditionalEdges("router", routeLeft, map[string]string{"left": "left"})
	assertPanicsWith(t, "already exists", func() {
		g.AddConditionalEdges("router", routeLeft, map[string]string{"left": "left"})
	})
}

func TestStateGraph_ValidateAllowsAllInterruptSentinel(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNode("n", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NoNodeResult(), nil
	})
	g.AddEdge(START, "n")
	g.AddEdge("n", END)

	if _, err := g.Compile(CompileOptions{InterruptBefore: []string{All}, InterruptAfter: []string{All}}); err != nil {
		t.Fatalf("compile with All interrupt sentinel: %v", err)
	}
}

func TestStateGraph_CompileUsesBuilderInterruptDefaults(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNode("n", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NoNodeResult(), nil
	})
	g.AddEdge(START, "n")
	g.AddEdge("n", END)
	g.SetInterruptBefore("n")
	g.SetInterruptAfter("n")

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile with builder interrupt defaults: %v", err)
	}
	if len(compiled.interruptBefore) != 1 || compiled.interruptBefore[0] != "n" {
		t.Fatalf("compiled interrupt before = %#v, want [n]", compiled.interruptBefore)
	}
	if len(compiled.interruptAfter) != 1 || compiled.interruptAfter[0] != "n" {
		t.Fatalf("compiled interrupt after = %#v, want [n]", compiled.interruptAfter)
	}
}

func TestStateGraph_CompileOptionInterruptsOverrideBuilderDefaults(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNode("a", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NoNodeResult(), nil
	})
	g.AddNode("b", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NoNodeResult(), nil
	})
	g.AddEdge(START, "a")
	g.AddEdge("a", "b")
	g.AddEdge("b", END)
	g.SetInterruptBefore("a")
	g.SetInterruptAfter("a")

	compiled, err := g.Compile(CompileOptions{InterruptBefore: []string{"b"}, InterruptAfter: []string{}})
	if err != nil {
		t.Fatalf("compile with interrupt overrides: %v", err)
	}
	if len(compiled.interruptBefore) != 1 || compiled.interruptBefore[0] != "b" {
		t.Fatalf("compiled interrupt before = %#v, want [b]", compiled.interruptBefore)
	}
	if len(compiled.interruptAfter) != 0 {
		t.Fatalf("compiled interrupt after = %#v, want empty", compiled.interruptAfter)
	}
}

func TestStateGraph_ValidateRejectsReservedChannelIdentifiers(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNode("n", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NoNodeResult(), nil
	})
	g.AddEdge(START, "n")
	g.AddEdge("n", END)
	g.channels[pregelError] = NewLastValue()

	if _, err := g.Compile(); err == nil || !strings.Contains(err.Error(), "channel name") {
		t.Fatalf("expected reserved channel validation error, got %v", err)
	}
}

func TestStateGraph_ValidateRejectsUnknownDeclaredDestinations(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNode("n", func(_ context.Context, _ map[string]any) (NodeResult, error) {
		return NoNodeResult(), nil
	})
	g.AddEdge(START, "n")
	g.AddEdge("n", END)
	g.nodes["n"].Destinations = append(g.nodes["n"].Destinations, "missing")

	if _, err := g.Compile(); err == nil || !strings.Contains(err.Error(), "declares unknown destination") {
		t.Fatalf("expected unknown destination validation error, got %v", err)
	}
}

func assertPanicsWith(t *testing.T, contains string, fn func()) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("expected panic containing %q", contains)
		}
		if contains == "" {
			return
		}
		msg := ""
		switch v := r.(type) {
		case error:
			msg = v.Error()
		case string:
			msg = v
		default:
			msg = "unknown panic"
		}
		if !strings.Contains(msg, contains) {
			t.Fatalf("panic %q does not contain %q", msg, contains)
		}
	}()
	fn()
}
