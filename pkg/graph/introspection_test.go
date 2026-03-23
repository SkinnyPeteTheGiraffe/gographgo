package graph

import (
	"context"
	"strings"
	"testing"
)

func TestCompiledStateGraphGetGraph(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNode("router", func(ctx context.Context, state map[string]any) (NodeResult, error) {
		return NoNodeResult(), nil
	})
	g.AddNode("left", func(ctx context.Context, state map[string]any) (NodeResult, error) {
		return NoNodeResult(), nil
	})
	g.AddNode("right", func(ctx context.Context, state map[string]any) (NodeResult, error) {
		return NoNodeResult(), nil
	})
	g.AddNode("join", func(ctx context.Context, state map[string]any) (NodeResult, error) {
		return NoNodeResult(), nil
	})

	g.AddEdge(START, "router")
	g.AddConditionalEdges("router", func(ctx context.Context, state map[string]any) (Route, error) {
		return Route{}, nil
	}, map[string]string{"l": "left", "r": "right"})
	g.AddEdges([]string{"left", "right"}, "join")
	g.AddEdge("join", END)

	g.SetNodeSubgraphs("router", "decision")
	g.SetNodeRetryPolicy("router", RetryPolicy{MaxAttempts: 2})
	g.SetNodeCachePolicy("router", &CachePolicy{})
	g.nodes["router"].Metadata = map[string]any{"component": "router"}

	compiled, err := g.Compile(CompileOptions{Name: "IntrospectionGraph", InterruptBefore: []string{"router"}})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	info := compiled.GetGraph()
	if info.Name != "IntrospectionGraph" {
		t.Fatalf("graph name = %q, want IntrospectionGraph", info.Name)
	}
	if len(info.Nodes) != 6 {
		t.Fatalf("node count = %d, want 6", len(info.Nodes))
	}

	router := graphNodeByName(info.Nodes, "router")
	if router == nil {
		t.Fatal("router node not found")
	}
	if router.Kind != GraphNodeRegular {
		t.Fatalf("router kind = %q, want %q", router.Kind, GraphNodeRegular)
	}
	if router.Metadata["component"] != "router" {
		t.Fatalf("router metadata = %#v", router.Metadata)
	}
	if !router.HasCachePolicy {
		t.Fatal("router HasCachePolicy = false, want true")
	}
	if router.RetryPolicyCount != 1 {
		t.Fatalf("router RetryPolicyCount = %d, want 1", router.RetryPolicyCount)
	}
	if len(router.Subgraphs) != 1 || router.Subgraphs[0] != "decision" {
		t.Fatalf("router subgraphs = %#v, want [decision]", router.Subgraphs)
	}

	if len(info.InterruptBefore) != 1 || info.InterruptBefore[0] != "router" {
		t.Fatalf("interrupt before = %#v, want [router]", info.InterruptBefore)
	}

	if !hasEdge(info.Edges, GraphEdgeInfo{Source: START, Target: "router", Kind: GraphEdgeDirect, Resolved: true}) {
		t.Fatalf("missing direct edge START->router in %#v", info.Edges)
	}
	if !hasEdge(info.Edges, GraphEdgeInfo{Source: "router", Target: "left", Kind: GraphEdgeConditional, Label: "l", Resolved: true}) {
		t.Fatalf("missing conditional edge router->left in %#v", info.Edges)
	}
	if !hasEdge(info.Edges, GraphEdgeInfo{Source: "right", Target: "join", Kind: GraphEdgeWaiting, Resolved: true}) {
		t.Fatalf("missing waiting edge right->join in %#v", info.Edges)
	}
}

func TestGraphInfoMermaid(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.AddNode("a", func(ctx context.Context, state map[string]any) (NodeResult, error) {
		return NoNodeResult(), nil
	})
	g.AddNode("b", func(ctx context.Context, state map[string]any) (NodeResult, error) {
		return NoNodeResult(), nil
	})
	g.AddEdge(START, "a")
	g.AddConditionalEdges("a", func(ctx context.Context, state map[string]any) (Route, error) {
		return Route{}, nil
	}, map[string]string{"go": "b"})
	g.AddEdge("b", END)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	mermaid := compiled.GetGraph().Mermaid()
	if !strings.Contains(mermaid, "flowchart TD") {
		t.Fatalf("mermaid output missing flowchart header: %s", mermaid)
	}
	if !strings.Contains(mermaid, START) || !strings.Contains(mermaid, END) {
		t.Fatalf("mermaid output missing boundary nodes: %s", mermaid)
	}
	if !strings.Contains(mermaid, "-.->|go|") {
		t.Fatalf("mermaid output missing conditional edge label: %s", mermaid)
	}
	if !strings.Contains(mermaid, "-->") {
		t.Fatalf("mermaid output missing direct edge connector: %s", mermaid)
	}
}

func TestGraphInfoSchemasAndSubgraphs(t *testing.T) {
	g := NewStateGraph[map[string]any]()
	g.SetInputSchema(struct {
		Prompt string `json:"prompt"`
	}{})
	g.SetOutputSchema(struct {
		Response string `json:"response"`
	}{})
	g.SetContextSchema(struct {
		Tenant string `json:"tenant"`
	}{})
	g.AddNode("router", func(ctx context.Context, state map[string]any) (NodeResult, error) {
		return NoNodeResult(), nil
	})
	g.AddEdge(START, "router")
	g.AddEdge("router", END)
	g.SetNodeSubgraphs("router", "decision")

	compiled, err := g.Compile(CompileOptions{Name: "SchemaGraph"})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	info := compiled.GetGraph()
	schemas := info.Schemas()
	if schemas.Input["type"] != "object" {
		t.Fatalf("input schema = %#v", schemas.Input)
	}
	if schemas.Output["type"] != "object" {
		t.Fatalf("output schema = %#v", schemas.Output)
	}
	if schemas.Context["type"] != "object" {
		t.Fatalf("context schema = %#v", schemas.Context)
	}

	subgraphs := info.Subgraphs()
	if len(subgraphs) != 1 {
		t.Fatalf("subgraphs len = %d, want 1", len(subgraphs))
	}
	if subgraphs[0].Name != "decision" || subgraphs[0].ParentNode != "router" || subgraphs[0].Namespace != "router/decision" {
		t.Fatalf("subgraph = %#v", subgraphs[0])
	}
}

func graphNodeByName(nodes []GraphNodeInfo, name string) *GraphNodeInfo {
	for i := range nodes {
		if nodes[i].Name == name {
			return &nodes[i]
		}
	}
	return nil
}

func hasEdge(edges []GraphEdgeInfo, want GraphEdgeInfo) bool {
	for _, edge := range edges {
		if edge.Source != want.Source {
			continue
		}
		if edge.Target != want.Target {
			continue
		}
		if edge.Kind != want.Kind {
			continue
		}
		if want.Label != "" && edge.Label != want.Label {
			continue
		}
		if want.Branch != "" && edge.Branch != want.Branch {
			continue
		}
		if edge.Resolved != want.Resolved {
			continue
		}
		return true
	}
	return false
}
