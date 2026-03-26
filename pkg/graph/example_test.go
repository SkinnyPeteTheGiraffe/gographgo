package graph_test

import (
	"context"
	"fmt"
	"log"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/graph"
)

// Example demonstrates basic graph usage.
func ExampleStateGraph() {
	// Create a graph builder using map[string]any for state
	builder := graph.NewStateGraph[map[string]any]()

	// Add nodes
	builder.AddNode("init", func(_ context.Context, _ map[string]any) (graph.NodeResult, error) {
		return graph.NodeWrites(graph.DynMap(map[string]any{"count": 0})), nil
	})

	builder.AddNode("add_item", func(_ context.Context, state map[string]any) (graph.NodeResult, error) {
		count := 0
		if c, ok := state["count"]; ok {
			count = c.(int)
		}
		items := []string{}
		if i, ok := state["items"]; ok {
			items = i.([]string)
		}
		items = append(items, fmt.Sprintf("item_%d", count))
		return graph.NodeWrites(graph.DynMap(map[string]any{
			"items": items,
			"count": count + 1,
		})), nil
	})

	// Add edges
	builder.AddEdge(graph.Start, "init")
	builder.AddEdge("init", "add_item")
	builder.AddEdge("add_item", graph.End)

	// Compile graph
	g, err := builder.Compile()
	if err != nil {
		log.Fatal(err)
	}

	// Execute
	ctx := context.Background()
	result, err := g.Invoke(ctx, map[string]any{})
	if err != nil {
		log.Fatal(err)
	}

	finalState := result.Value.(map[string]any)
	fmt.Printf("Count: %v\n", finalState["count"])
	fmt.Printf("Items: %v\n", finalState["items"])

	// Output:
	// Count: 1
	// Items: [item_0]
}
