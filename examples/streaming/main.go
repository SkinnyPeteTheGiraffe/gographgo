// Package main demonstrates streaming graph execution in gographgo.
//
// Stream emits events as each node completes rather than waiting for the
// entire graph to finish. This is useful for showing progress in UIs or
// piping partial results downstream.
//
// This example uses two stream modes simultaneously via StreamDuplex:
//   - StreamModeUpdates  – emits node name + state delta after each node
//   - StreamModeValues   – emits the full state snapshot after each node
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/graph"
)

// State is a simple counter with a log of visited nodes.
type State struct {
	Count   int
	Visited []string
}

func makeCounterNode(name string) graph.Node[State] {
	return func(ctx context.Context, s State) (graph.NodeResult, error) {
		visited := append(append([]string(nil), s.Visited...), name)
		return graph.NodeWrites(map[string]graph.Dynamic{
			"Count":   graph.Dyn(s.Count + 1),
			"Visited": graph.Dyn(visited),
		}), nil
	}
}

func main() {
	builder := graph.NewStateGraph[State]()

	builder.AddNode("step_a", makeCounterNode("step_a"))
	builder.AddNode("step_b", makeCounterNode("step_b"))
	builder.AddNode("step_c", makeCounterNode("step_c"))

	builder.AddEdge(graph.START, "step_a")
	builder.AddEdge("step_a", "step_b")
	builder.AddEdge("step_b", "step_c")
	builder.AddEdge("step_c", graph.END)

	compiled, err := builder.Compile()
	if err != nil {
		log.Fatalf("compile: %v", err)
	}

	// StreamDuplex subscribes to multiple modes in one pass.
	ch := compiled.StreamDuplex(
		context.Background(),
		State{},
		graph.StreamModeUpdates,
		graph.StreamModeValues,
	)

	for part := range ch {
		if part.Err != nil {
			log.Fatalf("stream error: %v", part.Err)
		}

		switch part.Type {
		case graph.StreamModeUpdates:
			// Data is map[string]any{nodeName: stateWrites}
			fmt.Printf("[updates] %v\n", part.Data)

		case graph.StreamModeValues:
			// Data is the full state after this step.
			if s, ok := part.Data.(State); ok {
				fmt.Printf("[values]  count=%d visited=%v\n", s.Count, s.Visited)
			}
		}
	}
}
