// Package main demonstrates the simplest possible gographgo graph.
//
// This example builds a linear pipeline:
//
//	START → greet → shout → END
//
// State is a typed Go struct. Each node reads the current state and returns
// channel writes that are merged back into the state before the next node runs.
package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/graph"
)

// State holds all mutable data that flows through the graph.
type State struct {
	Message string
	Steps   int
}

func main() {
	// 1. Create a builder for our State type.
	builder := graph.NewStateGraph[State]()

	// 2. Define nodes. Each node receives the current state and returns
	// writes that update specific fields.
	builder.AddNode("greet", func(ctx context.Context, s State) (graph.NodeResult, error) {
		return graph.NodeWrites(map[string]graph.Dynamic{
			"Message": graph.Dyn("Hello, gographgo!"),
			"Steps":   graph.Dyn(s.Steps + 1),
		}), nil
	})

	builder.AddNode("shout", func(ctx context.Context, s State) (graph.NodeResult, error) {
		return graph.NodeWrites(map[string]graph.Dynamic{
			"Message": graph.Dyn(strings.ToUpper(s.Message)),
			"Steps":   graph.Dyn(s.Steps + 1),
		}), nil
	})

	// 3. Wire up edges: START → greet → shout → END.
	builder.AddEdge(graph.START, "greet")
	builder.AddEdge("greet", "shout")
	builder.AddEdge("shout", graph.END)

	// 4. Compile validates the graph topology and returns an executable graph.
	compiled, err := builder.Compile()
	if err != nil {
		log.Fatalf("compile: %v", err)
	}

	// 5. Invoke runs the graph to completion and returns the final state.
	result, err := compiled.Invoke(context.Background(), State{})
	if err != nil {
		log.Fatalf("invoke: %v", err)
	}

	final := result.Value.(State)
	fmt.Printf("Message: %s\n", final.Message)
	fmt.Printf("Steps:   %d\n", final.Steps)
}
