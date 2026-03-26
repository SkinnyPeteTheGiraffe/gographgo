// Package main demonstrates conditional routing in gographgo.
//
// This example models a simple review pipeline:
//
//	Start → review → approve OR reject → notify → End
//
// The "review" node sets a Score. A conditional edge then routes to
// "approve" or "reject" based on that score. Both paths converge at
// "notify" before the graph exits.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/graph"
)

// State holds review pipeline data.
type State struct {
	Item    string
	Score   int
	Verdict string
	Notice  string
}

func main() {
	builder := graph.NewStateGraph[State]()

	// review assigns a score.
	builder.AddNode("review", func(ctx context.Context, s State) (graph.NodeResult, error) {
		score := len(s.Item) * 10 // simple deterministic score for demonstration
		return graph.NodeWrites(map[string]graph.Dynamic{
			"Score": graph.Dyn(score),
		}), nil
	})

	// approve handles high-scoring items.
	builder.AddNode("approve", func(ctx context.Context, s State) (graph.NodeResult, error) {
		return graph.NodeWrites(map[string]graph.Dynamic{
			"Verdict": graph.Dyn("approved"),
		}), nil
	})

	// reject handles low-scoring items.
	builder.AddNode("reject", func(ctx context.Context, s State) (graph.NodeResult, error) {
		return graph.NodeWrites(map[string]graph.Dynamic{
			"Verdict": graph.Dyn("rejected"),
		}), nil
	})

	// notify sends a summary regardless of verdict.
	builder.AddNode("notify", func(ctx context.Context, s State) (graph.NodeResult, error) {
		msg := fmt.Sprintf("Item %q scored %d and was %s.", s.Item, s.Score, s.Verdict)
		return graph.NodeWrites(map[string]graph.Dynamic{
			"Notice": graph.Dyn(msg),
		}), nil
	})

	// Wire static edges.
	builder.AddEdge(graph.Start, "review")
	builder.AddEdge("approve", "notify")
	builder.AddEdge("reject", "notify")
	builder.AddEdge("notify", graph.End)

	// Conditional edge: after "review", route based on score.
	builder.AddConditionalEdges("review",
		func(ctx context.Context, s State) (graph.Route, error) {
			if s.Score >= 50 {
				return graph.RouteTo("approve"), nil
			}
			return graph.RouteTo("reject"), nil
		},
		// PathMap lists all possible destinations so the graph can validate them.
		map[string]string{
			"approve": "approve",
			"reject":  "reject",
		},
	)

	compiled, err := builder.Compile()
	if err != nil {
		log.Fatalf("compile: %v", err)
	}

	for _, item := range []string{"hi", "gographgo"} {
		result, err := compiled.Invoke(context.Background(), State{Item: item})
		if err != nil {
			log.Fatalf("invoke: %v", err)
		}
		final := result.Value.(State)
		fmt.Println(final.Notice)
	}
}
