// Package main demonstrates human-in-the-loop (interrupt/resume) in gographgo.
//
// Execution flow:
//
//	Start → ask_user → process → End
//
// The "ask_user" node calls NodeInterrupt, which halts execution and surfaces
// a question to the caller. The graph resumes when Invoke is called again on
// the same thread ID with a Command carrying the user's answer.
//
// A Checkpointer is required: it persists state between the two Invoke calls
// so the graph knows where to resume from.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/checkpoint"
	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/graph"
)

// State carries the question and the user's answer.
type State struct {
	Question string
	Answer   string
	Summary  string
}

func main() {
	// InMemorySaver persists checkpoints in memory. For production use
	// pkg/checkpoint/postgres or pkg/checkpoint/sqlite instead.
	saver := checkpoint.NewInMemorySaver()

	builder := graph.NewStateGraph[State]()

	// ask_user pauses execution and surfaces a question.
	// On the first run, NodeInterrupt halts the node. On resume the graph
	// re-runs the node from the top; NodeInterrupt then returns the provided
	// answer instead of halting.
	builder.AddNode("ask_user", func(ctx context.Context, s State) (graph.NodeResult, error) {
		answer := graph.NodeInterrupt(ctx, graph.Dyn(s.Question))
		return graph.NodeWrites(map[string]graph.Dynamic{
			"Answer": answer,
		}), nil
	})

	// process uses the captured answer.
	builder.AddNode("process", func(ctx context.Context, s State) (graph.NodeResult, error) {
		summary := fmt.Sprintf("User answered %q to %q", s.Answer, s.Question)
		return graph.NodeWrites(map[string]graph.Dynamic{
			"Summary": graph.Dyn(summary),
		}), nil
	})

	builder.AddEdge(graph.Start, "ask_user")
	builder.AddEdge("ask_user", "process")
	builder.AddEdge("process", graph.End)

	compiled, err := builder.Compile(graph.CompileOptions{Checkpointer: saver})
	if err != nil {
		log.Fatalf("compile: %v", err)
	}

	// All calls sharing the same ThreadID form one conversation thread.
	cfg := graph.Config{
		ThreadID:     "thread-1",
		Checkpointer: saver,
	}
	ctx := graph.WithConfig(context.Background(), cfg)

	// --- First invocation: graph halts at the interrupt ---
	initial := State{Question: "What is your favourite language?"}
	first, err := compiled.Invoke(ctx, initial)
	if err != nil {
		log.Fatalf("first invoke: %v", err)
	}

	if len(first.Interrupts) == 0 {
		log.Fatal("expected an interrupt, got none")
	}
	question := first.Interrupts[0].Value.Value()
	fmt.Printf("Graph is waiting for input: %v\n", question)

	// --- Resume: supply the user's answer via Command ---
	// Command.Resume can be a single value (matched positionally) or a
	// map[string]Dynamic keyed by interrupt ID for multi-interrupt graphs.
	resumeInput := State{} // state is restored from checkpoint; input is ignored
	resumeCtx := graph.WithConfig(ctx, graph.Config{
		ThreadID:     cfg.ThreadID,
		Checkpointer: saver,
		Metadata: map[string]any{
			graph.ConfigKeyResume: "Go",
		},
	})

	second, err := compiled.Invoke(resumeCtx, resumeInput)
	if err != nil {
		log.Fatalf("resume invoke: %v", err)
	}

	final := second.Value.(State)
	fmt.Printf("Summary: %s\n", final.Summary)
}
