package graph_test

import (
	"context"
	"testing"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/graph"
)

func BenchmarkPregelInvoke(b *testing.B) {
	b.ReportAllocs()
	builder := graph.NewStateGraph[map[string]any]()
	builder.AddNode("start", func(_ context.Context, _ map[string]any) (graph.NodeResult, error) {
		return graph.NodeWrites(graph.DynMap(map[string]any{"count": 1})), nil
	})
	builder.AddNode("double", func(_ context.Context, state map[string]any) (graph.NodeResult, error) {
		count, _ := state["count"].(int)
		return graph.NodeWrites(graph.DynMap(map[string]any{"count": count * 2})), nil
	})
	builder.AddEdge(graph.Start, "start")
	builder.AddEdge("start", "double")
	builder.AddEdge("double", graph.End)

	compiled, err := builder.Compile()
	if err != nil {
		b.Fatalf("Compile() error = %v", err)
	}

	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := compiled.Invoke(ctx, map[string]any{}); err != nil {
			b.Fatalf("Invoke() error = %v", err)
		}
	}
}
