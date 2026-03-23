package checkpoint_test

import (
	"context"
	"testing"
	"time"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/checkpoint"
)

func BenchmarkJSONSerializerRoundTrip(b *testing.B) {
	b.ReportAllocs()
	ser := checkpoint.JSONSerializer{}
	value := map[string]any{
		"messages": []any{"a", "b", map[string]any{"x": 1}},
		"count":    42,
		"nested":   map[string]any{"ok": true},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sv, err := ser.Serialize(value)
		if err != nil {
			b.Fatalf("Serialize() error = %v", err)
		}
		if _, err := ser.Deserialize(sv); err != nil {
			b.Fatalf("Deserialize() error = %v", err)
		}
	}
}

func BenchmarkInMemorySaverPutWrites(b *testing.B) {
	b.ReportAllocs()
	ctx := context.Background()
	s := checkpoint.NewInMemorySaverWithSerializer(checkpoint.JSONSerializer{})

	stored, err := s.Put(ctx, &checkpoint.Config{ThreadID: "bench"}, &checkpoint.Checkpoint{
		V:               1,
		ID:              "cp-bench",
		TS:              time.Now().UTC().Format(time.RFC3339Nano),
		ChannelValues:   map[string]any{"counter": 1},
		ChannelVersions: map[string]checkpoint.Version{},
		VersionsSeen:    map[string]map[string]checkpoint.Version{},
	}, &checkpoint.CheckpointMetadata{Source: "loop", Step: 0})
	if err != nil {
		b.Fatalf("Put() error = %v", err)
	}

	writes := []checkpoint.PendingWrite{
		{Channel: "messages", Value: map[string]any{"text": "hello"}},
		{Channel: "metrics", Value: map[string]any{"latency_ms": 12.5}},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.PutWrites(ctx, stored, writes, "task-bench"); err != nil {
			b.Fatalf("PutWrites() error = %v", err)
		}
	}
}
