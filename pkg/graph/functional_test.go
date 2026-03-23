package graph

import (
	"context"
	"testing"
	"time"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/checkpoint"
)

func TestTask_Composition(t *testing.T) {
	addOne := NewTask("add_one", func(ctx context.Context, input int) (int, error) {
		return input + 1, nil
	})
	double := NewTask("double", func(ctx context.Context, input int) (int, error) {
		return input * 2, nil
	})

	v1, err := addOne.Invoke(context.Background(), 20)
	if err != nil {
		t.Fatalf("add_one invoke: %v", err)
	}
	v2, err := double.Invoke(context.Background(), v1)
	if err != nil {
		t.Fatalf("double invoke: %v", err)
	}
	if v2 != 42 {
		t.Fatalf("result = %d, want 42", v2)
	}
}

func TestTask_AsyncRunsInParallel(t *testing.T) {
	slow := NewTask("slow", func(ctx context.Context, input int) (int, error) {
		time.Sleep(80 * time.Millisecond)
		return input * 2, nil
	})

	start := time.Now()
	f1 := slow.Async(context.Background(), 10)
	f2 := slow.Async(context.Background(), 11)

	v1, err := f1.Await()
	if err != nil {
		t.Fatalf("await first: %v", err)
	}
	v2, err := f2.Await()
	if err != nil {
		t.Fatalf("await second: %v", err)
	}

	if v1 != 20 || v2 != 22 {
		t.Fatalf("values = (%d, %d), want (20, 22)", v1, v2)
	}
	if elapsed := time.Since(start); elapsed >= 150*time.Millisecond {
		t.Fatalf("elapsed = %s, want < 150ms (parallel execution)", elapsed)
	}
}

func TestEntrypoint_InvokeSuccess(t *testing.T) {
	addOne := NewTask("add_one", func(ctx context.Context, input int) (int, error) {
		return input + 1, nil
	})
	double := NewTask("double", func(ctx context.Context, input int) (int, error) {
		return input * 2, nil
	})

	workflow := NewEntrypoint("math", func(ctx context.Context, input int) (EntrypointResult[int], error) {
		n, err := addOne.Invoke(ctx, input)
		if err != nil {
			return EntrypointResult[int]{}, err
		}
		n, err = double.Invoke(ctx, n)
		if err != nil {
			return EntrypointResult[int]{}, err
		}
		return Return(n), nil
	})

	out, err := workflow.Invoke(context.Background(), 20)
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	v, ok := out.Value.(int)
	if !ok {
		t.Fatalf("output type = %T, want int", out.Value)
	}
	if v != 42 {
		t.Fatalf("value = %d, want 42", v)
	}
}

func TestEntrypoint_ReturnWithSavePersistsAndLoadsPrevious(t *testing.T) {
	saver := checkpoint.NewInMemorySaver()
	workflow := NewEntrypoint("save_split", func(ctx context.Context, input string) (EntrypointResult[string], error) {
		if input == "first" {
			return ReturnWithSave("visible:first", map[string]any{"persisted": "checkpoint-first"}), nil
		}
		prev, _ := GetEntrypointPrevious(ctx).(map[string]any)
		return Return("visible:" + prev["persisted"].(string)), nil
	}, EntrypointOptions{Checkpointer: saver})

	ctx := WithConfig(context.Background(), Config{ThreadID: "entrypoint-save", Checkpointer: saver})

	first, err := workflow.Invoke(ctx, "first")
	if err != nil {
		t.Fatalf("first invoke: %v", err)
	}
	if first.Value != "visible:first" {
		t.Fatalf("first value = %v, want visible:first", first.Value)
	}

	tuple, err := saver.GetTuple(context.Background(), (&Config{ThreadID: "entrypoint-save"}).CheckpointConfig())
	if err != nil {
		t.Fatalf("get tuple: %v", err)
	}
	if tuple == nil || tuple.Checkpoint == nil {
		t.Fatal("expected persisted checkpoint")
	}
	saved, ok := tuple.Checkpoint.ChannelValues[entrypointSaveChannel].(map[string]any)
	if !ok {
		t.Fatalf("saved value type = %T, want map[string]any", tuple.Checkpoint.ChannelValues[entrypointSaveChannel])
	}
	if saved["persisted"] != "checkpoint-first" {
		t.Fatalf("saved persisted = %v, want checkpoint-first", saved["persisted"])
	}

	second, err := workflow.Invoke(ctx, "second")
	if err != nil {
		t.Fatalf("second invoke: %v", err)
	}
	if second.Value != "visible:checkpoint-first" {
		t.Fatalf("second value = %v, want visible:checkpoint-first", second.Value)
	}
}

func TestEntrypoint_StreamEmitsTaskAndFinalValue(t *testing.T) {
	addOne := NewTask("add_one", func(ctx context.Context, input int) (int, error) {
		return input + 1, nil
	})
	workflow := NewEntrypoint("streaming", func(ctx context.Context, input int) (EntrypointResult[int], error) {
		n, err := addOne.Invoke(ctx, input)
		if err != nil {
			return EntrypointResult[int]{}, err
		}
		return Return(n), nil
	})

	stream := workflow.StreamDuplex(context.Background(), 41, StreamModeTasks, StreamModeValues)
	seenStart := false
	seenFinish := false
	seenFinal := false

	for part := range stream {
		if part.Err != nil {
			t.Fatalf("stream error: %v", part.Err)
		}
		if part.Type == StreamModeTasks {
			payload, _ := part.Data.(map[string]any)
			if payload["name"] == "add_one" && payload["event"] == "start" {
				seenStart = true
			}
			if payload["name"] == "add_one" && payload["event"] == "finish" {
				seenFinish = true
			}
		}
		if part.Type == StreamModeValues && part.Data == 42 {
			seenFinal = true
		}
	}

	if !seenStart {
		t.Fatal("expected task start event")
	}
	if !seenFinish {
		t.Fatal("expected task finish event")
	}
	if !seenFinal {
		t.Fatal("expected final values event")
	}
}
