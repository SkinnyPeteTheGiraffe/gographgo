package graph

import (
	"context"
	"reflect"
	"testing"
)

func TestUIMessageReducer_MergesAndDeletes(t *testing.T) {
	left := []AnyUIMessage{{
		Type:  UIMessageType,
		ID:    "ui-1",
		Name:  "panel",
		Props: map[string]any{"title": "before", "count": 1},
	}}
	right := []AnyUIMessage{{
		Type:     UIMessageType,
		ID:       "ui-1",
		Name:     "panel",
		Props:    map[string]any{"count": 2, "status": "ready"},
		Metadata: map[string]any{"merge": true},
	}, {
		Type: RemoveUIMessageType,
		ID:   "missing",
	}}

	got := UIMessageReducer(left, right)
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1", len(got))
	}
	wantProps := map[string]any{"title": "before", "count": 2, "status": "ready"}
	if !reflect.DeepEqual(got[0].Props, wantProps) {
		t.Fatalf("props = %#v, want %#v", got[0].Props, wantProps)
	}

	got = UIMessageReducer(got, []AnyUIMessage{{Type: RemoveUIMessageType, ID: "ui-1"}})
	if len(got) != 0 {
		t.Fatalf("len after delete = %d, want 0", len(got))
	}
}

func TestPushAndDeleteUIMessage_StreamAndState(t *testing.T) {
	g := NewUIStateGraph()
	g.AddNode("show", func(ctx context.Context, _ UIState) (NodeResult, error) {
		message, cmd := PushUIMessage(ctx, "banner", map[string]any{"text": "hello"}, UIPushOptions{MessageID: "msg-1", Merge: true})
		if message.Type != UIMessageType {
			t.Fatalf("message type = %q, want %q", message.Type, UIMessageType)
		}
		return NodeCommand(cmd), nil
	})
	g.AddNode("hide", func(ctx context.Context, state UIState) (NodeResult, error) {
		if len(state.UI) != 1 {
			t.Fatalf("ui len before delete = %d, want 1", len(state.UI))
		}
		_, cmd := DeleteUIMessage(ctx, state.UI[0].ID, UIDeleteOptions{})
		return NodeCommand(cmd), nil
	})
	g.AddEdge(START, "show")
	g.AddEdge("show", "hide")
	g.AddEdge("hide", END)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ctx := WithConfig(context.Background(), Config{Metadata: map[string]any{
		"run_id":   "run-1",
		"run_name": "ui-flow",
		"tags":     []string{"demo"},
	}})

	var custom []AnyUIMessage
	var final UIState
	for part := range compiled.StreamDuplex(ctx, UIState{}, StreamModeCustom, StreamModeValues) {
		if part.Err != nil {
			t.Fatalf("stream err: %v", part.Err)
		}
		switch part.Type {
		case StreamModeCustom:
			message, ok := part.Data.(AnyUIMessage)
			if !ok {
				t.Fatalf("custom payload type = %T, want AnyUIMessage", part.Data)
			}
			custom = append(custom, message)
		case StreamModeValues:
			state, ok := part.Data.(UIState)
			if ok {
				final = state
			}
		}
	}

	if len(custom) != 2 {
		t.Fatalf("custom events = %d, want 2", len(custom))
	}
	if custom[0].Metadata["message_id"] != "msg-1" {
		t.Fatalf("message_id = %#v, want msg-1", custom[0].Metadata["message_id"])
	}
	if custom[0].Metadata["run_id"] != "run-1" {
		t.Fatalf("run_id = %#v, want run-1", custom[0].Metadata["run_id"])
	}
	if custom[0].Metadata["name"] != "ui-flow" {
		t.Fatalf("name = %#v, want ui-flow", custom[0].Metadata["name"])
	}
	if merge, ok := custom[0].Metadata["merge"].(bool); !ok || !merge {
		t.Fatalf("merge metadata = %#v, want true", custom[0].Metadata["merge"])
	}
	if custom[1].Type != RemoveUIMessageType {
		t.Fatalf("delete type = %q, want %q", custom[1].Type, RemoveUIMessageType)
	}
	if len(final.UI) != 0 {
		t.Fatalf("final ui len = %d, want 0", len(final.UI))
	}
}
