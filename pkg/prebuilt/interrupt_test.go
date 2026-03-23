package prebuilt_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/prebuilt"
)

func TestBuildToolInterrupt(t *testing.T) {
	interrupt := prebuilt.BuildToolInterrupt(
		prebuilt.ToolCall{ID: "call-1", Name: "run", Args: map[string]any{"cmd": "ls"}},
		prebuilt.HumanInterruptConfig{AllowAccept: true, AllowIgnore: true},
		"approve command",
	)
	if interrupt.ID == "" {
		t.Fatal("interrupt ID should not be empty")
	}
	payload, ok := interrupt.Value.Value().(prebuilt.HumanInterrupt)
	if !ok {
		t.Fatalf("payload type = %T, want HumanInterrupt", interrupt.Value.Value())
	}
	if payload.ActionRequest.Action != "run" {
		t.Fatalf("action = %q, want run", payload.ActionRequest.Action)
	}
}

func TestHumanResponseJSONRoundTrip(t *testing.T) {
	t.Run("respond", func(t *testing.T) {
		input := prebuilt.RespondHumanResponse("looks good")
		encoded, err := json.Marshal(input)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		var decoded prebuilt.HumanResponse
		if err := json.Unmarshal(encoded, &decoded); err != nil {
			t.Fatalf("Unmarshal: %v", err)
		}
		text, ok := decoded.Args.(prebuilt.HumanResponseText)
		if !ok || string(text) != "looks good" {
			t.Fatalf("decoded args = %#v, want HumanResponseText(looks good)", decoded.Args)
		}
	})

	t.Run("edit", func(t *testing.T) {
		input := prebuilt.EditHumanResponse(prebuilt.ActionRequest{
			Action: "calc",
			Args:   map[string]any{"a": 2, "b": 3},
		})
		encoded, err := json.Marshal(input)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		var decoded prebuilt.HumanResponse
		if err := json.Unmarshal(encoded, &decoded); err != nil {
			t.Fatalf("Unmarshal: %v", err)
		}
		action, ok := decoded.Args.(prebuilt.HumanResponseActionRequest)
		if !ok || action.ActionRequest.Action != "calc" {
			t.Fatalf("decoded args = %#v, want HumanResponseActionRequest(calc)", decoded.Args)
		}
	})
}

func TestInterrupt_PanicsWithoutResume(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic")
		}
	}()
	_ = prebuilt.Interrupt(context.Background(), "approve")
}
