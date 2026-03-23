package prebuilt_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/graph"
	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/prebuilt"
)

func TestToolNode_Run(t *testing.T) {
	node := prebuilt.NewToolNode([]prebuilt.Tool{
		prebuilt.NewToolFunc("echo", func(_ context.Context, args map[string]any) (any, error) {
			return args["text"], nil
		}),
		prebuilt.NewToolFunc("sum", func(_ context.Context, args map[string]any) (any, error) {
			a, _ := args["a"].(int)
			b, _ := args["b"].(int)
			return map[string]int{"result": a + b}, nil
		}),
	})

	got, err := node.Run(context.Background(), []prebuilt.ToolCall{
		{ID: "1", Name: "echo", Args: map[string]any{"text": "hi"}},
		{ID: "2", Name: "sum", Args: map[string]any{"a": 2, "b": 3}},
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	if got[0].Status != "ok" || got[0].Content != "hi" {
		t.Fatalf("first result = %+v", got[0])
	}
	if got[1].Status != "ok" || got[1].Content != `{"result":5}` {
		t.Fatalf("second result = %+v", got[1])
	}
}

func TestToolNode_Run_UnknownTool(t *testing.T) {
	node := prebuilt.NewToolNode([]prebuilt.Tool{
		prebuilt.NewToolFunc("echo", func(_ context.Context, args map[string]any) (any, error) {
			return args["text"], nil
		}),
	})

	got, err := node.Run(context.Background(), []prebuilt.ToolCall{{ID: "1", Name: "missing", Args: nil}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1", len(got))
	}
	if got[0].Status != "error" {
		t.Fatalf("status = %q, want error", got[0].Status)
	}
}

func TestToolNode_Run_DisableToolErrorHandling(t *testing.T) {
	node := prebuilt.NewToolNode(
		[]prebuilt.Tool{
			prebuilt.NewToolFunc("boom", func(_ context.Context, _ map[string]any) (any, error) {
				return nil, errors.New("boom")
			}),
		},
		prebuilt.WithToolErrorHandling(false),
	)

	_, err := node.Run(context.Background(), []prebuilt.ToolCall{{ID: "1", Name: "boom"}})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestToolNode_Run_DefaultErrorHandlingOnlyInvocationErrors(t *testing.T) {
	node := prebuilt.NewToolNode([]prebuilt.Tool{
		prebuilt.NewToolFunc("boom", func(_ context.Context, _ map[string]any) (any, error) {
			return nil, errors.New("boom")
		}),
	})

	_, err := node.Run(context.Background(), []prebuilt.ToolCall{{ID: "1", Name: "boom"}})
	if err == nil {
		t.Fatal("expected unhandled tool execution error")
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Fatalf("err = %v, want boom", err)
	}
}

func TestToolNode_Run_WithToolErrorHandlingTrueHandlesExecutionErrors(t *testing.T) {
	node := prebuilt.NewToolNode(
		[]prebuilt.Tool{
			prebuilt.NewToolFunc("boom", func(_ context.Context, _ map[string]any) (any, error) {
				return nil, errors.New("boom")
			}),
		},
		prebuilt.WithToolErrorHandling(true),
	)

	got, err := node.Run(context.Background(), []prebuilt.ToolCall{{ID: "1", Name: "boom", Args: map[string]any{}}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(got) != 1 || got[0].Status != "error" {
		t.Fatalf("result = %+v, want handled error message", got)
	}
	if !strings.Contains(got[0].Content, "boom") {
		t.Fatalf("content = %q, want boom", got[0].Content)
	}
}

func TestToolNode_Run_WithToolErrorMessageCatchesExecutionErrors(t *testing.T) {
	node := prebuilt.NewToolNode(
		[]prebuilt.Tool{
			prebuilt.NewToolFunc("boom", func(_ context.Context, _ map[string]any) (any, error) {
				return nil, errors.New("tool exploded")
			}),
		},
		prebuilt.WithToolErrorMessage("custom failure"),
	)

	got, err := node.Run(context.Background(), []prebuilt.ToolCall{{ID: "1", Name: "boom", Args: map[string]any{}}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(got) != 1 || got[0].Status != "error" || got[0].Content != "custom failure" {
		t.Fatalf("result = %+v, want custom handled execution error", got)
	}
}

func TestToolNode_Run_WithToolCallWrapper(t *testing.T) {
	node := prebuilt.NewToolNode(
		[]prebuilt.Tool{
			prebuilt.NewToolFunc("add", func(_ context.Context, args map[string]any) (any, error) {
				a, _ := args["a"].(int)
				b, _ := args["b"].(int)
				return a + b, nil
			}),
		},
		prebuilt.WithToolCallWrapper(func(
			req prebuilt.ToolCallRequest,
			execute prebuilt.ToolCallExecutor,
		) (prebuilt.ToolMessage, error) {
			call := req.ToolCall
			call.Args["a"] = 10
			return execute(req.Override(&call, nil, nil))
		}),
	)

	got, err := node.Run(context.Background(), []prebuilt.ToolCall{{ID: "1", Name: "add", Args: map[string]any{"a": 1, "b": 2}}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(got) != 1 || got[0].Content != "12" {
		t.Fatalf("result = %+v, want content 12", got)
	}
}

func TestToolNode_Run_UnknownToolInterceptionOrder(t *testing.T) {
	node := prebuilt.NewToolNode(
		[]prebuilt.Tool{
			prebuilt.NewToolFunc("echo", func(_ context.Context, args map[string]any) (any, error) {
				return args["text"], nil
			}),
		},
		prebuilt.WithToolCallWrapper(func(
			req prebuilt.ToolCallRequest,
			execute prebuilt.ToolCallExecutor,
		) (prebuilt.ToolMessage, error) {
			if req.Tool == nil {
				return prebuilt.ToolMessage{
					ToolCallID: req.ToolCall.ID,
					Name:       req.ToolCall.Name,
					Status:     "ok",
					Content:    "intercepted",
				}, nil
			}
			return execute(req)
		}),
	)

	got, err := node.Run(context.Background(), []prebuilt.ToolCall{{ID: "1", Name: "missing"}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(got) != 1 || got[0].Content != "intercepted" {
		t.Fatalf("result = %+v, want intercepted", got)
	}

	passthrough := prebuilt.NewToolNode(
		[]prebuilt.Tool{prebuilt.NewToolFunc("echo", func(_ context.Context, _ map[string]any) (any, error) { return "ok", nil })},
		prebuilt.WithToolErrorHandling(false),
		prebuilt.WithToolCallWrapper(func(req prebuilt.ToolCallRequest, execute prebuilt.ToolCallExecutor) (prebuilt.ToolMessage, error) {
			return execute(req)
		}),
	)
	got, err = passthrough.Run(context.Background(), []prebuilt.ToolCall{{ID: "2", Name: "unknown"}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(got) != 1 || got[0].Status != "error" {
		t.Fatalf("result = %+v, want unknown tool error message", got)
	}
}

func TestToolNode_RunResults_CommandOutputs(t *testing.T) {
	node := prebuilt.NewToolNode([]prebuilt.Tool{
		prebuilt.NewToolFunc("echo", func(_ context.Context, args map[string]any) (any, error) {
			return args["text"], nil
		}),
		prebuilt.NewToolFuncWithRuntime("handoff", func(_ context.Context, _ map[string]any, runtime prebuilt.ToolRuntime) (any, error) {
			return prebuilt.ToolCommand{
				Update: map[string]any{
					"messages": []prebuilt.Message{{Role: "tool", ToolCallID: runtime.ToolCallID, Content: "handoff ready"}},
				},
				Goto: graph.RouteTo("next"),
			}, nil
		}),
	})

	results, err := node.RunResults(context.Background(), []prebuilt.ToolCall{
		{ID: "1", Name: "echo", Args: map[string]any{"text": "hello"}},
		{ID: "2", Name: "handoff", Args: map[string]any{}},
	})
	if err != nil {
		t.Fatalf("RunResults: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("len = %d, want 2", len(results))
	}
	if results[0].Message == nil || results[0].Message.Content != "hello" {
		t.Fatalf("first result = %#v", results[0])
	}
	if results[1].Command == nil {
		t.Fatalf("second result = %#v, want command", results[1])
	}
	messages, ok := results[1].Command.Update["messages"]
	if !ok {
		t.Fatalf("command update = %#v, want messages", results[1].Command.Update)
	}
	parsed, ok := messages.Value().([]prebuilt.Message)
	if !ok || len(parsed) != 1 {
		t.Fatalf("parsed command messages = %#v", messages.Value())
	}
	if parsed[0].Name != "handoff" {
		t.Fatalf("message name = %q, want handoff", parsed[0].Name)
	}

	_, err = node.Run(context.Background(), []prebuilt.ToolCall{{ID: "2", Name: "handoff"}})
	if err == nil || !strings.Contains(err.Error(), "RunWithState does not support command outputs") {
		t.Fatalf("Run err = %v, want command output guidance", err)
	}
}

func TestToolNode_RunResults_CommandValidationRequiresMatchingToolMessage(t *testing.T) {
	node := prebuilt.NewToolNode([]prebuilt.Tool{
		prebuilt.NewToolFunc("bad_command", func(_ context.Context, _ map[string]any) (any, error) {
			return prebuilt.ToolCommand{Update: map[string]any{"messages": []prebuilt.Message{{Role: "tool", ToolCallID: "different", Content: "oops"}}}}, nil
		}),
	})

	_, err := node.RunResults(context.Background(), []prebuilt.ToolCall{{ID: "1", Name: "bad_command"}})
	if err == nil {
		t.Fatal("expected command validation error")
	}
	if !strings.Contains(err.Error(), "matching tool message") {
		t.Fatalf("err = %v, want matching tool message error", err)
	}
}

func TestToolNode_Run_WithErrorFilter(t *testing.T) {
	node := prebuilt.NewToolNode(
		[]prebuilt.Tool{
			prebuilt.NewToolFunc("boom", func(_ context.Context, _ map[string]any) (any, error) {
				return nil, context.Canceled
			}),
		},
		prebuilt.WithToolErrorFilter(func(err error) bool {
			return !errors.Is(err, context.Canceled)
		}),
	)

	_, err := node.Run(context.Background(), []prebuilt.ToolCall{{ID: "1", Name: "boom"}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestToolNode_Run_WithErrorMessageAndTypeFilter(t *testing.T) {
	node := prebuilt.NewToolNode(
		[]prebuilt.Tool{
			prebuilt.NewToolFunc("typed", func(_ context.Context, _ map[string]any) (any, error) {
				return nil, handledError{msg: "handled"}
			}),
		},
		prebuilt.WithToolHandledErrorTypes(handledError{}),
		prebuilt.WithToolErrorMessage("custom failure"),
	)

	got, err := node.Run(context.Background(), []prebuilt.ToolCall{{ID: "1", Name: "typed"}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(got) != 1 || got[0].Status != "error" || got[0].Content != "custom failure" {
		t.Fatalf("result = %+v, want custom handled error", got)
	}
}

func TestToolNode_Run_StateStoreRuntimeInjection(t *testing.T) {
	store := &testStore{}
	node := prebuilt.NewToolNode([]prebuilt.Tool{
		prebuilt.NewToolFuncWithRuntime("runtime", func(_ context.Context, args map[string]any, runtime prebuilt.ToolRuntime) (any, error) {
			if runtime.ToolCallID != "runtime-1" {
				return nil, fmt.Errorf("tool_call_id = %q", runtime.ToolCallID)
			}
			if runtime.State != "state-value" {
				return nil, fmt.Errorf("state = %v", runtime.State)
			}
			if runtime.Store == nil {
				return nil, errors.New("store missing")
			}
			if _, ok := args["x"]; !ok {
				return nil, errors.New("args missing")
			}
			return "ok", nil
		}),
	})

	ctx := graph.WithStore(context.Background(), store)
	got, err := node.RunWithState(ctx, []prebuilt.ToolCall{{ID: "runtime-1", Name: "runtime", Args: map[string]any{"x": 1}}}, "state-value")
	if err != nil {
		t.Fatalf("RunWithState: %v", err)
	}
	if len(got) != 1 || got[0].Status != "ok" || got[0].Content != "ok" {
		t.Fatalf("result = %+v", got)
	}
}

func TestToolNode_Run_ContentBlocksOutput(t *testing.T) {
	node := prebuilt.NewToolNode([]prebuilt.Tool{
		prebuilt.NewToolFunc("blocks", func(_ context.Context, _ map[string]any) (any, error) {
			return []any{
				map[string]any{"type": "text", "text": "first"},
				map[string]any{"type": "image_url", "image_url": map[string]any{"url": "https://example.com/image.png"}},
			}, nil
		}),
	})

	got, err := node.Run(context.Background(), []prebuilt.ToolCall{{ID: "1", Name: "blocks"}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1", len(got))
	}
	if got[0].Content != "" {
		t.Fatalf("content = %q, want empty", got[0].Content)
	}
	if len(got[0].ContentBlocks) != 2 {
		t.Fatalf("content blocks = %d, want 2", len(got[0].ContentBlocks))
	}
}

func TestToolNode_Run_FiltersInjectedValidationErrors(t *testing.T) {
	node := prebuilt.NewToolNode(
		[]prebuilt.Tool{
			prebuilt.NewToolFunc("validate", func(_ context.Context, _ map[string]any) (any, error) {
				return "unreachable", nil
			}),
		},
		prebuilt.WithToolArgumentValidators(map[string]prebuilt.ToolArgsValidator{
			"validate": func(_ map[string]any) error {
				return testValidationError{issues: []prebuilt.ToolArgValidationIssue{
					{Field: prebuilt.InjectedStateArg, Message: "internal problem"},
					{Field: "user.name", Message: "required"},
				}}
			},
		}),
	)

	got, err := node.RunWithState(context.Background(), []prebuilt.ToolCall{{ID: "1", Name: "validate", Args: map[string]any{"user": map[string]any{}}}}, map[string]any{"messages": []any{}})
	if err != nil {
		t.Fatalf("RunWithState: %v", err)
	}
	if len(got) != 1 || got[0].Status != "error" {
		t.Fatalf("result = %+v", got)
	}
	if got[0].Content == "" || strings.Contains(got[0].Content, prebuilt.InjectedStateArg) {
		t.Fatalf("content = %q, should include only user-facing validation detail", got[0].Content)
	}
}

func TestToolNode_Run_FiltersInjectedValidationErrorsFromInvoke(t *testing.T) {
	node := prebuilt.NewToolNode(
		[]prebuilt.Tool{
			prebuilt.NewToolFunc("validate", func(_ context.Context, _ map[string]any) (any, error) {
				return nil, testValidationError{issues: []prebuilt.ToolArgValidationIssue{
					{Field: prebuilt.InjectedRuntimeArg, Message: "invalid runtime"},
					{Field: "query", Message: "must not be empty"},
				}}
			}),
		},
	)

	got, err := node.Run(context.Background(), []prebuilt.ToolCall{{ID: "1", Name: "validate", Args: map[string]any{"query": ""}}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(got) != 1 || got[0].Status != "error" {
		t.Fatalf("result = %+v", got)
	}
	if strings.Contains(got[0].Content, prebuilt.InjectedRuntimeArg) {
		t.Fatalf("content = %q should not include injected runtime errors", got[0].Content)
	}
}

func TestToolNode_Run_BubblesGraphBubbleUpErrors(t *testing.T) {
	node := prebuilt.NewToolNode([]prebuilt.Tool{
		prebuilt.NewToolFunc("bubble", func(_ context.Context, _ map[string]any) (any, error) {
			return nil, &graph.GraphBubbleUp{Message: "bubble"}
		}),
	})

	_, err := node.Run(context.Background(), []prebuilt.ToolCall{{ID: "1", Name: "bubble"}})
	if err == nil {
		t.Fatal("expected bubble-up error")
	}
	var bubble *graph.GraphBubbleUp
	if !errors.As(err, &bubble) {
		t.Fatalf("err type = %T, want *graph.GraphBubbleUp", err)
	}
}

func TestToolNode_Run_RepanicsInterruptSignal(t *testing.T) {
	node := prebuilt.NewToolNode([]prebuilt.Tool{
		prebuilt.NewToolFunc("interrupt", func(ctx context.Context, _ map[string]any) (any, error) {
			_ = graph.NodeInterrupt(ctx, graph.Dyn("approve"))
			return nil, nil
		}),
	})

	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	_, _ = node.Run(context.Background(), []prebuilt.ToolCall{{ID: "1", Name: "interrupt"}})
}

type handledError struct{ msg string }

func (e handledError) Error() string { return e.msg }

type testValidationError struct {
	issues []prebuilt.ToolArgValidationIssue
}

func (e testValidationError) Error() string { return "validation failed" }

func (e testValidationError) ValidationIssues() []prebuilt.ToolArgValidationIssue { return e.issues }
