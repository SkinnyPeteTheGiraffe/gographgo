package prebuilt_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/checkpoint"
	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/graph"
	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/prebuilt"
)

type scriptedModel struct {
	responses          []prebuilt.ModelResponse
	structuredResponse any
	inputs             [][]prebuilt.Message
	mu                 sync.Mutex
	structuredCalls    int
	idx                int
}

func (m *scriptedModel) Generate(_ context.Context, messages []prebuilt.Message) (prebuilt.ModelResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.idx >= len(m.responses) {
		return prebuilt.ModelResponse{}, errors.New("no scripted response")
	}
	m.inputs = append(m.inputs, cloneMessages(messages))
	r := m.responses[m.idx]
	m.idx++
	return r, nil
}

func (m *scriptedModel) GenerateStructured(
	_ context.Context,
	_ []prebuilt.Message,
	_ any,
) (any, error) {
	m.mu.Lock()
	m.structuredCalls++
	m.mu.Unlock()
	if m.structuredResponse == nil {
		return nil, errors.New("no structured response configured")
	}
	return m.structuredResponse, nil
}

func TestReactAgent_StrictChatHistoryValidation(t *testing.T) {
	model := &scriptedModel{responses: []prebuilt.ModelResponse{{Content: "done"}}}
	agent := prebuilt.CreateReactAgent(model, nil)

	_, err := agent.Invoke(context.Background(), prebuilt.AgentState{Messages: []prebuilt.Message{
		{Role: "user", Content: "hi"},
		{Role: "assistant", Content: "calling tool", ToolCalls: []prebuilt.ToolCall{{ID: "call-1", Name: "echo", Args: map[string]any{"text": "x"}}}},
	}})
	if err == nil {
		t.Fatal("expected invalid chat history error")
	}
	if !strings.Contains(err.Error(), "tool call") {
		t.Fatalf("err = %v, want tool call validation message", err)
	}
	if model.idx != 0 {
		t.Fatalf("model generate calls = %d, want 0", model.idx)
	}
}

type promptRunnable struct {
	messages []prebuilt.Message
}

func (p promptRunnable) Prompt(
	_ context.Context,
	_ prebuilt.AgentState,
	_ prebuilt.AgentRuntime,
) ([]prebuilt.Message, error) {
	return cloneMessages(p.messages), nil
}

type selectorModel struct {
	name string
}

func (m *selectorModel) Generate(_ context.Context, _ []prebuilt.Message) (prebuilt.ModelResponse, error) {
	return prebuilt.ModelResponse{Content: m.name}, nil
}

type boundModel struct {
	responses []prebuilt.ModelResponse
	bound     []string
	idx       int
}

func (m *boundModel) Generate(_ context.Context, _ []prebuilt.Message) (prebuilt.ModelResponse, error) {
	if m.idx >= len(m.responses) {
		return prebuilt.ModelResponse{}, errors.New("no scripted response")
	}
	r := m.responses[m.idx]
	m.idx++
	return r, nil
}

func (m *boundModel) BoundToolNames() []string {
	out := make([]string, len(m.bound))
	copy(out, m.bound)
	return out
}

type testStore struct{}

func (testStore) Get(_ []string, _ string) (value any, ok bool, err error) { return nil, false, nil }
func (testStore) Set(_ []string, _ string, _ any) error                    { return nil }
func (testStore) Delete(_ []string, _ string) error                        { return nil }
func (testStore) List(_ []string, _ string) ([]string, error)              { return nil, nil }
func (testStore) Search(_ []string, _ string, _ int) ([]string, error) {
	return nil, nil
}

type customState struct {
	Structured any
	History    []prebuilt.Message
	Steps      int
}

type customStateSchema struct{}

func (customStateSchema) DecodeState(state any) (prebuilt.AgentState, error) {
	s, ok := state.(customState)
	if !ok {
		return prebuilt.AgentState{}, fmt.Errorf("unexpected custom state type %T", state)
	}
	return prebuilt.AgentState{
		Messages:           cloneMessages(s.History),
		RemainingSteps:     s.Steps,
		StructuredResponse: s.Structured,
	}, nil
}

func (customStateSchema) EncodeState(state prebuilt.AgentState) (any, error) {
	return customState{
		History:    cloneMessages(state.Messages),
		Steps:      state.RemainingSteps,
		Structured: state.StructuredResponse,
	}, nil
}

func TestReactAgent_InvokeToolLoop(t *testing.T) {
	model := &scriptedModel{responses: []prebuilt.ModelResponse{
		{Content: "let me call tools", ToolCalls: []prebuilt.ToolCall{{ID: "1", Name: "echo", Args: map[string]any{"text": "hi"}}}},
		{Content: "done"},
	}}

	agent := prebuilt.CreateReactAgent(model, []prebuilt.Tool{
		prebuilt.NewToolFunc("echo", func(_ context.Context, args map[string]any) (any, error) {
			return fmt.Sprintf("echo:%v", args["text"]), nil
		}),
	})

	result, err := agent.Invoke(context.Background(), prebuilt.AgentState{
		Messages: []prebuilt.Message{{Role: "user", Content: "say hi"}},
	})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if len(result.Interrupts) != 0 {
		t.Fatalf("interrupts len = %d, want 0", len(result.Interrupts))
	}
	if len(result.State.Messages) != 4 {
		t.Fatalf("messages len = %d, want 4", len(result.State.Messages))
	}
	if result.State.Messages[2].Role != "tool" || result.State.Messages[2].Content != "echo:hi" {
		t.Fatalf("tool message = %+v", result.State.Messages[2])
	}
}

func TestReactAgent_InvokeInterruptAndResume(t *testing.T) {
	model := &scriptedModel{responses: []prebuilt.ModelResponse{
		{Content: "call tool", ToolCalls: []prebuilt.ToolCall{{ID: "1", Name: "calc", Args: map[string]any{"a": 2, "b": 3}}}},
		{Content: "final"},
	}}

	agent := prebuilt.CreateReactAgent(
		model,
		[]prebuilt.Tool{
			prebuilt.NewToolFunc("calc", func(_ context.Context, args map[string]any) (any, error) {
				a, _ := args["a"].(int)
				b, _ := args["b"].(int)
				return a + b, nil
			}),
		},
		prebuilt.WithInterruptBeforeTools(prebuilt.HumanInterruptConfig{AllowAccept: true, AllowEdit: true}, "approve tool call"),
	)

	first, err := agent.Invoke(context.Background(), prebuilt.AgentState{Messages: []prebuilt.Message{{Role: "user", Content: "run calc"}}})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if len(first.Interrupts) != 1 {
		t.Fatalf("interrupts len = %d, want 1", len(first.Interrupts))
	}
	if first.Pending == nil {
		t.Fatal("expected pending calls")
	}

	second, err := agent.Resume(context.Background(), first.Pending, map[string]prebuilt.HumanResponse{
		"1": {Type: prebuilt.HumanResponseAccept},
	})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if len(second.State.Messages) != 4 {
		t.Fatalf("messages len = %d, want 4", len(second.State.Messages))
	}
	if second.State.Messages[2].Role != "tool" || second.State.Messages[2].Content != "5" {
		t.Fatalf("tool message = %+v", second.State.Messages[2])
	}
}

func TestReactAgent_ToolNodeOptionsReachCreateReactAgent(t *testing.T) {
	model := &scriptedModel{responses: []prebuilt.ModelResponse{
		{Content: "call tool", ToolCalls: []prebuilt.ToolCall{{ID: "1", Name: "echo", Args: map[string]any{"text": "hi"}}}},
		{Content: "done"},
	}}

	wrapped := false
	agent := prebuilt.CreateReactAgent(
		model,
		[]prebuilt.Tool{
			prebuilt.NewToolFunc("echo", func(_ context.Context, args map[string]any) (any, error) {
				return args["text"], nil
			}),
		},
		prebuilt.WithAgentToolNodeOptions(prebuilt.WithToolCallWrapper(func(req prebuilt.ToolCallRequest, execute prebuilt.ToolCallExecutor) (prebuilt.ToolMessage, error) {
			wrapped = true
			return execute(req)
		})),
	)

	result, err := agent.Invoke(context.Background(), prebuilt.AgentState{
		Messages: []prebuilt.Message{{Role: "user", Content: "say hi"}},
	})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if !wrapped {
		t.Fatal("expected tool call wrapper to run")
	}
	if result.State.Messages[2].Content != "hi" {
		t.Fatalf("tool message = %+v", result.State.Messages[2])
	}
}

func TestReactAgent_ResumeWithTypedHumanResponses(t *testing.T) {
	t.Run("respond", func(t *testing.T) {
		model := &scriptedModel{responses: []prebuilt.ModelResponse{
			{Content: "call tool", ToolCalls: []prebuilt.ToolCall{{ID: "1", Name: "echo", Args: map[string]any{"text": "model"}}}},
			{Content: "final"},
		}}

		agent := prebuilt.CreateReactAgent(
			model,
			[]prebuilt.Tool{
				prebuilt.NewToolFunc("echo", func(_ context.Context, args map[string]any) (any, error) {
					return args["text"], nil
				}),
			},
			prebuilt.WithInterruptBeforeTools(prebuilt.HumanInterruptConfig{AllowRespond: true}, "respond"),
		)

		first, err := agent.Invoke(context.Background(), prebuilt.AgentState{Messages: []prebuilt.Message{{Role: "user", Content: "hi"}}})
		if err != nil {
			t.Fatalf("Invoke: %v", err)
		}
		second, err := agent.Resume(context.Background(), first.Pending, map[string]prebuilt.HumanResponse{
			"1": prebuilt.RespondHumanResponse("manual"),
		})
		if err != nil {
			t.Fatalf("Resume: %v", err)
		}
		if second.State.Messages[2].Content != "manual" {
			t.Fatalf("tool content = %q, want manual", second.State.Messages[2].Content)
		}
	})

	t.Run("edit", func(t *testing.T) {
		model := &scriptedModel{responses: []prebuilt.ModelResponse{
			{Content: "call tool", ToolCalls: []prebuilt.ToolCall{{ID: "1", Name: "calc", Args: map[string]any{"a": 2, "b": 3}}}},
			{Content: "final"},
		}}

		agent := prebuilt.CreateReactAgent(
			model,
			[]prebuilt.Tool{
				prebuilt.NewToolFunc("calc", func(_ context.Context, args map[string]any) (any, error) {
					a, _ := args["a"].(int)
					b, _ := args["b"].(int)
					return a + b, nil
				}),
			},
			prebuilt.WithInterruptBeforeTools(prebuilt.HumanInterruptConfig{AllowEdit: true}, "edit"),
		)

		first, err := agent.Invoke(context.Background(), prebuilt.AgentState{Messages: []prebuilt.Message{{Role: "user", Content: "hi"}}})
		if err != nil {
			t.Fatalf("Invoke: %v", err)
		}
		second, err := agent.Resume(context.Background(), first.Pending, map[string]prebuilt.HumanResponse{
			"1": prebuilt.EditHumanResponse(prebuilt.ActionRequest{
				Action: "calc",
				Args:   map[string]any{"a": 4, "b": 1},
			}),
		})
		if err != nil {
			t.Fatalf("Resume: %v", err)
		}
		if second.State.Messages[2].Content != "5" {
			t.Fatalf("tool content = %q, want 5", second.State.Messages[2].Content)
		}
	})
}

func TestReactAgent_ValidationRepromptCycle(t *testing.T) {
	model := &scriptedModel{responses: []prebuilt.ModelResponse{
		{Content: "bad call", ToolCalls: []prebuilt.ToolCall{{ID: "1", Name: "sum", Args: map[string]any{"a": "nope"}}}},
		{Content: "good call", ToolCalls: []prebuilt.ToolCall{{ID: "2", Name: "sum", Args: map[string]any{"a": 1, "b": 2}}}},
		{Content: "done"},
	}}

	validator := prebuilt.NewValidationNode(map[string]prebuilt.ToolArgsValidator{
		"sum": func(args map[string]any) error {
			if _, ok := args["a"].(int); !ok {
				return fmt.Errorf("a must be int")
			}
			if _, ok := args["b"].(int); !ok {
				return fmt.Errorf("b must be int")
			}
			return nil
		},
	}, nil)

	agent := prebuilt.CreateReactAgent(
		model,
		[]prebuilt.Tool{
			prebuilt.NewToolFunc("sum", func(_ context.Context, args map[string]any) (any, error) {
				a, _ := args["a"].(int)
				b, _ := args["b"].(int)
				return a + b, nil
			}),
		},
		prebuilt.WithAgentValidation(validator),
	)

	result, err := agent.Invoke(context.Background(), prebuilt.AgentState{Messages: []prebuilt.Message{{Role: "user", Content: "sum"}}})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if len(result.State.Messages) < 6 {
		t.Fatalf("messages len = %d, want at least 6", len(result.State.Messages))
	}
	if result.State.Messages[2].Role != "tool" {
		t.Fatalf("expected validation tool-style message, got %+v", result.State.Messages[2])
	}
}

func TestReactAgent_ReturnDirectStopsLoop(t *testing.T) {
	model := &scriptedModel{responses: []prebuilt.ModelResponse{
		{Content: "call now", ToolCalls: []prebuilt.ToolCall{{ID: "1", Name: "direct", Args: map[string]any{"x": 1}}}},
		{Content: "should not be used"},
	}}

	agent := prebuilt.CreateReactAgent(model, []prebuilt.Tool{
		prebuilt.NewToolFuncWithReturnDirect("direct", true, func(_ context.Context, args map[string]any) (any, error) {
			return fmt.Sprintf("direct:%v", args["x"]), nil
		}),
	})

	result, err := agent.Invoke(context.Background(), prebuilt.AgentState{
		Messages:       []prebuilt.Message{{Role: "user", Content: "go"}},
		RemainingSteps: 5,
	})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if len(result.State.Messages) != 3 {
		t.Fatalf("messages len = %d, want 3", len(result.State.Messages))
	}
	if model.idx != 1 {
		t.Fatalf("model calls = %d, want 1", model.idx)
	}
}

func TestReactAgent_DefaultRemainingStepsAllowsReturnDirectExecution(t *testing.T) {
	model := &scriptedModel{responses: []prebuilt.ModelResponse{{
		Content:   "direct call",
		ToolCalls: []prebuilt.ToolCall{{ID: "1", Name: "direct", Args: map[string]any{"x": 1}}},
	}}}

	invocations := 0
	agent := prebuilt.CreateReactAgent(model, []prebuilt.Tool{
		prebuilt.NewToolFuncWithReturnDirect("direct", true, func(_ context.Context, _ map[string]any) (any, error) {
			invocations++
			return "direct", nil
		}),
	})

	result, err := agent.Invoke(context.Background(), prebuilt.AgentState{
		Messages: []prebuilt.Message{{Role: "user", Content: "go"}},
	})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if invocations != 1 {
		t.Fatalf("tool invocations = %d, want 1", invocations)
	}
	if len(result.State.Messages) != 3 {
		t.Fatalf("messages len = %d, want 3", len(result.State.Messages))
	}
	if result.State.Messages[2].Role != "tool" || result.State.Messages[2].Content != "direct" {
		t.Fatalf("tool message = %+v", result.State.Messages[2])
	}
}

func TestReactAgent_V2ExecutesToolCallsConcurrentlyWithStableOutputOrder(t *testing.T) {
	model := &scriptedModel{responses: []prebuilt.ModelResponse{
		{Content: "run fanout", ToolCalls: []prebuilt.ToolCall{
			{ID: "1", Name: "counter", Args: map[string]any{"label": "slow"}},
			{ID: "2", Name: "counter", Args: map[string]any{"label": "fast"}},
		}},
		{Content: "done"},
	}}

	slowStarted := make(chan struct{}, 1)
	fastStarted := make(chan struct{}, 1)
	releaseSlow := make(chan struct{})
	release := func() {
		select {
		case <-releaseSlow:
			return
		default:
			close(releaseSlow)
		}
	}
	defer release()

	agent := prebuilt.CreateReactAgent(
		model,
		[]prebuilt.Tool{
			prebuilt.NewToolFunc("counter", func(ctx context.Context, args map[string]any) (any, error) {
				label, _ := args["label"].(string)
				switch label {
				case "slow":
					slowStarted <- struct{}{}
					select {
					case <-releaseSlow:
						return "ok:slow", nil
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				case "fast":
					fastStarted <- struct{}{}
					return "ok:fast", nil
				default:
					return "ok:" + label, nil
				}
			}),
		},
		prebuilt.WithAgentVersion(prebuilt.ReactAgentVersionV2),
	)

	testCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	type invokeResult struct {
		err    error
		result prebuilt.AgentResult
	}
	resultCh := make(chan invokeResult, 1)
	go func() {
		result, err := agent.Invoke(testCtx, prebuilt.AgentState{Messages: []prebuilt.Message{{Role: "user", Content: "fanout"}}})
		resultCh <- invokeResult{result: result, err: err}
	}()

	select {
	case <-slowStarted:
	case <-testCtx.Done():
		t.Fatalf("timed out waiting for slow tool start: %v", testCtx.Err())
	}

	select {
	case <-fastStarted:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("fast tool did not start while slow tool was still running")
	}

	release()
	out := <-resultCh
	if out.err != nil {
		t.Fatalf("Invoke: %v", out.err)
	}
	if len(out.result.State.Messages) != 5 {
		t.Fatalf("messages len = %d, want 5", len(out.result.State.Messages))
	}
	if out.result.State.Messages[2].Content != "ok:slow" || out.result.State.Messages[3].Content != "ok:fast" {
		t.Fatalf("tool message ordering = %+v %+v", out.result.State.Messages[2], out.result.State.Messages[3])
	}
}

func TestReactAgent_ReturnDirectSkipsStructuredResponseGeneration(t *testing.T) {
	model := &scriptedModel{
		responses: []prebuilt.ModelResponse{{
			Content:   "call direct",
			ToolCalls: []prebuilt.ToolCall{{ID: "1", Name: "direct", Args: map[string]any{"x": 1}}},
		}},
		structuredResponse: map[string]any{"answer": "should not run"},
	}

	agent := prebuilt.CreateReactAgent(
		model,
		[]prebuilt.Tool{prebuilt.NewToolFuncWithReturnDirect("direct", true, func(_ context.Context, _ map[string]any) (any, error) {
			return "direct-result", nil
		})},
		prebuilt.WithAgentResponseFormat(prebuilt.AgentResponseFormat{Schema: map[string]any{"type": "object"}}),
	)

	result, err := agent.Invoke(context.Background(), prebuilt.AgentState{
		Messages:       []prebuilt.Message{{Role: "user", Content: "go"}},
		RemainingSteps: 5,
	})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if result.StructuredResponse != nil {
		t.Fatalf("structured response = %#v, want nil", result.StructuredResponse)
	}
	if result.State.StructuredResponse != nil {
		t.Fatalf("state structured response = %#v, want nil", result.State.StructuredResponse)
	}
	if model.structuredCalls != 0 {
		t.Fatalf("GenerateStructured calls = %d, want 0", model.structuredCalls)
	}
}

func TestReactAgent_ToolCommandControlFlowParity(t *testing.T) {
	for _, version := range []prebuilt.ReactAgentVersion{prebuilt.ReactAgentVersionV1, prebuilt.ReactAgentVersionV2} {
		t.Run(string(version), func(t *testing.T) {
			model := &scriptedModel{responses: []prebuilt.ModelResponse{
				{Content: "call mixed tools", ToolCalls: []prebuilt.ToolCall{
					{ID: "1", Name: "echo", Args: map[string]any{"text": "hello"}},
					{ID: "2", Name: "command", Args: map[string]any{}},
				}},
				{Content: "done"},
			}}

			agent := prebuilt.CreateReactAgent(
				model,
				[]prebuilt.Tool{
					prebuilt.NewToolFunc("echo", func(_ context.Context, args map[string]any) (any, error) {
						return fmt.Sprintf("echo:%s", args["text"]), nil
					}),
					prebuilt.NewToolFuncWithRuntime("command", func(_ context.Context, _ map[string]any, runtime prebuilt.ToolRuntime) (any, error) {
						return prebuilt.ToolCommand{
							Update: map[string]any{
								"messages": []prebuilt.Message{{Role: "tool", ToolCallID: runtime.ToolCallID, Content: "command applied"}},
								"score":    7,
							},
						}, nil
					}),
				},
				prebuilt.WithAgentVersion(version),
			)

			result, err := agent.Invoke(context.Background(), prebuilt.AgentState{
				Messages: []prebuilt.Message{{Role: "user", Content: "run tools"}},
			})
			if err != nil {
				t.Fatalf("Invoke: %v", err)
			}
			if len(result.State.Messages) != 5 {
				t.Fatalf("messages len = %d, want 5", len(result.State.Messages))
			}
			if result.State.Messages[2].Role != "tool" || result.State.Messages[2].Content != "echo:hello" {
				t.Fatalf("first tool message = %+v", result.State.Messages[2])
			}
			if result.State.Messages[3].Role != "tool" || result.State.Messages[3].Content != "command applied" {
				t.Fatalf("second tool message = %+v", result.State.Messages[3])
			}
			if got := result.State.Values["score"]; got != 7 {
				t.Fatalf("state values score = %#v, want 7", got)
			}
		})
	}
}

func TestReactAgent_ToolCommandParentBubbles(t *testing.T) {
	model := &scriptedModel{responses: []prebuilt.ModelResponse{{
		Content: "route to parent",
		ToolCalls: []prebuilt.ToolCall{{
			ID:   "1",
			Name: "parent_route",
			Args: map[string]any{},
		}},
	}}}

	parent := graph.CommandParent
	agent := prebuilt.CreateReactAgent(
		model,
		[]prebuilt.Tool{
			prebuilt.NewToolFuncWithRuntime("parent_route", func(_ context.Context, _ map[string]any, runtime prebuilt.ToolRuntime) (any, error) {
				return prebuilt.ToolCommand{
					Graph: &parent,
					Update: map[string]any{
						"messages": []prebuilt.Message{{Role: "tool", ToolCallID: runtime.ToolCallID, Content: "route"}},
					},
					Goto: graph.RouteTo("coordinator"),
				}, nil
			}),
		},
	)

	_, err := agent.Invoke(context.Background(), prebuilt.AgentState{Messages: []prebuilt.Message{{Role: "user", Content: "delegate"}}})
	if err == nil {
		t.Fatal("expected parent command error")
	}
	var parentErr *graph.ParentCommand
	if !errors.As(err, &parentErr) {
		t.Fatalf("err type = %T, want *graph.ParentCommand", err)
	}
}

func TestReactAgent_V2FanoutExecutesEachToolCallOnce(t *testing.T) {
	model := &scriptedModel{responses: []prebuilt.ModelResponse{
		{Content: "run fanout", ToolCalls: []prebuilt.ToolCall{
			{ID: "1", Name: "counter", Args: map[string]any{"label": "a"}},
			{ID: "2", Name: "counter", Args: map[string]any{"label": "b"}},
		}},
		{Content: "done"},
	}}

	var execMu sync.Mutex
	execCounts := map[string]int{}
	agent := prebuilt.CreateReactAgent(
		model,
		[]prebuilt.Tool{
			prebuilt.NewToolFunc("counter", func(_ context.Context, args map[string]any) (any, error) {
				label, _ := args["label"].(string)
				execMu.Lock()
				execCounts[label]++
				execMu.Unlock()
				return "ok:" + label, nil
			}),
		},
		prebuilt.WithAgentVersion(prebuilt.ReactAgentVersionV2),
	)

	result, err := agent.Invoke(context.Background(), prebuilt.AgentState{Messages: []prebuilt.Message{{Role: "user", Content: "fanout"}}})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if execCounts["a"] != 1 || execCounts["b"] != 1 {
		t.Fatalf("execution counts = %#v, want each call once", execCounts)
	}
	if len(result.State.Messages) != 5 {
		t.Fatalf("messages len = %d, want 5", len(result.State.Messages))
	}
	if result.State.Messages[2].Content != "ok:a" || result.State.Messages[3].Content != "ok:b" {
		t.Fatalf("tool message ordering = %+v %+v", result.State.Messages[2], result.State.Messages[3])
	}
}

func TestReactAgent_V2InterruptResumeExecutesOnlyUnansweredCalls(t *testing.T) {
	model := &scriptedModel{responses: []prebuilt.ModelResponse{
		{Content: "run fanout", ToolCalls: []prebuilt.ToolCall{
			{ID: "1", Name: "counter", Args: map[string]any{"label": "a"}},
			{ID: "2", Name: "counter", Args: map[string]any{"label": "b"}},
		}},
		{Content: "done"},
	}}

	var execMu sync.Mutex
	execCounts := map[string]int{}
	agent := prebuilt.CreateReactAgent(
		model,
		[]prebuilt.Tool{
			prebuilt.NewToolFunc("counter", func(_ context.Context, args map[string]any) (any, error) {
				label, _ := args["label"].(string)
				execMu.Lock()
				execCounts[label]++
				execMu.Unlock()
				return "ok:" + label, nil
			}),
		},
		prebuilt.WithAgentVersion(prebuilt.ReactAgentVersionV2),
		prebuilt.WithInterruptBeforeTools(prebuilt.HumanInterruptConfig{AllowRespond: true}, "approve"),
	)

	first, err := agent.Invoke(context.Background(), prebuilt.AgentState{
		Messages:       []prebuilt.Message{{Role: "user", Content: "fanout"}},
		RemainingSteps: 5,
	})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if first.Pending == nil || len(first.Pending.Calls) != 2 {
		t.Fatalf("pending = %#v, want two pending calls", first.Pending)
	}

	second, err := agent.Resume(context.Background(), first.Pending, map[string]prebuilt.HumanResponse{
		"1": prebuilt.RespondHumanResponse("manual:a"),
	})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if execCounts["a"] != 0 || execCounts["b"] != 1 {
		t.Fatalf("execution counts = %#v, want only unanswered call executed", execCounts)
	}
	if len(second.State.Messages) < 5 {
		t.Fatalf("messages len = %d, want >= 5", len(second.State.Messages))
	}
	if second.State.Messages[2].Role != "tool" || second.State.Messages[2].Content != "manual:a" {
		t.Fatalf("first tool response = %+v, want manual response", second.State.Messages[2])
	}
	if second.State.Messages[3].Role != "tool" || second.State.Messages[3].Content != "ok:b" {
		t.Fatalf("second tool response = %+v, want executed call", second.State.Messages[3])
	}
}

func TestReactAgent_InterruptResumeReturnDirectSkipsModelAndStructuredResponse(t *testing.T) {
	model := &scriptedModel{responses: []prebuilt.ModelResponse{
		{Content: "run fanout", ToolCalls: []prebuilt.ToolCall{
			{ID: "1", Name: "direct", Args: map[string]any{"label": "manual"}},
			{ID: "2", Name: "counter", Args: map[string]any{"label": "b"}},
		}},
		{Content: "should not be used"},
	}, structuredResponse: map[string]any{"answer": "should-not-run"}}

	execCounts := map[string]int{}
	agent := prebuilt.CreateReactAgent(
		model,
		[]prebuilt.Tool{
			prebuilt.NewToolFuncWithReturnDirect("direct", true, func(_ context.Context, args map[string]any) (any, error) {
				execCounts["direct"]++
				return "direct:" + fmt.Sprint(args["label"]), nil
			}),
			prebuilt.NewToolFunc("counter", func(_ context.Context, args map[string]any) (any, error) {
				label, _ := args["label"].(string)
				execCounts[label]++
				return "ok:" + label, nil
			}),
		},
		prebuilt.WithAgentVersion(prebuilt.ReactAgentVersionV2),
		prebuilt.WithInterruptBeforeTools(prebuilt.HumanInterruptConfig{AllowRespond: true}, "approve"),
		prebuilt.WithAgentResponseFormat(prebuilt.AgentResponseFormat{Schema: map[string]any{"type": "object"}}),
	)

	first, err := agent.Invoke(context.Background(), prebuilt.AgentState{
		Messages:       []prebuilt.Message{{Role: "user", Content: "fanout"}},
		RemainingSteps: 5,
	})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if first.Pending == nil || len(first.Pending.Calls) != 2 {
		t.Fatalf("pending = %#v, want two pending calls", first.Pending)
	}

	second, err := agent.Resume(context.Background(), first.Pending, map[string]prebuilt.HumanResponse{
		"1": prebuilt.RespondHumanResponse("manual:direct"),
	})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if model.idx != 1 {
		t.Fatalf("model calls = %d, want 1", model.idx)
	}
	if model.structuredCalls != 0 {
		t.Fatalf("GenerateStructured calls = %d, want 0", model.structuredCalls)
	}
	if execCounts["direct"] != 0 || execCounts["b"] != 1 {
		t.Fatalf("execution counts = %#v, want only non-responded non-direct call executed", execCounts)
	}
	if len(second.State.Messages) != 4 {
		t.Fatalf("messages len = %d, want 4", len(second.State.Messages))
	}
	if second.State.Messages[2].Content != "manual:direct" || second.State.Messages[3].Content != "ok:b" {
		t.Fatalf("tool messages = %+v %+v", second.State.Messages[2], second.State.Messages[3])
	}
	if second.StructuredResponse != nil {
		t.Fatalf("structured response = %#v, want nil", second.StructuredResponse)
	}
}

func TestReactAgent_DefaultToolErrorPolicyPropagatesExecutionErrors(t *testing.T) {
	model := &scriptedModel{responses: []prebuilt.ModelResponse{{
		Content:   "call broken tool",
		ToolCalls: []prebuilt.ToolCall{{ID: "1", Name: "boom", Args: map[string]any{}}},
	}}}

	agent := prebuilt.CreateReactAgent(
		model,
		[]prebuilt.Tool{
			prebuilt.NewToolFunc("boom", func(_ context.Context, _ map[string]any) (any, error) {
				return nil, errors.New("tool exploded")
			}),
		},
	)

	_, err := agent.Invoke(context.Background(), prebuilt.AgentState{Messages: []prebuilt.Message{{Role: "user", Content: "go"}}})
	if err == nil {
		t.Fatal("expected tool execution error")
	}
	if !strings.Contains(err.Error(), "tool exploded") {
		t.Fatalf("err = %v, want tool exploded", err)
	}
}

func TestReactAgent_PromptSupport(t *testing.T) {
	t.Run("string", func(t *testing.T) {
		model := &scriptedModel{responses: []prebuilt.ModelResponse{{Content: "done"}}}
		agent := prebuilt.CreateReactAgent(
			model,
			nil,
			prebuilt.WithAgentPrompt("You are helpful."),
		)
		_, err := agent.Invoke(context.Background(), prebuilt.AgentState{Messages: []prebuilt.Message{{Role: "user", Content: "hello"}}})
		if err != nil {
			t.Fatalf("Invoke: %v", err)
		}
		if len(model.inputs) != 1 || len(model.inputs[0]) != 2 {
			t.Fatalf("prompted input length = %d", len(model.inputs[0]))
		}
		if model.inputs[0][0].Role != "system" || model.inputs[0][0].Content != "You are helpful." {
			t.Fatalf("unexpected first message: %+v", model.inputs[0][0])
		}
	})

	t.Run("callable and runnable", func(t *testing.T) {
		callableModel := &scriptedModel{responses: []prebuilt.ModelResponse{{Content: "done"}}}
		callable := prebuilt.PromptFunc(func(
			_ context.Context,
			_ prebuilt.AgentState,
			_ prebuilt.AgentRuntime,
		) ([]prebuilt.Message, error) {
			return []prebuilt.Message{{Role: "system", Content: "callable"}}, nil
		})
		agent := prebuilt.CreateReactAgent(callableModel, nil, prebuilt.WithAgentPrompt(callable))
		_, err := agent.Invoke(context.Background(), prebuilt.AgentState{Messages: []prebuilt.Message{{Role: "user", Content: "x"}}})
		if err != nil {
			t.Fatalf("Invoke: %v", err)
		}
		if callableModel.inputs[0][0].Content != "callable" {
			t.Fatalf("callable prompt content = %q", callableModel.inputs[0][0].Content)
		}

		runnableModel := &scriptedModel{responses: []prebuilt.ModelResponse{{Content: "done"}}}
		r := promptRunnable{messages: []prebuilt.Message{{Role: "system", Content: "runnable"}}}
		agent = prebuilt.CreateReactAgent(runnableModel, nil, prebuilt.WithAgentPrompt(r))
		_, err = agent.Invoke(context.Background(), prebuilt.AgentState{Messages: []prebuilt.Message{{Role: "user", Content: "x"}}})
		if err != nil {
			t.Fatalf("Invoke: %v", err)
		}
		if runnableModel.inputs[0][0].Content != "runnable" {
			t.Fatalf("runnable prompt content = %q", runnableModel.inputs[0][0].Content)
		}
	})
}

func TestReactAgent_StructuredResponse(t *testing.T) {
	model := &scriptedModel{
		responses:          []prebuilt.ModelResponse{{Content: "done"}},
		structuredResponse: map[string]any{"answer": "42"},
	}
	agent := prebuilt.CreateReactAgent(
		model,
		nil,
		prebuilt.WithAgentResponseFormat(prebuilt.AgentResponseFormat{Schema: map[string]any{"type": "object"}}),
	)

	result, err := agent.Invoke(context.Background(), prebuilt.AgentState{Messages: []prebuilt.Message{{Role: "user", Content: "q"}}})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if result.StructuredResponse == nil {
		t.Fatal("expected structured response")
	}
	out, ok := result.StructuredResponse.(map[string]any)
	if !ok || out["answer"] != "42" {
		t.Fatalf("structured response = %#v", result.StructuredResponse)
	}
	if result.State.StructuredResponse == nil {
		t.Fatal("expected structured response in state")
	}
}

func TestReactAgent_PreAndPostModelHooks(t *testing.T) {
	model := &scriptedModel{responses: []prebuilt.ModelResponse{
		{Content: "with call", ToolCalls: []prebuilt.ToolCall{{ID: "1", Name: "echo", Args: map[string]any{"text": "hi"}}}},
		{Content: "done"},
	}}

	preCalls := 0
	postCalls := 0

	agent := prebuilt.CreateReactAgent(
		model,
		[]prebuilt.Tool{
			prebuilt.NewToolFunc("echo", func(_ context.Context, args map[string]any) (any, error) {
				return args["text"], nil
			}),
		},
		prebuilt.WithAgentPreModelHook(func(
			_ context.Context,
			state prebuilt.AgentState,
			_ prebuilt.AgentRuntime,
		) (prebuilt.PreModelHookResult, error) {
			preCalls++
			return prebuilt.PreModelHookResult{
				State:            state,
				LLMInputMessages: []prebuilt.Message{{Role: "system", Content: "trimmed"}},
			}, nil
		}),
		prebuilt.WithAgentPostModelHook(func(
			_ context.Context,
			state prebuilt.AgentState,
			_ prebuilt.ModelResponse,
			_ prebuilt.AgentRuntime,
		) (prebuilt.AgentState, error) {
			postCalls++
			return state, nil
		}),
	)

	result, err := agent.Invoke(context.Background(), prebuilt.AgentState{Messages: []prebuilt.Message{{Role: "user", Content: "go"}}})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if preCalls == 0 || postCalls == 0 {
		t.Fatalf("expected both hooks called, pre=%d post=%d", preCalls, postCalls)
	}
	if len(model.inputs) == 0 || len(model.inputs[0]) != 1 || model.inputs[0][0].Content != "trimmed" {
		t.Fatalf("pre-hook model input = %#v", model.inputs)
	}
	if len(result.State.Messages) != 4 {
		t.Fatalf("messages len = %d, want 4", len(result.State.Messages))
	}
}

func TestReactAgent_CustomStateSchema(t *testing.T) {
	model := &scriptedModel{responses: []prebuilt.ModelResponse{{Content: "done"}}}
	agent := prebuilt.CreateReactAgent(
		model,
		nil,
		prebuilt.WithAgentStateSchema(customStateSchema{}),
	)

	result, err := agent.InvokeState(context.Background(), customState{
		History: []prebuilt.Message{{Role: "user", Content: "hello"}},
		Steps:   5,
	})
	if err != nil {
		t.Fatalf("InvokeState: %v", err)
	}
	custom, ok := result.State.(customState)
	if !ok {
		t.Fatalf("state type = %T, want customState", result.State)
	}
	if len(custom.History) != 2 {
		t.Fatalf("history len = %d, want 2", len(custom.History))
	}
}

func TestReactAgent_ContextSchemaAndDynamicModel(t *testing.T) {
	modelA := &selectorModel{name: "A"}
	modelB := &selectorModel{name: "B"}

	type runContext struct {
		Model string
	}

	selected := 0
	agent := prebuilt.CreateReactAgent(
		nil,
		nil,
		prebuilt.WithAgentStore(testStore{}),
		prebuilt.WithAgentContextSchema(prebuilt.AgentContextSchemaFunc(func(value any) (any, error) {
			m, ok := value.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("expected map context, got %T", value)
			}
			model, _ := m["model"].(string)
			return runContext{Model: model}, nil
		})),
		prebuilt.WithAgentModelSelector(func(_ context.Context, _ prebuilt.AgentState, runtime prebuilt.AgentRuntime) (prebuilt.AgentModel, error) {
			selected++
			rc, ok := runtime.Context.(runContext)
			if !ok {
				return nil, fmt.Errorf("unexpected runtime context type %T", runtime.Context)
			}
			if runtime.Store == nil {
				return nil, errors.New("runtime store should be set")
			}
			if rc.Model == "B" {
				return modelB, nil
			}
			return modelA, nil
		}),
	)

	result, err := agent.InvokeWithOptions(
		context.Background(),
		prebuilt.AgentState{Messages: []prebuilt.Message{{Role: "user", Content: "hello"}}},
		prebuilt.AgentInvokeOptions{Context: map[string]any{"model": "B"}},
	)
	if err != nil {
		t.Fatalf("InvokeWithOptions: %v", err)
	}
	if selected == 0 {
		t.Fatal("expected dynamic selector call")
	}
	if got := result.State.Messages[len(result.State.Messages)-1].Content; got != "B" {
		t.Fatalf("assistant content = %q, want B", got)
	}
}

func TestReactAgent_CheckpointerAndPendingResume(t *testing.T) {
	model := &scriptedModel{responses: []prebuilt.ModelResponse{
		{Content: "tool", ToolCalls: []prebuilt.ToolCall{{ID: "1", Name: "calc", Args: map[string]any{"a": 2, "b": 3}}}},
		{Content: "done"},
	}}
	saver := checkpoint.NewInMemorySaver()

	agent := prebuilt.CreateReactAgent(
		model,
		[]prebuilt.Tool{
			prebuilt.NewToolFunc("calc", func(_ context.Context, args map[string]any) (any, error) {
				a, _ := args["a"].(int)
				b, _ := args["b"].(int)
				return a + b, nil
			}),
		},
		prebuilt.WithAgentCheckpointer(saver),
		prebuilt.WithInterruptBeforeTools(prebuilt.HumanInterruptConfig{AllowAccept: true}, "approve"),
	)

	first, err := agent.InvokeWithOptions(
		context.Background(),
		prebuilt.AgentState{Messages: []prebuilt.Message{{Role: "user", Content: "run"}}},
		prebuilt.AgentInvokeOptions{ThreadID: "thread-checkpointer"},
	)
	if err != nil {
		t.Fatalf("InvokeWithOptions: %v", err)
	}
	if first.Checkpoint == nil || first.Checkpoint.CheckpointID == "" {
		t.Fatalf("checkpoint config = %#v", first.Checkpoint)
	}

	second, err := agent.ResumeWithOptions(
		context.Background(),
		nil,
		map[string]prebuilt.HumanResponse{"1": {Type: prebuilt.HumanResponseAccept}},
		prebuilt.AgentInvokeOptions{ThreadID: "thread-checkpointer"},
	)
	if err != nil {
		t.Fatalf("ResumeWithOptions: %v", err)
	}
	if len(second.State.Messages) != 4 {
		t.Fatalf("messages len = %d, want 4", len(second.State.Messages))
	}
	if second.State.Messages[2].Content != "5" {
		t.Fatalf("tool output = %q, want 5", second.State.Messages[2].Content)
	}
}

func TestReactAgent_InterruptBeforeAndAfterNodes(t *testing.T) {
	model := &scriptedModel{responses: []prebuilt.ModelResponse{{Content: "done"}}}
	agent := prebuilt.CreateReactAgent(
		model,
		nil,
		prebuilt.WithAgentInterruptBefore([]string{"agent"}, prebuilt.HumanInterruptConfig{AllowAccept: true}, "pause"),
	)

	first, err := agent.Invoke(context.Background(), prebuilt.AgentState{Messages: []prebuilt.Message{{Role: "user", Content: "x"}}})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if first.Pending == nil || first.Pending.Stage != prebuilt.PendingStageBeforeAgent {
		t.Fatalf("pending stage = %#v", first.Pending)
	}

	resumed, err := agent.Resume(context.Background(), first.Pending, nil)
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if len(resumed.State.Messages) != 2 {
		t.Fatalf("messages len = %d, want 2", len(resumed.State.Messages))
	}

	afterModel := &scriptedModel{responses: []prebuilt.ModelResponse{
		{Content: "tools", ToolCalls: []prebuilt.ToolCall{{ID: "1", Name: "echo", Args: map[string]any{"text": "hi"}}}},
		{Content: "done"},
	}}
	afterAgent := prebuilt.CreateReactAgent(
		afterModel,
		[]prebuilt.Tool{prebuilt.NewToolFunc("echo", func(_ context.Context, args map[string]any) (any, error) {
			return args["text"], nil
		})},
		prebuilt.WithAgentInterruptAfter([]string{"tools"}, prebuilt.HumanInterruptConfig{AllowAccept: true}, "after-tools"),
	)

	afterResult, err := afterAgent.Invoke(context.Background(), prebuilt.AgentState{Messages: []prebuilt.Message{{Role: "user", Content: "go"}}})
	if err != nil {
		t.Fatalf("Invoke (after): %v", err)
	}
	if afterResult.Pending == nil || afterResult.Pending.Stage != prebuilt.PendingStageAfterTools {
		t.Fatalf("after pending stage = %#v", afterResult.Pending)
	}
}

func TestReactAgent_ResumeAfterAgentInterruptReentersBeforeTools(t *testing.T) {
	model := &scriptedModel{responses: []prebuilt.ModelResponse{{
		Content: "call tool",
		ToolCalls: []prebuilt.ToolCall{{
			ID:   "1",
			Name: "echo",
			Args: map[string]any{"text": "hi"},
		}},
	}}}

	executed := 0
	agent := prebuilt.CreateReactAgent(
		model,
		[]prebuilt.Tool{prebuilt.NewToolFunc("echo", func(_ context.Context, args map[string]any) (any, error) {
			executed++
			return args["text"], nil
		})},
		prebuilt.WithAgentInterruptAfter([]string{"agent"}, prebuilt.HumanInterruptConfig{AllowAccept: true}, "after-agent"),
		prebuilt.WithInterruptBeforeTools(prebuilt.HumanInterruptConfig{AllowRespond: true}, "before-tools"),
	)

	first, err := agent.Invoke(context.Background(), prebuilt.AgentState{Messages: []prebuilt.Message{{Role: "user", Content: "go"}}})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if first.Pending == nil || first.Pending.Stage != prebuilt.PendingStageAfterAgent {
		t.Fatalf("first pending = %#v, want after_agent", first.Pending)
	}

	second, err := agent.Resume(context.Background(), first.Pending, map[string]prebuilt.HumanResponse{
		"1": prebuilt.RespondHumanResponse("manual"),
	})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if executed != 0 {
		t.Fatalf("tool executions = %d, want 0 before before-tools interrupt", executed)
	}
	if second.Pending == nil || second.Pending.Stage != prebuilt.PendingStageBeforeTools {
		t.Fatalf("second pending = %#v, want before_tools", second.Pending)
	}
	if len(second.Pending.Calls) != 1 || second.Pending.Calls[0].ID != "1" {
		t.Fatalf("pending calls = %#v, want original call", second.Pending.Calls)
	}
}

func TestReactAgent_VersionAsyncAndToolBindingValidation(t *testing.T) {
	t.Run("invalid version", func(t *testing.T) {
		model := &scriptedModel{responses: []prebuilt.ModelResponse{{Content: "done"}}}
		agent := prebuilt.CreateReactAgent(model, nil, prebuilt.WithAgentVersion("bad"))
		_, err := agent.Invoke(context.Background(), prebuilt.AgentState{Messages: []prebuilt.Message{{Role: "user", Content: "x"}}})
		if err == nil {
			t.Fatal("expected invalid version error")
		}
	})

	t.Run("async invoke", func(t *testing.T) {
		model := &scriptedModel{responses: []prebuilt.ModelResponse{{Content: "done"}}}
		agent := prebuilt.CreateReactAgent(model, nil)
		ch := agent.InvokeAsync(context.Background(), prebuilt.AgentState{Messages: []prebuilt.Message{{Role: "user", Content: "x"}}})
		async := <-ch
		if async.Err != nil {
			t.Fatalf("InvokeAsync: %v", async.Err)
		}
		if len(async.Result.State.Messages) != 2 {
			t.Fatalf("messages len = %d, want 2", len(async.Result.State.Messages))
		}
	})

	t.Run("tool binding validation", func(t *testing.T) {
		model := &boundModel{
			responses: []prebuilt.ModelResponse{{Content: "done"}},
			bound:     []string{"wrong"},
		}
		agent := prebuilt.CreateReactAgent(
			model,
			[]prebuilt.Tool{prebuilt.NewToolFunc("echo", func(_ context.Context, _ map[string]any) (any, error) { return "ok", nil })},
		)
		_, err := agent.Invoke(context.Background(), prebuilt.AgentState{Messages: []prebuilt.Message{{Role: "user", Content: "x"}}})
		if err == nil {
			t.Fatal("expected tool binding validation error")
		}
	})

	t.Run("v1 still supported", func(t *testing.T) {
		model := &scriptedModel{responses: []prebuilt.ModelResponse{
			{Content: "tool", ToolCalls: []prebuilt.ToolCall{{ID: "1", Name: "echo", Args: map[string]any{"text": "hi"}}}},
			{Content: "done"},
		}}
		agent := prebuilt.CreateReactAgent(
			model,
			[]prebuilt.Tool{prebuilt.NewToolFunc("echo", func(_ context.Context, args map[string]any) (any, error) { return args["text"], nil })},
			prebuilt.WithAgentVersion(prebuilt.ReactAgentVersionV1),
		)
		result, err := agent.Invoke(context.Background(), prebuilt.AgentState{Messages: []prebuilt.Message{{Role: "user", Content: "x"}}})
		if err != nil {
			t.Fatalf("Invoke v1: %v", err)
		}
		if len(result.State.Messages) != 4 {
			t.Fatalf("messages len = %d, want 4", len(result.State.Messages))
		}
	})
}

func cloneMessages(in []prebuilt.Message) []prebuilt.Message {
	out := make([]prebuilt.Message, len(in))
	copy(out, in)
	for i := range out {
		calls := make([]prebuilt.ToolCall, len(out[i].ToolCalls))
		copy(calls, out[i].ToolCalls)
		for j := range calls {
			if calls[j].Args == nil {
				continue
			}
			args := make(map[string]any, len(calls[j].Args))
			for k, v := range calls[j].Args {
				args[k] = v
			}
			calls[j].Args = args
		}
		out[i].ToolCalls = calls
	}
	return out
}

var _ graph.Store = testStore{}
