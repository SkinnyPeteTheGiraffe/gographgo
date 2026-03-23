# Prebuilt Agents

The `pkg/prebuilt` package sits on top of the core graph API and gives you a ready-made agentic loop — the classic **ReAct** (Reason + Act) pattern where a model reasons, calls tools, gets results, and reasons again until it's done.

You bring a model and some tools. gographgo brings the loop.

---

## The shape of a ReAct agent

```
START
  └─► agent (LLM call)
        ├─► END              (no tool calls in response)
        └─► tools (execute)
              └─► agent      (loop back with tool results)
```

The loop continues until the model stops requesting tools (or hits the step limit).

---

## Quick start

```go
import "github.com/SkinnyPeteTheGiraffe/gographgo/pkg/prebuilt"

// 1. Implement AgentModel for your LLM
model := MyLLMClient{}

// 2. Define tools
tools := []prebuilt.Tool{
    prebuilt.NewToolFunc("search", func(ctx context.Context, args map[string]any) (any, error) {
        query := args["query"].(string)
        return search(query), nil
    }),
}

// 3. Create the agent
agent := prebuilt.CreateReactAgent(model, tools)

// 4. Run it
result, err := agent.Invoke(ctx, prebuilt.AgentState{
    Messages: []prebuilt.Message{
        {Role: "user", Content: "What is the capital of France?"},
    },
}, graph.Config{})
```

The agent returns an `AgentResult` with `State` (final messages + any other state), `Interrupts`, and optionally `StructuredResponse`.

---

## `AgentModel` — bringing your own LLM

The `AgentModel` interface is intentionally minimal:

```go
type AgentModel interface {
    Generate(ctx context.Context, messages []Message) (ModelResponse, error)
}
```

Implement it for whatever LLM you're using — OpenAI, Anthropic, Gemini, Ollama, a mock, anything:

```go
type MyModel struct {
    client *openai.Client
}

func (m MyModel) Generate(ctx context.Context, msgs []prebuilt.Message) (prebuilt.ModelResponse, error) {
    // translate msgs to OpenAI format, call API, translate response back
    resp, err := m.client.Chat(ctx, toOpenAIMessages(msgs))
    return prebuilt.ModelResponse{
        Content:   resp.Content,
        ToolCalls: toToolCalls(resp.ToolCalls),
    }, err
}
```

### Optional model interfaces

Your model can implement additional interfaces for extra capabilities:

| Interface | Method | Purpose |
|---|---|---|
| `ToolBinderModel` | `BindTools(tools) (AgentModel, error)` | Return a copy of the model with tools registered (for API-level tool binding) |
| `ToolBindingModel` | `BoundToolNames() []string` | Introspect which tools are currently bound |
| `StructuredOutputModel` | `GenerateStructured(ctx, messages, schema) (any, error)` | Return structured JSON instead of text |

---

## Tools

Tools are the actions an agent can take. They implement `prebuilt.Tool`:

```go
type Tool interface {
    Name() string
    Invoke(ctx context.Context, args map[string]any) (any, error)
}
```

### `NewToolFunc` — quickest way to define a tool

```go
tool := prebuilt.NewToolFunc("calculator", func(ctx context.Context, args map[string]any) (any, error) {
    a := args["a"].(float64)
    b := args["b"].(float64)
    op := args["op"].(string)
    switch op {
    case "add": return a + b, nil
    case "mul": return a * b, nil
    default:    return nil, fmt.Errorf("unknown op %q", op)
    }
})
```

### Optional tool interfaces

| Interface | Method | What you get |
|---|---|---|
| `RuntimeAwareTool` | `InvokeWithRuntime(ctx, args, runtime)` | Full `ToolRuntime` (state, config, store, stream writer) |
| `StateAwareTool` | `InvokeWithState(ctx, args, state)` | Current graph state |
| `StoreAwareTool` | `InvokeWithStore(ctx, args, store)` | Cross-thread KV store |
| `ReturnDirectTool` | `ReturnDirect() bool` | When true, tool result is returned directly to the user without another model call |

```go
type MyRichTool struct{}

func (t MyRichTool) Name() string { return "log_event" }

func (t MyRichTool) InvokeWithRuntime(ctx context.Context, args map[string]any, rt prebuilt.ToolRuntime) (any, error) {
    // Access current state, stream events, config, store...
    rt.StreamWriter(fmt.Sprintf("logging event for thread %s", rt.ThreadID))
    return "logged", nil
}
```

---

## `ToolNode` — run tools yourself

If you want the tool execution logic without the full agent loop, `ToolNode` runs a list of `ToolCall` requests and returns `ToolMessage` results:

```go
node := prebuilt.NewToolNode(tools)

results, err := node.Run(ctx, toolCalls)
// results is []prebuilt.ToolMessage
```

Tools are executed in parallel by default. Error handling is configurable:

```go
node := prebuilt.NewToolNode(tools,
    prebuilt.WithToolErrorHandling(true),
    prebuilt.WithToolErrorFormatter(func(err error) string {
        return fmt.Sprintf("Tool failed: %v. Please try again with different arguments.", err)
    }),
)
```

With error handling enabled, tool errors are returned as `ToolMessage` with `Status: "error"` instead of bubbling up, allowing the model to retry.

### Intercepting tool calls

```go
node := prebuilt.NewToolNode(tools,
    prebuilt.WithToolCallWrapper(func(req prebuilt.ToolCallRequest, next prebuilt.ToolCallExecutor) (prebuilt.ToolMessage, error) {
        // log, rate-limit, validate, etc.
        log.Printf("calling tool %s", req.ToolCall.Name)
        return next(req)
    }),
)
```

---

## Agent state

The default state for `ReactAgent` is `prebuilt.AgentState`:

```go
type AgentState struct {
    Messages         []Message
    RemainingSteps   int         // managed — counts down from max steps
    StructuredResponse any       // populated when ResponseFormat is set
    Values           map[string]any // extra state fields
}
```

You can use a custom state type by providing a `WithStateSchema` option that handles encoding/decoding between your type and `AgentState`.

---

## Configuring the agent

All agent options are passed to `CreateReactAgent`:

```go
agent := prebuilt.CreateReactAgent(model, tools,
    prebuilt.WithMaxSteps(20),
    prebuilt.WithName("my-agent"),
    prebuilt.WithPromptString("You are a helpful assistant. Be concise."),
    prebuilt.WithCheckpointer(saver),
    prebuilt.WithStore(store),
)
```

### System prompt options

| Option | What it does |
|---|---|
| `WithPromptString(s)` | Static system prompt string |
| `WithPromptMessage(msg)` | Static `*Message` as system prompt |
| `WithPromptFunc(fn)` | Dynamic prompt — called before each model invocation |
| `WithPromptRunnable(r)` | Prompt from a `PromptRunnable` interface |

### Hooks

```go
agent := prebuilt.CreateReactAgent(model, tools,
    // Runs before each LLM call — can modify messages or state
    prebuilt.WithPreModelHook(func(ctx context.Context, s prebuilt.AgentState, rt prebuilt.AgentRuntime) (prebuilt.PreModelHookResult, error) {
        // Add dynamic context to messages
        return prebuilt.PreModelHookResult{
            Messages: append(systemMessages(), s.Messages...),
        }, nil
    }),

    // Runs after each LLM call — can modify state based on response
    prebuilt.WithPostModelHook(func(ctx context.Context, s prebuilt.AgentState, resp prebuilt.ModelResponse, rt prebuilt.AgentRuntime) (prebuilt.AgentState, error) {
        log.Printf("model used %d tool calls", len(resp.ToolCalls))
        return s, nil
    }),
)
```

### Structured output

Request a structured JSON response from the model when the loop completes:

```go
type Answer struct {
    Summary string `json:"summary"`
    Confidence float64 `json:"confidence"`
}

agent := prebuilt.CreateReactAgent(model, tools,
    prebuilt.WithResponseFormat(&prebuilt.AgentResponseFormat{
        Schema: Answer{},
    }),
)

result, _ := agent.Invoke(ctx, state, cfg)
answer := result.StructuredResponse.(Answer)
```

The model must implement `StructuredOutputModel` for this to work.

---

## Tool execution versions

```go
prebuilt.WithVersion(prebuilt.ReactAgentVersionV1) // all tools run together (default)
prebuilt.WithVersion(prebuilt.ReactAgentVersionV2) // each tool call is a separate fan-out task
```

V2 is useful when you want tool calls to run concurrently as separate graph tasks with their own checkpoints.

---

## Tool argument validation

Validate tool arguments before execution to give the model better error messages:

```go
agent := prebuilt.CreateReactAgent(model, tools,
    prebuilt.WithToolValidator("calculator", func(args map[string]any) error {
        if _, ok := args["a"]; !ok {
            return fmt.Errorf("missing required argument 'a'")
        }
        return nil
    }),
)
```

Validation errors are formatted into the `ToolMessage` so the model can self-correct.

---

## Streaming the agent

```go
ch := agent.Stream(ctx, initialState, cfg, graph.StreamModeUpdates, graph.StreamModeCustom)

for part := range ch {
    if part.Err != nil { log.Fatal(part.Err) }
    // handle stream events
}
```

---

## Human-in-the-loop with agents

Enable human approval before tool execution:

```go
agent := prebuilt.CreateReactAgent(model, tools,
    prebuilt.WithCheckpointer(saver),
    prebuilt.WithHumanInterruptSupport(
        prebuilt.HumanInterruptConfig{
            AllowAccept:  true,
            AllowIgnore:  true,
            AllowRespond: true,
        },
        "Review each tool call before it executes",
    ),
)

result, _ := agent.Invoke(ctx, initialState, cfg)

if result.Pending != nil {
    // Show the pending tool calls to the user
    for _, call := range result.Pending.Calls {
        fmt.Printf("Approve: %s(%v)? ", call.Name, call.Args)
    }

    // Resume with the human's decision
    result, _ = agent.Resume(ctx, cfg, prebuilt.AcceptHumanResponse())
}
```

Available responses:

| Constructor | Effect |
|---|---|
| `prebuilt.AcceptHumanResponse()` | Run the tool call as proposed |
| `prebuilt.IgnoreHumanResponse()` | Skip the tool call, continue the agent loop |
| `prebuilt.RespondHumanResponse(text)` | Send a text message back to the model |
| `prebuilt.EditHumanResponse(actionRequest)` | Replace the tool call args with edited version |

---

## `ToolsCondition` — routing helper

When building custom agent graphs from scratch, `ToolsCondition` routes based on whether the last message contains tool calls:

```go
builder.AddConditionalEdges("agent",
    func(ctx context.Context, s State) (graph.Route, error) {
        return prebuilt.ToolsCondition(s.ToAgentState()), nil
    },
    map[string]string{"tools": "tools", "__end__": graph.END},
)
```

Or with custom options:

```go
prebuilt.ToolsConditionWithOptions(state, prebuilt.ToolsConditionOptions[State]{
    Messages:       func(s State) []prebuilt.Message { return s.Messages },
    OnToolCalls:    graph.RouteTo("tools"),
    OnNoToolCalls:  graph.RouteTo("format_response"),
})
```

---

## What's next

- [Human-in-the-Loop](./human-in-the-loop.md) — detailed interrupt/resume reference
- [Checkpointing](./checkpointing.md) — persist agent state across sessions
- [Remote SDK](./sdk.md) — run agents on a remote server
