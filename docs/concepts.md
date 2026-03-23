# Core Concepts

This page covers the mental model behind gographgo — what graphs are, how execution works, what state really means, and how all the pieces fit together. If you're coming from LangGraph (Python or JS), most of this will feel familiar. If you're new, start here.

---

## The big idea

gographgo lets you model a computation as a **directed graph**. Nodes do work. Edges describe which node runs next. A shared **state** object flows between them — each node reads from it and writes updates back.

The runtime takes care of scheduling nodes, merging writes into state, persisting checkpoints, streaming events, and handling parallelism. You just write functions.

---

## Graphs

Everything starts with `StateGraph`. It's a builder — you add nodes and edges to it, then compile it into an executable `CompiledStateGraph`.

```go
builder := graph.NewStateGraph[MyState]()

// ... add nodes and edges ...

compiled, err := builder.Compile()
```

The type parameter `MyState` is your state type — a plain Go struct (or `map[string]any` if you prefer to live dangerously). The graph is generic over this type, so every node receives and returns your exact type.

### Type parameters

The full signature is:

```go
StateGraph[State, Context, Input, Output any]
```

- **State** — the shape of the shared mutable state flowing between nodes.
- **Context** — optional static context injected at compile time (user IDs, DB connections, config). Available via `graph.GetRuntime(ctx).Context`. Defaults to `any`.
- **Input** — the type accepted by `Invoke`. Defaults to `State`.
- **Output** — the type returned by `Invoke`. Defaults to `State`.

If you only care about `State`, use `NewStateGraph[State]()`. For explicit type control:

```go
builder := graph.NewStateGraphWithTypes[State, Context, Input, Output]()
```

---

## Nodes

A node is a function. It receives the current state, does some work, and returns updates to write back.

```go
builder.AddNode("summarise", func(ctx context.Context, s MyState) (graph.NodeResult, error) {
    summary := summarise(s.Text)
    return graph.NodeWrites(map[string]graph.Dynamic{
        "Summary": graph.Dyn(summary),
    }), nil
})
```

The return type `NodeResult` has three forms:

| Constructor | What it does |
|---|---|
| `graph.NodeWrites(map[string]graph.Dynamic{...})` | Write specific fields into state |
| `graph.NodeState(graph.Dyn(newState))` | Replace the entire state |
| `graph.NodeCommand(cmd)` | Return a `Command` for routing control |
| `graph.NoNodeResult()` | Write nothing (read-only node) |

### Dynamic values

`graph.Dynamic` is a thin type-safe wrapper for runtime values. Use `graph.Dyn(value)` to box a value and `.Value()` to unbox it:

```go
graph.Dyn("hello")           // boxes a string
graph.Dyn(42)                // boxes an int
graph.DynMap(map[string]any{ // boxes a map as individual field writes
    "Name": "Alice",
    "Age":  30,
})
```

`DynMap` is a shorthand when you're writing multiple fields at once — it converts `map[string]any` into `map[string]graph.Dynamic` for you.

### Reserved node names

`graph.START` (`"__start__"`) and `graph.END` (`"__end__"`) are special sentinels. Don't name your nodes with those strings, or anything containing `|` or `:`.

---

## Edges

Edges define the execution order. There are three kinds.

### Static edges

The simplest kind. After node A runs, always run node B.

```go
builder.AddEdge(graph.START, "fetch")
builder.AddEdge("fetch",     "process")
builder.AddEdge("process",   graph.END)
```

`graph.START` is the implicit source — it fires on invocation. `graph.END` is the sink — reaching it stops execution.

### Conditional edges

After node A runs, call a function to decide which node runs next.

```go
builder.AddConditionalEdges("router",
    func(ctx context.Context, s State) (graph.Route, error) {
        if s.Score >= 70 {
            return graph.RouteTo("approve"), nil
        }
        return graph.RouteTo("reject"), nil
    },
    map[string]string{
        "approve": "approve",
        "reject":  "reject",
    },
)
```

The `pathMap` (third argument) maps the string keys you might return to concrete node names. It's used for graph validation and introspection — the runtime needs to know all possible destinations at compile time.

You can also route to multiple nodes at once:

```go
return graph.RouteToMany("check_a", "check_b", "check_c"), nil
```

### Fan-in (waiting) edges

If you fan out to multiple nodes and want to wait for all of them before continuing, use `AddEdges`:

```go
builder.AddEdge(graph.START, "fetch_a")
builder.AddEdge(graph.START, "fetch_b")
builder.AddEdges([]string{"fetch_a", "fetch_b"}, "merge") // waits for both
builder.AddEdge("merge", graph.END)
```

`fetch_a` and `fetch_b` run in parallel. `merge` only fires once both complete.

### Sequences

For a straight pipeline of nodes, `AddSequence` wires them all up in one call:

```go
builder.AddSequence(
    []string{"parse", "validate", "store"},
    []graph.Node[State]{parseFn, validateFn, storeFn},
)
// equivalent to: AddNode + AddEdge for each step
```

---

## State and channels

Every field in your state struct is backed by a **channel** — the runtime's internal name for the per-field storage slot. By default, every field uses a `LastValue` channel: each superstep, the last write wins. If multiple nodes try to write the same field in the same superstep, the runtime raises an `InvalidUpdateError`.

### Reducers

When you want multiple concurrent writes to merge rather than conflict, register a reducer:

```go
// All nodes can now append to Logs without conflicting
builder.RegisterReducer("Logs", func(a, b any) any {
    return append(a.([]string), b.([]string)...)
})
```

Alternatively, annotate your state struct with the `reducer` tag:

```go
type State struct {
    Logs []string `reducer:"append"`
}
// built-in: "append" works for slices
```

### Overwrite

To bypass a reducer and replace a channel's accumulated value directly, wrap your value in `graph.Overwrite`:

```go
return graph.NodeWrites(map[string]graph.Dynamic{
    "Logs": graph.Dyn(graph.Overwrite{Value: []string{"reset"}}),
}), nil
```

### Messages — a special reducer

For chat-style graphs, there's a built-in additive message channel:

```go
type State struct {
    Messages []graph.Message
}
```

The `AddMessages` reducer merges messages by ID (upsert semantics). Writing a message with an existing ID updates it; writing a new ID appends it. To clear all messages, write `graph.RemoveAllMessagesSentinel()`.

Use `graph.NewStateGraph[graph.MessagesState]()` or `graph.NewMessagesStateGraph()` to get a graph pre-wired with this reducer.

### Managed values

Some values aren't written by nodes at all — they're computed by the runtime and injected into state before each superstep. Built-ins:

```go
builder.AddManagedValue("remaining_steps", graph.RemainingStepsManager{})
builder.AddManagedValue("is_last_step",    graph.IsLastStepManager{})
builder.AddManagedValue("now",             graph.CurrentUnixTimestampManager{})
```

After this, nodes can read `state.RemainingSteps` (or whatever field name you used) and it will be populated automatically.

---

## Compilation

`Compile()` validates the graph topology, sets up internal channels, and returns a `*CompiledStateGraph`. It accepts optional `CompileOptions`:

```go
compiled, err := builder.Compile(graph.CompileOptions{
    Checkpointer:    saver,          // enable checkpointing
    Store:           store,          // enable cross-thread KV store
    Cache:           cache,          // enable node output caching
    Context:         myContextValue, // static runtime context
    InterruptBefore: []string{"confirm"}, // pause before "confirm" runs
    InterruptAfter:  []string{"notify"},  // pause after "notify" runs
})
```

---

## Invocation

```go
// Blocking — waits for the graph to finish
result, err := compiled.Invoke(ctx, initialState)

finalState := result.Value.(MyState)
interrupts := result.Interrupts // non-empty if graph paused
```

Pass a `graph.Config` via `graph.WithConfig(ctx, cfg)` to configure runtime behaviour (thread ID, checkpointer, recursion limit, etc.) per invocation:

```go
cfg := graph.Config{
    ThreadID:     "user-session-123",
    Checkpointer: saver,
    RecursionLimit: 50,
}
ctx := graph.WithConfig(context.Background(), cfg)
result, err := compiled.Invoke(ctx, initialState)
```

---

## The Pregel execution model

Under the hood, gographgo uses a **Pregel-style superstep** scheduler (the same model LangGraph uses).

1. The runtime collects all nodes scheduled to run in this step.
2. It executes them — in parallel where possible, respecting barrier constraints.
3. Each node's writes are buffered until all nodes in the step have completed.
4. All buffered writes are merged into state (via channel semantics / reducers).
5. The scheduler looks at the updated state and edges to determine the next set of nodes.
6. Repeat until `END` is reached or the recursion limit is hit.

The `RecursionLimit` (default 25) is the maximum number of supersteps. Exceed it and you get a `GraphRecursionError`.

---

## Introspection

A compiled graph can describe itself:

```go
info := compiled.GetGraph()

fmt.Println(info.Mermaid()) // prints a Mermaid flowchart

for _, node := range info.Nodes {
    fmt.Printf("node: %s (has cache: %v)\n", node.Name, node.HasCachePolicy)
}
```

The `GraphInfo` struct exposes nodes, edges, channels, managed values, interrupt points, and JSON schemas for input/output/context.

---

## What's next

- [Checkpointing](./checkpointing.md) — how to persist state across invocations
- [Streaming](./streaming.md) — how to consume events as the graph runs
- [Human-in-the-Loop](./human-in-the-loop.md) — how to pause and resume execution
- [Prebuilt Agents](./prebuilt-agents.md) — the ReactAgent and tool ecosystem
