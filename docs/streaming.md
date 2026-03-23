# Streaming

`Invoke` gives you the final state after the graph finishes. Streaming gives you everything as it happens — node completions, state diffs, custom data, debug events — emitted on a channel as the graph runs.

This is the right approach for:
- **Progress UIs** — show which step is running
- **Token streaming** — surface LLM output token-by-token
- **Pipelines** — pass partial results downstream without waiting for full completion
- **Debugging** — watch the graph think step by step

---

## The basics

```go
ch := compiled.Stream(ctx, initialState, graph.StreamModeUpdates)

for part := range ch {
    if part.Err != nil {
        log.Fatal(part.Err)
    }
    fmt.Printf("[%s] %v\n", part.Type, part.Data)
}
```

`Stream` returns a `<-chan graph.StreamPart`. The channel closes when the graph finishes (or errors). **Always drain the channel** — the runtime blocks until you consume events.

Every `StreamPart` has:

```go
type StreamPart struct {
    Type       graph.StreamMode // which mode produced this event
    Data       any              // the payload (type depends on mode)
    Namespace  []string         // checkpoint namespace (relevant for subgraphs)
    Interrupts []graph.Interrupt // set when execution pauses
    Err        error            // non-nil only on terminal error; check this field
}
```

---

## Stream modes

### `StreamModeValues` — full state after each superstep

Emits the entire state snapshot after every superstep. You see the world as it is after each batch of nodes completes.

```go
ch := compiled.Stream(ctx, initialState, graph.StreamModeValues)

for part := range ch {
    if part.Err != nil { log.Fatal(part.Err) }
    state := part.Data.(MyState)
    fmt.Printf("state after step: %+v\n", state)
}
```

You'll see one event per superstep, plus an initial event for the state before execution starts.

---

### `StreamModeUpdates` — per-node diffs

Emits only what changed after each node, keyed by node name. Smaller payloads, useful when you want to track which node produced which change.

```go
ch := compiled.Stream(ctx, initialState, graph.StreamModeUpdates)

for part := range ch {
    if part.Err != nil { log.Fatal(part.Err) }
    // Data is map[string]any{nodeName: updatedFields}
    updates := part.Data.(map[string]any)
    for node, delta := range updates {
        fmt.Printf("node %q updated: %v\n", node, delta)
    }
}
```

---

### `StreamModeCustom` — node-emitted events

Nodes can push arbitrary data into the stream mid-execution using the `StreamWriter` from the runtime. Use this for token-level output, progress signals, or debug hints.

```go
builder.AddNode("llm", func(ctx context.Context, s State) (graph.NodeResult, error) {
    writer := graph.GetRuntime(ctx).StreamWriter
    for _, token := range tokens {
        writer(token) // emits a custom stream part
    }
    return graph.NodeWrites(...), nil
})
```

On the consumer side:

```go
ch := compiled.Stream(ctx, initialState, graph.StreamModeCustom)

for part := range ch {
    if part.Err != nil { log.Fatal(part.Err) }
    fmt.Print(part.Data) // whatever the node wrote
}
```

---

### `StreamModeMessages` — LLM message chunks

Emits `graph.MessageChunk` payloads for token-by-token LLM output. The node writes `MessageChunk` values via `StreamWriter` (or uses a prebuilt model integration that does this automatically).

```go
ch := compiled.Stream(ctx, initialState, graph.StreamModeMessages)

for part := range ch {
    if part.Err != nil { log.Fatal(part.Err) }
    chunk := part.Data.(graph.MessageChunk)
    fmt.Print(chunk.Chunk) // stream to user in real time
}
```

---

### `StreamModeCheckpoints` — checkpoint write events

Emits an event each time the runtime saves a checkpoint. The payload is the `checkpoint.CheckpointTuple`. Useful for observability or replication.

```go
ch := compiled.Stream(ctx, initialState, graph.StreamModeCheckpoints)

for part := range ch {
    if part.Err != nil { log.Fatal(part.Err) }
    // part.Data contains checkpoint metadata
    fmt.Printf("checkpoint written: %v\n", part.Data)
}
```

---

### `StreamModeTasks` — task lifecycle events

Emits events when tasks start and finish. A task corresponds to a single node execution within a superstep. Useful for fine-grained progress tracking.

```go
ch := compiled.Stream(ctx, initialState, graph.StreamModeTasks)

for part := range ch {
    if part.Err != nil { log.Fatal(part.Err) }
    fmt.Printf("task event: %v\n", part.Data)
}
```

---

### `StreamModeDebug` — verbose runtime internals

Emits a high volume of runtime-internal events. Primarily useful when you're trying to understand exactly what the scheduler is doing.

```go
ch := compiled.Stream(ctx, initialState, graph.StreamModeDebug)
```

Not recommended in production — the event rate is high and the payloads are internal structures.

---

## Multiple modes at once — `StreamDuplex`

`StreamDuplex` lets you subscribe to several modes in a single pass. All events share the same channel, distinguished by their `Type` field.

```go
ch := compiled.StreamDuplex(ctx, initialState,
    graph.StreamModeUpdates,
    graph.StreamModeCustom,
    graph.StreamModeCheckpoints,
)

for part := range ch {
    if part.Err != nil { log.Fatal(part.Err) }

    switch part.Type {
    case graph.StreamModeUpdates:
        fmt.Printf("[update] %v\n", part.Data)
    case graph.StreamModeCustom:
        fmt.Printf("[custom] %v\n", part.Data)
    case graph.StreamModeCheckpoints:
        fmt.Printf("[checkpoint saved]\n")
    }
}
```

`Stream(ctx, input, mode)` is just `StreamDuplex(ctx, input, mode)` — they're the same method.

---

## Error handling

A `StreamPart` with a non-nil `Err` field is always the last event. After it, the channel closes and no more events are emitted.

```go
for part := range ch {
    if part.Err != nil {
        // Handle terminal error — this is the last event
        log.Printf("graph failed: %v", part.Err)
        break
    }
    // ... handle part.Data
}
```

Not checking `part.Err` is a bug waiting to happen.

---

## Streaming with config

Like `Invoke`, streaming respects `Config` injected via context:

```go
cfg := graph.Config{
    ThreadID:     "session-xyz",
    Checkpointer: saver,
    Debug:        true, // also enables debug events on StreamModeDebug
}
ctx := graph.WithConfig(context.Background(), cfg)

ch := compiled.Stream(ctx, initialState, graph.StreamModeUpdates)
```

---

## Subgraph namespaces

When a graph contains subgraphs, events from child graphs carry a `Namespace` identifying their origin:

```go
for part := range ch {
    if len(part.Namespace) > 0 {
        fmt.Printf("[subgraph: %v] %v\n", part.Namespace, part.Data)
    } else {
        fmt.Printf("[root] %v\n", part.Data)
    }
}
```

---

## Custom stream emit helper

Inside a node, instead of writing raw values to `StreamWriter`, you can write a `graph.StreamEmit` to explicitly declare the mode:

```go
writer := graph.GetRuntime(ctx).StreamWriter
writer(graph.StreamEmit{
    Mode: graph.StreamModeCustom,
    Data: map[string]any{"progress": 0.5},
})
```

This is equivalent to the node writing directly to `StreamModeCustom`. It's useful when a node wants to emit to a specific mode rather than relying on the consumer to infer it.

---

## Performance considerations

- The stream channel has a buffer of 16. If your consumer is slow, the graph will back-pressure and wait. Don't do expensive work inside the `for range` loop without goroutine offloading.
- `StreamModeValues` copies the full state on every superstep. For very large state objects, prefer `StreamModeUpdates`.
- `StreamModeDebug` produces a lot of events — only enable it when actively debugging.

---

## What's next

- [Human-in-the-Loop](./human-in-the-loop.md) — stream events include interrupt notifications
- [Prebuilt Agents](./prebuilt-agents.md) — ReactAgent integrates directly with streaming
