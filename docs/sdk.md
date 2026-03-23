# Remote SDK

The `pkg/sdk` package is an HTTP client for LangGraph-compatible servers — including a gographgo server running `cmd/server`. It lets you interact with graphs deployed remotely as if they were local: create threads, kick off runs, stream events, inspect state, and manage assistants.

Use this when:
- Your graph runs in a separate service and your application is the client
- You want to fan out work across multiple server instances
- You're integrating with an existing LangGraph deployment

---

## Connecting

```go
import "github.com/SkinnyPeteTheGiraffe/gographgo/pkg/sdk"

client, err := sdk.New(sdk.Config{
    BaseURL: "http://localhost:8080",
    APIKey:  "optional-api-key",          // or set GOGRAPHGO_API_KEY env var
    Headers: map[string]string{           // optional custom headers
        "X-Tenant-ID": "acme-corp",
    },
})
```

`sdk.Config.HTTPClient` can also be set if you want to bring your own `*http.Client` (custom timeouts, transport, middleware, etc.).

---

## Threads

A **thread** is a named execution timeline — the remote equivalent of a `ThreadID`. Create one before running anything:

```go
thread, err := client.CreateThread(ctx, "my-thread-id", map[string]any{
    "user_id": "user-123",
    "source":  "web",
})
fmt.Println(thread.ID) // "my-thread-id"
```

Or fetch an existing thread:

```go
thread, err := client.GetThread(ctx, "my-thread-id")
```

---

## Runs

A **run** is a single invocation of a graph on a thread. Create one and wait for it to finish:

```go
run, err := client.CreateRun(ctx, "my-thread-id", "my-assistant-id", map[string]any{
    "message": "hello",
}, nil)

// Poll until done
completed, err := client.WaitRun(ctx, "my-thread-id", run.ID, 500*time.Millisecond)
fmt.Println(completed.Status) // "success" | "error" | "interrupted" | "timeout"
fmt.Println(completed.Output)
```

For more control, use `CreateRunWithOptions`:

```go
run, err := client.CreateRunWithOptions(ctx, "my-thread-id", sdk.RunCreateRequest{
    AssistantID: "my-assistant",
    Input:       map[string]any{"query": "..."},
    Config: &sdk.RunConfig{
        RecursionLimit: 50,
        Tags:           []string{"prod"},
    },
    Metadata: map[string]any{"request_id": "req-xyz"},
})
```

### Run statuses

| Status | Meaning |
|---|---|
| `pending` | Queued, not started |
| `running` | Actively executing |
| `success` | Finished successfully |
| `error` | Failed with an error |
| `timeout` | Hit the step timeout |
| `interrupted` | Paused waiting for input |

---

## Streaming runs

Streaming is the main reason to use the SDK — get events as the graph executes rather than waiting for it to finish:

```go
parts, errs := client.StreamRunWithOptions(ctx, "my-thread-id", sdk.RunStreamRequest{
    AssistantID: "my-assistant",
    Input:       map[string]any{"query": "What is gographgo?"},
    StreamModes: []sdk.StreamMode{sdk.StreamModeUpdates, sdk.StreamModeCustom},
})

for {
    select {
    case part, ok := <-parts:
        if !ok { return } // stream closed
        fmt.Printf("[%s] %s\n", part.Event, string(part.Data))

    case err := <-errs:
        if err != nil { log.Fatal(err) }
    }
}
```

### Typed stream events

`StreamRunWithOptionsTyped` gives you a discriminated union instead of raw `json.RawMessage`:

```go
parts, errs := client.StreamRunWithOptionsTyped(ctx, "my-thread-id", req)

for {
    select {
    case part, ok := <-parts:
        if !ok { return }
        switch p := part.(type) {
        case *sdk.ValuesStreamPart:
            fmt.Printf("state: %v\n", p.Data)
        case *sdk.UpdatesStreamPart:
            fmt.Printf("node updates: %v\n", p.Data)
        case *sdk.MessagesStreamPart:
            fmt.Printf("message chunk: %v\n", p.Data)
        case *sdk.CustomStreamPart:
            fmt.Printf("custom: %v\n", p.Data)
        case *sdk.MetadataStreamPart:
            // run started metadata
        case *sdk.UnknownStreamPart:
            // unrecognised event type
        }
    case err := <-errs:
        if err != nil { log.Fatal(err) }
    }
}
```

Available typed parts: `ValuesStreamPart`, `UpdatesStreamPart`, `MessagesStreamPart`, `CustomStreamPart`, `CheckpointStreamPart`, `TasksStreamPart`, `DebugStreamPart`, `MetadataStreamPart`, `UnknownStreamPart`.

### Raw run events

For lower-level access, stream the raw server-sent events:

```go
events, errs := client.StreamRunEvents(ctx, "my-thread-id", run.ID)

for event := range events {
    fmt.Printf("type=%s thread=%s run=%s payload=%v\n",
        event.Type, event.ThreadID, event.RunID, event.Payload)
}
```

---

## Thread state

Inspect the current state of a thread (equivalent to `compiled.GetState`):

```go
state, err := client.GetThreadState(ctx, "my-thread-id")

fmt.Println(state.Values)      // channel values as map[string]any
fmt.Println(state.CheckpointID)
fmt.Println(state.Metadata)
```

See the full history:

```go
history, err := client.ListThreadHistory(ctx, "my-thread-id")

for _, checkpoint := range history {
    fmt.Printf("step %v at %v\n",
        checkpoint.Metadata.Step,
        checkpoint.Checkpoint.TS,
    )
}
```

Update state between runs (equivalent to `compiled.UpdateState`):

```go
err = client.UpdateThreadState(ctx, "my-thread-id", map[string]any{
    "Status": "manually_approved",
})
```

---

## Assistants

An **assistant** is a named, versioned configuration of a graph — a specific graph ID paired with config, context, and metadata. The Assistants API lets you manage these configurations server-side.

```go
// Create
assistant, err := client.Assistants.Create(ctx, sdk.AssistantCreateRequest{
    GraphID:     "my-graph",
    Name:        "Customer Support Agent",
    Description: "Handles tier-1 support queries",
    Config: map[string]any{
        "max_steps": 30,
    },
    Metadata: map[string]any{
        "team": "support",
    },
})

// Retrieve
assistant, err = client.Assistants.Get(ctx, assistant.ID)

// List with filters
results, err := client.Assistants.Search(ctx, sdk.AssistantSearchRequest{
    Metadata: map[string]any{"team": "support"},
    Limit:    10,
})

// Update
updated, err := client.Assistants.Update(ctx, assistant.ID, sdk.AssistantUpdateRequest{
    Name: "Customer Support Agent v2",
})

// Delete
err = client.Assistants.Delete(ctx, assistant.ID)
```

Assistants maintain a version history — use `client.Assistants.GetVersions` to list past configurations.

---

## Scheduled runs (Cron)

Run a graph on a cron schedule:

```go
cron, err := client.Cron.Create(ctx, sdk.CronCreateRequest{
    AssistantID: "my-assistant",
    Schedule:    "0 9 * * MON-FRI", // 9am weekdays
    Payload: map[string]any{
        "report_type": "daily_summary",
    },
})

// List active crons
crons, err := client.Cron.Search(ctx, sdk.CronSearchRequest{
    AssistantID: &assistant.ID,
})

// Delete
err = client.Cron.Delete(ctx, cron.ID)
```

---

## Store (cross-thread KV)

The server exposes a global key-value store accessible from any graph and any thread. Namespaces partition the store:

```go
// Put
err := client.Store.Put(ctx, sdk.StoreItemPutRequest{
    Namespace: []string{"users", "alice"},
    Key:       "preferences",
    Value:     map[string]any{"theme": "dark", "lang": "en"},
    TTL:       ptr(7 * 24 * time.Hour), // optional expiry
})

// Get
item, err := client.Store.Get(ctx, []string{"users", "alice"}, "preferences")
fmt.Println(item.Value)

// Search
results, err := client.Store.Search(ctx, sdk.StoreSearchRequest{
    Namespace: []string{"users"},
    Query:     "alice",
    Limit:     5,
})

// List namespaces
namespaces, err := client.Store.ListNamespaces(ctx, sdk.StoreNamespaceListRequest{
    Prefix: []string{"users"},
})

// Delete
err = client.Store.Delete(ctx, []string{"users", "alice"}, "preferences")
```

---

## Server info

Check server version and capabilities:

```go
info, err := client.GetInfo(ctx)
fmt.Println(info.Version)
fmt.Println(info.Capabilities)
```

---

## Stream modes reference

| `sdk.StreamMode` | What you get |
|---|---|
| `StreamModeValues` | Full state after each superstep |
| `StreamModeUpdates` | Per-node state diffs |
| `StreamModeMessages` | Message/token chunks |
| `StreamModeCustom` | Node-emitted custom data |
| `StreamModeCheckpoints` | Checkpoint write events |
| `StreamModeTasks` | Task start/finish events |
| `StreamModeDebug` | Runtime internals |
| `StreamModeEvents` | High-level run lifecycle events |

---

## What's next

- [Concepts](./concepts.md) — understand what graphs, nodes, and threads mean
- [Checkpointing](./checkpointing.md) — how threads persist their state
- [Human-in-the-Loop](./human-in-the-loop.md) — interrupt/resume via the SDK
