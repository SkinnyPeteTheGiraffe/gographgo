# Checkpointing

Checkpointing is how gographgo remembers what happened. Without it, every `Invoke` call starts fresh. With it, the graph saves a snapshot after every superstep — so you can resume interrupted runs, inspect past state, fork execution timelines, and build stateful multi-turn conversations.

If you're building anything that involves:
- **Human-in-the-loop** — you need a checkpointer (the resume has to pick up from somewhere)
- **Long-running workflows** — tolerate failure, replay from last saved step
- **Multi-turn agents** — each conversation turn resumes the previous thread
- **Time travel / debugging** — replay from any past snapshot

...then you need a checkpointer.

---

## The minimal setup

Attach a `Saver` when you compile the graph and supply a `ThreadID` when you invoke it:

```go
import "github.com/SkinnyPeteTheGiraffe/gographgo/pkg/checkpoint"

saver := checkpoint.NewInMemorySaver()

compiled, err := builder.Compile(graph.CompileOptions{
    Checkpointer: saver,
})

cfg := graph.Config{
    ThreadID:     "my-thread",
    Checkpointer: saver,
}
result, err := compiled.Invoke(graph.WithConfig(ctx, cfg), initialState)
```

That's it. The graph will save a checkpoint after every superstep for the thread `"my-thread"`.

---

## Backends

### `checkpoint.NewInMemorySaver()`

Thread-safe, lives in process memory. Great for tests, development, and single-server deployments where persistence across restarts isn't needed.

```go
saver := checkpoint.NewInMemorySaver()
```

### `checkpoint/sqlite`

Single-file SQLite persistence. Good for edge deployments, local tools, and anything that needs real durability without a server.

```go
import "github.com/SkinnyPeteTheGiraffe/gographgo/pkg/checkpoint/sqlite"

saver, err := sqlite.NewSaver("./checkpoints.db")
```

### `checkpoint/postgres`

PostgreSQL backend for multi-instance / production deployments.

```go
import (
    "github.com/SkinnyPeteTheGiraffe/gographgo/pkg/checkpoint"
    "github.com/SkinnyPeteTheGiraffe/gographgo/pkg/checkpoint/postgres"
)

saver, err := postgres.OpenWithOptions(
    "postgres://user:pass@host/db",
    checkpoint.JSONSerializer{},
    postgres.Options{}, // AutoMigrate=false (no runtime DDL)
)
```

Production recommendation: pre-provision the Postgres schema in CI/CD and keep
`AutoMigrate` disabled in app/runtime code.

If you want library-managed setup (local development or explicit opt-in), use
either of these flows:

```go
// Explicit migration call.
saver, err := postgres.OpenWithOptions(dsn, checkpoint.JSONSerializer{}, postgres.Options{})
if err != nil {
    return err
}
if err := saver.Migrate(ctx); err != nil {
    return err
}
```

```go
// One-step opt-in migration at construction time.
saver, err := postgres.OpenWithOptions(
    dsn,
    checkpoint.JSONSerializer{},
    postgres.Options{AutoMigrate: true},
)
```

For compatibility with older behavior, `postgres.OpenAutoMigrate(...)` and
`postgres.NewAutoMigrate(...)` are also available.

---

## Thread IDs

A **thread** is a named execution timeline. All checkpoints for the same `ThreadID` form an ordered history of supersteps for that thread.

Think of threads as conversation sessions, workflow instances, or job runs. One user might have one thread per chat session. A batch job might assign one thread per item.

```go
// Each user gets their own isolated timeline
cfg := graph.Config{
    ThreadID:     fmt.Sprintf("user-%s", userID),
    Checkpointer: saver,
}
```

There's no registration step — just use any string as `ThreadID` and the backend creates a slot for it automatically.

---

## Resuming

To resume a thread, invoke the graph with the same `ThreadID`. The runtime loads the latest checkpoint and continues from where it left off.

```go
// First invocation — graph runs until interrupted or complete
first, _ := compiled.Invoke(graph.WithConfig(ctx, cfg), initialState)

// Second invocation — graph resumes the thread automatically
second, _ := compiled.Invoke(graph.WithConfig(ctx, cfg), State{})
```

The `State{}` you pass on resume is generally ignored for fields that already exist in the checkpoint — the persisted state takes precedence. New fields not yet in the checkpoint will be populated from the input.

---

## Reading state

You can read the current state of a thread without running it:

```go
snapshot, err := compiled.GetState(ctx, &graph.Config{
    ThreadID:     "my-thread",
    Checkpointer: saver,
})

fmt.Println(snapshot.Values)   // current channel values
fmt.Println(snapshot.Next)     // nodes scheduled to run next
fmt.Println(snapshot.Metadata) // checkpoint metadata
```

---

## State history

To see all past snapshots for a thread (newest first):

```go
history, err := compiled.GetStateHistory(ctx, &graph.Config{
    ThreadID:     "my-thread",
    Checkpointer: saver,
})

for _, snapshot := range history {
    fmt.Printf("step %d — %v\n",
        snapshot.Metadata["step"],
        snapshot.CreatedAt,
    )
}
```

---

## Forking from a past checkpoint

To replay execution from an earlier point in history, pass a `CheckpointID` in the config:

```go
// Get the checkpoint you want to fork from
history, _ := compiled.GetStateHistory(ctx, &graph.Config{ThreadID: "my-thread", Checkpointer: saver})
target := history[3] // some past snapshot

// Invoke from that specific checkpoint
forkCfg := graph.Config{
    ThreadID:     "my-thread-fork",  // new thread to avoid polluting the original
    CheckpointID: target.Config.CheckpointID,
    Checkpointer: saver,
}
result, _ := compiled.Invoke(graph.WithConfig(ctx, forkCfg), State{})
```

This is "time travel" — you can branch from any past decision point to explore alternative paths.

---

## Directly updating state

Sometimes you want to modify state between invocations — patching a field, injecting a correction, or manually triggering a node re-run:

```go
err := compiled.UpdateState(ctx, &graph.Config{
    ThreadID:     "my-thread",
    Checkpointer: saver,
}, map[string]any{
    "Score":   95,
    "Comment": "manually approved",
})
```

The update is written as a new checkpoint entry in the thread's history. The next `Invoke` picks up from there.

If you need the updated config (e.g. to get the new `CheckpointID`):

```go
newCfg, err := compiled.UpdateStateWithConfig(ctx, &graph.Config{...}, updates)
```

---

## Durability mode

The `Durability` field on `Config` controls when checkpoints are written to the backend:

| Mode | Behaviour |
|---|---|
| `graph.DurabilityAsync` (default) | Checkpoints are written in the background — lower latency |
| `graph.DurabilitySync` | Each checkpoint is written before the next superstep starts — safer under sudden failure |
| `graph.DurabilityExit` | Only one checkpoint is written when the graph exits — minimal I/O |

```go
cfg := graph.Config{
    ThreadID:     "job-42",
    Checkpointer: saver,
    Durability:   graph.DurabilitySync,
}
```

---

## Checkpoint namespaces

Subgraphs get their own checkpoint namespace derived from the parent's. You'll only need to think about `CheckpointNS` if you're writing a custom backend or doing deep introspection — the runtime manages it automatically.

---

## The `Saver` interface

If none of the built-in backends fit, implement your own:

```go
type Saver interface {
    GetTuple(ctx context.Context, config *Config) (*CheckpointTuple, error)
    Put(ctx context.Context, config *Config, cp *Checkpoint, meta *CheckpointMetadata) (*Config, error)
    PutWrites(ctx context.Context, config *Config, writes []PendingWrite, taskID string) error
    List(ctx context.Context, config *Config, opts ListOptions) ([]*CheckpointTuple, error)
    DeleteThread(ctx context.Context, threadID string) error
    DeleteForRuns(ctx context.Context, runIDs []string) error
    CopyThread(ctx context.Context, sourceThreadID, targetThreadID string) error
    Prune(ctx context.Context, threadIDs []string, strategy PruneStrategy) error
}
```

A checkpoint stores:
- `ChannelValues` — the serialised state of every channel
- `ChannelVersions` — monotonic version tokens per channel (for optimistic concurrency)
- `VersionsSeen` — which version each node last processed (prevents double-execution)
- `PendingSends` — queued `Send` writes not yet consumed

---

## Pruning

To avoid unbounded growth, prune old checkpoints:

```go
// Delete all but the latest checkpoint per namespace for a set of threads
saver.Prune(ctx, []string{"thread-1", "thread-2"}, checkpoint.PruneStrategyKeepLatest)

// Delete all checkpoints for a thread entirely
saver.DeleteThread(ctx, "old-thread")
```

---

## What's next

- [Human-in-the-Loop](./human-in-the-loop.md) — checkpointing is required for interrupt/resume
- [Streaming](./streaming.md) — watch state evolve in real time as checkpoints are written
