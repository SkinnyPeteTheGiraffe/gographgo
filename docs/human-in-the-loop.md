# Human-in-the-Loop

Sometimes a graph needs to pause, ask a human something, and then continue. Maybe you want an agent to propose an action before it executes it. Maybe you need sign-off before an irreversible operation. Maybe the workflow just needs a human decision that can't be automated.

gographgo handles this through **interrupts** — a first-class mechanism to halt execution mid-graph, surface a value to the caller, and resume from exactly where you left off once a response arrives.

A `Checkpointer` is required for interrupt/resume across separate `Invoke` calls. The graph can still be interrupted without one, but it can't be resumed.

---

## The pattern

1. A node calls `graph.NodeInterrupt(ctx, value)` — this halts the node and surfaces `value` to the caller.
2. The caller inspects `result.Interrupts`, does whatever it needs (shows a prompt, calls an API, asks a human), and gets a response.
3. The caller invokes the graph again on the same thread, passing the response via `CONFIG_KEY_RESUME`.
4. The runtime re-runs the interrupted node from the top. When it hits `NodeInterrupt` again, it returns the resume value instead of halting.
5. The node continues normally, and the graph proceeds.

---

## `NodeInterrupt`

```go
builder.AddNode("confirm", func(ctx context.Context, s State) (graph.NodeResult, error) {
    // Pause here and surface the pending action to the caller.
    // On the first run: halts, returns nothing.
    // On resume: returns the Dynamic value provided by the caller.
    response := graph.NodeInterrupt(ctx, graph.Dyn(map[string]any{
        "action": "delete_record",
        "id":     s.RecordID,
    }))

    if response.Value() != "approved" {
        return graph.NodeWrites(map[string]graph.Dynamic{
            "Status": graph.Dyn("cancelled"),
        }), nil
    }

    // Proceed with the deletion
    err := deleteRecord(s.RecordID)
    if err != nil {
        return graph.NodeResult{}, err
    }
    return graph.NodeWrites(map[string]graph.Dynamic{
        "Status": graph.Dyn("deleted"),
    }), nil
})
```

A few things to know about `NodeInterrupt`:
- It takes any `Dynamic` value — this is what the caller sees in `result.Interrupts[i].Value`.
- Each call is assigned a unique `ID` (UUID). This ID is how you target specific interrupts when there are multiple.
- Multiple `NodeInterrupt` calls within the same node are matched **positionally** to resume values on the next run.
- The function **panics** internally (with a recoverable signal) to halt the node — this is intentional. Don't `recover()` from it.

---

## First invocation — catching the interrupt

```go
saver := checkpoint.NewInMemorySaver()
compiled, _ := builder.Compile(graph.CompileOptions{Checkpointer: saver})

cfg := graph.Config{ThreadID: "thread-1", Checkpointer: saver}
ctx := graph.WithConfig(context.Background(), cfg)

result, err := compiled.Invoke(ctx, initialState)
// err is nil — an interrupt is not an error

if len(result.Interrupts) > 0 {
    iv := result.Interrupts[0]
    fmt.Printf("Graph paused: %v (id=%s)\n", iv.Value.Value(), iv.ID)

    // ... present the question/action to a human ...
}
```

`result.Interrupts` is non-empty when the graph paused. The graph has already saved a checkpoint at this point. `result.Value` contains the state up to the interrupted node (fields written by nodes that already completed are preserved).

---

## Resuming with a positional value

The simplest resume: a single interrupt, a single response. Pass the answer via `CONFIG_KEY_RESUME`:

```go
resumeCfg := graph.Config{
    ThreadID:     "thread-1",
    Checkpointer: saver,
    Metadata: map[string]any{
        graph.CONFIG_KEY_RESUME: "approved", // matched positionally to the first NodeInterrupt call
    },
}
ctx = graph.WithConfig(context.Background(), resumeCfg)

result, err = compiled.Invoke(ctx, State{}) // input is ignored; state is restored from checkpoint
```

The runtime restores the saved state, re-runs the interrupted node, and when it hits `NodeInterrupt`, it returns `Dyn("approved")` instead of halting. The node carries on.

---

## Resuming with a map when multiple interrupts are pending

If multiple nodes interrupted simultaneously (fan-out scenario), you need to target each interrupt by its ID:

```go
// Get the IDs from the first result
idA := result.Interrupts[0].ID
idB := result.Interrupts[1].ID

resumeCfg := graph.Config{
    ThreadID:     "thread-1",
    Checkpointer: saver,
    Metadata: map[string]any{
        graph.CONFIG_KEY_RESUME_MAP: map[string]any{
            idA: "yes",
            idB: "no",
        },
    },
}
```

When `CONFIG_KEY_RESUME_MAP` is set, each interrupt gets matched by its ID rather than position.

---

## Interrupt before / after a node (no code changes needed)

If you want to pause without changing node code, configure it at compile time:

```go
compiled, _ := builder.Compile(graph.CompileOptions{
    Checkpointer:    saver,
    InterruptBefore: []string{"dangerous_node"},
    InterruptAfter:  []string{"write_to_db"},
})
```

- `InterruptBefore` — graph pauses right before the named node runs. The interrupt value is the node's name.
- `InterruptAfter` — graph pauses right after the named node runs. The interrupt value is the node's name.

Resume works the same way. The interrupt value from compile-time interrupts is just the node name string; you can pass anything as the resume value (or `nil`/`""` if the node doesn't use it).

---

## The `Command` approach — routing on resume

For more control, a node can return a `Command` instead of calling `NodeInterrupt`. Commands let the node specify both a state update and a routing decision:

```go
builder.AddNode("check", func(ctx context.Context, s State) (graph.NodeResult, error) {
    if s.NeedsApproval {
        // Interrupt and tell the runtime which node to resume at
        return graph.NodeCommand(&graph.Command{
            Goto: graph.RouteTo("await_approval"),
            Update: map[string]graph.Dynamic{
                "PendingAction": graph.Dyn(s.ProposedAction),
            },
        }), nil
    }
    return graph.NodeWrites(map[string]graph.Dynamic{
        "Status": graph.Dyn("auto-approved"),
    }), nil
})
```

`Command.Goto` lets you jump to any node unconditionally, bypassing the normal edge routing. Combined with `NodeInterrupt` or compile-time `InterruptBefore/After`, this gives you full control over the pause/resume flow.

---

## Checking for pending interrupts without invoking

If you want to inspect the graph's current state to see if it's waiting for input:

```go
snapshot, err := compiled.GetState(ctx, &graph.Config{
    ThreadID:     "thread-1",
    Checkpointer: saver,
})

if len(snapshot.Interrupts) > 0 {
    fmt.Printf("Thread is paused at: %v\n", snapshot.Interrupts)
}
fmt.Printf("Nodes scheduled next: %v\n", snapshot.Next)
```

---

## Prebuilt: `ReactAgent` human interrupt support

The `prebuilt.ReactAgent` has a higher-level wrapper for human approval flows:

```go
import "github.com/SkinnyPeteTheGiraffe/gographgo/pkg/prebuilt"

agent := prebuilt.CreateReactAgent(model, tools,
    prebuilt.WithHumanInterruptSupport(
        prebuilt.HumanInterruptConfig{
            AllowAccept:  true,
            AllowIgnore:  true,
            AllowRespond: true,
            AllowEdit:    true,
        },
        "Review this tool call before it executes",
    ),
)

result, err := agent.Invoke(ctx, initialState, cfg)
if result.Pending != nil {
    // Agent is waiting — show the tool calls to the user
    for _, call := range result.Pending.Calls {
        fmt.Printf("Tool: %s, Args: %v\n", call.Name, call.Args)
    }

    // Resume with a response
    result, err = agent.Resume(ctx, cfg,
        prebuilt.AcceptHumanResponse(), // or IgnoreHumanResponse(), EditHumanResponse(...)
    )
}
```

The `HumanInterruptConfig` declares which response types are valid (`Accept`, `Ignore`, `Respond`, `Edit`). The `HumanResponse` constructors produce the correct resume payload.

---

## Tips and gotchas

**State is restored, not re-passed.** On resume, the `input` argument to `Invoke` is ignored for any channel already present in the checkpoint. Only channels not yet in the checkpoint are populated from input. You can safely pass a zero value.

**Node re-runs from the top.** When resuming, the entire interrupted node function runs again from the beginning — not from the line after `NodeInterrupt`. This means side effects before `NodeInterrupt` will be re-executed. Design your nodes so that pre-interrupt work is idempotent, or use the positional resume to guard it.

**Multiple `NodeInterrupt` calls in one node are fine.** They're matched to resume values by position (or by ID if you use `CONFIG_KEY_RESUME_MAP`). The node must hit each call in the same order on re-run for matching to work.

**Without a checkpointer, you can still interrupt.** `result.Interrupts` will be populated. But you cannot resume — the next `Invoke` starts fresh. Only add a checkpointer when you actually need resume capability.

---

## What's next

- [Checkpointing](./checkpointing.md) — required for interrupt/resume across calls
- [Prebuilt Agents](./prebuilt-agents.md) — ReactAgent's built-in human approval loop
