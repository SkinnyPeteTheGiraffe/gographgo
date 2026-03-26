# gographgo

**Almost like LangGraph, but it's Go.**

gographgo is a faithful Go port of [LangGraph](https://github.com/langchain-ai/langgraph) — a framework for building stateful, graph-based workflows and AI agents. It gives you the full power of directed-graph execution (nodes, edges, conditional routing, fan-out, fan-in, interrupt/resume) with Go's type system, goroutines, and performance characteristics doing the heavy lifting.

No Python runtime. No TypeScript build step. Just a Go module.

```
go get github.com/SkinnyPeteTheGiraffe/gographgo
```

> **AI agents and LLM tooling:** [`docs/llms.md`](./docs/llms.md) contains the full technical documentation in a single, condensed file optimised for LLM consumption. Start there instead of crawling individual pages.

---

## Why does this exist?

LangGraph is genuinely great at modelling complex, branching, stateful workflows — especially agentic ones. But if your stack is Go, you've been stuck either:

- Calling out to a Python/JS sidecar process, or
- Re-implementing the logic yourself and losing parity

gographgo gives you the same conceptual model — Pregel superstep execution, checkpointed state, conditional edges, human-in-the-loop interrupts, ReAct agents — compiled directly into your Go binary.

---

## The 30-second pitch

```go
type State struct {
    Message string
    Steps   int
}

builder := graph.NewStateGraph[State]()

builder.AddNode("greet", func(ctx context.Context, s State) (graph.NodeResult, error) {
    return graph.NodeWrites(map[string]graph.Dynamic{
        "Message": graph.Dyn("Hello, world!"),
        "Steps":   graph.Dyn(s.Steps + 1),
    }), nil
})

builder.AddEdge(graph.START, "greet")
builder.AddEdge("greet", graph.END)

compiled, _ := builder.Compile()
result, _ := compiled.Invoke(context.Background(), State{})

fmt.Println(result.Value.(State).Message) // Hello, world!
```

That's it. You define a state type, add nodes that read and write to it, wire them together with edges, compile, and invoke. Everything else — superstep scheduling, channel merging, checkpointing, streaming — is handled by the runtime.

---

## Feature highlights

| Feature | What it does |
|---|---|
| **Typed state graphs** | Generic `StateGraph[State]` — your state is a real Go struct, not a `map[string]any` |
| **Conditional routing** | Branch to different nodes based on current state |
| **Fan-out / fan-in** | Dispatch to multiple nodes in parallel, wait for all with barrier edges |
| **Streaming** | Emit state updates, node diffs, debug events, and custom data as nodes run |
| **Checkpointing** | Persist graph state between invocations — in-memory, SQLite, or Postgres |
| **Interrupt / resume** | Pause mid-execution for human review, then continue exactly where you left off |
| **ReAct agents** | Pre-built agent loop with tool calling and model abstraction |
| **Subgraphs** | Compose full graphs as nodes inside other graphs |
| **Store** | Cross-thread key-value storage accessible from any node |
| **Remote SDK** | HTTP client for LangGraph-compatible servers |

---

## Project layout

```
.
├── pkg/graph         ← core graph API (start here)
├── pkg/checkpoint    ← persistence backends (memory, SQLite, Postgres)
├── pkg/prebuilt      ← ReactAgent, ToolNode, condition helpers
├── pkg/sdk           ← HTTP client for remote LangGraph servers
├── examples/         ← runnable examples (good starting points)
└── docs/             ← in-depth documentation
```

The public API lives entirely in `pkg/*`. Internal plumbing lives in `internal/*` and is not part of the stable surface.

---

## Examples

The [`examples/`](./examples) directory contains self-contained runnable programs:

| Example | What it shows |
|---|---|
| [`hello_graph`](./examples/hello_graph/main.go) | Simplest possible graph — sequential pipeline with typed state |
| [`branching`](./examples/branching/main.go) | Conditional edges: route to `approve` or `reject` based on a score |
| [`human_in_loop`](./examples/human_in_loop/main.go) | `NodeInterrupt` + resume with an `InMemorySaver` checkpointer |
| [`streaming`](./examples/streaming/main.go) | `StreamDuplex` with `StreamModeUpdates` and `StreamModeValues` |

Run any of them with:

```bash
go run ./examples/hello_graph
go run ./examples/branching
go run ./examples/human_in_loop
go run ./examples/streaming
```

---

## Documentation

| Doc | Topic |
|---|---|
| [Concepts](./docs/concepts.md) | Graphs, nodes, edges, state, channels, the Pregel model |
| [Checkpointing](./docs/checkpointing.md) | Persisting state — backends, thread IDs, history |
| [Streaming](./docs/streaming.md) | Streaming modes and how to consume them |
| [Human-in-the-Loop](./docs/human-in-the-loop.md) | Interrupts, resumes, approval flows |
| [Prebuilt Agents](./docs/prebuilt-agents.md) | ReactAgent, ToolNode, tool interfaces |
| [Remote SDK](./docs/sdk.md) | HTTP client for LangGraph-compatible servers |

---

## Before you open a PR

```bash
gofmt -w .
go test ./...
go vet ./...
golangci-lint run ./...   # if golangci-lint is available
```

---

## License

See [LICENSE](./LICENSE).
