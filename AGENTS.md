# AGENTS Instructions

gographgo is a Go-native graph execution framework for building stateful, agentic workflows. It is its own project — not a port, not a migration target. Design decisions should be made for Go first.

---

## Project layout

Single Go module (`go.mod`), organised by Go package boundaries.

- `cmd/graph` — graph-focused executable entrypoint.
- `cmd/server` — server executable entrypoint.
- `internal/cli` — private CLI implementation.
- `internal/compile` — private compile/planning internals.
- `internal/runtime` — private runtime orchestration.
- `internal/server` — private server internals.
- `internal/testutil` — internal test helpers shared across packages.
- `pkg/graph` — public core graph framework (start here for most work).
- `pkg/checkpoint` — checkpoint interfaces and implementations.
- `pkg/checkpoint/postgres` — Postgres backend.
- `pkg/checkpoint/sqlite` — SQLite backend.
- `pkg/prebuilt` — higher-level agent and tool APIs.
- `pkg/sdk` — HTTP client for remote graph servers.
- `docs/` — documentation (Markdown, one file per topic).
- `examples/` — self-contained runnable examples.

Dependency direction rules — never violate these:

- `cmd/*` may import `internal/*` and `pkg/*`.
- `internal/*` may import `pkg/*` and other `internal/*`.
- `pkg/*` must not import `internal/*`.
- `pkg/checkpoint/postgres` and `pkg/checkpoint/sqlite` are implementations under `pkg/checkpoint`.

```
.
├── cmd
│   ├── graph
│   └── server
├── internal
│   ├── cli
│   ├── compile
│   ├── runtime
│   ├── server
│   └── testutil
├── pkg
│   ├── checkpoint
│   │   ├── postgres
│   │   └── sqlite
│   ├── graph
│   ├── prebuilt
│   └── sdk
├── docs
└── examples
```

---

## Architecture principles

- **Go-native first.** Design APIs for Go idioms — small interfaces, explicit error returns, value semantics, context propagation. Do not reproduce patterns from other languages just because they existed upstream.
- **Public API lives in `pkg/*`.** If it is not in `pkg/`, it is not part of the stable surface.
- **Keep interfaces small.** Prefer composing small interfaces over large ones. If a caller only needs one method, the interface should expose one method.
- **Avoid unnecessary abstraction.** Do not introduce layers, wrappers, or indirection unless there is a concrete, present need. Three similar lines are better than a premature abstraction.
- **The runtime is the source of truth.** `internal/runtime` drives execution. When understanding behaviour, read the runtime before reading the builder.

---

## Working in this codebase

### Before touching code

Read the relevant `pkg/*` files before suggesting changes. Do not propose modifications to code you haven't read. The public API surface is large — understand the existing design before adding to it.

### Key files to know

| File | Why it matters |
|---|---|
| `pkg/graph/state_graph.go` | Builder API — `AddNode`, `AddEdge`, `Compile`, etc. |
| `pkg/graph/compiled_graph.go` | `Invoke`, `Stream`, `GetState`, `UpdateState` |
| `pkg/graph/pregel.go` + `pregel_loop.go` | Core superstep execution engine |
| `pkg/graph/types.go` | All shared types: `Command`, `Route`, `Send`, `StreamPart`, etc. |
| `pkg/graph/node.go` | `NodeResult`, `Node[State]`, `ChannelWrite` |
| `pkg/graph/channel.go` + `channel_*.go` | Channel implementations |
| `pkg/graph/interrupt.go` | `NodeInterrupt` — read before touching interrupt/resume |
| `pkg/checkpoint/memory.go` | Reference implementation of `Saver` |
| `pkg/prebuilt/agent.go` | `ReactAgent` and `CreateReactAgent` |

### State and channels

Every field in the state struct maps to a channel. Writes are buffered per superstep and merged after all tasks in the step complete. The default channel (`LastValue`) rejects multiple writes in the same superstep — use a reducer or `BinaryOperatorAggregate` when fan-out nodes write to the same field.

### Adding a node

Nodes are `func(ctx context.Context, state State) (NodeResult, error)`. Return `NodeWrites(map[string]Dynamic{...})` for partial updates, `NodeState(Dyn(newState))` for full replacement, or `NodeCommand(cmd)` for routing control. Return `NoNodeResult()` for read-only nodes.

### Adding tests

- Unit tests go in `_test.go` files alongside the package (`package graph` or `package graph_test`).
- Integration tests that require external services (Postgres) must call `t.Skip(...)` when unavailable or when `-short` is passed.
- Use `internal/testutil` helpers rather than duplicating setup logic.
- Tests must pass with `-race`. Do not use `time.Sleep` as a synchronisation mechanism.

### Docs and examples

- `docs/` contains one Markdown file per topic. Update the relevant doc when changing public API behaviour.
- `examples/` contains standalone `main` packages. Each example must compile and run correctly with `go run ./examples/<name>`.
- Do not add doc comments that restate the function signature. Comments should explain why, not what.

---

## Required checks before a PR

```bash
gofmt -w .
go build ./...
go vet ./...
go test -race ./...
golangci-lint run ./...   # if available
```

CI runs all of these. A PR that breaks any of them will not merge. The CI config is at `.github/workflows/ci.yml` and the lint config is at `.golangci.yml`.

---

## Coding conventions

- Standard Go doc comments — `//` above the declaration, full sentence, ends with a period.
- Single backticks for inline code in comments.
- No Sphinx-style, JSDoc, or Python docstring formatting.
- Error strings are lowercase and do not end with punctuation.
- Prefer returning a descriptive `error` over panicking; only panic for programmer errors that indicate a broken invariant (not runtime conditions).
- Use `context.Context` as the first argument to any function that may block, do I/O, or needs cancellation.
- Unexported types and functions need comments only when their purpose is non-obvious.
