package graph

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/checkpoint"
)

const entrypointSaveChannel = "__entrypoint_save__"

type functionalRuntimeContextKey struct{}
type entrypointPreviousContextKey struct{}

type functionalRuntime struct {
	emit   func(StreamPart)
	nextID atomic.Uint64
}

func newFunctionalRuntime(emit func(StreamPart)) *functionalRuntime {
	return &functionalRuntime{emit: emit}
}

func (r *functionalRuntime) emitTask(name string, input any, event string) string {
	if r == nil || r.emit == nil {
		return ""
	}
	id := fmt.Sprintf("task-%d", r.nextID.Add(1))
	r.emit(StreamPart{
		Type: StreamModeTasks,
		Data: map[string]any{
			"id":    id,
			"name":  name,
			"input": input,
			"event": event,
		},
	})
	return id
}

func (r *functionalRuntime) emitTaskFinish(name, id string) {
	if r == nil || r.emit == nil {
		return
	}
	r.emit(StreamPart{
		Type: StreamModeTasks,
		Data: map[string]any{
			"id":    id,
			"name":  name,
			"event": "finish",
		},
	})
}

// WithEntrypointPrevious stores the previous persisted entrypoint value in ctx.
func WithEntrypointPrevious(ctx context.Context, value any) context.Context {
	return context.WithValue(ctx, entrypointPreviousContextKey{}, value)
}

// GetEntrypointPrevious returns the previous persisted entrypoint value.
func GetEntrypointPrevious(ctx context.Context) any {
	return ctx.Value(entrypointPreviousContextKey{})
}

// Task wraps a typed function as a reusable callable unit.
//
// Tasks run synchronously via Invoke or concurrently via Async.
type Task[Input, Output any] struct {
	fn   func(ctx context.Context, input Input) (Output, error)
	name string
}

// NewTask creates a named callable task.
func NewTask[Input, Output any](
	name string,
	fn func(ctx context.Context, input Input) (Output, error),
) Task[Input, Output] {
	if name == "" {
		panic("task name must not be empty")
	}
	if fn == nil {
		panic("task function must not be nil")
	}
	return Task[Input, Output]{name: name, fn: fn}
}

// Name returns the task's declared name.
func (t Task[Input, Output]) Name() string {
	return t.name
}

// Invoke executes the task synchronously.
func (t Task[Input, Output]) Invoke(ctx context.Context, input Input) (Output, error) {
	var zero Output
	if t.fn == nil {
		return zero, fmt.Errorf("task '%s' has no function", t.name)
	}
	runtime, _ := ctx.Value(functionalRuntimeContextKey{}).(*functionalRuntime)
	startID := runtime.emitTask(t.name, input, "start")
	defer runtime.emitTaskFinish(t.name, startID)
	return t.fn(ctx, input)
}

// TaskFuture is an asynchronous task result.
type TaskFuture[Output any] struct {
	result Output
	err    error
	done   chan struct{}
}

// Await waits for the task result.
func (f *TaskFuture[Output]) Await() (Output, error) {
	<-f.done
	return f.result, f.err
}

// Async executes the task in a goroutine and returns a future.
func (t Task[Input, Output]) Async(ctx context.Context, input Input) *TaskFuture[Output] {
	fut := &TaskFuture[Output]{done: make(chan struct{})}
	go func() {
		defer close(fut.done)
		fut.result, fut.err = t.Invoke(ctx, input)
	}()
	return fut
}

// EntrypointResult is the result produced by an entrypoint workflow function.
//
// Value is returned to the caller. SaveValue is persisted when checkpointer
// configuration is present. If SaveSet is false, Value is persisted.
type EntrypointResult[Output any] struct {
	Value     Output
	SaveValue any
	SaveSet   bool
}

// Return creates an entrypoint result where the persisted value equals value.
func Return[Output any](value Output) EntrypointResult[Output] {
	return EntrypointResult[Output]{Value: value}
}

// ReturnWithSave creates an entrypoint result with a distinct persisted value.
func ReturnWithSave[Output any](value Output, save any) EntrypointResult[Output] {
	return EntrypointResult[Output]{Value: value, SaveValue: save, SaveSet: true}
}

// EntrypointOptions configures a compiled entrypoint workflow.
type EntrypointOptions struct {
	Checkpointer checkpoint.Saver
	Store        Store
	Context      any
	Durability   DurabilityMode
	Name         string
	Debug        bool
}

// EntrypointWorkflow is an executable functional workflow.
//
// It supports Invoke and Stream interfaces similar to compiled graphs.
type EntrypointWorkflow[Input, Output any] struct {
	checkpointer checkpoint.Saver
	store        Store
	contextValue any
	fn           func(ctx context.Context, input Input) (EntrypointResult[Output], error)
	name         string
	durability   DurabilityMode
	debug        bool
}

// NewEntrypoint creates a functional workflow entrypoint.
func NewEntrypoint[Input, Output any](
	name string,
	fn func(ctx context.Context, input Input) (EntrypointResult[Output], error),
	options ...EntrypointOptions,
) *EntrypointWorkflow[Input, Output] {
	if name == "" {
		panic("entrypoint name must not be empty")
	}
	if fn == nil {
		panic("entrypoint function must not be nil")
	}
	var opts EntrypointOptions
	if len(options) > 0 {
		opts = options[0]
	}
	if opts.Name == "" {
		opts.Name = name
	}
	return &EntrypointWorkflow[Input, Output]{
		name:         opts.Name,
		fn:           fn,
		checkpointer: opts.Checkpointer,
		store:        opts.Store,
		contextValue: opts.Context,
		debug:        opts.Debug,
		durability:   opts.Durability,
	}
}

// Invoke runs the workflow and returns the final value.
func (w *EntrypointWorkflow[Input, Output]) Invoke(ctx context.Context, input Input) (GraphOutput, error) {
	out, err := w.run(ctx, input, nil, nil)
	if err != nil {
		return GraphOutput{}, err
	}
	return GraphOutput{Value: out}, nil
}

// Stream runs the workflow and emits events for mode.
func (w *EntrypointWorkflow[Input, Output]) Stream(ctx context.Context, input Input, mode StreamMode) <-chan StreamPart {
	return w.StreamDuplex(ctx, input, mode)
}

// StreamDuplex runs the workflow once and emits events for all provided modes.
func (w *EntrypointWorkflow[Input, Output]) StreamDuplex(ctx context.Context, input Input, modes ...StreamMode) <-chan StreamPart {
	out := make(chan StreamPart, 16)
	go func() {
		defer close(out)
		modeSet := newStreamModeSet(modes...)
		if modeSet.empty() {
			modeSet = newStreamModeSet(StreamModeValues)
		}
		if modeSet.enabled(StreamModeValues) {
			out <- StreamPart{Type: StreamModeValues, Data: input}
		}
		_, err := w.run(ctx, input, out, modeSet)
		if err != nil {
			out <- StreamPart{Err: fmt.Errorf("entrypoint '%s': %w", w.name, err)}
		}
	}()
	return out
}

func (w *EntrypointWorkflow[Input, Output]) run(
	ctx context.Context,
	input Input,
	streamOut chan<- StreamPart,
	streamModes streamModeSet,
) (Output, error) {
	var zero Output
	if w == nil || w.fn == nil {
		return zero, fmt.Errorf("entrypoint is not configured")
	}
	config := GetConfig(ctx)
	config = w.mergeConfig(config)
	ctx = WithConfig(ctx, config)

	store := effectiveStore(config, w.store)
	if store != nil {
		ctx = WithStore(ctx, store)
	}

	previous, tuple, metaStep, err := loadEntrypointPrevious(ctx, config)
	if err != nil {
		return zero, err
	}
	ctx = WithEntrypointPrevious(ctx, previous)
	ctx = WithRuntime(ctx, &Runtime{Context: w.contextValue, Store: store, Previous: previous})

	emitter := func(part StreamPart) {
		if streamOut == nil {
			return
		}
		if !streamModes.enabled(part.Type) {
			return
		}
		streamOut <- part
	}
	ctx = context.WithValue(ctx, functionalRuntimeContextKey{}, newFunctionalRuntime(emitter))

	result, err := w.fn(ctx, input)
	if err != nil {
		return zero, err
	}

	saveValue := any(result.Value)
	if result.SaveSet {
		saveValue = result.SaveValue
	}
	if err := persistEntrypointSave(ctx, config, tuple, metaStep, saveValue); err != nil {
		return zero, err
	}

	emitter(StreamPart{Type: StreamModeValues, Data: result.Value})
	return result.Value, nil
}

func (w *EntrypointWorkflow[Input, Output]) mergeConfig(config Config) Config {
	if w.checkpointer != nil && config.Checkpointer == nil {
		config.Checkpointer = w.checkpointer
	}
	if w.store != nil && config.Store == nil {
		config.Store = w.store
	}
	if config.RecursionLimit == 0 {
		config.RecursionLimit = DefaultConfig().RecursionLimit
	}
	if w.debug {
		config.Debug = true
	}
	if config.Durability == "" {
		if w.durability != "" {
			config.Durability = w.durability
		} else {
			config.Durability = DurabilityAsync
		}
	}
	return config
}

func loadEntrypointPrevious(ctx context.Context, config Config) (any, *checkpoint.CheckpointTuple, int, error) {
	if config.Checkpointer == nil || config.ThreadID == "" {
		return nil, nil, -1, nil
	}
	tuple, err := config.Checkpointer.GetTuple(ctx, config.CheckpointConfig())
	if err != nil {
		return nil, nil, -1, fmt.Errorf("loading entrypoint checkpoint: %w", err)
	}
	step := -1
	if tuple != nil && tuple.Metadata != nil {
		step = tuple.Metadata.Step
	}
	if tuple == nil || tuple.Checkpoint == nil || tuple.Checkpoint.ChannelValues == nil {
		return nil, tuple, step, nil
	}
	return tuple.Checkpoint.ChannelValues[entrypointSaveChannel], tuple, step, nil
}

func persistEntrypointSave(ctx context.Context, config Config, tuple *checkpoint.CheckpointTuple, step int, saveValue any) error {
	if config.Checkpointer == nil || config.ThreadID == "" {
		return nil
	}

	var cp *checkpoint.Checkpoint
	if tuple != nil && tuple.Checkpoint != nil {
		cp = checkpoint.CopyCheckpoint(tuple.Checkpoint)
	} else {
		cp = checkpoint.EmptyCheckpoint(fmt.Sprintf("%d", time.Now().UnixNano()))
	}
	if cp.ChannelValues == nil {
		cp.ChannelValues = map[string]any{}
	}
	cp.ChannelValues[entrypointSaveChannel] = saveValue
	cp.TS = time.Now().UTC().Format(time.RFC3339Nano)

	if cp.ChannelVersions == nil {
		cp.ChannelVersions = map[string]checkpoint.Version{}
	}
	maxVersion := int64(0)
	for _, v := range cp.ChannelVersions {
		n, convErr := checkpoint.VersionAsInt64(v)
		if convErr == nil && n > maxVersion {
			maxVersion = n
		}
	}
	cp.ChannelVersions[entrypointSaveChannel] = maxVersion + 1
	cp.UpdatedChannels = []string{entrypointSaveChannel}
	if cp.VersionsSeen == nil {
		cp.VersionsSeen = map[string]map[string]checkpoint.Version{}
	}

	_, err := config.Checkpointer.Put(ctx, config.CheckpointConfig(), cp, &checkpoint.CheckpointMetadata{Source: "update", Step: step + 1})
	if err != nil {
		return fmt.Errorf("saving entrypoint checkpoint: %w", err)
	}
	return nil
}
