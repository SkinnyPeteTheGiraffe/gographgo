package graph

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/checkpoint"
)

// CompiledStateGraph is an executable graph compiled from a StateGraph.
// It provides Invoke and Stream methods for graph execution, backed by the
// Pregel superstep runtime.
type CompiledStateGraph[State, Context, Input, Output any] struct {
	checkpointer    checkpoint.Saver
	store           Store
	cache           Cache
	contextValue    any
	builder         *StateGraph[State, Context, Input, Output]
	name            string
	durability      DurabilityMode
	interruptBefore []string
	interruptAfter  []string
	stepTimeout     time.Duration
	maxConcurrency  int
	debug           bool
}

// Invoke executes the graph with the given input and returns the final state.
// It blocks until execution completes or ctx is canceled.
func (g *CompiledStateGraph[State, Context, Input, Output]) Invoke(ctx context.Context, input Input) (GraphOutput, error) {
	config := GetConfig(ctx)
	config = g.mergeConfig(config)
	ctx = WithConfig(ctx, config)

	res, err := runPregelLoop[State, Context, Input, Output](ctx, pregelLoopOptions[State, Context, Input, Output]{
		graph:           g.builder,
		input:           input,
		config:          config,
		store:           g.store,
		cache:           g.cache,
		contextValue:    g.contextValue,
		streamOut:       nil,
		streamModes:     nil,
		interruptBefore: g.interruptBefore,
		interruptAfter:  g.interruptAfter,
	})
	if err != nil {
		return GraphOutput{}, err
	}

	outVal, ok := res.output.(Output)
	if !ok {
		return GraphOutput{}, fmt.Errorf("graph '%s': output type mismatch: expected %T, got %T", g.name, *new(Output), res.output)
	}
	return GraphOutput{
		Value:      outVal,
		Interrupts: res.interrupts,
	}, nil
}

// Stream executes the graph and emits StreamPart values on the returned channel.
// The channel is closed when execution finishes. Callers must drain the channel.
// If a terminal error occurs, a StreamPart with a non-nil Err field is emitted
// as the last value before the channel closes.
func (g *CompiledStateGraph[State, Context, Input, Output]) Stream(ctx context.Context, input Input, mode StreamMode) <-chan StreamPart {
	return g.StreamDuplex(ctx, input, mode)
}

// StreamDuplex executes the graph once and emits a union of stream parts for
// all requested modes.
func (g *CompiledStateGraph[State, Context, Input, Output]) StreamDuplex(ctx context.Context, input Input, modes ...StreamMode) <-chan StreamPart {
	out := make(chan StreamPart, 16)

	go func() {
		defer close(out)

		config := GetConfig(ctx)
		config = g.mergeConfig(config)
		ctx = WithConfig(ctx, config)

		modeSet := newStreamModeSet(modes...)
		if modeSet.empty() {
			modeSet = newStreamModeSet(StreamModeValues)
		}

		// Emit the initial state for values mode before execution starts.
		if modeSet.enabled(StreamModeValues) {
			out <- StreamPart{
				Type:      StreamModeValues,
				Data:      input,
				Namespace: splitCheckpointNamespace(config.CheckpointNS),
			}
		}

		_, err := runPregelLoop[State, Context, Input, Output](ctx, pregelLoopOptions[State, Context, Input, Output]{
			graph:           g.builder,
			input:           input,
			config:          config,
			store:           g.store,
			cache:           g.cache,
			contextValue:    g.contextValue,
			streamOut:       out,
			streamModes:     modeSet,
			interruptBefore: g.interruptBefore,
			interruptAfter:  g.interruptAfter,
		})
		if err != nil {
			out <- StreamPart{Err: fmt.Errorf("graph '%s': %w", g.name, err)}
		}
	}()

	return out
}

// GetState returns a snapshot of the graph's current state for the given config.
// Requires a Checkpointer to be configured.
//
// The returned StateSnapshot is fully populated: Values, Next, Tasks, Metadata,
// CreatedAt, ParentConfig, and Interrupts are all set when the underlying
// checkpoint contains the relevant data.
func (g *CompiledStateGraph[State, Context, Input, Output]) GetState(ctx context.Context, config *Config) (*StateSnapshot, error) {
	if config == nil {
		config = &Config{}
	}
	cfg := g.mergeConfig(*config)

	if cfg.Checkpointer == nil {
		return nil, fmt.Errorf("GetState requires a Checkpointer; configure one via CompileOptions or Config")
	}

	cpCfg := cfg.CheckpointConfig()
	tuple, err := cfg.Checkpointer.GetTuple(ctx, cpCfg)
	if err != nil {
		return nil, fmt.Errorf("fetching checkpoint: %w", err)
	}

	snap := &StateSnapshot{Config: config}
	if tuple == nil {
		return snap, nil
	}

	// Prefer the config stored inside the tuple so the snapshot carries a
	// self-consistent reference (thread_id, checkpoint_id, checkpoint_ns).
	if tuple.Config != nil {
		snap.Config = graphConfigFromCheckpointConfig(tuple.Config)
	}

	snap.Metadata = metadataMapFromCheckpointMetadata(tuple.Metadata)
	snap.ParentConfig = graphConfigFromCheckpointConfig(tuple.ParentConfig)

	if tuple.Checkpoint != nil {
		snap.Values = tuple.Checkpoint.ChannelValues
		snap.CreatedAt = parseCheckpointTS(tuple.Checkpoint.TS)
	}

	snap.Next, snap.Tasks, snap.Interrupts = snapshotNextAndTasks(tuple.PendingWrites)
	return snap, nil
}

// snapshotNextAndTasks derives the Next node list, pending Tasks, and active
// Interrupts from the pending writes stored in a checkpoint tuple.
//
// Pending interrupt writes are the authoritative record of which tasks were
// paused before or during execution. Each write carries the target node name
// and the interrupt value, so we can reconstruct the task list without
// re-executing graph logic that depends on runtime state (e.g. prevNodes).
//
// Correctness guarantees:
//   - interruptBefore: the interrupted node has not run; it appears in Next and Tasks.
//   - User-triggered interrupt (Interrupt() inside a node): the node is paused
//     mid-execution; it appears in Next and Tasks.
//   - interruptAfter: the node ran; the interrupt signals a pause after that step.
//     The same node appears in Next and Tasks because resume will re-visit it.
//   - Completed graph (no pending interrupt writes): Next and Tasks are empty.
func snapshotNextAndTasks(pendingWrites []checkpoint.PendingWrite) (next []string, tasks []PregelTask, interrupts []Interrupt) {
	if len(pendingWrites) == 0 {
		return
	}

	// Collect interrupt values per task ID, preserving insertion order for
	// deterministic output.
	type taskEntry struct {
		id         string
		name       string
		path       []any
		interrupts []Interrupt
	}
	var order []string
	byID := make(map[string]*taskEntry)

	for _, w := range pendingWrites {
		if w.Channel != pregelInterrupt {
			continue
		}
		iw, ok := extractInterruptWrite(w.Value)
		if !ok {
			continue
		}
		entry, seen := byID[w.TaskID]
		if !seen {
			entry = &taskEntry{
				id:   w.TaskID,
				name: iw.Node,
				path: append([]any(nil), iw.Path...),
			}
			byID[w.TaskID] = entry
			order = append(order, w.TaskID)
		}
		if iw.Interrupt.ID != "" {
			entry.interrupts = append(entry.interrupts, iw.Interrupt)
			interrupts = append(interrupts, iw.Interrupt)
		}
	}

	seenNames := make(map[string]bool, len(order))
	for _, id := range order {
		e := byID[id]
		tasks = append(tasks, PregelTask{
			ID:         e.id,
			Name:       e.name,
			Path:       e.path,
			Interrupts: append([]Interrupt(nil), e.interrupts...),
		})
		if !seenNames[e.name] {
			next = append(next, e.name)
			seenNames[e.name] = true
		}
	}
	return
}

// extractInterruptWrite unpacks an interrupt write value from the variety of
// forms it may be stored as (interruptWrite, *interruptWrite, Interrupt, etc.).
func extractInterruptWrite(value any) (interruptWrite, bool) {
	switch v := value.(type) {
	case interruptWrite:
		return v, true
	case *interruptWrite:
		if v != nil {
			return *v, true
		}
	case Interrupt:
		return interruptWrite{Interrupt: v}, true
	case *Interrupt:
		if v != nil {
			return interruptWrite{Interrupt: *v}, true
		}
	}
	return interruptWrite{}, false
}

// GetStateHistory returns checkpoint-backed state snapshots for the given
// config, ordered per saver.List semantics (newest first).
// Requires a Checkpointer to be configured.
func (g *CompiledStateGraph[State, Context, Input, Output]) GetStateHistory(ctx context.Context, config *Config) ([]StateSnapshot, error) {
	if config == nil {
		config = &Config{}
	}
	cfg := g.mergeConfig(*config)

	if cfg.Checkpointer == nil {
		return nil, fmt.Errorf("GetStateHistory requires a Checkpointer; configure one via CompileOptions or Config")
	}

	tuples, err := cfg.Checkpointer.List(ctx, cfg.CheckpointConfig(), checkpoint.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("listing checkpoints: %w", err)
	}

	history := make([]StateSnapshot, 0, len(tuples))
	for _, tuple := range tuples {
		if tuple == nil {
			continue
		}

		snap := StateSnapshot{
			Config:       graphConfigFromCheckpointConfig(tuple.Config),
			Metadata:     metadataMapFromCheckpointMetadata(tuple.Metadata),
			ParentConfig: graphConfigFromCheckpointConfig(tuple.ParentConfig),
		}
		if snap.Config == nil {
			snap.Config = graphConfigFromCheckpointConfig(cfg.CheckpointConfig())
		}
		if tuple.Checkpoint != nil {
			snap.Values = tuple.Checkpoint.ChannelValues
			snap.CreatedAt = parseCheckpointTS(tuple.Checkpoint.TS)
		}
		snap.Next, snap.Tasks, snap.Interrupts = snapshotNextAndTasks(tuple.PendingWrites)

		history = append(history, snap)
	}

	return history, nil
}

// AsSubgraphNode wraps a compiled graph as a node callable from a parent graph.
//
// The node reuses parent run configuration (checkpointer/store/cache/thread),
// appends a task-scoped checkpoint namespace segment (`<node>:<task_id>`), and
// invokes the subgraph with the parent node input.
//
// When the subgraph contains a node that returns Command with Graph set to
// CommandParent, the command is transparently forwarded to this graph level.
// The command's Graph routing is cleared before it is returned so that the
// parent graph processes the update and goto fields as its own.
func (g *CompiledStateGraph[State, Context, Input, Output]) AsSubgraphNode(nodeName string) func(context.Context, Input) (NodeResult, error) {
	return g.AsSubgraphNodeWithOptions(nodeName, SubgraphNodeOptions{Persistence: SubgraphPersistenceTaskScope})
}

// AsStatefulSubgraphNode wraps a compiled graph as a stateful subgraph node.
// Child checkpoints are keyed by node name instead of task id so state can
// accumulate across invocations on the same thread/namespace.
func (g *CompiledStateGraph[State, Context, Input, Output]) AsStatefulSubgraphNode(nodeName string) func(context.Context, Input) (NodeResult, error) {
	return g.AsSubgraphNodeWithOptions(nodeName, SubgraphNodeOptions{Persistence: SubgraphPersistenceNodeScope})
}

// AsSubgraphNodeWithOptions wraps a compiled graph as a node callable from a
// parent graph with explicit checkpoint/persistence controls.
func (g *CompiledStateGraph[State, Context, Input, Output]) AsSubgraphNodeWithOptions(nodeName string, options SubgraphNodeOptions) func(context.Context, Input) (NodeResult, error) {
	return func(ctx context.Context, input Input) (NodeResult, error) {
		cfg := GetConfig(ctx)
		childCfg := cfg
		childCfg.CheckpointNS = subgraphCheckpointNamespaceWithMode(cfg.CheckpointNS, nodeName, GetTaskID(ctx), options.Persistence)
		childCfg.CheckpointID = ""
		if options.DisableCheckpointer {
			childCfg.Checkpointer = disabledCheckpointSaver{}
			childCfg.CheckpointID = ""
		}
		childCtx := WithConfig(ctx, childCfg)

		out, err := g.Invoke(childCtx, input)
		if err != nil {
			// A node in the subgraph issued a command targeting the parent graph.
			// Strip the graph routing field and return the command for processing
			// at this level — this graph is the intended target.
			var pce *parentCommandError
			if errors.As(err, &pce) {
				cmd := *pce.cmd
				cmd.Graph = nil
				return NodeCommand(&cmd), nil
			}
			return NodeResult{}, err
		}
		if len(out.Interrupts) > 0 {
			panic(nodeInterruptSignal{interrupt: out.Interrupts[0]})
		}
		return NodeState(Dyn(out.Value)), nil
	}
}

// UpdateState applies an update to the graph's persisted state.
// Requires a Checkpointer to be configured.
func (g *CompiledStateGraph[State, Context, Input, Output]) UpdateState(ctx context.Context, config *Config, values map[string]any) error {
	_, err := g.UpdateStateWithConfig(ctx, config, values)
	return err
}

// UpdateStateWithConfig applies an update to the graph's persisted state and
// returns the next checkpoint-aware graph config.
func (g *CompiledStateGraph[State, Context, Input, Output]) UpdateStateWithConfig(ctx context.Context, config *Config, values map[string]any) (*Config, error) {
	if config == nil {
		config = &Config{}
	}
	cfg := g.mergeConfig(*config)

	if cfg.Checkpointer == nil {
		return nil, fmt.Errorf("UpdateState requires a Checkpointer; configure one via CompileOptions or Config")
	}
	if cfg.ThreadID == "" {
		return nil, fmt.Errorf("UpdateState requires Config.ThreadID when using a Checkpointer")
	}

	cpCfg := cfg.CheckpointConfig()
	tuple, err := cfg.Checkpointer.GetTuple(ctx, cpCfg)
	if err != nil {
		return nil, fmt.Errorf("loading checkpoint for update: %w", err)
	}

	merged := map[string]any{}
	channelVersions := map[string]checkpoint.Version{}
	versionsSeen := map[string]map[string]checkpoint.Version{}
	step := -1
	if tuple != nil && tuple.Checkpoint != nil {
		for k, v := range tuple.Checkpoint.ChannelValues {
			merged[k] = v
		}
		channelVersions = cloneVersionTokenMap(tuple.Checkpoint.ChannelVersions)
		versionsSeen = cloneNestedVersionTokenMap(tuple.Checkpoint.VersionsSeen)
		if tuple.Metadata != nil {
			step = tuple.Metadata.Step
		}
	}
	updatedChannels := make([]string, 0, len(values))
	merged = mergeState(merged, values)
	for k := range values {
		updatedChannels = append(updatedChannels, k)
	}
	sort.Strings(updatedChannels)

	cp := &checkpoint.Checkpoint{
		V:               1,
		TS:              time.Now().UTC().Format(time.RFC3339Nano),
		ChannelValues:   merged,
		ChannelVersions: channelVersions,
		VersionsSeen:    versionsSeen,
		UpdatedChannels: updatedChannels,
	}
	nextCP, err := cfg.Checkpointer.Put(ctx, cpCfg, cp, &checkpoint.CheckpointMetadata{Source: "update", Step: step + 1})
	if err != nil {
		return nil, err
	}
	next := cfg
	if nextCP != nil {
		next.CheckpointID = nextCP.CheckpointID
		if next.CheckpointNS == "" {
			next.CheckpointNS = nextCP.CheckpointNS
		}
		if next.ThreadID == "" {
			next.ThreadID = nextCP.ThreadID
		}
	}
	return &next, nil
}

// BulkUpdateState applies state updates grouped by superstep and returns the
// updated config for the last checkpoint produced.
func (g *CompiledStateGraph[State, Context, Input, Output]) BulkUpdateState(ctx context.Context, config *Config, supersteps [][]StateUpdate) (*Config, error) {
	if len(supersteps) == 0 {
		return nil, fmt.Errorf("no supersteps provided")
	}
	if config == nil {
		config = &Config{}
	}
	current := *config
	for i, superstep := range supersteps {
		if len(superstep) == 0 {
			return nil, fmt.Errorf("superstep %d has no updates", i)
		}
		updates := make(map[string]any)
		for _, update := range superstep {
			if update.AsNode != nil && *update.AsNode != "" {
				if _, ok := g.builder.nodes[*update.AsNode]; !ok {
					return nil, fmt.Errorf("superstep %d references unknown node %q", i, *update.AsNode)
				}
			}
			for k, v := range update.Values {
				updates[k] = v
			}
		}
		next, err := g.UpdateStateWithConfig(ctx, &current, updates)
		if err != nil {
			return nil, err
		}
		if next != nil {
			current = *next
		}
	}
	return &current, nil
}

func subgraphCheckpointNamespaceWithMode(parentNS, nodeName, taskID string, mode SubgraphPersistenceMode) string {
	nsPart := strings.TrimSpace(nodeName)
	if nsPart == "" {
		nsPart = "subgraph"
	}
	if mode == "" {
		mode = SubgraphPersistenceTaskScope
	}
	if mode == SubgraphPersistenceTaskScope {
		if strings.TrimSpace(taskID) == "" {
			taskID = "unknown"
		}
		nsPart = fmt.Sprintf("%s:%s", nsPart, taskID)
	}
	if strings.TrimSpace(parentNS) == "" {
		return nsPart
	}
	return parentNS + "|" + nsPart
}

// mergeConfig merges the compiled graph's default config with a caller-provided
// config, giving precedence to the caller's non-zero values.
func (g *CompiledStateGraph[State, Context, Input, Output]) mergeConfig(config Config) Config {
	if g.checkpointer != nil && config.Checkpointer == nil {
		config.Checkpointer = g.checkpointer
	}
	if g.store != nil && config.Store == nil {
		config.Store = g.store
	}
	if g.cache != nil && config.Cache == nil {
		config.Cache = g.cache
	}
	if config.RecursionLimit == 0 {
		config.RecursionLimit = DefaultConfig().RecursionLimit
	}
	if g.debug {
		config.Debug = true
	}
	if config.Durability == "" {
		if g.durability != "" {
			config.Durability = g.durability
		} else {
			config.Durability = DurabilityAsync
		}
	}
	if config.StepTimeout <= 0 && g.stepTimeout > 0 {
		config.StepTimeout = g.stepTimeout
	}
	if config.MaxConcurrency <= 0 && g.maxConcurrency > 0 {
		config.MaxConcurrency = g.maxConcurrency
	}
	return config
}

// Helper function to check if a string is in a slice.
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// mergeState merges a node's output update into the current state.
//
// For map[string]any states, keys from update are shallow-merged into a copy
// of current. For all other state types, the update is applied directly when it
// satisfies the State type constraint; otherwise current is returned unchanged.
//
// Full reducer support (per-key merge functions, annotated state schemas) is
// handled by the channel layer in the Pregel runtime.
func mergeState[State any](current State, update any) State {
	// map[string]any: shallow merge — update keys overwrite current keys.
	if currentMap, ok := any(current).(map[string]any); ok {
		if updateMap, ok := update.(map[string]any); ok {
			result := make(map[string]any, len(currentMap)+len(updateMap))
			for k, v := range currentMap {
				result[k] = v
			}
			for k, v := range updateMap {
				result[k] = v
			}
			if s, ok := any(result).(State); ok {
				return s
			}
		}
	}

	// Concrete State type: replace current with update when type matches.
	if updated, ok := update.(State); ok {
		return updated
	}

	return current
}

func graphConfigFromCheckpointConfig(cfg *checkpoint.Config) *Config {
	if cfg == nil {
		return nil
	}
	return &Config{
		ThreadID:     cfg.ThreadID,
		CheckpointID: cfg.CheckpointID,
		CheckpointNS: cfg.CheckpointNS,
	}
}

func metadataMapFromCheckpointMetadata(meta *checkpoint.CheckpointMetadata) map[string]any {
	if meta == nil {
		return nil
	}
	out := map[string]any{
		"source": meta.Source,
		"step":   meta.Step,
	}
	if len(meta.Parents) > 0 {
		parents := make(map[string]string, len(meta.Parents))
		for k, v := range meta.Parents {
			parents[k] = v
		}
		out["parents"] = parents
	}
	if meta.RunID != "" {
		out["run_id"] = meta.RunID
	}
	return out
}

func parseCheckpointTS(ts string) *time.Time {
	if strings.TrimSpace(ts) == "" {
		return nil
	}
	if parsed, err := time.Parse(time.RFC3339Nano, ts); err == nil {
		parsed = parsed.UTC()
		return &parsed
	}
	if parsed, err := time.Parse(time.RFC3339, ts); err == nil {
		parsed = parsed.UTC()
		return &parsed
	}
	return nil
}

type disabledCheckpointSaver struct{}

func (disabledCheckpointSaver) GetTuple(context.Context, *checkpoint.Config) (*checkpoint.CheckpointTuple, error) {
	return nil, nil
}

func (disabledCheckpointSaver) Put(context.Context, *checkpoint.Config, *checkpoint.Checkpoint, *checkpoint.CheckpointMetadata) (*checkpoint.Config, error) {
	return nil, nil
}

func (disabledCheckpointSaver) PutWrites(context.Context, *checkpoint.Config, []checkpoint.PendingWrite, string) error {
	return nil
}

func (disabledCheckpointSaver) List(context.Context, *checkpoint.Config, checkpoint.ListOptions) ([]*checkpoint.CheckpointTuple, error) {
	return nil, nil
}

func (disabledCheckpointSaver) DeleteThread(context.Context, string) error {
	return nil
}

func (disabledCheckpointSaver) DeleteForRuns(context.Context, []string) error {
	return nil
}

func (disabledCheckpointSaver) CopyThread(context.Context, string, string) error {
	return nil
}

func (disabledCheckpointSaver) Prune(context.Context, []string, checkpoint.PruneStrategy) error {
	return nil
}
