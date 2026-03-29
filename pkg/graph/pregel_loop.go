package graph

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/checkpoint"
)

// pregelLoopResult is the output of a completed Pregel execution.
type pregelLoopResult struct {
	output     any
	interrupts []Interrupt
}

// pregelLoopOptions carries the runtime parameters for a Pregel superstep loop.
type pregelLoopOptions[State, Context, Input, Output any] struct {
	graph           *StateGraph[State, Context, Input, Output]
	store           Store
	cache           Cache
	contextValue    any
	streamOut       chan<- StreamPart // nil when called from Invoke
	streamModes     streamModeSet
	interruptBefore []string // from CompiledStateGraph
	interruptAfter  []string // from CompiledStateGraph
	input           Input
	config          Config
}

type pregelLoopState[State any] struct {
	tasks                []pregelTask[State]
	pendingWrites        []checkpoint.PendingWrite
	resumedTaskIDs       map[string]struct{}
	interrupts           []Interrupt
	externalStateVersion string
	channelVersions      map[string]int64
	versionsSeen         map[string]map[string]int64
	prevNodes            []string
	lastUpdatedChannels  []string
	versionCounter       int64
	step                 int
	hasResume            bool
	isReplaying          bool
	allowDynamicChannels bool
}

type resolvedStateAuthority struct {
	stateStore StateStore
	mode       StateStoreMode
}

func (s *pregelLoopState[State]) bumpChannelVersion(channel string) {
	s.versionCounter++
	s.channelVersions[channel] = s.versionCounter
}

// runPregelLoop executes the Pregel superstep loop for a compiled StateGraph.
//
// The loop follows the Pregel execution model with the correct PUSH-before-PULL
// ordering that enables Send-based fan-out:
//
//  1. Map input to channels.
//  2. Ensure the TASKS channel (Topic) exists for Send fan-out.
//  3. Prepare the initial task set (nodes triggered by Start).
//  4. Repeat each superstep:
//     a. Check interrupt-before.
//     b. Execute all tasks concurrently.
//     c. Collect writes; check interrupt-after.
//     d. Apply writes to channels (including Send objects → TASKS channel),
//     including consume and empty-step lifecycle updates.
//     e. Prepare next tasks from updated channel state.
//     f. Checkpoint (if configured).
//     g. Advance step; check recursion limit.
//  5. Finish channels, build output.
//
// Mirrors Python LangGraph's pregel/_loop.py superstep loop.
func runPregelLoop[State, Context, Input, Output any](ctx context.Context, opts pregelLoopOptions[State, Context, Input, Output]) (pregelLoopResult, error) {
	g := opts.graph
	config := opts.config
	baseNamespace := splitCheckpointNamespace(config.CheckpointNS)
	authority, err := resolveStateAuthority(config)
	if err != nil {
		return pregelLoopResult{}, err
	}

	runtimeSchema := runtimeChannelSchema(g)
	channels := newPregelChannelMap(runtimeSchema)
	allowDynamicChannels := allowsDynamicStateChannels[State]()
	requestedCheckpointID := config.CheckpointID
	loadedTuple, pendingWrites, restoredChannels, err := loadPregelCheckpointState(ctx, runtimeSchema, &config, authority)
	if err != nil {
		return pregelLoopResult{}, err
	}
	authoritativeState, err := loadAuthoritativeStateIfNeeded(ctx, authority, config)
	if err != nil {
		return pregelLoopResult{}, err
	}
	if restoredChannels != nil {
		channels = restoredChannels
	}

	loopState := pregelLoopState[State]{
		channelVersions:      make(map[string]int64),
		versionsSeen:         make(map[string]map[string]int64),
		allowDynamicChannels: allowDynamicChannels,
	}
	initializeLoopStateFromCheckpoint(&loopState, requestedCheckpointID, loadedTuple)
	pendingWrites, err = configureLoopResumeAndInput(channels, pendingWrites, opts.input, config, authoritativeState, &loopState)
	if err != nil {
		return pregelLoopResult{}, err
	}
	if authoritativeState != nil {
		loopState.externalStateVersion = authoritativeState.Version
		if err := validateAuthoritativeStateConsistency(config, loadedTuple, authoritativeState, loopState.hasResume, loopState.isReplaying); err != nil {
			return pregelLoopResult{}, err
		}
	}
	initializeLoopVersionCounter(channels, &loopState)

	// Prepare initial tasks (step 0: nodes from Start, a TASKS channel is empty).
	tasks, err := prepareNextTasks(ctx, g, config, channels, pendingWrites, loopState.prevNodes, loopState.step, false)
	if err != nil {
		return pregelLoopResult{}, fmt.Errorf("preparing initial tasks: %w", err)
	}
	if loopState.hasResume {
		if resumed := prepareResumedTasks(g, config, channels, pendingWrites, loopState.resumedTaskIDs); len(resumed) > 0 {
			tasks = resumed
		}
	}
	loopState.tasks = tasks
	loopState.pendingWrites = pendingWrites

	durability := config.Durability
	if durability == "" {
		durability = DurabilityAsync
	}

	asyncCheckpoint := &checkpointAsyncWriter{}
	defer asyncCheckpoint.Wait()

	for len(loopState.tasks) > 0 {
		shouldStop, stepErr := executePregelStep(ctx, opts, g, channels, &config, baseNamespace, durability, asyncCheckpoint, &loopState)
		if stepErr != nil {
			return pregelLoopResult{}, stepErr
		}
		if shouldStop {
			break
		}
	}
	return finalizePregelLoop(ctx, opts, channels, baseNamespace, config, durability, asyncCheckpoint, loopState)
}

func finalizePregelLoop[State, Context, Input, Output any](
	ctx context.Context,
	opts pregelLoopOptions[State, Context, Input, Output],
	channels *pregelChannelMap,
	baseNamespace []string,
	config Config,
	durability DurabilityMode,
	asyncCheckpoint *checkpointAsyncWriter,
	loopState pregelLoopState[State],
) (pregelLoopResult, error) {
	if config.Checkpointer != nil && config.ThreadID != "" {
		if durability == DurabilityAsync {
			asyncCheckpoint.Wait()
		}
		saveCheckpoint(ctx, config, channels, loopState.step, loopState.channelVersions, loopState.versionsSeen, loopState.lastUpdatedChannels, nil, loopState.pendingWrites, loopState.externalStateVersion, opts.streamOut, opts.streamModes, baseNamespace)
	}
	channels.finishAll()
	state, err := stateFromChannels[State](channels)
	if err != nil {
		return pregelLoopResult{}, fmt.Errorf("building output state: %w", err)
	}
	emitFinalValues(opts.streamOut, opts.streamModes, baseNamespace, state)
	output, err := stateFromChannels[Output](channels)
	if err != nil {
		return pregelLoopResult{}, fmt.Errorf("building graph output: %w", err)
	}
	return pregelLoopResult{output: output, interrupts: loopState.interrupts}, nil
}

func emitFinalValues(out chan<- StreamPart, modes streamModeSet, namespace []string, state any) {
	if out == nil || !modes.enabled(StreamModeValues) {
		return
	}
	out <- StreamPart{Type: StreamModeValues, Data: state, Namespace: append([]string(nil), namespace...)}
}

func loadPregelCheckpointState(
	ctx context.Context,
	runtimeSchema map[string]Channel,
	config *Config,
	authority resolvedStateAuthority,
) (tuple *checkpoint.CheckpointTuple, pendingWrites []checkpoint.PendingWrite, channels *pregelChannelMap, err error) {
	if config == nil || config.Checkpointer == nil || config.ThreadID == "" {
		return nil, nil, nil, nil
	}
	tuple, err = config.Checkpointer.GetTuple(ctx, config.CheckpointConfig())
	if err != nil {
		return nil, nil, nil, fmt.Errorf("loading checkpoint: %w", err)
	}
	if tuple == nil || tuple.Checkpoint == nil {
		return tuple, nil, nil, nil
	}
	var restored *pregelChannelMap
	if !isAuthoritativeExternalStateMode(authority.mode) {
		var restoreErr error
		restored, restoreErr = restoreFromCheckpoint(runtimeSchema, tuple.Checkpoint.ChannelValues)
		if restoreErr != nil {
			return nil, nil, nil, fmt.Errorf("restoring checkpoint channels: %w", restoreErr)
		}
	}
	pendingWrites = append(pendingWrites, tuple.PendingWrites...)
	if tuple.Config != nil && tuple.Config.CheckpointID != "" {
		config.CheckpointID = tuple.Config.CheckpointID
	}
	return tuple, pendingWrites, restored, nil
}

func initializeLoopStateFromCheckpoint[State any](
	state *pregelLoopState[State],
	requestedCheckpointID string,
	tuple *checkpoint.CheckpointTuple,
) {
	if tuple != nil && tuple.Checkpoint != nil {
		if len(tuple.Checkpoint.ChannelVersions) > 0 {
			state.channelVersions = parseVersionMap(tuple.Checkpoint.ChannelVersions)
		}
		if len(tuple.Checkpoint.VersionsSeen) > 0 {
			state.versionsSeen = parseNestedVersionMap(tuple.Checkpoint.VersionsSeen)
		}
		if tuple.Metadata != nil {
			state.step = tuple.Metadata.Step + 1
		}
	}
	state.isReplaying = requestedCheckpointID != "" && tuple != nil && tuple.Checkpoint != nil
}

func configureLoopResumeAndInput[State, Input any](
	channels *pregelChannelMap,
	pendingWrites []checkpoint.PendingWrite,
	input Input,
	config Config,
	authoritativeState *AuthoritativeStateSnapshot,
	state *pregelLoopState[State],
) ([]checkpoint.PendingWrite, error) {
	resumeMap, resumeVals, hasResume, err := resumeConfig(config.Metadata)
	if err != nil {
		return nil, err
	}
	if hasResume && (config.Checkpointer == nil || config.ThreadID == "") {
		return nil, fmt.Errorf("interrupt resume requires a checkpointer and thread id")
	}
	var resumedTaskIDs map[string]struct{}
	pendingWrites, resumedTaskIDs, err = applyResumeWrites(pendingWrites, resumeMap, resumeVals)
	if err != nil {
		return nil, err
	}
	switch {
	case authoritativeState != nil:
		if err := mapInputToChannels(channels, authoritativeState.Values); err != nil {
			return nil, fmt.Errorf("mapping authoritative state to channels: %w", err)
		}
	case !hasResume && !state.isReplaying:
		if err := mapInputToChannels(channels, any(input)); err != nil {
			return nil, fmt.Errorf("mapping input to channels: %w", err)
		}
	case hasResume:
		seen := make(map[string]int64, len(state.channelVersions))
		for key, version := range state.channelVersions {
			seen[key] = version
		}
		state.versionsSeen[pregelInterrupt] = seen
	}
	state.hasResume = hasResume
	state.resumedTaskIDs = resumedTaskIDs
	return pendingWrites, nil
}

func initializeLoopVersionCounter[State any](channels *pregelChannelMap, state *pregelLoopState[State]) {
	if len(state.channelVersions) > 0 {
		for _, version := range state.channelVersions {
			if version > state.versionCounter {
				state.versionCounter = version
			}
		}
		return
	}
	for key, channel := range channels.channels {
		if channel.IsAvailable() {
			state.bumpChannelVersion(key)
		}
	}
}

func executePregelStep[State, Context, Input, Output any](
	ctx context.Context,
	opts pregelLoopOptions[State, Context, Input, Output],
	g *StateGraph[State, Context, Input, Output],
	channels *pregelChannelMap,
	config *Config,
	baseNamespace []string,
	durability DurabilityMode,
	asyncCheckpoint *checkpointAsyncWriter,
	state *pregelLoopState[State],
) (bool, error) {
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	if stopped, err := maybeInterruptBeforeStep(opts, baseNamespace, state); stopped || err != nil {
		return stopped, err
	}

	for _, task := range state.tasks {
		emitTaskStart(opts.streamOut, task, state.step, opts.streamModes, baseNamespace)
	}

	results, taskErr := executeTasks(ctx, g, state.tasks, executeTasksOptions{
		config:         config,
		stepTimeout:    config.StepTimeout,
		maxConcurrency: config.MaxConcurrency,
		store:          effectiveStore(*config, opts.store),
		cache:          effectiveCache(*config, opts.cache),
		contextValue:   opts.contextValue,
		isReplaying:    state.isReplaying,
		step:           state.step,
		stop:           config.RecursionLimit,
		streamOut:      opts.streamOut,
		streamModes:    opts.streamModes,
		namespace:      baseNamespace,
	})

	stepPending := collectSpecialWrites(results)
	if stopped, err := maybeInterruptAfterStep(opts, g, channels, baseNamespace, results, stepPending, state); stopped || err != nil {
		return stopped, err
	}
	if err := handlePregelTaskError(ctx, *config, channels, durability, asyncCheckpoint, opts, taskErr, stepPending, state); err != nil {
		return false, err
	}

	hasNodeInterrupt := resultSetHasNodeInterrupt(results)
	cleanupResumedTaskWrites(state, taskErr, hasNodeInterrupt)

	for _, r := range results {
		emitTaskFinish(opts.streamOut, r, state.step, opts.streamModes, baseNamespace)
	}
	updated, err := applyResultsToChannels(g, channels, results, state)
	if err != nil {
		return false, fmt.Errorf("applying writes at step %d: %w", state.step, err)
	}
	state.lastUpdatedChannels = updatedChannelsFromChanged(updated)
	if hasNodeInterrupt {
		return true, nil
	}

	for _, r := range results {
		emitUpdate(opts.streamOut, g, channels, r, state.step, opts.streamModes, baseNamespace)
	}
	if err := advancePregelStep(ctx, g, channels, *config, opts, baseNamespace, durability, asyncCheckpoint, updated, state); err != nil {
		return false, err
	}
	return false, nil
}

func maybeInterruptBeforeStep[State, Context, Input, Output any](
	opts pregelLoopOptions[State, Context, Input, Output],
	baseNamespace []string,
	state *pregelLoopState[State],
) (bool, error) {
	interruptTasksInput := state.tasks
	if hasAllInterrupt(opts.interruptBefore) {
		interruptTasksInput = visibleTasksForInterruptBefore(state.tasks)
	}
	matches := interruptTasks(interruptTasksInput, opts.interruptBefore, state.channelVersions, state.versionsSeen)
	if len(matches) == 0 {
		return false, nil
	}
	beforeWrites := make([]checkpoint.PendingWrite, 0, len(matches))
	for _, task := range matches {
		iv := NewInterrupt(
			Dyn(fmt.Sprintf("Interrupted before node: %s", task.name)),
			fmt.Sprintf("before_%s_%d", task.name, state.step),
		)
		state.interrupts = append(state.interrupts, iv)
		emitInterrupt(opts.streamOut, iv, opts.streamModes, baseNamespace)
		beforeWrites = append(beforeWrites, checkpoint.PendingWrite{
			TaskID:  task.id,
			Channel: pregelInterrupt,
			Value: interruptWrite{
				Interrupt: iv,
				Node:      task.name,
				Path:      append([]any(nil), task.path...),
			},
		})
	}
	state.pendingWrites = mergePendingWrites(state.pendingWrites, beforeWrites)
	return true, nil
}

func visibleTasksForInterruptBefore[State any](tasks []pregelTask[State]) []pregelTask[State] {
	visible := make([]pregelTask[State], 0, len(tasks))
	for _, task := range tasks {
		if taskHasTag(task, TagHidden) {
			continue
		}
		visible = append(visible, task)
	}
	return visible
}

func maybeInterruptAfterStep[State, Context, Input, Output any](
	opts pregelLoopOptions[State, Context, Input, Output],
	g *StateGraph[State, Context, Input, Output],
	channels *pregelChannelMap,
	baseNamespace []string,
	results []pregelTaskResult,
	stepPending []checkpoint.PendingWrite,
	state *pregelLoopState[State],
) (bool, error) {
	interruptResultsInput := results
	if hasAllInterrupt(opts.interruptAfter) {
		interruptResultsInput = visibleTaskResultsForInterruptAfter(results)
	}
	matches := interruptResults(interruptResultsInput, opts.interruptAfter, state.channelVersions, state.versionsSeen)
	if len(matches) == 0 {
		return false, nil
	}
	afterWrites := make([]checkpoint.PendingWrite, 0, len(matches))
	for _, r := range matches {
		iv := NewInterrupt(
			Dyn(fmt.Sprintf("Interrupted after node: %s", r.node)),
			fmt.Sprintf("after_%s_%d", r.node, state.step),
		)
		state.interrupts = append(state.interrupts, iv)
		emitInterrupt(opts.streamOut, iv, opts.streamModes, baseNamespace)
		afterWrites = append(afterWrites, checkpoint.PendingWrite{
			TaskID:  r.taskID,
			Channel: pregelInterrupt,
			Value: interruptWrite{
				Interrupt: iv,
				Node:      r.node,
				Path:      append([]any(nil), r.path...),
			},
		})
	}
	state.pendingWrites = mergePendingWrites(state.pendingWrites, append(stepPending, afterWrites...))
	for _, task := range state.tasks {
		markTaskVersionsSeen(task, state.channelVersions, state.versionsSeen)
	}
	var versionBumped map[string]bool
	var err error
	state.interrupts, versionBumped, _, err = applyTaskWrites(g, channels, state.tasks, results, state.interrupts, state.allowDynamicChannels)
	if err != nil {
		return false, fmt.Errorf("applying writes at step %d: %w", state.step, err)
	}
	for key := range versionBumped {
		state.bumpChannelVersion(key)
	}
	return true, nil
}

func visibleTaskResultsForInterruptAfter(results []pregelTaskResult) []pregelTaskResult {
	visible := make([]pregelTaskResult, 0, len(results))
	for _, result := range results {
		if containsString(result.tags, TagHidden) {
			continue
		}
		visible = append(visible, result)
	}
	return visible
}

func handlePregelTaskError[State, Context, Input, Output any](
	ctx context.Context,
	config Config,
	channels *pregelChannelMap,
	durability DurabilityMode,
	asyncCheckpoint *checkpointAsyncWriter,
	opts pregelLoopOptions[State, Context, Input, Output],
	taskErr error,
	stepPending []checkpoint.PendingWrite,
	state *pregelLoopState[State],
) error {
	state.pendingWrites = mergePendingWrites(state.pendingWrites, stepPending)
	if taskErr == nil {
		return nil
	}
	var pce *parentCommandError
	if errors.As(taskErr, &pce) {
		return taskErr
	}
	if config.Checkpointer != nil && config.ThreadID != "" {
		if durability == DurabilityAsync {
			asyncCheckpoint.Wait()
		}
		saveCheckpoint(ctx, config, channels, state.step, state.channelVersions, state.versionsSeen, nil, nil, state.pendingWrites, state.externalStateVersion, opts.streamOut, opts.streamModes, splitCheckpointNamespace(config.CheckpointNS))
	}
	return taskErr
}

func resultSetHasNodeInterrupt(results []pregelTaskResult) bool {
	for _, r := range results {
		if len(r.interrupts) > 0 {
			return true
		}
	}
	return false
}

func cleanupResumedTaskWrites[State any](state *pregelLoopState[State], taskErr error, hasNodeInterrupt bool) {
	if taskErr != nil || hasNodeInterrupt || len(state.resumedTaskIDs) == 0 {
		return
	}
	state.pendingWrites = removeConsumedInterruptWrites(state.pendingWrites, state.resumedTaskIDs)
	state.resumedTaskIDs = nil
}

func applyResultsToChannels[State, Context, Input, Output any](
	g *StateGraph[State, Context, Input, Output],
	channels *pregelChannelMap,
	results []pregelTaskResult,
	state *pregelLoopState[State],
) (map[string]bool, error) {
	for _, task := range state.tasks {
		markTaskVersionsSeen(task, state.channelVersions, state.versionsSeen)
	}
	var (
		versionBumped map[string]bool
		updated       map[string]bool
		err           error
	)
	state.interrupts, versionBumped, updated, err = applyTaskWrites(g, channels, state.tasks, results, state.interrupts, state.allowDynamicChannels)
	if err != nil {
		return nil, err
	}
	for key := range versionBumped {
		state.bumpChannelVersion(key)
	}
	return updated, nil
}

func advancePregelStep[State, Context, Input, Output any](
	ctx context.Context,
	g *StateGraph[State, Context, Input, Output],
	channels *pregelChannelMap,
	config Config,
	opts pregelLoopOptions[State, Context, Input, Output],
	baseNamespace []string,
	durability DurabilityMode,
	asyncCheckpoint *checkpointAsyncWriter,
	updated map[string]bool,
	state *pregelLoopState[State],
) error {
	state.prevNodes = completedTaskNodeNames(state.tasks)
	state.step++
	if state.step >= config.RecursionLimit {
		return &GraphRecursionError{Message: fmt.Sprintf("recursion limit of %d exceeded", config.RecursionLimit)}
	}

	tasks, err := prepareNextTasks(ctx, g, config, channels, state.pendingWrites, state.prevNodes, state.step, false)
	if err != nil {
		return fmt.Errorf("preparing tasks at step %d: %w", state.step, err)
	}
	state.tasks = tasks
	finished := make(map[string]bool)
	if len(state.tasks) == 0 {
		finished = finishChannels(channels)
		for channel := range finished {
			state.bumpChannelVersion(channel)
		}
		if len(finished) > 0 {
			state.lastUpdatedChannels = updatedChannelsFromChanged(finished)
			state.tasks, err = prepareNextTasks(ctx, g, config, channels, state.pendingWrites, state.prevNodes, state.step, true)
			if err != nil {
				return fmt.Errorf("preparing deferred tasks at step %d: %w", state.step, err)
			}
		}
	}
	checkpointChanged := mergeChangedChannels(updated, finished)
	checkpointByDurability(ctx, durability, asyncCheckpoint, checkpointSnapshot{
		config:               &config,
		channels:             channels,
		step:                 state.step,
		channelVersions:      state.channelVersions,
		versionsSeen:         state.versionsSeen,
		updatedChannels:      updatedChannelsFromChanged(checkpointChanged),
		next:                 nextNodeNames(state.tasks),
		pendingWrites:        state.pendingWrites,
		externalStateVersion: state.externalStateVersion,
		streamOut:            opts.streamOut,
		streamModes:          opts.streamModes,
		namespace:            baseNamespace,
	})
	return nil
}

func completedTaskNodeNames[State any](tasks []pregelTask[State]) []string {
	nodes := make([]string, 0, len(tasks))
	for _, task := range tasks {
		nodes = append(nodes, task.name)
	}
	return nodes
}

// executeTasks runs all tasks for a superstep concurrently, collecting their
// writes. Each task runs in its own goroutine; results are gathered after all
//
//	goroutines are complete.
//
// Mirrors Python LangGraph's BackgroundExecutor / AsyncBackgroundExecutor
// parallel task execution model.
type executeTasksOptions struct {
	config         *Config
	store          Store
	cache          Cache
	contextValue   any
	streamOut      chan<- StreamPart
	streamModes    streamModeSet
	namespace      []string
	stepTimeout    time.Duration
	maxConcurrency int
	step           int
	stop           int
	isReplaying    bool
}

func executeTasks[State, Context, Input, Output any](
	ctx context.Context,
	g *StateGraph[State, Context, Input, Output],
	tasks []pregelTask[State],
	opts executeTasksOptions,
) ([]pregelTaskResult, error) {
	results := make([]pregelTaskResult, len(tasks))
	waitCtx := ctx
	var cancel context.CancelFunc
	if opts.stepTimeout > 0 {
		waitCtx, cancel = context.WithTimeout(ctx, opts.stepTimeout)
		defer cancel()
	}

	executor := newBackgroundExecutor(waitCtx, opts.maxConcurrency)
	defer executor.close()
	futures, err := submitTaskFutures(executor, g, tasks, opts)
	if err != nil {
		return results, err
	}
	if err := awaitTaskResults(waitCtx, futures, results); err != nil {
		return handleAwaitTaskError(results, err, opts)
	}
	return summarizeTaskResults(tasks, results, opts)
}

func submitTaskFutures[State, Context, Input, Output any](
	executor *backgroundExecutor,
	g *StateGraph[State, Context, Input, Output],
	tasks []pregelTask[State],
	opts executeTasksOptions,
) ([]*taskFuture, error) {
	futures := make([]*taskFuture, len(tasks))
	for i, task := range tasks {
		future, err := submitOneTaskFuture(executor, g, task, opts)
		if err != nil {
			return nil, err
		}
		futures[i] = future
	}
	return futures, nil
}

func submitOneTaskFuture[State, Context, Input, Output any](
	executor *backgroundExecutor,
	g *StateGraph[State, Context, Input, Output],
	task pregelTask[State],
	opts executeTasksOptions,
) (*taskFuture, error) {
	if task.call != nil {
		return executor.submit(func(taskCtx context.Context) pregelTaskResult {
			return executeCallTask(taskCtx, task, opts)
		}), nil
	}
	node := g.nodes[task.name]
	if node == nil {
		return nil, fmt.Errorf("node '%s' not found", task.name)
	}
	return executor.submit(func(taskCtx context.Context) pregelTaskResult {
		return executeTask(taskCtx, node, task, executeTaskOptions[State]{
			config:       opts.config,
			store:        opts.store,
			cache:        opts.cache,
			contextValue: opts.contextValue,
			isReplaying:  opts.isReplaying,
			step:         opts.step,
			stop:         opts.stop,
			managed:      g.managed,
			streamOut:    opts.streamOut,
			streamModes:  opts.streamModes,
			namespace:    opts.namespace,
		})
	}), nil
}

func handleAwaitTaskError(results []pregelTaskResult, err error, opts executeTasksOptions) ([]pregelTaskResult, error) {
	if errors.Is(err, context.DeadlineExceeded) && opts.stepTimeout > 0 {
		return results, &GraphStepTimeoutError{Step: opts.step, Timeout: opts.stepTimeout}
	}
	return results, err
}

func summarizeTaskResults[State any](tasks []pregelTask[State], results []pregelTaskResult, opts executeTasksOptions) ([]pregelTaskResult, error) {
	var first error
	var firstParentCmd *Command
	for i := range results {
		r := results[i]
		if r.parentCmd != nil && firstParentCmd == nil {
			firstParentCmd = r.parentCmd
		}
		if first == nil {
			first = summarizeTaskError(tasks[i], r, opts)
		}
	}
	if firstParentCmd != nil {
		return results, &parentCommandError{cmd: firstParentCmd}
	}
	return results, first
}

func summarizeTaskError[State any](task pregelTask[State], result pregelTaskResult, opts executeTasksOptions) error {
	if result.err == nil || len(result.interrupts) > 0 {
		return nil
	}
	if opts.stepTimeout > 0 && errors.Is(result.err, context.DeadlineExceeded) {
		return &GraphStepTimeoutError{Step: opts.step, Timeout: opts.stepTimeout}
	}
	if task.call != nil {
		return fmt.Errorf("call task '%s': %w", task.name, result.err)
	}
	return fmt.Errorf("node '%s': %w", task.name, result.err)
}

func awaitTaskResults(ctx context.Context, futures []*taskFuture, results []pregelTaskResult) error {
	cases := []reflect.SelectCase{{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(ctx.Done()),
	}}
	caseToTask := []int{-1}
	remaining := 0
	for i, future := range futures {
		if future == nil {
			continue
		}
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(future.resultCh)})
		caseToTask = append(caseToTask, i)
		remaining++
	}

	for remaining > 0 {
		chosen, recv, ok := reflect.Select(cases)
		if chosen == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
			return context.Canceled
		}
		idx := caseToTask[chosen]
		if ok {
			result, _ := recv.Interface().(pregelTaskResult)
			results[idx] = result
		}
		cases[chosen].Chan = reflect.Value{}
		caseToTask[chosen] = -1
		remaining--
	}

	return nil
}

func executeCallTask[State any](ctx context.Context, task pregelTask[State], opts executeTasksOptions) pregelTaskResult {
	call := task.call
	if call == nil || call.Fn == nil {
		return pregelTaskResult{taskID: task.id, node: task.name, path: append([]any(nil), task.path...), checkpointNS: task.checkpointNS, tags: append([]string(nil), task.tags...), err: fmt.Errorf("invalid call task")}
	}
	runInput := task.input
	callTask := task
	var zero State
	callTask.input = zero

	callNode := &NodeSpec[State]{
		Name:        task.name,
		RetryPolicy: call.RetryPolicy,
		CachePolicy: call.CachePolicy,
		Fn: func(callCtx context.Context, _ State) (NodeResult, error) {
			return call.Fn(callCtx, Dyn(runInput))
		},
	}

	return executeTask(ctx, callNode, callTask, executeTaskOptions[State]{
		config:       opts.config,
		store:        opts.store,
		cache:        opts.cache,
		contextValue: opts.contextValue,
		isReplaying:  opts.isReplaying,
		step:         opts.step,
		stop:         opts.stop,
		streamOut:    opts.streamOut,
		streamModes:  opts.streamModes,
		namespace:    opts.namespace,
	})
}

// executeTask runs a single node function, handling interrupt signals and
// Command return values.
type executeTaskOptions[State any] struct {
	config       *Config
	store        Store
	cache        Cache
	contextValue any
	managed      map[string]ManagedValueSpec
	streamOut    chan<- StreamPart
	streamModes  streamModeSet
	namespace    []string
	step         int
	stop         int
	isReplaying  bool
}

func executeTask[State any](ctx context.Context, node *NodeSpec[State], task pregelTask[State], opts executeTaskOptions[State]) pregelTaskResult {
	inputState, inputErr := coerceStateInput[State](task.input)
	if inputErr != nil {
		result := baseTaskResult(task)
		result.err = fmt.Errorf("invalid task input for node '%s': %w", task.name, inputErr)
		return result
	}

	sp := &taskScratchpad{resume: task.resumeValues}
	taskCtx, pgScratch := buildTaskExecutionContext(ctx, task, opts, sp)

	if len(opts.managed) > 0 {
		resolved, err := applyManagedValuesToState(inputState, opts.managed, pgScratch)
		if err != nil {
			result := baseTaskResult(task)
			result.err = err
			return result
		}
		inputState = resolved
	}

	var (
		pendingWrites []pregelWrite
		mu            sync.Mutex
	)
	sp.onResume = func(resume []Dynamic) {
		mu.Lock()
		pendingWrites = append(pendingWrites, pregelWrite{channel: pregelResume, value: resumeValues(resume)})
		mu.Unlock()
	}
	taskCtx = WithSend(taskCtx, func(targetNode string, arg Dynamic) {
		// Write a Send object to the TASKS channel (a Topic) rather than
		// directly to the target node's channel. prepareNextTasks reads the
		// TASKS channel in the next superstep and creates PUSH tasks with
		// Send.Arg as the node input, mirroring Python LangGraph behavior.
		mu.Lock()
		pendingWrites = append(pendingWrites, pregelWrite{
			channel: pregelTasks,
			value:   Send{Node: targetNode, Arg: arg},
		})
		mu.Unlock()
	})
	taskCtx = WithCall(taskCtx, func(call Call) {
		if call.Fn == nil {
			return
		}
		mu.Lock()
		pendingWrites = append(pendingWrites, pregelWrite{channel: pregelTasks, value: call})
		mu.Unlock()
	})

	result, nodeIvs, execErr := executeTaskNode(taskCtx, node, task, inputState, opts)
	cacheKey := task.cacheKey
	if execErr == nil {
		execErr = maybeCacheTaskResult(taskCtx, opts.cache, cacheKey, result, nodeIvs)
	}

	if len(nodeIvs) > 0 {
		return interruptedTaskResult(task, nodeIvs, pendingWrites)
	}

	writes, parentCmd, execErr := taskResultWrites(node, result, execErr)
	if parentCmd != nil {
		result := baseTaskResult(task)
		result.parentCmd = parentCmd
		return result
	}
	writes = append(writes, pendingWrites...)
	final := baseTaskResult(task)
	final.writes = writes
	final.err = execErr
	return final
}

func baseTaskResult[State any](task pregelTask[State]) pregelTaskResult {
	return pregelTaskResult{
		taskID:       task.id,
		node:         task.name,
		path:         append([]any(nil), task.path...),
		checkpointNS: task.checkpointNS,
		tags:         append([]string(nil), task.tags...),
	}
}

func buildTaskExecutionContext[State any](ctx context.Context, task pregelTask[State], opts executeTaskOptions[State], sp *taskScratchpad) (context.Context, *PregelScratchpad) {
	taskCtx := context.WithValue(ctx, taskIDContextKey{}, task.id)
	taskCtx = withTaskScratchpad(taskCtx, sp)
	cfg := DefaultConfig()
	if opts.config != nil {
		cfg = *opts.config
	}
	taskCtx = WithConfig(taskCtx, cfg)
	if opts.store != nil {
		taskCtx = WithStore(taskCtx, opts.store)
	}
	customWriter := GetStreamWriter(taskCtx)
	if opts.streamOut != nil {
		customWriter = func(v any) {
			mode, data, ns := streamWriterPayload(v, opts.namespace)
			if mode == StreamModeMessages && taskHasTag(task, TagNoStream) {
				return
			}
			if !opts.streamModes.enabled(mode) {
				return
			}
			opts.streamOut <- StreamPart{Type: mode, Data: data, Namespace: ns}
		}
		taskCtx = WithStreamWriter(taskCtx, customWriter)
	}
	pgScratch := &PregelScratchpad{
		Step:          opts.step,
		Stop:          opts.stop,
		Config:        &cfg,
		Store:         opts.store,
		ChannelValues: stateToChannelValues(task.input),
	}
	taskCtx = WithPregelScratchpad(taskCtx, pgScratch)
	taskCtx = WithRuntime(taskCtx, &Runtime{
		Context:      opts.contextValue,
		Store:        opts.store,
		StreamWriter: customWriter,
		ExecutionInfo: &ExecutionInfo{
			NodeAttempt:          1,
			NodeFirstAttemptTime: time.Now().Unix(),
			IsReplaying:          opts.isReplaying,
		},
	})
	return taskCtx, pgScratch
}

func executeTaskNode[State any](
	taskCtx context.Context,
	node *NodeSpec[State],
	task pregelTask[State],
	inputState State,
	opts executeTaskOptions[State],
) (taskResult NodeResult, taskInterrupt []Interrupt, taskErr error) {
	var (
		result  NodeResult
		nodeIvs []Interrupt
	)

	runFn := func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				if sig, ok := r.(nodeInterruptSignal); ok {
					nodeIvs = append(nodeIvs, sig.interrupt)
					err = &nodeInterruptError{interrupt: sig.interrupt}
					return
				}
				panic(r)
			}
		}()
		result, err = node.Fn(taskCtx, inputState)
		return err
	}

	cacheKey := task.cacheKey
	fromCache, execErr := loadTaskResultFromCache(taskCtx, opts.cache, cacheKey, task.name, &result)
	if execErr == nil && !fromCache {
		execErr = runNodeWithRetry(taskCtx, node, runFn)
	}
	if execErr == nil && !fromCache && len(task.writers) > 0 {
		result, execErr = runTaskWriters(taskCtx, task, inputState, result)
	}
	return result, nodeIvs, execErr
}

func loadTaskResultFromCache(
	ctx context.Context,
	cache Cache,
	cacheKey *CacheKey,
	nodeName string,
	result *NodeResult,
) (bool, error) {
	if cache == nil || cacheKey == nil {
		return false, nil
	}
	cached, hit, err := cache.Get(ctx, *cacheKey)
	if err != nil {
		return false, err
	}
	if !hit {
		return false, nil
	}
	cachedResult, ok := cached.(NodeResult)
	if !ok {
		return false, fmt.Errorf("cache entry for node '%s' has invalid type %T", nodeName, cached)
	}
	*result = cachedResult
	return true, nil
}

func runNodeWithRetry[State any](ctx context.Context, node *NodeSpec[State], runFn func() error) error {
	if node != nil && len(node.RetryPolicy) > 0 {
		return executeWithRetryPolicies(ctx, node.RetryPolicy, runFn)
	}
	return runFn()
}

func runTaskWriters[State any](ctx context.Context, task pregelTask[State], input State, result NodeResult) (NodeResult, error) {
	taskMeta := TaskContext[State]{
		ID:           task.id,
		Name:         task.name,
		Path:         append([]any(nil), task.path...),
		CheckpointNS: task.checkpointNS,
		Triggers:     append([]string(nil), task.triggers...),
		Tags:         append([]string(nil), task.tags...),
		Input:        input,
		IsPush:       task.isPush,
		InputSchema:  task.inputSchema,
		Subgraphs:    append([]string(nil), task.subgraphs...),
	}
	if task.cacheKey != nil {
		k := *task.cacheKey
		taskMeta.CacheKey = &k
	}
	var err error
	for _, writer := range task.writers {
		result, err = writer(ctx, taskMeta, result)
		if err != nil {
			return result, err
		}
	}
	return result, nil
}

func maybeCacheTaskResult(ctx context.Context, cache Cache, cacheKey *CacheKey, result NodeResult, interrupts []Interrupt) error {
	if cache == nil || cacheKey == nil || len(interrupts) > 0 {
		return nil
	}
	return cache.Set(ctx, *cacheKey, result)
}

func interruptedTaskResult[State any](task pregelTask[State], nodeIvs []Interrupt, pendingWrites []pregelWrite) pregelTaskResult {
	writes := make([]pregelWrite, 0, len(nodeIvs)+len(pendingWrites))
	for _, iv := range nodeIvs {
		writes = append(writes, pregelWrite{channel: pregelInterrupt, value: iv})
	}
	writes = append(writes, pendingWrites...)
	result := baseTaskResult(task)
	result.writes = writes
	result.interrupts = nodeIvs
	return result
}

func taskResultWrites[State any](node *NodeSpec[State], result NodeResult, execErr error) ([]pregelWrite, *Command, error) {
	if execErr == nil {
		writes, err := outputToWrites(node, result)
		return writes, nil, err
	}
	var pce *parentCommandError
	if errors.As(execErr, &pce) {
		return nil, pce.cmd, nil
	}
	return []pregelWrite{{channel: pregelError, value: execErr.Error()}}, nil, execErr
}

func effectiveStore(config Config, compiled Store) Store {
	if s, ok := config.Store.(Store); ok && s != nil {
		return s
	}
	return compiled
}

func effectiveCache(config Config, compiled Cache) Cache {
	if c, ok := config.Cache.(Cache); ok && c != nil {
		return c
	}
	return compiled
}

func buildNodeCacheKey[State any](node *NodeSpec[State], task pregelTask[State], config Config) (CacheKey, bool) {
	if node == nil || node.CachePolicy == nil {
		return CacheKey{}, false
	}
	keyFn := node.CachePolicy.KeyFunc
	if keyFn == nil {
		keyFn = DefaultCacheKey
	}
	key := CacheKey{
		NS:  []string{config.ThreadID, config.CheckpointNS, node.Name},
		Key: keyFn(task.input),
	}
	if node.CachePolicy.TTL != nil {
		ttl := int(node.CachePolicy.TTL.Seconds())
		key.TTL = &ttl
	}
	return key, true
}

func buildCallCacheKey[State any](task pregelTask[State], config Config) (CacheKey, bool) {
	if task.call == nil || task.call.CachePolicy == nil {
		return CacheKey{}, false
	}
	keyFn := task.call.CachePolicy.KeyFunc
	if keyFn == nil {
		keyFn = DefaultCacheKey
	}
	key := CacheKey{
		NS:  []string{config.ThreadID, config.CheckpointNS, task.name},
		Key: keyFn(task.input),
	}
	if task.call.CachePolicy.TTL != nil {
		ttl := int(task.call.CachePolicy.TTL.Seconds())
		key.TTL = &ttl
	}
	return key, true
}

func coerceStateInput[State any](input any) (State, error) {
	if v, ok := input.(State); ok {
		return v, nil
	}
	var zero State
	target := reflect.TypeOf(zero)
	if target == nil {
		if input == nil {
			return zero, nil
		}
		if v, ok := input.(State); ok {
			return v, nil
		}
		return zero, fmt.Errorf("cannot convert %T to %T", input, zero)
	}
	converted, err := coerceValueToType(input, target)
	if err != nil {
		return zero, err
	}
	v, ok := converted.Interface().(State)
	if !ok {
		return zero, fmt.Errorf("cannot convert %T to %T", input, zero)
	}
	return v, nil
}

func stateToChannelValues(state any) map[string]any {
	if m, ok := any(state).(map[string]any); ok {
		out := make(map[string]any, len(m))
		for k, v := range m {
			out[k] = v
		}
		return out
	}
	return map[string]any{}
}

func applyManagedValuesToState[State any](input State, managed map[string]ManagedValueSpec, scratchpad *PregelScratchpad) (State, error) {
	if len(managed) == 0 {
		return input, nil
	}
	if mapped, ok, err := applyManagedValuesToMapState[State](input, managed, scratchpad); ok || err != nil {
		return mapped, err
	}
	return applyManagedValuesToStructState(input, managed, scratchpad)
}

func applyManagedValuesToMapState[State any](input State, managed map[string]ManagedValueSpec, scratchpad *PregelScratchpad) (appliedState State, successState bool, applyErr error) {
	m, ok := any(input).(map[string]any)
	if !ok {
		var zero State
		return zero, false, nil
	}
	out := make(map[string]any, len(m)+len(managed))
	for k, v := range m {
		out[k] = v
	}
	for name, spec := range managed {
		out[name] = spec.Get(scratchpad)
	}
	typed, castOK := any(out).(State)
	if !castOK {
		var zero State
		return zero, true, fmt.Errorf("managed value injection failed converting map to state type")
	}
	return typed, true, nil
}

func applyManagedValuesToStructState[State any](input State, managed map[string]ManagedValueSpec, scratchpad *PregelScratchpad) (State, error) {
	rv := reflect.ValueOf(input)
	rt := reflect.TypeOf(input)
	if rt == nil || rt.Kind() != reflect.Struct {
		return input, nil
	}
	out := reflect.New(rt).Elem()
	out.Set(rv)
	for name, spec := range managed {
		applyManagedValueToField(out, name, spec.Get(scratchpad))
	}
	typed, ok := out.Interface().(State)
	if ok {
		return typed, nil
	}
	return input, nil
}

func applyManagedValueToField(out reflect.Value, name string, raw any) {
	field := out.FieldByName(name)
	if !field.IsValid() || !field.CanSet() {
		return
	}
	value := reflect.ValueOf(raw)
	if !value.IsValid() {
		return
	}
	if value.Type().AssignableTo(field.Type()) {
		field.Set(value)
		return
	}
	if value.Type().ConvertibleTo(field.Type()) {
		field.Set(value.Convert(field.Type()))
	}
}

// nodeInterruptError wraps an interrupt signal as an error (used internally
// to distinguish interrupt from other errors in retry loops).
type nodeInterruptError struct {
	interrupt Interrupt
}

func (e *nodeInterruptError) Error() string {
	return "node interrupted"
}

// outputToWrites converts a typed NodeResult into pregel writes.
func outputToWrites[State any](node *NodeSpec[State], output NodeResult) ([]pregelWrite, error) {
	if output.Command != nil {
		return outputCommandWrites(node, output.Command)
	}
	if output.State != nil {
		return outputStateWrites(output.State.Value())
	}
	if len(output.Writes) == 0 {
		return nil, nil
	}
	return outputPartialWrites(output.Writes), nil
}

func outputCommandWrites[State any](node *NodeSpec[State], command *Command) ([]pregelWrite, error) {
	stateWrites, sendTargets, _, _, err := commandResult(command)
	if err != nil {
		return nil, err
	}
	if err := validateCommandDestinations(node, sendTargets); err != nil {
		return nil, err
	}
	writes := append([]pregelWrite(nil), stateWrites...)
	for _, s := range sendTargets {
		writes = append(writes, pregelWrite{channel: pregelTasks, value: s})
	}
	return writes, nil
}

func outputStateWrites(state any) ([]pregelWrite, error) {
	if m, ok := state.(map[string]any); ok {
		writes := make([]pregelWrite, 0, len(m))
		for k, v := range m {
			writes = append(writes, pregelWrite{channel: k, value: v})
		}
		return writes, nil
	}
	rt := reflect.TypeOf(state)
	if rt != nil && rt.Kind() == reflect.Struct {
		return outputStructWrites(reflect.ValueOf(state), rt), nil
	}
	return nil, fmt.Errorf("unsupported node state output type %T", state)
}

func outputStructWrites(rv reflect.Value, rt reflect.Type) []pregelWrite {
	writes := make([]pregelWrite, 0, rt.NumField())
	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)
		if !field.IsExported() {
			continue
		}
		writes = append(writes, pregelWrite{channel: field.Name, value: rv.Field(i).Interface()})
	}
	return writes
}

func outputPartialWrites(writes map[string]Dynamic) []pregelWrite {
	out := make([]pregelWrite, 0, len(writes))
	for k, v := range writes {
		out = append(out, pregelWrite{channel: k, value: v.Value()})
	}
	return out
}

func validateCommandDestinations[State any](node *NodeSpec[State], sends []Send) error {
	if node == nil || len(node.Destinations) == 0 {
		return nil
	}
	allowed := make(map[string]struct{}, len(node.Destinations))
	for _, d := range node.Destinations {
		allowed[d] = struct{}{}
	}
	for _, s := range sends {
		if _, ok := allowed[s.Node]; ok {
			continue
		}
		return fmt.Errorf("command from node '%s' routes to '%s' which is not declared in destinations", node.Name, s.Node)
	}
	return nil
}

// --- stream emission helpers ---

func emitTaskStart[State any](out chan<- StreamPart, task pregelTask[State], step int, modes streamModeSet, namespace []string) {
	if out == nil {
		return
	}
	if taskHasTag(task, TagHidden) {
		return
	}
	payload := map[string]any{
		"id":              task.id,
		"name":            task.name,
		"path":            task.path,
		"checkpoint_ns":   task.checkpointNS,
		"checkpoint_path": splitCheckpointNamespace(task.checkpointNS),
		"step":            step,
		"triggers":        task.triggers,
		"input":           task.input,
		"input_schema":    task.inputSchema,
		"cache_key":       task.cacheKey,
		"subgraphs":       task.subgraphs,
	}
	if modes.enabled(StreamModeTasks) {
		out <- StreamPart{
			Type:      StreamModeTasks,
			Namespace: append([]string(nil), namespace...),
			Data:      payload,
		}
	}
	emitDebugEvent(out, modes, namespace, step, "task", payload)
}

func emitTaskFinish(out chan<- StreamPart, r pregelTaskResult, step int, modes streamModeSet, namespace []string) {
	if out == nil {
		return
	}
	if containsString(r.tags, TagHidden) {
		return
	}
	payload := taskResultPayload(r)
	payload["step"] = step
	payload["path"] = r.path
	if modes.enabled(StreamModeTasks) {
		out <- StreamPart{
			Type:      StreamModeTasks,
			Namespace: append([]string(nil), namespace...),
			Data:      payload,
		}
	}
	emitDebugEvent(out, modes, namespace, step, "task_result", payload)
}

func emitUpdate[State, Context, Input, Output any](out chan<- StreamPart, g *StateGraph[State, Context, Input, Output], channels *pregelChannelMap, r pregelTaskResult, _ int, modes streamModeSet, namespace []string) {
	if out == nil {
		return
	}
	if nodeSpecHasTag(g.nodes[r.node], TagHidden) {
		return
	}
	switch {
	case modes.enabled(StreamModeUpdates):
		// Emit what the node wrote.
		update := make(map[string]any, len(r.writes))
		for _, w := range r.writes {
			update[w.channel] = w.value
		}
		out <- StreamPart{
			Type:      StreamModeUpdates,
			Namespace: append([]string(nil), namespace...),
			Data:      map[string]any{r.node: update},
		}
	case modes.enabled(StreamModeValues):
		// Full state snapshot after each update.
		stateMap := channels.toStateMap()
		out <- StreamPart{
			Type:      StreamModeValues,
			Namespace: append([]string(nil), namespace...),
			Data:      stateMap,
		}
	}
}

func emitInterrupt(out chan<- StreamPart, iv Interrupt, modes streamModeSet, namespace []string) {
	if out == nil {
		return
	}
	if !modes.enabled(StreamModeUpdates) {
		return
	}
	out <- StreamPart{
		Type:       StreamModeUpdates,
		Namespace:  append([]string(nil), namespace...),
		Interrupts: []Interrupt{iv},
		Data: map[string]any{
			pregelInterrupt: iv,
		},
	}
}

// saveCheckpoint persists the current channel/version state to the configured
// checkpointer. Errors are logged as best-effort; a failed checkpoint does not
// abort execution.
func saveCheckpoint(
	ctx context.Context,
	config Config,
	channels *pregelChannelMap,
	step int,
	channelVersions map[string]int64,
	versionsSeen map[string]map[string]int64,
	updatedChannels []string,
	next []string,
	pendingWrites []checkpoint.PendingWrite,
	externalStateVersion string,
	streamOut chan<- StreamPart,
	streamModes streamModeSet,
	namespace []string,
) {
	if config.Checkpointer == nil || config.ThreadID == "" {
		return
	}
	cpCfg := config.CheckpointConfig()
	snap := channels.snapshot()
	cp := &checkpoint.Checkpoint{
		V:               1,
		ID:              fmt.Sprintf("%d", time.Now().UnixNano()),
		TS:              time.Now().UTC().Format(time.RFC3339Nano),
		ChannelValues:   snap,
		ChannelVersions: checkpointVersionMap(channelVersions),
		VersionsSeen:    checkpointNestedVersionMap(versionsSeen),
		UpdatedChannels: append([]string(nil), updatedChannels...),
	}
	meta := &checkpoint.CheckpointMetadata{
		Source: "loop",
		Step:   step,
	}
	if strings.TrimSpace(externalStateVersion) != "" {
		meta.Extra = map[string]any{ExternalStateVersionMetadataKey: externalStateVersion}
	}
	nextCfg, err := config.Checkpointer.Put(ctx, cpCfg, cp, meta)
	if err != nil {
		return
	}
	if nextCfg != nil {
		config.CheckpointID = nextCfg.CheckpointID
	}
	if len(pendingWrites) > 0 {
		writeCfg := cpCfg
		if nextCfg != nil {
			writeCfg = nextCfg
		}
		_ = config.Checkpointer.PutWrites(ctx, writeCfg, pendingWrites, "")
	}
	payload := checkpointStreamPayload(cp, meta, config, pendingWrites, next)
	if streamOut != nil && streamModes.enabled(StreamModeCheckpoints) {
		streamOut <- StreamPart{
			Type:      StreamModeCheckpoints,
			Namespace: append([]string(nil), namespace...),
			Data:      payload,
		}
	}
	emitDebugEvent(streamOut, streamModes, namespace, step, "checkpoint", payload)
}

type checkpointSnapshot struct {
	config               *Config
	channels             *pregelChannelMap
	channelVersions      map[string]int64
	versionsSeen         map[string]map[string]int64
	streamOut            chan<- StreamPart
	streamModes          streamModeSet
	updatedChannels      []string
	next                 []string
	pendingWrites        []checkpoint.PendingWrite
	externalStateVersion string
	namespace            []string
	step                 int
}

type checkpointAsyncWriter struct {
	wg sync.WaitGroup
}

func (w *checkpointAsyncWriter) Submit(fn func()) {
	w.Wait()
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		fn()
	}()
}

func (w *checkpointAsyncWriter) Wait() {
	w.wg.Wait()
}

func checkpointByDurability(ctx context.Context, durability DurabilityMode, writer *checkpointAsyncWriter, snap checkpointSnapshot) {
	if snap.config == nil || snap.config.Checkpointer == nil || snap.config.ThreadID == "" {
		return
	}
	switch durability {
	case DurabilityExit:
		return
	case DurabilityAsync:
		configCopy := *snap.config
		channelsCopy := snap.channels.snapshot()
		channelVersionsCopy := cloneVersionMap(snap.channelVersions)
		versionsSeenCopy := cloneNestedVersionMap(snap.versionsSeen)
		updatedChannelsCopy := append([]string(nil), snap.updatedChannels...)
		nextCopy := append([]string(nil), snap.next...)
		pendingWritesCopy := clonePendingWrites(snap.pendingWrites)
		externalStateVersion := snap.externalStateVersion
		namespaceCopy := append([]string(nil), snap.namespace...)
		writer.Submit(func() {
			saveCheckpointFromSnapshot(ctx, configCopy, channelsCopy, snap.step, channelVersionsCopy, versionsSeenCopy, updatedChannelsCopy, nextCopy, pendingWritesCopy, externalStateVersion, snap.streamOut, snap.streamModes, namespaceCopy)
		})
	default:
		saveCheckpoint(ctx, *snap.config, snap.channels, snap.step, snap.channelVersions, snap.versionsSeen, snap.updatedChannels, snap.next, snap.pendingWrites, snap.externalStateVersion, snap.streamOut, snap.streamModes, snap.namespace)
	}
}

func saveCheckpointFromSnapshot(
	ctx context.Context,
	config Config,
	channelValues map[string]any,
	step int,
	channelVersions map[string]int64,
	versionsSeen map[string]map[string]int64,
	updatedChannels []string,
	next []string,
	pendingWrites []checkpoint.PendingWrite,
	externalStateVersion string,
	streamOut chan<- StreamPart,
	streamModes streamModeSet,
	namespace []string,
) {
	if config.Checkpointer == nil || config.ThreadID == "" {
		return
	}
	cpCfg := config.CheckpointConfig()
	cp := &checkpoint.Checkpoint{
		V:               1,
		ID:              fmt.Sprintf("%d", time.Now().UnixNano()),
		TS:              time.Now().UTC().Format(time.RFC3339Nano),
		ChannelValues:   channelValues,
		ChannelVersions: checkpointVersionMap(channelVersions),
		VersionsSeen:    checkpointNestedVersionMap(versionsSeen),
		UpdatedChannels: append([]string(nil), updatedChannels...),
	}
	meta := &checkpoint.CheckpointMetadata{
		Source: "loop",
		Step:   step,
	}
	if strings.TrimSpace(externalStateVersion) != "" {
		meta.Extra = map[string]any{ExternalStateVersionMetadataKey: externalStateVersion}
	}
	nextCfg, err := config.Checkpointer.Put(ctx, cpCfg, cp, meta)
	if err != nil {
		return
	}
	if nextCfg != nil {
		config.CheckpointID = nextCfg.CheckpointID
	}
	if len(pendingWrites) > 0 {
		writeCfg := cpCfg
		if nextCfg != nil {
			writeCfg = nextCfg
		}
		_ = config.Checkpointer.PutWrites(ctx, writeCfg, pendingWrites, "")
	}
	payload := checkpointStreamPayload(cp, meta, config, pendingWrites, next)
	if streamOut != nil && streamModes.enabled(StreamModeCheckpoints) {
		streamOut <- StreamPart{
			Type:      StreamModeCheckpoints,
			Namespace: append([]string(nil), namespace...),
			Data:      payload,
		}
	}
	emitDebugEvent(streamOut, streamModes, namespace, step, "checkpoint", payload)
}

func cloneVersionMap(in map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneNestedVersionMap(in map[string]map[string]int64) map[string]map[string]int64 {
	out := make(map[string]map[string]int64, len(in))
	for k, inner := range in {
		copyInner := make(map[string]int64, len(inner))
		for ik, iv := range inner {
			copyInner[ik] = iv
		}
		out[k] = copyInner
	}
	return out
}

func clonePendingWrites(in []checkpoint.PendingWrite) []checkpoint.PendingWrite {
	out := make([]checkpoint.PendingWrite, len(in))
	copy(out, in)
	return out
}

type streamModeSet map[StreamMode]struct{}

func newStreamModeSet(modes ...StreamMode) streamModeSet {
	set := make(streamModeSet, len(modes))
	for _, mode := range modes {
		if mode == "" {
			continue
		}
		set[mode] = struct{}{}
	}
	return set
}

func (s streamModeSet) empty() bool {
	return len(s) == 0
}

func (s streamModeSet) enabled(mode StreamMode) bool {
	if len(s) == 0 {
		return false
	}
	if _, ok := s[mode]; ok {
		return true
	}
	if mode != StreamModeDebug {
		_, ok := s[StreamModeDebug]
		return ok
	}
	return false
}

func runtimeChannelSchema[State, Context, Input, Output any](g *StateGraph[State, Context, Input, Output]) map[string]Channel {
	schema := make(map[string]Channel, len(g.channels)+len(g.waitingEdges)+1)
	for name, channel := range g.channels {
		schema[name] = channel
	}
	for _, we := range g.waitingEdges {
		if we.Target == End {
			continue
		}
		channelName := waitingEdgeChannelName(we)
		if _, exists := schema[channelName]; exists {
			continue
		}
		sources := append([]string(nil), we.Sources...)
		node := g.nodes[we.Target]
		if node != nil && node.Defer {
			schema[channelName] = NewNamedBarrierValueAfterFinish(sources)
			continue
		}
		schema[channelName] = NewNamedBarrierValue(sources)
	}
	if _, ok := schema[pregelTasks]; !ok {
		schema[pregelTasks] = NewTopic(false)
	}
	return schema
}

func splitCheckpointNamespace(ns string) []string {
	if ns == "" {
		return nil
	}
	parts := strings.Split(ns, "|")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		out = append(out, trimmed)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func mergeNamespaces(base, override []string) []string {
	if len(override) == 0 {
		return append([]string(nil), base...)
	}
	ns := make([]string, 0, len(base)+len(override))
	ns = append(ns, base...)
	ns = append(ns, override...)
	return ns
}

func streamWriterPayload(v any, namespace []string) (mode StreamMode, data any, outNamespace []string) {
	if emit, ok := v.(StreamEmit); ok {
		mode = emit.Mode
		if mode == "" {
			mode = StreamModeCustom
		}
		return mode, emit.Data, mergeNamespaces(namespace, emit.Namespace)
	}
	return StreamModeCustom, v, append([]string(nil), namespace...)
}

func markTaskVersionsSeen[State any](task pregelTask[State], channelVersions map[string]int64, versionsSeen map[string]map[string]int64) {
	seen := make(map[string]int64, len(channelVersions))
	for channel, version := range channelVersions {
		seen[channel] = version
	}
	versionsSeen[task.name] = seen
}

func resolveStateAuthority(config Config) (resolvedStateAuthority, error) {
	mode := effectiveStateMode(config)
	stateStore := config.StateStore
	if config.CheckpointStore != nil {
		if stateStore == nil {
			stateStore = config.CheckpointStore.StateStore()
		}
		if mode == "" {
			mode = config.CheckpointStore.Mode()
		}
	}
	if mode == "" {
		mode = StateStoreModeCheckpointAuthoritative
	}

	resolved := resolvedStateAuthority{mode: mode, stateStore: stateStore}
	if !isAuthoritativeExternalStateMode(mode) {
		return resolved, nil
	}
	if config.Checkpointer == nil {
		return resolvedStateAuthority{}, fmt.Errorf("authoritative_external_state mode requires a checkpointer")
	}
	if strings.TrimSpace(config.ThreadID) == "" {
		return resolvedStateAuthority{}, fmt.Errorf("authoritative_external_state mode requires Config.ThreadID")
	}
	if stateStore == nil {
		return resolvedStateAuthority{}, fmt.Errorf("authoritative_external_state mode requires Config.StateStore or Config.CheckpointStore.StateStore")
	}
	return resolved, nil
}

func loadAuthoritativeStateIfNeeded(ctx context.Context, authority resolvedStateAuthority, config Config) (*AuthoritativeStateSnapshot, error) {
	if !isAuthoritativeExternalStateMode(authority.mode) {
		return nil, nil
	}
	if authority.stateStore == nil {
		return nil, fmt.Errorf("authoritative_external_state mode requires a state store")
	}
	snapshot, err := authority.stateStore.Read(ctx, StateStoreReadRequest{
		ThreadID:     config.ThreadID,
		CheckpointID: config.CheckpointID,
		CheckpointNS: config.CheckpointNS,
	})
	if err != nil {
		return nil, fmt.Errorf("loading authoritative state: %w", err)
	}
	if snapshot == nil {
		return nil, fmt.Errorf("authoritative_external_state mode requires state store to return a snapshot")
	}
	if strings.TrimSpace(snapshot.Version) == "" {
		return nil, fmt.Errorf("authoritative_external_state mode requires state store snapshot version")
	}
	if snapshot.Values == nil {
		snapshot.Values = map[string]any{}
	}
	return snapshot, nil
}

func validateAuthoritativeStateConsistency(
	config Config,
	tuple *checkpoint.CheckpointTuple,
	snapshot *AuthoritativeStateSnapshot,
	hasResume bool,
	isReplaying bool,
) error {
	if snapshot == nil || (!hasResume && !isReplaying) {
		return nil
	}
	if tuple == nil || tuple.Checkpoint == nil {
		return nil
	}
	checkpointVersion := checkpointExternalStateVersion(tuple.Metadata)
	if checkpointVersion == snapshot.Version {
		return nil
	}
	checkpointID := ""
	if tuple.Checkpoint != nil {
		checkpointID = tuple.Checkpoint.ID
	}
	return &ExternalStateConflictError{
		ThreadID:          config.ThreadID,
		CheckpointID:      checkpointID,
		CheckpointNS:      config.CheckpointNS,
		CheckpointVersion: checkpointVersion,
		ExternalVersion:   snapshot.Version,
	}
}

func checkpointExternalStateVersion(meta *checkpoint.CheckpointMetadata) string {
	if meta == nil || len(meta.Extra) == 0 {
		return ""
	}
	raw, ok := meta.Extra[ExternalStateVersionMetadataKey]
	if !ok || raw == nil {
		return ""
	}
	if s, ok := raw.(string); ok {
		return strings.TrimSpace(s)
	}
	return strings.TrimSpace(fmt.Sprint(raw))
}

func checkpointVersionMap(in map[string]int64) map[string]checkpoint.Version {
	out := make(map[string]checkpoint.Version, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func checkpointNestedVersionMap(in map[string]map[string]int64) map[string]map[string]checkpoint.Version {
	out := make(map[string]map[string]checkpoint.Version, len(in))
	for k, v := range in {
		inner := make(map[string]checkpoint.Version, len(v))
		for ik, iv := range v {
			inner[ik] = iv
		}
		out[k] = inner
	}
	return out
}

func cloneVersionTokenMap(in map[string]checkpoint.Version) map[string]checkpoint.Version {
	out := make(map[string]checkpoint.Version, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneNestedVersionTokenMap(in map[string]map[string]checkpoint.Version) map[string]map[string]checkpoint.Version {
	out := make(map[string]map[string]checkpoint.Version, len(in))
	for k, v := range in {
		inner := make(map[string]checkpoint.Version, len(v))
		for ik, iv := range v {
			inner[ik] = iv
		}
		out[k] = inner
	}
	return out
}

func finishChannels(channels *pregelChannelMap) map[string]bool {
	changed := make(map[string]bool)
	for name, channel := range channels.channels {
		if channel.Finish() {
			changed[name] = true
		}
	}
	return changed
}

func mergeChangedChannels(sets ...map[string]bool) map[string]bool {
	out := make(map[string]bool)
	for _, set := range sets {
		for channel, changed := range set {
			if changed {
				out[channel] = true
			}
		}
	}
	return out
}

func updatedChannelsFromChanged(changed map[string]bool) []string {
	if len(changed) == 0 {
		return nil
	}
	out := make([]string, 0, len(changed))
	for channel, isChanged := range changed {
		if isChanged {
			out = append(out, channel)
		}
	}
	if len(out) == 0 {
		return nil
	}
	sort.Strings(out)
	return out
}

func emitDebugEvent(out chan<- StreamPart, modes streamModeSet, namespace []string, step int, eventType string, payload any) {
	if out == nil || !modes.enabled(StreamModeDebug) {
		return
	}
	out <- StreamPart{
		Type:      StreamModeDebug,
		Namespace: append([]string(nil), namespace...),
		Data: map[string]any{
			"step":      step,
			"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
			"type":      eventType,
			"payload":   payload,
		},
	}
}

func taskResultPayload(r pregelTaskResult) map[string]any {
	result := map[string]any{}
	var errMsg any
	for _, w := range r.writes {
		switch w.channel {
		case pregelInterrupt, pregelResume:
			continue
		case pregelError:
			errMsg = w.value
			continue
		default:
			result[w.channel] = w.value
		}
	}
	if errMsg == nil && r.err != nil {
		errMsg = r.err.Error()
	}
	interrupts := append([]Interrupt(nil), r.interrupts...)
	return map[string]any{
		"id":              r.taskID,
		"name":            r.node,
		"checkpoint_ns":   r.checkpointNS,
		"checkpoint_path": splitCheckpointNamespace(r.checkpointNS),
		"error":           errMsg,
		"interrupts":      interrupts,
		"result":          result,
	}
}

func checkpointStreamPayload(cp *checkpoint.Checkpoint, meta *checkpoint.CheckpointMetadata, config Config, pendingWrites []checkpoint.PendingWrite, next []string) map[string]any {
	return map[string]any{
		"config":        checkpointStreamConfig(cp, config),
		"metadata":      checkpointStreamMetadata(meta),
		"values":        checkpointStreamValues(cp),
		"next":          checkpointStreamNext(next, pendingWrites),
		"parent_config": nil,
		"tasks":         checkpointStreamTasks(next, pendingWrites),
	}
}

func checkpointStreamValues(cp *checkpoint.Checkpoint) map[string]any {
	values := map[string]any{}
	if cp != nil && cp.ChannelValues != nil {
		for k, v := range cp.ChannelValues {
			values[k] = v
		}
	}
	return values
}

func checkpointStreamConfig(cp *checkpoint.Checkpoint, config Config) map[string]any {
	cfg := map[string]any{
		"thread_id":     config.ThreadID,
		"checkpoint_ns": config.CheckpointNS,
	}
	if cp != nil && cp.ID != "" {
		cfg["checkpoint_id"] = cp.ID
	}
	return cfg
}

func checkpointStreamMetadata(meta *checkpoint.CheckpointMetadata) map[string]any {
	metadata := map[string]any{}
	if meta == nil {
		return metadata
	}
	metadata["source"] = meta.Source
	metadata["step"] = meta.Step
	if version := checkpointExternalStateVersion(meta); version != "" {
		metadata[ExternalStateVersionMetadataKey] = version
	}
	if meta.RunID != "" {
		metadata["run_id"] = meta.RunID
	}
	if len(meta.Parents) > 0 {
		parents := make(map[string]string, len(meta.Parents))
		for k, v := range meta.Parents {
			parents[k] = v
		}
		metadata["parents"] = parents
	}
	return metadata
}

func checkpointStreamNext(next []string, pendingWrites []checkpoint.PendingWrite) []string {
	nextNodes := append([]string(nil), next...)
	pendingNext, _, _ := snapshotNextAndTasks(pendingWrites)
	if len(nextNodes) == 0 && len(pendingNext) > 0 {
		nextNodes = append(nextNodes, pendingNext...)
	}
	return nextNodes
}

func checkpointStreamTasks(next []string, pendingWrites []checkpoint.PendingWrite) []map[string]any {
	checkpointTasks := make([]map[string]any, 0)
	_, pendingTaskList, _ := snapshotNextAndTasks(pendingWrites)
	for _, task := range pendingTaskList {
		checkpointTasks = append(checkpointTasks, map[string]any{
			"id":         task.ID,
			"name":       task.Name,
			"interrupts": append([]Interrupt(nil), task.Interrupts...),
			"state":      nil,
		})
	}
	if len(checkpointTasks) == 0 {
		for _, name := range next {
			checkpointTasks = append(checkpointTasks, map[string]any{
				"name":  name,
				"state": nil,
			})
		}
	}
	return checkpointTasks
}

func nextNodeNames[State any](tasks []pregelTask[State]) []string {
	if len(tasks) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(tasks))
	out := make([]string, 0, len(tasks))
	for _, task := range tasks {
		if task.name == "" {
			continue
		}
		if _, ok := seen[task.name]; ok {
			continue
		}
		seen[task.name] = struct{}{}
		out = append(out, task.name)
	}
	sort.Strings(out)
	if len(out) == 0 {
		return nil
	}
	return out
}
