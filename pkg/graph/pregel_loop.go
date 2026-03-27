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
	requestedCheckpointID := config.CheckpointID

	runtimeSchema := runtimeChannelSchema(g)
	channels := newPregelChannelMap(runtimeSchema)
	allowDynamicChannels := allowsDynamicStateChannels[State]()

	var pendingWrites []checkpoint.PendingWrite
	var loadedTuple *checkpoint.CheckpointTuple
	if config.Checkpointer != nil && config.ThreadID != "" {
		tuple, loadErr := config.Checkpointer.GetTuple(ctx, config.CheckpointConfig())
		if loadErr != nil {
			return pregelLoopResult{}, fmt.Errorf("loading checkpoint: %w", loadErr)
		}
		loadedTuple = tuple
		if tuple != nil && tuple.Checkpoint != nil {
			restored, restoreErr := restoreFromCheckpoint(runtimeSchema, tuple.Checkpoint.ChannelValues)
			if restoreErr != nil {
				return pregelLoopResult{}, fmt.Errorf("restoring checkpoint channels: %w", restoreErr)
			}
			channels = restored
			pendingWrites = append(pendingWrites, tuple.PendingWrites...)
			if tuple.Config != nil && tuple.Config.CheckpointID != "" {
				config.CheckpointID = tuple.Config.CheckpointID
			}
		}
	}

	var (
		interrupts          []Interrupt
		prevNodes           []string
		step                int
		err                 error
		lastUpdatedChannels []string

		channelVersions = make(map[string]int64)
		versionsSeen    = make(map[string]map[string]int64)
		versionCounter  int64
	)

	if loadedTuple != nil && loadedTuple.Checkpoint != nil {
		if len(loadedTuple.Checkpoint.ChannelVersions) > 0 {
			channelVersions = parseVersionMap(loadedTuple.Checkpoint.ChannelVersions)
		}
		if len(loadedTuple.Checkpoint.VersionsSeen) > 0 {
			versionsSeen = parseNestedVersionMap(loadedTuple.Checkpoint.VersionsSeen)
		}
		if loadedTuple.Metadata != nil {
			step = loadedTuple.Metadata.Step + 1
		}
	}
	isReplaying := requestedCheckpointID != "" && loadedTuple != nil && loadedTuple.Checkpoint != nil

	resumeMap, resumeVals, hasResume, cfgErr := resumeConfig(config.Metadata)
	if cfgErr != nil {
		return pregelLoopResult{}, cfgErr
	}
	if hasResume && (config.Checkpointer == nil || config.ThreadID == "") {
		return pregelLoopResult{}, fmt.Errorf("interrupt resume requires a checkpointer and thread id")
	}
	pendingWrites, _, err = applyResumeWrites(pendingWrites, resumeMap, resumeVals)
	if err != nil {
		return pregelLoopResult{}, err
	}

	// Map input into channels unless this invocation is explicitly resuming.
	if !hasResume && !isReplaying {
		if err := mapInputToChannels(channels, any(opts.input)); err != nil {
			return pregelLoopResult{}, fmt.Errorf("mapping input to channels: %w", err)
		}
	} else if hasResume {
		seen := make(map[string]int64, len(channelVersions))
		for k, v := range channelVersions {
			seen[k] = v
		}
		versionsSeen[pregelInterrupt] = seen
	}

	bumpChannelVersion := func(channel string) {
		versionCounter++
		channelVersions[channel] = versionCounter
	}
	if len(channelVersions) > 0 {
		for _, version := range channelVersions {
			if version > versionCounter {
				versionCounter = version
			}
		}
	} else {
		for key, ch := range channels.channels {
			if ch.IsAvailable() {
				bumpChannelVersion(key)
			}
		}
	}

	// Prepare initial tasks (step 0: nodes from Start, a TASKS channel is empty).
	tasks, err := prepareNextTasks(ctx, g, config, channels, pendingWrites, prevNodes, step, false)
	if err != nil {
		return pregelLoopResult{}, fmt.Errorf("preparing initial tasks: %w", err)
	}
	if hasResume {
		if resumed := prepareResumedTasks(g, config, channels, pendingWrites); len(resumed) > 0 {
			tasks = resumed
		}
	}

	durability := config.Durability
	if durability == "" {
		durability = DurabilityAsync
	}

	asyncCheckpoint := &checkpointAsyncWriter{}
	defer asyncCheckpoint.Wait()

	for len(tasks) > 0 {
		if ctx.Err() != nil {
			return pregelLoopResult{}, ctx.Err()
		}

		// --- interrupt-before check ---
		interruptTasksInput := tasks
		if hasAllInterrupt(opts.interruptBefore) {
			visible := make([]pregelTask[State], 0, len(tasks))
			for _, task := range tasks {
				if taskHasTag(task, TagHidden) {
					continue
				}
				visible = append(visible, task)
			}
			interruptTasksInput = visible
		}
		if matches := interruptTasks(interruptTasksInput, opts.interruptBefore, channelVersions, versionsSeen); len(matches) > 0 {
			beforeWrites := make([]checkpoint.PendingWrite, 0, len(matches))
			for _, task := range matches {
				iv := NewInterrupt(
					Dyn(fmt.Sprintf("Interrupted before node: %s", task.name)),
					fmt.Sprintf("before_%s_%d", task.name, step),
				)
				interrupts = append(interrupts, iv)
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
			pendingWrites = mergePendingWrites(pendingWrites, beforeWrites)
			break
		}

		// --- emit task-start events ---
		for _, task := range tasks {
			emitTaskStart(opts.streamOut, task, step, opts.streamModes, baseNamespace)
		}

		// --- execute tasks (concurrent) ---
		results, taskErr := executeTasks(ctx, g, tasks, executeTasksOptions{
			config:         &config,
			stepTimeout:    config.StepTimeout,
			maxConcurrency: config.MaxConcurrency,
			store:          effectiveStore(config, opts.store),
			cache:          effectiveCache(config, opts.cache),
			contextValue:   opts.contextValue,
			isReplaying:    isReplaying,
			step:           step,
			stop:           config.RecursionLimit,
			streamOut:      opts.streamOut,
			streamModes:    opts.streamModes,
			namespace:      baseNamespace,
		})

		stepPending := collectSpecialWrites(results)

		// --- interrupt-after check ---
		interruptResultsInput := results
		if hasAllInterrupt(opts.interruptAfter) {
			visible := make([]pregelTaskResult, 0, len(results))
			for _, result := range results {
				if containsString(result.tags, TagHidden) {
					continue
				}
				visible = append(visible, result)
			}
			interruptResultsInput = visible
		}
		if matches := interruptResults(interruptResultsInput, opts.interruptAfter, channelVersions, versionsSeen); len(matches) > 0 {
			afterWrites := make([]checkpoint.PendingWrite, 0, len(matches))
			for _, r := range matches {
				iv := NewInterrupt(
					Dyn(fmt.Sprintf("Interrupted after node: %s", r.node)),
					fmt.Sprintf("after_%s_%d", r.node, step),
				)
				interrupts = append(interrupts, iv)
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
			pendingWrites = mergePendingWrites(pendingWrites, append(stepPending, afterWrites...))
			// Apply writes before breaking so state is up to date.
			for _, task := range tasks {
				markTaskVersionsSeen(task, channelVersions, versionsSeen)
			}
			var versionBumped map[string]bool
			interrupts, versionBumped, _, err = applyTaskWrites(g, channels, tasks, results, interrupts, allowDynamicChannels)
			if err != nil {
				return pregelLoopResult{}, fmt.Errorf("applying writes at step %d: %w", step, err)
			}
			for key := range versionBumped {
				bumpChannelVersion(key)
			}
			break
		}

		// --- handle task errors ---
		if taskErr != nil {
			// Parent commands propagate immediately to the caller (AsSubgraphNode).
			// No checkpoint is saved: the subgraph is terminating early so the
			// parent can apply the command.
			var pce *parentCommandError
			if errors.As(taskErr, &pce) {
				return pregelLoopResult{}, taskErr
			}
			pendingWrites = mergePendingWrites(pendingWrites, stepPending)
			if config.Checkpointer != nil && config.ThreadID != "" {
				if durability == DurabilityAsync {
					asyncCheckpoint.Wait()
				}
				saveCheckpoint(ctx, config, channels, step, channelVersions, versionsSeen, nil, nil, pendingWrites, opts.streamOut, opts.streamModes, baseNamespace)
			}
			return pregelLoopResult{}, taskErr
		}
		pendingWrites = mergePendingWrites(pendingWrites, stepPending)

		hasNodeInterrupt := false
		for _, r := range results {
			if len(r.interrupts) > 0 {
				hasNodeInterrupt = true
				break
			}
		}

		// --- emit task-finish / update events ---
		for _, r := range results {
			emitTaskFinish(opts.streamOut, r, step, opts.streamModes, baseNamespace)
		}

		// --- apply writes to channels ---
		// This includes any Send objects written to the TASKS channel by
		// nodes that called GetSend(ctx) or returned a Command with Goto.
		for _, task := range tasks {
			markTaskVersionsSeen(task, channelVersions, versionsSeen)
		}
		var (
			versionBumped map[string]bool
			updated       map[string]bool
		)
		interrupts, versionBumped, updated, err = applyTaskWrites(g, channels, tasks, results, interrupts, allowDynamicChannels)
		if err != nil {
			return pregelLoopResult{}, fmt.Errorf("applying writes at step %d: %w", step, err)
		}
		for key := range versionBumped {
			bumpChannelVersion(key)
		}
		lastUpdatedChannels = updatedChannelsFromChanged(updated)
		if hasNodeInterrupt {
			break
		}

		// --- emit values/updates stream events ---
		for _, r := range results {
			emitUpdate(opts.streamOut, g, channels, r, step, opts.streamModes, baseNamespace)
		}

		// --- advance step counter ---
		completedNodes := make([]string, 0, len(tasks))
		for _, t := range tasks {
			completedNodes = append(completedNodes, t.name)
		}
		prevNodes = completedNodes
		step++

		if step >= config.RecursionLimit {
			return pregelLoopResult{}, &GraphRecursionError{
				Message: fmt.Sprintf("recursion limit of %d exceeded", config.RecursionLimit),
			}
		}

		// --- prepare next tasks ---
		// prepareNextTasks reads the TASKS channel (which now has the Send
		// objects written by this step's tasks) to create PUSH tasks. Calling
		// step lifecycle updates in applyTaskWrites keeps this data valid.
		tasks, err = prepareNextTasks(ctx, g, config, channels, pendingWrites, prevNodes, step, false)
		if err != nil {
			return pregelLoopResult{}, fmt.Errorf("preparing tasks at step %d: %w", step, err)
		}

		finished := make(map[string]bool)
		if len(tasks) == 0 {
			finished = finishChannels(channels)
			for channel := range finished {
				bumpChannelVersion(channel)
			}
			if len(finished) > 0 {
				lastUpdatedChannels = updatedChannelsFromChanged(finished)
				tasks, err = prepareNextTasks(ctx, g, config, channels, pendingWrites, prevNodes, step, true)
				if err != nil {
					return pregelLoopResult{}, fmt.Errorf("preparing deferred tasks at step %d: %w", step, err)
				}
			}
		}
		checkpointChanged := mergeChangedChannels(updated, finished)

		// --- checkpoint ---
		checkpointByDurability(ctx, durability, asyncCheckpoint, checkpointSnapshot{
			config:          &config,
			channels:        channels,
			step:            step,
			channelVersions: channelVersions,
			versionsSeen:    versionsSeen,
			updatedChannels: updatedChannelsFromChanged(checkpointChanged),
			next:            nextNodeNames(tasks),
			pendingWrites:   pendingWrites,
			streamOut:       opts.streamOut,
			streamModes:     opts.streamModes,
			namespace:       baseNamespace,
		})
	}

	if config.Checkpointer != nil && config.ThreadID != "" {
		if durability == DurabilityAsync {
			asyncCheckpoint.Wait()
		}
		saveCheckpoint(ctx, config, channels, step, channelVersions, versionsSeen, lastUpdatedChannels, nil, pendingWrites, opts.streamOut, opts.streamModes, baseNamespace)
	}

	// Finish all channels.
	channels.finishAll()

	// Build full state for streaming.
	state, err := stateFromChannels[State](channels)
	if err != nil {
		return pregelLoopResult{}, fmt.Errorf("building output state: %w", err)
	}

	// Emit final values event using the full state.
	if opts.streamOut != nil && opts.streamModes.enabled(StreamModeValues) {
		opts.streamOut <- StreamPart{
			Type:      StreamModeValues,
			Data:      state,
			Namespace: append([]string(nil), baseNamespace...),
		}
	}

	// Build the caller-facing output from the Output type's channels.
	// When Output == State this is equivalent; when Output is a distinct
	// subset schema only its fields are populated from the channel map.
	output, err := stateFromChannels[Output](channels)
	if err != nil {
		return pregelLoopResult{}, fmt.Errorf("building graph output: %w", err)
	}

	return pregelLoopResult{
		output:     output,
		interrupts: interrupts,
	}, nil
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
	futures := make([]*taskFuture, len(tasks))
	waitCtx := ctx
	var cancel context.CancelFunc
	if opts.stepTimeout > 0 {
		waitCtx, cancel = context.WithTimeout(ctx, opts.stepTimeout)
		defer cancel()
	}

	executor := newBackgroundExecutor(waitCtx, opts.maxConcurrency)
	defer executor.close()

	for i, task := range tasks {
		if task.call != nil {
			futures[i] = executor.submit(func(taskCtx context.Context) pregelTaskResult {
				return executeCallTask(taskCtx, task, opts)
			})
			continue
		}

		node := g.nodes[task.name]
		if node == nil {
			return results, fmt.Errorf("node '%s' not found", task.name)
		}

		futures[i] = executor.submit(func(taskCtx context.Context) pregelTaskResult {
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
		})
	}

	if err := awaitTaskResults(waitCtx, futures, results); err != nil {
		if errors.Is(err, context.DeadlineExceeded) && opts.stepTimeout > 0 {
			return results, &GraphStepTimeoutError{Step: opts.step, Timeout: opts.stepTimeout}
		}
		return results, err
	}

	var first error
	var firstParentCmd *Command
	for i := range futures {
		r := results[i]
		if r.parentCmd != nil && firstParentCmd == nil {
			firstParentCmd = r.parentCmd
		}
		if r.err != nil && len(r.interrupts) == 0 && first == nil && opts.stepTimeout > 0 && errors.Is(r.err, context.DeadlineExceeded) {
			first = &GraphStepTimeoutError{Step: opts.step, Timeout: opts.stepTimeout}
			continue
		}
		if r.err != nil && len(r.interrupts) == 0 && first == nil {
			if tasks[i].call != nil {
				first = fmt.Errorf("call task '%s': %w", tasks[i].name, r.err)
			} else {
				first = fmt.Errorf("node '%s': %w", tasks[i].name, r.err)
			}
		}
	}
	// Parent commands take priority: propagate before regular errors.
	if firstParentCmd != nil {
		return results, &parentCommandError{cmd: firstParentCmd}
	}
	return results, first
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
		return pregelTaskResult{taskID: task.id, node: task.name, path: append([]any(nil), task.path...), checkpointNS: task.checkpointNS, tags: append([]string(nil), task.tags...), err: fmt.Errorf("invalid task input for node '%s': %w", task.name, inputErr)}
	}

	// Build per-task context: task ID + scratchpad (for interrupt) + Send.
	sp := &taskScratchpad{resume: task.resumeValues}
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

	if len(opts.managed) > 0 {
		resolved, err := applyManagedValuesToState(inputState, opts.managed, pgScratch)
		if err != nil {
			return pregelTaskResult{taskID: task.id, node: task.name, path: append([]any(nil), task.path...), checkpointNS: task.checkpointNS, tags: append([]string(nil), task.tags...), err: err}
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

	var (
		result    NodeResult
		execErr   error
		nodeIvs   []Interrupt
		fromCache bool
	)

	runFn := func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				if sig, ok := r.(nodeInterruptSignal); ok {
					nodeIvs = append(nodeIvs, sig.interrupt)
					err = &nodeInterruptError{interrupt: sig.interrupt}
					return
				}
				// Re-panic for unrelated panics.
				panic(r)
			}
		}()
		result, err = node.Fn(taskCtx, inputState)
		return err
	}

	cacheKey := task.cacheKey
	canCache := cacheKey != nil
	if canCache && opts.cache != nil {
		cached, hit, cacheErr := opts.cache.Get(taskCtx, *cacheKey)
		if cacheErr != nil {
			execErr = cacheErr
		} else if hit {
			cachedResult, ok := cached.(NodeResult)
			if !ok {
				execErr = fmt.Errorf("cache entry for node '%s' has invalid type %T", task.name, cached)
			} else {
				result = cachedResult
				fromCache = true
			}
		}
	}

	if execErr == nil && !fromCache {
		if len(node.RetryPolicy) > 0 {
			execErr = executeWithRetryPolicies(taskCtx, node.RetryPolicy, runFn)
		} else {
			execErr = runFn()
		}
	}

	if execErr == nil && !fromCache && len(task.writers) > 0 {
		taskMeta := TaskContext[State]{
			ID:           task.id,
			Name:         task.name,
			Path:         append([]any(nil), task.path...),
			CheckpointNS: task.checkpointNS,
			Triggers:     append([]string(nil), task.triggers...),
			Tags:         append([]string(nil), task.tags...),
			Input:        inputState,
			IsPush:       task.isPush,
			InputSchema:  task.inputSchema,
			Subgraphs:    append([]string(nil), task.subgraphs...),
		}
		if task.cacheKey != nil {
			k := *task.cacheKey
			taskMeta.CacheKey = &k
		}
		for _, writer := range task.writers {
			result, execErr = writer(taskCtx, taskMeta, result)
			if execErr != nil {
				break
			}
		}
	}

	if execErr == nil && canCache && opts.cache != nil && len(nodeIvs) == 0 {
		if err := opts.cache.Set(taskCtx, *cacheKey, result); err != nil {
			execErr = err
		}
	}

	// If the node was interrupted, the record interrupt writes (not a fatal error).
	if len(nodeIvs) > 0 {
		writes := make([]pregelWrite, len(nodeIvs))
		for i, iv := range nodeIvs {
			writes[i] = pregelWrite{channel: pregelInterrupt, value: iv}
		}
		writes = append(writes, pendingWrites...)
		return pregelTaskResult{
			taskID:       task.id,
			node:         task.name,
			path:         append([]any(nil), task.path...),
			checkpointNS: task.checkpointNS,
			tags:         append([]string(nil), task.tags...),
			writes:       writes,
			interrupts:   nodeIvs,
			err:          nil, // not a fatal error
		}
	}

	// Convert node output to channel writes.
	var writes []pregelWrite
	if execErr == nil {
		writes, execErr = outputToWrites(node, result)
	}
	if execErr != nil {
		// Parent command: surface to the loop without a local write or error.
		// The loop propagates it upward so AsSubgraphNode can apply it to the
		// enclosing graph.
		var pce *parentCommandError
		if errors.As(execErr, &pce) {
			return pregelTaskResult{
				taskID:       task.id,
				node:         task.name,
				path:         append([]any(nil), task.path...),
				checkpointNS: task.checkpointNS,
				tags:         append([]string(nil), task.tags...),
				parentCmd:    pce.cmd,
			}
		}
		writes = append(writes, pregelWrite{channel: pregelError, value: execErr.Error()})
	}
	writes = append(writes, pendingWrites...)

	return pregelTaskResult{
		taskID:       task.id,
		node:         task.name,
		path:         append([]any(nil), task.path...),
		checkpointNS: task.checkpointNS,
		tags:         append([]string(nil), task.tags...),
		writes:       writes,
		err:          execErr,
	}
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
	if m, ok := any(input).(map[string]any); ok {
		out := make(map[string]any, len(m)+len(managed))
		for k, v := range m {
			out[k] = v
		}
		for name, spec := range managed {
			out[name] = spec.Get(scratchpad)
		}
		if typed, ok := any(out).(State); ok {
			return typed, nil
		}
		var zero State
		return zero, fmt.Errorf("managed value injection failed converting map to state type")
	}

	rv := reflect.ValueOf(input)
	rt := reflect.TypeOf(input)
	if rt != nil && rt.Kind() == reflect.Struct {
		out := reflect.New(rt).Elem()
		out.Set(rv)
		for name, spec := range managed {
			field := out.FieldByName(name)
			if !field.IsValid() || !field.CanSet() {
				continue
			}
			value := reflect.ValueOf(spec.Get(scratchpad))
			if !value.IsValid() {
				continue
			}
			if value.Type().AssignableTo(field.Type()) {
				field.Set(value)
				continue
			}
			if value.Type().ConvertibleTo(field.Type()) {
				field.Set(value.Convert(field.Type()))
			}
		}
		if typed, ok := out.Interface().(State); ok {
			return typed, nil
		}
	}
	return input, nil
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
		stateWrites, sendTargets, _, _, err := commandResult(output.Command)
		if err != nil {
			return nil, err
		}
		if err := validateCommandDestinations(node, sendTargets); err != nil {
			return nil, err
		}
		writes := stateWrites
		for _, s := range sendTargets {
			writes = append(writes, pregelWrite{channel: pregelTasks, value: s})
		}
		return writes, nil
	}

	if output.State != nil {
		state := output.State.Value()
		if m, ok := state.(map[string]any); ok {
			writes := make([]pregelWrite, 0, len(m))
			for k, v := range m {
				writes = append(writes, pregelWrite{channel: k, value: v})
			}
			return writes, nil
		}
		rt := reflect.TypeOf(state)
		rv := reflect.ValueOf(state)
		if rt.Kind() == reflect.Struct {
			writes := make([]pregelWrite, 0, rt.NumField())
			for i := 0; i < rt.NumField(); i++ {
				f := rt.Field(i)
				if !f.IsExported() {
					continue
				}
				writes = append(writes, pregelWrite{channel: f.Name, value: rv.Field(i).Interface()})
			}
			return writes, nil
		}
		return nil, fmt.Errorf("unsupported node state output type %T", state)
	}

	if len(output.Writes) == 0 {
		return nil, nil
	}
	writes := make([]pregelWrite, 0, len(output.Writes))
	for k, v := range output.Writes {
		writes = append(writes, pregelWrite{channel: k, value: v.Value()})
	}
	return writes, nil
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
	config          *Config
	channels        *pregelChannelMap
	channelVersions map[string]int64
	versionsSeen    map[string]map[string]int64
	streamOut       chan<- StreamPart
	streamModes     streamModeSet
	updatedChannels []string
	next            []string
	pendingWrites   []checkpoint.PendingWrite
	namespace       []string
	step            int
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
		namespaceCopy := append([]string(nil), snap.namespace...)
		writer.Submit(func() {
			saveCheckpointFromSnapshot(ctx, configCopy, channelsCopy, snap.step, channelVersionsCopy, versionsSeenCopy, updatedChannelsCopy, nextCopy, pendingWritesCopy, snap.streamOut, snap.streamModes, namespaceCopy)
		})
	default:
		saveCheckpoint(ctx, *snap.config, snap.channels, snap.step, snap.channelVersions, snap.versionsSeen, snap.updatedChannels, snap.next, snap.pendingWrites, snap.streamOut, snap.streamModes, snap.namespace)
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

func mergeNamespaces(base []string, override []string) []string {
	if len(override) == 0 {
		return append([]string(nil), base...)
	}
	ns := make([]string, 0, len(base)+len(override))
	ns = append(ns, base...)
	ns = append(ns, override...)
	return ns
}

func streamWriterPayload(v any, namespace []string) (StreamMode, any, []string) {
	if emit, ok := v.(StreamEmit); ok {
		mode := emit.Mode
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
	values := map[string]any{}
	if cp != nil && cp.ChannelValues != nil {
		for k, v := range cp.ChannelValues {
			values[k] = v
		}
	}
	cfg := map[string]any{
		"thread_id":     config.ThreadID,
		"checkpoint_ns": config.CheckpointNS,
	}
	if cp != nil && cp.ID != "" {
		cfg["checkpoint_id"] = cp.ID
	}
	metadata := map[string]any{}
	if meta != nil {
		metadata["source"] = meta.Source
		metadata["step"] = meta.Step
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
	}

	nextNodes := append([]string(nil), next...)
	checkpointTasks := make([]map[string]any, 0)
	pendingNext, pendingTaskList, _ := snapshotNextAndTasks(pendingWrites)
	if len(nextNodes) == 0 && len(pendingNext) > 0 {
		nextNodes = append(nextNodes, pendingNext...)
	}
	for _, task := range pendingTaskList {
		checkpointTasks = append(checkpointTasks, map[string]any{
			"id":         task.ID,
			"name":       task.Name,
			"interrupts": append([]Interrupt(nil), task.Interrupts...),
			"state":      nil,
		})
	}
	if len(checkpointTasks) == 0 {
		for _, name := range nextNodes {
			checkpointTasks = append(checkpointTasks, map[string]any{
				"name":  name,
				"state": nil,
			})
		}
	}

	return map[string]any{
		"config":        cfg,
		"metadata":      metadata,
		"values":        values,
		"next":          nextNodes,
		"parent_config": nil,
		"tasks":         checkpointTasks,
	}
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
