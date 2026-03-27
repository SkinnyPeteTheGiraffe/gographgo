package graph

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/zeebo/xxh3"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/checkpoint"
)

// prepareNextTasks determines which tasks (nodes) should run in the next
// superstep, given the current channel state and completed nodes.
//
// Task ordering follows Python LangGraph's PUSH-before-PULL convention:
//
//  1. PUSH tasks — dispatched via Send fan-out. The pregelTasks channel
//     (a Topic) accumulates Send objects written by the previous step's nodes.
//     Each Send becomes a task whose input is Send.Arg, not the shared state.
//     Branch functions returning Send objects also produce PUSH tasks.
//
//  2. PULL tasks — triggered by graph edges. For step == 0 these are entry
//     nodes reachable from Start; for later steps they are nodes reachable
//     from the previous step's completed nodes via direct edges or conditional
//     branches that return string node names.
//
// This mirrors Python LangGraph's pregel/_algo.py:prepare_next_tasks.
func prepareNextTasks[State, Context, Input, Output any](
	ctx context.Context,
	g *StateGraph[State, Context, Input, Output],
	config Config,
	channels *pregelChannelMap,
	pendingWrites []checkpoint.PendingWrite,
	prevNodes []string,
	step int,
	allowDeferred bool,
) ([]pregelTask[State], error) {
	tasks := collectPushTasksFromChannel(g, config, channels, pendingWrites, step)
	pullCandidates, branchSends, branchSources, err := pullCandidatesFromTopology(ctx, g, channels, prevNodes, step)
	if err != nil {
		return nil, err
	}
	tasks = append(tasks, branchSendTasks(g, config, pendingWrites, step, branchSends, len(tasks))...)
	pullTasks, err := buildPullTasks(g, config, channels, pendingWrites, step, allowDeferred, pullCandidates, branchSources)
	if err != nil {
		return nil, err
	}
	tasks = append(tasks, pullTasks...)
	return tasks, nil
}

func collectPushTasksFromChannel[State, Context, Input, Output any](
	g *StateGraph[State, Context, Input, Output],
	config Config,
	channels *pregelChannelMap,
	pendingWrites []checkpoint.PendingWrite,
	step int,
) []pregelTask[State] {
	pushChannel, ok := channels.channels[pregelTasks]
	if !ok || !pushChannel.IsAvailable() {
		return nil
	}
	raw, err := pushChannel.Get()
	if err != nil {
		return nil
	}
	items, ok := raw.Value().([]Dynamic)
	if !ok {
		return nil
	}
	tasks := make([]pregelTask[State], 0, len(items))
	for idx, item := range items {
		if task, taskOK := pushPacketTask(g, config, pendingWrites, step, idx, item); taskOK {
			tasks = append(tasks, task)
		}
	}
	return tasks
}

func pushPacketTask[State, Context, Input, Output any](
	g *StateGraph[State, Context, Input, Output],
	config Config,
	pendingWrites []checkpoint.PendingWrite,
	step int,
	idx int,
	packet Dynamic,
) (pregelTask[State], bool) {
	switch item := packet.Value().(type) {
	case Send:
		if item.Node == "" || item.Node == End {
			return pregelTask[State]{}, false
		}
		task := pregelTask[State]{name: item.Node, path: []any{"push", idx}, input: item.Arg.Value(), isPush: true, triggers: []string{pregelTasks}}
		setTaskNodeMetadata(g, &task)
		finalizeNodeTask(config, pendingWrites, step, g.nodes[task.name], &task)
		return task, true
	case Call:
		if item.Fn == nil {
			return pregelTask[State]{}, false
		}
		name := item.Name
		if name == "" {
			name = "call"
		}
		call := item
		task := pregelTask[State]{name: name, path: []any{"push", idx, "call", name}, input: item.Arg.Value(), isPush: true, triggers: []string{pregelTasks}, call: &call}
		finalizeCallTask(config, pendingWrites, step, &task)
		return task, true
	default:
		return pregelTask[State]{}, false
	}
}

func pullCandidatesFromTopology[State, Context, Input, Output any](
	ctx context.Context,
	g *StateGraph[State, Context, Input, Output],
	channels *pregelChannelMap,
	prevNodes []string,
	step int,
) (pullCandidates []string, branchSends []Send, branchSources map[string][]string, err error) {
	if step == 0 {
		return entryNodes(g), nil, map[string][]string{}, nil
	}
	seen := make(map[string]bool)
	candidates := make([]string, 0)
	branchSends = make([]Send, 0)
	branchSources = make(map[string][]string)
	for _, nodeName := range prevNodes {
		candidates = appendDirectEdgeCandidates(g, nodeName, seen, candidates)
		nextCandidates, sends, branchErr := appendConditionalBranchCandidates(ctx, g, channels, nodeName, seen, branchSources)
		if branchErr != nil {
			return nil, nil, nil, branchErr
		}
		candidates = append(candidates, nextCandidates...)
		branchSends = append(branchSends, sends...)
	}
	candidates = appendWaitingEdgeCandidates(g, channels, prevNodes, seen, candidates)
	return candidates, branchSends, branchSources, nil
}

func appendDirectEdgeCandidates[State, Context, Input, Output any](g *StateGraph[State, Context, Input, Output], nodeName string, seen map[string]bool, out []string) []string {
	for _, edge := range g.edges {
		if edge.Source != nodeName || edge.Target == End || seen[edge.Target] {
			continue
		}
		out = append(out, edge.Target)
		seen[edge.Target] = true
	}
	return out
}

func appendConditionalBranchCandidates[State, Context, Input, Output any](
	ctx context.Context,
	g *StateGraph[State, Context, Input, Output],
	channels *pregelChannelMap,
	nodeName string,
	seen map[string]bool,
	branchSources map[string][]string,
) ([]string, []Send, error) {
	branches, ok := g.branches[nodeName]
	if !ok {
		return nil, nil, nil
	}
	candidates := make([]string, 0)
	branchSends := make([]Send, 0)
	for _, branch := range branches {
		state, err := stateFromChannels[State](channels)
		if err != nil {
			return nil, nil, fmt.Errorf("reading state for branch '%s': %w", branch.Name, err)
		}
		result, err := branch.Path(ctx, state)
		if err != nil {
			return nil, nil, fmt.Errorf("branch '%s': %w", branch.Name, err)
		}
		strs, sends, err := routingTargetsBoth(result, branch.PathMap)
		if err != nil {
			return nil, nil, fmt.Errorf("branch '%s' returned invalid route type: %w", branch.Name, err)
		}
		for _, target := range strs {
			if !seen[target] {
				candidates = append(candidates, target)
				seen[target] = true
			}
			if !containsString(branchSources[target], nodeName) {
				branchSources[target] = append(branchSources[target], nodeName)
			}
		}
		branchSends = append(branchSends, sends...)
	}
	return candidates, branchSends, nil
}

func appendWaitingEdgeCandidates[State, Context, Input, Output any](
	g *StateGraph[State, Context, Input, Output],
	channels *pregelChannelMap,
	prevNodes []string,
	seen map[string]bool,
	out []string,
) []string {
	for _, waiting := range g.waitingEdges {
		if waiting.Target == End || seen[waiting.Target] {
			continue
		}
		joinChannel := waitingEdgeChannelName(waiting)
		join, ok := channels.channels[joinChannel]
		if ok {
			if !join.IsAvailable() {
				continue
			}
		} else if !allSourcesCompleted(g, waiting.Sources, prevNodes) {
			continue
		}
		out = append(out, waiting.Target)
		seen[waiting.Target] = true
	}
	return out
}

func branchSendTasks[State, Context, Input, Output any](
	g *StateGraph[State, Context, Input, Output],
	config Config,
	pendingWrites []checkpoint.PendingWrite,
	step int,
	sends []Send,
	baseOffset int,
) []pregelTask[State] {
	tasks := make([]pregelTask[State], 0, len(sends))
	for i, s := range sends {
		if s.Node == "" || s.Node == End {
			continue
		}
		task := pregelTask[State]{name: s.Node, path: []any{"push", "branch", s.Node, baseOffset + i}, input: s.Arg.Value(), isPush: true, triggers: []string{"branch"}}
		setTaskNodeMetadata(g, &task)
		finalizeNodeTask(config, pendingWrites, step, g.nodes[task.name], &task)
		tasks = append(tasks, task)
	}
	return tasks
}

func buildPullTasks[State, Context, Input, Output any](
	g *StateGraph[State, Context, Input, Output],
	config Config,
	channels *pregelChannelMap,
	pendingWrites []checkpoint.PendingWrite,
	step int,
	allowDeferred bool,
	candidates []string,
	branchSources map[string][]string,
) ([]pregelTask[State], error) {
	tasks := make([]pregelTask[State], 0, len(candidates))
	for _, name := range candidates {
		node := g.nodes[name]
		if node == nil {
			return nil, fmt.Errorf("node '%s' referenced but not found in graph", name)
		}
		if node.Defer && !allowDeferred {
			continue
		}
		state, err := stateFromChannels[State](channels)
		if err != nil {
			return nil, fmt.Errorf("reading state for node '%s': %w", name, err)
		}
		task := pregelTask[State]{name: name, path: []any{"pull", name}, triggers: triggerChannels(g, name, branchSources[name]), input: state}
		setTaskNodeMetadata(g, &task)
		finalizeNodeTask(config, pendingWrites, step, node, &task)
		tasks = append(tasks, task)
	}
	return tasks, nil
}

func finalizeNodeTask[State any](config Config, pendingWrites []checkpoint.PendingWrite, step int, node *NodeSpec[State], task *pregelTask[State]) {
	task.id = pregelTaskID(config, step, task.path, task.name, task.triggers)
	task.checkpointNS = taskCheckpointNamespace(config.CheckpointNS, task.name, task.id)
	task.resumeValues = taskResumeValues(pendingWrites, task.id)
	if key, ok := buildNodeCacheKey(node, *task, config); ok {
		task.cacheKey = &key
	}
}

func finalizeCallTask[State any](config Config, pendingWrites []checkpoint.PendingWrite, step int, task *pregelTask[State]) {
	task.id = pregelTaskID(config, step, task.path, task.name, task.triggers)
	task.checkpointNS = taskCheckpointNamespace(config.CheckpointNS, task.name, task.id)
	task.resumeValues = taskResumeValues(pendingWrites, task.id)
	if key, ok := buildCallCacheKey(*task, config); ok {
		task.cacheKey = &key
	}
}

func setTaskNodeMetadata[State, Context, Input, Output any](g *StateGraph[State, Context, Input, Output], task *pregelTask[State]) {
	node := g.nodes[task.name]
	if node == nil {
		return
	}
	task.inputSchema = node.InputSchema
	task.writers = append([]ChannelWrite[State](nil), node.Writers...)
	task.subgraphs = append([]string(nil), node.Subgraphs...)
	task.tags = append([]string(nil), nodeTags(node)...)
}

func pregelTaskID(config Config, step int, path []any, name string, triggers []string) string {
	sorted := append([]string(nil), triggers...)
	sort.Strings(sorted)
	payload := fmt.Sprintf("%s|%d|%s|%v|%v", config.CheckpointNS, step, name, path, sorted)
	hi := xxh3.HashString(payload)
	lo := xxh3.HashString("task:" + payload)
	hex := fmt.Sprintf("%016x%016x", hi, lo)
	return fmt.Sprintf("%s-%s-%s-%s-%s", hex[:8], hex[8:12], hex[12:16], hex[16:20], hex[20:32])
}

// allSourcesCompleted checks if all sources in a waiting edge have completed
// in the current superstep (i.e., are in prevNodes).
func allSourcesCompleted[State, Context, Input, Output any](_ *StateGraph[State, Context, Input, Output], sources, prevNodes []string) bool {
	prevSet := make(map[string]bool, len(prevNodes))
	for _, n := range prevNodes {
		prevSet[n] = true
	}
	for _, src := range sources {
		if !prevSet[src] {
			return false
		}
	}
	return true
}

// entryNodes returns the node names reachable directly from Start.
func entryNodes[State, Context, Input, Output any](g *StateGraph[State, Context, Input, Output]) []string {
	var nodes []string
	seen := make(map[string]bool)
	for _, edge := range g.edges {
		if edge.Source == Start && edge.Target != End && !seen[edge.Target] {
			nodes = append(nodes, edge.Target)
			seen[edge.Target] = true
		}
	}
	return nodes
}

func triggerChannels[State, Context, Input, Output any](g *StateGraph[State, Context, Input, Output], nodeName string, branchSources []string) []string {
	var triggers []string
	seen := make(map[string]bool)

	for _, edge := range g.edges {
		if edge.Target != nodeName {
			continue
		}
		if seen[edge.Source] {
			continue
		}
		triggers = append(triggers, edge.Source)
		seen[edge.Source] = true
	}

	for _, we := range g.waitingEdges {
		if we.Target != nodeName {
			continue
		}
		joinChannel := waitingEdgeChannelName(we)
		if seen[joinChannel] {
			continue
		}
		triggers = append(triggers, joinChannel)
		seen[joinChannel] = true
	}

	for _, src := range branchSources {
		if seen[src] {
			continue
		}
		triggers = append(triggers, src)
		seen[src] = true
	}

	sort.Strings(triggers)
	return triggers
}

func waitingEdgeChannelName(we WaitingEdge) string {
	return fmt.Sprintf("join:%s:%s", strings.Join(we.Sources, "+"), we.Target)
}

func taskCheckpointNamespace(parentNS, taskName, taskID string) string {
	nodeNS := taskName
	if strings.TrimSpace(parentNS) != "" {
		nodeNS = parentNS + "|" + taskName
	}
	if strings.TrimSpace(taskID) == "" {
		return nodeNS
	}
	return nodeNS + ":" + taskID
}

func containsString(items []string, target string) bool {
	for _, item := range items {
		if item == target {
			return true
		}
	}
	return false
}

// routingTargetsBoth converts the return value of a branch Path function into
// a pair of (string node names, Send objects), respecting PathMap if provided.
//
// String names become PULL task candidates; Send objects become PUSH tasks
// with their Arg passed as the node input. This separation mirrors Python
// LangGraph's PUSH vs PULL task distinction in prepare_next_tasks.
func routingTargetsBoth(route Route, pathMap map[string]string) ([]string, []Send, error) {
	strs := routingTargets(route, pathMap)

	sends := make([]Send, 0, len(route.Sends))
	for _, s := range route.Sends {
		if s.Node == "" || s.Node == End {
			continue
		}
		sends = append(sends, s)
	}

	return strs, sends, nil
}

// routingTargets extracts only the string node names from a branch result.
// Kept for callers that do not need Send objects.
func routingTargets(route Route, pathMap map[string]string) []string {
	strs := make([]string, 0, len(route.Nodes))
	for _, node := range route.Nodes {
		if node == "" || node == End {
			continue
		}
		target := node
		if mapped, ok := pathMap[node]; ok {
			target = mapped
		}
		if target == "" || target == End {
			continue
		}
		strs = append(strs, target)
	}
	return strs
}

// applyTaskWrites applies superstep writes with channel lifecycle behavior:
//
//  1. Consume channels that were read by this step's tasks.
//  2. Apply explicit writes for known channels.
//  3. Apply empty updates to available channels not written in this step.
//
// Unknown channel writes are ignored.
//
// Returns:
//   - Interrupts with task-generated interrupt records appended,
//   - versionBumped channels whose lifecycle changed this step,
//   - updatedChannels that are still available after updates.
func applyTaskWrites[State, Context, Input, Output any](
	g *StateGraph[State, Context, Input, Output],
	channels *pregelChannelMap,
	tasks []pregelTask[State],
	results []pregelTaskResult,
	interrupts []Interrupt,
	allowDynamicChannels bool,
) (nextInterrupts []Interrupt, versionBumped, updatedChannels map[string]bool, err error) {
	versionBumped = make(map[string]bool)
	updatedChannels = make(map[string]bool)

	if len(results) == 0 {
		return interrupts, versionBumped, updatedChannels, nil
	}
	taskByID := taskIndexByID(tasks)
	orderedResults := orderedTaskResults(results, taskByID)
	waitingBySource := waitingEdgesBySource(g)
	collected := collectStepWrites(channels, orderedResults, taskByID, waitingBySource, interrupts, allowDynamicChannels)
	interrupts = collected.interrupts
	for channel := range applyConsumedChannels(channels, collected.consumed) {
		versionBumped[channel] = true
	}
	changed, applyErr := channels.applyBatch(collected.explicitWrites)
	if applyErr != nil {
		return interrupts, versionBumped, updatedChannels, applyErr
	}
	updateChangedChannels(channels, changed, versionBumped, updatedChannels)
	if updateErr := applyEmptyStepUpdates(channels, collected.bumpStep, collected.written, versionBumped, updatedChannels); updateErr != nil {
		return interrupts, versionBumped, updatedChannels, updateErr
	}

	return interrupts, versionBumped, updatedChannels, nil
}

type stepWriteCollection struct {
	consumed       map[string]struct{}
	written        map[string]bool
	explicitWrites []pregelWrite
	interrupts     []Interrupt
	bumpStep       bool
}

func taskIndexByID[State any](tasks []pregelTask[State]) map[string]pregelTask[State] {
	out := make(map[string]pregelTask[State], len(tasks))
	for _, task := range tasks {
		out[task.id] = task
	}
	return out
}

func orderedTaskResults[State any](results []pregelTaskResult, taskByID map[string]pregelTask[State]) []pregelTaskResult {
	ordered := append([]pregelTaskResult(nil), results...)
	sort.SliceStable(ordered, func(i, j int) bool {
		leftPath := ordered[i].path
		if task, ok := taskByID[ordered[i].taskID]; ok {
			leftPath = task.path
		}
		rightPath := ordered[j].path
		if task, ok := taskByID[ordered[j].taskID]; ok {
			rightPath = task.path
		}
		leftKey := taskPathSortKey(leftPath)
		rightKey := taskPathSortKey(rightPath)
		if leftKey == rightKey {
			return ordered[i].taskID < ordered[j].taskID
		}
		return leftKey < rightKey
	})
	return ordered
}

func waitingEdgesBySource[State, Context, Input, Output any](g *StateGraph[State, Context, Input, Output]) map[string][]WaitingEdge {
	out := make(map[string][]WaitingEdge)
	if g == nil {
		return out
	}
	for _, waiting := range g.waitingEdges {
		if waiting.Target == End {
			continue
		}
		for _, src := range waiting.Sources {
			out[src] = append(out[src], waiting)
		}
	}
	return out
}

func collectStepWrites[State any](
	channels *pregelChannelMap,
	results []pregelTaskResult,
	taskByID map[string]pregelTask[State],
	waitingBySource map[string][]WaitingEdge,
	interrupts []Interrupt,
	allowDynamicChannels bool,
) stepWriteCollection {
	collected := stepWriteCollection{
		consumed:       make(map[string]struct{}),
		explicitWrites: make([]pregelWrite, 0),
		written:        make(map[string]bool),
		interrupts:     interrupts,
	}
	for _, result := range results {
		task, ok := taskByID[result.taskID]
		if ok && len(task.triggers) > 0 {
			collected.bumpStep = true
		}
		collectConsumedChannels(channels, task.triggers, collected.consumed)
		collected.explicitWrites, collected.interrupts = collectExplicitWrites(channels, result, allowDynamicChannels, collected.explicitWrites, collected.written, collected.interrupts)
		collected.explicitWrites = addWaitingEdgeWrites(channels, waitingBySource, result, ok, collected.explicitWrites, collected.written)
	}
	return collected
}

func collectConsumedChannels(channels *pregelChannelMap, triggers []string, consumed map[string]struct{}) {
	for _, trigger := range triggers {
		if isInternalPregelChannel(trigger) {
			continue
		}
		if _, exists := channels.channels[trigger]; !exists {
			continue
		}
		if _, seen := consumed[trigger]; seen {
			continue
		}
		consumed[trigger] = struct{}{}
	}
}

func collectExplicitWrites(
	channels *pregelChannelMap,
	result pregelTaskResult,
	allowDynamicChannels bool,
	existing []pregelWrite,
	written map[string]bool,
	interrupts []Interrupt,
) ([]pregelWrite, []Interrupt) {
	for _, write := range result.writes {
		switch write.channel {
		case pregelInterrupt:
			if iv, ok := write.value.(Interrupt); ok && !containsString(result.tags, TagHidden) {
				interrupts = append(interrupts, iv)
			}
			continue
		case pregelResume, pregelError:
			continue
		}
		if _, exists := channels.channels[write.channel]; !exists {
			if !allowDynamicChannels {
				continue
			}
			channels.channels[write.channel] = NewLastValue()
		}
		existing = append(existing, write)
		written[write.channel] = true
	}
	return existing, interrupts
}

func addWaitingEdgeWrites(
	channels *pregelChannelMap,
	waitingBySource map[string][]WaitingEdge,
	result pregelTaskResult,
	taskExists bool,
	existing []pregelWrite,
	written map[string]bool,
) []pregelWrite {
	if !taskExists || result.err != nil || len(result.interrupts) > 0 {
		return existing
	}
	waits := waitingBySource[result.node]
	if len(waits) == 0 {
		return existing
	}
	seenJoinChannels := make(map[string]struct{}, len(waits))
	for _, waiting := range waits {
		joinChannel := waitingEdgeChannelName(waiting)
		if _, dup := seenJoinChannels[joinChannel]; dup {
			continue
		}
		seenJoinChannels[joinChannel] = struct{}{}
		if _, exists := channels.channels[joinChannel]; !exists {
			continue
		}
		existing = append(existing, pregelWrite{channel: joinChannel, value: result.node})
		written[joinChannel] = true
	}
	return existing
}

func applyConsumedChannels(channels *pregelChannelMap, consumed map[string]struct{}) map[string]bool {
	consumeOrder := make([]string, 0, len(consumed))
	for channel := range consumed {
		consumeOrder = append(consumeOrder, channel)
	}
	sort.Strings(consumeOrder)
	return channels.consumeChannels(consumeOrder)
}

func updateChangedChannels(channels *pregelChannelMap, changed, versionBumped, updatedChannels map[string]bool) {
	for channel := range changed {
		versionBumped[channel] = true
		if channels.channels[channel].IsAvailable() {
			updatedChannels[channel] = true
		}
	}
}

func applyEmptyStepUpdates(channels *pregelChannelMap, bumpStep bool, written, versionBumped, updatedChannels map[string]bool) error {
	if !bumpStep {
		return nil
	}
	allChannels := make([]string, 0, len(channels.channels))
	for channel := range channels.channels {
		allChannels = append(allChannels, channel)
	}
	sort.Strings(allChannels)
	for _, channel := range allChannels {
		if written[channel] {
			continue
		}
		ch := channels.channels[channel]
		if !ch.IsAvailable() {
			continue
		}
		changed, err := ch.Update(nil)
		if err != nil {
			return err
		}
		if changed {
			versionBumped[channel] = true
			if ch.IsAvailable() {
				updatedChannels[channel] = true
			}
		}
	}
	return nil
}

func taskPathSortKey(path []any) string {
	if len(path) == 0 {
		return ""
	}
	limit := len(path)
	if limit > 3 {
		limit = 3
	}
	parts := make([]string, 0, limit)
	for i := 0; i < limit; i++ {
		parts = append(parts, taskPathPartSortKey(path[i]))
	}
	return strings.Join(parts, "|")
}

func taskPathPartSortKey(part any) string {
	switch v := part.(type) {
	case int:
		return fmt.Sprintf("%010d", v)
	case int8:
		return fmt.Sprintf("%010d", v)
	case int16:
		return fmt.Sprintf("%010d", v)
	case int32:
		return fmt.Sprintf("%010d", v)
	case int64:
		return fmt.Sprintf("%010d", v)
	case uint:
		return fmt.Sprintf("%010d", v)
	case uint8:
		return fmt.Sprintf("%010d", v)
	case uint16:
		return fmt.Sprintf("%010d", v)
	case uint32:
		return fmt.Sprintf("%010d", v)
	case uint64:
		return fmt.Sprintf("%010d", v)
	case []any:
		nested := make([]string, 0, len(v))
		for _, item := range v {
			nested = append(nested, taskPathPartSortKey(item))
		}
		return "~" + strings.Join(nested, ",")
	default:
		return fmt.Sprintf("%v", v)
	}
}

func isInternalPregelChannel(channel string) bool {
	switch channel {
	case pregelInput, pregelInterrupt, pregelResume, pregelError, pregelPush, pregelTasks:
		return true
	default:
		return false
	}
}

func allowsDynamicStateChannels[State any]() bool {
	var zero State
	_, ok := any(zero).(map[string]any)
	return ok
}

// shouldInterruptBefore returns true if any pending task matches an
// interrupt-before entry. Mirrors Python LangGraph's should_interrupt.
func shouldInterruptBefore[State any](tasks []pregelTask[State], interruptBefore []string) bool {
	for _, task := range tasks {
		for _, name := range interruptBefore {
			if task.name == name || name == All {
				return true
			}
		}
	}
	return false
}

// shouldInterruptAfter returns true if any completed task matches an
// interrupt-after entry.
func shouldInterruptAfter(results []pregelTaskResult, interruptAfter []string) bool {
	for _, r := range results {
		for _, name := range interruptAfter {
			if r.node == name || name == All {
				return true
			}
		}
	}
	return false
}

// stateFromChannels reconstructs a State value from the current channel map.
//
// Resolution order:
//
//  1. map[string]any — channel values are assembled into a map, internal
//     Pregel channels are stripped, and the result is returned directly.
//
//  2. Struct — each exported field is read from the channel whose name
//     matches the field name. Struct field channels may use any channel type
//     (LastValue, BinaryOperatorAggregate, etc.).  Unknown or empty channels
//     leave the corresponding field at its zero value.
//
//  3. Fall-through — reads from the __input__ channel for scalar/opaque types
//     passed through without decomposition.
//
// This mirrors Python LangGraph's read_channels / map_output_values for typed
// TypedDict/dataclass state.
func stateFromChannels[State any](channels *pregelChannelMap) (State, error) {
	var zero State

	if state, ok := stateFromMapChannels[State](channels, zero); ok {
		return state, nil
	}
	if state, ok := stateFromStructChannels[State](channels, zero); ok {
		return state, nil
	}
	if state, ok := stateFromInputChannel[State](channels); ok {
		return state, nil
	}
	return zero, nil
}

func stateFromMapChannels[State any](channels *pregelChannelMap, zero State) (State, bool) {
	if _, ok := any(zero).(map[string]any); !ok {
		var none State
		return none, false
	}
	stateMap := channels.toStateMap()
	for channel := range stateMap {
		if isInternalPregelChannel(channel) || strings.HasPrefix(channel, "join:") {
			delete(stateMap, channel)
		}
	}
	state, ok := any(stateMap).(State)
	return state, ok
}

func stateFromStructChannels[State any](channels *pregelChannelMap, zero State) (State, bool) {
	rt := reflect.TypeOf(zero)
	if rt == nil || rt.Kind() != reflect.Struct {
		var none State
		return none, false
	}
	result := reflect.New(rt).Elem()
	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)
		if !field.IsExported() {
			continue
		}
		channel, ok := channels.channels[field.Name]
		if !ok {
			continue
		}
		value, err := channel.Get()
		if err != nil {
			continue
		}
		setStructFieldFromChannelValue(result.Field(i), value.Value())
	}
	state, ok := result.Interface().(State)
	return state, ok
}

func setStructFieldFromChannelValue(field reflect.Value, raw any) {
	rv := reflect.ValueOf(raw)
	if !rv.IsValid() {
		return
	}
	if rv.Type().AssignableTo(field.Type()) {
		field.Set(rv)
		return
	}
	if rv.Type().ConvertibleTo(field.Type()) {
		field.Set(rv.Convert(field.Type()))
	}
}

func stateFromInputChannel[State any](channels *pregelChannelMap) (State, bool) {
	channel, ok := channels.channels[pregelInput]
	if !ok {
		var none State
		return none, false
	}
	value, err := channel.Get()
	if err != nil {
		var none State
		return none, false
	}
	state, ok := value.Value().(State)
	return state, ok
}
