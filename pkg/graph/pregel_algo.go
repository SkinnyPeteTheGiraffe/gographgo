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
	var tasks []pregelTask[State]

	// --- 1. PUSH tasks from the TASKS channel ---
	// The pregelTasks Topic channel accumulates Send objects written by the
	// previous superstep. Each Send{Node, Arg} becomes an independent push
	// task whose input is Arg — not the shared graph state.
	if pushCh, ok := channels.channels[pregelTasks]; ok && pushCh.IsAvailable() {
		raw, err := pushCh.Get()
		if err == nil {
			if sends, ok := raw.Value().([]Dynamic); ok {
				for idx, item := range sends {
					switch packet := item.Value().(type) {
					case Send:
						if packet.Node == "" || packet.Node == End {
							continue
						}
						path := []any{"push", idx}
						task := pregelTask[State]{
							name:     packet.Node,
							path:     path,
							input:    packet.Arg.Value(),
							isPush:   true,
							triggers: []string{pregelTasks},
						}
						setTaskNodeMetadata(g, &task)
						task.id = pregelTaskID(config, step, task.path, task.name, task.triggers)
						task.checkpointNS = taskCheckpointNamespace(config.CheckpointNS, task.name, task.id)
						task.resumeValues = taskResumeValues(pendingWrites, task.id)
						if key, ok := buildNodeCacheKey(g.nodes[task.name], task, config); ok {
							task.cacheKey = &key
						}
						tasks = append(tasks, task)
					case Call:
						if packet.Fn == nil {
							continue
						}
						name := packet.Name
						if name == "" {
							name = "call"
						}
						path := []any{"push", idx, "call", name}
						call := packet
						task := pregelTask[State]{
							name:     name,
							path:     path,
							input:    packet.Arg.Value(),
							isPush:   true,
							triggers: []string{pregelTasks},
							call:     &call,
						}
						task.id = pregelTaskID(config, step, task.path, task.name, task.triggers)
						task.checkpointNS = taskCheckpointNamespace(config.CheckpointNS, task.name, task.id)
						task.resumeValues = taskResumeValues(pendingWrites, task.id)
						if key, ok := buildCallCacheKey(task, config); ok {
							task.cacheKey = &key
						}
						tasks = append(tasks, task)
					}
				}
			}
		}
	}

	// --- 2. PULL candidates from graph topology ---
	var pullCandidates []string
	var branchSends []Send // Sends returned directly by branch functions.
	branchSources := make(map[string][]string)

	if step == 0 {
		pullCandidates = entryNodes(g)
	} else {
		seen := make(map[string]bool)
		for _, nodeName := range prevNodes {
			// Direct edges.
			for _, edge := range g.edges {
				if edge.Source == nodeName && edge.Target != End && !seen[edge.Target] {
					pullCandidates = append(pullCandidates, edge.Target)
					seen[edge.Target] = true
				}
			}
			// Conditional branches.
			if branches, ok := g.branches[nodeName]; ok {
				for _, branch := range branches {
					state, err := stateFromChannels[State](channels)
					if err != nil {
						return nil, fmt.Errorf("reading state for branch '%s': %w", branch.Name, err)
					}
					result, err := branch.Path(ctx, state)
					if err != nil {
						return nil, fmt.Errorf("branch '%s': %w", branch.Name, err)
					}
					strs, sends, err := routingTargetsBoth(result, branch.PathMap)
					if err != nil {
						return nil, fmt.Errorf("branch '%s' returned invalid route type: %w", branch.Name, err)
					}
					for _, t := range strs {
						if !seen[t] {
							pullCandidates = append(pullCandidates, t)
							seen[t] = true
						}
						if !containsString(branchSources[t], nodeName) {
							branchSources[t] = append(branchSources[t], nodeName)
						}
					}
					branchSends = append(branchSends, sends...)
				}
			}
		}

		for _, we := range g.waitingEdges {
			if we.Target == End {
				continue
			}
			joinChannel := waitingEdgeChannelName(we)
			join, ok := channels.channels[joinChannel]
			if seen[we.Target] {
				continue
			}
			if ok {
				if !join.IsAvailable() {
					continue
				}
			} else if !allSourcesCompleted(g, we.Sources, prevNodes) {
				// Fallback for cases where waiting-edge channels are unavailable
				// (for example, partial/manual channel maps in tests or tooling).
				continue
			}
			pullCandidates = append(pullCandidates, we.Target)
			seen[we.Target] = true
		}
	}

	// PUSH tasks from branch-returned Send objects.
	for _, s := range branchSends {
		if s.Node != "" && s.Node != End {
			path := []any{"push", "branch", s.Node, len(tasks)}
			task := pregelTask[State]{
				name:     s.Node,
				path:     path,
				input:    s.Arg.Value(),
				isPush:   true,
				triggers: []string{"branch"},
			}
			setTaskNodeMetadata(g, &task)
			task.id = pregelTaskID(config, step, task.path, task.name, task.triggers)
			task.checkpointNS = taskCheckpointNamespace(config.CheckpointNS, task.name, task.id)
			task.resumeValues = taskResumeValues(pendingWrites, task.id)
			if key, ok := buildNodeCacheKey(g.nodes[task.name], task, config); ok {
				task.cacheKey = &key
			}
			tasks = append(tasks, task)
		}
	}

	// PULL tasks: one per candidate node, input is current graph state.
	for _, name := range pullCandidates {
		if g.nodes[name] == nil {
			return nil, fmt.Errorf("node '%s' referenced but not found in graph", name)
		}

		// Check if node has defer flag - if so, skip scheduling it this step
		if node := g.nodes[name]; node != nil && node.Defer && !allowDeferred {
			continue
		}

		state, err := stateFromChannels[State](channels)
		if err != nil {
			return nil, fmt.Errorf("reading state for node '%s': %w", name, err)
		}
		triggers := triggerChannels(g, name, branchSources[name])
		task := pregelTask[State]{
			name:     name,
			path:     []any{"pull", name},
			triggers: triggers,
			input:    state,
		}
		setTaskNodeMetadata(g, &task)
		task.id = pregelTaskID(config, step, task.path, task.name, task.triggers)
		task.checkpointNS = taskCheckpointNamespace(config.CheckpointNS, task.name, task.id)
		task.resumeValues = taskResumeValues(pendingWrites, task.id)
		if key, ok := buildNodeCacheKey(g.nodes[task.name], task, config); ok {
			task.cacheKey = &key
		}
		tasks = append(tasks, task)
	}

	return tasks, nil
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
func allSourcesCompleted[State, Context, Input, Output any](_ *StateGraph[State, Context, Input, Output], sources []string, prevNodes []string) bool {
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
) ([]Interrupt, map[string]bool, map[string]bool, error) {
	versionBumped := make(map[string]bool)
	updatedChannels := make(map[string]bool)

	if len(results) == 0 {
		return interrupts, versionBumped, updatedChannels, nil
	}

	taskByID := make(map[string]pregelTask[State], len(tasks))
	for _, task := range tasks {
		taskByID[task.id] = task
	}
	orderedResults := append([]pregelTaskResult(nil), results...)
	sort.SliceStable(orderedResults, func(i, j int) bool {
		leftPath := orderedResults[i].path
		if task, ok := taskByID[orderedResults[i].taskID]; ok {
			leftPath = task.path
		}
		rightPath := orderedResults[j].path
		if task, ok := taskByID[orderedResults[j].taskID]; ok {
			rightPath = task.path
		}
		leftKey := taskPathSortKey(leftPath)
		rightKey := taskPathSortKey(rightPath)
		if leftKey == rightKey {
			return orderedResults[i].taskID < orderedResults[j].taskID
		}
		return leftKey < rightKey
	})

	waitingBySource := make(map[string][]WaitingEdge)
	if g != nil {
		for _, we := range g.waitingEdges {
			if we.Target == End {
				continue
			}
			for _, src := range we.Sources {
				waitingBySource[src] = append(waitingBySource[src], we)
			}
		}
	}

	bumpStep := false
	consumed := make(map[string]struct{})
	explicitWrites := make([]pregelWrite, 0)
	written := make(map[string]bool)

	for _, result := range orderedResults {
		task, ok := taskByID[result.taskID]
		if ok && len(task.triggers) > 0 {
			bumpStep = true
		}

		for _, trigger := range task.triggers {
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
			explicitWrites = append(explicitWrites, write)
			written[write.channel] = true
		}

		if !ok || result.err != nil || len(result.interrupts) > 0 {
			continue
		}
		if waits := waitingBySource[result.node]; len(waits) > 0 {
			seenJoinChannels := make(map[string]struct{}, len(waits))
			for _, we := range waits {
				joinChannel := waitingEdgeChannelName(we)
				if _, dup := seenJoinChannels[joinChannel]; dup {
					continue
				}
				seenJoinChannels[joinChannel] = struct{}{}
				if _, exists := channels.channels[joinChannel]; !exists {
					continue
				}
				explicitWrites = append(explicitWrites, pregelWrite{channel: joinChannel, value: result.node})
				written[joinChannel] = true
			}
		}
	}

	consumeOrder := make([]string, 0, len(consumed))
	for channel := range consumed {
		consumeOrder = append(consumeOrder, channel)
	}
	sort.Strings(consumeOrder)
	for channel := range channels.consumeChannels(consumeOrder) {
		versionBumped[channel] = true
	}

	changed, err := channels.applyBatch(explicitWrites)
	if err != nil {
		return interrupts, versionBumped, updatedChannels, err
	}
	for channel := range changed {
		versionBumped[channel] = true
		if channels.channels[channel].IsAvailable() {
			updatedChannels[channel] = true
		}
	}

	if bumpStep {
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
				return interrupts, versionBumped, updatedChannels, err
			}
			if changed {
				versionBumped[channel] = true
				if ch.IsAvailable() {
					updatedChannels[channel] = true
				}
			}
		}
	}

	return interrupts, versionBumped, updatedChannels, nil
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

	// map[string]any: build map from available channel values.
	if _, ok := any(zero).(map[string]any); ok {
		stateMap := channels.toStateMap()
		for channel := range stateMap {
			if isInternalPregelChannel(channel) || strings.HasPrefix(channel, "join:") {
				delete(stateMap, channel)
			}
		}
		if s, ok := any(stateMap).(State); ok {
			return s, nil
		}
	}

	// Struct reflection: populate each exported field from its named channel.
	rt := reflect.TypeOf(zero)
	if rt != nil && rt.Kind() == reflect.Struct {
		result := reflect.New(rt).Elem()
		for i := 0; i < rt.NumField(); i++ {
			f := rt.Field(i)
			if !f.IsExported() {
				continue
			}
			ch, ok := channels.channels[f.Name]
			if !ok {
				continue
			}
			val, err := ch.Get()
			if err != nil {
				// EmptyChannelError — leave field at zero value.
				continue
			}
			fv := result.Field(i)
			rv := reflect.ValueOf(val.Value())
			if !rv.IsValid() {
				continue
			}
			if rv.Type().AssignableTo(fv.Type()) {
				fv.Set(rv)
			} else if rv.Type().ConvertibleTo(fv.Type()) {
				fv.Set(rv.Convert(fv.Type()))
			}
		}
		if s, ok := result.Interface().(State); ok {
			return s, nil
		}
	}

	// Fall-through: try to read from __input__ a channel for opaque/scalar State.
	if ch, ok := channels.channels[pregelInput]; ok {
		val, err := ch.Get()
		if err == nil {
			if s, ok := val.Value().(State); ok {
				return s, nil
			}
		}
	}

	return zero, nil
}
