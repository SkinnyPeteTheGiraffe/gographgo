package graph

import (
	"fmt"
	"reflect"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/checkpoint"
)

type interruptWrite struct {
	Interrupt Interrupt
	Node      string
	Path      []any
}

type pendingInterrupt struct {
	TaskID      string
	InterruptID string
	Node        string
	Path        []any
}

func resumeConfig(metadata map[string]any) (resumeMap map[string]Dynamic, resumeValues []Dynamic, hasResume bool, err error) {
	if len(metadata) == 0 {
		return nil, nil, false, nil
	}

	if raw, ok := metadata[ConfigKeyResumeMap]; ok {
		rm, err := normalizeResumeMap(raw)
		if err != nil {
			return nil, nil, false, err
		}
		resumeMap = rm
	}

	if raw, ok := metadata[ConfigKeyResume]; ok {
		values, err := normalizeResumeValues(raw)
		if err != nil {
			return nil, nil, false, err
		}
		resumeValues = values
	}

	hasResume = len(resumeMap) > 0 || len(resumeValues) > 0
	return resumeMap, resumeValues, hasResume, nil
}

func normalizeResumeMap(raw any) (map[string]Dynamic, error) {
	if raw == nil {
		return nil, nil
	}
	rm, ok := raw.(map[string]any)
	if !ok {
		if typed, ok := raw.(map[string]Dynamic); ok {
			out := make(map[string]Dynamic, len(typed))
			for k, v := range typed {
				out[k] = v
			}
			return out, nil
		}
		return nil, fmt.Errorf("%s must be map[string]any or map[string]Dynamic, got %T", ConfigKeyResumeMap, raw)
	}
	out := make(map[string]Dynamic, len(rm))
	for k, v := range rm {
		out[k] = Dyn(v)
	}
	return out, nil
}

func normalizeResumeValues(raw any) ([]Dynamic, error) {
	if raw == nil {
		return nil, nil
	}
	if value, ok := raw.(Dynamic); ok {
		return []Dynamic{value}, nil
	}
	if values, ok := raw.([]Dynamic); ok {
		return resumeValues(values), nil
	}
	if values, ok := raw.([]any); ok {
		out := make([]Dynamic, 0, len(values))
		for _, v := range values {
			out = append(out, Dyn(v))
		}
		return out, nil
	}
	return []Dynamic{Dyn(raw)}, nil
}

func normalizeCommandResume(raw any) (map[string]Dynamic, []Dynamic, error) {
	if raw == nil {
		return nil, nil, nil
	}

	switch resume := raw.(type) {
	case map[string]Dynamic:
		mapped, err := normalizeResumeMap(resume)
		if err != nil {
			return nil, nil, err
		}
		return mapped, nil, nil
	case map[string]any:
		mapped, err := normalizeResumeMap(resume)
		if err != nil {
			return nil, nil, err
		}
		return mapped, nil, nil
	}

	rv := reflect.ValueOf(raw)
	if rv.IsValid() && rv.Kind() == reflect.Map {
		return nil, nil, fmt.Errorf("command resume map must use string keys, got %T", raw)
	}

	values, err := normalizeResumeValues(raw)
	if err != nil {
		return nil, nil, err
	}
	return nil, values, nil
}

func mergePendingWrites(existing, updates []checkpoint.PendingWrite) []checkpoint.PendingWrite {
	if len(updates) == 0 {
		return existing
	}
	out := append([]checkpoint.PendingWrite(nil), existing...)
	indexByKey := make(map[string]int, len(out))
	for i, w := range out {
		if !isSpecialPendingChannel(w.Channel) {
			continue
		}
		indexByKey[pendingKey(w.TaskID, w.Channel)] = i
	}
	for _, w := range updates {
		if !isSpecialPendingChannel(w.Channel) {
			out = append(out, w)
			continue
		}
		key := pendingKey(w.TaskID, w.Channel)
		if idx, ok := indexByKey[key]; ok {
			out[idx] = w
			continue
		}
		out = append(out, w)
		indexByKey[key] = len(out) - 1
	}
	return out
}

func pendingKey(taskID, channel string) string {
	return taskID + "|" + channel
}

func isSpecialPendingChannel(channel string) bool {
	switch channel {
	case pregelInterrupt, pregelResume, pregelError:
		return true
	default:
		return false
	}
}

func pendingInterrupts(writes []checkpoint.PendingWrite) []pendingInterrupt {
	interruptByTask := interruptsByTask(writes)
	resumed := make(map[string]bool)
	for _, w := range writes {
		if w.Channel == pregelResume {
			resumed[w.TaskID] = true
		}
	}
	out := make([]pendingInterrupt, 0, len(interruptByTask))
	for taskID, iv := range interruptByTask {
		if resumed[taskID] {
			continue
		}
		out = append(out, iv)
	}
	return out
}

func interruptsByTask(writes []checkpoint.PendingWrite) map[string]pendingInterrupt {
	out := make(map[string]pendingInterrupt)
	for _, w := range writes {
		if w.Channel != pregelInterrupt {
			continue
		}
		if iv, ok := pendingInterruptFromValue(w.Value); ok {
			iv.TaskID = w.TaskID
			out[w.TaskID] = iv
		}
	}
	return out
}

func pendingInterruptFromValue(value any) (pendingInterrupt, bool) {
	switch v := value.(type) {
	case interruptWrite:
		if v.Interrupt.ID == "" {
			return pendingInterrupt{}, false
		}
		return pendingInterrupt{InterruptID: v.Interrupt.ID, Node: v.Node, Path: append([]any(nil), v.Path...)}, true
	case *interruptWrite:
		if v == nil || v.Interrupt.ID == "" {
			return pendingInterrupt{}, false
		}
		return pendingInterrupt{InterruptID: v.Interrupt.ID, Node: v.Node, Path: append([]any(nil), v.Path...)}, true
	case Interrupt:
		if v.ID == "" {
			return pendingInterrupt{}, false
		}
		return pendingInterrupt{InterruptID: v.ID}, true
	case *Interrupt:
		if v == nil || v.ID == "" {
			return pendingInterrupt{}, false
		}
		return pendingInterrupt{InterruptID: v.ID}, true
	case []Interrupt:
		if len(v) == 0 || v[0].ID == "" {
			return pendingInterrupt{}, false
		}
		return pendingInterrupt{InterruptID: v[0].ID}, true
	case []any:
		for _, item := range v {
			if iv, ok := pendingInterruptFromValue(item); ok {
				return iv, true
			}
		}
	}
	return pendingInterrupt{}, false
}

func applyResumeWrites(
	pending []checkpoint.PendingWrite,
	resumeMap map[string]Dynamic,
	resumeValues []Dynamic,
) ([]checkpoint.PendingWrite, []pendingInterrupt, error) {
	unresolved := pendingInterrupts(pending)
	if err := validateResumeRequest(unresolved, resumeMap, resumeValues); err != nil {
		return pending, nil, err
	}
	if len(unresolved) == 0 {
		return pending, nil, nil
	}

	mapUpdates, err := resumeMapUpdates(unresolved, resumeMap)
	if err != nil {
		return pending, nil, err
	}
	valueUpdates, err := resumeValueUpdates(unresolved, resumeValues)
	if err != nil {
		return pending, nil, err
	}
	mapUpdates = append(mapUpdates, valueUpdates...)
	if len(mapUpdates) == 0 {
		return pending, unresolved, nil
	}
	pending = mergePendingWrites(pending, mapUpdates)
	return pending, unresolved, nil
}

func validateResumeRequest(unresolved []pendingInterrupt, resumeMap map[string]Dynamic, resumeValues []Dynamic) error {
	if len(unresolved) > 0 {
		return nil
	}
	if len(resumeMap) == 0 && len(resumeValues) == 0 {
		return nil
	}
	return fmt.Errorf("no pending interrupts to resume")
}

func resumeMapUpdates(unresolved []pendingInterrupt, resumeMap map[string]Dynamic) ([]checkpoint.PendingWrite, error) {
	if len(resumeMap) == 0 {
		return nil, nil
	}
	byID := make(map[string]pendingInterrupt, len(unresolved))
	for _, iv := range unresolved {
		byID[iv.InterruptID] = iv
	}
	updates := make([]checkpoint.PendingWrite, 0, len(resumeMap))
	for id, value := range resumeMap {
		iv, ok := byID[id]
		if !ok {
			return nil, fmt.Errorf("%s contains unknown interrupt id %q", ConfigKeyResumeMap, id)
		}
		updates = append(updates, checkpoint.PendingWrite{
			TaskID:  iv.TaskID,
			Channel: pregelResume,
			Value:   []Dynamic{value},
		})
	}
	return updates, nil
}

func resumeValueUpdates(unresolved []pendingInterrupt, resumeValues []Dynamic) ([]checkpoint.PendingWrite, error) {
	if len(resumeValues) == 0 {
		return nil, nil
	}
	if len(unresolved) > 1 {
		return nil, fmt.Errorf("multiple pending interrupts require %s with interrupt ids", ConfigKeyResumeMap)
	}
	return []checkpoint.PendingWrite{{
		TaskID:  unresolved[0].TaskID,
		Channel: pregelResume,
		Value:   resumeValues,
	}}, nil
}

func taskResumeValues(pending []checkpoint.PendingWrite, taskID string) []Dynamic {
	if taskID == "" {
		return nil
	}
	for i := len(pending) - 1; i >= 0; i-- {
		w := pending[i]
		if w.TaskID != taskID || w.Channel != pregelResume {
			continue
		}
		values, err := normalizeResumeValues(w.Value)
		if err != nil {
			return nil
		}
		return values
	}
	return nil
}

func parseVersionMap(in map[string]checkpoint.Version) map[string]int64 {
	out := make(map[string]int64, len(in))
	for k, v := range in {
		parsed, err := parseVersion(v)
		if err != nil {
			continue
		}
		out[k] = parsed
	}
	return out
}

func parseNestedVersionMap(in map[string]map[string]checkpoint.Version) map[string]map[string]int64 {
	out := make(map[string]map[string]int64, len(in))
	for k, inner := range in {
		out[k] = parseVersionMap(inner)
	}
	return out
}

func parseVersion(raw checkpoint.Version) (int64, error) {
	return checkpoint.VersionAsInt64(raw)
}

func hasNewUpdatesSinceLastInterrupt(channelVersions map[string]int64, versionsSeen map[string]map[string]int64) bool {
	if len(channelVersions) == 0 {
		return false
	}
	seen := versionsSeen[pregelInterrupt]
	for channel, version := range channelVersions {
		if version > seen[channel] {
			return true
		}
	}
	return false
}

func interruptTasks[State any](
	tasks []pregelTask[State],
	interruptNodes []string,
	channelVersions map[string]int64,
	versionsSeen map[string]map[string]int64,
) []pregelTask[State] {
	if len(interruptNodes) == 0 || !hasNewUpdatesSinceLastInterrupt(channelVersions, versionsSeen) {
		return nil
	}
	if !shouldInterruptBefore(tasks, interruptNodes) {
		return nil
	}
	out := make([]pregelTask[State], 0, len(tasks))
	for _, task := range tasks {
		for _, name := range interruptNodes {
			if name == All || name == task.name {
				out = append(out, task)
				break
			}
		}
	}
	return out
}

func interruptResults(
	results []pregelTaskResult,
	interruptNodes []string,
	channelVersions map[string]int64,
	versionsSeen map[string]map[string]int64,
) []pregelTaskResult {
	if len(interruptNodes) == 0 || !hasNewUpdatesSinceLastInterrupt(channelVersions, versionsSeen) {
		return nil
	}
	if !shouldInterruptAfter(results, interruptNodes) {
		return nil
	}
	out := make([]pregelTaskResult, 0, len(results))
	for _, result := range results {
		for _, name := range interruptNodes {
			if name == All || name == result.node {
				out = append(out, result)
				break
			}
		}
	}
	return out
}

func collectSpecialWrites(results []pregelTaskResult) []checkpoint.PendingWrite {
	out := make([]checkpoint.PendingWrite, 0)
	for _, r := range results {
		for _, w := range r.writes {
			if !isSpecialPendingChannel(w.channel) {
				continue
			}
			value := w.value
			if w.channel == pregelInterrupt {
				if iv, ok := w.value.(Interrupt); ok {
					value = interruptWrite{Interrupt: iv, Node: r.node, Path: append([]any(nil), r.path...)}
				}
			}
			out = append(out, checkpoint.PendingWrite{
				TaskID:  r.taskID,
				Channel: w.channel,
				Value:   value,
			})
		}
	}
	return out
}

func prepareResumedTasks[State, Context, Input, Output any](
	g *StateGraph[State, Context, Input, Output],
	config Config,
	channels *pregelChannelMap,
	pendingWrites []checkpoint.PendingWrite,
) []pregelTask[State] {
	interruptByTask := interruptsByTask(pendingWrites)
	if len(interruptByTask) == 0 {
		return nil
	}
	out := make([]pregelTask[State], 0, len(interruptByTask))
	for taskID, pending := range interruptByTask {
		resumes := taskResumeValues(pendingWrites, taskID)
		if len(resumes) == 0 {
			continue
		}
		if pending.Node == "" || g.nodes[pending.Node] == nil {
			continue
		}
		state, err := stateFromChannels[State](channels)
		if err != nil {
			continue
		}
		task := pregelTask[State]{
			id:           taskID,
			name:         pending.Node,
			path:         append([]any(nil), pending.Path...),
			checkpointNS: taskCheckpointNamespace(config.CheckpointNS, pending.Node, taskID),
			input:        state,
			triggers:     []string{pregelResume},
			resumeValues: resumes,
		}
		setTaskNodeMetadata(g, &task)
		out = append(out, task)
	}
	return out
}
