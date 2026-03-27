package graph

import (
	"context"

	"github.com/google/uuid"
)

const (
	// UIMessageType marks a UI add/update event.
	UIMessageType = "ui"

	// RemoveUIMessageType marks a UI removal event.
	RemoveUIMessageType = "remove-ui"

	defaultUIStateKey = "UI"
)

// AnyUIMessage is the wire shape used for UI state updates and stream events.
//
// `UIMessage` and `RemoveUIMessage` are aliases of this type so callers can use
// intent-revealing names while sharing a single reducer-friendly representation.
type AnyUIMessage struct {
	Props    map[string]any `json:"props,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
	Type     string         `json:"type"`
	ID       string         `json:"id"`
	Name     string         `json:"name,omitempty"`
}

// UIMessage is a UI add/update event.
type UIMessage = AnyUIMessage

// RemoveUIMessage is a UI removal event.
type RemoveUIMessage = AnyUIMessage

// UIState is a convenience state envelope for UI helper flows.
type UIState struct {
	UI []AnyUIMessage
}

// UIPushOptions configures PushUIMessage behavior.
type UIPushOptions struct {
	Metadata  map[string]any
	ID        string
	MessageID string
	StateKey  string
	Namespace []string
	Merge     bool
}

// UIDeleteOptions configures DeleteUIMessage behavior.
type UIDeleteOptions struct {
	StateKey  string
	Namespace []string
}

// UIMessageReducer merges UI updates into state.
//
// Messages are upserted by ID. A `remove-ui` update removes the matching UI
// message. When a UI update carries `metadata["merge"] == true`, its `props`
// are shallow-merged onto the existing message with the same ID.
func UIMessageReducer(left, right []AnyUIMessage) []AnyUIMessage {
	if len(left) == 0 && len(right) == 0 {
		return nil
	}

	merged := cloneUIMessages(left)
	byID := make(map[string]int, len(merged))
	for i, message := range merged {
		byID[message.ID] = i
	}

	toRemove := make(map[string]struct{})
	for _, message := range cloneUIMessages(right) {
		if message.ID == "" {
			message.ID = uuid.New().String()
		}

		if idx, ok := byID[message.ID]; ok {
			if message.Type == RemoveUIMessageType {
				toRemove[message.ID] = struct{}{}
				continue
			}
			delete(toRemove, message.ID)
			if shouldMergeUIProps(message) {
				message.Props = mergeUIProps(merged[idx].Props, message.Props)
			}
			merged[idx] = message
			continue
		}

		if message.Type == RemoveUIMessageType {
			continue
		}
		byID[message.ID] = len(merged)
		merged = append(merged, message)
	}

	if len(toRemove) == 0 {
		return merged
	}
	result := merged[:0]
	for _, message := range merged {
		if _, remove := toRemove[message.ID]; remove {
			continue
		}
		result = append(result, message)
	}
	return result
}

// NewUIStateGraph returns a StateGraph pre-configured with the UI reducer on
// the `UI` field.
func NewUIStateGraph() *StateGraph[UIState, any, UIState, UIState] {
	g := NewStateGraph[UIState]()
	g.channels[defaultUIStateKey] = NewBinaryOperatorAggregate(func(a, b any) any {
		left, _ := a.([]AnyUIMessage)
		right, _ := b.([]AnyUIMessage)
		return UIMessageReducer(left, right)
	})
	return g
}

// PushUIMessage emits a UI event and returns a Command that appends it to UI state.
//
// Callers typically return the resulting command with `NodeCommand(...)`.
func PushUIMessage(ctx context.Context, name string, props map[string]any, options UIPushOptions) (UIMessage, *Command) {
	message := UIMessage{
		Type:     UIMessageType,
		ID:       options.ID,
		Name:     name,
		Props:    cloneAnyMap(props),
		Metadata: buildUIMetadata(ctx, options.Metadata, options.MessageID, options.Merge),
	}
	if message.ID == "" {
		message.ID = uuid.New().String()
	}
	emitUIEvent(ctx, message, options.Namespace)
	return message, uiStateCommand(message, options.StateKey)
}

// DeleteUIMessage emits a UI removal event and returns a Command that removes
// the message from UI state.
//
// Callers typically return the resulting command with `NodeCommand(...)`.
func DeleteUIMessage(ctx context.Context, id string, options UIDeleteOptions) (RemoveUIMessage, *Command) {
	message := RemoveUIMessage{Type: RemoveUIMessageType, ID: id}
	emitUIEvent(ctx, message, options.Namespace)
	return message, uiStateCommand(message, options.StateKey)
}

func buildUIMetadata(ctx context.Context, extra map[string]any, messageID string, merge bool) map[string]any {
	metadata := map[string]any{}
	if cfgMeta := GetConfig(ctx).Metadata; len(cfgMeta) > 0 {
		if runID, ok := cfgMeta["run_id"]; ok {
			metadata["run_id"] = runID
		}
		if tags, ok := cfgMeta["tags"]; ok {
			metadata["tags"] = tags
		}
		if runName, ok := cfgMeta["run_name"]; ok {
			metadata["name"] = runName
		}
	}
	for key, value := range cloneAnyMap(extra) {
		metadata[key] = value
	}
	if messageID != "" {
		metadata["message_id"] = messageID
	}
	if merge {
		metadata["merge"] = true
	}
	if len(metadata) == 0 {
		return nil
	}
	return metadata
}

func emitUIEvent(ctx context.Context, message AnyUIMessage, namespace []string) {
	GetStreamWriter(ctx)(StreamEmit{
		Mode:      StreamModeCustom,
		Data:      cloneUIMessage(message),
		Namespace: append([]string(nil), namespace...),
	})
}

func uiStateCommand(message AnyUIMessage, stateKey string) *Command {
	if stateKey == "" {
		stateKey = defaultUIStateKey
	}
	return &Command{Update: map[string]Dynamic{stateKey: Dyn([]AnyUIMessage{cloneUIMessage(message)})}}
}

func shouldMergeUIProps(message AnyUIMessage) bool {
	if message.Metadata == nil {
		return false
	}
	merge, ok := message.Metadata["merge"].(bool)
	return ok && merge
}

func cloneUIMessage(message AnyUIMessage) AnyUIMessage {
	return AnyUIMessage{
		Type:     message.Type,
		ID:       message.ID,
		Name:     message.Name,
		Props:    cloneAnyMap(message.Props),
		Metadata: cloneAnyMap(message.Metadata),
	}
}

func cloneUIMessages(messages []AnyUIMessage) []AnyUIMessage {
	if len(messages) == 0 {
		return nil
	}
	out := make([]AnyUIMessage, len(messages))
	for i, message := range messages {
		out[i] = cloneUIMessage(message)
	}
	return out
}

func mergeUIProps(base, overlay map[string]any) map[string]any {
	switch {
	case len(base) == 0 && len(overlay) == 0:
		return nil
	case len(base) == 0:
		return cloneAnyMap(overlay)
	case len(overlay) == 0:
		return cloneAnyMap(base)
	}
	out := cloneAnyMap(base)
	for key, value := range overlay {
		out[key] = value
	}
	return out
}
