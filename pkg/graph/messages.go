package graph

import "github.com/google/uuid"

// Message represents a chat message with a role, content, and unique ID.
//
// This is a Go-native message type used by MessagesState. It is intentionally
// simple — it does not depend on any LLM SDK type. Nodes that use LLM clients
// may use any message type internally and convert as needed.
type Message struct {
	// ID is a unique identifier. Auto-assigned on creation if empty.
	ID string

	// Role is the message author role: "system", "user", "assistant", "tool".
	Role string

	// Content is the message text content.
	Content string

	// ToolCallID links a tool result to the originating tool call.
	ToolCallID string

	// Name is an optional name for the message author.
	Name string
}

// RemoveAllMessages is the sentinel ID token used for remove-all updates.
const RemoveAllMessages = "__remove_all__"

// RemoveAllMessagesSentinel returns the explicit remove-all sentinel Message
// for use with AddMessages updates.
//
// Example:
//
//	right := []graph.Message{
//		graph.RemoveAllMessagesSentinel(),
//		{ID: "3", Role: "user", Content: "fresh start"},
//	}
func RemoveAllMessagesSentinel() Message {
	return Message{ID: RemoveAllMessages, Role: "remove"}
}

// MessagesState is the canonical state type for message-centric graphs.
// Its "messages" field uses the AddMessages reducer, which merges incoming
// messages by ID (upsert semantics): new IDs are appended; existing IDs
// replace the prior message; Role == "remove" signals deletion.
//
// This type is intended for chat-style state flows.
type MessagesState struct {
	Messages []Message
}

// MessageGraph is a convenience alias for a StateGraph configured around
// MessagesState.
type MessageGraph = StateGraph[MessagesState, any, MessagesState, MessagesState]

// AddMessages merges right into left using upsert-by-ID semantics.
//
// Behavior is:
//   - Messages in right with new IDs are appended.
//   - Messages in right whose ID matches a message in left replace it.
//   - A Message with Role == "remove" causes the message with that ID to be deleted.
//   - If right contains RemoveAllMessagesSentinel(), the entire list is
//     replaced by messages appearing after that sentinel in right.
//   - For backward compatibility, Content == RemoveAllMessages is still treated
//     as remove-all.
//   - Messages with empty IDs are assigned new UUIDs.
//
// AddMessages can be used as a reducer in BinaryOperatorAggregate:
//
//	graph.channels["messages"] = graph.NewBinaryOperatorAggregate(func(a, b any) any {
//	    left, _ := a.([]graph.Message)
//	    right, _ := b.([]graph.Message)
//	    return graph.AddMessages(left, right)
//	})
func AddMessages(left, right []Message) []Message {
	// Assign missing IDs.
	for i := range left {
		if left[i].ID == "" {
			left[i].ID = uuid.New().String()
		}
	}

	// Check for remove-all sentinel in right.
	removeAllIdx := -1
	for i := range right {
		if right[i].ID == "" {
			right[i].ID = uuid.New().String()
		}
		if isRemoveAllMessagesSentinel(right[i]) || right[i].Content == RemoveAllMessages {
			removeAllIdx = i
		}
	}
	if removeAllIdx >= 0 {
		return append([]Message(nil), right[removeAllIdx+1:]...)
	}

	// Build index of left by ID.
	merged := append([]Message(nil), left...)
	indexByID := make(map[string]int, len(merged))
	for i, m := range merged {
		indexByID[m.ID] = i
	}

	idsToRemove := make(map[string]bool)
	for _, m := range right {
		if m.Role == "remove" {
			idsToRemove[m.ID] = true
			continue
		}
		if idx, exists := indexByID[m.ID]; exists {
			merged[idx] = m
		} else {
			indexByID[m.ID] = len(merged)
			merged = append(merged, m)
		}
	}

	if len(idsToRemove) == 0 {
		return merged
	}
	out := merged[:0]
	for _, m := range merged {
		if !idsToRemove[m.ID] {
			out = append(out, m)
		}
	}
	return out
}

func isRemoveAllMessagesSentinel(m Message) bool {
	return m.Role == "remove" && m.ID == RemoveAllMessages
}

func newMessagesStateGraph() *StateGraph[MessagesState, any, MessagesState, MessagesState] {
	g := NewStateGraph[MessagesState]()
	g.channels["Messages"] = NewBinaryOperatorAggregate(func(a, b any) any {
		left, _ := a.([]Message)
		right, _ := b.([]Message)
		return AddMessages(left, right)
	})
	return g
}

// NewMessageGraph returns a MessageGraph pre-configured with the AddMessages
// reducer on the `Messages` field.
func NewMessageGraph() *MessageGraph {
	return newMessagesStateGraph()
}

// NewMessagesStateGraph returns a StateGraph pre-configured for MessagesState
// with the AddMessages reducer on the "Messages" field.
//
// Use this as a starting point for chat / agent graphs that accumulate
// message history.
func NewMessagesStateGraph() *StateGraph[MessagesState, any, MessagesState, MessagesState] {
	return newMessagesStateGraph()
}
