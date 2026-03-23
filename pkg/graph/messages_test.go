package graph_test

import (
	"testing"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/graph"
)

// helpers

func msg(id, role, content string) graph.Message {
	return graph.Message{ID: id, Role: role, Content: content}
}

// TestAddMessages_AppendNewMessages verifies that messages with new IDs are appended.
func TestAddMessages_AppendNewMessages(t *testing.T) {
	left := []graph.Message{msg("1", "user", "hello")}
	right := []graph.Message{msg("2", "assistant", "hi")}
	got := graph.AddMessages(left, right)
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	if got[1].ID != "2" || got[1].Content != "hi" {
		t.Errorf("second message = %+v", got[1])
	}
}

// TestAddMessages_Upsert verifies that messages with existing IDs replace in place.
func TestAddMessages_Upsert(t *testing.T) {
	left := []graph.Message{msg("1", "user", "hello"), msg("2", "assistant", "old")}
	right := []graph.Message{msg("2", "assistant", "updated")}
	got := graph.AddMessages(left, right)
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	if got[1].Content != "updated" {
		t.Errorf("content = %q, want %q", got[1].Content, "updated")
	}
}

// TestAddMessages_Remove verifies that role=="remove" deletes the message.
func TestAddMessages_Remove(t *testing.T) {
	left := []graph.Message{msg("1", "user", "hello"), msg("2", "assistant", "hi")}
	right := []graph.Message{{ID: "1", Role: "remove"}}
	got := graph.AddMessages(left, right)
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1", len(got))
	}
	if got[0].ID != "2" {
		t.Errorf("remaining ID = %q, want %q", got[0].ID, "2")
	}
}

// TestAddMessages_RemoveAll verifies the explicit remove-all sentinel clears history.
func TestAddMessages_RemoveAll(t *testing.T) {
	left := []graph.Message{msg("1", "user", "hello"), msg("2", "assistant", "hi")}
	right := []graph.Message{
		graph.RemoveAllMessagesSentinel(),
		msg("3", "user", "fresh start"),
	}
	got := graph.AddMessages(left, right)
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1", len(got))
	}
	if got[0].ID != "3" {
		t.Errorf("ID = %q, want %q", got[0].ID, "3")
	}
}

// TestAddMessages_RemoveAllLegacyContentSentinel keeps backward compatibility.
func TestAddMessages_RemoveAllLegacyContentSentinel(t *testing.T) {
	left := []graph.Message{msg("1", "user", "hello"), msg("2", "assistant", "hi")}
	right := []graph.Message{
		{Content: graph.RemoveAllMessages},
		msg("3", "user", "fresh start"),
	}
	got := graph.AddMessages(left, right)
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1", len(got))
	}
	if got[0].ID != "3" {
		t.Errorf("ID = %q, want %q", got[0].ID, "3")
	}
}

func TestRemoveAllMessagesSentinel(t *testing.T) {
	s := graph.RemoveAllMessagesSentinel()
	if s.Role != "remove" {
		t.Fatalf("role = %q, want remove", s.Role)
	}
	if s.ID != graph.RemoveAllMessages {
		t.Fatalf("id = %q, want %q", s.ID, graph.RemoveAllMessages)
	}
}

// TestAddMessages_AutoAssignIDs verifies that empty IDs get UUIDs.
func TestAddMessages_AutoAssignIDs(t *testing.T) {
	left := []graph.Message{{Role: "user", Content: "hello"}}
	right := []graph.Message{{Role: "assistant", Content: "hi"}}
	got := graph.AddMessages(left, right)
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	for i, m := range got {
		if m.ID == "" {
			t.Errorf("got[%d] has empty ID", i)
		}
	}
}

// TestAddMessages_EmptyRight returns left unchanged.
func TestAddMessages_EmptyRight(t *testing.T) {
	left := []graph.Message{msg("1", "user", "hello")}
	got := graph.AddMessages(left, nil)
	if len(got) != 1 || got[0].ID != "1" {
		t.Errorf("unexpected result = %+v", got)
	}
}

// TestAddMessages_BothEmpty returns empty slice.
func TestAddMessages_BothEmpty(t *testing.T) {
	got := graph.AddMessages(nil, nil)
	if len(got) != 0 {
		t.Errorf("len = %d, want 0", len(got))
	}
}

// TestAddMessages_OrderPreserved verifies that ordering is maintained.
func TestAddMessages_OrderPreserved(t *testing.T) {
	left := []graph.Message{msg("a", "user", "1"), msg("b", "user", "2")}
	right := []graph.Message{msg("c", "user", "3")}
	got := graph.AddMessages(left, right)
	ids := []string{got[0].ID, got[1].ID, got[2].ID}
	if ids[0] != "a" || ids[1] != "b" || ids[2] != "c" {
		t.Errorf("order = %v, want [a b c]", ids)
	}
}

// TestNewMessagesStateGraph verifies the factory creates a usable graph.
func TestNewMessagesStateGraph(t *testing.T) {
	g := graph.NewMessagesStateGraph()
	if g == nil {
		t.Fatal("NewMessagesStateGraph returned nil")
	}
}
