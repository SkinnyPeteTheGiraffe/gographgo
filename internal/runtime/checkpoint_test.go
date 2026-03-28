package runtime

import (
	"testing"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/graph"
)

func TestEmptyCheckpoint(t *testing.T) {
	snap := EmptyCheckpoint()
	if snap == nil {
		t.Fatal("EmptyCheckpoint returned nil")
	}
	if len(snap) != 0 {
		t.Fatalf("expected empty map, got %v", snap)
	}
}

func TestCreateCheckpoint_OnlyPersistsAvailableChannels(t *testing.T) {
	ch1 := graph.NewLastValue()
	ch2 := graph.NewLastValue()

	ch1.Update([]graph.Dynamic{graph.Dyn("hello")}) //nolint:errcheck // test setup value; error path is not under test here

	channels := map[string]graph.Channel{
		"key1": ch1,
		"key2": ch2, // no value — should not appear in snapshot
	}

	snap := CreateCheckpoint(channels)
	if _, ok := snap["key1"]; !ok {
		t.Error("expected key1 in snapshot")
	}
	if _, ok := snap["key2"]; ok {
		t.Error("expected key2 to be absent from snapshot (no value)")
	}
	if snap["key1"] != "hello" {
		t.Errorf("expected key1=hello, got %v", snap["key1"])
	}
}

func TestChannelsFromCheckpoint_RestoresValues(t *testing.T) {
	schema := map[string]graph.Channel{
		"x": graph.NewLastValue(),
		"y": graph.NewLastValue(),
	}
	snap := map[string]any{
		"x": 42,
	}

	restored, err := ChannelsFromCheckpoint(schema, snap)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	xVal, err := restored["x"].Get()
	if err != nil {
		t.Fatalf("expected x to have a value: %v", err)
	}
	if xVal.Value() != 42 {
		t.Errorf("expected x=42, got %v", xVal)
	}

	// y should be empty (not in snapshot).
	_, err = restored["y"].Get()
	if err == nil {
		t.Error("expected y to be empty after restore without checkpoint")
	}
}

func TestChannelsFromCheckpoint_MissingSchemaKeyGetsLastValue(t *testing.T) {
	schema := map[string]graph.Channel{} // empty schema
	snap := map[string]any{
		"unknown": "value",
	}

	restored, err := ChannelsFromCheckpoint(schema, snap)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	val, err := restored["unknown"].Get()
	if err != nil {
		t.Fatalf("expected unknown channel to have value: %v", err)
	}
	if val.Value() != "value" {
		t.Errorf("expected unknown=value, got %v", val)
	}
}
