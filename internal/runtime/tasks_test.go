package runtime

import (
	"fmt"
	"testing"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/graph"
)

func TestShouldInterrupt_MatchByName(t *testing.T) {
	tasks := []Task{
		{ID: "1", Node: "alpha"},
		{ID: "2", Node: "beta"},
	}
	if !ShouldInterrupt(tasks, []string{"beta"}) {
		t.Error("expected ShouldInterrupt=true for 'beta'")
	}
	if ShouldInterrupt(tasks, []string{"gamma"}) {
		t.Error("expected ShouldInterrupt=false for unknown node")
	}
}

func TestShouldInterrupt_WildcardAll(t *testing.T) {
	tasks := []Task{
		{ID: "1", Node: "anything"},
	}
	if !ShouldInterrupt(tasks, []string{graph.All}) {
		t.Error("expected ShouldInterrupt=true for wildcard All")
	}
}

func TestShouldInterrupt_EmptyInterruptList(t *testing.T) {
	tasks := []Task{{ID: "1", Node: "n"}}
	if ShouldInterrupt(tasks, nil) {
		t.Error("expected ShouldInterrupt=false for empty interrupt list")
	}
}

func TestApplyWrites_DistributesWritesToChannels(t *testing.T) {
	channels := map[string]graph.Channel{
		"x": graph.NewLastValue(),
	}

	results := []TaskResult{
		{
			TaskID: "t1",
			Node:   "node_a",
			Writes: []Write{
				{Channel: "x", Value: 99},
			},
		},
	}

	_, err := ApplyWrites(channels, results)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	val, err := channels["x"].Get()
	if err != nil {
		t.Fatalf("expected x to have a value: %v", err)
	}
	if val.Value() != 99 {
		t.Errorf("expected x=99, got %v", val)
	}
}

func TestApplyWrites_SeparatesInterruptWrites(t *testing.T) {
	channels := map[string]graph.Channel{}

	iv := graph.NewInterrupt(graph.Dyn("pause"), "id1")
	results := []TaskResult{
		{
			TaskID: "t1",
			Node:   "node",
			Writes: []Write{
				{Channel: "__interrupt__", Value: iv},
			},
		},
	}

	interrupts, err := ApplyWrites(channels, results)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(interrupts) != 1 {
		t.Fatalf("expected 1 interrupt, got %d", len(interrupts))
	}
	if interrupts[0].ID != "id1" {
		t.Errorf("expected interrupt ID=id1, got %v", interrupts[0].ID)
	}
}

func TestApplyWrites_SkipsErroredTasks(t *testing.T) {
	channels := map[string]graph.Channel{
		"x": graph.NewLastValue(),
	}

	results := []TaskResult{
		{
			Node:   "failed",
			Err:    fmt.Errorf("oops"), // This task errored
			Writes: []Write{{Channel: "x", Value: 1}},
		},
	}
	// Writes from errored tasks should be skipped.
	_, err := ApplyWrites(channels, results)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_, err = channels["x"].Get()
	if err == nil {
		t.Error("expected x to be empty (errored task writes skipped)")
	}
}

func TestApplyWrites_AutoCreatesLastValueForUnknownChannel(t *testing.T) {
	channels := map[string]graph.Channel{} // no pre-defined channels

	results := []TaskResult{
		{
			Node:   "n",
			Writes: []Write{{Channel: "new_key", Value: "created"}},
		},
	}
	_, err := ApplyWrites(channels, results)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	val, err := channels["new_key"].Get()
	if err != nil {
		t.Fatalf("expected new_key: %v", err)
	}
	if val.Value() != "created" {
		t.Errorf("expected new_key=created, got %v", val)
	}
}
