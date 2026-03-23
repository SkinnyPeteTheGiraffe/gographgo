package runtime

import (
	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/graph"
)

// Task represents a unit of work to execute in a Pregel superstep.
//
// This mirrors Python LangGraph's pregel/_algo.py:PregelTaskWrites and the
// task descriptors used throughout the Pregel execution model.
type Task struct {
	// ID is the unique task identifier (UUID).
	ID string

	// Node is the name of the node to execute.
	Node string

	// Triggers lists the channel names that triggered this task.
	Triggers []string

	// Input is the state passed to the node function.
	Input any

	// Writes collects channel writes produced by the node.
	Writes []Write
}

// Write is a pending channel update produced by a task.
type Write struct {
	// Channel is the target channel name.
	Channel string

	// Value is the value to write.
	Value any
}

// TaskResult holds the outcome of an executed Task.
type TaskResult struct {
	// TaskID is the ID of the task that produced this result.
	TaskID string

	// Node is the name of the node that ran.
	Node string

	// Writes contains the channel updates produced by the node.
	Writes []Write

	// Interrupts contains any interrupt values raised during execution.
	Interrupts []graph.Interrupt

	// Err is set if the node returned an error.
	Err error
}

// ShouldInterrupt returns true if any task in the list matches one of the
// given node names or the wildcard All sentinel.
//
// Mirrors Python LangGraph's pregel/_algo.py:should_interrupt.
func ShouldInterrupt(tasks []Task, nodeNames []string) bool {
	for _, task := range tasks {
		for _, name := range nodeNames {
			if task.Node == name || name == graph.All {
				return true
			}
		}
	}
	return false
}

// ApplyWrites applies the writes from a set of TaskResults to a channel map.
// Interrupt writes (channel == "__interrupt__") are separated out and returned.
//
// Mirrors Python LangGraph's pregel/_algo.py:apply_writes.
func ApplyWrites(
	channels map[string]graph.Channel,
	results []TaskResult,
) ([]graph.Interrupt, error) {
	// Group writes by channel.
	grouped := make(map[string][]graph.Dynamic)
	var interrupts []graph.Interrupt

	for _, r := range results {
		if r.Err != nil {
			continue
		}
		for _, w := range r.Writes {
			if w.Channel == "__interrupt__" {
				if iv, ok := w.Value.(graph.Interrupt); ok {
					interrupts = append(interrupts, iv)
				}
				continue
			}
			grouped[w.Channel] = append(grouped[w.Channel], graph.Dyn(w.Value))
		}
		interrupts = append(interrupts, r.Interrupts...)
	}

	for key, values := range grouped {
		ch, ok := channels[key]
		if !ok {
			ch = graph.NewLastValue()
			channels[key] = ch
		}
		if _, err := ch.Update(values); err != nil {
			return interrupts, err
		}
	}

	return interrupts, nil
}
