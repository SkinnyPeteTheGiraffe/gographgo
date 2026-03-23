package graph

import (
	"context"

	"github.com/google/uuid"
)

// --- Scratchpad: per-task mutable state for interrupt tracking ---

// taskScratchpad holds mutable state scoped to a single task execution.
// It tracks which interrupt calls have been made and what resume values are
// available from a prior run.
//
// Mirrors Python LangGraph's _internal/_scratchpad.py Scratchpad class.
type taskScratchpad struct {
	// resume holds the resume values provided by a Command(resume=...) call.
	// They are matched positionally to interrupt() calls within the node.
	resume []Dynamic

	// counter is the number of interrupt() calls made so far in this task.
	counter int

	// onResume is called whenever NodeInterrupt returns a resume value.
	// It allows the runtime to persist RESUME-channel writes for parity with
	// LangGraph's interrupt scratchpad behavior.
	onResume func(resume []Dynamic)
}

// interruptCounter increments the counter and returns the old value.
func (s *taskScratchpad) interruptCounter() int {
	idx := s.counter
	s.counter++
	return idx
}

// taskScratchpadKey is the context key for the per-task scratchpad.
type taskScratchpadKey struct{}

// withTaskScratchpad returns a context carrying the given scratchpad.
func withTaskScratchpad(ctx context.Context, sp *taskScratchpad) context.Context {
	return context.WithValue(ctx, taskScratchpadKey{}, sp)
}

// getTaskScratchpad retrieves the scratchpad from context, or returns a new
// empty one if none is present.
func getTaskScratchpad(ctx context.Context) *taskScratchpad {
	if sp, ok := ctx.Value(taskScratchpadKey{}).(*taskScratchpad); ok {
		return sp
	}
	return &taskScratchpad{}
}

// --- nodeInterruptSignal: the panic payload used to halt a node ---

// nodeInterruptSignal is the panic value raised by Interrupt when no resume
// value is available. It carries the interrupt's value and a generated ID.
// The task executor recovers this and records the Interrupt.
type nodeInterruptSignal struct {
	interrupt Interrupt
}

// --- Public interrupt() function ---

// NodeInterrupt pauses the current node and surfaces value to the graph caller.
//
// On the first invocation within a node, NodeInterrupt raises an internal
// signal that halts the node and records an Interrupt in the run's output.
// The graph stops executing (or continues to the next interrupt-after boundary).
//
// When the graph is resumed with a Command{Resume: resumeValue}, the node is
// re-executed from the beginning. Each call to NodeInterrupt within the node
// is matched positionally to the resume values; once matched, NodeInterrupt
// returns the provided resume value instead of halting.
//
// Multiple NodeInterrupt calls per node are supported — matched in order.
//
// A Checkpointer must be configured for interrupt/resume to work across
// separate invocations. Without a checkpointer the graph can still be
// interrupted but cannot be resumed.
//
// Mirrors Python LangGraph's langgraph.types.interrupt() (renamed to
// NodeInterrupt in Go to avoid collision with the Interrupt struct type).
func NodeInterrupt(ctx context.Context, value Dynamic) Dynamic {
	sp := getTaskScratchpad(ctx)
	idx := sp.interruptCounter()

	// If we have a resume value for this interrupt index, return it.
	if idx < len(sp.resume) {
		if sp.onResume != nil {
			sp.onResume(resumeValues(sp.resume))
		}
		return sp.resume[idx]
	}

	// No resume value — generate an interrupt ID and panic.
	iv := NewInterrupt(value, uuid.New().String())
	panic(nodeInterruptSignal{interrupt: iv})
}

// resumeValues returns a cloned slice of positional resume values.
func resumeValues(resume []Dynamic) []Dynamic {
	if len(resume) == 0 {
		return nil
	}
	out := make([]Dynamic, len(resume))
	copy(out, resume)
	return out
}
