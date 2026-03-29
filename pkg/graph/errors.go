package graph

import (
	"fmt"
	"time"
)

// ErrorCode represents specific error conditions in graph execution.
type ErrorCode string

const (
	// ErrorCodeGraphRecursionLimit is raised when the graph has exhausted
	// the maximum number of steps.
	ErrorCodeGraphRecursionLimit ErrorCode = "GRAPH_RECURSION_LIMIT"

	// ErrorCodeGraphStepTimeout is raised when a superstep exceeds its
	// configured execution timeout.
	ErrorCodeGraphStepTimeout ErrorCode = "GRAPH_STEP_TIMEOUT"

	// ErrorCodeInvalidConcurrentGraphUpdate is raised when attempting to
	// update a channel with an invalid set of updates.
	ErrorCodeInvalidConcurrentGraphUpdate ErrorCode = "INVALID_CONCURRENT_GRAPH_UPDATE"

	// ErrorCodeInvalidGraphNodeReturnValue is raised when a node returns
	// an invalid value type.
	ErrorCodeInvalidGraphNodeReturnValue ErrorCode = "INVALID_GRAPH_NODE_RETURN_VALUE"

	// ErrorCodeMultipleSubgraphs is raised when multiple subgraphs are
	// detected in a context that doesn't support them.
	ErrorCodeMultipleSubgraphs ErrorCode = "MULTIPLE_SUBGRAPHS"

	// ErrorCodeInvalidChatHistory is raised when chat history is invalid.
	ErrorCodeInvalidChatHistory ErrorCode = "INVALID_CHAT_HISTORY"
)

// CreateErrorMessage creates a standardized error message with its error code.
func CreateErrorMessage(message string, code ErrorCode) string {
	return fmt.Sprintf("%s [%s]", message, code)
}

// GraphRecursionError is raised when the graph has exhausted the maximum
// number of steps. This prevents infinite loops.
type GraphRecursionError struct {
	Message string
}

func (e *GraphRecursionError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return "graph recursion limit exceeded"
}

// GraphStepTimeoutError is raised when a superstep exceeds its timeout.
type GraphStepTimeoutError struct {
	Step    int
	Timeout time.Duration
}

func (e *GraphStepTimeoutError) Error() string {
	if e.Timeout > 0 {
		if e.Step >= 0 {
			return fmt.Sprintf("step %d exceeded timeout %s", e.Step, e.Timeout)
		}
		return fmt.Sprintf("step exceeded timeout %s", e.Timeout)
	}
	if e.Step >= 0 {
		return fmt.Sprintf("step %d exceeded timeout", e.Step)
	}
	return "step exceeded timeout"
}

// InvalidUpdateError is raised when attempting to update a channel with
// an invalid set of updates.
type InvalidUpdateError struct {
	Message string
}

func (e *InvalidUpdateError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return "invalid update"
}

// EmptyChannelError is raised when attempting to read from an empty channel.
type EmptyChannelError struct {
	Channel string
}

func (e *EmptyChannelError) Error() string {
	if e.Channel != "" {
		return fmt.Sprintf("channel '%s' is empty", e.Channel)
	}
	return "channel is empty"
}

// GraphBubbleUp is the base type for exceptions that bubble up through the graph.
type GraphBubbleUp struct {
	Value   any
	Message string
}

func (e *GraphBubbleUp) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return "graph bubble up"
}

// GraphInterrupt is raised when a subgraph is interrupted. It is suppressed
// by the root graph and never raised directly to users.
type GraphInterrupt struct {
	Interrupts []Interrupt
}

func (e *GraphInterrupt) Error() string {
	return "graph interrupted"
}

// ParentCommand is raised when a command should be sent to the parent graph.
type ParentCommand struct {
	Command Command
}

func (e *ParentCommand) Error() string {
	return "parent command"
}

// EmptyInputError is raised when graph receives an empty input.
type EmptyInputError struct {
	Message string
}

func (e *EmptyInputError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return "empty input"
}

// TaskNotFound is raised when the executor is unable to find a task
// (for distributed mode).
type TaskNotFound struct {
	TaskID string
}

func (e *TaskNotFound) Error() string {
	if e.TaskID != "" {
		return fmt.Sprintf("task not found: %s", e.TaskID)
	}
	return "task not found"
}

// ExternalStateConflictError is raised when a checkpoint resume/replay request
// no longer matches the externally authoritative state version.
type ExternalStateConflictError struct {
	ThreadID          string
	CheckpointID      string
	CheckpointNS      string
	CheckpointVersion string
	ExternalVersion   string
}

func (e *ExternalStateConflictError) Error() string {
	return fmt.Sprintf(
		"external state conflict for thread %q checkpoint %q (ns=%q): checkpoint version %q does not match external version %q",
		e.ThreadID,
		e.CheckpointID,
		e.CheckpointNS,
		e.CheckpointVersion,
		e.ExternalVersion,
	)
}
