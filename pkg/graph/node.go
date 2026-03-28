package graph

import "context"

// NodeResult is the typed output of a node execution.
type NodeResult struct {
	// Writes contains channel updates produced by the node.
	Writes map[string]Dynamic

	// State contains a whole-state replacement value.
	State *Dynamic

	// Command contains control-flow and update instructions.
	Command *Command
}

// NoNodeResult returns an empty node result.
func NoNodeResult() NodeResult {
	return NodeResult{}
}

// NodeWrites returns a NodeResult containing channel writes.
func NodeWrites(writes map[string]Dynamic) NodeResult {
	return NodeResult{Writes: writes}
}

// NodeState returns a NodeResult containing whole-state output.
func NodeState(state Dynamic) NodeResult {
	return NodeResult{State: &state}
}

// NodeCommand returns a NodeResult containing a command.
func NodeCommand(cmd *Command) NodeResult {
	return NodeResult{Command: cmd}
}

// Node represents a node function that can be executed in a graph.
// The function receives the current state and returns updates to the state.
type Node[State any] func(ctx context.Context, state State) (NodeResult, error)

// TaskContext carries per-task metadata available to ChannelWrite hooks.
type TaskContext[State any] struct {
	Input        State
	InputSchema  any
	CacheKey     *CacheKey
	ID           string
	Name         string
	CheckpointNS string
	Path         []any
	Triggers     []string
	Tags         []string
	Subgraphs    []string
	IsPush       bool
}

// ChannelWrite is a per-node output hook that can transform a node result.
//
// Hooks run after node execution and before conversion to channel writes.
// They run sequentially in registration order.
type ChannelWrite[State any] func(ctx context.Context, task TaskContext[State], result NodeResult) (NodeResult, error)

// NodeSpec contains the specification for a node.
type NodeSpec[State any] struct {
	// Name is the node identifier.
	Name string

	// Fn is the node function.
	Fn Node[State]

	// InputSchema is the inferred or explicit node input schema descriptor.
	InputSchema any

	// Metadata contains additional node information.
	Metadata map[string]any

	// RetryPolicy configures retry behavior.
	RetryPolicy []RetryPolicy

	// CachePolicy configures caching behavior.
	CachePolicy *CachePolicy

	// Writers are per-node output transformation hooks.
	Writers []ChannelWrite[State]

	// Subgraphs identifies subgraphs attached to this node for task metadata.
	Subgraphs []string

	// Destinations are possible routing destinations.
	Destinations []string

	// Defer indicates deferred execution.
	Defer bool
}

// BranchSpec contains the specification for a conditional branch.
type BranchSpec[State any] struct {
	// Name is the branch identifier.
	Name string

	// Path is the function that determines the next node(s).
	Path func(ctx context.Context, state State) (Route, error)

	// PathMap maps route node keys to concrete node names.
	PathMap map[string]string

	// Ends are the possible destination nodes.
	Ends []string
}

// Edge represents a directed edge between nodes.
type Edge struct {
	// Source is the starting node.
	Source string

	// Target is the destination node.
	Target string
}

// WaitingEdge represents an edge that waits for multiple sources.
type WaitingEdge struct {
	Target  string
	Sources []string
}
