// Package graph provides the core graph framework APIs for gographgo.
//
// This package is a Go port of LangGraph's graph module, providing
// state-based graph construction and execution capabilities.
package graph

// Constants for graph execution.
const (
	// Start is the first (virtual) node in a graph.
	// All graph executions begin at this node.
	Start = "__start__"

	// End is the last (virtual) node in a graph.
	// Graph execution terminates when reaching this node.
	End = "__end__"

	// TagHidden is used to hide nodes/edges from certain
	// tracing and streaming environments.
	TagHidden = "gographgo:hidden"

	// TagNoStream disables streaming for chat models.
	TagNoStream = "nostream"

	// ConfigKeyResume stores a single resume value (or ordered resume values)
	// in Config.Metadata for interrupt resume.
	ConfigKeyResume = "resume"

	// ConfigKeyResumeMap stores interrupt-id keyed resume values in
	// Config.Metadata. This matches LangGraph's ConfigKeyResumeMap behavior.
	ConfigKeyResumeMap = "resume_map"
)
