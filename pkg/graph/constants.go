// Package graph provides the core graph framework APIs for gographgo.
//
// This package is a Go port of LangGraph's graph module, providing
// state-based graph construction and execution capabilities.
package graph

// Constants for graph execution.
const (
	// START is the first (virtual) node in a graph.
	// All graph executions begin at this node.
	START = "__start__"

	// END is the last (virtual) node in a graph.
	// Graph execution terminates when reaching this node.
	END = "__end__"

	// TAG_HIDDEN is used to hide nodes/edges from certain
	// tracing and streaming environments.
	TAG_HIDDEN = "langsmith:hidden"

	// TAG_NOSTREAM disables streaming for chat models.
	TAG_NOSTREAM = "nostream"

	// CONFIG_KEY_RESUME stores a single resume value (or ordered resume values)
	// in Config.Metadata for interrupt resume.
	CONFIG_KEY_RESUME = "resume"

	// CONFIG_KEY_RESUME_MAP stores interrupt-id keyed resume values in
	// Config.Metadata. This matches LangGraph's CONFIG_KEY_RESUME_MAP behavior.
	CONFIG_KEY_RESUME_MAP = "resume_map"
)
