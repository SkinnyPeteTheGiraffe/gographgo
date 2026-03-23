// Package compile contains private compile-time planning internals for gographgo.
//
// These constants define the internal Pregel protocol keys used during graph
// compilation and execution. They mirror Python LangGraph's
// langgraph/_internal/_constants.py values.
package compile

// Internal Pregel protocol constants.
const (
	// --- Reserved write keys ---

	// INPUT is the channel name for the graph's input value.
	INPUT = "__input__"

	// INTERRUPT is the channel name for dynamic interrupt values raised by nodes.
	INTERRUPT = "__interrupt__"

	// RESUME is the channel name for values passed to resume a node after an interrupt.
	RESUME = "__resume__"

	// ERROR is the channel name for errors raised by nodes.
	ERROR = "__error__"

	// TASKS is the channel name for Send objects returned by nodes/edges.
	TASKS = "__pregel_tasks"

	// RETURN is the channel name for writes of a task where we record the return value.
	RETURN = "__return__"

	// PREVIOUS is the channel name for the implicit branch that handles Control values.
	PREVIOUS = "__previous__"

	// PUSH denotes push-style tasks (created by Send objects).
	PUSH = "__pregel_push"

	// PULL denotes pull-style tasks (triggered by edges).
	PULL = "__pregel_pull"

	// NullTaskID is the task ID used for writes not associated with a task.
	NullTaskID = "00000000-0000-0000-0000-000000000000"

	// NSep separates namespace levels in checkpoint_ns.
	NSep = "|"

	// NSEnd separates the namespace from the task_id within a level.
	NSEnd = ":"

	// CONF is the key for the configurable dict in RunnableConfig.
	CONF = "configurable"

	// --- Reserved config.configurable keys ---

	// ConfigKeySend holds the write function injected by the runtime.
	ConfigKeySend = "__pregel_send"

	// ConfigKeyRead holds the read function that returns a copy of the current state.
	ConfigKeyRead = "__pregel_read"

	// ConfigKeyCall holds the call function for sub-invocations.
	ConfigKeyCall = "__pregel_call"

	// ConfigKeyCheckpointer holds a CheckpointSaver passed from parent to child graphs.
	ConfigKeyCheckpointer = "__pregel_checkpointer"

	// ConfigKeyStream holds a StreamProtocol passed from parent to child graphs.
	ConfigKeyStream = "__pregel_stream"

	// ConfigKeyCache holds a BaseCache made available to subgraphs.
	ConfigKeyCache = "__pregel_cache"

	// ConfigKeyResumingNode indicates if subgraphs should resume from a previous checkpoint.
	ConfigKeyResumingNode = "__pregel_resuming"

	// ConfigKeyTaskID holds the task ID for the current task.
	ConfigKeyTaskID = "__pregel_task_id"

	// ConfigKeyThreadID holds the thread ID for the current invocation.
	ConfigKeyThreadID = "thread_id"

	// ConfigKeyCheckpointMap holds a mapping of checkpoint_ns → checkpoint_id.
	ConfigKeyCheckpointMap = "checkpoint_map"

	// ConfigKeyCheckpointID holds the current checkpoint_id, if any.
	ConfigKeyCheckpointID = "checkpoint_id"

	// ConfigKeyCheckpointNS holds the current checkpoint namespace ("" for root graph).
	ConfigKeyCheckpointNS = "checkpoint_ns"

	// ConfigKeyScratchpad holds a mutable dict for temporary storage scoped to the current task.
	ConfigKeyScratchpad = "__pregel_scratchpad"

	// ConfigKeyStreamWriter holds the custom StreamWriter injected into nodes.
	ConfigKeyStreamWriter = "__stream_writer"

	// ConfigKeyDurability holds the durability mode (sync/async/exit).
	ConfigKeyDurability = "__pregel_durability"
)
