package prebuilt

import "github.com/SkinnyPeteTheGiraffe/gographgo/pkg/graph"

// ToolsCondition routes to `tools` when the last assistant message has tool calls.
func ToolsCondition(state AgentState) graph.Route {
	return ToolsConditionWithOptions(state, ToolsConditionOptions[AgentState]{})
}

// ToolsConditionOptions customizes tools-condition routing behavior.
type ToolsConditionOptions[State any] struct {
	// Messages extracts the chat history to inspect. When nil and State is
	// AgentState, the helper uses AgentState.Messages.
	Messages func(State) []Message

	// OnToolCalls is returned when the last assistant message has tool calls.
	// Defaults to routing to `tools`.
	OnToolCalls graph.Route

	// OnNoToolCalls is returned when there are no pending tool calls.
	// Defaults to routing to `END`.
	OnNoToolCalls graph.Route
}

// ToolsConditionWithOptions routes based on the last assistant tool call while
// allowing custom state shapes and route destinations.
func ToolsConditionWithOptions[State any](state State, options ToolsConditionOptions[State]) graph.Route {
	messages := extractToolsConditionMessages(state, options.Messages)
	if len(messages) == 0 {
		return fallbackToolsRoute(options.OnNoToolCalls, graph.RouteTo(graph.END))
	}
	last := messages[len(messages)-1]
	if last.Role == "assistant" && len(last.ToolCalls) > 0 {
		return fallbackToolsRoute(options.OnToolCalls, graph.RouteTo(toolsNodeName))
	}
	return fallbackToolsRoute(options.OnNoToolCalls, graph.RouteTo(graph.END))
}

func extractToolsConditionMessages[State any](state State, extractor func(State) []Message) []Message {
	if extractor != nil {
		return extractor(state)
	}
	if agentState, ok := any(state).(AgentState); ok {
		return agentState.Messages
	}
	return nil
}

func fallbackToolsRoute(route, fallback graph.Route) graph.Route {
	if len(route.Nodes) == 0 && len(route.Sends) == 0 {
		return fallback
	}
	return route
}

// CreateToolCallingExecutor is an alias kept for parity with LangGraph naming.
func CreateToolCallingExecutor(model AgentModel, tools []Tool, opts ...ReactAgentOption) *ReactAgent {
	return CreateReactAgent(model, tools, opts...)
}
