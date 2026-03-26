package prebuilt_test

import (
	"testing"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/graph"
	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/prebuilt"
)

func TestToolsCondition(t *testing.T) {
	route := prebuilt.ToolsCondition(prebuilt.AgentState{Messages: []prebuilt.Message{{Role: "assistant", ToolCalls: []prebuilt.ToolCall{{ID: "1", Name: "x"}}}}})
	if len(route.Nodes) != 1 || route.Nodes[0] != "tools" {
		t.Fatalf("route = %+v, want tools", route)
	}

	route = prebuilt.ToolsCondition(prebuilt.AgentState{Messages: []prebuilt.Message{{Role: "assistant", Content: "done"}}})
	if len(route.Nodes) != 1 || route.Nodes[0] != graph.End {
		t.Fatalf("route = %+v, want End", route)
	}
}

func TestToolsConditionWithOptions_CustomStateAndRoutes(t *testing.T) {
	type customState struct {
		History []prebuilt.Message
	}

	route := prebuilt.ToolsConditionWithOptions(customState{
		History: []prebuilt.Message{{Role: "assistant", ToolCalls: []prebuilt.ToolCall{{ID: "1", Name: "lookup"}}}},
	}, prebuilt.ToolsConditionOptions[customState]{
		Messages: func(state customState) []prebuilt.Message {
			return state.History
		},
		OnToolCalls:   graph.RouteTo("custom-tools"),
		OnNoToolCalls: graph.RouteTo("done"),
	})
	if len(route.Nodes) != 1 || route.Nodes[0] != "custom-tools" {
		t.Fatalf("route = %+v, want custom-tools", route)
	}

	route = prebuilt.ToolsConditionWithOptions(customState{
		History: []prebuilt.Message{{Role: "assistant", Content: "done"}},
	}, prebuilt.ToolsConditionOptions[customState]{
		Messages: func(state customState) []prebuilt.Message {
			return state.History
		},
		OnNoToolCalls: graph.RouteTo("done"),
	})
	if len(route.Nodes) != 1 || route.Nodes[0] != "done" {
		t.Fatalf("route = %+v, want done", route)
	}
}
