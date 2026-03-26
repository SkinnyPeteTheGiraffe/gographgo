package graph

import (
	"context"
	"reflect"
	"testing"
)

func TestNewMessageGraph_EquivalentToManualMessagesGraph(t *testing.T) {
	buildFlow := func(g *StateGraph[MessagesState, any, MessagesState, MessagesState]) {
		g.AddNode("append", func(_ context.Context, _ MessagesState) (NodeResult, error) {
			return NodeWrites(DynMap(map[string]any{
				"Messages": []Message{{ID: "1", Role: "assistant", Content: "hello"}},
			})), nil
		})
		g.AddNode("update", func(_ context.Context, _ MessagesState) (NodeResult, error) {
			return NodeWrites(DynMap(map[string]any{
				"Messages": []Message{
					{ID: "1", Role: "assistant", Content: "hello again"},
					{ID: "2", Role: "user", Content: "thanks"},
				},
			})), nil
		})
		g.AddEdge(Start, "append")
		g.AddEdge("append", "update")
		g.AddEdge("update", End)
	}

	manual := NewStateGraph[MessagesState]()
	manual.channels["Messages"] = NewBinaryOperatorAggregate(func(a, b any) any {
		left, _ := a.([]Message)
		right, _ := b.([]Message)
		return AddMessages(left, right)
	})
	buildFlow(manual)

	convenience := NewMessageGraph()
	buildFlow(convenience)

	input := MessagesState{Messages: []Message{{ID: "0", Role: "system", Content: "start"}}}
	manualOut := invokeMessagesState(t, manual, input)
	convenienceOut := invokeMessagesState(t, convenience, input)

	if !reflect.DeepEqual(convenienceOut, manualOut) {
		t.Fatalf("convenience output mismatch:\n got:  %+v\n want: %+v", convenienceOut, manualOut)
	}
}

func invokeMessagesState(
	t *testing.T,
	g *StateGraph[MessagesState, any, MessagesState, MessagesState],
	input MessagesState,
) MessagesState {
	t.Helper()

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	out, err := compiled.Invoke(context.Background(), input)
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}

	state, ok := out.Value.(MessagesState)
	if !ok {
		t.Fatalf("output type = %T, want MessagesState", out.Value)
	}
	return state
}
