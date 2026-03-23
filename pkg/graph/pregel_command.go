package graph

import "fmt"

// parentCommandError signals that a Command targeted the parent graph via
// Command.Graph == CommandParent. It is returned by commandResult and
// propagated up through runPregelLoop so that AsSubgraphNode can apply the
// command to the parent graph.
//
// Reaching a runPregelLoop that is not nested inside AsSubgraphNode means
// there is no parent graph; that condition is reported as an error.
type parentCommandError struct {
	cmd *Command
}

func (e *parentCommandError) Error() string {
	return fmt.Sprintf("no parent graph: Command.Graph is %q but no parent graph exists at this level", CommandParent)
}

// commandResult extracts the channel writes and routing information from a
// Command returned by a node. It mirrors Python LangGraph's Command handling
// in the Pregel loop.
//
// A node can return *Command to:
//   - Apply state updates (Command.Update)
//   - Route to specific nodes (Command.Goto)
//   - Resume pending interrupts (Command.Resume)
//
// Returns parentCommandError when cmd.Graph == CommandParent so the caller can
// propagate the command to the enclosing graph rather than handling it locally.
//
// Returns:
//   - writes: channel writes from Command.Update
//   - sendTargets: nodes/Send objects to route to from Command.Goto
//   - resumeMap: interrupt-id keyed resume values for interrupt resumption
//   - resumeValues: ordered/single resume values for interrupt resumption
func commandResult(cmd *Command) (writes []pregelWrite, sendTargets []Send, resumeMap map[string]any, resumeValues []any, err error) {
	if cmd == nil {
		return
	}

	// Commands with Graph == CommandParent must be handled by the parent graph.
	// Return parentCommandError so the caller can propagate the signal upward.
	if cmd.Graph != nil && *cmd.Graph == CommandParent {
		err = &parentCommandError{cmd: cmd}
		return
	}

	// Extract state update writes.
	if len(cmd.Update) > 0 {
		for k, v := range cmd.Update {
			writes = append(writes, pregelWrite{channel: k, value: v.Value()})
		}
	}

	// Extract Goto routing targets.
	for _, n := range cmd.Goto.Nodes {
		if n == "" || n == END {
			continue
		}
		sendTargets = append(sendTargets, Send{Node: n, Arg: Dyn(unwrapCommandUpdate(cmd.Update))})
	}
	for _, s := range cmd.Goto.Sends {
		if s.Node == "" || s.Node == END {
			continue
		}
		sendTargets = append(sendTargets, s)
	}

	// Extract resume payload.
	rm, rv, err := normalizeCommandResume(cmd.Resume)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	if len(rm) > 0 {
		resumeMap = make(map[string]any, len(rm))
		for id, value := range rm {
			resumeMap[id] = value.Value()
		}
	}
	resumeValues = make([]any, 0, len(rv))
	for _, value := range rv {
		resumeValues = append(resumeValues, value.Value())
	}
	return
}

func unwrapCommandUpdate(update map[string]Dynamic) map[string]any {
	if len(update) == 0 {
		return nil
	}
	out := make(map[string]any, len(update))
	for k, v := range update {
		out[k] = v.Value()
	}
	return out
}

// extractCommandFromOutput checks if a node's return value is a *Command or
// Command (value type), and returns (cmd, true) if so.
func extractCommandFromOutput(output any) (*Command, bool) {
	if output == nil {
		return nil, false
	}
	if cmd, ok := output.(*Command); ok {
		return cmd, true
	}
	if cmd, ok := output.(Command); ok {
		return &cmd, true
	}
	return nil, false
}
