package prebuilt

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/graph"
	"github.com/google/uuid"
)

// HumanInterruptConfig controls which human actions are allowed.
type HumanInterruptConfig struct {
	AllowIgnore  bool `json:"allow_ignore"`
	AllowRespond bool `json:"allow_respond"`
	AllowEdit    bool `json:"allow_edit"`
	AllowAccept  bool `json:"allow_accept"`
}

// ActionRequest is a request for human action.
type ActionRequest struct {
	Args   map[string]any `json:"args"`
	Action string         `json:"action"`
}

// HumanInterrupt is an interrupt payload requesting human intervention.
type HumanInterrupt struct {
	Description   string               `json:"description,omitempty"`
	ActionRequest ActionRequest        `json:"action_request"`
	Config        HumanInterruptConfig `json:"config"`
}

// Interrupt pauses graph execution from inside tools or hooks.
//
// This is a prebuilt-package convenience wrapper around `graph.NodeInterrupt`.
func Interrupt(ctx context.Context, value any) any {
	return graph.NodeInterrupt(ctx, graph.Dyn(value)).Value()
}

// HumanResponseType lists allowed human response variants.
type HumanResponseType string

const (
	HumanResponseAccept  HumanResponseType = "accept"
	HumanResponseIgnore  HumanResponseType = "ignore"
	HumanResponseRespond HumanResponseType = "response"
	HumanResponseEdit    HumanResponseType = "edit"
)

// HumanResponseArgs is a typed union for human-response payload variants.
type HumanResponseArgs interface {
	humanResponseArgs()
}

// NoHumanResponseArgs represents empty args (accept/ignore).
type NoHumanResponseArgs struct{}

func (NoHumanResponseArgs) humanResponseArgs() {}

// HumanResponseText represents a free-form human text response.
type HumanResponseText string

func (HumanResponseText) humanResponseArgs() {}

// HumanResponseActionRequest represents an edited action request.
type HumanResponseActionRequest struct {
	ActionRequest ActionRequest
}

func (HumanResponseActionRequest) humanResponseArgs() {}

// HumanResponse is the response payload supplied when resuming.
type HumanResponse struct {
	Args HumanResponseArgs `json:"args"`
	Type HumanResponseType `json:"type"`
}

// AcceptHumanResponse creates an accept response.
func AcceptHumanResponse() HumanResponse {
	return HumanResponse{Type: HumanResponseAccept, Args: NoHumanResponseArgs{}}
}

// IgnoreHumanResponse creates an ignore response.
func IgnoreHumanResponse() HumanResponse {
	return HumanResponse{Type: HumanResponseIgnore, Args: NoHumanResponseArgs{}}
}

// RespondHumanResponse creates a text response.
func RespondHumanResponse(text string) HumanResponse {
	return HumanResponse{Type: HumanResponseRespond, Args: HumanResponseText(text)}
}

// EditHumanResponse creates an edit response.
func EditHumanResponse(request ActionRequest) HumanResponse {
	return HumanResponse{Type: HumanResponseEdit, Args: HumanResponseActionRequest{ActionRequest: request}}
}

func (r HumanResponse) MarshalJSON() ([]byte, error) {
	type wire struct {
		Args any               `json:"args,omitempty"`
		Type HumanResponseType `json:"type"`
	}
	out := wire{Type: r.Type}
	switch v := r.Args.(type) {
	case nil:
		out.Args = nil
	case NoHumanResponseArgs:
		out.Args = nil
	case *NoHumanResponseArgs:
		out.Args = nil
	case HumanResponseText:
		out.Args = string(v)
	case *HumanResponseText:
		if v != nil {
			out.Args = string(*v)
		}
	case HumanResponseActionRequest:
		out.Args = v.ActionRequest
	case *HumanResponseActionRequest:
		if v != nil {
			out.Args = v.ActionRequest
		}
	default:
		return nil, fmt.Errorf("prebuilt: unsupported human response args type %T", r.Args)
	}
	return json.Marshal(out)
}

func (r *HumanResponse) UnmarshalJSON(data []byte) error {
	type wire struct {
		Type HumanResponseType `json:"type"`
		Args json.RawMessage   `json:"args"`
	}
	var in wire
	if err := json.Unmarshal(data, &in); err != nil {
		return err
	}
	r.Type = in.Type
	r.Args = NoHumanResponseArgs{}

	trimmed := strings.TrimSpace(string(in.Args))
	if trimmed == "" || trimmed == "null" {
		return nil
	}

	switch in.Type {
	case HumanResponseRespond:
		var text string
		if err := json.Unmarshal(in.Args, &text); err != nil {
			return fmt.Errorf("prebuilt: response args must be string for type %q: %w", in.Type, err)
		}
		r.Args = HumanResponseText(text)
		return nil
	case HumanResponseEdit:
		var req ActionRequest
		if err := json.Unmarshal(in.Args, &req); err != nil {
			return fmt.Errorf("prebuilt: response args must be action_request object for type %q: %w", in.Type, err)
		}
		r.Args = HumanResponseActionRequest{ActionRequest: req}
		return nil
	default:
		// Accept/ignore (and unknown future values) treat args as optional.
		return nil
	}
}

func responseText(args HumanResponseArgs) (string, bool) {
	switch v := args.(type) {
	case HumanResponseText:
		return string(v), true
	case *HumanResponseText:
		if v == nil {
			return "", false
		}
		return string(*v), true
	default:
		return "", false
	}
}

func responseEditActionRequest(args HumanResponseArgs) (ActionRequest, bool) {
	switch v := args.(type) {
	case HumanResponseActionRequest:
		return v.ActionRequest, true
	case *HumanResponseActionRequest:
		if v == nil {
			return ActionRequest{}, false
		}
		return v.ActionRequest, true
	default:
		return ActionRequest{}, false
	}
}

// BuildToolInterrupt creates a graph.Interrupt for a tool call requiring human approval.
func BuildToolInterrupt(call ToolCall, cfg HumanInterruptConfig, description string) graph.Interrupt {
	request := HumanInterrupt{
		ActionRequest: ActionRequest{Action: call.Name, Args: cloneArgs(call.Args)},
		Config:        cfg,
		Description:   description,
	}
	return graph.NewInterrupt(graph.Dyn(request), uuid.New().String())
}
