package graph

import (
	"context"
	"fmt"
	"time"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/sdk"
)

const remoteGraphDefaultWaitInterval = 100 * time.Millisecond

// RemoteGraphOptions configures a RemoteGraph adapter.
type RemoteGraphOptions struct {
	// ThreadID is the default thread for remote state and run operations.
	ThreadID string

	// AssistantID is the default assistant used when starting runs.
	AssistantID string

	// RunCreate sets default run creation options used by Invoke.
	RunCreate sdk.RunCreateRequest

	// RunStream sets default run stream options used by Stream APIs.
	RunStream sdk.RunStreamRequest

	// WaitInterval controls Invoke polling cadence for run completion.
	WaitInterval time.Duration
}

// RemoteGraph provides a graph-like adapter backed by a remote sdk.Client.
//
// It mirrors key CompiledStateGraph operations while delegating execution and
// state persistence to a remote HTTP server.
type RemoteGraph struct {
	client *sdk.Client
	opts   RemoteGraphOptions
}

// RemoteGraphUpdateStateOptions configures remote thread state updates.
type RemoteGraphUpdateStateOptions struct {
	// AsNode attributes the update to the given node name.
	AsNode string
}

// NewRemoteGraph creates a RemoteGraph adapter.
func NewRemoteGraph(client *sdk.Client, opts RemoteGraphOptions) (*RemoteGraph, error) {
	if client == nil {
		return nil, fmt.Errorf("remote graph requires a non-nil sdk client")
	}
	if opts.WaitInterval <= 0 {
		opts.WaitInterval = remoteGraphDefaultWaitInterval
	}
	return &RemoteGraph{client: client, opts: opts}, nil
}

// Invoke executes one remote run and waits for completion.
func (g *RemoteGraph) Invoke(ctx context.Context, input map[string]any) (GraphOutput, error) {
	threadID := g.resolveRunThreadID(ctx)

	req := cloneRunCreateRequest(g.opts.RunCreate)
	req.Input = cloneAnyMap(input)
	req.AssistantID = g.resolveAssistantID(req.AssistantID)
	g.applyGraphConfigToRunCreate(ctx, &req)

	run, err := g.client.CreateRunWithOptions(ctx, threadID, req)
	if err != nil {
		return GraphOutput{}, err
	}
	if run.ThreadID != "" {
		threadID = run.ThreadID
	}

	final, err := g.client.WaitRun(ctx, threadID, run.ID, g.opts.WaitInterval)
	if err != nil {
		return GraphOutput{}, err
	}
	if final.Status == sdk.RunStatusError || final.Status == sdk.RunStatusInterrupted || final.Status == sdk.RunStatusTimeout {
		if final.Error != "" {
			return GraphOutput{}, fmt.Errorf("remote run failed: %s", final.Error)
		}
		return GraphOutput{}, fmt.Errorf("remote run failed")
	}

	interrupts, output := extractInterrupts(final.Output)
	return GraphOutput{Value: output, Interrupts: interrupts}, nil
}

// Stream executes one remote run and emits a single stream mode.
func (g *RemoteGraph) Stream(ctx context.Context, input map[string]any, mode StreamMode) <-chan StreamPart {
	return g.StreamDuplex(ctx, input, mode)
}

// StreamDuplex executes one remote run and emits all requested stream modes.
func (g *RemoteGraph) StreamDuplex(ctx context.Context, input map[string]any, modes ...StreamMode) <-chan StreamPart {
	out := make(chan StreamPart, 32)

	typedParts, typedErrs := g.StreamTyped(ctx, input, modes...)

	go func() {
		defer close(out)
		for part := range typedParts {
			out <- typedToGraphStreamPart(part)
		}
		for err := range typedErrs {
			if err != nil {
				out <- StreamPart{Err: err}
				return
			}
		}
	}()

	return out
}

// StreamTyped executes one remote run and returns sdk typed stream parts.
func (g *RemoteGraph) StreamTyped(
	ctx context.Context,
	input map[string]any,
	modes ...StreamMode,
) (parts <-chan sdk.StreamPartV2, errs <-chan error) {
	threadID := g.resolveRunThreadID(ctx)

	req := cloneRunStreamRequest(g.opts.RunStream)
	req.Input = cloneAnyMap(input)
	req.AssistantID = g.resolveAssistantID(req.AssistantID)
	g.applyGraphConfigToRunStream(ctx, &req)

	if len(modes) > 0 {
		req.StreamMode = streamModeStrings(modes...)
	} else if len(req.StreamMode) == 0 {
		req.StreamMode = []string{string(StreamModeValues)}
	}

	return g.client.StreamRunWithOptionsTyped(ctx, threadID, req)
}

// GetState returns the latest thread state as a StateSnapshot.
func (g *RemoteGraph) GetState(ctx context.Context) (*StateSnapshot, error) {
	threadID, err := g.resolveThreadID(ctx)
	if err != nil {
		return nil, err
	}

	state, err := g.client.GetThreadState(ctx, threadID)
	if err != nil {
		return nil, err
	}

	snap := &StateSnapshot{
		Values:   state.Values,
		Config:   &Config{ThreadID: state.ThreadID, CheckpointID: state.CheckpointID},
		Metadata: state.Metadata,
	}
	return snap, nil
}

// UpdateState applies an out-of-band thread state update.
func (g *RemoteGraph) UpdateState(ctx context.Context, values map[string]any) error {
	return g.UpdateStateWithOptions(ctx, values, RemoteGraphUpdateStateOptions{})
}

// UpdateStateWithOptions applies an out-of-band thread state update.
func (g *RemoteGraph) UpdateStateWithOptions(ctx context.Context, values map[string]any, options RemoteGraphUpdateStateOptions) error {
	threadID, err := g.resolveThreadID(ctx)
	if err != nil {
		return err
	}

	runtimeCfg := GetConfig(ctx)
	req := sdk.ThreadUpdateStateRequest{Values: cloneAnyMap(values)}
	if runtimeCfg.CheckpointID != "" {
		req.CheckpointID = runtimeCfg.CheckpointID
	}
	if options.AsNode != "" {
		req.AsNode = options.AsNode
	}

	_, err = g.client.Threads.UpdateState(ctx, threadID, req)
	return err
}

// GetGraph returns the assistant graph.
func (g *RemoteGraph) GetGraph(ctx context.Context, xray any) (map[string]any, error) {
	assistantID, err := g.resolveAssistantIDForIntrospection()
	if err != nil {
		return nil, err
	}
	return g.client.Assistants.GetGraph(ctx, assistantID, xray)
}

// GetSchemas returns the assistant graph schemas.
func (g *RemoteGraph) GetSchemas(ctx context.Context) (map[string]any, error) {
	assistantID, err := g.resolveAssistantIDForIntrospection()
	if err != nil {
		return nil, err
	}
	return g.client.Assistants.GetSchemas(ctx, assistantID)
}

// GetSubgraphs returns assistant subgraph descriptors.
func (g *RemoteGraph) GetSubgraphs(ctx context.Context, namespace string, recurse bool) (map[string]any, error) {
	assistantID, err := g.resolveAssistantIDForIntrospection()
	if err != nil {
		return nil, err
	}
	return g.client.Assistants.GetSubgraphs(ctx, assistantID, namespace, recurse)
}

// GetStateHistory returns checkpoint-backed thread snapshots (newest first).
func (g *RemoteGraph) GetStateHistory(ctx context.Context) ([]StateSnapshot, error) {
	threadID, err := g.resolveThreadID(ctx)
	if err != nil {
		return nil, err
	}

	tuples, err := g.client.ListThreadHistory(ctx, threadID)
	if err != nil {
		return nil, err
	}

	history := make([]StateSnapshot, 0, len(tuples))
	for i := range tuples {
		tuple := tuples[i]
		snap := StateSnapshot{
			Config:       graphConfigFromCheckpointConfig(tuple.Config),
			Metadata:     metadataMapFromCheckpointMetadata(tuple.Metadata),
			ParentConfig: graphConfigFromCheckpointConfig(tuple.ParentConfig),
		}
		if snap.Config == nil {
			snap.Config = &Config{ThreadID: threadID}
		}
		if tuple.Checkpoint != nil {
			snap.Values = tuple.Checkpoint.ChannelValues
			snap.CreatedAt = parseCheckpointTS(tuple.Checkpoint.TS)
		}
		history = append(history, snap)
	}

	return history, nil
}

func (g *RemoteGraph) resolveThreadID(ctx context.Context) (string, error) {
	threadID := g.resolveRunThreadID(ctx)
	if threadID != "" {
		return threadID, nil
	}
	return "", fmt.Errorf("remote graph requires Config.ThreadID or RemoteGraphOptions.ThreadID")
}

func (g *RemoteGraph) resolveRunThreadID(ctx context.Context) string {
	runtimeCfg := GetConfig(ctx)
	if runtimeCfg.ThreadID != "" {
		return runtimeCfg.ThreadID
	}
	if g.opts.ThreadID != "" {
		return g.opts.ThreadID
	}
	return ""
}

func (g *RemoteGraph) resolveAssistantID(current string) string {
	if g.opts.AssistantID != "" {
		return g.opts.AssistantID
	}
	return current
}

func (g *RemoteGraph) resolveAssistantIDForIntrospection() (string, error) {
	assistantID := g.resolveAssistantID("")
	if assistantID == "" {
		return "", fmt.Errorf("remote graph requires RemoteGraphOptions.AssistantID for assistant introspection")
	}
	return assistantID, nil
}

func (g *RemoteGraph) applyGraphConfigToRunCreate(ctx context.Context, req *sdk.RunCreateRequest) {
	cfg := GetConfig(ctx)
	if cfg.CheckpointID != "" {
		req.CheckpointID = cfg.CheckpointID
	}
	if cfg.Durability != "" {
		req.Durability = string(cfg.Durability)
	}
	if len(cfg.Metadata) > 0 {
		req.Metadata = mergeAnyMaps(req.Metadata, cfg.Metadata)
	}
}

func (g *RemoteGraph) applyGraphConfigToRunStream(ctx context.Context, req *sdk.RunStreamRequest) {
	cfg := GetConfig(ctx)
	if cfg.CheckpointID != "" {
		req.CheckpointID = cfg.CheckpointID
	}
	if cfg.Durability != "" {
		req.Durability = string(cfg.Durability)
	}
	if len(cfg.Metadata) > 0 {
		req.Metadata = mergeAnyMaps(req.Metadata, cfg.Metadata)
	}
}

func cloneRunCreateRequest(in sdk.RunCreateRequest) sdk.RunCreateRequest {
	out := in
	out.Input = cloneAnyMap(in.Input)
	out.Command = cloneAnyMap(in.Command)
	out.Metadata = cloneAnyMap(in.Metadata)
	out.Config = cloneAnyMap(in.Config)
	out.Context = cloneAnyMap(in.Context)
	out.StreamMode = append([]string(nil), in.StreamMode...)
	out.Checkpoint = cloneAnyMap(in.Checkpoint)
	return out
}

func cloneRunStreamRequest(in sdk.RunStreamRequest) sdk.RunStreamRequest {
	return cloneRunCreateRequest(sdk.RunCreateRequest(in))
}

func cloneAnyMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func mergeAnyMaps(base, overlay map[string]any) map[string]any {
	out := cloneAnyMap(base)
	if out == nil {
		out = map[string]any{}
	}
	for k, v := range overlay {
		out[k] = v
	}
	return out
}

func streamModeStrings(modes ...StreamMode) []string {
	if len(modes) == 0 {
		return nil
	}
	out := make([]string, 0, len(modes))
	for _, mode := range modes {
		if mode == "" {
			continue
		}
		out = append(out, string(mode))
	}
	return out
}

func extractInterrupts(output map[string]any) (interrupts []Interrupt, cleaned map[string]any) {
	if len(output) == 0 {
		return nil, output
	}
	intsRaw, ok := output["__interrupt__"]
	if !ok {
		return nil, output
	}

	clean := cloneAnyMap(output)
	delete(clean, "__interrupt__")
	return decodeInterrupts(intsRaw), clean
}

func decodeInterrupts(raw any) []Interrupt {
	items, ok := raw.([]any)
	if !ok {
		return nil
	}
	out := make([]Interrupt, 0, len(items))
	for _, item := range items {
		asMap, ok := item.(map[string]any)
		if !ok {
			continue
		}
		id, _ := asMap["id"].(string)
		value, hasValue := asMap["value"]
		if !hasValue {
			value = asMap
		}
		out = append(out, Interrupt{ID: id, Value: Dyn(value)})
	}
	return out
}

func typedToGraphStreamPart(part sdk.StreamPartV2) StreamPart {
	switch p := part.(type) {
	case sdk.ValuesStreamPart:
		return StreamPart{Type: StreamMode(p.Type), Namespace: append([]string(nil), p.NS...), Data: p.Data, Interrupts: decodeInterruptsFromMaps(p.Interrupts)}
	case sdk.UpdatesStreamPart:
		return StreamPart{Type: StreamMode(p.Type), Namespace: append([]string(nil), p.NS...), Data: p.Data, Interrupts: decodeInterruptsFromMaps(p.Interrupts)}
	case sdk.MessagesStreamPart:
		return StreamPart{Type: StreamMode(p.Type), Namespace: append([]string(nil), p.NS...), Data: p.Data, Interrupts: decodeInterruptsFromMaps(p.Interrupts)}
	case sdk.CustomStreamPart:
		return StreamPart{Type: StreamMode(p.Type), Namespace: append([]string(nil), p.NS...), Data: p.Data, Interrupts: decodeInterruptsFromMaps(p.Interrupts)}
	case sdk.CheckpointStreamPart:
		return StreamPart{Type: StreamMode(p.Type), Namespace: append([]string(nil), p.NS...), Data: p.Data, Interrupts: decodeInterruptsFromMaps(p.Interrupts)}
	case sdk.TasksStreamPart:
		return StreamPart{Type: StreamMode(p.Type), Namespace: append([]string(nil), p.NS...), Data: p.Data, Interrupts: decodeInterruptsFromMaps(p.Interrupts)}
	case sdk.DebugStreamPart:
		return StreamPart{Type: StreamMode(p.Type), Namespace: append([]string(nil), p.NS...), Data: p.Data, Interrupts: decodeInterruptsFromMaps(p.Interrupts)}
	case sdk.MetadataStreamPart:
		return StreamPart{Type: StreamMode(p.Type), Namespace: append([]string(nil), p.NS...), Data: p.Data, Interrupts: decodeInterruptsFromMaps(p.Interrupts)}
	case sdk.UnknownStreamPart:
		return StreamPart{Type: StreamMode(p.Type), Namespace: append([]string(nil), p.NS...), Data: p.Data, Interrupts: decodeInterruptsFromMaps(p.Interrupts)}
	default:
		return StreamPart{Err: fmt.Errorf("unsupported typed stream part %T", part)}
	}
}

func decodeInterruptsFromMaps(raw []map[string]any) []Interrupt {
	if len(raw) == 0 {
		return nil
	}
	out := make([]Interrupt, 0, len(raw))
	for _, item := range raw {
		id, _ := item["id"].(string)
		value, ok := item["value"]
		if !ok {
			value = item
		}
		out = append(out, Interrupt{ID: id, Value: Dyn(value)})
	}
	return out
}
