package prebuilt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/checkpoint"
	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/graph"
	"github.com/google/uuid"
)

const (
	agentNodeName     = "agent"
	toolsNodeName     = "tools"
	defaultCheckpoint = "prebuilt/react"
	remainingStepsMsg = "Sorry, need more steps to process this request."
)

// Message is a Go-native agent message that supports model tool calls.
type Message struct {
	ID         string     `json:"id,omitempty"`
	Role       string     `json:"role"`
	Content    string     `json:"content"`
	Name       string     `json:"name,omitempty"`
	ToolCallID string     `json:"tool_call_id,omitempty"`
	ToolCalls  []ToolCall `json:"tool_calls,omitempty"`
}

// AgentState is the default state envelope used by ReactAgent.
type AgentState struct {
	Messages           []Message      `json:"messages"`
	RemainingSteps     int            `json:"remaining_steps,omitempty"`
	StructuredResponse any            `json:"structured_response,omitempty"`
	Values             map[string]any `json:"values,omitempty"`
}

// ModelResponse is the assistant turn produced by an AgentModel.
type ModelResponse struct {
	Content   string
	ToolCalls []ToolCall
	Name      string
}

// AgentModel is the model contract used by CreateReactAgent.
type AgentModel interface {
	Generate(ctx context.Context, messages []Message) (ModelResponse, error)
}

// StructuredOutputModel supports response-format generation.
type StructuredOutputModel interface {
	GenerateStructured(ctx context.Context, messages []Message, schema any) (any, error)
}

// ToolBindingModel exposes already-bound model tool names.
type ToolBindingModel interface {
	BoundToolNames() []string
}

// ToolBinderModel allows models to bind provided tools when needed.
type ToolBinderModel interface {
	BindTools(tools []Tool) (AgentModel, error)
}

// AgentModelSelector resolves a model at runtime from state/runtime context.
type AgentModelSelector func(ctx context.Context, state AgentState, runtime AgentRuntime) (AgentModel, error)

// PromptRunnable supplies model input messages from state/runtime.
type PromptRunnable interface {
	Prompt(ctx context.Context, state AgentState, runtime AgentRuntime) ([]Message, error)
}

// PromptFunc supplies model input messages from state/runtime.
type PromptFunc func(ctx context.Context, state AgentState, runtime AgentRuntime) ([]Message, error)

// PreModelHookResult is the output from a pre-model hook.
type PreModelHookResult struct {
	State            AgentState
	LLMInputMessages []Message
}

// PreModelHook runs before model invocation.
type PreModelHook func(ctx context.Context, state AgentState, runtime AgentRuntime) (PreModelHookResult, error)

// PostModelHook runs after model invocation and can update state.
type PostModelHook func(ctx context.Context, state AgentState, response ModelResponse, runtime AgentRuntime) (AgentState, error)

// AgentResponseFormat configures structured response generation.
type AgentResponseFormat struct {
	Prompt string
	Schema any
}

// AgentStateSchema translates custom state shapes to/from AgentState.
type AgentStateSchema interface {
	DecodeState(state any) (AgentState, error)
	EncodeState(state AgentState) (any, error)
}

// AgentContextSchema validates/transforms runtime context values.
type AgentContextSchema interface {
	DecodeContext(context any) (any, error)
}

// AgentRuntime carries invocation-scoped context passed to hooks/selectors.
type AgentRuntime struct {
	Context      any
	Store        graph.Store
	ThreadID     string
	CheckpointID string
	CheckpointNS string
	Metadata     map[string]any
}

// ReactAgentVersion controls tool execution strategy.
type ReactAgentVersion string

const (
	// ReactAgentVersionV1 executes all tool calls in one tool-node run.
	ReactAgentVersionV1 ReactAgentVersion = "v1"
	// ReactAgentVersionV2 executes each tool call as a fan-out run.
	ReactAgentVersionV2 ReactAgentVersion = "v2"
)

// PendingStage identifies where execution was interrupted.
type PendingStage string

const (
	PendingStageBeforeAgent PendingStage = "before_agent"
	PendingStageAfterAgent  PendingStage = "after_agent"
	PendingStageBeforeTools PendingStage = "before_tools"
	PendingStageAfterTools  PendingStage = "after_tools"
)

// AgentResult is the invoke result including state and optional interrupts.
type AgentResult struct {
	State              AgentState
	Interrupts         []graph.Interrupt
	Pending            *PendingToolCalls
	StructuredResponse any
	Checkpoint         *checkpoint.Config
}

// AgentStateResult returns custom-schema state from InvokeState/ResumeState.
type AgentStateResult struct {
	State              any
	Interrupts         []graph.Interrupt
	Pending            *PendingToolCalls
	StructuredResponse any
	Checkpoint         *checkpoint.Config
}

// AgentAsyncResult is delivered by async invoke/resume helpers.
type AgentAsyncResult struct {
	Result AgentResult
	Err    error
}

// PendingToolCalls holds deferred tool calls awaiting human responses.
type PendingToolCalls struct {
	State AgentState
	Calls []ToolCall
	Stage PendingStage
}

// ReactAgent executes a ReAct-style model/tool loop.
type ReactAgent struct {
	model            AgentModel
	modelSelector    AgentModelSelector
	toolNode         *ToolNode
	tools            []Tool
	validationNode   *ValidationNode
	maxSteps         int
	name             string
	version          ReactAgentVersion
	promptString     string
	promptMessage    *Message
	promptFunc       PromptFunc
	promptRunnable   PromptRunnable
	responseFormat   *AgentResponseFormat
	preModelHook     PreModelHook
	postModelHook    PostModelHook
	stateSchema      AgentStateSchema
	contextSchema    AgentContextSchema
	store            graph.Store
	checkpointer     checkpoint.Saver
	checkpointNS     string
	interruptBefore  map[string]struct{}
	interruptAfter   map[string]struct{}
	interruptCfg     HumanInterruptConfig
	interruptDesc    string
	constructionErrs error
}

// ReactAgentOption configures CreateReactAgent.
type ReactAgentOption func(*ReactAgent)

type defaultStateSchema struct{}

func (defaultStateSchema) DecodeState(state any) (AgentState, error) {
	s, ok := state.(AgentState)
	if !ok {
		var decoded AgentState
		b, err := json.Marshal(state)
		if err != nil {
			return AgentState{}, fmt.Errorf("prebuilt: default state schema expects AgentState-compatible value, got %T", state)
		}
		if err := json.Unmarshal(b, &decoded); err != nil {
			return AgentState{}, fmt.Errorf("prebuilt: failed to decode AgentState from %T: %w", state, err)
		}
		return cloneState(decoded), nil
	}
	return cloneState(s), nil
}

func (defaultStateSchema) EncodeState(state AgentState) (any, error) {
	return cloneState(state), nil
}

type passthroughContextSchema struct{}

func (passthroughContextSchema) DecodeContext(value any) (any, error) {
	return value, nil
}

// AgentStateSchemaFunc adapts decode/encode functions into AgentStateSchema.
type AgentStateSchemaFunc struct {
	Decode func(any) (AgentState, error)
	Encode func(AgentState) (any, error)
}

// DecodeState decodes a custom state value.
func (f AgentStateSchemaFunc) DecodeState(state any) (AgentState, error) {
	if f.Decode == nil {
		return AgentState{}, errors.New("prebuilt: state schema decode function is nil")
	}
	return f.Decode(state)
}

// EncodeState encodes AgentState into a custom state value.
func (f AgentStateSchemaFunc) EncodeState(state AgentState) (any, error) {
	if f.Encode == nil {
		return nil, errors.New("prebuilt: state schema encode function is nil")
	}
	return f.Encode(state)
}

// AgentContextSchemaFunc adapts a function into AgentContextSchema.
type AgentContextSchemaFunc func(any) (any, error)

// DecodeContext decodes a custom context value.
func (f AgentContextSchemaFunc) DecodeContext(value any) (any, error) {
	if f == nil {
		return nil, errors.New("prebuilt: context schema function is nil")
	}
	return f(value)
}

// AgentInvokeOptions controls invoke/resume runtime behavior.
type AgentInvokeOptions struct {
	ThreadID     string
	CheckpointID string
	CheckpointNS string
	Context      any
	Metadata     map[string]any
}

// WithAgentMaxSteps sets the maximum loop iterations.
func WithAgentMaxSteps(maxSteps int) ReactAgentOption {
	return func(a *ReactAgent) {
		if maxSteps > 0 {
			a.maxSteps = maxSteps
		}
	}
}

// WithAgentValidation sets an optional validation node for tool calls.
func WithAgentValidation(node *ValidationNode) ReactAgentOption {
	return func(a *ReactAgent) {
		a.validationNode = node
	}
}

// WithAgentToolNodeOptions applies ToolNode options to the agent's tool node.
func WithAgentToolNodeOptions(opts ...ToolNodeOption) ReactAgentOption {
	return func(a *ReactAgent) {
		if len(opts) == 0 {
			return
		}
		a.toolNode = NewToolNode(a.tools, opts...)
	}
}

// WithInterruptBeforeTools enables interrupt-before-tools behavior.
func WithInterruptBeforeTools(cfg HumanInterruptConfig, description string) ReactAgentOption {
	return WithAgentInterruptBefore([]string{toolsNodeName}, cfg, description)
}

// WithAgentInterruptBefore interrupts before selected nodes.
func WithAgentInterruptBefore(nodes []string, cfg HumanInterruptConfig, description string) ReactAgentOption {
	return func(a *ReactAgent) {
		if err := validateInterruptNodes(nodes); err != nil {
			a.recordConstructionError(err)
			return
		}
		for _, node := range nodes {
			a.interruptBefore[node] = struct{}{}
		}
		a.interruptCfg = cfg
		a.interruptDesc = description
	}
}

// WithAgentInterruptAfter interrupts after selected nodes.
func WithAgentInterruptAfter(nodes []string, cfg HumanInterruptConfig, description string) ReactAgentOption {
	return func(a *ReactAgent) {
		if err := validateInterruptNodes(nodes); err != nil {
			a.recordConstructionError(err)
			return
		}
		for _, node := range nodes {
			a.interruptAfter[node] = struct{}{}
		}
		a.interruptCfg = cfg
		a.interruptDesc = description
	}
}

// WithAgentName sets an optional name recorded on assistant messages.
func WithAgentName(name string) ReactAgentOption {
	return func(a *ReactAgent) {
		a.name = name
	}
}

// WithAgentPrompt configures prompt behavior.
//
// Supported values:
//   - `string`
//   - `Message` (typically role=system)
//   - `PromptFunc`
//   - `PromptRunnable`
func WithAgentPrompt(prompt any) ReactAgentOption {
	return func(a *ReactAgent) {
		switch p := prompt.(type) {
		case nil:
			a.promptString = ""
			a.promptMessage = nil
			a.promptFunc = nil
			a.promptRunnable = nil
		case string:
			a.promptString = p
			a.promptMessage = nil
			a.promptFunc = nil
			a.promptRunnable = nil
		case Message:
			msg := p
			a.promptMessage = &msg
			a.promptString = ""
			a.promptFunc = nil
			a.promptRunnable = nil
		case PromptFunc:
			a.promptFunc = p
			a.promptString = ""
			a.promptMessage = nil
			a.promptRunnable = nil
		case PromptRunnable:
			a.promptRunnable = p
			a.promptString = ""
			a.promptMessage = nil
			a.promptFunc = nil
		default:
			a.recordConstructionError(fmt.Errorf("prebuilt: unsupported prompt type %T", prompt))
		}
	}
}

// WithAgentResponseFormat enables structured response generation at completion.
//
// `format` may be either a schema value (`any`) or an `AgentResponseFormat`.
func WithAgentResponseFormat(format any) ReactAgentOption {
	return func(a *ReactAgent) {
		if format == nil {
			a.responseFormat = nil
			return
		}
		switch f := format.(type) {
		case AgentResponseFormat:
			a.responseFormat = &f
		case *AgentResponseFormat:
			if f == nil {
				a.responseFormat = nil
				return
			}
			copyVal := *f
			a.responseFormat = &copyVal
		default:
			a.responseFormat = &AgentResponseFormat{Schema: format}
		}
	}
}

// WithAgentPreModelHook sets a pre-model hook.
func WithAgentPreModelHook(hook PreModelHook) ReactAgentOption {
	return func(a *ReactAgent) {
		a.preModelHook = hook
	}
}

// WithAgentPostModelHook sets a post-model hook.
func WithAgentPostModelHook(hook PostModelHook) ReactAgentOption {
	return func(a *ReactAgent) {
		a.postModelHook = hook
	}
}

// WithAgentStateSchema sets a custom state schema bridge.
func WithAgentStateSchema(schema AgentStateSchema) ReactAgentOption {
	return func(a *ReactAgent) {
		if schema == nil {
			a.recordConstructionError(errors.New("prebuilt: state schema must not be nil"))
			return
		}
		a.stateSchema = schema
	}
}

// WithAgentContextSchema sets a custom context schema bridge.
func WithAgentContextSchema(schema AgentContextSchema) ReactAgentOption {
	return func(a *ReactAgent) {
		if schema == nil {
			a.recordConstructionError(errors.New("prebuilt: context schema must not be nil"))
			return
		}
		a.contextSchema = schema
	}
}

// WithAgentCheckpointer configures checkpoint persistence for invokes/resumes.
func WithAgentCheckpointer(saver checkpoint.Saver) ReactAgentOption {
	return func(a *ReactAgent) {
		a.checkpointer = saver
	}
}

// WithAgentStore configures runtime store access for hooks/selectors.
func WithAgentStore(store graph.Store) ReactAgentOption {
	return func(a *ReactAgent) {
		a.store = store
	}
}

// WithAgentVersion selects v1 or v2 tool execution behavior.
func WithAgentVersion(version ReactAgentVersion) ReactAgentOption {
	return func(a *ReactAgent) {
		a.version = version
	}
}

// WithAgentModelSelector sets runtime model selection.
func WithAgentModelSelector(selector AgentModelSelector) ReactAgentOption {
	return func(a *ReactAgent) {
		a.modelSelector = selector
	}
}

// WithAgentCheckpointNamespace sets default checkpoint namespace.
func WithAgentCheckpointNamespace(namespace string) ReactAgentOption {
	return func(a *ReactAgent) {
		if strings.TrimSpace(namespace) == "" {
			a.recordConstructionError(errors.New("prebuilt: checkpoint namespace must not be empty"))
			return
		}
		a.checkpointNS = namespace
	}
}

// CreateReactAgent constructs a ReAct-style agent around a model and tools.
func CreateReactAgent(model AgentModel, tools []Tool, opts ...ReactAgentOption) *ReactAgent {
	a := &ReactAgent{
		model:           model,
		toolNode:        NewToolNode(tools),
		tools:           cloneTools(tools),
		maxSteps:        25,
		version:         ReactAgentVersionV2,
		stateSchema:     defaultStateSchema{},
		contextSchema:   passthroughContextSchema{},
		checkpointNS:    defaultCheckpoint,
		interruptBefore: map[string]struct{}{},
		interruptAfter:  map[string]struct{}{},
	}
	for _, opt := range opts {
		opt(a)
	}
	if a.model == nil && a.modelSelector == nil {
		a.recordConstructionError(errors.New("prebuilt: model must not be nil unless model selector is provided"))
	}
	if a.version != ReactAgentVersionV1 && a.version != ReactAgentVersionV2 {
		a.recordConstructionError(fmt.Errorf("prebuilt: invalid agent version %q", a.version))
	}
	if a.version == ReactAgentVersionV1 && a.postModelHook != nil {
		a.recordConstructionError(errors.New("prebuilt: post_model_hook requires version v2"))
	}
	if a.model != nil {
		boundModel, err := a.ensureModelTools(a.model)
		if err != nil {
			a.recordConstructionError(err)
		} else {
			a.model = boundModel
		}
	}
	return a
}

// Invoke runs the model/tool loop until completion or interrupt.
func (a *ReactAgent) Invoke(ctx context.Context, state AgentState) (AgentResult, error) {
	return a.InvokeWithOptions(ctx, state, AgentInvokeOptions{})
}

// InvokeWithOptions runs the model/tool loop with runtime/checkpoint options.
func (a *ReactAgent) InvokeWithOptions(
	ctx context.Context,
	state AgentState,
	options AgentInvokeOptions,
) (AgentResult, error) {
	result, err := a.InvokeStateWithOptions(ctx, state, options)
	if err != nil {
		return AgentResult{}, err
	}
	typed, ok := result.State.(AgentState)
	if !ok {
		return AgentResult{}, fmt.Errorf("prebuilt: expected AgentState output, got %T", result.State)
	}
	return AgentResult{
		State:              typed,
		Interrupts:         result.Interrupts,
		Pending:            result.Pending,
		StructuredResponse: result.StructuredResponse,
		Checkpoint:         result.Checkpoint,
	}, nil
}

// InvokeState runs the agent with a custom state schema.
func (a *ReactAgent) InvokeState(ctx context.Context, state any) (AgentStateResult, error) {
	return a.InvokeStateWithOptions(ctx, state, AgentInvokeOptions{})
}

// InvokeStateWithOptions runs the agent with custom state and runtime options.
func (a *ReactAgent) InvokeStateWithOptions(
	ctx context.Context,
	state any,
	options AgentInvokeOptions,
) (AgentStateResult, error) {
	if a.constructionErrs != nil {
		return AgentStateResult{}, a.constructionErrs
	}
	if err := ctx.Err(); err != nil {
		return AgentStateResult{}, err
	}
	decoded, err := a.stateSchema.DecodeState(state)
	if err != nil {
		return AgentStateResult{}, err
	}
	runtime, err := a.buildRuntime(options)
	if err != nil {
		return AgentStateResult{}, err
	}
	loadedState, loadedPending, err := a.loadCheckpoint(ctx, options)
	if err != nil {
		return AgentStateResult{}, err
	}
	if loadedState != nil {
		decoded = mergeLoadedState(*loadedState, decoded)
	}

	core, err := a.invokeCore(ctx, decoded, runtime)
	if err != nil {
		return AgentStateResult{}, err
	}

	encodedState, err := a.stateSchema.EncodeState(core.state)
	if err != nil {
		return AgentStateResult{}, err
	}
	if core.pending == nil && loadedPending != nil && len(core.interrupts) == 0 {
		core.pending = loadedPending
	}
	checkpointCfg, err := a.persistCheckpoint(ctx, options, core.state, core.pending)
	if err != nil {
		return AgentStateResult{}, err
	}

	return AgentStateResult{
		State:              encodedState,
		Interrupts:         core.interrupts,
		Pending:            core.pending,
		StructuredResponse: core.state.StructuredResponse,
		Checkpoint:         checkpointCfg,
	}, nil
}

// InvokeAsync runs Invoke in a background goroutine.
func (a *ReactAgent) InvokeAsync(ctx context.Context, state AgentState) <-chan AgentAsyncResult {
	out := make(chan AgentAsyncResult, 1)
	go func() {
		defer close(out)
		result, err := a.Invoke(ctx, state)
		out <- AgentAsyncResult{Result: result, Err: err}
	}()
	return out
}

// Resume continues an interrupted agent run with human responses.
func (a *ReactAgent) Resume(
	ctx context.Context,
	pending *PendingToolCalls,
	responses map[string]HumanResponse,
) (AgentResult, error) {
	return a.ResumeWithOptions(ctx, pending, responses, AgentInvokeOptions{})
}

// ResumeWithOptions continues an interrupted run with runtime/checkpoint options.
func (a *ReactAgent) ResumeWithOptions(
	ctx context.Context,
	pending *PendingToolCalls,
	responses map[string]HumanResponse,
	options AgentInvokeOptions,
) (AgentResult, error) {
	result, err := a.ResumeStateWithOptions(ctx, pending, responses, options)
	if err != nil {
		return AgentResult{}, err
	}
	typed, ok := result.State.(AgentState)
	if !ok {
		return AgentResult{}, fmt.Errorf("prebuilt: expected AgentState output, got %T", result.State)
	}
	return AgentResult{
		State:              typed,
		Interrupts:         result.Interrupts,
		Pending:            result.Pending,
		StructuredResponse: result.StructuredResponse,
		Checkpoint:         result.Checkpoint,
	}, nil
}

// ResumeStateWithOptions resumes an interrupted run with custom state schema.
func (a *ReactAgent) ResumeStateWithOptions(
	ctx context.Context,
	pending *PendingToolCalls,
	responses map[string]HumanResponse,
	options AgentInvokeOptions,
) (AgentStateResult, error) {
	if a.constructionErrs != nil {
		return AgentStateResult{}, a.constructionErrs
	}
	if pending == nil {
		if a.checkpointer != nil && options.ThreadID != "" {
			loadedState, loadedPending, err := a.loadCheckpoint(ctx, options)
			if err != nil {
				return AgentStateResult{}, err
			}
			if loadedPending != nil {
				pending = loadedPending
				if pending.State.Messages == nil && loadedState != nil {
					pending.State = *loadedState
				}
			}
		}
	}
	if pending == nil {
		return AgentStateResult{}, fmt.Errorf("prebuilt: pending tool calls must not be nil")
	}
	runtime, err := a.buildRuntime(options)
	if err != nil {
		return AgentStateResult{}, err
	}
	state := cloneState(pending.State)

	switch pending.Stage {
	case PendingStageBeforeAgent:
		core, invokeErr := a.invokeCoreFrom(ctx, state, runtime, true)
		if invokeErr != nil {
			return AgentStateResult{}, invokeErr
		}
		return a.finalizeStateResult(ctx, core, options)
	case PendingStageAfterAgent, PendingStageBeforeTools:
		state, toolErr := a.applyPendingToolResponses(ctx, state, pending.Calls, responses)
		if toolErr != nil {
			return AgentStateResult{}, toolErr
		}
		if pending.Stage == PendingStageAfterAgent && a.shouldInterruptBefore(toolsNodeName) {
			return a.finalizeStateResult(ctx, invokeCoreResult{
				state: state,
				interrupts: []graph.Interrupt{
					buildNodeInterrupt(toolsNodeName, a.interruptCfg, a.interruptDesc),
				},
				pending: &PendingToolCalls{
					State: cloneState(state),
					Calls: cloneToolCalls(pending.Calls),
					Stage: PendingStageBeforeTools,
				},
			}, options)
		}
		if shouldReturnDirectFromHistory(a.toolNode, state.Messages) {
			return a.finalizeStateResult(ctx, invokeCoreResult{state: state}, options)
		}
		state.RemainingSteps = max(1, state.RemainingSteps-1)
		core, invokeErr := a.invokeCore(ctx, state, runtime)
		if invokeErr != nil {
			return AgentStateResult{}, invokeErr
		}
		return a.finalizeStateResult(ctx, core, options)
	case PendingStageAfterTools:
		if shouldReturnDirectFromHistory(a.toolNode, state.Messages) {
			return a.finalizeStateResult(ctx, invokeCoreResult{state: state}, options)
		}
		state.RemainingSteps = max(1, state.RemainingSteps-1)
		core, invokeErr := a.invokeCore(ctx, state, runtime)
		if invokeErr != nil {
			return AgentStateResult{}, invokeErr
		}
		return a.finalizeStateResult(ctx, core, options)
	default:
		return AgentStateResult{}, fmt.Errorf("prebuilt: unknown pending stage %q", pending.Stage)
	}
}

// ResumeAsync runs Resume in a background goroutine.
func (a *ReactAgent) ResumeAsync(
	ctx context.Context,
	pending *PendingToolCalls,
	responses map[string]HumanResponse,
) <-chan AgentAsyncResult {
	out := make(chan AgentAsyncResult, 1)
	go func() {
		defer close(out)
		result, err := a.Resume(ctx, pending, responses)
		out <- AgentAsyncResult{Result: result, Err: err}
	}()
	return out
}

type invokeCoreResult struct {
	state      AgentState
	interrupts []graph.Interrupt
	pending    *PendingToolCalls
}

func (a *ReactAgent) invokeCore(ctx context.Context, state AgentState, runtime AgentRuntime) (invokeCoreResult, error) {
	return a.invokeCoreFrom(ctx, state, runtime, false)
}

func (a *ReactAgent) invokeCoreFrom(
	ctx context.Context,
	state AgentState,
	runtime AgentRuntime,
	skipBeforeAgent bool,
) (invokeCoreResult, error) {
	if state.RemainingSteps <= 0 {
		state.RemainingSteps = a.maxSteps
	}
	current := cloneState(state)

	for step := 0; step < current.RemainingSteps; step++ {
		if err := ctx.Err(); err != nil {
			return invokeCoreResult{}, err
		}
		if (!skipBeforeAgent || step != 0) && a.shouldInterruptBefore(agentNodeName) {
			return invokeCoreResult{
				state: current,
				interrupts: []graph.Interrupt{
					buildNodeInterrupt(agentNodeName, a.interruptCfg, a.interruptDesc),
				},
				pending: &PendingToolCalls{State: cloneState(current), Stage: PendingStageBeforeAgent},
			}, nil
		}

		modelInputState := cloneState(current)
		llmInput := cloneMessages(current.Messages)
		if a.preModelHook != nil {
			hookOut, err := a.preModelHook(ctx, cloneState(current), runtime)
			if err != nil {
				return invokeCoreResult{}, err
			}
			modelInputState = cloneState(hookOut.State)
			current = cloneState(hookOut.State)
			if len(hookOut.LLMInputMessages) > 0 {
				llmInput = cloneMessages(hookOut.LLMInputMessages)
			} else {
				llmInput = cloneMessages(current.Messages)
			}
		}
		promptedInput, err := a.applyPrompt(ctx, modelInputState, runtime, llmInput)
		if err != nil {
			return invokeCoreResult{}, err
		}
		if err := validateChatHistory(promptedInput); err != nil {
			return invokeCoreResult{}, err
		}

		model, err := a.resolveModel(ctx, modelInputState, runtime)
		if err != nil {
			return invokeCoreResult{}, err
		}
		resp, err := model.Generate(ctx, promptedInput)
		if err != nil {
			return invokeCoreResult{}, err
		}
		assistant := Message{Role: "assistant", Content: resp.Content, Name: resp.Name, ToolCalls: cloneToolCalls(resp.ToolCalls)}
		if assistant.Name == "" {
			assistant.Name = a.name
		}
		current.Messages = append(current.Messages, assistant)

		if a.postModelHook != nil {
			updated, hookErr := a.postModelHook(ctx, cloneState(current), resp, runtime)
			if hookErr != nil {
				return invokeCoreResult{}, hookErr
			}
			current = cloneState(updated)
		}

		pendingCalls := pendingToolCalls(resp.ToolCalls, current.Messages)
		if a.shouldInterruptAfter(agentNodeName) {
			return invokeCoreResult{
				state: current,
				interrupts: []graph.Interrupt{
					buildNodeInterrupt(agentNodeName, a.interruptCfg, a.interruptDesc),
				},
				pending: &PendingToolCalls{
					State: cloneState(current),
					Calls: cloneToolCalls(pendingCalls),
					Stage: PendingStageAfterAgent,
				},
			}, nil
		}

		if len(pendingCalls) == 0 {
			if err := a.generateStructuredResponse(ctx, &current, runtime); err != nil {
				return invokeCoreResult{}, err
			}
			return invokeCoreResult{state: current}, nil
		}

		if needsMoreSteps(current.RemainingSteps-step, pendingCalls) {
			current.Messages[len(current.Messages)-1] = Message{Role: "assistant", Name: assistant.Name, Content: remainingStepsMsg}
			if err := a.generateStructuredResponse(ctx, &current, runtime); err != nil {
				return invokeCoreResult{}, err
			}
			return invokeCoreResult{state: current}, nil
		}

		if a.validationNode != nil {
			validationMessages, err := a.validationNode.Validate(ctx, pendingCalls)
			if err != nil {
				return invokeCoreResult{}, err
			}
			if hasValidationErrors(validationMessages) {
				current.Messages = append(current.Messages, toolMessagesToMessages(validationMessages)...)
				continue
			}
		}

		if a.shouldInterruptBefore(toolsNodeName) {
			interrupts := make([]graph.Interrupt, 0, len(pendingCalls))
			for _, call := range pendingCalls {
				interrupts = append(interrupts, BuildToolInterrupt(call, a.interruptCfg, a.interruptDesc))
			}
			if len(interrupts) == 0 {
				interrupts = append(interrupts, buildNodeInterrupt(toolsNodeName, a.interruptCfg, a.interruptDesc))
			}
			return invokeCoreResult{
				state:      current,
				interrupts: interrupts,
				pending: &PendingToolCalls{
					State: cloneState(current),
					Calls: cloneToolCalls(pendingCalls),
					Stage: PendingStageBeforeTools,
				},
			}, nil
		}

		updatedState, err := a.executeToolCalls(ctx, current, pendingCalls)
		if err != nil {
			return invokeCoreResult{}, err
		}
		current = updatedState

		if a.shouldInterruptAfter(toolsNodeName) {
			return invokeCoreResult{
				state: current,
				interrupts: []graph.Interrupt{
					buildNodeInterrupt(toolsNodeName, a.interruptCfg, a.interruptDesc),
				},
				pending: &PendingToolCalls{
					State: cloneState(current),
					Stage: PendingStageAfterTools,
				},
			}, nil
		}

		if anyReturnDirect(a.toolNode, pendingCalls) {
			return invokeCoreResult{state: current}, nil
		}
	}

	if err := a.generateStructuredResponse(ctx, &current, runtime); err != nil {
		return invokeCoreResult{}, err
	}
	return invokeCoreResult{state: current}, nil
}

func needsMoreSteps(remainingBudget int, pendingCalls []ToolCall) bool {
	if len(pendingCalls) == 0 {
		return false
	}
	return remainingBudget < 2
}

func (a *ReactAgent) executeToolCalls(
	ctx context.Context,
	state AgentState,
	calls []ToolCall,
) (AgentState, error) {
	if len(calls) == 0 {
		return state, nil
	}
	if a.version == ReactAgentVersionV1 {
		outputs, err := a.toolNode.RunResultsWithState(ctx, calls, cloneState(state))
		if err != nil {
			return AgentState{}, err
		}
		return applyToolOutputs(state, outputs)
	}
	baseState := cloneState(state)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make([]ToolCallResult, len(calls))
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	for i := range calls {
		idx := i
		call := calls[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			outputs, err := a.toolNode.RunResultsWithState(runCtx, []ToolCall{call}, cloneState(baseState))
			if err != nil {
				cancel()
				select {
				case errCh <- err:
				default:
				}
				return
			}
			if len(outputs) != 1 {
				cancel()
				select {
				case errCh <- fmt.Errorf("prebuilt: expected single tool output for call %q, got %d", call.ID, len(outputs)):
				default:
				}
				return
			}
			results[idx] = outputs[0]
		}()
	}
	wg.Wait()
	select {
	case err := <-errCh:
		return AgentState{}, err
	default:
	}
	out := cloneState(state)
	updated, applyErr := applyToolOutputs(out, results)
	if applyErr != nil {
		return AgentState{}, applyErr
	}
	out = updated
	return out, nil
}

func applyToolOutputs(state AgentState, outputs []ToolCallResult) (AgentState, error) {
	updated := cloneState(state)
	for _, output := range outputs {
		switch {
		case output.Message != nil:
			updated.Messages = append(updated.Messages, toolMessagesToMessages([]ToolMessage{*output.Message})...)
		case output.Command != nil:
			if err := applyToolCommand(&updated, *output.Command); err != nil {
				return AgentState{}, err
			}
		default:
			return AgentState{}, errors.New("prebuilt: tool output missing message and command")
		}
	}
	return updated, nil
}

func applyToolCommand(state *AgentState, command graph.Command) error {
	if state == nil {
		return errors.New("prebuilt: state must not be nil")
	}
	if command.Graph != nil {
		if *command.Graph == graph.CommandParent {
			return &graph.ParentCommand{Command: command}
		}
		return fmt.Errorf("prebuilt: unsupported command graph target %q", *command.Graph)
	}
	if command.Update == nil {
		return nil
	}

	for key, dynamicValue := range command.Update {
		value := dynamicValue.Value()
		switch key {
		case "messages":
			messages, err := coerceCommandMessages(value)
			if err != nil {
				return err
			}
			state.Messages = mergeAgentMessages(state.Messages, messages)
		case "remaining_steps":
			steps, ok := asInt(value)
			if !ok {
				return fmt.Errorf("prebuilt: command update key %q expects int, got %T", key, value)
			}
			state.RemainingSteps = steps
		case "structured_response":
			state.StructuredResponse = value
		default:
			if state.Values == nil {
				state.Values = map[string]any{}
			}
			state.Values[key] = value
		}
	}
	return nil
}

func mergeAgentMessages(left []Message, right []Message) []Message {
	if len(right) == 0 {
		return cloneMessages(left)
	}

	merged := cloneMessages(left)
	for i := range merged {
		if merged[i].ID == "" {
			merged[i].ID = uuid.New().String()
		}
	}

	updates := cloneMessages(right)
	removeAllIdx := -1
	for i := range updates {
		if updates[i].ID == "" {
			updates[i].ID = uuid.New().String()
		}
		if isRemoveAllMessage(updates[i]) {
			removeAllIdx = i
		}
	}
	if removeAllIdx >= 0 {
		return updates[removeAllIdx+1:]
	}

	indexByID := make(map[string]int, len(merged))
	for idx, message := range merged {
		indexByID[message.ID] = idx
	}

	toRemove := make(map[string]struct{})
	for _, message := range updates {
		if message.Role == "remove" {
			toRemove[message.ID] = struct{}{}
			continue
		}
		if existingIdx, ok := indexByID[message.ID]; ok {
			merged[existingIdx] = message
			continue
		}
		indexByID[message.ID] = len(merged)
		merged = append(merged, message)
	}

	if len(toRemove) == 0 {
		return merged
	}
	out := merged[:0]
	for _, message := range merged {
		if _, remove := toRemove[message.ID]; remove {
			continue
		}
		out = append(out, message)
	}
	return out
}

func isRemoveAllMessage(message Message) bool {
	if message.Role != "remove" {
		return false
	}
	return message.ID == graph.RemoveAllMessages || message.Content == graph.RemoveAllMessages
}

func asInt(value any) (int, bool) {
	switch typed := value.(type) {
	case int:
		return typed, true
	case int8:
		return int(typed), true
	case int16:
		return int(typed), true
	case int32:
		return int(typed), true
	case int64:
		return int(typed), true
	case uint:
		return int(typed), true
	case uint8:
		return int(typed), true
	case uint16:
		return int(typed), true
	case uint32:
		return int(typed), true
	case uint64:
		return int(typed), true
	case float32:
		return int(typed), true
	case float64:
		return int(typed), true
	default:
		return 0, false
	}
}

func (a *ReactAgent) applyPendingToolResponses(
	ctx context.Context,
	state AgentState,
	calls []ToolCall,
	responses map[string]HumanResponse,
) (AgentState, error) {
	if responses == nil {
		responses = map[string]HumanResponse{}
	}
	updated := cloneState(state)
	callsToExecute := make([]ToolCall, 0, len(calls))

	for _, call := range calls {
		resp, ok := responses[call.ID]
		if !ok {
			callsToExecute = append(callsToExecute, call)
			continue
		}

		switch resp.Type {
		case HumanResponseAccept:
			callsToExecute = append(callsToExecute, call)
		case HumanResponseIgnore:
			updated.Messages = append(updated.Messages, Message{
				Role:       "tool",
				Name:       call.Name,
				ToolCallID: call.ID,
				Content:    "ignored by human",
			})
		case HumanResponseRespond:
			text, _ := responseText(resp.Args)
			updated.Messages = append(updated.Messages, Message{
				Role:       "tool",
				Name:       call.Name,
				ToolCallID: call.ID,
				Content:    text,
			})
		case HumanResponseEdit:
			if req, ok := responseEditActionRequest(resp.Args); ok {
				edited := call
				edited.Name = req.Action
				edited.Args = cloneArgs(req.Args)
				callsToExecute = append(callsToExecute, edited)
			} else {
				callsToExecute = append(callsToExecute, call)
			}
		default:
			callsToExecute = append(callsToExecute, call)
		}
	}

	if len(callsToExecute) == 0 {
		return updated, nil
	}
	return a.executeToolCalls(ctx, updated, callsToExecute)
}

func (a *ReactAgent) applyPrompt(
	ctx context.Context,
	state AgentState,
	runtime AgentRuntime,
	messages []Message,
) ([]Message, error) {
	if a.promptFunc != nil {
		return a.promptFunc(ctx, cloneState(state), runtime)
	}
	if a.promptRunnable != nil {
		return a.promptRunnable.Prompt(ctx, cloneState(state), runtime)
	}
	if a.promptMessage != nil {
		out := make([]Message, 0, len(messages)+1)
		out = append(out, *a.promptMessage)
		out = append(out, cloneMessages(messages)...)
		return out, nil
	}
	if a.promptString != "" {
		out := make([]Message, 0, len(messages)+1)
		out = append(out, Message{Role: "system", Content: a.promptString})
		out = append(out, cloneMessages(messages)...)
		return out, nil
	}
	return cloneMessages(messages), nil
}

func (a *ReactAgent) resolveModel(
	ctx context.Context,
	state AgentState,
	runtime AgentRuntime,
) (AgentModel, error) {
	model := a.model
	if a.modelSelector != nil {
		resolved, err := a.modelSelector(ctx, cloneState(state), runtime)
		if err != nil {
			return nil, err
		}
		model = resolved
	}
	if model == nil {
		return nil, errors.New("prebuilt: resolved model is nil")
	}
	return a.ensureModelTools(model)
}

func (a *ReactAgent) ensureModelTools(model AgentModel) (AgentModel, error) {
	if len(a.tools) == 0 {
		return model, nil
	}
	if bound, ok := model.(ToolBindingModel); ok {
		if err := validateBoundTools(bound.BoundToolNames(), a.tools); err != nil {
			return nil, err
		}
		return model, nil
	}
	if binder, ok := model.(ToolBinderModel); ok {
		boundModel, err := binder.BindTools(cloneTools(a.tools))
		if err != nil {
			return nil, err
		}
		if bound, ok := boundModel.(ToolBindingModel); ok {
			if err := validateBoundTools(bound.BoundToolNames(), a.tools); err != nil {
				return nil, err
			}
		}
		return boundModel, nil
	}
	return model, nil
}

func (a *ReactAgent) generateStructuredResponse(
	ctx context.Context,
	state *AgentState,
	runtime AgentRuntime,
) error {
	if a.responseFormat == nil {
		return nil
	}
	format := *a.responseFormat
	if format.Schema == nil {
		return errors.New("prebuilt: response format schema must not be nil")
	}
	model, err := a.resolveModel(ctx, *state, runtime)
	if err != nil {
		return err
	}
	structuredModel, ok := model.(StructuredOutputModel)
	if !ok {
		return fmt.Errorf("prebuilt: model %T does not support structured responses", model)
	}
	messages := cloneMessages(state.Messages)
	if format.Prompt != "" {
		messages = append([]Message{{Role: "system", Content: format.Prompt}}, messages...)
	}
	structured, err := structuredModel.GenerateStructured(ctx, messages, format.Schema)
	if err != nil {
		return err
	}
	state.StructuredResponse = structured
	return nil
}

func (a *ReactAgent) shouldInterruptBefore(node string) bool {
	_, ok := a.interruptBefore[node]
	return ok
}

func (a *ReactAgent) shouldInterruptAfter(node string) bool {
	_, ok := a.interruptAfter[node]
	return ok
}

func (a *ReactAgent) buildRuntime(options AgentInvokeOptions) (AgentRuntime, error) {
	ctxValue, err := a.contextSchema.DecodeContext(options.Context)
	if err != nil {
		return AgentRuntime{}, err
	}
	ns := options.CheckpointNS
	if ns == "" {
		ns = a.checkpointNS
	}
	return AgentRuntime{
		Context:      ctxValue,
		Store:        a.store,
		ThreadID:     options.ThreadID,
		CheckpointID: options.CheckpointID,
		CheckpointNS: ns,
		Metadata:     cloneAnyMap(options.Metadata),
	}, nil
}

func (a *ReactAgent) loadCheckpoint(
	ctx context.Context,
	options AgentInvokeOptions,
) (*AgentState, *PendingToolCalls, error) {
	if a.checkpointer == nil || strings.TrimSpace(options.ThreadID) == "" {
		return nil, nil, nil
	}
	ns := options.CheckpointNS
	if ns == "" {
		ns = a.checkpointNS
	}
	tuple, err := a.checkpointer.GetTuple(ctx, &checkpoint.Config{
		ThreadID:     options.ThreadID,
		CheckpointID: options.CheckpointID,
		CheckpointNS: ns,
	})
	if err != nil {
		return nil, nil, err
	}
	if tuple == nil || tuple.Checkpoint == nil {
		return nil, nil, nil
	}
	var loadedState *AgentState
	if rawState, ok := tuple.Checkpoint.ChannelValues["agent_state"]; ok {
		decoded, err := a.stateSchema.DecodeState(rawState)
		if err != nil {
			return nil, nil, err
		}
		loadedState = &decoded
	}
	var loadedPending *PendingToolCalls
	if rawPending, ok := tuple.Checkpoint.ChannelValues["pending_tool_calls"]; ok {
		decoded, err := decodePendingToolCalls(rawPending)
		if err != nil {
			return nil, nil, err
		}
		loadedPending = decoded
	}
	return loadedState, loadedPending, nil
}

func (a *ReactAgent) persistCheckpoint(
	ctx context.Context,
	options AgentInvokeOptions,
	state AgentState,
	pending *PendingToolCalls,
) (*checkpoint.Config, error) {
	if a.checkpointer == nil || strings.TrimSpace(options.ThreadID) == "" {
		return nil, nil
	}
	ns := options.CheckpointNS
	if ns == "" {
		ns = a.checkpointNS
	}
	cp := checkpoint.EmptyCheckpoint(newAgentCheckpointID())
	cp.ChannelValues["agent_state"] = cloneState(state)
	if pending != nil {
		cp.ChannelValues["pending_tool_calls"] = clonePending(*pending)
	}
	if state.StructuredResponse != nil {
		cp.ChannelValues["structured_response"] = state.StructuredResponse
	}
	meta := &checkpoint.CheckpointMetadata{
		Source: "loop",
		Step:   len(state.Messages),
	}
	if runID, ok := options.Metadata["run_id"].(string); ok {
		meta.RunID = runID
	}
	return a.checkpointer.Put(ctx, &checkpoint.Config{
		ThreadID:     options.ThreadID,
		CheckpointID: options.CheckpointID,
		CheckpointNS: ns,
	}, cp, meta)
}

func (a *ReactAgent) finalizeStateResult(
	ctx context.Context,
	core invokeCoreResult,
	options AgentInvokeOptions,
) (AgentStateResult, error) {
	encoded, err := a.stateSchema.EncodeState(core.state)
	if err != nil {
		return AgentStateResult{}, err
	}
	checkpointCfg, err := a.persistCheckpoint(ctx, options, core.state, core.pending)
	if err != nil {
		return AgentStateResult{}, err
	}
	return AgentStateResult{
		State:              encoded,
		Interrupts:         core.interrupts,
		Pending:            core.pending,
		StructuredResponse: core.state.StructuredResponse,
		Checkpoint:         checkpointCfg,
	}, nil
}

func (a *ReactAgent) recordConstructionError(err error) {
	if err == nil {
		return
	}
	if a.constructionErrs == nil {
		a.constructionErrs = err
		return
	}
	a.constructionErrs = errors.Join(a.constructionErrs, err)
}

func hasValidationErrors(messages []ToolMessage) bool {
	for _, m := range messages {
		if m.Status == "error" {
			return true
		}
	}
	return false
}

func anyReturnDirect(node *ToolNode, calls []ToolCall) bool {
	if node == nil {
		return false
	}
	for _, call := range calls {
		if node.IsReturnDirect(call.Name) {
			return true
		}
	}
	return false
}

func shouldReturnDirectFromHistory(node *ToolNode, messages []Message) bool {
	if node == nil || len(messages) == 0 {
		return false
	}
	idx := len(messages) - 1
	for idx >= 0 {
		message := messages[idx]
		if message.Role != "tool" {
			break
		}
		if node.IsReturnDirect(message.Name) {
			return true
		}
		idx--
	}
	if idx < 0 {
		return false
	}
	assistant := messages[idx]
	if assistant.Role != "assistant" || len(assistant.ToolCalls) == 0 {
		return false
	}
	for _, call := range assistant.ToolCalls {
		if node.IsReturnDirect(call.Name) {
			return true
		}
	}
	return false
}

func validateChatHistory(messages []Message) error {
	type missingToolCall struct {
		ID   string
		Name string
	}
	if len(messages) == 0 {
		return nil
	}
	toolResultIDs := make(map[string]struct{}, len(messages))
	allCalls := make([]missingToolCall, 0)
	for _, message := range messages {
		if message.Role == "assistant" {
			for _, call := range message.ToolCalls {
				allCalls = append(allCalls, missingToolCall{ID: call.ID, Name: call.Name})
			}
			continue
		}
		if message.Role == "tool" {
			id := strings.TrimSpace(message.ToolCallID)
			if id != "" {
				toolResultIDs[id] = struct{}{}
			}
		}
	}
	if len(allCalls) == 0 {
		return nil
	}
	missing := make([]missingToolCall, 0)
	for _, call := range allCalls {
		id := strings.TrimSpace(call.ID)
		if id == "" {
			missing = append(missing, call)
			continue
		}
		if _, ok := toolResultIDs[id]; !ok {
			missing = append(missing, call)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	limit := min(3, len(missing))
	preview := make([]string, 0, limit)
	for i := 0; i < limit; i++ {
		preview = append(preview, fmt.Sprintf("{id:%q name:%q}", missing[i].ID, missing[i].Name))
	}
	return fmt.Errorf(
		"prebuilt: found assistant tool calls without matching tool messages; first missing: %s",
		strings.Join(preview, ", "),
	)
}

func toolMessagesToMessages(in []ToolMessage) []Message {
	out := make([]Message, 0, len(in))
	for _, m := range in {
		content := m.Content
		if content == "" && len(m.ContentBlocks) > 0 {
			if b, err := json.Marshal(m.ContentBlocks); err == nil {
				content = string(b)
			}
		}
		out = append(out, Message{
			Role:       "tool",
			Name:       m.Name,
			ToolCallID: m.ToolCallID,
			Content:    content,
		})
	}
	return out
}

func cloneState(in AgentState) AgentState {
	out := in
	out.Messages = cloneMessages(in.Messages)
	out.Values = cloneAnyMap(in.Values)
	return out
}

func mergeLoadedState(loaded, input AgentState) AgentState {
	out := cloneState(loaded)
	if len(input.Messages) > 0 {
		out.Messages = append(out.Messages, cloneMessages(input.Messages)...)
	}
	if input.RemainingSteps > 0 {
		out.RemainingSteps = input.RemainingSteps
	}
	if input.StructuredResponse != nil {
		out.StructuredResponse = input.StructuredResponse
	}
	if len(input.Values) > 0 {
		if out.Values == nil {
			out.Values = map[string]any{}
		}
		for key, value := range input.Values {
			out.Values[key] = value
		}
	}
	return out
}

func cloneMessages(in []Message) []Message {
	out := make([]Message, len(in))
	copy(out, in)
	for i := range out {
		out[i].ToolCalls = cloneToolCalls(out[i].ToolCalls)
	}
	return out
}

func cloneToolCalls(in []ToolCall) []ToolCall {
	out := make([]ToolCall, len(in))
	copy(out, in)
	for i := range out {
		out[i].Args = cloneArgs(out[i].Args)
	}
	return out
}

func cloneTools(in []Tool) []Tool {
	out := make([]Tool, len(in))
	copy(out, in)
	return out
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

func clonePending(in PendingToolCalls) *PendingToolCalls {
	copyVal := &PendingToolCalls{
		State: cloneState(in.State),
		Calls: cloneToolCalls(in.Calls),
		Stage: in.Stage,
	}
	return copyVal
}

func decodePendingToolCalls(raw any) (*PendingToolCalls, error) {
	pending, ok := raw.(PendingToolCalls)
	if ok {
		return clonePending(pending), nil
	}
	pendingPtr, ok := raw.(*PendingToolCalls)
	if ok {
		if pendingPtr == nil {
			return nil, nil
		}
		return clonePending(*pendingPtr), nil
	}
	var decoded PendingToolCalls
	b, err := json.Marshal(raw)
	if err == nil {
		if unmarshalErr := json.Unmarshal(b, &decoded); unmarshalErr == nil {
			return clonePending(decoded), nil
		}
	}
	return nil, fmt.Errorf("prebuilt: cannot decode pending tool calls from %T", raw)
}

func pendingToolCalls(calls []ToolCall, messages []Message) []ToolCall {
	if len(calls) == 0 {
		return nil
	}
	toolResults := make(map[string]struct{}, len(messages))
	for _, m := range messages {
		if m.Role != "tool" || strings.TrimSpace(m.ToolCallID) == "" {
			continue
		}
		toolResults[m.ToolCallID] = struct{}{}
	}
	out := make([]ToolCall, 0, len(calls))
	for _, call := range calls {
		if _, ok := toolResults[call.ID]; ok {
			continue
		}
		out = append(out, call)
	}
	return out
}

func buildNodeInterrupt(node string, cfg HumanInterruptConfig, description string) graph.Interrupt {
	request := HumanInterrupt{
		ActionRequest: ActionRequest{Action: node, Args: map[string]any{}},
		Config:        cfg,
		Description:   description,
	}
	return graph.NewInterrupt(graph.Dyn(request), uuid.New().String())
}

func validateInterruptNodes(nodes []string) error {
	for _, node := range nodes {
		if node != agentNodeName && node != toolsNodeName {
			return fmt.Errorf("prebuilt: invalid interrupt node %q (expected %q or %q)", node, agentNodeName, toolsNodeName)
		}
	}
	return nil
}

func validateBoundTools(boundNames []string, tools []Tool) error {
	if len(boundNames) == 0 {
		return nil
	}
	expected := make([]string, 0, len(tools))
	expectedSet := make(map[string]struct{}, len(tools))
	for _, tool := range tools {
		if tool == nil {
			continue
		}
		name := tool.Name()
		expected = append(expected, name)
		expectedSet[name] = struct{}{}
	}
	if len(boundNames) != len(expected) {
		return fmt.Errorf(
			"prebuilt: number of bound tools and configured tools must match (got %d bound, expected %d)",
			len(boundNames),
			len(expected),
		)
	}
	missing := make([]string, 0)
	for _, name := range expected {
		if !slices.Contains(boundNames, name) {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("prebuilt: missing tools in model binding: %v", missing)
	}
	for _, name := range boundNames {
		if _, ok := expectedSet[name]; !ok {
			return fmt.Errorf("prebuilt: model bound unknown tool %q", name)
		}
	}
	return nil
}

func newAgentCheckpointID() string {
	return fmt.Sprintf("%s-%s", time.Now().UTC().Format("20060102T150405.000000000Z"), uuid.New().String())
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
