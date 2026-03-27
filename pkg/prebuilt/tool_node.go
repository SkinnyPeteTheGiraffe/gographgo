// Package prebuilt provides prebuilt agent and tool orchestration primitives.
package prebuilt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/graph"
)

const invalidToolNameErrorTemplate = "Error: %s is not a valid tool, try one of [%s]."

const toolInvocationErrorTemplate = "Error invoking tool '%s' with kwargs %v with error:\n %s\n Please fix the error and try again."

const (
	// InjectedStateArg is the reserved arg key for injected graph state.
	InjectedStateArg = "__state__"
	// InjectedStoreArg is the reserved arg key for injected graph store.
	InjectedStoreArg = "__store__"
	// InjectedRuntimeArg is the reserved arg key for injected tool runtime.
	InjectedRuntimeArg = "__runtime__"
)

// ToolCall is a single model-requested tool invocation.
type ToolCall struct {
	Args map[string]any `json:"args"`
	ID   string         `json:"id"`
	Name string         `json:"name"`
}

// ToolMessage is the result envelope returned by ToolNode.
type ToolMessage struct {
	ToolCallID string `json:"tool_call_id"`
	Name       string `json:"name"`
	Content    string `json:"content"`
	Status     string `json:"status,omitempty"`
	// ContentBlocks carries structured block content output when returned by tools.
	ContentBlocks []map[string]any `json:"content_blocks,omitempty"`
}

// ToolCommand allows tools to update state and direct control flow.
type ToolCommand struct {
	Graph  *string        `json:"graph,omitempty"`
	Update map[string]any `json:"update,omitempty"`
	Goto   graph.Route    `json:"goto,omitempty"`
}

// Tool defines the execution contract for a callable tool.
type Tool interface {
	Name() string
	Invoke(ctx context.Context, args map[string]any) (any, error)
}

// ToolRuntime carries invocation-scoped runtime values for tools.
type ToolRuntime struct {
	State        any
	Context      any
	Store        graph.Store
	StreamWriter func(any)
	ToolCallID   string
	Config       graph.Config
}

// RuntimeAwareTool can receive a full runtime object during invocation.
type RuntimeAwareTool interface {
	InvokeWithRuntime(ctx context.Context, args map[string]any, runtime ToolRuntime) (any, error)
}

// StateAwareTool can receive graph state directly during invocation.
type StateAwareTool interface {
	InvokeWithState(ctx context.Context, args map[string]any, state any) (any, error)
}

// StoreAwareTool can receive graph store directly during invocation.
type StoreAwareTool interface {
	InvokeWithStore(ctx context.Context, args map[string]any, store graph.Store) (any, error)
}

// ReturnDirectTool marks a tool whose output should terminate the loop.
type ReturnDirectTool interface {
	ReturnDirect() bool
}

// ToolFunc adapts a function into a Tool implementation.
type ToolFunc struct {
	fn           func(context.Context, map[string]any) (any, error)
	runtimeFn    func(context.Context, map[string]any, ToolRuntime) (any, error)
	stateFn      func(context.Context, map[string]any, any) (any, error)
	storeFn      func(context.Context, map[string]any, graph.Store) (any, error)
	name         string
	returnDirect bool
}

// NewToolFunc creates a Tool from name and function.
func NewToolFunc(name string, fn func(context.Context, map[string]any) (any, error)) Tool {
	return &ToolFunc{name: name, fn: fn}
}

// NewToolFuncWithReturnDirect creates a Tool whose output can end the agent loop.
func NewToolFuncWithReturnDirect(
	name string,
	returnDirect bool,
	fn func(context.Context, map[string]any) (any, error),
) Tool {
	return &ToolFunc{name: name, fn: fn, returnDirect: returnDirect}
}

// NewToolFuncWithRuntime creates a Tool that receives ToolRuntime directly.
func NewToolFuncWithRuntime(
	name string,
	fn func(context.Context, map[string]any, ToolRuntime) (any, error),
) Tool {
	return &ToolFunc{name: name, runtimeFn: fn}
}

// NewToolFuncWithState creates a Tool that receives graph state directly.
func NewToolFuncWithState(
	name string,
	fn func(context.Context, map[string]any, any) (any, error),
) Tool {
	return &ToolFunc{name: name, stateFn: fn}
}

// NewToolFuncWithStore creates a Tool that receives graph store directly.
func NewToolFuncWithStore(
	name string,
	fn func(context.Context, map[string]any, graph.Store) (any, error),
) Tool {
	return &ToolFunc{name: name, storeFn: fn}
}

// Name returns the tool name.
func (t *ToolFunc) Name() string {
	return t.name
}

// Invoke executes the wrapped function.
func (t *ToolFunc) Invoke(ctx context.Context, args map[string]any) (any, error) {
	if t.fn == nil {
		return nil, fmt.Errorf("tool %q has no invoke function", t.name)
	}
	return t.fn(ctx, args)
}

// InvokeWithRuntime executes the runtime-aware wrapped function.
func (t *ToolFunc) InvokeWithRuntime(ctx context.Context, args map[string]any, runtime ToolRuntime) (any, error) {
	if t.runtimeFn == nil {
		return nil, fmt.Errorf("tool %q has no runtime invoke function", t.name)
	}
	return t.runtimeFn(ctx, args, runtime)
}

// InvokeWithState executes the state-aware wrapped function.
func (t *ToolFunc) InvokeWithState(ctx context.Context, args map[string]any, state any) (any, error) {
	if t.stateFn == nil {
		return nil, fmt.Errorf("tool %q has no state invoke function", t.name)
	}
	return t.stateFn(ctx, args, state)
}

// InvokeWithStore executes the store-aware wrapped function.
func (t *ToolFunc) InvokeWithStore(ctx context.Context, args map[string]any, store graph.Store) (any, error) {
	if t.storeFn == nil {
		return nil, fmt.Errorf("tool %q has no store invoke function", t.name)
	}
	return t.storeFn(ctx, args, store)
}

// ReturnDirect indicates whether this tool should terminate agent looping.
func (t *ToolFunc) ReturnDirect() bool {
	return t.returnDirect
}

// ToolErrorHandler maps a tool execution error into ToolMessage content.
type ToolErrorHandler func(call ToolCall, err error) string

// ToolErrorFilter decides whether an error should be converted into a ToolMessage.
type ToolErrorFilter func(err error) bool

// ToolErrorFormatter maps an error to content text.
type ToolErrorFormatter func(err error) string

// ToolArgValidationIssue is a single structured validation issue.
type ToolArgValidationIssue struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

// ToolValidationError is implemented by validation errors with structured issues.
type ToolValidationError interface {
	error
	ValidationIssues() []ToolArgValidationIssue
}

// ToolInvocationError reports invocation failures with filtered validation details.
type ToolInvocationError struct {
	ToolName       string
	ToolArgs       map[string]any
	Source         error
	FilteredErrors []ToolArgValidationIssue
}

// Error formats the invocation error for tool reprompt behavior.
func (e *ToolInvocationError) Error() string {
	errorText := ""
	if len(e.FilteredErrors) > 0 {
		parts := make([]string, 0, len(e.FilteredErrors))
		for _, issue := range e.FilteredErrors {
			field := strings.TrimSpace(issue.Field)
			if field == "" {
				parts = append(parts, issue.Message)
				continue
			}
			parts = append(parts, field+": "+issue.Message)
		}
		errorText = strings.Join(parts, "\n")
	} else if e.Source != nil {
		errorText = e.Source.Error()
	}
	return fmt.Sprintf(toolInvocationErrorTemplate, e.ToolName, e.ToolArgs, errorText)
}

// Unwrap exposes the source error.
func (e *ToolInvocationError) Unwrap() error {
	return e.Source
}

// ToolCallRequest is passed to an interceptor around tool execution.
type ToolCallRequest struct {
	Tool     Tool
	State    any
	ToolCall ToolCall
}

// Override returns a shallow clone with selected overrides applied.
func (r ToolCallRequest) Override(toolCall *ToolCall, tool Tool, state any) ToolCallRequest {
	if toolCall != nil {
		r.ToolCall = *toolCall
	}
	if tool != nil {
		r.Tool = tool
	}
	if state != nil {
		r.State = state
	}
	return r
}

// ToolCallExecutor executes a tool request; wrappers may call this multiple times.
type ToolCallExecutor func(request ToolCallRequest) (ToolMessage, error)

// ToolCallWrapper intercepts tool execution with retry/short-circuit capabilities.
type ToolCallWrapper func(request ToolCallRequest, execute ToolCallExecutor) (ToolMessage, error)

// ToolCallResult is a single tool invocation result.
//
// Exactly one of Message or Command is set.
type ToolCallResult struct {
	Message *ToolMessage
	Command *graph.Command
}

// ToolCallResultExecutor executes tool requests that may return messages or commands.
type ToolCallResultExecutor func(request ToolCallRequest) (ToolCallResult, error)

// ToolCallResultWrapper intercepts tool execution with full message/command outputs.
type ToolCallResultWrapper func(request ToolCallRequest, execute ToolCallResultExecutor) (ToolCallResult, error)

func DefaultToolErrorHandler(_ ToolCall, err error) string {
	var invocationErr *ToolInvocationError
	if errors.As(err, &invocationErr) {
		return invocationErr.Error()
	}
	return fmt.Sprintf("Error: %v\n Please fix your mistakes.", err)
}

// DefaultToolErrorFilter handles only invocation/validation errors.
func DefaultToolErrorFilter(err error) bool {
	var invocationErr *ToolInvocationError
	return errors.As(err, &invocationErr)
}

// ToolNode runs tool calls, typically from an assistant tool_call list.
//
// Results are returned in input order, while execution is parallelized.
type ToolNode struct {
	toolsByName      map[string]Tool
	validatorsByName map[string]ToolArgsValidator
	errorHandler     ToolErrorHandler
	errorFilter      ToolErrorFilter
	wrapToolCall     ToolCallWrapper
	wrapToolResult   ToolCallResultWrapper
	handleToolErrors bool
}

// ToolNodeOption configures ToolNode behavior.
type ToolNodeOption func(*ToolNode)

// WithToolErrorHandling enables/disables conversion of tool errors into ToolMessage.
func WithToolErrorHandling(enabled bool) ToolNodeOption {
	return func(n *ToolNode) {
		n.handleToolErrors = enabled
		if enabled {
			n.errorFilter = func(error) bool { return true }
		}
	}
}

// WithToolErrorHandler sets a custom error-to-message mapper.
func WithToolErrorHandler(handler ToolErrorHandler) ToolNodeOption {
	return func(n *ToolNode) {
		if handler != nil {
			n.errorHandler = handler
			n.errorFilter = func(error) bool { return true }
		}
	}
}

// WithToolErrorFilter sets a custom handled-error predicate.
func WithToolErrorFilter(filter ToolErrorFilter) ToolNodeOption {
	return func(n *ToolNode) {
		if filter != nil {
			n.errorFilter = filter
		}
	}
}

// WithToolErrorMessage uses a static content message for handled tool errors.
func WithToolErrorMessage(message string) ToolNodeOption {
	return func(n *ToolNode) {
		n.errorHandler = func(_ ToolCall, _ error) string { return message }
		n.errorFilter = func(error) bool { return true }
	}
}

// WithToolErrorFormatter maps handled tool errors with an error-only formatter.
func WithToolErrorFormatter(formatter ToolErrorFormatter) ToolNodeOption {
	return func(n *ToolNode) {
		if formatter == nil {
			return
		}
		n.errorHandler = func(_ ToolCall, err error) string { return formatter(err) }
		n.errorFilter = func(error) bool { return true }
	}
}

// WithToolHandledErrorTypes restricts handled errors to specified concrete types.
// Pass zero values or typed nil pointers for the desired error types.
func WithToolHandledErrorTypes(examples ...error) ToolNodeOption {
	types := make([]reflect.Type, 0, len(examples))
	for _, example := range examples {
		if example == nil {
			continue
		}
		types = append(types, reflect.TypeOf(example))
	}
	return func(n *ToolNode) {
		if len(types) == 0 {
			return
		}
		n.errorFilter = func(err error) bool {
			for _, want := range types {
				if errorMatchesType(err, want) {
					return true
				}
			}
			return false
		}
	}
}

// WithToolArgumentValidators registers per-tool argument validators.
func WithToolArgumentValidators(validators map[string]ToolArgsValidator) ToolNodeOption {
	return func(n *ToolNode) {
		if len(validators) == 0 {
			return
		}
		n.validatorsByName = make(map[string]ToolArgsValidator, len(validators))
		for name, validator := range validators {
			n.validatorsByName[name] = validator
		}
	}
}

// WithToolCallWrapper sets a wrapper that can intercept tool execution.
func WithToolCallWrapper(wrapper ToolCallWrapper) ToolNodeOption {
	return func(n *ToolNode) {
		n.wrapToolCall = wrapper
	}
}

// WithToolCallResultWrapper sets a wrapper that can intercept message/command tool execution.
func WithToolCallResultWrapper(wrapper ToolCallResultWrapper) ToolNodeOption {
	return func(n *ToolNode) {
		n.wrapToolResult = wrapper
	}
}

// NewToolNode builds a ToolNode from a list of tools.
func NewToolNode(tools []Tool, opts ...ToolNodeOption) *ToolNode {
	n := &ToolNode{
		toolsByName:      make(map[string]Tool, len(tools)),
		validatorsByName: map[string]ToolArgsValidator{},
		handleToolErrors: true,
		errorHandler:     DefaultToolErrorHandler,
		errorFilter:      DefaultToolErrorFilter,
	}
	for _, tool := range tools {
		if tool == nil {
			continue
		}
		n.toolsByName[tool.Name()] = tool
	}
	for _, opt := range opts {
		opt(n)
	}
	return n
}

// ToolsByName returns a copy of the current tool map.
func (n *ToolNode) ToolsByName() map[string]Tool {
	out := make(map[string]Tool, len(n.toolsByName))
	for k, v := range n.toolsByName {
		out[k] = v
	}
	return out
}

// IsReturnDirect reports whether the named tool should short-circuit the loop.
func (n *ToolNode) IsReturnDirect(toolName string) bool {
	tool, ok := n.toolsByName[toolName]
	if !ok {
		return false
	}
	direct, ok := tool.(ReturnDirectTool)
	return ok && direct.ReturnDirect()
}

// Run executes calls in parallel and returns tool messages in input order.
func (n *ToolNode) Run(ctx context.Context, calls []ToolCall) ([]ToolMessage, error) {
	return n.RunWithState(ctx, calls, nil)
}

// RunWithState executes calls in parallel with optional state available to wrappers.
func (n *ToolNode) RunWithState(ctx context.Context, calls []ToolCall, state any) ([]ToolMessage, error) {
	results, err := n.RunResultsWithState(ctx, calls, state)
	if err != nil {
		return nil, err
	}
	out := make([]ToolMessage, 0, len(results))
	for _, result := range results {
		if result.Message == nil {
			return nil, errors.New("prebuilt: RunWithState does not support command outputs; use RunResultsWithState")
		}
		out = append(out, *result.Message)
	}
	return out, nil
}

// RunResults executes calls in parallel and returns message/command outputs in input order.
func (n *ToolNode) RunResults(ctx context.Context, calls []ToolCall) ([]ToolCallResult, error) {
	return n.RunResultsWithState(ctx, calls, nil)
}

// RunResultsWithState executes calls in parallel with optional state and returns message/command outputs.
func (n *ToolNode) RunResultsWithState(ctx context.Context, calls []ToolCall, state any) ([]ToolCallResult, error) {
	if len(calls) == 0 {
		return nil, nil
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make([]ToolCallResult, len(calls))
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	var (
		panicValue any
		panicOnce  sync.Once
	)

	for i := range calls {
		idx := i
		call := calls[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					panicOnce.Do(func() { panicValue = r })
					cancel()
				}
			}()
			if err := runCtx.Err(); err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}

			tool := n.toolsByName[call.Name]

			request := ToolCallRequest{ToolCall: call, Tool: tool, State: state}
			exec := func(req ToolCallRequest) (ToolCallResult, error) {
				return n.executeOne(runCtx, req)
			}

			var (
				result ToolCallResult
				err    error
			)
			if n.wrapToolResult != nil {
				result, err = n.wrapToolResult(request, exec)
			} else if n.wrapToolCall != nil {
				msg, wrapErr := n.wrapToolCall(request, func(req ToolCallRequest) (ToolMessage, error) {
					execResult, execErr := exec(req)
					if execErr != nil {
						return ToolMessage{}, execErr
					}
					if execResult.Message == nil {
						return ToolMessage{}, errors.New("prebuilt: wrapper execute received command output; use WithToolCallResultWrapper")
					}
					return *execResult.Message, nil
				})
				result = toolMessageResult(msg)
				err = wrapErr
			} else {
				result, err = exec(request)
			}
			if err != nil {
				if n.handleToolErrors && n.errorFilter(err) {
					msg := ToolMessage{
						ToolCallID: call.ID,
						Name:       call.Name,
						Status:     "error",
						Content:    n.errorHandler(call, err),
					}
					results[idx] = toolMessageResult(msg)
					return
				}
				cancel()
				select {
				case errCh <- err:
				default:
				}
				return
			}
			results[idx] = result
		}()
	}

	wg.Wait()
	if panicValue != nil {
		panic(panicValue)
	}
	select {
	case err := <-errCh:
		return nil, err
	default:
	}
	return results, nil
}

func (n *ToolNode) executeOne(ctx context.Context, req ToolCallRequest) (ToolCallResult, error) {
	call := req.ToolCall
	tool := req.Tool
	if tool == nil {
		msg := ToolMessage{
			ToolCallID: call.ID,
			Name:       call.Name,
			Status:     "error",
			Content:    fmt.Sprintf(invalidToolNameErrorTemplate, call.Name, n.joinToolNames()),
		}
		return toolMessageResult(msg), nil
	}
	runtime := buildToolRuntime(ctx, req)

	invokeArgs := cloneArgs(call.Args)
	invokeArgs[InjectedStateArg] = runtime.State
	invokeArgs[InjectedStoreArg] = runtime.Store
	invokeArgs[InjectedRuntimeArg] = runtime

	if validator, ok := n.validatorsByName[call.Name]; ok && validator != nil {
		if err := validator(cloneArgs(invokeArgs)); err != nil {
			wrapped := wrapToolInvocationError(call, err)
			invErr, ok := wrapped.(*ToolInvocationError)
			if !ok {
				invErr = &ToolInvocationError{ToolName: call.Name, ToolArgs: cloneArgs(call.Args), Source: wrapped}
			}
			invErr.FilteredErrors = filterInjectedIssues(invErr.FilteredErrors)
			if !n.handleToolErrors || !n.errorFilter(invErr) {
				return ToolCallResult{}, invErr
			}
			return toolMessageResult(ToolMessage{
				ToolCallID: call.ID,
				Name:       call.Name,
				Status:     "error",
				Content:    n.errorHandler(call, invErr),
			}), nil
		}
	}

	var (
		value any
		err   error
	)
	switch t := tool.(type) {
	case *ToolFunc:
		switch {
		case t.runtimeFn != nil:
			value, err = t.runtimeFn(ctx, cloneArgs(call.Args), runtime)
		case t.stateFn != nil:
			value, err = t.stateFn(ctx, cloneArgs(call.Args), runtime.State)
		case t.storeFn != nil:
			value, err = t.storeFn(ctx, cloneArgs(call.Args), runtime.Store)
		default:
			value, err = t.Invoke(ctx, invokeArgs)
		}
	case RuntimeAwareTool:
		value, err = t.InvokeWithRuntime(ctx, cloneArgs(call.Args), runtime)
	case StateAwareTool:
		value, err = t.InvokeWithState(ctx, cloneArgs(call.Args), runtime.State)
	case StoreAwareTool:
		value, err = t.InvokeWithStore(ctx, cloneArgs(call.Args), runtime.Store)
	default:
		value, err = tool.Invoke(ctx, invokeArgs)
	}
	if err != nil {
		if isGraphBubbleUp(err) {
			return ToolCallResult{}, err
		}
		handledErr := wrapToolInvocationErrorIfValidation(call, err)
		if invErr, ok := handledErr.(*ToolInvocationError); ok {
			invErr.FilteredErrors = filterInjectedIssues(invErr.FilteredErrors)
		}
		if !n.handleToolErrors || !n.errorFilter(handledErr) {
			return ToolCallResult{}, handledErr
		}
		return toolMessageResult(ToolMessage{
			ToolCallID: call.ID,
			Name:       call.Name,
			Status:     "error",
			Content:    n.errorHandler(call, handledErr),
		}), nil
	}

	if cmd, ok, cmdErr := coerceToolCommand(value); ok || cmdErr != nil {
		if cmdErr != nil {
			if !n.handleToolErrors || !n.errorFilter(cmdErr) {
				return ToolCallResult{}, cmdErr
			}
			return toolMessageResult(ToolMessage{
				ToolCallID: call.ID,
				Name:       call.Name,
				Status:     "error",
				Content:    n.errorHandler(call, cmdErr),
			}), nil
		}
		validated, err := validateToolCommand(cmd, call)
		if err != nil {
			if !n.handleToolErrors || !n.errorFilter(err) {
				return ToolCallResult{}, err
			}
			return toolMessageResult(ToolMessage{
				ToolCallID: call.ID,
				Name:       call.Name,
				Status:     "error",
				Content:    n.errorHandler(call, err),
			}), nil
		}
		return toolCommandResult(validated), nil
	}

	content, blocks, convErr := contentOutput(value)
	if convErr != nil {
		if !n.handleToolErrors || !n.errorFilter(convErr) {
			return ToolCallResult{}, convErr
		}
		return toolMessageResult(ToolMessage{
			ToolCallID: call.ID,
			Name:       call.Name,
			Status:     "error",
			Content:    n.errorHandler(call, convErr),
		}), nil
	}

	return toolMessageResult(ToolMessage{
		ToolCallID:    call.ID,
		Name:          call.Name,
		Status:        "ok",
		Content:       content,
		ContentBlocks: blocks,
	}), nil
}

func isGraphBubbleUp(err error) bool {
	if err == nil {
		return false
	}
	var bubble *graph.GraphBubbleUp
	if errors.As(err, &bubble) {
		return true
	}
	var interrupt *graph.GraphInterrupt
	if errors.As(err, &interrupt) {
		return true
	}
	var parent *graph.ParentCommand
	return errors.As(err, &parent)
}

func (n *ToolNode) joinToolNames() string {
	if len(n.toolsByName) == 0 {
		return ""
	}
	names := make([]string, 0, len(n.toolsByName))
	for name := range n.toolsByName {
		names = append(names, name)
	}
	sort.Strings(names)
	out := ""
	for i, name := range names {
		if i > 0 {
			out += ", "
		}
		out += name
	}
	return out
}

func contentOutput(v any) (string, []map[string]any, error) {
	if v == nil {
		return "", nil, nil
	}
	if s, ok := v.(string); ok {
		return s, nil, nil
	}
	if blocks, ok := asContentBlocks(v); ok {
		return "", blocks, nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "", nil, err
	}
	return string(b), nil, nil
}

func toolMessageResult(message ToolMessage) ToolCallResult {
	copyVal := message
	return ToolCallResult{Message: &copyVal}
}

func toolCommandResult(command graph.Command) ToolCallResult {
	cmd := cloneGraphCommand(command)
	return ToolCallResult{Command: &cmd}
}

func cloneGraphCommand(in graph.Command) graph.Command {
	out := in
	if in.Graph != nil {
		graphName := *in.Graph
		out.Graph = &graphName
	}
	if in.Update != nil {
		out.Update = make(map[string]graph.Dynamic, len(in.Update))
		for key, value := range in.Update {
			out.Update[key] = value
		}
	}
	out.Goto.Nodes = append([]string(nil), in.Goto.Nodes...)
	out.Goto.Sends = append([]graph.Send(nil), in.Goto.Sends...)
	return out
}

func coerceToolCommand(value any) (graph.Command, bool, error) {
	switch typed := value.(type) {
	case graph.Command:
		return cloneGraphCommand(typed), true, nil
	case *graph.Command:
		if typed == nil {
			return graph.Command{}, false, nil
		}
		return cloneGraphCommand(*typed), true, nil
	case ToolCommand:
		return graph.Command{
			Graph:  typed.Graph,
			Update: graph.DynMap(cloneAnyMap(typed.Update)),
			Goto:   typed.Goto,
		}, true, nil
	case *ToolCommand:
		if typed == nil {
			return graph.Command{}, false, nil
		}
		return graph.Command{
			Graph:  typed.Graph,
			Update: graph.DynMap(cloneAnyMap(typed.Update)),
			Goto:   typed.Goto,
		}, true, nil
	default:
		return graph.Command{}, false, nil
	}
}

func validateToolCommand(command graph.Command, call ToolCall) (graph.Command, error) {
	if command.Update == nil {
		return command, nil
	}

	update := make(map[string]graph.Dynamic, len(command.Update))
	for key, value := range command.Update {
		update[key] = value
	}
	command.Update = update

	messagesValue, hasMessages := command.Update["messages"]
	if command.Graph == nil {
		if !hasMessages {
			return graph.Command{}, fmt.Errorf(
				"prebuilt: command update for tool %q must include messages with a matching tool_call_id",
				call.Name,
			)
		}
	}
	if !hasMessages {
		return command, nil
	}

	messages, err := coerceCommandMessages(messagesValue.Value())
	if err != nil {
		return graph.Command{}, err
	}
	hasMatchingToolMessage := false
	for i := range messages {
		if messages[i].Role != "tool" {
			continue
		}
		if messages[i].ToolCallID == call.ID {
			messages[i].Name = call.Name
			hasMatchingToolMessage = true
		}
	}
	if command.Graph == nil && !hasMatchingToolMessage {
		return graph.Command{}, fmt.Errorf(
			"prebuilt: command update for tool %q must include a matching tool message for tool_call_id %q",
			call.Name,
			call.ID,
		)
	}
	command.Update["messages"] = graph.Dyn(messages)
	return command, nil
}

func coerceCommandMessages(value any) ([]Message, error) {
	switch typed := value.(type) {
	case []Message:
		out := make([]Message, len(typed))
		copy(out, typed)
		return out, nil
	case []ToolMessage:
		out := make([]Message, 0, len(typed))
		for _, message := range typed {
			out = append(out, toolMessageToMessage(message))
		}
		return out, nil
	case []any:
		out := make([]Message, 0, len(typed))
		for _, item := range typed {
			message, err := coerceCommandMessage(item)
			if err != nil {
				return nil, err
			}
			out = append(out, message)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("prebuilt: unsupported command messages update type %T", value)
	}
}

func coerceCommandMessage(value any) (Message, error) {
	switch typed := value.(type) {
	case Message:
		return typed, nil
	case ToolMessage:
		return toolMessageToMessage(typed), nil
	case map[string]any:
		message := Message{}
		if role, ok := typed["role"].(string); ok {
			message.Role = role
		}
		if content, ok := typed["content"].(string); ok {
			message.Content = content
		}
		if name, ok := typed["name"].(string); ok {
			message.Name = name
		}
		if toolCallID, ok := typed["tool_call_id"].(string); ok {
			message.ToolCallID = toolCallID
		}
		if id, ok := typed["id"].(string); ok {
			message.ID = id
		}
		if message.Role == "" {
			message.Role = "tool"
		}
		return message, nil
	default:
		return Message{}, fmt.Errorf("prebuilt: unsupported command message type %T", value)
	}
}

func toolMessageToMessage(message ToolMessage) Message {
	content := message.Content
	if content == "" && len(message.ContentBlocks) > 0 {
		if encoded, err := json.Marshal(message.ContentBlocks); err == nil {
			content = string(encoded)
		}
	}
	return Message{
		Role:       "tool",
		Name:       message.Name,
		ToolCallID: message.ToolCallID,
		Content:    content,
	}
}

func asContentBlocks(v any) ([]map[string]any, bool) {
	items, ok := v.([]any)
	if !ok {
		return nil, false
	}
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		block, ok := item.(map[string]any)
		if !ok {
			return nil, false
		}
		typ, ok := block["type"].(string)
		if !ok || strings.TrimSpace(typ) == "" {
			return nil, false
		}
		out = append(out, block)
	}
	return out, true
}

func wrapToolInvocationError(call ToolCall, err error) error {
	if err == nil {
		return nil
	}
	if invErr, ok := err.(*ToolInvocationError); ok {
		return invErr
	}
	issues := extractValidationIssues(err)
	return &ToolInvocationError{
		ToolName:       call.Name,
		ToolArgs:       cloneArgs(call.Args),
		Source:         err,
		FilteredErrors: issues,
	}
}

func wrapToolInvocationErrorIfValidation(call ToolCall, err error) error {
	if err == nil {
		return nil
	}
	if _, ok := err.(*ToolInvocationError); ok {
		return err
	}
	if issues := extractValidationIssues(err); len(issues) > 0 {
		return &ToolInvocationError{
			ToolName:       call.Name,
			ToolArgs:       cloneArgs(call.Args),
			Source:         err,
			FilteredErrors: issues,
		}
	}
	return err
}

func extractValidationIssues(err error) []ToolArgValidationIssue {
	var validationErr ToolValidationError
	if errors.As(err, &validationErr) {
		issues := validationErr.ValidationIssues()
		out := make([]ToolArgValidationIssue, len(issues))
		copy(out, issues)
		return out
	}
	return nil
}

func filterInjectedIssues(issues []ToolArgValidationIssue) []ToolArgValidationIssue {
	if len(issues) == 0 {
		return nil
	}
	out := make([]ToolArgValidationIssue, 0, len(issues))
	for _, issue := range issues {
		top := strings.TrimSpace(strings.Split(issue.Field, ".")[0])
		switch top {
		case InjectedStateArg, InjectedStoreArg, InjectedRuntimeArg:
			continue
		default:
			out = append(out, issue)
		}
	}
	return out
}

func buildToolRuntime(ctx context.Context, req ToolCallRequest) ToolRuntime {
	cfg := graph.GetConfig(ctx)
	rt := graph.GetRuntime(ctx)
	store := graph.GetStore(ctx)
	return ToolRuntime{
		ToolCallID:   req.ToolCall.ID,
		State:        req.State,
		Config:       cfg,
		Store:        store,
		Context:      rt.Context,
		StreamWriter: rt.StreamWriter,
	}
}

func errorMatchesType(err error, want reflect.Type) bool {
	if err == nil || want == nil {
		return false
	}
	for cur := err; cur != nil; cur = errors.Unwrap(cur) {
		curType := reflect.TypeOf(cur)
		if curType == want {
			return true
		}
		if want.Kind() == reflect.Interface && curType.Implements(want) {
			return true
		}
		if want.Kind() == reflect.Pointer && curType.AssignableTo(want) {
			return true
		}
	}
	return false
}

func cloneArgs(in map[string]any) map[string]any {
	if in == nil {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
