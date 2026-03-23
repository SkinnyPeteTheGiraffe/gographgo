package graph

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"
)

// Internal Pregel protocol channel names.
// These mirror Python LangGraph's _internal/_constants.py.
const (
	pregelInput      = "__input__"
	pregelInterrupt  = "__interrupt__"
	pregelResume     = "__resume__"
	pregelError      = "__error__"
	pregelPush       = "__pregel_push"
	pregelTasks      = "__pregel_tasks"
	pregelNullTaskID = "00000000-0000-0000-0000-000000000000"
)

// pregelWrite is a pending write to a named channel produced by a task.
type pregelWrite struct {
	channel string
	value   any
}

// pregelTaskResult holds the output of a completed task.
type pregelTaskResult struct {
	taskID       string
	node         string
	path         []any
	checkpointNS string
	writes       []pregelWrite
	interrupts   []Interrupt // interrupts raised inside the node via Interrupt()
	err          error
	// parentCmd is non-nil when the task issued a Command targeting the parent
	// graph (Command.Graph == CommandParent). The loop propagates this upward
	// rather than handling it locally.
	parentCmd *Command
}

// pregelTask is an in-flight task for a single superstep.
type pregelTask[State any] struct {
	id           string
	name         string
	path         []any
	checkpointNS string
	triggers     []string
	input        any
	inputSchema  any
	cacheKey     *CacheKey
	writers      []ChannelWrite[State]
	subgraphs    []string
	call         *Call
	resumeValues []Dynamic // positional resume values from Command{Resume: ...}
	// isPush marks tasks that were dispatched via Send fan-out. Their input
	// is the Send.Arg value (not the shared graph state), mirroring Python
	// LangGraph's PUSH vs PULL task distinction.
	isPush bool
}

// sendContextKey is the context key for the runtime Send function.
type sendContextKey struct{}

// callContextKey is the context key for the runtime functional Call function.
type callContextKey struct{}

// streamWriterContextKey is the context key for the custom StreamWriter.
type streamWriterContextKey struct{}

// taskIDContextKey is the context key for the current task ID.
type taskIDContextKey struct{}

// WithSend returns a context with the given Send function injected so that
// node code can call graph.GetSend(ctx) to fan-out messages.
func WithSend(ctx context.Context, fn func(node string, arg Dynamic)) context.Context {
	return context.WithValue(ctx, sendContextKey{}, fn)
}

// GetSend retrieves the Send function from context. Returns a no-op if absent.
func GetSend(ctx context.Context) func(node string, arg Dynamic) {
	if fn, ok := ctx.Value(sendContextKey{}).(func(string, Dynamic)); ok {
		return fn
	}
	return func(string, Dynamic) {}
}

// WithCall returns a context with the given Call function injected so that
// node code can call graph.GetCall(ctx) to dispatch functional PUSH tasks.
func WithCall(ctx context.Context, fn func(call Call)) context.Context {
	return context.WithValue(ctx, callContextKey{}, fn)
}

// GetCall retrieves the Call function from context. Returns a no-op if absent.
func GetCall(ctx context.Context) func(Call) {
	if fn, ok := ctx.Value(callContextKey{}).(func(Call)); ok {
		return fn
	}
	return func(Call) {}
}

// WithStreamWriter returns a context carrying the custom StreamWriter.
func WithStreamWriter(ctx context.Context, w StreamWriter) context.Context {
	return context.WithValue(ctx, streamWriterContextKey{}, w)
}

// GetStreamWriter retrieves the StreamWriter from context. Returns a no-op if absent.
func GetStreamWriter(ctx context.Context) StreamWriter {
	if w, ok := ctx.Value(streamWriterContextKey{}).(StreamWriter); ok {
		return w
	}
	return func(any) {}
}

// GetTaskID retrieves the current Pregel task ID from context.
// Returns an empty string when called outside task execution.
func GetTaskID(ctx context.Context) string {
	if id, ok := ctx.Value(taskIDContextKey{}).(string); ok {
		return id
	}
	return ""
}

// StreamMessage emits a messages-mode chunk through the injected StreamWriter.
//
// Nodes can call this while running under StreamModeMessages or StreamModeDebug
// to mirror token/chunk streaming behavior from model backends.
func StreamMessage(ctx context.Context, chunk any, metadata map[string]any) {
	md := map[string]any(nil)
	if metadata != nil {
		md = make(map[string]any, len(metadata))
		for k, v := range metadata {
			md[k] = v
		}
	}
	GetStreamWriter(ctx)(StreamEmit{
		Mode: StreamModeMessages,
		Data: MessageChunk{Chunk: chunk, Metadata: md},
	})
}

// executeWithRetry runs fn with the given retry policy, applying exponential
// backoff with optional jitter between attempts.
//
// Mirrors Python LangGraph's _internal/_retry.py retry logic.
func executeWithRetry(ctx context.Context, policy RetryPolicy, fn func() error) error {
	return executeWithRetryPolicies(ctx, []RetryPolicy{policy}, fn)
}

// executeWithRetryPolicies runs fn with a list of retry policies.
//
// On each failure, the first matching policy (in declaration order) is used.
// If no policy matches, execution fails immediately.
//
// Mirrors Python LangGraph's run_with_retry behavior where retry policy can be
// configured as a sequence and matching is exception-driven per failure.
func executeWithRetryPolicies(ctx context.Context, policies []RetryPolicy, fn func() error) error {
	if len(policies) == 0 {
		return fn()
	}

	attempts := 0
	var lastErr error

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		setRuntimeExecutionAttempt(ctx, attempts+1)
		lastErr = fn()
		if lastErr == nil {
			return nil
		}

		policy, maxAttempts, ok := matchingRetryPolicy(policies, lastErr)
		if !ok {
			return lastErr
		}

		attempts++
		if attempts >= maxAttempts {
			return fmt.Errorf("all %d attempts failed: %w", attempts, lastErr)
		}

		sleep := retrySleepDuration(policy, attempts)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleep):
		}
	}
}

func setRuntimeExecutionAttempt(ctx context.Context, attempt int) {
	rt := GetRuntime(ctx)
	if rt == nil {
		return
	}
	if rt.ExecutionInfo == nil {
		rt.ExecutionInfo = &ExecutionInfo{}
	}
	rt.ExecutionInfo.NodeAttempt = attempt
	if rt.ExecutionInfo.NodeFirstAttemptTime == 0 {
		rt.ExecutionInfo.NodeFirstAttemptTime = time.Now().Unix()
	}
}

func matchingRetryPolicy(policies []RetryPolicy, err error) (RetryPolicy, int, bool) {
	for _, policy := range policies {
		retryOn := policy.RetryOn
		if retryOn == nil {
			retryOn = DefaultRetryOn
		}
		if !retryOn(err) {
			continue
		}
		maxAttempts := policy.MaxAttempts
		if maxAttempts <= 0 {
			maxAttempts = 1
		}
		return policy, maxAttempts, true
	}
	return RetryPolicy{}, 0, false
}

func retrySleepDuration(policy RetryPolicy, attempts int) time.Duration {
	interval := policy.InitialInterval
	if interval < 0 {
		interval = 0
	}
	if policy.BackoffFactor > 0 && attempts > 1 {
		interval = time.Duration(float64(interval) * math.Pow(policy.BackoffFactor, float64(attempts-1)))
	}
	if policy.MaxInterval > 0 {
		interval = time.Duration(math.Min(float64(interval), float64(policy.MaxInterval)))
	}
	if policy.Jitter {
		jitterFraction := rand.Float64() * 0.25
		interval = time.Duration(float64(interval) * (1 + jitterFraction))
	}
	return interval
}
