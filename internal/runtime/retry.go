// Package runtime contains private runtime orchestration internals for gographgo.
//
// This package implements the execution primitives used by the Pregel runtime,
// mirroring Python LangGraph's langgraph/_internal/_retry.py behavior.
package runtime

import (
	"context"
	"math"
	"math/rand"
	"time"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/graph"
)

// ExecuteWithRetry runs fn under the given RetryPolicy.
//
// It retries on errors that satisfy policy.RetryOn (defaults to any non-nil
// error when RetryOn is nil), applying exponential backoff between attempts.
// Jitter, when enabled, adds up to 25% random duration to each sleep.
//
// The attempt loop is context-aware: a canceled context aborts the retry
// loop and returns ctx.Err() immediately.
func ExecuteWithRetry(ctx context.Context, policy graph.RetryPolicy, fn func() error) error {
	retryOn := policy.RetryOn
	if retryOn == nil {
		retryOn = func(err error) bool { return err != nil }
	}

	interval := policy.InitialInterval
	maxAttempts := policy.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 1
	}

	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		lastErr = fn()
		if lastErr == nil {
			return nil
		}

		if !retryOn(lastErr) {
			return lastErr
		}

		// No sleep after the last attempt.
		if attempt == maxAttempts-1 {
			break
		}

		sleep := jitteredSleep(interval, policy.Jitter)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleep):
		}

		// Advance interval with backoff, capped at MaxInterval.
		if policy.BackoffFactor > 0 {
			next := time.Duration(math.Min(
				float64(interval)*policy.BackoffFactor,
				float64(policy.MaxInterval),
			))
			interval = next
		}
	}

	return lastErr
}

// jitteredSleep computes the sleep duration for a retry interval, optionally
// adding up to 25% random jitter.
func jitteredSleep(base time.Duration, jitter bool) time.Duration {
	if !jitter {
		return base
	}
	extra := time.Duration(float64(base) * rand.Float64() * 0.25)
	return base + extra
}
