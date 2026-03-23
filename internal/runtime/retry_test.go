package runtime

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/graph"
)

func TestExecuteWithRetry_SuccessFirstAttempt(t *testing.T) {
	calls := 0
	err := ExecuteWithRetry(context.Background(), graph.RetryPolicy{
		MaxAttempts:     3,
		InitialInterval: time.Millisecond,
		BackoffFactor:   2,
		MaxInterval:     time.Second,
	}, func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}
}

func TestExecuteWithRetry_ExhaustsAttempts(t *testing.T) {
	calls := 0
	sentinel := errors.New("fail")
	err := ExecuteWithRetry(context.Background(), graph.RetryPolicy{
		MaxAttempts:     3,
		InitialInterval: time.Millisecond,
		BackoffFactor:   1,
		MaxInterval:     time.Second,
		RetryOn:         graph.DefaultRetryOn,
	}, func() error {
		calls++
		return sentinel
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestExecuteWithRetry_NilRetryOnDefaultsToAnyError(t *testing.T) {
	var calls int32
	err := ExecuteWithRetry(context.Background(), graph.RetryPolicy{
		MaxAttempts:     2,
		InitialInterval: time.Millisecond,
		BackoffFactor:   1,
		MaxInterval:     time.Second,
		RetryOn:         nil, // should default to retrying on any error
	}, func() error {
		n := atomic.AddInt32(&calls, 1)
		if n < 2 {
			return errors.New("transient")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if calls != 2 {
		t.Fatalf("expected 2 calls, got %d", calls)
	}
}

func TestExecuteWithRetry_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := ExecuteWithRetry(ctx, graph.RetryPolicy{
		MaxAttempts:     5,
		InitialInterval: time.Second,
		BackoffFactor:   1,
		MaxInterval:     time.Second,
	}, func() error {
		return errors.New("fail")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestExecuteWithRetry_ZeroMaxAttemptsTreatedAsOne(t *testing.T) {
	calls := 0
	_ = ExecuteWithRetry(context.Background(), graph.RetryPolicy{
		MaxAttempts: 0, // should be normalised to 1
	}, func() error {
		calls++
		return errors.New("fail")
	})
	if calls != 1 {
		t.Fatalf("expected 1 call for MaxAttempts=0, got %d", calls)
	}
}
