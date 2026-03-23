package sdk_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/SkinnyPeteTheGiraffe/gographgo/internal/server"
	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/checkpoint"
	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/sdk"
)

type slowRunner struct{}

func (slowRunner) Run(ctx context.Context, req server.RunRequest) (server.RunResult, error) {
	select {
	case <-ctx.Done():
		return server.RunResult{}, ctx.Err()
	case <-time.After(250 * time.Millisecond):
	}
	return server.RunResult{Output: req.Input, State: req.Input}, nil
}

func TestClientNetworkFailure(t *testing.T) {
	t.Parallel()
	client, err := sdk.New(sdk.Config{BaseURL: "http://127.0.0.1:1", HTTPClient: &http.Client{Timeout: 150 * time.Millisecond}})
	if err != nil {
		t.Fatalf("sdk.New() error = %v", err)
	}
	_, err = client.GetThread(context.Background(), "missing")
	if err == nil {
		t.Fatal("expected network error")
	}
}

func TestStreamRunEventsCancellationRace(t *testing.T) {
	t.Parallel()
	s := server.New(server.Options{Runner: slowRunner{}, Checkpointer: checkpoint.NewInMemorySaver()})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	client, err := sdk.New(sdk.Config{BaseURL: ts.URL})
	if err != nil {
		t.Fatalf("sdk.New() error = %v", err)
	}

	ctx := context.Background()
	thread, err := client.CreateThread(ctx, "thread-cancel", nil)
	if err != nil {
		t.Fatalf("CreateThread() error = %v", err)
	}
	run, err := client.CreateRun(ctx, thread.ID, "assistant", map[string]any{"x": 1}, nil)
	if err != nil {
		t.Fatalf("CreateRun() error = %v", err)
	}

	streamCtx, cancel := context.WithCancel(ctx)
	events, errs := client.StreamRunEvents(streamCtx, thread.ID, run.ID)
	cancel()

	timeout := time.After(2 * time.Second)
	for events != nil || errs != nil {
		select {
		case _, ok := <-events:
			if !ok {
				events = nil
			}
		case err, ok := <-errs:
			if !ok {
				errs = nil
				continue
			}
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Fatalf("unexpected stream error: %v", err)
			}
		case <-timeout:
			t.Fatal("timed out waiting for stream shutdown")
		}
	}
}
