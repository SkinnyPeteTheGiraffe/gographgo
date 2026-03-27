package sdk_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/sdk"
)

func TestRunsEvents_UsesSSEEventTypeFallback(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		expectedPath := "/v1/threads/thread-1/runs/run-1/events"
		if r.URL.Path != expectedPath {
			t.Fatalf("path = %q, want %q", r.URL.Path, expectedPath)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Fatal("response writer does not support flushing")
		}
		_, _ = fmt.Fprint(w, "event: run.started\ndata: {\"thread_id\":\"thread-1\",\"run_id\":\"run-1\"}\n\n")
		flusher.Flush()
		_, _ = fmt.Fprint(w, "event: run.completed\ndata: {\"type\":\"from-payload\",\"thread_id\":\"thread-1\",\"run_id\":\"run-1\"}\n\n")
		flusher.Flush()
	}))
	defer ts.Close()

	client, err := sdk.New(sdk.Config{BaseURL: ts.URL})
	if err != nil {
		t.Fatalf("sdk.New() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	events, errs := client.Runs.Events(ctx, "thread-1", "run-1")

	first := mustReadRunEvent(t, events)
	if first.Type != "run.started" {
		t.Fatalf("first event type = %q, want run.started", first.Type)
	}

	second := mustReadRunEvent(t, events)
	if second.Type != "from-payload" {
		t.Fatalf("second event type = %q, want from-payload", second.Type)
	}

	select {
	case err, ok := <-errs:
		if ok && err != nil {
			t.Fatalf("unexpected stream error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for errors channel to close")
	}
}

func TestRunsEvents_InvalidEventPayloadReturnsError(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Fatal("response writer does not support flushing")
		}
		_, _ = fmt.Fprint(w, "event: run.started\ndata: {\"thread_id\":\"thread-1\",\"run_id\":\"run-1\"\n\n")
		flusher.Flush()
	}))
	defer ts.Close()

	client, err := sdk.New(sdk.Config{BaseURL: ts.URL})
	if err != nil {
		t.Fatalf("sdk.New() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	events, errs := client.Runs.Events(ctx, "thread-1", "run-1")

	select {
	case err := <-errs:
		if err == nil {
			t.Fatal("expected non-nil stream error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for stream error")
	}

	select {
	case _, ok := <-events:
		if ok {
			t.Fatal("expected events channel to be closed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for events channel close")
	}
}

func mustReadRunEvent(t *testing.T, events <-chan sdk.RunEvent) sdk.RunEvent {
	t.Helper()
	select {
	case evt, ok := <-events:
		if !ok {
			t.Fatal("events channel closed unexpectedly")
		}
		return evt
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for run event")
		return sdk.RunEvent{}
	}
}
