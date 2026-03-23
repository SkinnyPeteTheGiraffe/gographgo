package sdk_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/SkinnyPeteTheGiraffe/gographgo/internal/server"
	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/checkpoint"
	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/sdk"
)

type runner struct{}

func (runner) Run(_ context.Context, req server.RunRequest) (server.RunResult, error) {
	value := ""
	if req.Input != nil {
		if v, ok := req.Input["message"].(string); ok {
			value = v
		}
	}
	return server.RunResult{
		Output: map[string]any{"text": "processed:" + value},
		State:  map[string]any{"last": value},
	}, nil
}

func TestClient_E2E(t *testing.T) {
	s := server.New(server.Options{Runner: runner{}, Checkpointer: checkpoint.NewInMemorySaver()})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	client, err := sdk.New(sdk.Config{BaseURL: ts.URL})
	if err != nil {
		t.Fatalf("sdk.New: %v", err)
	}

	ctx := context.Background()
	thread, err := client.CreateThread(ctx, "", map[string]any{"project": "phase7"})
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	if thread.ID == "" {
		t.Fatal("thread id empty")
	}

	run, err := client.CreateRun(ctx, thread.ID, "assistant-1", map[string]any{"message": "hello"}, nil)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	events, errs := client.StreamRunEvents(ctx, thread.ID, run.ID)

	final, err := client.WaitRun(ctx, thread.ID, run.ID, 25*time.Millisecond)
	if err != nil {
		t.Fatalf("WaitRun: %v", err)
	}
	if final.Status != sdk.RunStatusSuccess {
		t.Fatalf("status = %s, want success", final.Status)
	}
	if final.Output["text"] != "processed:hello" {
		t.Fatalf("output = %v", final.Output)
	}

	state, err := client.GetThreadState(ctx, thread.ID)
	if err != nil {
		t.Fatalf("GetThreadState: %v", err)
	}
	if state.Values["last"] != "hello" {
		t.Fatalf("state = %v", state.Values)
	}

	history, err := client.ListThreadHistory(ctx, thread.ID)
	if err != nil {
		t.Fatalf("ListThreadHistory: %v", err)
	}
	if len(history) == 0 {
		t.Fatal("expected non-empty history")
	}

	_, err = client.PutStoreValue(ctx, thread.ID, "profile", "name", map[string]any{"first": "Ada"})
	if err != nil {
		t.Fatalf("PutStoreValue: %v", err)
	}
	storeVal, err := client.GetStoreValue(ctx, thread.ID, "profile", "name")
	if err != nil {
		t.Fatalf("GetStoreValue: %v", err)
	}
	if storeVal.Value["first"] != "Ada" {
		t.Fatalf("store value = %v", storeVal.Value)
	}

	haveCompleted := false
	deadline := time.After(2 * time.Second)
	for !haveCompleted {
		select {
		case evt, ok := <-events:
			if !ok {
				haveCompleted = true
				break
			}
			if evt.Type == "run.completed" {
				haveCompleted = true
			}
		case err, ok := <-errs:
			if ok && err != nil {
				t.Fatalf("stream error: %v", err)
			}
		case <-deadline:
			t.Fatal("timed out waiting for completed event")
		}
	}
}

func TestClient_NewValidation(t *testing.T) {
	if _, err := sdk.New(sdk.Config{}); err == nil {
		t.Fatal("expected error for empty BaseURL")
	}
	if _, err := sdk.New(sdk.Config{BaseURL: "::://bad"}); err == nil {
		t.Fatal("expected URL parse error")
	}
}

func TestClient_NotFound(t *testing.T) {
	s := server.New(server.Options{})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	client, err := sdk.New(sdk.Config{BaseURL: ts.URL})
	if err != nil {
		t.Fatalf("sdk.New: %v", err)
	}

	_, err = client.GetThread(context.Background(), "missing")
	if err == nil {
		t.Fatal("expected not found error")
	}
	if got := err.Error(); got == "" || !strings.Contains(got, "404") {
		t.Fatalf("err = %q, expected http status", got)
	}
}

func TestClient_UsesAPIKeyFromConfigAndHeaders(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret" {
			t.Fatalf("authorization header = %q", r.Header.Get("Authorization"))
		}
		if r.Header.Get("X-Trace-ID") != "trace-123" {
			t.Fatalf("trace header = %q", r.Header.Get("X-Trace-ID"))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"thread_id":"t1","created_at":"2026-01-01T00:00:00Z"}`))
	}))
	defer ts.Close()

	client, err := sdk.New(sdk.Config{BaseURL: ts.URL, APIKey: "secret", Headers: map[string]string{"X-Trace-ID": "trace-123"}})
	if err != nil {
		t.Fatalf("sdk.New: %v", err)
	}

	if _, err := client.GetThread(context.Background(), "t1"); err != nil {
		t.Fatalf("GetThread: %v", err)
	}
}

func TestClient_UsesAPIKeyFromEnvironment(t *testing.T) {
	t.Setenv("GOGRAPHGO_API_KEY", "env-secret")

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer env-secret" {
			t.Fatalf("authorization header = %q", r.Header.Get("Authorization"))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"thread_id":"t1","created_at":"2026-01-01T00:00:00Z"}`))
	}))
	defer ts.Close()

	client, err := sdk.New(sdk.Config{BaseURL: ts.URL})
	if err != nil {
		t.Fatalf("sdk.New: %v", err)
	}

	if _, err := client.GetThread(context.Background(), "t1"); err != nil {
		t.Fatalf("GetThread: %v", err)
	}
}
