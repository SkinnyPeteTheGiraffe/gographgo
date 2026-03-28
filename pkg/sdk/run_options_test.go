package sdk_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/sdk"
)

func TestRunsClientRunOptionsAreSerialized(t *testing.T) {
	t.Parallel()

	expectedCheckpoint := map[string]any{"checkpoint_id": "cp-prev"}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/threads/t1/runs":
			body := readBodyMap(t, r.Body)
			assertRunOptionsInBody(t, body, expectedCheckpoint)
			writeJSON(t, w, map[string]any{"run_id": "r1", "thread_id": "t1", "status": "pending", "created_at": time.Now().UTC()})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/threads/t1/runs/stream":
			body := readBodyMap(t, r.Body)
			assertRunOptionsInBody(t, body, expectedCheckpoint)
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("event: values\ndata: {\"ok\":true}\n\n"))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer ts.Close()

	client, err := sdk.New(sdk.Config{BaseURL: ts.URL})
	if err != nil {
		t.Fatalf("sdk.New() error = %v", err)
	}

	ctx := context.Background()
	req := sdk.RunCreateRequest{
		AssistantID:       "a1",
		Input:             map[string]any{"question": "hi"},
		InterruptBefore:   []string{"n1", "n2"},
		InterruptAfter:    []string{"n3"},
		Webhook:           "https://example.test/hook",
		MultitaskStrategy: "enqueue",
		IfNotExists:       "create",
		AfterSeconds:      30,
		Durability:        "sync",
		StreamSubgraphs:   true,
		Checkpoint:        expectedCheckpoint,
		CheckpointID:      "cp-42",
	}

	if _, err := client.Runs.Create(ctx, "t1", req); err != nil {
		t.Fatalf("Runs.Create() error = %v", err)
	}
	if _, err := client.CreateRunWithOptions(ctx, "t1", req); err != nil {
		t.Fatalf("CreateRunWithOptions() error = %v", err)
	}

	parts, errs := client.Runs.Stream(ctx, "t1", sdk.RunStreamRequest(req))
	select {
	case part := <-parts:
		if part.Event != "values" {
			t.Fatalf("Runs.Stream() event = %q, want values", part.Event)
		}
	case err := <-errs:
		t.Fatalf("Runs.Stream() error = %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for stream response")
	}

	parts, errs = client.StreamRunWithOptions(ctx, "t1", sdk.RunStreamRequest(req))
	select {
	case part := <-parts:
		if part.Event != "values" {
			t.Fatalf("StreamRunWithOptions() event = %q, want values", part.Event)
		}
	case err := <-errs:
		t.Fatalf("StreamRunWithOptions() error = %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for wrapper stream response")
	}

	typedParts, typedErrs := client.StreamRunWithOptionsTyped(ctx, "t1", sdk.RunStreamRequest(req))
	select {
	case part := <-typedParts:
		if _, ok := part.(sdk.ValuesStreamPart); !ok {
			t.Fatalf("StreamRunWithOptionsTyped() part = %T, want sdk.ValuesStreamPart", part)
		}
	case err := <-typedErrs:
		t.Fatalf("StreamRunWithOptionsTyped() error = %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for typed wrapper stream response")
	}
}

func assertRunOptionsInBody(t *testing.T, body, expectedCheckpoint map[string]any) {
	t.Helper()

	if body["assistant_id"] != "a1" {
		t.Fatalf("assistant_id = %v", body["assistant_id"])
	}
	if body["webhook"] != "https://example.test/hook" {
		t.Fatalf("webhook = %v", body["webhook"])
	}
	if body["multitask_strategy"] != "enqueue" {
		t.Fatalf("multitask_strategy = %v", body["multitask_strategy"])
	}
	if body["if_not_exists"] != "create" {
		t.Fatalf("if_not_exists = %v", body["if_not_exists"])
	}
	if body["after_seconds"] != float64(30) {
		t.Fatalf("after_seconds = %v", body["after_seconds"])
	}
	if body["durability"] != "sync" {
		t.Fatalf("durability = %v", body["durability"])
	}
	if body["stream_subgraphs"] != true {
		t.Fatalf("stream_subgraphs = %v", body["stream_subgraphs"])
	}
	if body["checkpoint_id"] != "cp-42" {
		t.Fatalf("checkpoint_id = %v", body["checkpoint_id"])
	}

	interruptBefore, ok := body["interrupt_before"].([]any)
	if !ok || len(interruptBefore) != 2 || interruptBefore[0] != "n1" || interruptBefore[1] != "n2" {
		t.Fatalf("interrupt_before = %#v", body["interrupt_before"])
	}
	interruptAfter, ok := body["interrupt_after"].([]any)
	if !ok || len(interruptAfter) != 1 || interruptAfter[0] != "n3" {
		t.Fatalf("interrupt_after = %#v", body["interrupt_after"])
	}

	checkpointMap, ok := body["checkpoint"].(map[string]any)
	if !ok {
		t.Fatalf("checkpoint type = %T", body["checkpoint"])
	}
	if checkpointMap["checkpoint_id"] != expectedCheckpoint["checkpoint_id"] {
		t.Fatalf("checkpoint = %#v", checkpointMap)
	}
}
