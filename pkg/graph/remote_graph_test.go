package graph_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/checkpoint"
	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/graph"
	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/sdk"
)

func TestRemoteGraph_InvokeGetUpdateStateAndHistory(t *testing.T) {
	t.Parallel()

	type captures struct {
		sync.Mutex
		invokeThread string
		invokeReq    sdk.RunCreateRequest
		updateReqs   []sdk.ThreadUpdateStateRequest
	}
	cap := &captures{}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(path, "/runs"):
			threadID := pathPart(path, 2)
			var req sdk.RunCreateRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode invoke request: %v", err)
			}
			cap.Lock()
			cap.invokeThread = threadID
			cap.invokeReq = req
			cap.Unlock()
			writeJSON(t, w, http.StatusAccepted, map[string]any{
				"run_id":       "run-1",
				"thread_id":    threadID,
				"assistant_id": req.AssistantID,
				"status":       "queued",
				"created_at":   time.Now().UTC().Format(time.RFC3339Nano),
			})
		case r.Method == http.MethodGet && strings.Contains(path, "/runs/run-1"):
			threadID := pathPart(path, 2)
			writeJSON(t, w, http.StatusOK, map[string]any{
				"run_id":       "run-1",
				"thread_id":    threadID,
				"assistant_id": "assistant-default",
				"status":       "completed",
				"output":       map[string]any{"result": "ok"},
				"created_at":   time.Now().UTC().Format(time.RFC3339Nano),
			})
		case r.Method == http.MethodGet && strings.HasSuffix(path, "/state"):
			threadID := pathPart(path, 2)
			writeJSON(t, w, http.StatusOK, map[string]any{
				"thread_id":     threadID,
				"checkpoint_id": "cp-latest",
				"values":        map[string]any{"count": 7},
				"metadata":      map[string]any{"source": "loop", "step": float64(3)},
			})
		case r.Method == http.MethodPost && strings.HasSuffix(path, "/state"):
			var req sdk.ThreadUpdateStateRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode update state request: %v", err)
			}
			cap.Lock()
			cap.updateReqs = append(cap.updateReqs, req)
			cap.Unlock()
			writeJSON(t, w, http.StatusOK, map[string]any{"checkpoint": map[string]any{"checkpoint_id": "cp-next"}})
		case r.Method == http.MethodGet && strings.HasSuffix(path, "/history"):
			threadID := pathPart(path, 2)
			writeJSON(t, w, http.StatusOK, []*checkpoint.CheckpointTuple{
				{
					Config: &checkpoint.Config{ThreadID: threadID, CheckpointID: "cp-h1"},
					Checkpoint: &checkpoint.Checkpoint{
						ID:            "cp-h1",
						TS:            time.Now().UTC().Format(time.RFC3339Nano),
						ChannelValues: map[string]any{"count": 7},
					},
					Metadata: &checkpoint.CheckpointMetadata{Source: "loop", Step: 3},
				},
			})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer ts.Close()

	client, err := sdk.New(sdk.Config{BaseURL: ts.URL})
	if err != nil {
		t.Fatalf("sdk.New: %v", err)
	}

	remote, err := graph.NewRemoteGraph(client, graph.RemoteGraphOptions{
		ThreadID:    "thread-default",
		AssistantID: "assistant-default",
		RunCreate:   sdk.RunCreateRequest{Metadata: map[string]any{"from_default": true}},
	})
	if err != nil {
		t.Fatalf("NewRemoteGraph: %v", err)
	}

	ctx := graph.WithConfig(context.Background(), graph.Config{
		ThreadID:     "thread-ctx",
		CheckpointID: "cp-ctx",
		Durability:   graph.DurabilitySync,
		Metadata:     map[string]any{"from_ctx": "yes"},
	})

	out, err := remote.Invoke(ctx, map[string]any{"input": "hello"})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	value, ok := out.Value.(map[string]any)
	if !ok {
		t.Fatalf("Invoke value type = %T, want map[string]any", out.Value)
	}
	if value["result"] != "ok" {
		t.Fatalf("Invoke value = %#v, want result=ok", value)
	}

	state, err := remote.GetState(ctx)
	if err != nil {
		t.Fatalf("GetState: %v", err)
	}
	if state.Config == nil || state.Config.ThreadID != "thread-ctx" {
		t.Fatalf("GetState config = %#v", state.Config)
	}
	values, ok := state.Values.(map[string]any)
	if !ok || values["count"] != float64(7) {
		t.Fatalf("GetState values = %#v", state.Values)
	}

	if err := remote.UpdateState(ctx, map[string]any{"count": 9}); err != nil {
		t.Fatalf("UpdateState: %v", err)
	}
	if err := remote.UpdateStateWithOptions(ctx, map[string]any{"count": 10}, graph.RemoteGraphUpdateStateOptions{AsNode: "worker"}); err != nil {
		t.Fatalf("UpdateStateWithOptions: %v", err)
	}

	history, err := remote.GetStateHistory(ctx)
	if err != nil {
		t.Fatalf("GetStateHistory: %v", err)
	}
	if len(history) != 1 {
		t.Fatalf("GetStateHistory len = %d, want 1", len(history))
	}

	cap.Lock()
	defer cap.Unlock()
	if cap.invokeThread != "thread-ctx" {
		t.Fatalf("invoke thread = %q, want thread-ctx", cap.invokeThread)
	}
	if cap.invokeReq.AssistantID != "assistant-default" {
		t.Fatalf("assistant_id = %q, want assistant-default", cap.invokeReq.AssistantID)
	}
	if cap.invokeReq.Durability != "sync" {
		t.Fatalf("durability = %q, want sync", cap.invokeReq.Durability)
	}
	if cap.invokeReq.CheckpointID != "cp-ctx" {
		t.Fatalf("checkpoint_id = %q, want cp-ctx", cap.invokeReq.CheckpointID)
	}
	if cap.invokeReq.Metadata["from_default"] != true || cap.invokeReq.Metadata["from_ctx"] != "yes" {
		t.Fatalf("merged metadata = %#v", cap.invokeReq.Metadata)
	}
	if len(cap.updateReqs) != 2 {
		t.Fatalf("update request count = %d, want 2", len(cap.updateReqs))
	}
	if cap.updateReqs[0].CheckpointID != "cp-ctx" || cap.updateReqs[0].AsNode != "" {
		t.Fatalf("first update req = %#v, want checkpoint_id=cp-ctx and empty as_node", cap.updateReqs[0])
	}
	if cap.updateReqs[1].CheckpointID != "cp-ctx" || cap.updateReqs[1].AsNode != "worker" {
		t.Fatalf("second update req = %#v, want checkpoint_id=cp-ctx and as_node=worker", cap.updateReqs[1])
	}
}

func TestRemoteGraph_AssistantIntrospection(t *testing.T) {
	t.Parallel()

	type captures struct {
		sync.Mutex
		graphXray        string
		subgraphsRecurse string
		subgraphsPath    string
	}
	cap := &captures{}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case r.Method == http.MethodGet && path == "/v1/assistants/assistant-1/graph":
			cap.Lock()
			cap.graphXray = r.URL.Query().Get("xray")
			cap.Unlock()
			writeJSON(t, w, http.StatusOK, map[string]any{"nodes": []any{"a", "b"}})
		case r.Method == http.MethodGet && path == "/v1/assistants/assistant-1/schemas":
			writeJSON(t, w, http.StatusOK, map[string]any{"input": map[string]any{"type": "object"}})
		case r.Method == http.MethodGet && strings.HasPrefix(path, "/v1/assistants/assistant-1/subgraphs"):
			cap.Lock()
			cap.subgraphsPath = path
			cap.subgraphsRecurse = r.URL.Query().Get("recurse")
			cap.Unlock()
			writeJSON(t, w, http.StatusOK, map[string]any{"subgraphs": []any{map[string]any{"namespace": "foo"}}})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer ts.Close()

	client, err := sdk.New(sdk.Config{BaseURL: ts.URL})
	if err != nil {
		t.Fatalf("sdk.New: %v", err)
	}

	remote, err := graph.NewRemoteGraph(client, graph.RemoteGraphOptions{AssistantID: "assistant-1"})
	if err != nil {
		t.Fatalf("NewRemoteGraph: %v", err)
	}

	gotGraph, err := remote.GetGraph(context.Background(), true)
	if err != nil {
		t.Fatalf("GetGraph: %v", err)
	}
	if got, ok := gotGraph["nodes"].([]any); !ok || len(got) != 2 {
		t.Fatalf("GetGraph result = %#v", gotGraph)
	}

	gotSchemas, err := remote.GetSchemas(context.Background())
	if err != nil {
		t.Fatalf("GetSchemas: %v", err)
	}
	if gotSchemas["input"] == nil {
		t.Fatalf("GetSchemas result = %#v", gotSchemas)
	}

	gotSubgraphs, err := remote.GetSubgraphs(context.Background(), "foo", true)
	if err != nil {
		t.Fatalf("GetSubgraphs: %v", err)
	}
	if gotSubgraphs["subgraphs"] == nil {
		t.Fatalf("GetSubgraphs result = %#v", gotSubgraphs)
	}

	cap.Lock()
	defer cap.Unlock()
	if cap.graphXray != "true" {
		t.Fatalf("graph xray query = %q, want true", cap.graphXray)
	}
	if cap.subgraphsPath != "/v1/assistants/assistant-1/subgraphs/foo" {
		t.Fatalf("subgraphs path = %q, want /v1/assistants/assistant-1/subgraphs/foo", cap.subgraphsPath)
	}
	if cap.subgraphsRecurse != "true" {
		t.Fatalf("subgraphs recurse query = %q, want true", cap.subgraphsRecurse)
	}
}

func TestRemoteGraph_AssistantIntrospectionRequiresAssistantID(t *testing.T) {
	t.Parallel()

	client, err := sdk.New(sdk.Config{BaseURL: "http://example.com"})
	if err != nil {
		t.Fatalf("sdk.New: %v", err)
	}

	remote, err := graph.NewRemoteGraph(client, graph.RemoteGraphOptions{})
	if err != nil {
		t.Fatalf("NewRemoteGraph: %v", err)
	}

	if _, err := remote.GetSchemas(context.Background()); err == nil {
		t.Fatal("GetSchemas err = nil, want error")
	}
}

func TestRemoteGraph_StreamAndTypedStream(t *testing.T) {
	t.Parallel()

	type captures struct {
		sync.Mutex
		streamReqs []sdk.RunStreamRequest
	}
	cap := &captures{}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/runs/stream") {
			var req sdk.RunStreamRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode stream request: %v", err)
			}
			cap.Lock()
			cap.streamReqs = append(cap.streamReqs, req)
			cap.Unlock()

			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("event: values|root\ndata: {\"count\":1,\"__interrupt__\":[{\"id\":\"i1\",\"value\":\"need-input\"}]}\n\n"))
			_, _ = w.Write([]byte("event: updates|worker\ndata: {\"worker\":{\"count\":2}}\n\n"))
			_, _ = w.Write([]byte("event: end\ndata: {}\n\n"))
			return
		}
		t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
	}))
	defer ts.Close()

	client, err := sdk.New(sdk.Config{BaseURL: ts.URL})
	if err != nil {
		t.Fatalf("sdk.New: %v", err)
	}

	remote, err := graph.NewRemoteGraph(client, graph.RemoteGraphOptions{
		ThreadID:    "thread-s",
		AssistantID: "assistant-s",
		RunStream:   sdk.RunStreamRequest{Metadata: map[string]any{"source": "default"}},
	})
	if err != nil {
		t.Fatalf("NewRemoteGraph: %v", err)
	}

	ctx := context.Background()
	stream := remote.StreamDuplex(ctx, map[string]any{"message": "hi"}, graph.StreamModeValues, graph.StreamModeUpdates)
	parts := make([]graph.StreamPart, 0, 2)
	for part := range stream {
		if part.Err != nil {
			t.Fatalf("StreamDuplex err: %v", part.Err)
		}
		parts = append(parts, part)
	}
	if len(parts) != 2 {
		t.Fatalf("StreamDuplex part count = %d, want 2", len(parts))
	}
	if parts[0].Type != graph.StreamModeValues || len(parts[0].Interrupts) != 1 || parts[0].Interrupts[0].ID != "i1" {
		t.Fatalf("first part = %#v", parts[0])
	}
	if parts[1].Type != graph.StreamModeUpdates {
		t.Fatalf("second part type = %q, want updates", parts[1].Type)
	}

	typedParts, typedErrs := remote.StreamTyped(ctx, map[string]any{"message": "typed"}, graph.StreamModeValues)
	select {
	case part := <-typedParts:
		if _, ok := part.(sdk.ValuesStreamPart); !ok {
			t.Fatalf("typed stream part = %T, want sdk.ValuesStreamPart", part)
		}
	case err := <-typedErrs:
		t.Fatalf("typed stream error: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for typed stream")
	}

	cap.Lock()
	defer cap.Unlock()
	if len(cap.streamReqs) < 2 {
		t.Fatalf("stream request count = %d, want at least 2", len(cap.streamReqs))
	}
	if got := strings.Join(cap.streamReqs[0].StreamMode, ","); got != "values,updates" {
		t.Fatalf("duplex stream_mode = %q, want values,updates", got)
	}
	if cap.streamReqs[0].AssistantID != "assistant-s" {
		t.Fatalf("duplex assistant_id = %q, want assistant-s", cap.streamReqs[0].AssistantID)
	}
}

func TestRemoteGraph_ThreadlessInvokeAndStream(t *testing.T) {
	t.Parallel()

	type captures struct {
		sync.Mutex
		createRunPath    string
		streamCreatePath string
		getRunPath       string
	}
	cap := &captures{}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/runs":
			cap.Lock()
			cap.createRunPath = r.URL.Path
			cap.Unlock()
			writeJSON(t, w, http.StatusAccepted, map[string]any{
				"run_id":       "run-top",
				"thread_id":    "generated-thread",
				"assistant_id": "assistant-top",
				"status":       "queued",
				"created_at":   time.Now().UTC().Format(time.RFC3339Nano),
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/threads/generated-thread/runs/run-top":
			cap.Lock()
			cap.getRunPath = r.URL.Path
			cap.Unlock()
			writeJSON(t, w, http.StatusOK, map[string]any{
				"run_id":       "run-top",
				"thread_id":    "generated-thread",
				"assistant_id": "assistant-top",
				"status":       "completed",
				"output":       map[string]any{"result": "top-ok"},
				"created_at":   time.Now().UTC().Format(time.RFC3339Nano),
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/runs/stream":
			cap.Lock()
			cap.streamCreatePath = r.URL.Path
			cap.Unlock()
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("event: values\ndata: {\"count\":1}\n\n"))
			_, _ = w.Write([]byte("event: end\ndata: {}\n\n"))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer ts.Close()

	client, err := sdk.New(sdk.Config{BaseURL: ts.URL})
	if err != nil {
		t.Fatalf("sdk.New: %v", err)
	}

	remote, err := graph.NewRemoteGraph(client, graph.RemoteGraphOptions{AssistantID: "assistant-top"})
	if err != nil {
		t.Fatalf("NewRemoteGraph: %v", err)
	}

	out, err := remote.Invoke(context.Background(), map[string]any{"hello": "world"})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	value, ok := out.Value.(map[string]any)
	if !ok || value["result"] != "top-ok" {
		t.Fatalf("Invoke value = %#v", out.Value)
	}

	stream := remote.Stream(context.Background(), map[string]any{"hello": "stream"}, graph.StreamModeValues)
	parts := make([]graph.StreamPart, 0, 1)
	for part := range stream {
		if part.Err != nil {
			t.Fatalf("Stream err: %v", part.Err)
		}
		parts = append(parts, part)
	}
	if len(parts) != 1 || parts[0].Type != graph.StreamModeValues {
		t.Fatalf("stream parts = %#v", parts)
	}

	cap.Lock()
	defer cap.Unlock()
	if cap.createRunPath != "/v1/runs" {
		t.Fatalf("create run path = %q, want /v1/runs", cap.createRunPath)
	}
	if cap.getRunPath != "/v1/threads/generated-thread/runs/run-top" {
		t.Fatalf("get run path = %q, want generated thread path", cap.getRunPath)
	}
	if cap.streamCreatePath != "/v1/runs/stream" {
		t.Fatalf("stream create path = %q, want /v1/runs/stream", cap.streamCreatePath)
	}
}

func pathPart(path string, index int) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if index < 0 || index >= len(parts) {
		return ""
	}
	return parts[index]
}

func writeJSON(t *testing.T, w http.ResponseWriter, status int, body any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		t.Fatalf("encode json response: %v", err)
	}
}
