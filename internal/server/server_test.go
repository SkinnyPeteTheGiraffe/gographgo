package server_test

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/SkinnyPeteTheGiraffe/gographgo/internal/server"
	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/checkpoint"
)

type testRunner struct{}

func (testRunner) Run(_ context.Context, req server.RunRequest) (server.RunResult, error) {
	msg := ""
	if req.Input != nil {
		if m, ok := req.Input["message"].(string); ok {
			msg = m
		}
	}
	return server.RunResult{
		Output: map[string]any{"echo": msg},
		State:  map[string]any{"last_message": msg},
	}, nil
}

func TestServer_RunStateHistoryStoreAndEvents(t *testing.T) {
	s := server.New(server.Options{Checkpointer: checkpoint.NewInMemorySaver(), Runner: testRunner{}})
	httpSrv := httptest.NewServer(s.Handler())
	defer httpSrv.Close()

	thread := postJSON[server.Thread](t, httpSrv.URL+"/v1/threads", `{"metadata":{"owner":"test"}}`, http.StatusCreated)
	if thread.ID == "" {
		t.Fatal("thread id empty")
	}

	run := postJSON[server.Run](t, httpSrv.URL+"/v1/threads/"+thread.ID+"/runs", `{"assistant_id":"a1","input":{"message":"hi"}}`, http.StatusAccepted)
	if run.Status != server.RunStatusPending {
		t.Fatalf("run status = %s, want pending", run.Status)
	}

	final := waitRun(t, httpSrv.URL, thread.ID, run.ID)
	if final.Status != server.RunStatusSuccess {
		t.Fatalf("final status = %s, want success", final.Status)
	}
	if final.Output["echo"] != "hi" {
		t.Fatalf("output = %v, want echo=hi", final.Output)
	}

	state := getJSON[server.ThreadState](t, httpSrv.URL+"/v1/threads/"+thread.ID+"/state", http.StatusOK)
	if state.Values["last_message"] != "hi" {
		t.Fatalf("state = %v, want last_message=hi", state.Values)
	}
	if state.CheckpointID == "" {
		t.Fatal("expected checkpoint id")
	}

	history := getJSON[[]checkpoint.CheckpointTuple](t, httpSrv.URL+"/v1/threads/"+thread.ID+"/history", http.StatusOK)
	if len(history) == 0 {
		t.Fatal("expected checkpoint history")
	}

	putJSONNoResp(t, httpSrv.URL+"/v1/threads/"+thread.ID+"/store/profile/name", `{"value":{"first":"Ada"}}`, http.StatusOK)
	storeVal := getJSON[server.StoreValue](t, httpSrv.URL+"/v1/threads/"+thread.ID+"/store/profile/name", http.StatusOK)
	if storeVal.Value["first"] != "Ada" {
		t.Fatalf("store value = %v, want first=Ada", storeVal.Value)
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, httpSrv.URL+"/v1/threads/"+thread.ID+"/runs/"+run.ID+"/events", nil)
	if err != nil {
		t.Fatalf("new events request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("events get: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	reader := bufio.NewScanner(resp.Body)
	foundCompleted := false
	deadline := time.After(2 * time.Second)
	for !foundCompleted {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for run.completed event")
		default:
		}
		if !reader.Scan() {
			break
		}
		line := reader.Text()
		if strings.HasPrefix(line, "event: run.completed") {
			foundCompleted = true
		}
	}
	if !foundCompleted {
		t.Fatal("did not receive run.completed event")
	}
}

func TestServer_ThreadStoreRejectsUnsupportedOptions(t *testing.T) {
	s := server.New(server.Options{})
	httpSrv := httptest.NewServer(s.Handler())
	defer httpSrv.Close()

	thread := postJSON[server.Thread](t, httpSrv.URL+"/v1/threads", `{}`, http.StatusCreated)

	putReq, err := http.NewRequestWithContext(context.Background(), http.MethodPut, httpSrv.URL+"/v1/threads/"+thread.ID+"/store/profile/name", strings.NewReader(`{"value":{"first":"Ada"},"index":{"fields":["first"]}}`))
	if err != nil {
		t.Fatalf("new put request: %v", err)
	}
	putReq.Header.Set("Content-Type", "application/json")
	putResp, err := http.DefaultClient.Do(putReq)
	if err != nil {
		t.Fatalf("put request: %v", err)
	}
	defer func() { _ = putResp.Body.Close() }()
	if putResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("put status = %d, want %d", putResp.StatusCode, http.StatusBadRequest)
	}

	putJSONNoResp(t, httpSrv.URL+"/v1/threads/"+thread.ID+"/store/profile/name", `{"value":{"first":"Ada"}}`, http.StatusOK)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, httpSrv.URL+"/v1/threads/"+thread.ID+"/store/profile/name?refresh_ttl=true", nil)
	if err != nil {
		t.Fatalf("new get request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("get status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
}

func TestServer_ThreadStoreNamespaces(t *testing.T) {
	s := server.New(server.Options{})
	httpSrv := httptest.NewServer(s.Handler())
	defer httpSrv.Close()

	thread := postJSON[server.Thread](t, httpSrv.URL+"/v1/threads", `{}`, http.StatusCreated)

	putJSONNoResp(t, httpSrv.URL+"/v1/threads/"+thread.ID+"/store/profile.user/name", `{"value":{"first":"Ada"}}`, http.StatusOK)
	putJSONNoResp(t, httpSrv.URL+"/v1/threads/"+thread.ID+"/store/profile.session/token", `{"value":{"token":"abc"}}`, http.StatusOK)

	ns := postJSON[map[string][][]string](t, httpSrv.URL+"/v1/threads/"+thread.ID+"/store/namespaces", `{"prefix":["profile"]}`, http.StatusOK)
	if len(ns["namespaces"]) != 2 {
		t.Fatalf("namespaces = %#v, want 2 entries", ns["namespaces"])
	}

	bad := postJSON[map[string]any](t, httpSrv.URL+"/v1/threads/"+thread.ID+"/store/namespaces", `{"max_depth":1}`, http.StatusBadRequest)
	if bad["error"] == nil {
		t.Fatalf("bad response = %#v, want error", bad)
	}
}

func postJSON[T any](t *testing.T, url, body string, wantStatus int) T {
	ctx := context.Background()
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != wantStatus {
		t.Fatalf("status = %d, want %d", resp.StatusCode, wantStatus)
	}
	var out T
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return out
}

func putJSONNoResp(t *testing.T, url, body string, wantStatus int) {
	ctx := context.Background()
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != wantStatus {
		t.Fatalf("status = %d, want %d", resp.StatusCode, wantStatus)
	}
}

func getJSON[T any](t *testing.T, url string, wantStatus int) T {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != wantStatus {
		t.Fatalf("status = %d, want %d", resp.StatusCode, wantStatus)
	}
	var out T
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return out
}

func waitRun(t *testing.T, baseURL, threadID, runID string) server.Run {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		run := getJSON[server.Run](t, baseURL+"/v1/threads/"+threadID+"/runs/"+runID, http.StatusOK)
		if run.Status == server.RunStatusSuccess || run.Status == server.RunStatusError || run.Status == server.RunStatusInterrupted || run.Status == server.RunStatusTimeout {
			return run
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("timed out waiting for run completion")
	return server.Run{}
}
