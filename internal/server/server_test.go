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

	resp, err := http.Get(httpSrv.URL + "/v1/threads/" + thread.ID + "/runs/" + run.ID + "/events")
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

func postJSON[T any](t *testing.T, url, body string, wantStatus int) T {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
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
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, url, strings.NewReader(body))
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
	resp, err := http.Get(url)
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
