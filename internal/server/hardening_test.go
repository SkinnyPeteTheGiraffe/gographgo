package server_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/SkinnyPeteTheGiraffe/gographgo/internal/server"
	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/checkpoint"
)

type failingRunner struct{}

func (failingRunner) Run(_ context.Context, _ server.RunRequest) (server.RunResult, error) {
	return server.RunResult{}, errors.New("runner exploded")
}

type failingSaver struct{}

func (failingSaver) GetTuple(_ context.Context, _ *checkpoint.Config) (*checkpoint.CheckpointTuple, error) {
	return nil, errors.New("checkpoint backend unavailable")
}

func (failingSaver) Put(_ context.Context, config *checkpoint.Config, _ *checkpoint.Checkpoint, _ *checkpoint.CheckpointMetadata) (*checkpoint.Config, error) {
	return config, errors.New("checkpoint write failed")
}

func (failingSaver) PutWrites(_ context.Context, _ *checkpoint.Config, _ []checkpoint.PendingWrite, _ string) error {
	return errors.New("write failed")
}

func (failingSaver) List(_ context.Context, _ *checkpoint.Config, _ checkpoint.ListOptions) ([]*checkpoint.CheckpointTuple, error) {
	return nil, errors.New("history unavailable")
}

func (failingSaver) DeleteThread(_ context.Context, _ string) error {
	return errors.New("delete thread failed")
}

func (failingSaver) DeleteForRuns(_ context.Context, _ []string) error {
	return errors.New("delete runs failed")
}

func (failingSaver) CopyThread(_ context.Context, _, _ string) error {
	return errors.New("copy thread failed")
}

func (failingSaver) Prune(_ context.Context, _ []string, _ checkpoint.PruneStrategy) error {
	return errors.New("prune failed")
}

func TestServer_RunFailureStatus(t *testing.T) {
	s := server.New(server.Options{Runner: failingRunner{}, Checkpointer: checkpoint.NewInMemorySaver()})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	thread := postJSON[server.Thread](t, ts.URL+"/v1/threads", `{}`, http.StatusCreated)
	run := postJSON[server.Run](t, ts.URL+"/v1/threads/"+thread.ID+"/runs", `{"input":{"x":1}}`, http.StatusAccepted)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		current := getJSON[server.Run](t, ts.URL+"/v1/threads/"+thread.ID+"/runs/"+run.ID, http.StatusOK)
		if current.Status == server.RunStatusError {
			if !strings.Contains(current.Error, "runner exploded") {
				t.Fatalf("run error = %q", current.Error)
			}
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("timed out waiting for failed run")
}

func TestServer_StateErrorWhenCheckpointerFails(t *testing.T) {
	s := server.New(server.Options{Runner: testRunner{}, Checkpointer: failingSaver{}})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	thread := postJSON[server.Thread](t, ts.URL+"/v1/threads", `{}`, http.StatusCreated)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, ts.URL+"/v1/threads/"+thread.ID+"/state", nil)
	if err != nil {
		t.Fatalf("new GET /state request = %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /state error = %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusInternalServerError)
	}
}

func TestServer_BadJSONReturns400(t *testing.T) {
	s := server.New(server.Options{})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, ts.URL+"/v1/threads", strings.NewReader(`{"thread_id":1}`))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
}
