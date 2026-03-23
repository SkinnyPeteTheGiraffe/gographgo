package cli

import (
	"bytes"
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/SkinnyPeteTheGiraffe/gographgo/internal/server"
	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/checkpoint"
)

func TestRun(t *testing.T) {
	t.Parallel()
	srv := server.New(server.Options{Checkpointer: checkpoint.NewInMemorySaver()})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	dir := t.TempDir()
	configPath := filepath.Join(dir, "langgraph.json")
	configBody := `{"graphs":{"agent":"./agent.py:graph"},"server":{"url":"` + ts.URL + `"},"run":{"thread_id":"thread-a"}}`
	if err := os.WriteFile(configPath, []byte(configBody), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	buf := new(bytes.Buffer)
	err := Run(context.Background(), buf, RunOptions{
		ConfigPath: configPath,
		GraphID:    "agent",
		InputJSON:  `{"hello":"world"}`,
		Wait:       true,
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, `"status": "success"`) {
		t.Fatalf("expected success run output, got: %s", out)
	}
	if !strings.Contains(out, `"thread_id": "thread-a"`) {
		t.Fatalf("expected configured thread id in output, got: %s", out)
	}
}

func TestRunUsesAuthConfigAPIKey(t *testing.T) {
	t.Parallel()
	srv := server.New(server.Options{Checkpointer: checkpoint.NewInMemorySaver(), APIKey: "secret"})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	dir := t.TempDir()
	configPath := filepath.Join(dir, "langgraph.json")
	configBody := `{
		"graphs":{"agent":"./agent.py:graph"},
		"server":{"url":"` + ts.URL + `"},
		"auth":{"mode":"bearer","api_key":"secret"}
	}`
	if err := os.WriteFile(configPath, []byte(configBody), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	buf := new(bytes.Buffer)
	err := Run(context.Background(), buf, RunOptions{
		ConfigPath: configPath,
		GraphID:    "agent",
		InputJSON:  `{"hello":"world"}`,
		Wait:       true,
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if !strings.Contains(buf.String(), `"status": "success"`) {
		t.Fatalf("expected success run output, got: %s", buf.String())
	}
}
