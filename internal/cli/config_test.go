package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadConfigDefaults(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "langgraph.json")
	data := `{"graphs":{"agent":"./agent.py:graph"}}`
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, gotPath, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if gotPath != path {
		t.Fatalf("config path = %q, want %q", gotPath, path)
	}
	if cfg.Server.URL != "http://127.0.0.1:8080" {
		t.Fatalf("server url = %q", cfg.Server.URL)
	}
	if cfg.Server.Command != "go run ./cmd/server" {
		t.Fatalf("server command = %q", cfg.Server.Command)
	}
	if cfg.Run.ThreadID != "default" {
		t.Fatalf("thread id = %q", cfg.Run.ThreadID)
	}
}

func TestResolveEnv(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	envPath := filepath.Join(dir, ".env")
	if err := os.WriteFile(envPath, []byte("A=1\nB=2\n"), 0o644); err != nil {
		t.Fatalf("write env: %v", err)
	}

	cfg := &Config{
		EnvFile: ".env",
		Env:     map[string]string{"B": "override", "C": "3"},
	}
	out, err := ResolveEnv(cfg, filepath.Join(dir, "langgraph.json"))
	if err != nil {
		t.Fatalf("ResolveEnv() error = %v", err)
	}
	if out["A"] != "1" || out["B"] != "override" || out["C"] != "3" {
		t.Fatalf("unexpected merged env: %#v", out)
	}
}

func TestLoadConfigRejectsMissingGraphs(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "langgraph.json")
	if err := os.WriteFile(path, []byte(`{"name":"x"}`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	_, _, err := LoadConfig(path)
	if err == nil || !strings.Contains(err.Error(), "graphs must not be empty") {
		t.Fatalf("expected graphs error, got %v", err)
	}
}

func TestLoadConfigExtendedFields(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "langgraph.json")
	data := `{
		"graphs":{"agent":"./agent.go:Graph"},
		"dependencies":["./shared", "github.com/acme/toolkit"],
		"dockerfile_lines":["RUN apt-get update", "ENV MODE=prod"],
		"store":{"driver":"sqlite","dsn":"file:data.db"},
		"auth":{"mode":"bearer"},
		"http":{"disable_runs":true},
		"ui":{"dashboard":"./ui/dashboard"},
		"webhooks":{"url":{"require_https":true}},
		"checkpointer":{"backend":"sqlite"},
		"disable_persistence":true
	}`
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, _, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	if len(cfg.Dependencies) != 2 {
		t.Fatalf("dependencies len = %d, want 2", len(cfg.Dependencies))
	}
	if len(cfg.DockerfileLines) != 2 {
		t.Fatalf("dockerfile_lines len = %d, want 2", len(cfg.DockerfileLines))
	}
	if cfg.Store["driver"] != "sqlite" {
		t.Fatalf("store.driver = %#v", cfg.Store["driver"])
	}
	if cfg.Auth["mode"] != "bearer" {
		t.Fatalf("auth.mode = %#v", cfg.Auth["mode"])
	}
	if cfg.HTTP["disable_runs"] != true {
		t.Fatalf("http.disable_runs = %#v", cfg.HTTP["disable_runs"])
	}
	if cfg.UI["dashboard"] != "./ui/dashboard" {
		t.Fatalf("ui.dashboard = %#v", cfg.UI["dashboard"])
	}
	if cfg.Checkpointer["backend"] != "sqlite" {
		t.Fatalf("checkpointer.backend = %#v", cfg.Checkpointer["backend"])
	}
	if !cfg.DisablePersistence {
		t.Fatalf("disable_persistence = false, want true")
	}
}

func TestLoadConfigRejectsNonGoRuntimeFields(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		body string
		want string
	}{
		{name: "python_version", body: `{"graphs":{"agent":"./agent.go:Graph"},"python_version":"3.12"}`, want: "python_version is not supported"},
		{name: "node_version", body: `{"graphs":{"agent":"./agent.go:Graph"},"node_version":"20"}`, want: "node_version is not supported"},
		{name: "pip_config_file", body: `{"graphs":{"agent":"./agent.go:Graph"},"pip_config_file":"./pip.conf"}`, want: "pip_config_file is not supported"},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			path := filepath.Join(dir, "langgraph.json")
			if err := os.WriteFile(path, []byte(tt.body), 0o644); err != nil {
				t.Fatalf("write config: %v", err)
			}
			_, _, err := LoadConfig(path)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("expected %q error, got %v", tt.want, err)
			}
		})
	}
}

func TestResolveEnvInjectsRuntimeConfig(t *testing.T) {
	t.Parallel()
	cfg := &Config{
		Store:              map[string]any{"driver": "memory"},
		Auth:               map[string]any{"mode": "bearer"},
		HTTP:               map[string]any{"disable_threads": true},
		UI:                 map[string]any{"app": "./ui"},
		Webhooks:           map[string]any{"url": map[string]any{"require_https": true}},
		Checkpointer:       map[string]any{"backend": "sqlite"},
		Dependencies:       []string{"./shared"},
		DisablePersistence: true,
	}
	out, err := ResolveEnv(cfg, filepath.Join(t.TempDir(), "langgraph.json"))
	if err != nil {
		t.Fatalf("ResolveEnv() error = %v", err)
	}
	for _, key := range []string{
		"GOGRAPHGO_STORE",
		"GOGRAPHGO_AUTH",
		"GOGRAPHGO_HTTP",
		"GOGRAPHGO_UI",
		"GOGRAPHGO_WEBHOOKS",
		"GOGRAPHGO_CHECKPOINTER",
		"GOGRAPHGO_DEPENDENCIES",
	} {
		if out[key] == "" {
			t.Fatalf("expected %s to be set", key)
		}
	}
	if out["GOGRAPHGO_DISABLE_PERSISTENCE"] != "true" {
		t.Fatalf("GOGRAPHGO_DISABLE_PERSISTENCE = %q", out["GOGRAPHGO_DISABLE_PERSISTENCE"])
	}
}
