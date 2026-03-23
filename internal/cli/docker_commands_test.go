package cli

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type recordedCommand struct {
	name string
	args []string
	env  map[string]string
}

func TestDockerfile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "langgraph.json")
	if err := os.WriteFile(configPath, []byte(`{"graphs":{"agent":"./agent.py:graph"},"dockerfile_lines":["RUN apt-get update -y"]}`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	outputPath := filepath.Join(dir, "Dockerfile")
	out := new(bytes.Buffer)
	err := Dockerfile(context.Background(), out, DockerfileOptions{
		ConfigPath: configPath,
		OutputPath: outputPath,
	})
	if err != nil {
		t.Fatalf("Dockerfile() error = %v", err)
	}
	b, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read dockerfile: %v", err)
	}
	content := string(b)
	if !strings.Contains(content, "go build -o /out/server ./cmd/server") {
		t.Fatalf("expected build command in Dockerfile, got: %s", content)
	}
	if !strings.Contains(content, "RUN apt-get update -y") {
		t.Fatalf("expected custom dockerfile line in Dockerfile, got: %s", content)
	}
	if !strings.Contains(out.String(), outputPath) {
		t.Fatalf("expected output path in output, got: %q", out.String())
	}
}

func TestBuildWithRunner(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "langgraph.json")
	if err := os.WriteFile(configPath, []byte(`{"graphs":{"agent":"./agent.py:graph"}}`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	dockerfilePath := filepath.Join(dir, "Dockerfile")
	if err := os.WriteFile(dockerfilePath, []byte("FROM scratch\n"), 0o644); err != nil {
		t.Fatalf("write dockerfile: %v", err)
	}

	var calls []recordedCommand
	runner := func(_ context.Context, _ io.Writer, _ io.Writer, name string, args []string, env map[string]string) error {
		copied := append([]string(nil), args...)
		calls = append(calls, recordedCommand{name: name, args: copied, env: env})
		return nil
	}

	stdout := new(bytes.Buffer)
	stderr := new(bytes.Buffer)
	err := buildWithRunner(context.Background(), stdout, stderr, BuildOptions{
		ConfigPath:  configPath,
		Tag:         "registry.example.com/team/server:latest",
		Dockerfile:  dockerfilePath,
		ContextPath: dir,
		Pull:        true,
		NoCache:     true,
		BuildArgs:   []string{"A=1"},
	}, runner)
	if err != nil {
		t.Fatalf("buildWithRunner() error = %v", err)
	}
	if len(calls) != 1 {
		t.Fatalf("calls = %d, want 1", len(calls))
	}
	if calls[0].name != "docker" {
		t.Fatalf("command = %q, want docker", calls[0].name)
	}
	joined := strings.Join(calls[0].args, " ")
	for _, want := range []string{"build", "-t", "registry.example.com/team/server:latest", "-f", dockerfilePath, "--pull", "--no-cache", "--build-arg", "A=1", dir} {
		if !strings.Contains(joined, want) {
			t.Fatalf("expected %q in args %q", want, joined)
		}
	}
}

func TestUpWithRunner(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "langgraph.json")
	configBody := `{"graphs":{"agent":"./agent.py:graph"},"env":{"A":"1"}}`
	if err := os.WriteFile(configPath, []byte(configBody), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	composePath := filepath.Join(dir, "docker-compose.yml")
	if err := os.WriteFile(composePath, []byte("services: {}\n"), 0o644); err != nil {
		t.Fatalf("write compose file: %v", err)
	}

	var got recordedCommand
	runner := func(_ context.Context, _ io.Writer, _ io.Writer, name string, args []string, env map[string]string) error {
		got = recordedCommand{name: name, args: append([]string(nil), args...), env: env}
		return nil
	}

	err := upWithRunner(context.Background(), io.Discard, io.Discard, UpOptions{
		ConfigPath:    configPath,
		ComposeFile:   composePath,
		ComposeBinary: "docker",
		Port:          9001,
		Recreate:      true,
		Pull:          true,
		Wait:          true,
		Detach:        true,
	}, runner)
	if err != nil {
		t.Fatalf("upWithRunner() error = %v", err)
	}
	if got.name != "docker" {
		t.Fatalf("command = %q, want docker", got.name)
	}
	joined := strings.Join(got.args, " ")
	for _, want := range []string{"compose", "-f", composePath, "up", "--remove-orphans", "--force-recreate", "--renew-anon-volumes", "--pull", "always", "--wait", "--detach"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("expected %q in args %q", want, joined)
		}
	}
	if got.env["A"] != "1" {
		t.Fatalf("env A = %q", got.env["A"])
	}
	if got.env["PORT"] != "9001" {
		t.Fatalf("env PORT = %q", got.env["PORT"])
	}
}

func TestDeployWithRunner(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "langgraph.json")
	if err := os.WriteFile(configPath, []byte(`{"graphs":{"agent":"./agent.py:graph"}}`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	dockerfilePath := filepath.Join(dir, "Dockerfile")
	if err := os.WriteFile(dockerfilePath, []byte("FROM scratch\n"), 0o644); err != nil {
		t.Fatalf("write dockerfile: %v", err)
	}

	var calls []recordedCommand
	runner := func(_ context.Context, _ io.Writer, _ io.Writer, name string, args []string, env map[string]string) error {
		calls = append(calls, recordedCommand{name: name, args: append([]string(nil), args...), env: env})
		return nil
	}

	err := deployWithRunner(context.Background(), io.Discard, io.Discard, DeployOptions{
		BuildOptions: BuildOptions{
			ConfigPath:  configPath,
			Tag:         "registry.example.com/team/server:latest",
			Dockerfile:  dockerfilePath,
			ContextPath: dir,
		},
	}, runner)
	if err != nil {
		t.Fatalf("deployWithRunner() error = %v", err)
	}
	if len(calls) != 2 {
		t.Fatalf("calls = %d, want 2", len(calls))
	}
	if calls[0].name != "docker" || len(calls[0].args) == 0 || calls[0].args[0] != "build" {
		t.Fatalf("first call = %#v, want docker build", calls[0])
	}
	if calls[1].name != "docker" || len(calls[1].args) != 2 || calls[1].args[0] != "push" {
		t.Fatalf("second call = %#v, want docker push", calls[1])
	}
}
