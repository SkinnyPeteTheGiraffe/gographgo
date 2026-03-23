package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDevPassesRuntimeConfigEnvToServerCommand(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	configPath := filepath.Join(dir, "langgraph.json")
	configBody := `{
		"graphs":{"agent":"./agent.go:Graph"},
		"server":{"command":"env"},
		"checkpointer":{"backend":"sqlite","path":"./state.db"},
		"auth":{"mode":"bearer","api_key":"secret"},
		"store":{"backend":"memory"}
	}`
	if err := os.WriteFile(configPath, []byte(configBody), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	stdout := new(bytes.Buffer)
	stderr := new(bytes.Buffer)
	err := Dev(context.Background(), stdout, stderr, DevOptions{ConfigPath: configPath})
	if err != nil {
		t.Fatalf("Dev() error = %v (stderr: %s)", err, stderr.String())
	}
	out := stdout.String()
	for _, want := range []string{
		`GOGRAPHGO_CHECKPOINTER={"backend":"sqlite","path":"./state.db"}`,
		`GOGRAPHGO_AUTH={"api_key":"secret","mode":"bearer"}`,
		`GOGRAPHGO_STORE={"backend":"memory"}`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected %q in command env output, got: %s", want, out)
		}
	}
}
