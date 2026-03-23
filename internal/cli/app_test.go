package cli

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestMainVersion(t *testing.T) {
	t.Parallel()
	out := new(bytes.Buffer)
	errOut := new(bytes.Buffer)
	code := Main(context.Background(), []string{"--version"}, out, errOut)
	if code != 0 {
		t.Fatalf("exit code = %d", code)
	}
	if !strings.Contains(out.String(), "gographgo graph") {
		t.Fatalf("unexpected version output %q", out.String())
	}
}

func TestMainUnknownCommand(t *testing.T) {
	t.Parallel()
	out := new(bytes.Buffer)
	errOut := new(bytes.Buffer)
	code := Main(context.Background(), []string{"wat"}, out, errOut)
	if code != 2 {
		t.Fatalf("exit code = %d", code)
	}
	if !strings.Contains(errOut.String(), "unknown command") {
		t.Fatalf("unexpected stderr output %q", errOut.String())
	}
}

func TestMainHelpIncludesDockerCommands(t *testing.T) {
	t.Parallel()
	out := new(bytes.Buffer)
	errOut := new(bytes.Buffer)
	code := Main(context.Background(), []string{"help"}, out, errOut)
	if code != 0 {
		t.Fatalf("exit code = %d", code)
	}
	help := out.String()
	for _, cmd := range []string{"up", "build", "deploy", "dockerfile"} {
		if !strings.Contains(help, cmd) {
			t.Fatalf("help missing command %q: %s", cmd, help)
		}
	}
}
