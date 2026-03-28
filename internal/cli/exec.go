package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"strings"
)

// RunCommand executes a command with args and optional environment overrides.
func RunCommand(ctx context.Context, stdout, stderr io.Writer, name string, args []string, env map[string]string) error {
	cmdName := strings.TrimSpace(name)
	if cmdName == "" {
		return fmt.Errorf("empty command")
	}

	// #nosec G204 -- CLI execution intentionally runs user-selected commands.
	cmd := exec.CommandContext(ctx, cmdName, args...)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.Stdin = os.Stdin
	cmd.Env = mergeEnv(os.Environ(), env)

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("command failed: %w", err)
	}
	return nil
}

// RunShellCommand executes a shell command with optional environment overrides.
func RunShellCommand(ctx context.Context, stdout, stderr io.Writer, command string, env map[string]string) error {
	cmdText := strings.TrimSpace(command)
	if cmdText == "" {
		return fmt.Errorf("empty command")
	}

	// #nosec G204 -- CLI shell mode intentionally executes configured shell text.
	cmd := exec.CommandContext(ctx, "sh", "-c", cmdText)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.Stdin = os.Stdin
	cmd.Env = mergeEnv(os.Environ(), env)

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("command failed: %w", err)
	}
	return nil
}

func mergeEnv(base []string, extra map[string]string) []string {
	if len(extra) == 0 {
		return base
	}
	seen := map[string]int{}
	out := append([]string(nil), base...)
	for i, kv := range out {
		if idx := strings.IndexByte(kv, '='); idx > 0 {
			seen[kv[:idx]] = i
		}
	}
	keys := make([]string, 0, len(extra))
	for k := range extra {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := extra[k]
		kv := k + "=" + v
		if i, ok := seen[k]; ok {
			out[i] = kv
		} else {
			seen[k] = len(out)
			out = append(out, kv)
		}
	}
	return out
}
