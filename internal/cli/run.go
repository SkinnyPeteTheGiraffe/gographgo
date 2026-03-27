package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/SkinnyPeteTheGiraffe/gographgo/internal/runtimecfg"
	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/sdk"
)

// RunOptions configures the `graph run` command.
type RunOptions struct {
	ConfigPath  string
	GraphID     string
	AssistantID string
	ThreadID    string
	InputJSON   string
	Wait        bool
}

// Run executes a thread run and writes the run JSON to stdout.
func Run(ctx context.Context, stdout io.Writer, opts RunOptions) error {
	cfg, cfgPath, err := LoadConfig(opts.ConfigPath)
	if err != nil {
		return err
	}

	graphID, graphRef, err := resolveGraphSelection(cfg, opts.GraphID)
	if err != nil {
		return err
	}

	assistantID := strings.TrimSpace(opts.AssistantID)
	if assistantID == "" {
		assistantID = strings.TrimSpace(cfg.Run.AssistantID)
	}
	if assistantID == "" {
		assistantID = graphID
	}

	threadID := strings.TrimSpace(opts.ThreadID)
	if threadID == "" {
		threadID = cfg.Run.ThreadID
	}
	if threadID == "" {
		threadID = "default"
	}

	input, err := parseInputObject(opts.InputJSON)
	if err != nil {
		return err
	}
	env, err := ResolveEnv(cfg, cfgPath)
	if err != nil {
		return err
	}
	apiKey, err := runtimecfg.APIKeyFromAuthConfig(cfg.Auth, env["GOGRAPHGO_API_KEY"])
	if err != nil {
		return err
	}

	client, err := sdk.New(sdk.Config{BaseURL: cfg.Server.URL, APIKey: apiKey})
	if err != nil {
		return err
	}

	_, _ = client.CreateThread(ctx, threadID, map[string]any{"graph_id": graphID, "config": cfgPath})

	run, err := client.CreateRun(ctx, threadID, assistantID, input, map[string]any{"graph_id": graphID, "graph_ref": graphRef})
	if err != nil {
		return err
	}

	if opts.Wait {
		run, err = client.WaitRun(ctx, threadID, run.ID, 100*time.Millisecond)
		if err != nil {
			return err
		}
	}

	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(run)
}

func resolveGraphSelection(cfg *Config, graphID string) (selectedID string, graphRef string, err error) {
	want := strings.TrimSpace(graphID)
	if want != "" {
		ref, ok := cfg.Graphs[want]
		if !ok {
			return "", "", fmt.Errorf("unknown graph %q", want)
		}
		return want, ref, nil
	}

	if len(cfg.Graphs) == 1 {
		for id, ref := range cfg.Graphs {
			return id, ref, nil
		}
	}

	ids := make([]string, 0, len(cfg.Graphs))
	for id := range cfg.Graphs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return "", "", fmt.Errorf("multiple graphs configured; pass --graph (%s)", strings.Join(ids, ", "))
}

func parseInputObject(raw string) (map[string]any, error) {
	if strings.TrimSpace(raw) == "" {
		return map[string]any{}, nil
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, fmt.Errorf("invalid --input JSON object: %w", err)
	}
	if out == nil {
		return map[string]any{}, nil
	}
	return out, nil
}
