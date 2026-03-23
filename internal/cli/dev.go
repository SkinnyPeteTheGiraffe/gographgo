package cli

import (
	"context"
	"fmt"
	"io"
)

// DevOptions configures the `graph dev` command.
type DevOptions struct {
	ConfigPath string
}

// Dev starts the configured local development server command.
func Dev(ctx context.Context, stdout, stderr io.Writer, opts DevOptions) error {
	cfg, cfgPath, err := LoadConfig(opts.ConfigPath)
	if err != nil {
		return err
	}
	env, err := ResolveEnv(cfg, cfgPath)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(stderr, "starting: %s\n", cfg.Server.Command); err != nil {
		return err
	}
	return RunShellCommand(ctx, stdout, stderr, cfg.Server.Command, env)
}
