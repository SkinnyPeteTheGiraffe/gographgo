package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
)

const Version = "dev"

// Main runs the graph CLI and returns an exit code.
func Main(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}

	if len(args) == 0 {
		printUsage(stderr)
		return 2
	}

	switch args[0] {
	case "help", "-h", "--help":
		printUsage(stdout)
		return 0
	case "version", "--version":
		_, _ = fmt.Fprintf(stdout, "gographgo graph %s\n", Version)
		return 0
	case "new":
		return runNewCommand(args[1:], stdout, stderr)
	case "run":
		return runRunCommand(ctx, args[1:], stdout, stderr)
	case "dev":
		return runDevCommand(ctx, args[1:], stdout, stderr)
	case "up":
		return runUpCommand(ctx, args[1:], stdout, stderr)
	case "build":
		return runBuildCommand(ctx, args[1:], stdout, stderr)
	case "deploy":
		return runDeployCommand(ctx, args[1:], stdout, stderr)
	case "dockerfile":
		return runDockerfileCommand(ctx, args[1:], stdout, stderr)
	default:
		_, _ = fmt.Fprintf(stderr, "unknown command %q\n\n", args[0])
		printUsage(stderr)
		return 2
	}
}

func runNewCommand(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("new", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var templateID string
	fs.StringVar(&templateID, "template", "", "template id")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	path := "."
	if fs.NArg() > 0 {
		path = fs.Arg(0)
	}
	if fs.NArg() > 1 {
		_, _ = fmt.Fprintln(stderr, "new accepts at most one path argument")
		return 2
	}
	if err := CreateNew(path, templateID, stdout); err != nil {
		_, _ = fmt.Fprintln(stderr, err.Error())
		return 1
	}
	return 0
}

func runRunCommand(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var opts RunOptions
	fs.StringVar(&opts.ConfigPath, "config", "", "config file path")
	fs.StringVar(&opts.GraphID, "graph", "", "graph id")
	fs.StringVar(&opts.AssistantID, "assistant-id", "", "assistant id")
	fs.StringVar(&opts.ThreadID, "thread-id", "", "thread id")
	fs.StringVar(&opts.InputJSON, "input", "{}", "JSON object input")
	fs.BoolVar(&opts.Wait, "wait", true, "wait for completion")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() > 0 {
		_, _ = fmt.Fprintln(stderr, "run does not accept positional arguments")
		return 2
	}
	if err := Run(ctx, stdout, opts); err != nil {
		_, _ = fmt.Fprintln(stderr, err.Error())
		return 1
	}
	return 0
}

func runDevCommand(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("dev", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var opts DevOptions
	fs.StringVar(&opts.ConfigPath, "config", "", "config file path")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() > 0 {
		_, _ = fmt.Fprintln(stderr, "dev does not accept positional arguments")
		return 2
	}
	if err := Dev(ctx, stdout, stderr, opts); err != nil {
		return reportCommandError(stderr, err)
	}
	return 0
}

func runUpCommand(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("up", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var opts UpOptions
	fs.StringVar(&opts.ConfigPath, "config", "", "config file path")
	fs.StringVar(&opts.ComposeFile, "compose-file", "", "docker compose file path")
	fs.IntVar(&opts.Port, "port", 8080, "api port exposed by compose stack")
	fs.BoolVar(&opts.Recreate, "recreate", false, "force container recreation")
	fs.BoolVar(&opts.Pull, "pull", false, "pull newer base images")
	fs.BoolVar(&opts.Wait, "wait", false, "wait for all services to be healthy")
	fs.BoolVar(&opts.Detach, "detach", false, "run in detached mode")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() > 0 {
		_, _ = fmt.Fprintln(stderr, "up does not accept positional arguments")
		return 2
	}
	if err := Up(ctx, stdout, stderr, opts); err != nil {
		return reportCommandError(stderr, err)
	}
	return 0
}

func runBuildCommand(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("build", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var opts BuildOptions
	var buildArgs stringSliceFlag
	fs.StringVar(&opts.ConfigPath, "config", "", "config file path")
	fs.StringVar(&opts.Tag, "tag", "", "image tag, for example ghcr.io/acme/server:latest")
	fs.StringVar(&opts.Dockerfile, "file", "", "dockerfile path")
	fs.StringVar(&opts.ContextPath, "context", "", "docker build context path")
	fs.BoolVar(&opts.Pull, "pull", false, "always attempt to pull newer base image")
	fs.BoolVar(&opts.NoCache, "no-cache", false, "disable docker build cache")
	fs.Var(&buildArgs, "build-arg", "build arg in KEY=VALUE format (repeatable)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() > 0 {
		_, _ = fmt.Fprintln(stderr, "build does not accept positional arguments")
		return 2
	}
	opts.BuildArgs = append(opts.BuildArgs, buildArgs...)
	if err := Build(ctx, stdout, stderr, opts); err != nil {
		return reportCommandError(stderr, err)
	}
	return 0
}

func runDeployCommand(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("deploy", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var opts DeployOptions
	var buildArgs stringSliceFlag
	fs.StringVar(&opts.ConfigPath, "config", "", "config file path")
	fs.StringVar(&opts.Tag, "tag", "", "published image tag")
	fs.StringVar(&opts.Dockerfile, "file", "", "dockerfile path")
	fs.StringVar(&opts.ContextPath, "context", "", "docker build context path")
	fs.BoolVar(&opts.Pull, "pull", false, "always attempt to pull newer base image")
	fs.BoolVar(&opts.NoCache, "no-cache", false, "disable docker build cache")
	fs.BoolVar(&opts.SkipBuild, "skip-build", false, "skip image build and only push")
	fs.BoolVar(&opts.DryRun, "dry-run", false, "print publish command without executing")
	fs.Var(&buildArgs, "build-arg", "build arg in KEY=VALUE format (repeatable)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() > 0 {
		_, _ = fmt.Fprintln(stderr, "deploy does not accept positional arguments")
		return 2
	}
	opts.BuildArgs = append(opts.BuildArgs, buildArgs...)
	if err := Deploy(ctx, stdout, stderr, opts); err != nil {
		return reportCommandError(stderr, err)
	}
	return 0
}

func runDockerfileCommand(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("dockerfile", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var opts DockerfileOptions
	fs.StringVar(&opts.ConfigPath, "config", "", "config file path")
	fs.StringVar(&opts.BuilderImage, "builder-image", "", "builder image")
	fs.StringVar(&opts.BaseImage, "base-image", "", "runtime base image")
	fs.BoolVar(&opts.Overwrite, "overwrite", false, "overwrite existing output file")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() > 1 {
		_, _ = fmt.Fprintln(stderr, "dockerfile accepts at most one output path")
		return 2
	}
	if fs.NArg() == 1 {
		opts.OutputPath = fs.Arg(0)
	}
	if err := Dockerfile(ctx, stdout, opts); err != nil {
		return reportCommandError(stderr, err)
	}
	return 0
}

func reportCommandError(stderr io.Writer, err error) int {
	if !errors.Is(err, context.Canceled) {
		_, _ = fmt.Fprintln(stderr, err.Error())
	}
	return 1
}

func printUsage(w io.Writer) {
	_, _ = fmt.Fprint(w, strings.TrimSpace(`Usage:
  graph <command> [flags]

Commands:
  new       Create a new project from a template
  run       Create a thread run against the configured server
  dev       Start the configured local server command
  up        Start the local container stack via Docker Compose
  build     Build a container image for the server runtime
  deploy    Publish a container image to your registry
  dockerfile Generate a Dockerfile for server runtime
  version   Print CLI version
  help      Show this help
`)+"\n")
}
