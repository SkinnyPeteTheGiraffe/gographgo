package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

type commandRunner func(ctx context.Context, stdout, stderr io.Writer, name string, args []string, env map[string]string) error

type stringSliceFlag []string

func (s *stringSliceFlag) String() string {
	if s == nil {
		return ""
	}
	return strings.Join(*s, ",")
}

func (s *stringSliceFlag) Set(value string) error {
	v := strings.TrimSpace(value)
	if v == "" {
		return fmt.Errorf("value must not be empty")
	}
	*s = append(*s, v)
	return nil
}

// UpOptions configures the `graph up` command.
type UpOptions struct {
	ConfigPath    string
	ComposeFile   string
	ComposeBinary string
	Port          int
	Recreate      bool
	Pull          bool
	Wait          bool
	Detach        bool
}

// Up starts the local container stack using Docker Compose.
func Up(ctx context.Context, stdout, stderr io.Writer, opts UpOptions) error {
	return upWithRunner(ctx, stdout, stderr, opts, RunCommand)
}

func upWithRunner(ctx context.Context, stdout, stderr io.Writer, opts UpOptions, runner commandRunner) error {
	cfg, cfgPath, err := LoadConfig(opts.ConfigPath)
	if err != nil {
		return err
	}
	env, err := ResolveEnv(cfg, cfgPath)
	if err != nil {
		return err
	}

	composeFile := strings.TrimSpace(opts.ComposeFile)
	if composeFile == "" {
		composeFile = filepath.Join(filepath.Dir(cfgPath), "docker-compose.yml")
	}
	composeFile, err = filepath.Abs(composeFile)
	if err != nil {
		return err
	}
	if _, err := os.Stat(composeFile); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("compose file not found at %s", composeFile)
		}
		return err
	}

	cmdName, cmdPrefix, err := detectComposeCommand(opts.ComposeBinary)
	if err != nil {
		return err
	}

	if opts.Port > 0 {
		env["PORT"] = strconv.Itoa(opts.Port)
	}

	if _, err := fmt.Fprintf(stderr, "starting container stack with %s\n", composeFile); err != nil {
		return err
	}

	args := append([]string{}, cmdPrefix...)
	args = append(args, "-f", composeFile, "up", "--remove-orphans")
	if opts.Recreate {
		args = append(args, "--force-recreate", "--renew-anon-volumes")
	}
	if opts.Pull {
		args = append(args, "--pull", "always")
	}
	if opts.Wait {
		args = append(args, "--wait")
	}
	if opts.Detach {
		args = append(args, "--detach")
	} else {
		args = append(args, "--abort-on-container-exit")
	}

	return runner(ctx, stdout, stderr, cmdName, args, env)
}

func detectComposeCommand(preferred string) (string, []string, error) {
	want := strings.TrimSpace(preferred)
	if want != "" {
		if want == "docker-compose" {
			return "docker-compose", nil, nil
		}
		if want == "docker" {
			return "docker", []string{"compose"}, nil
		}
		return "", nil, fmt.Errorf("unsupported compose binary %q", want)
	}
	if _, err := exec.LookPath("docker"); err == nil {
		return "docker", []string{"compose"}, nil
	}
	if _, err := exec.LookPath("docker-compose"); err == nil {
		return "docker-compose", nil, nil
	}
	return "", nil, fmt.Errorf("docker compose is required but was not found")
}

// BuildOptions configures the `graph build` command.
type BuildOptions struct {
	ConfigPath  string
	Tag         string
	Dockerfile  string
	ContextPath string
	BuildArgs   []string
	Pull        bool
	NoCache     bool
}

// Build builds a container image for the local server runtime.
func Build(ctx context.Context, stdout, stderr io.Writer, opts BuildOptions) error {
	return buildWithRunner(ctx, stdout, stderr, opts, RunCommand)
}

func buildWithRunner(ctx context.Context, stdout, stderr io.Writer, opts BuildOptions, runner commandRunner) error {
	_, cfgPath, err := LoadConfig(opts.ConfigPath)
	if err != nil {
		return err
	}

	tag := strings.TrimSpace(opts.Tag)
	if tag == "" {
		return fmt.Errorf("--tag is required")
	}

	contextPath := strings.TrimSpace(opts.ContextPath)
	if contextPath == "" {
		contextPath = filepath.Dir(cfgPath)
	}
	contextPath, err = filepath.Abs(contextPath)
	if err != nil {
		return err
	}

	dockerfilePath := strings.TrimSpace(opts.Dockerfile)
	if dockerfilePath == "" {
		dockerfilePath = filepath.Join(contextPath, "Dockerfile")
	}
	dockerfilePath, err = filepath.Abs(dockerfilePath)
	if err != nil {
		return err
	}
	if _, err := os.Stat(dockerfilePath); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("dockerfile not found at %s (run `graph dockerfile %s` first)", dockerfilePath, dockerfilePath)
		}
		return err
	}

	if _, err := fmt.Fprintf(stderr, "building image %s\n", tag); err != nil {
		return err
	}

	args := []string{"build", "-t", tag, "-f", dockerfilePath}
	if opts.Pull {
		args = append(args, "--pull")
	}
	if opts.NoCache {
		args = append(args, "--no-cache")
	}
	for _, item := range opts.BuildArgs {
		if strings.TrimSpace(item) == "" {
			continue
		}
		args = append(args, "--build-arg", item)
	}
	args = append(args, contextPath)

	return runner(ctx, stdout, stderr, "docker", args, nil)
}

// DeployOptions configures the `graph deploy` command.
type DeployOptions struct {
	BuildOptions
	SkipBuild bool
	DryRun    bool
}

// Deploy builds and publishes a container image to the configured registry.
func Deploy(ctx context.Context, stdout, stderr io.Writer, opts DeployOptions) error {
	return deployWithRunner(ctx, stdout, stderr, opts, RunCommand)
}

func deployWithRunner(ctx context.Context, stdout, stderr io.Writer, opts DeployOptions, runner commandRunner) error {
	tag := strings.TrimSpace(opts.Tag)
	if tag == "" {
		return fmt.Errorf("--tag is required")
	}

	if !opts.SkipBuild {
		if err := buildWithRunner(ctx, stdout, stderr, opts.BuildOptions, runner); err != nil {
			return err
		}
	}

	if _, err := fmt.Fprintf(stderr, "publishing image %s\n", tag); err != nil {
		return err
	}
	if opts.DryRun {
		_, _ = fmt.Fprintf(stdout, "docker push %s\n", tag)
		return nil
	}

	return runner(ctx, stdout, stderr, "docker", []string{"push", tag}, nil)
}

// DockerfileOptions configures the `graph dockerfile` command.
type DockerfileOptions struct {
	ConfigPath   string
	OutputPath   string
	BuilderImage string
	BaseImage    string
	Overwrite    bool
}

// Dockerfile writes a production Dockerfile for the server runtime.
func Dockerfile(_ context.Context, stdout io.Writer, opts DockerfileOptions) error {
	cfg, cfgPath, err := LoadConfig(opts.ConfigPath)
	if err != nil {
		return err
	}

	builderImage := strings.TrimSpace(opts.BuilderImage)
	if builderImage == "" {
		builderImage = "golang:1.24-alpine"
	}
	baseImage := strings.TrimSpace(opts.BaseImage)
	if baseImage == "" {
		baseImage = "gcr.io/distroless/base-debian12:nonroot"
	}

	outputPath := strings.TrimSpace(opts.OutputPath)
	if outputPath == "" {
		outputPath = filepath.Join(filepath.Dir(cfgPath), "Dockerfile")
	}
	outputPath, err = filepath.Abs(outputPath)
	if err != nil {
		return err
	}

	if _, err := os.Stat(outputPath); err == nil && !opts.Overwrite {
		return fmt.Errorf("file already exists at %s (pass --overwrite to replace)", outputPath)
	}
	if err := os.MkdirAll(filepath.Dir(outputPath), 0o750); err != nil {
		return err
	}

	content := renderDockerfile(builderImage, baseImage, cfg.DockerfileLines)
	if err := os.WriteFile(outputPath, []byte(content), 0o600); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(stdout, "wrote %s\n", outputPath)
	return nil
}

func renderDockerfile(builderImage, baseImage string, dockerfileLines []string) string {
	extra := ""
	if len(dockerfileLines) > 0 {
		extra = strings.Join(dockerfileLines, "\n") + "\n\n"
	}
	return fmt.Sprintf(`FROM %s AS builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/server ./cmd/server

FROM %s
WORKDIR /app

%sCOPY --from=builder /out/server /usr/local/bin/server

EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/server"]
`, builderImage, baseImage, extra)
}
