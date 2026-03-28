package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const (
	// DefaultConfigFile is the default config filename used by the CLI.
	DefaultConfigFile = "langgraph.json"
)

// Config describes a project-level CLI configuration.
type Config struct {
	Webhooks           map[string]any    `json:"webhooks,omitempty"`
	Checkpointer       map[string]any    `json:"checkpointer,omitempty"`
	Env                map[string]string `json:"env,omitempty"`
	Store              map[string]any    `json:"store,omitempty"`
	Auth               map[string]any    `json:"auth,omitempty"`
	HTTP               map[string]any    `json:"http,omitempty"`
	UI                 map[string]any    `json:"ui,omitempty"`
	Graphs             map[string]string `json:"graphs,omitempty"`
	Run                RunConfig         `json:"run,omitempty"`
	Server             ServerConfig      `json:"server,omitempty"`
	PIPConfigFile      string            `json:"pip_config_file,omitempty"`
	PythonVersion      string            `json:"python_version,omitempty"`
	NodeVersion        string            `json:"node_version,omitempty"`
	Name               string            `json:"name,omitempty"`
	EnvFile            string            `json:"env_file,omitempty"`
	DockerfileLines    []string          `json:"dockerfile_lines,omitempty"`
	Dependencies       []string          `json:"dependencies,omitempty"`
	DisablePersistence bool              `json:"disable_persistence,omitempty"`
}

// ServerConfig contains server connectivity and startup options.
type ServerConfig struct {
	URL     string `json:"url,omitempty"`
	Command string `json:"command,omitempty"`
}

// RunConfig contains default run parameters.
type RunConfig struct {
	AssistantID string `json:"assistant_id,omitempty"`
	ThreadID    string `json:"thread_id,omitempty"`
}

// LoadConfig reads and validates config from a file path.
// Empty configPath resolves to DefaultConfigFile in cwd.
func LoadConfig(configPath string) (*Config, string, error) {
	if strings.TrimSpace(configPath) == "" {
		configPath = DefaultConfigFile
	}
	abs, err := filepath.Abs(configPath)
	if err != nil {
		return nil, "", err
	}
	b, err := os.ReadFile(abs)
	if err != nil {
		return nil, "", err
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return nil, "", fmt.Errorf("invalid JSON in %s: %w", abs, err)
	}

	cfg := defaultConfig()
	if err := decodeKnown(raw, &cfg); err != nil {
		return nil, "", err
	}
	if len(cfg.Graphs) == 0 {
		return nil, "", fmt.Errorf("config %s: graphs must not be empty", abs)
	}
	if cfg.Server.URL == "" {
		cfg.Server.URL = "http://127.0.0.1:8080"
	}
	if cfg.Server.Command == "" {
		cfg.Server.Command = "go run ./cmd/server"
	}
	if cfg.Run.ThreadID == "" {
		cfg.Run.ThreadID = "default"
	}
	if cfg.Name == "" {
		cfg.Name = filepath.Base(filepath.Dir(abs))
	}

	if err := validateConfig(cfg); err != nil {
		return nil, "", err
	}
	return &cfg, abs, nil
}

// ResolveEnv returns merged env vars from config inline map and env file.
// Inline map wins on conflicts.
func ResolveEnv(cfg *Config, configFilePath string) (map[string]string, error) {
	out := map[string]string{}
	if cfg == nil {
		return out, nil
	}
	if err := mergeEnvFile(out, cfg.EnvFile, configFilePath); err != nil {
		return nil, err
	}
	for k, v := range cfg.Env {
		out[k] = v
	}

	jsonEnv := []jsonEnvEntry{
		{value: cfg.Store, key: "GOGRAPHGO_STORE"},
		{value: cfg.Auth, key: "GOGRAPHGO_AUTH"},
		{value: cfg.HTTP, key: "GOGRAPHGO_HTTP"},
		{value: cfg.UI, key: "GOGRAPHGO_UI"},
		{value: cfg.Webhooks, key: "GOGRAPHGO_WEBHOOKS"},
		{value: cfg.Checkpointer, key: "GOGRAPHGO_CHECKPOINTER"},
	}
	if err := appendJSONEnvEntries(out, jsonEnv); err != nil {
		return nil, err
	}
	if len(cfg.Dependencies) > 0 {
		if err := appendJSONEnv(out, "GOGRAPHGO_DEPENDENCIES", cfg.Dependencies); err != nil {
			return nil, err
		}
	}
	if cfg.DisablePersistence {
		out["GOGRAPHGO_DISABLE_PERSISTENCE"] = "true"
	}
	return out, nil
}

// GraphIDs returns sorted graph IDs in config.
func GraphIDs(cfg *Config) []string {
	if cfg == nil {
		return nil
	}
	out := make([]string, 0, len(cfg.Graphs))
	for id := range cfg.Graphs {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func defaultConfig() Config {
	return Config{
		Graphs: map[string]string{},
		Env:    map[string]string{},
	}
}

func decodeKnown(raw map[string]json.RawMessage, cfg *Config) error {
	if err := decodeStandardConfigFields(raw, cfg); err != nil {
		return err
	}

	if v, ok := raw["env"]; ok {
		var envMap map[string]string
		if err := json.Unmarshal(v, &envMap); err == nil {
			cfg.Env = envMap
		} else {
			var envFile string
			if err := json.Unmarshal(v, &envFile); err != nil {
				return fmt.Errorf("invalid env: expected object or string path")
			}
			cfg.EnvFile = envFile
		}
	}
	return nil
}

func mergeEnvFile(out map[string]string, envFile, configFilePath string) error {
	if envFile == "" {
		return nil
	}
	base := filepath.Dir(configFilePath)
	path := envFile
	if !filepath.IsAbs(path) {
		path = filepath.Join(base, path)
	}
	vars, err := parseDotEnv(path)
	if err != nil {
		return err
	}
	for k, v := range vars {
		out[k] = v
	}
	return nil
}

type jsonEnvEntry struct {
	value any
	key   string
}

func appendJSONEnvEntries(out map[string]string, entries []jsonEnvEntry) error {
	for _, entry := range entries {
		if err := appendJSONEnv(out, entry.key, entry.value); err != nil {
			return err
		}
	}
	return nil
}

type knownFieldDecoder struct {
	decode func(json.RawMessage) error
	key    string
	label  string
}

func decodeStandardConfigFields(raw map[string]json.RawMessage, cfg *Config) error {
	decoders := []knownFieldDecoder{
		{key: "name", label: "name", decode: func(v json.RawMessage) error { return json.Unmarshal(v, &cfg.Name) }},
		{key: "graphs", label: "graphs", decode: func(v json.RawMessage) error { return json.Unmarshal(v, &cfg.Graphs) }},
		{key: "dependencies", label: "dependencies", decode: func(v json.RawMessage) error { return json.Unmarshal(v, &cfg.Dependencies) }},
		{key: "dockerfile_lines", label: "dockerfile_lines", decode: func(v json.RawMessage) error { return json.Unmarshal(v, &cfg.DockerfileLines) }},
		{key: "store", label: "store", decode: func(v json.RawMessage) error { return json.Unmarshal(v, &cfg.Store) }},
		{key: "auth", label: "auth", decode: func(v json.RawMessage) error { return json.Unmarshal(v, &cfg.Auth) }},
		{key: "http", label: "http", decode: func(v json.RawMessage) error { return json.Unmarshal(v, &cfg.HTTP) }},
		{key: "ui", label: "ui", decode: func(v json.RawMessage) error { return json.Unmarshal(v, &cfg.UI) }},
		{key: "webhooks", label: "webhooks", decode: func(v json.RawMessage) error { return json.Unmarshal(v, &cfg.Webhooks) }},
		{key: "checkpointer", label: "checkpointer", decode: func(v json.RawMessage) error { return json.Unmarshal(v, &cfg.Checkpointer) }},
		{key: "disable_persistence", label: "disable_persistence", decode: func(v json.RawMessage) error { return json.Unmarshal(v, &cfg.DisablePersistence) }},
		{key: "python_version", label: "python_version", decode: func(v json.RawMessage) error { return json.Unmarshal(v, &cfg.PythonVersion) }},
		{key: "node_version", label: "node_version", decode: func(v json.RawMessage) error { return json.Unmarshal(v, &cfg.NodeVersion) }},
		{key: "pip_config_file", label: "pip_config_file", decode: func(v json.RawMessage) error { return json.Unmarshal(v, &cfg.PIPConfigFile) }},
		{key: "server", label: "server", decode: func(v json.RawMessage) error { return json.Unmarshal(v, &cfg.Server) }},
		{key: "run", label: "run", decode: func(v json.RawMessage) error { return json.Unmarshal(v, &cfg.Run) }},
		{key: "env_file", label: "env_file", decode: func(v json.RawMessage) error { return json.Unmarshal(v, &cfg.EnvFile) }},
	}
	for _, decoder := range decoders {
		if err := decodeKnownField(raw, decoder); err != nil {
			return err
		}
	}
	return nil
}

func decodeKnownField(raw map[string]json.RawMessage, decoder knownFieldDecoder) error {
	v, ok := raw[decoder.key]
	if !ok {
		return nil
	}
	if err := decoder.decode(v); err != nil {
		return fmt.Errorf("invalid %s: %w", decoder.label, err)
	}
	return nil
}

func validateConfig(cfg Config) error {
	if strings.TrimSpace(cfg.PythonVersion) != "" {
		return fmt.Errorf("python_version is not supported by the Go runtime; remove this field")
	}
	if strings.TrimSpace(cfg.NodeVersion) != "" {
		return fmt.Errorf("node_version is not supported by the Go runtime; remove this field")
	}
	if strings.TrimSpace(cfg.PIPConfigFile) != "" {
		return fmt.Errorf("pip_config_file is not supported by the Go runtime; remove this field")
	}

	for id, p := range cfg.Graphs {
		if strings.TrimSpace(id) == "" {
			return fmt.Errorf("graphs key must not be empty")
		}
		if strings.TrimSpace(p) == "" {
			return fmt.Errorf("graph %q path must not be empty", id)
		}
		if !strings.Contains(p, ":") {
			return fmt.Errorf("graph %q path must use '<path>:<symbol>' format", id)
		}
	}
	if cfg.Server.URL != "" && !strings.HasPrefix(cfg.Server.URL, "http://") && !strings.HasPrefix(cfg.Server.URL, "https://") {
		return fmt.Errorf("server.url must start with http:// or https://")
	}
	for i, dep := range cfg.Dependencies {
		if strings.TrimSpace(dep) == "" {
			return fmt.Errorf("dependencies[%d] must not be empty", i)
		}
	}
	for i, line := range cfg.DockerfileLines {
		if strings.TrimSpace(line) == "" {
			return fmt.Errorf("dockerfile_lines[%d] must not be empty", i)
		}
	}
	return nil
}

func appendJSONEnv(out map[string]string, key string, value any) error {
	if value == nil {
		return nil
	}
	b, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal %s: %w", key, err)
	}
	out[key] = string(b)
	return nil
}

func parseDotEnv(path string) (map[string]string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read env file %s: %w", path, err)
	}
	lines := strings.Split(string(b), "\n")
	out := map[string]string{}
	for i, raw := range lines {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid env assignment at %s:%d", path, i+1)
		}
		k := strings.TrimSpace(parts[0])
		v := strings.TrimSpace(parts[1])
		v = strings.Trim(v, `"`)
		out[k] = v
	}
	return out, nil
}
