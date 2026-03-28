package runtimecfg

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/checkpoint"
	checkpointpostgres "github.com/SkinnyPeteTheGiraffe/gographgo/pkg/checkpoint/postgres"
	checkpointsqlite "github.com/SkinnyPeteTheGiraffe/gographgo/pkg/checkpoint/sqlite"
)

type ServerBootstrap struct {
	Addr         string
	Checkpointer checkpoint.Saver
	APIKey       string
}

func LoadServerBootstrapFromEnv() (ServerBootstrap, error) {
	return LoadServerBootstrap(os.Getenv)
}

func LoadServerBootstrap(getenv func(string) string) (ServerBootstrap, error) {
	addr := strings.TrimSpace(getenv("GOGRAPHGO_SERVER_ADDR"))
	if addr == "" {
		addr = ":8080"
	}

	disablePersistence, err := parseBoolEnv(getenv, "GOGRAPHGO_DISABLE_PERSISTENCE")
	if err != nil {
		return ServerBootstrap{}, err
	}

	storeCfg, err := parseObjectEnv(getenv, "GOGRAPHGO_STORE")
	if err != nil {
		return ServerBootstrap{}, err
	}
	if err := validateStoreConfig(storeCfg); err != nil {
		return ServerBootstrap{}, err
	}

	authCfg, err := parseObjectEnv(getenv, "GOGRAPHGO_AUTH")
	if err != nil {
		return ServerBootstrap{}, err
	}
	apiKey, err := APIKeyFromAuthConfig(authCfg, getenv("GOGRAPHGO_API_KEY"))
	if err != nil {
		return ServerBootstrap{}, err
	}

	checkpointerCfg, err := parseObjectEnv(getenv, "GOGRAPHGO_CHECKPOINTER")
	if err != nil {
		return ServerBootstrap{}, err
	}
	checkpointer, err := checkpointerFromConfig(checkpointerCfg, disablePersistence)
	if err != nil {
		return ServerBootstrap{}, err
	}

	return ServerBootstrap{Addr: addr, Checkpointer: checkpointer, APIKey: apiKey}, nil
}

func APIKeyFromAuthConfig(auth map[string]any, fallbackAPIKey string) (string, error) {
	key, err := optionalString(auth, "api_key", "apikey", "key", "token", "value")
	if err != nil {
		return "", err
	}
	if key == "" {
		key = strings.TrimSpace(fallbackAPIKey)
	}

	mode, err := optionalString(auth, "mode", "type")
	if err != nil {
		return "", err
	}
	mode = strings.ToLower(mode)

	switch mode {
	case "", "bearer", "api_key", "apikey", "token":
		return key, nil
	case "none", "off", "disabled":
		if key != "" {
			return "", fmt.Errorf("auth mode %q cannot include an API key", mode)
		}
		return "", nil
	default:
		return "", fmt.Errorf("unsupported auth mode %q", mode)
	}
}

func checkpointerFromConfig(cfg map[string]any, disablePersistence bool) (checkpoint.Saver, error) {
	backend, err := configBackend(cfg, "memory")
	if err != nil {
		return nil, err
	}

	if disablePersistence {
		if backend != "" && backend != "memory" {
			return nil, fmt.Errorf("disable_persistence cannot be combined with %q checkpointer backend", backend)
		}
		return nil, nil
	}

	switch backend {
	case "", "memory", "in_memory", "inmemory":
		return checkpoint.NewInMemorySaver(), nil
	case "sqlite":
		conn, err := optionalString(cfg, "dsn", "path", "url", "connection", "conn")
		if err != nil {
			return nil, err
		}
		conn = strings.TrimSpace(conn)
		if conn == "" {
			conn = "file:gographgo-checkpoints.db"
		}
		return checkpointsqlite.Open(conn, checkpoint.JSONSerializer{})
	case "postgres", "postgresql":
		conn, err := optionalString(cfg, "dsn", "url", "connection", "conn", "database_url")
		if err != nil {
			return nil, err
		}
		conn = strings.TrimSpace(conn)
		if conn == "" {
			return nil, fmt.Errorf("postgres checkpointer requires connection string in one of: dsn, url, connection, conn, database_url")
		}
		return checkpointpostgres.OpenAutoMigrate(conn, checkpoint.JSONSerializer{})
	default:
		return nil, fmt.Errorf("unsupported checkpointer backend %q", backend)
	}
}

func validateStoreConfig(cfg map[string]any) error {
	backend, err := configBackend(cfg, "memory")
	if err != nil {
		return err
	}
	if backend == "" || backend == "memory" || backend == "in_memory" || backend == "inmemory" {
		return nil
	}
	return fmt.Errorf("unsupported store backend %q for local Go server runtime", backend)
}

func configBackend(cfg map[string]any, fallback string) (string, error) {
	backend, err := optionalString(cfg, "backend", "type", "driver")
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(backend) == "" {
		return fallback, nil
	}
	return strings.ToLower(strings.TrimSpace(backend)), nil
}

func parseObjectEnv(getenv func(string) string, key string) (map[string]any, error) {
	raw := strings.TrimSpace(getenv(key))
	if raw == "" {
		return nil, nil
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, fmt.Errorf("invalid %s: %w", key, err)
	}
	if out == nil {
		return nil, fmt.Errorf("invalid %s: expected JSON object", key)
	}
	return out, nil
}

func parseBoolEnv(getenv func(string) string, key string) (bool, error) {
	raw := strings.TrimSpace(getenv(key))
	if raw == "" {
		return false, nil
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("invalid %s: %w", key, err)
	}
	return v, nil
}

func optionalString(m map[string]any, keys ...string) (string, error) {
	if len(m) == 0 {
		return "", nil
	}
	for _, key := range keys {
		value, ok := m[key]
		if !ok {
			continue
		}
		s, ok := value.(string)
		if !ok {
			return "", fmt.Errorf("invalid %q: expected string", key)
		}
		s = strings.TrimSpace(s)
		if s != "" {
			return s, nil
		}
	}
	return "", nil
}
