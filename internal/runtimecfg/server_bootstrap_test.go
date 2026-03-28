package runtimecfg

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/checkpoint"
)

func TestLoadServerBootstrapDefaults(t *testing.T) {
	t.Parallel()

	bootstrap, err := LoadServerBootstrap(func(string) string { return "" })
	if err != nil {
		t.Fatalf("LoadServerBootstrap() error = %v", err)
	}
	if bootstrap.Addr != ":8080" {
		t.Fatalf("addr = %q, want :8080", bootstrap.Addr)
	}
	if bootstrap.Checkpointer == nil {
		t.Fatalf("checkpointer is nil, want in-memory saver")
	}
}

func TestLoadServerBootstrapSQLiteAndAuth(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "checkpoints.db")
	env := map[string]string{
		"GOGRAPHGO_CHECKPOINTER": fmt.Sprintf(`{"backend":"sqlite","path":%q}`, dbPath),
		"GOGRAPHGO_AUTH":         `{"mode":"bearer","api_key":"secret"}`,
	}
	bootstrap, err := LoadServerBootstrap(getFromMap(env))
	if err != nil {
		t.Fatalf("LoadServerBootstrap() error = %v", err)
	}
	if closer, ok := bootstrap.Checkpointer.(interface{ Close() error }); ok {
		t.Cleanup(func() {
			if closeErr := closer.Close(); closeErr != nil {
				t.Fatalf("checkpointer close error = %v", closeErr)
			}
		})
	}
	if bootstrap.APIKey != "secret" {
		t.Fatalf("api key = %q, want secret", bootstrap.APIKey)
	}
	if got := fmt.Sprintf("%T", bootstrap.Checkpointer); !strings.Contains(got, "sqlite.Saver") {
		t.Fatalf("checkpointer type = %s, want sqlite saver", got)
	}
	if _, err := bootstrap.Checkpointer.List(context.Background(), nil, checkpoint.ListOptions{}); err != nil {
		t.Fatalf("sqlite checkpointer list error = %v", err)
	}
}

func TestLoadServerBootstrapRejectsUnsupportedStoreBackend(t *testing.T) {
	t.Parallel()

	env := map[string]string{"GOGRAPHGO_STORE": `{"backend":"postgres"}`}
	_, err := LoadServerBootstrap(getFromMap(env))
	if err == nil || !strings.Contains(err.Error(), "unsupported store backend") {
		t.Fatalf("expected unsupported store backend error, got %v", err)
	}
}

func TestLoadServerBootstrapRejectsUnsupportedAuthMode(t *testing.T) {
	t.Parallel()

	env := map[string]string{"GOGRAPHGO_AUTH": `{"mode":"oauth"}`}
	_, err := LoadServerBootstrap(getFromMap(env))
	if err == nil || !strings.Contains(err.Error(), "unsupported auth mode") {
		t.Fatalf("expected unsupported auth mode error, got %v", err)
	}
}

func TestLoadServerBootstrapDisablePersistence(t *testing.T) {
	t.Parallel()

	env := map[string]string{"GOGRAPHGO_DISABLE_PERSISTENCE": "true"}
	bootstrap, err := LoadServerBootstrap(getFromMap(env))
	if err != nil {
		t.Fatalf("LoadServerBootstrap() error = %v", err)
	}
	if bootstrap.Checkpointer != nil {
		t.Fatalf("checkpointer = %T, want nil when persistence disabled", bootstrap.Checkpointer)
	}
}

func getFromMap(values map[string]string) func(string) string {
	return func(key string) string {
		return values[key]
	}
}
