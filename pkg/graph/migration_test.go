package graph

import (
	"fmt"
	"testing"
)

func TestDefaultCheckpointMigrator_AlreadyCurrent(t *testing.T) {
	migrator := DefaultCheckpointMigrator()

	checkpoint := map[string]any{
		"v":                3,
		"channel_values":   map[string]any{"foo": "bar"},
		"channel_versions": map[string]any{"branch:to:node1": 5},
		"versions_seen":    map[string]any{},
	}

	if err := migrator.MigrateCheckpoint(checkpoint); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if checkpoint["v"] != 3 {
		t.Errorf("expected version to remain 3, got %v", checkpoint["v"])
	}
}

func TestDefaultCheckpointMigrator_StartPrefixMigration(t *testing.T) {
	migrator := DefaultCheckpointMigrator()

	checkpoint := map[string]any{
		"v":                2,
		"channel_values":   map[string]any{"node1": "value1", "node2": "value2"},
		"channel_versions": map[string]any{"start:node1": 5, "start:node2": 3},
		"versions_seen": map[string]any{
			"node1": map[string]any{"start:node1": 5},
			"node2": map[string]any{"start:node2": 3},
		},
	}

	if err := migrator.MigrateCheckpoint(checkpoint); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if checkpoint["v"] != 3 {
		t.Errorf("expected version to be 3, got %v", checkpoint["v"])
	}

	versions, ok := checkpoint["channel_versions"].(map[string]any)
	if !ok {
		t.Fatal("channel_versions is not a map")
	}

	if _, exists := versions["start:node1"]; exists {
		t.Error("expected start:node1 to be migrated away")
	}
	if v, exists := versions["branch:to:node1"]; !exists || v != 5 {
		t.Errorf("expected branch:to:node1=5, got %v", v)
	}

	seen, ok := checkpoint["versions_seen"].(map[string]any)
	if !ok {
		t.Fatal("versions_seen is not a map")
	}

	for nodeName, nodeSeen := range seen {
		nodeSeenMap, ok := nodeSeen.(map[string]any)
		if !ok {
			t.Fatalf("versions_seen[%s] is not a map", nodeName)
		}
		expectedPrefix := fmt.Sprintf("start:%s", nodeName)
		if _, exists := nodeSeenMap[expectedPrefix]; exists {
			t.Errorf("versions_seen[%s] still has %s", nodeName, expectedPrefix)
		}
		expectedNewPrefix := fmt.Sprintf("branch:to:%s", nodeName)
		if _, exists := nodeSeenMap[expectedNewPrefix]; !exists {
			t.Errorf("versions_seen[%s] missing %s", nodeName, expectedNewPrefix)
		}
	}
}

func TestDefaultCheckpointMigrator_BranchThreeColonMigration(t *testing.T) {
	migrator := DefaultCheckpointMigrator()

	checkpoint := map[string]any{
		"v":                2,
		"channel_values":   map[string]any{},
		"channel_versions": map[string]any{"branch:foo:bar:node1": 5},
		"versions_seen": map[string]any{
			"node1": map[string]any{"branch:foo:bar:node1": 5},
		},
	}

	if err := migrator.MigrateCheckpoint(checkpoint); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	versions, ok := checkpoint["channel_versions"].(map[string]any)
	if !ok {
		t.Fatal("channel_versions is not a map")
	}

	if _, exists := versions["branch:foo:bar:node1"]; exists {
		t.Error("expected branch:foo:bar:node1 to be migrated")
	}
	if v, exists := versions["branch:to:node1"]; !exists || v != 5 {
		t.Errorf("expected branch:to:node1=5, got %v", v)
	}
}

func TestDefaultCheckpointMigrator_NodeKeyMigration(t *testing.T) {
	migrator := DefaultCheckpointMigrator()

	checkpoint := map[string]any{
		"v":                2,
		"channel_values":   map[string]any{"worker": "done"},
		"channel_versions": map[string]any{"worker": 4},
		"versions_seen": map[string]any{
			"worker":        map[string]any{"worker": 4},
			pregelInterrupt: map[string]any{"worker": 3},
		},
	}

	if err := migrator.MigrateCheckpoint(checkpoint); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	versions, ok := checkpoint["channel_versions"].(map[string]any)
	if !ok {
		t.Fatal("channel_versions is not a map")
	}
	if _, exists := versions["worker"]; exists {
		t.Fatal("expected worker version key to be migrated")
	}
	if v, exists := versions["branch:to:worker"]; !exists || v != 4 {
		t.Fatalf("expected branch:to:worker=4, got %v", v)
	}

	values, ok := checkpoint["channel_values"].(map[string]any)
	if !ok {
		t.Fatal("channel_values is not a map")
	}
	if _, exists := values["worker"]; exists {
		t.Fatal("expected worker value key to be migrated")
	}
	if v, exists := values["branch:to:worker"]; !exists || v != "done" {
		t.Fatalf("expected branch:to:worker value 'done', got %v", v)
	}

	seen, ok := checkpoint["versions_seen"].(map[string]any)
	if !ok {
		t.Fatal("versions_seen is not a map")
	}
	for key, raw := range seen {
		nodeSeen, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("versions_seen[%s] is not a map", key)
		}
		if _, exists := nodeSeen["worker"]; exists {
			t.Fatalf("versions_seen[%s] still has worker", key)
		}
		if _, exists := nodeSeen["branch:to:worker"]; !exists {
			t.Fatalf("versions_seen[%s] missing branch:to:worker", key)
		}
	}
}

func TestMultiMigrator(t *testing.T) {
	migrator := NewMultiMigrator(
		DefaultCheckpointMigrator(),
		DefaultCheckpointMigrator(),
	)

	checkpoint := map[string]any{
		"v":                2,
		"channel_values":   map[string]any{},
		"channel_versions": map[string]any{"start:node1": 5},
		"versions_seen":    map[string]any{},
	}

	if err := migrator.MigrateCheckpoint(checkpoint); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if checkpoint["v"] != 3 {
		t.Errorf("expected version to be 3, got %v", checkpoint["v"])
	}
}

func TestMultiMigrator_StopsOnError(t *testing.T) {
	errMigrator := CheckpointMigratorFunc(func(_ map[string]any) error {
		return fmt.Errorf("migration error")
	})

	migrator := NewMultiMigrator(errMigrator, DefaultCheckpointMigrator())

	checkpoint := map[string]any{
		"v":                2,
		"channel_values":   map[string]any{},
		"channel_versions": map[string]any{"start:node1": 5},
		"versions_seen":    map[string]any{},
	}

	if err := migrator.MigrateCheckpoint(checkpoint); err == nil {
		t.Fatal("expected error, got nil")
	}
}
