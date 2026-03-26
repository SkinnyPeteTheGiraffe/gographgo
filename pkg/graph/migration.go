package graph

import (
	"fmt"
	"strings"
)

type CheckpointMigrator interface {
	MigrateCheckpoint(checkpoint map[string]any) error
}

type ChannelMigration struct {
	OldPrefix string
	NewPrefix string
}

type VersionMigration struct {
	FromVersion int
	ToVersion   int
}

type CheckpointMigratorFunc func(checkpoint map[string]any) error

func (f CheckpointMigratorFunc) MigrateCheckpoint(checkpoint map[string]any) error {
	return f(checkpoint)
}

func DefaultCheckpointMigrator() CheckpointMigrator {
	return CheckpointMigratorFunc(func(checkpoint map[string]any) error {
		v, ok := checkpoint["v"].(int)
		if !ok {
			return fmt.Errorf("checkpoint missing version field")
		}

		if v >= 3 {
			return nil
		}

		values, _ := checkpoint["channel_values"].(map[string]any)
		versions, _ := checkpoint["channel_versions"].(map[string]any)
		seen, _ := checkpoint["versions_seen"].(map[string]any)

		if versions == nil {
			return nil
		}

		for k := range versions {
			if strings.HasPrefix(k, "start:") {
				node := strings.TrimPrefix(k, "start:")
				newK := fmt.Sprintf("branch:to:%s", node)
				migrateChannelKey(k, newK, values, versions, seen)
			} else if strings.HasPrefix(k, "branch:") && strings.Count(k, ":") == 3 {
				parts := strings.Split(k, ":")
				node := parts[len(parts)-1]
				newK := fmt.Sprintf("branch:to:%s", node)
				migrateChannelKey(k, newK, values, versions, seen)
			}
		}

		migrateNodeToBranch(values, versions, seen)

		checkpoint["v"] = 3
		return nil
	})
}

func migrateChannelKey(oldK, newK string, values, versions, seen map[string]any) {
	if versions == nil {
		return
	}

	newV := versions[oldK]
	delete(versions, oldK)
	if existingV, exists := versions[newK]; exists {
		versions[newK] = maxVersion(existingV, newV)
	} else {
		versions[newK] = newV
	}

	if values != nil {
		if v, exists := values[oldK]; exists {
			delete(values, oldK)
			if _, exists := values[newK]; !exists {
				values[newK] = v
			}
		}
	}

	for _, nodeSeen := range seen {
		if nodeSeenMap, ok := nodeSeen.(map[string]any); ok {
			if v, exists := nodeSeenMap[oldK]; exists {
				delete(nodeSeenMap, oldK)
				if _, exists := nodeSeenMap[newK]; !exists {
					nodeSeenMap[newK] = v
				}
			}
		}
	}
}

func migrateNodeToBranch(values, versions, seen map[string]any) {
	if versions == nil {
		return
	}

	nodeKeys := make([]string, 0, len(versions))
	for k := range versions {
		if !strings.HasPrefix(k, "branch:") && !strings.HasPrefix(k, "start:") && k != Start && k != End {
			nodeKeys = append(nodeKeys, k)
		}
	}

	if len(nodeKeys) == 0 {
		return
	}

	for _, k := range nodeKeys {
		newK := fmt.Sprintf("branch:to:%s", k)
		migrateChannelKey(k, newK, values, versions, seen)
	}
}

func maxVersion(a, b any) any {
	av, aOk := toFloat64(a)
	bv, bOk := toFloat64(b)
	if aOk && bOk {
		if av > bv {
			return a
		}
		return b
	}
	if aOk {
		return a
	}
	return b
}

func toFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case float64:
		return n, true
	case string:
		var f float64
		_, err := fmt.Sscanf(n, "%f", &f)
		return f, err == nil
	default:
		return 0, false
	}
}

type MultiMigrator struct {
	migrators []CheckpointMigrator
}

func NewMultiMigrator(migrators ...CheckpointMigrator) *MultiMigrator {
	return &MultiMigrator{migrators: migrators}
}

func (m *MultiMigrator) MigrateCheckpoint(checkpoint map[string]any) error {
	for _, migrator := range m.migrators {
		if err := migrator.MigrateCheckpoint(checkpoint); err != nil {
			return err
		}
	}
	return nil
}
