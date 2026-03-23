package checkpoint

import (
	"fmt"
	"strconv"
	"strings"
)

// Migrator applies checkpoint schema/data migrations in-place.
type Migrator interface {
	Migrate(*Checkpoint) error
}

// MigratorFunc adapts a function to Migrator.
type MigratorFunc func(*Checkpoint) error

// Migrate implements Migrator.
func (f MigratorFunc) Migrate(cp *Checkpoint) error {
	return f(cp)
}

// MultiMigrator runs migrations in order and stops at first error.
type MultiMigrator struct {
	migrators []Migrator
}

// NewMultiMigrator constructs a migration pipeline.
func NewMultiMigrator(migrators ...Migrator) *MultiMigrator {
	return &MultiMigrator{migrators: append([]Migrator(nil), migrators...)}
}

// Migrate applies all configured migrators.
func (m *MultiMigrator) Migrate(cp *Checkpoint) error {
	if m == nil {
		return nil
	}
	for _, migrator := range m.migrators {
		if err := migrator.Migrate(cp); err != nil {
			return err
		}
	}
	return nil
}

// DefaultMigrator provides baseline migration compatibility for historical
// checkpoint formats.
func DefaultMigrator() Migrator {
	return MigratorFunc(func(cp *Checkpoint) error {
		if cp == nil {
			return fmt.Errorf("checkpoint migrator: checkpoint must not be nil")
		}
		if cp.V <= 0 {
			cp.V = 1
		}
		if cp.ChannelValues == nil {
			cp.ChannelValues = map[string]any{}
		}
		if cp.ChannelVersions == nil {
			cp.ChannelVersions = map[string]Version{}
		}
		if cp.VersionsSeen == nil {
			cp.VersionsSeen = map[string]map[string]Version{}
		}
		if cp.UpdatedChannels == nil {
			cp.UpdatedChannels = nil
		}

		migrateChannelKeyMap(cp.ChannelValues)
		migrateVersionKeyMap(cp.ChannelVersions)
		for node, versions := range cp.VersionsSeen {
			if versions == nil {
				cp.VersionsSeen[node] = map[string]Version{}
				continue
			}
			migrateVersionKeyMap(versions)
		}

		cp.V = 1
		return nil
	})
}

func applyDefaultMigration(cp *Checkpoint) error {
	return DefaultMigrator().Migrate(cp)
}

// ApplyDefaultMigration applies built-in migrations to a checkpoint in-place.
func ApplyDefaultMigration(cp *Checkpoint) error {
	return applyDefaultMigration(cp)
}

func migrateChannelKeyMap(m map[string]any) {
	if m == nil {
		return
	}
	for k, v := range m {
		migrated := migrateChannelKey(k)
		if migrated == k {
			continue
		}
		delete(m, k)
		if _, exists := m[migrated]; !exists {
			m[migrated] = v
		}
	}
}

func migrateVersionKeyMap(m map[string]Version) {
	if m == nil {
		return
	}
	for k, v := range m {
		migrated := migrateChannelKey(k)
		if migrated == k {
			continue
		}
		delete(m, k)
		if existing, exists := m[migrated]; exists {
			if compareVersion(existing, v) < 0 {
				m[migrated] = v
			}
		} else {
			m[migrated] = v
		}
	}
}

func compareVersion(a, b Version) int {
	ai, aok := versionToFloat(a)
	bi, bok := versionToFloat(b)
	if aok && bok {
		switch {
		case ai < bi:
			return -1
		case ai > bi:
			return 1
		default:
			return 0
		}
	}
	as := fmt.Sprint(a)
	bs := fmt.Sprint(b)
	if as < bs {
		return -1
	}
	if as > bs {
		return 1
	}
	return 0
}

func versionToFloat(v Version) (float64, bool) {
	switch n := any(v).(type) {
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	case string:
		parsed, err := strconv.ParseFloat(n, 64)
		if err == nil {
			return parsed, true
		}
	}
	return 0, false
}

func migrateChannelKey(key string) string {
	if strings.HasPrefix(key, "start:") {
		return "branch:to:" + strings.TrimPrefix(key, "start:")
	}
	if strings.HasPrefix(key, "branch:to:") {
		return key
	}
	if strings.HasPrefix(key, "branch:__start__:") {
		return "branch:to:" + strings.TrimPrefix(key, "branch:__start__:")
	}
	if strings.HasPrefix(key, "branch:") && strings.Count(key, ":") == 3 {
		parts := strings.Split(key, ":")
		if len(parts) == 4 && parts[1] == "to" {
			return key
		}
		return "branch:to:" + parts[len(parts)-1]
	}
	return key
}
