package checkpoint

import "testing"

func TestDefaultMigrator_MigratesLegacyChannelKeys(t *testing.T) {
	cp := &Checkpoint{
		V: 0,
		ChannelValues: map[string]any{
			"start:alpha":         1,
			"branch:foo:bar:beta": 2,
		},
		ChannelVersions: map[string]Version{
			"start:alpha":         "1",
			"branch:foo:bar:beta": "2",
		},
		VersionsSeen: map[string]map[string]Version{
			"n1": {
				"start:alpha": "1",
			},
		},
	}

	if err := DefaultMigrator().Migrate(cp); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	if cp.V != 1 {
		t.Fatalf("expected version 1, got %d", cp.V)
	}
	if _, ok := cp.ChannelValues["start:alpha"]; ok {
		t.Fatal("expected start:alpha key to be migrated")
	}
	if _, ok := cp.ChannelVersions["branch:foo:bar:beta"]; ok {
		t.Fatal("expected branch:foo:bar:beta key to be migrated")
	}
	if got := cp.ChannelVersions["branch:to:alpha"]; got != "1" {
		t.Fatalf("expected branch:to:alpha version 1, got %q", got)
	}
	if got := cp.VersionsSeen["n1"]["branch:to:alpha"]; got != "1" {
		t.Fatalf("expected versions_seen key migrated, got %q", got)
	}
}

func TestMultiMigrator_StopsOnError(t *testing.T) {
	cp := EmptyCheckpoint("id")
	m := NewMultiMigrator(
		MigratorFunc(func(*Checkpoint) error { return nil }),
		MigratorFunc(func(*Checkpoint) error { return assertErr{} }),
	)
	if err := m.Migrate(cp); err == nil {
		t.Fatal("expected error")
	}
}

func TestDefaultMigrator_MigratesBranchStartLegacyKey(t *testing.T) {
	cp := &Checkpoint{
		V: 1,
		ChannelValues: map[string]any{
			"branch:__start__:alpha": 1,
		},
		ChannelVersions: map[string]Version{
			"branch:__start__:alpha": "3",
		},
		VersionsSeen: map[string]map[string]Version{
			"n1": {
				"branch:__start__:alpha": "3",
			},
		},
	}

	if err := DefaultMigrator().Migrate(cp); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	if _, ok := cp.ChannelValues["branch:__start__:alpha"]; ok {
		t.Fatal("expected branch:__start__:alpha key to be migrated")
	}
	if got := cp.ChannelVersions["branch:to:alpha"]; got != "3" {
		t.Fatalf("expected branch:to:alpha version 3, got %v", got)
	}
	if got := cp.VersionsSeen["n1"]["branch:to:alpha"]; got != "3" {
		t.Fatalf("expected versions_seen key migrated, got %v", got)
	}
}

func TestDefaultMigrator_DoesNotRemapBranchToWithColonTarget(t *testing.T) {
	cp := &Checkpoint{
		V: 1,
		ChannelValues: map[string]any{
			"branch:to:child:1": 1,
		},
		ChannelVersions: map[string]Version{
			"branch:to:child:1": "2",
		},
		VersionsSeen: map[string]map[string]Version{
			"n1": {
				"branch:to:child:1": "2",
			},
		},
	}

	if err := DefaultMigrator().Migrate(cp); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	if _, ok := cp.ChannelValues["branch:to:child:1"]; !ok {
		t.Fatal("expected branch:to:child:1 key to remain unchanged")
	}
	if _, ok := cp.ChannelVersions["branch:to:1"]; ok {
		t.Fatal("unexpected remap to branch:to:1")
	}
	if got := cp.VersionsSeen["n1"]["branch:to:child:1"]; got != "2" {
		t.Fatalf("expected versions_seen key unchanged, got %v", got)
	}
}

type assertErr struct{}

func (assertErr) Error() string { return "boom" }
