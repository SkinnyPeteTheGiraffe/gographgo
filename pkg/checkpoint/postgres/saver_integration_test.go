package postgres_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/checkpoint"
	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/checkpoint/postgres"
	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
)

var (
	embeddedPG     *embeddedpostgres.EmbeddedPostgres
	embeddedPGDSN  string
	embeddedPGErr  error
	embeddedPGBase string
)

func TestMain(m *testing.M) {
	port, err := freeTCPPort()
	if err != nil {
		embeddedPGErr = fmt.Errorf("find free port: %w", err)
	} else {
		embeddedPGBase, err = os.MkdirTemp("", "gographgo-embedded-postgres-")
		if err != nil {
			embeddedPGErr = fmt.Errorf("make temp dir: %w", err)
		} else {
			cfg := embeddedpostgres.
				DefaultConfig().
				Port(uint32(port)).
				Username("postgres").
				Password("postgres").
				Database("postgres").
				DataPath(filepath.Join(embeddedPGBase, "data")).
				RuntimePath(filepath.Join(embeddedPGBase, "runtime")).
				BinariesPath(filepath.Join(embeddedPGBase, "binaries"))
			embeddedPG = embeddedpostgres.NewDatabase(cfg)
			embeddedPGErr = embeddedPG.Start()
			if embeddedPGErr == nil {
				embeddedPGDSN = fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/postgres?sslmode=disable", port)
			}
		}
	}

	code := m.Run()
	if embeddedPG != nil {
		_ = embeddedPG.Stop()
	}
	if embeddedPGBase != "" {
		_ = os.RemoveAll(embeddedPGBase)
	}
	os.Exit(code)
}

func freeTCPPort() (int, error) {
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer func() { _ = ln.Close() }()
	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		return 0, fmt.Errorf("unexpected address type: %T", ln.Addr())
	}
	return addr.Port, nil
}

func newSaver(t *testing.T) *postgres.Saver {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping embedded-postgres integration tests in short mode")
	}
	if embeddedPGErr != nil {
		t.Skipf("embedded-postgres unavailable: %v", embeddedPGErr)
	}

	db, err := sql.Open("pgx", embeddedPGDSN)
	if err != nil {
		t.Fatalf("open pgx: %v", err)
	}
	db.SetConnMaxLifetime(2 * time.Minute)
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	t.Cleanup(func() { _ = db.Close() })

	s, err := postgres.New(db, checkpoint.JSONSerializer{})
	if err != nil {
		t.Fatalf("new saver: %v", err)
	}
	return s
}

func newDB(t *testing.T) *sql.DB {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping embedded-postgres integration tests in short mode")
	}
	if embeddedPGErr != nil {
		t.Skipf("embedded-postgres unavailable: %v", embeddedPGErr)
	}
	db, err := sql.Open("pgx", embeddedPGDSN)
	if err != nil {
		t.Fatalf("open pgx: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func mkCP(id string, values map[string]any) *checkpoint.Checkpoint {
	if values == nil {
		values = map[string]any{}
	}
	return &checkpoint.Checkpoint{
		V:               1,
		ID:              id,
		TS:              time.Now().UTC().Format(time.RFC3339Nano),
		ChannelValues:   values,
		ChannelVersions: map[string]checkpoint.Version{},
		VersionsSeen:    map[string]map[string]checkpoint.Version{},
	}
}

func mkMeta(source string, step int) *checkpoint.CheckpointMetadata {
	return &checkpoint.CheckpointMetadata{Source: source, Step: step}
}

func TestSaver_PutGetTuple_EmbeddedPostgres(t *testing.T) {
	ctx := context.Background()
	s := newSaver(t)

	threadID := fmt.Sprintf("thread-%d", time.Now().UnixNano())
	stored, err := s.Put(ctx, &checkpoint.Config{ThreadID: threadID}, mkCP("cp-001", map[string]any{"counter": 1}), mkMeta("loop", 0))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if stored.CheckpointID != "cp-001" {
		t.Fatalf("CheckpointID = %q, want cp-001", stored.CheckpointID)
	}

	got, err := s.GetTuple(ctx, &checkpoint.Config{ThreadID: threadID})
	if err != nil {
		t.Fatalf("GetTuple: %v", err)
	}
	if got == nil {
		t.Fatal("expected tuple")
	}
	if got.Checkpoint.ChannelValues["counter"] != float64(1) {
		t.Fatalf("counter = %v, want 1", got.Checkpoint.ChannelValues["counter"])
	}
}

func TestSaver_PostgresSchemaParity_EmbeddedPostgres(t *testing.T) {
	ctx := context.Background()
	_ = newSaver(t)
	db := newDB(t)

	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM checkpoint_migrations`).Scan(&count); err != nil {
		t.Fatalf("query checkpoint_migrations: %v", err)
	}
	if count == 0 {
		t.Fatal("expected applied checkpoint migrations")
	}

	assertColumn := func(table, column, dataType string) {
		t.Helper()
		var gotType string
		err := db.QueryRowContext(ctx, `
SELECT data_type
FROM information_schema.columns
WHERE table_name = $1 AND column_name = $2`, table, column).Scan(&gotType)
		if err != nil {
			t.Fatalf("query column %s.%s: %v", table, column, err)
		}
		if gotType != dataType {
			t.Fatalf("column %s.%s data_type=%q, want %q", table, column, gotType, dataType)
		}
	}

	assertColumn("checkpoints", "checkpoint", "jsonb")
	assertColumn("checkpoints", "metadata", "jsonb")
	assertColumn("checkpoints", "type", "text")
	assertColumn("checkpoint_writes", "task_path", "text")
	assertColumn("checkpoint_writes", "blob", "bytea")
	assertColumn("checkpoint_writes", "type", "text")
	assertColumn("checkpoint_blobs", "blob", "bytea")
	assertColumn("checkpoint_blobs", "type", "text")
}

func TestSaver_Put_StoresBlobChannels_EmbeddedPostgres(t *testing.T) {
	ctx := context.Background()
	s := newSaver(t)
	db := newDB(t)

	threadID := fmt.Sprintf("thread-blob-%d", time.Now().UnixNano())
	cp := mkCP("cp-blob-001", map[string]any{
		"counter": 1,
		"payload": map[string]any{"nested": "value"},
	})
	cp.ChannelVersions["counter"] = "1"
	cp.ChannelVersions["payload"] = "1"
	stored, err := s.Put(ctx, &checkpoint.Config{ThreadID: threadID}, cp, mkMeta("loop", 0))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	var rawJSON string
	if err := db.QueryRowContext(ctx, `
SELECT checkpoint::text
FROM checkpoints
WHERE thread_id = $1 AND checkpoint_ns = $2 AND checkpoint_id = $3`, threadID, "", stored.CheckpointID).Scan(&rawJSON); err != nil {
		t.Fatalf("query checkpoint json: %v", err)
	}
	if rawJSON == "" {
		t.Fatal("expected checkpoint JSON payload")
	}

	var blobRows int
	if err := db.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM checkpoint_blobs
WHERE thread_id = $1 AND checkpoint_ns = $2 AND channel = 'payload'`, threadID, "").Scan(&blobRows); err != nil {
		t.Fatalf("query checkpoint_blobs: %v", err)
	}
	if blobRows == 0 {
		t.Fatal("expected payload channel stored in checkpoint_blobs")
	}

	tuple, err := s.GetTuple(ctx, &checkpoint.Config{ThreadID: threadID, CheckpointID: stored.CheckpointID})
	if err != nil {
		t.Fatalf("GetTuple: %v", err)
	}
	if tuple == nil {
		t.Fatal("expected tuple")
	}
	payload, ok := tuple.Checkpoint.ChannelValues["payload"].(map[string]any)
	if !ok {
		t.Fatalf("payload type = %T, want map[string]any", tuple.Checkpoint.ChannelValues["payload"])
	}
	if payload["nested"] != "value" {
		t.Fatalf("payload nested = %v, want value", payload["nested"])
	}
}

func TestSaver_PutWrites_StoresTaskPathAndType_EmbeddedPostgres(t *testing.T) {
	ctx := context.Background()
	s := newSaver(t)
	db := newDB(t)

	threadID := fmt.Sprintf("thread-write-schema-%d", time.Now().UnixNano())
	stored, err := s.Put(ctx, &checkpoint.Config{ThreadID: threadID}, mkCP("cp-001", nil), mkMeta("loop", 0))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.PutWrites(ctx, stored, []checkpoint.PendingWrite{{Channel: "alpha", Value: map[string]any{"k": "v"}}}, "task-1"); err != nil {
		t.Fatalf("PutWrites: %v", err)
	}

	var taskPath string
	var typeName sql.NullString
	if err := db.QueryRowContext(ctx, `
SELECT task_path, type
FROM checkpoint_writes
WHERE thread_id = $1 AND checkpoint_ns = $2 AND checkpoint_id = $3 AND task_id = $4`, threadID, "", stored.CheckpointID, "task-1").Scan(&taskPath, &typeName); err != nil {
		t.Fatalf("query checkpoint_writes: %v", err)
	}
	if taskPath != "" {
		t.Fatalf("task_path = %q, want empty default", taskPath)
	}
	if !typeName.Valid || typeName.String == "" {
		t.Fatal("expected typed write payload in checkpoint_writes.type")
	}
}

func TestSaver_PutWrites_EmbeddedPostgres(t *testing.T) {
	ctx := context.Background()
	s := newSaver(t)

	threadID := fmt.Sprintf("thread-%d", time.Now().UnixNano())
	stored, err := s.Put(ctx, &checkpoint.Config{ThreadID: threadID}, mkCP("cp-001", nil), mkMeta("loop", 0))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	if err := s.PutWrites(ctx, stored, []checkpoint.PendingWrite{{Channel: "a", Value: "x"}}, "task-1"); err != nil {
		t.Fatalf("PutWrites first regular: %v", err)
	}
	if err := s.PutWrites(ctx, stored, []checkpoint.PendingWrite{{Channel: "a", Value: "x"}}, "task-1"); err != nil {
		t.Fatalf("PutWrites second regular: %v", err)
	}
	if err := s.PutWrites(ctx, stored, []checkpoint.PendingWrite{{Channel: "__error__", Value: "one"}}, "task-1"); err != nil {
		t.Fatalf("PutWrites first special: %v", err)
	}
	if err := s.PutWrites(ctx, stored, []checkpoint.PendingWrite{{Channel: "__error__", Value: "two"}}, "task-1"); err != nil {
		t.Fatalf("PutWrites second special: %v", err)
	}

	got, err := s.GetTuple(ctx, &checkpoint.Config{ThreadID: threadID, CheckpointID: "cp-001"})
	if err != nil {
		t.Fatalf("GetTuple: %v", err)
	}
	if got == nil {
		t.Fatal("expected tuple")
	}
	if len(got.PendingWrites) != 2 {
		t.Fatalf("PendingWrites len = %d, want 2", len(got.PendingWrites))
	}

	var sawRegular, sawSpecial bool
	for _, w := range got.PendingWrites {
		if w.Channel == "a" {
			sawRegular = true
		}
		if w.Channel == "__error__" {
			sawSpecial = true
			if w.Value != "two" {
				t.Fatalf("special write value = %v, want two", w.Value)
			}
		}
	}
	if !sawRegular || !sawSpecial {
		t.Fatalf("missing expected writes, regular=%v special=%v", sawRegular, sawSpecial)
	}
}

func TestSaver_PutWrites_SpecialChannels_EmbeddedPostgres(t *testing.T) {
	ctx := context.Background()
	s := newSaver(t)

	threadID := fmt.Sprintf("thread-special-channels-%d", time.Now().UnixNano())
	stored, err := s.Put(ctx, &checkpoint.Config{ThreadID: threadID}, mkCP("cp-001", nil), mkMeta("loop", 0))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.PutWrites(ctx, stored, []checkpoint.PendingWrite{{Channel: "__error__", Value: "boom"}, {Channel: "__interrupt__", Value: map[string]any{"reason": "human"}}}, "task-1"); err != nil {
		t.Fatalf("PutWrites: %v", err)
	}

	got, err := s.GetTuple(ctx, &checkpoint.Config{ThreadID: threadID, CheckpointID: "cp-001"})
	if err != nil {
		t.Fatalf("GetTuple: %v", err)
	}
	if got == nil {
		t.Fatal("expected tuple")
	}
	if len(got.PendingWrites) != 2 {
		t.Fatalf("PendingWrites len = %d, want 2", len(got.PendingWrites))
	}

	sawError := false
	sawInterrupt := false
	for _, w := range got.PendingWrites {
		switch w.Channel {
		case "__error__":
			sawError = w.Value == "boom"
		case "__interrupt__":
			m, ok := w.Value.(map[string]any)
			if ok && m["reason"] == "human" {
				sawInterrupt = true
			}
		}
	}
	if !sawError || !sawInterrupt {
		t.Fatalf("missing expected special writes, sawError=%v sawInterrupt=%v writes=%+v", sawError, sawInterrupt, got.PendingWrites)
	}
}

func TestSaver_List_FilterBeforeLimit_EmbeddedPostgres(t *testing.T) {
	ctx := context.Background()
	s := newSaver(t)

	threadID := fmt.Sprintf("thread-%d", time.Now().UnixNano())
	cfg := &checkpoint.Config{ThreadID: threadID}
	_, _ = s.Put(ctx, cfg, mkCP("cp-001", nil), mkMeta("input", -1))
	_, _ = s.Put(ctx, cfg, mkCP("cp-002", nil), mkMeta("loop", 0))
	_, _ = s.Put(ctx, cfg, mkCP("cp-003", nil), mkMeta("loop", 1))

	got, err := s.List(ctx, cfg, checkpoint.ListOptions{
		Filter: map[string]any{"source": "loop"},
		Before: &checkpoint.Config{CheckpointID: "cp-003"},
		Limit:  5,
	})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1", len(got))
	}
	if got[0].Checkpoint.ID != "cp-002" {
		t.Fatalf("checkpoint id = %q, want cp-002", got[0].Checkpoint.ID)
	}
}

func TestSaver_List_GlobalSearch_EmbeddedPostgres(t *testing.T) {
	ctx := context.Background()
	s := newSaver(t)

	marker := fmt.Sprintf("run-global-search-%d", time.Now().UnixNano())
	if _, err := s.Put(ctx, &checkpoint.Config{ThreadID: fmt.Sprintf("thread-global-a-%d", time.Now().UnixNano()), CheckpointNS: ""}, mkCP("cp-001", nil), &checkpoint.CheckpointMetadata{Source: "input", Step: 0, RunID: marker}); err != nil {
		t.Fatalf("Put thread-global-a: %v", err)
	}
	if _, err := s.Put(ctx, &checkpoint.Config{ThreadID: fmt.Sprintf("thread-global-b-%d", time.Now().UnixNano()), CheckpointNS: ""}, mkCP("cp-002", nil), &checkpoint.CheckpointMetadata{Source: "loop", Step: 1, RunID: marker}); err != nil {
		t.Fatalf("Put thread-global-b: %v", err)
	}

	inputOnly, err := s.List(ctx, nil, checkpoint.ListOptions{Filter: map[string]any{"source": "input", "run_id": marker}})
	if err != nil {
		t.Fatalf("List input-only global search: %v", err)
	}
	if len(inputOnly) != 1 {
		t.Fatalf("input-only global search len = %d, want 1", len(inputOnly))
	}
	if inputOnly[0].Metadata == nil || inputOnly[0].Metadata.Source != "input" {
		t.Fatalf("input-only result metadata = %+v, want source=input", inputOnly[0].Metadata)
	}

	both, err := s.List(ctx, nil, checkpoint.ListOptions{Filter: map[string]any{"run_id": marker}})
	if err != nil {
		t.Fatalf("List global search all: %v", err)
	}
	if len(both) != 2 {
		t.Fatalf("global search len = %d, want 2", len(both))
	}
}

func TestSaver_List_FilterExtraMetadata_EmbeddedPostgres(t *testing.T) {
	ctx := context.Background()
	s := newSaver(t)

	threadID := fmt.Sprintf("thread-extra-filter-%d", time.Now().UnixNano())
	cfg := &checkpoint.Config{ThreadID: threadID}
	_, _ = s.Put(ctx, cfg, mkCP("cp-001", nil), &checkpoint.CheckpointMetadata{Source: "loop", Step: 0, Extra: map[string]any{"score": 10.0}})
	_, _ = s.Put(ctx, cfg, mkCP("cp-002", nil), &checkpoint.CheckpointMetadata{Source: "loop", Step: 1, Extra: map[string]any{"score": 20.0, "flags": map[string]any{"ok": true}}})

	got, err := s.List(ctx, cfg, checkpoint.ListOptions{Filter: map[string]any{"score": 20.0, "flags": map[string]any{"ok": true}}})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1", len(got))
	}
	if got[0].Checkpoint.ID != "cp-002" {
		t.Fatalf("checkpoint id = %q, want cp-002", got[0].Checkpoint.ID)
	}
}

func TestSaver_LegacyCheckpointJSONBlobReferenceSurvivesPrune_EmbeddedPostgres(t *testing.T) {
	ctx := context.Background()
	s := newSaver(t)
	db := newDB(t)

	threadID := fmt.Sprintf("thread-legacy-camel-%d", time.Now().UnixNano())
	cp := mkCP("cp-blob-legacy-001", map[string]any{
		"payload": map[string]any{"nested": "value"},
	})
	cp.ChannelVersions["payload"] = "1"
	stored, err := s.Put(ctx, &checkpoint.Config{ThreadID: threadID}, cp, mkMeta("loop", 0))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	if err := rewriteCheckpointJSONToCamelCase(ctx, db, threadID, "", stored.CheckpointID); err != nil {
		t.Fatalf("rewrite checkpoint JSON to CamelCase: %v", err)
	}

	if err := s.Prune(ctx, []string{threadID}, checkpoint.PruneStrategyKeepLatest); err != nil {
		t.Fatalf("Prune keep_latest: %v", err)
	}

	tuple, err := s.GetTuple(ctx, &checkpoint.Config{ThreadID: threadID, CheckpointID: stored.CheckpointID})
	if err != nil {
		t.Fatalf("GetTuple: %v", err)
	}
	if tuple == nil {
		t.Fatal("expected tuple")
	}
	payload, ok := tuple.Checkpoint.ChannelValues["payload"].(map[string]any)
	if !ok {
		t.Fatalf("payload type = %T, want map[string]any", tuple.Checkpoint.ChannelValues["payload"])
	}
	if payload["nested"] != "value" {
		t.Fatalf("payload nested = %v, want value", payload["nested"])
	}
}

func TestSaver_LegacyPendingSendsMigrationFromParentWrites_EmbeddedPostgres(t *testing.T) {
	ctx := context.Background()
	s := newSaver(t)
	db := newDB(t)

	threadID := fmt.Sprintf("thread-pending-migrate-%d", time.Now().UnixNano())
	cfg, err := s.Put(ctx, &checkpoint.Config{ThreadID: threadID, CheckpointNS: ""}, mkCP("cp-001", nil), mkMeta("loop", 0))
	if err != nil {
		t.Fatalf("Put cp-001: %v", err)
	}
	if err := s.PutWrites(ctx, cfg, []checkpoint.PendingWrite{{Channel: "__pregel_tasks", Value: "send-1"}, {Channel: "__pregel_tasks", Value: "send-2"}}, "task-1"); err != nil {
		t.Fatalf("PutWrites task-1: %v", err)
	}
	if err := s.PutWrites(ctx, cfg, []checkpoint.PendingWrite{{Channel: "__pregel_tasks", Value: "send-3"}}, "task-2"); err != nil {
		t.Fatalf("PutWrites task-2: %v", err)
	}

	cfg, err = s.Put(ctx, cfg, mkCP("cp-002", nil), mkMeta("loop", 1))
	if err != nil {
		t.Fatalf("Put cp-002: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
UPDATE checkpoints
SET checkpoint = checkpoint - 'pending_sends'
WHERE thread_id = $1 AND checkpoint_ns = $2 AND checkpoint_id = $3`, threadID, "", cfg.CheckpointID); err != nil {
		t.Fatalf("remove pending_sends key: %v", err)
	}

	tuple, err := s.GetTuple(ctx, &checkpoint.Config{ThreadID: threadID, CheckpointNS: "", CheckpointID: cfg.CheckpointID})
	if err != nil {
		t.Fatalf("GetTuple: %v", err)
	}
	if tuple == nil {
		t.Fatal("expected tuple")
	}
	if len(tuple.Checkpoint.PendingSends) != 3 {
		t.Fatalf("PendingSends len = %d, want 3", len(tuple.Checkpoint.PendingSends))
	}

	list, err := s.List(ctx, &checkpoint.Config{ThreadID: threadID, CheckpointNS: ""}, checkpoint.ListOptions{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("List len = %d, want 2", len(list))
	}
	if len(list[0].Checkpoint.PendingSends) != 3 {
		t.Fatalf("latest PendingSends len = %d, want 3", len(list[0].Checkpoint.PendingSends))
	}
}

func rewriteCheckpointJSONToCamelCase(ctx context.Context, db *sql.DB, threadID, checkpointNS, checkpointID string) error {
	var raw string
	if err := db.QueryRowContext(ctx, `
SELECT checkpoint::text
FROM checkpoints
WHERE thread_id = $1 AND checkpoint_ns = $2 AND checkpoint_id = $3`, threadID, checkpointNS, checkpointID).Scan(&raw); err != nil {
		return err
	}

	var parsed map[string]any
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return err
	}

	legacy := map[string]any{
		"V":               parsed["v"],
		"ID":              parsed["id"],
		"TS":              parsed["ts"],
		"ChannelValues":   parsed["channel_values"],
		"ChannelVersions": parsed["channel_versions"],
		"VersionsSeen":    parsed["versions_seen"],
		"UpdatedChannels": parsed["updated_channels"],
		"PendingSends":    parsed["pending_sends"],
	}
	payload, err := json.Marshal(legacy)
	if err != nil {
		return err
	}

	_, err = db.ExecContext(ctx, `
UPDATE checkpoints
SET checkpoint = $1::jsonb
WHERE thread_id = $2 AND checkpoint_ns = $3 AND checkpoint_id = $4`, string(payload), threadID, checkpointNS, checkpointID)
	return err
}

func TestSaver_DeleteThread_EmbeddedPostgres(t *testing.T) {
	ctx := context.Background()
	s := newSaver(t)

	threadDelete := fmt.Sprintf("thread-delete-%d", time.Now().UnixNano())
	threadKeep := fmt.Sprintf("thread-keep-%d", time.Now().UnixNano())

	stored, err := s.Put(ctx, &checkpoint.Config{ThreadID: threadDelete, CheckpointNS: ""}, mkCP("cp-001", nil), mkMeta("loop", 0))
	if err != nil {
		t.Fatalf("Put root: %v", err)
	}
	if _, err := s.Put(ctx, &checkpoint.Config{ThreadID: threadDelete, CheckpointNS: "child:1"}, mkCP("cp-002", nil), mkMeta("loop", 0)); err != nil {
		t.Fatalf("Put child: %v", err)
	}
	if _, err := s.Put(ctx, &checkpoint.Config{ThreadID: threadKeep, CheckpointNS: ""}, mkCP("cp-003", nil), mkMeta("loop", 0)); err != nil {
		t.Fatalf("Put keep: %v", err)
	}
	if err := s.PutWrites(ctx, stored, []checkpoint.PendingWrite{{Channel: "a", Value: "x"}}, "task-1"); err != nil {
		t.Fatalf("PutWrites: %v", err)
	}

	if err := s.DeleteThread(ctx, threadDelete); err != nil {
		t.Fatalf("DeleteThread: %v", err)
	}

	got, _ := s.GetTuple(ctx, &checkpoint.Config{ThreadID: threadDelete, CheckpointNS: ""})
	if got != nil {
		t.Fatalf("deleted root tuple = %+v, want nil", got)
	}
	got, _ = s.GetTuple(ctx, &checkpoint.Config{ThreadID: threadDelete, CheckpointNS: "child:1"})
	if got != nil {
		t.Fatalf("deleted child tuple = %+v, want nil", got)
	}
	got, _ = s.GetTuple(ctx, &checkpoint.Config{ThreadID: threadKeep, CheckpointNS: ""})
	if got == nil {
		t.Fatal("expected keep thread to remain")
	}
}

func TestSaver_DeleteForRunsCopyThreadPrune_EmbeddedPostgres(t *testing.T) {
	ctx := context.Background()
	s := newSaver(t)

	threadID := fmt.Sprintf("thread-runs-%d", time.Now().UnixNano())
	storedA, err := s.Put(ctx, &checkpoint.Config{ThreadID: threadID, CheckpointNS: ""}, mkCP("cp-001", map[string]any{"v": 1}), &checkpoint.CheckpointMetadata{Source: "loop", Step: 0, RunID: "run-a"})
	if err != nil {
		t.Fatalf("Put run-a root: %v", err)
	}
	storedB, err := s.Put(ctx, &checkpoint.Config{ThreadID: threadID, CheckpointNS: ""}, mkCP("cp-002", map[string]any{"v": 2}), &checkpoint.CheckpointMetadata{Source: "loop", Step: 1, RunID: "run-b"})
	if err != nil {
		t.Fatalf("Put run-b root: %v", err)
	}
	storedC, err := s.Put(ctx, &checkpoint.Config{ThreadID: threadID, CheckpointNS: "child:1"}, mkCP("cp-003", map[string]any{"v": 3}), &checkpoint.CheckpointMetadata{Source: "loop", Step: 1, RunID: "run-a"})
	if err != nil {
		t.Fatalf("Put run-a child: %v", err)
	}
	if err := s.PutWrites(ctx, storedA, []checkpoint.PendingWrite{{Channel: "ch", Value: "root"}}, "task-1"); err != nil {
		t.Fatalf("PutWrites root: %v", err)
	}
	if err := s.PutWrites(ctx, storedC, []checkpoint.PendingWrite{{Channel: "ch", Value: "child"}}, "task-2"); err != nil {
		t.Fatalf("PutWrites child: %v", err)
	}

	if err := s.DeleteForRuns(ctx, []string{"run-a"}); err != nil {
		t.Fatalf("DeleteForRuns: %v", err)
	}
	got, _ := s.GetTuple(ctx, &checkpoint.Config{ThreadID: threadID, CheckpointNS: "", CheckpointID: "cp-001"})
	if got != nil {
		t.Fatalf("cp-001 should be removed, got %+v", got)
	}
	got, _ = s.GetTuple(ctx, &checkpoint.Config{ThreadID: threadID, CheckpointNS: "child:1", CheckpointID: "cp-003"})
	if got != nil {
		t.Fatalf("cp-003 should be removed, got %+v", got)
	}
	got, _ = s.GetTuple(ctx, &checkpoint.Config{ThreadID: threadID, CheckpointNS: "", CheckpointID: storedB.CheckpointID})
	if got == nil {
		t.Fatal("cp-002 should remain")
	}

	targetThread := fmt.Sprintf("thread-copy-%d", time.Now().UnixNano())
	if err := s.CopyThread(ctx, threadID, targetThread); err != nil {
		t.Fatalf("CopyThread: %v", err)
	}
	copied, _ := s.GetTuple(ctx, &checkpoint.Config{ThreadID: targetThread, CheckpointNS: "", CheckpointID: storedB.CheckpointID})
	if copied == nil {
		t.Fatal("expected copied checkpoint")
	}
	if copied.Checkpoint.ChannelValues["v"] != float64(2) {
		t.Fatalf("copied value = %v, want 2", copied.Checkpoint.ChannelValues["v"])
	}

	if _, err := s.Put(ctx, &checkpoint.Config{ThreadID: targetThread, CheckpointNS: ""}, mkCP("cp-004", map[string]any{"v": 4}), mkMeta("loop", 3)); err != nil {
		t.Fatalf("Put cp-004: %v", err)
	}
	if _, err := s.Put(ctx, &checkpoint.Config{ThreadID: targetThread, CheckpointNS: ""}, mkCP("cp-005", map[string]any{"v": 5}), mkMeta("loop", 4)); err != nil {
		t.Fatalf("Put cp-005: %v", err)
	}

	if err := s.Prune(ctx, []string{targetThread}, checkpoint.PruneStrategyKeepLatest); err != nil {
		t.Fatalf("Prune keep_latest: %v", err)
	}
	list, err := s.List(ctx, &checkpoint.Config{ThreadID: targetThread, CheckpointNS: ""}, checkpoint.ListOptions{})
	if err != nil {
		t.Fatalf("List after prune: %v", err)
	}
	if len(list) != 1 || list[0].Checkpoint.ID != "cp-005" {
		t.Fatalf("post-prune checkpoints = %+v, want only cp-005", list)
	}

	if err := s.Prune(ctx, []string{targetThread}, checkpoint.PruneStrategyDelete); err != nil {
		t.Fatalf("Prune delete: %v", err)
	}
	gone, _ := s.GetTuple(ctx, &checkpoint.Config{ThreadID: targetThread, CheckpointNS: ""})
	if gone != nil {
		t.Fatalf("target thread should be deleted, got %+v", gone)
	}
}
