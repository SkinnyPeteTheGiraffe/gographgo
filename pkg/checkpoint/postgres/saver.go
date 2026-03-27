// Package postgres provides a Postgres-backed checkpoint saver.
package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/checkpoint"

	_ "github.com/jackc/pgx/v5/stdlib"
)

const (
	writeChannelError     = "__error__"
	writeChannelScheduled = "__scheduled__"
	writeChannelInterrupt = "__interrupt__"
	writeChannelResume    = "__resume__"
	writeChannelTasks     = "__pregel_tasks"
	checkpointTypeJSON    = "json"
)

var postgresMigrations = []string{
	`CREATE TABLE IF NOT EXISTS checkpoint_migrations (
  v BIGINT PRIMARY KEY
);`,
	`CREATE TABLE IF NOT EXISTS checkpoints (
  thread_id TEXT NOT NULL,
  checkpoint_ns TEXT NOT NULL DEFAULT '',
  checkpoint_id TEXT NOT NULL,
  parent_checkpoint_id TEXT,
  type TEXT,
  checkpoint JSONB NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id)
);`,
	`CREATE TABLE IF NOT EXISTS checkpoint_blobs (
  thread_id TEXT NOT NULL,
  checkpoint_ns TEXT NOT NULL DEFAULT '',
  channel TEXT NOT NULL,
  version TEXT NOT NULL,
  type TEXT NOT NULL,
  blob BYTEA,
  PRIMARY KEY (thread_id, checkpoint_ns, channel, version)
);`,
	`CREATE TABLE IF NOT EXISTS checkpoint_writes (
  thread_id TEXT NOT NULL,
  checkpoint_ns TEXT NOT NULL DEFAULT '',
  checkpoint_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  idx INTEGER NOT NULL,
  channel TEXT NOT NULL,
  type TEXT,
  blob BYTEA NOT NULL,
  PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id, task_id, idx)
);`,
	`ALTER TABLE checkpoint_writes ADD COLUMN IF NOT EXISTS task_path TEXT NOT NULL DEFAULT '';`,
	`CREATE INDEX IF NOT EXISTS checkpoints_thread_id_idx ON checkpoints(thread_id);`,
	`CREATE INDEX IF NOT EXISTS checkpoint_blobs_thread_id_idx ON checkpoint_blobs(thread_id);`,
	`CREATE INDEX IF NOT EXISTS checkpoint_writes_thread_id_idx ON checkpoint_writes(thread_id);`,
	`DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name = 'checkpoints' AND column_name = 'checkpoint' AND udt_name = 'bytea'
  ) THEN
    ALTER TABLE checkpoints ADD COLUMN IF NOT EXISTS checkpoint_jsonb JSONB;
    UPDATE checkpoints
    SET checkpoint_jsonb = COALESCE(checkpoint_jsonb, convert_from(checkpoint, 'UTF8')::jsonb);
    ALTER TABLE checkpoints DROP COLUMN checkpoint;
    ALTER TABLE checkpoints RENAME COLUMN checkpoint_jsonb TO checkpoint;
  END IF;

  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name = 'checkpoints' AND column_name = 'metadata' AND udt_name = 'bytea'
  ) THEN
    ALTER TABLE checkpoints ADD COLUMN IF NOT EXISTS metadata_jsonb JSONB DEFAULT '{}'::jsonb;
    UPDATE checkpoints
    SET metadata_jsonb = COALESCE(metadata_jsonb, convert_from(metadata, 'UTF8')::jsonb);
    ALTER TABLE checkpoints DROP COLUMN metadata;
    ALTER TABLE checkpoints RENAME COLUMN metadata_jsonb TO metadata;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name = 'checkpoints' AND column_name = 'type'
  ) THEN
    ALTER TABLE checkpoints ADD COLUMN type TEXT;
  END IF;

  ALTER TABLE checkpoints ALTER COLUMN metadata SET DEFAULT '{}'::jsonb;
  UPDATE checkpoints SET metadata = '{}'::jsonb WHERE metadata IS NULL;
  ALTER TABLE checkpoints ALTER COLUMN metadata SET NOT NULL;

  IF EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_name = 'writes'
  ) THEN
    INSERT INTO checkpoint_writes (thread_id, checkpoint_ns, checkpoint_id, task_id, task_path, idx, channel, type, blob)
    SELECT thread_id, checkpoint_ns, checkpoint_id, task_id, '', idx, channel, NULL, value
    FROM writes
    ON CONFLICT (thread_id, checkpoint_ns, checkpoint_id, task_id, idx) DO NOTHING;
  END IF;
END
$$;`,
}

// Saver persists checkpoints to a Postgres database.
type Saver struct {
	db         *sql.DB
	serializer checkpoint.Serializer
}

// Open opens a Postgres database and initializes a Saver.
func Open(connString string, serializer checkpoint.Serializer) (*Saver, error) {
	db, err := sql.Open("pgx", connString)
	if err != nil {
		return nil, err
	}
	s, err := New(db, serializer)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// New creates a Saver from an existing sql.DB.
func New(db *sql.DB, serializer checkpoint.Serializer) (*Saver, error) {
	if db == nil {
		return nil, fmt.Errorf("checkpoint/postgres: db must not be nil")
	}
	if serializer == nil {
		serializer = checkpoint.IdentitySerializer{}
	}
	s := &Saver{db: db, serializer: serializer}
	if err := s.setup(context.Background()); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Saver) setup(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, postgresMigrations[0]); err != nil {
		return fmt.Errorf("checkpoint/postgres: create migrations table: %w", err)
	}

	var version sql.NullInt64
	if err := s.db.QueryRowContext(ctx, `SELECT MAX(v) FROM checkpoint_migrations`).Scan(&version); err != nil {
		return fmt.Errorf("checkpoint/postgres: read migration version: %w", err)
	}
	start := int64(-1)
	if version.Valid {
		start = version.Int64
	}

	for i := start + 1; i < int64(len(postgresMigrations)); i++ {
		if _, err := s.db.ExecContext(ctx, postgresMigrations[i]); err != nil {
			return fmt.Errorf("checkpoint/postgres: apply migration %d: %w", i, err)
		}
		if _, err := s.db.ExecContext(ctx, `INSERT INTO checkpoint_migrations (v) VALUES ($1) ON CONFLICT (v) DO NOTHING`, i); err != nil {
			return fmt.Errorf("checkpoint/postgres: record migration %d: %w", i, err)
		}
	}
	return nil
}

// Close closes the underlying database handle.
func (s *Saver) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// GetTuple implements checkpoint.Saver.
func (s *Saver) GetTuple(ctx context.Context, config *checkpoint.Config) (*checkpoint.CheckpointTuple, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if config == nil || config.ThreadID == "" {
		return nil, nil
	}

	ns := config.CheckpointNS
	var row *sql.Row
	if config.CheckpointID != "" {
		row = s.db.QueryRowContext(ctx,
			`SELECT checkpoint_id, parent_checkpoint_id, checkpoint::text, metadata::text
FROM checkpoints
WHERE thread_id = $1 AND checkpoint_ns = $2 AND checkpoint_id = $3`,
			config.ThreadID, ns, config.CheckpointID,
		)
	} else {
		row = s.db.QueryRowContext(ctx,
			`SELECT checkpoint_id, parent_checkpoint_id, checkpoint::text, metadata::text
FROM checkpoints
WHERE thread_id = $1 AND checkpoint_ns = $2
ORDER BY checkpoint_id DESC
LIMIT 1`,
			config.ThreadID, ns,
		)
	}

	var (
		checkpointID      string
		parentCheckpoint  sql.NullString
		checkpointPayload string
		metadataPayload   string
	)
	if err := row.Scan(&checkpointID, &parentCheckpoint, &checkpointPayload, &metadataPayload); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("checkpoint/postgres: get checkpoint: %w", err)
	}

	cp, err := s.decodeCheckpoint(ctx, []byte(checkpointPayload), config.ThreadID, ns, parentCheckpoint.String)
	if err != nil {
		return nil, err
	}
	meta, err := s.decodeMetadata([]byte(metadataPayload))
	if err != nil {
		return nil, err
	}

	writes, err := s.loadWrites(ctx, config.ThreadID, ns, checkpointID)
	if err != nil {
		return nil, err
	}

	tuple := &checkpoint.CheckpointTuple{
		Config: &checkpoint.Config{
			ThreadID:     config.ThreadID,
			CheckpointNS: ns,
			CheckpointID: checkpointID,
		},
		Checkpoint:    cp,
		Metadata:      meta,
		PendingWrites: writes,
	}
	if parentCheckpoint.Valid && parentCheckpoint.String != "" {
		tuple.ParentConfig = &checkpoint.Config{
			ThreadID:     config.ThreadID,
			CheckpointNS: ns,
			CheckpointID: parentCheckpoint.String,
		}
	}
	return tuple, nil
}

// Put implements checkpoint.Saver.
func (s *Saver) Put(
	ctx context.Context,
	config *checkpoint.Config,
	cp *checkpoint.Checkpoint,
	meta *checkpoint.CheckpointMetadata,
) (*checkpoint.Config, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if config == nil || config.ThreadID == "" {
		return config, fmt.Errorf("checkpoint/postgres: Config.ThreadID must not be empty")
	}
	if cp == nil {
		return config, fmt.Errorf("checkpoint/postgres: checkpoint must not be nil")
	}

	cpToStore := checkpoint.CopyCheckpoint(cp)
	if cpToStore.ID == "" {
		cpToStore.ID = strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	cpToStore.TS = checkpoint.EnsureTimestamp(cpToStore.TS)

	cpPayload, err := s.encodeCheckpoint(cpToStore)
	if err != nil {
		return config, err
	}
	metaPayload, err := s.encodeMetadata(meta)
	if err != nil {
		return config, err
	}

	ns := config.CheckpointNS
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return config, fmt.Errorf("checkpoint/postgres: begin put: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := s.putCheckpointBlobs(ctx, tx, config.ThreadID, ns, cpToStore); err != nil {
		return config, err
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO checkpoints
(thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, type, checkpoint, metadata)
VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb)
ON CONFLICT (thread_id, checkpoint_ns, checkpoint_id)
DO UPDATE SET
  parent_checkpoint_id = EXCLUDED.parent_checkpoint_id,
  type = EXCLUDED.type,
  checkpoint = EXCLUDED.checkpoint,
  metadata = EXCLUDED.metadata`,
		config.ThreadID,
		ns,
		cpToStore.ID,
		nullIfEmpty(config.CheckpointID),
		checkpointTypeJSON,
		string(cpPayload),
		string(metaPayload),
	)
	if err != nil {
		return config, fmt.Errorf("checkpoint/postgres: put checkpoint: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return config, fmt.Errorf("checkpoint/postgres: commit put: %w", err)
	}

	return &checkpoint.Config{
		ThreadID:     config.ThreadID,
		CheckpointNS: ns,
		CheckpointID: cpToStore.ID,
	}, nil
}

// PutWrites implements checkpoint.Saver.
func (s *Saver) PutWrites(
	ctx context.Context,
	config *checkpoint.Config,
	writes []checkpoint.PendingWrite,
	taskID string,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if config == nil || config.ThreadID == "" || config.CheckpointID == "" {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("checkpoint/postgres: begin put_writes: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	insertIgnoreStmt, err := tx.PrepareContext(ctx,
		`INSERT INTO checkpoint_writes
(thread_id, checkpoint_ns, checkpoint_id, task_id, task_path, idx, channel, type, blob)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (thread_id, checkpoint_ns, checkpoint_id, task_id, idx) DO NOTHING`,
	)
	if err != nil {
		return fmt.Errorf("checkpoint/postgres: prepare insert writes: %w", err)
	}
	defer func() { _ = insertIgnoreStmt.Close() }()

	insertOrReplaceStmt, err := tx.PrepareContext(ctx,
		`INSERT INTO checkpoint_writes
(thread_id, checkpoint_ns, checkpoint_id, task_id, task_path, idx, channel, type, blob)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (thread_id, checkpoint_ns, checkpoint_id, task_id, idx)
DO UPDATE SET channel = EXCLUDED.channel, type = EXCLUDED.type, blob = EXCLUDED.blob`,
	)
	if err != nil {
		return fmt.Errorf("checkpoint/postgres: prepare upsert writes: %w", err)
	}
	defer func() { _ = insertOrReplaceStmt.Close() }()

	for i, w := range writes {
		id := taskID
		if id == "" {
			id = w.TaskID
		}
		slot, special := writeSlot(w.Channel, i)
		typeName, payload, encErr := s.encodePendingWriteValue(w.Value)
		if encErr != nil {
			return encErr
		}

		stmt := insertIgnoreStmt
		if special {
			stmt = insertOrReplaceStmt
		}
		if _, execErr := stmt.ExecContext(
			ctx,
			config.ThreadID,
			config.CheckpointNS,
			config.CheckpointID,
			id,
			"",
			slot,
			w.Channel,
			typeName,
			payload,
		); execErr != nil {
			return fmt.Errorf("checkpoint/postgres: put write: %w", execErr)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("checkpoint/postgres: commit put_writes: %w", err)
	}
	return nil
}

// List implements checkpoint.Saver.
func (s *Saver) List(
	ctx context.Context,
	config *checkpoint.Config,
	opts checkpoint.ListOptions,
) ([]*checkpoint.CheckpointTuple, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if config != nil && config.ThreadID == "" {
		return nil, nil
	}

	args := make([]any, 0, 4)
	hasConfig := config != nil
	hasBefore := opts.Before != nil && opts.Before.CheckpointID != ""
	hasLimit := opts.Limit > 0

	query := `SELECT thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, checkpoint::text, metadata::text
FROM checkpoints`
	if hasConfig {
		args = append(args, config.ThreadID, config.CheckpointNS)
	}
	if hasBefore {
		args = append(args, opts.Before.CheckpointID)
	}
	if hasLimit {
		args = append(args, opts.Limit)
	}

	switch {
	case hasConfig && hasBefore && hasLimit:
		query += ` WHERE thread_id = $1 AND checkpoint_ns = $2 AND checkpoint_id < $3 ORDER BY checkpoint_id DESC LIMIT $4`
	case hasConfig && hasBefore:
		query += ` WHERE thread_id = $1 AND checkpoint_ns = $2 AND checkpoint_id < $3 ORDER BY checkpoint_id DESC`
	case hasConfig && hasLimit:
		query += ` WHERE thread_id = $1 AND checkpoint_ns = $2 ORDER BY checkpoint_id DESC LIMIT $3`
	case hasConfig:
		query += ` WHERE thread_id = $1 AND checkpoint_ns = $2 ORDER BY checkpoint_id DESC`
	case hasBefore && hasLimit:
		query += ` WHERE checkpoint_id < $1 ORDER BY checkpoint_id DESC LIMIT $2`
	case hasBefore:
		query += ` WHERE checkpoint_id < $1 ORDER BY checkpoint_id DESC`
	case hasLimit:
		query += ` ORDER BY checkpoint_id DESC LIMIT $1`
	default:
		query += ` ORDER BY checkpoint_id DESC`
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("checkpoint/postgres: list checkpoints: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]*checkpoint.CheckpointTuple, 0)
	for rows.Next() {
		var (
			threadID          string
			namespace         string
			checkpointID      string
			parentCheckpoint  sql.NullString
			checkpointPayload string
			metadataPayload   string
		)
		if err := rows.Scan(&threadID, &namespace, &checkpointID, &parentCheckpoint, &checkpointPayload, &metadataPayload); err != nil {
			return nil, fmt.Errorf("checkpoint/postgres: scan checkpoint: %w", err)
		}

		parentID := ""
		if parentCheckpoint.Valid {
			parentID = parentCheckpoint.String
		}
		cp, err := s.decodeCheckpoint(ctx, []byte(checkpointPayload), threadID, namespace, parentID)
		if err != nil {
			return nil, err
		}
		meta, err := s.decodeMetadata([]byte(metadataPayload))
		if err != nil {
			return nil, err
		}
		if !checkpoint.MetadataMatchesFilter(meta, opts.Filter) {
			continue
		}

		pendingWrites, err := s.loadWrites(ctx, threadID, namespace, checkpointID)
		if err != nil {
			return nil, err
		}

		tuple := &checkpoint.CheckpointTuple{
			Config: &checkpoint.Config{
				ThreadID:     threadID,
				CheckpointNS: namespace,
				CheckpointID: checkpointID,
			},
			Checkpoint:    cp,
			Metadata:      meta,
			PendingWrites: pendingWrites,
		}
		if parentCheckpoint.Valid && parentCheckpoint.String != "" {
			tuple.ParentConfig = &checkpoint.Config{
				ThreadID:     threadID,
				CheckpointNS: namespace,
				CheckpointID: parentCheckpoint.String,
			}
		}
		out = append(out, tuple)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("checkpoint/postgres: iterate checkpoints: %w", err)
	}
	return out, nil
}

// DeleteThread implements checkpoint.Saver.
func (s *Saver) DeleteThread(ctx context.Context, threadID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if threadID == "" {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("checkpoint/postgres: begin delete_thread: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, `DELETE FROM checkpoint_writes WHERE thread_id = $1`, threadID); err != nil {
		return fmt.Errorf("checkpoint/postgres: delete writes for thread: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM checkpoint_blobs WHERE thread_id = $1`, threadID); err != nil {
		return fmt.Errorf("checkpoint/postgres: delete blobs for thread: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM checkpoints WHERE thread_id = $1`, threadID); err != nil {
		return fmt.Errorf("checkpoint/postgres: delete checkpoints for thread: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("checkpoint/postgres: commit delete_thread: %w", err)
	}
	return nil
}

// DeleteForRuns implements checkpoint.Saver.
func (s *Saver) DeleteForRuns(ctx context.Context, runIDs []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(runIDs) == 0 {
		return nil
	}
	runSet := make(map[string]struct{}, len(runIDs))
	for _, runID := range runIDs {
		if runID != "" {
			runSet[runID] = struct{}{}
		}
	}
	if len(runSet) == 0 {
		return nil
	}

	type checkpointKey struct {
		threadID     string
		namespace    string
		checkpointID string
	}

	rows, err := s.db.QueryContext(ctx, `SELECT thread_id, checkpoint_ns, checkpoint_id, metadata::text FROM checkpoints`)
	if err != nil {
		return fmt.Errorf("checkpoint/postgres: query checkpoints for delete_for_runs: %w", err)
	}
	keys := make([]checkpointKey, 0)
	for rows.Next() {
		var (
			threadID     string
			ns           string
			checkpointID string
			metadataRaw  string
		)
		if err := rows.Scan(&threadID, &ns, &checkpointID, &metadataRaw); err != nil {
			_ = rows.Close()
			return fmt.Errorf("checkpoint/postgres: scan checkpoint for delete_for_runs: %w", err)
		}
		meta, err := s.decodeMetadata([]byte(metadataRaw))
		if err != nil {
			_ = rows.Close()
			return err
		}
		if meta == nil {
			continue
		}
		if _, ok := runSet[meta.RunID]; ok {
			keys = append(keys, checkpointKey{threadID: threadID, namespace: ns, checkpointID: checkpointID})
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("checkpoint/postgres: iterate checkpoints for delete_for_runs: %w", err)
	}
	_ = rows.Close()
	if len(keys) == 0 {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("checkpoint/postgres: begin delete_for_runs: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	for _, key := range keys {
		if _, err := tx.ExecContext(
			ctx,
			`DELETE FROM checkpoint_writes WHERE thread_id = $1 AND checkpoint_ns = $2 AND checkpoint_id = $3`,
			key.threadID,
			key.namespace,
			key.checkpointID,
		); err != nil {
			return fmt.Errorf("checkpoint/postgres: delete writes for run: %w", err)
		}
		if _, err := tx.ExecContext(
			ctx,
			`DELETE FROM checkpoints WHERE thread_id = $1 AND checkpoint_ns = $2 AND checkpoint_id = $3`,
			key.threadID,
			key.namespace,
			key.checkpointID,
		); err != nil {
			return fmt.Errorf("checkpoint/postgres: delete checkpoints for run: %w", err)
		}
	}
	if err := s.deleteOrphanBlobsTx(ctx, tx); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("checkpoint/postgres: commit delete_for_runs: %w", err)
	}
	return nil
}

// CopyThread implements checkpoint.Saver.
func (s *Saver) CopyThread(ctx context.Context, sourceThreadID, targetThreadID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if sourceThreadID == "" || targetThreadID == "" || sourceThreadID == targetThreadID {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("checkpoint/postgres: begin copy_thread: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO checkpoints
		 (thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, type, checkpoint, metadata)
		 SELECT $1, checkpoint_ns, checkpoint_id, parent_checkpoint_id, type, checkpoint, metadata
		 FROM checkpoints
		 WHERE thread_id = $2
		 ON CONFLICT (thread_id, checkpoint_ns, checkpoint_id)
		 DO UPDATE SET
		   parent_checkpoint_id = EXCLUDED.parent_checkpoint_id,
		   type = EXCLUDED.type,
		   checkpoint = EXCLUDED.checkpoint,
		   metadata = EXCLUDED.metadata`,
		targetThreadID,
		sourceThreadID,
	); err != nil {
		return fmt.Errorf("checkpoint/postgres: copy checkpoints: %w", err)
	}

	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO checkpoint_writes
		 (thread_id, checkpoint_ns, checkpoint_id, task_id, task_path, idx, channel, type, blob)
		 SELECT $1, checkpoint_ns, checkpoint_id, task_id, task_path, idx, channel, type, blob
		 FROM checkpoint_writes
		 WHERE thread_id = $2
		 ON CONFLICT (thread_id, checkpoint_ns, checkpoint_id, task_id, idx)
		 DO UPDATE SET
		   channel = EXCLUDED.channel,
		   type = EXCLUDED.type,
		   blob = EXCLUDED.blob,
		   task_path = EXCLUDED.task_path`,
		targetThreadID,
		sourceThreadID,
	); err != nil {
		return fmt.Errorf("checkpoint/postgres: copy writes: %w", err)
	}

	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO checkpoint_blobs
		 (thread_id, checkpoint_ns, channel, version, type, blob)
		 SELECT $1, checkpoint_ns, channel, version, type, blob
		 FROM checkpoint_blobs
		 WHERE thread_id = $2
		 ON CONFLICT (thread_id, checkpoint_ns, channel, version) DO NOTHING`,
		targetThreadID,
		sourceThreadID,
	); err != nil {
		return fmt.Errorf("checkpoint/postgres: copy blobs: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("checkpoint/postgres: commit copy_thread: %w", err)
	}
	return nil
}

// Prune implements checkpoint.Saver.
func (s *Saver) Prune(ctx context.Context, threadIDs []string, strategy checkpoint.PruneStrategy) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(threadIDs) == 0 {
		return nil
	}
	if strategy == "" {
		strategy = checkpoint.PruneStrategyKeepLatest
	}
	if strategy != checkpoint.PruneStrategyKeepLatest && strategy != checkpoint.PruneStrategyDelete {
		return fmt.Errorf("checkpoint/postgres: unsupported prune strategy %q", strategy)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("checkpoint/postgres: begin prune: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	for _, threadID := range threadIDs {
		if threadID == "" {
			continue
		}

		if strategy == checkpoint.PruneStrategyDelete {
			if _, err := tx.ExecContext(ctx, `DELETE FROM checkpoint_writes WHERE thread_id = $1`, threadID); err != nil {
				return fmt.Errorf("checkpoint/postgres: prune delete writes: %w", err)
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM checkpoint_blobs WHERE thread_id = $1`, threadID); err != nil {
				return fmt.Errorf("checkpoint/postgres: prune delete blobs: %w", err)
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM checkpoints WHERE thread_id = $1`, threadID); err != nil {
				return fmt.Errorf("checkpoint/postgres: prune delete checkpoints: %w", err)
			}
			continue
		}

		rows, err := tx.QueryContext(ctx,
			`SELECT checkpoint_ns, MAX(checkpoint_id)
			 FROM checkpoints
			 WHERE thread_id = $1
			 GROUP BY checkpoint_ns`,
			threadID,
		)
		if err != nil {
			return fmt.Errorf("checkpoint/postgres: prune query latest: %w", err)
		}

		latestByNS := make(map[string]string)
		for rows.Next() {
			var ns, checkpointID string
			if err := rows.Scan(&ns, &checkpointID); err != nil {
				_ = rows.Close()
				return fmt.Errorf("checkpoint/postgres: prune scan latest: %w", err)
			}
			latestByNS[ns] = checkpointID
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return fmt.Errorf("checkpoint/postgres: prune iterate latest: %w", err)
		}
		_ = rows.Close()

		for ns, latestID := range latestByNS {
			if _, err := tx.ExecContext(
				ctx,
				`DELETE FROM checkpoint_writes
				 WHERE thread_id = $1 AND checkpoint_ns = $2 AND checkpoint_id <> $3`,
				threadID,
				ns,
				latestID,
			); err != nil {
				return fmt.Errorf("checkpoint/postgres: prune delete writes keep_latest: %w", err)
			}
			if _, err := tx.ExecContext(
				ctx,
				`DELETE FROM checkpoints
				 WHERE thread_id = $1 AND checkpoint_ns = $2 AND checkpoint_id <> $3`,
				threadID,
				ns,
				latestID,
			); err != nil {
				return fmt.Errorf("checkpoint/postgres: prune delete checkpoints keep_latest: %w", err)
			}
		}
		if err := s.deleteOrphanBlobsTx(ctx, tx); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("checkpoint/postgres: commit prune: %w", err)
	}
	return nil
}

func (s *Saver) loadWrites(ctx context.Context, threadID, ns, checkpointID string) ([]checkpoint.PendingWrite, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT task_id, channel, type, blob
FROM checkpoint_writes
WHERE thread_id = $1 AND checkpoint_ns = $2 AND checkpoint_id = $3
ORDER BY task_path, task_id, idx`,
		threadID, ns, checkpointID,
	)
	if err != nil {
		return nil, fmt.Errorf("checkpoint/postgres: load writes: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]checkpoint.PendingWrite, 0)
	for rows.Next() {
		var (
			taskID   string
			channel  string
			typeName sql.NullString
			payload  []byte
		)
		if err := rows.Scan(&taskID, &channel, &typeName, &payload); err != nil {
			return nil, fmt.Errorf("checkpoint/postgres: scan write: %w", err)
		}
		value, err := s.decodePendingWriteValue(typeName, payload)
		if err != nil {
			return nil, err
		}
		out = append(out, checkpoint.PendingWrite{TaskID: taskID, Channel: channel, Value: value})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("checkpoint/postgres: iterate writes: %w", err)
	}
	return out, nil
}

func writeSlot(channel string, idx int) (slot int, special bool) {
	switch channel {
	case writeChannelError:
		return -1, true
	case writeChannelScheduled:
		return -2, true
	case writeChannelInterrupt:
		return -3, true
	case writeChannelResume:
		return -4, true
	default:
		return idx, false
	}
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func (s *Saver) encodeCheckpoint(cp *checkpoint.Checkpoint) ([]byte, error) {
	out := checkpoint.CopyCheckpoint(cp)
	if out.ChannelValues == nil {
		out.ChannelValues = map[string]any{}
	}
	for k, v := range out.ChannelValues {
		if shouldStoreInBlob(v) {
			delete(out.ChannelValues, k)
			continue
		}
		dv, err := s.decodeMaybeSerialized(v)
		if err != nil {
			return nil, fmt.Errorf("checkpoint/postgres: normalize inline checkpoint value: %w", err)
		}
		out.ChannelValues[k] = dv
	}
	for i, v := range out.PendingSends {
		sv, err := checkpoint.SerializeForStorage(s.serializer, v)
		if err != nil {
			return nil, fmt.Errorf("checkpoint/postgres: serialize pending send: %w", err)
		}
		out.PendingSends[i] = sv
	}
	b, err := checkpoint.MarshalCheckpointForStorage(out)
	if err != nil {
		return nil, fmt.Errorf("checkpoint/postgres: marshal checkpoint: %w", err)
	}
	return b, nil
}

func (s *Saver) decodeCheckpoint(
	ctx context.Context,
	payload []byte,
	threadID, checkpointNS, parentCheckpointID string,
) (*checkpoint.Checkpoint, error) {
	cp, hasPendingSends, err := checkpoint.UnmarshalCheckpointFromStorage(payload)
	if err != nil {
		return nil, checkpoint.NewUnsupportedPersistenceFormatError(
			"checkpoint/postgres",
			"decode checkpoint payload",
			err,
		)
	}
	out := checkpoint.CopyCheckpoint(cp)
	if out.ChannelValues == nil {
		out.ChannelValues = map[string]any{}
	}
	for k, v := range out.ChannelValues {
		dv, err := s.decodeMaybeSerialized(v)
		if err != nil {
			return nil, fmt.Errorf("checkpoint/postgres: deserialize checkpoint value: %w", err)
		}
		out.ChannelValues[k] = dv
	}
	blobs, err := s.loadBlobValues(ctx, threadID, checkpointNS, out.ChannelVersions)
	if err != nil {
		return nil, err
	}
	for channel, value := range blobs {
		out.ChannelValues[channel] = value
	}
	for i, v := range out.PendingSends {
		dv, err := s.decodeMaybeSerialized(v)
		if err != nil {
			return nil, fmt.Errorf("checkpoint/postgres: deserialize pending send: %w", err)
		}
		out.PendingSends[i] = dv
	}
	if !hasPendingSends && parentCheckpointID != "" {
		migrated, err := s.loadLegacyPendingSendsFromParent(ctx, threadID, checkpointNS, parentCheckpointID)
		if err != nil {
			return nil, err
		}
		if len(migrated) > 0 {
			out.PendingSends = append(out.PendingSends, migrated...)
			if _, ok := out.ChannelVersions[writeChannelTasks]; !ok {
				out.ChannelVersions[writeChannelTasks] = migratedPendingSendVersion(out.ChannelVersions)
			}
		}
	}
	if err := checkpoint.ApplyDefaultMigration(out); err != nil {
		return nil, fmt.Errorf("checkpoint/postgres: migrate checkpoint: %w", err)
	}
	return out, nil
}

func (s *Saver) encodeMetadata(meta *checkpoint.CheckpointMetadata) ([]byte, error) {
	if meta == nil {
		return []byte("{}"), nil
	}
	out := *meta
	if len(meta.Parents) > 0 {
		out.Parents = make(map[string]string, len(meta.Parents))
		for k, v := range meta.Parents {
			out.Parents[k] = v
		}
	}
	b, err := checkpoint.MarshalMetadataForStorage(&out)
	if err != nil {
		return nil, fmt.Errorf("checkpoint/postgres: marshal metadata: %w", err)
	}
	return b, nil
}

func (s *Saver) decodeMetadata(payload []byte) (*checkpoint.CheckpointMetadata, error) {
	if len(payload) == 0 {
		return nil, nil
	}
	meta, err := checkpoint.UnmarshalMetadataFromStorage(payload)
	if err != nil {
		return nil, fmt.Errorf("checkpoint/postgres: unmarshal metadata: %w", err)
	}
	if meta == nil {
		return nil, nil
	}
	out := *meta
	if len(meta.Parents) > 0 {
		out.Parents = make(map[string]string, len(meta.Parents))
		for k, v := range meta.Parents {
			out.Parents[k] = v
		}
	}
	if len(meta.Extra) > 0 {
		out.Extra = make(map[string]any, len(meta.Extra))
		for k, v := range meta.Extra {
			out.Extra[k] = v
		}
	}
	return &out, nil
}

func (s *Saver) encodePendingWriteValue(value any) (typeName string, payload []byte, err error) {
	typeName, payload, err = s.dumpsTyped(value)
	if err != nil {
		return "", nil, fmt.Errorf("checkpoint/postgres: serialize write value: %w", err)
	}
	return typeName, payload, nil
}

func (s *Saver) decodePendingWriteValue(typeName sql.NullString, payload []byte) (any, error) {
	if typeName.Valid && typeName.String != "" {
		dv, err := s.loadsTyped(typeName.String, payload)
		if err != nil {
			return nil, fmt.Errorf("checkpoint/postgres: deserialize write typed value: %w", err)
		}
		return dv, nil
	}

	var raw any
	if err := json.Unmarshal(payload, &raw); err != nil {
		dv, deserErr := s.loadsTyped("bytes", payload)
		if deserErr == nil {
			return dv, nil
		}
		return nil, fmt.Errorf("checkpoint/postgres: unmarshal write value: %w", err)
	}
	dv, err := checkpoint.DeserializeFromStorage(s.serializer, raw)
	if err != nil {
		return nil, fmt.Errorf("checkpoint/postgres: deserialize write value: %w", err)
	}
	return dv, nil
}

func (s *Saver) putCheckpointBlobs(
	ctx context.Context,
	tx *sql.Tx,
	threadID, checkpointNS string,
	cp *checkpoint.Checkpoint,
) error {
	if cp == nil || len(cp.ChannelVersions) == 0 {
		return nil
	}
	for channel, version := range cp.ChannelVersions {
		value, ok := cp.ChannelValues[channel]
		if !ok || !shouldStoreInBlob(value) {
			continue
		}
		typeName, payload, err := s.dumpsTyped(value)
		if err != nil {
			return fmt.Errorf("checkpoint/postgres: serialize checkpoint blob value: %w", err)
		}
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO checkpoint_blobs (thread_id, checkpoint_ns, channel, version, type, blob)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (thread_id, checkpoint_ns, channel, version) DO NOTHING`,
			threadID,
			checkpointNS,
			channel,
			checkpointVersionString(version),
			typeName,
			payload,
		); err != nil {
			return fmt.Errorf("checkpoint/postgres: put checkpoint blob: %w", err)
		}
	}
	return nil
}

func (s *Saver) loadBlobValues(
	ctx context.Context,
	threadID, checkpointNS string,
	versions map[string]checkpoint.Version,
) (map[string]any, error) {
	if len(versions) == 0 {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT channel, version, type, blob
FROM checkpoint_blobs
WHERE thread_id = $1 AND checkpoint_ns = $2`,
		threadID,
		checkpointNS,
	)
	if err != nil {
		return nil, fmt.Errorf("checkpoint/postgres: load checkpoint blobs: %w", err)
	}
	defer func() { _ = rows.Close() }()

	expected := make(map[string]string, len(versions))
	for channel, version := range versions {
		expected[channel] = checkpointVersionString(version)
	}

	out := make(map[string]any)
	for rows.Next() {
		var channel, version, typeName string
		var payload []byte
		if err := rows.Scan(&channel, &version, &typeName, &payload); err != nil {
			return nil, fmt.Errorf("checkpoint/postgres: scan checkpoint blob: %w", err)
		}
		if expected[channel] != version || typeName == "empty" {
			continue
		}
		value, err := s.loadsTyped(typeName, payload)
		if err != nil {
			return nil, fmt.Errorf("checkpoint/postgres: deserialize checkpoint blob: %w", err)
		}
		out[channel] = value
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("checkpoint/postgres: iterate checkpoint blobs: %w", err)
	}
	return out, nil
}

func (s *Saver) deleteOrphanBlobsTx(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `
DELETE FROM checkpoint_blobs bl
WHERE NOT EXISTS (
  SELECT 1
  FROM checkpoints c
  JOIN LATERAL jsonb_each_text(COALESCE(c.checkpoint -> 'channel_versions', c.checkpoint -> 'ChannelVersions', '{}'::jsonb)) cv(channel, version) ON TRUE
  WHERE c.thread_id = bl.thread_id
    AND c.checkpoint_ns = bl.checkpoint_ns
    AND cv.channel = bl.channel
    AND cv.version = bl.version
)`); err != nil {
		return fmt.Errorf("checkpoint/postgres: delete orphan blobs: %w", err)
	}
	return nil
}

func (s *Saver) decodeMaybeSerialized(value any) (any, error) {
	if looksSerializedValue(value) {
		return checkpoint.DeserializeFromStorage(s.serializer, value)
	}
	return value, nil
}

func (s *Saver) dumpsTyped(value any) (typeName string, payload []byte, err error) {
	raw, err := checkpoint.SerializeForStorage(s.serializer, value)
	if err != nil {
		return "", nil, err
	}
	sv, ok := raw.(checkpoint.SerializedValue)
	if ok {
		return sv.Type, sv.Data, nil
	}
	return "", nil, fmt.Errorf("checkpoint/postgres: unexpected serialized value type %T", raw)
}

func (s *Saver) loadsTyped(typeName string, payload []byte) (any, error) {
	v, err := checkpoint.DeserializeFromStorage(
		s.serializer,
		checkpoint.SerializedValue{Type: typeName, Data: payload},
	)
	if err != nil {
		return nil, checkpoint.NewUnsupportedPersistenceFormatError(
			"checkpoint/postgres",
			fmt.Sprintf("deserialize typed value %q", typeName),
			err,
		)
	}
	return v, nil
}

func looksSerializedValue(value any) bool {
	m, ok := value.(map[string]any)
	if !ok {
		return false
	}
	typeName, _ := m["type"].(string)
	if typeName == "" {
		return false
	}
	_, ok = m["data"]
	return ok
}

func shouldStoreInBlob(value any) bool {
	switch value.(type) {
	case nil, bool, string,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return false
	default:
		return true
	}
}

func checkpointVersionString(v checkpoint.Version) string {
	if v == nil {
		return ""
	}
	return fmt.Sprint(v)
}

func migratedPendingSendVersion(versions map[string]checkpoint.Version) checkpoint.Version {
	if len(versions) == 0 {
		return int64(1)
	}

	var (
		maxInt int64
		hasInt bool
		maxLex string
	)
	for _, version := range versions {
		if asInt, err := checkpoint.VersionAsInt64(version); err == nil {
			if !hasInt || asInt > maxInt {
				maxInt = asInt
				hasInt = true
			}
			continue
		}
		asString := checkpointVersionString(version)
		if asString > maxLex {
			maxLex = asString
		}
	}
	if hasInt {
		return maxInt
	}
	if maxLex != "" {
		return maxLex
	}
	return int64(1)
}

func (s *Saver) loadLegacyPendingSendsFromParent(
	ctx context.Context,
	threadID, checkpointNS, parentCheckpointID string,
) ([]any, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT type, blob
FROM checkpoint_writes
WHERE thread_id = $1 AND checkpoint_ns = $2 AND checkpoint_id = $3 AND channel = $4
ORDER BY task_path, task_id, idx`,
		threadID,
		checkpointNS,
		parentCheckpointID,
		writeChannelTasks,
	)
	if err != nil {
		return nil, fmt.Errorf("checkpoint/postgres: load legacy pending sends: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]any, 0)
	for rows.Next() {
		var (
			typeName sql.NullString
			payload  []byte
		)
		if err := rows.Scan(&typeName, &payload); err != nil {
			return nil, fmt.Errorf("checkpoint/postgres: scan legacy pending send: %w", err)
		}
		value, err := s.decodePendingWriteValue(typeName, payload)
		if err != nil {
			return nil, err
		}
		out = append(out, value)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("checkpoint/postgres: iterate legacy pending sends: %w", err)
	}
	return out, nil
}
