// Package sqlite provides an SQLite-backed checkpoint saver.
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/checkpoint"
	_ "modernc.org/sqlite"
)

const (
	writeChannelError     = "__error__"
	writeChannelScheduled = "__scheduled__"
	writeChannelInterrupt = "__interrupt__"
	writeChannelResume    = "__resume__"
)

// Saver persists checkpoints to a SQLite database.
type Saver struct {
	db                   *sql.DB
	serializer           checkpoint.Serializer
	hasCheckpointTypeCol bool
	hasWriteTypeCol      bool
}

// Open opens a SQLite database and initializes a Saver.
func Open(connString string, serializer checkpoint.Serializer) (*Saver, error) {
	db, err := sql.Open("sqlite", connString)
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
		return nil, fmt.Errorf("checkpoint/sqlite: db must not be nil")
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
	_, err := s.db.ExecContext(ctx, `
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS checkpoints (
  thread_id TEXT NOT NULL,
  checkpoint_ns TEXT NOT NULL DEFAULT '',
  checkpoint_id TEXT NOT NULL,
  parent_checkpoint_id TEXT,
  checkpoint BLOB NOT NULL,
  metadata BLOB,
  PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id)
);
CREATE TABLE IF NOT EXISTS writes (
  thread_id TEXT NOT NULL,
  checkpoint_ns TEXT NOT NULL DEFAULT '',
  checkpoint_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  idx INTEGER NOT NULL,
  channel TEXT NOT NULL,
  value BLOB NOT NULL,
  PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id, task_id, idx)
);
`)
	if err != nil {
		return fmt.Errorf("checkpoint/sqlite: setup schema: %w", err)
	}
	if err := s.validateCompatibilitySchema(ctx); err != nil {
		return err
	}
	return nil
}

func (s *Saver) validateCompatibilitySchema(ctx context.Context) error {
	hasTypeCol, err := sqliteTableHasColumn(ctx, s.db, "checkpoints", "type")
	if err != nil {
		return fmt.Errorf("checkpoint/sqlite: inspect checkpoints schema: %w", err)
	}
	s.hasCheckpointTypeCol = hasTypeCol
	hasWriteTypeCol, err := sqliteTableHasColumn(ctx, s.db, "writes", "type")
	if err != nil {
		return fmt.Errorf("checkpoint/sqlite: inspect writes schema: %w", err)
	}
	s.hasWriteTypeCol = hasWriteTypeCol
	return nil
}

func sqliteTableHasColumn(ctx context.Context, db *sql.DB, tableName, columnName string) (bool, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s)", tableName))
	if err != nil {
		return false, err
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var (
			cid        int
			name       string
			colType    string
			notNull    int
			defaultVal sql.NullString
			pk         int
		)
		if err := rows.Scan(&cid, &name, &colType, &notNull, &defaultVal, &pk); err != nil {
			return false, err
		}
		if name == columnName {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	return false, nil
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
	var (
		row *sql.Row
		id  string
	)
	if config.CheckpointID != "" {
		id = config.CheckpointID
		selectCols := "checkpoint_id, parent_checkpoint_id, checkpoint, metadata"
		if s.hasCheckpointTypeCol {
			selectCols = "checkpoint_id, parent_checkpoint_id, type, checkpoint, metadata"
		}
		row = s.db.QueryRowContext(ctx,
			`SELECT `+selectCols+`
 FROM checkpoints
 WHERE thread_id = ? AND checkpoint_ns = ? AND checkpoint_id = ?`,
			config.ThreadID, ns, config.CheckpointID,
		)
	} else {
		selectCols := "checkpoint_id, parent_checkpoint_id, checkpoint, metadata"
		if s.hasCheckpointTypeCol {
			selectCols = "checkpoint_id, parent_checkpoint_id, type, checkpoint, metadata"
		}
		row = s.db.QueryRowContext(ctx,
			`SELECT `+selectCols+`
 FROM checkpoints
 WHERE thread_id = ? AND checkpoint_ns = ?
 ORDER BY checkpoint_id DESC
 LIMIT 1`,
			config.ThreadID, ns,
		)
	}

	var (
		checkpointID      string
		parentCheckpoint  sql.NullString
		checkpointType    sql.NullString
		checkpointPayload []byte
		metadataPayload   []byte
	)
	var scanErr error
	if s.hasCheckpointTypeCol {
		scanErr = row.Scan(&checkpointID, &parentCheckpoint, &checkpointType, &checkpointPayload, &metadataPayload)
	} else {
		scanErr = row.Scan(&checkpointID, &parentCheckpoint, &checkpointPayload, &metadataPayload)
	}
	if scanErr != nil {
		if scanErr == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("checkpoint/sqlite: get checkpoint: %w", scanErr)
	}
	if id == "" {
		id = checkpointID
	}

	cp, err := s.decodeCheckpoint(checkpointType, checkpointPayload)
	if err != nil {
		return nil, err
	}
	meta, err := s.decodeMetadata(metadataPayload)
	if err != nil {
		return nil, err
	}

	writes, err := s.loadWrites(ctx, config.ThreadID, ns, id)
	if err != nil {
		return nil, err
	}

	tuple := &checkpoint.CheckpointTuple{
		Config: &checkpoint.Config{
			ThreadID:     config.ThreadID,
			CheckpointNS: ns,
			CheckpointID: id,
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
		return config, fmt.Errorf("checkpoint/sqlite: Config.ThreadID must not be empty")
	}
	if cp == nil {
		return config, fmt.Errorf("checkpoint/sqlite: checkpoint must not be nil")
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
	_, err = s.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO checkpoints
 (thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, checkpoint, metadata)
 VALUES (?, ?, ?, ?, ?, ?)`,
		config.ThreadID,
		ns,
		cpToStore.ID,
		nullIfEmpty(config.CheckpointID),
		cpPayload,
		metaPayload,
	)
	if err != nil {
		return config, fmt.Errorf("checkpoint/sqlite: put checkpoint: %w", err)
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
		return fmt.Errorf("checkpoint/sqlite: begin put_writes: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	insertIgnoreStmt, err := tx.PrepareContext(ctx,
		`INSERT OR IGNORE INTO writes
 (thread_id, checkpoint_ns, checkpoint_id, task_id, idx, channel, value)
 VALUES (?, ?, ?, ?, ?, ?, ?)`,
	)
	if err != nil {
		return fmt.Errorf("checkpoint/sqlite: prepare insert writes: %w", err)
	}
	defer func() { _ = insertIgnoreStmt.Close() }()

	insertOrReplaceStmt, err := tx.PrepareContext(ctx,
		`INSERT OR REPLACE INTO writes
 (thread_id, checkpoint_ns, checkpoint_id, task_id, idx, channel, value)
 VALUES (?, ?, ?, ?, ?, ?, ?)`,
	)
	if err != nil {
		return fmt.Errorf("checkpoint/sqlite: prepare upsert writes: %w", err)
	}
	defer func() { _ = insertOrReplaceStmt.Close() }()

	for i, w := range writes {
		id := taskID
		if id == "" {
			id = w.TaskID
		}
		slot, special := writeSlot(w.Channel, i)
		payload, encErr := s.encodePendingWriteValue(w.Value)
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
			slot,
			w.Channel,
			payload,
		); execErr != nil {
			return fmt.Errorf("checkpoint/sqlite: put write: %w", execErr)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("checkpoint/sqlite: commit put_writes: %w", err)
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
	selectCols := "thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, checkpoint, metadata"
	if s.hasCheckpointTypeCol {
		selectCols = "thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, type, checkpoint, metadata"
	}
	query := `SELECT ` + selectCols + `
FROM checkpoints
WHERE 1=1`
	if config != nil {
		query += ` AND thread_id = ? AND checkpoint_ns = ?`
		args = append(args, config.ThreadID, config.CheckpointNS)
	}
	if opts.Before != nil && opts.Before.CheckpointID != "" {
		query += ` AND checkpoint_id < ?`
		args = append(args, opts.Before.CheckpointID)
	}
	query += ` ORDER BY checkpoint_id DESC`
	if opts.Limit > 0 {
		query += ` LIMIT ?`
		args = append(args, opts.Limit)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("checkpoint/sqlite: list checkpoints: %w", err)
	}
	type listedCheckpoint struct {
		threadID        string
		namespace       string
		id              string
		parent          sql.NullString
		checkpointType  sql.NullString
		checkpointBytes []byte
		metadataBytes   []byte
	}
	listed := make([]listedCheckpoint, 0)
	for rows.Next() {
		var (
			threadID          string
			namespace         string
			checkpointID      string
			parentCheckpoint  sql.NullString
			checkpointType    sql.NullString
			checkpointPayload []byte
			metadataPayload   []byte
		)
		if s.hasCheckpointTypeCol {
			err = rows.Scan(&threadID, &namespace, &checkpointID, &parentCheckpoint, &checkpointType, &checkpointPayload, &metadataPayload)
		} else {
			err = rows.Scan(&threadID, &namespace, &checkpointID, &parentCheckpoint, &checkpointPayload, &metadataPayload)
		}
		if err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("checkpoint/sqlite: scan checkpoint: %w", err)
		}
		listed = append(listed, listedCheckpoint{
			threadID:        threadID,
			namespace:       namespace,
			id:              checkpointID,
			parent:          parentCheckpoint,
			checkpointType:  checkpointType,
			checkpointBytes: checkpointPayload,
			metadataBytes:   metadataPayload,
		})
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("checkpoint/sqlite: iterate checkpoints: %w", err)
	}
	_ = rows.Close()

	out := make([]*checkpoint.CheckpointTuple, 0, len(listed))
	for _, item := range listed {
		cp, err := s.decodeCheckpoint(item.checkpointType, item.checkpointBytes)
		if err != nil {
			return nil, err
		}
		meta, err := s.decodeMetadata(item.metadataBytes)
		if err != nil {
			return nil, err
		}
		if !checkpoint.MetadataMatchesFilter(meta, opts.Filter) {
			continue
		}

		pendingWrites, err := s.loadWrites(ctx, item.threadID, item.namespace, item.id)
		if err != nil {
			return nil, err
		}

		tuple := &checkpoint.CheckpointTuple{
			Config: &checkpoint.Config{
				ThreadID:     item.threadID,
				CheckpointNS: item.namespace,
				CheckpointID: item.id,
			},
			Checkpoint:    cp,
			Metadata:      meta,
			PendingWrites: pendingWrites,
		}
		if item.parent.Valid && item.parent.String != "" {
			tuple.ParentConfig = &checkpoint.Config{
				ThreadID:     item.threadID,
				CheckpointNS: item.namespace,
				CheckpointID: item.parent.String,
			}
		}
		out = append(out, tuple)
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
		return fmt.Errorf("checkpoint/sqlite: begin delete_thread: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, `DELETE FROM writes WHERE thread_id = ?`, threadID); err != nil {
		return fmt.Errorf("checkpoint/sqlite: delete writes for thread: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM checkpoints WHERE thread_id = ?`, threadID); err != nil {
		return fmt.Errorf("checkpoint/sqlite: delete checkpoints for thread: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("checkpoint/sqlite: commit delete_thread: %w", err)
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

	rows, err := s.db.QueryContext(ctx, `SELECT thread_id, checkpoint_ns, checkpoint_id, metadata FROM checkpoints`)
	if err != nil {
		return fmt.Errorf("checkpoint/sqlite: query checkpoints for delete_for_runs: %w", err)
	}
	keys := make([]checkpointKey, 0)
	for rows.Next() {
		var (
			threadID     string
			ns           string
			checkpointID string
			metadataRaw  []byte
		)
		if err := rows.Scan(&threadID, &ns, &checkpointID, &metadataRaw); err != nil {
			_ = rows.Close()
			return fmt.Errorf("checkpoint/sqlite: scan checkpoint for delete_for_runs: %w", err)
		}
		meta, err := s.decodeMetadata(metadataRaw)
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
		return fmt.Errorf("checkpoint/sqlite: iterate checkpoints for delete_for_runs: %w", err)
	}
	_ = rows.Close()
	if len(keys) == 0 {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("checkpoint/sqlite: begin delete_for_runs: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	for _, key := range keys {
		if _, err := tx.ExecContext(
			ctx,
			`DELETE FROM writes WHERE thread_id = ? AND checkpoint_ns = ? AND checkpoint_id = ?`,
			key.threadID,
			key.namespace,
			key.checkpointID,
		); err != nil {
			return fmt.Errorf("checkpoint/sqlite: delete writes for run: %w", err)
		}
		if _, err := tx.ExecContext(
			ctx,
			`DELETE FROM checkpoints WHERE thread_id = ? AND checkpoint_ns = ? AND checkpoint_id = ?`,
			key.threadID,
			key.namespace,
			key.checkpointID,
		); err != nil {
			return fmt.Errorf("checkpoint/sqlite: delete checkpoints for run: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("checkpoint/sqlite: commit delete_for_runs: %w", err)
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
		return fmt.Errorf("checkpoint/sqlite: begin copy_thread: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(
		ctx,
		`INSERT OR REPLACE INTO checkpoints
	 (thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, checkpoint, metadata)
	 SELECT ?, checkpoint_ns, checkpoint_id, parent_checkpoint_id, checkpoint, metadata
	 FROM checkpoints
	 WHERE thread_id = ?`,
		targetThreadID,
		sourceThreadID,
	); err != nil {
		return fmt.Errorf("checkpoint/sqlite: copy checkpoints: %w", err)
	}

	if _, err := tx.ExecContext(
		ctx,
		`INSERT OR REPLACE INTO writes
	 (thread_id, checkpoint_ns, checkpoint_id, task_id, idx, channel, value)
	 SELECT ?, checkpoint_ns, checkpoint_id, task_id, idx, channel, value
	 FROM writes
	 WHERE thread_id = ?`,
		targetThreadID,
		sourceThreadID,
	); err != nil {
		return fmt.Errorf("checkpoint/sqlite: copy writes: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("checkpoint/sqlite: commit copy_thread: %w", err)
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
		return fmt.Errorf("checkpoint/sqlite: unsupported prune strategy %q", strategy)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("checkpoint/sqlite: begin prune: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	for _, threadID := range threadIDs {
		if threadID == "" {
			continue
		}

		if strategy == checkpoint.PruneStrategyDelete {
			if _, err := tx.ExecContext(ctx, `DELETE FROM writes WHERE thread_id = ?`, threadID); err != nil {
				return fmt.Errorf("checkpoint/sqlite: prune delete writes: %w", err)
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM checkpoints WHERE thread_id = ?`, threadID); err != nil {
				return fmt.Errorf("checkpoint/sqlite: prune delete checkpoints: %w", err)
			}
			continue
		}

		rows, err := tx.QueryContext(ctx,
			`SELECT checkpoint_ns, MAX(checkpoint_id)
			 FROM checkpoints
			 WHERE thread_id = ?
			 GROUP BY checkpoint_ns`,
			threadID,
		)
		if err != nil {
			return fmt.Errorf("checkpoint/sqlite: prune query latest: %w", err)
		}

		latestByNS := make(map[string]string)
		for rows.Next() {
			var ns, checkpointID string
			if err := rows.Scan(&ns, &checkpointID); err != nil {
				_ = rows.Close()
				return fmt.Errorf("checkpoint/sqlite: prune scan latest: %w", err)
			}
			latestByNS[ns] = checkpointID
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return fmt.Errorf("checkpoint/sqlite: prune iterate latest: %w", err)
		}
		_ = rows.Close()

		for ns, latestID := range latestByNS {
			if _, err := tx.ExecContext(
				ctx,
				`DELETE FROM writes
				 WHERE thread_id = ? AND checkpoint_ns = ? AND checkpoint_id <> ?`,
				threadID,
				ns,
				latestID,
			); err != nil {
				return fmt.Errorf("checkpoint/sqlite: prune delete writes keep_latest: %w", err)
			}
			if _, err := tx.ExecContext(
				ctx,
				`DELETE FROM checkpoints
				 WHERE thread_id = ? AND checkpoint_ns = ? AND checkpoint_id <> ?`,
				threadID,
				ns,
				latestID,
			); err != nil {
				return fmt.Errorf("checkpoint/sqlite: prune delete checkpoints keep_latest: %w", err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("checkpoint/sqlite: commit prune: %w", err)
	}
	return nil
}

func (s *Saver) loadWrites(ctx context.Context, threadID, ns, checkpointID string) ([]checkpoint.PendingWrite, error) {
	query := `SELECT task_id, channel, value
 FROM writes
 WHERE thread_id = ? AND checkpoint_ns = ? AND checkpoint_id = ?
 ORDER BY task_id, idx`
	if s.hasWriteTypeCol {
		query = `SELECT task_id, channel, type, value
 FROM writes
 WHERE thread_id = ? AND checkpoint_ns = ? AND checkpoint_id = ?
 ORDER BY task_id, idx`
	}
	rows, err := s.db.QueryContext(ctx,
		query,
		threadID, ns, checkpointID,
	)
	if err != nil {
		return nil, fmt.Errorf("checkpoint/sqlite: load writes: %w", err)
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
		if s.hasWriteTypeCol {
			err = rows.Scan(&taskID, &channel, &typeName, &payload)
		} else {
			err = rows.Scan(&taskID, &channel, &payload)
		}
		if err != nil {
			return nil, fmt.Errorf("checkpoint/sqlite: scan write: %w", err)
		}
		value, err := s.decodePendingWriteValue(typeName, payload)
		if err != nil {
			return nil, err
		}
		out = append(out, checkpoint.PendingWrite{TaskID: taskID, Channel: channel, Value: value})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("checkpoint/sqlite: iterate writes: %w", err)
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
		sv, err := checkpoint.SerializeForStorage(s.serializer, v)
		if err != nil {
			return nil, fmt.Errorf("checkpoint/sqlite: serialize checkpoint value: %w", err)
		}
		out.ChannelValues[k] = sv
	}
	for i, v := range out.PendingSends {
		sv, err := checkpoint.SerializeForStorage(s.serializer, v)
		if err != nil {
			return nil, fmt.Errorf("checkpoint/sqlite: serialize pending send: %w", err)
		}
		out.PendingSends[i] = sv
	}
	b, err := checkpoint.MarshalCheckpointForStorage(out)
	if err != nil {
		return nil, fmt.Errorf("checkpoint/sqlite: marshal checkpoint: %w", err)
	}
	return b, nil
}

func (s *Saver) decodeCheckpoint(typeName sql.NullString, payload []byte) (*checkpoint.Checkpoint, error) {
	var cp *checkpoint.Checkpoint
	if typeName.Valid && typeName.String != "" {
		typedValue, err := s.loadsTyped(typeName.String, payload)
		if err != nil {
			return nil, err
		}
		cp, err = checkpointFromDecodedAny(typedValue)
		if err != nil {
			return nil, checkpoint.NewUnsupportedPersistenceFormatError(
				"checkpoint/sqlite",
				"decode typed checkpoint payload",
				err,
			)
		}
	} else {
		parsed, _, err := checkpoint.UnmarshalCheckpointFromStorage(payload)
		if err != nil {
			return nil, checkpoint.NewUnsupportedPersistenceFormatError(
				"checkpoint/sqlite",
				"decode checkpoint payload",
				err,
			)
		}
		cp = parsed
	}

	out := checkpoint.CopyCheckpoint(cp)
	for k, v := range out.ChannelValues {
		dv, err := s.decodeMaybeSerialized(v)
		if err != nil {
			return nil, checkpoint.NewUnsupportedPersistenceFormatError(
				"checkpoint/sqlite",
				fmt.Sprintf("deserialize checkpoint value for channel %q", k),
				err,
			)
		}
		out.ChannelValues[k] = dv
	}
	for i, v := range out.PendingSends {
		dv, err := s.decodeMaybeSerialized(v)
		if err != nil {
			return nil, checkpoint.NewUnsupportedPersistenceFormatError(
				"checkpoint/sqlite",
				"deserialize pending send",
				err,
			)
		}
		out.PendingSends[i] = dv
	}
	if err := checkpoint.ApplyDefaultMigration(out); err != nil {
		return nil, fmt.Errorf("checkpoint/sqlite: migrate checkpoint: %w", err)
	}
	return out, nil
}

func (s *Saver) encodeMetadata(meta *checkpoint.CheckpointMetadata) ([]byte, error) {
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
	b, err := checkpoint.MarshalMetadataForStorage(&out)
	if err != nil {
		return nil, fmt.Errorf("checkpoint/sqlite: marshal metadata: %w", err)
	}
	return b, nil
}

func (s *Saver) decodeMetadata(payload []byte) (*checkpoint.CheckpointMetadata, error) {
	if len(payload) == 0 {
		return nil, nil
	}
	meta, err := checkpoint.UnmarshalMetadataFromStorage(payload)
	if err != nil {
		return nil, fmt.Errorf("checkpoint/sqlite: unmarshal metadata: %w", err)
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

func (s *Saver) encodePendingWriteValue(value any) ([]byte, error) {
	sv, err := checkpoint.SerializeForStorage(s.serializer, value)
	if err != nil {
		return nil, fmt.Errorf("checkpoint/sqlite: serialize write value: %w", err)
	}
	b, err := json.Marshal(sv)
	if err != nil {
		return nil, fmt.Errorf("checkpoint/sqlite: marshal write value: %w", err)
	}
	return b, nil
}

func (s *Saver) decodePendingWriteValue(typeName sql.NullString, payload []byte) (any, error) {
	if typeName.Valid && typeName.String != "" {
		dv, err := s.loadsTyped(typeName.String, payload)
		if err != nil {
			return nil, err
		}
		return dv, nil
	}

	var raw any
	if err := json.Unmarshal(payload, &raw); err != nil {
		dv, deserErr := s.loadsTyped("bytes", payload)
		if deserErr == nil {
			return dv, nil
		}
		return nil, fmt.Errorf("checkpoint/sqlite: unmarshal write value: %w", err)
	}
	dv, err := checkpoint.DeserializeFromStorage(s.serializer, raw)
	if err != nil {
		return nil, checkpoint.NewUnsupportedPersistenceFormatError(
			"checkpoint/sqlite",
			"deserialize pending write value",
			err,
		)
	}
	return dv, nil
}

func (s *Saver) decodeMaybeSerialized(value any) (any, error) {
	if looksSerializedValue(value) {
		return checkpoint.DeserializeFromStorage(s.serializer, value)
	}
	return value, nil
}

func (s *Saver) loadsTyped(typeName string, payload []byte) (any, error) {
	v, err := checkpoint.DeserializeFromStorage(
		s.serializer,
		checkpoint.SerializedValue{Type: typeName, Data: payload},
	)
	if err != nil {
		return nil, checkpoint.NewUnsupportedPersistenceFormatError(
			"checkpoint/sqlite",
			fmt.Sprintf("deserialize typed value %q", typeName),
			err,
		)
	}
	return v, nil
}

func checkpointFromDecodedAny(value any) (*checkpoint.Checkpoint, error) {
	if value == nil {
		return nil, fmt.Errorf("checkpoint payload is nil")
	}
	b, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	cp, _, err := checkpoint.UnmarshalCheckpointFromStorage(b)
	if err != nil {
		return nil, err
	}
	return cp, nil
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
