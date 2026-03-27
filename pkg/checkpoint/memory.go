package checkpoint

import (
	"context"
	"fmt"
	"sync"
	"time"
)

const (
	writeChannelError     = "__error__"
	writeChannelScheduled = "__scheduled__"
	writeChannelInterrupt = "__interrupt__"
	writeChannelResume    = "__resume__"
)

// InMemorySaver is a thread-safe in-memory implementation of Saver.
//
// It stores checkpoints in a nested map: threadID → checkpointNS →
// checkpointID → CheckpointTuple. Pending writes are stored separately.
//
// Only use InMemorySaver for testing or debugging. For production, use
// a persistent backend (Postgres, SQLite, etc.).
//
// Mirrors Python's langgraph.checkpoint.memory.InMemorySaver.
type InMemorySaver struct {
	serializer Serializer
	storage    map[string]map[string]map[string]*CheckpointTuple
	writes     map[checkpointKey][]PendingWrite
	blobs      map[blobKey]serializedBlob
	mu         sync.RWMutex
}

type checkpointKey struct {
	threadID     string
	ns           string
	checkpointID string
}

type blobKey struct {
	threadID string
	ns       string
	channel  string
	version  string
}

type serializedBlob struct {
	value any
	empty bool
}

// NewInMemorySaver creates an empty InMemorySaver.
func NewInMemorySaver() *InMemorySaver {
	return NewInMemorySaverWithSerializer(IdentitySerializer{})
}

// NewInMemorySaverWithSerializer creates an empty InMemorySaver using a
// caller-provided serializer. If serializer is nil, IdentitySerializer is used.
func NewInMemorySaverWithSerializer(serializer Serializer) *InMemorySaver {
	if serializer == nil {
		serializer = IdentitySerializer{}
	}
	return &InMemorySaver{
		serializer: serializer,
		storage:    make(map[string]map[string]map[string]*CheckpointTuple),
		writes:     make(map[checkpointKey][]PendingWrite),
		blobs:      make(map[blobKey]serializedBlob),
	}
}

// GetTuple returns the most recent checkpoint for config, or nil if none exists.
//
// If config.CheckpointID is set, that specific checkpoint is fetched.
// Otherwise, the latest checkpoint for ThreadID+CheckpointNS is returned.
func (s *InMemorySaver) GetTuple(ctx context.Context, config *Config) (*CheckpointTuple, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if config == nil || config.ThreadID == "" {
		return nil, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	ns := config.CheckpointNS
	byNS := s.storage[config.ThreadID]
	if byNS == nil {
		return nil, nil
	}
	byID := byNS[ns]
	if len(byID) == 0 {
		return nil, nil
	}

	if config.CheckpointID != "" {
		// Specific checkpoint requested.
		t, ok := byID[config.CheckpointID]
		if !ok {
			return nil, nil
		}
		return s.hydrateTupleLocked(t)
	}

	// Return the latest checkpoint for this namespace.
	return s.hydrateTupleLocked(s.latestLocked(config.ThreadID, ns))
}

// Put stores a checkpoint and returns the updated config with CheckpointID set.
func (s *InMemorySaver) Put(
	ctx context.Context,
	config *Config,
	cp *Checkpoint,
	meta *CheckpointMetadata,
) (*Config, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if config == nil || config.ThreadID == "" {
		return config, fmt.Errorf("checkpoint.Put: Config.ThreadID must not be empty")
	}
	if cp == nil {
		return config, fmt.Errorf("checkpoint.Put: checkpoint must not be nil")
	}

	cpToStore := CopyCheckpoint(cp)
	if cpToStore.ID == "" {
		cpToStore.ID = newCheckpointID()
	}
	cpToStore.TS = EnsureTimestamp(cpToStore.TS)

	serializedCheckpoint, err := serializeCheckpoint(cpToStore, s.serializer)
	if err != nil {
		return config, fmt.Errorf("checkpoint.Put: serialize checkpoint: %w", err)
	}
	serializedMetadata, err := serializeMetadata(meta, s.serializer)
	if err != nil {
		return config, fmt.Errorf("checkpoint.Put: serialize metadata: %w", err)
	}

	ns := config.CheckpointNS
	t := &CheckpointTuple{
		Config:     &Config{ThreadID: config.ThreadID, CheckpointNS: ns, CheckpointID: serializedCheckpoint.ID},
		Checkpoint: serializedCheckpoint,
		Metadata:   serializedMetadata,
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.storage[config.ThreadID] == nil {
		s.storage[config.ThreadID] = make(map[string]map[string]*CheckpointTuple)
	}
	if s.storage[config.ThreadID][ns] == nil {
		s.storage[config.ThreadID][ns] = make(map[string]*CheckpointTuple)
	}

	// Parent linkage follows the caller-provided checkpoint ID (fork/resume
	// aware), mirroring LangGraph behavior.
	if config.CheckpointID != "" {
		t.ParentConfig = &Config{
			ThreadID:     config.ThreadID,
			CheckpointNS: ns,
			CheckpointID: config.CheckpointID,
		}
	}

	s.storage[config.ThreadID][ns][serializedCheckpoint.ID] = t
	s.storeCheckpointBlobsLocked(config.ThreadID, ns, serializedCheckpoint)

	return &Config{
		ThreadID:     config.ThreadID,
		CheckpointNS: ns,
		CheckpointID: serializedCheckpoint.ID,
	}, nil
}

// PutWrites records pending writes associated with a checkpoint.
func (s *InMemorySaver) PutWrites(
	ctx context.Context,
	config *Config,
	writes []PendingWrite,
	taskID string,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if config == nil || config.ThreadID == "" || config.CheckpointID == "" {
		return nil
	}
	key := checkpointKey{
		threadID:     config.ThreadID,
		ns:           config.CheckpointNS,
		checkpointID: config.CheckpointID,
	}

	serializedWrites, err := serializePendingWrites(writes, s.serializer)
	if err != nil {
		return fmt.Errorf("checkpoint.PutWrites: serialize writes: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	existing := s.writes[key]

	regularSeen := make(map[string]map[int]struct{})
	specialPos := make(map[string]map[int]int)
	regularCounter := make(map[string]int)

	for i, w := range existing {
		id := w.TaskID
		if id == "" {
			id = taskID
			existing[i].TaskID = id
		}

		slot, special := writeSlot(w.Channel, 0)
		if special {
			if specialPos[id] == nil {
				specialPos[id] = make(map[int]int)
			}
			specialPos[id][slot] = i
			continue
		}

		slot = regularCounter[id]
		regularCounter[id]++
		if regularSeen[id] == nil {
			regularSeen[id] = make(map[int]struct{})
		}
		regularSeen[id][slot] = struct{}{}
	}

	for i, w := range serializedWrites {
		id := taskID
		if id == "" {
			id = w.TaskID
		}
		w.TaskID = id

		slot, special := writeSlot(w.Channel, i)
		if special {
			if specialPos[id] != nil {
				if pos, ok := specialPos[id][slot]; ok {
					existing[pos] = w
					continue
				}
			}
			existing = append(existing, w)
			if specialPos[id] == nil {
				specialPos[id] = make(map[int]int)
			}
			specialPos[id][slot] = len(existing) - 1
			continue
		}

		if regularSeen[id] != nil {
			if _, ok := regularSeen[id][slot]; ok {
				continue
			}
		}
		existing = append(existing, w)
		if regularSeen[id] == nil {
			regularSeen[id] = make(map[int]struct{})
		}
		regularSeen[id][slot] = struct{}{}
	}

	s.writes[key] = existing
	return nil
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

// List returns all checkpoints for the given config, newest first.
// opts.Limit caps the number of results; opts.Before restricts to checkpoints
// with IDs strictly less than opts.Before.CheckpointID.
func (s *InMemorySaver) List(
	ctx context.Context,
	config *Config,
	opts ListOptions,
) ([]*CheckpointTuple, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if config != nil && config.ThreadID == "" {
		return nil, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Collect all tuples and sort newest-first.
	tuples := make([]*CheckpointTuple, 0)
	if config == nil {
		for _, byNS := range s.storage {
			for _, byID := range byNS {
				for _, t := range byID {
					if opts.Before != nil && opts.Before.CheckpointID != "" {
						if t.Checkpoint.ID >= opts.Before.CheckpointID {
							continue
						}
					}
					tuples = append(tuples, t)
				}
			}
		}
	} else {
		byNS := s.storage[config.ThreadID]
		if byNS == nil {
			return nil, nil
		}
		ns := config.CheckpointNS
		byID := byNS[ns]
		for _, t := range byID {
			if opts.Before != nil && opts.Before.CheckpointID != "" {
				if t.Checkpoint.ID >= opts.Before.CheckpointID {
					continue
				}
			}
			tuples = append(tuples, t)
		}
	}

	// Sort newest-first (descending by checkpoint ID string).
	sortByIDDesc(tuples)

	if opts.Limit > 0 && len(tuples) > opts.Limit {
		tuples = tuples[:opts.Limit]
	}

	// Attach pending writes and deserialize.
	result := make([]*CheckpointTuple, 0, len(tuples))
	for _, t := range tuples {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		hydrated, err := s.hydrateTupleLocked(t)
		if err != nil {
			return nil, err
		}
		if !MetadataMatchesFilter(hydrated.Metadata, opts.Filter) {
			continue
		}
		result = append(result, hydrated)
	}
	return result, nil
}

// DeleteThread removes all checkpoints and writes for a thread.
func (s *InMemorySaver) DeleteThread(ctx context.Context, threadID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if threadID == "" {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.storage, threadID)
	for k := range s.writes {
		if k.threadID == threadID {
			delete(s.writes, k)
		}
	}
	for k := range s.blobs {
		if k.threadID == threadID {
			delete(s.blobs, k)
		}
	}
	return nil
}

// DeleteForRuns removes all checkpoints and writes associated with run IDs.
func (s *InMemorySaver) DeleteForRuns(ctx context.Context, runIDs []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(runIDs) == 0 {
		return nil
	}
	runSet := make(map[string]struct{}, len(runIDs))
	for _, id := range runIDs {
		if id != "" {
			runSet[id] = struct{}{}
		}
	}
	if len(runSet) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for threadID, byNS := range s.storage {
		for ns, byID := range byNS {
			for checkpointID, tuple := range byID {
				if tuple == nil || tuple.Metadata == nil {
					continue
				}
				if _, ok := runSet[tuple.Metadata.RunID]; !ok {
					continue
				}
				delete(byID, checkpointID)
				delete(s.writes, checkpointKey{threadID: threadID, ns: ns, checkpointID: checkpointID})
				s.deleteBlobRefsForCheckpointLocked(threadID, ns, tuple.Checkpoint)
			}
			if len(byID) == 0 {
				delete(byNS, ns)
			}
		}
		if len(byNS) == 0 {
			delete(s.storage, threadID)
		}
	}

	return nil
}

// CopyThread copies all checkpoints and writes from source to target.
func (s *InMemorySaver) CopyThread(ctx context.Context, sourceThreadID, targetThreadID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if sourceThreadID == "" || targetThreadID == "" || sourceThreadID == targetThreadID {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	sourceByNS := s.storage[sourceThreadID]
	if len(sourceByNS) == 0 {
		return nil
	}

	if s.storage[targetThreadID] == nil {
		s.storage[targetThreadID] = make(map[string]map[string]*CheckpointTuple)
	}

	for ns, sourceByID := range sourceByNS {
		if s.storage[targetThreadID][ns] == nil {
			s.storage[targetThreadID][ns] = make(map[string]*CheckpointTuple)
		}
		targetByID := s.storage[targetThreadID][ns]
		for checkpointID, tuple := range sourceByID {
			copied := cloneSerializedTuple(tuple, targetThreadID)
			targetByID[checkpointID] = copied

			sourceKey := checkpointKey{threadID: sourceThreadID, ns: ns, checkpointID: checkpointID}
			targetKey := checkpointKey{threadID: targetThreadID, ns: ns, checkpointID: checkpointID}
			if writes := s.writes[sourceKey]; len(writes) > 0 {
				copiedWrites := make([]PendingWrite, len(writes))
				copy(copiedWrites, writes)
				s.writes[targetKey] = copiedWrites
			} else {
				delete(s.writes, targetKey)
			}
			s.copyBlobRefsForCheckpointLocked(sourceThreadID, targetThreadID, ns, tuple.Checkpoint)
		}
	}

	return nil
}

// Prune removes checkpoints for matching thread IDs according to strategy.
func (s *InMemorySaver) Prune(ctx context.Context, threadIDs []string, strategy PruneStrategy) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(threadIDs) == 0 {
		return nil
	}
	if strategy == "" {
		strategy = PruneStrategyKeepLatest
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, threadID := range threadIDs {
		if threadID == "" {
			continue
		}
		switch strategy {
		case PruneStrategyDelete:
			delete(s.storage, threadID)
			for k := range s.writes {
				if k.threadID == threadID {
					delete(s.writes, k)
				}
			}
			for k := range s.blobs {
				if k.threadID == threadID {
					delete(s.blobs, k)
				}
			}
		case PruneStrategyKeepLatest:
			byNS := s.storage[threadID]
			for ns, byID := range byNS {
				var latestID string
				for checkpointID := range byID {
					if checkpointID > latestID {
						latestID = checkpointID
					}
				}
				for checkpointID := range byID {
					if checkpointID == latestID {
						continue
					}
					tuple := byID[checkpointID]
					delete(byID, checkpointID)
					delete(s.writes, checkpointKey{threadID: threadID, ns: ns, checkpointID: checkpointID})
					s.deleteBlobRefsForCheckpointLocked(threadID, ns, tuple.Checkpoint)
				}
				if len(byID) == 0 {
					delete(byNS, ns)
				}
			}
			if len(byNS) == 0 {
				delete(s.storage, threadID)
			}
		default:
			return fmt.Errorf("checkpoint.Prune: unsupported strategy %q", strategy)
		}
	}

	return nil
}

// hydrateTupleLocked returns a fully materialized tuple with deserialized
// checkpoint values and pending writes attached.
// Must be called with at least a read lock held.
func (s *InMemorySaver) hydrateTupleLocked(t *CheckpointTuple) (*CheckpointTuple, error) {
	if t == nil {
		return nil, nil
	}

	cp, err := deserializeCheckpoint(t.Checkpoint, s.serializer)
	if err != nil {
		return nil, err
	}
	meta, err := deserializeMetadata(t.Metadata, s.serializer)
	if err != nil {
		return nil, err
	}

	out := &CheckpointTuple{
		Config:       copyConfig(t.Config),
		Checkpoint:   cp,
		Metadata:     meta,
		ParentConfig: copyConfig(t.ParentConfig),
	}

	key := checkpointKey{
		threadID:     t.Config.ThreadID,
		ns:           t.Config.CheckpointNS,
		checkpointID: t.Checkpoint.ID,
	}
	if values, err := s.loadCheckpointBlobsLocked(t.Config.ThreadID, t.Config.CheckpointNS, cp.ChannelVersions); err != nil {
		return nil, err
	} else if len(values) > 0 {
		if cp.ChannelValues == nil {
			cp.ChannelValues = map[string]any{}
		}
		for channel, value := range values {
			cp.ChannelValues[channel] = value
		}
	}
	w := s.writes[key]
	if len(w) == 0 {
		return out, nil
	}
	deserializedWrites, err := deserializePendingWrites(w, s.serializer)
	if err != nil {
		return nil, err
	}
	out.PendingWrites = deserializedWrites
	return out, nil
}

// latestLocked returns the latest tuple for (threadID, ns) without acquiring locks.
// Caller must hold at least a read lock.
func (s *InMemorySaver) latestLocked(threadID, ns string) *CheckpointTuple {
	byNS := s.storage[threadID]
	if byNS == nil {
		return nil
	}
	byID := byNS[ns]
	if len(byID) == 0 {
		return nil
	}
	var latestID string
	for checkpointID := range byID {
		if checkpointID > latestID {
			latestID = checkpointID
		}
	}
	return byID[latestID]
}

// sortByIDDesc sorts tuples in-place, descending by Checkpoint.ID.
func sortByIDDesc(tuples []*CheckpointTuple) {
	n := len(tuples)
	for i := 0; i < n-1; i++ {
		for j := i + 1; j < n; j++ {
			if tuples[j].Checkpoint.ID > tuples[i].Checkpoint.ID {
				tuples[i], tuples[j] = tuples[j], tuples[i]
			}
		}
	}
}

// newCheckpointID generates a new timestamp-prefixed checkpoint ID string.
// In production, use UUID6 (monotonically increasing UUID); here we use a
// simple timestamp + random suffix sufficient for in-memory use.
func newCheckpointID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

func copyConfig(cfg *Config) *Config {
	if cfg == nil {
		return nil
	}
	out := *cfg
	return &out
}

func cloneSerializedTuple(t *CheckpointTuple, threadID string) *CheckpointTuple {
	if t == nil {
		return nil
	}
	out := &CheckpointTuple{
		Config:       copyConfig(t.Config),
		Checkpoint:   nil,
		Metadata:     copyMetadata(t.Metadata),
		ParentConfig: copyConfig(t.ParentConfig),
	}
	if t.Checkpoint != nil {
		out.Checkpoint = CopyCheckpoint(t.Checkpoint)
	}
	if out.Config != nil {
		out.Config.ThreadID = threadID
	}
	if out.ParentConfig != nil {
		out.ParentConfig.ThreadID = threadID
	}
	if len(t.PendingWrites) > 0 {
		out.PendingWrites = append([]PendingWrite(nil), t.PendingWrites...)
	}
	return out
}

func copyMetadata(meta *CheckpointMetadata) *CheckpointMetadata {
	if meta == nil {
		return nil
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
	return &out
}

func (s *InMemorySaver) storeCheckpointBlobsLocked(threadID, ns string, cp *Checkpoint) {
	if cp == nil || len(cp.ChannelVersions) == 0 {
		return
	}
	for channel, version := range cp.ChannelVersions {
		key := blobKey{
			threadID: threadID,
			ns:       ns,
			channel:  channel,
			version:  fmt.Sprint(version),
		}
		if _, exists := s.blobs[key]; exists {
			continue
		}
		value, ok := cp.ChannelValues[channel]
		if !ok {
			s.blobs[key] = serializedBlob{empty: true}
			continue
		}
		s.blobs[key] = serializedBlob{value: cloneSerializedBlobValue(value)}
	}
}

func (s *InMemorySaver) loadCheckpointBlobsLocked(threadID, ns string, versions map[string]Version) (map[string]any, error) {
	if len(versions) == 0 {
		return nil, nil
	}
	out := make(map[string]any)
	for channel, version := range versions {
		key := blobKey{threadID: threadID, ns: ns, channel: channel, version: fmt.Sprint(version)}
		blob, ok := s.blobs[key]
		if !ok || blob.empty {
			continue
		}
		decoded, err := s.serializer.Deserialize(cloneSerializedBlobValue(blob.value))
		if err != nil {
			return nil, err
		}
		out[channel] = decoded
	}
	return out, nil
}

func (s *InMemorySaver) deleteBlobRefsForCheckpointLocked(threadID, ns string, cp *Checkpoint) {
	if cp == nil || len(cp.ChannelVersions) == 0 {
		return
	}
	for channel, version := range cp.ChannelVersions {
		delete(s.blobs, blobKey{threadID: threadID, ns: ns, channel: channel, version: fmt.Sprint(version)})
	}
}

func (s *InMemorySaver) copyBlobRefsForCheckpointLocked(sourceThreadID, targetThreadID, ns string, cp *Checkpoint) {
	if cp == nil || len(cp.ChannelVersions) == 0 {
		return
	}
	for channel, version := range cp.ChannelVersions {
		source := blobKey{threadID: sourceThreadID, ns: ns, channel: channel, version: fmt.Sprint(version)}
		blob, ok := s.blobs[source]
		if !ok {
			continue
		}
		target := blobKey{threadID: targetThreadID, ns: ns, channel: channel, version: fmt.Sprint(version)}
		s.blobs[target] = serializedBlob{empty: blob.empty, value: cloneSerializedBlobValue(blob.value)}
	}
}

func cloneSerializedBlobValue(value any) any {
	if b, ok := value.([]byte); ok {
		return append([]byte(nil), b...)
	}
	return value
}

func serializeCheckpoint(cp *Checkpoint, serializer Serializer) (*Checkpoint, error) {
	if cp == nil {
		return nil, nil
	}
	out := CopyCheckpoint(cp)
	if out.ChannelValues == nil {
		out.ChannelValues = map[string]any{}
	}
	for k, v := range out.ChannelValues {
		sv, err := serializer.Serialize(v)
		if err != nil {
			return nil, err
		}
		out.ChannelValues[k] = sv
	}
	if len(out.PendingSends) > 0 {
		pending := make([]any, len(out.PendingSends))
		for i, v := range out.PendingSends {
			sv, err := serializer.Serialize(v)
			if err != nil {
				return nil, err
			}
			pending[i] = sv
		}
		out.PendingSends = pending
	}
	return out, nil
}

func deserializeCheckpoint(cp *Checkpoint, serializer Serializer) (*Checkpoint, error) {
	if cp == nil {
		return nil, nil
	}
	out := CopyCheckpoint(cp)
	for k, v := range out.ChannelValues {
		dv, err := serializer.Deserialize(v)
		if err != nil {
			return nil, err
		}
		out.ChannelValues[k] = dv
	}
	if len(out.PendingSends) > 0 {
		pending := make([]any, len(out.PendingSends))
		for i, v := range out.PendingSends {
			dv, err := serializer.Deserialize(v)
			if err != nil {
				return nil, err
			}
			pending[i] = dv
		}
		out.PendingSends = pending
	}
	if err := applyDefaultMigration(out); err != nil {
		return nil, err
	}
	return out, nil
}

func serializeMetadata(meta *CheckpointMetadata, _ Serializer) (*CheckpointMetadata, error) {
	if meta == nil {
		return nil, nil
	}
	out := *meta
	if len(meta.Parents) > 0 {
		parents := make(map[string]string, len(meta.Parents))
		for k, v := range meta.Parents {
			parents[k] = v
		}
		out.Parents = parents
	}
	if len(meta.Extra) > 0 {
		extra := make(map[string]any, len(meta.Extra))
		for k, v := range meta.Extra {
			extra[k] = v
		}
		out.Extra = extra
	}
	return &out, nil
}

func deserializeMetadata(meta *CheckpointMetadata, _ Serializer) (*CheckpointMetadata, error) {
	if meta == nil {
		return nil, nil
	}
	out := *meta
	if len(meta.Parents) > 0 {
		parents := make(map[string]string, len(meta.Parents))
		for k, v := range meta.Parents {
			parents[k] = v
		}
		out.Parents = parents
	}
	if len(meta.Extra) > 0 {
		extra := make(map[string]any, len(meta.Extra))
		for k, v := range meta.Extra {
			extra[k] = v
		}
		out.Extra = extra
	}
	return &out, nil
}

func serializePendingWrites(writes []PendingWrite, serializer Serializer) ([]PendingWrite, error) {
	out := make([]PendingWrite, len(writes))
	for i, w := range writes {
		value, err := serializer.Serialize(w.Value)
		if err != nil {
			return nil, err
		}
		out[i] = PendingWrite{TaskID: w.TaskID, Channel: w.Channel, Value: value}
	}
	return out, nil
}

func deserializePendingWrites(writes []PendingWrite, serializer Serializer) ([]PendingWrite, error) {
	out := make([]PendingWrite, len(writes))
	for i, w := range writes {
		value, err := serializer.Deserialize(w.Value)
		if err != nil {
			return nil, err
		}
		out[i] = PendingWrite{TaskID: w.TaskID, Channel: w.Channel, Value: value}
	}
	return out, nil
}
