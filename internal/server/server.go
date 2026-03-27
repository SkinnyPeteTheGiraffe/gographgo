package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/checkpoint"
	graphpkg "github.com/SkinnyPeteTheGiraffe/gographgo/pkg/graph"
	"github.com/google/uuid"
)

// Server provides HTTP endpoints for threads/runs/state/events.
type Server struct {
	checkpointer checkpoint.Saver
	runner       GraphRunner
	introspector GraphIntrospector
	threads      map[string]*Thread
	runs         map[string]map[string]*runRecord
	state        map[string]map[string]any
	store        map[string]map[string]map[string]map[string]any
	assistants   map[string]*assistantRecord
	crons        map[string]*Cron
	globalStore  *graphpkg.InMemoryStore
	apiKey       string
	mu           sync.RWMutex
}

type runRecord struct {
	run    *Run
	subs   map[int]chan RunEvent
	done   chan struct{}
	events []RunEvent
	nextID int
	closed bool
}

type assistantRecord struct {
	assistant *Assistant
	versions  []AssistantVersion
	latest    int
}

// Options configures a server instance.
type Options struct {
	Checkpointer checkpoint.Saver
	Runner       GraphRunner
	Introspector GraphIntrospector
	APIKey       string
}

// New creates an HTTP API server.
func New(opts Options) *Server {
	introspector := opts.Introspector
	if introspector == nil {
		if fromRunner, ok := opts.Runner.(GraphIntrospector); ok {
			introspector = fromRunner
		}
	}
	return &Server{
		checkpointer: opts.Checkpointer,
		runner:       opts.Runner,
		introspector: introspector,
		apiKey:       strings.TrimSpace(opts.APIKey),
		threads:      make(map[string]*Thread),
		runs:         make(map[string]map[string]*runRecord),
		state:        make(map[string]map[string]any),
		store:        make(map[string]map[string]map[string]map[string]any),
		assistants:   make(map[string]*assistantRecord),
		crons:        make(map[string]*Cron),
		globalStore:  graphpkg.NewInMemoryStore(),
	}
}

// Handler returns the root HTTP handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/info", s.handleInfo)
	mux.HandleFunc("/v1/assistants", s.handleAssistants)
	mux.HandleFunc("/v1/assistants/", s.handleAssistantSubresources)
	mux.HandleFunc("/v1/runs", s.handleTopLevelRuns)
	mux.HandleFunc("/v1/runs/", s.handleTopLevelRunSubresources)
	mux.HandleFunc("/v1/store/items", s.handleGlobalStoreItems)
	mux.HandleFunc("/v1/store/items/search", s.handleGlobalStoreSearch)
	mux.HandleFunc("/v1/store/namespaces", s.handleGlobalStoreNamespaces)
	mux.HandleFunc("/v1/threads", s.handleThreads)
	mux.HandleFunc("/v1/threads/", s.handleThreadSubresources)
	if s.apiKey == "" {
		return mux
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := strings.TrimSpace(r.Header.Get("X-API-Key"))
		if token == "" {
			authz := strings.TrimSpace(r.Header.Get("Authorization"))
			if strings.HasPrefix(strings.ToLower(authz), "bearer ") {
				token = strings.TrimSpace(authz[len("Bearer "):])
			}
		}
		if token != s.apiKey {
			w.Header().Set("WWW-Authenticate", `Bearer realm="gographgo"`)
			writeError(w, http.StatusUnauthorized, fmt.Errorf("unauthorized"))
			return
		}
		mux.ServeHTTP(w, r)
	})
}

func (s *Server) handleThreads(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/v1/threads" {
		http.NotFound(w, r)
		return
	}
	switch r.Method {
	case http.MethodPost:
		var req CreateThreadRequest
		if err := decodeJSON(r, &req); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		id := req.ThreadID
		if id == "" {
			id = uuid.New().String()
		}
		now := time.Now().UTC()
		thread := &Thread{ID: id, CreatedAt: now, Metadata: cloneMap(req.Metadata)}

		s.mu.Lock()
		if _, exists := s.threads[id]; exists {
			s.mu.Unlock()
			writeError(w, http.StatusConflict, fmt.Errorf("thread already exists"))
			return
		}
		s.threads[id] = thread
		s.mu.Unlock()

		writeJSON(w, http.StatusCreated, thread)
	default:
		writeMethodNotAllowed(w, http.MethodPost)
	}
}

func (s *Server) handleThreadSubresources(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/v1/threads/")
	parts := splitPath(path)
	if len(parts) == 0 {
		http.NotFound(w, r)
		return
	}
	if parts[0] == "search" {
		s.handleThreadsSearch(w, r)
		return
	}
	if parts[0] == "count" {
		s.handleThreadsCount(w, r)
		return
	}
	if parts[0] == "prune" {
		s.handleThreadsPrune(w, r)
		return
	}
	threadID := parts[0]

	if len(parts) == 1 {
		s.handleThread(w, r, threadID)
		return
	}

	switch parts[1] {
	case "runs":
		s.handleRuns(w, r, threadID, parts[2:])
	case "state":
		s.handleState(w, r, threadID)
	case "copy":
		s.handleThreadCopy(w, r, threadID)
	case "history", "checkpoints":
		s.handleHistory(w, r, threadID)
	case "stream":
		s.handleThreadStream(w, r, threadID)
	case "store":
		s.handleStore(w, r, threadID, parts[2:])
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) handleThread(w http.ResponseWriter, r *http.Request, threadID string) {
	switch r.Method {
	case http.MethodGet:
		thread, ok := s.getThread(threadID)
		if !ok {
			writeError(w, http.StatusNotFound, fmt.Errorf("thread not found"))
			return
		}
		writeJSON(w, http.StatusOK, thread)
	case http.MethodPatch:
		var req struct {
			Metadata map[string]any `json:"metadata,omitempty"`
		}
		if err := decodeJSON(r, &req); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		s.mu.Lock()
		thread := s.threads[threadID]
		if thread == nil {
			s.mu.Unlock()
			writeError(w, http.StatusNotFound, fmt.Errorf("thread not found"))
			return
		}
		if req.Metadata != nil {
			thread.Metadata = cloneMap(req.Metadata)
		}
		out := *thread
		out.Metadata = cloneMap(thread.Metadata)
		s.mu.Unlock()
		writeJSON(w, http.StatusOK, out)
	case http.MethodDelete:
		s.mu.Lock()
		if _, ok := s.threads[threadID]; !ok {
			s.mu.Unlock()
			writeError(w, http.StatusNotFound, fmt.Errorf("thread not found"))
			return
		}
		delete(s.threads, threadID)
		delete(s.runs, threadID)
		delete(s.state, threadID)
		delete(s.store, threadID)
		s.mu.Unlock()
		if s.checkpointer != nil {
			if err := s.checkpointer.DeleteThread(r.Context(), threadID); err != nil {
				writeError(w, http.StatusInternalServerError, err)
				return
			}
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		writeMethodNotAllowed(w, http.MethodGet, http.MethodPatch, http.MethodDelete)
	}
}

func (s *Server) handleThreadsSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, http.MethodPost)
		return
	}
	var req struct {
		Metadata map[string]any `json:"metadata,omitempty"`
		Status   string         `json:"status,omitempty"`
		IDs      []string       `json:"ids,omitempty"`
		Limit    int            `json:"limit,omitempty"`
		Offset   int            `json:"offset,omitempty"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	s.mu.RLock()
	out := make([]Thread, 0, len(s.threads))
	for threadID, thread := range s.threads {
		if len(req.IDs) > 0 && !contains(req.IDs, threadID) {
			continue
		}
		if req.Status != "" && s.threadStatusLocked(threadID) != req.Status {
			continue
		}
		if !matchesMetadata(thread.Metadata, req.Metadata) {
			continue
		}
		copyThread := *thread
		copyThread.Metadata = cloneMap(thread.Metadata)
		out = append(out, copyThread)
	}
	s.mu.RUnlock()

	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	if req.Offset > 0 {
		if req.Offset >= len(out) {
			out = []Thread{}
		} else {
			out = out[req.Offset:]
		}
	}
	if req.Limit > 0 && req.Limit < len(out) {
		out = out[:req.Limit]
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleThreadsCount(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, http.MethodPost)
		return
	}
	var req struct {
		Metadata map[string]any `json:"metadata,omitempty"`
		Status   string         `json:"status,omitempty"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	s.mu.RLock()
	count := 0
	for threadID, thread := range s.threads {
		if req.Status != "" && s.threadStatusLocked(threadID) != req.Status {
			continue
		}
		if !matchesMetadata(thread.Metadata, req.Metadata) {
			continue
		}
		count++
	}
	s.mu.RUnlock()
	writeJSON(w, http.StatusOK, map[string]any{"count": count})
}

func (s *Server) handleThreadsPrune(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, http.MethodPost)
		return
	}
	var req struct {
		Strategy  string   `json:"strategy,omitempty"`
		ThreadIDs []string `json:"thread_ids"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	strategy := checkpoint.PruneStrategy(req.Strategy)
	if strategy == "" {
		strategy = checkpoint.PruneStrategyKeepLatest
	}
	if strategy != checkpoint.PruneStrategyKeepLatest && strategy != checkpoint.PruneStrategyDelete {
		writeError(w, http.StatusBadRequest, fmt.Errorf("invalid prune strategy"))
		return
	}
	if s.checkpointer != nil {
		if err := s.checkpointer.Prune(r.Context(), req.ThreadIDs, strategy); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
	}
	s.mu.Lock()
	for _, threadID := range req.ThreadIDs {
		runs := s.runs[threadID]
		switch strategy {
		case checkpoint.PruneStrategyDelete:
			delete(s.runs, threadID)
			delete(s.state, threadID)
		case checkpoint.PruneStrategyKeepLatest:
			if len(runs) > 1 {
				latestID := ""
				var latestCreated time.Time
				for runID, rec := range runs {
					if rec.run == nil {
						continue
					}
					if latestID == "" || rec.run.CreatedAt.After(latestCreated) {
						latestID = runID
						latestCreated = rec.run.CreatedAt
					}
				}
				for runID := range runs {
					if runID != latestID {
						delete(runs, runID)
					}
				}
			}
		}
	}
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"pruned_count": len(req.ThreadIDs)})
}

func (s *Server) handleThreadCopy(w http.ResponseWriter, r *http.Request, threadID string) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, http.MethodPost)
		return
	}
	thread, ok := s.getThread(threadID)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Errorf("thread not found"))
		return
	}
	targetID := uuid.New().String()
	s.mu.Lock()
	s.threads[targetID] = &Thread{ID: targetID, CreatedAt: time.Now().UTC(), Metadata: cloneMap(thread.Metadata)}
	if state := s.state[threadID]; state != nil {
		s.state[targetID] = cloneMap(state)
	}
	if bucket := s.store[threadID]; bucket != nil {
		copied := make(map[string]map[string]map[string]any, len(bucket))
		for namespace, values := range bucket {
			nsCopy := make(map[string]map[string]any, len(values))
			for key, value := range values {
				nsCopy[key] = cloneMap(value)
			}
			copied[namespace] = nsCopy
		}
		s.store[targetID] = copied
	}
	s.mu.Unlock()
	if s.checkpointer != nil {
		if err := s.checkpointer.CopyThread(r.Context(), threadID, targetID); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
	}
	writeJSON(w, http.StatusCreated, map[string]any{"thread_id": targetID})
}

func (s *Server) handleThreadStream(w http.ResponseWriter, r *http.Request, threadID string) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, http.MethodGet)
		return
	}
	if _, ok := s.getThread(threadID); !ok {
		writeError(w, http.StatusNotFound, fmt.Errorf("thread not found"))
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	reconnectLocation := "/v1/threads/" + threadID + "/stream"
	if raw := r.URL.RawQuery; raw != "" {
		reconnectLocation += "?" + raw
	}
	w.Header().Set("Location", reconnectLocation)

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("streaming unsupported"))
		return
	}

	lastID := parseLastEventID(r.Header.Get("Last-Event-ID"))
	ticker := time.NewTicker(75 * time.Millisecond)
	defer ticker.Stop()

	for {
		events := s.collectThreadEvents(threadID)
		for idx, evt := range events {
			id := idx + 1
			if id <= lastID {
				continue
			}
			if err := writeSSEWithID(w, evt.Type, evt, id); err != nil {
				return
			}
			lastID = id
			flusher.Flush()
		}
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Server) handleRuns(w http.ResponseWriter, r *http.Request, threadID string, suffix []string) {
	if len(suffix) == 0 {
		if r.Method != http.MethodPost {
			writeMethodNotAllowed(w, http.MethodPost)
			return
		}
		s.createRun(w, r, threadID)
		return
	}
	if suffix[0] == "stream" {
		if r.Method != http.MethodPost {
			writeMethodNotAllowed(w, http.MethodPost)
			return
		}
		s.streamCreateRun(w, r, threadID)
		return
	}
	if suffix[0] == "crons" {
		s.handleThreadCrons(w, r, threadID, suffix[1:])
		return
	}
	runID := suffix[0]
	if len(suffix) == 1 {
		s.handleRunResource(w, r, threadID, runID)
		return
	}
	if len(suffix) == 2 {
		s.handleRunSubresource(w, r, threadID, runID, suffix[1])
		return
	}
	http.NotFound(w, r)
}

func (s *Server) createRun(w http.ResponseWriter, r *http.Request, threadID string) {
	if _, ok := s.getThread(threadID); !ok {
		writeError(w, http.StatusNotFound, fmt.Errorf("thread not found"))
		return
	}

	var req CreateRunRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	now := time.Now().UTC()
	s.mu.Lock()
	run := s.createRunLocked(threadID, req)
	s.mu.Unlock()

	s.publishEvent(threadID, run.ID, RunEvent{Type: "run.created", ThreadID: threadID, RunID: run.ID, Timestamp: now, Payload: map[string]any{"status": string(RunStatusPending)}})

	w.Header().Set("Content-Location", fmt.Sprintf("/v1/threads/%s/runs/%s", threadID, run.ID))
	writeJSON(w, http.StatusAccepted, run)

	runCtx := context.WithoutCancel(r.Context())
	go s.executeRun(runCtx, run.ID, threadID)
}

func (s *Server) createRunLocked(threadID string, req CreateRunRequest) *Run {
	runID := uuid.New().String()
	now := time.Now().UTC()
	run := &Run{
		ID:          runID,
		ThreadID:    threadID,
		AssistantID: req.AssistantID,
		Status:      RunStatusPending,
		Input:       cloneMap(req.Input),
		Metadata:    cloneMap(req.Metadata),
		CreatedAt:   now,
	}
	if s.runs[threadID] == nil {
		s.runs[threadID] = make(map[string]*runRecord)
	}
	rec := &runRecord{run: run, subs: make(map[int]chan RunEvent), done: make(chan struct{})}
	s.runs[threadID][runID] = rec
	return run
}

func (s *Server) executeRun(ctx context.Context, runID, threadID string) {
	now := time.Now().UTC()
	s.mu.Lock()
	rec := s.runs[threadID][runID]
	if rec == nil {
		s.mu.Unlock()
		return
	}
	rec.run.Status = RunStatusRunning
	rec.run.StartedAt = &now
	req := RunRequest{
		ThreadID:    threadID,
		RunID:       runID,
		AssistantID: rec.run.AssistantID,
		Input:       cloneMap(rec.run.Input),
		Metadata:    cloneMap(rec.run.Metadata),
	}
	s.mu.Unlock()

	s.publishEvent(threadID, runID, RunEvent{Type: "run.started", ThreadID: threadID, RunID: runID, Timestamp: now, Payload: map[string]any{"status": string(RunStatusRunning)}})

	runner := s.runner
	if runner == nil {
		runner = passthroughRunner{}
	}

	result, err := runner.Run(ctx, req)
	ended := time.Now().UTC()

	s.mu.Lock()
	defer s.mu.Unlock()
	rec = s.runs[threadID][runID]
	rec.run.EndedAt = &ended
	if err != nil {
		rec.run.Status = RunStatusError
		rec.run.Error = err.Error()
		s.publishEventLocked(rec, RunEvent{Type: "run.error", ThreadID: threadID, RunID: runID, Timestamp: ended, Payload: map[string]any{"error": err.Error(), "status": string(RunStatusError)}})
		s.closeRecordLocked(rec)
		return
	}

	rec.run.Status = RunStatusSuccess
	rec.run.Output = cloneMap(result.Output)
	state := cloneMap(result.State)
	if len(state) == 0 {
		state = cloneMap(result.Output)
	}
	s.state[threadID] = state

	if s.checkpointer != nil {
		cp := checkpoint.EmptyCheckpoint(uuid.New().String())
		cp.ChannelValues = cloneMap(state)
		meta := &checkpoint.CheckpointMetadata{Source: "loop", Step: 1, RunID: runID}
		_, _ = s.checkpointer.Put(ctx, &checkpoint.Config{ThreadID: threadID}, cp, meta)
	}

	s.publishEventLocked(rec, RunEvent{Type: "run.completed", ThreadID: threadID, RunID: runID, Timestamp: ended, Payload: map[string]any{"output": cloneMap(result.Output), "status": string(RunStatusSuccess)}})
	s.closeRecordLocked(rec)
}

func (s *Server) getRun(w http.ResponseWriter, _ *http.Request, threadID, runID string) {
	rec, ok := s.getRunRecord(threadID, runID)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Errorf("run not found"))
		return
	}
	writeJSON(w, http.StatusOK, rec.run)
}

func (s *Server) streamRunEvents(w http.ResponseWriter, r *http.Request, threadID, runID string) {
	rec, ok := s.getRunRecord(threadID, runID)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Errorf("run not found"))
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Location", fmt.Sprintf("/v1/threads/%s/runs/%s/events", threadID, runID))

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("streaming unsupported"))
		return
	}

	lastID := parseLastEventID(r.Header.Get("Last-Event-ID"))
	for idx, evt := range rec.events {
		eventID := idx + 1
		if eventID <= lastID {
			continue
		}
		if err := writeSSEWithID(w, evt.Type, evt, eventID); err != nil {
			return
		}
		lastID = eventID
	}
	flusher.Flush()

	s.mu.Lock()
	rec = s.runs[threadID][runID]
	if rec.closed {
		s.mu.Unlock()
		return
	}
	subID := rec.nextID
	rec.nextID++
	ch := make(chan RunEvent, 16)
	rec.subs[subID] = ch
	s.mu.Unlock()
	defer s.removeSubscriber(threadID, runID, subID)

	for {
		select {
		case <-r.Context().Done():
			return
		case evt, ok := <-ch:
			if !ok {
				return
			}
			lastID++
			if err := writeSSEWithID(w, evt.Type, evt, lastID); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func (s *Server) handleState(w http.ResponseWriter, r *http.Request, threadID string) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		writeMethodNotAllowed(w, http.MethodGet, http.MethodPost)
		return
	}
	if _, ok := s.getThread(threadID); !ok {
		writeError(w, http.StatusNotFound, fmt.Errorf("thread not found"))
		return
	}
	if r.Method == http.MethodPost {
		var req struct {
			Values       map[string]any `json:"values"`
			Checkpoint   map[string]any `json:"checkpoint,omitempty"`
			AsNode       string         `json:"as_node,omitempty"`
			CheckpointID string         `json:"checkpoint_id,omitempty"`
		}
		if err := decodeJSON(r, &req); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		if req.Values == nil {
			writeError(w, http.StatusBadRequest, fmt.Errorf("values are required"))
			return
		}
		checkpointID := req.CheckpointID
		if checkpointID == "" {
			checkpointID = checkpointIDFromMap(req.Checkpoint)
		}
		baseValues := map[string]any{}
		metaStep := -1
		if s.checkpointer != nil {
			tuple, err := s.checkpointer.GetTuple(r.Context(), &checkpoint.Config{ThreadID: threadID, CheckpointID: checkpointID})
			if err != nil {
				writeError(w, http.StatusInternalServerError, err)
				return
			}
			if tuple != nil && tuple.Checkpoint != nil {
				baseValues = cloneMap(tuple.Checkpoint.ChannelValues)
				if tuple.Metadata != nil {
					metaStep = tuple.Metadata.Step
				}
			}
		} else if checkpointID != "" {
			writeError(w, http.StatusBadRequest, fmt.Errorf("checkpoint updates require a configured checkpointer"))
			return
		}
		if len(baseValues) == 0 {
			s.mu.RLock()
			baseValues = cloneMap(s.state[threadID])
			s.mu.RUnlock()
		}
		merged := mergeMaps(baseValues, req.Values)

		s.mu.Lock()
		s.state[threadID] = cloneMap(merged)
		s.mu.Unlock()

		storedCheckpointID := checkpointID
		if storedCheckpointID == "" {
			storedCheckpointID = uuid.New().String()
		}
		if s.checkpointer != nil {
			cp := checkpoint.EmptyCheckpoint(storedCheckpointID)
			cp.ChannelValues = cloneMap(merged)
			meta := &checkpoint.CheckpointMetadata{Source: "update", Step: metaStep + 1}
			if req.AsNode != "" {
				meta.Extra = map[string]any{"as_node": req.AsNode}
			}
			cfg, err := s.checkpointer.Put(r.Context(), &checkpoint.Config{ThreadID: threadID, CheckpointID: checkpointID}, cp, meta)
			if err != nil {
				writeError(w, http.StatusInternalServerError, err)
				return
			}
			if cfg != nil && cfg.CheckpointID != "" {
				storedCheckpointID = cfg.CheckpointID
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"checkpoint": map[string]any{"thread_id": threadID, "checkpoint_id": storedCheckpointID}})
		return
	}

	if s.checkpointer != nil {
		tuple, err := s.checkpointer.GetTuple(r.Context(), &checkpoint.Config{ThreadID: threadID})
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		if tuple != nil {
			meta := map[string]any{}
			if tuple.Metadata != nil {
				meta["source"] = tuple.Metadata.Source
				meta["step"] = tuple.Metadata.Step
				meta["run_id"] = tuple.Metadata.RunID
				for k, v := range tuple.Metadata.Extra {
					meta[k] = v
				}
			}
			writeJSON(w, http.StatusOK, ThreadState{
				ThreadID:     threadID,
				CheckpointID: tuple.Checkpoint.ID,
				Values:       cloneMap(tuple.Checkpoint.ChannelValues),
				Metadata:     meta,
				CreatedAt:    parseCheckpointTime(tuple),
				ParentConfig: checkpointConfigMap(tuple.ParentConfig),
			})
			return
		}
	}

	s.mu.RLock()
	values := cloneMap(s.state[threadID])
	s.mu.RUnlock()
	writeJSON(w, http.StatusOK, ThreadState{ThreadID: threadID, Values: values})
}

func (s *Server) handleHistory(w http.ResponseWriter, r *http.Request, threadID string) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		writeMethodNotAllowed(w, http.MethodGet, http.MethodPost)
		return
	}
	if _, ok := s.getThread(threadID); !ok {
		writeError(w, http.StatusNotFound, fmt.Errorf("thread not found"))
		return
	}
	if r.Method == http.MethodPost {
		var req struct {
			Before     map[string]any `json:"before,omitempty"`
			Metadata   map[string]any `json:"metadata,omitempty"`
			Checkpoint map[string]any `json:"checkpoint,omitempty"`
			Limit      int            `json:"limit,omitempty"`
		}
		if err := decodeJSON(r, &req); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		tuples := []*checkpoint.CheckpointTuple{}
		if s.checkpointer != nil {
			var err error
			tuples, err = s.checkpointer.List(
				r.Context(),
				&checkpoint.Config{ThreadID: threadID, CheckpointID: checkpointIDFromMap(req.Checkpoint)},
				checkpoint.ListOptions{Limit: req.Limit, Before: checkpointConfigFromMap(req.Before), Filter: req.Metadata},
			)
			if err != nil {
				writeError(w, http.StatusInternalServerError, err)
				return
			}
		}
		states := make([]ThreadState, 0, len(tuples))
		for _, tuple := range tuples {
			state := ThreadState{ThreadID: threadID}
			if tuple != nil && tuple.Checkpoint != nil {
				state.CheckpointID = tuple.Checkpoint.ID
				state.Values = cloneMap(tuple.Checkpoint.ChannelValues)
			}
			if tuple != nil && tuple.Metadata != nil {
				state.Metadata = map[string]any{"source": tuple.Metadata.Source, "step": tuple.Metadata.Step, "run_id": tuple.Metadata.RunID}
				for k, v := range tuple.Metadata.Extra {
					state.Metadata[k] = v
				}
			}
			state.CreatedAt = parseCheckpointTime(tuple)
			state.ParentConfig = checkpointConfigMap(tuple.ParentConfig)
			states = append(states, state)
		}
		writeJSON(w, http.StatusOK, states)
		return
	}
	if s.checkpointer == nil {
		writeJSON(w, http.StatusOK, []*checkpoint.CheckpointTuple{})
		return
	}
	tuples, err := s.checkpointer.List(r.Context(), &checkpoint.Config{ThreadID: threadID}, checkpoint.ListOptions{})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, tuples)
}

func (s *Server) handleStore(w http.ResponseWriter, r *http.Request, threadID string, suffix []string) {
	if len(suffix) == 1 && suffix[0] == "namespaces" {
		if r.Method != http.MethodPost {
			writeMethodNotAllowed(w, http.MethodPost)
			return
		}
		var req struct {
			MaxDepth *int     `json:"max_depth,omitempty"`
			Prefix   []string `json:"prefix,omitempty"`
			Suffix   []string `json:"suffix,omitempty"`
			Limit    int      `json:"limit,omitempty"`
			Offset   int      `json:"offset,omitempty"`
		}
		if err := decodeJSON(r, &req); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		if err := rejectUnsupportedStoreNamespaces(req.MaxDepth); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}

		s.mu.RLock()
		bucket := s.store[threadID]
		s.mu.RUnlock()
		namespaces := make([][]string, 0, len(bucket))
		for ns := range bucket {
			parts := strings.Split(ns, ".")
			if len(req.Prefix) > 0 {
				if len(parts) < len(req.Prefix) {
					continue
				}
				matched := true
				for i, part := range req.Prefix {
					if parts[i] != part {
						matched = false
						break
					}
				}
				if !matched {
					continue
				}
			}
			if len(req.Suffix) > 0 {
				if len(parts) < len(req.Suffix) {
					continue
				}
				start := len(parts) - len(req.Suffix)
				matched := true
				for i, part := range req.Suffix {
					if parts[start+i] != part {
						matched = false
						break
					}
				}
				if !matched {
					continue
				}
			}
			namespaces = append(namespaces, parts)
		}
		sort.Slice(namespaces, func(i, j int) bool {
			return strings.Join(namespaces[i], ".") < strings.Join(namespaces[j], ".")
		})
		if req.Offset > 0 {
			if req.Offset >= len(namespaces) {
				namespaces = [][]string{}
			} else {
				namespaces = namespaces[req.Offset:]
			}
		}
		if req.Limit > 0 && req.Limit < len(namespaces) {
			namespaces = namespaces[:req.Limit]
		}
		writeJSON(w, http.StatusOK, map[string]any{"namespaces": namespaces})
		return
	}

	if len(suffix) < 2 {
		http.NotFound(w, r)
		return
	}
	namespace := suffix[0]
	key := suffix[1]

	switch r.Method {
	case http.MethodPut:
		var req struct {
			Value map[string]any `json:"value"`
			Index any            `json:"index,omitempty"`
			TTL   *int           `json:"ttl,omitempty"`
		}
		if err := decodeJSON(r, &req); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		if err := rejectUnsupportedStorePut(req.Index, req.TTL); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		s.mu.Lock()
		if s.store[threadID] == nil {
			s.store[threadID] = make(map[string]map[string]map[string]any)
		}
		if s.store[threadID][namespace] == nil {
			s.store[threadID][namespace] = make(map[string]map[string]any)
		}
		s.store[threadID][namespace][key] = cloneMap(req.Value)
		s.mu.Unlock()
		writeJSON(w, http.StatusOK, StoreValue{Namespace: namespace, Key: key, Value: cloneMap(req.Value)})
	case http.MethodGet:
		refreshTTL, err := parseOptionalBoolQueryValue(r.URL.Query().Get("refresh_ttl"))
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		if err := rejectUnsupportedStoreSearch(nil, r.URL.Query().Get("query"), refreshTTL); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		s.mu.RLock()
		v := cloneMap(s.store[threadID][namespace][key])
		s.mu.RUnlock()
		if v == nil {
			writeError(w, http.StatusNotFound, fmt.Errorf("store key not found"))
			return
		}
		writeJSON(w, http.StatusOK, StoreValue{Namespace: namespace, Key: key, Value: v})
	default:
		writeMethodNotAllowed(w, http.MethodGet, http.MethodPut)
	}
}

func (s *Server) handleInfo(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/v1/info" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, http.MethodGet)
		return
	}
	writeJSON(w, http.StatusOK, Info{
		Version: "v1",
		Capabilities: map[string]any{
			"assistants":                     true,
			"assistant_graph_introspection":  s.introspector != nil,
			"assistant_schemas":              s.introspector != nil,
			"assistant_subgraphs":            s.introspector != nil,
			"thread_state_update_checkpoint": s.checkpointer != nil,
			"thread_history_metadata_filter": s.checkpointer != nil,
			"store_ttl":                      true,
			"store_vector_search":            true,
			"store_query_filter":             true,
			"store_refresh_ttl":              true,
			"store_max_depth":                true,
			"custom_auth_hooks":              false,
			"cors":                           false,
			"metrics":                        false,
			"openapi":                        false,
			"runs": map[string]any{
				"top_level": true,
				"stream":    true,
				"batch":     true,
			},
			"crons": true,
			"store": map[string]any{
				"global": true,
				"thread": true,
			},
		},
	})
}

func (s *Server) handleAssistants(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/v1/assistants" {
		http.NotFound(w, r)
		return
	}
	switch r.Method {
	case http.MethodPost:
		var req struct {
			Config      map[string]any `json:"config,omitempty"`
			Context     map[string]any `json:"context,omitempty"`
			Metadata    map[string]any `json:"metadata,omitempty"`
			AssistantID string         `json:"assistant_id,omitempty"`
			GraphID     string         `json:"graph_id,omitempty"`
			IfExists    string         `json:"if_exists,omitempty"`
			Name        string         `json:"name,omitempty"`
			Description string         `json:"description,omitempty"`
		}
		if err := decodeJSON(r, &req); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		now := time.Now().UTC()
		assistantID := req.AssistantID
		if assistantID == "" {
			assistantID = uuid.New().String()
		}
		if req.IfExists != "" && req.IfExists != "raise" && req.IfExists != "do_nothing" {
			writeError(w, http.StatusBadRequest, fmt.Errorf("invalid if_exists %q", req.IfExists))
			return
		}
		assistant := &Assistant{
			ID:          assistantID,
			GraphID:     req.GraphID,
			Name:        req.Name,
			Description: req.Description,
			Config:      cloneMap(req.Config),
			Context:     cloneMap(req.Context),
			Metadata:    cloneMap(req.Metadata),
			CreatedAt:   now,
			UpdatedAt:   now,
		}

		s.mu.Lock()
		if existing, exists := s.assistants[assistantID]; exists {
			s.mu.Unlock()
			if req.IfExists == "do_nothing" {
				writeJSON(w, http.StatusOK, cloneAssistant(existing.assistant))
				return
			}
			writeError(w, http.StatusConflict, fmt.Errorf("assistant already exists"))
			return
		}
		s.assistants[assistantID] = &assistantRecord{
			assistant: assistant,
			versions:  []AssistantVersion{{Version: 1, Metadata: cloneMap(req.Metadata), CreatedAt: now}},
			latest:    1,
		}
		s.mu.Unlock()

		writeJSON(w, http.StatusCreated, assistant)
	default:
		writeMethodNotAllowed(w, http.MethodPost)
	}
}

func (s *Server) handleAssistantSubresources(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/v1/assistants/")
	parts := splitPath(path)
	if len(parts) == 0 {
		http.NotFound(w, r)
		return
	}
	if parts[0] == "search" {
		s.handleAssistantsSearch(w, r)
		return
	}
	if parts[0] == "count" {
		s.handleAssistantsCount(w, r)
		return
	}

	assistantID := parts[0]
	if len(parts) == 1 {
		s.handleAssistant(w, r, assistantID)
		return
	}

	switch parts[1] {
	case "graph":
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w, http.MethodGet)
			return
		}
		assistant, ok := s.getAssistant(assistantID)
		if !ok {
			writeError(w, http.StatusNotFound, fmt.Errorf("assistant not found"))
			return
		}
		info, err := s.resolveAssistantGraph(r.Context(), assistant)
		if err != nil {
			s.writeAssistantGraphError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, AssistantGraphResponse{
			AssistantID: assistant.ID,
			GraphID:     assistant.GraphID,
			Graph:       info,
			Mermaid:     info.Mermaid(),
		})
	case "schemas":
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w, http.MethodGet)
			return
		}
		assistant, ok := s.getAssistant(assistantID)
		if !ok {
			writeError(w, http.StatusNotFound, fmt.Errorf("assistant not found"))
			return
		}
		info, err := s.resolveAssistantGraph(r.Context(), assistant)
		if err != nil {
			s.writeAssistantGraphError(w, err)
			return
		}
		schemas := info.Schemas()
		writeJSON(w, http.StatusOK, AssistantSchemasResponse{
			AssistantID: assistant.ID,
			GraphID:     assistant.GraphID,
			Input:       schemas.Input,
			Output:      schemas.Output,
			Context:     schemas.Context,
		})
	case "subgraphs":
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w, http.MethodGet)
			return
		}
		assistant, ok := s.getAssistant(assistantID)
		if !ok {
			writeError(w, http.StatusNotFound, fmt.Errorf("assistant not found"))
			return
		}
		info, err := s.resolveAssistantGraph(r.Context(), assistant)
		if err != nil {
			s.writeAssistantGraphError(w, err)
			return
		}
		namespace := ""
		if len(parts) > 2 {
			namespace = strings.Join(parts[2:], "/")
		}
		recurse := strings.EqualFold(r.URL.Query().Get("recurse"), "true")
		subgraphs := filterSubgraphs(info.Subgraphs(), namespace, recurse)
		out := make([]AssistantSubgraph, 0, len(subgraphs))
		for _, subgraph := range subgraphs {
			out = append(out, AssistantSubgraph{
				Name:       subgraph.Name,
				ParentNode: subgraph.ParentNode,
				Namespace:  subgraph.Namespace,
			})
		}
		writeJSON(w, http.StatusOK, AssistantSubgraphsResponse{AssistantID: assistant.ID, GraphID: assistant.GraphID, Subgraphs: out})
	case "versions":
		s.handleAssistantVersions(w, r, assistantID)
	case "latest":
		s.handleAssistantLatest(w, r, assistantID)
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) handleAssistant(w http.ResponseWriter, r *http.Request, assistantID string) {
	switch r.Method {
	case http.MethodGet:
		assistant, ok := s.getAssistant(assistantID)
		if !ok {
			writeError(w, http.StatusNotFound, fmt.Errorf("assistant not found"))
			return
		}
		writeJSON(w, http.StatusOK, assistant)
	case http.MethodPatch:
		var req struct {
			Config      map[string]any `json:"config,omitempty"`
			Context     map[string]any `json:"context,omitempty"`
			Metadata    map[string]any `json:"metadata,omitempty"`
			GraphID     string         `json:"graph_id,omitempty"`
			Name        string         `json:"name,omitempty"`
			Description string         `json:"description,omitempty"`
		}
		if err := decodeJSON(r, &req); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		now := time.Now().UTC()
		s.mu.Lock()
		rec := s.assistants[assistantID]
		if rec == nil {
			s.mu.Unlock()
			writeError(w, http.StatusNotFound, fmt.Errorf("assistant not found"))
			return
		}
		if req.GraphID != "" {
			rec.assistant.GraphID = req.GraphID
		}
		if req.Name != "" {
			rec.assistant.Name = req.Name
		}
		if req.Description != "" {
			rec.assistant.Description = req.Description
		}
		if req.Config != nil {
			rec.assistant.Config = cloneMap(req.Config)
		}
		if req.Context != nil {
			rec.assistant.Context = cloneMap(req.Context)
		}
		if req.Metadata != nil {
			rec.assistant.Metadata = cloneMap(req.Metadata)
		}
		rec.assistant.UpdatedAt = now
		nextVersion := len(rec.versions) + 1
		rec.versions = append(rec.versions, AssistantVersion{Version: nextVersion, Metadata: cloneMap(rec.assistant.Metadata), CreatedAt: now})
		rec.latest = nextVersion
		assistant := cloneAssistant(rec.assistant)
		s.mu.Unlock()
		writeJSON(w, http.StatusOK, assistant)
	case http.MethodDelete:
		deleteThreads, err := parseOptionalBoolQuery(r, "delete_threads")
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}

		deletedThreadIDs := []string(nil)
		s.mu.Lock()
		if _, ok := s.assistants[assistantID]; !ok {
			s.mu.Unlock()
			writeError(w, http.StatusNotFound, fmt.Errorf("assistant not found"))
			return
		}
		if deleteThreads {
			deletedThreadIDs = s.deleteAssistantThreadsLocked(assistantID)
		}
		delete(s.assistants, assistantID)
		s.mu.Unlock()
		if s.checkpointer != nil {
			for _, threadID := range deletedThreadIDs {
				if err := s.checkpointer.DeleteThread(r.Context(), threadID); err != nil {
					writeError(w, http.StatusInternalServerError, err)
					return
				}
			}
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		writeMethodNotAllowed(w, http.MethodGet, http.MethodPatch, http.MethodDelete)
	}
}

func (s *Server) handleAssistantsSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, http.MethodPost)
		return
	}
	var req struct {
		Metadata map[string]any `json:"metadata,omitempty"`
		GraphID  string         `json:"graph_id,omitempty"`
		Name     string         `json:"name,omitempty"`
		Limit    int            `json:"limit,omitempty"`
		Offset   int            `json:"offset,omitempty"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	all := s.filteredAssistants(req.Metadata, req.GraphID, req.Name)
	page := applyOffsetLimitAssistants(all, req.Offset, req.Limit)
	if next, ok := assistantPaginationNext(len(all), req.Offset, len(page), req.Limit); ok {
		w.Header().Set("X-Pagination-Next", next)
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) handleAssistantsCount(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, http.MethodPost)
		return
	}
	var req struct {
		Metadata map[string]any `json:"metadata,omitempty"`
		GraphID  string         `json:"graph_id,omitempty"`
		Name     string         `json:"name,omitempty"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"count": len(s.filteredAssistants(req.Metadata, req.GraphID, req.Name))})
}

func (s *Server) handleAssistantVersions(w http.ResponseWriter, r *http.Request, assistantID string) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, http.MethodPost)
		return
	}
	var req struct {
		Metadata map[string]any `json:"metadata,omitempty"`
		Limit    int            `json:"limit,omitempty"`
		Offset   int            `json:"offset,omitempty"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	s.mu.RLock()
	rec := s.assistants[assistantID]
	if rec == nil {
		s.mu.RUnlock()
		writeError(w, http.StatusNotFound, fmt.Errorf("assistant not found"))
		return
	}
	versions := append([]AssistantVersion(nil), rec.versions...)
	s.mu.RUnlock()
	if len(req.Metadata) > 0 {
		filtered := make([]AssistantVersion, 0, len(versions))
		for _, version := range versions {
			if matchesMetadata(version.Metadata, req.Metadata) {
				filtered = append(filtered, version)
			}
		}
		versions = filtered
	}

	if req.Offset > 0 {
		if req.Offset >= len(versions) {
			versions = []AssistantVersion{}
		} else {
			versions = versions[req.Offset:]
		}
	}
	if req.Limit > 0 && req.Limit < len(versions) {
		versions = versions[:req.Limit]
	}
	writeJSON(w, http.StatusOK, versions)
}

func (s *Server) handleAssistantLatest(w http.ResponseWriter, r *http.Request, assistantID string) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, http.MethodPost)
		return
	}
	var req struct {
		Version int `json:"version"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	s.mu.Lock()
	rec := s.assistants[assistantID]
	if rec == nil {
		s.mu.Unlock()
		writeError(w, http.StatusNotFound, fmt.Errorf("assistant not found"))
		return
	}
	if req.Version > 0 && req.Version <= len(rec.versions) {
		rec.latest = req.Version
	}
	assistant := cloneAssistant(rec.assistant)
	s.mu.Unlock()

	writeJSON(w, http.StatusOK, assistant)
}

func (s *Server) handleTopLevelRuns(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/v1/runs" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, http.MethodPost)
		return
	}
	s.createRunTopLevel(w, r)
}

func (s *Server) handleTopLevelRunSubresources(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/v1/runs/")
	parts := splitPath(path)
	if len(parts) == 0 {
		http.NotFound(w, r)
		return
	}

	switch parts[0] {
	case "stream":
		if r.Method != http.MethodPost {
			writeMethodNotAllowed(w, http.MethodPost)
			return
		}
		s.streamCreateRun(w, r, "")
	case "batch":
		s.handleRunsBatch(w, r)
	case "cancel":
		s.handleRunsCancelMany(w, r)
	case "crons":
		s.handleGlobalCrons(w, r, parts[1:])
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) createRunTopLevel(w http.ResponseWriter, r *http.Request) {
	threadID := uuid.New().String()
	now := time.Now().UTC()

	s.mu.Lock()
	s.threads[threadID] = &Thread{ID: threadID, CreatedAt: now, Metadata: map[string]any{"source": "top_level_run"}}
	s.mu.Unlock()

	s.createRun(w, r, threadID)
}

func (s *Server) handleRunsBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, http.MethodPost)
		return
	}
	var reqs []CreateRunRequest
	if err := decodeJSON(r, &reqs); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	out := make([]Run, 0, len(reqs))
	for _, req := range reqs {
		threadID := uuid.New().String()
		now := time.Now().UTC()
		s.mu.Lock()
		s.threads[threadID] = &Thread{ID: threadID, CreatedAt: now, Metadata: map[string]any{"source": "batch_run"}}
		run := s.createRunLocked(threadID, req)
		s.mu.Unlock()
		s.publishEvent(threadID, run.ID, RunEvent{Type: "run.created", ThreadID: threadID, RunID: run.ID, Timestamp: now, Payload: map[string]any{"status": string(RunStatusPending)}})
		runCtx := context.WithoutCancel(r.Context())
		go s.executeRun(runCtx, run.ID, threadID)
		out = append(out, *run)
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleRunsCancelMany(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, http.MethodPost)
		return
	}
	var req struct {
		ThreadID string   `json:"thread_id,omitempty"`
		Status   string   `json:"status,omitempty"`
		RunIDs   []string `json:"run_ids,omitempty"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	targets := make([][2]string, 0)
	s.mu.RLock()
	for tid, runs := range s.runs {
		if req.ThreadID != "" && req.ThreadID != tid {
			continue
		}
		for rid, rec := range runs {
			if len(req.RunIDs) > 0 && !contains(req.RunIDs, rid) {
				continue
			}
			if req.Status != "" && string(rec.run.Status) != req.Status {
				continue
			}
			targets = append(targets, [2]string{tid, rid})
		}
	}
	s.mu.RUnlock()

	for _, target := range targets {
		s.cancelRun(target[0], target[1])
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleRunResource(w http.ResponseWriter, r *http.Request, threadID, runID string) {
	switch r.Method {
	case http.MethodGet:
		s.getRun(w, r, threadID, runID)
	case http.MethodDelete:
		s.cancelRun(threadID, runID)
		w.WriteHeader(http.StatusNoContent)
	default:
		writeMethodNotAllowed(w, http.MethodGet, http.MethodDelete)
	}
}

func (s *Server) handleRunSubresource(w http.ResponseWriter, r *http.Request, threadID, runID, resource string) {
	switch resource {
	case "events":
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w, http.MethodGet)
			return
		}
		s.streamRunEvents(w, r, threadID, runID)
	case "cancel":
		if r.Method != http.MethodPost {
			writeMethodNotAllowed(w, http.MethodPost)
			return
		}
		s.cancelRun(threadID, runID)
		w.WriteHeader(http.StatusNoContent)
	case "join":
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w, http.MethodGet)
			return
		}
		s.joinRun(w, r, threadID, runID)
	case "stream":
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w, http.MethodGet)
			return
		}
		s.streamJoinedRun(w, r, threadID, runID)
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) streamCreateRun(w http.ResponseWriter, r *http.Request, threadID string) {
	if threadID == "" {
		threadID = uuid.New().String()
		now := time.Now().UTC()
		s.mu.Lock()
		s.threads[threadID] = &Thread{ID: threadID, CreatedAt: now, Metadata: map[string]any{"source": "stream_run"}}
		s.mu.Unlock()
	}
	if _, ok := s.getThread(threadID); !ok {
		writeError(w, http.StatusNotFound, fmt.Errorf("thread not found"))
		return
	}

	var req CreateRunRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	now := time.Now().UTC()
	s.mu.Lock()
	run := s.createRunLocked(threadID, req)
	s.mu.Unlock()

	s.publishEvent(threadID, run.ID, RunEvent{Type: "run.created", ThreadID: threadID, RunID: run.ID, Timestamp: now, Payload: map[string]any{"status": string(RunStatusPending)}})
	runCtx := context.WithoutCancel(r.Context())
	go s.executeRun(runCtx, run.ID, threadID)

	modes := normalizeStreamModes(req.StreamMode)
	query := url.Values{}
	query.Set("stream_mode", strings.Join(modes, ","))
	w.Header().Set("Location", fmt.Sprintf("/v1/threads/%s/runs/%s/stream?%s", threadID, run.ID, query.Encode()))

	if err := s.awaitRunCompletionAndStream(r.Context(), w, threadID, run.ID, modes, parseLastEventID(r.Header.Get("Last-Event-ID")), false); err != nil {
		writeError(w, http.StatusInternalServerError, err)
	}
}

func (s *Server) awaitRunCompletionAndStream(ctx context.Context, w http.ResponseWriter, threadID, runID string, modes []string, lastEventID int, cancelOnDisconnect bool) error {
	modes = normalizeStreamModes(modes)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		return fmt.Errorf("streaming unsupported")
	}

	nextID := 1
	writePart := func(event string, payload any) error {
		nextID++
		if nextID <= lastEventID {
			return nil
		}
		if err := writeSSEWithID(w, event, payload, nextID); err != nil {
			return err
		}
		flusher.Flush()
		return nil
	}

	if lastEventID < 1 {
		if err := writeSSEWithID(w, "metadata", map[string]any{"thread_id": threadID, "run_id": runID, "stream_mode": append([]string(nil), modes...)}, 1); err != nil {
			return err
		}
		flusher.Flush()
	}

	rec, ok := s.getRunRecord(threadID, runID)
	if !ok {
		return fmt.Errorf("run not found")
	}
	if rec.done == nil {
		return fmt.Errorf("run completion unavailable")
	}
	replayedEvents := len(rec.events)

	for _, evt := range rec.events {
		parts := s.streamPartsForRunEvent(threadID, runID, modes, evt)
		for _, part := range parts {
			if err := writePart(part.Event, part.Payload); err != nil {
				return err
			}
		}
	}

	s.mu.Lock()
	runsByThread := s.runs[threadID]
	original := runsByThread[runID]
	if original == nil {
		s.mu.Unlock()
		return fmt.Errorf("run not found")
	}
	if original.closed {
		missed := make([]RunEvent, 0)
		if replayedEvents < len(original.events) {
			missed = append(missed, original.events[replayedEvents:]...)
		}
		s.mu.Unlock()
		for _, evt := range missed {
			parts := s.streamPartsForRunEvent(threadID, runID, modes, evt)
			for _, part := range parts {
				if err := writePart(part.Event, part.Payload); err != nil {
					return err
				}
			}
		}
		if err := writePart("end", map[string]any{}); err != nil {
			return err
		}
		return nil
	}
	subID := original.nextID
	original.nextID++
	ch := make(chan RunEvent, 16)
	original.subs[subID] = ch
	s.mu.Unlock()
	defer s.removeSubscriber(threadID, runID, subID)

	for {
		select {
		case <-ctx.Done():
			if cancelOnDisconnect {
				s.cancelRun(threadID, runID)
			}
			return ctx.Err()
		case evt, ok := <-ch:
			if !ok {
				if err := writePart("end", map[string]any{}); err != nil {
					return err
				}
				return nil
			}
			parts := s.streamPartsForRunEvent(threadID, runID, modes, evt)
			for _, part := range parts {
				if err := writePart(part.Event, part.Payload); err != nil {
					return err
				}
			}
		}
	}
}

func (s *Server) joinRun(w http.ResponseWriter, r *http.Request, threadID, runID string) {
	rec, ok := s.getRunRecord(threadID, runID)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Errorf("run not found"))
		return
	}
	if rec.done != nil {
		select {
		case <-r.Context().Done():
			writeError(w, http.StatusRequestTimeout, r.Context().Err())
			return
		case <-rec.done:
		}
	}

	s.mu.RLock()
	values := cloneMap(s.state[threadID])
	s.mu.RUnlock()
	if values == nil {
		values = map[string]any{}
	}
	writeJSON(w, http.StatusOK, values)
}

func (s *Server) streamJoinedRun(w http.ResponseWriter, r *http.Request, threadID, runID string) {
	modes := normalizeStreamModes(strings.Split(r.URL.Query().Get("stream_mode"), ","))
	query := url.Values{}
	query.Set("stream_mode", strings.Join(modes, ","))
	if rawCancel := strings.TrimSpace(r.URL.Query().Get("cancel_on_disconnect")); rawCancel != "" {
		query.Set("cancel_on_disconnect", rawCancel)
	}
	reconnectLocation := "/v1/threads/" + threadID + "/runs/" + runID + "/stream"
	if encoded := query.Encode(); encoded != "" {
		reconnectLocation += "?" + encoded
	}
	w.Header().Set("Location", reconnectLocation)
	cancelOnDisconnect := strings.EqualFold(r.URL.Query().Get("cancel_on_disconnect"), "true")
	if err := s.awaitRunCompletionAndStream(r.Context(), w, threadID, runID, modes, parseLastEventID(r.Header.Get("Last-Event-ID")), cancelOnDisconnect); err != nil {
		writeError(w, http.StatusInternalServerError, err)
	}
}

type streamPart struct {
	Payload any
	Event   string
}

func normalizeStreamModes(raw []string) []string {
	if len(raw) == 0 {
		return []string{"values"}
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(raw))
	for _, mode := range raw {
		mode = strings.ToLower(strings.TrimSpace(mode))
		if mode == "" {
			continue
		}
		if _, ok := seen[mode]; ok {
			continue
		}
		seen[mode] = struct{}{}
		out = append(out, mode)
	}
	if len(out) == 0 {
		return []string{"values"}
	}
	return out
}

func (s *Server) streamPartsForRunEvent(threadID, runID string, modes []string, evt RunEvent) []streamPart {
	rec, ok := s.getRunRecord(threadID, runID)
	if !ok {
		return nil
	}

	s.mu.RLock()
	values := cloneMap(s.state[threadID])
	s.mu.RUnlock()
	if values == nil {
		values = map[string]any{}
	}

	out := make([]streamPart, 0, len(modes))
	for _, mode := range modes {
		part, ok := s.streamPartForMode(mode, threadID, runID, evt, rec, values)
		if !ok {
			continue
		}
		out = append(out, part)
	}
	return out
}

func (s *Server) streamPartForMode(mode, threadID, runID string, evt RunEvent, rec *runRecord, values map[string]any) (streamPart, bool) {
	status, _ := evt.Payload["status"].(string)
	isTerminal := evt.Type == "run.completed" || evt.Type == "run.error" || evt.Type == "run.interrupted" || evt.Type == "run.timeout"

	switch mode {
	case "values":
		if !isTerminal {
			return streamPart{}, false
		}
		if len(values) > 0 {
			return streamPart{Event: "values", Payload: values}, true
		}
		return streamPart{Event: "values", Payload: cloneMap(rec.run.Output)}, true
	case "updates":
		payload := map[string]any{"run_id": runID, "status": status}
		if output, ok := evt.Payload["output"].(map[string]any); ok {
			payload["output"] = cloneMap(output)
		}
		if errMsg, ok := evt.Payload["error"].(string); ok && errMsg != "" {
			payload["error"] = errMsg
		}
		if isTerminal {
			payload["values"] = values
		}
		return streamPart{Event: "updates", Payload: payload}, true
	case "messages":
		if !isTerminal {
			return streamPart{}, false
		}
		messages, ok := rec.run.Output["messages"]
		if !ok {
			return streamPart{}, false
		}
		return streamPart{Event: "messages", Payload: messages}, true
	case "tasks":
		name := rec.run.AssistantID
		if name == "" {
			name = "run"
		}
		payload := map[string]any{"id": runID, "name": name, "event": evt.Type, "status": status}
		if errMsg, ok := evt.Payload["error"].(string); ok && errMsg != "" {
			payload["error"] = errMsg
		}
		return streamPart{Event: "tasks", Payload: payload}, true
	case "checkpoints":
		if !isTerminal {
			return streamPart{}, false
		}
		payload := map[string]any{"thread_id": threadID, "run_id": runID, "values": values}
		if s.checkpointer != nil {
			tuple, err := s.checkpointer.GetTuple(context.Background(), &checkpoint.Config{ThreadID: threadID})
			if err == nil && tuple != nil && tuple.Config != nil {
				payload["checkpoint_id"] = tuple.Config.CheckpointID
			}
		}
		return streamPart{Event: "checkpoints", Payload: payload}, true
	case "debug":
		payload := map[string]any{
			"type":      evt.Type,
			"thread_id": evt.ThreadID,
			"run_id":    evt.RunID,
			"status":    status,
			"timestamp": evt.Timestamp,
			"payload":   cloneMap(evt.Payload),
		}
		return streamPart{Event: "debug", Payload: payload}, true
	case "custom":
		if !isTerminal {
			return streamPart{}, false
		}
		custom, ok := rec.run.Output["custom"]
		if !ok {
			return streamPart{}, false
		}
		return streamPart{Event: "custom", Payload: custom}, true
	case "metadata":
		payload := map[string]any{"thread_id": threadID, "run_id": runID, "event": evt.Type}
		if status != "" {
			payload["status"] = status
		}
		return streamPart{Event: "metadata", Payload: payload}, true
	default:
		payload := map[string]any{"thread_id": threadID, "run_id": runID, "event": evt.Type, "payload": cloneMap(evt.Payload)}
		return streamPart{Event: mode, Payload: payload}, true
	}
}

func (s *Server) cancelRun(threadID, runID string) {
	ended := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	rec := s.runs[threadID][runID]
	if rec == nil || rec.run.Status == RunStatusSuccess || rec.run.Status == RunStatusError || rec.run.Status == RunStatusInterrupted || rec.run.Status == RunStatusTimeout {
		return
	}
	rec.run.Status = RunStatusInterrupted
	rec.run.Error = "run canceled"
	rec.run.EndedAt = &ended
	s.publishEventLocked(rec, RunEvent{Type: "run.interrupted", ThreadID: threadID, RunID: runID, Timestamp: ended, Payload: map[string]any{"error": "run canceled", "status": string(RunStatusInterrupted)}})
	s.closeRecordLocked(rec)
}

func (s *Server) handleGlobalStoreItems(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/v1/store/items" {
		http.NotFound(w, r)
		return
	}
	switch r.Method {
	case http.MethodPut:
		var req struct {
			Index     any            `json:"index,omitempty"`
			Value     map[string]any `json:"value"`
			TTL       *int           `json:"ttl,omitempty"`
			Key       string         `json:"key"`
			Namespace []string       `json:"namespace"`
		}
		if err := decodeJSON(r, &req); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		if len(req.Namespace) == 0 || req.Key == "" {
			writeError(w, http.StatusBadRequest, fmt.Errorf("namespace and key are required"))
			return
		}
		var ttl *time.Duration
		if req.TTL != nil && *req.TTL > 0 {
			d := time.Duration(*req.TTL) * time.Second
			ttl = &d
		}
		if err := s.globalStore.PutItem(r.Context(), req.Namespace, req.Key, cloneMap(req.Value), graphpkg.StorePutOptions{TTL: ttl, Index: req.Index}); err != nil {
			var invalidUpdate *graphpkg.InvalidUpdateError
			if errors.As(err, &invalidUpdate) {
				writeError(w, http.StatusBadRequest, err)
				return
			}
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case http.MethodGet:
		namespace := r.URL.Query().Get("namespace")
		key := r.URL.Query().Get("key")
		if namespace == "" || key == "" {
			writeError(w, http.StatusBadRequest, fmt.Errorf("namespace and key are required"))
			return
		}
		refreshTTL, err := parseOptionalBoolQueryValue(r.URL.Query().Get("refresh_ttl"))
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		item, err := s.globalStore.GetItem(r.Context(), strings.Split(namespace, "."), key, graphpkg.StoreGetOptions{RefreshTTL: refreshTTL})
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		if item == nil {
			writeError(w, http.StatusNotFound, fmt.Errorf("store item not found"))
			return
		}
		response := serverStoreItemFromGraph(item)
		writeJSON(w, http.StatusOK, cloneStoreItem(&response))
	case http.MethodDelete:
		var req struct {
			Key       string   `json:"key"`
			Namespace []string `json:"namespace"`
		}
		if err := decodeJSON(r, &req); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		if len(req.Namespace) == 0 || req.Key == "" {
			writeError(w, http.StatusBadRequest, fmt.Errorf("namespace and key are required"))
			return
		}
		if err := s.globalStore.DeleteItem(r.Context(), req.Namespace, req.Key); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		writeMethodNotAllowed(w, http.MethodPut, http.MethodGet, http.MethodDelete)
	}
}

func (s *Server) handleGlobalStoreSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, http.MethodPost)
		return
	}
	var req struct {
		Filter          map[string]any `json:"filter,omitempty"`
		RefreshTTL      *bool          `json:"refresh_ttl,omitempty"`
		Query           string         `json:"query,omitempty"`
		NamespacePrefix []string       `json:"namespace_prefix"`
		Limit           int            `json:"limit,omitempty"`
		Offset          int            `json:"offset,omitempty"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	items, err := s.globalStore.SearchItems(r.Context(), graphpkg.StoreSearchRequest{
		NamespacePrefix: req.NamespacePrefix,
		Limit:           req.Limit,
		Offset:          req.Offset,
		Filter:          req.Filter,
		Query:           req.Query,
		RefreshTTL:      req.RefreshTTL,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	out := make([]StoreItem, 0, len(items))
	for _, item := range items {
		copyItem := item
		response := serverStoreSearchItemFromGraph(&copyItem)
		out = append(out, *cloneStoreItem(&response))
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": out})
}

func (s *Server) handleGlobalStoreNamespaces(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, http.MethodPost)
		return
	}
	var req struct {
		MaxDepth *int     `json:"max_depth,omitempty"`
		Prefix   []string `json:"prefix,omitempty"`
		Suffix   []string `json:"suffix,omitempty"`
		Limit    int      `json:"limit,omitempty"`
		Offset   int      `json:"offset,omitempty"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	matchConditions := make([]graphpkg.NamespaceMatchCondition, 0, 2)
	if len(req.Prefix) > 0 {
		matchConditions = append(matchConditions, graphpkg.NamespaceMatchCondition{MatchType: graphpkg.NamespaceMatchPrefix, Path: append([]string(nil), req.Prefix...)})
	}
	if len(req.Suffix) > 0 {
		matchConditions = append(matchConditions, graphpkg.NamespaceMatchCondition{MatchType: graphpkg.NamespaceMatchSuffix, Path: append([]string(nil), req.Suffix...)})
	}
	namespaces, err := s.globalStore.ListNamespaces(r.Context(), graphpkg.StoreNamespaceListRequest{
		MatchConditions: matchConditions,
		MaxDepth:        req.MaxDepth,
		Limit:           req.Limit,
		Offset:          req.Offset,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"namespaces": namespaces})
}

func (s *Server) handleGlobalCrons(w http.ResponseWriter, r *http.Request, suffix []string) {
	if len(suffix) == 0 {
		if r.Method != http.MethodPost {
			writeMethodNotAllowed(w, http.MethodPost)
			return
		}
		s.createCron(w, r, "")
		return
	}

	if suffix[0] == "search" {
		s.searchCrons(w, r)
		return
	}
	if suffix[0] == "count" {
		s.countCrons(w, r)
		return
	}

	cronID := suffix[0]
	switch r.Method {
	case http.MethodPatch:
		s.updateCron(w, r, cronID)
	case http.MethodDelete:
		s.deleteCron(w, r, cronID)
	default:
		writeMethodNotAllowed(w, http.MethodPatch, http.MethodDelete)
	}
}

func (s *Server) handleThreadCrons(w http.ResponseWriter, r *http.Request, threadID string, suffix []string) {
	if len(suffix) != 0 {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, http.MethodPost)
		return
	}
	if _, ok := s.getThread(threadID); !ok {
		writeError(w, http.StatusNotFound, fmt.Errorf("thread not found"))
		return
	}
	s.createCron(w, r, threadID)
}

func (s *Server) createCron(w http.ResponseWriter, r *http.Request, threadID string) {
	var req struct {
		Input       map[string]any `json:"input,omitempty"`
		Enabled     *bool          `json:"enabled,omitempty"`
		EndTime     *time.Time     `json:"end_time,omitempty"`
		AssistantID string         `json:"assistant_id,omitempty"`
		Schedule    string         `json:"schedule,omitempty"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	now := time.Now().UTC()
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	cron := &Cron{
		ID:          uuid.New().String(),
		AssistantID: req.AssistantID,
		ThreadID:    threadID,
		Schedule:    req.Schedule,
		Payload:     cloneMap(req.Input),
		Enabled:     enabled,
		EndTime:     req.EndTime,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	s.mu.Lock()
	s.crons[cron.ID] = cron
	s.mu.Unlock()
	writeJSON(w, http.StatusCreated, cron)
}

func (s *Server) updateCron(w http.ResponseWriter, r *http.Request, cronID string) {
	var req struct {
		Input       map[string]any `json:"input,omitempty"`
		Enabled     *bool          `json:"enabled,omitempty"`
		EndTime     *time.Time     `json:"end_time,omitempty"`
		AssistantID string         `json:"assistant_id,omitempty"`
		Schedule    string         `json:"schedule,omitempty"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	now := time.Now().UTC()

	s.mu.Lock()
	cron := s.crons[cronID]
	if cron == nil {
		s.mu.Unlock()
		writeError(w, http.StatusNotFound, fmt.Errorf("cron not found"))
		return
	}
	if req.AssistantID != "" {
		cron.AssistantID = req.AssistantID
	}
	if req.Schedule != "" {
		cron.Schedule = req.Schedule
	}
	if req.Input != nil {
		cron.Payload = cloneMap(req.Input)
	}
	if req.Enabled != nil {
		cron.Enabled = *req.Enabled
	}
	if req.EndTime != nil {
		cron.EndTime = req.EndTime
	}
	cron.UpdatedAt = now
	out := cloneCron(cron)
	s.mu.Unlock()

	writeJSON(w, http.StatusOK, out)
}

func (s *Server) deleteCron(w http.ResponseWriter, _ *http.Request, cronID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.crons[cronID]; !ok {
		writeError(w, http.StatusNotFound, fmt.Errorf("cron not found"))
		return
	}
	delete(s.crons, cronID)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) searchCrons(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, http.MethodPost)
		return
	}
	var req struct {
		Enabled     *bool  `json:"enabled,omitempty"`
		AssistantID string `json:"assistant_id,omitempty"`
		ThreadID    string `json:"thread_id,omitempty"`
		Limit       int    `json:"limit,omitempty"`
		Offset      int    `json:"offset,omitempty"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	crons := s.filteredCrons(req.AssistantID, req.ThreadID, req.Enabled)
	if req.Offset > 0 {
		if req.Offset >= len(crons) {
			crons = []Cron{}
		} else {
			crons = crons[req.Offset:]
		}
	}
	if req.Limit > 0 && req.Limit < len(crons) {
		crons = crons[:req.Limit]
	}
	writeJSON(w, http.StatusOK, crons)
}

func (s *Server) countCrons(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, http.MethodPost)
		return
	}
	var req struct {
		AssistantID string `json:"assistant_id,omitempty"`
		ThreadID    string `json:"thread_id,omitempty"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"count": len(s.filteredCrons(req.AssistantID, req.ThreadID, nil))})
}

func (s *Server) getThread(threadID string) (*Thread, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.threads[threadID]
	if !ok {
		return nil, false
	}
	out := *t
	out.Metadata = cloneMap(t.Metadata)
	return &out, true
}

func (s *Server) getRunRecord(threadID, runID string) (*runRecord, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec := s.runs[threadID][runID]
	if rec == nil {
		return nil, false
	}
	runCopy := *rec.run
	runCopy.Input = cloneMap(rec.run.Input)
	runCopy.Output = cloneMap(rec.run.Output)
	runCopy.Metadata = cloneMap(rec.run.Metadata)
	events := append([]RunEvent(nil), rec.events...)
	return &runRecord{run: &runCopy, events: events, closed: rec.closed, done: rec.done}, true
}

func (s *Server) publishEvent(threadID, runID string, evt RunEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec := s.runs[threadID][runID]
	if rec == nil {
		return
	}
	s.publishEventLocked(rec, evt)
}

func (s *Server) publishEventLocked(rec *runRecord, evt RunEvent) {
	rec.events = append(rec.events, evt)
	for _, ch := range rec.subs {
		select {
		case ch <- evt:
		default:
		}
	}
}

func (s *Server) closeRecordLocked(rec *runRecord) {
	if rec.closed {
		return
	}
	rec.closed = true
	if rec.done != nil {
		close(rec.done)
	}
	for id, ch := range rec.subs {
		close(ch)
		delete(rec.subs, id)
	}
}

func (s *Server) removeSubscriber(threadID, runID string, subID int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec := s.runs[threadID][runID]
	if rec == nil {
		return
	}
	ch := rec.subs[subID]
	if ch == nil {
		return
	}
	delete(rec.subs, subID)
	close(ch)
}

func (s *Server) getAssistant(assistantID string) (*Assistant, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec := s.assistants[assistantID]
	if rec == nil {
		return nil, false
	}
	return cloneAssistant(rec.assistant), true
}

func cloneAssistant(in *Assistant) *Assistant {
	if in == nil {
		return nil
	}
	out := *in
	out.Config = cloneMap(in.Config)
	out.Context = cloneMap(in.Context)
	out.Metadata = cloneMap(in.Metadata)
	return &out
}

func (s *Server) filteredAssistants(metadata map[string]any, graphID, name string) []Assistant {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Assistant, 0, len(s.assistants))
	needle := strings.ToLower(strings.TrimSpace(name))
	for _, rec := range s.assistants {
		a := rec.assistant
		if graphID != "" && a.GraphID != graphID {
			continue
		}
		if needle != "" && !strings.Contains(strings.ToLower(a.Name), needle) {
			continue
		}
		if !matchesMetadata(a.Metadata, metadata) {
			continue
		}
		out = append(out, *cloneAssistant(a))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out
}

func applyOffsetLimitAssistants(in []Assistant, offset, limit int) []Assistant {
	out := in
	if offset > 0 {
		if offset >= len(out) {
			return []Assistant{}
		}
		out = out[offset:]
	}
	if limit > 0 && limit < len(out) {
		out = out[:limit]
	}
	return out
}

func assistantPaginationNext(total, offset, pageSize, limit int) (string, bool) {
	if limit <= 0 {
		return "", false
	}
	next := offset + pageSize
	if next >= total {
		return "", false
	}
	return strconv.Itoa(next), true
}

func matchesMetadata(actual, want map[string]any) bool {
	if len(want) == 0 {
		return true
	}
	for key, expected := range want {
		if actual == nil || actual[key] != expected {
			return false
		}
	}
	return true
}

func parseOptionalBoolQuery(r *http.Request, key string) (bool, error) {
	raw := strings.TrimSpace(r.URL.Query().Get(key))
	if raw == "" {
		return false, nil
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("invalid %s %q", key, raw)
	}
	return value, nil
}

func parseOptionalBoolQueryValue(raw string) (*bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid boolean %q", raw)
	}
	return &value, nil
}

func (s *Server) deleteAssistantThreadsLocked(assistantID string) []string {
	deleted := make([]string, 0)
	for threadID, thread := range s.threads {
		if thread == nil || thread.Metadata == nil {
			continue
		}
		if got, _ := thread.Metadata["assistant_id"].(string); got != assistantID {
			continue
		}
		delete(s.threads, threadID)
		delete(s.runs, threadID)
		delete(s.state, threadID)
		delete(s.store, threadID)
		deleted = append(deleted, threadID)
	}
	return deleted
}

func cloneStoreItem(in *StoreItem) *StoreItem {
	if in == nil {
		return nil
	}
	out := *in
	out.Namespace = append([]string(nil), in.Namespace...)
	out.Value = cloneMap(in.Value)
	if in.CreatedAt != nil {
		created := *in.CreatedAt
		out.CreatedAt = &created
	}
	if in.UpdatedAt != nil {
		updated := *in.UpdatedAt
		out.UpdatedAt = &updated
	}
	return &out
}

func serverStoreItemFromGraph(in *graphpkg.StoreItem) StoreItem {
	out := StoreItem{
		Namespace: append([]string(nil), in.Namespace...),
		Key:       in.Key,
		Value:     cloneMap(asStoreValueMap(in.Value)),
	}
	created := in.CreatedAt.UTC()
	updated := in.UpdatedAt.UTC()
	out.CreatedAt = &created
	out.UpdatedAt = &updated
	return out
}

func serverStoreSearchItemFromGraph(in *graphpkg.StoreSearchItem) StoreItem {
	out := serverStoreItemFromGraph(&in.StoreItem)
	if in.Score != nil {
		score := *in.Score
		out.Score = &score
	}
	return out
}

func asStoreValueMap(value any) map[string]any {
	asMap, ok := value.(map[string]any)
	if ok {
		return asMap
	}
	if value == nil {
		return nil
	}
	return map[string]any{"value": value}
}

func cloneCron(in *Cron) *Cron {
	if in == nil {
		return nil
	}
	out := *in
	out.Payload = cloneMap(in.Payload)
	if in.NextRunDate != nil {
		next := *in.NextRunDate
		out.NextRunDate = &next
	}
	if in.EndTime != nil {
		end := *in.EndTime
		out.EndTime = &end
	}
	return &out
}

func (s *Server) filteredCrons(assistantID, threadID string, enabled *bool) []Cron {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Cron, 0, len(s.crons))
	for _, cron := range s.crons {
		if assistantID != "" && cron.AssistantID != assistantID {
			continue
		}
		if threadID != "" && cron.ThreadID != threadID {
			continue
		}
		if enabled != nil && cron.Enabled != *enabled {
			continue
		}
		out = append(out, *cloneCron(cron))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out
}

func (s *Server) threadStatusLocked(threadID string) string {
	runs := s.runs[threadID]
	if len(runs) == 0 {
		return "idle"
	}
	latestStatus := "idle"
	var latestTime time.Time
	for _, rec := range runs {
		if rec == nil || rec.run == nil {
			continue
		}
		if rec.run.Status == RunStatusPending || rec.run.Status == RunStatusRunning {
			return "busy"
		}
		if rec.run.CreatedAt.After(latestTime) {
			latestTime = rec.run.CreatedAt
			switch rec.run.Status {
			case RunStatusInterrupted:
				latestStatus = "interrupted"
			case RunStatusError, RunStatusTimeout:
				latestStatus = "error"
			default:
				latestStatus = "idle"
			}
		}
	}
	return latestStatus
}

func (s *Server) collectThreadEvents(threadID string) []RunEvent {
	s.mu.RLock()
	runs := s.runs[threadID]
	events := make([]RunEvent, 0)
	for _, rec := range runs {
		events = append(events, rec.events...)
	}
	s.mu.RUnlock()
	sort.Slice(events, func(i, j int) bool { return events[i].Timestamp.Before(events[j].Timestamp) })
	return events
}

func contains(items []string, value string) bool {
	for _, item := range items {
		if item == value {
			return true
		}
	}
	return false
}

var (
	errAssistantGraphUnavailable = errors.New("assistant graph introspection is unavailable")
	errAssistantGraphNotFound    = errors.New("assistant graph not found")
)

func (s *Server) resolveAssistantGraph(ctx context.Context, assistant *Assistant) (graphpkg.GraphInfo, error) {
	if assistant == nil {
		return graphpkg.GraphInfo{}, fmt.Errorf("assistant not found")
	}
	if strings.TrimSpace(assistant.GraphID) == "" {
		return graphpkg.GraphInfo{}, fmt.Errorf("assistant %q has no graph_id", assistant.ID)
	}
	if s.introspector == nil {
		return graphpkg.GraphInfo{}, errAssistantGraphUnavailable
	}
	info, err := s.introspector.GraphInfo(ctx, assistant.GraphID)
	if err != nil {
		return graphpkg.GraphInfo{}, err
	}
	if len(info.Nodes) == 0 && len(info.Edges) == 0 {
		return graphpkg.GraphInfo{}, errAssistantGraphNotFound
	}
	return info, nil
}

func (s *Server) writeAssistantGraphError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errAssistantGraphUnavailable):
		writeError(w, http.StatusFailedDependency, err)
	case errors.Is(err, errAssistantGraphNotFound):
		writeError(w, http.StatusNotFound, err)
	default:
		writeError(w, http.StatusBadRequest, err)
	}
}

func filterSubgraphs(in []graphpkg.GraphSubgraphInfo, namespace string, recurse bool) []graphpkg.GraphSubgraphInfo {
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return append([]graphpkg.GraphSubgraphInfo(nil), in...)
	}
	out := make([]graphpkg.GraphSubgraphInfo, 0, len(in))
	for _, subgraph := range in {
		if subgraph.Namespace == namespace {
			out = append(out, subgraph)
			continue
		}
		if recurse && strings.HasPrefix(subgraph.Namespace, namespace+"/") {
			out = append(out, subgraph)
		}
	}
	return out
}

func writeSSEMode(w http.ResponseWriter, event string, payload any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, b)
	return err
}

func writeSSEWithID(w http.ResponseWriter, event string, payload any, id int) error {
	if evt, ok := payload.(RunEvent); ok {
		if evt.Type == "" {
			evt.Type = event
		}
		if _, err := w.Write([]byte("id: " + strconv.Itoa(id) + "\n")); err != nil {
			return err
		}
		return writeSSE(w, evt)
	}
	if _, err := w.Write([]byte("id: " + strconv.Itoa(id) + "\n")); err != nil {
		return err
	}
	return writeSSEMode(w, event, payload)
}

func parseLastEventID(raw string) int {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		return 0
	}
	return value
}

type passthroughRunner struct{}

func (passthroughRunner) Run(_ context.Context, req RunRequest) (RunResult, error) {
	out := cloneMap(req.Input)
	if out == nil {
		out = map[string]any{}
	}
	return RunResult{Output: out, State: out}, nil
}

func splitPath(in string) []string {
	parts := strings.Split(in, "/")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func cloneMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func mergeMaps(base, updates map[string]any) map[string]any {
	out := cloneMap(base)
	if out == nil {
		out = map[string]any{}
	}
	for k, v := range updates {
		out[k] = v
	}
	return out
}

func checkpointIDFromMap(raw map[string]any) string {
	if raw == nil {
		return ""
	}
	id, _ := raw["checkpoint_id"].(string)
	return strings.TrimSpace(id)
}

func checkpointConfigFromMap(raw map[string]any) *checkpoint.Config {
	if raw == nil {
		return nil
	}
	id := checkpointIDFromMap(raw)
	ns, _ := raw["checkpoint_ns"].(string)
	threadID, _ := raw["thread_id"].(string)
	if id == "" && ns == "" && threadID == "" {
		return nil
	}
	return &checkpoint.Config{ThreadID: strings.TrimSpace(threadID), CheckpointNS: strings.TrimSpace(ns), CheckpointID: id}
}

func checkpointConfigMap(cfg *checkpoint.Config) map[string]any {
	if cfg == nil {
		return nil
	}
	out := map[string]any{}
	if cfg.ThreadID != "" {
		out["thread_id"] = cfg.ThreadID
	}
	if cfg.CheckpointNS != "" {
		out["checkpoint_ns"] = cfg.CheckpointNS
	}
	if cfg.CheckpointID != "" {
		out["checkpoint_id"] = cfg.CheckpointID
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func parseCheckpointTime(tuple *checkpoint.CheckpointTuple) *time.Time {
	if tuple == nil || tuple.Checkpoint == nil {
		return nil
	}
	ts, err := time.Parse(time.RFC3339Nano, tuple.Checkpoint.TS)
	if err != nil {
		return nil
	}
	parsed := ts.UTC()
	return &parsed
}

func rejectUnsupportedStorePut(index any, ttl *int) error {
	if index != nil {
		return fmt.Errorf("store index configuration is not implemented")
	}
	if ttl != nil {
		return fmt.Errorf("store ttl is not implemented")
	}
	return nil
}

func rejectUnsupportedStoreSearch(filter map[string]any, query string, refreshTTL *bool) error {
	if len(filter) > 0 {
		return fmt.Errorf("store metadata filtering is not implemented")
	}
	if strings.TrimSpace(query) != "" {
		return fmt.Errorf("store query search is not implemented")
	}
	if refreshTTL != nil {
		return fmt.Errorf("store refresh_ttl is not implemented")
	}
	return nil
}

func rejectUnsupportedStoreNamespaces(maxDepth *int) error {
	if maxDepth != nil {
		return fmt.Errorf("store max_depth is not implemented")
	}
	return nil
}

func decodeJSON(r *http.Request, out any) error {
	if r.Body == nil {
		return nil
	}
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(out); err != nil {
		if err.Error() == "EOF" {
			return nil
		}
		return err
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]any{"error": err.Error()})
}

func writeMethodNotAllowed(w http.ResponseWriter, allowed ...string) {
	w.Header().Set("Allow", strings.Join(allowed, ", "))
	writeError(w, http.StatusMethodNotAllowed, fmt.Errorf("method not allowed"))
}

func writeSSE(w http.ResponseWriter, evt RunEvent) error {
	b, err := json.Marshal(evt)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", evt.Type, b)
	return err
}
