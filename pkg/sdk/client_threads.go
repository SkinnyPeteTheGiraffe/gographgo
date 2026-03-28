package sdk

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/checkpoint"
)

// Create creates a thread.
func (tc *ThreadsClient) Create(ctx context.Context, req ThreadCreateRequest) (*Thread, error) {
	var out Thread
	if err := tc.client.doJSON(ctx, http.MethodPost, "/v1/threads", req, &out, http.StatusCreated, http.StatusOK); err != nil {
		return nil, err
	}
	return &out, nil
}

// Get gets a thread by ID.
func (tc *ThreadsClient) Get(ctx context.Context, threadID string) (*Thread, error) {
	var out Thread
	if err := tc.client.doJSON(ctx, http.MethodGet, "/v1/threads/"+threadID, nil, &out, http.StatusOK); err != nil {
		return nil, err
	}
	return &out, nil
}

// Update updates thread metadata/options.
func (tc *ThreadsClient) Update(ctx context.Context, threadID string, req ThreadUpdateRequest) (*Thread, error) {
	var out Thread
	if err := tc.client.doJSON(ctx, http.MethodPatch, "/v1/threads/"+threadID, req, &out, http.StatusOK); err != nil {
		return nil, err
	}
	return &out, nil
}

// Delete deletes a thread.
func (tc *ThreadsClient) Delete(ctx context.Context, threadID string) error {
	return tc.client.doNoContent(ctx, http.MethodDelete, "/v1/threads/"+threadID, nil, http.StatusOK, http.StatusNoContent)
}

// Search finds threads by filter criteria.
func (tc *ThreadsClient) Search(ctx context.Context, req ThreadSearchRequest) ([]Thread, error) {
	var out []Thread
	if err := tc.client.doJSON(ctx, http.MethodPost, "/v1/threads/search", req, &out, http.StatusOK); err != nil {
		return nil, err
	}
	return out, nil
}

// Count returns thread count for matching filters.
func (tc *ThreadsClient) Count(ctx context.Context, req ThreadCountRequest) (int, error) {
	var raw json.RawMessage
	if err := tc.client.doJSON(ctx, http.MethodPost, "/v1/threads/count", req, &raw, http.StatusOK); err != nil {
		return 0, err
	}
	return decodeCount(raw)
}

// Copy copies a thread.
func (tc *ThreadsClient) Copy(ctx context.Context, threadID string) (map[string]any, error) {
	var out map[string]any
	if err := tc.client.doJSON(ctx, http.MethodPost, "/v1/threads/"+threadID+"/copy", nil, &out, http.StatusOK, http.StatusCreated); err != nil {
		return nil, err
	}
	return out, nil
}

// Prune prunes thread history according to the configured strategy.
func (tc *ThreadsClient) Prune(ctx context.Context, req ThreadPruneRequest) (*ThreadPruneResponse, error) {
	var out ThreadPruneResponse
	if err := tc.client.doJSON(ctx, http.MethodPost, "/v1/threads/prune", req, &out, http.StatusOK); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetState returns the latest state for a thread.
func (tc *ThreadsClient) GetState(ctx context.Context, threadID string) (*ThreadState, error) {
	var out ThreadState
	if err := tc.client.doJSON(ctx, http.MethodGet, "/v1/threads/"+threadID+"/state", nil, &out, http.StatusOK); err != nil {
		return nil, err
	}
	return &out, nil
}

// UpdateState applies out-of-band state updates for a thread.
func (tc *ThreadsClient) UpdateState(ctx context.Context, threadID string, req ThreadUpdateStateRequest) (*ThreadUpdateStateResponse, error) {
	var out ThreadUpdateStateResponse
	if err := tc.client.doJSON(ctx, http.MethodPost, "/v1/threads/"+threadID+"/state", req, &out, http.StatusOK); err != nil {
		return nil, err
	}
	return &out, nil
}

// ListHistory returns checkpoint history for a thread (GET endpoint).
func (tc *ThreadsClient) ListHistory(ctx context.Context, threadID string) ([]checkpoint.CheckpointTuple, error) {
	var out []checkpoint.CheckpointTuple
	if err := tc.client.doJSON(ctx, http.MethodGet, "/v1/threads/"+threadID+"/history", nil, &out, http.StatusOK); err != nil {
		return nil, err
	}
	return out, nil
}

// GetHistory returns thread state history with filters (POST endpoint).
func (tc *ThreadsClient) GetHistory(ctx context.Context, threadID string, req ThreadHistoryRequest) ([]ThreadState, error) {
	var out []ThreadState
	if err := tc.client.doJSON(ctx, http.MethodPost, "/v1/threads/"+threadID+"/history", req, &out, http.StatusOK); err != nil {
		return nil, err
	}
	return out, nil
}

// JoinStream streams thread-level events.
func (tc *ThreadsClient) JoinStream(ctx context.Context, threadID string, req ThreadJoinStreamRequest) (parts <-chan StreamPart, errs <-chan error) {
	p, err := withQuery(
		"/v1/threads/"+threadID+"/stream",
		map[string]any{"stream_mode": req.StreamMode},
	)
	if err != nil {
		errs := make(chan error, 1)
		closeErr(errs, err)
		return make(chan StreamPart), errs
	}
	headers := map[string]string{}
	if req.LastEventID != "" {
		headers["Last-Event-ID"] = req.LastEventID
	}
	return tc.client.streamSSE(ctx, http.MethodGet, p, nil, headers)
}

// JoinStreamTyped streams thread-level events as typed stream-mode parts.
func (tc *ThreadsClient) JoinStreamTyped(
	ctx context.Context,
	threadID string,
	req ThreadJoinStreamRequest,
) (parts <-chan StreamPartV2, errs <-chan error) {
	p, err := withQuery(
		"/v1/threads/"+threadID+"/stream",
		map[string]any{"stream_mode": req.StreamMode},
	)
	if err != nil {
		errs := make(chan error, 1)
		closeErr(errs, err)
		return make(chan StreamPartV2), errs
	}
	headers := map[string]string{}
	if req.LastEventID != "" {
		headers["Last-Event-ID"] = req.LastEventID
	}
	return tc.client.streamSSEV2(ctx, http.MethodGet, p, nil, headers)
}
