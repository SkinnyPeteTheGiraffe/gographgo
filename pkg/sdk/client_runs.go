package sdk

import (
	"context"
	"encoding/json"
	"net/http"
)

// Create creates a run.
func (rc *RunsClient) Create(ctx context.Context, threadID string, req RunCreateRequest) (*Run, error) {
	var out Run
	p := "/v1/runs"
	if threadID != "" {
		p = "/v1/threads/" + threadID + "/runs"
	}
	if err := rc.client.doJSON(ctx, http.MethodPost, p, req, &out, http.StatusAccepted, http.StatusCreated, http.StatusOK); err != nil {
		return nil, err
	}
	return &out, nil
}

// Get gets a run by ID.
func (rc *RunsClient) Get(ctx context.Context, threadID, runID string) (*Run, error) {
	var out Run
	if err := rc.client.doJSON(ctx, http.MethodGet, "/v1/threads/"+threadID+"/runs/"+runID, nil, &out, http.StatusOK); err != nil {
		return nil, err
	}
	return &out, nil
}

// Events streams persisted run events.
func (rc *RunsClient) Events(ctx context.Context, threadID, runID string) (<-chan RunEvent, <-chan error) {
	parts, errs := rc.client.streamSSE(ctx, http.MethodGet, "/v1/threads/"+threadID+"/runs/"+runID+"/events", nil, nil)
	out := make(chan RunEvent, 32)
	outErrs := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(outErrs)
		for part := range parts {
			var evt RunEvent
			if err := json.Unmarshal(part.Data, &evt); err != nil {
				outErrs <- err
				return
			}
			if evt.Type == "" {
				evt.Type = part.Event
			}
			select {
			case <-ctx.Done():
				return
			case out <- evt:
			}
		}
		for err := range errs {
			if err != nil {
				outErrs <- err
				return
			}
		}
	}()

	return out, outErrs
}

// Stream creates a run and streams stream-mode parts.
func (rc *RunsClient) Stream(ctx context.Context, threadID string, req RunStreamRequest) (<-chan StreamPart, <-chan error) {
	p := "/v1/runs/stream"
	if threadID != "" {
		p = "/v1/threads/" + threadID + "/runs/stream"
	}
	return rc.client.streamSSE(ctx, http.MethodPost, p, req, nil)
}

// StreamTyped creates a run and streams typed stream-mode parts.
func (rc *RunsClient) StreamTyped(
	ctx context.Context,
	threadID string,
	req RunStreamRequest,
) (<-chan StreamPartV2, <-chan error) {
	p := "/v1/runs/stream"
	if threadID != "" {
		p = "/v1/threads/" + threadID + "/runs/stream"
	}
	return rc.client.streamSSEV2(ctx, http.MethodPost, p, req, nil)
}

// CreateBatch creates a batch of runs.
func (rc *RunsClient) CreateBatch(ctx context.Context, req []RunCreateRequest) ([]Run, error) {
	var out []Run
	if err := rc.client.doJSON(ctx, http.MethodPost, "/v1/runs/batch", req, &out, http.StatusOK, http.StatusCreated); err != nil {
		return nil, err
	}
	return out, nil
}

// Cancel requests cancellation for a run.
func (rc *RunsClient) Cancel(ctx context.Context, threadID, runID string, opts RunCancelOptions) error {
	p, err := withQuery(
		"/v1/threads/"+threadID+"/runs/"+runID+"/cancel",
		map[string]any{
			"wait":   opts.Wait,
			"action": opts.Action,
		},
	)
	if err != nil {
		return err
	}
	return rc.client.doNoContent(ctx, http.MethodPost, p, nil, http.StatusOK, http.StatusAccepted, http.StatusNoContent)
}

// CancelMany requests cancellation for multiple runs.
func (rc *RunsClient) CancelMany(ctx context.Context, req RunCancelManyRequest) error {
	p, err := withQuery("/v1/runs/cancel", map[string]any{"action": req.Action})
	if err != nil {
		return err
	}
	body := map[string]any{}
	if req.ThreadID != "" {
		body["thread_id"] = req.ThreadID
	}
	if len(req.RunIDs) > 0 {
		body["run_ids"] = req.RunIDs
	}
	if req.Status != "" {
		body["status"] = req.Status
	}
	return rc.client.doNoContent(ctx, http.MethodPost, p, body, http.StatusOK, http.StatusAccepted, http.StatusNoContent)
}

// Join blocks until the run completes and returns the final thread state.
func (rc *RunsClient) Join(ctx context.Context, threadID, runID string) (map[string]any, error) {
	var out map[string]any
	if err := rc.client.doJSON(ctx, http.MethodGet, "/v1/threads/"+threadID+"/runs/"+runID+"/join", nil, &out, http.StatusOK); err != nil {
		return nil, err
	}
	return out, nil
}

// JoinStream streams output from a run until completion.
func (rc *RunsClient) JoinStream(ctx context.Context, threadID, runID string, req RunJoinStreamRequest) (<-chan StreamPart, <-chan error) {
	p, err := withQuery(
		"/v1/threads/"+threadID+"/runs/"+runID+"/stream",
		map[string]any{"cancel_on_disconnect": req.CancelOnDisconnect, "stream_mode": req.StreamMode},
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
	return rc.client.streamSSE(ctx, http.MethodGet, p, nil, headers)
}

// JoinStreamTyped streams output from a run as typed stream-mode parts.
func (rc *RunsClient) JoinStreamTyped(
	ctx context.Context,
	threadID string,
	runID string,
	req RunJoinStreamRequest,
) (<-chan StreamPartV2, <-chan error) {
	p, err := withQuery(
		"/v1/threads/"+threadID+"/runs/"+runID+"/stream",
		map[string]any{"cancel_on_disconnect": req.CancelOnDisconnect, "stream_mode": req.StreamMode},
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
	return rc.client.streamSSEV2(ctx, http.MethodGet, p, nil, headers)
}

// Delete deletes a run.
func (rc *RunsClient) Delete(ctx context.Context, threadID, runID string) error {
	return rc.client.doNoContent(ctx, http.MethodDelete, "/v1/threads/"+threadID+"/runs/"+runID, nil, http.StatusOK, http.StatusNoContent)
}
