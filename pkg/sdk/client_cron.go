package sdk

import (
	"context"
	"encoding/json"
	"net/http"
)

// CreateForThread creates a thread-scoped cron schedule.
func (cc *CronClient) CreateForThread(ctx context.Context, threadID string, req CronCreateRequest) (*Cron, error) {
	var out Cron
	if err := cc.client.doJSON(ctx, http.MethodPost, "/v1/threads/"+threadID+"/runs/crons", req, &out, http.StatusCreated, http.StatusOK); err != nil {
		return nil, err
	}
	return &out, nil
}

// Create creates a cron schedule.
func (cc *CronClient) Create(ctx context.Context, req CronCreateRequest) (*Cron, error) {
	var out Cron
	if err := cc.client.doJSON(ctx, http.MethodPost, "/v1/runs/crons", req, &out, http.StatusCreated, http.StatusOK); err != nil {
		return nil, err
	}
	return &out, nil
}

// Delete deletes a cron schedule.
func (cc *CronClient) Delete(ctx context.Context, cronID string) error {
	return cc.client.doNoContent(ctx, http.MethodDelete, "/v1/runs/crons/"+cronID, nil, http.StatusOK, http.StatusNoContent)
}

// Update updates a cron schedule.
func (cc *CronClient) Update(ctx context.Context, cronID string, req CronUpdateRequest) (*Cron, error) {
	var out Cron
	if err := cc.client.doJSON(ctx, http.MethodPatch, "/v1/runs/crons/"+cronID, req, &out, http.StatusOK); err != nil {
		return nil, err
	}
	return &out, nil
}

// Search searches cron schedules.
func (cc *CronClient) Search(ctx context.Context, req CronSearchRequest) ([]Cron, error) {
	var out []Cron
	if err := cc.client.doJSON(ctx, http.MethodPost, "/v1/runs/crons/search", req, &out, http.StatusOK); err != nil {
		return nil, err
	}
	return out, nil
}

// Count returns cron count for matching filters.
func (cc *CronClient) Count(ctx context.Context, req CronCountRequest) (int, error) {
	var raw json.RawMessage
	if err := cc.client.doJSON(ctx, http.MethodPost, "/v1/runs/crons/count", req, &raw, http.StatusOK); err != nil {
		return 0, err
	}
	return decodeCount(raw)
}
