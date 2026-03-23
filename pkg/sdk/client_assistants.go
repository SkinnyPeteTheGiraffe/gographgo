package sdk

import (
	"context"
	"encoding/json"
	"net/http"
)

// Get gets an assistant by ID.
func (ac *AssistantsClient) Get(ctx context.Context, assistantID string) (*Assistant, error) {
	var out Assistant
	if err := ac.client.doJSON(ctx, http.MethodGet, "/v1/assistants/"+assistantID, nil, &out, http.StatusOK); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetGraph returns the assistant graph JSON.
func (ac *AssistantsClient) GetGraph(ctx context.Context, assistantID string, xray any) (map[string]any, error) {
	p, err := withQuery("/v1/assistants/"+assistantID+"/graph", map[string]any{"xray": xray})
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := ac.client.doJSON(ctx, http.MethodGet, p, nil, &out, http.StatusOK); err != nil {
		return nil, err
	}
	return out, nil
}

// GetSchemas returns assistant graph schemas.
func (ac *AssistantsClient) GetSchemas(ctx context.Context, assistantID string) (map[string]any, error) {
	var out map[string]any
	if err := ac.client.doJSON(ctx, http.MethodGet, "/v1/assistants/"+assistantID+"/schemas", nil, &out, http.StatusOK); err != nil {
		return nil, err
	}
	return out, nil
}

// GetSubgraphs returns subgraph descriptors.
func (ac *AssistantsClient) GetSubgraphs(ctx context.Context, assistantID, namespace string, recurse bool) (map[string]any, error) {
	p := "/v1/assistants/" + assistantID + "/subgraphs"
	if namespace != "" {
		p += "/" + namespace
	}
	q, err := withQuery(p, map[string]any{"recurse": recurse})
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := ac.client.doJSON(ctx, http.MethodGet, q, nil, &out, http.StatusOK); err != nil {
		return nil, err
	}
	return out, nil
}

// Create creates an assistant.
func (ac *AssistantsClient) Create(ctx context.Context, req AssistantCreateRequest) (*Assistant, error) {
	var out Assistant
	if err := ac.client.doJSON(ctx, http.MethodPost, "/v1/assistants", req, &out, http.StatusCreated, http.StatusOK); err != nil {
		return nil, err
	}
	return &out, nil
}

// Update updates an assistant.
func (ac *AssistantsClient) Update(ctx context.Context, assistantID string, req AssistantUpdateRequest) (*Assistant, error) {
	var out Assistant
	if err := ac.client.doJSON(ctx, http.MethodPatch, "/v1/assistants/"+assistantID, req, &out, http.StatusOK); err != nil {
		return nil, err
	}
	return &out, nil
}

// Delete deletes an assistant.
func (ac *AssistantsClient) Delete(ctx context.Context, assistantID string, deleteThreads bool) error {
	p, err := withQuery("/v1/assistants/"+assistantID, map[string]any{"delete_threads": deleteThreads})
	if err != nil {
		return err
	}
	return ac.client.doNoContent(ctx, http.MethodDelete, p, nil, http.StatusOK, http.StatusNoContent)
}

// Search searches assistants.
func (ac *AssistantsClient) Search(ctx context.Context, req AssistantSearchRequest) ([]Assistant, error) {
	var out []Assistant
	if err := ac.client.doJSON(ctx, http.MethodPost, "/v1/assistants/search", req, &out, http.StatusOK); err != nil {
		return nil, err
	}
	return out, nil
}

func (ac *AssistantsClient) SearchObject(ctx context.Context, req AssistantSearchRequest) (*AssistantSearchResponse, error) {
	var (
		out  []Assistant
		next string
	)
	handler := func(resp *http.Response) error {
		next = resp.Header.Get("X-Pagination-Next")
		return nil
	}
	if err := ac.client.doJSONWithResponse(ctx, http.MethodPost, "/v1/assistants/search", req, &out, handler, http.StatusOK); err != nil {
		return nil, err
	}
	return &AssistantSearchResponse{Assistants: out, Next: next}, nil
}

// Count returns assistant count for matching filters.
func (ac *AssistantsClient) Count(ctx context.Context, req AssistantCountRequest) (int, error) {
	var raw json.RawMessage
	if err := ac.client.doJSON(ctx, http.MethodPost, "/v1/assistants/count", req, &raw, http.StatusOK); err != nil {
		return 0, err
	}
	return decodeCount(raw)
}

// GetVersions returns assistant versions.
func (ac *AssistantsClient) GetVersions(ctx context.Context, assistantID string, req AssistantVersionRequest) ([]AssistantVersion, error) {
	var out []AssistantVersion
	if err := ac.client.doJSON(ctx, http.MethodPost, "/v1/assistants/"+assistantID+"/versions", req, &out, http.StatusOK); err != nil {
		return nil, err
	}
	return out, nil
}

// SetLatest sets the assistant's active version.
func (ac *AssistantsClient) SetLatest(ctx context.Context, assistantID string, version int) (*Assistant, error) {
	var out Assistant
	if err := ac.client.doJSON(ctx, http.MethodPost, "/v1/assistants/"+assistantID+"/latest", map[string]any{"version": version}, &out, http.StatusOK); err != nil {
		return nil, err
	}
	return &out, nil
}
