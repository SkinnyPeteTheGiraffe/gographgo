package sdk

import (
	"context"
	"net/http"
	"strings"
)

// PutItem stores a global namespaced store item.
func (sc *StoreClient) PutItem(ctx context.Context, req StoreItemPutRequest) error {
	return sc.client.doNoContent(ctx, http.MethodPut, "/v1/store/items", req, http.StatusOK, http.StatusCreated, http.StatusNoContent)
}

// GetItem fetches a global namespaced store item.
func (sc *StoreClient) GetItem(ctx context.Context, namespace []string, key string, refreshTTL *bool) (*StoreItem, error) {
	q := map[string]any{"namespace": strings.Join(namespace, "."), "key": key}
	if refreshTTL != nil {
		q["refresh_ttl"] = *refreshTTL
	}
	p, err := withQuery("/v1/store/items", q)
	if err != nil {
		return nil, err
	}
	var out StoreItem
	if err := sc.client.doJSON(ctx, http.MethodGet, p, nil, &out, http.StatusOK); err != nil {
		return nil, err
	}
	return &out, nil
}

// DeleteItem deletes a global namespaced store item.
func (sc *StoreClient) DeleteItem(ctx context.Context, namespace []string, key string) error {
	body := map[string]any{"namespace": namespace, "key": key}
	return sc.client.doNoContent(ctx, http.MethodDelete, "/v1/store/items", body, http.StatusOK, http.StatusNoContent)
}

// SearchItems searches global store items.
func (sc *StoreClient) SearchItems(ctx context.Context, req StoreSearchRequest) (*StoreSearchResponse, error) {
	var out StoreSearchResponse
	if err := sc.client.doJSON(ctx, http.MethodPost, "/v1/store/items/search", req, &out, http.StatusOK); err != nil {
		return nil, err
	}
	return &out, nil
}

// ListNamespaces lists global store namespaces.
func (sc *StoreClient) ListNamespaces(ctx context.Context, req StoreNamespaceListRequest) (*StoreNamespaceListResponse, error) {
	var out StoreNamespaceListResponse
	if err := sc.client.doJSON(ctx, http.MethodPost, "/v1/store/namespaces", req, &out, http.StatusOK); err != nil {
		return nil, err
	}
	return &out, nil
}
