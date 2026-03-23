package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
)

type responseHandler func(*http.Response) error

func (c *Client) doJSON(ctx context.Context, method, reqPath string, in, out any, wantStatuses ...int) error {
	return c.doJSONWithResponse(ctx, method, reqPath, in, out, nil, wantStatuses...)
}

func (c *Client) doJSONWithResponse(
	ctx context.Context,
	method, reqPath string,
	in, out any,
	handler responseHandler,
	wantStatuses ...int,
) error {
	if len(wantStatuses) == 0 {
		wantStatuses = []int{http.StatusOK}
	}
	resp, err := c.doRequest(ctx, method, reqPath, in)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if !containsStatus(wantStatuses, resp.StatusCode) {
		return decodeHTTPError(resp)
	}
	if handler != nil {
		if err := handler(resp); err != nil {
			return err
		}
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (c *Client) doNoContent(ctx context.Context, method, reqPath string, in any, wantStatuses ...int) error {
	if len(wantStatuses) == 0 {
		wantStatuses = []int{http.StatusNoContent, http.StatusOK}
	}
	resp, err := c.doRequest(ctx, method, reqPath, in)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if !containsStatus(wantStatuses, resp.StatusCode) {
		return decodeHTTPError(resp)
	}
	return nil
}

func (c *Client) doRequest(ctx context.Context, method, reqPath string, in any) (*http.Response, error) {
	return c.doRequestWithHeaders(ctx, method, reqPath, in, nil)
}

func (c *Client) doRequestWithHeaders(
	ctx context.Context,
	method string,
	reqPath string,
	in any,
	headers map[string]string,
) (*http.Response, error) {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.absURL(reqPath), body)
	if err != nil {
		return nil, err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for key, value := range c.headers {
		req.Header.Set(key, value)
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	return c.httpClient.Do(req)
}

func (c *Client) absURL(reqPath string) string {
	rel, err := url.Parse(reqPath)
	if err != nil {
		u := *c.baseURL
		u.Path = path.Join(c.baseURL.Path, reqPath)
		return u.String()
	}
	u := *c.baseURL
	u.Path = path.Join(c.baseURL.Path, rel.Path)
	u.RawQuery = rel.RawQuery
	return u.String()
}

func decodeHTTPError(resp *http.Response) error {
	var payload struct {
		Error string `json:"error"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&payload)
	if payload.Error == "" {
		payload.Error = resp.Status
	}
	return fmt.Errorf("sdk: http %d: %s", resp.StatusCode, payload.Error)
}

func decodeCount(raw json.RawMessage) (int, error) {
	var direct int
	if err := json.Unmarshal(raw, &direct); err == nil {
		return direct, nil
	}
	var obj struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return 0, err
	}
	return obj.Count, nil
}
