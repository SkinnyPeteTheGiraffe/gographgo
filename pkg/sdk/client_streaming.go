package sdk

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
)

const maxSSEReconnectAttempts = 5

type sseStreamState struct {
	method            string
	path              string
	body              any
	lastEventID       string
	reconnectAttempts int
}

func (c *Client) streamSSE(
	ctx context.Context,
	method string,
	reqPath string,
	in any,
	headers map[string]string,
) (streamParts <-chan StreamPart, streamErrs <-chan error) {
	parts := make(chan StreamPart, 32)
	errs := make(chan error, 1)

	go func() {
		defer close(parts)
		defer close(errs)
		c.streamSSELoop(ctx, method, reqPath, in, headers, parts, errs)
	}()

	return parts, errs
}

func (c *Client) streamSSELoop(
	ctx context.Context,
	method string,
	reqPath string,
	in any,
	headers map[string]string,
	parts chan<- StreamPart,
	errs chan<- error,
) {
	state := sseStreamState{
		method:      method,
		path:        reqPath,
		body:        in,
		lastEventID: strings.TrimSpace(headers["Last-Event-ID"]),
	}

	for {
		if ctx.Err() != nil {
			return
		}
		resp, reconnectPath, err := c.openSSEStream(ctx, state, headers)
		if err != nil {
			errs <- err
			return
		}
		sawEnd, scanErr, ok := consumeSSEBody(ctx, resp.Body, &state.lastEventID, parts)
		if shouldStopSSELoop(ctx, ok, sawEnd, scanErr, reconnectPath, state.reconnectAttempts, errs) {
			return
		}
		state.reconnectAttempts++
		state.method = http.MethodGet
		state.path = reconnectPath
		state.body = nil
	}
}

func consumeSSEBody(ctx context.Context, body io.ReadCloser, lastEventID *string, parts chan<- StreamPart) (sawEnd bool, scanErr error, ok bool) {
	closeOnCancel := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = body.Close()
		case <-closeOnCancel:
		}
	}()

	sawEnd, scanErr, ok = consumeSSEStream(ctx, body, lastEventID, parts)
	close(closeOnCancel)
	_ = body.Close()
	return sawEnd, scanErr, ok
}

func shouldStopSSELoop(
	ctx context.Context,
	ok bool,
	sawEnd bool,
	scanErr error,
	reconnectPath string,
	reconnectAttempts int,
	errs chan<- error,
) bool {
	if !ok || ctx.Err() != nil {
		return true
	}

	if scanErr == nil {
		if sawEnd || reconnectPath == "" {
			return true
		}
		return reconnectAttempts >= maxSSEReconnectAttempts
	}

	if reconnectPath != "" && reconnectAttempts < maxSSEReconnectAttempts {
		return false
	}

	errs <- scanErr
	return true
}

func (c *Client) openSSEStream(ctx context.Context, state sseStreamState, headers map[string]string) (resp *http.Response, reconnectPath string, err error) {
	requestHeaders := makeSSEHeaders(headers, state.lastEventID)
	resp, err = c.doRequestWithHeaders(ctx, state.method, state.path, state.body, requestHeaders)
	if err != nil {
		return nil, "", err
	}
	if resp.StatusCode != http.StatusOK {
		err = decodeHTTPError(resp)
		_ = resp.Body.Close()
		return nil, "", err
	}
	contentType := strings.TrimSpace(resp.Header.Get("Content-Type"))
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		_ = resp.Body.Close()
		return nil, "", fmt.Errorf("sdk: invalid stream content type %q: %w", contentType, err)
	}
	if mediaType != "text/event-stream" {
		_ = resp.Body.Close()
		return nil, "", fmt.Errorf("sdk: expected text/event-stream response, got %q", contentType)
	}
	return resp, strings.TrimSpace(resp.Header.Get("Location")), nil
}

func makeSSEHeaders(base map[string]string, lastEventID string) map[string]string {
	headers := make(map[string]string, len(base)+3)
	for key, value := range base {
		if key == "Last-Event-ID" {
			continue
		}
		headers[key] = value
	}
	headers["Accept"] = "text/event-stream"
	headers["Cache-Control"] = "no-store"
	if lastEventID != "" {
		headers["Last-Event-ID"] = lastEventID
	}
	return headers
}

func consumeSSEStream(ctx context.Context, body io.Reader, lastEventID *string, out chan<- StreamPart) (sawEnd bool, scanErr error, ok bool) {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), 2*1024*1024)

	var (
		currentEvent string
		currentID    string
		dataLines    []string
	)

	dispatch := func() bool {
		if len(dataLines) == 0 {
			currentEvent = ""
			currentID = ""
			return true
		}
		if currentEvent == "end" {
			sawEnd = true
		}
		part := StreamPart{
			Event: currentEvent,
			ID:    currentID,
			Data:  json.RawMessage(strings.Join(dataLines, "\n")),
		}
		if part.ID != "" {
			*lastEventID = part.ID
		}
		select {
		case <-ctx.Done():
			return false
		case out <- part:
		}
		currentEvent = ""
		currentID = ""
		dataLines = dataLines[:0]
		return true
	}

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if !dispatch() {
				return sawEnd, nil, false
			}
			continue
		}
		switch {
		case strings.HasPrefix(line, "event:"):
			currentEvent = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		case strings.HasPrefix(line, "id:"):
			currentID = strings.TrimSpace(strings.TrimPrefix(line, "id:"))
		case strings.HasPrefix(line, "data:"):
			dataLines = append(dataLines, strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		}
	}
	if len(dataLines) > 0 && !dispatch() {
		return sawEnd, nil, false
	}
	return sawEnd, scanner.Err(), true
}

func (c *Client) streamSSEV2(
	ctx context.Context,
	method string,
	reqPath string,
	in any,
	headers map[string]string,
) (partCh <-chan StreamPartV2, errCh <-chan error) {
	rawParts, rawErrs := c.streamSSE(ctx, method, reqPath, in, headers)
	parts := make(chan StreamPartV2, 32)
	errs := make(chan error, 1)

	go func() {
		defer close(parts)
		defer close(errs)

		for part := range rawParts {
			decoded, skip, err := decodeStreamPartV2(part)
			if err != nil {
				errs <- err
				return
			}
			if skip {
				continue
			}
			select {
			case <-ctx.Done():
				return
			case parts <- decoded:
			}
		}

		for err := range rawErrs {
			if err != nil {
				errs <- err
				return
			}
		}
	}()

	return parts, errs
}

func decodeStreamPartV2(part StreamPart) (StreamPartV2, bool, error) {
	event := part.Event
	if event == "" {
		event = "events"
	}
	segments := strings.Split(event, "|")
	partType := segments[0]
	ns := []string{}
	if len(segments) > 1 {
		ns = segments[1:]
	}
	if partType == "end" {
		return nil, true, nil
	}

	interrupts := []map[string]any{}
	if partType == string(StreamModeValues) {
		var values map[string]any
		if err := json.Unmarshal(part.Data, &values); err != nil {
			return nil, false, fmt.Errorf("sdk: decode values stream part: %w", err)
		}
		if rawInterrupts, ok := values["__interrupt__"]; ok {
			var err error
			interrupts, err = toInterruptMaps(rawInterrupts)
			if err != nil {
				return nil, false, fmt.Errorf("sdk: decode values interrupts: %w", err)
			}
			delete(values, "__interrupt__")
		}
		return ValuesStreamPart{
			Type:       partType,
			NS:         ns,
			Data:       values,
			Interrupts: interrupts,
			Event:      part.Event,
			ID:         part.ID,
		}, false, nil
	}

	decoded, err := decodeStreamPartData(part.Data)
	if err != nil {
		return nil, false, fmt.Errorf("sdk: decode stream part data: %w", err)
	}

	asMap, _ := decoded.(map[string]any)
	if asMap == nil {
		asMap = map[string]any{"value": decoded}
	}

	switch partType {
	case string(StreamModeUpdates):
		return UpdatesStreamPart{Type: partType, NS: ns, Data: asMap, Interrupts: interrupts, Event: part.Event, ID: part.ID}, false, nil
	case string(StreamModeMessages):
		return MessagesStreamPart{Type: partType, NS: ns, Data: decoded, Interrupts: interrupts, Event: part.Event, ID: part.ID}, false, nil
	case string(StreamModeCustom):
		return CustomStreamPart{Type: partType, NS: ns, Data: decoded, Interrupts: interrupts, Event: part.Event, ID: part.ID}, false, nil
	case string(StreamModeCheckpoints):
		return CheckpointStreamPart{Type: partType, NS: ns, Data: asMap, Interrupts: interrupts, Event: part.Event, ID: part.ID}, false, nil
	case string(StreamModeTasks):
		return TasksStreamPart{Type: partType, NS: ns, Data: asMap, Interrupts: interrupts, Event: part.Event, ID: part.ID}, false, nil
	case string(StreamModeDebug):
		return DebugStreamPart{Type: partType, NS: ns, Data: asMap, Interrupts: interrupts, Event: part.Event, ID: part.ID}, false, nil
	case "metadata":
		return MetadataStreamPart{Type: partType, NS: ns, Data: asMap, Interrupts: interrupts, Event: part.Event, ID: part.ID}, false, nil
	default:
		return UnknownStreamPart{Type: partType, NS: ns, Data: decoded, Interrupts: interrupts, Event: part.Event, ID: part.ID}, false, nil
	}
}

func decodeStreamPartData(raw json.RawMessage) (any, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return nil, nil
	}
	var out any
	if err := json.Unmarshal(trimmed, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func toInterruptMaps(value any) ([]map[string]any, error) {
	if value == nil {
		return []map[string]any{}, nil
	}
	array, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("expected []any, got %T", value)
	}
	out := make([]map[string]any, 0, len(array))
	for i, item := range array {
		asMap, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("interrupt %d: expected map, got %T", i, item)
		}
		out = append(out, asMap)
	}
	return out, nil
}
