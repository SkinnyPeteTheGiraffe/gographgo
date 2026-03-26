package server

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestWriteSSE_RunEvent(t *testing.T) {
	rec := httptest.NewRecorder()
	evt := RunEvent{
		Type:      "run.completed",
		ThreadID:  "thread-1",
		RunID:     "run-1",
		Timestamp: time.Date(2026, 3, 26, 1, 2, 3, 0, time.UTC),
		Payload:   map[string]any{"status": "success"},
	}

	if err := writeSSE(rec, evt); err != nil {
		t.Fatalf("writeSSE: %v", err)
	}
	body := rec.Body.String()
	if !strings.HasPrefix(body, "event: run.completed\n") {
		t.Fatalf("SSE prefix = %q, want event line", body)
	}
	lines := strings.Split(strings.TrimSuffix(body, "\n\n"), "\n")
	if len(lines) != 2 || !strings.HasPrefix(lines[1], "data: ") {
		t.Fatalf("SSE lines = %#v, want event/data lines", lines)
	}
	var decoded RunEvent
	if err := json.Unmarshal([]byte(strings.TrimPrefix(lines[1], "data: ")), &decoded); err != nil {
		t.Fatalf("decode data line: %v", err)
	}
	if decoded.Type != "run.completed" || decoded.RunID != "run-1" {
		t.Fatalf("decoded event = %+v, want run.completed/run-1", decoded)
	}
}

func TestWriteSSEWithID_RunEvent(t *testing.T) {
	rec := httptest.NewRecorder()
	evt := RunEvent{
		Type:      "run.started",
		ThreadID:  "thread-1",
		RunID:     "run-2",
		Timestamp: time.Date(2026, 3, 26, 1, 2, 4, 0, time.UTC),
	}

	if err := writeSSEWithID(rec, evt.Type, evt, 7); err != nil {
		t.Fatalf("writeSSEWithID: %v", err)
	}
	body := rec.Body.String()
	if !strings.HasPrefix(body, "id: 7\n") {
		t.Fatalf("SSE id prefix = %q, want id line", body)
	}
	if !strings.Contains(body, "event: run.started\n") {
		t.Fatalf("SSE body = %q, want run.started event", body)
	}
}

func TestWriteSSEWithID_GenericPayload(t *testing.T) {
	rec := httptest.NewRecorder()
	payload := map[string]any{"thread_id": "t1", "ok": true}

	if err := writeSSEWithID(rec, "metadata", payload, 3); err != nil {
		t.Fatalf("writeSSEWithID generic: %v", err)
	}
	body := rec.Body.String()
	if !strings.HasPrefix(body, "id: 3\n") {
		t.Fatalf("SSE id prefix = %q, want id line", body)
	}
	if !strings.Contains(body, "event: metadata\n") {
		t.Fatalf("SSE body = %q, want metadata event", body)
	}
	if !strings.Contains(body, "data: ") {
		t.Fatalf("SSE body = %q, want data line", body)
	}
}
