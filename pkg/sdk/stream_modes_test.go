package sdk

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestDecodeStreamPartV2Modes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantMatch any
		name      string
		event     string
		data      string
	}{
		{name: "values", event: "values|root", data: `{"count":1}`, wantMatch: ValuesStreamPart{}},
		{name: "updates", event: "updates|node1", data: `{"node1":{"done":true}}`, wantMatch: UpdatesStreamPart{}},
		{name: "messages", event: "messages|node1", data: `[{"role":"assistant"},{"langgraph_node":"node1"}]`, wantMatch: MessagesStreamPart{}},
		{name: "custom", event: "custom|node1", data: `{"trace_id":"abc"}`, wantMatch: CustomStreamPart{}},
		{name: "checkpoints", event: "checkpoints|node1", data: `{"values":{"x":1}}`, wantMatch: CheckpointStreamPart{}},
		{name: "tasks", event: "tasks|node1", data: `{"id":"task1","name":"node1"}`, wantMatch: TasksStreamPart{}},
		{name: "debug", event: "debug|node1", data: `{"type":"task","payload":{"id":"task1"}}`, wantMatch: DebugStreamPart{}},
		{name: "metadata", event: "metadata", data: `{"run_id":"r1"}`, wantMatch: MetadataStreamPart{}},
		{name: "unknown", event: "events", data: `{"raw":true}`, wantMatch: UnknownStreamPart{}},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			part, skip, err := decodeStreamPartV2(StreamPart{Event: tt.event, Data: []byte(tt.data)})
			if err != nil {
				t.Fatalf("decodeStreamPartV2() error = %v", err)
			}
			if skip {
				t.Fatal("decodeStreamPartV2() skip = true, want false")
			}

			switch tt.wantMatch.(type) {
			case ValuesStreamPart:
				if _, ok := part.(ValuesStreamPart); !ok {
					t.Fatalf("decoded type = %T, want ValuesStreamPart", part)
				}
			case UpdatesStreamPart:
				if _, ok := part.(UpdatesStreamPart); !ok {
					t.Fatalf("decoded type = %T, want UpdatesStreamPart", part)
				}
			case MessagesStreamPart:
				if _, ok := part.(MessagesStreamPart); !ok {
					t.Fatalf("decoded type = %T, want MessagesStreamPart", part)
				}
			case CustomStreamPart:
				if _, ok := part.(CustomStreamPart); !ok {
					t.Fatalf("decoded type = %T, want CustomStreamPart", part)
				}
			case CheckpointStreamPart:
				if _, ok := part.(CheckpointStreamPart); !ok {
					t.Fatalf("decoded type = %T, want CheckpointStreamPart", part)
				}
			case TasksStreamPart:
				if _, ok := part.(TasksStreamPart); !ok {
					t.Fatalf("decoded type = %T, want TasksStreamPart", part)
				}
			case DebugStreamPart:
				if _, ok := part.(DebugStreamPart); !ok {
					t.Fatalf("decoded type = %T, want DebugStreamPart", part)
				}
			case MetadataStreamPart:
				if _, ok := part.(MetadataStreamPart); !ok {
					t.Fatalf("decoded type = %T, want MetadataStreamPart", part)
				}
			case UnknownStreamPart:
				if _, ok := part.(UnknownStreamPart); !ok {
					t.Fatalf("decoded type = %T, want UnknownStreamPart", part)
				}
			default:
				t.Fatalf("unsupported expected type %T", tt.wantMatch)
			}
		})
	}
}

func TestDecodeStreamPartV2ValuesInterrupts(t *testing.T) {
	t.Parallel()

	decoded, skip, err := decodeStreamPartV2(StreamPart{
		Event: "values|a|b",
		Data:  []byte(`{"x":1,"__interrupt__":[{"id":"i1"}]}`),
	})
	if err != nil {
		t.Fatalf("decodeStreamPartV2() error = %v", err)
	}
	if skip {
		t.Fatal("decodeStreamPartV2() skip = true, want false")
	}

	part, ok := decoded.(ValuesStreamPart)
	if !ok {
		t.Fatalf("decoded type = %T, want ValuesStreamPart", decoded)
	}
	if len(part.Interrupts) != 1 || part.Interrupts[0]["id"] != "i1" {
		t.Fatalf("interrupts = %#v", part.Interrupts)
	}
	if _, exists := part.Data["__interrupt__"]; exists {
		t.Fatalf("values data still contains __interrupt__: %#v", part.Data)
	}
	if len(part.NS) != 2 || part.NS[0] != "a" || part.NS[1] != "b" {
		t.Fatalf("ns = %#v", part.NS)
	}
}

func TestTypedStreamMethods(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/threads/t1/stream":
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("event: updates|worker\ndata: {\"delta\":1}\n\n"))
		case "/v1/threads/t1/runs/stream":
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("event: values|root\ndata: {\"x\":1,\"__interrupt__\":[]}\n\n"))
		case "/v1/threads/t1/runs/r1/stream":
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("event: tasks|root\ndata: {\"id\":\"task-1\"}\n\n"))
		default:
			t.Fatalf("unexpected request path %s", r.URL.Path)
		}
	}))
	defer ts.Close()

	client, err := New(Config{BaseURL: ts.URL})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx := context.Background()

	tParts, tErrs := client.Threads.JoinStreamTyped(ctx, "t1", ThreadJoinStreamRequest{StreamMode: []string{"updates"}})
	select {
	case part := <-tParts:
		if _, ok := part.(UpdatesStreamPart); !ok {
			t.Fatalf("thread typed part = %T, want UpdatesStreamPart", part)
		}
	case err := <-tErrs:
		t.Fatalf("Threads.JoinStreamTyped() error = %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for thread typed stream")
	}

	rParts, rErrs := client.Runs.StreamTyped(ctx, "t1", RunStreamRequest{AssistantID: "a1", StreamMode: []string{"values"}})
	select {
	case part := <-rParts:
		if _, ok := part.(ValuesStreamPart); !ok {
			t.Fatalf("run typed part = %T, want ValuesStreamPart", part)
		}
	case err := <-rErrs:
		t.Fatalf("Runs.StreamTyped() error = %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for run typed stream")
	}

	jParts, jErrs := client.Runs.JoinStreamTyped(ctx, "t1", "r1", RunJoinStreamRequest{StreamMode: []string{"tasks"}})
	select {
	case part := <-jParts:
		if _, ok := part.(TasksStreamPart); !ok {
			t.Fatalf("join typed part = %T, want TasksStreamPart", part)
		}
	case err := <-jErrs:
		t.Fatalf("Runs.JoinStreamTyped() error = %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for join typed stream")
	}
}

func TestStreamMethodsSendSSEHeaders(t *testing.T) {
	t.Parallel()

	requests := make(chan *http.Request, 3)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- r.Clone(r.Context())
		w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		_, _ = w.Write([]byte("event: values\ndata: {}\n\n"))
	}))
	defer ts.Close()

	client, err := New(Config{BaseURL: ts.URL})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx := context.Background()
	parts, errs := client.Runs.Stream(ctx, "", RunStreamRequest{AssistantID: "a1"})
	select {
	case <-parts:
	case err := <-errs:
		t.Fatalf("Runs.Stream() error = %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for run stream")
	}

	threadParts, threadErrs := client.Threads.JoinStream(ctx, "t1", ThreadJoinStreamRequest{LastEventID: "9"})
	select {
	case <-threadParts:
	case err := <-threadErrs:
		t.Fatalf("Threads.JoinStream() error = %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for thread stream")
	}

	joinParts, joinErrs := client.Runs.JoinStream(ctx, "t1", "r1", RunJoinStreamRequest{LastEventID: "11"})
	select {
	case <-joinParts:
	case err := <-joinErrs:
		t.Fatalf("Runs.JoinStream() error = %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for join stream")
	}

	for i := 0; i < 3; i++ {
		select {
		case req := <-requests:
			if got := req.Header.Get("Accept"); got != "text/event-stream" {
				t.Fatalf("request %d Accept = %q", i, got)
			}
			if got := req.Header.Get("Cache-Control"); got != "no-store" {
				t.Fatalf("request %d Cache-Control = %q", i, got)
			}
			if strings.Contains(req.URL.Path, "/stream") && req.Method == http.MethodGet && req.Header.Get("Last-Event-ID") == "" {
				t.Fatalf("request %d missing Last-Event-ID for reconnectable GET stream", i)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for captured request")
		}
	}
}

func TestStreamMethodsRejectNonEventStreamResponses(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer ts.Close()

	client, err := New(Config{BaseURL: ts.URL})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	_, errs := client.Runs.Stream(context.Background(), "", RunStreamRequest{AssistantID: "a1"})
	select {
	case err := <-errs:
		if err == nil || !strings.Contains(err.Error(), "text/event-stream") {
			t.Fatalf("Runs.Stream() err = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for stream error")
	}
}

func TestStreamMethodsReconnectOnLocationEOF(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := calls.Add(1)
		if call == 1 {
			if r.Method != http.MethodPost || r.URL.Path != "/v1/runs/stream" {
				t.Fatalf("first request = %s %s", r.Method, r.URL.Path)
			}
			if got := r.Header.Get("Last-Event-ID"); got != "" {
				t.Fatalf("first Last-Event-ID = %q, want empty", got)
			}
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("Location", "/v1/runs/stream/reconnect")
			_, _ = w.Write([]byte("id: 1\nevent: updates\ndata: {\"status\":\"running\"}\n\n"))
			return
		}

		if r.Method != http.MethodGet || r.URL.Path != "/v1/runs/stream/reconnect" {
			t.Fatalf("reconnect request = %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Last-Event-ID"); got != "1" {
			t.Fatalf("reconnect Last-Event-ID = %q, want 1", got)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Location", "/v1/runs/stream/reconnect")
		_, _ = w.Write([]byte("id: 2\nevent: updates\ndata: {\"status\":\"success\"}\n\nid: 3\nevent: end\ndata: {}\n\n"))
	}))
	defer ts.Close()

	client, err := New(Config{BaseURL: ts.URL})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	parts, errs := client.Runs.Stream(context.Background(), "", RunStreamRequest{AssistantID: "a1", StreamMode: []string{"updates"}})
	gotEvents := make([]string, 0, 3)
	timeout := time.After(2 * time.Second)
	for len(gotEvents) < 3 {
		select {
		case part, ok := <-parts:
			if !ok {
				t.Fatalf("parts channel closed early with events %#v", gotEvents)
			}
			gotEvents = append(gotEvents, part.Event)
		case err := <-errs:
			if err != nil {
				t.Fatalf("Runs.Stream() error = %v", err)
			}
		case <-timeout:
			t.Fatalf("timeout waiting for reconnected stream events; got %#v", gotEvents)
		}
	}

	if gotEvents[0] != "updates" || gotEvents[1] != "updates" || gotEvents[2] != "end" {
		t.Fatalf("stream events = %#v, want [updates updates end]", gotEvents)
	}
	if calls.Load() != 2 {
		t.Fatalf("request count = %d, want 2", calls.Load())
	}
}

func TestShouldStopSSELoop(t *testing.T) {
	t.Parallel()

	scanErr := errors.New("scan failed")

	tests := []struct {
		ctx               context.Context
		scanErr           error
		reconnectPath     string
		name              string
		reconnectAttempts int
		ok                bool
		sawEnd            bool
		wantStop          bool
		wantErr           bool
	}{
		{
			name:     "stop when consume reports not ok",
			ctx:      context.Background(),
			ok:       false,
			wantStop: true,
		},
		{
			name:     "stop when context canceled",
			ctx:      canceledContext(),
			ok:       true,
			wantStop: true,
		},
		{
			name:          "stop cleanly on end event",
			ctx:           context.Background(),
			ok:            true,
			sawEnd:        true,
			scanErr:       nil,
			reconnectPath: "/v1/runs/stream/reconnect",
			wantStop:      true,
		},
		{
			name:          "stop cleanly without reconnect location",
			ctx:           context.Background(),
			ok:            true,
			sawEnd:        false,
			scanErr:       nil,
			reconnectPath: "",
			wantStop:      true,
		},
		{
			name:              "continue reconnect on clean EOF when attempts remain",
			ctx:               context.Background(),
			ok:                true,
			sawEnd:            false,
			scanErr:           nil,
			reconnectPath:     "/v1/runs/stream/reconnect",
			reconnectAttempts: maxSSEReconnectAttempts - 1,
			wantStop:          false,
		},
		{
			name:              "stop reconnect on clean EOF when attempts exhausted",
			ctx:               context.Background(),
			ok:                true,
			sawEnd:            false,
			scanErr:           nil,
			reconnectPath:     "/v1/runs/stream/reconnect",
			reconnectAttempts: maxSSEReconnectAttempts,
			wantStop:          true,
		},
		{
			name:              "continue retry when scan fails and reconnect available",
			ctx:               context.Background(),
			ok:                true,
			scanErr:           scanErr,
			reconnectPath:     "/v1/runs/stream/reconnect",
			reconnectAttempts: maxSSEReconnectAttempts - 1,
			wantStop:          false,
		},
		{
			name:          "emit scan error when reconnect missing",
			ctx:           context.Background(),
			ok:            true,
			scanErr:       scanErr,
			reconnectPath: "",
			wantStop:      true,
			wantErr:       true,
		},
		{
			name:              "emit scan error when reconnect attempts exhausted",
			ctx:               context.Background(),
			ok:                true,
			scanErr:           scanErr,
			reconnectPath:     "/v1/runs/stream/reconnect",
			reconnectAttempts: maxSSEReconnectAttempts,
			wantStop:          true,
			wantErr:           true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			errs := make(chan error, 1)
			gotStop := shouldStopSSELoop(tt.ctx, tt.ok, tt.sawEnd, tt.scanErr, tt.reconnectPath, tt.reconnectAttempts, errs)
			if gotStop != tt.wantStop {
				t.Fatalf("shouldStopSSELoop() = %v, want %v", gotStop, tt.wantStop)
			}

			select {
			case err := <-errs:
				if !tt.wantErr {
					t.Fatalf("unexpected error sent: %v", err)
				}
				if !errors.Is(err, scanErr) {
					t.Fatalf("error sent = %v, want %v", err, scanErr)
				}
			default:
				if tt.wantErr {
					t.Fatal("expected error to be sent")
				}
			}
		})
	}
}

func canceledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}
