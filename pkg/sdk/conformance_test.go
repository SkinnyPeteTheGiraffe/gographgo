package sdk_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/SkinnyPeteTheGiraffe/gographgo/internal/server"
	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/checkpoint"
	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/sdk"
)

type conformanceRunner struct{}

func (conformanceRunner) Run(_ context.Context, req server.RunRequest) (server.RunResult, error) {
	msg := ""
	if req.Input != nil {
		if v, ok := req.Input["message"].(string); ok {
			msg = v
		}
	}
	return server.RunResult{
		Output: map[string]any{"text": "ok:" + msg},
		State:  map[string]any{"last": msg},
	}, nil
}

func TestWireConformanceGolden(t *testing.T) {
	t.Parallel()

	s := server.New(server.Options{Runner: conformanceRunner{}, Checkpointer: checkpoint.NewInMemorySaver()})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	client, err := sdk.New(sdk.Config{BaseURL: ts.URL})
	if err != nil {
		t.Fatalf("sdk.New() error = %v", err)
	}

	ctx := context.Background()
	thread, err := client.CreateThread(ctx, "thread-conformance", map[string]any{"suite": "phase9"})
	if err != nil {
		t.Fatalf("CreateThread() error = %v", err)
	}

	run, err := client.CreateRun(
		ctx,
		thread.ID,
		"assistant-conformance",
		map[string]any{"message": "hello"},
		map[string]any{"suite": "phase9"},
	)
	if err != nil {
		t.Fatalf("CreateRun() error = %v", err)
	}

	finalRun, err := client.WaitRun(ctx, thread.ID, run.ID, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("WaitRun() error = %v", err)
	}

	state, err := client.GetThreadState(ctx, thread.ID)
	if err != nil {
		t.Fatalf("GetThreadState() error = %v", err)
	}

	history, err := client.ListThreadHistory(ctx, thread.ID)
	if err != nil {
		t.Fatalf("ListThreadHistory() error = %v", err)
	}

	events, errs := client.StreamRunEvents(ctx, thread.ID, run.ID)
	streamEvents := make([]sdk.RunEvent, 0, 4)
	for evt := range events {
		streamEvents = append(streamEvents, evt)
	}
	for err := range errs {
		if err != nil {
			t.Fatalf("StreamRunEvents() error = %v", err)
		}
	}

	assertJSONGolden(t, "thread.json", normalizeDynamic(thread, ""))
	assertJSONGolden(t, "run.json", normalizeDynamic(finalRun, ""))
	assertJSONGolden(t, "state.json", normalizeDynamic(state, ""))
	assertJSONGolden(t, "history.json", normalizeDynamic(history, ""))
	assertJSONGolden(t, "events.json", normalizeDynamic(streamEvents, ""))
}

func assertJSONGolden(t *testing.T, filename string, got any) {
	t.Helper()
	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	err := enc.Encode(got)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	path := filepath.Join("testdata", "conformance", filename)
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("os.ReadFile(%q) error = %v", path, err)
	}
	if normalizeGoldenNewlines(string(want)) != normalizeGoldenNewlines(buf.String()) {
		t.Fatalf("golden mismatch for %s\nwant:\n%s\n\ngot:\n%s\n", path, string(want), buf.String())
	}
}

func normalizeGoldenNewlines(s string) string {
	return strings.ReplaceAll(s, "\r\n", "\n")
}

func normalizeDynamic(v any, parentKey string) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, raw := range x {
			switch k {
			case "created_at", "started_at", "ended_at", "timestamp", "ts", "TS":
				out[k] = "<ts>"
			case "run_id", "RunID":
				out[k] = "<run_id>"
			case "checkpoint_id", "CheckpointID":
				out[k] = "<checkpoint_id>"
			case "ID":
				if parentKey == "Checkpoint" {
					out[k] = "<checkpoint_id>"
				} else {
					out[k] = normalizeDynamic(raw, k)
				}
			default:
				out[k] = normalizeDynamic(raw, k)
			}
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i := range x {
			out[i] = normalizeDynamic(x[i], parentKey)
		}
		return out
	case *sdk.Thread:
		return normalizeDynamic(toMap(x), parentKey)
	case *sdk.Run:
		return normalizeDynamic(toMap(x), parentKey)
	case *sdk.ThreadState:
		return normalizeDynamic(toMap(x), parentKey)
	case []checkpoint.CheckpointTuple:
		return normalizeDynamic(toAnySliceCheckpoint(x), parentKey)
	case []sdk.RunEvent:
		return normalizeDynamic(toAnySliceEvents(x), parentKey)
	default:
		return x
	}
}

func toMap(v any) map[string]any {
	b, _ := json.Marshal(v)
	out := map[string]any{}
	_ = json.Unmarshal(b, &out)
	return out
}

func toAnySliceCheckpoint(v []checkpoint.CheckpointTuple) []any {
	out := make([]any, len(v))
	for i := range v {
		out[i] = toMap(v[i])
	}
	return out
}

func toAnySliceEvents(v []sdk.RunEvent) []any {
	out := make([]any, len(v))
	for i := range v {
		out[i] = toMap(v[i])
	}
	return out
}
