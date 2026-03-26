package sdk_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/sdk"
)

func BenchmarkStreamRunEventsParser(b *testing.B) {
	b.ReportAllocs()
	const perStreamEvents = 32

	h := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		for i := 0; i < perStreamEvents; i++ {
			_, _ = fmt.Fprintf(
				w,
				"event: run.progress\ndata: {\"type\":\"run.progress\",\"thread_id\":\"t\",\"run_id\":\"r\",\"timestamp\":\"2026-03-22T00:00:00Z\",\"payload\":{\"i\":%d}}\n\n",
				i,
			)
		}
	})
	ts := httptest.NewServer(h)
	defer ts.Close()

	client, err := sdk.New(sdk.Config{BaseURL: ts.URL})
	if err != nil {
		b.Fatalf("sdk.New() error = %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		events, errs := client.StreamRunEvents(b.Context(), "t", "r")
		count := 0
		for range events {
			count++
		}
		for err := range errs {
			if err != nil {
				b.Fatalf("stream error: %v", err)
			}
		}
		if count != perStreamEvents {
			b.Fatalf("event count = %d, want %d", count, perStreamEvents)
		}
	}
}
