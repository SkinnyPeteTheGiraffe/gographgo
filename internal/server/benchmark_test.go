package server

import (
	"testing"
	"time"
)

func BenchmarkPublishEventFanout(b *testing.B) {
	b.ReportAllocs()

	rec := &runRecord{run: &Run{}, subs: make(map[int]chan RunEvent)}
	for i := 0; i < 64; i++ {
		rec.subs[i] = make(chan RunEvent, 1)
	}

	s := &Server{}
	evt := RunEvent{Type: "run.progress", ThreadID: "t", RunID: "r", Timestamp: time.Date(2026, 3, 22, 0, 0, 0, 0, time.UTC)}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.publishEventLocked(rec, evt)
		for _, ch := range rec.subs {
			select {
			case <-ch:
			default:
			}
		}
	}
}
