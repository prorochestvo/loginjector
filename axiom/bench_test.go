package axiom

import (
	"bytes"
	"fmt"
	"testing"
	"time"
)

func BenchmarkAppendEvent(b *testing.B) {
	h, err := NewHandler("xaat-secret", "logs", withBaseURL("http://127.0.0.1:1"), WithFields(map[string]any{"service": "api"}))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := h.Close(); err != nil {
			b.Errorf("close handler: %v", err)
		}
	})

	msg := []byte(fmt.Sprintf("%0120s", "event message"))
	ev := newEvent(time.Now(), 4, true, msg)
	b.ReportAllocs()
	for range b.N {
		if _, err := h.appendEvent(nil, ev); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCompressPayload(b *testing.B) {
	h, err := NewHandler("xaat-secret", "logs", withBaseURL("http://127.0.0.1:1"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := h.Close(); err != nil {
			b.Errorf("close handler: %v", err)
		}
	})
	ev := newEvent(time.Now(), 4, true, []byte(fmt.Sprintf("%0120s", "event message")))
	var payload bytes.Buffer
	for range 1000 {
		line, err := h.appendEvent(nil, ev)
		if err != nil {
			b.Fatal(err)
		}
		payload.Write(line)
	}
	b.ReportAllocs()
	b.SetBytes(int64(payload.Len()))
	for range b.N {
		if _, err := h.compressPayload(payload.Bytes()); err != nil {
			b.Fatal(err)
		}
	}
}
