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
	defer h.Close()

	msg := []byte(fmt.Sprintf("%0120s", "event message"))
	ev := newEvent(time.Now(), 4, true, msg)
	b.ReportAllocs()
	for range b.N {
		if _, err := h.appendEvent(make([]byte, 0, 256), ev); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeBatch(b *testing.B) {
	h, err := NewHandler("xaat-secret", "logs", withBaseURL("http://127.0.0.1:1"))
	if err != nil {
		b.Fatal(err)
	}
	defer h.Close()

	msg := []byte(fmt.Sprintf("%0120s", "event message"))
	ev := newEvent(time.Now(), 4, true, msg)
	batch := make([]queuedEvent, 1000)
	var total int64
	for i := range batch {
		line, err := h.appendEvent(make([]byte, 0, 256), ev)
		if err != nil {
			b.Fatal(err)
		}
		batch[i] = queuedEvent{line: line, cost: int64(len(line))}
		total += int64(len(line))
	}

	var buf bytes.Buffer
	b.ReportAllocs()
	b.SetBytes(total)
	for range b.N {
		if _, err := h.encodeBatch(batch, &buf); err != nil {
			b.Fatal(err)
		}
	}
}
