package httptap

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewAccessHandler exercises NewAccessHandler across all enumerated scenarios.
func TestNewAccessHandler(t *testing.T) {
	t.Parallel()

	// reTimestamp matches the RFC3339Nano timestamp prefix of each log line.
	reTimestamp := regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?Z`)
	// reDuration matches the trailing duration field.
	reDuration := regexp.MustCompile(`\d+\.\d{3}ms\n$`)

	t.Run("default_status_200", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		next := func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("hello"))
		}
		h := NewAccessHandler(&buf, next)
		require.NotNil(t, h)

		req := httptest.NewRequest(http.MethodGet, "/api/v1/rates", nil)
		rec := httptest.NewRecorder()
		h(rec, req)

		line := buf.String()
		require.Contains(t, line, "[200]")
	})

	t.Run("explicit_status_404", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		next := func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}
		h := NewAccessHandler(&buf, next)

		req := httptest.NewRequest(http.MethodGet, "/missing", nil)
		rec := httptest.NewRecorder()
		h(rec, req)

		require.Contains(t, buf.String(), "[404]")
	})

	t.Run("explicit_status_500", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		next := func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}
		h := NewAccessHandler(&buf, next)

		req := httptest.NewRequest(http.MethodPost, "/boom", nil)
		rec := httptest.NewRecorder()
		h(rec, req)

		require.Contains(t, buf.String(), "[500]")
	})

	t.Run("repeat_writeheader_ignored", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		next := func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusCreated)
			w.WriteHeader(http.StatusInternalServerError) // must be ignored
		}
		h := NewAccessHandler(&buf, next)

		req := httptest.NewRequest(http.MethodPut, "/item", nil)
		rec := httptest.NewRecorder()
		h(rec, req)

		line := buf.String()
		require.Contains(t, line, "[201]")
		require.NotContains(t, line, "[500]")
	})

	t.Run("query_string_stripped", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		next := func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}
		h := NewAccessHandler(&buf, next)

		req := httptest.NewRequest(http.MethodGet, "/x?secret=token", nil)
		rec := httptest.NewRecorder()
		h(rec, req)

		line := buf.String()
		require.Contains(t, line, "/x ")
		require.NotContains(t, line, "secret")
	})

	t.Run("duration_format", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		next := func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}
		h := NewAccessHandler(&buf, next)

		req := httptest.NewRequest(http.MethodGet, "/timing", nil)
		rec := httptest.NewRecorder()
		h(rec, req)

		require.Regexp(t, reDuration, buf.String())
	})

	t.Run("timestamp_format", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		next := func(_ http.ResponseWriter, _ *http.Request) {}
		h := NewAccessHandler(&buf, next)

		req := httptest.NewRequest(http.MethodGet, "/ts", nil)
		rec := httptest.NewRecorder()
		h(rec, req)

		require.Regexp(t, reTimestamp, buf.String())
	})

	t.Run("nil_out_panics", func(t *testing.T) {
		t.Parallel()

		next := func(_ http.ResponseWriter, _ *http.Request) {}
		require.PanicsWithValue(t, "httptap: out is nil", func() {
			NewAccessHandler(nil, next)
		})
	})

	t.Run("nil_next_panics", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		require.PanicsWithValue(t, "httptap: next is nil", func() {
			NewAccessHandler(&buf, nil)
		})
	})

	t.Run("flusher_exposed", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		flushed := false
		fw := &fakeFlusherWriter{
			ResponseRecorder: httptest.NewRecorder(),
			onFlush:          func() { flushed = true },
		}

		next := func(w http.ResponseWriter, _ *http.Request) {
			f, ok := w.(http.Flusher)
			if !assert.True(t, ok, "handler must see http.Flusher") {
				return
			}
			f.Flush()
		}
		h := NewAccessHandler(&buf, next)

		req := httptest.NewRequest(http.MethodGet, "/flush", nil)
		h(fw, req)

		require.True(t, flushed, "Flush must reach the underlying fake")
	})

	t.Run("flusher_not_exposed", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		pw := &plainResponseWriter{rec: httptest.NewRecorder()}

		var gotFlusher bool
		next := func(w http.ResponseWriter, _ *http.Request) {
			_, gotFlusher = w.(http.Flusher)
		}
		h := NewAccessHandler(&buf, next)

		req := httptest.NewRequest(http.MethodGet, "/plain", nil)
		h(pw, req)

		require.False(t, gotFlusher, "plain writer must NOT be advertised as http.Flusher")
	})

	t.Run("hijacker_exposed_and_logs", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		hw := &fakeHijackerWriter{ResponseRecorder: httptest.NewRecorder()}

		next := func(w http.ResponseWriter, _ *http.Request) {
			hj, ok := w.(http.Hijacker)
			if !assert.True(t, ok, "handler must see http.Hijacker") {
				return
			}
			conn, _, err := hj.Hijack()
			if !assert.NoError(t, err) {
				return
			}
			_ = conn.Close()
			// no WriteHeader or Write — status stays 0
		}
		h := NewAccessHandler(&buf, next)

		req := httptest.NewRequest(http.MethodGet, "/hijack", nil)
		h(hw, req)

		line := buf.String()
		require.NotEmpty(t, line, "log line must be emitted even after hijack")
		require.Contains(t, line, "[000]", "hijacked-without-write must render as [000]")
	})

	t.Run("pusher_exposed", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		pushed := ""
		pw := &fakePusherWriter{
			ResponseRecorder: httptest.NewRecorder(),
			onPush:           func(target string) { pushed = target },
		}

		next := func(w http.ResponseWriter, _ *http.Request) {
			p, ok := w.(http.Pusher)
			if !assert.True(t, ok, "handler must see http.Pusher") {
				return
			}
			assert.NoError(t, p.Push("/resource", nil))
		}
		h := NewAccessHandler(&buf, next)

		req := httptest.NewRequest(http.MethodGet, "/push", nil)
		h(pw, req)

		require.Equal(t, "/resource", pushed, "Push must delegate to the underlying fake")
	})

	t.Run("all_three_exposed", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		combo := &fakeComboAllThree{
			rec:     httptest.NewRecorder(),
			onFlush: func() {},
			onPush:  func(string) {},
		}

		var gotFlusher, gotHijacker, gotPusher bool
		next := func(w http.ResponseWriter, _ *http.Request) {
			_, gotFlusher = w.(http.Flusher)
			_, gotHijacker = w.(http.Hijacker)
			_, gotPusher = w.(http.Pusher)
		}
		h := NewAccessHandler(&buf, next)

		req := httptest.NewRequest(http.MethodGet, "/combo", nil)
		h(combo, req)

		require.True(t, gotFlusher, "all_three must expose http.Flusher")
		require.True(t, gotHijacker, "all_three must expose http.Hijacker")
		require.True(t, gotPusher, "all_three must expose http.Pusher")
	})

	t.Run("panic_in_next_still_logs", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		next := func(_ http.ResponseWriter, _ *http.Request) {
			panic("intentional test panic")
		}
		h := NewAccessHandler(&buf, next)

		req := httptest.NewRequest(http.MethodGet, "/panic", nil)
		rec := httptest.NewRecorder()

		require.Panics(t, func() { h(rec, req) }, "panic must propagate to caller")
		require.NotEmpty(t, buf.String(), "log line must be emitted despite panic")
	})

	t.Run("concurrent_writes_ForRaceCondition", func(t *testing.T) {
		t.Parallel()

		const N = 200
		lw := &lockedWriter{w: io.Discard}

		next := func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}
		h := NewAccessHandler(lw, next)

		var wg sync.WaitGroup
		wg.Add(N)
		for range N {
			go func() {
				defer wg.Done()
				req := httptest.NewRequest(http.MethodGet, "/race", nil)
				rec := httptest.NewRecorder()
				h(rec, req)
			}()
		}
		wg.Wait()
	})
}

// BenchmarkNewAccessHandler measures per-request allocations on the steady-state
// path (plain http.ResponseWriter, no optional interfaces, io.Discard output).
// Budget: <= 5 allocs/op.
func BenchmarkNewAccessHandler(b *testing.B) {
	next := func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}
	h := NewAccessHandler(io.Discard, next)

	req := httptest.NewRequest(http.MethodGet, "/bench", nil)
	// plainResponseWriter has no optional interfaces, keeping the fast path (no composite wrapping).
	pw := &plainResponseWriter{rec: httptest.NewRecorder()}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		h(pw, req)
	}
}
