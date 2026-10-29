package extensions

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sync"
	"testing"

	"github.com/prorochestvo/loginjector"
	"github.com/prorochestvo/loginjector/httptap"
	"github.com/prorochestvo/loginjector/internal/httpx"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Compile-time assertion for accessResponseWriter.
var _ httpx.Tap = (*accessResponseWriter)(nil)
var _ http.ResponseWriter = (*accessResponseWriter)(nil)

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
		require.PanicsWithValue(t, "loginjector: out is nil", func() {
			NewAccessHandler(nil, next)
		})
	})

	t.Run("nil_next_panics", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		require.PanicsWithValue(t, "loginjector: next is nil", func() {
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

	t.Run("path_newline_does_not_split_line", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		next := func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}
		h := NewAccessHandler(&buf, next)

		// Create a request with a newline in the path (percent-encoded)
		req := httptest.NewRequest(http.MethodGet, "/a%0A2026-01-01T00:00:00Z%20[200]%20GET%20/admin", nil)
		rec := httptest.NewRecorder()
		h(rec, req)

		output := buf.String()
		// Count newlines: should be exactly one (the terminator)
		newlineCount := 0
		for _, c := range output {
			if c == '\n' {
				newlineCount++
			}
		}
		require.Equal(t, 1, newlineCount, "output should contain exactly one newline (the terminator)")
		// The escaped path should be in the output
		require.Contains(t, output, "%0A")
	})

	t.Run("no_write_logs_implicit_200", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		next := func(_ http.ResponseWriter, _ *http.Request) {}
		h := NewAccessHandler(&buf, next)

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()
		h(rec, req)

		require.Contains(t, buf.String(), "[200]")
	})

	t.Run("informational_then_final", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		slw := newStatusLogWriter()

		next := func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusEarlyHints) // Informational
			w.WriteHeader(http.StatusNoContent)  // Final status
		}
		h := NewAccessHandler(&buf, next)

		h(slw, httptest.NewRequest(http.MethodGet, "/", nil))

		require.Contains(t, buf.String(), "[204]")
		require.Equal(t, []int{http.StatusEarlyHints, http.StatusNoContent}, slw.statusCodes)
	})

	t.Run("informational_then_write", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		slw := newStatusLogWriter()

		next := func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusEarlyHints) // Informational
			_, _ = w.Write([]byte("body"))
		}
		h := NewAccessHandler(&buf, next)

		h(slw, httptest.NewRequest(http.MethodGet, "/", nil))

		require.Contains(t, buf.String(), "[200]")
	})

	t.Run("flush_before_writeheader_logs_200", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		rec := httptest.NewRecorder()

		next := func(w http.ResponseWriter, _ *http.Request) {
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			w.WriteHeader(http.StatusNotFound)
		}
		h := NewAccessHandler(&buf, next)

		h(rec, httptest.NewRequest(http.MethodGet, "/", nil))

		require.Contains(t, buf.String(), "[200]")
		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("panic_before_write_logs_000", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		next := func(_ http.ResponseWriter, _ *http.Request) {
			panic("boom")
		}
		h := NewAccessHandler(&buf, next)

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()

		require.Panics(t, func() { h(rec, req) })
		require.Contains(t, buf.String(), "[000]")
	})

	t.Run("access_over_recover_flush_logs_404", func(t *testing.T) {
		t.Parallel()

		var accessBuf bytes.Buffer
		l, _ := newAccessRecoverTestLogger(t)

		next := func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte("not found"))
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}

		recoverH := httptap.NewRecoverHandler(l, logLevelInfo, next)
		accessH := NewAccessHandler(&accessBuf, recoverH)

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		accessH(rec, req)

		require.Contains(t, accessBuf.String(), "[404]")
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

// plainResponseWriter implements only http.ResponseWriter with no optional
// interfaces. It intentionally does NOT embed *httptest.ResponseRecorder (which
// is itself an http.Flusher), so type-asserting the wrapped interceptor to
// http.Flusher returns false.
type plainResponseWriter struct {
	rec *httptest.ResponseRecorder
}

func (p *plainResponseWriter) Header() http.Header         { return p.rec.Header() }
func (p *plainResponseWriter) Write(b []byte) (int, error) { return p.rec.Write(b) }
func (p *plainResponseWriter) WriteHeader(code int)        { p.rec.WriteHeader(code) }

var _ http.ResponseWriter = &plainResponseWriter{} // compile-time check

type fakeFlusherWriter struct {
	*httptest.ResponseRecorder
	onFlush func()
}

func (f *fakeFlusherWriter) Flush() { f.onFlush() }

var _ http.ResponseWriter = &fakeFlusherWriter{} // compile-time check
var _ http.Flusher = &fakeFlusherWriter{}        // compile-time check

type fakeHijackerWriter struct {
	*httptest.ResponseRecorder
}

func (h *fakeHijackerWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	// return a pair of connected net.Conn via net.Pipe for a realistic response.
	server, client := net.Pipe()
	_ = client.Close()
	br := bufio.NewReadWriter(bufio.NewReader(server), bufio.NewWriter(server))
	return server, br, nil
}

var _ http.ResponseWriter = &fakeHijackerWriter{} // compile-time check
var _ http.Hijacker = &fakeHijackerWriter{}       // compile-time check

type fakePusherWriter struct {
	*httptest.ResponseRecorder
	onPush func(string)
}

func (p *fakePusherWriter) Push(target string, _ *http.PushOptions) error {
	p.onPush(target)
	return nil
}

var _ http.ResponseWriter = &fakePusherWriter{} // compile-time check
var _ http.Pusher = &fakePusherWriter{}         // compile-time check

// fakeComboAllThree implements http.ResponseWriter + http.Flusher + http.Hijacker + http.Pusher.
type fakeComboAllThree struct {
	rec     *httptest.ResponseRecorder
	onFlush func()
	onPush  func(string)
}

func (f *fakeComboAllThree) Header() http.Header         { return f.rec.Header() }
func (f *fakeComboAllThree) Write(b []byte) (int, error) { return f.rec.Write(b) }
func (f *fakeComboAllThree) WriteHeader(code int)        { f.rec.WriteHeader(code) }
func (f *fakeComboAllThree) Flush()                      { f.onFlush() }
func (f *fakeComboAllThree) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	server, client := net.Pipe()
	_ = client.Close()
	br := bufio.NewReadWriter(bufio.NewReader(server), bufio.NewWriter(server))
	return server, br, nil
}
func (f *fakeComboAllThree) Push(target string, _ *http.PushOptions) error {
	f.onPush(target)
	return nil
}

var _ http.ResponseWriter = &fakeComboAllThree{} // compile-time check
var _ http.Flusher = &fakeComboAllThree{}        // compile-time check
var _ http.Hijacker = &fakeComboAllThree{}       // compile-time check
var _ http.Pusher = &fakeComboAllThree{}         // compile-time check

// lockedWriter is a thread-safe io.Writer used in the race subtest to prevent
// data races on a shared writer from masking handler-level races.
type lockedWriter struct {
	mu sync.Mutex
	w  interface{ Write([]byte) (int, error) }
}

func (l *lockedWriter) Write(b []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(b)
}

// statusLogWriter is a fake http.ResponseWriter + http.Flusher that records
// every WriteHeader call (including 1xx informational), modeling net/http's
// 1xx semantics (informational codes do not commit a final status).
type statusLogWriter struct {
	*httptest.ResponseRecorder
	statusCodes     []int
	headerSnapshots []http.Header
	finalStatus     int
	finalHeaders    http.Header
	body            []byte
	flushed         bool
}

func newStatusLogWriter() *statusLogWriter {
	return &statusLogWriter{
		ResponseRecorder: httptest.NewRecorder(),
		statusCodes:      make([]int, 0),
		headerSnapshots:  make([]http.Header, 0),
	}
}

func (s *statusLogWriter) WriteHeader(code int) {
	s.statusCodes = append(s.statusCodes, code)
	// Record a header snapshot at each WriteHeader call.
	headerSnapshot := s.Header().Clone()
	s.headerSnapshots = append(s.headerSnapshots, headerSnapshot)
	// Track the final status (the last non-1xx code).
	if code < 100 || code > 199 || code == 101 {
		s.finalStatus = code
		s.finalHeaders = headerSnapshot
	}
	// Don't call the recorder's WriteHeader; it treats all codes as final.
	// Instead, just record the code.
}

func (s *statusLogWriter) Write(b []byte) (int, error) {
	s.body = append(s.body, b...)
	return s.ResponseRecorder.Write(b)
}

func (s *statusLogWriter) Flush() {
	s.flushed = true
}

var _ http.ResponseWriter = &statusLogWriter{}
var _ http.Flusher = &statusLogWriter{}

// newAccessRecoverTestLogger creates a Logger backed by a thread-safe buffer.
func newAccessRecoverTestLogger(t *testing.T) (*loginjector.Logger, *bytes.Buffer) {
	t.Helper()
	buf := &bytes.Buffer{}
	lw := &lockedWriter{w: buf}
	l := loginjector.NewLogger(logLevelInfo, lw)
	return l, buf
}

const logLevelInfo loginjector.LogLevel = 1
