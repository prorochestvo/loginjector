package httptap

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prorochestvo/loginjector/internal/httpx"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/prorochestvo/loginjector"
)

var _ http.ResponseWriter = (*interceptor)(nil)
var _ httpx.Tap = (*interceptor)(nil)

// TestBuildBodySnippet covers buildBodySnippet including the exact-boundary edge cases.
func TestBuildBodySnippet(t *testing.T) {
	t.Parallel()

	t.Run("zero_returns_nil", func(t *testing.T) {
		t.Parallel()
		buf := bytes.NewBufferString("some content")
		result := buildBodySnippet(buf, 0, false)
		require.Nil(t, result, "maxBytes==0 must return nil")
	})

	t.Run("negative_returns_full", func(t *testing.T) {
		t.Parallel()
		content := "hello world"
		buf := bytes.NewBufferString(content)
		result := buildBodySnippet(buf, -1, false)
		require.Equal(t, content, string(result))
	})

	t.Run("body_exactly_at_cap_no_truncation_marker", func(t *testing.T) {
		t.Parallel()
		// a body of exactly N bytes captured into a cappedWriter of limit N must NOT
		// produce a truncation marker — truncated=false means nothing was dropped.
		const N = 10
		content := strings.Repeat("a", N)
		buf := bytes.NewBufferString(content)
		result := buildBodySnippet(buf, N, false)
		require.NotContains(t, string(result), "…[truncated]",
			"body of exactly cap bytes must NOT produce truncation marker")
		require.Equal(t, content, string(result))
	})

	t.Run("body_one_over_cap_has_truncation_marker", func(t *testing.T) {
		t.Parallel()
		// simulate cappedWriter having buffered N bytes from a body of N+1 (truncated=true).
		const N = 10
		buf := bytes.NewBufferString(strings.Repeat("b", N))
		result := buildBodySnippet(buf, N, true)
		require.Contains(t, string(result), "…[truncated]",
			"body exceeding cap must contain truncation marker")
	})

	t.Run("truncation_marker_via_handler_boundary", func(t *testing.T) {
		t.Parallel()
		// integration: send exactly N bytes → no marker; send N+1 bytes → marker.
		const N = 15
		b1 := bytes.NewBufferString("")
		l1 := loginjector.NewLogger(logLevelInfo, b1)
		h1 := NewPayloadHandlerWithOptions(l1, logLevelInfo, func(w http.ResponseWriter, r *http.Request) {
			defer func() { _ = r.Body.Close() }()
			_, _ = io.ReadAll(r.Body)
			_, _ = w.Write([]byte("ok"))
		}, WithMaxRequestBody(N), WithSummaryWriter(io.Discard))

		req1 := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(strings.Repeat("x", N)))
		h1(httptest.NewRecorder(), req1)
		require.NotContains(t, b1.String(), "…[truncated]",
			"body of exactly %d bytes must NOT produce truncation marker", N)

		b2 := bytes.NewBufferString("")
		l2 := loginjector.NewLogger(logLevelInfo, b2)
		h2 := NewPayloadHandlerWithOptions(l2, logLevelInfo, func(w http.ResponseWriter, r *http.Request) {
			defer func() { _ = r.Body.Close() }()
			_, _ = io.ReadAll(r.Body)
			_, _ = w.Write([]byte("ok"))
		}, WithMaxRequestBody(N), WithSummaryWriter(io.Discard))

		req2 := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(strings.Repeat("x", N+1)))
		h2(httptest.NewRecorder(), req2)
		require.Contains(t, b2.String(), "…[truncated]",
			"body of %d bytes (one over cap) must produce truncation marker", N+1)
	})
}

func TestWrapInterceptor(t *testing.T) {
	t.Parallel()

	t.Run("flusher_passthrough", func(t *testing.T) {
		t.Parallel()

		flushed := false
		fw := &fakeFlusherWriter{ResponseRecorder: httptest.NewRecorder(), onFlush: func() { flushed = true }}

		req := httptest.NewRequest(http.MethodGet, "/", bytes.NewBufferString(""))
		wrapped, _, _ := newInterceptorWithConfig(fw, req, defaultPayloadConfig())

		f, ok := wrapped.(http.Flusher)
		require.True(t, ok, "wrapped writer must be http.Flusher when underlying is")
		f.Flush()
		require.True(t, flushed, "Flush must delegate to the underlying writer")
	})

	t.Run("plain_writer_not_advertised_as_flusher", func(t *testing.T) {
		t.Parallel()

		plain := httptest.NewRecorder() // httptest.ResponseRecorder IS a Flusher; use a plain writer
		req := httptest.NewRequest(http.MethodGet, "/", bytes.NewBufferString(""))
		wrapped, _, _ := newInterceptorWithConfig(&plainResponseWriter{plain}, req, defaultPayloadConfig())

		_, ok := wrapped.(http.Flusher)
		require.False(t, ok, "plain writer must NOT be advertised as http.Flusher")
	})

	t.Run("hijacker_passthrough", func(t *testing.T) {
		t.Parallel()

		hw := &fakeHijackerWriter{ResponseRecorder: httptest.NewRecorder()}
		req := httptest.NewRequest(http.MethodGet, "/", bytes.NewBufferString(""))
		wrapped, _, _ := newInterceptorWithConfig(hw, req, defaultPayloadConfig())

		h, ok := wrapped.(http.Hijacker)
		require.True(t, ok, "wrapped writer must be http.Hijacker when underlying is")

		conn, rw, err := h.Hijack()
		require.NoError(t, err)
		require.NotNil(t, conn)
		require.NotNil(t, rw)
	})

	t.Run("plain_writer_not_advertised_as_hijacker", func(t *testing.T) {
		t.Parallel()

		plain := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", bytes.NewBufferString(""))
		wrapped, _, _ := newInterceptorWithConfig(&plainResponseWriter{plain}, req, defaultPayloadConfig())

		_, ok := wrapped.(http.Hijacker)
		require.False(t, ok, "plain writer must NOT be advertised as http.Hijacker")
	})

	t.Run("pusher_passthrough", func(t *testing.T) {
		t.Parallel()

		pushed := ""
		pw := &fakePusherWriter{ResponseRecorder: httptest.NewRecorder(), onPush: func(target string) { pushed = target }}
		req := httptest.NewRequest(http.MethodGet, "/", bytes.NewBufferString(""))
		wrapped, _, _ := newInterceptorWithConfig(pw, req, defaultPayloadConfig())

		p, ok := wrapped.(http.Pusher)
		require.True(t, ok, "wrapped writer must be http.Pusher when underlying is")
		err := p.Push("/pushed", nil)
		require.NoError(t, err)
		require.Equal(t, "/pushed", pushed)
	})

	t.Run("unsupported_writer_reports_not_supported", func(t *testing.T) {
		t.Parallel()

		// a bare plainResponseWriter implements none of the optional interfaces.
		plain := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", bytes.NewBufferString(""))
		wrapped, _, _ := newInterceptorWithConfig(&plainResponseWriter{plain}, req, defaultPayloadConfig())

		_, isFlusher := wrapped.(http.Flusher)
		_, isHijacker := wrapped.(http.Hijacker)
		_, isPusher := wrapped.(http.Pusher)

		require.False(t, isFlusher, "plain writer must not be http.Flusher")
		require.False(t, isHijacker, "plain writer must not be http.Hijacker")
		require.False(t, isPusher, "plain writer must not be http.Pusher")
	})

	t.Run("flusher_does_not_change_log_output", func(t *testing.T) {
		// wrapping a Flusher-capable writer must produce the same log as wrapping a plain one.
		t.Parallel()

		const reqBody = "flush-body"
		const resBody = "flush-response"

		handler := func(w http.ResponseWriter, r *http.Request) {
			defer func() { _ = r.Body.Close() }()
			_, _ = io.ReadAll(r.Body)
			_, _ = w.Write([]byte(resBody))
		}

		bPlain := bytes.NewBufferString("")
		lPlain := loginjector.NewLogger(logLevelInfo, bPlain)
		hPlain := NewPayloadHandlerWithOptions(lPlain, logLevelInfo, handler, WithSummaryWriter(io.Discard))

		bFlusher := bytes.NewBufferString("")
		lFlusher := loginjector.NewLogger(logLevelInfo, bFlusher)
		hFlusher := NewPayloadHandlerWithOptions(lFlusher, logLevelInfo, handler, WithSummaryWriter(io.Discard))

		plainReq := httptest.NewRequest(http.MethodPost, "/log-check", bytes.NewBufferString(reqBody))
		plainReq.Header.Set("Content-Type", "text/plain")
		hPlain(httptest.NewRecorder(), plainReq)

		flusherReq := httptest.NewRequest(http.MethodPost, "/log-check", bytes.NewBufferString(reqBody))
		flusherReq.Header.Set("Content-Type", "text/plain")
		// use a Flusher-backed recorder.
		hFlusher(&fakeFlusherWriter{ResponseRecorder: httptest.NewRecorder(), onFlush: func() {}}, flusherReq)

		// both logs must contain the same key tokens.
		for _, token := range []string{reqBody, resBody, "/log-check", "200"} {
			assert.Contains(t, bPlain.String(), token)
			assert.Contains(t, bFlusher.String(), token)
		}
	})

	// runtime verification that WrapWithOptionalInterfaces selects the correct composite
	// type for every multi-interface combination.

	t.Run("flusher_hijacker_combo", func(t *testing.T) {
		t.Parallel()

		flushed := false
		fw := &fakeComboFlusherHijacker{
			rec:     httptest.NewRecorder(),
			onFlush: func() { flushed = true },
		}
		req := httptest.NewRequest(http.MethodGet, "/", bytes.NewBufferString(""))
		wrapped, _, _ := newInterceptorWithConfig(fw, req, defaultPayloadConfig())

		f, hasFlusher := wrapped.(http.Flusher)
		_, hasHijacker := wrapped.(http.Hijacker)
		_, hasPusher := wrapped.(http.Pusher)

		require.True(t, hasFlusher, "flusher+hijacker combo must expose Flusher")
		require.True(t, hasHijacker, "flusher+hijacker combo must expose Hijacker")
		require.False(t, hasPusher, "flusher+hijacker combo must NOT expose Pusher")

		f.Flush()
		require.True(t, flushed, "Flush must delegate to the underlying fake")
	})

	t.Run("flusher_pusher_combo", func(t *testing.T) {
		t.Parallel()

		pushed := ""
		fw := &fakeComboFlusherPusher{
			rec:    httptest.NewRecorder(),
			onPush: func(target string) { pushed = target },
		}
		req := httptest.NewRequest(http.MethodGet, "/", bytes.NewBufferString(""))
		wrapped, _, _ := newInterceptorWithConfig(fw, req, defaultPayloadConfig())

		_, hasFlusher := wrapped.(http.Flusher)
		_, hasHijacker := wrapped.(http.Hijacker)
		p, hasPusher := wrapped.(http.Pusher)

		require.True(t, hasFlusher, "flusher+pusher combo must expose Flusher")
		require.False(t, hasHijacker, "flusher+pusher combo must NOT expose Hijacker")
		require.True(t, hasPusher, "flusher+pusher combo must expose Pusher")

		require.NoError(t, p.Push("/pushed", nil))
		require.Equal(t, "/pushed", pushed, "Push must delegate to the underlying fake")
	})

	t.Run("hijacker_pusher_combo", func(t *testing.T) {
		t.Parallel()

		pushed := ""
		fw := &fakeComboHijackerPusher{
			rec:    httptest.NewRecorder(),
			onPush: func(target string) { pushed = target },
		}
		req := httptest.NewRequest(http.MethodGet, "/", bytes.NewBufferString(""))
		wrapped, _, _ := newInterceptorWithConfig(fw, req, defaultPayloadConfig())

		_, hasFlusher := wrapped.(http.Flusher)
		_, hasHijacker := wrapped.(http.Hijacker)
		p, hasPusher := wrapped.(http.Pusher)

		require.False(t, hasFlusher, "hijacker+pusher combo must NOT expose Flusher")
		require.True(t, hasHijacker, "hijacker+pusher combo must expose Hijacker")
		require.True(t, hasPusher, "hijacker+pusher combo must expose Pusher")

		require.NoError(t, p.Push("/hp-pushed", nil))
		require.Equal(t, "/hp-pushed", pushed, "Push must delegate to the underlying fake")
	})

	t.Run("all_three_combo", func(t *testing.T) {
		t.Parallel()

		flushed := false
		pushed := ""
		fw := &fakeComboAllThree{
			rec:     httptest.NewRecorder(),
			onFlush: func() { flushed = true },
			onPush:  func(target string) { pushed = target },
		}
		req := httptest.NewRequest(http.MethodGet, "/", bytes.NewBufferString(""))
		wrapped, _, _ := newInterceptorWithConfig(fw, req, defaultPayloadConfig())

		f, hasFlusher := wrapped.(http.Flusher)
		_, hasHijacker := wrapped.(http.Hijacker)
		p, hasPusher := wrapped.(http.Pusher)

		require.True(t, hasFlusher, "all-three combo must expose Flusher")
		require.True(t, hasHijacker, "all-three combo must expose Hijacker")
		require.True(t, hasPusher, "all-three combo must expose Pusher")

		f.Flush()
		require.True(t, flushed, "Flush must delegate to the underlying fake")
		require.NoError(t, p.Push("/all-pushed", nil))
		require.Equal(t, "/all-pushed", pushed, "Push must delegate to the underlying fake")
	})

	t.Run("hijack_skips_trailing_flush", func(t *testing.T) {
		t.Parallel()

		// a handler that hijacks the connection. after hijacking, the middleware must NOT
		// call Flush() on the original ResponseWriter — doing so would be a use-after-hijack.
		flushCalled := false
		fw := &fakeComboFlusherHijacker{
			rec:     httptest.NewRecorder(),
			onFlush: func() { flushCalled = true },
		}

		b := bytes.NewBufferString("")
		l := loginjector.NewLogger(logLevelInfo, b)

		h := NewPayloadHandlerWithOptions(l, logLevelInfo, func(w http.ResponseWriter, r *http.Request) {
			defer func() { _ = r.Body.Close() }()
			hj, ok := w.(http.Hijacker)
			if !assert.True(t, ok, "handler must see Hijacker") {
				return
			}
			conn, _, err := hj.Hijack()
			if !assert.NoError(t, err) {
				return
			}
			assert.NotNil(t, conn)
			_ = conn.Close()
		}, WithSummaryWriter(io.Discard))

		req := httptest.NewRequest(http.MethodGet, "/hijack", bytes.NewBufferString(""))
		require.NotPanics(t, func() { h(fw, req) }, "handler must not panic after hijack")
		require.False(t, flushCalled, "Flush must not be called after hijack")
	})

	t.Run("statusLogWriter_models_1xx_semantics", func(t *testing.T) {
		t.Parallel()

		// statusLogWriter records every WriteHeader call,
		// modeling net/http's 1xx informational handling.
		slw := newStatusLogWriter()
		slw.WriteHeader(http.StatusContinue)
		slw.WriteHeader(http.StatusOK)
		slw.Flush()

		require.Equal(t, []int{http.StatusContinue, http.StatusOK}, slw.statusCodes, "all WriteHeader calls must be recorded")
		require.True(t, slw.flushed, "Flush must be recorded")
	})
}

// logLevelInfo is a package-wide test fixture used across httptap tests.
const logLevelInfo loginjector.LogLevel = 1

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

// fakeComboFlusherHijacker implements http.ResponseWriter + http.Flusher + http.Hijacker.
type fakeComboFlusherHijacker struct {
	rec     *httptest.ResponseRecorder
	onFlush func()
}

func (f *fakeComboFlusherHijacker) Header() http.Header         { return f.rec.Header() }
func (f *fakeComboFlusherHijacker) Write(b []byte) (int, error) { return f.rec.Write(b) }
func (f *fakeComboFlusherHijacker) WriteHeader(code int)        { f.rec.WriteHeader(code) }
func (f *fakeComboFlusherHijacker) Flush()                      { f.onFlush() }
func (f *fakeComboFlusherHijacker) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	server, client := net.Pipe()
	_ = client.Close()
	br := bufio.NewReadWriter(bufio.NewReader(server), bufio.NewWriter(server))
	return server, br, nil
}

var _ http.ResponseWriter = &fakeComboFlusherHijacker{} // compile-time check
var _ http.Flusher = &fakeComboFlusherHijacker{}        // compile-time check
var _ http.Hijacker = &fakeComboFlusherHijacker{}       // compile-time check

// fakeComboFlusherPusher implements http.ResponseWriter + http.Flusher + http.Pusher.
type fakeComboFlusherPusher struct {
	rec    *httptest.ResponseRecorder
	onPush func(string)
}

func (f *fakeComboFlusherPusher) Header() http.Header         { return f.rec.Header() }
func (f *fakeComboFlusherPusher) Write(b []byte) (int, error) { return f.rec.Write(b) }
func (f *fakeComboFlusherPusher) WriteHeader(code int)        { f.rec.WriteHeader(code) }
func (f *fakeComboFlusherPusher) Flush()                      {}
func (f *fakeComboFlusherPusher) Push(target string, _ *http.PushOptions) error {
	f.onPush(target)
	return nil
}

var _ http.ResponseWriter = &fakeComboFlusherPusher{} // compile-time check
var _ http.Flusher = &fakeComboFlusherPusher{}        // compile-time check
var _ http.Pusher = &fakeComboFlusherPusher{}         // compile-time check

// fakeComboHijackerPusher implements http.ResponseWriter + http.Hijacker + http.Pusher.
type fakeComboHijackerPusher struct {
	rec    *httptest.ResponseRecorder
	onPush func(string)
}

func (f *fakeComboHijackerPusher) Header() http.Header         { return f.rec.Header() }
func (f *fakeComboHijackerPusher) Write(b []byte) (int, error) { return f.rec.Write(b) }
func (f *fakeComboHijackerPusher) WriteHeader(code int)        { f.rec.WriteHeader(code) }
func (f *fakeComboHijackerPusher) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	server, client := net.Pipe()
	_ = client.Close()
	br := bufio.NewReadWriter(bufio.NewReader(server), bufio.NewWriter(server))
	return server, br, nil
}
func (f *fakeComboHijackerPusher) Push(target string, _ *http.PushOptions) error {
	f.onPush(target)
	return nil
}

var _ http.ResponseWriter = &fakeComboHijackerPusher{} // compile-time check
var _ http.Hijacker = &fakeComboHijackerPusher{}       // compile-time check
var _ http.Pusher = &fakeComboHijackerPusher{}         // compile-time check

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
