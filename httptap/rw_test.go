package httptap

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	loginjector "github.com/prorochestvo/loginjector"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// compile-time checks that each concrete wrapper satisfies its claimed interfaces.
var _ http.ResponseWriter = &flusherRW{}
var _ http.Flusher = &flusherRW{}
var _ http.ResponseWriter = &hijackerRW{}
var _ http.Hijacker = &hijackerRW{}
var _ http.ResponseWriter = &pusherRW{}
var _ http.Pusher = &pusherRW{}
var _ http.ResponseWriter = &flusherHijackerRW{}
var _ http.Flusher = &flusherHijackerRW{}
var _ http.Hijacker = &flusherHijackerRW{}
var _ http.ResponseWriter = &flusherPusherRW{}
var _ http.Flusher = &flusherPusherRW{}
var _ http.Pusher = &flusherPusherRW{}
var _ http.ResponseWriter = &hijackerPusherRW{}
var _ http.Hijacker = &hijackerPusherRW{}
var _ http.Pusher = &hijackerPusherRW{}
var _ http.ResponseWriter = &allThreeRW{}
var _ http.Flusher = &allThreeRW{}
var _ http.Hijacker = &allThreeRW{}
var _ http.Pusher = &allThreeRW{}

// TestWrapInterceptor covers the dynamic interface pass-through.
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

	// runtime verification that wrapWithOptionalInterfaces selects the correct composite
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
}
