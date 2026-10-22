package httptap

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	loginjector "github.com/prorochestvo/loginjector"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// compile-time assertions for recoverResponseWriter.
var _ http.ResponseWriter = (*recoverResponseWriter)(nil)
var _ tapWriter = (*recoverResponseWriter)(nil)

// newRecoverTestLogger creates a Logger backed by a thread-safe buffer for recover tests.
func newRecoverTestLogger(t *testing.T) (*loginjector.Logger, *bytes.Buffer) {
	t.Helper()
	buf := &bytes.Buffer{}
	lw := &lockedWriter{w: buf}
	l := loginjector.NewLogger(logLevelInfo, lw)
	return l, buf
}

// TestNewRecoverHandler covers all NewRecoverHandler scenarios.
func TestNewRecoverHandler(t *testing.T) {
	t.Parallel()

	t.Run("nil_logger_panics", func(t *testing.T) {
		t.Parallel()
		require.PanicsWithValue(t, "httptap: logger is nil", func() {
			NewRecoverHandler(nil, logLevelInfo, func(_ http.ResponseWriter, _ *http.Request) {})
		})
	})

	t.Run("nil_next_panics", func(t *testing.T) {
		t.Parallel()
		l, _ := newRecoverTestLogger(t)
		require.PanicsWithValue(t, "httptap: next is nil", func() {
			NewRecoverHandler(l, logLevelInfo, nil)
		})
	})

	t.Run("clean_path_implicit_200", func(t *testing.T) {
		t.Parallel()
		l, logBuf := newRecoverTestLogger(t)
		next := func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("ok"))
		}
		h := NewRecoverHandler(l, logLevelInfo, next)

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		h(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		require.Equal(t, "ok", rec.Body.String())
		require.Empty(t, logBuf.String(), "clean path must not emit a log line")
	})

	t.Run("clean_path_explicit_201", func(t *testing.T) {
		t.Parallel()
		l, logBuf := newRecoverTestLogger(t)
		next := func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte("created"))
		}
		h := NewRecoverHandler(l, logLevelInfo, next)

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/", nil)
		h(rec, req)

		require.Equal(t, http.StatusCreated, rec.Code)
		require.Equal(t, "created", rec.Body.String())
		require.Empty(t, logBuf.String())
	})

	t.Run("clean_path_preserves_headers", func(t *testing.T) {
		t.Parallel()
		l, _ := newRecoverTestLogger(t)
		next := func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("X-Custom", "value123")
			_, _ = w.Write([]byte("body"))
		}
		h := NewRecoverHandler(l, logLevelInfo, next)

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		h(rec, req)

		require.Equal(t, "value123", rec.Header().Get("X-Custom"))
		require.Equal(t, "body", rec.Body.String())
	})

	t.Run("panic_before_writeheader", func(t *testing.T) {
		t.Parallel()
		l, logBuf := newRecoverTestLogger(t)
		next := func(_ http.ResponseWriter, _ *http.Request) {
			panic("something exploded")
		}
		h := NewRecoverHandler(l, logLevelInfo, next)

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/boom", nil)
		h(rec, req)

		require.Equal(t, http.StatusInternalServerError, rec.Code)
		//nolint:testifylint // response body bytes are the public contract
		require.Equal(t, `{"error":"something went wrong"}`, rec.Body.String())
		require.Equal(t, "application/json; charset=utf-8", rec.Header().Get("Content-Type"))
		require.Equal(t, "nosniff", rec.Header().Get("X-Content-Type-Options"))

		log := logBuf.String()
		assert.Contains(t, log, "panic:", "log must contain the panic keyword")
		assert.Contains(t, log, "something exploded", "log must contain the panic value")
		assert.Contains(t, log, "os=", "log must contain runtime descriptor")
		assert.Contains(t, log, "Test", "log stack must contain a test function frame")
	})

	t.Run("panic_after_writeheader_200", func(t *testing.T) {
		t.Parallel()
		l, _ := newRecoverTestLogger(t)
		next := func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("partial"))
			panic("late panic")
		}
		h := NewRecoverHandler(l, logLevelInfo, next)

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		h(rec, req)

		// the buffer absorbed the partial write; the wire must get a clean 500.
		require.Equal(t, http.StatusInternalServerError, rec.Code)
		//nolint:testifylint // response body bytes are the public contract
		require.Equal(t, `{"error":"something went wrong"}`, rec.Body.String())
		// "partial" must NOT appear on the wire — this is the load-bearing assertion.
		assert.NotContains(t, rec.Body.String(), "partial",
			"partial write must not reach the wire after a panic")
	})

	t.Run("panic_with_ErrAbortHandler", func(t *testing.T) {
		t.Parallel()
		l, logBuf := newRecoverTestLogger(t)
		next := func(_ http.ResponseWriter, _ *http.Request) {
			panic(http.ErrAbortHandler)
		}
		h := NewRecoverHandler(l, logLevelInfo, next)

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", nil)

		// the handler re-panics ErrAbortHandler; wrap the call to capture it.
		var repanicked interface{}
		func() {
			defer func() { repanicked = recover() }()
			h(rec, req)
		}()

		require.Equal(t, http.ErrAbortHandler, repanicked, "ErrAbortHandler must be re-panicked")
		require.Empty(t, logBuf.String(), "ErrAbortHandler must not produce a log line")
		// no body should have been written by the recover middleware itself.
		require.Empty(t, rec.Body.String(), "ErrAbortHandler must not produce a response body")
	})

	t.Run("panic_after_hijack", func(t *testing.T) {
		t.Parallel()
		l, logBuf := newRecoverTestLogger(t)

		hijackCalled := false
		hw := &fakeHijackerWriter{ResponseRecorder: httptest.NewRecorder()}

		next := func(w http.ResponseWriter, _ *http.Request) {
			hj, ok := w.(http.Hijacker)
			if !assert.True(t, ok, "handler must see Hijacker") {
				return
			}
			_, _, err := hj.Hijack()
			if !assert.NoError(t, err) {
				return
			}
			hijackCalled = true
			panic("panic after hijack")
		}
		h := NewRecoverHandler(l, logLevelInfo, next)

		// wrap the call: the handler must NOT re-panic after hijack.
		require.NotPanics(t, func() { h(hw, httptest.NewRequest(http.MethodGet, "/", nil)) })

		require.True(t, hijackCalled)
		// log line must be emitted.
		require.Contains(t, logBuf.String(), "panic:")
		// the ResponseRecorder must not have received WriteHeader or Write after hijack.
		require.Equal(t, 200, hw.Code, "recorder code must stay at default 200 (not a real 500 from the middleware)")
		require.Empty(t, hw.Body.String(), "body must not be written after hijack")
	})

	t.Run("flusher_passthrough", func(t *testing.T) {
		t.Parallel()
		l, _ := newRecoverTestLogger(t)
		flushed := false
		fw := &fakeFlusherWriter{
			ResponseRecorder: httptest.NewRecorder(),
			onFlush:          func() { flushed = true },
		}

		next := func(w http.ResponseWriter, _ *http.Request) {
			f, ok := w.(http.Flusher)
			if !assert.True(t, ok, "handler must see http.Flusher when underlying writer is Flusher") {
				return
			}
			f.Flush()
		}
		h := NewRecoverHandler(l, logLevelInfo, next)

		h(fw, httptest.NewRequest(http.MethodGet, "/", nil))

		// wrapWithOptionalInterfaces forwards Flush to the real underlying writer's Flusher.
		// Calling Flush does NOT flush buffered body bytes — it delegates to the real Flusher.
		require.True(t, flushed, "Flush must be forwarded to the real underlying Flusher")
	})

	t.Run("panic_after_write_and_flush_buffered_body_not_on_wire", func(t *testing.T) {
		t.Parallel()
		l, _ := newRecoverTestLogger(t)
		fw := &fakeFlusherWriter{
			ResponseRecorder: httptest.NewRecorder(),
			onFlush:          func() {},
		}

		next := func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("partial-body"))
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			panic("flush then panic")
		}
		h := NewRecoverHandler(l, logLevelInfo, next)

		h(fw, httptest.NewRequest(http.MethodGet, "/", nil))

		// the buffered partial-body must not reach the wire; the 500 body replaces it.
		require.Equal(t, http.StatusInternalServerError, fw.Code)
		require.NotContains(t, fw.Body.String(), "partial-body",
			"flushed partial body must not reach the wire after a panic")
		//nolint:testifylint // response body bytes are the public contract
		require.Equal(t, `{"error":"something went wrong"}`, fw.Body.String())
	})

	t.Run("nothing_handler_implicit_200", func(t *testing.T) {
		t.Parallel()
		l, _ := newRecoverTestLogger(t)

		// a handler that does absolutely nothing should produce the same response
		// as if it had been called without the recover wrapper.
		next := func(_ http.ResponseWriter, _ *http.Request) {}
		h := NewRecoverHandler(l, logLevelInfo, next)

		withRecover := httptest.NewRecorder()
		h(withRecover, httptest.NewRequest(http.MethodGet, "/", nil))

		// httptest.ResponseRecorder.Code defaults to 200 and remains 200 when
		// neither Write nor WriteHeader is called — same as net/http's behaviour.
		require.Equal(t, http.StatusOK, withRecover.Code)
		require.Empty(t, withRecover.Body.String())
	})

	t.Run("option_WithFallbackResponse/custom_body_on_panic", func(t *testing.T) {
		t.Parallel()
		l, _ := newRecoverTestLogger(t)
		next := func(_ http.ResponseWriter, _ *http.Request) { panic("oops") }
		h := NewRecoverHandler(l, logLevelInfo, next,
			WithFallbackResponse([]byte("oops")),
		)

		rec := httptest.NewRecorder()
		h(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		require.Equal(t, "oops", rec.Body.String())
	})

	t.Run("option_WithFallbackResponse/empty_body_is_allowed", func(t *testing.T) {
		t.Parallel()
		l, _ := newRecoverTestLogger(t)
		next := func(_ http.ResponseWriter, _ *http.Request) { panic("oops") }
		h := NewRecoverHandler(l, logLevelInfo, next,
			WithFallbackResponse([]byte{}),
		)

		rec := httptest.NewRecorder()
		h(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		require.Empty(t, rec.Body.String())
	})

	t.Run("option_WithFallbackContentType/custom_content_type_on_panic", func(t *testing.T) {
		t.Parallel()
		l, _ := newRecoverTestLogger(t)
		next := func(_ http.ResponseWriter, _ *http.Request) { panic("oops") }
		h := NewRecoverHandler(l, logLevelInfo, next,
			WithFallbackContentType("text/plain; charset=utf-8"),
		)

		rec := httptest.NewRecorder()
		h(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		require.Equal(t, http.StatusInternalServerError, rec.Code)
		require.Equal(t, "text/plain; charset=utf-8", rec.Header().Get("Content-Type"))
	})

	t.Run("option_WithPublicMessage/custom_message_in_log_not_body", func(t *testing.T) {
		t.Parallel()
		l, logBuf := newRecoverTestLogger(t)
		next := func(_ http.ResponseWriter, _ *http.Request) { panic("oops") }
		h := NewRecoverHandler(l, logLevelInfo, next,
			WithPublicMessage("server error"),
		)

		rec := httptest.NewRecorder()
		h(rec, httptest.NewRequest(http.MethodGet, "/", nil))

		// log line must contain the custom public message.
		assert.Contains(t, logBuf.String(), "server error")
		// response body must still be the default (WithPublicMessage does not change the body).
		//nolint:testifylint // response body bytes are the public contract
		assert.Equal(t, `{"error":"something went wrong"}`, rec.Body.String())
	})

	t.Run("hijacker_passthrough", func(t *testing.T) {
		t.Parallel()
		l, _ := newRecoverTestLogger(t)
		hw := &fakeHijackerWriter{ResponseRecorder: httptest.NewRecorder()}

		hijacked := false
		next := func(w http.ResponseWriter, _ *http.Request) {
			hj, ok := w.(http.Hijacker)
			if !assert.True(t, ok, "handler must see http.Hijacker") {
				return
			}
			conn, rw, err := hj.Hijack()
			if !assert.NoError(t, err) {
				return
			}
			assert.NotNil(t, conn)
			assert.NotNil(t, rw)
			hijacked = true
		}
		h := NewRecoverHandler(l, logLevelInfo, next)

		h(hw, httptest.NewRequest(http.MethodGet, "/", nil))
		require.True(t, hijacked)
	})

	t.Run("pusher_passthrough", func(t *testing.T) {
		t.Parallel()
		l, _ := newRecoverTestLogger(t)
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
			assert.NoError(t, p.Push("/pushed-resource", nil))
		}
		h := NewRecoverHandler(l, logLevelInfo, next)

		h(pw, httptest.NewRequest(http.MethodGet, "/", nil))
		require.Equal(t, "/pushed-resource", pushed)
	})

	t.Run("last_option_wins", func(t *testing.T) {
		t.Parallel()
		l, logBuf := newRecoverTestLogger(t)
		next := func(_ http.ResponseWriter, _ *http.Request) { panic("oops") }
		h := NewRecoverHandler(l, logLevelInfo, next,
			WithPublicMessage("a"),
			WithPublicMessage("b"),
		)

		rec := httptest.NewRecorder()
		h(rec, httptest.NewRequest(http.MethodGet, "/", nil))

		assert.Contains(t, logBuf.String(), "b", "last WithPublicMessage must win")
		assert.NotContains(t, logBuf.String(), `"a"`)
	})

	t.Run("concurrent_panics_ForRaceCondition", func(t *testing.T) {
		t.Parallel()
		const total = 100
		const halfPanic = total / 2

		logBuf := &bytes.Buffer{}
		lw := &lockedWriter{w: logBuf}
		l := loginjector.NewLogger(logLevelInfo, lw)

		next := func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/panic" {
				panic("concurrent panic")
			}
			_, _ = w.Write([]byte("ok"))
		}
		h := NewRecoverHandler(l, logLevelInfo, next)

		var wg sync.WaitGroup
		for i := range total {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				var path string
				if i < halfPanic {
					path = "/panic"
				} else {
					path = "/ok"
				}
				rec := httptest.NewRecorder()
				req := httptest.NewRequest(http.MethodGet, path, nil)
				h(rec, req)
			}(i)
		}
		wg.Wait()

		// halfPanic panic log lines must be present; each contains "panic:".
		log := logBuf.String()
		count := strings.Count(log, "panic:")
		assert.Equal(t, halfPanic, count, "exactly halfPanic log lines must be emitted")
	})
}
