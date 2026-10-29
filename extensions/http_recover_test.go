package extensions

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/prorochestvo/loginjector/internal/httpx"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/prorochestvo/loginjector"
)

// compile-time assertions for recoverResponseWriter.
var _ http.ResponseWriter = (*recoverResponseWriter)(nil)
var _ httpx.Tap = (*recoverResponseWriter)(nil)

// TestNewRecoverHandler covers all NewRecoverHandler scenarios.
func TestNewRecoverHandler(t *testing.T) {
	t.Parallel()

	t.Run("nil_logger_panics", func(t *testing.T) {
		t.Parallel()
		require.PanicsWithValue(t, "loginjector: logger is nil", func() {
			NewRecoverHandler(nil, logLevelInfo, func(_ http.ResponseWriter, _ *http.Request) {})
		})
	})

	t.Run("nil_next_panics", func(t *testing.T) {
		t.Parallel()
		l, _ := newRecoverTestLogger(t)
		require.PanicsWithValue(t, "loginjector: next is nil", func() {
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

		// WrapWithOptionalInterfaces forwards Flush to the real underlying writer's Flusher.
		// Calling Flush does NOT flush buffered body bytes — it delegates to the real Flusher.
		require.True(t, flushed, "Flush must be forwarded to the real underlying Flusher")
	})

	t.Run("panic_after_flush_aborts", func(t *testing.T) {
		t.Parallel()
		l, logBuf := newRecoverTestLogger(t)
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

		// panic should be caught, logged, and re-panicked as http.ErrAbortHandler.
		require.PanicsWithValue(t, http.ErrAbortHandler, func() {
			h(fw, httptest.NewRequest(http.MethodGet, "/", nil))
		})

		// the partial-body is on the wire (it was flushed before the panic).
		require.Equal(t, http.StatusOK, fw.Code, "status must be 200 (committed before panic)")
		require.Contains(t, fw.Body.String(), "partial-body", "flushed partial body must be on wire")
		// the panic must be logged.
		require.Contains(t, logBuf.String(), "panic:", "panic must be logged")
		require.Contains(t, logBuf.String(), "flush then panic", "panic message must be logged")
	})

	t.Run("flush_without_header_call_records_implicit_200", func(t *testing.T) {
		t.Parallel()
		l, _ := newRecoverTestLogger(t)
		sw := newStatusLogWriter()

		next := func(w http.ResponseWriter, _ *http.Request) {
			w.(http.Flusher).Flush()
			w.WriteHeader(http.StatusInternalServerError)
		}
		h := NewRecoverHandler(l, logLevelInfo, next)

		h(sw, httptest.NewRequest(http.MethodGet, "/", nil))

		require.Equal(t, []int{http.StatusInternalServerError}, sw.statusCodes,
			"a WriteHeader after the implicit-200 commit must reach the real writer as a superfluous call")
	})

	t.Run("flush_first_applies_header_deletion", func(t *testing.T) {
		t.Parallel()
		l, _ := newRecoverTestLogger(t)
		fw := &fakeFlusherWriter{ResponseRecorder: httptest.NewRecorder(), onFlush: func() {}}
		fw.Header().Set("X-Outer", "1")

		next := func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Del("X-Outer")
			w.(http.Flusher).Flush()
		}
		h := NewRecoverHandler(l, logLevelInfo, next)

		h(fw, httptest.NewRequest(http.MethodGet, "/", nil))

		require.Empty(t, fw.Header().Get("X-Outer"), "a header deleted before the first Flush must not be sent")
	})

	t.Run("clean_path_without_write_applies_header_deletion", func(t *testing.T) {
		t.Parallel()
		l, _ := newRecoverTestLogger(t)
		rec := httptest.NewRecorder()
		rec.Header().Set("X-Outer", "1")

		next := func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Del("X-Outer")
		}
		h := NewRecoverHandler(l, logLevelInfo, next)

		h(rec, httptest.NewRequest(http.MethodGet, "/", nil))

		require.Empty(t, rec.Header().Get("X-Outer"), "a header deleted by a handler that never writes must not be sent")
	})

	t.Run("flush_commits_buffered_status", func(t *testing.T) {
		t.Parallel()
		l, _ := newRecoverTestLogger(t)
		fw := &fakeFlusherWriter{
			ResponseRecorder: httptest.NewRecorder(),
			onFlush:          func() {},
		}

		next := func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte("body"))
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
		h := NewRecoverHandler(l, logLevelInfo, next)

		h(fw, httptest.NewRequest(http.MethodGet, "/", nil))

		require.Equal(t, http.StatusNotFound, fw.Code, "flushed status must be sent")
		require.Equal(t, "body", fw.Body.String(), "flushed body must be sent")
	})

	t.Run("panic_before_flush_still_500", func(t *testing.T) {
		t.Parallel()
		l, logBuf := newRecoverTestLogger(t)
		fw := &fakeFlusherWriter{
			ResponseRecorder: httptest.NewRecorder(),
			onFlush:          func() {},
		}

		next := func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("partial-body"))
			panic("before flush")
		}
		h := NewRecoverHandler(l, logLevelInfo, next)

		h(fw, httptest.NewRequest(http.MethodGet, "/", nil))

		// panic before flush should result in 500 with fallback body.
		require.Equal(t, http.StatusInternalServerError, fw.Code, "status must be 500")
		require.NotContains(t, fw.Body.String(), "partial-body", "partial body must not be sent")
		require.Contains(t, logBuf.String(), "panic:", "panic must be logged")
	})

	t.Run("flush_after_hijack_is_noop", func(t *testing.T) {
		t.Parallel()
		l, _ := newRecoverTestLogger(t)
		fc := &fakeComboFlusherHijacker{
			rec:     httptest.NewRecorder(),
			onFlush: func() {},
		}

		next := func(w http.ResponseWriter, _ *http.Request) {
			hj, ok := w.(http.Hijacker)
			if !ok {
				return
			}
			_, _, _ = hj.Hijack()
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
		h := NewRecoverHandler(l, logLevelInfo, next)

		require.NotPanics(t, func() {
			h(fc, httptest.NewRequest(http.MethodGet, "/", nil))
		}, "Flush after Hijack must not panic")
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

	t.Run("clean_path_applies_header_deletion", func(t *testing.T) {
		t.Parallel()
		l, _ := newRecoverTestLogger(t)
		next := func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Del("X-Outer")
			_, _ = w.Write([]byte("body"))
		}
		h := NewRecoverHandler(l, logLevelInfo, next)

		rec := httptest.NewRecorder()
		rec.Header().Set("X-Outer", "1")
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		h(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		require.Empty(t, rec.Result().Header.Get("X-Outer"), "deleted header must not be present")
	})

	t.Run("clean_path_headers_frozen_at_writeheader", func(t *testing.T) {
		t.Parallel()
		l, _ := newRecoverTestLogger(t)
		next := func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Header().Set("X-Late", "1")
			_, _ = w.Write([]byte("body"))
		}
		h := NewRecoverHandler(l, logLevelInfo, next)

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		h(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		require.Empty(t, rec.Result().Header.Get("X-Late"), "headers set after WriteHeader must not be included")
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

	t.Run("clean_path_declared_trailer_sent", func(t *testing.T) {
		t.Parallel()
		l, _ := newRecoverTestLogger(t)
		next := func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Trailer", "X-Checksum")
			_, _ = w.Write([]byte("body"))
			w.Header().Set("X-Checksum", "abc")
		}
		h := NewRecoverHandler(l, logLevelInfo, next)

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		h(rec, req)

		require.Equal(t, "abc", rec.Result().Trailer.Get("X-Checksum"))
		require.Empty(t, rec.Result().Header.Get("X-Checksum"))
	})

	t.Run("clean_path_prefixed_trailer_sent", func(t *testing.T) {
		t.Parallel()
		l, _ := newRecoverTestLogger(t)
		next := func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("body"))
			// Set a trailer after the body write.
			// The http.TrailerPrefix signals that this should be sent as a trailer.
			w.Header().Set(http.TrailerPrefix+"X-Sum", "s1")
		}
		h := NewRecoverHandler(l, logLevelInfo, next)

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		h(rec, req)

		// The ResponseRecorder's Result() separates headers with TrailerPrefix into the Trailer map
		require.Equal(t, "s1", rec.Result().Trailer.Get("X-Sum"))
	})

	t.Run("informational_without_header_call_keeps_real_headers", func(t *testing.T) {
		t.Parallel()
		l, _ := newRecoverTestLogger(t)
		slw := newStatusLogWriter()

		next := func(w http.ResponseWriter, _ *http.Request) {
			// no Header() call first: the 1xx path clones the real headers itself.
			w.WriteHeader(http.StatusEarlyHints)
			_, _ = w.Write([]byte("body"))
		}
		h := NewRecoverHandler(l, logLevelInfo, next)

		slw.Header().Set("X-Outer", "1")
		h(slw, httptest.NewRequest(http.MethodGet, "/", nil))

		require.Contains(t, slw.statusCodes, http.StatusEarlyHints)
		require.Equal(t, "1", slw.headerSnapshots[0].Get("X-Outer"), "the 1xx must carry the real headers")
		require.Equal(t, "1", slw.finalHeaders.Get("X-Outer"), "the final response must keep the real headers")
	})

	t.Run("informational_forwarded_immediately", func(t *testing.T) {
		t.Parallel()
		l, _ := newRecoverTestLogger(t)
		slw := newStatusLogWriter()

		next := func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Link", "</a.css>; rel=preload")
			w.WriteHeader(http.StatusEarlyHints)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("body"))
		}
		h := NewRecoverHandler(l, logLevelInfo, next)

		h(slw, httptest.NewRequest(http.MethodGet, "/", nil))

		require.Equal(t, []int{http.StatusEarlyHints, http.StatusOK}, slw.statusCodes)
		require.Equal(t, "</a.css>; rel=preload", slw.headerSnapshots[0].Get("Link"))
	})

	t.Run("panic_after_informational_sends_500_without_handler_headers", func(t *testing.T) {
		t.Parallel()
		l, _ := newRecoverTestLogger(t)
		slw := newStatusLogWriter()

		next := func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Length", "999")
			w.WriteHeader(http.StatusEarlyHints)
			panic("boom")
		}
		h := NewRecoverHandler(l, logLevelInfo, next)

		h(slw, httptest.NewRequest(http.MethodGet, "/", nil))

		require.Equal(t, http.StatusInternalServerError, slw.finalStatus)
		require.Empty(t, slw.finalHeaders.Get("Content-Length"))
	})

	t.Run("flush_then_more_writes_stream_through", func(t *testing.T) {
		t.Parallel()
		l, _ := newRecoverTestLogger(t)
		fw := &fakeFlusherWriter{
			ResponseRecorder: httptest.NewRecorder(),
			onFlush:          func() {},
		}

		var aWasWritten bool
		next := func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("a"))
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
				aWasWritten = fw.Body.String() == "a"
			}
			_, _ = w.Write([]byte("b"))
		}
		h := NewRecoverHandler(l, logLevelInfo, next)

		h(fw, httptest.NewRequest(http.MethodGet, "/", nil))

		require.True(t, aWasWritten, "body 'a' must be on wire before next returns")
		require.Equal(t, "ab", fw.Body.String())
	})

	t.Run("flush_first_then_declared_trailer_sent", func(t *testing.T) {
		t.Parallel()
		l, _ := newRecoverTestLogger(t)

		next := func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Trailer", "X-Checksum")
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			_, _ = w.Write([]byte("body"))
			w.Header().Set("X-Checksum", "abc")
		}
		h := NewRecoverHandler(l, logLevelInfo, next)

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		h(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		require.Equal(t, "abc", rec.Result().Trailer.Get("X-Checksum"))
	})

	t.Run("panic_after_write_and_flush_buffered_body_not_on_wire", func(t *testing.T) {
		t.Parallel()
		l, logBuf := newRecoverTestLogger(t)
		fw := &fakeFlusherWriter{
			ResponseRecorder: httptest.NewRecorder(),
			onFlush:          func() {},
		}

		next := func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("partial"))
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			panic("boom")
		}
		h := NewRecoverHandler(l, logLevelInfo, next)

		require.PanicsWithValue(t, http.ErrAbortHandler, func() {
			h(fw, httptest.NewRequest(http.MethodGet, "/", nil))
		})

		require.Equal(t, http.StatusOK, fw.Code)
		require.Equal(t, "partial", fw.Body.String())
		require.Contains(t, logBuf.String(), "panic:")
	})
}

// newRecoverTestLogger creates a Logger backed by a thread-safe buffer for recover tests.
func newRecoverTestLogger(t *testing.T) (*loginjector.Logger, *bytes.Buffer) {
	t.Helper()
	buf := &bytes.Buffer{}
	lw := &lockedWriter{w: buf}
	l := loginjector.NewLogger(logLevelInfo, lw)
	return l, buf
}
