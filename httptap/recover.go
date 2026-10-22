package httptap

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"strings"

	loginjector "github.com/prorochestvo/loginjector"
)

// NewRecoverHandler returns an HTTP middleware that recovers any panic in next,
// logs the panic with a stack trace via logger at level, and responds with HTTP
// 500 plus a JSON body.
//
// Behaviour:
//   - On a clean return from next, the buffered response is flushed to the
//     underlying ResponseWriter as if no middleware were present.
//   - If next calls Flush, the buffered response (headers and any body written
//     so far) is committed to the wire immediately. Subsequent writes forward
//     through to the real writer. A panic after Flush is logged and re-panicked
//     as http.ErrAbortHandler, which aborts the connection in the outer server.
//   - On a panic before Flush (or before any write), the buffered response is
//     discarded and a fresh 500 response is written with Content-Type
//     "application/json; charset=utf-8" and body {"error":"something went wrong"}.
//     Both are overridable via options.
//   - http.ErrAbortHandler is re-panicked silently, matching net/http's own
//     server recovery. No log line, no 500 body.
//   - If next hijacks the connection before panicking, the panic is logged but
//     no response can be written — the caller owns the conn.
//
// The handler buffers the entire response body in memory before Flush is called
// or the handler returns. This is necessary so a panic AFTER a WriteHeader call
// can be replaced with a 500 response. SSE, WebSocket, and HTTP/2-push handlers
// that call Flush are supported: Flush commits the response, and subsequent
// streaming writes forward through. A panic after Flush is logged and aborts
// the connection.
//
// The stack trace logged uses loginjector.NewStackTraceError(); it contains
// absolute build-host file paths and MUST NOT be returned to clients. Only the
// configured fallback body reaches the client.
//
// A nil logger or next is a programmer error and panics.
func NewRecoverHandler(
	logger *loginjector.Logger,
	level loginjector.LogLevel,
	next http.HandlerFunc,
	opts ...RecoverOption,
) http.HandlerFunc {
	if logger == nil {
		panic("httptap: logger is nil")
	}
	if next == nil {
		panic("httptap: next is nil")
	}

	cfg := defaultRecoverConfig()
	for _, o := range opts {
		o(&cfg)
	}

	return func(w http.ResponseWriter, r *http.Request) {
		rrw := &recoverResponseWriter{ResponseWriter: w}
		wrapped := wrapWithOptionalInterfaces(rrw, w)

		defer func() {
			pv := recover()
			if pv != nil {
				//nolint:errorlint // recover value compared by identity, matching net/http's ErrAbortHandler handling
				if pv == http.ErrAbortHandler {
					panic(pv) // re-panic silently per stdlib convention.
				}
				ste := loginjector.NewStackTraceError()
				joined := errors.Join(
					fmt.Errorf("panic: %v", pv),
					ste,
					loginjector.NewHttpError(http.StatusInternalServerError),
					loginjector.NewPublicError(cfg.publicMessage, nil),
				)
				logMsg := joined.Error() + " " + ste.Runtime() + "\n" + ste.Stack()
				if _, err := logger.WriteLog(level, []byte(logMsg)); err != nil {
					println(err.Error()) // matches existing httptap convention.
				}
				if rrw.hijacked {
					return
				}
				if rrw.committed {
					// panic after commit: connection is already active on the wire,
					// re-panic with ErrAbortHandler to abort the connection.
					panic(http.ErrAbortHandler)
				}
				// panic before commit: buffer absorbed everything; the wire is still
				// clean, so we can write the 500 response directly to the real ResponseWriter.
				h := w.Header()
				h.Set("Content-Type", cfg.contentType)
				h.Set("X-Content-Type-Options", "nosniff")
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write(cfg.fallbackBody)
				return
			}
			// clean path: flush buffered response to the real writer.
			if rrw.hijacked {
				return
			}
			if rrw.committed {
				// already flushed by beforeFlush; nothing more to do.
				return
			}
			if err := rrw.flushTo(w); err != nil {
				println(err.Error())
			}
		}()

		next(wrapped, r)
	}
}

// WithFallbackResponse overrides the response body written on panic recovery.
// The body is sent verbatim. Default is `{"error":"something went wrong"}`.
// Pair with WithFallbackContentType if the body is not JSON.
func WithFallbackResponse(body []byte) RecoverOption {
	return func(c *recoverConfig) { c.fallbackBody = body }
}

// WithFallbackContentType overrides the Content-Type header set on the panic
// response. Default is "application/json; charset=utf-8".
func WithFallbackContentType(ct string) RecoverOption {
	return func(c *recoverConfig) { c.contentType = ct }
}

// WithPublicMessage overrides the message embedded in the PublicError that is
// joined into the logged error chain. It does NOT change the response body —
// use WithFallbackResponse for that. Default is "something went wrong".
func WithPublicMessage(msg string) RecoverOption {
	return func(c *recoverConfig) { c.publicMessage = msg }
}

// recoverConfig holds the tunables for NewRecoverHandler.
type recoverConfig struct {
	publicMessage string
	contentType   string
	fallbackBody  []byte
}

// defaultRecoverConfig returns the zero-options baseline config.
func defaultRecoverConfig() recoverConfig {
	return recoverConfig{
		publicMessage: "something went wrong",
		contentType:   "application/json; charset=utf-8",
		fallbackBody:  []byte(`{"error":"something went wrong"}`),
	}
}

// RecoverOption configures NewRecoverHandler.
type RecoverOption func(*recoverConfig)

// recoverResponseWriter buffers headers, status, and body so a panic after
// WriteHeader can still be replaced with a 500 response. Nothing is forwarded
// to the underlying http.ResponseWriter until flushTo is called on the clean
// path, or until the recover middleware writes the 500 directly on the panic path.
//
// The snapshot field holds a clone of the buffered headers at the moment a final
// status is committed, capturing header deletions and preserving the exact headers
// that were in effect when WriteHeader was called.
//
// The committed field tracks whether beforeFlush has been called and the buffered
// response has been flushed to the real writer. After commit, all writes forward
// to the real writer.
type recoverResponseWriter struct {
	http.ResponseWriter
	headers     http.Header
	snapshot    http.Header
	body        bytes.Buffer
	statusCode  int
	wroteHeader bool
	committed   bool
	hijacked    bool
}

// Header returns the buffered header map, lazily cloning the underlying writer's
// headers on first call so that mutations are isolated to the buffer.
func (rrw *recoverResponseWriter) Header() http.Header {
	if rrw.headers == nil {
		rrw.headers = rrw.ResponseWriter.Header().Clone()
	}
	return rrw.headers
}

// WriteHeader records the status code in the buffer. Informational responses
// (100-199 except 101) are forwarded immediately without recording. The first
// final status is recorded and a snapshot of the buffered headers is taken.
// After commit, subsequent calls are forwarded to the real writer (which reports
// the superfluous call). Before commit, they are silent no-ops.
func (rrw *recoverResponseWriter) WriteHeader(code int) {
	if isInformational(code) {
		// forward 1xx immediately: save the real header map, apply buffered headers,
		// call WriteHeader, then restore.
		orig := rrw.ResponseWriter.Header().Clone()
		if rrw.headers != nil {
			replaceHeader(rrw.ResponseWriter.Header(), rrw.headers)
		}
		rrw.ResponseWriter.WriteHeader(code)
		replaceHeader(rrw.ResponseWriter.Header(), orig)
		return
	}
	if rrw.wroteHeader {
		// already have a final status recorded.
		if rrw.committed {
			// after commit: forward to real writer (it will report the superfluous call).
			rrw.ResponseWriter.WriteHeader(code)
		}
		// before commit: silent no-op
		return
	}
	// first final status: record it and snapshot the headers.
	rrw.statusCode = code
	rrw.wroteHeader = true
	// snapshot the buffered headers at the moment the final status is committed.
	if rrw.headers != nil {
		rrw.snapshot = rrw.headers.Clone()
	} else {
		rrw.snapshot = make(http.Header)
	}
}

// replaceHeader clears dst and copies all headers from src to dst.
func replaceHeader(dst, src http.Header) {
	clear(dst)
	for k, v := range src {
		// make a copy of the slice so the caller's mutations don't affect us.
		dst[k] = append([]string(nil), v...)
	}
}

// copyTrailers copies trailer headers from the live buffered map to the real map.
// Trailer keys are those declared in snapshot["Trailer"] (comma-separated, trimmed,
// canonicalised) and those with http.TrailerPrefix in the live buffered headers.
func copyTrailers(realHeader, live, snapshot http.Header) {
	if snapshot == nil {
		return
	}
	// Get the declared trailer names from the snapshot
	declared := snapshot.Get("Trailer")
	trailerMap := make(map[string]struct{})
	if declared != "" {
		for _, name := range strings.Split(declared, ",") {
			trailerMap[http.CanonicalHeaderKey(strings.TrimSpace(name))] = struct{}{}
		}
	}

	// Copy trailers from the live map to the real map
	for k, v := range live {
		key := strings.TrimSpace(k)
		if strings.HasPrefix(key, http.TrailerPrefix) {
			// Remove the prefix and copy to real
			trailerKey := strings.TrimPrefix(key, http.TrailerPrefix)
			realHeader[trailerKey] = append([]string(nil), v...)
		} else if _, isDeclared := trailerMap[http.CanonicalHeaderKey(key)]; isDeclared {
			// Declared trailer, copy to real
			realHeader[key] = append([]string(nil), v...)
		}
	}
}

// Write appends b to the in-memory body buffer, or forwards to the real writer
// if already committed. Implicitly calls WriteHeader(200) if WriteHeader has not
// been called yet, matching the stdlib contract.
func (rrw *recoverResponseWriter) Write(b []byte) (int, error) {
	if !rrw.wroteHeader {
		rrw.WriteHeader(http.StatusOK)
	}
	if rrw.committed {
		// after commit: forward to real writer.
		return rrw.ResponseWriter.Write(b)
	}
	// before commit: buffer the write.
	return rrw.body.Write(b)
}

// markHijacked satisfies tapWriter so wrapWithOptionalInterfaces can set the
// hijacked flag when a downstream handler calls Hijack.
func (rrw *recoverResponseWriter) markHijacked() { rrw.hijacked = true }

// beforeFlush satisfies tapWriter. It commits the buffered response when not
// already hijacked or committed, then resets the buffer so subsequent writes
// forward to the real writer.
func (rrw *recoverResponseWriter) beforeFlush() {
	if rrw.hijacked || rrw.committed {
		return
	}
	// commit the buffered response
	if rrw.wroteHeader {
		// final status was recorded: write headers, status, and body to the real writer.
		if rrw.snapshot != nil {
			replaceHeader(rrw.ResponseWriter.Header(), rrw.snapshot)
		}
		rrw.ResponseWriter.WriteHeader(rrw.statusCode)
	} else if rrw.headers != nil {
		// no final status was recorded: apply buffered headers.
		// The real writer will send an implicit 200 when the body write happens.
		dst := rrw.ResponseWriter.Header()
		for k, v := range rrw.headers {
			// make a copy of the slice so mutations don't affect us.
			dst[k] = append([]string(nil), v...)
		}
		// record the implicit 200 as if it were committed.
		rrw.statusCode = http.StatusOK
		rrw.wroteHeader = true
		rrw.snapshot = rrw.headers.Clone()
	}
	// write the buffered body to the real writer.
	_, _ = rrw.ResponseWriter.Write(rrw.body.Bytes())
	// copy trailers that were set after the body write.
	copyTrailers(rrw.ResponseWriter.Header(), rrw.headers, rrw.snapshot)
	// reset the body buffer and mark as committed.
	rrw.body.Reset()
	rrw.committed = true
}

// flushTo copies the buffered headers, status, and body to the real writer on
// the clean (no-panic) path. If the response is already committed (flushed via
// beforeFlush), this is a no-op. If next never called WriteHeader, no explicit
// status is written here: net/http issues an implicit 200 when the body write
// triggers the first header flush, faithfully reproducing the handler's
// observable behaviour.
func (rrw *recoverResponseWriter) flushTo(w http.ResponseWriter) error {
	if rrw.committed {
		// already flushed; nothing to do.
		return nil
	}
	if rrw.wroteHeader && rrw.snapshot != nil {
		// final status was recorded: use the snapshot (which includes deletions).
		replaceHeader(w.Header(), rrw.snapshot)
		w.WriteHeader(rrw.statusCode)
	} else if rrw.headers != nil {
		// no final status was recorded: apply buffered headers as-is.
		dst := w.Header()
		for k, v := range rrw.headers {
			// make a copy of the slice so the caller's mutations don't affect us.
			dst[k] = append([]string(nil), v...)
		}
	}
	// write the buffered body.
	_, err := w.Write(rrw.body.Bytes())
	// copy trailers that were set after the body write.
	copyTrailers(w.Header(), rrw.headers, rrw.snapshot)
	return err
}

var _ http.ResponseWriter = (*recoverResponseWriter)(nil)
var _ tapWriter = (*recoverResponseWriter)(nil)
