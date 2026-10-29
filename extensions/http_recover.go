package extensions

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/prorochestvo/loginjector/internal/httpx"

	"github.com/prorochestvo/loginjector"
)

// RecoverOption configures NewRecoverHandler.
type RecoverOption func(*recoverConfig)

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
		panic("loginjector: logger is nil")
	}
	if next == nil {
		panic("loginjector: next is nil")
	}

	cfg := defaultRecoverConfig()
	for _, o := range opts {
		o(&cfg)
	}

	return func(w http.ResponseWriter, r *http.Request) {
		rrw := &recoverResponseWriter{ResponseWriter: w}
		wrapped := httpx.WrapWithOptionalInterfaces(rrw, w)

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
					println(err.Error()) // the logger failed; stderr is the last resort.
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
				// already flushed by BeforeFlush; copy any trailers set after the flush.
				realHeader := w.Header()
				copyTrailers(realHeader, rrw.headers, rrw.snapshot)
				return
			}
			if err := rrw.flushTo(w); err != nil {
				println(err.Error())
			}
		}()

		next(wrapped, r)
	}
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

// recoverResponseWriter buffers headers, status, and body so a panic after
// WriteHeader can still be replaced with a 500 response. Nothing is forwarded
// to the underlying http.ResponseWriter until flushTo is called on the clean
// path, or until the recover middleware writes the 500 directly on the panic path.
//
// The snapshot field holds a clone of the buffered headers at the moment a final
// status is committed, capturing header deletions and preserving the exact headers
// that were in effect when WriteHeader was called.
//
// The committed field tracks whether BeforeFlush has been called and the buffered
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
	if httpx.IsInformational(code) {
		rrw.writeInformational(code)
		return
	}
	if rrw.wroteHeader {
		if rrw.committed {
			// the real writer reports the superfluous call.
			rrw.ResponseWriter.WriteHeader(code)
		}
		return
	}
	rrw.statusCode = code
	rrw.wroteHeader = true
	if rrw.headers != nil {
		rrw.snapshot = rrw.headers.Clone()
	} else {
		rrw.snapshot = make(http.Header)
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
		return rrw.ResponseWriter.Write(b)
	}
	return rrw.body.Write(b)
}

// MarkHijacked satisfies httpx.Tap so WrapWithOptionalInterfaces can set the
// hijacked flag when a downstream handler calls Hijack.
func (rrw *recoverResponseWriter) MarkHijacked() { rrw.hijacked = true }

// BeforeFlush satisfies httpx.Tap. It commits the buffered response when not
// already hijacked or committed, then resets the buffer so subsequent writes
// forward to the real writer.
func (rrw *recoverResponseWriter) BeforeFlush() {
	if rrw.hijacked || rrw.committed {
		return
	}
	realHeader := rrw.ResponseWriter.Header()
	if rrw.wroteHeader {
		if rrw.snapshot != nil {
			replaceHeader(realHeader, rrw.snapshot)
		}
		rrw.ResponseWriter.WriteHeader(rrw.statusCode)
	} else {
		// the real writer sends an implicit 200 on the body write, so record that
		// 200 and freeze the headers now.
		h := rrw.Header()
		replaceHeader(realHeader, h)
		rrw.statusCode = http.StatusOK
		rrw.wroteHeader = true
		rrw.snapshot = h.Clone()
	}
	body := rrw.body.Bytes()
	_, _ = rrw.ResponseWriter.Write(body)
	copyTrailers(realHeader, rrw.headers, rrw.snapshot)
	rrw.body.Reset()
	rrw.committed = true
}

// flushTo copies the buffered headers, status, and body to the real writer on
// the clean (no-panic) path. If the response is already committed (flushed via
// BeforeFlush), this is a no-op. If next never called WriteHeader, no explicit
// status is written here: net/http issues an implicit 200 when the body write
// triggers the first header flush, faithfully reproducing the handler's
// observable behaviour.
func (rrw *recoverResponseWriter) flushTo(w http.ResponseWriter) error {
	if rrw.committed {
		return nil
	}
	realHeader := w.Header()
	if rrw.wroteHeader && rrw.snapshot != nil {
		// the snapshot carries the handler's deletions.
		replaceHeader(realHeader, rrw.snapshot)
		w.WriteHeader(rrw.statusCode)
	} else if rrw.headers != nil {
		// no final status was recorded: the buffered map, deletions included, is the
		// header set the handler saw.
		replaceHeader(realHeader, rrw.headers)
	}
	body := rrw.body.Bytes()
	_, err := w.Write(body)
	copyTrailers(realHeader, rrw.headers, rrw.snapshot)
	return err
}

// writeInformational forwards a 1xx response immediately with the handler's
// buffered headers, restoring the real header map afterwards even if the real
// WriteHeader panics.
func (rrw *recoverResponseWriter) writeInformational(code int) {
	realHeader := rrw.ResponseWriter.Header()
	orig := realHeader.Clone()
	defer replaceHeader(realHeader, orig)
	buffered := rrw.Header()
	replaceHeader(realHeader, buffered)
	rrw.ResponseWriter.WriteHeader(code)
}

// copyTrailers copies trailer headers from the live buffered map to the real map.
// Trailer keys are those declared in snapshot["Trailer"] (comma-separated, trimmed,
// canonicalised) and those with http.TrailerPrefix in the live buffered headers.
func copyTrailers(realHeader, live, snapshot http.Header) {
	if snapshot == nil {
		return
	}
	declared := snapshot.Get("Trailer")
	trailerMap := make(map[string]struct{})
	if declared != "" {
		for _, name := range strings.Split(declared, ",") {
			name = strings.TrimSpace(name)
			trailerMap[http.CanonicalHeaderKey(name)] = struct{}{}
		}
	}

	for k, v := range live {
		key := strings.TrimSpace(k)
		if strings.HasPrefix(key, http.TrailerPrefix) {
			// keep the prefix so net/http sends the key as a trailer.
			realHeader[key] = append([]string(nil), v...)
		} else if _, isDeclared := trailerMap[http.CanonicalHeaderKey(key)]; isDeclared {
			// a declared trailer gains the prefix so net/http sends it as a trailer.
			realHeader[http.TrailerPrefix+key] = append([]string(nil), v...)
		}
	}
}

// replaceHeader clears dst and copies all headers from src to dst.
func replaceHeader(dst, src http.Header) {
	clear(dst)
	for k, v := range src {
		// copy the slice so later mutations of src do not leak into dst.
		dst[k] = append([]string(nil), v...)
	}
}
