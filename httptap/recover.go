package httptap

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"

	loginjector "github.com/prorochestvo/loginjector"
)

// NewRecoverHandler returns an HTTP middleware that recovers any panic in next,
// logs the panic with a stack trace via logger at level, and responds with HTTP
// 500 plus a JSON body.
//
// Behaviour:
//   - On a clean return from next, the response is flushed to the underlying
//     ResponseWriter as if no middleware were present.
//   - On a panic, the buffered response is discarded and a fresh 500 response
//     is written with Content-Type "application/json; charset=utf-8" and body
//     {"error":"something went wrong"}. Both are overridable via options.
//   - http.ErrAbortHandler is re-panicked silently, matching net/http's own
//     server recovery. No log line, no 500 body.
//   - If next hijacks the connection before panicking, the panic is logged but
//     no response can be written — the caller owns the conn.
//
// The handler buffers the entire response body in memory before forwarding it.
// This is necessary so a panic AFTER a WriteHeader call can still be replaced
// with a 500 response. Do not place this middleware in front of SSE, WebSocket,
// or HTTP/2-push handlers: streaming is lost. Wrap only routes that produce
// bounded response bodies.
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
				// buffer absorbed everything; the wire is still clean, so we can
				// write the 500 response directly to the real ResponseWriter.
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
type recoverResponseWriter struct {
	http.ResponseWriter
	headers     http.Header
	body        bytes.Buffer
	statusCode  int
	wroteHeader bool
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

// WriteHeader records the status code in the buffer. First call wins; subsequent
// calls are no-ops, matching the stdlib http.ResponseWriter contract.
func (rrw *recoverResponseWriter) WriteHeader(code int) {
	if rrw.wroteHeader {
		return
	}
	rrw.statusCode = code
	rrw.wroteHeader = true
}

// Write appends b to the in-memory body buffer. Implicitly calls WriteHeader(200)
// if WriteHeader has not been called yet, matching the stdlib contract.
func (rrw *recoverResponseWriter) Write(b []byte) (int, error) {
	if !rrw.wroteHeader {
		rrw.WriteHeader(http.StatusOK)
	}
	return rrw.body.Write(b)
}

// markHijacked satisfies hijackMarker so wrapWithOptionalInterfaces can set the
// hijacked flag when a downstream handler calls Hijack.
func (rrw *recoverResponseWriter) markHijacked() { rrw.hijacked = true }

// flushTo copies the buffered headers, status, and body to the real writer on
// the clean (no-panic) path. If next never called WriteHeader, no explicit
// status is written here: net/http issues an implicit 200 when the body write
// triggers the first header flush, faithfully reproducing the handler's
// observable behaviour.
func (rrw *recoverResponseWriter) flushTo(w http.ResponseWriter) error {
	if rrw.headers != nil {
		dst := w.Header()
		for k, v := range rrw.headers {
			dst[k] = v
		}
	}
	if rrw.wroteHeader {
		w.WriteHeader(rrw.statusCode)
	}
	_, err := w.Write(rrw.body.Bytes())
	return err
}

var _ http.ResponseWriter = (*recoverResponseWriter)(nil)
var _ hijackMarker = (*recoverResponseWriter)(nil)
