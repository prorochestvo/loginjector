// access.go holds the lightweight production access-log handler; see payload.go
// for the debug full-dump variant.

package httptap

import (
	"fmt"
	"io"
	"net/http"
	"time"
)

// NewAccessHandler returns an HTTP middleware that emits one line per request
// in the form:
//
//	2026-05-30T14:23:11.123456789Z [200] GET /api/v1/rates 12.345ms
//
// It is the production access-log variant: status, method, path, duration; no
// header dumping, no body capture, no goroutine fan-out, no *Logger coupling.
// For the debug full-dump variant see NewPayloadHandler.
//
// out is the destination for log lines. The handler writes synchronously; out
// MUST be safe for concurrent Write calls (an *os.File or os.Stdout qualifies;
// a raw *bytes.Buffer does not — guard it externally). The status field is
// zero-padded to three digits; duration is rendered in milliseconds with three
// decimal places. Path is r.URL.Path captured before next runs, so downstream
// path rewrites do not affect the logged value.
//
// A nil out or next is a programmer error and panics.
//
// Format is intentionally fixed. Anything beyond these four fields (request ID,
// client IP, JSON output, structured fields) must go through a separate
// ...WithOptions constructor in a future change, not by mutating this signature.
func NewAccessHandler(out io.Writer, next http.HandlerFunc) http.HandlerFunc {
	if out == nil {
		panic("httptap: out is nil")
	}
	if next == nil {
		panic("httptap: next is nil")
	}

	return func(w http.ResponseWriter, r *http.Request) {
		method := r.Method
		path := escapeControl(r.URL.Path)
		start := time.Now()

		arw := &accessResponseWriter{ResponseWriter: w}
		wrapped := wrapWithOptionalInterfaces(arw, w)

		defer func() {
			elapsed := time.Since(start)
			_, _ = fmt.Fprintf(out, "%s [%03d] %s %s %0.3fms\n",
				time.Now().UTC().Format(time.RFC3339Nano),
				arw.statusCode, method, path,
				float64(elapsed)/1e6,
			)
		}()

		next(wrapped, r)
	}
}

// accessResponseWriter is a lightweight http.ResponseWriter wrapper that records
// only the status code. It does not buffer request or response bodies. It
// satisfies the hijackMarker interface so it can be passed to
// wrapWithOptionalInterfaces.
type accessResponseWriter struct {
	http.ResponseWriter
	statusCode  int
	wroteHeader bool
	hijacked    bool
}

// WriteHeader records the status code on the first call only; subsequent calls
// are silent no-ops, mirroring net/http's own "WriteHeader more than once"
// handling.
func (a *accessResponseWriter) WriteHeader(code int) {
	if a.wroteHeader {
		return
	}
	a.statusCode = code
	a.wroteHeader = true
	a.ResponseWriter.WriteHeader(code)
}

// Write triggers an implicit WriteHeader(http.StatusOK) if no header has been
// set yet, then forwards b to the underlying writer unchanged. The underlying
// writer's return values are returned unmodified.
func (a *accessResponseWriter) Write(b []byte) (int, error) {
	if !a.wroteHeader {
		a.WriteHeader(http.StatusOK)
	}
	return a.ResponseWriter.Write(b)
}

// markHijacked satisfies the hijackMarker interface; it records that the
// connection has been hijacked so the field is available for future use.
func (a *accessResponseWriter) markHijacked() { a.hijacked = true }

// compile-time interface assertions.
var _ http.ResponseWriter = (*accessResponseWriter)(nil)
var _ hijackMarker = (*accessResponseWriter)(nil)
