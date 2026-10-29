// http.go holds the lightweight production access-log handler; see
// httptap/payload.go for the debug full-dump variant.

package extensions

import (
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/prorochestvo/loginjector/internal/httpx"
)

// NewAccessHandler returns an HTTP middleware that emits one line per request
// in the form:
//
//	2026-05-30T14:23:11.123456789Z [200] GET /api/v1/rates 12.345ms
//
// It is the production access-log variant: status, method, path, duration; no
// header dumping, no body capture, no goroutine fan-out, no *Logger coupling.
// For the debug full-dump variant see httptap.NewPayloadHandler.
//
// out is the destination for log lines. The handler writes synchronously; out
// MUST be safe for concurrent Write calls (an *os.File or os.Stdout qualifies;
// a raw *bytes.Buffer does not — guard it externally). The status field is
// zero-padded to three digits; duration is rendered in milliseconds with three
// decimal places. Path is r.URL.Path captured before next runs, so downstream
// path rewrites do not affect the logged value.
//
// Status code is [200] for a handler that returns normally without writing;
// [000] when the connection is hijacked or the handler panics before committing
// a status; or the recorded status for any other case.
//
// A nil out or next is a programmer error and panics.
//
// Format is intentionally fixed. Anything beyond these four fields (request ID,
// client IP, JSON output, structured fields) must go through a separate
// ...WithOptions constructor in a future change, not by mutating this signature.
func NewAccessHandler(out io.Writer, next http.HandlerFunc) http.HandlerFunc {
	if out == nil {
		panic("loginjector: out is nil")
	}
	if next == nil {
		panic("loginjector: next is nil")
	}

	return func(w http.ResponseWriter, r *http.Request) {
		method := r.Method
		path := httpx.EscapeControl(r.URL.Path)
		start := time.Now()

		arw := &accessResponseWriter{ResponseWriter: w}
		wrapped := httpx.WrapWithOptionalInterfaces(arw, w)

		completed := false
		defer func() {
			elapsed := time.Since(start)
			code := arw.statusCode
			if !arw.wroteHeader && !arw.hijacked && completed {
				code = http.StatusOK
			}
			_, _ = fmt.Fprintf(out, "%s [%03d] %s %s %0.3fms\n",
				time.Now().UTC().Format(time.RFC3339Nano),
				code,
				method,
				path,
				float64(elapsed)/1e6,
			)
		}()

		next(wrapped, r)
		completed = true
	}
}

// accessResponseWriter is a lightweight http.ResponseWriter wrapper that records
// only the status code. It does not buffer request or response bodies. It
// satisfies httpx.Tap so it can be passed to
// httpx.WrapWithOptionalInterfaces.
type accessResponseWriter struct {
	http.ResponseWriter
	statusCode  int
	wroteHeader bool
	hijacked    bool
}

// WriteHeader records the status code on the first final call only; informational
// responses (100-199 except 101) are forwarded without recording. Subsequent final
// calls are silent no-ops, mirroring net/http's own "WriteHeader more than once"
// handling.
func (a *accessResponseWriter) WriteHeader(code int) {
	if httpx.IsInformational(code) {
		// forward 1xx without recording
		a.ResponseWriter.WriteHeader(code)
		return
	}
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

// MarkHijacked satisfies httpx.Tap; it records that the
// connection has been hijacked.
func (a *accessResponseWriter) MarkHijacked() { a.hijacked = true }

// BeforeFlush records the implicit 200 that the real Flush commits when no
// final status has been written yet.
func (a *accessResponseWriter) BeforeFlush() {
	if a.hijacked {
		return
	}
	if !a.wroteHeader {
		a.statusCode = http.StatusOK
		a.wroteHeader = true
	}
}
