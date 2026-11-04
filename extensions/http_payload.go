package extensions

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/prorochestvo/loginjector/internal/httpx"

	"github.com/prorochestvo/loginjector"
)

// PayloadOption configures NewPayloadHandlerWithOptions. Options are applied in
// order; later options win. The zero set of options reproduces the exact
// behaviour of NewPayloadHandler.
type PayloadOption func(*payloadConfig)

// WithMaxRequestBody caps the number of bytes buffered from the request body
// for logging. n < 0 explicitly overrides the 64 KiB default with unlimited
// capture; n == 0 omits the request body from the log entirely; n > 0 buffers at
// most n bytes and appends a truncation marker when the body exceeds the limit.
// The downstream handler always receives the full, untruncated body regardless
// of this setting.
func WithMaxRequestBody(n int) PayloadOption {
	return func(c *payloadConfig) { c.maxRequestBody = n }
}

// WithMaxResponseBody caps the number of bytes buffered from the response body
// for logging. n < 0 explicitly overrides the 64 KiB default with unlimited
// capture; n == 0 omits the response body from the log entirely; n > 0 buffers
// at most n bytes and appends a truncation marker when the body exceeds the
// limit. The downstream client always receives the full, untruncated response
// regardless of this setting.
func WithMaxResponseBody(n int) PayloadOption {
	return func(c *payloadConfig) { c.maxResponseBody = n }
}

// WithoutBodies disables request and response body capture for this handler. It
// is equivalent to applying WithMaxRequestBody(0) and WithMaxResponseBody(0)
// together.
func WithoutBodies() PayloadOption {
	return func(c *payloadConfig) {
		c.maxRequestBody = 0
		c.maxResponseBody = 0
	}
}

// WithRedactHeaders adds header names to the redaction set used when logging
// request and response headers. It adds names and cannot remove the built-in set.
// The default set (Authorization,
// Proxy-Authorization, Cookie, Set-Cookie, X-Api-Key, X-Auth-Token) is always
// present and cannot be removed; this option is additive only. Matching is
// case-insensitive.
func WithRedactHeaders(names ...string) PayloadOption {
	return func(c *payloadConfig) {
		for _, n := range names {
			c.redactHeaders[redactKey(n)] = struct{}{}
		}
	}
}

// WithRedactQueryParams adds query parameter names to the redaction set used
// when logging request URIs. It adds names and cannot remove the built-in set.
// The default set (access_token, refresh_token,
// id_token, client_secret, api_key, apikey, password) is always present and
// cannot be removed; this option is additive only. Matching is case-insensitive.
func WithRedactQueryParams(names ...string) PayloadOption {
	return func(c *payloadConfig) {
		for _, n := range names {
			c.redactQueryParams[redactKey(n)] = struct{}{}
		}
	}
}

// WithSummaryWriter sets a per-handler destination for the one-line timing/size
// summary. When set, this writer takes precedence over the default os.Stdout.
// Pass io.Discard to silence the summary for this handler only without affecting
// other handlers. Precedence: per-handler WithSummaryWriter > os.Stdout.
func WithSummaryWriter(w io.Writer) PayloadOption {
	return func(c *payloadConfig) { c.summaryWriter = w }
}

// NewPayloadHandler is the debug variant: dumps full request and response
// payloads; for the production access-log variant see [NewAccessHandler].
//
// It creates a new HTTP middleware handler that logs the full request and
// response payloads (request line, headers, bodies, response status) to the
// given logger at the specified level. The Authorization, Proxy-Authorization,
// Cookie, Set-Cookie, X-Api-Key, and X-Auth-Token headers are always redacted.
// Query parameters access_token, refresh_token, id_token, client_secret, api_key,
// apikey, and password are logged with values redacted as [REDACTED].
// Matching is case-insensitive; options only add names.
// This function is equivalent to calling NewPayloadHandlerWithOptions with no
// options.
//
// A nil logger or nextFunc is a programmer error and panics.
func NewPayloadHandler(logger *loginjector.Logger, level loginjector.LogLevel, nextFunc http.HandlerFunc) http.HandlerFunc {
	return NewPayloadHandlerWithOptions(logger, level, nextFunc)
}

// NewPayloadHandlerWithOptions creates an HTTP middleware handler like
// NewPayloadHandler but accepts functional options to tune body-capture limits,
// header and query redaction, and the summary destination. Calling it with no
// options produces byte-identical logger output and the same stdout summary as
// NewPayloadHandler.
// The default sensitive header names and credential query parameters are always
// redacted case-insensitively; options only add names.
//
// A nil logger or nextFunc is a programmer error and panics.
//
// Available options:
//   - WithMaxRequestBody / WithMaxResponseBody — cap or disable body buffering.
//   - WithoutBodies — disable both request and response body capture.
//   - WithRedactHeaders — add header names to the always-on redaction set.
//   - WithRedactQueryParams — add query parameter names to the always-on redaction set.
//   - WithSummaryWriter — override the summary destination for this handler only.
func NewPayloadHandlerWithOptions(logger *loginjector.Logger, level loginjector.LogLevel, nextFunc http.HandlerFunc, opts ...PayloadOption) http.HandlerFunc {
	if logger == nil {
		panic("loginjector: logger is nil")
	}
	if nextFunc == nil {
		panic("loginjector: nextFunc is nil")
	}

	cfg := defaultPayloadConfig()
	for _, o := range opts {
		o(&cfg)
	}

	return func(w http.ResponseWriter, r *http.Request) {
		iw, ir, i := newInterceptorWithConfig(w, r, cfg)

		defer func(i *interceptor, inboundedAt time.Time) {
			elapsed := time.Since(inboundedAt)
			payloadSize := i.payload.Len()
			responseSize := i.response.Len()
			responseCode := i.code
			escapedPath := httpx.EscapeControl(r.URL.Path)

			wg := sync.WaitGroup{}
			wg.Add(1)
			go func(wg *sync.WaitGroup, i *interceptor) {
				defer wg.Done()
				_, err := logger.WriteLog(level, i.bytes(cfg))
				if err != nil {
					println(err.Error())
				}
			}(&wg, i)
			defer wg.Wait()

			// best-effort diagnostic summary; honours the documented fmt.Fprint* exception.
			sw := cfg.summaryWriter
			if sw == nil {
				sw = os.Stdout
			}
			_, _ = fmt.Fprintf(sw, "[%d] %s %s: %0.3f msec; ↓%0.2fKb; ↑%0.2fKb;\n",
				responseCode,
				r.Method,
				escapedPath,
				float64(elapsed)/1000000,
				float64(payloadSize)/1024,
				float64(responseSize)/1024,
			)
		}(i, time.Now().UTC())

		nextFunc(iw, ir)

		// skip flush when the connection has been hijacked: the caller now owns
		// the underlying conn and calling Flush on the original ResponseWriter is
		// undefined.
		if !i.hijacked {
			if f, ok := w.(http.Flusher); ok && f != nil {
				f.Flush()
			}
		}
	}
}

// defaultMaxBody is the default per-request body-capture cap (64 KiB). It bounds
// the memory a debug handler buffers per request; a larger body is truncated in
// the log while the downstream handler/client still receives it in full. Pass
// WithMaxRequestBody(-1) / WithMaxResponseBody(-1) to opt back into unlimited
// capture.
const defaultMaxBody = 64 << 10 // 65536

// payloadConfig holds all tunables for NewPayloadHandlerWithOptions.
type payloadConfig struct {
	// maxRequestBody and maxResponseBody: <0 = unlimited, 0 = omit, >0 = cap at N bytes.
	maxRequestBody  int
	maxResponseBody int
	// redactHeaders is lowercase-folded via redactKey(); always contains
	// the built-in sensitive set and may contain caller-supplied additions.
	redactHeaders map[string]struct{}
	// redactQueryParams holds lowercase parameter names to redact; always contains
	// the built-in set (access_token, refresh_token, etc.) and may contain caller-supplied
	// additions.
	redactQueryParams map[string]struct{}
	// summaryWriter, when non-nil, overrides os.Stdout for the per-request summary line
	// for this handler only.
	summaryWriter io.Writer
}

// defaultPayloadConfig returns the baseline config: 64 KiB body-capture cap,
// fixed redaction sets, os.Stdout summary.
func defaultPayloadConfig() payloadConfig {
	m := make(map[string]struct{}, len(defaultRedactHeaders))
	for _, h := range defaultRedactHeaders {
		m[redactKey(h)] = struct{}{}
	}
	q := make(map[string]struct{}, len(defaultRedactQueryParams))
	for name := range defaultRedactQueryParams {
		q[redactKey(name)] = struct{}{}
	}
	return payloadConfig{
		maxRequestBody:    defaultMaxBody,
		maxResponseBody:   defaultMaxBody,
		redactHeaders:     m,
		redactQueryParams: q,
	}
}
