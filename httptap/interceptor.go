package httptap

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// newInterceptorWithConfig creates a new interceptor that captures request and
// response payloads according to cfg. The response status defaults to 200
// (http.StatusOK) and is overridden by WriteHeader, mirroring net/http's own
// defaulting behaviour. The returned http.ResponseWriter dynamically exposes
// http.Flusher, http.Hijacker, and http.Pusher if and only if the underlying w
// implements those interfaces, so streaming and websocket handlers downstream
// are not broken by the wrapping.
func newInterceptorWithConfig(w http.ResponseWriter, r *http.Request, cfg payloadConfig) (http.ResponseWriter, *http.Request, *interceptor) {
	i := &interceptor{
		code:           http.StatusOK, // default; overridden by WriteHeader
		payload:        new(bytes.Buffer),
		response:       new(bytes.Buffer),
		request:        r,
		cfg:            cfg,
		ResponseWriter: w,
	}

	// wire up request body tee according to the body-cap setting.
	switch {
	case cfg.maxRequestBody == 0:
		// omit: r.Body is already an io.ReadCloser; nothing to tee, no re-wrap needed.
	case cfg.maxRequestBody > 0:
		// bounded capture: tee into a counting writer that stops buffering past the cap
		// but always returns len(b) so the real body stream is fully delivered downstream.
		cw := &cappedWriter{buf: i.payload, limit: cfg.maxRequestBody}
		i.requestCap = cw
		r.Body = struct {
			io.Reader
			io.Closer
		}{
			Reader: io.TeeReader(r.Body, cw),
			Closer: r.Body,
		}
	default:
		// unlimited: tee everything into the payload buffer (legacy behaviour).
		r.Body = struct {
			io.Reader
			io.Closer
		}{
			Reader: io.TeeReader(r.Body, i.payload),
			Closer: r.Body,
		}
	}

	wrapped := wrapWithOptionalInterfaces(i, w)
	return wrapped, r, i
}

// interceptor captures the HTTP status code, request body, and response body
// for logging. It embeds http.ResponseWriter to satisfy the interface; the Write
// and WriteHeader methods are overridden to intercept the response. Optional
// interfaces (http.Flusher, http.Hijacker, http.Pusher) are not implemented here
// directly — wrapWithOptionalInterfaces returns a composite type that exposes
// exactly the interfaces the underlying ResponseWriter has, so streaming and
// websocket handlers are not silently broken by wrapping.
//
// The hijacked field is set to true when the connection has been hijacked via
// the http.Hijacker interface. Once hijacked, the trailing flush guard in
// NewPayloadHandlerWithOptions skips Flush() on the original ResponseWriter to
// avoid calling Flush on an already-hijacked connection.
//
// The wroteHeader field tracks whether a final status code has been recorded
// (by WriteHeader, Write, or Flush).
type interceptor struct {
	code              int
	payload           *bytes.Buffer
	response          *bytes.Buffer
	request           *http.Request
	cfg               payloadConfig
	hijacked          bool
	wroteHeader       bool
	responseTruncated bool
	// requestCap, when non-nil, is the cappedWriter used to bound the request
	// body log; its truncated field indicates whether bytes were discarded.
	requestCap *cappedWriter
	http.ResponseWriter
}

// Write overrides http.ResponseWriter.Write to capture the response body.
// If WriteHeader was never called, the status defaults to http.StatusOK. The
// full b is always forwarded to the underlying writer; body capping applies only
// to the logged buffer.
func (i *interceptor) Write(b []byte) (int, error) {
	if i.code == 0 {
		// defense-in-depth: newInterceptorWithConfig already seeds 200, but guard against
		// zero in case interceptor is constructed in tests.
		i.code = http.StatusOK
	}
	i.wroteHeader = true

	// buffer the response for logging, respecting the per-handler cap.
	var bufErr error
	switch {
	case i.cfg.maxResponseBody == 0:
		// omit: skip buffering entirely.
	case i.cfg.maxResponseBody > 0:
		// bounded: buffer at most maxResponseBody bytes; discard excess.
		remaining := i.cfg.maxResponseBody - i.response.Len()
		if remaining > 0 {
			toBuffer := b
			if len(toBuffer) > remaining {
				toBuffer = toBuffer[:remaining]
				i.responseTruncated = true
			}
			_, bufErr = i.response.Write(toBuffer)
		} else {
			// all of b is beyond the cap; bytes are being discarded.
			i.responseTruncated = true
		}
	default:
		// unlimited (legacy behaviour).
		_, bufErr = i.response.Write(b)
	}

	// always forward the full original bytes to the real writer — never truncate
	// the client's response.
	n, wErr := i.ResponseWriter.Write(b)
	return n, errors.Join(bufErr, wErr)
}

// WriteHeader overrides http.ResponseWriter.WriteHeader to capture the HTTP
// status code. Informational responses (100-199 except 101) are forwarded
// without recording. The first final status is recorded; later calls are
// forwarded to the underlying writer (which reports the superfluous call).
func (i *interceptor) WriteHeader(statusCode int) {
	if isInformational(statusCode) {
		// forward 1xx without recording
		i.ResponseWriter.WriteHeader(statusCode)
		return
	}
	if !i.wroteHeader {
		i.code = statusCode
		i.wroteHeader = true
	}
	i.ResponseWriter.WriteHeader(statusCode)
}

// bytes assembles the logged representation of the intercepted request and
// response using cfg for redaction and body-cap settings.
func (i *interceptor) bytes(cfg payloadConfig) []byte {
	requestRawQuery := redactRawQuery(i.request.URL.RawQuery, cfg.redactQueryParams)
	if requestRawQuery != "" {
		requestRawQuery = "?" + escapeControl(requestRawQuery)
	}
	requestHead := fmt.Sprintf(
		"%s %s%s\n",
		i.request.Method,
		escapeControl(i.request.URL.Path),
		requestRawQuery,
	)
	requestHeadersAsString := strings.TrimSpace(headerToStringWithRedact(&i.request.Header, cfg.redactHeaders))
	responseHead := fmt.Sprintf("%s %d %s\n", i.request.Proto, i.code, http.StatusText(i.code))
	responseHeaders := i.Header()
	responseHeadersAsString := strings.TrimSpace(headerToStringWithRedact(&responseHeaders, cfg.redactHeaders))

	requestTruncated := i.requestCap != nil && i.requestCap.truncated
	payloadSnippet := buildBodySnippet(i.payload, cfg.maxRequestBody, requestTruncated)
	responseSnippet := buildBodySnippet(i.response, cfg.maxResponseBody, i.responseTruncated)

	l := len(requestHead) + len(requestHeadersAsString) + len(payloadSnippet) +
		len(responseSnippet) + len(responseHead) + len(responseHeadersAsString) + 3
	dataset := make([]byte, 0, l)

	dataset = append(dataset, []byte(requestHead)...)
	dataset = append(dataset, []byte(requestHeadersAsString)...)
	dataset = append(dataset, byte('\n'))
	dataset = append(dataset, payloadSnippet...)
	dataset = append(dataset, byte('\n'))
	dataset = append(dataset, []byte(responseHead)...)
	dataset = append(dataset, []byte(responseHeadersAsString)...)
	dataset = append(dataset, byte('\n'))
	dataset = append(dataset, responseSnippet...)

	return dataset
}

// buildBodySnippet returns the trimmed content of buf suitable for inclusion in
// the log. When maxBytes == 0 it returns nil (body omitted). When maxBytes < 0
// it returns the full trimmed buffer (unlimited). When maxBytes > 0 and
// truncated is true, a truncation marker is appended. The truncated flag must be
// set by the caller (cappedWriter.truncated for requests,
// interceptor.responseTruncated for responses) and is the only reliable source
// of truth — checking len(raw) >= maxBytes would give a false positive for a
// body whose length equals the cap exactly.
func buildBodySnippet(buf *bytes.Buffer, maxBytes int, truncated bool) []byte {
	if maxBytes == 0 {
		return nil
	}
	raw := bytes.TrimSpace(buf.Bytes())
	if maxBytes < 0 {
		return raw
	}
	// maxBytes > 0: append a truncation marker only when the caller signals
	// actual truncation.
	if truncated {
		marker := []byte("…[truncated]")
		out := make([]byte, 0, len(raw)+len(marker))
		out = append(out, raw...)
		out = append(out, marker...)
		return out
	}
	return raw
}

// cappedWriter is an io.Writer that forwards up to limit bytes to buf and
// silently discards the remainder. It is used to bound the logged copy of the
// request body without short-reading the actual body stream seen by the
// downstream handler (the io.TeeReader always returns len(b) to the real reader
// regardless of what cappedWriter reports, but we must return len(b) ourselves
// so TeeReader's progress accounting stays correct). The truncated field is set
// to true the first time any bytes are discarded.
type cappedWriter struct {
	buf       *bytes.Buffer
	limit     int
	truncated bool
}

// Write forwards up to c.limit bytes to c.buf; bytes beyond the cap are
// discarded. The full len(b) is always returned so the surrounding io.TeeReader
// continues delivering all bytes to the downstream handler.
func (c *cappedWriter) Write(b []byte) (int, error) {
	remaining := c.limit - c.buf.Len()
	if remaining > 0 {
		toWrite := b
		if len(toWrite) > remaining {
			toWrite = toWrite[:remaining]
			c.truncated = true
		}
		if _, err := c.buf.Write(toWrite); err != nil {
			// the logging-side copy failed, but the real body stream must not be
			// interrupted: return len(b) so io.TeeReader's progress accounting stays
			// correct and the downstream handler sees the complete request body.
			return len(b), err
		}
	} else {
		// all bytes in this call fall beyond the cap.
		c.truncated = true
	}
	return len(b), nil
}
