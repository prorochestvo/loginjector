package httptap

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	loginjector "github.com/prorochestvo/loginjector"
	"github.com/stretchr/testify/require"
)

var _ http.ResponseWriter = (*interceptor)(nil)

// TestBuildBodySnippet covers buildBodySnippet including the exact-boundary edge cases.
func TestBuildBodySnippet(t *testing.T) {
	t.Parallel()

	t.Run("zero_returns_nil", func(t *testing.T) {
		t.Parallel()
		buf := bytes.NewBufferString("some content")
		result := buildBodySnippet(buf, 0, false)
		require.Nil(t, result, "maxBytes==0 must return nil")
	})

	t.Run("negative_returns_full", func(t *testing.T) {
		t.Parallel()
		content := "hello world"
		buf := bytes.NewBufferString(content)
		result := buildBodySnippet(buf, -1, false)
		require.Equal(t, content, string(result))
	})

	t.Run("body_exactly_at_cap_no_truncation_marker", func(t *testing.T) {
		t.Parallel()
		// a body of exactly N bytes captured into a cappedWriter of limit N must NOT
		// produce a truncation marker — truncated=false means nothing was dropped.
		const N = 10
		content := strings.Repeat("a", N)
		buf := bytes.NewBufferString(content)
		result := buildBodySnippet(buf, N, false)
		require.NotContains(t, string(result), "…[truncated]",
			"body of exactly cap bytes must NOT produce truncation marker")
		require.Equal(t, content, string(result))
	})

	t.Run("body_one_over_cap_has_truncation_marker", func(t *testing.T) {
		t.Parallel()
		// simulate cappedWriter having buffered N bytes from a body of N+1 (truncated=true).
		const N = 10
		buf := bytes.NewBufferString(strings.Repeat("b", N))
		result := buildBodySnippet(buf, N, true)
		require.Contains(t, string(result), "…[truncated]",
			"body exceeding cap must contain truncation marker")
	})

	t.Run("truncation_marker_via_handler_boundary", func(t *testing.T) {
		t.Parallel()
		// integration: send exactly N bytes → no marker; send N+1 bytes → marker.
		const N = 15
		b1 := bytes.NewBufferString("")
		l1 := loginjector.NewLogger(logLevelInfo, b1)
		h1 := NewPayloadHandlerWithOptions(l1, logLevelInfo, func(w http.ResponseWriter, r *http.Request) {
			defer func() { _ = r.Body.Close() }()
			_, _ = io.ReadAll(r.Body)
			_, _ = w.Write([]byte("ok"))
		}, WithMaxRequestBody(N), WithSummaryWriter(io.Discard))

		req1 := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(strings.Repeat("x", N)))
		h1(httptest.NewRecorder(), req1)
		require.NotContains(t, b1.String(), "…[truncated]",
			"body of exactly %d bytes must NOT produce truncation marker", N)

		b2 := bytes.NewBufferString("")
		l2 := loginjector.NewLogger(logLevelInfo, b2)
		h2 := NewPayloadHandlerWithOptions(l2, logLevelInfo, func(w http.ResponseWriter, r *http.Request) {
			defer func() { _ = r.Body.Close() }()
			_, _ = io.ReadAll(r.Body)
			_, _ = w.Write([]byte("ok"))
		}, WithMaxRequestBody(N), WithSummaryWriter(io.Discard))

		req2 := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(strings.Repeat("x", N+1)))
		h2(httptest.NewRecorder(), req2)
		require.Contains(t, b2.String(), "…[truncated]",
			"body of %d bytes (one over cap) must produce truncation marker", N+1)
	})
}
