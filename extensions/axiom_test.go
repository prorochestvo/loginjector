package extensions

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prorochestvo/loginjector"
	"github.com/stretchr/testify/require"
)

var (
	_ loginjector.LevelWriter = (*AxiomHandler)(nil)
	_ io.Closer               = (*AxiomHandler)(nil)
	_ fmt.Formatter           = (*AxiomHandler)(nil)
)

func TestNewAxiomHandler(t *testing.T) {
	t.Parallel()
	t.Run("rejects an invalid token", func(t *testing.T) {
		t.Parallel()
		ax, err := NewAxiomHandler("xapt-personal", "logs")
		require.Nil(t, ax)
		require.ErrorContains(t, err, "xaat-")
	})
	t.Run("rejects an invalid domain", func(t *testing.T) {
		t.Parallel()
		_, err := NewAxiomHandler("xaat-secret", "logs", WithAxiomDomain("https://example.com"))
		require.Error(t, err)
	})
	t.Run("rejects reserved fields", func(t *testing.T) {
		t.Parallel()
		_, err := NewAxiomHandler("xaat-secret", "logs", WithAxiomFields(map[string]any{"_private": true}))
		require.Error(t, err)
	})
	t.Run("rejects a nil level name", func(t *testing.T) {
		t.Parallel()
		_, err := NewAxiomHandler("xaat-secret", "logs", WithAxiomLevelName(nil))
		require.Error(t, err)
	})
	t.Run("rejects a zero batch size", func(t *testing.T) {
		t.Parallel()
		_, err := NewAxiomHandler("xaat-secret", "logs", WithAxiomBatchSize(0))
		require.Error(t, err)
	})
	t.Run("rejects a small maximum batch size", func(t *testing.T) {
		t.Parallel()
		_, err := NewAxiomHandler("xaat-secret", "logs", WithAxiomMaxBatchBytes(1))
		require.Error(t, err)
	})
	t.Run("rejects a zero flush interval", func(t *testing.T) {
		t.Parallel()
		_, err := NewAxiomHandler("xaat-secret", "logs", WithAxiomFlushInterval(0))
		require.Error(t, err)
	})
	t.Run("rejects a zero queue size", func(t *testing.T) {
		t.Parallel()
		_, err := NewAxiomHandler("xaat-secret", "logs", WithAxiomQueueSize(0))
		require.Error(t, err)
	})
	t.Run("rejects a zero maximum queue size", func(t *testing.T) {
		t.Parallel()
		_, err := NewAxiomHandler("xaat-secret", "logs", WithAxiomMaxQueueBytes(0))
		require.Error(t, err)
	})
	t.Run("rejects a nil HTTP client", func(t *testing.T) {
		t.Parallel()
		_, err := NewAxiomHandler("xaat-secret", "logs", WithAxiomHTTPClient(nil))
		require.Error(t, err)
	})
	t.Run("rejects negative retries", func(t *testing.T) {
		t.Parallel()
		_, err := NewAxiomHandler("xaat-secret", "logs", WithAxiomMaxRetries(-1))
		require.Error(t, err)
	})
	t.Run("rejects a nil error handler", func(t *testing.T) {
		t.Parallel()
		_, err := NewAxiomHandler("xaat-secret", "logs", WithAxiomErrorHandler(nil))
		require.Error(t, err)
	})
	t.Run("disables compression", func(t *testing.T) {
		t.Parallel()
		encoding := make(chan string, 1)
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			encoding <- r.Header.Get("Content-Encoding")
			w.WriteHeader(http.StatusNoContent)
		}))
		defer server.Close()
		ax := newTestAxiomHandler(t, server, WithAxiomCompression(false))
		_, err := ax.Write([]byte("event"))
		require.NoError(t, err)
		require.NoError(t, ax.Flush(context.Background()))
		require.Empty(t, <-encoding)
	})
	t.Run("adds static fields", func(t *testing.T) {
		t.Parallel()
		body := make(chan []byte, 1)
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			p, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("read body: %v", err)
			}
			body <- p
			w.WriteHeader(http.StatusNoContent)
		}))
		defer server.Close()
		ax := newTestAxiomHandler(t, server, WithAxiomCompression(false), WithAxiomFields(map[string]any{"service": "api"}))
		_, err := ax.Write([]byte("event"))
		require.NoError(t, err)
		require.NoError(t, ax.Flush(context.Background()))
		var event map[string]any
		require.NoError(t, json.Unmarshal(bytes.TrimSpace(<-body), &event))
		require.Equal(t, "api", event["service"])
	})
}

func TestAxiomHandler_Close(t *testing.T) {
	t.Parallel()
	ax, err := NewAxiomHandler("xaat-secret", "logs")
	require.NoError(t, err)
	require.NoError(t, ax.Close())
	before := ax.Stats().Dropped
	p := []byte("event")
	n, err := ax.Write(p)
	require.NoError(t, err)
	require.Equal(t, len(p), n)
	require.Equal(t, before+1, ax.Stats().Dropped)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.ErrorIs(t, ax.Flush(ctx), ErrAxiomClosed)
	require.ErrorIs(t, ax.Shutdown(ctx), ErrAxiomClosed)
}

func TestAxiomHandler_Format(t *testing.T) {
	t.Parallel()
	ax, err := NewAxiomHandler("xaat-secret-token", "logs")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ax.Close() })
	t.Run("value formatting", func(t *testing.T) {
		t.Parallel()
		require.NotContains(t, fmt.Sprintf("%v", *ax), "xaat-secret-token")
		require.NotContains(t, fmt.Sprintf("%+v", *ax), "xaat-secret-token")
		require.NotContains(t, fmt.Sprintf("%#v", *ax), "xaat-secret-token")
		require.NotContains(t, fmt.Sprintf("%s", *ax), "xaat-secret-token")
	})
	t.Run("pointer formatting", func(t *testing.T) {
		t.Parallel()
		require.NotContains(t, fmt.Sprintf("%v", ax), "xaat-secret-token")
		require.NotContains(t, fmt.Sprintf("%+v", ax), "xaat-secret-token")
		require.NotContains(t, fmt.Sprintf("%#v", ax), "xaat-secret-token")
		require.NotContains(t, fmt.Sprintf("%s", ax), "xaat-secret-token")
	})
	require.Equal(t, "*extensions.AxiomHandler", fmt.Sprintf("%T", ax))
	require.Equal(t, "Axiom Handler", fmt.Sprintf("%v", ax))
}

func newTestAxiomHandler(t *testing.T, server *httptest.Server, opts ...AxiomOption) *AxiomHandler {
	t.Helper()
	opts = append(opts, WithAxiomDomain(strings.TrimPrefix(server.URL, "https://")), WithAxiomHTTPClient(server.Client()), WithAxiomBatchSize(1), WithAxiomMaxRetries(0))
	ax, err := NewAxiomHandler("xaat-secret", "logs", opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ax.Close() })
	return ax
}
