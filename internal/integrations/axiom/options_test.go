package axiom

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/prorochestvo/loginjector"
	"github.com/stretchr/testify/require"
)

func TestWithDomain(t *testing.T) {
	t.Parallel()
	t.Run("bare host accepted", func(t *testing.T) {
		t.Parallel()
		h, err := NewHandler(
			"xaat-secret",
			"logs",
			WithDomain("eu-central-1.aws.edge.axiom.co"),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = h.Close() })
		require.True(
			t,
			strings.HasPrefix(h.cfg.ingestURL, "https://eu-central-1.aws.edge.axiom.co/"),
		)
	})
	assertBadDomain := func(t *testing.T, domain string) {
		t.Helper()
		_, err := NewHandler(
			"xaat-secret",
			"logs",
			WithDomain(domain),
		)
		require.ErrorContains(t, err, "bare host")
	}
	t.Run("empty", func(t *testing.T) { t.Parallel(); assertBadDomain(t, "") })
	t.Run("scheme", func(t *testing.T) { t.Parallel(); assertBadDomain(t, "https://example.com") })
	t.Run("path", func(t *testing.T) { t.Parallel(); assertBadDomain(t, "example.com/path") })
	t.Run("query", func(t *testing.T) { t.Parallel(); assertBadDomain(t, "example.com?x=y") })
	t.Run("fragment", func(t *testing.T) { t.Parallel(); assertBadDomain(t, "example.com#frag") })
	t.Run("whitespace", func(t *testing.T) { t.Parallel(); assertBadDomain(t, "example .com") })
	t.Run("user info", func(t *testing.T) { t.Parallel(); assertBadDomain(t, "user@example.com") })
}

func TestWithFields(t *testing.T) {
	t.Parallel()
	t.Run("map is pre-encoded", func(t *testing.T) {
		t.Parallel()
		fields := map[string]any{"service": "before"}
		h, err := NewHandler(
			"xaat-secret",
			"logs",
			WithFields(fields),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = h.Close() })
		fields["service"] = "after"
		require.Equal(t, []byte(`"service":"before"`), h.cfg.fields)
		dst, err := h.appendEvent(
			nil,
			newEvent(
				time.Now(),
				0,
				false,
				[]byte("message"),
			),
		)
		require.NoError(t, err)
		var eventFields map[string]any
		require.NoError(
			t,
			json.Unmarshal(
				dst,
				&eventFields,
			),
		)
		require.Equal(t, "before", eventFields["service"])
	})
	t.Run("later option replaces earlier", func(t *testing.T) {
		t.Parallel()
		h, err := NewHandler(
			"xaat-secret",
			"logs",
			WithFields(map[string]any{"first": 1}),
			WithFields(map[string]any{"second": 2}),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = h.Close() })
		require.Equal(t, []byte(`"second":2`), h.cfg.fields)
	})
	t.Run("nil map", func(t *testing.T) {
		t.Parallel()
		h, err := NewHandler(
			"xaat-secret",
			"logs",
			WithFields(nil),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = h.Close() })
		require.Empty(t, h.cfg.fields)
	})
	t.Run("reserved keys", func(t *testing.T) {
		t.Parallel()
		assertReserved := func(t *testing.T, key string) {
			t.Helper()
			_, err := NewHandler(
				"xaat-secret",
				"logs",
				WithFields(map[string]any{key: true}),
			)
			require.ErrorContains(t, err, "field name")
		}
		t.Run("_time", func(t *testing.T) { t.Parallel(); assertReserved(t, "_time") })
		t.Run("level", func(t *testing.T) { t.Parallel(); assertReserved(t, "level") })
		t.Run("level_num", func(t *testing.T) { t.Parallel(); assertReserved(t, "level_num") })
		t.Run("message", func(t *testing.T) { t.Parallel(); assertReserved(t, "message") })
		t.Run("message_truncated", func(t *testing.T) { t.Parallel(); assertReserved(t, "message_truncated") })
		t.Run("other underscore key", func(t *testing.T) { t.Parallel(); assertReserved(t, "_custom") })
	})
	t.Run("empty key", func(t *testing.T) {
		t.Parallel()
		_, err := NewHandler(
			"xaat-secret",
			"logs",
			WithFields(map[string]any{"": 1}),
		)
		require.ErrorContains(t, err, "field name")
	})
	t.Run("invalid UTF-8 key", func(t *testing.T) {
		t.Parallel()
		_, err := NewHandler(
			"xaat-secret",
			"logs",
			WithFields(map[string]any{string([]byte{'b', 0xff}): 1}),
		)
		require.ErrorContains(t, err, "valid UTF-8")
	})
	t.Run("key longer than 200 bytes", func(t *testing.T) {
		t.Parallel()
		_, err := NewHandler(
			"xaat-secret",
			"logs",
			WithFields(map[string]any{strings.Repeat("k", 201): 1}),
		)
		require.ErrorContains(t, err, "field name")
	})
	t.Run("non-encodable value", func(t *testing.T) {
		t.Parallel()
		_, err := NewHandler(
			"xaat-secret",
			"logs",
			WithFields(map[string]any{"channel": make(chan int)}),
		)
		require.ErrorContains(t, err, "encode static fields")
	})
	t.Run("fragment size cap", func(t *testing.T) {
		t.Parallel()
		_, err := NewHandler(
			"xaat-secret",
			"logs",
			WithFields(map[string]any{"large": strings.Repeat(
				"a",
				(64<<10)+1,
			)}),
		)
		require.ErrorContains(t, err, "64 KiB")
	})
}

func TestWithLevelName(t *testing.T) {
	t.Parallel()
	t.Run("custom function", func(t *testing.T) {
		t.Parallel()
		fn := func(_ loginjector.LogLevel) string { return "custom" }
		h, err := NewHandler(
			"xaat-secret",
			"logs",
			WithLevelName(fn),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = h.Close() })
		require.Equal(
			t,
			"custom",
			h.cfg.levelName(0),
		)
	})
	t.Run("nil function rejected", func(t *testing.T) {
		t.Parallel()
		_, err := NewHandler(
			"xaat-secret",
			"logs",
			WithLevelName(nil),
		)
		require.ErrorContains(t, err, "must not be nil")
	})
}

func TestWithBatchSize(t *testing.T) {
	t.Parallel()
	t.Run("bounds accepted", func(t *testing.T) {
		t.Parallel()
		t.Run("one", func(t *testing.T) {
			t.Parallel()
			h, err := NewHandler(
				"xaat-secret",
				"logs",
				WithBatchSize(1),
			)
			require.NoError(t, err)
			t.Cleanup(func() { _ = h.Close() })
			require.Equal(t, 1, h.cfg.batchSize)
		})
		t.Run("maximum", func(t *testing.T) {
			t.Parallel()
			h, err := NewHandler(
				"xaat-secret",
				"logs",
				WithBatchSize(10000),
			)
			require.NoError(t, err)
			t.Cleanup(func() { _ = h.Close() })
			require.Equal(t, 10000, h.cfg.batchSize)
		})
	})
	t.Run("zero rejected", func(t *testing.T) {
		t.Parallel()
		_, err := NewHandler(
			"xaat-secret",
			"logs",
			WithBatchSize(0),
		)
		require.Error(t, err)
	})
	t.Run("above maximum rejected", func(t *testing.T) {
		t.Parallel()
		_, err := NewHandler(
			"xaat-secret",
			"logs",
			WithBatchSize(10001),
		)
		require.Error(t, err)
	})
}

func TestWithMaxBatchBytes(t *testing.T) {
	t.Parallel()
	t.Run("minimum accepted", func(t *testing.T) {
		t.Parallel()
		h, err := NewHandler(
			"xaat-secret",
			"logs",
			WithMaxBatchBytes(
				1<<20,
			),
			WithMaxQueueBytes(
				1<<20,
			),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = h.Close() })
		require.Equal(
			t,
			1<<20,
			h.cfg.maxBatchBytes,
		)
	})
	t.Run("below minimum rejected", func(t *testing.T) {
		t.Parallel()
		_, err := NewHandler(
			"xaat-secret",
			"logs",
			WithMaxBatchBytes(
				(1<<20)-1,
			),
		)
		require.Error(t, err)
	})
}

func TestWithFlushInterval(t *testing.T) {
	t.Parallel()
	t.Run("positive accepted", func(t *testing.T) {
		t.Parallel()
		h, err := NewHandler(
			"xaat-secret",
			"logs",
			WithFlushInterval(
				2*time.Second,
			),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = h.Close() })
		require.Equal(
			t,
			2*time.Second,
			h.cfg.flushInterval,
		)
	})
	t.Run("nonpositive rejected", func(t *testing.T) {
		t.Parallel()
		_, err := NewHandler(
			"xaat-secret",
			"logs",
			WithFlushInterval(0),
		)
		require.Error(t, err)
	})
}

func TestWithQueueSize(t *testing.T) {
	t.Parallel()
	t.Run("positive accepted", func(t *testing.T) {
		t.Parallel()
		h, err := NewHandler(
			"xaat-secret",
			"logs",
			WithQueueSize(17),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = h.Close() })
		require.Equal(t, 17, h.cfg.queueSize)
	})
	t.Run("nonpositive rejected", func(t *testing.T) {
		t.Parallel()
		_, err := NewHandler(
			"xaat-secret",
			"logs",
			WithQueueSize(0),
		)
		require.Error(t, err)
	})
	t.Run("above upper bound rejected", func(t *testing.T) {
		t.Parallel()
		var cfg config
		require.Error(
			t,
			WithQueueSize(
				1<<20+1,
			)(
				&cfg,
			),
		)
	})
	t.Run("upper bound accepted", func(t *testing.T) {
		t.Parallel()
		var cfg config
		require.NoError(
			t,
			WithQueueSize(
				1<<20,
			)(
				&cfg,
			),
		)
	})
}

func TestWithMaxQueueBytes(t *testing.T) {
	t.Parallel()
	t.Run("positive accepted", func(t *testing.T) {
		t.Parallel()
		h, err := NewHandler(
			"xaat-secret",
			"logs",
			WithMaxQueueBytes(
				8<<20,
			),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = h.Close() })
		require.Equal(
			t,
			8<<20,
			h.cfg.maxQueueBytes,
		)
	})
	t.Run("nonpositive rejected", func(t *testing.T) {
		t.Parallel()
		_, err := NewHandler(
			"xaat-secret",
			"logs",
			WithMaxQueueBytes(0),
		)
		require.Error(t, err)
	})
}

func TestWithHTTPClient(t *testing.T) {
	t.Parallel()
	t.Run("client is externally owned", func(t *testing.T) {
		t.Parallel()
		client := &http.Client{}
		h, err := NewHandler(
			"xaat-secret",
			"logs",
			WithHTTPClient(client),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = h.Close() })
		require.Same(t, client, h.cfg.httpClient)
		require.False(t, h.cfg.ownsClient)
	})
	t.Run("nil client rejected", func(t *testing.T) {
		t.Parallel()
		_, err := NewHandler(
			"xaat-secret",
			"logs",
			WithHTTPClient(nil),
		)
		require.ErrorContains(t, err, "must not be nil")
	})
}

func TestWithMaxRetries(t *testing.T) {
	t.Parallel()
	t.Run("zero disables retries", func(t *testing.T) {
		t.Parallel()
		h, err := NewHandler(
			"xaat-secret",
			"logs",
			WithMaxRetries(0),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = h.Close() })
		require.Zero(t, h.cfg.maxRetries)
	})
	t.Run("negative rejected", func(t *testing.T) {
		t.Parallel()
		_, err := NewHandler(
			"xaat-secret",
			"logs",
			WithMaxRetries(
				-1,
			),
		)
		require.Error(t, err)
	})
}

func TestWithCompression(t *testing.T) {
	t.Parallel()
	t.Run("disabled", func(t *testing.T) {
		t.Parallel()
		h, err := NewHandler(
			"xaat-secret",
			"logs",
			WithCompression(false),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = h.Close() })
		require.False(t, h.cfg.compression)
	})
}

func TestWithErrorHandler(t *testing.T) {
	t.Parallel()
	t.Run("custom handler", func(t *testing.T) {
		t.Parallel()
		fn := func(error) {}
		h, err := NewHandler(
			"xaat-secret",
			"logs",
			WithErrorHandler(fn),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = h.Close() })
		require.NotNil(t, h.cfg.errorHandler)
	})
	t.Run("nil handler rejected", func(t *testing.T) {
		t.Parallel()
		_, err := NewHandler(
			"xaat-secret",
			"logs",
			WithErrorHandler(nil),
		)
		require.ErrorContains(t, err, "must not be nil")
	})
}

func withClock(now func() time.Time, wait func(context.Context, time.Duration) bool) Option {
	return func(cfg *config) error {
		if now == nil || wait == nil {
			return errors.New("clock functions must not be nil")
		}
		cfg.now = now
		cfg.wait = wait
		return nil
	}
}

func withBaseURL(baseURL string) Option {
	return func(cfg *config) error {
		parsed, err := url.Parse(baseURL)
		if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Path != "" {
			return errors.New("base URL must contain only a scheme and host")
		}
		cfg.baseURL = strings.TrimSuffix(baseURL, "/")
		return nil
	}
}

func withErrorOutput(output io.Writer) Option {
	return func(cfg *config) error {
		if output == nil {
			return errors.New("error output must not be nil")
		}
		cfg.errorOutput = output
		return nil
	}
}
