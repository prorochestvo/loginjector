package axiom

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/prorochestvo/loginjector"
	"github.com/prorochestvo/loginjector/levels"
	"github.com/stretchr/testify/require"
)

var (
	_ loginjector.LevelWriter = (*Handler)(nil)
	_ io.Closer               = (*Handler)(nil)
	_ fmt.Formatter           = (*Handler)(nil)
)

func TestNewHandler(t *testing.T) {
	t.Parallel()
	t.Run("defaults", func(t *testing.T) {
		t.Parallel()
		h, err := NewHandler("xaat-secret", "logs")
		require.NoError(t, err)
		t.Cleanup(func() { _ = h.Close() })
		require.Equal(t, "https://us-east-1.aws.edge.axiom.co/v1/ingest/logs", h.cfg.ingestURL)
		require.Equal(t, 1000, h.cfg.batchSize)
		require.Equal(
			t,
			4<<20,
			h.cfg.maxBatchBytes,
		)
		require.Equal(t, time.Second, h.cfg.flushInterval)
		require.Equal(t, 10000, h.cfg.queueSize)
		require.Equal(
			t,
			32<<20,
			h.cfg.maxQueueBytes,
		)
		require.Equal(t, 3, h.cfg.maxRetries)
		require.True(t, h.cfg.compression)
		require.True(t, h.cfg.ownsClient)
		require.Equal(
			t,
			10*time.Second,
			h.cfg.httpClient.Timeout,
		)
	})
	t.Run("empty token", func(t *testing.T) {
		t.Parallel()
		_, err := NewHandler("", "logs")
		require.ErrorContains(t, err, "axiom:")
		require.NotContains(
			t,
			err.Error(),
			"secret",
		)
	})
	t.Run("personal token rejected", func(t *testing.T) {
		t.Parallel()
		_, err := NewHandler("xapt-secret", "logs")
		require.ErrorContains(t, err, "xaat-")
		require.ErrorContains(t, err, "personal tokens are not supported")
		require.NotContains(
			t,
			err.Error(),
			"xapt-secret",
		)
	})
	t.Run("token whitespace", func(t *testing.T) {
		t.Parallel()
		_, err := NewHandler("xaat-secret value", "logs")
		require.ErrorContains(t, err, "axiom:")
		require.NotContains(
			t,
			err.Error(),
			"xaat-secret",
		)
	})
	t.Run("token control", func(t *testing.T) {
		t.Parallel()
		_, err := NewHandler("xaat-secret\x7f", "logs")
		require.ErrorContains(t, err, "axiom:")
		require.NotContains(
			t,
			err.Error(),
			"xaat-secret",
		)
	})
	t.Run("empty dataset", func(t *testing.T) {
		t.Parallel()
		_, err := NewHandler("xaat-secret", "")
		require.ErrorContains(t, err, "dataset")
	})
	t.Run("dataset slash", func(t *testing.T) {
		t.Parallel()
		_, err := NewHandler("xaat-secret", "logs/raw")
		require.ErrorContains(t, err, "slash")
	})
	t.Run("bad option wrapped with context", func(t *testing.T) {
		t.Parallel()
		_, err := NewHandler(
			"xaat-secret",
			"logs",
			WithBatchSize(0),
		)
		require.ErrorContains(t, err, "apply option 1")
		require.ErrorContains(t, err, "batch size")
		require.NotContains(
			t,
			err.Error(),
			"xaat-secret",
		)
	})
	t.Run("nil option", func(t *testing.T) {
		t.Parallel()
		_, err := NewHandler("xaat-secret", "logs", nil)
		require.ErrorContains(t, err, "option must not be nil")
	})
	t.Run("queue byte cross check", func(t *testing.T) {
		t.Parallel()
		_, err := NewHandler(
			"xaat-secret",
			"logs",
			WithMaxQueueBytes(
				1<<20,
			),
		)
		require.ErrorContains(t, err, "at least maximum batch bytes")
	})
	t.Run("dataset path escaping", func(t *testing.T) {
		t.Parallel()
		h, err := NewHandler("xaat-secret", "logs group?west")
		require.NoError(t, err)
		t.Cleanup(func() { _ = h.Close() })
		require.Equal(t, "https://us-east-1.aws.edge.axiom.co/v1/ingest/logs%20group%3Fwest", h.cfg.ingestURL)
	})
	t.Run("base URL seam", func(t *testing.T) {
		t.Parallel()
		baseURL := "http://127.0.0.1:8080"
		h, err := NewHandler(
			"xaat-secret",
			"logs",
			withBaseURL(baseURL),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = h.Close() })
		require.Equal(
			t,
			baseURL+"/v1/ingest/logs",
			h.cfg.ingestURL,
		)
	})
}

func Test_newEvent(t *testing.T) {
	t.Parallel()
	t.Run("trims one newline", func(t *testing.T) {
		t.Parallel()
		ev := newEvent(
			time.Date(
				2026,
				10,
				22,
				3,
				4,
				5,
				6,
				time.FixedZone(
					"west",
					-3600,
				),
			),
			1,
			true,
			[]byte("line\n"),
		)
		require.Equal(t, "line", ev.message)
		require.Equal(
			t,
			time.UTC,
			ev.time.Location(),
		)
		require.Equal(
			t,
			4,
			ev.time.Hour(),
		)
	})
	t.Run("trims CRLF", func(t *testing.T) {
		t.Parallel()
		ev := newEvent(
			time.Now(),
			0,
			false,
			[]byte("line\r\n"),
		)
		require.Equal(t, "line", ev.message)
	})
	t.Run("trims multiple trailing line breaks", func(t *testing.T) {
		t.Parallel()
		ev := newEvent(
			time.Now(),
			0,
			false,
			[]byte("line\r\n\n\r"),
		)
		require.Equal(t, "line", ev.message)
	})
	t.Run("truncates on UTF-8 boundary", func(t *testing.T) {
		t.Parallel()
		input := strings.Repeat(
			"界",
			maxMessageLen/3+1,
		)
		ev := newEvent(
			time.Now(),
			0,
			false,
			[]byte(input),
		)
		require.True(t, ev.truncated)
		require.LessOrEqual(
			t,
			len(ev.message),
			maxMessageLen,
		)
		require.True(
			t,
			utf8.ValidString(ev.message),
		)
	})
	t.Run("does not truncate short message", func(t *testing.T) {
		t.Parallel()
		ev := newEvent(
			time.Now(),
			0,
			false,
			[]byte("short"),
		)
		require.False(t, ev.truncated)
	})
	t.Run("invalid UTF-8 normalized size stays within field limit", func(t *testing.T) {
		t.Parallel()
		input := bytes.Repeat([]byte{0xff}, maxMessageLen)
		ev := newEvent(
			time.Now(),
			0,
			false,
			input,
		)
		h, err := NewHandler("xaat-secret", "logs")
		require.NoError(t, err)
		t.Cleanup(func() { _ = h.Close() })
		line, err := h.appendEvent(nil, ev)
		require.NoError(t, err)
		var decoded map[string]any
		require.NoError(
			t,
			json.Unmarshal(
				line,
				&decoded,
			),
		)
		message := decoded["message"].(string)
		require.LessOrEqual(
			t,
			len(message),
			maxMessageLen,
		)
		require.True(t, decoded["message_truncated"].(bool))
	})
	t.Run("copies caller bytes", func(t *testing.T) {
		t.Parallel()
		input := []byte("before")
		ev := newEvent(
			time.Now(),
			0,
			false,
			input,
		)
		copy(input, "after!")
		require.Equal(t, "before", ev.message)
	})
}

func TestHandler_Format(t *testing.T) {
	t.Parallel()
	check := func(t *testing.T, formatted string) {
		t.Helper()
		require.NotContains(t, formatted, "xaat-super-secret-token")
	}
	h, err := NewHandler("xaat-super-secret-token", "logs")
	require.NoError(t, err)
	t.Cleanup(func() { _ = h.Close() })
	t.Run("pointer %v", func(t *testing.T) {
		t.Parallel()
		check(
			t,
			fmt.Sprintf("%v", h),
		)
	})
	t.Run("pointer %+v", func(t *testing.T) {
		t.Parallel()
		check(
			t,
			fmt.Sprintf("%+v", h),
		)
	})
	t.Run("pointer %#v", func(t *testing.T) {
		t.Parallel()
		check(
			t,
			fmt.Sprintf("%#v", h),
		)
	})
	t.Run("pointer %s", func(t *testing.T) {
		t.Parallel()
		check(
			t,
			fmt.Sprintf("%s", h),
		)
	})
	require.NoError(
		t,
		h.Close(),
	)
	value := reflect.ValueOf(h).Elem().Interface()
	t.Run("value %+v", func(t *testing.T) {
		t.Parallel()
		check(
			t,
			fmt.Sprintf("%+v", value),
		)
	})
	t.Run("value %#v", func(t *testing.T) {
		t.Parallel()
		check(
			t,
			fmt.Sprintf("%#v", value),
		)
	})
}

func TestHandler_WriteLevel(t *testing.T) {
	t.Parallel()
	t.Run("level is included", func(t *testing.T) {
		t.Parallel()
		got := make(chan []map[string]any, 1)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			events, _, err := requestEvents(r)
			if err != nil {
				http.Error(
					w,
					err.Error(),
					http.StatusBadRequest,
				)
				return
			}
			got <- events
			responseJSON(w, http.StatusOK, `{"ingested":1}`)
		}))
		h := testHandler(
			t,
			server,
			WithFlushInterval(time.Hour),
		)
		_, err := h.WriteLevel(4, []byte("x"))
		require.NoError(t, err)
		flushHandler(t, h)
		event := (<-got)[0]
		require.Equal(t, "error", event["level"])
		require.EqualValues(t, 4, event["level_num"])
	})
	t.Run("plain write omits level", func(t *testing.T) {
		t.Parallel()
		got := make(chan []map[string]any, 1)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			events, _, err := requestEvents(r)
			if err != nil {
				http.Error(
					w,
					err.Error(),
					http.StatusBadRequest,
				)
				return
			}
			got <- events
			responseJSON(w, http.StatusOK, `{"ingested":1}`)
		}))
		h := testHandler(
			t,
			server,
			WithFlushInterval(time.Hour),
		)
		_, err := h.Write([]byte("x"))
		require.NoError(t, err)
		flushHandler(t, h)
		event := (<-got)[0]
		_, hasLevel := event["level"]
		_, hasLevelNum := event["level_num"]
		require.False(t, hasLevel)
		require.False(t, hasLevelNum)
	})
}

func Test_resetBatch(t *testing.T) {
	t.Parallel()
	batch := []queuedEvent{{line: []byte("large")}, {line: []byte("batch")}}
	backing := batch[:cap(batch)]
	reset := resetBatch(batch)
	require.Empty(t, reset)
	for _, item := range backing {
		require.Nil(t, item.line)
	}
}

func TestHandler_appendEvent(t *testing.T) {
	t.Parallel()
	t.Run("level present including zero", func(t *testing.T) {
		t.Parallel()
		h, err := NewHandler("xaat-secret", "logs")
		require.NoError(t, err)
		t.Cleanup(func() { _ = h.Close() })
		dst, err := h.appendEvent(
			nil,
			newEvent(
				time.Date(
					2026,
					10,
					22,
					1,
					2,
					3,
					4,
					time.UTC,
				),
				0,
				true,
				[]byte("hello"),
			),
		)
		require.NoError(t, err)
		var fields map[string]any
		require.NoError(
			t,
			json.Unmarshal(
				dst,
				&fields,
			),
		)
		require.Equal(t, "unknown", fields["level"])
		require.InDelta(
			t,
			0,
			fields["level_num"],
			0,
		)
		require.Equal(t, "hello", fields["message"])
		require.Equal(t, "2026-10-22T01:02:03.000000004Z", fields["_time"])
		require.Equal(t, "\n", string(dst[len(dst)-1]))
	})
	t.Run("level absent", func(t *testing.T) {
		t.Parallel()
		h, err := NewHandler("xaat-secret", "logs")
		require.NoError(t, err)
		t.Cleanup(func() { _ = h.Close() })
		dst, err := h.appendEvent(
			nil,
			newEvent(
				time.Now(),
				levels.Warning,
				false,
				[]byte("plain"),
			),
		)
		require.NoError(t, err)
		var fields map[string]any
		require.NoError(
			t,
			json.Unmarshal(
				dst,
				&fields,
			),
		)
		require.NotContains(t, fields, "level")
		require.NotContains(t, fields, "level_num")
	})
	t.Run("default level name", func(t *testing.T) {
		t.Parallel()
		h, err := NewHandler("xaat-secret", "logs")
		require.NoError(t, err)
		t.Cleanup(func() { _ = h.Close() })
		dst, err := h.appendEvent(
			nil,
			newEvent(
				time.Now(),
				levels.Warning,
				true,
				[]byte("warning"),
			),
		)
		require.NoError(t, err)
		var fields map[string]any
		require.NoError(
			t,
			json.Unmarshal(
				dst,
				&fields,
			),
		)
		require.Equal(t, "warning", fields["level"])
	})
	t.Run("custom level name", func(t *testing.T) {
		t.Parallel()
		h, err := NewHandler(
			"xaat-secret",
			"logs",
			WithLevelName(func(level loginjector.LogLevel) string {
				return "severity-" + string(
					rune(
						'0'+level,
					),
				)
			}),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = h.Close() })
		dst, err := h.appendEvent(
			nil,
			newEvent(
				time.Now(),
				4,
				true,
				[]byte("x"),
			),
		)
		require.NoError(t, err)
		var fields map[string]any
		require.NoError(
			t,
			json.Unmarshal(
				dst,
				&fields,
			),
		)
		require.Equal(t, "severity-4", fields["level"])
	})
	t.Run("static fields merged", func(t *testing.T) {
		t.Parallel()
		h, err := NewHandler(
			"xaat-secret",
			"logs",
			WithFields(map[string]any{"service": "api", "enabled": true}),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = h.Close() })
		dst, err := h.appendEvent(
			nil,
			newEvent(
				time.Now(),
				0,
				false,
				[]byte("msg"),
			),
		)
		require.NoError(t, err)
		var fields map[string]any
		require.NoError(
			t,
			json.Unmarshal(
				dst,
				&fields,
			),
		)
		require.Equal(t, "api", fields["service"])
		require.Equal(t, true, fields["enabled"])
	})
	t.Run("invalid UTF-8 is JSON encoded", func(t *testing.T) {
		t.Parallel()
		h, err := NewHandler("xaat-secret", "logs")
		require.NoError(t, err)
		t.Cleanup(func() { _ = h.Close() })
		dst, err := h.appendEvent(
			nil,
			newEvent(
				time.Now(),
				0,
				false,
				[]byte{0xff},
			),
		)
		require.NoError(t, err)
		var fields map[string]any
		require.NoError(
			t,
			json.Unmarshal(
				dst,
				&fields,
			),
		)
		require.Equal(t, "�", fields["message"])
	})
	t.Run("truncated marker", func(t *testing.T) {
		t.Parallel()
		h, err := NewHandler("xaat-secret", "logs")
		require.NoError(t, err)
		t.Cleanup(func() { _ = h.Close() })
		dst, err := h.appendEvent(
			nil,
			newEvent(
				time.Now(),
				0,
				false,
				[]byte(
					strings.Repeat(
						"x",
						maxMessageLen+1,
					),
				),
			),
		)
		require.NoError(t, err)
		var fields map[string]any
		require.NoError(
			t,
			json.Unmarshal(
				dst,
				&fields,
			),
		)
		require.Equal(t, true, fields["message_truncated"])
	})
	t.Run("does not HTML-escape", func(t *testing.T) {
		t.Parallel()
		h, err := NewHandler(
			"xaat-secret",
			"logs",
			WithCompression(false),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = h.Close() })
		line, err := h.appendEvent(
			nil,
			newEvent(
				time.Now(),
				0,
				false,
				[]byte("<tag>&"),
			),
		)
		require.NoError(t, err)
		require.Contains(t, string(line), `"<tag>&"`)
		var got map[string]any
		require.NoError(
			t,
			json.Unmarshal(
				bytes.TrimSpace(line),
				&got,
			),
		)
		require.Equal(t, "<tag>&", got["message"])
	})
}

func BenchmarkAppendEvent(b *testing.B) {
	h, err := NewHandler(
		"xaat-secret",
		"logs",
		withBaseURL("http://127.0.0.1:1"),
		WithFields(map[string]any{"service": "api"}),
	)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := h.Close(); err != nil {
			b.Errorf("close handler: %v", err)
		}
	})

	msg := []byte(
		fmt.Sprintf("%0120s", "event message"),
	)
	ev := newEvent(
		time.Now(),
		4,
		true,
		msg,
	)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := h.appendEvent(nil, ev); err != nil {
			b.Fatal(err)
		}
	}
}
