package axiom

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	loginjector "github.com/prorochestvo/loginjector"
	"github.com/prorochestvo/loginjector/levels"
	"github.com/stretchr/testify/require"
)

func Test_newEvent(t *testing.T) {
	t.Parallel()
	t.Run("trims one newline", func(t *testing.T) {
		t.Parallel()
		ev := newEvent(time.Date(2026, 10, 22, 3, 4, 5, 6, time.FixedZone("west", -3600)), 1, true, []byte("line\n"))
		require.Equal(t, "line", ev.message)
		require.Equal(t, time.UTC, ev.time.Location())
		require.Equal(t, 4, ev.time.Hour())
	})
	t.Run("trims CRLF", func(t *testing.T) {
		t.Parallel()
		ev := newEvent(time.Now(), 0, false, []byte("line\r\n"))
		require.Equal(t, "line", ev.message)
	})
	t.Run("trims multiple trailing line breaks", func(t *testing.T) {
		t.Parallel()
		ev := newEvent(time.Now(), 0, false, []byte("line\r\n\n\r"))
		require.Equal(t, "line", ev.message)
	})
	t.Run("truncates on UTF-8 boundary", func(t *testing.T) {
		t.Parallel()
		input := strings.Repeat("界", maxMessageLen/3+1)
		ev := newEvent(time.Now(), 0, false, []byte(input))
		require.True(t, ev.truncated)
		require.LessOrEqual(t, len(ev.message), maxMessageLen)
		require.True(t, utf8.ValidString(ev.message))
	})
	t.Run("does not truncate short message", func(t *testing.T) {
		t.Parallel()
		ev := newEvent(time.Now(), 0, false, []byte("short"))
		require.False(t, ev.truncated)
	})
	t.Run("copies caller bytes", func(t *testing.T) {
		t.Parallel()
		input := []byte("before")
		ev := newEvent(time.Now(), 0, false, input)
		copy(input, "after!")
		require.Equal(t, "before", ev.message)
	})
}

func TestHandler_appendEvent(t *testing.T) {
	t.Parallel()
	t.Run("level present including zero", func(t *testing.T) {
		t.Parallel()
		h, err := NewHandler("xaat-secret", "logs")
		require.NoError(t, err)
		dst, err := h.appendEvent(nil, newEvent(time.Date(2026, 10, 22, 1, 2, 3, 4, time.UTC), 0, true, []byte("hello")))
		require.NoError(t, err)
		var fields map[string]any
		require.NoError(t, json.Unmarshal(dst, &fields))
		require.Equal(t, "unknown", fields["level"])
		require.InDelta(t, 0, fields["level_num"], 0)
		require.Equal(t, "hello", fields["message"])
		require.Equal(t, "2026-10-22T01:02:03.000000004Z", fields["_time"])
		require.Equal(t, "\n", string(dst[len(dst)-1]))
	})
	t.Run("level absent", func(t *testing.T) {
		t.Parallel()
		h, err := NewHandler("xaat-secret", "logs")
		require.NoError(t, err)
		dst, err := h.appendEvent(nil, newEvent(time.Now(), levels.Warning, false, []byte("plain")))
		require.NoError(t, err)
		var fields map[string]any
		require.NoError(t, json.Unmarshal(dst, &fields))
		require.NotContains(t, fields, "level")
		require.NotContains(t, fields, "level_num")
	})
	t.Run("default level name", func(t *testing.T) {
		t.Parallel()
		h, err := NewHandler("xaat-secret", "logs")
		require.NoError(t, err)
		dst, err := h.appendEvent(nil, newEvent(time.Now(), levels.Warning, true, []byte("warning")))
		require.NoError(t, err)
		var fields map[string]any
		require.NoError(t, json.Unmarshal(dst, &fields))
		require.Equal(t, "warning", fields["level"])
	})
	t.Run("custom level name", func(t *testing.T) {
		t.Parallel()
		h, err := NewHandler("xaat-secret", "logs", WithLevelName(func(level loginjector.LogLevel) string {
			return "severity-" + string(rune('0'+level))
		}))
		require.NoError(t, err)
		dst, err := h.appendEvent(nil, newEvent(time.Now(), 4, true, []byte("x")))
		require.NoError(t, err)
		var fields map[string]any
		require.NoError(t, json.Unmarshal(dst, &fields))
		require.Equal(t, "severity-4", fields["level"])
	})
	t.Run("static fields merged", func(t *testing.T) {
		t.Parallel()
		h, err := NewHandler("xaat-secret", "logs", WithFields(map[string]any{"service": "api", "enabled": true}))
		require.NoError(t, err)
		dst, err := h.appendEvent(nil, newEvent(time.Now(), 0, false, []byte("msg")))
		require.NoError(t, err)
		var fields map[string]any
		require.NoError(t, json.Unmarshal(dst, &fields))
		require.Equal(t, "api", fields["service"])
		require.Equal(t, true, fields["enabled"])
	})
	t.Run("invalid UTF-8 is JSON encoded", func(t *testing.T) {
		t.Parallel()
		h, err := NewHandler("xaat-secret", "logs")
		require.NoError(t, err)
		dst, err := h.appendEvent(nil, newEvent(time.Now(), 0, false, []byte{0xff}))
		require.NoError(t, err)
		var fields map[string]any
		require.NoError(t, json.Unmarshal(dst, &fields))
		require.Equal(t, "�", fields["message"])
	})
	t.Run("truncated marker", func(t *testing.T) {
		t.Parallel()
		h, err := NewHandler("xaat-secret", "logs")
		require.NoError(t, err)
		dst, err := h.appendEvent(nil, newEvent(time.Now(), 0, false, []byte(strings.Repeat("x", maxMessageLen+1))))
		require.NoError(t, err)
		var fields map[string]any
		require.NoError(t, json.Unmarshal(dst, &fields))
		require.Equal(t, true, fields["message_truncated"])
	})
}
