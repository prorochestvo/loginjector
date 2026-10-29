package loginjector

import (
	"bytes"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTelegramHandler(t *testing.T) {
	t.Parallel()
	t.Run("delivers to the live API", func(t *testing.T) {
		t.Parallel()
		botToken := os.Getenv("TELEGRAM_BOT_TOKEN")
		chatID := os.Getenv("TELEGRAM_BOT_CHAT_ID")

		if botToken == "" || chatID == "" {
			t.Skipf("TELEGRAM_BOT_TOKEN or TELEGRAM_BOT_CHAT_ID not set")
		}

		m := time.Now().UTC().Format(time.RFC3339) + ": 14C225CB-9BE0-40D8-8FB3-6218FE17AE53"

		h := TelegramHandler(botToken, chatID, "test.log", "LogInjector", "<b>demo</b> of telegram handler")
		_, err := h.Write([]byte(m))
		require.NoError(t, err)
	})

	t.Run("redacts the bot token from a surfaced request error", func(t *testing.T) {
		t.Parallel()

		// "%zz" is an invalid URL escape, so http.NewRequest fails deterministically and
		// fast without any network — and net/url echoes the offending URL (token included)
		// verbatim into the error string, exercising the redactToken closure on the
		// request-build return site. The token is >= 8 chars so the guard does not skip it.
		botToken := "1234567890ABCDEF%zz"

		h := TelegramHandler(botToken, "chat", "test.log", "label")
		_, err := h.Write([]byte("payload"))

		require.Error(t, err)
		require.NotContains(t, err.Error(), botToken,
			"the bot token must never appear in the surfaced error")
		require.Contains(t, err.Error(), "***",
			"the redaction placeholder must replace the token")
		require.Contains(t, err.Error(), "bot***/sendDocument",
			"the literal bot prefix stays; only the secret is masked")
	})

	t.Run("short token is left unredacted by the guard", func(t *testing.T) {
		t.Parallel()

		// a token below the 8-char guard is not redacted (it is not a valid Telegram token
		// and would over-match unrelated substrings). "%zz" still forces the build error.
		botToken := "%zz"

		h := TelegramHandler(botToken, "chat", "test.log", "label")
		_, err := h.Write([]byte("payload"))

		require.Error(t, err)
		require.NotContains(t, err.Error(), "***",
			"a sub-8-char token must not be redacted")
	})
}

var _ io.Writer = (*errWriter)(nil)

type errWriter struct{ err error }

func (e *errWriter) Write(_ []byte) (int, error) { return 0, e.err }

func TestTimestampedPrintHandler(t *testing.T) {
	t.Parallel()

	fixedTime := time.Date(2024, 3, 15, 10, 30, 45, 0, time.UTC)
	fixedClock := withClock(func() time.Time { return fixedTime })
	tsPrefix := "2024/03/15 10:30:45"
	// indent is the width of the timestamp prefix plus trailing space
	indent := strings.Repeat(" ", len(tsPrefix)+1)

	t.Run("single line", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		h := TimestampedPrintHandler(fixedClock, WithOutput(&buf))
		_, err := h.Write([]byte("hello"))
		require.NoError(t, err)
		require.Equal(t, tsPrefix+" hello\n", buf.String())
	})

	t.Run("multiline indent", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		h := TimestampedPrintHandler(fixedClock, WithOutput(&buf))
		_, err := h.Write([]byte("line1\nline2\nline3"))
		require.NoError(t, err)
		want := tsPrefix + " line1\n" + indent + "line2\n" + indent + "line3\n"
		require.Equal(t, want, buf.String())
	})

	t.Run("trailing whitespace trimmed equivalence", func(t *testing.T) {
		t.Parallel()

		var buf1, buf2 bytes.Buffer
		h1 := TimestampedPrintHandler(fixedClock, WithOutput(&buf1))
		h2 := TimestampedPrintHandler(fixedClock, WithOutput(&buf2))
		_, err := h1.Write([]byte("hi\n"))
		require.NoError(t, err)
		_, err = h2.Write([]byte("hi"))
		require.NoError(t, err)
		require.Equal(t, buf1.String(), buf2.String())
	})

	t.Run("internal blank line preserved", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		h := TimestampedPrintHandler(fixedClock, WithOutput(&buf))
		_, err := h.Write([]byte("a\n\nb"))
		require.NoError(t, err)
		want := tsPrefix + " a\n" + indent + "\n" + indent + "b\n"
		require.Equal(t, want, buf.String())
	})

	t.Run("custom WithTimeLayout", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		h := TimestampedPrintHandler(fixedClock, WithOutput(&buf), WithTimeLayout("15:04:05"))
		_, err := h.Write([]byte("msg"))
		require.NoError(t, err)
		require.True(t, strings.HasPrefix(buf.String(), "10:30:45 msg"))
	})

	t.Run("sink error surfaced", func(t *testing.T) {
		t.Parallel()

		want := errors.New("disk full")
		h := TimestampedPrintHandler(fixedClock, WithOutput(&errWriter{err: want}))
		_, err := h.Write([]byte("boom"))
		require.ErrorIs(t, err, want)
	})

	t.Run("empty input", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		h := TimestampedPrintHandler(fixedClock, WithOutput(&buf))
		_, err := h.Write([]byte(""))
		require.NoError(t, err)
		require.Equal(t, tsPrefix+"\n", buf.String())
	})

	t.Run("whitespace only input", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		h := TimestampedPrintHandler(fixedClock, WithOutput(&buf))
		_, err := h.Write([]byte("   \n  "))
		require.NoError(t, err)
		require.Equal(t, tsPrefix+"\n", buf.String())
	})
}

func TestTimestampedHandler(t *testing.T) {
	t.Parallel()

	fixedTime := time.Date(2024, 3, 15, 10, 30, 45, 0, time.UTC)
	fixedClock := withClock(func() time.Time { return fixedTime })
	tsPrefix := "2024/03/15 10:30:45"
	indent := strings.Repeat(" ", len(tsPrefix)+1)

	t.Run("single line forwarded to inner", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		h := TimestampedHandler(&buf, fixedClock)
		_, err := h.Write([]byte("hello"))
		require.NoError(t, err)
		require.Equal(t, tsPrefix+" hello\n", buf.String())
	})

	t.Run("multiline indents continuation lines", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		h := TimestampedHandler(&buf, fixedClock)
		_, err := h.Write([]byte("line1\nline2\nline3"))
		require.NoError(t, err)
		want := tsPrefix + " line1\n" + indent + "line2\n" + indent + "line3\n"
		require.Equal(t, want, buf.String())
	})

	t.Run("byte-for-byte match with TimestampedPrintHandler", func(t *testing.T) {
		t.Parallel()

		// the decorator over a buffer must render identically to the stdout printer
		// pointed at the same buffer via WithOutput — only the sink differs.
		for _, in := range []string{"hello", "a\nb\nc", "a\n\nb", "", "   \n  ", "trailing\n"} {
			var decorated, printer bytes.Buffer
			hDec := TimestampedHandler(&decorated, fixedClock)
			hPrint := TimestampedPrintHandler(fixedClock, WithOutput(&printer))
			_, err := hDec.Write([]byte(in))
			require.NoError(t, err)
			_, err = hPrint.Write([]byte(in))
			require.NoError(t, err)
			require.Equal(t, printer.String(), decorated.String(), "input %q must render identically", in)
		}
	})

	t.Run("WithOutput is ignored; inner is the sink", func(t *testing.T) {
		t.Parallel()

		var inner, redirect bytes.Buffer
		h := TimestampedHandler(&inner, fixedClock, WithOutput(&redirect))
		_, err := h.Write([]byte("payload"))
		require.NoError(t, err)
		require.Equal(t, tsPrefix+" payload\n", inner.String(), "output must go to inner, not WithOutput")
		require.Empty(t, redirect.String(), "WithOutput must be ignored by TimestampedHandler")
	})

	t.Run("sink error surfaced", func(t *testing.T) {
		t.Parallel()

		want := errors.New("disk full")
		h := TimestampedHandler(&errWriter{err: want}, fixedClock)
		_, err := h.Write([]byte("boom"))
		require.ErrorIs(t, err, want)
	})
}

//nolint:paralleltest // captureStdout redirects the process-global os.Stdout
func TestPrintHandler(t *testing.T) {
	// serial: captureStdout redirects the process-global os.Stdout.
	m := uniqueToken()
	out := captureStdout(t, func() {
		h := PrintHandler()
		n, err := h.Write([]byte("  " + m + "  "))
		require.NoError(t, err)
		require.Equal(t, len("  "+m+"  "), n, "Write must report the original message length")
	})
	require.Equal(t, m+"\n", out, "PrintHandler must write a trimmed line to stdout")
}
