package extensions

import (
	"bytes"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prorochestvo/loginjector"
	"github.com/stretchr/testify/require"
)

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
	t.Run("no doubled timestamp with Logger.StdLog", func(t *testing.T) {
		t.Parallel()

		var output bytes.Buffer
		inner := TimestampedHandler(&output, fixedClock)
		logger := loginjector.NewLogger(1, inner)
		logger.StdLog(1, "sqlite ").Printf("query ran")

		want := "2024/03/15 10:30:45 sqlite query ran\n"
		require.Equal(t, want, output.String())
		require.Equal(t, 1, strings.Count(output.String(), "2024/03/15 10:30:45"))
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

// captureStdout redirects os.Stdout for a single test callback.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	original := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stdout = writer
	defer func() { os.Stdout = original }()

	done := make(chan string, 1)
	go func() {
		data, _ := io.ReadAll(reader)
		done <- string(data)
	}()

	fn()
	_ = writer.Close()
	output := <-done
	_ = reader.Close()
	return output
}
