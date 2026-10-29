package extensions

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/prorochestvo/loginjector/internal/sink"
)

// PrintOption configures TimestampedPrintHandler.
type PrintOption func(*printConfig)

// printConfig holds the resolved configuration for TimestampedPrintHandler.
type printConfig struct {
	layout string
	out    io.Writer
	clock  func() time.Time
}

// WithTimeLayout overrides the timestamp layout used by TimestampedPrintHandler.
// The default layout is sink.DefaultTimeLayout.
func WithTimeLayout(layout string) PrintOption {
	return func(c *printConfig) { c.layout = layout }
}

// WithOutput overrides the sink used by TimestampedPrintHandler. The default is
// os.Stdout. This is primarily intended to redirect output for testing, but it is a
// legitimate consumer need (e.g. writing timestamped messages to a file instead of
// stdout).
func WithOutput(w io.Writer) PrintOption {
	return func(c *printConfig) { c.out = w }
}

// TimestampedHandler wraps inner so each message is prefixed with a leading
// timestamp before being forwarded to inner, indenting the continuation lines of a
// multi-line message so the body reads as a block. It is the composable form of
// TimestampedPrintHandler: any io.Writer (a file handler, a buffer, another sink)
// can be given sink-side timestamps. The returned writer is mutex-guarded; the
// logger never calls its Write concurrently.
//
// The timestamp layout defaults to sink.DefaultTimeLayout. The first line of a
// message is prefixed with "<timestamp> "; subsequent lines are indented by the
// same width so multi-line payloads read as a visual block. Trailing whitespace and
// newlines are trimmed from the input before formatting; the output always ends
// with exactly one newline. An empty or whitespace-only input produces a single
// "<timestamp>\n" line.
//
// WithTimeLayout applies; WithOutput is ignored because the sink is the explicit
// inner argument. Use TimestampedPrintHandler when you want the os.Stdout
// instantiation with WithOutput support.
func TimestampedHandler(inner io.Writer, opts ...PrintOption) io.Writer {
	cfg := printConfig{
		layout: sink.DefaultTimeLayout,
		out:    inner,
		clock:  time.Now,
	}
	for _, option := range opts {
		option(&cfg)
	}
	// The sink is the explicit inner argument; WithOutput must not redirect it.
	cfg.out = inner
	return sink.NewTimestamped(cfg.out, cfg.layout, cfg.clock)
}

// TimestampedPrintHandler prints each message to stdout with a leading timestamp,
// indenting the continuation lines of a multi-line message so the body reads as a
// block. Unlike PrintHandler, which writes to stdout via a plain writer, this
// handler adds a timestamp; the sink is os.Stdout by default and overridable with
// WithOutput. The returned writer is mutex-guarded; the logger never calls its
// Write concurrently. It is the os.Stdout instantiation of TimestampedHandler.
//
// The timestamp layout defaults to sink.DefaultTimeLayout. The first line of a message
// is prefixed with "<timestamp> "; subsequent lines are indented by the same width so
// multi-line payloads read as a visual block. Trailing whitespace and newlines are
// trimmed from the input before formatting; the output always ends with exactly one
// newline. An empty or whitespace-only input produces a single "<timestamp>\n" line.
func TimestampedPrintHandler(opts ...PrintOption) io.Writer {
	cfg := printConfig{
		layout: sink.DefaultTimeLayout,
		out:    os.Stdout,
		clock:  time.Now,
	}
	for _, option := range opts {
		option(&cfg)
	}
	return sink.NewTimestamped(cfg.out, cfg.layout, cfg.clock)
}

// PrintHandler writes each message to os.Stdout as a plain line, trimming
// surrounding whitespace and appending a single newline. It adds no timestamp; for
// the timestamped console sink (which is the default when NewLogger is given no
// handlers) use TimestampedPrintHandler. The returned writer is mutex-guarded; the
// logger never calls its Write concurrently.
func PrintHandler() io.Writer {
	return sink.NewFunc(func(msg []byte) (int, error) {
		if _, err := fmt.Fprintln(os.Stdout, string(bytes.TrimSpace(msg))); err != nil {
			return 0, err
		}
		return len(msg), nil
	})
}

// withClock injects a fixed clock into timestamped print handlers for deterministic
// timestamp assertions.
func withClock(fn func() time.Time) PrintOption {
	return func(c *printConfig) { c.clock = fn }
}
