package sink

import (
	"bytes"
	"io"
	"strings"
	"time"
)

// DefaultTimeLayout is the default timestamp layout for rendered log messages.
const DefaultTimeLayout = "2006/01/02 15:04:05"

// NewTimestamped returns a guarded writer that prefixes each message with a timestamp.
func NewTimestamped(out io.Writer, layout string, clock func() time.Time) *Func {
	// precompute the continuation indent once — the rendered width of a fixed-width
	// layout is constant, so formatting a reference time gives the exact width.
	const sep = " "
	indent := strings.Repeat(" ", len(time.Time{}.Format(layout))+len(sep))

	return NewFunc(func(msg []byte) (int, error) {
		ts := clock().Format(layout)

		trimmed := bytes.TrimSpace(msg)

		var b strings.Builder

		// fast-path: single-line message (no '\n' in trimmed body).
		if bytes.IndexByte(trimmed, '\n') < 0 {
			b.WriteString(ts)
			if len(trimmed) > 0 {
				b.WriteString(sep)
				b.Write(trimmed)
			}
			b.WriteByte('\n')
		} else {
			body := string(trimmed)
			lines := strings.Split(body, "\n")
			b.WriteString(ts)
			if lines[0] != "" {
				b.WriteString(sep)
				b.WriteString(lines[0])
			}
			b.WriteByte('\n')
			for _, ln := range lines[1:] {
				b.WriteString(indent)
				b.WriteString(ln)
				b.WriteByte('\n')
			}
		}

		if _, err := io.WriteString(out, b.String()); err != nil {
			return 0, err
		}
		return len(msg), nil
	})
}
