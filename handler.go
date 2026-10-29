package loginjector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/prorochestvo/loginjector/internal/sink"
)

func TelegramHandler(botToken, chatID, fileName string, labels ...string) io.Writer {
	const telegramAPI = "https://api.telegram.org/bot"
	url := fmt.Sprintf("%s%s/sendDocument", telegramAPI, botToken)
	// redactToken masks botToken in an error string before it is surfaced: a failed
	// request build or client.Do returns a *url.Error whose text embeds the full URL,
	// token included, which would otherwise land in logs and log files.
	redactToken := func(s string) string {
		// guard: an empty or very short token would over-match unrelated substrings, and
		// no real Telegram token is that short, so skip redaction below the threshold.
		if len(botToken) < 8 {
			return s
		}
		return strings.ReplaceAll(s, botToken, "***")
	}
	w := sink.NewFunc(func(msg []byte) (int, error) {
		payload := &bytes.Buffer{}
		parts := multipart.NewWriter(payload)

		if filePart, err := parts.CreateFormFile("document", fileName); err != nil {
			return 0, fmt.Errorf("could not create request form file, details: %w", err)
		} else if _, err = filePart.Write(msg); err != nil {
			return 0, fmt.Errorf("could not write file part to request, details: %w", err)
		}

		if err := parts.WriteField("chat_id", chatID); err != nil {
			return 0, fmt.Errorf("could not write chat_id field to request: %w", err)
		}

		caption := strings.Join(labels, "\n")
		caption = fmt.Sprintf("%s %s", time.Now().UTC().Format("2006-01-02 15:04:05"), caption)
		caption = strings.TrimSpace(caption)
		if err := parts.WriteField("caption", caption); err != nil {
			return 0, fmt.Errorf("could not write caption field to request: %w", err)
		}

		if err := parts.WriteField("parse_mode", "HTML"); err != nil {
			return 0, fmt.Errorf("could not write parse_mode field to request: %w", err)
		}

		contentType := parts.FormDataContentType()

		if err := parts.Close(); err != nil {
			return 0, fmt.Errorf("could not close request: %w", err)
		}

		request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, payload)
		if err != nil {
			return 0, fmt.Errorf("could not create HTTP request: %v", redactToken(err.Error()))
		}
		request.Header.Set("Content-Type", contentType)

		r, err := (&http.Client{Timeout: time.Second * 20}).Do(request)
		if err != nil {
			return 0, fmt.Errorf("could not send HTTP request: %v", redactToken(err.Error()))
		}
		// close error is discarded; logging here would re-enter the logger.
		defer func() { _ = r.Body.Close() }()

		if r.StatusCode != http.StatusOK {
			return 0, fmt.Errorf("could not to deliver message, status code: %v", r.StatusCode)
		}

		var response struct {
			Ok bool `json:"ok"`
		}
		rawResponse := bytes.NewBufferString("")
		err = json.NewDecoder(io.TeeReader(r.Body, rawResponse)).Decode(&response)
		if err != nil || !response.Ok {
			//nolint:errorlint // err may be nil when response.Ok is false; %w would render %!w(<nil>)
			return 0, fmt.Errorf("could not decode response: %v\n%s", err, rawResponse.String())
		}

		return len(msg), nil
	})
	return w
}

// PrintOption configures TimestampedPrintHandler.
type PrintOption func(*printConfig)

// WithTimeLayout overrides the timestamp layout used by TimestampedPrintHandler.
// The default layout is "2006/01/02 15:04:05".
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

// withClock is an unexported test seam that injects a fixed clock into
// TimestampedPrintHandler to make timestamp-sensitive assertions deterministic.
func withClock(fn func() time.Time) PrintOption {
	return func(c *printConfig) { c.clock = fn }
}

// printConfig holds the resolved configuration for TimestampedPrintHandler.
type printConfig struct {
	layout string
	out    io.Writer
	clock  func() time.Time
}

// TimestampedHandler wraps inner so each message is prefixed with a leading
// timestamp before being forwarded to inner, indenting the continuation lines of a
// multi-line message so the body reads as a block. It is the composable form of
// TimestampedPrintHandler: any io.Writer (a file handler, a buffer, another sink)
// can be given sink-side timestamps. The returned writer is mutex-guarded; the
// logger never calls its Write concurrently.
//
// The timestamp layout defaults to "2006/01/02 15:04:05". The first line of a
// message is prefixed with "<timestamp> "; subsequent lines are indented by the
// same width so multi-line payloads read as a visual block. Trailing whitespace and
// newlines are trimmed from the input before formatting; the output always ends
// with exactly one newline. An empty or whitespace-only input produces a single
// "<timestamp>\n" line.
//
// WithTimeLayout and withClock apply; WithOutput is ignored because the sink is the
// explicit inner argument. Use TimestampedPrintHandler when you want the os.Stdout
// instantiation with WithOutput support.
func TimestampedHandler(inner io.Writer, opts ...PrintOption) io.Writer {
	cfg := printConfig{
		layout: sink.DefaultTimeLayout,
		out:    inner,
		clock:  time.Now,
	}
	for _, o := range opts {
		o(&cfg)
	}
	// the sink is the explicit inner argument; WithOutput must not redirect it.
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
// The timestamp layout defaults to "2006/01/02 15:04:05". The first line of a message
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
	for _, o := range opts {
		o(&cfg)
	}
	return sink.NewTimestamped(cfg.out, cfg.layout, cfg.clock)
}

// PrintHandler writes each message to os.Stdout as a plain line, trimming
// surrounding whitespace and appending a single newline. It adds no timestamp; for
// the timestamped console sink (which is the default when NewLogger is given no
// handlers) use TimestampedPrintHandler. The returned writer is mutex-guarded; the
// logger never calls its Write concurrently.
func PrintHandler() io.Writer {
	w := sink.NewFunc(func(msg []byte) (int, error) {
		if _, err := fmt.Fprintln(os.Stdout, string(bytes.TrimSpace(msg))); err != nil {
			return 0, err
		}
		return len(msg), nil
	})
	return w
}

// rotatingFileConfig holds the resolved configuration for RotatingFileHandler.
