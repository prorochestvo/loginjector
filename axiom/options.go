package axiom

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
	"unicode"

	loginjector "github.com/prorochestvo/loginjector"
)

// Option configures an Axiom Handler.
type Option func(*config) error

var errorOutput io.Writer = os.Stderr

// WithDomain sets the bare Axiom edge host. The default is
// "us-east-1.aws.edge.axiom.co".
func WithDomain(domain string) Option {
	return func(cfg *config) error {
		cfg.domain = domain
		cfg.baseURL = ""
		return nil
	}
}

// WithFields sets static fields on every event. Calling this option more than
// once replaces the fields set by the earlier option.
func WithFields(fields map[string]any) Option {
	fragment, err := encodeFields(fields)
	return func(cfg *config) error {
		if err != nil {
			return err
		}
		cfg.fields = fragment
		return nil
	}
}

// WithLevelName sets the name used to encode levels. The default is
// levels.Name, which returns "unknown" outside levels 1 through 6; level_num
// remains present so those events can still be filtered.
func WithLevelName(fn func(loginjector.LogLevel) string) Option {
	return func(cfg *config) error {
		if fn == nil {
			return errors.New("level name function must not be nil")
		}
		cfg.levelName = fn
		return nil
	}
}

// WithBatchSize sets the maximum events in a batch. It must be between 1 and
// 10000. The default is 1000.
func WithBatchSize(n int) Option {
	return func(cfg *config) error {
		if n < 1 || n > 10000 {
			return fmt.Errorf("batch size must be between 1 and 10000: %d", n)
		}
		cfg.batchSize = n
		return nil
	}
}

// WithMaxBatchBytes sets the maximum uncompressed batch size. It must be at
// least 1 MiB. The default is 4 MiB.
func WithMaxBatchBytes(n int) Option {
	return func(cfg *config) error {
		if n < 1<<20 {
			return fmt.Errorf("maximum batch bytes must be at least 1 MiB: %d", n)
		}
		cfg.maxBatchBytes = n
		return nil
	}
}

// WithFlushInterval sets the interval between batch flushes. It must be
// positive. The default is one second.
func WithFlushInterval(d time.Duration) Option {
	return func(cfg *config) error {
		if d <= 0 {
			return fmt.Errorf("flush interval must be positive: %s", d)
		}
		cfg.flushInterval = d
		return nil
	}
}

// WithQueueSize sets the maximum queued event count. It must be between 1 and
// 1048576. The default is 10000.
func WithQueueSize(n int) Option {
	return func(cfg *config) error {
		if n <= 0 || n > 1<<20 {
			return fmt.Errorf("queue size must be between 1 and 1048576: %d", n)
		}
		cfg.queueSize = n
		return nil
	}
}

// WithMaxQueueBytes sets the maximum bytes held by queued events. It must be
// positive, and the final value must be at least WithMaxBatchBytes. The default
// is 32 MiB.
func WithMaxQueueBytes(n int) Option {
	return func(cfg *config) error {
		if n <= 0 {
			return fmt.Errorf("maximum queue bytes must be positive: %d", n)
		}
		cfg.maxQueueBytes = n
		return nil
	}
}

// WithHTTPClient sets the HTTP client used for delivery. The supplied client is
// not owned by the Handler. By default, the Handler creates a client with a
// ten-second timeout.
func WithHTTPClient(c *http.Client) Option {
	return func(cfg *config) error {
		if c == nil {
			return errors.New("HTTP client must not be nil")
		}
		cfg.httpClient = c
		cfg.ownsClient = false
		return nil
	}
}

// WithMaxRetries sets the number of retries after the initial delivery
// attempt. Zero disables retries. The default is three.
func WithMaxRetries(n int) Option {
	return func(cfg *config) error {
		if n < 0 {
			return fmt.Errorf("maximum retries must not be negative: %d", n)
		}
		cfg.maxRetries = n
		return nil
	}
}

// WithCompression enables or disables gzip compression. It is enabled by
// default.
func WithCompression(enabled bool) Option {
	return func(cfg *config) error {
		cfg.compression = enabled
		return nil
	}
}

// WithErrorHandler sets the function that receives delivery errors. The
// default writes one line to os.Stderr. Calls are rate-limited by the Handler.
// The function may log through a Logger containing this Handler, but must not
// call Flush, Shutdown, or Close.
func WithErrorHandler(fn func(error)) Option {
	return func(cfg *config) error {
		if fn == nil {
			return errors.New("error handler must not be nil")
		}
		cfg.errorHandler = fn
		return nil
	}
}

type config struct {
	token         string
	dataset       string
	domain        string
	baseURL       string
	ingestURL     string
	fields        []byte
	levelName     func(loginjector.LogLevel) string
	batchSize     int
	maxBatchBytes int
	flushInterval time.Duration
	queueSize     int
	maxQueueBytes int
	httpClient    *http.Client
	ownsClient    bool
	maxRetries    int
	compression   bool
	errorHandler  func(error)
	now           func() time.Time
	wait          func(context.Context, time.Duration) bool
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

func encodeFields(fields map[string]any) ([]byte, error) {
	const maxFragmentBytes = 64 << 10
	if len(fields) == 0 {
		return nil, nil
	}
	reserved := map[string]struct{}{
		"_time": {}, "level": {}, "level_num": {}, "message": {}, "message_truncated": {},
	}
	for key := range fields {
		if key == "" || len(key) > 200 || strings.HasPrefix(key, "_") {
			return nil, fmt.Errorf("invalid static field name %q", key)
		}
		if _, ok := reserved[key]; ok {
			return nil, fmt.Errorf("reserved static field name %q", key)
		}
	}
	encoded, err := json.Marshal(fields)
	if err != nil {
		return nil, fmt.Errorf("encode static fields: %w", err)
	}
	if len(encoded)-2 > maxFragmentBytes {
		return nil, errors.New("static fields exceed 64 KiB")
	}
	return encoded[1 : len(encoded)-1], nil
}

func validDomain(domain string) error {
	if domain == "" || strings.Contains(domain, "://") || strings.ContainsAny(domain, "/?#@") {
		return errors.New("domain must be a bare host")
	}
	for _, r := range domain {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return errors.New("domain must be a bare host")
		}
	}
	parsed, err := url.Parse("https://" + domain)
	if err != nil || parsed.Host != domain || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.User != nil {
		return errors.New("domain must be a bare host")
	}
	return nil
}

func defaultErrorHandler(err error) {
	message := strings.NewReplacer("\r", `\r`, "\n", `\n`).Replace(err.Error())
	_, _ = fmt.Fprintln(errorOutput, message)
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
