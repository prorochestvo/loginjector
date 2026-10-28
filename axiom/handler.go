package axiom

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/prorochestvo/loginjector"
	"github.com/prorochestvo/loginjector/levels"
)

// Handler asynchronously batches events for delivery to Axiom. The zero value
// is unusable; construct a Handler with NewHandler.
type Handler struct {
	cfg          *config
	mu           sync.RWMutex
	closed       bool
	queue        chan queuedEvent
	wake         chan struct{}
	done         chan struct{}
	shutdownOnce sync.Once
	cancel       context.CancelFunc
	pendingBytes atomic.Int64
	admitted     atomic.Uint64
	completed    atomic.Uint64
	progress     chan struct{}
	progressMu   sync.Mutex
	compressBuf  bytes.Buffer
	gzipWriter   *gzip.Writer
	sent         atomic.Uint64
	dropped      atomic.Uint64
	failed       atomic.Uint64
	errorMu      sync.Mutex
	lastError    time.Time
	suppressed   uint64
	dropReported uint64
}

// NewHandler validates the token, dataset, and options and starts a delivery worker.
// The token must be an Axiom API token with ingest permission, and the dataset is
// the name of the destination dataset.
func NewHandler(token Token, dataset Dataset, opts ...Option) (*Handler, error) {
	tokenString := string(token)
	datasetString := string(dataset)
	if tokenString == "" {
		return nil, errors.New("axiom: token must not be empty")
	}
	if strings.HasPrefix(tokenString, "xapt-") {
		return nil, errors.New("axiom: an xaat- API token with ingest permission is required; personal tokens are not supported")
	}
	for _, r := range tokenString {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return nil, errors.New("axiom: token contains whitespace or a control character")
		}
	}
	if datasetString == "" {
		return nil, errors.New("axiom: dataset must not be empty")
	}
	if strings.Contains(datasetString, "/") {
		return nil, errors.New("axiom: dataset must not contain a slash")
	}

	transport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		transport = &http.Transport{}
	}
	cfg := config{
		token:         tokenString,
		dataset:       datasetString,
		domain:        defaultDomain,
		levelName:     levels.Name,
		batchSize:     defaultBatchSize,
		maxBatchBytes: defaultMaxBatchBytes,
		flushInterval: time.Second,
		queueSize:     defaultQueueSize,
		maxQueueBytes: defaultMaxQueueBytes,
		httpClient: &http.Client{
			Timeout:       defaultHTTPTimeout,
			Transport:     transport.Clone(),
			CheckRedirect: checkRedirect,
		},
		ownsClient:  true,
		maxRetries:  defaultMaxRetries,
		compression: true,
		errorOutput: os.Stderr,
		now:         time.Now,
		wait:        waitRetry,
	}
	cfg.errorHandler = func(err error) { defaultErrorHandler(cfg.errorOutput, err) }
	for i, opt := range opts {
		if opt == nil {
			return nil, fmt.Errorf(
				"axiom: apply option %d: option must not be nil",
				i+1,
			)
		}
		if err := opt(
			&cfg,
		); err != nil {
			return nil, fmt.Errorf(
				"axiom: apply option %d: %w",
				i+1,
				err,
			)
		}
	}
	if cfg.baseURL == "" {
		if err := validateDomain(cfg.domain); err != nil {
			return nil, fmt.Errorf("axiom: %w", err)
		}
		cfg.baseURL = "https://" + cfg.domain
	}
	if cfg.maxQueueBytes < cfg.maxBatchBytes {
		return nil, errors.New("axiom: maximum queue bytes must be at least maximum batch bytes")
	}
	cfg.ingestURL = strings.TrimSuffix(cfg.baseURL, "/") + "/v1/ingest/" + url.PathEscape(datasetString)
	workerCtx, cancel := context.WithCancel(
		context.Background(),
	)
	h := &Handler{cfg: &cfg, queue: make(chan queuedEvent, cfg.queueSize), wake: make(chan struct{}, 1), done: make(chan struct{}), progress: make(chan struct{}), cancel: cancel}
	if cfg.compression {
		h.gzipWriter = gzip.NewWriter(
			&h.compressBuf,
		)
	}
	go h.run(workerCtx)
	return h, nil
}

// Format keeps every fmt verb from exposing private configuration fields.
func (h *Handler) Format(state fmt.State, _ rune) { _, _ = io.WriteString(state, "Axiom Handler") }

// Write queues an unlevelled log event.
func (h *Handler) Write(p []byte) (int, error) { return h.admit(0, false, p) }

// WriteLevel queues a log event with its severity.
func (h *Handler) WriteLevel(level loginjector.LogLevel, p []byte) (int, error) {
	return h.admit(level, true, p)
}

// Flush sends events admitted before the call and waits for their completion.
func (h *Handler) Flush(ctx context.Context) error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return ErrClosed
	}
	target := h.admitted.Load()
	h.mu.Unlock()
	select {
	case h.wake <- struct{}{}:
	default:
	}
	for h.completed.Load() < target {
		h.progressMu.Lock()
		progress := h.progress
		h.progressMu.Unlock()
		if h.completed.Load() >= target {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-progress:
		case <-h.done:
			if h.completed.Load() < target {
				return ErrClosed
			}
		}
	}
	return nil
}

// Shutdown stops admission, drains queued events, and waits for the worker.
func (h *Handler) Shutdown(ctx context.Context) error {
	h.shutdownOnce.Do(func() {
		h.mu.Lock()
		h.closed = true
		h.mu.Unlock()
		select {
		case h.wake <- struct{}{}:
		default:
		}
	})
	select {
	case <-h.done:
		if h.cfg.ownsClient {
			h.cfg.httpClient.CloseIdleConnections()
		}
		return nil
	case <-ctx.Done():
		select {
		case <-h.done:
			if h.cfg.ownsClient {
				h.cfg.httpClient.CloseIdleConnections()
			}
			return nil
		default:
		}
		h.cancel()
		<-h.done
		if h.cfg.ownsClient {
			h.cfg.httpClient.CloseIdleConnections()
		}
		return ctx.Err()
	}
}

// Close shuts down the handler with a five second deadline.
func (h *Handler) Close() error {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()
	return h.Shutdown(ctx)
}

// Stats returns the current event counters.
func (h *Handler) Stats() Stats {
	return Stats{Sent: h.sent.Load(), Dropped: h.dropped.Load(), Failed: h.failed.Load()}
}

func (h *Handler) admit(level loginjector.LogLevel, hasLevel bool, p []byte) (int, error) {
	msg := newEvent(
		h.cfg.now(),
		level,
		hasLevel,
		p,
	)
	encoded, err := h.appendEvent(nil, msg)
	if err != nil {
		return 0, err
	}
	cost := int64(
		len(encoded),
	)
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.closed || h.pendingBytes.Add(cost) > int64(h.cfg.maxQueueBytes) {
		if !h.closed {
			h.pendingBytes.Add(
				-cost,
			)
		}
		h.dropped.Add(1)
		return len(p), nil
	}
	select {
	case h.queue <- queuedEvent{line: encoded, cost: cost}:
		h.admitted.Add(1)
	default:
		h.pendingBytes.Add(
			-cost,
		)
		h.dropped.Add(1)
	}
	return len(p), nil
}

// appendEvent appends one NDJSON event to dst.
func (h *Handler) appendEvent(dst []byte, ev event) ([]byte, error) {
	dst = slices.Grow(
		dst,
		len(ev.message)+len(h.cfg.fields)+128,
	)
	dst = append(dst, '{')
	dst = append(dst, `"_time":`...)
	dst = append(dst, '"')
	dst = ev.time.UTC().AppendFormat(dst, time.RFC3339Nano)
	dst = append(dst, '"')
	if ev.hasLevel {
		dst = append(dst, `,"level":`...)
		var marshalErr error
		dst, marshalErr = appendJSONString(
			dst,
			h.cfg.levelName(ev.level),
		)
		if marshalErr != nil {
			return nil, fmt.Errorf("encode level name: %w", marshalErr)
		}
		dst = append(dst, `,"level_num":`...)
		dst = strconv.AppendInt(
			dst,
			int64(ev.level),
			10,
		)
	}
	dst = append(dst, `,"message":`...)
	var err error
	dst, err = appendJSONString(dst, ev.message)
	if err != nil {
		return nil, fmt.Errorf("encode event message: %w", err)
	}
	if ev.truncated {
		dst = append(dst, `,"message_truncated":true`...)
	}
	if len(h.cfg.fields) != 0 {
		dst = append(dst, ',')
		dst = append(dst, h.cfg.fields...)
	}
	dst = append(dst, '}', '\n')
	return dst, nil
}

// Token is an Axiom API token with ingest permission.
type Token string

// Dataset is the name of an Axiom dataset.
type Dataset string

// Stats reports event counts, not batch counts.
type Stats struct {
	Sent    uint64 // Events successfully delivered.
	Dropped uint64 // Events refused at admission.
	Failed  uint64 // Events whose delivery failed.
}

const (
	defaultDomain        = "us-east-1.aws.edge.axiom.co"
	defaultBatchSize     = 1000
	defaultMaxBatchBytes = 4 << 20
	defaultQueueSize     = 10000
	defaultMaxQueueBytes = 32 << 20
	defaultMaxRetries    = 3
	defaultHTTPTimeout   = 10 * time.Second
	maxBatchSize         = 10000
	minBatchBytes        = 1 << 20
	maxQueueSize         = 1 << 20
	maxMessageLen        = 512 << 10
	userAgent            = "loginjector-axiom"
)

// ErrClosed is returned by Flush after shutdown has started.
var ErrClosed = errors.New("axiom: handler is closed")

var stringJSONPool = sync.Pool{New: func() any {
	return newStringJSONBuffer()
}}

type stringJSONBuffer struct {
	buf bytes.Buffer
	enc *json.Encoder
}

type queuedEvent struct {
	line []byte
	cost int64
}

type event struct {
	time      time.Time
	level     loginjector.LogLevel
	hasLevel  bool
	message   string
	truncated bool
}

func newStringJSONBuffer() *stringJSONBuffer {
	item := &stringJSONBuffer{}
	item.enc = json.NewEncoder(
		&item.buf,
	)
	item.enc.SetEscapeHTML(false)
	return item
}

func checkRedirect(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

func appendJSONString(dst []byte, value string) ([]byte, error) {
	item, ok := stringJSONPool.Get().(*stringJSONBuffer)
	if !ok {
		item = newStringJSONBuffer()
	}
	item.buf.Reset()
	err := item.enc.Encode(value)
	if err == nil {
		encoded := item.buf.Bytes()
		dst = append(dst, encoded[:len(encoded)-1]...)
	}
	stringJSONPool.Put(item)
	return dst, err
}

func newEvent(now time.Time, level loginjector.LogLevel, hasLevel bool, p []byte) event {
	// trim and truncate on the caller's bytes before converting, so the event owns a
	// copy of exactly the kept bytes and never pins a larger backing array.
	trimmed := bytes.TrimRight(p, "\r\n")
	truncated := false
	normalized, sourceEnd := 0, 0
	for sourceEnd < len(trimmed) {
		_, size := utf8.DecodeRune(trimmed[sourceEnd:])
		cost := size
		if size == 1 && trimmed[sourceEnd] >= utf8.RuneSelf {
			cost = 3
		}
		if normalized+cost > maxMessageLen {
			break
		}
		normalized += cost
		sourceEnd += size
	}
	if sourceEnd < len(trimmed) {
		trimmed = trimmed[:sourceEnd]
		truncated = true
	}
	return event{
		time:      now.UTC(),
		level:     level,
		hasLevel:  hasLevel,
		message:   string(trimmed),
		truncated: truncated,
	}
}
