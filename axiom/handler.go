package axiom

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	loginjector "github.com/prorochestvo/loginjector"
	"github.com/prorochestvo/loginjector/levels"
)

// ErrClosed is returned by Flush after shutdown has started.
var ErrClosed = errors.New("axiom: handler is closed")

type stringJSONBuffer struct {
	buf bytes.Buffer
	enc *json.Encoder
}

var stringJSONPool = sync.Pool{New: func() any {
	item := &stringJSONBuffer{}
	item.enc = json.NewEncoder(&item.buf)
	item.enc.SetEscapeHTML(false)
	return item
}}

// Stats reports event counts, not batch counts.
type Stats struct {
	Sent    uint64
	Dropped uint64
	Failed  uint64
}

type queuedEvent struct {
	line []byte
	cost int64
}

// Handler asynchronously batches events for delivery to Axiom.
type Handler struct {
	cfg          config
	mu           sync.RWMutex
	closed       bool
	queue        chan queuedEvent
	wake         chan struct{}
	done         chan struct{}
	shutdownOnce sync.Once
	shutdownErr  error
	cancel       context.CancelFunc
	inFlight     atomic.Int64
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
func NewHandler(token, dataset string, opts ...Option) (*Handler, error) {
	if token == "" {
		return nil, errors.New("axiom: token must not be empty")
	}
	if strings.HasPrefix(token, "xapt-") {
		return nil, errors.New("axiom: an xaat- API token with ingest permission is required; personal tokens are not supported")
	}
	for _, r := range token {
		if unicode.IsSpace(r) || unicode.IsControl(r) || r < 0x20 || r == 0x7f {
			return nil, errors.New("axiom: token contains whitespace or a control character")
		}
	}
	if dataset == "" {
		return nil, errors.New("axiom: dataset must not be empty")
	}
	if strings.Contains(dataset, "/") {
		return nil, errors.New("axiom: dataset must not contain a slash")
	}

	transport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		transport = &http.Transport{}
	}
	cfg := config{
		token:         token,
		dataset:       dataset,
		domain:        defaultDomain,
		levelName:     levels.Name,
		batchSize:     1000,
		maxBatchBytes: 4 << 20,
		flushInterval: time.Second,
		queueSize:     10000,
		maxQueueBytes: 32 << 20,
		httpClient:    &http.Client{Timeout: 10 * time.Second, Transport: transport.Clone(), CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		ownsClient:    true,
		maxRetries:    3,
		compression:   true,
		errorHandler:  defaultErrorHandler,
		now:           time.Now,
		wait:          waitRetry,
	}
	for i, opt := range opts {
		if opt == nil {
			return nil, fmt.Errorf("axiom: apply option %d: option must not be nil", i+1)
		}
		if err := opt(&cfg); err != nil {
			return nil, fmt.Errorf("axiom: apply option %d: %w", i+1, err)
		}
	}
	if cfg.baseURL == "" {
		if err := validDomain(cfg.domain); err != nil {
			return nil, fmt.Errorf("axiom: %w", err)
		}
		cfg.baseURL = "https://" + cfg.domain
	}
	if cfg.maxQueueBytes < cfg.maxBatchBytes {
		return nil, errors.New("axiom: maximum queue bytes must be at least maximum batch bytes")
	}
	cfg.ingestURL = strings.TrimSuffix(cfg.baseURL, "/") + "/v1/ingest/" + url.PathEscape(dataset)
	workerCtx, cancel := context.WithCancel(context.Background())
	h := &Handler{cfg: cfg, queue: make(chan queuedEvent, cfg.queueSize), wake: make(chan struct{}, 1), done: make(chan struct{}), progress: make(chan struct{}), cancel: cancel}
	if cfg.compression {
		h.gzipWriter = gzip.NewWriter(&h.compressBuf)
	}
	go h.run(workerCtx)
	return h, nil
}

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
		select {
		case <-h.done:
		case <-ctx.Done():
			select {
			case <-h.done:
				h.shutdownErr = nil
			default:
				h.cancel()
				<-h.done
				h.shutdownErr = ctx.Err()
			}
		}
		if h.cfg.ownsClient {
			h.cfg.httpClient.CloseIdleConnections()
		}
	})
	return h.shutdownErr
}

// Close shuts down the handler with a five second deadline.
func (h *Handler) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return h.Shutdown(ctx)
}

// Stats returns the current event counters.
func (h *Handler) Stats() Stats {
	return Stats{Sent: h.sent.Load(), Dropped: h.dropped.Load(), Failed: h.failed.Load()}
}

func (h *Handler) admit(level loginjector.LogLevel, hasLevel bool, p []byte) (int, error) {
	msg := newEvent(h.cfg.now(), level, hasLevel, p)
	encoded, err := h.appendEvent(nil, msg)
	if err != nil {
		return 0, err
	}
	cost := int64(len(encoded))
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.closed || h.inFlight.Add(cost) > int64(h.cfg.maxQueueBytes) {
		if !h.closed {
			h.inFlight.Add(-cost)
		}
		h.dropped.Add(1)
		return len(p), nil
	}
	select {
	case h.queue <- queuedEvent{line: encoded, cost: cost}:
		h.admitted.Add(1)
	default:
		h.inFlight.Add(-cost)
		h.dropped.Add(1)
	}
	return len(p), nil
}

// appendEvent appends one NDJSON event to dst.
func (h *Handler) appendEvent(dst []byte, ev event) ([]byte, error) {
	if cap(dst)-len(dst) < len(ev.message)+128 {
		grown := make([]byte, len(dst), len(dst)+len(ev.message)+128)
		copy(grown, dst)
		dst = grown
	}
	dst = append(dst, '{')
	dst = append(dst, `"_time":`...)
	dst = append(dst, '"')
	dst = ev.time.UTC().AppendFormat(dst, time.RFC3339Nano)
	dst = append(dst, '"')
	if ev.hasLevel {
		dst = append(dst, `,"level":`...)
		var marshalErr error
		dst, marshalErr = appendJSONString(dst, h.cfg.levelName(ev.level))
		if marshalErr != nil {
			return nil, fmt.Errorf("encode level name: %w", marshalErr)
		}
		dst = append(dst, `,"level_num":`...)
		dst = strconv.AppendInt(dst, int64(ev.level), 10)
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

func appendJSONString(dst []byte, value string) ([]byte, error) {
	item, ok := stringJSONPool.Get().(*stringJSONBuffer)
	if !ok {
		item = &stringJSONBuffer{}
		item.enc = json.NewEncoder(&item.buf)
		item.enc.SetEscapeHTML(false)
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

const (
	defaultDomain = "us-east-1.aws.edge.axiom.co"
	maxMessageLen = 512 << 10
	userAgent     = "loginjector-axiom"
)

type event struct {
	time      time.Time
	level     loginjector.LogLevel
	hasLevel  bool
	message   string
	truncated bool
}

func newEvent(now time.Time, level loginjector.LogLevel, hasLevel bool, p []byte) event {
	// trim and truncate on the caller's bytes before converting, so the event owns a
	// copy of exactly the kept bytes and never pins a larger backing array.
	trimmed := bytes.TrimRight(p, "\r\n")
	truncated := false
	if len(trimmed) > maxMessageLen {
		end := maxMessageLen
		for back := 0; back < utf8.UTFMax-1 && end > 0 && !utf8.RuneStart(trimmed[end]); back++ {
			end--
		}
		trimmed = trimmed[:end]
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
