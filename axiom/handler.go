package axiom

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	loginjector "github.com/prorochestvo/loginjector"
	"github.com/prorochestvo/loginjector/levels"
)

// Handler encodes events for delivery to Axiom.
type Handler struct {
	cfg config
}

// NewHandler validates the token, dataset, and options and constructs a Handler.
// It does not start a goroutine or open a connection.
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
		httpClient:    &http.Client{Timeout: 10 * time.Second},
		ownsClient:    true,
		maxRetries:    3,
		compression:   true,
		errorHandler:  defaultErrorHandler,
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
	return &Handler{cfg: cfg}, nil
}

// appendEvent appends one NDJSON event to dst.
func (h *Handler) appendEvent(dst []byte, ev event) ([]byte, error) {
	dst = append(dst, '{')
	dst = append(dst, `"_time":`...)
	timestamp, err := json.Marshal(ev.time.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return nil, fmt.Errorf("encode event time: %w", err)
	}
	dst = append(dst, timestamp...)
	if ev.hasLevel {
		levelName, marshalErr := json.Marshal(h.cfg.levelName(ev.level))
		if marshalErr != nil {
			return nil, fmt.Errorf("encode level name: %w", marshalErr)
		}
		dst = append(dst, `,"level":`...)
		dst = append(dst, levelName...)
		dst = append(dst, `,"level_num":`...)
		dst = strconv.AppendInt(dst, int64(ev.level), 10)
	}
	message, err := json.Marshal(ev.message)
	if err != nil {
		return nil, fmt.Errorf("encode event message: %w", err)
	}
	dst = append(dst, `,"message":`...)
	dst = append(dst, message...)
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
		for end > 0 && !utf8.RuneStart(trimmed[end]) {
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
