package axiom

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	maxResponseBytes = 64 << 10
	errorBodyBytes   = 1 << 10
	errorInterval    = 30 * time.Second
)

type ingestResponse struct {
	Ingested int `json:"ingested"`
	Failed   int `json:"failed"`
	Failures []struct {
		Error string `json:"error"`
	} `json:"failures"`
}

func (h *Handler) run(ctx context.Context) {
	defer close(h.done)
	ticker := time.NewTicker(h.cfg.flushInterval)
	defer ticker.Stop()
	var batch []queuedEvent
	var encoded bytes.Buffer
	flush := func() {
		if len(batch) == 0 {
			return
		}
		payload, err := h.encodeBatch(batch, &encoded)
		if err != nil {
			h.failed.Add(uint64(len(batch)))
			h.release(batch)
			h.report(err)
			h.complete(uint64(len(batch)))
			batch = batch[:0]
			encoded.Reset()
			return
		}
		h.deliver(ctx, payload, len(batch))
		h.release(batch)
		batch = batch[:0]
		encoded.Reset()
	}
	for {
		select {
		case item := <-h.queue:
			if len(batch) > 0 && (len(batch) >= h.cfg.batchSize || encoded.Len()+int(item.cost) > h.cfg.maxBatchBytes) {
				flush()
			}
			encoded.Write(item.line)
			batch = append(batch, item)
			if len(batch) >= h.cfg.batchSize || encoded.Len() >= h.cfg.maxBatchBytes {
				flush()
			}
		case <-ticker.C:
			flush()
			h.reportDrops()
		case <-h.wake:
			flush()
		case <-ctx.Done():
			for {
				select {
				case item := <-h.queue:
					batch = append(batch, item)
				default:
					if len(batch) > 0 {
						h.failed.Add(uint64(len(batch)))
						h.complete(uint64(len(batch)))
						h.release(batch)
					}
					h.reportDrops()
					h.reportSuppressed()
					return
				}
			}
		}
		h.reportDrops()
		h.mu.RLock()
		shuttingDown := h.closed
		h.mu.RUnlock()
		if shuttingDown && len(h.queue) == 0 {
			flush()
			if len(batch) == 0 {
				h.reportSuppressed()
				return
			}
		}
	}
}

func (h *Handler) encodeBatch(batch []queuedEvent, dst *bytes.Buffer) ([]byte, error) {
	dst.Reset()
	for _, item := range batch {
		dst.Write(item.line)
	}
	payload := append([]byte(nil), dst.Bytes()...)
	if !h.cfg.compression {
		return payload, nil
	}
	var compressed bytes.Buffer
	zw := gzip.NewWriter(&compressed)
	if _, err := zw.Write(payload); err != nil {
		return nil, fmt.Errorf("axiom: compress batch: %w", err)
	}
	if err := zw.Close(); err != nil {
		return nil, fmt.Errorf("axiom: finish batch compression: %w", err)
	}
	return append([]byte(nil), compressed.Bytes()...), nil
}

func (h *Handler) deliver(ctx context.Context, payload []byte, count int) {
	var finalErr error
	for attempt := 0; attempt <= h.cfg.maxRetries; attempt++ {
		if ctx.Err() != nil {
			finalErr = ctx.Err()
			break
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.cfg.ingestURL, bytes.NewReader(payload))
		if err != nil {
			finalErr = fmt.Errorf("axiom: create ingest request: %w", err)
			break
		}
		req.Header.Set("Authorization", "Bearer "+h.cfg.token)
		req.Header.Set("Content-Type", "application/x-ndjson")
		req.Header.Set("User-Agent", userAgent)
		if h.cfg.compression {
			req.Header.Set("Content-Encoding", "gzip")
		}
		resp, doErr := h.cfg.httpClient.Do(req)
		if doErr != nil {
			finalErr = fmt.Errorf("axiom: ingest request failed: %w", doErr)
			if attempt < h.cfg.maxRetries && h.cfg.wait(ctx, retryBackoff(attempt)) {
				continue
			}
			break
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
		_, drainErr := io.Copy(io.Discard, resp.Body)
		closeErr := resp.Body.Close()
		bodyErr := errors.Join(readErr, drainErr, closeErr)
		if bodyErr != nil {
			finalErr = fmt.Errorf("axiom: read or close response body: %w", bodyErr)
			if attempt < h.cfg.maxRetries && h.cfg.wait(ctx, retryBackoff(attempt)) {
				continue
			}
			break
		}
		switch {
		case resp.StatusCode == http.StatusOK:
			var result ingestResponse
			if err := json.Unmarshal(body, &result); err != nil {
				h.sent.Add(uint64(count))
				h.report(fmt.Errorf("axiom: decode HTTP 200 response: %w", err))
				h.complete(uint64(count))
				return
			}
			h.sent.Add(uint64(result.Ingested))
			h.failed.Add(uint64(result.Failed))
			if result.Failed > 0 {
				reason := "Axiom reported failed events"
				if len(result.Failures) > 0 && result.Failures[0].Error != "" {
					reason = result.Failures[0].Error
				}
				h.report(fmt.Errorf("axiom: %d events failed: %s", result.Failed, reason))
			}
			h.complete(uint64(count))
			return
		case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
			finalErr = responseError(resp.StatusCode, body)
			if attempt < h.cfg.maxRetries {
				delay := retryBackoff(attempt)
				if d, ok := parseRetryAfter(resp.Header.Get("Retry-After"), h.cfg.now()); ok {
					delay = d
				}
				if h.cfg.wait(ctx, delay) {
					continue
				}
			}
		default:
			h.failed.Add(uint64(count))
			h.report(responseError(resp.StatusCode, body))
			h.complete(uint64(count))
			return
		}
		break
	}
	h.failed.Add(uint64(count))
	if finalErr != nil {
		h.report(finalErr)
	}
	h.complete(uint64(count))
}

func (h *Handler) complete(count uint64) {
	h.completed.Add(count)
	select {
	case h.progress <- struct{}{}:
	default:
	}
}

func (h *Handler) release(batch []queuedEvent) {
	for _, item := range batch {
		h.inFlight.Add(-item.cost)
	}
}

func (h *Handler) report(err error) {
	if err == nil {
		return
	}
	now := h.cfg.now()
	h.errorMu.Lock()
	if !h.lastError.IsZero() && now.Sub(h.lastError) < errorInterval {
		h.suppressed++
		h.errorMu.Unlock()
		return
	}
	suppressed := h.suppressed
	h.suppressed = 0
	h.lastError = now
	h.errorMu.Unlock()
	if suppressed > 0 {
		err = fmt.Errorf("%w (suppressed %d additional errors)", err, suppressed)
	}
	err = fmt.Errorf("%s", strings.ReplaceAll(err.Error(), h.cfg.token, "[redacted]"))
	func() { defer func() { _ = recover() }(); h.cfg.errorHandler(err) }()
}

func (h *Handler) reportSuppressed() {
	h.errorMu.Lock()
	suppressed := h.suppressed
	h.suppressed = 0
	h.errorMu.Unlock()
	if suppressed == 0 {
		return
	}
	err := fmt.Errorf("axiom: suppressed %d additional errors", suppressed)
	err = fmt.Errorf("%s", strings.ReplaceAll(err.Error(), h.cfg.token, "[redacted]"))
	func() { defer func() { _ = recover() }(); h.cfg.errorHandler(err) }()
}

func (h *Handler) reportDrops() {
	dropped := h.dropped.Load()
	h.errorMu.Lock()
	delta := dropped - h.dropReported
	if delta > 0 {
		h.dropReported = dropped
	}
	h.errorMu.Unlock()
	if delta > 0 {
		h.report(fmt.Errorf("axiom: dropped %d events", delta))
	}
}

func responseError(status int, body []byte) error {
	if len(body) > errorBodyBytes {
		body = body[:errorBodyBytes]
	}
	message := strings.TrimSpace(string(body))
	if message == "" {
		return fmt.Errorf("axiom: HTTP %d", status)
	}
	return fmt.Errorf("axiom: HTTP %d: %s", status, message)
}

func retryBackoff(attempt int) time.Duration {
	d := 250 * time.Millisecond
	for i := 0; i < attempt && d < 10*time.Second; i++ {
		d *= 2
	}
	if d > 10*time.Second {
		d = 10 * time.Second
	}
	return time.Duration(rand.Int64N(int64(d) + 1))
}

func parseRetryAfter(value string, now time.Time) (time.Duration, bool) {
	if seconds, err := strconv.Atoi(strings.TrimSpace(value)); err == nil && seconds >= 0 {
		d := time.Duration(seconds) * time.Second
		if d > 30*time.Second {
			d = 30 * time.Second
		}
		return d, true
	}
	if when, err := http.ParseTime(value); err == nil {
		d := when.Sub(now)
		if d < 0 {
			d = 0
		}
		if d > 30*time.Second {
			d = 30 * time.Second
		}
		return d, true
	}
	return 0, false
}

func waitRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

var _ io.Writer = (*Handler)(nil)
