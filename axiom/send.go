package axiom

import (
	"bytes"
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
		var payload []byte
		var err error
		if h.cfg.compression {
			payload, err = h.compressPayload(
				encoded.Bytes(),
			)
		} else {
			payload = append(
				[]byte(nil),
				encoded.Bytes()...,
			)
		}
		if err != nil {
			h.failed.Add(
				uint64(
					len(batch),
				),
			)
			h.release(batch)
			h.report(err)
			h.complete(
				uint64(
					len(batch),
				),
			)
			batch = batch[:0]
			encoded.Reset()
			return
		}
		h.deliver(
			ctx,
			payload,
			len(batch),
		)
		h.release(batch)
		batch = batch[:0]
		encoded.Reset()
	}
	add := func(item queuedEvent) {
		if len(batch) > 0 && (len(batch) >= h.cfg.batchSize || encoded.Len()+int(item.cost) > h.cfg.maxBatchBytes) {
			flush()
		}
		encoded.Write(item.line)
		batch = append(batch, item)
		if len(batch) >= h.cfg.batchSize || encoded.Len() >= h.cfg.maxBatchBytes {
			flush()
		}
	}
	for {
		select {
		case item := <-h.queue:
			add(item)
		case <-ticker.C:
			flush()
			h.reportDrops()
		case <-h.wake:
		drainQueue:
			for {
				select {
				case item := <-h.queue:
					add(item)
				default:
					break drainQueue
				}
			}
			flush()
		case <-ctx.Done():
			for {
				select {
				case item := <-h.queue:
					batch = append(batch, item)
				default:
					if len(batch) > 0 {
						h.failed.Add(
							uint64(
								len(batch),
							),
						)
						h.complete(
							uint64(
								len(batch),
							),
						)
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

func (h *Handler) compressPayload(payload []byte) ([]byte, error) {
	h.compressBuf.Reset()
	h.gzipWriter.Reset(
		&h.compressBuf,
	)
	if _, err := h.gzipWriter.Write(payload); err != nil {
		return nil, fmt.Errorf("axiom: compress batch: %w", err)
	}
	if err := h.gzipWriter.Close(); err != nil {
		return nil, fmt.Errorf("axiom: finish batch compression: %w", err)
	}
	return append(
		[]byte(nil),
		h.compressBuf.Bytes()...,
	), nil
}

func (h *Handler) deliver(ctx context.Context, payload []byte, count int) {
	var finalErr error
	for attempt := 0; attempt <= h.cfg.maxRetries; attempt++ {
		if ctx.Err() != nil {
			finalErr = ctx.Err()
			break
		}
		req, err := http.NewRequestWithContext(
			ctx,
			http.MethodPost,
			h.cfg.ingestURL,
			bytes.NewReader(payload),
		)
		if err != nil {
			msg := fmt.Sprintf("axiom: create ingest request: %v", err)
			finalErr = errors.New(
				h.redact(msg),
			)
			break
		}
		req.Header.Set(
			"Authorization",
			"Bearer "+h.cfg.token,
		)
		req.Header.Set("Content-Type", "application/x-ndjson")
		req.Header.Set("User-Agent", userAgent)
		if h.cfg.compression {
			req.Header.Set("Content-Encoding", "gzip")
		}
		resp, doErr := h.cfg.httpClient.Do(req)
		if doErr != nil {
			msg := fmt.Sprintf("axiom: ingest request failed: %v", doErr)
			finalErr = errors.New(
				h.redact(msg),
			)
			if attempt < h.cfg.maxRetries && h.cfg.wait(
				ctx,
				retryBackoff(attempt),
			) {
				continue
			}
			break
		}
		body, readErr := io.ReadAll(
			io.LimitReader(resp.Body, maxResponseBytes),
		)
		_, drainErr := io.CopyN(io.Discard, resp.Body, maxResponseBytes)
		if errors.Is(drainErr, io.EOF) {
			drainErr = nil
		}
		closeErr := resp.Body.Close()
		bodyErr := errors.Join(readErr, drainErr, closeErr)
		if bodyErr != nil {
			msg := fmt.Sprintf("axiom: read or close response body: %v", bodyErr)
			finalErr = errors.New(
				h.redact(msg),
			)
			if attempt < h.cfg.maxRetries && h.cfg.wait(
				ctx,
				retryBackoff(attempt),
			) {
				continue
			}
			break
		}
		switch {
		case resp.StatusCode == http.StatusOK:
			var result ingestResponse
			if err := json.Unmarshal(
				body,
				&result,
			); err != nil {
				h.failed.Add(
					uint64(count),
				)
				msg := fmt.Sprintf("axiom: decode HTTP 200 response: %v", err)
				h.report(
					errors.New(
						h.redact(msg),
					),
				)
				h.complete(
					uint64(count),
				)
				return
			}
			h.accountResponse(result, count)
			h.complete(
				uint64(count),
			)
			return
		case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
			finalErr = h.responseError(resp.StatusCode, body)
			if attempt < h.cfg.maxRetries {
				delay := retryBackoff(attempt)
				if d, ok := parseRetryAfter(
					resp.Header.Get("Retry-After"),
					h.cfg.now(),
				); ok {
					delay = d
				}
				if h.cfg.wait(ctx, delay) {
					continue
				}
			}
		default:
			h.failed.Add(
				uint64(count),
			)
			h.report(
				h.responseError(resp.StatusCode, body),
			)
			h.complete(
				uint64(count),
			)
			return
		}
		break
	}
	h.failed.Add(
		uint64(count),
	)
	if finalErr != nil {
		h.report(finalErr)
	}
	h.complete(
		uint64(count),
	)
}

func (h *Handler) accountResponse(result ingestResponse, count int) {
	ingested := min(
		max(result.Ingested, 0),
		count,
	)
	failed := min(
		max(result.Failed, 0),
		count-ingested,
	)
	inconsistent := result.Ingested != ingested || result.Failed != failed || ingested+failed != count
	h.sent.Add(
		uint64(ingested),
	)
	h.failed.Add(
		uint64(failed),
	)
	var accountingErr error
	if remainder := count - ingested - failed; inconsistent || remainder > 0 {
		h.failed.Add(
			uint64(remainder),
		)
		if remainder > 0 {
			accountingErr = fmt.Errorf(
				"axiom: response accounted for %d of %d events",
				ingested+failed,
				count,
			)
		} else {
			accountingErr = fmt.Errorf(
				"axiom: response counts were inconsistent: ingested %d, failed %d for %d events",
				result.Ingested,
				result.Failed,
				count,
			)
		}
	}
	var failedErr error
	if result.Failed > 0 {
		reason := "Axiom reported failed events"
		if len(result.Failures) > 0 && result.Failures[0].Error != "" {
			reason = h.redact(result.Failures[0].Error)
			if len(reason) > errorBodyBytes {
				reason = reason[:errorBodyBytes]
			}
		}
		failedErr = fmt.Errorf("axiom: %d events failed: %s", result.Failed, reason)
	}
	switch {
	case failedErr != nil && accountingErr != nil:
		msg := fmt.Sprintf("%s (%s)", failedErr, accountingErr)
		h.report(
			errors.New(
				h.redact(msg),
			),
		)
	case failedErr != nil:
		h.report(failedErr)
	case accountingErr != nil:
		h.report(accountingErr)
	}
}

func (h *Handler) complete(count uint64) {
	h.completed.Add(count)
	h.progressMu.Lock()
	close(h.progress)
	h.progress = make(chan struct{})
	h.progressMu.Unlock()
}

func (h *Handler) release(batch []queuedEvent) {
	for _, item := range batch {
		h.pendingBytes.Add(
			-item.cost,
		)
	}
}

func (h *Handler) report(err error) {
	dropDelta := h.pendingDrops()
	h.reportWith(err, dropDelta)
}

func (h *Handler) pendingDrops() uint64 {
	h.errorMu.Lock()
	defer h.errorMu.Unlock()
	return h.dropped.Load() - h.dropReported
}

func (h *Handler) reportWith(err error, dropDelta uint64) bool {
	if err == nil && dropDelta == 0 {
		return false
	}
	now := h.cfg.now()
	h.errorMu.Lock()
	if !h.lastError.IsZero() && now.Sub(h.lastError) < errorInterval {
		if err != nil {
			h.suppressed++
		}
		h.errorMu.Unlock()
		return false
	}
	suppressed := h.suppressed
	h.suppressed = 0
	h.lastError = now
	h.errorMu.Unlock()
	err = summaryError(err, suppressed, dropDelta)
	return h.emit(err, dropDelta)
}

func (h *Handler) emit(err error, dropDelta uint64) bool {
	if err == nil {
		return false
	}
	err = errors.New(
		h.redact(
			err.Error(),
		),
	)
	called := false
	func() {
		defer func() {
			if v := recover(); v != nil {
				_, _ = fmt.Fprintf(h.cfg.errorOutput, "axiom: error handler panicked: %v\n", v)
			}
		}()
		h.cfg.errorHandler(err)
		called = true
	}()
	if called && dropDelta > 0 {
		h.errorMu.Lock()
		h.dropReported += dropDelta
		h.errorMu.Unlock()
	}
	return called
}

func (h *Handler) reportSuppressed() {
	h.errorMu.Lock()
	suppressed := h.suppressed
	h.suppressed = 0
	h.errorMu.Unlock()
	dropDelta := h.pendingDrops()
	if suppressed == 0 && dropDelta == 0 {
		return
	}
	h.emit(
		summaryError(nil, suppressed, dropDelta),
		dropDelta,
	)
}

func (h *Handler) reportDrops() {
	dropped := h.dropped.Load()
	h.errorMu.Lock()
	delta := dropped - h.dropReported
	h.errorMu.Unlock()
	if delta > 0 {
		h.reportWith(nil, delta)
	}
}

func (h *Handler) redact(s string) string {
	return strings.ReplaceAll(s, h.cfg.token, "[redacted]")
}

func (h *Handler) responseError(status int, body []byte) error {
	body = []byte(
		h.redact(
			string(body),
		),
	)
	if len(body) > errorBodyBytes {
		body = body[:errorBodyBytes]
	}
	message := strings.TrimSpace(
		string(body),
	)
	if message == "" {
		return fmt.Errorf("axiom: HTTP %d", status)
	}
	msg := fmt.Sprintf("axiom: HTTP %d: %s", status, message)
	return errors.New(
		h.redact(msg),
	)
}

const (
	maxResponseBytes = 1 << 20
	errorBodyBytes   = 1 << 10
	errorInterval    = 30 * time.Second
	maxRetryAfter    = 30 * time.Second
	maxRetryBackoff  = 10 * time.Second
)

type ingestResponse struct {
	Ingested int `json:"ingested"`
	Failed   int `json:"failed"`
	Failures []struct {
		Error string `json:"error"`
	} `json:"failures"`
}

func summaryError(err error, suppressed, dropDelta uint64) error {
	message := ""
	if err != nil {
		message = err.Error()
	}
	if suppressed > 0 {
		if message == "" {
			message = fmt.Sprintf("axiom: suppressed %d additional errors", suppressed)
		} else {
			message += fmt.Sprintf(" (suppressed %d additional errors)", suppressed)
		}
	}
	if dropDelta > 0 {
		if message == "" {
			message = fmt.Sprintf("axiom: dropped %d events", dropDelta)
		} else {
			message += fmt.Sprintf(" (axiom: dropped %d events)", dropDelta)
		}
	}
	return errors.New(message)
}

func retryBackoff(attempt int) time.Duration {
	d := 250 * time.Millisecond
	for i := 0; i < attempt && d < maxRetryBackoff; i++ {
		d *= 2
	}
	if d > maxRetryBackoff {
		d = maxRetryBackoff
	}
	return time.Duration(
		rand.Int64N(
			int64(d) + 1,
		),
	)
}

func parseRetryAfter(value string, now time.Time) (time.Duration, bool) {
	if seconds, err := strconv.ParseInt(
		strings.TrimSpace(value),
		10,
		64,
	); err == nil {
		if seconds < 0 {
			return 0, false
		}
		if seconds >= int64(
			maxRetryAfter/time.Second,
		) {
			return maxRetryAfter, true
		}
		return time.Duration(seconds) * time.Second, true
	}
	if when, err := http.ParseTime(value); err == nil {
		d := when.Sub(now)
		if d < 0 {
			d = 0
		}
		if d > maxRetryAfter {
			d = maxRetryAfter
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
