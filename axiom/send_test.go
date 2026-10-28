package axiom

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	loginjector "github.com/prorochestvo/loginjector"
	"github.com/stretchr/testify/require"
)

func newTestServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	return httptest.NewServer(handler)
}

func testHandler(t *testing.T, server *httptest.Server, opts ...Option) *Handler {
	t.Helper()
	opts = append([]Option{withBaseURL(server.URL)}, opts...)
	h, err := NewHandler("xaat-secret", "logs", opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = h.Close() })
	return h
}

func responseJSON(w http.ResponseWriter, status int, body string) {
	w.WriteHeader(status)
	_, _ = io.WriteString(w, body)
}

func requestEvents(r *http.Request) ([]map[string]any, []byte, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, nil, err
	}
	payload := body
	if r.Header.Get("Content-Encoding") == "gzip" {
		zr, err := gzip.NewReader(strings.NewReader(string(body)))
		if err != nil {
			return nil, body, err
		}
		payload, err = io.ReadAll(zr)
		closeErr := zr.Close()
		if err != nil {
			return nil, body, err
		}
		if closeErr != nil {
			return nil, body, closeErr
		}
	}
	var events []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(string(payload)), "\n") {
		if line == "" {
			continue
		}
		var event map[string]any
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			return nil, body, err
		}
		events = append(events, event)
	}
	return events, body, nil
}

func flushHandler(t *testing.T, h *Handler) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, h.Flush(ctx))
}

func TestHandler_batchByCount(t *testing.T) {
	t.Parallel()
	var requests atomic.Int64
	var eventCount atomic.Int64
	messages := make(chan string, 3)
	server := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		events, _, err := requestEvents(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		requests.Add(1)
		eventCount.Add(int64(len(events)))
		for _, event := range events {
			messages <- event["message"].(string)
		}
		responseJSON(w, 200, `{"ingested":`+jsonNumber(len(events))+`,"failed":0}`)
	}))
	defer server.Close()
	h := testHandler(t, server, WithBatchSize(2))
	for _, msg := range []string{"one", "two", "three"} {
		_, err := h.Write([]byte(msg))
		require.NoError(t, err)
	}
	flushHandler(t, h)
	require.EqualValues(t, 2, requests.Load())
	require.EqualValues(t, 3, eventCount.Load())
	require.ElementsMatch(t, []string{"one", "two", "three"}, []string{<-messages, <-messages, <-messages})
	require.Equal(t, uint64(3), h.Stats().Sent)
}

func TestHandler_batchFillsByCountOnly(t *testing.T) {
	t.Parallel()
	var requests atomic.Int64
	eventCounts := make(chan int, 3)
	server := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		events, _, err := requestEvents(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		requests.Add(1)
		eventCounts <- len(events)
		responseJSON(w, 200, `{"ingested":`+jsonNumber(len(events))+`,"failed":0}`)
	}))
	defer server.Close()
	h := testHandler(t, server, WithBatchSize(3), WithFlushInterval(time.Hour), WithMaxRetries(0))
	for _, msg := range []string{"one", "two", "three"} {
		_, err := h.Write([]byte(msg))
		require.NoError(t, err)
	}
	select {
	case count := <-eventCounts:
		require.Equal(t, 3, count)
	case <-time.After(5 * time.Second):
		t.Fatal("count-triggered batch was not delivered")
	}
	require.NoError(t, h.Close())
	require.EqualValues(t, 1, requests.Load())
	require.Equal(t, uint64(3), h.Stats().Sent)
}

func TestHandler_batchByBytes(t *testing.T) {
	t.Parallel()
	var requests atomic.Int64
	server := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		events, _, err := requestEvents(r)
		if err != nil {
			http.Error(w, "bad", http.StatusBadRequest)
			return
		}
		responseJSON(w, 200, `{"ingested":`+jsonNumber(len(events))+`}`)
	}))
	defer server.Close()
	h := testHandler(t, server, WithMaxBatchBytes(1<<20))
	// Two encoded lines each exceed half the batch limit.
	msg := strings.Repeat("x", 600<<10)
	_, err := h.Write([]byte(msg))
	require.NoError(t, err)
	_, err = h.Write([]byte(msg))
	require.NoError(t, err)
	flushHandler(t, h)
	require.EqualValues(t, 2, requests.Load())
}

func TestHandler_batchByInterval(t *testing.T) {
	t.Parallel()
	got := make(chan struct{}, 1)
	server := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		got <- struct{}{}
		responseJSON(w, 200, `{"ingested":1}`)
	}))
	defer server.Close()
	h := testHandler(t, server, WithFlushInterval(20*time.Millisecond))
	_, err := h.Write([]byte("tick"))
	require.NoError(t, err)
	select {
	case <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("flush interval did not send batch")
	}
}

func TestHandler_failedResponse(t *testing.T) {
	t.Parallel()
	var reported atomic.Int64
	server := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		responseJSON(w, 200, `{"ingested":0,"failed":1,"failures":[{"error":"bad row"}]}`)
	}))
	defer server.Close()
	h := testHandler(t, server, WithBatchSize(1), WithErrorHandler(func(error) { reported.Add(1) }))
	_, err := h.Write([]byte("x"))
	require.NoError(t, err)
	flushHandler(t, h)
	require.Equal(t, uint64(1), h.Stats().Failed)
	require.EqualValues(t, 1, reported.Load())
}

func TestHandler_retryAfterHTTPDate(t *testing.T) {
	t.Parallel()
	var attempts atomic.Int64
	var delaysMu sync.Mutex
	var delays []time.Duration
	now := time.Date(2026, 10, 28, 0, 0, 0, 0, time.UTC)
	server := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) == 1 {
			w.Header().Set("Retry-After", now.Add(7*time.Second).Format(http.TimeFormat))
			responseJSON(w, 429, "limited")
			return
		}
		responseJSON(w, 200, `{"ingested":1}`)
	}))
	defer server.Close()
	h := testHandler(t, server, WithBatchSize(1), WithMaxRetries(1), WithCompression(false), withClock(func() time.Time { return now }, func(_ context.Context, d time.Duration) bool {
		delaysMu.Lock()
		delays = append(delays, d)
		delaysMu.Unlock()
		return true
	}))
	_, err := h.Write([]byte("retry"))
	require.NoError(t, err)
	flushHandler(t, h)
	require.EqualValues(t, 2, attempts.Load())
	require.Equal(t, []time.Duration{7 * time.Second}, delays)
}

func TestHandler_retryAfterSeconds(t *testing.T) {
	t.Parallel()
	var attempts atomic.Int64
	var delay atomic.Int64
	bodies := make(chan []byte, 2)
	server := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read failed", http.StatusBadRequest)
			return
		}
		bodies <- body
		if attempts.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			responseJSON(w, 429, "limited")
			return
		}
		responseJSON(w, 200, `{"ingested":1}`)
	}))
	defer server.Close()
	h := testHandler(t, server, WithBatchSize(1), WithMaxRetries(1), withClock(time.Now, func(_ context.Context, d time.Duration) bool { delay.Store(int64(d)); return true }))
	_, err := h.Write([]byte("retry"))
	require.NoError(t, err)
	flushHandler(t, h)
	require.EqualValues(t, 2, attempts.Load())
	require.EqualValues(t, time.Second, delay.Load())
	first, second := <-bodies, <-bodies
	require.NotEmpty(t, first)
	require.Equal(t, first, second)
}

func TestHandler_5xxRetriesAnd400DoesNot(t *testing.T) {
	t.Parallel()
	var attempts atomic.Int64
	var reports atomic.Int64
	server := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) <= 3 {
			responseJSON(w, 503, "down")
			return
		}
		responseJSON(w, 400, "invalid")
	}))
	defer server.Close()
	h := testHandler(t, server, WithBatchSize(1), WithMaxRetries(2), WithErrorHandler(func(error) { reports.Add(1) }), withClock(time.Now, func(context.Context, time.Duration) bool { return true }))
	_, err := h.Write([]byte("retry"))
	require.NoError(t, err)
	flushHandler(t, h)
	_, err = h.Write([]byte("no retry"))
	require.NoError(t, err)
	flushHandler(t, h)
	require.EqualValues(t, 4, attempts.Load())
	require.Equal(t, uint64(2), h.Stats().Failed)
	require.EqualValues(t, 1, reports.Load())
}

func TestHandler_queueFullDrops(t *testing.T) {
	t.Parallel()
	started, release := make(chan struct{}), make(chan struct{})
	server := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		select {
		case <-started:
		default:
			close(started)
		}
		<-release
		responseJSON(w, 200, `{"ingested":1}`)
	}))
	defer server.Close()
	h := testHandler(t, server, WithBatchSize(1), WithQueueSize(1), WithMaxRetries(0))
	_, err := h.Write([]byte("first"))
	require.NoError(t, err)
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("request did not start")
	}
	_, _ = h.Write([]byte("queued"))
	start := time.Now()
	_, err = h.Write([]byte("dropped"))
	require.NoError(t, err)
	require.Less(t, time.Since(start), time.Second)
	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = h.Shutdown(ctx)
	require.GreaterOrEqual(t, h.Stats().Dropped, uint64(1))
}

func TestHandler_byteBudgetDrops(t *testing.T) {
	t.Parallel()
	started, release := make(chan struct{}), make(chan struct{})
	server := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		select {
		case <-started:
		default:
			close(started)
		}
		<-release
		responseJSON(w, 200, `{"ingested":1}`)
	}))
	defer server.Close()
	h := testHandler(t, server, WithBatchSize(1), WithMaxBatchBytes(1<<20), WithQueueSize(10), WithMaxQueueBytes(1<<20))
	message := []byte(strings.Repeat("x", 300<<10))
	_, err := h.Write(message)
	require.NoError(t, err)
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("request did not start")
	}
	for range 3 {
		_, err = h.Write(message)
		require.NoError(t, err)
	}
	require.GreaterOrEqual(t, h.Stats().Dropped, uint64(1))
	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = h.Shutdown(ctx)
}

func TestHandler_errorHandlerRateLimited(t *testing.T) {
	t.Parallel()
	var attempts, reports atomic.Int64
	now := time.Date(2026, 10, 28, 0, 0, 0, 0, time.UTC)
	var clockMu sync.Mutex
	server := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { attempts.Add(1); responseJSON(w, 400, "bad") }))
	defer server.Close()
	h := testHandler(t, server, WithBatchSize(1), WithMaxRetries(0), WithErrorHandler(func(error) { reports.Add(1) }), withClock(func() time.Time { clockMu.Lock(); defer clockMu.Unlock(); return now }, func(context.Context, time.Duration) bool { return true }))
	_, err := h.Write([]byte("a"))
	require.NoError(t, err)
	flushHandler(t, h)
	clockMu.Lock()
	now = now.Add(time.Second)
	clockMu.Unlock()
	_, err = h.Write([]byte("b"))
	require.NoError(t, err)
	flushHandler(t, h)
	clockMu.Lock()
	now = now.Add(30 * time.Second)
	clockMu.Unlock()
	_, err = h.Write([]byte("c"))
	require.NoError(t, err)
	flushHandler(t, h)
	require.EqualValues(t, 3, attempts.Load())
	require.EqualValues(t, 2, reports.Load())
}

func TestHandler_errorHandlerPanicContinues(t *testing.T) {
	t.Parallel()
	var requests atomic.Int64
	server := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { requests.Add(1); responseJSON(w, 400, "bad") }))
	defer server.Close()
	h := testHandler(t, server, WithBatchSize(1), WithMaxRetries(0), WithErrorHandler(func(error) { panic("handler") }))
	_, err := h.Write([]byte("a"))
	require.NoError(t, err)
	flushHandler(t, h)
	_, err = h.Write([]byte("b"))
	require.NoError(t, err)
	flushHandler(t, h)
	require.EqualValues(t, 2, requests.Load())
}

func TestHandler_flushDeadline(t *testing.T) {
	t.Parallel()
	started, release := make(chan struct{}), make(chan struct{})
	server := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release
		responseJSON(w, 200, `{"ingested":1}`)
	}))
	defer server.Close()
	defer close(release)
	h := testHandler(t, server, WithBatchSize(1))
	_, err := h.Write([]byte("hang"))
	require.NoError(t, err)
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, h.Flush(ctx), context.DeadlineExceeded)
}

func TestHandler_closeIdempotent(t *testing.T) {
	t.Parallel()
	var count atomic.Int64
	server := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { count.Add(1); responseJSON(w, 200, `{"ingested":1}`) }))
	defer server.Close()
	h := testHandler(t, server)
	_, err := h.Write([]byte("queued"))
	require.NoError(t, err)
	require.NoError(t, h.Close())
	require.NoError(t, h.Close())
	require.EqualValues(t, 1, count.Load())
	n, err := h.Write([]byte("after"))
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, uint64(1), h.Stats().Dropped)
	require.ErrorIs(t, h.Flush(context.Background()), ErrClosed)
}

func TestHandler_shutdownDuringBackoff(t *testing.T) {
	t.Parallel()
	started := make(chan struct{})
	server := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		w.Header().Set("Retry-After", "30")
		responseJSON(w, 429, "later")
	}))
	defer server.Close()
	waiting := make(chan struct{})
	h := testHandler(t, server, WithBatchSize(1), WithMaxRetries(3), withClock(time.Now, func(ctx context.Context, _ time.Duration) bool { close(waiting); <-ctx.Done(); return false }))
	_, err := h.Write([]byte("backoff"))
	require.NoError(t, err)
	<-started
	<-waiting
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.ErrorIs(t, h.Shutdown(ctx), context.DeadlineExceeded)
	<-h.done
	require.Equal(t, uint64(1), h.Stats().Failed)
}

func TestHandler_shutdownHangingServer(t *testing.T) {
	t.Parallel()
	started, release := make(chan struct{}), make(chan struct{})
	server := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release
		responseJSON(w, 200, `{"ingested":1}`)
	}))
	defer server.Close()
	h := testHandler(t, server, WithBatchSize(1))
	_, err := h.Write([]byte("hang"))
	require.NoError(t, err)
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("request did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, h.Shutdown(ctx), context.DeadlineExceeded)
	close(release)
	select {
	case <-h.done:
	default:
		t.Fatal("worker did not exit")
	}
	require.Equal(t, uint64(1), h.Stats().Failed)
}

func TestHandler_callerMutatesSlice(t *testing.T) {
	t.Parallel()
	got := make(chan string, 1)
	server := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		events, _, _ := requestEvents(r)
		got <- events[0]["message"].(string)
		responseJSON(w, 200, `{"ingested":1}`)
	}))
	defer server.Close()
	h := testHandler(t, server, WithBatchSize(1))
	p := []byte("original")
	_, err := h.Write(p)
	require.NoError(t, err)
	copy(p, "mutated!")
	flushHandler(t, h)
	require.Equal(t, "original", <-got)
}

func TestHandler_tokenNeverInErrors(t *testing.T) {
	t.Parallel()
	var errorsMu sync.Mutex
	var messages []string
	server := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { responseJSON(w, 400, "xaat-secret leaked?") }))
	defer server.Close()
	h := testHandler(t, server, WithBatchSize(1), WithMaxRetries(0), WithErrorHandler(func(err error) { errorsMu.Lock(); messages = append(messages, err.Error()); errorsMu.Unlock() }))
	_, err := h.Write([]byte("x"))
	require.NoError(t, err)
	flushHandler(t, h)
	errorsMu.Lock()
	defer errorsMu.Unlock()
	require.Len(t, messages, 1)
	require.NotContains(t, messages[0], "xaat-secret")
}

func TestHandler_WriteForRaceCondition(t *testing.T) {
	t.Parallel()
	server := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		events, _, err := requestEvents(r)
		if err != nil {
			http.Error(w, "bad", http.StatusBadRequest)
			return
		}
		responseJSON(w, 200, `{"ingested":`+jsonNumber(len(events))+`}`)
	}))
	defer server.Close()
	h := testHandler(t, server, WithBatchSize(100), WithQueueSize(10000))
	logger := loginjector.NewLogger(0, h)
	const workers, writes = 50, 1000
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range writes {
				logger.WriteLog(1, []byte("race"))
			}
		}()
	}
	wg.Wait()
	require.NoError(t, h.Close())
	stats := h.Stats()
	require.Equal(t, uint64(workers*writes), stats.Sent+stats.Dropped+stats.Failed)
}

//nolint:paralleltest // goroutine count is process-wide
func TestHandler_noGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()
	server := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { responseJSON(w, 200, `{"ingested":1}`) }))
	h := testHandler(t, server, WithBatchSize(1))
	_, err := h.Write([]byte("x"))
	require.NoError(t, err)
	require.NoError(t, h.Close())
	server.Close()
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	require.LessOrEqual(t, runtime.NumGoroutine(), before)
}

func TestHandler_errorHandlerLogsIntoOwnLogger(t *testing.T) {
	t.Parallel()
	var requests atomic.Int64
	events := make(chan []map[string]any, 2)
	server := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		decoded, _, decodeErr := requestEvents(r)
		if decodeErr != nil {
			http.Error(w, decodeErr.Error(), http.StatusBadRequest)
			return
		}
		events <- decoded
		responseJSON(w, 400, "bad")
	}))
	defer server.Close()
	var logger *loginjector.Logger
	reported := make(chan struct{})
	var reportOnce sync.Once
	h, err := NewHandler("xaat-secret", "logs", withBaseURL(server.URL), WithBatchSize(1), WithMaxRetries(0), WithErrorHandler(func(error) {
		reportOnce.Do(func() {
			logger.WriteLog(1, []byte("reported"))
			close(reported)
		})
	}))
	require.NoError(t, err)
	logger = loginjector.NewLogger(0, h)
	_, err = h.Write([]byte("failure"))
	require.NoError(t, err)
	select {
	case <-reported:
	case <-time.After(5 * time.Second):
		t.Fatal("error handler did not report")
	}
	require.NoError(t, h.Close())
	require.EqualValues(t, 2, requests.Load())
	first, second := <-events, <-events
	require.Len(t, first, 1)
	require.Len(t, second, 1)
	require.Equal(t, "reported", second[0]["message"])
	require.Equal(t, uint64(2), h.Stats().Failed)
	require.Zero(t, h.Stats().Dropped)
}

func TestHandler_retryBodyIdentical(t *testing.T) {
	t.Parallel()
	var bodiesMu sync.Mutex
	var bodies [][]byte
	var count atomic.Int64
	server := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		bodiesMu.Lock()
		bodies = append(bodies, b)
		bodiesMu.Unlock()
		if count.Add(1) == 1 {
			responseJSON(w, 503, "retry")
		} else {
			responseJSON(w, 200, `{"ingested":1}`)
		}
	}))
	defer server.Close()
	h := testHandler(t, server, WithBatchSize(1), WithMaxRetries(1), WithCompression(false), withClock(time.Now, func(context.Context, time.Duration) bool { return true }))
	_, err := h.Write([]byte("retry body"))
	require.NoError(t, err)
	flushHandler(t, h)
	bodiesMu.Lock()
	defer bodiesMu.Unlock()
	require.Len(t, bodies, 2)
	require.NotEmpty(t, bodies[0])
	require.Equal(t, bodies[0], bodies[1])
}

func TestHandler_oversizedSingleEvent(t *testing.T) {
	t.Parallel()
	var eventCount atomic.Int64
	server := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		events, _, err := requestEvents(r)
		if err != nil {
			http.Error(w, "bad", http.StatusBadRequest)
			return
		}
		eventCount.Store(int64(len(events)))
		responseJSON(w, 200, `{"ingested":1}`)
	}))
	defer server.Close()
	h := testHandler(t, server, WithMaxBatchBytes(1<<20), WithBatchSize(10))
	message := strings.Repeat("<\x01", 256<<10)
	_, err := h.Write([]byte(message))
	require.NoError(t, err)
	flushHandler(t, h)
	require.EqualValues(t, 1, eventCount.Load())
}

func TestHandler_dropReportWithoutTransportErrors(t *testing.T) {
	t.Parallel()
	var reportsMu sync.Mutex
	var reports []string
	started, release := make(chan struct{}), make(chan struct{})
	server := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		select {
		case <-started:
		default:
			close(started)
		}
		<-release
		responseJSON(w, 200, `{"ingested":1}`)
	}))
	defer server.Close()
	h := testHandler(t, server, WithBatchSize(1), WithQueueSize(1), WithFlushInterval(20*time.Millisecond), WithErrorHandler(func(err error) { reportsMu.Lock(); reports = append(reports, err.Error()); reportsMu.Unlock() }))
	_, _ = h.Write([]byte("one"))
	<-started
	_, _ = h.Write([]byte("two"))
	_, _ = h.Write([]byte("overflow"))
	close(release)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		reportsMu.Lock()
		found := false
		for _, msg := range reports {
			found = found || strings.Contains(msg, "axiom: dropped")
		}
		reportsMu.Unlock()
		if found {
			break
		}
		runtime.Gosched()
	}
	reportsMu.Lock()
	defer reportsMu.Unlock()
	require.NotEmpty(t, reports)
	require.Contains(t, strings.Join(reports, " "), "axiom: dropped")
}

func TestHandler_rateLimiterClock(t *testing.T) {
	t.Parallel()
	var reportsMu sync.Mutex
	var reports []string
	var nowMu sync.Mutex
	now := time.Date(2026, 10, 28, 0, 0, 0, 0, time.UTC)
	server := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { responseJSON(w, 400, "bad") }))
	defer server.Close()
	h := testHandler(t, server, WithBatchSize(1), WithMaxRetries(0), WithErrorHandler(func(err error) { reportsMu.Lock(); reports = append(reports, err.Error()); reportsMu.Unlock() }), withClock(func() time.Time { nowMu.Lock(); defer nowMu.Unlock(); return now }, func(context.Context, time.Duration) bool { return true }))
	for _, advance := range []time.Duration{0, time.Second, 30 * time.Second} {
		nowMu.Lock()
		now = now.Add(advance)
		nowMu.Unlock()
		_, err := h.Write([]byte("x"))
		require.NoError(t, err)
		flushHandler(t, h)
	}
	reportsMu.Lock()
	defer reportsMu.Unlock()
	require.Len(t, reports, 2)
	require.Contains(t, reports[1], "suppressed 1")
}

func TestHandler_flushIdle(t *testing.T) {
	t.Parallel()
	server := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { responseJSON(w, 200, `{"ingested":1}`) }))
	defer server.Close()
	h := testHandler(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, h.Flush(ctx))
}

func TestHandler_maxRetriesZero(t *testing.T) {
	t.Parallel()
	var attempts atomic.Int64
	server := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { attempts.Add(1); responseJSON(w, 503, "down") }))
	defer server.Close()
	h := testHandler(t, server, WithBatchSize(1), WithMaxRetries(0), withClock(time.Now, func(context.Context, time.Duration) bool { t.Fatal("unexpected retry wait"); return false }))
	_, err := h.Write([]byte("x"))
	require.NoError(t, err)
	flushHandler(t, h)
	require.EqualValues(t, 1, attempts.Load())
}

func jsonNumber(n int) string { return strconv.Itoa(n) }
