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
	"net/http/httptest"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/prorochestvo/loginjector"
	"github.com/stretchr/testify/require"
)

func testHandler(t *testing.T, server *httptest.Server, opts ...Option) *Handler {
	t.Helper()
	t.Cleanup(server.Close)
	opts = append([]Option{withBaseURL(server.URL)}, opts...)
	h, err := NewHandler("xaat-secret", "logs", opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = h.Close() })
	return h
}

func TestHandler_Write(t *testing.T) {
	t.Parallel()
	t.Run("exactly full batch sends without flush", func(t *testing.T) {
		t.Parallel()
		got := make(chan int, 1)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			events, _, err := requestEvents(r)
			if err != nil {
				http.Error(w, "bad", http.StatusBadRequest)
				return
			}
			got <- len(events)
			responseJSON(
				w,
				200,
				`{"ingested":`+strconv.Itoa(
					len(events),
				)+`}`,
			)
		}))
		t.Cleanup(server.Close)
		fixed := time.Date(
			2026,
			10,
			28,
			0,
			0,
			0,
			0,
			time.UTC,
		)
		h := testHandler(
			t,
			server,
			WithCompression(false),
			WithMaxBatchBytes(
				1<<20,
			),
			WithFlushInterval(time.Hour),
			withClock(func() time.Time { return fixed }, func(context.Context, time.Duration) bool { return true }),
		)
		lineLen := func(message string) int {
			line, err := h.appendEvent(
				nil,
				newEvent(
					fixed,
					0,
					false,
					[]byte(message),
				),
			)
			require.NoError(t, err)
			return len(line)
		}
		msgs := []string{strings.Repeat(
			"a",
			300<<10,
		), strings.Repeat(
			"b",
			300<<10,
		), ""}
		msgs[2] = strings.Repeat(
			"c",
			(1<<20)-lineLen(msgs[0])-lineLen(msgs[1])-lineLen(""),
		)
		for _, message := range msgs {
			_, err := h.Write([]byte(message))
			require.NoError(t, err)
		}
		select {
		case count := <-got:
			require.Equal(t, 3, count)
		case <-time.After(
			2 * time.Second,
		):
			t.Fatal("a batch at exactly maxBatchBytes was not sent until Flush")
		}
	})

	t.Run("batch by count", func(t *testing.T) {
		t.Parallel()
		var requests atomic.Int64
		var eventCount atomic.Int64
		messages := make(chan string, 3)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			events, _, err := requestEvents(r)
			if err != nil {
				http.Error(
					w,
					err.Error(),
					http.StatusBadRequest,
				)
				return
			}
			requests.Add(1)
			eventCount.Add(int64(len(events)))
			for _, event := range events {
				messages <- event["message"].(string)
			}
			responseJSON(
				w,
				200,
				`{"ingested":`+strconv.Itoa(
					len(events),
				)+`,"failed":0}`,
			)
		}))
		t.Cleanup(server.Close)
		h := testHandler(
			t,
			server,
			WithBatchSize(2),
		)
		for _, msg := range []string{"one", "two", "three"} {
			_, err := h.Write([]byte(msg))
			require.NoError(t, err)
		}
		flushHandler(t, h)
		require.EqualValues(
			t,
			2,
			requests.Load(),
		)
		require.EqualValues(
			t,
			3,
			eventCount.Load(),
		)
		require.ElementsMatch(t, []string{"one", "two", "three"}, []string{<-messages, <-messages, <-messages})
		require.Equal(t, uint64(3), h.Stats().Sent)
	})

	t.Run("batch fills by count only", func(t *testing.T) {
		t.Parallel()
		var requests atomic.Int64
		eventCounts := make(chan int, 3)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			events, _, err := requestEvents(r)
			if err != nil {
				http.Error(
					w,
					err.Error(),
					http.StatusBadRequest,
				)
				return
			}
			requests.Add(1)
			eventCounts <- len(events)
			responseJSON(
				w,
				200,
				`{"ingested":`+strconv.Itoa(
					len(events),
				)+`,"failed":0}`,
			)
		}))
		t.Cleanup(server.Close)
		h := testHandler(
			t,
			server,
			WithBatchSize(3),
			WithFlushInterval(time.Hour),
			WithMaxRetries(0),
		)
		for _, msg := range []string{"one", "two", "three"} {
			_, err := h.Write([]byte(msg))
			require.NoError(t, err)
		}
		select {
		case count := <-eventCounts:
			require.Equal(t, 3, count)
		case <-time.After(
			5 * time.Second,
		):
			t.Fatal("count-triggered batch was not delivered")
		}
		require.NoError(
			t,
			h.Close(),
		)
		require.EqualValues(
			t,
			1,
			requests.Load(),
		)
		require.Equal(t, uint64(3), h.Stats().Sent)
	})

	t.Run("batch by bytes", func(t *testing.T) {
		t.Parallel()
		var requests atomic.Int64
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requests.Add(1)
			events, _, err := requestEvents(r)
			if err != nil {
				http.Error(w, "bad", http.StatusBadRequest)
				return
			}
			responseJSON(
				w,
				200,
				`{"ingested":`+strconv.Itoa(
					len(events),
				)+`}`,
			)
		}))
		t.Cleanup(server.Close)
		h := testHandler(
			t,
			server,
			WithMaxBatchBytes(
				1<<20,
			),
		)
		// two encoded lines each exceed half the batch limit.
		msg := strings.Repeat(
			"x",
			600<<10,
		)
		_, err := h.Write([]byte(msg))
		require.NoError(t, err)
		_, err = h.Write([]byte(msg))
		require.NoError(t, err)
		flushHandler(t, h)
		require.EqualValues(
			t,
			2,
			requests.Load(),
		)
	})

	t.Run("batch by bytes exact boundary", func(t *testing.T) {
		t.Parallel()
		assertBatch := func(t *testing.T, over bool, want int) {
			t.Helper()
			var batches [][]map[string]any
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				events, _, err := requestEvents(r)
				if err != nil {
					http.Error(w, "bad", http.StatusBadRequest)
					return
				}
				batches = append(batches, events)
				responseJSON(
					w,
					200,
					`{"ingested":`+strconv.Itoa(
						len(events),
					)+`}`,
				)
			}))
			t.Cleanup(server.Close)
			fixed := time.Date(
				2026,
				10,
				28,
				0,
				0,
				0,
				0,
				time.UTC,
			)
			h := testHandler(
				t,
				server,
				WithCompression(false),
				WithMaxBatchBytes(
					1<<20,
				),
				WithFlushInterval(time.Hour),
				withClock(func() time.Time { return fixed }, func(context.Context, time.Duration) bool { return true }),
			)
			msgs := []string{strings.Repeat(
				"a",
				300<<10,
			), strings.Repeat(
				"b",
				300<<10,
			), ""}
			lineLen := func(message string) int {
				line, err := h.appendEvent(
					nil,
					newEvent(
						fixed,
						0,
						false,
						[]byte(message),
					),
				)
				require.NoError(t, err)
				return len(line)
			}
			target := 1 << 20
			if over {
				target++
			}
			msgs[2] = strings.Repeat(
				"c",
				target-lineLen(msgs[0])-lineLen(msgs[1])-lineLen(""),
			)
			for _, message := range msgs {
				_, err := h.Write([]byte(message))
				require.NoError(t, err)
			}
			flushHandler(t, h)
			require.Len(t, batches, want)
			count := 0
			for _, batch := range batches {
				count += len(batch)
			}
			require.Equal(t, 3, count)
		}
		t.Run("exactly 1 MiB batches together", func(t *testing.T) {
			t.Parallel()
			assertBatch(t, false, 1)
		})
		t.Run("one byte over splits", func(t *testing.T) {
			t.Parallel()
			assertBatch(t, true, 2)
		})
	})

	t.Run("queue full drops", func(t *testing.T) {
		t.Parallel()
		started, release := make(chan struct{}), make(chan struct{})
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			select {
			case <-started:
			default:
				close(started)
			}
			<-release
			responseJSON(w, 200, `{"ingested":1}`)
		}))
		t.Cleanup(server.Close)
		h := testHandler(
			t,
			server,
			WithBatchSize(1),
			WithQueueSize(1),
			WithMaxRetries(0),
		)
		_, err := h.Write([]byte("first"))
		require.NoError(t, err)
		select {
		case <-started:
		case <-time.After(
			5 * time.Second,
		):
			t.Fatal("request did not start")
		}
		_, _ = h.Write([]byte("queued"))
		start := time.Now()
		_, err = h.Write([]byte("dropped"))
		require.NoError(t, err)
		require.Less(
			t,
			time.Since(start),
			time.Second,
		)
		close(release)
		ctx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()
		_ = h.Shutdown(ctx)
		require.GreaterOrEqual(t, h.Stats().Dropped, uint64(1))
	})

	t.Run("byte budget drops", func(t *testing.T) {
		t.Parallel()
		started, release := make(chan struct{}), make(chan struct{})
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			select {
			case <-started:
			default:
				close(started)
			}
			<-release
			responseJSON(w, 200, `{"ingested":1}`)
		}))
		t.Cleanup(server.Close)
		h := testHandler(
			t,
			server,
			WithBatchSize(1),
			WithMaxBatchBytes(
				1<<20,
			),
			WithQueueSize(10),
			WithMaxQueueBytes(
				1<<20,
			),
		)
		message := []byte(
			strings.Repeat(
				"x",
				300<<10,
			),
		)
		_, err := h.Write(message)
		require.NoError(t, err)
		select {
		case <-started:
		case <-time.After(
			5 * time.Second,
		):
			t.Fatal("request did not start")
		}
		for range 3 {
			_, err = h.Write(message)
			require.NoError(t, err)
		}
		require.GreaterOrEqual(t, h.Stats().Dropped, uint64(1))
		close(release)
		ctx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()
		_ = h.Shutdown(ctx)
	})

	t.Run("caller mutates slice", func(t *testing.T) {
		t.Parallel()
		got := make(chan string, 1)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			events, _, _ := requestEvents(r)
			got <- events[0]["message"].(string)
			responseJSON(w, 200, `{"ingested":1}`)
		}))
		t.Cleanup(server.Close)
		h := testHandler(
			t,
			server,
			WithBatchSize(1),
		)
		p := []byte("original")
		_, err := h.Write(p)
		require.NoError(t, err)
		copy(p, "mutated!")
		flushHandler(t, h)
		require.Equal(
			t,
			"original",
			<-got,
		)
	})

	t.Run("oversized single event", func(t *testing.T) {
		t.Parallel()
		var eventCount atomic.Int64
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			events, _, err := requestEvents(r)
			if err != nil {
				http.Error(w, "bad", http.StatusBadRequest)
				return
			}
			eventCount.Store(int64(len(events)))
			responseJSON(w, 200, `{"ingested":1}`)
		}))
		t.Cleanup(server.Close)
		h := testHandler(
			t,
			server,
			WithMaxBatchBytes(
				1<<20,
			),
			WithBatchSize(10),
		)
		message := strings.Repeat(
			"<\x01",
			256<<10,
		)
		_, err := h.Write([]byte(message))
		require.NoError(t, err)
		flushHandler(t, h)
		require.EqualValues(
			t,
			1,
			eventCount.Load(),
		)
	})
	t.Run("batch by interval", func(t *testing.T) {
		t.Parallel()
		got := make(chan struct{}, 1)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			got <- struct{}{}
			responseJSON(w, 200, `{"ingested":1}`)
		}))
		t.Cleanup(server.Close)
		h := testHandler(
			t,
			server,
			WithFlushInterval(
				20*time.Millisecond,
			),
		)
		_, err := h.Write([]byte("tick"))
		require.NoError(t, err)
		select {
		case <-got:
		case <-time.After(
			5 * time.Second,
		):
			t.Fatal("flush interval did not send batch")
		}
	})
}

func TestHandler_WriteForRaceCondition(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		events, _, err := requestEvents(r)
		if err != nil {
			http.Error(w, "bad", http.StatusBadRequest)
			return
		}
		responseJSON(
			w,
			200,
			`{"ingested":`+strconv.Itoa(
				len(events),
			)+`}`,
		)
	}))
	t.Cleanup(server.Close)
	h := testHandler(
		t,
		server,
		WithBatchSize(100),
		WithQueueSize(10000),
	)
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
	ctx, cancel := context.WithTimeout(
		context.Background(),
		30*time.Second,
	)
	defer cancel()
	require.NoError(
		t,
		h.Shutdown(ctx),
	)
	stats := h.Stats()
	require.Equal(
		t,
		uint64(
			workers*writes,
		),
		stats.Sent+stats.Dropped+stats.Failed,
	)
}

func TestHandler_Flush(t *testing.T) {
	t.Parallel()
	t.Run("concurrent flush all return", func(t *testing.T) {
		t.Parallel()
		var count atomic.Int64
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ev, _, _ := requestEvents(r)
			count.Add(int64(len(ev)))
			responseJSON(
				w,
				200,
				`{"ingested":`+strconv.Itoa(
					len(ev),
				)+`}`,
			)
		}))
		t.Cleanup(server.Close)
		h := testHandler(
			t,
			server,
			WithFlushInterval(time.Hour),
		)
		for range 1010 {
			_, err := h.Write([]byte("x"))
			require.NoError(t, err)
		}
		var wg sync.WaitGroup
		errs := make(chan error, 8)
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(
					context.Background(),
					10*time.Second,
				)
				defer cancel()
				errs <- h.Flush(ctx)
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			require.NoError(t, err)
		}
		require.EqualValues(
			t,
			1010,
			count.Load(),
		)
		require.Equal(t, uint64(1010), h.Stats().Sent)
	})
	t.Run("flush deadline", func(t *testing.T) {
		t.Parallel()
		started, release := make(chan struct{}), make(chan struct{})
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			close(started)
			<-release
			responseJSON(w, 200, `{"ingested":1}`)
		}))
		t.Cleanup(server.Close)
		defer close(release)
		h := testHandler(
			t,
			server,
			WithBatchSize(1),
		)
		_, err := h.Write([]byte("hang"))
		require.NoError(t, err)
		<-started
		ctx, cancel := context.WithTimeout(
			context.Background(),
			50*time.Millisecond,
		)
		defer cancel()
		require.ErrorIs(
			t,
			h.Flush(ctx),
			context.DeadlineExceeded,
		)
	})
	t.Run("flush idle", func(t *testing.T) {
		t.Parallel()
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { responseJSON(w, 200, `{"ingested":1}`) }))
		t.Cleanup(server.Close)
		h := testHandler(t, server)
		ctx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()
		require.NoError(
			t,
			h.Flush(ctx),
		)
	})
	t.Run("flush wake drains queue", func(t *testing.T) {
		t.Parallel()
		var eventCount atomic.Int64
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			events, _, err := requestEvents(r)
			if err != nil {
				http.Error(
					w,
					err.Error(),
					http.StatusBadRequest,
				)
				return
			}
			eventCount.Add(int64(len(events)))
			responseJSON(
				w,
				200,
				`{"ingested":`+strconv.Itoa(
					len(events),
				)+`,"failed":0}`,
			)
		}))
		t.Cleanup(server.Close)
		h := testHandler(
			t,
			server,
			WithFlushInterval(time.Hour),
		)
		for i := 1; i <= 1010; i++ {
			_, err := fmt.Fprintf(h, "event-%04d", i)
			require.NoError(t, err)
		}
		ctx, cancel := context.WithTimeout(
			context.Background(),
			2*time.Second,
		)
		defer cancel()
		require.NoError(
			t,
			h.Flush(ctx),
		)
		require.EqualValues(
			t,
			1010,
			eventCount.Load(),
		)
		stats := h.Stats()
		require.Equal(t, uint64(1010), stats.Sent)
		require.Zero(t, stats.Failed)
		require.Zero(t, stats.Dropped)
	})

}

func TestHandler_Shutdown(t *testing.T) {
	t.Parallel()
	t.Run("shutdown after worker done returns nil", func(t *testing.T) {
		t.Parallel()
		for range 64 {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { responseJSON(w, 200, `{"ingested":1}`) }))
			t.Cleanup(server.Close)
			h := testHandler(t, server)
			h.mu.Lock()
			h.closed = true
			h.mu.Unlock()
			h.wake <- struct{}{}
			select {
			case <-h.done:
			case <-time.After(
				5 * time.Second,
			):
				t.Fatal("worker did not exit")
			}
			ctx, cancel := context.WithCancel(
				context.Background(),
			)
			cancel()
			require.NoError(
				t,
				h.Shutdown(ctx),
			)
		}
	})
	t.Run("shutdown during backoff", func(t *testing.T) {
		t.Parallel()
		started := make(chan struct{})
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			close(started)
			w.Header().Set("Retry-After", "30")
			responseJSON(w, 429, "later")
		}))
		t.Cleanup(server.Close)
		waiting := make(chan struct{})
		h := testHandler(
			t,
			server,
			WithBatchSize(1),
			WithMaxRetries(3),
			withClock(time.Now, func(ctx context.Context, _ time.Duration) bool { close(waiting); <-ctx.Done(); return false }),
		)
		_, err := h.Write([]byte("backoff"))
		require.NoError(t, err)
		<-started
		<-waiting
		ctx, cancel := context.WithTimeout(
			context.Background(),
			time.Second,
		)
		defer cancel()
		require.ErrorIs(
			t,
			h.Shutdown(ctx),
			context.DeadlineExceeded,
		)
		<-h.done
		require.Equal(t, uint64(1), h.Stats().Failed)
	})
	t.Run("shutdown hanging server", func(t *testing.T) {
		t.Parallel()
		started, release := make(chan struct{}), make(chan struct{})
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			close(started)
			<-release
			responseJSON(w, 200, `{"ingested":1}`)
		}))
		t.Cleanup(server.Close)
		var releaseOnce sync.Once
		defer releaseOnce.Do(func() { close(release) })
		h := testHandler(
			t,
			server,
			WithBatchSize(1),
		)
		_, err := h.Write([]byte("hang"))
		require.NoError(t, err)
		select {
		case <-started:
		case <-time.After(
			5 * time.Second,
		):
			t.Fatal("request did not start")
		}
		ctx, cancel := context.WithTimeout(
			context.Background(),
			50*time.Millisecond,
		)
		defer cancel()
		require.ErrorIs(
			t,
			h.Shutdown(ctx),
			context.DeadlineExceeded,
		)
		releaseOnce.Do(func() { close(release) })
		select {
		case <-h.done:
		default:
			t.Fatal("worker did not exit")
		}
		require.Equal(t, uint64(1), h.Stats().Failed)
	})
	t.Run("concurrent shutdown uses each callers deadline", func(t *testing.T) {
		t.Parallel()
		started, release := make(chan struct{}), make(chan struct{})
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			close(started)
			<-release
			responseJSON(w, 200, `{"ingested":1}`)
		}))
		t.Cleanup(server.Close)
		var releaseOnce sync.Once
		defer releaseOnce.Do(func() { close(release) })
		h := testHandler(
			t,
			server,
			WithBatchSize(1),
		)
		_, err := h.Write([]byte("hang"))
		require.NoError(t, err)
		select {
		case <-started:
		case <-time.After(
			5 * time.Second,
		):
			t.Fatal("request did not start")
		}
		firstDone := make(chan error, 1)
		go func() {
			firstDone <- h.Shutdown(
				context.Background(),
			)
		}()
		secondCtx, cancel := context.WithTimeout(
			context.Background(),
			200*time.Millisecond,
		)
		defer cancel()
		start := time.Now()
		err = h.Shutdown(secondCtx)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Less(
			t,
			time.Since(start),
			time.Second,
		)
		releaseOnce.Do(func() { close(release) })
		require.NoError(
			t,
			<-firstDone,
		)
	})
	t.Run("shutdown reports suppressed", func(t *testing.T) {
		t.Parallel()
		now := time.Date(
			2026,
			10,
			28,
			0,
			0,
			0,
			0,
			time.UTC,
		)
		var clockMu sync.Mutex
		var reportsMu sync.Mutex
		var reports []string
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { responseJSON(w, 400, "bad") }))
		t.Cleanup(server.Close)
		h := testHandler(
			t,
			server,
			WithBatchSize(1),
			WithMaxRetries(0),
			WithErrorHandler(func(err error) {
				reportsMu.Lock()
				reports = append(
					reports,
					err.Error(),
				)
				reportsMu.Unlock()
			}),
			withClock(func() time.Time { clockMu.Lock(); defer clockMu.Unlock(); return now }, func(context.Context, time.Duration) bool { return true }),
		)
		for i, message := range []string{"first", "second"} {
			_, err := h.Write([]byte(message))
			require.NoError(t, err)
			flushHandler(t, h)
			if i == 0 {
				clockMu.Lock()
				now = now.Add(time.Second)
				clockMu.Unlock()
			}
		}
		h.dropped.Add(2)
		require.NoError(
			t,
			h.Close(),
		)
		reportsMu.Lock()
		defer reportsMu.Unlock()
		require.Len(t, reports, 2)
		require.Contains(t, reports[1], "suppressed 1")
		require.Contains(t, reports[1], "axiom: dropped 2 events")
	})
}

// not parallel: noGoroutineLeak reads the process-wide goroutine count
//
//nolint:paralleltest,tparallel // noGoroutineLeak reads the process-wide goroutine count
func TestHandler_Close(t *testing.T) {
	t.Run("close idempotent", func(t *testing.T) {
		t.Parallel()
		var count atomic.Int64
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { count.Add(1); responseJSON(w, 200, `{"ingested":1}`) }))
		t.Cleanup(server.Close)
		h := testHandler(t, server)
		_, err := h.Write([]byte("queued"))
		require.NoError(t, err)
		require.NoError(
			t,
			h.Close(),
		)
		require.NoError(
			t,
			h.Close(),
		)
		require.EqualValues(
			t,
			1,
			count.Load(),
		)
		n, err := h.Write([]byte("after"))
		require.NoError(t, err)
		require.Equal(t, 5, n)
		require.Equal(t, uint64(1), h.Stats().Dropped)
		require.ErrorIs(
			t,
			h.Flush(
				context.Background(),
			),
			ErrClosed,
		)
	})
	t.Run("no goroutine leak", func(t *testing.T) {
		before := runtime.NumGoroutine()
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { responseJSON(w, 200, `{"ingested":1}`) }))
		t.Cleanup(server.Close)
		h := testHandler(
			t,
			server,
			WithBatchSize(1),
		)
		_, err := h.Write([]byte("x"))
		require.NoError(t, err)
		require.NoError(
			t,
			h.Close(),
		)
		server.Close()
		deadline := time.Now().Add(
			5 * time.Second,
		)
		for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
			runtime.Gosched()
		}
		require.LessOrEqual(
			t,
			runtime.NumGoroutine(),
			before,
		)
	})

}

func TestHandler_deliver(t *testing.T) {
	t.Parallel()
	t.Run("max retries zero", func(t *testing.T) {
		t.Parallel()
		var attempts atomic.Int64
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { attempts.Add(1); responseJSON(w, 503, "down") }))
		t.Cleanup(server.Close)
		var waited atomic.Bool
		h := testHandler(
			t,
			server,
			WithBatchSize(1),
			WithMaxRetries(0),
			withClock(time.Now, func(context.Context, time.Duration) bool { waited.Store(true); return false }),
		)
		_, err := h.Write([]byte("x"))
		require.NoError(t, err)
		flushHandler(t, h)
		require.EqualValues(
			t,
			1,
			attempts.Load(),
		)
		require.False(
			t,
			waited.Load(),
		)
	})
	t.Run("failed response", func(t *testing.T) {
		t.Parallel()
		var reported atomic.Int64
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			responseJSON(w, 200, `{"ingested":0,"failed":1,"failures":[{"error":"bad row"}]}`)
		}))
		t.Cleanup(server.Close)
		h := testHandler(
			t,
			server,
			WithBatchSize(1),
			WithErrorHandler(func(error) { reported.Add(1) }),
		)
		_, err := h.Write([]byte("x"))
		require.NoError(t, err)
		flushHandler(t, h)
		require.Equal(t, uint64(1), h.Stats().Failed)
		require.EqualValues(
			t,
			1,
			reported.Load(),
		)
	})
	t.Run("redirect is not followed", func(t *testing.T) {
		t.Parallel()
		var targetCalls atomic.Int64
		target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			targetCalls.Add(1)
			responseJSON(w, 200, `{"ingested":1}`)
		}))
		t.Cleanup(target.Close)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(
				w,
				r,
				target.URL,
				http.StatusTemporaryRedirect,
			)
		}))
		t.Cleanup(server.Close)
		h := testHandler(
			t,
			server,
			WithBatchSize(1),
			WithMaxRetries(0),
		)
		_, err := h.Write([]byte("x"))
		require.NoError(t, err)
		flushHandler(t, h)
		require.Equal(t, uint64(1), h.Stats().Failed)
		require.Zero(
			t,
			targetCalls.Load(),
		)
	})
	t.Run("retry after HTTP date", func(t *testing.T) {
		t.Parallel()
		var attempts atomic.Int64
		var delaysMu sync.Mutex
		var delays []time.Duration
		now := time.Date(
			2026,
			10,
			28,
			0,
			0,
			0,
			0,
			time.UTC,
		)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if attempts.Add(1) == 1 {
				w.Header().Set(
					"Retry-After",
					now.Add(
						7*time.Second,
					).Format(http.TimeFormat),
				)
				responseJSON(w, 429, "limited")
				return
			}
			responseJSON(w, 200, `{"ingested":1}`)
		}))
		t.Cleanup(server.Close)
		h := testHandler(
			t,
			server,
			WithBatchSize(1),
			WithMaxRetries(1),
			WithCompression(false),
			withClock(func() time.Time { return now }, func(_ context.Context, d time.Duration) bool {
				delaysMu.Lock()
				delays = append(delays, d)
				delaysMu.Unlock()
				return true
			}),
		)
		_, err := h.Write([]byte("retry"))
		require.NoError(t, err)
		flushHandler(t, h)
		require.EqualValues(
			t,
			2,
			attempts.Load(),
		)
		require.Equal(t, []time.Duration{7 * time.Second}, delays)
	})
	t.Run("retry after seconds", func(t *testing.T) {
		t.Parallel()
		var attempts atomic.Int64
		var delay atomic.Int64
		bodies := make(chan []byte, 2)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
		t.Cleanup(server.Close)
		h := testHandler(
			t,
			server,
			WithBatchSize(1),
			WithMaxRetries(1),
			withClock(time.Now, func(_ context.Context, d time.Duration) bool {
				delay.Store(int64(d))
				return true
			}),
		)
		_, err := h.Write([]byte("retry"))
		require.NoError(t, err)
		flushHandler(t, h)
		require.EqualValues(
			t,
			2,
			attempts.Load(),
		)
		require.EqualValues(
			t,
			time.Second,
			delay.Load(),
		)
		first, second := <-bodies, <-bodies
		require.NotEmpty(t, first)
		require.Equal(t, first, second)
	})
	t.Run("retry after overflow clamps before conversion", func(t *testing.T) {
		t.Parallel()
		var calls atomic.Int64
		delays := make(chan time.Duration, 1)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if calls.Add(1) == 1 {
				w.Header().Set("Retry-After", "9223372037")
				responseJSON(w, 429, "limited")
				return
			}
			responseJSON(w, 200, `{"ingested":1}`)
		}))
		t.Cleanup(server.Close)
		h := testHandler(
			t,
			server,
			WithBatchSize(1),
			WithMaxRetries(1),
			withClock(time.Now, func(_ context.Context, d time.Duration) bool { delays <- d; return true }),
		)
		_, err := h.Write([]byte("retry"))
		require.NoError(t, err)
		flushHandler(t, h)
		require.EqualValues(
			t,
			2,
			calls.Load(),
		)
		require.Equal(
			t,
			30*time.Second,
			<-delays,
		)
	})
	t.Run("5xx retries and 400 does not", func(t *testing.T) {
		t.Parallel()
		var attempts atomic.Int64
		var reports atomic.Int64
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if attempts.Add(1) <= 3 {
				responseJSON(w, 503, "down")
				return
			}
			responseJSON(w, 400, "invalid")
		}))
		t.Cleanup(server.Close)
		h := testHandler(
			t,
			server,
			WithBatchSize(1),
			WithMaxRetries(2),
			WithErrorHandler(func(error) { reports.Add(1) }),
			withClock(time.Now, func(context.Context, time.Duration) bool { return true }),
		)
		_, err := h.Write([]byte("retry"))
		require.NoError(t, err)
		flushHandler(t, h)
		_, err = h.Write([]byte("no retry"))
		require.NoError(t, err)
		flushHandler(t, h)
		require.EqualValues(
			t,
			4,
			attempts.Load(),
		)
		require.Equal(t, uint64(2), h.Stats().Failed)
		require.EqualValues(
			t,
			1,
			reports.Load(),
		)
	})
	t.Run("retry body identical", func(t *testing.T) {
		t.Parallel()
		var bodiesMu sync.Mutex
		var bodies [][]byte
		var count atomic.Int64
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
		t.Cleanup(server.Close)
		h := testHandler(
			t,
			server,
			WithBatchSize(1),
			WithMaxRetries(1),
			WithCompression(false),
			withClock(time.Now, func(context.Context, time.Duration) bool { return true }),
		)
		_, err := h.Write([]byte("retry body"))
		require.NoError(t, err)
		flushHandler(t, h)
		bodiesMu.Lock()
		defer bodiesMu.Unlock()
		require.Len(t, bodies, 2)
		require.NotEmpty(t, bodies[0])
		require.Equal(t, bodies[0], bodies[1])
	})
	t.Run("200 incomplete accounting", func(t *testing.T) {
		t.Parallel()
		check := func(
			t *testing.T,
			body string,
			sent uint64,
			failed uint64,
			wantReported int64,
		) {
			t.Helper()
			var reported atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { responseJSON(w, 200, body) }))
			t.Cleanup(server.Close)
			h := testHandler(
				t,
				server,
				WithBatchSize(2),
				WithErrorHandler(func(error) { reported.Add(1) }),
			)
			for range 2 {
				_, err := h.Write([]byte("x"))
				require.NoError(t, err)
			}
			flushHandler(t, h)
			st := h.Stats()
			require.Equal(
				t,
				uint64(2),
				st.Sent+st.Dropped+st.Failed,
			)
			require.Equal(t, sent, st.Sent)
			require.Equal(t, failed, st.Failed)
			require.Equal(
				t,
				wantReported,
				reported.Load(),
			)
		}
		t.Run("empty response", func(t *testing.T) {
			t.Parallel()
			check(
				t,
				`{}`,
				0,
				2,
				1,
			)
		})
		t.Run("malformed response", func(t *testing.T) {
			t.Parallel()
			check(
				t,
				`{"ingested":`,
				0,
				2,
				1,
			)
		})
		t.Run("inconsistent accounting", func(t *testing.T) {
			t.Parallel()
			check(
				t,
				`{"ingested":3,"failed":3}`,
				2,
				0,
				1,
			)
		})
	})
	t.Run("failure reason capped", func(t *testing.T) {
		t.Parallel()
		t.Run("does not split a rune", func(t *testing.T) {
			t.Parallel()
			reason := strings.Repeat(
				"a",
				errorBodyBytes-2,
			) + "界" + strings.Repeat("b", 20)
			body, err := json.Marshal(map[string]any{"ingested": 0, "failed": 1, "failures": []any{map[string]string{"error": reason}}})
			require.NoError(t, err)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				responseJSON(w, http.StatusOK, string(body))
			}))
			t.Cleanup(server.Close)
			reported := make(chan string, 1)
			h := testHandler(
				t,
				server,
				WithBatchSize(1),
				WithErrorHandler(func(err error) { reported <- err.Error() }),
			)
			_, err = h.Write([]byte("x"))
			require.NoError(t, err)
			flushHandler(t, h)
			select {
			case text := <-reported:
				require.True(
					t,
					utf8.ValidString(text),
				)
			case <-time.After(
				5 * time.Second,
			):
				t.Fatal("error was not reported")
			}
		})
		reason, reported := strings.Repeat(
			"x",
			4<<10,
		), make(chan string, 1)
		body, err := json.Marshal(map[string]any{"ingested": 0, "failed": 1, "failures": []any{map[string]string{"error": reason}}})
		require.NoError(t, err)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			responseJSON(w, 200, string(body))
		}))
		t.Cleanup(server.Close)
		h := testHandler(
			t,
			server,
			WithBatchSize(1),
			WithErrorHandler(func(err error) { reported <- err.Error() }),
		)
		_, err = h.Write([]byte("x"))
		require.NoError(t, err)
		flushHandler(t, h)
		select {
		case text := <-reported:
			require.Less(
				t,
				len(text),
				2<<10,
			)
		case <-time.After(
			5 * time.Second,
		):
			t.Fatal("error was not reported")
		}
	})
	t.Run("failure reason redacts before cap", func(t *testing.T) {
		t.Parallel()
		token := "xaat-secret-prefix-that-must-not-leak"
		reason := strings.Repeat(
			"a",
			errorBodyBytes-12,
		) + token
		body, err := json.Marshal(map[string]any{"ingested": 0, "failed": 1, "failures": []any{map[string]string{"error": reason}}})
		require.NoError(t, err)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			responseJSON(w, 200, string(body))
		}))
		t.Cleanup(server.Close)
		reported := make(chan string, 1)
		h, err := NewHandler(
			Token(token),
			"logs",
			withBaseURL(server.URL),
			WithBatchSize(1),
			WithFlushInterval(time.Hour),
			WithErrorHandler(func(err error) { reported <- err.Error() }),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = h.Close() })
		_, err = h.Write([]byte("x"))
		require.NoError(t, err)
		flushHandler(t, h)
		got := <-reported
		require.NotContains(t, got, token[:8])
	})
	t.Run("response error redacts before cap", func(t *testing.T) {
		t.Parallel()
		token := "xaat-secret-prefix-that-must-not-leak"
		reason := strings.Repeat(
			"a",
			errorBodyBytes-12,
		) + token
		reported := make(chan string, 1)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			responseJSON(w, http.StatusInternalServerError, reason)
		}))
		t.Cleanup(server.Close)
		h, err := NewHandler(
			Token(token),
			"logs",
			withBaseURL(server.URL),
			WithMaxRetries(0),
			WithBatchSize(1),
			WithFlushInterval(time.Hour),
			WithErrorHandler(func(err error) { reported <- err.Error() }),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = h.Close() })
		_, err = h.Write([]byte("x"))
		require.NoError(t, err)
		flushHandler(t, h)
		got := <-reported
		require.NotContains(t, got, token[:8])
	})
	t.Run("token never in errors", func(t *testing.T) {
		t.Parallel()
		var errorsMu sync.Mutex
		var messages []string
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { responseJSON(w, 400, "xaat-secret leaked?") }))
		t.Cleanup(server.Close)
		h := testHandler(
			t,
			server,
			WithBatchSize(1),
			WithMaxRetries(0),
			WithErrorHandler(func(err error) {
				errorsMu.Lock()
				messages = append(
					messages,
					err.Error(),
				)
				errorsMu.Unlock()
			}),
		)
		_, err := h.Write([]byte("x"))
		require.NoError(t, err)
		flushHandler(t, h)
		errorsMu.Lock()
		defer errorsMu.Unlock()
		require.Len(t, messages, 1)
		require.NotContains(t, messages[0], "xaat-secret")
	})
}

func TestHandler_reportWith(t *testing.T) {
	t.Parallel()
	t.Run("error handler rate limited", func(t *testing.T) {
		t.Parallel()
		var attempts, reports atomic.Int64
		now := time.Date(
			2026,
			10,
			28,
			0,
			0,
			0,
			0,
			time.UTC,
		)
		var clockMu sync.Mutex
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { attempts.Add(1); responseJSON(w, 400, "bad") }))
		t.Cleanup(server.Close)
		h := testHandler(
			t,
			server,
			WithBatchSize(1),
			WithMaxRetries(0),
			WithErrorHandler(func(error) { reports.Add(1) }),
			withClock(func() time.Time { clockMu.Lock(); defer clockMu.Unlock(); return now }, func(context.Context, time.Duration) bool { return true }),
		)
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
		now = now.Add(
			30 * time.Second,
		)
		clockMu.Unlock()
		_, err = h.Write([]byte("c"))
		require.NoError(t, err)
		flushHandler(t, h)
		require.EqualValues(
			t,
			3,
			attempts.Load(),
		)
		require.EqualValues(
			t,
			2,
			reports.Load(),
		)
	})
	t.Run("error handler panic continues", func(t *testing.T) {
		t.Parallel()
		var requests atomic.Int64
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { requests.Add(1); responseJSON(w, 400, "bad") }))
		t.Cleanup(server.Close)
		h := testHandler(
			t,
			server,
			WithBatchSize(1),
			WithMaxRetries(0),
			WithErrorHandler(func(error) { panic("handler") }),
		)
		_, err := h.Write([]byte("a"))
		require.NoError(t, err)
		flushHandler(t, h)
		_, err = h.Write([]byte("b"))
		require.NoError(t, err)
		flushHandler(t, h)
		require.EqualValues(
			t,
			2,
			requests.Load(),
		)
	})
	t.Run("error handler panic writes stderr", func(t *testing.T) {
		t.Parallel()
		var requests atomic.Int64
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			requests.Add(1)
			responseJSON(w, 400, "bad")
		}))
		t.Cleanup(server.Close)
		output := &lockedTestBuffer{}
		h := testHandler(
			t,
			server,
			withErrorOutput(output),
			WithBatchSize(1),
			WithMaxRetries(0),
			WithErrorHandler(func(error) {
				panic(
					errors.New("boom xaat-secret"),
				)
			}),
		)
		for _, message := range []string{"first", "second"} {
			_, err := h.Write([]byte(message))
			require.NoError(t, err)
			flushHandler(t, h)
		}
		require.EqualValues(
			t,
			2,
			requests.Load(),
		)
		require.Contains(
			t,
			output.String(),
			"axiom: error handler panicked: boom",
		)
		require.NotContains(
			t,
			output.String(),
			"xaat-secret",
		)
	})
	t.Run("error handler logs into own logger", func(t *testing.T) {
		t.Parallel()
		var requests atomic.Int64
		events := make(chan []map[string]any, 2)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requests.Add(1)
			decoded, _, decodeErr := requestEvents(r)
			if decodeErr != nil {
				http.Error(
					w,
					decodeErr.Error(),
					http.StatusBadRequest,
				)
				return
			}
			events <- decoded
			responseJSON(w, 400, "bad")
		}))
		t.Cleanup(server.Close)
		var logger *loginjector.Logger
		reported := make(chan struct{})
		var reportOnce sync.Once
		h, err := NewHandler(
			"xaat-secret",
			"logs",
			withBaseURL(server.URL),
			WithBatchSize(1),
			WithMaxRetries(0),
			WithErrorHandler(func(error) {
				reportOnce.Do(func() {
					logger.WriteLog(1, []byte("reported"))
					close(reported)
				})
			}),
		)
		require.NoError(t, err)
		logger = loginjector.NewLogger(0, h)
		_, err = h.Write([]byte("failure"))
		require.NoError(t, err)
		select {
		case <-reported:
		case <-time.After(
			5 * time.Second,
		):
			t.Fatal("error handler did not report")
		}
		require.NoError(
			t,
			h.Close(),
		)
		require.EqualValues(
			t,
			2,
			requests.Load(),
		)
		var first, second []map[string]any
		select {
		case first = <-events:
		case <-time.After(
			5 * time.Second,
		):
			t.Fatal("first event was not received")
		}
		select {
		case second = <-events:
		case <-time.After(
			5 * time.Second,
		):
			t.Fatal("second event was not received")
		}
		require.Len(t, first, 1)
		require.Len(t, second, 1)
		require.Equal(t, "reported", second[0]["message"])
		require.Equal(t, uint64(2), h.Stats().Failed)
		require.Zero(t, h.Stats().Dropped)
	})
	t.Run("dropped request text still reports pending drops", func(t *testing.T) {
		t.Parallel()
		reported := make(chan string, 1)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { responseJSON(w, 400, "dropped request") }))
		t.Cleanup(server.Close)
		h := testHandler(
			t,
			server,
			WithBatchSize(1),
			WithMaxRetries(0),
			WithFlushInterval(time.Hour),
			WithErrorHandler(func(err error) { reported <- err.Error() }),
		)
		h.dropped.Add(3)
		_, err := h.Write([]byte("x"))
		require.NoError(t, err)
		flushHandler(t, h)
		got := <-reported
		require.Contains(t, got, "dropped request")
		require.Contains(t, got, "axiom: dropped 3 events")
		h.errorMu.Lock()
		reportedDrops := h.dropReported
		h.errorMu.Unlock()
		require.Equal(t, uint64(3), reportedDrops)
	})
	t.Run("drop counts survive rate limit", func(t *testing.T) {
		t.Parallel()
		now := time.Date(
			2026,
			10,
			28,
			0,
			0,
			0,
			0,
			time.UTC,
		)
		var clockMu sync.Mutex
		var reportsMu sync.Mutex
		var reports []string
		started, release := make(chan struct{}, 1), make(chan struct{})
		firstDone := make(chan struct{}, 1)
		var request atomic.Int64
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if request.Add(1) == 1 {
				responseJSON(w, 400, "initial")
				firstDone <- struct{}{}
				return
			}
			select {
			case started <- struct{}{}:
			default:
			}
			<-release
			responseJSON(w, 200, `{"ingested":1}`)
		}))
		t.Cleanup(server.Close)
		h := testHandler(
			t,
			server,
			WithBatchSize(1),
			WithQueueSize(1),
			WithMaxQueueBytes(
				1<<20,
			),
			WithMaxBatchBytes(
				1<<20,
			),
			WithFlushInterval(time.Hour),
			WithMaxRetries(0),
			WithErrorHandler(func(err error) {
				reportsMu.Lock()
				reports = append(
					reports,
					err.Error(),
				)
				reportsMu.Unlock()
			}),
			withClock(func() time.Time { clockMu.Lock(); defer clockMu.Unlock(); return now }, func(context.Context, time.Duration) bool { return true }),
		)
		_, err := h.Write([]byte("first failure"))
		require.NoError(t, err)
		select {
		case <-firstDone:
		case <-time.After(
			5 * time.Second,
		):
			t.Fatal("first request did not finish")
		}
		_, err = h.Write([]byte("blocked"))
		require.NoError(t, err)
		select {
		case <-started:
		case <-time.After(
			5 * time.Second,
		):
			t.Fatal("worker did not enter blocked request")
		}
		// the worker is blocked in request two; fill the queue and create drops at t=0.
		_, err = h.Write([]byte("queued"))
		require.NoError(t, err)
		for range 5 {
			_, err := h.Write([]byte("drop at zero"))
			require.NoError(t, err)
		}
		require.GreaterOrEqual(t, h.Stats().Dropped, uint64(1))
		clockMu.Lock()
		now = now.Add(
			10 * time.Second,
		)
		clockMu.Unlock()
		for range 5 {
			_, err := h.Write([]byte("drop at ten"))
			require.NoError(t, err)
		}
		clockMu.Lock()
		now = now.Add(
			21 * time.Second,
		)
		clockMu.Unlock()
		close(release)
		ctx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()
		require.NoError(
			t,
			h.Shutdown(ctx),
		)
		reportsMu.Lock()
		defer reportsMu.Unlock()
		require.Len(t, reports, 2)
		require.Contains(
			t,
			reports[len(reports)-1],
			fmt.Sprintf("dropped %d events", h.Stats().Dropped),
		)
	})
	t.Run("drop notice not counted as suppressed", func(t *testing.T) {
		t.Parallel()
		now := time.Date(
			2026,
			10,
			28,
			0,
			0,
			0,
			0,
			time.UTC,
		)
		reports := make(chan string, 2)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { responseJSON(w, 200, `{"ingested":1}`) }))
		t.Cleanup(server.Close)
		var errorCalls atomic.Int64
		h := testHandler(
			t,
			server,
			WithFlushInterval(time.Hour),
			WithErrorHandler(func(err error) { errorCalls.Add(1); reports <- err.Error() }),
			withClock(func() time.Time { return now }, func(context.Context, time.Duration) bool { return true }),
		)

		h.report(
			errors.New("first error"),
		)
		require.EqualValues(
			t,
			1,
			errorCalls.Load(),
		)
		h.report(
			errors.New("suppressed error"),
		)
		require.EqualValues(
			t,
			1,
			errorCalls.Load(),
		)
		h.dropped.Add(2)
		h.reportDrops()
		h.reportSuppressed()

		first := <-reports
		final := <-reports
		require.Contains(t, first, "first error")
		require.Contains(t, final, "suppressed 1")
		require.NotContains(t, final, "suppressed 2")
		require.Contains(t, final, "axiom: dropped 2 events")
	})
	t.Run("drop report without transport errors", func(t *testing.T) {
		t.Parallel()
		var reportsMu sync.Mutex
		var reports []string
		started, release := make(chan struct{}), make(chan struct{})
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			select {
			case <-started:
			default:
				close(started)
			}
			<-release
			responseJSON(w, 200, `{"ingested":1}`)
		}))
		t.Cleanup(server.Close)
		h := testHandler(
			t,
			server,
			WithBatchSize(1),
			WithQueueSize(1),
			WithFlushInterval(
				20*time.Millisecond,
			),
			WithErrorHandler(func(err error) {
				reportsMu.Lock()
				reports = append(
					reports,
					err.Error(),
				)
				reportsMu.Unlock()
			}),
		)
		_, _ = h.Write([]byte("one"))
		<-started
		_, _ = h.Write([]byte("two"))
		_, _ = h.Write([]byte("overflow"))
		close(release)
		deadline := time.Now().Add(
			5 * time.Second,
		)
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
		require.Contains(
			t,
			strings.Join(reports, " "),
			"axiom: dropped",
		)
	})
	t.Run("rate limiter clock", func(t *testing.T) {
		t.Parallel()
		var reportsMu sync.Mutex
		var reports []string
		var nowMu sync.Mutex
		now := time.Date(
			2026,
			10,
			28,
			0,
			0,
			0,
			0,
			time.UTC,
		)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { responseJSON(w, 400, "bad") }))
		t.Cleanup(server.Close)
		h := testHandler(
			t,
			server,
			WithBatchSize(1),
			WithMaxRetries(0),
			WithErrorHandler(func(err error) {
				reportsMu.Lock()
				reports = append(
					reports,
					err.Error(),
				)
				reportsMu.Unlock()
			}),
			withClock(func() time.Time { nowMu.Lock(); defer nowMu.Unlock(); return now }, func(context.Context, time.Duration) bool { return true }),
		)
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
	})
}

func BenchmarkCompressPayload(b *testing.B) {
	h, err := NewHandler(
		"xaat-secret",
		"logs",
		withBaseURL("http://127.0.0.1:1"),
	)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := h.Close(); err != nil {
			b.Errorf("close handler: %v", err)
		}
	})
	ev := newEvent(
		time.Now(),
		4,
		true,
		[]byte(fmt.Sprintf("%0120s", "event message")),
	)
	var payload bytes.Buffer
	for range 1000 {
		line, err := h.appendEvent(nil, ev)
		if err != nil {
			b.Fatal(err)
		}
		payload.Write(line)
	}
	b.ReportAllocs()
	b.SetBytes(int64(payload.Len()))
	b.ResetTimer()
	for range b.N {
		if _, err := h.compressPayload(
			payload.Bytes(),
		); err != nil {
			b.Fatal(err)
		}
	}
}

type lockedTestBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *lockedTestBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(p)
}

func (b *lockedTestBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
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
		zr, err := gzip.NewReader(
			strings.NewReader(string(body)),
		)
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
	for _, line := range strings.Split(
		strings.TrimSpace(string(payload)),
		"\n",
	) {
		if line == "" {
			continue
		}
		var event map[string]any
		if err := json.Unmarshal(
			[]byte(line),
			&event,
		); err != nil {
			return nil, body, err
		}
		events = append(events, event)
	}
	return events, body, nil
}

func flushHandler(t *testing.T, h *Handler) {
	t.Helper()
	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()
	require.NoError(
		t,
		h.Flush(ctx),
	)
}
