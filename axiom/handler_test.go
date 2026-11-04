package axiom

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/prorochestvo/loginjector"
	"github.com/prorochestvo/loginjector/levels"
	"github.com/stretchr/testify/require"
)

var (
	_ loginjector.LevelWriter = (*Handler)(nil)
	_ io.Closer               = (*Handler)(nil)
	_ fmt.Formatter           = (*Handler)(nil)
)

func TestNewHandler(t *testing.T) {
	t.Parallel()
	t.Run("defaults", func(t *testing.T) {
		t.Parallel()
		h, err := NewHandler("xaat-secret", "logs")
		require.NoError(t, err)
		t.Cleanup(func() { _ = h.Close() })
		require.Equal(t, "https://us-east-1.aws.edge.axiom.co/v1/ingest/logs", h.cfg.ingestURL)
		require.Equal(t, 1000, h.cfg.batchSize)
		require.Equal(
			t,
			4<<20,
			h.cfg.maxBatchBytes,
		)
		require.Equal(t, time.Second, h.cfg.flushInterval)
		require.Equal(t, 10000, h.cfg.queueSize)
		require.Equal(
			t,
			32<<20,
			h.cfg.maxQueueBytes,
		)
		require.Equal(t, 3, h.cfg.maxRetries)
		require.True(t, h.cfg.compression)
		require.True(t, h.cfg.ownsClient)
		require.Equal(
			t,
			10*time.Second,
			h.cfg.httpClient.Timeout,
		)
	})
	t.Run("empty token", func(t *testing.T) {
		t.Parallel()
		_, err := NewHandler("", "logs")
		require.ErrorContains(t, err, "axiom:")
		require.NotContains(
			t,
			err.Error(),
			"secret",
		)
	})
	t.Run("personal token rejected", func(t *testing.T) {
		t.Parallel()
		_, err := NewHandler("xapt-secret", "logs")
		require.ErrorContains(t, err, "xaat-")
		require.ErrorContains(t, err, "personal tokens are not supported")
		require.NotContains(
			t,
			err.Error(),
			"xapt-secret",
		)
	})
	t.Run("token whitespace", func(t *testing.T) {
		t.Parallel()
		_, err := NewHandler("xaat-secret value", "logs")
		require.ErrorContains(t, err, "axiom:")
		require.NotContains(
			t,
			err.Error(),
			"xaat-secret",
		)
	})
	t.Run("token control", func(t *testing.T) {
		t.Parallel()
		_, err := NewHandler("xaat-secret\x7f", "logs")
		require.ErrorContains(t, err, "axiom:")
		require.NotContains(
			t,
			err.Error(),
			"xaat-secret",
		)
	})
	t.Run("empty dataset", func(t *testing.T) {
		t.Parallel()
		_, err := NewHandler("xaat-secret", "")
		require.ErrorContains(t, err, "dataset")
	})
	t.Run("dataset slash", func(t *testing.T) {
		t.Parallel()
		_, err := NewHandler("xaat-secret", "logs/raw")
		require.ErrorContains(t, err, "slash")
	})
	t.Run("bad option wrapped with context", func(t *testing.T) {
		t.Parallel()
		_, err := NewHandler(
			"xaat-secret",
			"logs",
			WithBatchSize(0),
		)
		require.ErrorContains(t, err, "apply option 1")
		require.ErrorContains(t, err, "batch size")
		require.NotContains(
			t,
			err.Error(),
			"xaat-secret",
		)
	})
	t.Run("nil option", func(t *testing.T) {
		t.Parallel()
		_, err := NewHandler("xaat-secret", "logs", nil)
		require.ErrorContains(t, err, "option must not be nil")
	})
	t.Run("queue byte cross check", func(t *testing.T) {
		t.Parallel()
		_, err := NewHandler(
			"xaat-secret",
			"logs",
			WithMaxQueueBytes(
				1<<20,
			),
		)
		require.ErrorContains(t, err, "at least maximum batch bytes")
	})
	t.Run("dataset path escaping", func(t *testing.T) {
		t.Parallel()
		h, err := NewHandler("xaat-secret", "logs group?west")
		require.NoError(t, err)
		t.Cleanup(func() { _ = h.Close() })
		require.Equal(t, "https://us-east-1.aws.edge.axiom.co/v1/ingest/logs%20group%3Fwest", h.cfg.ingestURL)
	})
	t.Run("base URL seam", func(t *testing.T) {
		t.Parallel()
		baseURL := "http://127.0.0.1:8080"
		h, err := NewHandler(
			"xaat-secret",
			"logs",
			withBaseURL(baseURL),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = h.Close() })
		require.Equal(
			t,
			baseURL+"/v1/ingest/logs",
			h.cfg.ingestURL,
		)
	})
}

func TestHandler_Format(t *testing.T) {
	t.Parallel()
	check := func(t *testing.T, formatted string) {
		t.Helper()
		require.NotContains(t, formatted, "xaat-super-secret-token")
	}
	h, err := NewHandler("xaat-super-secret-token", "logs")
	require.NoError(t, err)
	t.Cleanup(func() { _ = h.Close() })
	t.Run("pointer %v", func(t *testing.T) {
		t.Parallel()
		check(
			t,
			fmt.Sprintf("%v", h),
		)
	})
	t.Run("pointer %+v", func(t *testing.T) {
		t.Parallel()
		check(
			t,
			fmt.Sprintf("%+v", h),
		)
	})
	t.Run("pointer %#v", func(t *testing.T) {
		t.Parallel()
		check(
			t,
			fmt.Sprintf("%#v", h),
		)
	})
	t.Run("pointer %s", func(t *testing.T) {
		t.Parallel()
		check(
			t,
			fmt.Sprintf("%s", h),
		)
	})
	require.NoError(
		t,
		h.Close(),
	)
	value := reflect.ValueOf(h).Elem().Interface()
	t.Run("value %+v", func(t *testing.T) {
		t.Parallel()
		check(
			t,
			fmt.Sprintf("%+v", value),
		)
	})
	t.Run("value %#v", func(t *testing.T) {
		t.Parallel()
		check(
			t,
			fmt.Sprintf("%#v", value),
		)
	})
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
		uint64(workers*writes),
		stats.Sent+stats.Dropped+stats.Failed,
	)
}

func TestHandler_WriteLevel(t *testing.T) {
	t.Parallel()
	t.Run("level is included", func(t *testing.T) {
		t.Parallel()
		got := make(chan []map[string]any, 1)
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
			got <- events
			responseJSON(w, http.StatusOK, `{"ingested":1}`)
		}))
		h := testHandler(
			t,
			server,
			WithFlushInterval(time.Hour),
		)
		_, err := h.WriteLevel(4, []byte("x"))
		require.NoError(t, err)
		flushHandler(t, h)
		event := (<-got)[0]
		require.Equal(t, "error", event["level"])
		require.EqualValues(t, 4, event["level_num"])
	})
	t.Run("plain write omits level", func(t *testing.T) {
		t.Parallel()
		got := make(chan []map[string]any, 1)
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
			got <- events
			responseJSON(w, http.StatusOK, `{"ingested":1}`)
		}))
		h := testHandler(
			t,
			server,
			WithFlushInterval(time.Hour),
		)
		_, err := h.Write([]byte("x"))
		require.NoError(t, err)
		flushHandler(t, h)
		event := (<-got)[0]
		_, hasLevel := event["level"]
		_, hasLevelNum := event["level_num"]
		require.False(t, hasLevel)
		require.False(t, hasLevelNum)
	})
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

// not parallel: the no goroutine leak subtest reads the process-wide goroutine count
//
//nolint:paralleltest,tparallel // the no goroutine leak subtest reads the process-wide goroutine count
func TestHandler_Close(t *testing.T) {
	t.Run("close idempotent", func(t *testing.T) {
		t.Parallel()
		var count atomic.Int64
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { count.Add(1); responseJSON(w, 200, `{"ingested":1}`) }))
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

func TestHandler_appendEvent(t *testing.T) {
	t.Parallel()
	t.Run("level present including zero", func(t *testing.T) {
		t.Parallel()
		h, err := NewHandler("xaat-secret", "logs")
		require.NoError(t, err)
		t.Cleanup(func() { _ = h.Close() })
		dst, err := h.appendEvent(
			nil,
			newEvent(
				time.Date(
					2026,
					10,
					22,
					1,
					2,
					3,
					4,
					time.UTC,
				),
				0,
				true,
				[]byte("hello"),
			),
		)
		require.NoError(t, err)
		var fields map[string]any
		require.NoError(
			t,
			json.Unmarshal(
				dst,
				&fields,
			),
		)
		require.Equal(t, "unknown", fields["level"])
		require.InDelta(
			t,
			0,
			fields["level_num"],
			0,
		)
		require.Equal(t, "hello", fields["message"])
		require.Equal(t, "2026-10-22T01:02:03.000000004Z", fields["_time"])
		require.Equal(t, "\n", string(dst[len(dst)-1]))
	})
	t.Run("level absent", func(t *testing.T) {
		t.Parallel()
		h, err := NewHandler("xaat-secret", "logs")
		require.NoError(t, err)
		t.Cleanup(func() { _ = h.Close() })
		dst, err := h.appendEvent(
			nil,
			newEvent(
				time.Now(),
				levels.Warning,
				false,
				[]byte("plain"),
			),
		)
		require.NoError(t, err)
		var fields map[string]any
		require.NoError(
			t,
			json.Unmarshal(
				dst,
				&fields,
			),
		)
		require.NotContains(t, fields, "level")
		require.NotContains(t, fields, "level_num")
	})
	t.Run("default level name", func(t *testing.T) {
		t.Parallel()
		h, err := NewHandler("xaat-secret", "logs")
		require.NoError(t, err)
		t.Cleanup(func() { _ = h.Close() })
		dst, err := h.appendEvent(
			nil,
			newEvent(
				time.Now(),
				levels.Warning,
				true,
				[]byte("warning"),
			),
		)
		require.NoError(t, err)
		var fields map[string]any
		require.NoError(
			t,
			json.Unmarshal(
				dst,
				&fields,
			),
		)
		require.Equal(t, "warning", fields["level"])
	})
	t.Run("custom level name", func(t *testing.T) {
		t.Parallel()
		h, err := NewHandler(
			"xaat-secret",
			"logs",
			WithLevelName(func(level loginjector.LogLevel) string {
				return "severity-" + string(
					rune(
						'0'+level,
					),
				)
			}),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = h.Close() })
		dst, err := h.appendEvent(
			nil,
			newEvent(
				time.Now(),
				4,
				true,
				[]byte("x"),
			),
		)
		require.NoError(t, err)
		var fields map[string]any
		require.NoError(
			t,
			json.Unmarshal(
				dst,
				&fields,
			),
		)
		require.Equal(t, "severity-4", fields["level"])
	})
	t.Run("static fields merged", func(t *testing.T) {
		t.Parallel()
		h, err := NewHandler(
			"xaat-secret",
			"logs",
			WithFields(map[string]any{"service": "api", "enabled": true}),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = h.Close() })
		dst, err := h.appendEvent(
			nil,
			newEvent(
				time.Now(),
				0,
				false,
				[]byte("msg"),
			),
		)
		require.NoError(t, err)
		var fields map[string]any
		require.NoError(
			t,
			json.Unmarshal(
				dst,
				&fields,
			),
		)
		require.Equal(t, "api", fields["service"])
		require.Equal(t, true, fields["enabled"])
	})
	t.Run("invalid UTF-8 is JSON encoded", func(t *testing.T) {
		t.Parallel()
		h, err := NewHandler("xaat-secret", "logs")
		require.NoError(t, err)
		t.Cleanup(func() { _ = h.Close() })
		dst, err := h.appendEvent(
			nil,
			newEvent(
				time.Now(),
				0,
				false,
				[]byte{0xff},
			),
		)
		require.NoError(t, err)
		var fields map[string]any
		require.NoError(
			t,
			json.Unmarshal(
				dst,
				&fields,
			),
		)
		require.Equal(t, "�", fields["message"])
	})
	t.Run("truncated marker", func(t *testing.T) {
		t.Parallel()
		h, err := NewHandler("xaat-secret", "logs")
		require.NoError(t, err)
		t.Cleanup(func() { _ = h.Close() })
		dst, err := h.appendEvent(
			nil,
			newEvent(
				time.Now(),
				0,
				false,
				[]byte(
					strings.Repeat(
						"x",
						maxMessageLen+1,
					),
				),
			),
		)
		require.NoError(t, err)
		var fields map[string]any
		require.NoError(
			t,
			json.Unmarshal(
				dst,
				&fields,
			),
		)
		require.Equal(t, true, fields["message_truncated"])
	})
	t.Run("does not HTML-escape", func(t *testing.T) {
		t.Parallel()
		h, err := NewHandler(
			"xaat-secret",
			"logs",
			WithCompression(false),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = h.Close() })
		line, err := h.appendEvent(
			nil,
			newEvent(
				time.Now(),
				0,
				false,
				[]byte("<tag>&"),
			),
		)
		require.NoError(t, err)
		require.Contains(t, string(line), `"<tag>&"`)
		var got map[string]any
		require.NoError(
			t,
			json.Unmarshal(
				bytes.TrimSpace(line),
				&got,
			),
		)
		require.Equal(t, "<tag>&", got["message"])
	})
}

func Test_newEvent(t *testing.T) {
	t.Parallel()
	t.Run("trims one newline", func(t *testing.T) {
		t.Parallel()
		ev := newEvent(
			time.Date(
				2026,
				10,
				22,
				3,
				4,
				5,
				6,
				time.FixedZone(
					"west",
					-3600,
				),
			),
			1,
			true,
			[]byte("line\n"),
		)
		require.Equal(t, "line", ev.message)
		require.Equal(
			t,
			time.UTC,
			ev.time.Location(),
		)
		require.Equal(
			t,
			4,
			ev.time.Hour(),
		)
	})
	t.Run("trims CRLF", func(t *testing.T) {
		t.Parallel()
		ev := newEvent(
			time.Now(),
			0,
			false,
			[]byte("line\r\n"),
		)
		require.Equal(t, "line", ev.message)
	})
	t.Run("trims multiple trailing line breaks", func(t *testing.T) {
		t.Parallel()
		ev := newEvent(
			time.Now(),
			0,
			false,
			[]byte("line\r\n\n\r"),
		)
		require.Equal(t, "line", ev.message)
	})
	t.Run("truncates on UTF-8 boundary", func(t *testing.T) {
		t.Parallel()
		input := strings.Repeat(
			"界",
			maxMessageLen/3+1,
		)
		ev := newEvent(
			time.Now(),
			0,
			false,
			[]byte(input),
		)
		require.True(t, ev.truncated)
		require.LessOrEqual(
			t,
			len(ev.message),
			maxMessageLen,
		)
		require.True(
			t,
			utf8.ValidString(ev.message),
		)
	})
	t.Run("does not truncate short message", func(t *testing.T) {
		t.Parallel()
		ev := newEvent(
			time.Now(),
			0,
			false,
			[]byte("short"),
		)
		require.False(t, ev.truncated)
	})
	t.Run("invalid UTF-8 normalized size stays within field limit", func(t *testing.T) {
		t.Parallel()
		input := bytes.Repeat([]byte{0xff}, maxMessageLen)
		ev := newEvent(
			time.Now(),
			0,
			false,
			input,
		)
		h, err := NewHandler("xaat-secret", "logs")
		require.NoError(t, err)
		t.Cleanup(func() { _ = h.Close() })
		line, err := h.appendEvent(nil, ev)
		require.NoError(t, err)
		var decoded map[string]any
		require.NoError(
			t,
			json.Unmarshal(
				line,
				&decoded,
			),
		)
		message := decoded["message"].(string)
		require.LessOrEqual(
			t,
			len(message),
			maxMessageLen,
		)
		require.True(t, decoded["message_truncated"].(bool))
	})
	t.Run("copies caller bytes", func(t *testing.T) {
		t.Parallel()
		input := []byte("before")
		ev := newEvent(
			time.Now(),
			0,
			false,
			input,
		)
		copy(input, "after!")
		require.Equal(t, "before", ev.message)
	})
}

func BenchmarkAppendEvent(b *testing.B) {
	h, err := NewHandler(
		"xaat-secret",
		"logs",
		withBaseURL("http://127.0.0.1:1"),
		WithFields(map[string]any{"service": "api"}),
	)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := h.Close(); err != nil {
			b.Errorf("close handler: %v", err)
		}
	})

	msg := []byte(
		fmt.Sprintf("%0120s", "event message"),
	)
	ev := newEvent(
		time.Now(),
		4,
		true,
		msg,
	)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := h.appendEvent(nil, ev); err != nil {
			b.Fatal(err)
		}
	}
}
