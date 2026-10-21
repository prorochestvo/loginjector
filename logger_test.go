package loginjector

import (
	"bytes"
	"io"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	logLevelDebug   LogLevel = 0x00
	logLevelInfo    LogLevel = 0x01
	logLevelWarning LogLevel = 0x02
	logLevelSevere  LogLevel = 0xF0
)

//nolint:paralleltest // captureStdout redirects the process-global os.Stdout
func TestNewLogger(t *testing.T) {
	l := NewLogger(logLevelDebug)
	require.NotNil(t, l)
	require.Equal(t, logLevelDebug, l.minimumLogLevel)
	require.Empty(t, l.hooks, "unexpected hooks count")
	require.Len(t, l.handlers, 1, "unexpected handlers count")

	// the default handler is a TimestampedPrintHandler on stdout: a write at/above
	// the minimum level must produce a timestamped line on stdout.
	m := uniqueToken()
	out := captureStdout(t, func() {
		ld := NewLogger(logLevelInfo)
		_, err := ld.WriteLog(logLevelInfo, []byte(m))
		require.NoError(t, err)
	})
	require.Contains(t, out, m, "default sink must write to stdout")
	require.Regexp(t, `^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} `, out,
		"default sink must prepend a timestamp")
}

func TestLogger_SetMinLevel(t *testing.T) {
	t.Parallel()
	l := NewLogger(logLevelDebug)
	require.NotNil(t, l)
	require.NotEqual(t, logLevelInfo, l.minimumLogLevel)

	l.SetMinLevel(logLevelInfo)
	require.Equal(t, logLevelInfo, l.minimumLogLevel)
}

func TestLogger_Hook(t *testing.T) {
	t.Parallel()
	t.Run("exact level routing", func(t *testing.T) {
		t.Parallel()
		m := uniqueToken()
		b := bytes.NewBufferString("")
		l := NewLogger(logLevelInfo, io.Discard)
		require.NotNil(t, l)
		require.Empty(t, l.hooks, "unexpected number of hooks")

		hookID := l.Hook(b, logLevelWarning)
		require.Len(t, l.hooks, 1, "unexpected number of hooks")
		require.Equal(t, hookID, l.hooks[0].ID, "unexpected hook[0].id")
		require.Equal(t, logLevelWarning, l.hooks[0].Level, "unexpected hook[0].Level")
		n, err := l.WriteLog(logLevelWarning, []byte(m))
		require.NoError(t, err)
		require.Equal(t, len(m), n)
		require.Contains(t, b.String(), m)

		b.Reset()
		m = uniqueToken()
		n, err = l.WriteLog(logLevelDebug, []byte(m))
		require.NoError(t, err)
		require.Equal(t, 0, n)
		require.NotContains(t, b.String(), m)

		b.Reset()
		m = uniqueToken()
		n, err = l.WriteLog(logLevelSevere, []byte(m))
		require.NoError(t, err)
		require.Equal(t, len(m), n)
		require.NotContains(t, b.String(), m)
	})

	t.Run("duplicate levels dedupe", func(t *testing.T) {
		t.Parallel()
		b := bytes.NewBufferString("")
		l := NewLogger(logLevelInfo, io.Discard)

		// a repeated level must register a single hook entry so the message is not
		// fanned out to the shared sink more than once.
		l.Hook(b, logLevelWarning, logLevelWarning, logLevelWarning)
		require.Len(t, l.hooks, 1, "repeated level must collapse to one hook entry")

		m := uniqueToken()
		_, err := l.WriteLog(logLevelWarning, []byte(m))
		require.NoError(t, err)
		require.Equal(t, 1, strings.Count(b.String(), m), "message must be written exactly once")
	})

	t.Run("distinct levels preserve first-seen order", func(t *testing.T) {
		t.Parallel()
		b := bytes.NewBufferString("")
		l := NewLogger(logLevelInfo, io.Discard)

		// Hook(w, A, B, A) must register two hooks (A and B), first occurrence wins.
		l.Hook(b, logLevelWarning, logLevelSevere, logLevelWarning)
		require.Len(t, l.hooks, 2, "distinct levels must produce one entry each")
		require.Equal(t, logLevelWarning, l.hooks[0].Level, "first-seen level must come first")
		require.Equal(t, logLevelSevere, l.hooks[1].Level, "second distinct level must come second")
	})

	t.Run("concurrent hook IDs are unique_ForRaceCondition", func(t *testing.T) {
		t.Parallel()
		// the per-logger counter runs under l.m in Hook, so concurrent registrations
		// must each receive a distinct ID without a data race.
		const n = 100
		l := NewLogger(logLevelInfo, io.Discard)

		var mu sync.Mutex
		ids := make(map[HookID]struct{}, n)
		var wg sync.WaitGroup
		for range n {
			wg.Add(1)
			go func() {
				defer wg.Done()
				id := l.Hook(io.Discard, logLevelWarning)
				mu.Lock()
				ids[id] = struct{}{}
				mu.Unlock()
			}()
		}
		wg.Wait()

		require.Len(t, ids, n, "each concurrent Hook call must return a distinct ID")
	})
}

// TestLogger_HookBelowMinimum verifies the hook-fires-below-minimum asymmetry: a hook
// registered on a level below minimumLogLevel still receives the message, while
// WriteLog reports n=0 because the handlers were gated out. This pins the single-sink
// fast-path's handlersActive=false branch, which no other test exercises functionally.
func TestLogger_HookBelowMinimum(t *testing.T) {
	t.Parallel()
	m := uniqueToken()
	b := bytes.NewBufferString("")
	l := NewLogger(logLevelInfo, io.Discard)

	l.Hook(b, logLevelDebug) // hook sits below the minimum level
	n, err := l.WriteLog(logLevelDebug, []byte(m))
	require.NoError(t, err)
	require.Equal(t, 0, n, "below-minimum write must report n=0")
	require.Contains(t, b.String(), m, "hook must fire even below the minimum level")
}

func TestLogger_Unhook(t *testing.T) {
	t.Parallel()
	m := uniqueToken()
	b := bytes.NewBufferString("")
	l := NewLogger(logLevelInfo, io.Discard)
	require.NotNil(t, l)

	// Warning, Severe, Severe dedupes to two distinct hook entries sharing one ID.
	hookID := l.Hook(b, logLevelWarning, logLevelSevere, logLevelSevere)
	require.Len(t, l.hooks, 2, "unexpected number of hooks")

	l.Unhook(hookID)
	require.Empty(t, l.hooks, "unexpected number of hooks")

	n, err := l.WriteLog(logLevelWarning, []byte(m))
	require.NoError(t, err)
	require.Len(t, m, n, "method returned an unexpected length")
	require.Empty(t, b.String(), "unexpected message")
}

func TestLogger_WriteLog(t *testing.T) {
	t.Parallel()
	m := uniqueToken()
	b := bytes.NewBufferString("")
	l := NewLogger(logLevelInfo, b)
	require.NotNil(t, l)

	n, err := l.WriteLog(logLevelSevere, []byte(m))
	require.NoError(t, err)
	require.Len(t, m, n)
	require.Equal(t, m, b.String())

	b.Reset()
	m = uniqueToken()

	n, err = l.WriteLog(logLevelDebug, []byte(m))
	require.NoError(t, err)
	require.Equal(t, 0, n)
	require.Empty(t, b.String())

	b.Reset()
	m = uniqueToken()

	n, err = l.WriteLog(logLevelSevere, []byte(m))
	require.NoError(t, err)
	require.Len(t, m, n)
	require.Equal(t, m, b.String())
}

//nolint:paralleltest // calls log.SetOutput on the std logger
func TestLogger_Write(t *testing.T) {
	m := uniqueToken()
	b := bytes.NewBufferString("")
	l := NewLogger(logLevelInfo, b)
	require.NotNil(t, l)

	prev := log.Writer()
	t.Cleanup(func() { log.SetOutput(prev) })
	log.SetOutput(l)

	n, err := l.Write([]byte(m))
	require.NoError(t, err)
	require.Len(t, m, n)
	require.Equal(t, m, b.String())

	b.Reset()
	m = uniqueToken()

	n, err = l.Write([]byte(m))
	require.NoError(t, err)
	require.Len(t, m, n)
	require.Equal(t, m, b.String())

	b.Reset()
	m = uniqueToken()
	log.Println(m)

	require.Contains(t, b.String(), m)
}

func TestLogger_PrintAndPanic(t *testing.T) {
	t.Parallel()
	m1 := "M1_" + uniqueToken()
	m2 := "M2_" + uniqueToken()
	m3 := "M3_" + uniqueToken()
	m4 := "M4_" + uniqueToken()
	m5 := "M5_" + uniqueToken()
	m6 := "M6_" + uniqueToken()
	m7 := "M7_" + uniqueToken()
	b := bytes.NewBufferString("")
	l := NewLogger(logLevelInfo, b)
	require.NotNil(t, l)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func(wg *sync.WaitGroup, l *Logger, txt string) {
		defer wg.Done()
		l.Printf(logLevelDebug, "%s", txt)
	}(&wg, l, m1)
	wg.Add(1)
	go func(wg *sync.WaitGroup, l *Logger, txt string) {
		defer wg.Done()
		l.Print(logLevelInfo, txt, uniqueToken(), uniqueToken())
	}(&wg, l, m2)
	wg.Add(1)
	go func(wg *sync.WaitGroup, l *Logger, txt string) {
		defer wg.Done()
		l.Printf(logLevelSevere, "%s", txt)
	}(&wg, l, m3)
	wg.Wait()
	wg.Add(1)
	go func(wg *sync.WaitGroup, l *Logger, txt string) {
		defer wg.Done()
		defer func() {
			_ = recover()
		}()
		l.Panicf(logLevelDebug, "%s", txt)
	}(&wg, l, m4)
	wg.Add(1)
	go func(wg *sync.WaitGroup, l *Logger, txt1, txt2 string) {
		defer wg.Done()
		defer func() {
			_ = recover()
		}()
		l.Panic(logLevelInfo, txt1, txt2, uniqueToken())
	}(&wg, l, m5, m6)
	wg.Wait()
	wg.Add(1)
	go func(wg *sync.WaitGroup, l *Logger, txt string) {
		defer wg.Done()
		defer func() {
			_ = recover()
		}()
		l.Panicf(logLevelSevere, "%s", txt)
	}(&wg, l, m7)
	wg.Wait()

	require.NotEmpty(t, b.String())
	require.NotContains(t, b.String(), m1)
	require.Contains(t, b.String(), m2)
	require.Contains(t, b.String(), m3)
	require.NotContains(t, b.String(), m4)
	require.Contains(t, b.String(), m5)
	require.Contains(t, b.String(), m6)
	require.Contains(t, b.String(), m7)
}

// BenchmarkWriteLog measures WriteLog throughput for the 0-sink, 1-sink, and N-sink cases.
// Run with: CGO_ENABLED=0 go test -bench=BenchmarkWriteLog -benchmem -run=^$ .
func BenchmarkWriteLog(b *testing.B) {
	msg := []byte("benchmark log message payload")

	b.Run("0 sinks (below minimum, no hooks)", func(b *testing.B) {
		l := NewLogger(logLevelInfo, io.Discard)
		b.ResetTimer()
		for range b.N {
			_, _ = l.WriteLog(logLevelDebug, msg)
		}
	})

	b.Run("1 sink (single handler)", func(b *testing.B) {
		l := NewLogger(logLevelInfo, io.Discard)
		b.ResetTimer()
		for range b.N {
			_, _ = l.WriteLog(logLevelInfo, msg)
		}
	})

	b.Run("1 sink (single hook below minimum)", func(b *testing.B) {
		l := NewLogger(logLevelInfo, io.Discard)
		l.Hook(io.Discard, logLevelDebug)
		b.ResetTimer()
		for range b.N {
			_, _ = l.WriteLog(logLevelDebug, msg)
		}
	})

	b.Run("4 sinks (concurrent fan-out)", func(b *testing.B) {
		l := NewLogger(logLevelInfo, io.Discard, io.Discard, io.Discard, io.Discard)
		b.ResetTimer()
		for range b.N {
			_, _ = l.WriteLog(logLevelInfo, msg)
		}
	})

	b.Run("2 hooks below minimum (concurrent fan-out, hooks only)", func(b *testing.B) {
		l := NewLogger(logLevelInfo, io.Discard)
		l.Hook(io.Discard, logLevelDebug)
		l.Hook(io.Discard, logLevelDebug)
		b.ResetTimer()
		for range b.N {
			_, _ = l.WriteLog(logLevelDebug, msg)
		}
	})
}

// TestWithMinLevel_Logger covers the LeveledHandler gate inside Logger.WriteLog.
func TestWithMinLevel_Logger(t *testing.T) {
	t.Parallel()

	t.Run("leveled_handler_below_threshold_skipped", func(t *testing.T) {
		t.Parallel()
		buf := &bytes.Buffer{}
		l := NewLogger(1, WithMinLevel(5, buf))
		n, err := l.WriteLog(2, []byte("msg"))
		require.NoError(t, err)
		require.Equal(t, 0, n)
		require.Empty(t, buf.String())
	})

	t.Run("leveled_handler_at_threshold_fires", func(t *testing.T) {
		t.Parallel()
		buf := &bytes.Buffer{}
		l := NewLogger(1, WithMinLevel(5, buf))
		n, err := l.WriteLog(5, []byte("hello"))
		require.NoError(t, err)
		require.Equal(t, 5, n)
		require.Equal(t, "hello", buf.String())
	})

	t.Run("mixed_handlers_each_use_own_threshold", func(t *testing.T) {
		t.Parallel()
		fileBuf := &bytes.Buffer{}
		consoleBuf := &bytes.Buffer{}
		// fileBuf fires at minLevel=3; consoleBuf fires at its own level=1.
		l := NewLogger(3, fileBuf, WithMinLevel(1, consoleBuf))

		// level 1: only consoleBuf (fileBuf gated at 3).
		_, err := l.WriteLog(1, []byte("level1"))
		require.NoError(t, err)
		require.Empty(t, fileBuf.String(), "fileBuf must not receive level-1 message")
		require.Contains(t, consoleBuf.String(), "level1")

		fileBuf.Reset()
		consoleBuf.Reset()

		// level 2: only consoleBuf.
		_, err = l.WriteLog(2, []byte("level2"))
		require.NoError(t, err)
		require.Empty(t, fileBuf.String(), "fileBuf must not receive level-2 message")
		require.Contains(t, consoleBuf.String(), "level2")

		fileBuf.Reset()
		consoleBuf.Reset()

		// level 3: both.
		_, err = l.WriteLog(3, []byte("level3"))
		require.NoError(t, err)
		require.Contains(t, fileBuf.String(), "level3")
		require.Contains(t, consoleBuf.String(), "level3")
	})

	t.Run("non_leveled_handler_unchanged", func(t *testing.T) {
		t.Parallel()
		fileBuf := &bytes.Buffer{}
		l := NewLogger(3, fileBuf)

		n, err := l.WriteLog(2, []byte("below"))
		require.NoError(t, err)
		require.Equal(t, 0, n)
		require.Empty(t, fileBuf.String())

		_, err = l.WriteLog(3, []byte("at"))
		require.NoError(t, err)
		require.Contains(t, fileBuf.String(), "at")
	})

	t.Run("leveled_handler_through_thread_safe_wrapper", func(t *testing.T) {
		t.Parallel()
		// this subtest is the critical proof that unwrapLeveled peels through ensureThreadSafe.
		inner := &bytes.Buffer{}
		wrapped := WithMinLevel(5, inner)
		l := NewLogger(1, wrapped) // ensureThreadSafe wraps 'wrapped' in a *writer

		// write at level 3 — below the leveled threshold of 5 but above minLevel=1.
		// if unwrapLeveled fails, the handler would fire (falling back to minLevel=1).
		n, err := l.WriteLog(3, []byte("should-not-appear"))
		require.NoError(t, err)
		require.Equal(t, 0, n, "handler must be gated at its own MinLevel, not the logger's")
		require.Empty(t, inner.String())

		// write at level 5 — at the leveled threshold.
		_, err = l.WriteLog(5, []byte("should-appear"))
		require.NoError(t, err)
		require.Contains(t, inner.String(), "should-appear")
	})

	t.Run("concurrent_writelog_with_leveled_handler_ForRaceCondition", func(t *testing.T) {
		t.Parallel()
		const n = 200

		fileBuf := &bytes.Buffer{}    // wrapped by ensureThreadSafe
		consoleBuf := &bytes.Buffer{} // wrapped by ensureThreadSafe
		// fileBuf at minLevel=3; consoleBuf leveled at 1.
		l := NewLogger(3, fileBuf, WithMinLevel(1, consoleBuf))

		var wg sync.WaitGroup
		for i := range n {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				level := LogLevel(1 + (i % 5)) // levels 1..5
				_, _ = l.WriteLog(level, []byte("x"))
			}(i)
		}
		wg.Wait()

		// consoleBuf receives all writes (leveled at 1); fileBuf only receives level>=3.
		// just assert no data race — the race detector handles the rest.
		require.NotEmpty(t, consoleBuf.String())
	})
}

func TestLogger_StdLog(t *testing.T) {
	t.Parallel()

	t.Run("prefix and level routing", func(t *testing.T) {
		t.Parallel()

		b := &bytes.Buffer{}
		l := NewLogger(logLevelInfo, b)

		dbLog := l.StdLog(logLevelWarning, "sqlite ")
		dbLog.Printf("x")

		// Lmsgprefix places the prefix immediately before the message with no
		// leading date/time from the std logger; the sink here adds no timestamp.
		require.Equal(t, "sqlite x\n", b.String())
	})

	t.Run("below level is gated", func(t *testing.T) {
		t.Parallel()

		b := &bytes.Buffer{}
		l := NewLogger(logLevelInfo, b) // minimum is Info

		dbLog := l.StdLog(logLevelDebug, "db ") // Debug is below Info
		dbLog.Printf("dropped")

		require.Empty(t, b.String(), "a StdLog below the minimum level must be gated out")
	})

	t.Run("no doubled timestamp when paired with TimestampedHandler sink", func(t *testing.T) {
		t.Parallel()

		fixed := time.Date(2024, 3, 15, 10, 30, 45, 0, time.UTC)
		inner := &bytes.Buffer{}
		sink := TimestampedHandler(inner, withClock(func() time.Time { return fixed }))
		l := NewLogger(logLevelInfo, sink)

		dbLog := l.StdLog(logLevelInfo, "sqlite ")
		dbLog.Printf("query ran")

		// exactly one timestamp: the sink stamps, the std logger uses Lmsgprefix only.
		out := inner.String()
		require.Equal(t, "2024/03/15 10:30:45 sqlite query ran\n", out)
		require.Equal(t, 1, strings.Count(out, "2024/03/15 10:30:45"), "line must carry exactly one timestamp")
	})
}

func TestLogger_WriterAs(t *testing.T) {
	t.Parallel()
	b := bytes.NewBufferString("")
	l := NewLogger(logLevelInfo, b)
	require.NotNil(t, l)

	w := l.WriterAs(logLevelWarning)
	m := uniqueToken()
	_, _ = w.Write([]byte(m))
	if s := b.String(); !strings.Contains(s, m) {
		t.Errorf("Write method wrote an unexpected message: %v", s)
	}
	b.Reset()

	w = l.WriterAs(logLevelDebug)
	m = uniqueToken()
	_, _ = w.Write([]byte(m))
	if s := b.String(); strings.Contains(s, m) {
		t.Errorf("Write method wrote an unexpected message: %v", s)
	}
	b.Reset()

	w = l.WriterAs(logLevelSevere)
	m = uniqueToken()
	_, _ = w.Write([]byte(m))
	if s := b.String(); !strings.Contains(s, m) {
		t.Errorf("Write method wrote an unexpected message: %v", s)
	}
}
