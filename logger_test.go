package loginjector

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prorochestvo/loginjector/internal/synctest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	_ LevelWriter = (*writer)(nil)
	_ LevelWriter = (*leveledWriter)(nil)
	_ LevelWriter = (*recordingLevelWriter)(nil)
	_ LevelWriter = (*panickingLevelWriter)(nil)
	_ LevelWriter = (*countingLevelWriter)(nil)
	_ LevelWriter = levelDiscardWriter{}
)

const (
	logLevelDebug   LogLevel = 0x00
	logLevelInfo    LogLevel = 0x01
	logLevelWarning LogLevel = 0x02
	logLevelSevere  LogLevel = 0xF0
)

//nolint:paralleltest // captureStdout redirects the process-global os.Stdout
func TestNewLogger(t *testing.T) {
	t.Run("already guarded writer is not wrapped again", func(t *testing.T) {
		inner := &recordingLevelWriter{}
		guarded := &writer{h: inner.Write, original: inner, shared: &sync.Mutex{}}
		logger := NewLogger(logLevelDebug, guarded)
		require.Same(t, guarded, logger.handlers[0])
	})

	t.Run("default handler writes timestamped lines to stdout", func(t *testing.T) {
		l := NewLogger(logLevelDebug)
		require.NotNil(t, l)
		require.Equal(t, logLevelDebug, l.minimumLogLevel)
		require.Empty(t, l.hooks, "unexpected hooks count")
		require.Len(
			t,
			l.handlers,
			1,
			"unexpected handlers count",
		)

		// the default handler is a TimestampedPrintHandler on stdout: a write at/above
		// the minimum level must produce a timestamped line on stdout.
		m := uniqueToken()
		out := captureStdout(t, func() {
			ld := NewLogger(logLevelInfo)
			_, err := ld.WriteLog(logLevelInfo, []byte(m))
			require.NoError(t, err)
		})
		require.Contains(
			t,
			out,
			m,
			"default sink must write to stdout",
		)
		require.Regexp(
			t,
			`^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} `,
			out,
			"default sink must prepend a timestamp",
		)
	})

	t.Run("non-pointer sink is not deduplicated and does not panic", func(t *testing.T) {
		var l *Logger
		require.NotPanics(t, func() {
			l = NewLogger(
				logLevelInfo,
				valueWriter{id: 1},
				valueWriter{id: 1},
				sliceWriter{lines: []string{"a"}},
				sliceWriter{lines: []string{"a"}},
			)
		})
		require.Len(t, l.handlers, 4)
		require.Empty(t, l.sinkLocks, "value-type sinks must not enter the shared-lock registry")
		require.NotPanics(t, func() {
			_, err := l.WriteLog(logLevelInfo, []byte(uniqueToken()))
			require.NoError(t, err)
		})
	})

	t.Run("WithMinLevel of a nil writer registers without panic", func(t *testing.T) {
		require.NotPanics(t, func() {
			NewLogger(
				logLevelInfo,
				WithMinLevel(logLevelSevere, nil),
			)
		})
	})
}

func TestNewLoggerForRaceCondition(t *testing.T) {
	t.Parallel()

	t.Run("same buffer twice", func(t *testing.T) {
		t.Parallel()
		buf := bytes.NewBufferString("")
		l := NewLogger(logLevelInfo, buf, buf)

		const goroutines = 16
		const iterations = 200
		var wg sync.WaitGroup
		for i := range goroutines {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := range iterations {
					msg := fmt.Sprintf("msg-%d-%d\n", i, j)
					_, _ = l.WriteLog(logLevelInfo, []byte(msg))
				}
			}()
		}
		wg.Wait()

		// each registration writes once per message: 2 registrations × 16 goroutines × 200 iterations.
		expectedLines := 2 * goroutines * iterations
		actualLines := strings.Count(
			buf.String(),
			"\n",
		)
		require.Equal(
			t,
			expectedLines,
			actualLines,
			"same buffer registered twice must write twice per message",
		)
	})

	t.Run("buffer and WithMinLevel of the same buffer", func(t *testing.T) {
		t.Parallel()
		buf := bytes.NewBufferString("")
		l := NewLogger(
			logLevelInfo,
			buf,
			WithMinLevel(logLevelInfo, buf),
		)

		const goroutines = 16
		const iterations = 200
		var wg sync.WaitGroup
		for i := range goroutines {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := range iterations {
					msg := fmt.Sprintf("msg-%d-%d\n", i, j)
					_, _ = l.WriteLog(logLevelInfo, []byte(msg))
				}
			}()
		}
		wg.Wait()

		// each registration writes once per message: 2 registrations × 16 goroutines × 200 iterations.
		expectedLines := 2 * goroutines * iterations
		actualLines := strings.Count(
			buf.String(),
			"\n",
		)
		require.Equal(
			t,
			expectedLines,
			actualLines,
			"same buffer via WithMinLevel must write twice per message",
		)
	})
}

func TestLogger_SetMinLevel(t *testing.T) {
	t.Parallel()

	t.Run("sets the minimum level", func(t *testing.T) {
		t.Parallel()
		l := NewLogger(logLevelDebug)
		require.NotNil(t, l)
		require.NotEqual(t, logLevelInfo, l.minimumLogLevel)

		l.SetMinLevel(logLevelInfo)
		require.Equal(t, logLevelInfo, l.minimumLogLevel)
	})

	t.Run("does not wait for a blocked sink", func(t *testing.T) {
		t.Parallel()
		a, b := &bytes.Buffer{}, &bytes.Buffer{}
		entered := make(chan struct{})
		release := make(chan struct{})
		var enterOnce sync.Once
		l := NewLogger(
			reentryMin,
			WithMinLevel(
				reentryHigh,
				writerFunc(func(p []byte) (int, error) {
					enterOnce.Do(func() { close(entered) })
					<-release
					return a.Write(p)
				}),
			),
		)
		l.Hook(b, reentryLow)

		blocked := runAsync(func() { _, _ = l.WriteLog(reentryHigh, []byte(uniqueToken())) })
		t.Cleanup(func() {
			close(release)
			if !closedWithin(blocked) {
				t.Error("parked WriteLog did not return after release")
			}
		})
		if !closedWithin(entered) {
			t.Fatal("sink A was never entered")
		}

		if !closedWithin(runAsync(func() { l.SetMinLevel(reentryNewMin) })) {
			t.Fatal("SetMinLevel waited for a blocked sink")
		}
		m := uniqueToken()
		if !closedWithin(runAsync(func() { _, _ = l.WriteLog(reentryLow, []byte(m)) })) {
			t.Fatal("WriteLog queued behind a blocked sink")
		}
		require.Contains(
			t,
			b.String(),
			m,
		)
	})
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

	t.Run("level writer receives hook level", func(t *testing.T) {
		t.Parallel()
		sink := &recordingLevelWriter{}
		l := NewLogger(logLevelSevere, io.Discard)
		l.Hook(sink, logLevelWarning)

		n, err := l.WriteLog(logLevelWarning, []byte("hook"))

		require.NoError(t, err)
		require.Equal(
			t,
			0,
			n,
			"a hook below the handler threshold does not count as a handler",
		)
		require.Equal(t, []LogLevel{logLevelWarning}, sink.levels)
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

	t.Run("zero-value logger", func(t *testing.T) {
		t.Parallel()
		var l Logger
		buf := &bytes.Buffer{}

		require.NotPanics(t, func() { l.Hook(buf, logLevelInfo) })
		require.Len(t, l.hooks, 1)

		m := uniqueToken()
		_, err := l.WriteLog(logLevelInfo, []byte(m))
		require.NoError(t, err)
		require.Contains(
			t,
			buf.String(),
			m,
		)
	})

	t.Run("handler registering a hook from its Write does not deadlock", func(t *testing.T) {
		t.Parallel()
		a, b, c := &bytes.Buffer{}, &bytes.Buffer{}, &bytes.Buffer{}
		var (
			l    *Logger
			once sync.Once
		)
		l = NewLogger(
			reentryMin,
			WithMinLevel(
				reentryHigh,
				writerFunc(func(p []byte) (int, error) {
					once.Do(func() { l.Hook(c, reentryLow) })
					return a.Write(p)
				}),
			),
		)
		l.Hook(b, reentryLow)

		m := uniqueToken()
		if !closedWithin(runAsync(func() { _, _ = l.WriteLog(reentryHigh, []byte(m)) })) {
			t.Fatal("WriteLog deadlocked: handler called Hook from its Write")
		}
		require.Contains(
			t,
			a.String(),
			m,
		)

		m2 := uniqueToken()
		_, err := l.WriteLog(reentryLow, []byte(m2))
		require.NoError(t, err)
		require.Contains(
			t,
			c.String(),
			m2,
			"hook registered from a sink must receive later messages",
		)
		require.Contains(
			t,
			b.String(),
			m2,
		)
	})
}

func TestLogger_HookForRaceCondition(t *testing.T) {
	t.Parallel()
	buf := bytes.NewBufferString("")
	l := NewLogger(logLevelInfo, buf)
	l.Hook(buf, logLevelInfo)

	const goroutines = 16
	const iterations = 200
	var wg sync.WaitGroup
	for i := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range iterations {
				msg := fmt.Sprintf("msg-%d-%d\n", i, j)
				_, _ = l.WriteLog(logLevelInfo, []byte(msg))
			}
		}()
	}
	wg.Wait()

	// each registration writes once per message: 2 registrations × 16 goroutines × 200 iterations.
	expectedLines := 2 * goroutines * iterations
	actualLines := strings.Count(
		buf.String(),
		"\n",
	)
	require.Equal(
		t,
		expectedLines,
		actualLines,
		"same buffer as handler and hook must write twice per message",
	)
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

	t.Run("level writer hook below minimum", func(t *testing.T) {
		t.Parallel()
		levelSink := &recordingLevelWriter{}
		l.Hook(levelSink, logLevelDebug)
		n, err := l.WriteLog(logLevelDebug, []byte(m))
		require.NoError(t, err)
		require.Equal(t, 0, n)
		require.Equal(t, []LogLevel{logLevelDebug}, levelSink.levels)
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
		require.Equal(
			t,
			"sqlite x\n",
			b.String(),
		)
	})

	t.Run("below level is gated", func(t *testing.T) {
		t.Parallel()

		b := &bytes.Buffer{}
		l := NewLogger(logLevelInfo, b) // minimum is Info

		dbLog := l.StdLog(logLevelDebug, "db ") // Debug is below Info
		dbLog.Printf("dropped")

		require.Empty(
			t,
			b.String(),
			"a StdLog below the minimum level must be gated out",
		)
	})

	t.Run("no doubled timestamp when paired with TimestampedHandler sink", func(t *testing.T) {
		t.Parallel()

		fixed := time.Date(
			2024,
			3,
			15,
			10,
			30,
			45,
			0,
			time.UTC,
		)
		inner := &bytes.Buffer{}
		sink := TimestampedHandler(
			inner,
			withClock(func() time.Time { return fixed }),
		)
		l := NewLogger(logLevelInfo, sink)

		dbLog := l.StdLog(logLevelInfo, "sqlite ")
		dbLog.Printf("query ran")

		// exactly one timestamp: the sink stamps, the std logger uses Lmsgprefix only.
		out := inner.String()
		require.Equal(t, "2024/03/15 10:30:45 sqlite query ran\n", out)
		require.Equal(
			t,
			1,
			strings.Count(out, "2024/03/15 10:30:45"),
			"line must carry exactly one timestamp",
		)
	})
}

func TestLogger_Unhook(t *testing.T) {
	t.Parallel()

	t.Run("removes every level of the hook", func(t *testing.T) {
		t.Parallel()
		m := uniqueToken()
		b := bytes.NewBufferString("")
		l := NewLogger(logLevelInfo, io.Discard)
		require.NotNil(t, l)

		// Warning, Severe, Severe dedupes to two distinct hook entries sharing one ID.
		hookID := l.Hook(
			b,
			logLevelWarning,
			logLevelSevere,
			logLevelSevere,
		)
		require.Len(
			t,
			l.hooks,
			2,
			"unexpected number of hooks",
		)

		l.Unhook(hookID)
		require.Empty(t, l.hooks, "unexpected number of hooks")

		n, err := l.WriteLog(logLevelWarning, []byte(m))
		require.NoError(t, err)
		require.Len(
			t,
			m,
			n,
			"method returned an unexpected length",
		)
		require.Empty(
			t,
			b.String(),
			"unexpected message",
		)

		require.NotPanics(t, func() { l.Unhook(hookID) }, "unhooking twice must be a no-op")
		require.NotPanics(t, func() { NewLogger(logLevelInfo, io.Discard).Unhook(hookID) }, "unhooking a logger without hooks must be a no-op")
	})

	t.Run("hook calling Unhook from its Write does not deadlock", func(t *testing.T) {
		t.Parallel()
		a, b := &bytes.Buffer{}, &bytes.Buffer{}
		l := NewLogger(
			reentryMin,
			WithMinLevel(reentryHigh, a),
		)
		var (
			id   HookID
			once sync.Once
		)
		id = l.Hook(
			writerFunc(func(p []byte) (int, error) {
				once.Do(func() { l.Unhook(id) })
				return b.Write(p)
			}),
			reentryLow,
		)

		m1 := uniqueToken()
		if !closedWithin(runAsync(func() { _, _ = l.WriteLog(reentryLow, []byte(m1)) })) {
			t.Fatal("WriteLog deadlocked: hook called Unhook from its Write")
		}
		require.Contains(
			t,
			b.String(),
			m1,
		)

		m2 := uniqueToken()
		_, err := l.WriteLog(reentryLow, []byte(m2))
		require.NoError(t, err)
		require.NotContains(
			t,
			b.String(),
			m2,
			"an unhooked hook must not receive later messages",
		)
		require.Empty(
			t,
			a.String(),
		)
	})
}

func TestLogger_UnhookForRaceCondition(t *testing.T) {
	t.Parallel()

	t.Run("thread-safe buffer", func(t *testing.T) {
		t.Parallel()
		safeBuf := synctest.NewBuffer(nil)
		runHookUnhookStress(
			NewLogger(logLevelWarning, io.Discard),
			safeBuf,
		)
	})

	t.Run("plain buffer", func(t *testing.T) {
		t.Parallel()
		buf := &bytes.Buffer{}
		runHookUnhookStress(
			NewLogger(logLevelWarning, buf),
			buf,
		)
	})

	t.Run("in-flight write to an unhooked sink", func(t *testing.T) {
		t.Parallel()
		buf := &bytes.Buffer{}
		pw := &parkWriter{buf: buf, entered: make(chan struct{}), release: make(chan struct{})}
		l := NewLogger(logLevelInfo, io.Discard)
		id := l.Hook(pw, logLevelDebug)

		var releaseOnce sync.Once
		release := func() { releaseOnce.Do(func() { close(pw.release) }) }
		line1, line2 := uniqueToken()+"\n", uniqueToken()+"\n"

		first := runAsync(func() { _, _ = l.WriteLog(logLevelDebug, []byte(line1)) })
		var second <-chan struct{}
		t.Cleanup(func() {
			release()
			if !closedWithin(first) {
				t.Error("first WriteLog did not return after release")
			}
			if second != nil && !closedWithin(second) {
				t.Error("second WriteLog did not return after release")
			}
		})
		if !closedWithin(pw.entered) {
			t.Fatal("first write never reached the sink")
		}

		if !closedWithin(runAsync(func() { l.Unhook(id) })) {
			t.Fatal("Unhook waited for an in-flight write")
		}
		if !closedWithin(runAsync(func() { l.Hook(pw, logLevelDebug) })) {
			t.Fatal("Hook waited for an in-flight write")
		}
		second = runAsync(func() { _, _ = l.WriteLog(logLevelDebug, []byte(line2)) })
		release()

		if !closedWithin(first) {
			t.Fatal("first WriteLog did not return")
		}
		if !closedWithin(second) {
			t.Fatal("second WriteLog did not return")
		}
		out := buf.String()
		require.Equal(
			t,
			2,
			strings.Count(out, "\n"),
		)
		require.Contains(t, out, line1)
		require.Contains(t, out, line2)
	})
}

func TestLogger_WriteForRaceCondition(t *testing.T) {
	t.Parallel()
	l := NewLogger(logLevelInfo, io.Discard)

	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				if i%2 == 0 {
					l.SetMinLevel(logLevelWarning)
					l.SetMinLevel(logLevelInfo)
				} else {
					_, err := l.Write([]byte("message"))
					assert.NoError(t, err)
				}
			}
		}()
	}
	wg.Wait()
}

func TestLogger_WriteLog(t *testing.T) {
	t.Parallel()

	t.Run("basic write", func(t *testing.T) {
		t.Parallel()
		m := uniqueToken()
		b := bytes.NewBufferString("")
		l := NewLogger(logLevelInfo, b)
		require.NotNil(t, l)

		n, err := l.WriteLog(logLevelSevere, []byte(m))
		require.NoError(t, err)
		require.Len(t, m, n)
		require.Equal(
			t,
			m,
			b.String(),
		)

		b.Reset()
		m = uniqueToken()

		n, err = l.WriteLog(logLevelDebug, []byte(m))
		require.NoError(t, err)
		require.Equal(t, 0, n)
		require.Empty(
			t,
			b.String(),
		)

		b.Reset()
		m = uniqueToken()

		n, err = l.WriteLog(logLevelSevere, []byte(m))
		require.NoError(t, err)
		require.Len(t, m, n)
		require.Equal(
			t,
			m,
			b.String(),
		)
	})

	t.Run("level writers receive levels through every logger adapter", func(t *testing.T) {
		t.Parallel()

		t.Run("registered directly", func(t *testing.T) {
			t.Parallel()
			sink := &recordingLevelWriter{}
			logger := NewLogger(logLevelDebug, sink)
			_, err := logger.WriteLog(logLevelWarning, []byte("direct"))
			require.NoError(t, err)
			require.Equal(t, []LogLevel{logLevelWarning}, sink.levels)
		})

		t.Run("single minimum wrapper", func(t *testing.T) {
			t.Parallel()
			sink := &recordingLevelWriter{}
			logger := NewLogger(
				logLevelDebug,
				WithMinLevel(logLevelInfo, sink),
			)
			_, err := logger.WriteLog(logLevelSevere, []byte("wrapped"))
			require.NoError(t, err)
			require.Equal(t, []LogLevel{logLevelSevere}, sink.levels)
		})

		t.Run("nested minimum wrappers", func(t *testing.T) {
			t.Parallel()
			sink := &recordingLevelWriter{}
			handler := WithMinLevel(
				logLevelDebug,
				WithMinLevel(logLevelInfo, sink),
			)
			logger := NewLogger(logLevelDebug, handler)
			_, err := logger.WriteLog(logLevelSevere, []byte("wrapped"))
			require.NoError(t, err)
			require.Equal(t, []LogLevel{logLevelSevere}, sink.levels)
		})

		t.Run("Printf", func(t *testing.T) {
			t.Parallel()
			sink := &recordingLevelWriter{}
			NewLogger(logLevelDebug, sink).Printf(logLevelWarning, "%s", "formatted")
			require.Equal(t, []LogLevel{logLevelWarning}, sink.levels)
		})

		t.Run("WriterAs", func(t *testing.T) {
			t.Parallel()
			sink := &recordingLevelWriter{}
			_, err := NewLogger(logLevelDebug, sink).WriterAs(logLevelSevere).Write([]byte("writer"))
			require.NoError(t, err)
			require.Equal(t, []LogLevel{logLevelSevere}, sink.levels)
		})

		t.Run("StdLog", func(t *testing.T) {
			t.Parallel()
			sink := &recordingLevelWriter{}
			NewLogger(logLevelDebug, sink).StdLog(logLevelInfo, "").Print("standard")
			require.Equal(t, []LogLevel{logLevelInfo}, sink.levels)
		})
	})

	t.Run("level writer panic is recovered through nested minimum wrappers", func(t *testing.T) {
		t.Parallel()
		sink := &panickingLevelWriter{}
		logger := NewLogger(
			logLevelDebug,
			WithMinLevel(
				logLevelInfo,
				WithMinLevel(logLevelWarning, sink),
			),
		)
		_, err := logger.WriteLog(logLevelSevere, []byte("panic"))
		require.Error(t, err)
		require.Contains(
			t,
			err.Error(),
			"panicked",
		)
		require.Contains(
			t,
			err.Error(),
			"*loginjector.panickingLevelWriter",
		)
		require.NotContains(
			t,
			err.Error(),
			"*loginjector.leveledWriter",
		)
	})

	t.Run("panicking sink among several is reported as an error", func(t *testing.T) {
		t.Parallel()
		msg := uniqueToken()
		safeBuf := bytes.NewBufferString("")
		panicky := &panickyWriter{panicOn: []int{1}}
		l := NewLogger(logLevelInfo, panicky, safeBuf)

		n, err := l.WriteLog(logLevelInfo, []byte(msg))

		require.Error(t, err)
		require.Contains(
			t,
			err.Error(),
			"panicked",
		)
		require.Contains(
			t,
			err.Error(),
			"boom",
		)
		require.ErrorIs(t, err, errBoom)
		require.Equal(
			t,
			len(msg),
			n,
			"n must reflect the message length",
		)
		require.Contains(
			t,
			safeBuf.String(),
			msg,
			"safe sink must receive the message",
		)
	})

	t.Run("panicking single sink returns an error", func(t *testing.T) {
		t.Parallel()
		msg := uniqueToken()
		panicky := &panickyWriter{panicOn: []int{1}}
		l := NewLogger(logLevelInfo, panicky)

		require.NotPanics(t, func() {
			_, err := l.WriteLog(logLevelInfo, []byte(msg))
			require.Error(t, err)
			require.Contains(
				t,
				err.Error(),
				"panicked",
			)
		})
	})

	t.Run("panic value that is not an error", func(t *testing.T) {
		t.Parallel()
		msg := uniqueToken()
		l := NewLogger(logLevelInfo, &panicWithStringWriter{})

		n, err := l.WriteLog(logLevelInfo, []byte(msg))

		require.Error(t, err)
		first, stack, _ := strings.Cut(
			err.Error(),
			"\n",
		)
		require.Contains(t, first, "panicked")
		require.Contains(t, first, "text panic")
		require.Contains(
			t,
			first,
			"*loginjector.panicWithStringWriter",
			"the user sink's type must be reported",
		)
		require.NotContains(
			t,
			first,
			"*loginjector.writer",
			"the guard's type must not be reported",
		)
		require.Contains(
			t,
			stack,
			"goroutine ",
			"the error must carry the stack trace",
		)
		require.NotZero(t, n)
	})

	t.Run("sink usable after a recovered panic", func(t *testing.T) {
		t.Parallel()
		msg1 := uniqueToken()
		msg2 := uniqueToken()
		resilient := &resilientWriter{panicOnce: true}
		l := NewLogger(logLevelInfo, resilient)

		_, err := l.WriteLog(logLevelInfo, []byte(msg1))
		require.Error(t, err, "first write should panic and return an error")

		_, err = l.WriteLog(logLevelInfo, []byte(msg2))
		require.NoError(t, err, "sink must be usable after a recovered panic")
		require.Contains(
			t,
			resilient.buf.String(),
			msg2,
		)
	})

	t.Run("sink logging to another sink while SetMinLevel is pending", func(t *testing.T) {
		t.Parallel()
		a, b := &bytes.Buffer{}, &bytes.Buffer{}
		setDone := make(chan struct{})
		inner := uniqueToken()
		var (
			l    *Logger
			once sync.Once
		)
		l = NewLogger(
			reentryMin,
			WithMinLevel(
				reentryHigh,
				writerFunc(func(p []byte) (int, error) {
					once.Do(func() {
						go func() {
							defer close(setDone)
							l.SetMinLevel(reentryNewMin)
						}()
					poll:
						for l.m.TryRLock() {
							l.m.RUnlock()
							select {
							case <-setDone:
								break poll
							default:
								runtime.Gosched()
							}
						}
						_, _ = l.WriteLog(reentryLow, []byte(inner))
					})
					return a.Write(p)
				}),
			),
		)
		l.Hook(b, reentryLow)
		t.Cleanup(func() {
			if !closedWithin(setDone) {
				t.Error("SetMinLevel goroutine did not return")
			}
		})

		outer := uniqueToken()
		if !closedWithin(runAsync(func() { _, _ = l.WriteLog(reentryHigh, []byte(outer)) })) {
			t.Fatal("WriteLog deadlocked: re-entrant write queued behind a pending SetMinLevel")
		}
		require.Contains(
			t,
			a.String(),
			outer,
		)
		require.Contains(
			t,
			b.String(),
			inner,
		)
		require.NotContains(
			t,
			b.String(),
			outer,
		)
	})

	t.Run("sink may log to a different sink of the same logger", func(t *testing.T) {
		t.Parallel()
		a, b := &bytes.Buffer{}, &bytes.Buffer{}
		inner := uniqueToken()
		var l *Logger
		l = NewLogger(
			reentryMin,
			WithMinLevel(
				reentryHigh,
				writerFunc(func(p []byte) (int, error) {
					_, _ = l.WriteLog(reentryLow, []byte(inner))
					return a.Write(p)
				}),
			),
		)
		l.Hook(b, reentryLow)

		outer := uniqueToken()
		if !closedWithin(runAsync(func() { _, _ = l.WriteLog(reentryHigh, []byte(outer)) })) {
			t.Fatal("WriteLog deadlocked: sink logged to a different sink")
		}
		require.Contains(
			t,
			a.String(),
			outer,
		)
		require.NotContains(
			t,
			a.String(),
			inner,
		)
		require.Contains(
			t,
			b.String(),
			inner,
		)
		require.NotContains(
			t,
			b.String(),
			outer,
		)
	})
}

func TestLogger_WriteLogForRaceCondition(t *testing.T) {
	t.Parallel()
	const goroutines = 16
	const iterations = 200

	buf1 := bytes.NewBufferString("")
	buf2 := bytes.NewBufferString("")
	panicky := &panickyWriter{panicOn: []int{7, 14, 21, 28, 35, 42, 49, 56, 63, 70, 77, 84, 91, 98}}
	l := NewLogger(
		logLevelInfo,
		buf1,
		panicky,
		buf2,
	)

	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range iterations {
				msg := fmt.Sprintf("msg-%d\n", i)
				_, _ = l.WriteLog(logLevelInfo, []byte(msg))
			}
		}()
	}
	wg.Wait()

	// each non-panicking buffer must hold exactly goroutines*iterations lines.
	expectedLines := goroutines * iterations
	actualLines1 := strings.Count(
		buf1.String(),
		"\n",
	)
	actualLines2 := strings.Count(
		buf2.String(),
		"\n",
	)
	require.Equal(
		t,
		expectedLines,
		actualLines1,
		"buf1 must receive exactly all messages",
	)
	require.Equal(
		t,
		expectedLines,
		actualLines2,
		"buf2 must receive exactly all messages",
	)
}

//nolint:paralleltest // calls log.SetOutput on the std logger
func TestLogger_Write(t *testing.T) {
	t.Run("writes at the minimum level", func(t *testing.T) {
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
		require.Equal(
			t,
			m,
			b.String(),
		)

		b.Reset()
		m = uniqueToken()

		n, err = l.Write([]byte(m))
		require.NoError(t, err)
		require.Len(t, m, n)
		require.Equal(
			t,
			m,
			b.String(),
		)

		b.Reset()
		m = uniqueToken()
		log.Println(m)

		require.Contains(
			t,
			b.String(),
			m,
		)
	})

	t.Run("filtered write reports full length", func(t *testing.T) {
		buf := &bytes.Buffer{}
		l := NewLogger(
			logLevelInfo,
			WithMinLevel(logLevelSevere, buf),
		)

		n, err := l.Write([]byte("abc"))
		require.NoError(t, err)
		require.Equal(t, 3, n)
		require.Empty(
			t,
			buf.String(),
		)
	})
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
func TestLogger_WriterAs(t *testing.T) {
	t.Parallel()

	t.Run("level routing", func(t *testing.T) {
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
	})

	t.Run("filtered write reports full length", func(t *testing.T) {
		t.Parallel()
		buf := &bytes.Buffer{}
		l := NewLogger(logLevelInfo, buf)

		n, err := l.WriterAs(logLevelDebug).Write([]byte("abc"))
		require.NoError(t, err)
		require.Equal(t, 3, n)
		require.Empty(
			t,
			buf.String(),
		)
	})

	t.Run("bufio flush of a filtered message succeeds", func(t *testing.T) {
		t.Parallel()
		l := NewLogger(logLevelInfo, &bytes.Buffer{})

		bw := bufio.NewWriter(
			l.WriterAs(logLevelDebug),
		)
		_, err := bw.WriteString("x")
		require.NoError(t, err)
		require.NoError(
			t,
			bw.Flush(),
		)
	})

	t.Run("error is passed through", func(t *testing.T) {
		t.Parallel()
		l := NewLogger(logLevelInfo, &failingWriter{})

		p := []byte("abc")
		n, err := l.WriterAs(logLevelWarning).Write(p)
		require.ErrorIs(t, err, errSinkFailed)
		require.LessOrEqual(
			t,
			n,
			len(p),
		)
	})
}

func TestLogger_WriterAsForRaceCondition(t *testing.T) {
	t.Parallel()
	l := NewLogger(logLevelInfo, &bytes.Buffer{})
	w := l.WriterAs(logLevelWarning)
	msg := []byte("message\n")

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				n, err := w.Write(msg)
				assert.NoError(t, err)
				assert.Equal(
					t,
					len(msg),
					n,
				)
			}
		}()
	}
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				l.SetMinLevel(logLevelSevere)
				l.SetMinLevel(logLevelInfo)
			}
		}()
	}
	wg.Wait()
}

// TestWithMinLevel_Logger covers the LeveledHandler gate inside Logger.WriteLog.
//
//nolint:paralleltest // captureStderr redirects the process-global os.Stderr
func TestLogger_SinkPanicNotice(t *testing.T) {
	t.Run("one stderr line names the sink and the panic value", func(t *testing.T) {
		l := NewLogger(logLevelInfo, &panicWithStringWriter{})
		out := captureStderr(t, func() {
			_, err := l.WriteLog(logLevelInfo, []byte("message"))
			require.Error(t, err)
		})
		require.Equal(t, "loginjector: sink *loginjector.panicWithStringWriter panicked: text panic\n", out)
	})

	t.Run("newlines in the panic value are flattened", func(t *testing.T) {
		l := NewLogger(logLevelInfo, &panicWithValueWriter{value: "first\nsecond"})
		out := captureStderr(t, func() {
			_, err := l.WriteLog(logLevelInfo, []byte("message"))
			require.Error(t, err)
		})
		require.Equal(t, "loginjector: sink *loginjector.panicWithValueWriter panicked: first second\n", out)
	})

	t.Run("level writer panic through nested minimum wrappers names inner sink", func(t *testing.T) {
		logger := NewLogger(
			logLevelDebug,
			WithMinLevel(
				logLevelInfo,
				WithMinLevel(
					logLevelWarning,
					&panickingLevelWriter{},
				),
			),
		)
		out := captureStderr(t, func() {
			_, err := logger.WriteLog(logLevelSevere, []byte("message"))
			require.Error(t, err)
		})
		require.Equal(t, "loginjector: sink *loginjector.panickingLevelWriter panicked: level panic\n", out)
	})
}

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

	b.Run("1 sink (LevelWriter)", func(b *testing.B) {
		l := NewLogger(logLevelInfo, levelDiscardWriter{})
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
		l := NewLogger(
			logLevelInfo,
			io.Discard,
			io.Discard,
			io.Discard,
			io.Discard,
		)
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

// Re-entrancy routing: sink A is WithMinLevel(reentryHigh, a) and fires only on
// WriteLog(reentryHigh); sink B is Hook(b, reentryLow) and fires only on
// WriteLog(reentryLow). The logger minimum sits between them, and a concurrent
// SetMinLevel moves it to reentryNewMin, which changes neither A's nor B's routing.
const (
	reentryLow    = logLevelDebug
	reentryMin    = logLevelInfo
	reentryNewMin = logLevelWarning
	reentryHigh   = logLevelSevere
)

// deadlockTimeout bounds every wait on a call that deadlocks on a broken logger.
const deadlockTimeout = 2 * time.Second

var (
	errBoom       = errors.New("boom")
	errSinkFailed = errors.New("sink failed")
)

// runAsync runs fn in a goroutine and returns a channel closed when fn returns.
func runAsync(fn func()) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	return done
}

// closedWithin reports whether done is closed within deadlockTimeout.
func closedWithin(done <-chan struct{}) bool {
	select {
	case <-done:
		return true
	case <-time.After(deadlockTimeout):
		return false
	}
}

// runHookUnhookStress runs 8 goroutines calling WriteLog at sink's level while 4 loop
// Hook/Unhook of sink and 2 loop SetMinLevel, then joins them all.
func runHookUnhookStress(l *Logger, sink io.Writer) {
	const level = logLevelWarning
	l.Hook(sink, level)

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				_, _ = l.WriteLog(level, []byte("x\n"))
			}
		}()
	}
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 50 {
				l.Unhook(
					l.Hook(sink, level),
				)
			}
		}()
	}
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				l.SetMinLevel(
					level - 1,
				)
				l.SetMinLevel(level)
			}
		}()
	}
	wg.Wait()
}

// panickyWriter panics with errBoom on the listed call numbers and writes to buf otherwise.
var _ io.Writer = (*panickyWriter)(nil)

type panickyWriter struct {
	buf      bytes.Buffer
	count    int
	panicOn  []int // call numbers (1-based) that panic
	panicCnt int   // index of the next entry in panicOn
}

func (p *panickyWriter) Write(data []byte) (int, error) {
	p.count++
	if p.panicCnt < len(p.panicOn) && p.count == p.panicOn[p.panicCnt] {
		p.panicCnt++
		panic(errBoom)
	}
	return p.buf.Write(data)
}

// panicWithStringWriter panics with a non-error string value.
var _ io.Writer = (*panicWithStringWriter)(nil)

type panicWithStringWriter struct{}

func (p *panicWithStringWriter) Write(_ []byte) (int, error) {
	panic("text panic")
}

// panicWithValueWriter panics with the configured value.
var _ io.Writer = (*panicWithValueWriter)(nil)

type panicWithValueWriter struct{ value any }

func (p *panicWithValueWriter) Write(_ []byte) (int, error) {
	panic(p.value)
}

// resilientWriter panics once then behaves normally.
var _ io.Writer = (*resilientWriter)(nil)

type resilientWriter struct {
	buf       bytes.Buffer
	panicOnce bool
}

func (r *resilientWriter) Write(data []byte) (int, error) {
	if r.panicOnce {
		r.panicOnce = false
		panic(
			errors.New("one-time panic"),
		)
	}
	return r.buf.Write(data)
}

// failingWriter always fails with errSinkFailed.
var _ io.Writer = (*failingWriter)(nil)

type failingWriter struct{}

func (f *failingWriter) Write(_ []byte) (int, error) { return 0, errSinkFailed }

// parkWriter parks its first Write until release is closed, signalling entered
// once it is inside; every Write then appends to buf. It is a pointer type so
// the logger deduplicates its registrations.
var _ io.Writer = (*parkWriter)(nil)

type parkWriter struct {
	buf     *bytes.Buffer
	entered chan struct{}
	release chan struct{}
	calls   atomic.Int32
}

func (pw *parkWriter) Write(p []byte) (int, error) {
	if pw.calls.Add(1) == 1 {
		close(pw.entered)
		<-pw.release
	}
	return pw.buf.Write(p)
}

// valueWriter is a comparable value-type sink.
var _ io.Writer = valueWriter{}

type valueWriter struct{ id int }

func (v valueWriter) Write(p []byte) (int, error) { return len(p), nil }

// sliceWriter is a non-comparable value-type sink: hashing it as a map key panics.
var _ io.Writer = sliceWriter{}

type sliceWriter struct{ lines []string }

func (s sliceWriter) Write(p []byte) (int, error) { return len(p), nil }

// writerFunc adapts a function to io.Writer.
var _ io.Writer = writerFunc(nil)

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

type recordingLevelWriter struct {
	levels []LogLevel
	buf    bytes.Buffer
}

func (w *recordingLevelWriter) Write(p []byte) (int, error) { return w.buf.Write(p) }

func (w *recordingLevelWriter) WriteLevel(level LogLevel, p []byte) (int, error) {
	w.levels = append(w.levels, level)
	return w.buf.Write(p)
}

type panickingLevelWriter struct{}

func (*panickingLevelWriter) Write([]byte) (int, error) { return 0, nil }

func (*panickingLevelWriter) WriteLevel(LogLevel, []byte) (int, error) { panic("level panic") }

type countingLevelWriter struct {
	counts     map[LogLevel]int
	writeCalls int
}

func (w *countingLevelWriter) Write(p []byte) (int, error) {
	w.writeCalls++
	return len(p), nil
}

func (w *countingLevelWriter) WriteLevel(level LogLevel, p []byte) (int, error) {
	w.counts[level]++
	return len(p), nil
}

type levelDiscardWriter struct{}

func (levelDiscardWriter) Write(p []byte) (int, error) { return len(p), nil }

func (levelDiscardWriter) WriteLevel(_ LogLevel, p []byte) (int, error) { return len(p), nil }
