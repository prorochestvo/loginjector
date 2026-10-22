package loginjector

import (
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
)

// LeveledHandler is an optional interface a handler may implement to declare a
// minimum log level distinct from the parent Logger's minimumLogLevel. When a
// handler implements LeveledHandler, Logger.WriteLog uses its MinLevel() instead
// of Logger.minimumLogLevel for the per-handler gate. Handlers that do not
// implement LeveledHandler keep the historical behaviour — they fire when
// level >= Logger.minimumLogLevel.
//
// MinLevel must be safe to call concurrently and must return the same value
// for the lifetime of the handler — Logger caches no result.
type LeveledHandler interface {
	io.Writer
	MinLevel() LogLevel
}

// Logger is describing the logger structure and its methods.
type Logger struct {
	minimumLogLevel LogLevel
	handlers        []io.Writer
	hooks           []*hook
	hookSeq         uint64 // per-logger hook-ID counter, guarded by m.
	m               sync.RWMutex
}

// NewLogger creates a new logger with the given minimum log level and handlers.
// If no handlers are provided, a default TimestampedPrintHandler (timestamped
// stdout) is added.
// Each handler is wrapped so the logger never calls its Write concurrently, which
// makes logging safe from multiple goroutines even with non-thread-safe handlers.
func NewLogger(minLevel LogLevel, handlers ...io.Writer) *Logger {
	if len(handlers) == 0 {
		handlers = []io.Writer{TimestampedPrintHandler()}
	}

	safeHandlers := make([]io.Writer, 0, len(handlers))
	for _, h := range handlers {
		safeHandlers = append(safeHandlers, ensureThreadSafe(h))
	}

	return &Logger{
		minimumLogLevel: minLevel,
		handlers:        safeHandlers,
	}
}

// SetMinLevel sets the minimum log level.
func (l *Logger) SetMinLevel(level LogLevel) {
	l.m.Lock()
	defer l.m.Unlock()

	l.minimumLogLevel = level
}

// Hook registers writer to fire on an EXACT match of any of the given levels
// (level plus additional), regardless of the logger's minimum level. Duplicate
// levels are collapsed to a single registration, so Hook(w, X, X) writes to w
// once per matching WriteLog, not twice. The returned HookID removes every
// registration created by this call when passed to Unhook.
//
// Hooks match on exact equality, not a threshold: a hook on level X does NOT
// fire for a message logged at level X+1. To alert on "level >= X" — the common
// case for routing high-severity messages to an out-of-band sink — register a
// WithMinLevel(X, writer) handler instead of enumerating exact levels, because
// an enumeration that omits one level silently drops that level's messages.
//
// The returned HookID is unique only within this logger; do not persist or
// compare it against a fixed format.
func (l *Logger) Hook(writer io.Writer, level LogLevel, additional ...LogLevel) HookID {
	l.m.Lock()
	defer l.m.Unlock()

	l.hookSeq++
	hID := HookID(fmt.Sprintf("hook-%d", l.hookSeq))

	// wrap once and share the guarded writer across every level entry so concurrent
	// WriteLog calls targeting different levels never write to the sink concurrently.
	w := ensureThreadSafe(writer)

	// register one hook per distinct level (first occurrence wins) so a repeated
	// level does not fan the same message out to the shared sink more than once.
	seen := make(map[LogLevel]struct{}, 1+len(additional))
	for _, logLevel := range append([]LogLevel{level}, additional...) {
		if _, ok := seen[logLevel]; ok {
			continue
		}
		seen[logLevel] = struct{}{}
		l.hooks = append(l.hooks, &hook{
			ID:     hID,
			Level:  logLevel,
			Writer: w,
		})
	}

	return hID
}

// StdLog returns a standard-library *log.Logger that writes through this logger at
// the given level, with the given prefix. It is configured with log.Lmsgprefix and
// no date/time flags: the emitter contributes only the prefix, and any timestamp is
// added by the sink (e.g. TimestampedPrintHandler or TimestampedHandler), so a line
// is never stamped twice. Use it to hand a *log.Logger to code that expects one —
// for example a database driver's logger — without hand-wiring the flags:
//
//	dbLog := logger.StdLog(levels.Info, "sqlite ")
//
// Each call returns a fresh *log.Logger over a fresh writer; the returned logger
// routes through WriteLog, which serializes writes per sink.
func (l *Logger) StdLog(level LogLevel, prefix string) *log.Logger {
	return log.New(l.WriterAs(level), prefix, log.Lmsgprefix)
}

// Unhook removes a hook from the logger.
func (l *Logger) Unhook(id HookID) {
	l.m.Lock()
	defer l.m.Unlock()

	items := make([]*hook, 0, len(l.hooks))

	for _, h := range l.hooks {
		if h.ID != id {
			items = append(items, h)
		}
	}

	l.hooks = items
}

// WriteLog writes a log message at the given level to all matching sinks.
//
// Hooks fire on an exact level match regardless of minimumLogLevel. Handlers fire
// only when level >= minimumLogLevel. When no sinks match (below the minimum and no
// matching hooks), WriteLog returns (0, nil). When exactly one sink matches it is
// written inline (no goroutine overhead). Two or more sinks run concurrently, each
// in its own goroutine, with errors joined via errors.Join.
//
// WriteLog holds the read lock for the entire duration so it is safe to call
// concurrently with Hook/Unhook/SetMinLevel.
func (l *Logger) WriteLog(level LogLevel, message []byte) (int, error) {
	l.m.RLock()
	defer l.m.RUnlock()

	n := len(message)

	// collect matching hooks first (fire regardless of minimumLogLevel).
	sinks := make([]io.Writer, 0, len(l.hooks)+len(l.handlers))
	for _, h := range l.hooks {
		if level == h.Level {
			sinks = append(sinks, h.Writer)
		}
	}

	// handlers fire when level >= their per-handler threshold. the threshold is
	// lh.MinLevel() if h implements LeveledHandler, else l.minimumLogLevel.
	anyHandlerActive := false
	for _, h := range l.handlers {
		threshold := l.minimumLogLevel
		if lh, ok := unwrapLeveled(h); ok {
			threshold = lh.MinLevel()
		}
		if level >= threshold {
			sinks = append(sinks, h)
			anyHandlerActive = true
		}
	}
	if !anyHandlerActive && len(sinks) == 0 {
		// below all thresholds with no matching hooks: nothing to do.
		return 0, nil
	}

	switch len(sinks) {
	case 1:
		// fast path: single sink, write inline without goroutine or WaitGroup.
		_, err := sinks[0].Write(message)
		if !anyHandlerActive {
			// sole sink was a hook below the minimum level; return 0 per contract.
			return 0, err
		}
		return n, err

	default:
		// concurrent fan-out for 2+ sinks — existing behaviour preserved. An empty
		// sink slice (only reachable for a zero-value Logger with no handlers) also
		// lands here and simply writes nothing.
		var (
			mu   sync.Mutex
			errs []error
			wg   sync.WaitGroup
		)

		// write fans the message out to a single sink and records any error under mu,
		// since the sinks run concurrently.
		write := func(w io.Writer) {
			defer wg.Done()
			if _, e := w.Write(message); e != nil {
				mu.Lock()
				errs = append(errs, e)
				mu.Unlock()
			}
		}

		for _, s := range sinks {
			wg.Add(1)
			go write(s)
		}
		wg.Wait()

		ret := n
		if !anyHandlerActive {
			// all sinks were hooks below the minimum level; return 0 per contract.
			ret = 0
		}
		return ret, errors.Join(errs...)
	}
}

// Printf writes a formatted log message.
func (l *Logger) Printf(level LogLevel, format string, args ...any) {
	m := fmt.Sprintf(format, args...)
	_, err := l.WriteLog(level, []byte(m+"\n"))
	if err != nil {
		println(err.Error())
	}
}

// Print writes a log message.
func (l *Logger) Print(level LogLevel, args ...any) {
	m := fmt.Sprint(args...)
	_, err := l.WriteLog(level, []byte(m+"\n"))
	if err != nil {
		println(err.Error())
	}
}

// Panicf writes a formatted log message at the given level and then panics with the message string.
// The panic is recoverable: deferred functions run and callers may use recover() to catch it.
// It does NOT call os.Exit — process death is not guaranteed.
func (l *Logger) Panicf(level LogLevel, format string, args ...any) {
	m := fmt.Sprintf(format, args...)
	_, err := l.WriteLog(level, []byte(m+"\n"))
	if err != nil {
		println(err.Error())
	}
	panic(m)
}

// Panic writes a log message at the given level and then panics with the message string.
// The panic is recoverable: deferred functions run and callers may use recover() to catch it.
// It does NOT call os.Exit — process death is not guaranteed.
func (l *Logger) Panic(level LogLevel, args ...any) {
	m := fmt.Sprint(args...)
	_, err := l.WriteLog(level, []byte(m+"\n"))
	if err != nil {
		println(err.Error())
	}
	panic(m)
}

// Write writes a log message with the minimum log level.
func (l *Logger) Write(message []byte) (int, error) {
	l.m.RLock()
	level := l.minimumLogLevel
	l.m.RUnlock()
	return l.WriteLog(level, message)
}

// WriterAs returns a writer that writes to the logger as the given log level.
func (l *Logger) WriterAs(level LogLevel) io.Writer {
	w := &writer{
		h: func(msg []byte) (int, error) {
			return l.WriteLog(level, msg)
		},
	}
	return w
}

// LogLevel is a log level.
type LogLevel int

// HookID is a unique identifier for a hook.
type HookID string

// hook is a log hook.
type hook struct {
	ID     HookID
	Level  LogLevel
	Writer io.Writer
}

// ensureThreadSafe wraps w in a mutex-guarded writer so the logger never invokes a
// given sink's Write concurrently. Writers produced by this package's handlers are
// already guarded and are returned unchanged.
//
// The original io.Writer reference is stored on the resulting *writer so that
// unwrapLeveled can peel through the wrapper and detect LeveledHandler on the
// underlying sink.
func ensureThreadSafe(w io.Writer) io.Writer {
	if _, ok := w.(*writer); ok {
		return w
	}
	return &writer{original: w, h: w.Write}
}

// unwrapLeveled returns the LeveledHandler view of h, peeling the ensureThreadSafe
// *writer wrapper if present. Returns (nil, false) when h is not leveled.
func unwrapLeveled(h io.Writer) (LeveledHandler, bool) {
	if w, ok := h.(*writer); ok {
		if lh, ok := w.original.(LeveledHandler); ok {
			return lh, true
		}
		return nil, false
	}
	if lh, ok := h.(LeveledHandler); ok {
		return lh, true
	}
	return nil, false
}
