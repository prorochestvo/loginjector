package loginjector

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"reflect"
	"runtime/debug"
	"strings"
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

// LevelWriter is an optional interface a handler or hook may implement to receive
// the severity of each message. Implementations must not retain p after returning.
// When a sink implements LevelWriter, Logger.WriteLog calls WriteLevel instead of
// Write. Package handlers that wrap a user writer through a closure, such as
// TimestampedHandler(ax), do not forward the level. An existing user type that
// already has a WriteLevel(LogLevel, []byte) method now receives calls through it.
type LevelWriter interface {
	io.Writer
	WriteLevel(level LogLevel, p []byte) (n int, err error)
}

// Logger is describing the logger structure and its methods.
//
// Re-entrancy: A sink (handler or hook) may call Hook, Unhook, or SetMinLevel
// without deadlock, and may log through the same logger to OTHER sinks. However,
// three cases deadlock:
//   - A sink calling WriteLog with a level that reaches the same sink again
//     (including another registration of it on this logger, e.g., as both a handler
//     and a hook).
//   - A sink writing to std log when the outer WriteLog came through std log
//     (log.Logger holds its own mutex).
//   - A sink writing to the same WriterAs adapter that brought the outer WriteLog
//     (its mutex is held for the whole fan-out, blocking the write).
//
// Sinks that log into each other (A→B and B→A) can deadlock under concurrent writes.
type Logger struct {
	minimumLogLevel LogLevel
	handlers        []io.Writer
	hooks           []*hook
	hookSeq         uint64                    // per-logger hook-ID counter, guarded by m.
	sinkLocks       map[io.Writer]*sync.Mutex // shared mutexes per sink identity, guarded by m, never pruned
	m               sync.RWMutex
}

// NewLogger creates a new logger with the given minimum log level and handlers.
// If no handlers are provided, a default TimestampedPrintHandler (timestamped
// stdout) is added.
// Each handler is wrapped so the logger never calls its Write concurrently, which
// makes logging safe from multiple goroutines even with non-thread-safe handlers.
// Multiple registrations of the same pointer-type sink share one mutex.
//
// Covered: a pointer sink registered multiple times (directly, via WithMinLevel, or as a hook).
// Not covered: a sink registered through a package factory wrapper (e.g., TimestampedHandler(buf)
// wrapped alongside buf directly — the factory returns a *writer without original); value-type
// sinks sharing state through a pointer; user wrapper types; or sharing across loggers.
//
// The logger retains every distinct pointer sink registered during its lifetime,
// including after Unhook, and everything reachable from it; per-request sinks on a
// long-lived logger accumulate. Reuse long-lived sinks.
//
// Duplicate registrations serialize; a sink must not log into another registration
// of itself.
func NewLogger(minLevel LogLevel, handlers ...io.Writer) *Logger {
	if len(handlers) == 0 {
		handlers = []io.Writer{TimestampedPrintHandler()}
	}

	l := &Logger{
		minimumLogLevel: minLevel,
	}

	// register through the logger so repeated pointer sinks share one mutex.
	safeHandlers := make([]io.Writer, 0, len(handlers))
	for _, h := range handlers {
		safeHandlers = append(
			safeHandlers,
			l.ensureThreadSafe(h),
		)
	}
	l.handlers = safeHandlers

	return l
}

// SetMinLevel sets the minimum log level. WriteLog calls starting after SetMinLevel
// returns observe the new level; an in-flight WriteLog may still use the old level.
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
//
// Multiple registrations of the same pointer-type sink share one mutex, and the
// sink stays retained for the logger's lifetime; see NewLogger for the covered
// cases, retention, and the duplicate-registration re-entrancy rule.
func (l *Logger) Hook(writer io.Writer, level LogLevel, additional ...LogLevel) HookID {
	l.m.Lock()
	defer l.m.Unlock()

	l.hookSeq++
	hID := HookID(fmt.Sprintf("hook-%d", l.hookSeq))

	// wrap once and share the guarded writer across every level entry so concurrent
	// WriteLog calls targeting different levels never write to the sink concurrently.
	w := l.ensureThreadSafe(writer)

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

// Unhook removes a hook from the logger. WriteLog calls starting after Unhook
// returns no longer reach the hook; an in-flight WriteLog may still write to it.
// The sink stays retained after Unhook; see NewLogger.
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
// WriteLog returns only after every matching sink returns; a slow sink delays the
// caller and serializes callers sharing that sink.
//
// If a sink panics during a write, the panic is recovered and reported as an error
// with the sink's type name, the panic value, and the stack trace; a one-line notice
// naming the sink type and the panic value is also written to os.Stderr. The panic is
// not propagated to the caller.
//
// WriteLog holds the read lock only while taking a snapshot of matching sinks, then
// releases it before writing to any sink. This means a sink may call Hook/Unhook/SetMinLevel
// without deadlock. However, a WriteLog call already in progress may still reach a hook
// that was removed, or use a level that was changed, after Unhook or SetMinLevel returns.
// Unhook and SetMinLevel only guarantee that WriteLog calls *starting* after they return
// observe the change.
func (l *Logger) WriteLog(level LogLevel, message []byte) (int, error) {
	// snapshot matching sinks under the read lock, then write without it, so a sink
	// may call Hook/Unhook/SetMinLevel.
	l.m.RLock()

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

	l.m.RUnlock()

	if len(sinks) == 0 {
		// no matching sinks: nothing to do.
		return 0, nil
	}

	switch len(sinks) {
	case 1:
		// fast path: single sink, write inline without goroutine or WaitGroup.
		err := writeSink(sinks[0], level, message)
		if !anyHandlerActive {
			// sole sink was a hook below the minimum level; return 0 per contract.
			return 0, err
		}
		return n, err

	default:
		// concurrent fan-out for 2+ sinks — collect errors under mu.
		var (
			mu   sync.Mutex
			errs []error
			wg   sync.WaitGroup
		)

		write := func(w io.Writer) {
			defer wg.Done()
			if err := writeSink(w, level, message); err != nil {
				mu.Lock()
				errs = append(errs, err)
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
// Per the io.Writer contract, when the message is filtered below the threshold,
// Write reports the full message length as written (like io.Discard).
func (l *Logger) Write(message []byte) (int, error) {
	l.m.RLock()
	level := l.minimumLogLevel
	l.m.RUnlock()
	n, err := l.WriteLog(level, message)
	// (0, nil) means the message was filtered; report it consumed, like io.Discard.
	if n == 0 && err == nil {
		return len(message), nil
	}
	return n, err
}

// WriterAs returns a writer that writes to the logger as the given log level.
// Per the io.Writer contract, when the message is filtered below the threshold,
// WriterAs reports the full message length as written (like io.Discard).
func (l *Logger) WriterAs(level LogLevel) io.Writer {
	w := &writer{
		h: func(msg []byte) (int, error) {
			n, err := l.WriteLog(level, msg)
			// (0, nil) means the message was filtered; report it consumed, like io.Discard.
			if n == 0 && err == nil {
				return len(msg), nil
			}
			return n, err
		},
	}
	return w
}

// ensureThreadSafe wraps w in a mutex-guarded writer so the logger never invokes a
// given sink's Write concurrently. Writers produced by this package's handlers are
// already guarded and are returned unchanged. Pointer-type sinks are deduplicated:
// the same pointer registered multiple times (directly, via WithMinLevel, or as a hook)
// gets one shared mutex so writes serialize.
//
// This method must be called with l.m held (or before l is published, in NewLogger).
func (l *Logger) ensureThreadSafe(w io.Writer) io.Writer {
	if _, ok := w.(*writer); ok {
		return w
	}

	key, canDedup := sinkIdentity(w)
	if !canDedup {
		return &writer{original: w, h: w.Write}
	}

	if l.sinkLocks == nil {
		l.sinkLocks = make(map[io.Writer]*sync.Mutex)
	}
	shared, exists := l.sinkLocks[key]
	if !exists {
		shared = &sync.Mutex{}
		l.sinkLocks[key] = shared
	}

	return &writer{original: w, h: w.Write, shared: shared}
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

// writeSink writes p to w, passing level to an optional LevelWriter. It recovers any
// panic and returns an error that carries the stack trace. A one-line notice goes to
// os.Stderr directly, not through the std log package, because std log output may
// itself be routed into this logger.
func writeSink(w io.Writer, level LogLevel, p []byte) (err error) {
	defer func() {
		if r := recover(); r != nil {
			typ := sinkTypeName(w)
			stack := debug.Stack()
			_, _ = fmt.Fprintf(
				os.Stderr,
				"loginjector: sink %s panicked: %s\n",
				typ,
				strings.ReplaceAll(
					fmt.Sprint(r),
					"\n",
					" ",
				),
			)
			if e, ok := r.(error); ok {
				err = fmt.Errorf(
					"loginjector: sink %s panicked: %w\n%s",
					typ,
					e,
					stack,
				)
			} else {
				err = fmt.Errorf(
					"loginjector: sink %s panicked: %v\n%s",
					typ,
					r,
					stack,
				)
			}
		}
	}()

	if lw, ok := w.(LevelWriter); ok {
		_, err = lw.WriteLevel(level, p)
	} else {
		_, err = w.Write(p)
	}
	return err
}

// sinkTypeName returns the type name of the unwrapped sink w.
// For a *writer with an original sink, it returns the type of the original.
// For a *leveledWriter, it peels through to the inner writer.
// Otherwise it returns the type of w.
func sinkTypeName(w io.Writer) string {
	for {
		switch sink := w.(type) {
		case *writer:
			if sink.original == nil {
				return fmt.Sprintf("%T", w)
			}
			w = sink.original
		case *leveledWriter:
			w = sink.inner
		default:
			return fmt.Sprintf("%T", w)
		}
	}
}

// sinkIdentity peels *leveledWriter layers and returns the identity key for deduplication.
// If the innermost writer is a *writer, it returns (nil, false) — already self-serialised.
// If it's nil or not a pointer type, it returns (nil, false) — not deduplicable.
// Otherwise it returns (inner, true) where inner is a pointer sink.
// Caller must not mutate the returned key.
func sinkIdentity(w io.Writer) (key io.Writer, ok bool) {
	for lw, isLeveled := w.(*leveledWriter); isLeveled; lw, isLeveled = w.(*leveledWriter) {
		w = lw.inner
	}

	// a *writer is already self-serialised.
	if _, isWriter := w.(*writer); isWriter {
		return nil, false
	}

	// a nil writer has no identity; a non-pointer type cannot be deduplicated reliably.
	if w == nil || reflect.TypeOf(w).Kind() != reflect.Pointer {
		return nil, false
	}

	return w, true
}
