package loginjector

import (
	"fmt"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/prorochestvo/loginjector/internal"
)

// TraceError is an error that provides a stack trace.
type TraceError interface {
	Line() string
	error
}

// NewTraceError creates a TraceError whose Line() is the caller's file:line (method).
// It is a pure leaf — it carries a line and no cause, so it has no Unwrap and does not
// participate in errors.Is/As traversal. Use NewTraceErrorf or WrapTraceError when you
// need a cause-carrying, unwrappable trace error.
func NewTraceError() TraceError {
	return NewTraceErrorSkip(1) // +1 for this delegation hop
}

// NewTraceErrorSkip creates a TraceError whose Line() is resolved skip frames above
// the caller of NewTraceErrorSkip. skip=0 is the direct caller; skip=1 is that
// caller's caller. Use it when wrapping NewTraceError in a helper so the line still
// points at your caller rather than the wrapper.
func NewTraceErrorSkip(skip int) TraceError {
	return &traceError{line: internal.LineTrace(skip + 2)}
}

// NewTraceErrorf creates a TraceError with a formatted message and the current
// call-site line. The message is built via fmt.Errorf, so a %w verb in format
// wraps the corresponding argument and errors.Is/errors.As traverse to it
// (through one intermediate fmt.Errorf node — Unwrap is always non-nil for the
// value produced here; it just terminates with nil when no %w was used). To
// have Unwrap return your cause directly with a single hop, use WrapTraceError.
// The stack line is captured at the point of this call, not inside any helper.
func NewTraceErrorf(format string, args ...any) TraceError {
	line := internal.LineTrace(2)
	// Use fmt.Errorf so that %w wrapping falls through automatically.
	// causeInMsg=true signals that the cause text is already embedded in msg
	// and must not be appended again in Error().
	wrapped := fmt.Errorf(format, args...)
	return &traceErrorWithCause{
		line:       line,
		msg:        wrapped.Error(),
		cause:      wrapped,
		causeInMsg: true,
	}
}

// WrapTraceError creates a TraceError with a formatted message, the current
// call-site line, and an explicit cause. errors.Unwrap, errors.Is, and
// errors.As traverse to cause. Passing a nil cause is safe; Unwrap returns nil.
// The message is rendered with fmt.Sprintf — do NOT use %w in the format string,
// it would produce a "%!w(...)" garbage token; rely on the cause argument for
// wrapping, or use NewTraceErrorf if you need %w in the format itself.
func WrapTraceError(cause error, format string, args ...any) TraceError {
	line := internal.LineTrace(2)
	msg := fmt.Sprintf(format, args...)
	return &traceErrorWithCause{line: line, msg: msg, cause: cause}
}

// StackTraceError captures a full stack trace and runtime environment details
// at the point of construction. Error() returns only the message and cause for
// compact log lines; the full stack is available on demand via Stack(). Runtime
// details (OS, arch, Go version) are cached once for the process lifetime.
type StackTraceError interface {
	// Stack returns the full debug.Stack() output captured at construction time.
	// The output contains absolute build-host file paths — do not forward it to
	// untrusted clients or HTTP responses.
	Stack() string
	// Runtime returns a string describing the OS, architecture, and Go version,
	// computed once on first use and cached for the process lifetime — every
	// StackTraceError value shares the same string. If
	// SetRuntimeDetailsProvider was called with a non-nil function before any
	// StackTraceError was constructed, the provider's output is appended to the
	// built-in os/arch/go descriptor.
	Runtime() string
	error
}

// NewStackTraceError creates a StackTraceError with the full call-stack captured
// at this call site. Error() returns an empty string because no message is supplied —
// call Stack() for the full trace. The empty Error() means this value contributes
// no text when joined via errors.Join or wrapped via fmt.Errorf("%w"); use
// NewStackTraceErrorf when you need a non-empty error string in the chain.
// Stack() contains absolute build-host file paths; do not forward it to untrusted clients.
func NewStackTraceError() StackTraceError {
	return &stackTraceError{stack: internal.StackTrace(), runtime: cachedRuntime()}
}

// NewStackTraceErrorf creates a StackTraceError with a formatted message and the
// full call-stack captured at this call site. Error() returns the message only;
// use Stack() for the full trace.
func NewStackTraceErrorf(format string, args ...any) StackTraceError {
	return &stackTraceError{
		msg:     fmt.Sprintf(format, args...),
		stack:   internal.StackTrace(),
		runtime: cachedRuntime(),
	}
}

// WrapStackTraceError creates a StackTraceError with a formatted message, the
// full call-stack captured at this call site, and an explicit cause. errors.Unwrap,
// errors.Is, and errors.As traverse to cause. Passing a nil cause is safe.
// The message is rendered with fmt.Sprintf — do NOT use %w in the format string;
// use the cause argument for wrapping.
func WrapStackTraceError(cause error, format string, args ...any) StackTraceError {
	return &stackTraceError{
		msg:     fmt.Sprintf(format, args...),
		stack:   internal.StackTrace(),
		runtime: cachedRuntime(),
		cause:   cause,
	}
}

// StackRedacted returns e.Stack() with each frame's absolute source path shortened
// to "<parent>/<base>" — the identical reduction LineTrace applies (e.g.
// "/home/ci/proj/main.go:10" becomes "proj/main.go:10"). The line numbers, goroutine
// header, and frame ordering are preserved; only the leading directory prefix of each
// frame path is dropped. This removes build-host absolute path prefixes, making the
// output safe to surface where Stack() is not.
//
// It is a package-level function rather than a method on StackTraceError so the
// interface stays sealed: adding a method to the exported interface would break any
// external type that implements it.
func StackRedacted(e StackTraceError) string {
	return internal.RedactStackPaths(e.Stack())
}

// PublicError separates a user-safe public message from the internal cause.
// Error() returns the internal detail for logging; PublicMessage() returns only
// the text that is safe to expose to clients and never leaks the internal cause.
type PublicError interface {
	// PublicMessage returns the client-safe text. It never includes any detail
	// from the internal cause, even if the public string is empty.
	PublicMessage() string
	error
}

// NewPublicError creates a PublicError from a client-safe message and an internal
// cause. Error() returns the cause's error text for logging, falling back to the
// public message only when cause is nil. PublicMessage() always returns only the
// public string. Unwrap returns cause, so errors.Is and errors.As traverse to it.
func NewPublicError(public string, cause error) PublicError {
	return &publicError{public: public, cause: cause}
}

// PublicDetailsError extends PublicError with a Details accessor that returns
// the same client-safe text as PublicMessage. It is the return type of
// NewPublicErrorDetails and exists so callers using the variadic constructor
// have an accessor name that matches the common consumer convention.
type PublicDetailsError interface {
	PublicError
	// Details returns the joined client-safe text. It is an alias of
	// PublicMessage and always returns the same value.
	Details() string
}

// NewPublicErrorDetails creates a PublicDetailsError by joining the given parts
// with a single space. The result has no internal cause; Error() and
// PublicMessage() both return the joined string. It is a convenience for the
// common pattern of building a public message from several parts and is
// semantically identical to NewPublicError(strings.Join(parts, " "), nil).
func NewPublicErrorDetails(parts ...string) PublicDetailsError {
	return &publicError{public: strings.Join(parts, " ")}
}

// WrapHttpError creates an HttpError that carries an underlying cause. StatusCode()
// returns code, Error() returns the HTTP status text followed by the cause detail,
// and errors.Unwrap traverses to cause. For standard HTTP codes with a nil cause,
// Error() matches NewHttpError(code). For unrecognised codes where http.StatusText
// returns an empty string, Error() falls back to "HTTP <code>" so the string is
// never empty — this diverges from NewHttpError(code), which returns "" for such
// codes; switching from NewHttpError to WrapHttpError(code, nil) on a bogus code
// therefore changes the Error() output.
func WrapHttpError(code int, cause error) HttpError {
	return &httpErrorWithCause{code: code, cause: cause}
}

// HttpError is an error that provides an HTTP status code.
type HttpError interface {
	StatusCode() int
	error
}

// NewHttpError creates a new HttpError with the given status code.
func NewHttpError(code int) HttpError {
	return &httpError{code: code}
}

// traceError is an error that provides a stack trace.
type traceError struct {
	line string
}

var _ TraceError = (*traceError)(nil)

func (e *traceError) Line() string {
	return e.line
}

func (e *traceError) Error() string {
	return e.line
}

// traceErrorWithCause is the richer trace error produced by NewTraceErrorf and
// WrapTraceError. It implements the TraceError interface and adds Unwrap.
// When causeInMsg is true, the cause's text is already embedded in msg (via
// fmt.Errorf %w expansion) and must not be appended again in Error().
type traceErrorWithCause struct {
	line       string
	msg        string
	cause      error
	causeInMsg bool
}

var _ TraceError = (*traceErrorWithCause)(nil)

func (e *traceErrorWithCause) Line() string {
	return e.line
}

func (e *traceErrorWithCause) Error() string {
	// concatenate the non-empty segments with ": " separators so an empty line,
	// empty msg, or absent cause cannot produce a "...:  :..." doubled separator.
	result := e.line
	if e.msg != "" {
		if result != "" {
			result += ": "
		}
		result += e.msg
	}
	if e.cause != nil && !e.causeInMsg {
		if result != "" {
			result += ": "
		}
		result += e.cause.Error()
	}
	return result
}

func (e *traceErrorWithCause) Unwrap() error {
	return e.cause
}

// stackTraceError is the concrete type backing the StackTraceError interface.
type stackTraceError struct {
	msg     string
	stack   string
	runtime string
	cause   error
}

var _ StackTraceError = (*stackTraceError)(nil)

func (e *stackTraceError) Stack() string {
	return e.stack
}

func (e *stackTraceError) Runtime() string {
	return e.runtime
}

func (e *stackTraceError) Error() string {
	switch {
	case e.msg == "" && e.cause == nil:
		return ""
	case e.msg == "" && e.cause != nil:
		return e.cause.Error()
	case e.cause == nil:
		return e.msg
	default:
		return e.msg + ": " + e.cause.Error()
	}
}

func (e *stackTraceError) Unwrap() error {
	return e.cause
}

// publicError is the concrete type backing the PublicError interface.
type publicError struct {
	public string
	cause  error
}

var _ PublicError = (*publicError)(nil)
var _ PublicDetailsError = (*publicError)(nil)

func (e *publicError) PublicMessage() string {
	return e.public
}

func (e *publicError) Details() string {
	return e.public
}

func (e *publicError) Error() string {
	if e.cause != nil {
		return e.cause.Error()
	}
	return e.public
}

func (e *publicError) Unwrap() error {
	return e.cause
}

type httpError struct {
	code int
}

var _ HttpError = (*httpError)(nil)

// StatusCode returns the HTTP status code.
func (e *httpError) StatusCode() int {
	return e.code
}

// Error returns the HTTP status text.
func (e *httpError) Error() string {
	return http.StatusText(e.code)
}

// httpErrorWithCause is the concrete type produced by WrapHttpError. It satisfies
// the existing HttpError interface and adds Unwrap for errors chain traversal.
type httpErrorWithCause struct {
	code  int
	cause error
}

var _ HttpError = (*httpErrorWithCause)(nil)

func (e *httpErrorWithCause) StatusCode() int {
	return e.code
}

func (e *httpErrorWithCause) Error() string {
	text := http.StatusText(e.code)
	if text == "" {
		text = fmt.Sprintf("HTTP %d", e.code)
	}
	if e.cause == nil {
		return text
	}
	return text + ": " + e.cause.Error()
}

func (e *httpErrorWithCause) Unwrap() error {
	return e.cause
}

// SetRuntimeDetailsProvider installs a process-wide provider that supplies
// additional runtime detail text appended to every StackTraceError's
// Runtime() output. The provider is called at most once per process; its
// return value is cached for the lifetime of the process, mirroring the
// caching behaviour of the built-in os/arch/Go-version runtime string.
//
// Call this at startup, before any StackTraceError is constructed. Calling
// it after a StackTraceError has been constructed has NO effect, because the
// runtime string is cached on first use (see Runtime() docs). Calling it
// more than once: the last call before the cache is populated wins; calls
// after the cache is populated are no-ops.
//
// The provider must be safe to call from any goroutine. Loginjector calls
// it under the same sync.Once that guards the built-in runtime string, so
// the provider sees no concurrent invocation; once cached, Runtime() never
// calls it again.
//
// Provider must not panic; a panic is recovered and the built-in base string
// (os/arch/go version) is used without the extra detail.
//
// Only one provider is active at a time; a subsequent SetRuntimeDetailsProvider
// call before the cache is populated replaces the first with no composition.
//
// The provider's return value is sanitized to keep Runtime() single-line: CR, LF, and
// TAB are collapsed to spaces and other control characters are stripped, so a multi-line
// or control-char-laden provider result cannot break the single-line log contract. Printable
// Unicode is preserved.
//
// Pass nil to clear a previously-set provider (useful in tests).
func SetRuntimeDetailsProvider(fn func() string) {
	runtimeDetailsExtraMu.Lock()
	runtimeDetailsExtraProvider = fn
	runtimeDetailsExtraMu.Unlock()
}

// RuntimeDetailsFrozen reports whether the process-wide runtime-details string has
// already been computed and cached (frozen). Once it returns true, a subsequent
// SetRuntimeDetailsProvider call is a no-op — the provider will never run and its
// detail will not appear in any StackTraceError's Runtime().
//
// This is an advisory diagnostic, not a synchronization primitive: it reports the
// freeze state at the moment of the call. It gives no guarantee that a provider set
// immediately after a false result will take effect (a concurrent StackTraceError
// construction may freeze the string in between). Use it at startup to warn that a
// provider was registered too late, not to gate registration.
func RuntimeDetailsFrozen() bool {
	return runtimeDetailsFrozen.Load()
}

// sanitizeRuntimeExtra keeps Runtime() single-line: CR/LF/TAB become spaces,
// other control chars are dropped. Runs inside the once.Do (single-threaded).
func sanitizeRuntimeExtra(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r == '\n' || r == '\r' || r == '\t':
			return ' '
		case r < 0x20 || r == 0x7f:
			return -1 // drop other control chars
		default:
			return r
		}
	}, s)
}

var (
	runtimeDetailsExtraMu       sync.Mutex
	runtimeDetailsExtraProvider func() string
)

// runtimeDetailsFrozen records whether the runtime-details string has been computed and
// cached. It is set once inside cachedRuntime's once.Do and read by RuntimeDetailsFrozen.
// It is the only cross-goroutine channel for the freeze fact and never reads the
// mutex-unguarded runtimeDetails.value, so the probe carries no data race.
var runtimeDetailsFrozen atomic.Bool

// runtimeDetails holds the once-computed OS/arch/Go-version string. The once
// field is a pointer so that test helpers can swap it atomically under
// runtimeDetailsExtraMu without data-racing against a concurrent Do in flight:
// the goroutine that already grabbed the old *sync.Once runs its Do to completion
// against the orphaned instance; the new *sync.Once starts fresh for the next call.
var runtimeDetails = struct {
	once  *sync.Once
	value string
}{once: &sync.Once{}}

// cachedRuntime returns the process-wide runtime descriptor, computing it exactly
// once regardless of how many StackTraceError values are created.
func cachedRuntime() string {
	// snapshot the current *sync.Once pointer under the mutex so a concurrent
	// test reset (which swaps the pointer under the same mutex) is seen atomically.
	runtimeDetailsExtraMu.Lock()
	o := runtimeDetails.once
	runtimeDetailsExtraMu.Unlock()

	o.Do(func() {
		runtimeDetailsFrozen.Store(true)

		base := fmt.Sprintf("os=%s arch=%s go=%s",
			runtime.GOOS, runtime.GOARCH, runtime.Version())

		// grab the provider under the mutex, then release before calling it so
		// the provider is free to call back into anything without deadlocking.
		runtimeDetailsExtraMu.Lock()
		provider := runtimeDetailsExtraProvider
		runtimeDetailsExtraMu.Unlock()

		if provider != nil {
			func() {
				// a panicking provider must not poison the runtime string for
				// the whole process; fall back to base on any panic.
				defer func() { _ = recover() }()
				if extra := provider(); extra != "" {
					base = base + " " + sanitizeRuntimeExtra(extra)
				}
			}()
		}

		runtimeDetails.value = base
	})
	return runtimeDetails.value
}
