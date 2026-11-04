package loginjector

import (
	"errors"
	"net/http"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHttpError(t *testing.T) {
	t.Parallel()
	key1 := "KEY_1"
	key2 := "KEY_2"
	key3 := "KEY_3"

	var err error = NewHttpError(http.StatusInternalServerError)
	require.Contains(t, err.Error(), http.StatusText(http.StatusInternalServerError))
	require.NotContains(t, err.Error(), key1)
	require.NotContains(t, err.Error(), key2)
	require.NotContains(t, err.Error(), key3)

	err = errors.Join(err, errors.New(key1))
	err = errors.Join(err, NewHttpError(http.StatusBadRequest))
	require.Contains(t, err.Error(), http.StatusText(http.StatusBadRequest))
	require.Contains(t, err.Error(), key1)
	require.NotContains(t, err.Error(), key2)
	require.NotContains(t, err.Error(), key3)

	err = errors.Join(err, errors.New(key2))
	require.Contains(t, err.Error(), http.StatusText(http.StatusInternalServerError))
	require.Contains(t, err.Error(), key1)
	require.Contains(t, err.Error(), key2)
	require.NotContains(t, err.Error(), key3)

	var httpErr HttpError
	require.ErrorAs(t, err, &httpErr, "could not convert error to httpCode")
	require.Equal(t, http.StatusInternalServerError, httpErr.StatusCode(), "unexpected status code")
}

func TestFullTrace(t *testing.T) {
	t.Parallel()
	key1 := "KEY_1"
	key2 := "KEY_2"
	key3 := "KEY_3"

	var err error = NewTraceError()
	require.Contains(t, err.Error(), "TestFullTrace")
	require.NotContains(t, err.Error(), key1)
	require.NotContains(t, err.Error(), key2)
	require.NotContains(t, err.Error(), key3)

	err = errors.Join(err, errors.New(key1))
	err = errors.Join(err, NewTraceError())
	require.Contains(t, err.Error(), "TestFullTrace")
	require.Contains(t, err.Error(), key1)
	require.NotContains(t, err.Error(), key2)
	require.NotContains(t, err.Error(), key3)

	err = errors.Join(err, errors.New(key2))
	require.Contains(t, err.Error(), "TestFullTrace")
	require.Contains(t, err.Error(), key1)
	require.Contains(t, err.Error(), key2)
	require.NotContains(t, err.Error(), key3)

	var traceErr TraceError
	require.ErrorAs(t, err, &traceErr, "could not convert error to fullTrace")
	require.Contains(t, traceErr.Line(), "error_test.go:", "unexpected status code")
	require.Contains(t, traceErr.Line(), "TestFullTrace", "unexpected status code")
}

func TestNewTraceErrorf(t *testing.T) {
	t.Parallel()

	// Call the constructor at the top-level test function so LineTrace reports
	// TestNewTraceErrorf, not an anonymous subtest closure (func1).
	errForLine := NewTraceErrorf("something went wrong: %d", 42)
	errForMsg := NewTraceErrorf("important context")

	t.Run("line points at call site", func(t *testing.T) {
		t.Parallel()
		var traceErr TraceError
		require.ErrorAs(t, errForLine, &traceErr)
		assert.Contains(t, traceErr.Line(), "error_test.go:")
		assert.Contains(t, traceErr.Line(), "TestNewTraceErrorf")
	})

	t.Run("message embedded in Error", func(t *testing.T) {
		t.Parallel()
		assert.Contains(t, errForMsg.Error(), "important context")
		assert.Contains(t, errForMsg.Error(), "error_test.go:")
	})

	t.Run("percent-w wrapping traverses to original cause", func(t *testing.T) {
		t.Parallel()
		cause := errors.New("root cause")
		err := NewTraceErrorf("wrapper: %w", cause)
		require.ErrorIs(t, err, cause)
		// unwrap once reaches the fmt.Errorf result which itself wraps cause
		unwrapped := errors.Unwrap(err)
		require.Error(t, unwrapped)
		assert.ErrorIs(t, unwrapped, cause)
	})

	t.Run("no percent-w gives nil Unwrap from fmt.Errorf result", func(t *testing.T) {
		t.Parallel()
		err := NewTraceErrorf("plain message %d", 1)
		// the inner fmt.Errorf result has no wrapped error, so chain stops there
		inner := errors.Unwrap(err)
		require.Error(t, inner) // the fmt.Errorf result itself
		assert.NoError(t, errors.Unwrap(inner))
	})

	t.Run("errors.Join interop", func(t *testing.T) {
		t.Parallel()
		cause := errors.New("sentinel")
		err := NewTraceErrorf("wrapped: %w", cause)
		joined := errors.Join(err, errors.New("other"))
		var traceErr TraceError
		require.ErrorAs(t, joined, &traceErr)
		assert.ErrorIs(t, joined, cause)
	})

	t.Run("satisfies TraceError interface", func(t *testing.T) {
		t.Parallel()
		var te = NewTraceErrorf("check")
		assert.NotEmpty(t, te.Line())
		assert.NotEmpty(t, te.Error())
	})
}

func TestWrapTraceError(t *testing.T) {
	t.Parallel()

	// Call at top-level so LineTrace resolves to TestWrapTraceError, not func1.
	cause0 := errors.New("cause for line test")
	errForLine := WrapTraceError(cause0, "context message")

	t.Run("line points at call site", func(t *testing.T) {
		t.Parallel()
		var traceErr TraceError
		require.ErrorAs(t, errForLine, &traceErr)
		assert.Contains(t, traceErr.Line(), "error_test.go:")
		assert.Contains(t, traceErr.Line(), "TestWrapTraceError")
	})

	t.Run("Unwrap returns the supplied cause", func(t *testing.T) {
		t.Parallel()
		cause := errors.New("underlying")
		err := WrapTraceError(cause, "description")
		assert.Equal(t, cause, errors.Unwrap(err))
		assert.ErrorIs(t, err, cause)
	})

	t.Run("errors.As traverses to typed cause", func(t *testing.T) {
		t.Parallel()
		httpCause := NewHttpError(http.StatusForbidden)
		err := WrapTraceError(httpCause, "forbidden path")
		var httpErr HttpError
		require.ErrorAs(t, err, &httpErr)
		assert.Equal(t, http.StatusForbidden, httpErr.StatusCode())
	})

	t.Run("nil cause is safe", func(t *testing.T) {
		t.Parallel()
		err := WrapTraceError(nil, "no cause here")
		require.NoError(t, errors.Unwrap(err))
		assert.Contains(t, err.Error(), "no cause here")
	})

	t.Run("Error contains message and line", func(t *testing.T) {
		t.Parallel()
		cause := errors.New("root")
		err := WrapTraceError(cause, "outer context")
		assert.Contains(t, err.Error(), "outer context")
		assert.Contains(t, err.Error(), "root")
		assert.Contains(t, err.Error(), "error_test.go:")
	})

	t.Run("errors.Join interop", func(t *testing.T) {
		t.Parallel()
		cause := errors.New("sentinel")
		err := WrapTraceError(cause, "join test")
		joined := errors.Join(err, errors.New("other"))
		var traceErr TraceError
		require.ErrorAs(t, joined, &traceErr)
		assert.ErrorIs(t, joined, cause)
	})

	t.Run("empty message does not produce a double separator", func(t *testing.T) {
		t.Parallel()
		cause := errors.New("root")
		err := WrapTraceError(cause, "")
		// the message segment is empty: Error() must not emit "...: :..." between
		// the line and the cause; it should drop the empty middle entirely.
		assert.NotContains(t, err.Error(), ": :", "empty msg must not create a doubled separator")
		assert.Contains(t, err.Error(), "root", "cause text must still be present")
		assert.Contains(t, err.Error(), "error_test.go:", "trace line must still be present")
	})
}

func TestNewStackTraceError(t *testing.T) {
	t.Parallel()

	t.Run("Stack is multiline and contains caller", func(t *testing.T) {
		t.Parallel()
		err := NewStackTraceError()
		stack := err.Stack()
		assert.Greater(t, strings.Count(stack, "\n"), 1, "stack should be multiline")
		assert.Contains(t, stack, "TestNewStackTraceError")
	})

	t.Run("Runtime contains GOOS GOARCH and Go version", func(t *testing.T) {
		t.Parallel()
		err := NewStackTraceError()
		rt := err.Runtime()
		assert.Contains(t, rt, runtime.GOOS)
		assert.Contains(t, rt, runtime.GOARCH)
		assert.Contains(t, rt, runtime.Version())
	})

	t.Run("Runtime string is cached across errors", func(t *testing.T) {
		t.Parallel()
		e1 := NewStackTraceError()
		e2 := NewStackTraceError()
		// value equality only: both errors report the same cached runtime descriptor.
		assert.Equal(t, e1.Runtime(), e2.Runtime())
	})

	t.Run("Error is empty for no-message variant", func(t *testing.T) {
		t.Parallel()
		err := NewStackTraceError()
		assert.Empty(t, err.Error())
	})

	t.Run("Unwrap returns nil for non-wrapping constructor", func(t *testing.T) {
		t.Parallel()
		err := NewStackTraceError()
		assert.NoError(t, errors.Unwrap(err))
	})

	t.Run("NewStackTraceErrorf message in Error", func(t *testing.T) {
		t.Parallel()
		err := NewStackTraceErrorf("disk at %d%%", 95)
		assert.Contains(t, err.Error(), "disk at 95%")
		assert.NotContains(t, err.Error(), "\n") // Error() must not embed the full stack
		assert.Greater(t, strings.Count(err.Stack(), "\n"), 1)
		assert.NoError(t, errors.Unwrap(err))
	})

	t.Run("WrapStackTraceError Unwrap and Is", func(t *testing.T) {
		t.Parallel()
		cause := errors.New("disk full")
		err := WrapStackTraceError(cause, "storage failure")
		assert.Contains(t, err.Error(), "storage failure")
		assert.Contains(t, err.Error(), "disk full")
		assert.Equal(t, cause, errors.Unwrap(err))
		assert.ErrorIs(t, err, cause)
	})

	t.Run("WrapStackTraceError nil cause is safe", func(t *testing.T) {
		t.Parallel()
		err := WrapStackTraceError(nil, "message only")
		assert.Equal(t, "message only", err.Error())
		assert.NoError(t, errors.Unwrap(err))
	})

	t.Run("errors.Join interop", func(t *testing.T) {
		t.Parallel()
		cause := errors.New("sentinel")
		err := WrapStackTraceError(cause, "wrap")
		joined := errors.Join(err, errors.New("other"))
		var stackErr StackTraceError
		require.ErrorAs(t, joined, &stackErr)
		assert.ErrorIs(t, joined, cause)
	})

	t.Run("runtime caching is race-free", func(t *testing.T) {
		t.Parallel()
		// hammer cachedRuntime from many goroutines under -race; the channel
		// receive provides happens-before so reading results[i] after <-done
		// is data-race-free. value equality across all results is sufficient
		// to prove the sync.Once path returned the same string each time.
		const n = 20
		results := make([]string, n)
		done := make(chan struct{})
		for i := range n {
			go func(idx int) {
				results[idx] = NewStackTraceError().Runtime()
				done <- struct{}{}
			}(i)
		}
		for range n {
			<-done
		}
		for i := 1; i < n; i++ {
			assert.Equal(t, results[0], results[i], "runtime string differs across goroutines")
		}
	})
}

func TestPublicError(t *testing.T) {
	t.Parallel()

	t.Run("PublicMessage returns only the safe text", func(t *testing.T) {
		t.Parallel()
		const secret = "SECRET_TOKEN"
		cause := errors.New("internal detail with " + secret)
		err := NewPublicError("something went wrong", cause)
		assert.Equal(t, "something went wrong", err.PublicMessage())
		assert.NotContains(t, err.PublicMessage(), secret)
	})

	t.Run("Error contains internal cause detail", func(t *testing.T) {
		t.Parallel()
		const secret = "SECRET_TOKEN"
		cause := errors.New("internal detail with " + secret)
		err := NewPublicError("safe message", cause)
		assert.Contains(t, err.Error(), secret)
		assert.NotContains(t, err.PublicMessage(), secret)
	})

	t.Run("Unwrap returns cause", func(t *testing.T) {
		t.Parallel()
		cause := errors.New("underlying")
		err := NewPublicError("public", cause)
		assert.Equal(t, cause, errors.Unwrap(err))
		assert.ErrorIs(t, err, cause)
	})

	t.Run("nil cause is safe", func(t *testing.T) {
		t.Parallel()
		err := NewPublicError("public only", nil)
		assert.Equal(t, "public only", err.PublicMessage())
		assert.Equal(t, "public only", err.Error())
		assert.NoError(t, errors.Unwrap(err))
	})

	t.Run("empty public message stays empty and does not leak cause", func(t *testing.T) {
		t.Parallel()
		cause := errors.New("sensitive internal")
		err := NewPublicError("", cause)
		assert.Empty(t, err.PublicMessage())
		assert.NotContains(t, err.PublicMessage(), "sensitive internal")
		// Error() should still expose the cause
		assert.Contains(t, err.Error(), "sensitive internal")
	})

	t.Run("errors.As traverses to typed cause", func(t *testing.T) {
		t.Parallel()
		httpCause := NewHttpError(http.StatusUnauthorized)
		err := NewPublicError("unauthorized", httpCause)
		var httpErr HttpError
		require.ErrorAs(t, err, &httpErr)
		assert.Equal(t, http.StatusUnauthorized, httpErr.StatusCode())
	})

	t.Run("errors.Join interop", func(t *testing.T) {
		t.Parallel()
		cause := errors.New("sentinel")
		err := NewPublicError("public", cause)
		joined := errors.Join(err, errors.New("other"))
		var pubErr PublicError
		require.ErrorAs(t, joined, &pubErr)
		assert.Equal(t, "public", pubErr.PublicMessage())
	})

	t.Run("composition with WrapHttpError", func(t *testing.T) {
		t.Parallel()
		cause := errors.New("db timeout")
		httpWrapped := WrapHttpError(http.StatusNotFound, cause)
		pubErr := NewPublicError("resource not found", httpWrapped)

		// PublicMessage is clean
		assert.Equal(t, "resource not found", pubErr.PublicMessage())
		assert.NotContains(t, pubErr.PublicMessage(), "db timeout")

		// errors.As reaches HttpError
		var httpErr HttpError
		require.ErrorAs(t, pubErr, &httpErr)
		assert.Equal(t, http.StatusNotFound, httpErr.StatusCode())

		// errors.Is reaches the original cause
		assert.ErrorIs(t, pubErr, cause)
	})
}

func TestNewPublicErrorDetails(t *testing.T) {
	t.Parallel()

	t.Run("joins parts with a single space", func(t *testing.T) {
		t.Parallel()
		err := NewPublicErrorDetails("invalid", "input", "field")
		assert.Equal(t, "invalid input field", err.PublicMessage())
		assert.Equal(t, "invalid input field", err.Details())
		assert.Equal(t, "invalid input field", err.Error())
	})

	t.Run("Details and PublicMessage return the same value", func(t *testing.T) {
		t.Parallel()
		err := NewPublicErrorDetails("one", "two")
		assert.Equal(t, err.PublicMessage(), err.Details())
	})

	t.Run("single part is returned verbatim without trailing space", func(t *testing.T) {
		t.Parallel()
		err := NewPublicErrorDetails("solo")
		assert.Equal(t, "solo", err.Details())
		assert.Equal(t, "solo", err.Error())
	})

	t.Run("no parts produces an empty public message", func(t *testing.T) {
		t.Parallel()
		err := NewPublicErrorDetails()
		assert.Empty(t, err.PublicMessage())
		assert.Empty(t, err.Details())
		assert.Empty(t, err.Error())
	})

	t.Run("nil cause: Unwrap returns nil", func(t *testing.T) {
		t.Parallel()
		err := NewPublicErrorDetails("x")
		assert.NoError(t, errors.Unwrap(err))
	})

	t.Run("satisfies PublicError interface", func(t *testing.T) {
		t.Parallel()
		var pe PublicError = NewPublicErrorDetails("a", "b")
		assert.Equal(t, "a b", pe.PublicMessage())
	})

	t.Run("errors.As traverses to PublicDetailsError", func(t *testing.T) {
		t.Parallel()
		err := NewPublicErrorDetails("not", "found")
		joined := errors.Join(err, errors.New("other"))
		var pde PublicDetailsError
		require.ErrorAs(t, joined, &pde)
		assert.Equal(t, "not found", pde.Details())
	})
}

// resetRuntimeCacheForTest clears the once-computed runtime string AND the provider
// so subsequent tests get a clean slate. Test-only; do not call from production code.
// The pointer swap is serialised under runtimeDetailsExtraMu, the same mutex that
// cachedRuntime uses when snapshotting the *sync.Once, so there is no data race
// between a concurrent cachedRuntime call and this reset.
func resetRuntimeCacheForTest() {
	runtimeDetailsExtraMu.Lock()
	runtimeDetails.once = &sync.Once{}
	runtimeDetails.value = ""
	runtimeDetailsExtraProvider = nil
	runtimeDetailsFrozen.Store(false)
	runtimeDetailsExtraMu.Unlock()
}

// TestSetRuntimeDetailsProvider covers the process-wide runtime details hook.
// These tests are NOT parallel at the top level because they mutate shared
// process-wide state (runtimeDetails.once / runtimeDetailsExtraProvider).
// Each subtest resets the state via t.Cleanup(resetRuntimeCacheForTest).
//
//nolint:paralleltest // mutates the process-wide runtime-details cache and provider
func TestSetRuntimeDetailsProvider(t *testing.T) {
	t.Run("no_provider_default", func(t *testing.T) {
		t.Cleanup(resetRuntimeCacheForTest)
		err := NewStackTraceError()
		rt := err.Runtime()
		assert.Contains(t, rt, "os=")
		assert.Contains(t, rt, "arch=")
		assert.Contains(t, rt, "go=")
		// must NOT end with a trailing space when no provider is set.
		assert.Equal(t, rt, strings.TrimRight(rt, " "))
	})

	t.Run("provider_appended", func(t *testing.T) {
		t.Cleanup(resetRuntimeCacheForTest)
		SetRuntimeDetailsProvider(func() string { return "cpu=test pid=1" })
		err := NewStackTraceError()
		rt := err.Runtime()
		assert.True(t, strings.HasSuffix(rt, " cpu=test pid=1"), "runtime must end with provider output; got: %q", rt)
	})

	t.Run("provider_empty_string", func(t *testing.T) {
		t.Cleanup(resetRuntimeCacheForTest)
		SetRuntimeDetailsProvider(func() string { return "" })
		err := NewStackTraceError()
		rt := err.Runtime()
		// must NOT end with trailing space when provider returns empty string.
		assert.Equal(t, rt, strings.TrimRight(rt, " "), "runtime must not end with trailing space")
		assert.Contains(t, rt, "os=")
	})

	t.Run("provider_set_after_first_call_is_ignored", func(t *testing.T) {
		t.Cleanup(resetRuntimeCacheForTest)
		// construct first error to warm the cache.
		first := NewStackTraceError()
		baseRuntime := first.Runtime()

		// now set a provider — cache is already populated, so this is a no-op.
		SetRuntimeDetailsProvider(func() string { return "injected=yes" })
		second := NewStackTraceError()

		assert.Equal(t, baseRuntime, second.Runtime(), "provider set after first construction must have no effect")
		assert.NotContains(t, second.Runtime(), "injected=yes")
	})

	t.Run("provider_nil_clears", func(t *testing.T) {
		t.Cleanup(resetRuntimeCacheForTest)
		SetRuntimeDetailsProvider(func() string { return "should=not-appear" })
		SetRuntimeDetailsProvider(nil) // clear before first construction.
		err := NewStackTraceError()
		rt := err.Runtime()
		assert.NotContains(t, rt, "should=not-appear")
		assert.Contains(t, rt, "os=")
	})

	t.Run("concurrent_construction_with_provider_ForRaceCondition", func(t *testing.T) {
		t.Cleanup(resetRuntimeCacheForTest)
		SetRuntimeDetailsProvider(func() string { return "cpu=concurrent" })

		const n = 100
		results := make([]string, n)
		var wg sync.WaitGroup
		for i := range n {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				results[idx] = NewStackTraceError().Runtime()
			}(i)
		}
		wg.Wait()

		// all goroutines must see the same cached string (sync.Once ensures exactly one invocation).
		for i := 1; i < n; i++ {
			assert.Equal(t, results[0], results[i], "runtime string must be identical across goroutines")
		}
		assert.True(t, strings.HasSuffix(results[0], " cpu=concurrent"), "provider output must be present")
	})

	t.Run("multiline_provider_is_single_lined", func(t *testing.T) {
		t.Cleanup(resetRuntimeCacheForTest)
		// a multi-line provider result with an embedded ESC control byte must be
		// collapsed to a single line so Runtime() keeps its single-line contract.
		SetRuntimeDetailsProvider(func() string { return "a\nb\x1b[31mc\td" })
		rt := NewStackTraceError().Runtime()

		assert.NotContains(t, rt, "\n", "Runtime() must stay single-line")
		assert.NotContains(t, rt, "\x1b", "ESC control byte must be stripped")
		// CR/LF/TAB become spaces; the ESC byte is dropped, so the visible chars survive.
		assert.True(t, strings.HasSuffix(rt, " a b[31mc d"),
			"CR/LF/TAB collapse to spaces and other control chars drop; got: %q", rt)
	})

	t.Run("single_line_provider_is_byte_identical", func(t *testing.T) {
		t.Cleanup(resetRuntimeCacheForTest)
		// a control-char-free provider result must pass through unchanged.
		SetRuntimeDetailsProvider(func() string { return "cpu=Xeon build=v1.2.3" })
		rt := NewStackTraceError().Runtime()
		assert.True(t, strings.HasSuffix(rt, " cpu=Xeon build=v1.2.3"),
			"a clean single-line provider result must be appended verbatim; got: %q", rt)
	})

	t.Run("panicking_provider_falls_back_to_base", func(t *testing.T) {
		t.Cleanup(resetRuntimeCacheForTest)
		// install a provider that always panics.
		SetRuntimeDetailsProvider(func() string { panic("provider exploded") })

		// must not crash; must return the base os/arch/go string.
		err := NewStackTraceError()
		rt := err.Runtime()

		assert.Contains(t, rt, "os=", "base os tag must be present after provider panic")
		assert.Contains(t, rt, "arch=", "base arch tag must be present after provider panic")
		assert.Contains(t, rt, "go=", "base go tag must be present after provider panic")
		// the panic value must not leak into the runtime string.
		assert.NotContains(t, rt, "provider exploded", "panic detail must not appear in runtime string")
		// must not end with a trailing space.
		assert.Equal(t, rt, strings.TrimRight(rt, " "), "runtime must not end with trailing space after provider panic")
	})
}

func TestWrapHttpError(t *testing.T) {
	t.Parallel()

	t.Run("StatusCode returns supplied code", func(t *testing.T) {
		t.Parallel()
		err := WrapHttpError(http.StatusTeapot, errors.New("cause"))
		var httpErr HttpError
		require.ErrorAs(t, err, &httpErr)
		assert.Equal(t, http.StatusTeapot, httpErr.StatusCode())
	})

	t.Run("Error contains status text and cause", func(t *testing.T) {
		t.Parallel()
		cause := errors.New("database unavailable")
		err := WrapHttpError(http.StatusInternalServerError, cause)
		assert.Contains(t, err.Error(), http.StatusText(http.StatusInternalServerError))
		assert.Contains(t, err.Error(), "database unavailable")
	})

	t.Run("Unwrap returns cause", func(t *testing.T) {
		t.Parallel()
		cause := errors.New("root")
		err := WrapHttpError(http.StatusBadGateway, cause)
		assert.Equal(t, cause, errors.Unwrap(err))
		assert.ErrorIs(t, err, cause)
	})

	t.Run("nil cause behaves like NewHttpError", func(t *testing.T) {
		t.Parallel()
		code := http.StatusNotFound
		plain := NewHttpError(code)
		wrapped := WrapHttpError(code, nil)
		assert.Equal(t, plain.Error(), wrapped.Error())
		assert.Equal(t, plain.StatusCode(), wrapped.StatusCode())
		assert.NoError(t, errors.Unwrap(wrapped))
	})

	t.Run("unknown code does not produce empty Error", func(t *testing.T) {
		t.Parallel()
		err := WrapHttpError(99999, nil)
		assert.NotEmpty(t, err.Error())
		assert.Contains(t, err.Error(), "99999")
	})

	t.Run("errors.As still finds HttpError", func(t *testing.T) {
		t.Parallel()
		cause := errors.New("cause")
		err := WrapHttpError(http.StatusForbidden, cause)
		var httpErr HttpError
		require.ErrorAs(t, err, &httpErr)
		assert.Equal(t, http.StatusForbidden, httpErr.StatusCode())
	})

	t.Run("errors.Join interop", func(t *testing.T) {
		t.Parallel()
		cause := errors.New("sentinel")
		err := WrapHttpError(http.StatusConflict, cause)
		joined := errors.Join(err, errors.New("other"))
		var httpErr HttpError
		require.ErrorAs(t, joined, &httpErr)
		assert.ErrorIs(t, joined, cause)
	})

	t.Run("existing TestHttpError style assertion still holds for new type", func(t *testing.T) {
		t.Parallel()
		cause := errors.New("extra detail")
		err := WrapHttpError(http.StatusInternalServerError, cause)
		assert.Contains(t, err.Error(), http.StatusText(http.StatusInternalServerError))
	})
}

// TestNewTraceError pins NewTraceError's resolved frame so an internal refactor that
// shifts the skip depth (e.g. inserting a wrapper between the constructor and LineTrace)
// fails CI, and proves NewTraceErrorSkip is the behavior-preserving primitive it delegates
// to. The constructors are called at the test's top level — not in a t.Run closure — so
// the resolved method is TestNewTraceError. The expected line is captured with
// runtime.Caller(0) one line below each constructor (a fixed 1-line offset, no rot-prone
// literal): gofmt keeps the two statements on adjacent lines.
func TestNewTraceError(t *testing.T) {
	t.Parallel()
	te := NewTraceError()
	_, file, teAnchor, _ := runtime.Caller(0) // one line below the constructor above
	teLine := teAnchor - 1

	teSkip := NewTraceErrorSkip(0)
	_, fileSkip, skipAnchor, _ := runtime.Caller(0)
	skipLine := skipAnchor - 1

	// NewTraceError() and NewTraceErrorSkip(0) each resolve their own direct caller, so on
	// THIS shared source line both resolve the same test frame and their Line()s are equal.
	// The equality is a same-call-site coincidence, not a claim that the two are semantically
	// identical — NewTraceError delegates to NewTraceErrorSkip(1), not (0). Keeping them on
	// one source line is what makes the frames match.
	eqDirect, eqSkip := NewTraceError(), NewTraceErrorSkip(0)

	t.Run("line points at the caller base file, line and method", func(t *testing.T) {
		t.Parallel()
		// assert only the base filename + line + method; the leading <parent>/ prefix is
		// the checkout dir on CI and is environment-dependent, so it is not asserted.
		assert.Contains(t, te.Line(), filepath.Base(file)+":"+strconv.Itoa(teLine))
		assert.Contains(t, te.Line(), "TestNewTraceError")
	})

	t.Run("Skip(0) resolves its direct caller frame", func(t *testing.T) {
		t.Parallel()
		assert.Contains(t, teSkip.Line(), filepath.Base(fileSkip)+":"+strconv.Itoa(skipLine))
		assert.Contains(t, teSkip.Line(), "TestNewTraceError")
	})

	t.Run("delegation is behavior-preserving", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, eqDirect.Line(), eqSkip.Line(),
			"NewTraceError().Line() == NewTraceErrorSkip(0).Line() holds because both are "+
				"called on the same source line, not because the two are semantically equal")
	})
}

// TestStackTraceError_StackRedacted covers the redacted-stack function: it must drop the
// absolute build-host prefix from every frame while keeping <parent>/<base>:<line> frames.
func TestStackTraceError_StackRedacted(t *testing.T) {
	t.Parallel()

	err := NewStackTraceError()
	full := err.Stack()
	redacted := StackRedacted(err)

	t.Run("no absolute frame prefix remains", func(t *testing.T) {
		t.Parallel()
		for _, ln := range strings.Split(redacted, "\n") {
			if !strings.HasPrefix(ln, "\t") {
				continue
			}
			body := strings.TrimPrefix(ln, "\t")
			require.False(t, strings.HasPrefix(body, "/"),
				"a redacted frame path must not be absolute, got: %q", ln)
		}
	})

	t.Run("keeps parent/base frames and actually shortens", func(t *testing.T) {
		t.Parallel()
		require.Regexp(t, `error_test\.go:\d+`, redacted,
			"the caller frame must survive as <parent>/<base>:<line>")
		require.NotEqual(t, full, redacted,
			"redaction must change the stack (this host uses absolute build paths)")
	})
}

// TestRuntimeDetailsFrozen covers the freeze-state probe. It is NOT parallel at the top
// level because it mutates process-wide runtime-details state shared with
// TestSetRuntimeDetailsProvider; each subtest resets that state first.
//
//nolint:paralleltest // mutates the process-wide runtime-details cache and provider
func TestRuntimeDetailsFrozen(t *testing.T) {
	t.Run("false before first construction, true after", func(t *testing.T) {
		resetRuntimeCacheForTest()
		t.Cleanup(resetRuntimeCacheForTest)

		require.False(t, RuntimeDetailsFrozen(),
			"must be false before any StackTraceError is constructed")
		_ = NewStackTraceError()
		require.True(t, RuntimeDetailsFrozen(),
			"must be true after the first StackTraceError freezes the string")
	})

	t.Run("concurrent constructors and probes are race-free", func(t *testing.T) {
		resetRuntimeCacheForTest()
		t.Cleanup(resetRuntimeCacheForTest)

		var wg sync.WaitGroup
		for range 50 {
			wg.Add(2)
			go func() { defer wg.Done(); _ = NewStackTraceError() }()
			go func() { defer wg.Done(); _ = RuntimeDetailsFrozen() }()
		}
		wg.Wait()
		require.True(t, RuntimeDetailsFrozen(),
			"the string must be frozen once the constructors have run")
	})
}
