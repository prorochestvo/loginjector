package internal

import (
	"regexp"
	"runtime/debug"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// callFromGeneric is a top-level generic function. The Go compiler appends
// "[...]" to its runtime name (e.g. "internal.callFromGeneric[...]"), which
// contains dots and would confuse the last-dot split without the generic-strip fix.
func callFromGeneric[T any]() string { return LineTrace(1) }

func TestStackTrace(t *testing.T) {
	t.Parallel()

	t.Run("dumps the calling goroutine header and frames", func(t *testing.T) {
		t.Parallel()
		result := StackTrace()
		require.True(t, strings.HasPrefix(result, "goroutine "),
			"stack dump must start with the goroutine header, got: %.40q", result)
		require.Contains(t, result, "internal.TestStackTrace",
			"stack dump must contain the calling test frame")
		require.Equal(t, strings.TrimSpace(result), result,
			"stack dump must be trimmed of surrounding whitespace")
	})

	t.Run("agrees with a same-goroutine debug.Stack capture", func(t *testing.T) { //nolint:paralleltest // both stack captures must run on the same goroutine
		// not parallel: both captures must run on this same goroutine so the
		// goroutine header and caller frame are directly comparable.
		got := StackTrace()
		want := strings.TrimSpace(string(debug.Stack()))

		// both must open with the goroutine header line.
		require.True(t, strings.HasPrefix(got, "goroutine "))
		require.True(t, strings.HasPrefix(want, "goroutine "))

		// both must name this test function as a frame.
		require.Contains(t, got, "internal.TestStackTrace")
		require.Contains(t, want, "internal.TestStackTrace")

		// both must reference this test file with a "file.go:line" locator in the
		// same layout debug.Stack emits. The two capture calls sit on different
		// source lines, so the line numbers differ — what must agree is that each
		// dump carries a stacktrace_test.go:<line> frame for the caller.
		fileLine := regexp.MustCompile(`stacktrace_test\.go:\d+`)
		require.Regexp(t, fileLine, got,
			"StackTrace output must carry the caller's file:line frame")
		require.Regexp(t, fileLine, want,
			"debug.Stack output must carry the caller's file:line frame")
	})
}

func TestLineTrace(t *testing.T) {
	t.Parallel()

	// "resolves caller frame" must be run at function top level (not inside a
	// t.Run closure) to land on tRunner at skip=2 — a closure adds an extra frame.
	// We capture the value here and verify inside the subtest so t.Run still
	// owns the reporting, but the skip count is fixed at the top-level call site.
	callerFrameResult := LineTrace(2)

	t.Run("resolves caller frame", func(t *testing.T) {
		t.Parallel()
		require.Contains(t, callerFrameResult, "testing/testing.go:")
		require.Contains(t, callerFrameResult, "tRunner")
	})

	t.Run("returns empty on invalid skip", func(t *testing.T) {
		t.Parallel()
		result := LineTrace(1 << 20)
		require.Empty(t, result)
	})

	t.Run("resolves own frame on skip=0", func(t *testing.T) {
		t.Parallel()
		result := LineTrace(0)
		require.Contains(t, result, "stacktrace.go:")
		require.Contains(t, result, "LineTrace")
	})

	t.Run("strips generic instantiation suffix", func(t *testing.T) {
		t.Parallel()
		result := callFromGeneric[int]()
		// must contain the bare function name, not "]" from "[...]"
		require.Contains(t, result, "callFromGeneric")
		require.NotContains(t, result, "]")
	})
}

func TestRedactStackPaths(t *testing.T) {
	t.Parallel()

	t.Run("shortens frame paths and drops the absolute prefix", func(t *testing.T) {
		t.Parallel()

		// a real dump: a goroutine header, func(...) lines (not tab-indented), and
		// tab-indented frame path lines with absolute build-host paths and +0x offsets.
		stack := StackTrace()
		redacted := RedactStackPaths(stack)

		// the goroutine header must survive untouched.
		require.True(t, strings.HasPrefix(redacted, "goroutine "),
			"the goroutine header must be preserved")

		// no tab-indented frame line may retain a leading absolute path segment.
		for _, ln := range strings.Split(redacted, "\n") {
			if !strings.HasPrefix(ln, "\t") {
				continue
			}
			body := strings.TrimPrefix(ln, "\t")
			require.False(t, strings.HasPrefix(body, "/"),
				"no frame path may keep an absolute prefix, got: %q", ln)
		}

		// the caller frame still resolves to <parent>/<base>:<line> with its +0x offset.
		require.Regexp(t, `internal/stacktrace_test\.go:\d+ \+0x`, redacted,
			"the redacted stack must keep <parent>/<base>:<line> +0x.. frames")
	})

	t.Run("leaves non-frame lines untouched", func(t *testing.T) {
		t.Parallel()

		// only tab-indented lines are frame paths; the header and func lines must pass through.
		in := "goroutine 1 [running]:\nmain.f(0x1)\n\t/home/ci/proj/main.go:10 +0x2a"
		out := RedactStackPaths(in)
		lines := strings.Split(out, "\n")
		require.Equal(t, "goroutine 1 [running]:", lines[0])
		require.Equal(t, "main.f(0x1)", lines[1])
		require.Equal(t, "\tproj/main.go:10 +0x2a", lines[2],
			"the absolute frame path must collapse to <parent>/<base>")
	})

	t.Run("shortens a build path containing a space without leaking it", func(t *testing.T) {
		t.Parallel()

		// a build path with a space in it (e.g. a macOS home dir "John Smith"): splitting
		// on the FIRST space would truncate mid-path and leak the full absolute prefix.
		in := "\t/Users/John Smith/go/proj/main.go:10 +0x1a"
		out := RedactStackPaths(in)
		require.Contains(t, out, "proj/main.go:10",
			"the frame must collapse to <parent>/<base>:<line>")
		require.NotContains(t, out, "/Users/John Smith",
			"the absolute build-host prefix must not leak")
		require.Equal(t, "\tproj/main.go:10 +0x1a", out,
			"the +0x offset must be preserved and split from the right")
	})
}
