package internal

import (
	"strconv"
	"testing"
)

// lineTraceSink prevents the compiler from eliding benchmark calls via dead-code elimination.
var lineTraceSink string

// stackTraceAtDepth recurses depth frames deep and then captures a stack trace,
// so BenchmarkStackTrace can measure a production-depth stack rather than the
// shallow test-runner stack. The //go:noinline keeps every recursion level a
// real frame in the dump.
//
//go:noinline
func stackTraceAtDepth(depth int) string {
	if depth > 0 {
		return stackTraceAtDepth(depth - 1)
	}
	return StackTrace()
}

// lineTraceFromHelper is a non-inlineable indirection so that BenchmarkLineTrace
// exercises skip=2 — the same depth as production callers of LineTrace.
//
//go:noinline
func lineTraceFromHelper() string { return LineTrace(2) }

// BenchmarkLineTrace measures the cost of capturing one call-site line via LineTrace.
// It calls through lineTraceFromHelper so that skip=2 matches the production call depth.
func BenchmarkLineTrace(b *testing.B) {
	b.ReportAllocs()
	var s string
	for range b.N {
		s = lineTraceFromHelper()
	}
	lineTraceSink = s
}

// BenchmarkStackTrace measures the cost of the full goroutine dump captured by
// StackTrace. The depth sub-benchmarks recurse to production-like stack depths,
// where the 8 KiB initial buffer avoids debug.Stack's 1 KiB-and-double re-walks;
// on a shallow stack the two are comparable, so the win shows only with depth.
func BenchmarkStackTrace(b *testing.B) {
	for _, depth := range []int{0, 10, 30} {
		b.Run("depth-"+strconv.Itoa(depth), func(b *testing.B) {
			b.ReportAllocs()
			var s string
			for range b.N {
				s = stackTraceAtDepth(depth)
			}
			lineTraceSink = s
		})
	}
}
