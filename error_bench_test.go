package loginjector

import (
	"fmt"
	"testing"
)

// benchTraceErrSink prevents the compiler from eliding the benchmarked constructor
// calls via dead-code elimination.
var benchTraceErrSink error

// newStackTraceErrorAtDepth builds a StackTraceError from depth frames deep so the
// benchmark exercises a realistic call depth. //go:noinline keeps the recursion from
// collapsing, so the captured stack actually grows with depth.
//
//go:noinline
func newStackTraceErrorAtDepth(depth int) error {
	if depth > 0 {
		return newStackTraceErrorAtDepth(depth - 1)
	}
	return NewStackTraceError()
}

// BenchmarkNewStackTraceError tracks the EXPENSIVE public constructor end-to-end: it
// captures the full goroutine stack (whose cost scales with call depth) plus the
// process-cached runtime descriptor. It is depth-parameterised (0/10/30) because the
// full-stack cost grows with depth — a regression in internal.StackTrace's buffer
// strategy surfaces here as a consumer would feel it. This is the panic-recovery-only
// constructor; the point of the benchmark is to keep it from silently getting slower.
func BenchmarkNewStackTraceError(b *testing.B) {
	for _, depth := range []int{0, 10, 30} {
		b.Run(fmt.Sprintf("depth=%d", depth), func(b *testing.B) {
			b.ReportAllocs()
			var e error
			for range b.N {
				e = newStackTraceErrorAtDepth(depth)
			}
			benchTraceErrSink = e
		})
	}
}

// BenchmarkNewTraceError tracks the CHEAP public constructor: a single call frame via
// runtime.Caller. It is depth-independent — one frame at a fixed skip regardless of total
// stack depth — so a flat benchmark suffices. This is the constructor for ubiquitous
// per-error context; its ratio against BenchmarkNewStackTraceError is what justifies the
// "LineTrace everywhere, StackTrace only on panic recovery" discipline.
func BenchmarkNewTraceError(b *testing.B) {
	b.ReportAllocs()
	var e error
	for range b.N {
		e = NewTraceError()
	}
	benchTraceErrSink = e
}
