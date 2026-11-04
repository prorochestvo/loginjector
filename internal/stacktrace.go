package internal

import (
	"path"
	"runtime"
	"strconv"
	"strings"
)

// StackTrace returns the full, trimmed goroutine stack dump for the calling
// goroutine: the goroutine header, every call frame, argument words, and the
// goroutine state. This is the EXPENSIVE trace path — reserve it for places
// where you must investigate deeply, such as panic recovery. It is a cold,
// failure-path call, not something to attach to every error; use LineTrace for
// ubiquitous per-error context.
//
// The output is byte-identical to strings.TrimSpace(string(debug.Stack())): it
// calls runtime.Stack directly with an 8 KiB initial buffer, doubling only if a
// deeper stack does not fit, which avoids debug.Stack's 1 KiB-and-double
// repeated runtime.Stack re-walks on production-depth stacks. The doubling is
// capped at 4 MiB so a misbehaving runtime.Stack cannot grow the buffer without
// bound.
//
// The returned string contains absolute build-host file paths; do not forward
// it to untrusted clients.
func StackTrace() string {
	// bufferCap bounds the grow-and-retry loop at 4 MiB (~10 doublings from the 8 KiB
	// start) — a backstop against a runaway loop, not a real limit on stack size:
	// production stack dumps never approach it.
	const bufferCap = 4 << 20

	buf := make([]byte, 8<<10)
	for {
		n := runtime.Stack(buf, false)
		// Return when the dump fits (n < len), or when the buffer reaches the cap. The
		// cap bounds the loop: runtime.Stack signals "did not fit" only by returning
		// n == len(buf), so a misbehaving runtime.Stack that always reported a full
		// buffer would otherwise double until it exhausts memory. At the cap the dump is
		// returned best-effort (possibly truncated) — which never happens for a real
		// stack, whose dump is kilobytes to low megabytes, far below the cap.
		if n < len(buf) || len(buf) >= bufferCap {
			return strings.TrimSpace(string(buf[:n]))
		}
		buf = make([]byte, 2*len(buf))
	}
}

// LineTrace is the CHEAP trace path: a single call frame (file:line + method)
// resolved via runtime.Caller in roughly half a microsecond, cheap enough to
// attach per-error context ubiquitously. Use StackTrace when you need the full
// multi-frame goroutine dump instead.
//
// It returns a single-line string of the form "file.go:line (method)"
// identifying the call frame skip levels above LineTrace itself. skip=0 is the
// LineTrace frame; skip=1 is its immediate caller; skip=2 is that caller's
// caller, and so on. Returns "" when runtime.Caller reports ok == false.
//
// Note: //go:noinline is deliberately absent. LineTrace exceeds the inline
// budget by an order of magnitude, and runtime.Caller correctly handles
// inlined call sites since Go 1.12, so the skip count remains stable even
// if a caller is inlined.
func LineTrace(skip int) string {
	pc, file, line, ok := runtime.Caller(skip)
	if !ok {
		return ""
	}

	// shorten the absolute file path to "<parent>/<base>" so the output matches
	// the old debug.Stack parser: stdlib frames become "testing/testing.go",
	// module-root files get their immediate parent directory prefix.
	shortFile := shortenFramePath(file)

	method := "unknown"
	if fn := runtime.FuncForPC(pc); fn != nil {
		name := fn.Name()
		// strip generic instantiation shape suffix appended by the compiler to
		// top-level generic functions ("pkg.Foo[...]"). It must be removed before
		// the last-dot split because the brackets contain dots. Methods on generic
		// types ("pkg.(*T[...]).Method") are safe — their "[...]" is not at end-of-string.
		name = strings.TrimSuffix(name, "[...]")
		// strip the package path prefix: the last dot separates package from function.
		if i := strings.LastIndex(name, "."); i >= 0 && i+1 < len(name) {
			name = name[i+1:]
		}
		if name != "" {
			method = name
		}
	}

	return shortFile + ":" + strconv.Itoa(line) + " (" + method + ")"
}

// shortenFramePath reduces an absolute source path to "<parent>/<base>" — e.g.
// "/home/ci/proj/main.go" becomes "proj/main.go" and a stdlib frame becomes
// "testing/testing.go". It is the single shortening rule shared by LineTrace and
// RedactStackPaths so the two paths cannot drift.
func shortenFramePath(file string) string {
	base := path.Base(file)
	parent := path.Base(path.Dir(file))
	if parent != "" && parent != "." && parent != "/" {
		return parent + "/" + base
	}
	return base
}

// RedactStackPaths rewrites each tab-indented frame line of a StackTrace() dump
// ("\t<abs-path>:<line> +0x..") so <abs-path> is shortened via shortenFramePath,
// stripping build-host absolute path prefixes. The goroutine header, func(...) lines,
// argument words, line numbers, and frame ordering are all preserved untouched; only
// the leading directory prefix of each tab-indented frame path is dropped.
func RedactStackPaths(stack string) string {
	lines := strings.Split(stack, "\n")
	for i, ln := range lines {
		if !strings.HasPrefix(ln, "\t") {
			continue // only frame path lines are tab-indented
		}
		body := ln[1:]
		suffix := ""
		// the optional offset is the trailing token starting with '+'; split from the
		// RIGHT so a build path containing a space is not truncated mid-path (which would
		// strip the ":line" and leave the full absolute path unrewritten — a path leak).
		if sp := strings.LastIndexByte(body, ' '); sp >= 0 && sp+1 < len(body) && body[sp+1] == '+' {
			body, suffix = body[:sp], body[sp:]
		}
		if colon := strings.LastIndexByte(body, ':'); colon >= 0 { // "<path>:<line>"
			lines[i] = "\t" + shortenFramePath(body[:colon]) + body[colon:] + suffix
		}
	}
	return strings.Join(lines, "\n")
}
