package loginjector

import (
	"io"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
)

// tokenSeq backs uniqueToken. It is package-global so tokens stay distinct across
// every test in this package, including inside goroutines.
var tokenSeq atomic.Uint64

// uniqueToken returns a short, process-unique string. Tests use it wherever they
// only need a distinct marker to search for in captured log output; it replaces
// the previous uuid-based random tokens without pulling in an external dependency.
func uniqueToken() string {
	return "tok-" + strconv.FormatUint(tokenSeq.Add(1), 10)
}

// captureStdout redirects os.Stdout to a pipe for the duration of fn and returns
// everything written to it. The caller must NOT run in parallel: os.Stdout is
// process-global. fn should construct any handler that binds os.Stdout inside the
// callback so it captures the redirected pipe.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	orig := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stdout = w
	defer func() { os.Stdout = orig }()

	done := make(chan string, 1)
	go func() {
		b, _ := io.ReadAll(r)
		done <- string(b)
	}()

	fn()

	_ = w.Close()
	out := <-done
	_ = r.Close()
	return out
}

// captureStderr redirects os.Stderr to a pipe for the duration of fn and returns
// everything written to it. The caller must NOT run in parallel: os.Stderr is
// process-global.
func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	orig := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stderr = w
	defer func() { os.Stderr = orig }()

	done := make(chan string, 1)
	go func() {
		b, _ := io.ReadAll(r)
		done <- string(b)
	}()

	fn()

	_ = w.Close()
	out := <-done
	_ = r.Close()
	return out
}
