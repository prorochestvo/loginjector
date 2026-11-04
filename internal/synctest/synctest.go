// Package synctest provides a mutex-guarded bytes.Buffer for the loginjector test
// suites. It is module-internal test scaffolding: both the root package's and the
// httptap package's test files may import it to obtain a buffer that is safe under
// concurrent Write/Read from many goroutines, without that type appearing in the
// shipped public API.
package synctest

import (
	"bytes"
	"io"
	"sync"
)

// compile-time assertion: SafeBuffer must satisfy io.ReadWriter.
var _ io.ReadWriter = (*SafeBuffer)(nil)

// SafeBuffer is a minimal, mutex-guarded wrapper around bytes.Buffer. The zero value
// is ready to use. Unlike bytes.Buffer, all methods on SafeBuffer are safe to call
// concurrently from multiple goroutines.
//
// The bytes.Buffer field is a named field (not embedded) to prevent accidental use of
// the unsynchronized bytes.Buffer method set, which would bypass the mutex and
// reintroduce data races.
type SafeBuffer struct {
	m sync.Mutex
	b bytes.Buffer
}

// NewBuffer creates a new SafeBuffer seeded with the contents of buf, mirroring
// bytes.NewBuffer. The caller should not use buf after this call.
func NewBuffer(buf []byte) *SafeBuffer {
	return &SafeBuffer{b: *bytes.NewBuffer(buf)}
}

// NewBufferString creates a new SafeBuffer seeded with the contents of s, mirroring
// bytes.NewBufferString.
func NewBufferString(s string) *SafeBuffer {
	return &SafeBuffer{b: *bytes.NewBufferString(s)}
}

// Write appends the contents of p to the buffer, growing the buffer as needed.
// It implements io.Writer.
func (sb *SafeBuffer) Write(p []byte) (int, error) {
	sb.m.Lock()
	defer sb.m.Unlock()
	return sb.b.Write(p)
}

// Read reads the next len(p) bytes from the buffer or until the buffer is drained.
// It implements io.Reader and preserves the io.EOF semantics of bytes.Buffer.
func (sb *SafeBuffer) Read(p []byte) (int, error) {
	sb.m.Lock()
	defer sb.m.Unlock()
	return sb.b.Read(p)
}

// String returns the unread portion of the buffer as a string.
func (sb *SafeBuffer) String() string {
	sb.m.Lock()
	defer sb.m.Unlock()
	return sb.b.String()
}

// Bytes returns a copy of the unread portion of the buffer as a byte slice.
// A copy is returned rather than the live underlying slice to prevent data races:
// bytes.Buffer.Bytes() aliases internal storage that a concurrent Write may mutate
// or reallocate, which would corrupt any slice the caller retained. This is an
// intentional divergence from bytes.Buffer semantics.
func (sb *SafeBuffer) Bytes() []byte {
	sb.m.Lock()
	defer sb.m.Unlock()
	return append([]byte(nil), sb.b.Bytes()...)
}
