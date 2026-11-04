// Package sink contains guarded writers shared by the root logger and extensions.
package sink

import "sync"

// Func is a mutex-guarded writer over a write function; concurrent Write calls run
// one at a time.
type Func struct {
	m     sync.Mutex
	write func(p []byte) (int, error)
}

// NewFunc returns a Func that serialises calls to write.
func NewFunc(write func(p []byte) (int, error)) *Func { return &Func{write: write} }

// Write calls the write function while holding the writer's mutex.
func (f *Func) Write(p []byte) (int, error) {
	f.m.Lock()
	defer f.m.Unlock()
	return f.write(p)
}
