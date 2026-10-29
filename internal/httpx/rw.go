// Package httpx provides response-writer composition and path escaping shared by
// httptap and extensions.
package httpx

import (
	"bufio"
	"net"
	"net/http"
)

// Tap records a wrapper's own bookkeeping when the response is hijacked or flushed.
// The wrapped handler must not call MarkHijacked or BeforeFlush. Calling MarkHijacked
// affects the logged status in payload and access handlers, suppresses the payload
// handler's trailing flush, skips the recover wrapper's 500 fallback after a panic,
// and prevents its buffered response from being delivered on a clean return. Calling
// BeforeFlush commits the recover wrapper's buffered response; a later panic is
// re-panicked as http.ErrAbortHandler instead of receiving a 500.
type Tap interface {
	http.ResponseWriter
	MarkHijacked()
	BeforeFlush()
}

// WrapWithOptionalInterfaces wraps base in the minimal composite type that
// advertises exactly the optional interfaces that w implements. When w
// implements none of them, base itself is returned directly — zero allocation on
// the fast path.
//
// The combinatorial switch covers all eight cases for the three boolean flags
// (Flusher, Hijacker, Pusher), following the httpsnoop pattern. Exotic
// interfaces (io.ReaderFrom, http.CloseNotifier) are not bridged; they can be
// added additively in a future change.
func WrapWithOptionalInterfaces(base Tap, w http.ResponseWriter) http.ResponseWriter {
	f, hasF := w.(http.Flusher)
	h, hasH := w.(http.Hijacker)
	p, hasP := w.(http.Pusher)

	switch {
	case hasF && hasH && hasP:
		return &allThreeRW{Tap: base, flusher: f, hijacker: h, pusher: p}
	case hasF && hasH:
		return &flusherHijackerRW{Tap: base, flusher: f, hijacker: h}
	case hasF && hasP:
		return &flusherPusherRW{Tap: base, flusher: f, pusher: p}
	case hasH && hasP:
		return &hijackerPusherRW{Tap: base, hijacker: h, pusher: p}
	case hasF:
		return &flusherRW{Tap: base, flusher: f}
	case hasH:
		return &hijackerRW{Tap: base, hijacker: h}
	case hasP:
		return &pusherRW{Tap: base, pusher: p}
	default:
		return base
	}
}

// IsInformational reports whether code is an HTTP informational status
// (100-199) excluding 101 Switching Protocols, which is final in net/http.
func IsInformational(code int) bool {
	return code >= 100 && code <= 199 && code != http.StatusSwitchingProtocols
}

// flusherRW wraps a Tap base and additionally satisfies http.Flusher.
type flusherRW struct {
	Tap
	flusher http.Flusher
}

// Flush calls BeforeFlush on the base, then delegates to the underlying http.Flusher.
func (v *flusherRW) Flush() {
	v.BeforeFlush()
	v.flusher.Flush()
}

// hijackerRW wraps a Tap base and additionally satisfies http.Hijacker.
type hijackerRW struct {
	Tap
	hijacker http.Hijacker
}

// Hijack delegates to the underlying http.Hijacker and marks the base as
// hijacked so the post-handler flush guard in NewPayloadHandlerWithOptions is
// skipped.
func (v *hijackerRW) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	v.MarkHijacked()
	return v.hijacker.Hijack()
}

// pusherRW wraps a Tap base and additionally satisfies http.Pusher.
type pusherRW struct {
	Tap
	pusher http.Pusher
}

// Push delegates to the underlying http.Pusher.
func (v *pusherRW) Push(target string, opts *http.PushOptions) error {
	return v.pusher.Push(target, opts)
}

// flusherHijackerRW wraps a Tap base and satisfies both http.Flusher
// and http.Hijacker.
type flusherHijackerRW struct {
	Tap
	flusher  http.Flusher
	hijacker http.Hijacker
}

func (v *flusherHijackerRW) Flush() {
	v.BeforeFlush()
	v.flusher.Flush()
}
func (v *flusherHijackerRW) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	v.MarkHijacked()
	return v.hijacker.Hijack()
}

// flusherPusherRW wraps a Tap base and satisfies both http.Flusher and
// http.Pusher.
type flusherPusherRW struct {
	Tap
	flusher http.Flusher
	pusher  http.Pusher
}

func (v *flusherPusherRW) Flush() {
	v.BeforeFlush()
	v.flusher.Flush()
}
func (v *flusherPusherRW) Push(target string, opts *http.PushOptions) error {
	return v.pusher.Push(target, opts)
}

// hijackerPusherRW wraps a Tap base and satisfies both http.Hijacker
// and http.Pusher.
type hijackerPusherRW struct {
	Tap
	hijacker http.Hijacker
	pusher   http.Pusher
}

func (v *hijackerPusherRW) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	v.MarkHijacked()
	return v.hijacker.Hijack()
}
func (v *hijackerPusherRW) Push(target string, opts *http.PushOptions) error {
	return v.pusher.Push(target, opts)
}

// allThreeRW wraps a Tap base and satisfies http.Flusher,
// http.Hijacker, and http.Pusher simultaneously.
type allThreeRW struct {
	Tap
	flusher  http.Flusher
	hijacker http.Hijacker
	pusher   http.Pusher
}

func (v *allThreeRW) Flush() {
	v.BeforeFlush()
	v.flusher.Flush()
}
func (v *allThreeRW) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	v.MarkHijacked()
	return v.hijacker.Hijack()
}
func (v *allThreeRW) Push(target string, opts *http.PushOptions) error {
	return v.pusher.Push(target, opts)
}
