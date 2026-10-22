package httptap

import (
	"bufio"
	"net"
	"net/http"
)

// tapWriter is implemented by interceptor types that need to record when a
// Hijack has happened so callers can suppress post-handler writes that assume
// the ResponseWriter is still live (Flush in the payload handler; no-op in the
// access handler — the log line still emits). It also provides a beforeFlush
// hook that runs before the real http.Flusher.Flush().
type tapWriter interface {
	http.ResponseWriter
	markHijacked()
	beforeFlush()
}

// markHijacked satisfies tapWriter for *interceptor by setting the hijacked
// flag that the post-handler flush guard in NewPayloadHandlerWithOptions reads.
func (i *interceptor) markHijacked() { i.hijacked = true }

// beforeFlush marks the final status as committed: the real Flush sends the
// recorded status, or an implicit 200 when none was written.
func (i *interceptor) beforeFlush() {
	if i.hijacked {
		return
	}
	i.wroteHeader = true
}

// flusherRW wraps a tapWriter base and additionally satisfies http.Flusher.
type flusherRW struct {
	tapWriter
	flusher http.Flusher
}

// Flush calls beforeFlush on the base, then delegates to the underlying http.Flusher.
func (v *flusherRW) Flush() {
	v.beforeFlush()
	v.flusher.Flush()
}

// hijackerRW wraps a tapWriter base and additionally satisfies http.Hijacker.
type hijackerRW struct {
	tapWriter
	hijacker http.Hijacker
}

// Hijack delegates to the underlying http.Hijacker and marks the base as
// hijacked so the post-handler flush guard in NewPayloadHandlerWithOptions is
// skipped.
func (v *hijackerRW) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	v.markHijacked()
	return v.hijacker.Hijack()
}

// pusherRW wraps a tapWriter base and additionally satisfies http.Pusher.
type pusherRW struct {
	tapWriter
	pusher http.Pusher
}

// Push delegates to the underlying http.Pusher.
func (v *pusherRW) Push(target string, opts *http.PushOptions) error {
	return v.pusher.Push(target, opts)
}

// flusherHijackerRW wraps a tapWriter base and satisfies both http.Flusher
// and http.Hijacker.
type flusherHijackerRW struct {
	tapWriter
	flusher  http.Flusher
	hijacker http.Hijacker
}

func (v *flusherHijackerRW) Flush() {
	v.beforeFlush()
	v.flusher.Flush()
}
func (v *flusherHijackerRW) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	v.markHijacked()
	return v.hijacker.Hijack()
}

// flusherPusherRW wraps a tapWriter base and satisfies both http.Flusher and
// http.Pusher.
type flusherPusherRW struct {
	tapWriter
	flusher http.Flusher
	pusher  http.Pusher
}

func (v *flusherPusherRW) Flush() {
	v.beforeFlush()
	v.flusher.Flush()
}
func (v *flusherPusherRW) Push(target string, opts *http.PushOptions) error {
	return v.pusher.Push(target, opts)
}

// hijackerPusherRW wraps a tapWriter base and satisfies both http.Hijacker
// and http.Pusher.
type hijackerPusherRW struct {
	tapWriter
	hijacker http.Hijacker
	pusher   http.Pusher
}

func (v *hijackerPusherRW) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	v.markHijacked()
	return v.hijacker.Hijack()
}
func (v *hijackerPusherRW) Push(target string, opts *http.PushOptions) error {
	return v.pusher.Push(target, opts)
}

// allThreeRW wraps a tapWriter base and satisfies http.Flusher,
// http.Hijacker, and http.Pusher simultaneously.
type allThreeRW struct {
	tapWriter
	flusher  http.Flusher
	hijacker http.Hijacker
	pusher   http.Pusher
}

func (v *allThreeRW) Flush() {
	v.beforeFlush()
	v.flusher.Flush()
}
func (v *allThreeRW) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	v.markHijacked()
	return v.hijacker.Hijack()
}
func (v *allThreeRW) Push(target string, opts *http.PushOptions) error {
	return v.pusher.Push(target, opts)
}

// compile-time assertions that every *RW type implements the interfaces it
// advertises. These live in the production file so go build (without -test)
// catches a broken implementation immediately.
var _ http.ResponseWriter = (*flusherRW)(nil)
var _ http.Flusher = (*flusherRW)(nil)
var _ http.ResponseWriter = (*hijackerRW)(nil)
var _ http.Hijacker = (*hijackerRW)(nil)
var _ http.ResponseWriter = (*pusherRW)(nil)
var _ http.Pusher = (*pusherRW)(nil)
var _ http.ResponseWriter = (*flusherHijackerRW)(nil)
var _ http.Flusher = (*flusherHijackerRW)(nil)
var _ http.Hijacker = (*flusherHijackerRW)(nil)
var _ http.ResponseWriter = (*flusherPusherRW)(nil)
var _ http.Flusher = (*flusherPusherRW)(nil)
var _ http.Pusher = (*flusherPusherRW)(nil)
var _ http.ResponseWriter = (*hijackerPusherRW)(nil)
var _ http.Hijacker = (*hijackerPusherRW)(nil)
var _ http.Pusher = (*hijackerPusherRW)(nil)
var _ http.ResponseWriter = (*allThreeRW)(nil)
var _ http.Flusher = (*allThreeRW)(nil)
var _ http.Hijacker = (*allThreeRW)(nil)
var _ http.Pusher = (*allThreeRW)(nil)

// wrapWithOptionalInterfaces wraps base in the minimal composite type that
// advertises exactly the optional interfaces that w implements. When w
// implements none of them, base itself is returned directly — zero allocation on
// the fast path.
//
// The combinatorial switch covers all eight cases for the three boolean flags
// (Flusher, Hijacker, Pusher), following the httpsnoop pattern. Exotic
// interfaces (io.ReaderFrom, http.CloseNotifier) are not bridged; they can be
// added additively in a future change.
func wrapWithOptionalInterfaces(base tapWriter, w http.ResponseWriter) http.ResponseWriter {
	f, hasF := w.(http.Flusher)
	h, hasH := w.(http.Hijacker)
	p, hasP := w.(http.Pusher)

	switch {
	case hasF && hasH && hasP:
		return &allThreeRW{tapWriter: base, flusher: f, hijacker: h, pusher: p}
	case hasF && hasH:
		return &flusherHijackerRW{tapWriter: base, flusher: f, hijacker: h}
	case hasF && hasP:
		return &flusherPusherRW{tapWriter: base, flusher: f, pusher: p}
	case hasH && hasP:
		return &hijackerPusherRW{tapWriter: base, hijacker: h, pusher: p}
	case hasF:
		return &flusherRW{tapWriter: base, flusher: f}
	case hasH:
		return &hijackerRW{tapWriter: base, hijacker: h}
	case hasP:
		return &pusherRW{tapWriter: base, pusher: p}
	default:
		return base
	}
}

// isInformational reports whether code is an HTTP informational status
// (100-199) excluding 101 Switching Protocols, which is final in net/http.
func isInformational(code int) bool {
	return code >= 100 && code <= 199 && code != http.StatusSwitchingProtocols
}
