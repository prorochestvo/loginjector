package httpx

import (
	"bufio"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// Compile-time checks that each concrete wrapper satisfies its claimed interfaces.
var _ http.ResponseWriter = &flusherRW{}
var _ http.Flusher = &flusherRW{}
var _ http.ResponseWriter = &hijackerRW{}
var _ http.Hijacker = &hijackerRW{}
var _ http.ResponseWriter = &pusherRW{}
var _ http.Pusher = &pusherRW{}
var _ http.ResponseWriter = &flusherHijackerRW{}
var _ http.Flusher = &flusherHijackerRW{}
var _ http.Hijacker = &flusherHijackerRW{}
var _ http.ResponseWriter = &flusherPusherRW{}
var _ http.Flusher = &flusherPusherRW{}
var _ http.Pusher = &flusherPusherRW{}
var _ http.ResponseWriter = &hijackerPusherRW{}
var _ http.Hijacker = &hijackerPusherRW{}
var _ http.Pusher = &hijackerPusherRW{}
var _ http.ResponseWriter = &allThreeRW{}
var _ http.Flusher = &allThreeRW{}
var _ http.Hijacker = &allThreeRW{}
var _ http.Pusher = &allThreeRW{}

func TestWrapWithOptionalInterfaces(t *testing.T) {
	t.Parallel()

	t.Run("none", func(t *testing.T) {
		t.Parallel()
		base := &hookRecordingBase{}
		got := WrapWithOptionalInterfaces(base, &hookRecordingBase{})
		require.Same(t, base, got)
		require.NotImplements(t, (*http.Flusher)(nil), got)
		require.NotImplements(t, (*http.Hijacker)(nil), got)
		require.NotImplements(t, (*http.Pusher)(nil), got)
	})
	t.Run("flusher", func(t *testing.T) {
		t.Parallel()
		got := WrapWithOptionalInterfaces(&hookRecordingBase{}, &flushResponseWriter{hookRecordingBase: &hookRecordingBase{}})
		require.Implements(t, (*http.Flusher)(nil), got)
		require.NotImplements(t, (*http.Hijacker)(nil), got)
		require.NotImplements(t, (*http.Pusher)(nil), got)
	})
	t.Run("hijacker", func(t *testing.T) {
		t.Parallel()
		got := WrapWithOptionalInterfaces(&hookRecordingBase{}, &hijackResponseWriter{&hookRecordingBase{}})
		require.NotImplements(t, (*http.Flusher)(nil), got)
		require.Implements(t, (*http.Hijacker)(nil), got)
		require.NotImplements(t, (*http.Pusher)(nil), got)
	})
	t.Run("pusher", func(t *testing.T) {
		t.Parallel()
		got := WrapWithOptionalInterfaces(&hookRecordingBase{}, &pushResponseWriter{&hookRecordingBase{}})
		require.NotImplements(t, (*http.Flusher)(nil), got)
		require.NotImplements(t, (*http.Hijacker)(nil), got)
		require.Implements(t, (*http.Pusher)(nil), got)
	})
	t.Run("flusher_hijacker", func(t *testing.T) {
		t.Parallel()
		got := WrapWithOptionalInterfaces(&hookRecordingBase{}, &flushHijackResponseWriter{&hookRecordingBase{}})
		require.Implements(t, (*http.Flusher)(nil), got)
		require.Implements(t, (*http.Hijacker)(nil), got)
		require.NotImplements(t, (*http.Pusher)(nil), got)
	})
	t.Run("flusher_pusher", func(t *testing.T) {
		t.Parallel()
		got := WrapWithOptionalInterfaces(&hookRecordingBase{}, &flushPushResponseWriter{&hookRecordingBase{}})
		require.Implements(t, (*http.Flusher)(nil), got)
		require.NotImplements(t, (*http.Hijacker)(nil), got)
		require.Implements(t, (*http.Pusher)(nil), got)
	})
	t.Run("hijacker_pusher", func(t *testing.T) {
		t.Parallel()
		got := WrapWithOptionalInterfaces(&hookRecordingBase{}, &hijackPushResponseWriter{&hookRecordingBase{}})
		require.NotImplements(t, (*http.Flusher)(nil), got)
		require.Implements(t, (*http.Hijacker)(nil), got)
		require.Implements(t, (*http.Pusher)(nil), got)
	})
	t.Run("all", func(t *testing.T) {
		t.Parallel()
		got := WrapWithOptionalInterfaces(&hookRecordingBase{}, &allResponseWriter{&hookRecordingBase{}})
		require.Implements(t, (*http.Flusher)(nil), got)
		require.Implements(t, (*http.Hijacker)(nil), got)
		require.Implements(t, (*http.Pusher)(nil), got)
	})
	t.Run("flush_calls_before_flush_hook", func(t *testing.T) {
		t.Parallel()

		// Test that BeforeFlush is called before the real Flusher.Flush().
		// Use a test stub that records both hook and flush calls in order.
		hookCalls := make([]string, 0)
		var mu sync.Mutex

		// Create a fake base that records BeforeFlush calls.
		fakeBase := &hookRecordingBase{
			onBeforeFlush: func() {
				mu.Lock()
				hookCalls = append(hookCalls, "hook")
				mu.Unlock()
			},
		}

		// Create a flusher wrapper around the base.
		wrapped := &flusherRW{
			Tap: fakeBase,
			flusher: &recordingFlusher{
				onFlush: func() {
					mu.Lock()
					hookCalls = append(hookCalls, "flush")
					mu.Unlock()
				},
			},
		}

		wrapped.Flush()

		require.Equal(t, []string{"hook", "flush"}, hookCalls, "BeforeFlush hook must run before Flush()")
	})
}

func TestIsInformational(t *testing.T) {
	t.Parallel()
	t.Run("100", func(t *testing.T) {
		t.Parallel()
		require.True(t, IsInformational(100))
	})
	t.Run("101", func(t *testing.T) {
		t.Parallel()
		// 101 Switching Protocols is final in net/http, so it is not informational here.
		require.False(t, IsInformational(101))
	})
	t.Run("199", func(t *testing.T) {
		t.Parallel()
		require.True(t, IsInformational(199))
	})
	t.Run("200", func(t *testing.T) {
		t.Parallel()
		require.False(t, IsInformational(200))
	})
	t.Run("99", func(t *testing.T) {
		t.Parallel()
		require.False(t, IsInformational(99))
	})
}

type flushResponseWriter struct {
	*hookRecordingBase
	onFlush func()
}

func (w *flushResponseWriter) Flush() {
	if w.onFlush != nil {
		w.onFlush()
	}
}

type hijackResponseWriter struct{ *hookRecordingBase }

func (*hijackResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) { return nil, nil, nil }

type pushResponseWriter struct{ *hookRecordingBase }

func (*pushResponseWriter) Push(string, *http.PushOptions) error { return nil }

type flushHijackResponseWriter struct{ *hookRecordingBase }

func (*flushHijackResponseWriter) Flush() {}
func (*flushHijackResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return nil, nil, nil
}

type flushPushResponseWriter struct{ *hookRecordingBase }

func (*flushPushResponseWriter) Flush()                               {}
func (*flushPushResponseWriter) Push(string, *http.PushOptions) error { return nil }

type hijackPushResponseWriter struct{ *hookRecordingBase }

func (*hijackPushResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return nil, nil, nil
}
func (*hijackPushResponseWriter) Push(string, *http.PushOptions) error { return nil }

type allResponseWriter struct{ *hookRecordingBase }

func (*allResponseWriter) Flush()                                       {}
func (*allResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) { return nil, nil, nil }
func (*allResponseWriter) Push(string, *http.PushOptions) error         { return nil }

// hookRecordingBase is a test stub that records BeforeFlush calls.
type hookRecordingBase struct {
	onBeforeFlush func()
}

func (h *hookRecordingBase) Header() http.Header         { return httptest.NewRecorder().Header() }
func (h *hookRecordingBase) Write(b []byte) (int, error) { return len(b), nil }
func (h *hookRecordingBase) WriteHeader(_ int)           {}
func (h *hookRecordingBase) MarkHijacked()               {}
func (h *hookRecordingBase) BeforeFlush() {
	if h.onBeforeFlush != nil {
		h.onBeforeFlush()
	}
}

var _ Tap = (*hookRecordingBase)(nil)

type recordingFlusher struct {
	onFlush func()
}

func (r *recordingFlusher) Flush() {
	if r.onFlush != nil {
		r.onFlush()
	}
}
