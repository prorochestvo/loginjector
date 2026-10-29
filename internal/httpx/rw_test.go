package httpx

import (
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
