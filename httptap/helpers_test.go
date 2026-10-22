package httptap

import (
	"bufio"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"

	loginjector "github.com/prorochestvo/loginjector"
)

// logLevelInfo is a package-wide test fixture used across httptap tests.
const logLevelInfo loginjector.LogLevel = 1

// tokenSeq backs uniqueToken.
var tokenSeq atomic.Uint64

// uniqueToken returns a short, process-unique string. Tests use it wherever they
// only need a distinct marker to search for in captured log output; it replaces
// the previous uuid-based random tokens without pulling in an external dependency.
func uniqueToken() string {
	return "tok-" + strconv.FormatUint(tokenSeq.Add(1), 10)
}

// plainResponseWriter implements only http.ResponseWriter with no optional
// interfaces. It intentionally does NOT embed *httptest.ResponseRecorder (which
// is itself an http.Flusher), so type-asserting the wrapped interceptor to
// http.Flusher returns false.
type plainResponseWriter struct {
	rec *httptest.ResponseRecorder
}

func (p *plainResponseWriter) Header() http.Header         { return p.rec.Header() }
func (p *plainResponseWriter) Write(b []byte) (int, error) { return p.rec.Write(b) }
func (p *plainResponseWriter) WriteHeader(code int)        { p.rec.WriteHeader(code) }

var _ http.ResponseWriter = &plainResponseWriter{} // compile-time check

type fakeFlusherWriter struct {
	*httptest.ResponseRecorder
	onFlush func()
}

func (f *fakeFlusherWriter) Flush() { f.onFlush() }

var _ http.ResponseWriter = &fakeFlusherWriter{} // compile-time check
var _ http.Flusher = &fakeFlusherWriter{}        // compile-time check

type fakeHijackerWriter struct {
	*httptest.ResponseRecorder
}

func (h *fakeHijackerWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	// return a pair of connected net.Conn via net.Pipe for a realistic response.
	server, client := net.Pipe()
	_ = client.Close()
	br := bufio.NewReadWriter(bufio.NewReader(server), bufio.NewWriter(server))
	return server, br, nil
}

var _ http.ResponseWriter = &fakeHijackerWriter{} // compile-time check
var _ http.Hijacker = &fakeHijackerWriter{}       // compile-time check

type fakePusherWriter struct {
	*httptest.ResponseRecorder
	onPush func(string)
}

func (p *fakePusherWriter) Push(target string, _ *http.PushOptions) error {
	p.onPush(target)
	return nil
}

var _ http.ResponseWriter = &fakePusherWriter{} // compile-time check
var _ http.Pusher = &fakePusherWriter{}         // compile-time check

// fakeComboFlusherHijacker implements http.ResponseWriter + http.Flusher + http.Hijacker.
type fakeComboFlusherHijacker struct {
	rec     *httptest.ResponseRecorder
	onFlush func()
}

func (f *fakeComboFlusherHijacker) Header() http.Header         { return f.rec.Header() }
func (f *fakeComboFlusherHijacker) Write(b []byte) (int, error) { return f.rec.Write(b) }
func (f *fakeComboFlusherHijacker) WriteHeader(code int)        { f.rec.WriteHeader(code) }
func (f *fakeComboFlusherHijacker) Flush()                      { f.onFlush() }
func (f *fakeComboFlusherHijacker) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	server, client := net.Pipe()
	_ = client.Close()
	br := bufio.NewReadWriter(bufio.NewReader(server), bufio.NewWriter(server))
	return server, br, nil
}

var _ http.ResponseWriter = &fakeComboFlusherHijacker{} // compile-time check
var _ http.Flusher = &fakeComboFlusherHijacker{}        // compile-time check
var _ http.Hijacker = &fakeComboFlusherHijacker{}       // compile-time check

// fakeComboFlusherPusher implements http.ResponseWriter + http.Flusher + http.Pusher.
type fakeComboFlusherPusher struct {
	rec    *httptest.ResponseRecorder
	onPush func(string)
}

func (f *fakeComboFlusherPusher) Header() http.Header         { return f.rec.Header() }
func (f *fakeComboFlusherPusher) Write(b []byte) (int, error) { return f.rec.Write(b) }
func (f *fakeComboFlusherPusher) WriteHeader(code int)        { f.rec.WriteHeader(code) }
func (f *fakeComboFlusherPusher) Flush()                      {}
func (f *fakeComboFlusherPusher) Push(target string, _ *http.PushOptions) error {
	f.onPush(target)
	return nil
}

var _ http.ResponseWriter = &fakeComboFlusherPusher{} // compile-time check
var _ http.Flusher = &fakeComboFlusherPusher{}        // compile-time check
var _ http.Pusher = &fakeComboFlusherPusher{}         // compile-time check

// fakeComboHijackerPusher implements http.ResponseWriter + http.Hijacker + http.Pusher.
type fakeComboHijackerPusher struct {
	rec    *httptest.ResponseRecorder
	onPush func(string)
}

func (f *fakeComboHijackerPusher) Header() http.Header         { return f.rec.Header() }
func (f *fakeComboHijackerPusher) Write(b []byte) (int, error) { return f.rec.Write(b) }
func (f *fakeComboHijackerPusher) WriteHeader(code int)        { f.rec.WriteHeader(code) }
func (f *fakeComboHijackerPusher) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	server, client := net.Pipe()
	_ = client.Close()
	br := bufio.NewReadWriter(bufio.NewReader(server), bufio.NewWriter(server))
	return server, br, nil
}
func (f *fakeComboHijackerPusher) Push(target string, _ *http.PushOptions) error {
	f.onPush(target)
	return nil
}

var _ http.ResponseWriter = &fakeComboHijackerPusher{} // compile-time check
var _ http.Hijacker = &fakeComboHijackerPusher{}       // compile-time check
var _ http.Pusher = &fakeComboHijackerPusher{}         // compile-time check

// fakeComboAllThree implements http.ResponseWriter + http.Flusher + http.Hijacker + http.Pusher.
type fakeComboAllThree struct {
	rec     *httptest.ResponseRecorder
	onFlush func()
	onPush  func(string)
}

func (f *fakeComboAllThree) Header() http.Header         { return f.rec.Header() }
func (f *fakeComboAllThree) Write(b []byte) (int, error) { return f.rec.Write(b) }
func (f *fakeComboAllThree) WriteHeader(code int)        { f.rec.WriteHeader(code) }
func (f *fakeComboAllThree) Flush()                      { f.onFlush() }
func (f *fakeComboAllThree) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	server, client := net.Pipe()
	_ = client.Close()
	br := bufio.NewReadWriter(bufio.NewReader(server), bufio.NewWriter(server))
	return server, br, nil
}
func (f *fakeComboAllThree) Push(target string, _ *http.PushOptions) error {
	f.onPush(target)
	return nil
}

var _ http.ResponseWriter = &fakeComboAllThree{} // compile-time check
var _ http.Flusher = &fakeComboAllThree{}        // compile-time check
var _ http.Hijacker = &fakeComboAllThree{}       // compile-time check
var _ http.Pusher = &fakeComboAllThree{}         // compile-time check

// safeBuffer is a thread-safe bytes.Buffer for the race-condition test.
type safeBuffer struct {
	mu  sync.Mutex
	buf []byte
}

func (s *safeBuffer) Write(b []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buf = append(s.buf, b...)
	return len(b), nil
}

// lockedWriter is a thread-safe io.Writer used in the race subtest to prevent
// data races on a shared writer from masking handler-level races.
type lockedWriter struct {
	mu sync.Mutex
	w  interface{ Write([]byte) (int, error) }
}

func (l *lockedWriter) Write(b []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(b)
}

// statusLogWriter is a fake http.ResponseWriter + http.Flusher that records
// every WriteHeader call (including 1xx informational), modeling net/http's
// 1xx semantics (informational codes do not commit a final status).
type statusLogWriter struct {
	*httptest.ResponseRecorder
	statusCodes     []int
	headerSnapshots []http.Header
	finalStatus     int
	finalHeaders    http.Header
	body            []byte
	flushed         bool
}

func newStatusLogWriter() *statusLogWriter {
	return &statusLogWriter{
		ResponseRecorder: httptest.NewRecorder(),
		statusCodes:      make([]int, 0),
		headerSnapshots:  make([]http.Header, 0),
	}
}

func (s *statusLogWriter) WriteHeader(code int) {
	s.statusCodes = append(s.statusCodes, code)
	// Record a header snapshot at each WriteHeader call.
	headerSnapshot := s.Header().Clone()
	s.headerSnapshots = append(s.headerSnapshots, headerSnapshot)
	// Track the final status (the last non-1xx code).
	if code < 100 || code > 199 || code == 101 {
		s.finalStatus = code
		s.finalHeaders = headerSnapshot
	}
	// Don't call the recorder's WriteHeader; it treats all codes as final.
	// Instead, just record the code.
}

func (s *statusLogWriter) Write(b []byte) (int, error) {
	s.body = append(s.body, b...)
	return s.ResponseRecorder.Write(b)
}

func (s *statusLogWriter) Flush() {
	s.flushed = true
}

var _ http.ResponseWriter = &statusLogWriter{}
var _ http.Flusher = &statusLogWriter{}

// hookRecordingBase is a test stub that implements tapWriter and records
// beforeFlush calls for testing the flush hook.
type hookRecordingBase struct {
	onBeforeFlush func()
}

func (h *hookRecordingBase) Header() http.Header         { return httptest.NewRecorder().Header() }
func (h *hookRecordingBase) Write(b []byte) (int, error) { return len(b), nil }
func (h *hookRecordingBase) WriteHeader(_ int)           {}
func (h *hookRecordingBase) markHijacked()               {}
func (h *hookRecordingBase) beforeFlush() {
	if h.onBeforeFlush != nil {
		h.onBeforeFlush()
	}
}

var _ tapWriter = &hookRecordingBase{}

// recordingFlusher is a test stub that records Flush calls.
type recordingFlusher struct {
	onFlush func()
}

func (r *recordingFlusher) Flush() {
	if r.onFlush != nil {
		r.onFlush()
	}
}
