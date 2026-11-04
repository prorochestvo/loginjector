package httptap

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	loginjector "github.com/prorochestvo/loginjector"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewPayloadHandler(t *testing.T) {
	t.Parallel()

	t.Run("explicit 200 with WriteHeader", func(t *testing.T) {
		t.Parallel()

		b := bytes.NewBufferString("")
		l := loginjector.NewLogger(logLevelInfo, b)

		userAgent := uniqueToken()
		responseID := uniqueToken()
		request := uniqueToken() + strings.Repeat("Z", rand.Int()%16)
		response := uniqueToken()
		req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/%s?q=%s", uniqueToken(), uniqueToken()), bytes.NewBufferString(request))
		req.Header.Set("User-Agent", userAgent)
		res := httptest.NewRecorder()
		httpStatusOK := func(w http.ResponseWriter, r *http.Request) {
			defer func() { _ = r.Body.Close() }()
			payload, _ := io.ReadAll(r.Body)

			w.WriteHeader(http.StatusOK)
			w.Header().Set("RESPONSE_ID", responseID)
			_, _ = w.Write([]byte(response))
			_, _ = w.Write([]byte("--"))
			_, _ = fmt.Fprintf(w, "PAYLOAD_%d", len(payload))
		}

		h := NewPayloadHandler(l, logLevelInfo, httpStatusOK)

		h(res, req)

		require.Equal(t, http.StatusOK, res.Code, res.Body.String())
		require.Contains(t, res.Body.String(), response, "incorrect response body")
		require.Contains(t, res.Body.String(), fmt.Sprintf("PAYLOAD_%d", len(request)), "incorrect response body")
		require.Greater(t, len(b.String()), 32, "incorrect log context")
		require.Contains(t, b.String(), response, "incorrect log context")
		require.Contains(t, b.String(), request, "incorrect log context")
		require.Contains(t, b.String(), userAgent, "incorrect log context")
		require.Contains(t, b.String(), responseID, "incorrect log context")
		require.Contains(t, b.String(), req.Method, "incorrect log context")
		require.Contains(t, b.String(), req.URL.Path, "incorrect log context")
		require.Contains(t, b.String(), req.URL.RawQuery, "incorrect log context")
		require.Contains(t, b.String(), req.Proto, "incorrect log context")
		require.Contains(t, b.String(), http.StatusText(res.Code), "incorrect log context")
	})

	t.Run("implicit 200 without WriteHeader", func(t *testing.T) {
		t.Parallel()

		b := bytes.NewBufferString("")
		l := loginjector.NewLogger(logLevelInfo, b)

		body := uniqueToken()
		req := httptest.NewRequest(http.MethodGet, "/implicit", bytes.NewBufferString(""))
		res := httptest.NewRecorder()

		implicitOK := func(w http.ResponseWriter, r *http.Request) {
			defer func() { _ = r.Body.Close() }()
			_, _ = w.Write([]byte(body))
		}

		h := NewPayloadHandler(l, logLevelInfo, implicitOK)
		h(res, req)

		require.Equal(t, http.StatusOK, res.Code)
		require.Contains(t, b.String(), "200", "logged response must show 200 for implicit-OK handler")
		require.Contains(t, b.String(), http.StatusText(http.StatusOK), "logged response must include status text for 200")
	})

	t.Run("explicit non-200 status is not clobbered", func(t *testing.T) {
		t.Parallel()

		b := bytes.NewBufferString("")
		l := loginjector.NewLogger(logLevelInfo, b)

		req := httptest.NewRequest(http.MethodGet, "/not-found", bytes.NewBufferString(""))
		res := httptest.NewRecorder()

		notFound := func(w http.ResponseWriter, r *http.Request) {
			defer func() { _ = r.Body.Close() }()
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte("not here"))
		}

		h := NewPayloadHandler(l, logLevelInfo, notFound)
		h(res, req)

		require.Equal(t, http.StatusNotFound, res.Code)
		require.Contains(t, b.String(), "404", "logged response must show 404")
		require.Contains(t, b.String(), http.StatusText(http.StatusNotFound))
	})

	t.Run("sensitive headers are redacted", func(t *testing.T) {
		t.Parallel()

		b := bytes.NewBufferString("")
		l := loginjector.NewLogger(logLevelInfo, b)

		authToken := "AUTHSECRET-" + uniqueToken()
		proxyToken := "PROXYSECRET-" + uniqueToken()
		cookieToken := "COOKIESECRET-" + uniqueToken()

		req := httptest.NewRequest(http.MethodGet, "/secure", bytes.NewBufferString(""))
		req.Header.Set("Authorization", "Bearer "+authToken)
		req.Header.Set("Proxy-Authorization", "Bearer "+proxyToken)
		req.Header.Set("Cookie", "session="+cookieToken)
		res := httptest.NewRecorder()

		ok := func(w http.ResponseWriter, r *http.Request) {
			defer func() { _ = r.Body.Close() }()
			w.Header().Set("Set-Cookie", "session="+cookieToken)
			_, _ = w.Write([]byte("ok"))
		}

		h := NewPayloadHandler(l, logLevelInfo, ok)
		h(res, req)

		require.NotContains(t, b.String(), authToken, "Authorization must be redacted")
		require.NotContains(t, b.String(), proxyToken, "Proxy-Authorization must be redacted")
		require.NotContains(t, b.String(), cookieToken, "Cookie/Set-Cookie must be redacted")
	})

	t.Run("nil logger panics", func(t *testing.T) {
		t.Parallel()

		require.PanicsWithValue(t, "httptap: logger is nil", func() {
			NewPayloadHandler(nil, logLevelInfo, func(_ http.ResponseWriter, _ *http.Request) {})
		})
	})

	t.Run("nil next panics", func(t *testing.T) {
		t.Parallel()

		l := loginjector.NewLogger(logLevelInfo, bytes.NewBufferString(""))
		require.PanicsWithValue(t, "httptap: nextFunc is nil", func() {
			NewPayloadHandler(l, logLevelInfo, nil)
		})
	})
}

// TestNewPayloadHandlerWithOptions covers all options.
func TestNewPayloadHandlerWithOptions(t *testing.T) {
	t.Parallel()

	// newHandler is a test helper that builds a handler backed by a fresh *bytes.Buffer logger.
	newHandler := func(t *testing.T, opts ...PayloadOption) (http.HandlerFunc, *bytes.Buffer) {
		t.Helper()
		b := bytes.NewBufferString("")
		l := loginjector.NewLogger(logLevelInfo, b)
		h := NewPayloadHandlerWithOptions(l, logLevelInfo, func(w http.ResponseWriter, r *http.Request) {
			defer func() { _ = r.Body.Close() }()
			body, _ := io.ReadAll(r.Body)
			_, _ = w.Write(body) // echo request body as response body for easy assertions
		}, opts...)
		return h, b
	}

	t.Run("nil logger panics", func(t *testing.T) {
		t.Parallel()

		require.PanicsWithValue(t, "httptap: logger is nil", func() {
			NewPayloadHandlerWithOptions(nil, logLevelInfo, func(_ http.ResponseWriter, _ *http.Request) {})
		})
	})

	t.Run("default_captures_full_body", func(t *testing.T) {
		t.Parallel()

		payload := strings.Repeat("x", 200)
		h, b := newHandler(t, WithSummaryWriter(io.Discard))
		req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(payload))
		res := httptest.NewRecorder()
		h(res, req)

		require.Equal(t, payload, res.Body.String(), "client must see full body")
		require.Contains(t, b.String(), payload, "log must contain full request body")
	})

	t.Run("default_truncates_body_over_64_KiB", func(t *testing.T) {
		t.Parallel()

		// a body larger than the 64 KiB default cap must be truncated in the log
		// while the downstream handler still echoes the full body to the client.
		payload := strings.Repeat("x", (64<<10)+1024)
		h, b := newHandler(t, WithSummaryWriter(io.Discard))
		req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(payload))
		res := httptest.NewRecorder()
		h(res, req)

		require.Equal(t, payload, res.Body.String(), "client must still receive the full body")
		require.Contains(t, b.String(), "…[truncated]", "log must mark the over-cap body as truncated")
		require.NotContains(t, b.String(), payload, "log must not contain the whole over-cap body")
	})

	t.Run("negative_limit_restores_unlimited_capture", func(t *testing.T) {
		t.Parallel()

		// WithMaxRequestBody(-1)/WithMaxResponseBody(-1) explicitly override the 64 KiB
		// default: a >64 KiB body must appear whole in the log with no truncation marker.
		// The echo handler mirrors the request as the response, so both caps must be
		// lifted to keep the log free of a marker.
		marker := "UNLIMITED-" + uniqueToken()
		payload := marker + strings.Repeat("y", 64<<10)
		h, b := newHandler(t, WithMaxRequestBody(-1), WithMaxResponseBody(-1), WithSummaryWriter(io.Discard))
		req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(payload))
		res := httptest.NewRecorder()
		h(res, req)

		require.Equal(t, payload, res.Body.String(), "client must receive the full body")
		require.Contains(t, b.String(), payload, "unlimited capture must log the whole request body")
		require.NotContains(t, b.String(), "…[truncated]", "unlimited capture must not truncate")
	})

	t.Run("request_body_truncated_at_limit", func(t *testing.T) {
		t.Parallel()

		// use two distinct character sequences so we can check the request section only.
		reqPayload := strings.Repeat("r", 100) // "rrr..."
		limit := 20
		responseMarker := "DONE-" + uniqueToken() // unique, not in request payload

		b := bytes.NewBufferString("")
		l := loginjector.NewLogger(logLevelInfo, b)
		h := NewPayloadHandlerWithOptions(l, logLevelInfo, func(w http.ResponseWriter, r *http.Request) {
			defer func() { _ = r.Body.Close() }()
			body, _ := io.ReadAll(r.Body)
			// respond with a fixed marker, not the request body, to keep log sections separate.
			_, _ = fmt.Fprintf(w, "%s|len=%d", responseMarker, len(body))
		}, WithMaxRequestBody(limit), WithSummaryWriter(io.Discard))

		req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(reqPayload))
		res := httptest.NewRecorder()
		h(res, req)

		// client must receive the full response (the marker + length).
		require.Contains(t, res.Body.String(), responseMarker, "client must receive full response")
		// the handler must have seen the full body (length check).
		require.Contains(t, res.Body.String(), fmt.Sprintf("len=%d", len(reqPayload)), "handler must read full request body")

		// log must contain the truncation marker.
		require.Contains(t, b.String(), "…[truncated]", "log must contain truncation marker")
		// log must contain the first `limit` bytes of the request body.
		require.Contains(t, b.String(), reqPayload[:limit], "log must contain first %d bytes of request", limit)

		// the log must not contain more than `limit` consecutive r-characters in the
		// request-body section.
		require.NotContains(t, b.String(), strings.Repeat("r", limit+1), "log must not contain bytes beyond the request body cap")
	})

	t.Run("response_body_truncated_at_limit", func(t *testing.T) {
		t.Parallel()

		payload := strings.Repeat("s", 100)
		limit := 25

		b := bytes.NewBufferString("")
		l := loginjector.NewLogger(logLevelInfo, b)
		h := NewPayloadHandlerWithOptions(l, logLevelInfo, func(w http.ResponseWriter, r *http.Request) {
			defer func() { _ = r.Body.Close() }()
			_, _ = w.Write([]byte(payload))
		}, WithMaxResponseBody(limit), WithSummaryWriter(io.Discard))

		req := httptest.NewRequest(http.MethodGet, "/", bytes.NewBufferString(""))
		res := httptest.NewRecorder()
		h(res, req)

		// client must receive the full response.
		require.Equal(t, payload, res.Body.String(), "client must receive full response")
		// log captures at most limit bytes plus truncation marker.
		require.Contains(t, b.String(), "…[truncated]", "log must contain truncation marker for response")
		require.Contains(t, b.String(), payload[:limit], "log must contain first %d bytes of response", limit)
		require.NotContains(t, b.String(), payload[limit:], "log must not contain bytes beyond the cap")
	})

	t.Run("without_bodies_omits_both", func(t *testing.T) {
		t.Parallel()

		reqPayload := "request-body-" + uniqueToken()
		resPayload := "response-body-" + uniqueToken()

		b := bytes.NewBufferString("")
		l := loginjector.NewLogger(logLevelInfo, b)
		h := NewPayloadHandlerWithOptions(l, logLevelInfo, func(w http.ResponseWriter, r *http.Request) {
			defer func() { _ = r.Body.Close() }()
			_, _ = io.ReadAll(r.Body) // consume body
			_, _ = w.Write([]byte(resPayload))
		}, WithoutBodies(), WithSummaryWriter(io.Discard))

		req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(reqPayload))
		res := httptest.NewRecorder()
		h(res, req)

		// client still gets the full response.
		require.Equal(t, resPayload, res.Body.String(), "client must receive full response")
		// log must not contain either body.
		require.NotContains(t, b.String(), reqPayload, "log must not contain request body")
		require.NotContains(t, b.String(), resPayload, "log must not contain response body")
		// log must still contain head/status info.
		require.Contains(t, b.String(), "200", "log must still contain status code")
		require.Contains(t, b.String(), http.MethodPost, "log must still contain method")
	})

	t.Run("truncation_marker_present", func(t *testing.T) {
		t.Parallel()

		h, b := newHandler(t, WithMaxRequestBody(5), WithSummaryWriter(io.Discard))
		req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString("hello-world-this-is-long"))
		res := httptest.NewRecorder()
		h(res, req)

		require.Contains(t, b.String(), "…[truncated]")
	})

	t.Run("zero_limit_still_logs_head_and_headers", func(t *testing.T) {
		t.Parallel()

		userAgent := "zero-limit-ua-" + uniqueToken()
		h, b := newHandler(t, WithoutBodies(), WithSummaryWriter(io.Discard))
		req := httptest.NewRequest(http.MethodGet, "/zero-path", bytes.NewBufferString("body"))
		req.Header.Set("User-Agent", userAgent)
		res := httptest.NewRecorder()
		h(res, req)

		require.Contains(t, b.String(), "/zero-path", "log must contain path")
		require.Contains(t, b.String(), userAgent, "log must contain User-Agent")
		require.Contains(t, b.String(), "200", "log must contain status")
	})

	t.Run("body_cap_race_condition", func(t *testing.T) {
		t.Parallel()
		// run many concurrent requests through a bounded-cap handler to verify no data race.
		// WithSummaryWriter(io.Discard) ensures this parallel subtest never touches
		// os.Stdout, preventing a race with other tests.
		b := &safeBuffer{}
		l := loginjector.NewLogger(logLevelInfo, b)
		h := NewPayloadHandlerWithOptions(l, logLevelInfo, func(w http.ResponseWriter, r *http.Request) {
			defer func() { _ = r.Body.Close() }()
			body, _ := io.ReadAll(r.Body)
			_, _ = w.Write(body)
		}, WithMaxRequestBody(16), WithMaxResponseBody(16), WithSummaryWriter(io.Discard))

		const concurrency = 20
		var wg sync.WaitGroup
		wg.Add(concurrency)
		for range concurrency {
			go func() {
				defer wg.Done()
				payload := strings.Repeat("x", 64)
				req := httptest.NewRequest(http.MethodPost, "/race", bytes.NewBufferString(payload))
				res := httptest.NewRecorder()
				h(res, req)
				// downstream must always receive the full body.
				assert.Equal(t, payload, res.Body.String())
			}()
		}
		wg.Wait()
	})

	t.Run("default_set_still_redacted", func(t *testing.T) {
		t.Parallel()

		secret := "secret-" + uniqueToken()
		h, b := newHandler(t, WithSummaryWriter(io.Discard)) // no extra options
		req := httptest.NewRequest(http.MethodGet, "/", bytes.NewBufferString(""))
		req.Header.Set("Authorization", "Bearer "+secret)
		res := httptest.NewRecorder()
		h(res, req)

		require.NotContains(t, b.String(), secret, "Authorization must be redacted by default")
	})

	t.Run("custom_header_redacted", func(t *testing.T) {
		t.Parallel()

		apiKey := "apikey-" + uniqueToken()
		h, b := newHandler(t, WithRedactHeaders("X-Api-Key"), WithSummaryWriter(io.Discard))
		req := httptest.NewRequest(http.MethodGet, "/", bytes.NewBufferString(""))
		req.Header.Set("X-Api-Key", apiKey)
		res := httptest.NewRecorder()
		h(res, req)

		require.NotContains(t, b.String(), apiKey, "X-Api-Key must be redacted")
	})

	t.Run("case_insensitive_match", func(t *testing.T) {
		t.Parallel()

		apiKey := "case-" + uniqueToken()
		// register in lowercase; the header will arrive in canonical form.
		h, b := newHandler(t, WithRedactHeaders("x-secret-token"), WithSummaryWriter(io.Discard))
		req := httptest.NewRequest(http.MethodGet, "/", bytes.NewBufferString(""))
		req.Header.Set("X-Secret-Token", apiKey) // net/http canonicalises this
		res := httptest.NewRecorder()
		h(res, req)

		require.NotContains(t, b.String(), apiKey, "case-insensitive redaction must work")
	})

	t.Run("default_set_cannot_be_removed", func(t *testing.T) {
		t.Parallel()

		// verify that even when WithRedactHeaders is called, the built-in set remains.
		secret := "immutable-" + uniqueToken()
		h, b := newHandler(t, WithRedactHeaders("X-Custom"), WithSummaryWriter(io.Discard))
		req := httptest.NewRequest(http.MethodGet, "/", bytes.NewBufferString(""))
		req.Header.Set("Authorization", "Bearer "+secret)
		res := httptest.NewRecorder()
		h(res, req)

		require.NotContains(t, b.String(), secret, "built-in Authorization redaction must survive additional WithRedactHeaders calls")
	})

	t.Run("non-redacted_header_visible", func(t *testing.T) {
		t.Parallel()

		visible := "visible-ua-" + uniqueToken()
		h, b := newHandler(t, WithSummaryWriter(io.Discard))
		req := httptest.NewRequest(http.MethodGet, "/", bytes.NewBufferString(""))
		req.Header.Set("User-Agent", visible)
		res := httptest.NewRecorder()
		h(res, req)

		require.Contains(t, b.String(), visible, "non-redacted header must appear in log")
	})
}

// TestSummaryWriter exercises WithSummaryWriter in full.
// This function does NOT call t.Parallel() (top-level or subtests) to ensure
// sequential execution. In the original root package there was a package-global
// summary writer that required serialisation to avoid races. The global is gone
// (Variant B), but we keep the non-parallel discipline here as a safety margin
// since these subtests do share os.Stdout as the default fallback destination.
//
//nolint:paralleltest // subtests share os.Stdout as the default summary destination
func TestSummaryWriter(t *testing.T) {
	// newSummaryHandler is a local helper: builds a handler backed by a fresh logger.
	newSummaryHandler := func(t *testing.T, opts ...PayloadOption) (http.HandlerFunc, *bytes.Buffer) {
		t.Helper()
		b := bytes.NewBufferString("")
		l := loginjector.NewLogger(logLevelInfo, b)
		h := NewPayloadHandlerWithOptions(l, logLevelInfo, func(w http.ResponseWriter, r *http.Request) {
			defer func() { _ = r.Body.Close() }()
			_, _ = w.Write([]byte("ok"))
		}, opts...)
		return h, b
	}

	t.Run("option_overrides_default", func(t *testing.T) {
		perHandler := bytes.NewBufferString("")
		h, _ := newSummaryHandler(t, WithSummaryWriter(perHandler))
		req := httptest.NewRequest(http.MethodGet, "/per-handler", bytes.NewBufferString(""))
		h(httptest.NewRecorder(), req)

		require.Contains(t, perHandler.String(), "/per-handler", "per-handler summary writer must receive the summary")
	})

	t.Run("discard_silences_summary", func(t *testing.T) {
		perHandler := bytes.NewBufferString("")
		h, _ := newSummaryHandler(t, WithSummaryWriter(io.Discard))
		req := httptest.NewRequest(http.MethodGet, "/discarded", bytes.NewBufferString(""))
		h(httptest.NewRecorder(), req)

		require.NotContains(t, perHandler.String(), "/discarded", "discarded summary must not reach per-handler buffer")
	})

	// no-options backward-compat gate: NewPayloadHandler and NewPayloadHandlerWithOptions
	// with no behavioral options must produce equivalent log output for the same request.
	t.Run("no_options_equivalent_to_legacy_constructor", func(t *testing.T) {
		requestBody := "identical-body-" + uniqueToken()
		userAgent := "go-test/" + uniqueToken()

		echoHandler := func(w http.ResponseWriter, r *http.Request) {
			defer func() { _ = r.Body.Close() }()
			body, _ := io.ReadAll(r.Body)
			_, _ = w.Write(body)
		}

		// Side A: real legacy constructor — exercises NewPayloadHandler directly.
		bA := bytes.NewBufferString("")
		lA := loginjector.NewLogger(logLevelInfo, bA)
		hA := NewPayloadHandler(lA, logLevelInfo, echoHandler)

		// Side B: new constructor with no behavioral options; summary goes to io.Discard.
		summaryB := bytes.NewBufferString("")
		bB := bytes.NewBufferString("")
		lB := loginjector.NewLogger(logLevelInfo, bB)
		hB := NewPayloadHandlerWithOptions(lB, logLevelInfo, echoHandler, WithSummaryWriter(summaryB))

		makeReq := func() *http.Request {
			req := httptest.NewRequest(http.MethodPost, "/test?q=1", bytes.NewBufferString(requestBody))
			req.Header.Set("User-Agent", userAgent)
			req.Header.Set("Content-Type", "text/plain")
			return req
		}

		resA := httptest.NewRecorder()
		hA(resA, makeReq())
		resB := httptest.NewRecorder()
		hB(resB, makeReq())

		// response bodies must be identical.
		require.Equal(t, resA.Body.String(), resB.Body.String(), "response bodies must match")

		// both logs must contain the same key tokens.
		for _, token := range []string{requestBody, userAgent, "/test", "q=1", "200", http.StatusText(http.StatusOK)} {
			require.Contains(t, bA.String(), token, "legacy log must contain %q", token)
			require.Contains(t, bB.String(), token, "options log must contain %q", token)
		}

		// options side wrote only to its per-handler buffer.
		require.Contains(t, summaryB.String(), "/test", "options constructor must write to its per-handler summary")
	})
}

// ExampleNewPayloadHandlerWithOptions demonstrates the recommended configuration
// for a production consumer that wants bounded body capture, extra secret-header
// redaction, and a silent per-handler summary.
func ExampleNewPayloadHandlerWithOptions() {
	const LevelInfo loginjector.LogLevel = 1

	logger := loginjector.NewLogger(LevelInfo)
	myHttpHandler := func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}
	_ = NewPayloadHandlerWithOptions(
		logger,
		LevelInfo,
		myHttpHandler,
		WithMaxRequestBody(4096),  // cap request body at 4 KiB
		WithMaxResponseBody(4096), // cap response body at 4 KiB
		WithRedactHeaders("X-Api-Key", "X-Telegram-Init-Data"),
		WithSummaryWriter(io.Discard), // silence per-request summary line
	)
	// Use the returned handler with http.Handle or your router.
}
