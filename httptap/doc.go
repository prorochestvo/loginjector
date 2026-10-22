// Package httptap is the opt-in HTTP-traffic tap for
// github.com/prorochestvo/loginjector. It wraps an http.HandlerFunc and either
// dumps full request/response payloads (NewPayloadHandler) or emits one
// access-log line per request (NewAccessHandler) to a root *loginjector.Logger
// or a plain io.Writer.
//
// The sub-package name follows the stdlib convention
// (net/http/httputil, httptest, httptrace): transport plus function. Consumers
// that do not need HTTP tapping simply omit this import.
//
// Usage example:
//
//	import (
//	    "io"
//	    "net/http"
//
//	    "github.com/prorochestvo/loginjector"
//	    "github.com/prorochestvo/loginjector/httptap"
//	)
//
//	const LevelInfo loginjector.LogLevel = 1
//
//	logger := loginjector.NewLogger(LevelInfo)
//	myHandler := func(w http.ResponseWriter, r *http.Request) {
//	    _, _ = w.Write([]byte("ok"))
//	}
//	logged := httptap.NewPayloadHandlerWithOptions(
//	    logger,
//	    LevelInfo,
//	    myHandler,
//	    httptap.WithMaxRequestBody(4096),
//	    httptap.WithMaxResponseBody(4096),
//	    httptap.WithRedactHeaders("X-Api-Key"),
//	    httptap.WithSummaryWriter(io.Discard),
//	)
//	http.HandleFunc("/api", logged)
package httptap
