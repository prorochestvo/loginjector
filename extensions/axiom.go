package extensions

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/prorochestvo/loginjector"
	"github.com/prorochestvo/loginjector/internal/integrations/axiom"
)

// ErrAxiomClosed is returned by Flush and Shutdown after Close or Shutdown.
// Write after closure is dropped and does not return this error.
var ErrAxiomClosed = axiom.ErrClosed

// AxiomHandler asynchronously batches log events for delivery to Axiom. It
// implements loginjector.LevelWriter, io.Closer, and fmt.Formatter.
type AxiomHandler struct {
	h *axiom.Handler
}

// NewAxiomHandler validates an Axiom ingest token, dataset, and options, then
// starts a delivery worker. The token must have ingest permission.
func NewAxiomHandler(token AxiomToken, dataset AxiomDataset, opts ...AxiomOption) (*AxiomHandler, error) {
	h, err := axiom.NewHandler(token, dataset, opts...)
	if err != nil {
		return nil, err
	}
	return &AxiomHandler{h: h}, nil
}

// Write queues an unlevelled log event for asynchronous delivery.
func (a *AxiomHandler) Write(p []byte) (int, error) { return a.h.Write(p) }

// WriteLevel queues a log event with its severity.
func (a *AxiomHandler) WriteLevel(level loginjector.LogLevel, p []byte) (int, error) {
	return a.h.WriteLevel(level, p)
}

// Flush sends events admitted before the call and waits for their completion.
func (a *AxiomHandler) Flush(ctx context.Context) error { return a.h.Flush(ctx) }

// Shutdown stops delivery after flushing events admitted before the call.
func (a *AxiomHandler) Shutdown(ctx context.Context) error { return a.h.Shutdown(ctx) }

// Close shuts down the handler and waits for queued events to finish.
func (a *AxiomHandler) Close() error { return a.h.Close() }

// Stats reports event counts, not batch counts: Sent counts delivered events,
// Dropped counts events refused at admission, and Failed counts failed deliveries.
func (a *AxiomHandler) Stats() AxiomStats { return a.h.Stats() }

// Format prevents formatting from exposing the handler's private configuration.
func (a *AxiomHandler) Format(state fmt.State, verb rune) { a.h.Format(state, verb) }

// AxiomOption configures an AxiomHandler.
type AxiomOption = axiom.Option

// WithAxiomDomain sets the bare Axiom edge host. The default is
// "us-east-1.aws.edge.axiom.co".
func WithAxiomDomain(domain string) AxiomOption { return axiom.WithDomain(domain) }

// WithAxiomFields sets static fields on every event. Calling this option more
// than once replaces the fields set by the earlier option.
func WithAxiomFields(fields map[string]any) AxiomOption { return axiom.WithFields(fields) }

// WithAxiomLevelName sets the name used to encode levels. The default is
// adapters.Name; level_num remains present for levels without a known name.
func WithAxiomLevelName(fn func(loginjector.LogLevel) string) AxiomOption {
	return axiom.WithLevelName(fn)
}

// WithAxiomBatchSize sets the maximum events in a batch, from 1 through 10000.
// The default is 1000.
func WithAxiomBatchSize(n int) AxiomOption { return axiom.WithBatchSize(n) }

// WithAxiomMaxBatchBytes sets the maximum uncompressed batch size. It must be
// at least 1 MiB; the default is 4 MiB.
func WithAxiomMaxBatchBytes(n int) AxiomOption { return axiom.WithMaxBatchBytes(n) }

// WithAxiomFlushInterval sets the positive interval between batch flushes. The
// default is one second.
func WithAxiomFlushInterval(d time.Duration) AxiomOption { return axiom.WithFlushInterval(d) }

// WithAxiomQueueSize sets the maximum queued event count, from 1 through
// 1048576. The default is 10000.
func WithAxiomQueueSize(n int) AxiomOption { return axiom.WithQueueSize(n) }

// WithAxiomMaxQueueBytes sets the positive byte limit for queued events. It
// must be at least the configured maximum batch size. The default is 32 MiB.
func WithAxiomMaxQueueBytes(n int) AxiomOption { return axiom.WithMaxQueueBytes(n) }

// WithAxiomHTTPClient sets the HTTP client used for delivery. The supplied
// client is not owned by the handler; by default a ten-second client is used.
func WithAxiomHTTPClient(client *http.Client) AxiomOption { return axiom.WithHTTPClient(client) }

// WithAxiomMaxRetries sets retries after the initial delivery attempt. Zero
// disables retries; the default is three.
func WithAxiomMaxRetries(n int) AxiomOption { return axiom.WithMaxRetries(n) }

// WithAxiomCompression enables or disables gzip compression, which is enabled
// by default.
func WithAxiomCompression(enabled bool) AxiomOption { return axiom.WithCompression(enabled) }

// WithAxiomErrorHandler sets the function that receives delivery errors. The
// default writes one line to os.Stderr. Calls are rate-limited by the handler;
// the function must not call Flush, Shutdown, or Close.
func WithAxiomErrorHandler(fn func(error)) AxiomOption { return axiom.WithErrorHandler(fn) }

// AxiomToken is an Axiom API token with ingest permission.
type AxiomToken = axiom.Token

// AxiomDataset is the name of an Axiom dataset.
type AxiomDataset = axiom.Dataset

// AxiomStats reports event counts, not batch counts: Sent counts events
// delivered, Dropped counts events refused at admission, and Failed counts
// events whose delivery failed.
type AxiomStats = axiom.Stats
