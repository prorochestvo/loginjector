package extensions_test

import (
	"io"
	"net/http"
	"os"
	"time"

	"github.com/prorochestvo/loginjector"
	"github.com/prorochestvo/loginjector/adapters"

	"github.com/prorochestvo/loginjector/extensions"
)

// ExampleNewAccessHandler shows the canonical production access-log sink: a
// RotatingFileHandler configured for a stable, tail -F-able path with age-based retention
// and gzipped backups, handed to NewAccessHandler. RotatingFileHandler returns a
// mutex-guarded writer, so it satisfies NewAccessHandler's requirement that out be safe
// for concurrent Write calls.
func ExampleNewAccessHandler() {
	// access.log stays at a fixed path so `tail -F access.log` follows it across rotations;
	// rotated backups are indexed, gzipped, and pruned once older than two weeks.
	maxAge := 14 * 24 * time.Hour
	out := extensions.RotatingFileHandler(
		"./logs",
		"access",
		extensions.WithStableCurrentName(),
		extensions.WithMaxAge(maxAge),
		extensions.WithCompress(),
	)

	app := func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}

	// Register the returned handler with http.Handle or your router.
	_ = extensions.NewAccessHandler(out, app)
}

// ExampleNewAxiomHandler demonstrates asynchronous Axiom delivery with a Logger.
func ExampleNewAxiomHandler() {
	ax, err := extensions.NewAxiomHandler(
		extensions.AxiomToken(os.Getenv("AXIOM_TOKEN")),
		"app-logs",
		extensions.WithAxiomDomain("eu-central-1.aws.edge.axiom.co"),
		extensions.WithAxiomFields(map[string]any{"service": "beacon", "env": "prod"}),
	)
	if err != nil {
		return
	}
	defer ax.Close()
	logger := loginjector.NewLogger(
		adapters.Info,
		extensions.TimestampedPrintHandler(),
		ax,
	)
	std := logger.StdLog(adapters.Info, "")
	std.Printf("started")
}

// ExampleAxiomHandler_withMinLevel demonstrates filtering Axiom events by level.
func ExampleAxiomHandler_withMinLevel() {
	ax, err := extensions.NewAxiomHandler(extensions.AxiomToken(os.Getenv("AXIOM_TOKEN")), "app-logs")
	if err != nil {
		return
	}
	defer ax.Close()

	warnings := loginjector.WithMinLevel(adapters.Warning, ax)
	logger := loginjector.NewLogger(adapters.Info, warnings)
	logger.Printf(adapters.Info, "not queued for Axiom")
	logger.Printf(adapters.Warning, "queued for Axiom")
}

// ExampleNewPayloadHandlerWithOptions demonstrates the recommended configuration
// for a production consumer that wants bounded body capture, extra secret-header
// redaction, and a silent per-handler summary.
func ExampleNewPayloadHandlerWithOptions() {
	logger := loginjector.NewLogger(adapters.Info)
	myHttpHandler := func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}
	_ = extensions.NewPayloadHandlerWithOptions(
		logger,
		adapters.Info,
		myHttpHandler,
		extensions.WithMaxRequestBody(4096),  // cap request body at 4 KiB
		extensions.WithMaxResponseBody(4096), // cap response body at 4 KiB
		extensions.WithRedactHeaders("X-Telegram-Init-Data"),
		extensions.WithSummaryWriter(io.Discard), // silence per-request summary line
	)
	// Use the returned handler with http.Handle or your router.
}
