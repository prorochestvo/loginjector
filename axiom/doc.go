// Package axiom provides an opt-in Axiom ingest sink for loginjector.
//
// The root loginjector package never imports axiom. Construct a Handler with an
// Axiom API token (an xaat- token with ingest permission), then pass it to a
// Logger. Delivery is best-effort: events may be dropped or fail, and retries
// after network errors can duplicate a batch Axiom already accepted. Stats
// counts each outcome. Close or Shutdown before exit; log.Fatal and os.Exit
// skip deferred cleanup and lose the final message and queue. Events not
// delivered by the shutdown deadline count as failed.
//
//	client, err := axiom.NewHandler(token, "app-logs")
//	if err != nil {
//		return err
//	}
//	defer client.Close()
//	logger := loginjector.NewLogger(levels.Info, client)
//	logger.Printf(levels.Info, "service started")
//
// WithFields adds static event fields. Calling WithFields more than once
// replaces the fields set by the earlier option.
package axiom
