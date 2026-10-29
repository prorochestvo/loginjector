// Package extensions provides opt-in sink factories and the file-logger bootstrap for loginjector.
// Sink factories return mutex-guarded io.Writer values.
//
// RotatingFileHandler and FileByFormatHandler rotate files by size or generated name.
// RotatingFileHandler can also rotate by age and compress backups. NewFileLogger
// attaches a timestamped stdout printer by default.
//
// Print handlers include PrintHandler for plain stdout lines,
// TimestampedPrintHandler for timestamped stdout lines, and TimestampedHandler for
// adding timestamps to another writer. WithTimeLayout configures the timestamp
// layout, and WithOutput selects the stdout handler's destination.
//
// TelegramHandler sends matching log messages to the Telegram Bot API as documents.
// It uses a synchronous HTTP POST with a 20-second timeout, so matching messages block
// their callers. Transport errors redact the bot token.
//
// HTTP access logging is available through NewAccessHandler, which emits one
// status, method, path, and duration line per request to an io.Writer.
//
// AxiomHandler asynchronously sends bounded batches to Axiom on a best-effort
// basis. Network retries can produce duplicates when Axiom accepted a batch but
// its response was lost. Call Close or Shutdown before exit to drain the queue;
// log.Fatal and os.Exit skip deferred cleanup and can lose queued events. Tokens
// must be xaat- API tokens with ingest permission; xapt- personal tokens are
// rejected.
//
// Axiom defaults:
//
//   - Domain: us-east-1.aws.edge.axiom.co
//   - Batch size: 1000 events
//   - Batch bytes: 4 MiB uncompressed
//   - Flush interval: 1 second
//   - Queue size: 10000 events
//   - Queue bytes: 32 MiB
//   - Retries: 3 after the initial attempt
//   - Compression: gzip
//   - Level name: adapters.Name
//
// Construct it with NewAxiomHandler and pass the result to a Logger.
package extensions
