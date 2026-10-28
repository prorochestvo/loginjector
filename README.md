# LogInjector

A small, dependency-light Go logging library. One `Logger` fans each message out to
several sinks at once — size-rotated files, the console, a Telegram chat — filtered by
severity levels you define. Application code logs through the standard library
(`log.New` over an `io.Writer`); LogInjector supplies that writer and handles routing,
rotation, and alerts.

## Features

- **Fan-out** — one `Logger` writes to many `io.Writer` sinks at once; safe for
  concurrent use.
- **Your own levels** — no baked-in DEBUG/INFO; higher `int` = more severe. The optional
  `levels` sub-package ships a ready `Debug..Critical` ladder.
- **Ready handlers** — rotating files, timestamped console, Telegram, with an optional
  per-handler minimum level.
- **Level hooks** — tee alerts (e.g. to Telegram) on exact levels without lowering the
  global threshold.
- **Stdlib-native usage** — hand components a `*log.Logger` via `StdLog`; they never
  import loginjector.
- **Opt-in HTTP tapping** (`httptap`) — payload dump, access log, and panic-recover
  middleware; the payload dump always redacts auth, cookie, and API-key headers and the
  values of credential query parameters.
- **Dependency-light** — no third-party runtime dependencies.

## Install

```sh
go get github.com/prorochestvo/loginjector
```

Requires Go 1.22.2+.

## Quick start

```go
import (
	"github.com/prorochestvo/loginjector"
	"github.com/prorochestvo/loginjector/levels"
)

// size-rotated files under ./logs (5 MiB × 7) + timestamped console echo.
l, err := loginjector.NewFileLogger("./logs", "app", levels.Info)
if err != nil {
	panic(err)
}
l.Printf(levels.Info, "user %q signed in", "alice")
```

**Recommended:** build one `Logger` in `main`, then give each component its own
standard-library front logger with `StdLog(level, prefix)` — the component depends on
`*log.Logger`, not on loginjector:

```go
// everything at Error and above is also delivered to Telegram.
l.Hook(loginjector.TelegramHandler(botToken, chatID, "errors.log", "prod"),
	levels.Error, levels.Severe, levels.Critical)

dbLog := l.StdLog(levels.Info, "sqlite ")  // = log.New(l.WriterAs(Info), "sqlite ", log.Lmsgprefix)
aiErr := l.StdLog(levels.Error, "openai ")
svc := NewService(dbLog, aiErr)            // takes *log.Logger

// inside the component, call sites stay standard library:
s.log.Printf("request sent (model=%s)", model) // "sqlite request sent (model=gpt-4o)"
```

The full API — handlers, error and closer helpers, `httptap` middleware, and every
option — is on [pkg.go.dev](https://pkg.go.dev/github.com/prorochestvo/loginjector).

## Axiom

The opt-in `axiom` package sends log events asynchronously to an Axiom dataset. Create an
API token with ingest permission (`xaat-` token; personal `xapt-` tokens are rejected),
then pass the handler to a `Logger`. The domain option takes a bare host name; its default
is `us-east-1.aws.edge.axiom.co`.

```go
import (
	"os"

	"github.com/prorochestvo/loginjector"
	"github.com/prorochestvo/loginjector/axiom"
	"github.com/prorochestvo/loginjector/levels"
)

func configure() error {
	ax, err := axiom.NewHandler(os.Getenv("AXIOM_TOKEN"), "app-logs",
		axiom.WithDomain("eu-central-1.aws.edge.axiom.co"),
		axiom.WithFields(map[string]any{"service": "beacon", "env": "prod"}))
	if err != nil {
		return err
	}
	defer ax.Close()
	logger := loginjector.NewLogger(levels.Info, loginjector.TimestampedPrintHandler(), ax)
	std := logger.StdLog(levels.Info, "")
	std.Printf("started")
	return nil
}
```

Events are newline-delimited JSON. Each has `_time` in UTC and `message`; messages have
trailing CR/LF removed and are truncated at 512 KiB after invalid bytes are replaced with U+FFFD. Events
written through `WriteLevel` also include `level` and `level_num`. `WithFields` adds static
fields to every event; its keys cannot be empty, start with `_`, or replace reserved event
fields.

The handler batches up to 1000 events or 4 MiB of uncompressed NDJSON, flushes every second,
and retries transient failures up to three times. `Stats()` reports Sent, Dropped, and Failed
event counts.

| Setting | Default | Constraint |
|---|---:|---|
| Domain | `us-east-1.aws.edge.axiom.co` | Bare host; set with `WithDomain` |
| Batch size | 1000 events | 1–10000 |
| Maximum uncompressed batch | 4 MiB | At least 1 MiB |
| Flush interval | 1 second | Positive duration |
| Queue | 10000 events / 32 MiB | Between 1 and 1048576 events; byte limit must cover a batch |
| Level name | `levels.Name` | Customizable with `WithLevelName` |
| HTTP client timeout | 10 seconds | Replaced with `WithHTTPClient` |
| Retries | 3 after initial attempt | Non-negative |
| Compression | gzip enabled | Configurable with `WithCompression` |

Delivery is best-effort. Events can be dropped when the queue is full or after `Close`, or
fail after retries are exhausted, on a 4xx response, or at the shutdown deadline. A retry
after a network error can duplicate a batch Axiom already accepted. `Stats()` counts each
outcome. Events not delivered by the `Close`/`Shutdown` deadline count as Failed. `Close`
uses a five-second deadline. `log.Fatal` and `os.Exit` skip deferred `Close`, so the final
message and queue are lost; log first, then call `Shutdown` explicitly before exiting. A
`log.New` front logger configured with date flags adds a timestamp inside the event's
`message`; use flags `0` or `Logger.StdLog` instead.

## Notes

- **`Panicf`/`Panic` `panic`, they do not call `os.Exit`** — a deferred `recover` can
  intercept them.
- **Emitter flags:** `StdLog` sets `log.Lmsgprefix` (prefix only) because the sink stamps
  the timestamp; a hand-built front logger with `log.LstdFlags` doubles it.
- **`httptap` redaction is always on:** the payload dump omits the default header set and
  masks the values of the default credential query parameters; both sets are listed in the
  `httptap` package docs on pkg.go.dev. Matching is case-insensitive, and options only add
  names. Credentials inside the values of other headers (`Referer`, `Location`) are not
  masked.
- **`NewRecoverHandler` buffers the response** until the handler returns or calls `Flush`.
  A `Flush` sends the buffered response; a panic after it aborts the connection
  (`http.ErrAbortHandler`) instead of sending a 500.
- **Stable-path rotation:** `RotatingFileHandler(dir, prefix, WithStableCurrentName())`
  keeps the live file at a fixed `prefix.log` path (backups stay indexed) so external
  tooling can follow it with `tail -F`; pair it with `WithMaxAge` and `WithCompress` for
  age-based retention and gzipped backups.

## License

MIT — see [LICENSE](LICENSE).
