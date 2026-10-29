# LogInjector

LogInjector is a small Go logging library. Its core `Logger` fans each message out to
`io.Writer` sinks and level hooks, with per-sink serialization and configurable severity
filtering. Applications keep using the standard library's `log.Logger` through
`Logger.StdLog` or `Logger.WriterAs`; sinks and HTTP middleware are opt-in packages.

## Install

```sh
go get github.com/prorochestvo/loginjector
```

Requires Go 1.22.2 or newer. Runtime dependencies are limited to the standard library.

## Quick start

```go
package main

import (
	"log"

	"github.com/prorochestvo/loginjector/adapters"
	"github.com/prorochestvo/loginjector/extensions"
)

func main() {
	logger, err := extensions.NewFileLogger("./logs", "app", adapters.Info)
	if err != nil {
		log.Fatal(err)
	}
	std := logger.StdLog(adapters.Info, "app ")
	std.Printf("service started")

	logger.Hook(
		extensions.TelegramHandler("bot-token", "chat-id", "errors.log"),
		adapters.Error,
		adapters.Severe,
		adapters.Critical,
	)
}
```

`Logger` itself defines no level constants: `LogLevel` is an `int`, and higher values are
more severe. Use `adapters` for its optional `Debug` through `Critical` ladder, or define
your own levels.

## Packages

| Package | Purpose |
|---|---|
| `github.com/prorochestvo/loginjector` | Core fan-out `Logger`, hooks, `WithMinLevel`, and error types. |
| `github.com/prorochestvo/loginjector/extensions` | Rotating and generated-name file sinks, print sinks, Telegram, HTTP payload capture and panic-recovery middleware, access logging, file-logger bootstrap, and Axiom. |
| `github.com/prorochestvo/loginjector/adapters` | Optional severity constants and parsing, plus `io.Closer` helpers. |

## Axiom

The opt-in Axiom sink batches events asynchronously on a bounded queue and delivers them on
a best-effort basis; call `Close` or `Shutdown` before exit to flush pending events. See
the `AxiomHandler` and `NewAxiomHandler` documentation for options, defaults and delivery details.

```go
ax, err := extensions.NewAxiomHandler(extensions.AxiomToken(token), "app-logs")
if err != nil {
	log.Fatal(err)
}
defer ax.Close()
logger := loginjector.NewLogger(adapters.Info, ax)
```

## Consumer notes

- `Logger.Panicf` and `Logger.Panic` panic; they do not call `os.Exit`.
- `MinLevel` applies to any sink that implements `LeveledHandler`; `WithMinLevel` is one
  way to add that interface. Hooks match exact levels even below the logger's minimum.
- `extensions.NewPayloadHandler` and `NewPayloadHandlerWithOptions` always redact the default
  sensitive headers and credential query parameters; options can add names but not remove defaults.
- `extensions.NewRecoverHandler` buffers responses until completion or flush. Streaming routes
  such as SSE and WebSockets should not use it.
- Telegram delivery is a synchronous HTTP request. Axiom delivery is asynchronous and
  bounded, so close its handler during shutdown.

The complete API and package documentation are available on
[pkg.go.dev](https://pkg.go.dev/github.com/prorochestvo/loginjector).

## License

MIT — see [LICENSE](LICENSE).
