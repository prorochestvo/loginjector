# CLAUDE.md

## What this is

`github.com/prorochestvo/loginjector` is a small, dependency-light Go **logging library** (not a service): a `Logger` fans a message out to a set of `io.Writer` handlers, plus ready-made handlers (console, file rotation, Telegram), level-targeted hooks, and orthogonal helpers (JSON, `io.Closer` wrappers, stack-trace / HTTP error types). Opt-in HTTP traffic tapping lives in `httptap/`. Consumers `go get` and embed it — no binary, no database, no HTTP server of its own.

## Build & Test Commands

**No Makefile** — use raw `go`. The package builds clean with `CGO_ENABLED=0` (pure Go, no cgo deps), but the race detector needs cgo on this arm64 host, so race tests run with `CGO_ENABLED=1` (append `-run <name>` for a single test/subtest):

```bash
CGO_ENABLED=1 go test -race ./...    # full suite (race needs cgo on this arm64 host)
CGO_ENABLED=0 go test ./...          # no-cgo build proof
go vet ./... && gofmt -l .           # vet + list unformatted
```

## Architecture

Three packages:

| Package | Location | Role |
|---------|----------|------|
| `loginjector` | repo root (`*.go`) | public API: `Logger`, handlers, hooks, helpers |
| `httptap` | `httptap/` | opt-in HTTP traffic tap — `NewPayloadHandler`, `NewAccessHandler`, options DSL, interface pass-through wrappers |
| `internal` | `internal/` | `StackTrace()` / `LineTrace()` — debug-stack parsing, used by `TraceError` |

### The fan-out model (`logger.go`)

`Logger` holds a `minimumLogLevel`, a slice of `handlers []io.Writer`, a slice of
`hooks`, and an `sync.RWMutex`. The central method is:

```go
func (l *Logger) WriteLog(level LogLevel, message []byte) (int, error)
```

For each call it dispatches the message to two distinct sinks, **each handler/hook
write running in its own goroutine**, joined with a `sync.WaitGroup` and aggregated via
`errors.Join`:

1. **Hooks** — fire on an **exact level match** (`level == hook.Level`), *regardless of
   `minimumLogLevel`*. This is the key asymmetry: a hook on a low-severity level still
   fires even when that level is below the configured minimum.
2. **Handlers** — fire only when `level >= minimumLogLevel`. Below the minimum,
   handlers are skipped and `WriteLog` returns `(0, nil)`.

Every handler/hook sink is wrapped in a mutex-guarded `writer` at registration
(`ensureThreadSafe` in `NewLogger`/`Hook`), so the logger never calls a given sink's
`Write` concurrently — logging from many goroutines is safe even with a plain,
non-thread-safe writer (e.g. a `bytes.Buffer`). Within a `WriteLog` call, errors from
the concurrent sinks are collected under a mutex and joined after `wg.Wait()`.

Convenience writers: `Printf`/`Print`, `Write` (`io.Writer` at `minimumLogLevel`), and `Fatalf`/`Fatal` — which **`panic`, they do not `os.Exit`** (non-obvious).

### LogLevel is consumer-defined

`LogLevel` is just `type LogLevel int`. **The root package ships no DEBUG/INFO/WARN/ERROR
constants** — the consumer declares their own, or imports the `levels` subpackage
(`Debug`..`Critical` = 1..6, plus `Parse`/`Name`). Severity ordering is by integer value:
higher = more severe, since the gate is `level < minimumLogLevel`.

### Plugging the logger into other writers

`WriterAs(level)` / `JoinAs(level, outputs...)` adapt the logger to an `io.Writer` at a fixed level — e.g. to hand it to the std `log` package or a third-party sink as an `io.Writer`.

### Handlers (`handler.go`)

Every handler is a **factory function returning `io.Writer`**, internally wrapping a mutex-guarded `writer` so each handler serializes its own writes. `TimestampedPrintHandler()` is the default when `NewLogger` gets no handlers. File handlers (`RotatingFileHandler`, `FileByFormatHandler`) share `.log` extension, `0640` perms, append mode, and oldest-first pruning past their max-files bound; runtime logs live under `./logs/`. `RotatingFileHandler` carries opt-in `RotatingFileOption`s — `WithStableCurrentName` (fixed `prefix.log` live path for `tail -F`), `WithMaxAge` (mtime retention), `WithCompress` (gzip backups) — all off by default and byte-identical when unused.

### HTTP traffic tap (`httptap/`)

The HTTP tap lives in `github.com/prorochestvo/loginjector/httptap`. Import it
separately when you need HTTP traffic tapping; the root package does not pull it in.

`httptap.NewPayloadHandler(logger, level, nextFunc)` wraps an `http.HandlerFunc` and logs the full request+response through the logger. **Security contract: `Authorization`, `Proxy-Authorization`, `Cookie`, `Set-Cookie` are always redacted (`headerToStringWithRedact`) — this default set is always-on and cannot be removed.** `NewPayloadHandlerWithOptions(...opts)` is a strict superset (zero options → byte-identical output); its options (`WithMaxRequestBody`/`WithMaxResponseBody`/`WithoutBodies`, additive-only `WithRedactHeaders`, `WithSummaryWriter`) live in `httptap/payload.go`. `NewAccessHandler(out, nextFunc)` is the lightweight one-line-per-request access log, writing to any `io.Writer` (no `*Logger`).

The `interceptor` doesn't implement `http.Flusher`/`Hijacker`/`Pusher` directly; `wrapWithOptionalInterfaces` returns one of eight composite `*RW` types advertising exactly the optional interfaces the underlying `ResponseWriter` has, so streaming/SSE/WebSocket/HTTP-2-push work through the middleware (`io.ReaderFrom`/`http.CloseNotifier` are not bridged).

### Cross-cutting helpers

`error.go` (`TraceError`, `HttpError`), `json.go` (`JsonEncode`/`JsonDecode`; `…AndClose` variants close after, `…Ex` variants also return raw bytes; optional `indent` variadic pretty-prints), `closers.go` (`CloseOr*` for `defer`-ing `io.Closer` cleanup).

## Conventions

Generic Go conventions (style, declaration order, test structure, godoc, error discipline,
build hygiene, organisation) come from the `stack-go` plugin skills and are not restated
anywhere in this repo. Project-specific constraints:

- **No CGO**: keep the package cgo-free (`CGO_ENABLED=0` builds clean); the `-race`
  gate is the exception — it needs cgo (see Build & Test). The only direct deps are
  `stretchr/testify` (tests) and `twinj/uuid` (hook IDs) — avoid adding heavy or
  CGO-dependent dependencies.
- **Race coverage is load-bearing**: concurrency-sensitive handlers have dedicated
  `…ForRaceCondition` tests — keep that coverage when touching the fan-out or the
  file handlers; tests always run with `-race`.
- **Legacy test names**: some existing tests (e.g. `TestJsonEncode_indent`,
  `TestFileByFormatHandlerV2`) predate the one-`Test*`-per-method rule; prefer the
  canonical subtest form for new and refactored tests, don't mass-rename.
- **No Makefile**: gates run as raw Go commands (`gofmt -l .`, `go vet ./...`,
  `CGO_ENABLED=1 go test -race ./...` — race needs cgo on this arm64 host).

## Working agreement

Plan-first pipeline; the canonical procedure is the `pipeline:working-agreement` skill — load it
before starting non-trivial work. Project delta:

- **Gate:** `gofmt -l .` clean, then `go vet ./...` and `CGO_ENABLED=1 go test -race ./...` green
  — the race detector needs cgo on this arm64 host.
- **Lenses:** standard three.
- **Branching:** standard (`type/<issue>-<slug>`, PR into `main`); long-running feature branches
  squash-merge, which the Release & CHANGELOG workflow below depends on.

## Release & CHANGELOG workflow

Long-running feature branches **squash-merge** into `main` — the whole branch lands as
one commit. The CHANGELOG entry for that release is therefore **one version section**
covering everything in the branch, not a sequence of per-commit entries written as the
branch grows.

While a branch is in flight, accumulate under `[Unreleased]` (Keep a Changelog format); on release promote it to `[X.Y.Z] - YYYY-MM-DD` and reset the stub. Next version = latest tag (`git tag --sort=-version:refname | head -1`) + implied SemVer bump (breaking → major, new `Added` → minor, else patch).

**Before writing or updating `CHANGELOG.md`, read the file first** even if you think
it doesn't exist — `ls CHANGELOG* CHANGES*` from `fish` aborts on the first non-matching
glob and silently skips the rest, so a present `CHANGELOG.md` can look absent. A prior
session may have already started the `[Unreleased]` entry; you may be completing it
rather than writing from scratch.
