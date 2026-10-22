# Changelog

All notable changes to this project will be documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- `TelegramHandler` now wraps its multipart-construction errors with `%w` instead of `%s`.
  The error messages are byte-identical; `errors.Is`/`errors.As` can now reach the
  underlying error. Errors that carry the redacted bot-token URL are unchanged and still
  do not wrap the original error.
- The whole tree passes the adopted golangci-lint configuration (`make lint`). Production
  changes are behaviour-preserving (octal literals, `errors.New` for constant messages,
  `http.NewRequestWithContext`, declaration order, removal of the unused internal
  `newInterceptor`). Tests run in parallel where isolated; tests that touch process-global
  state stay serial with a recorded reason.

### Fixed

- `RotatingFileHandler` without options pruned every `*.log` file in its folder, whatever
  its name, when the file count exceeded `WithMaxFiles` — deleting log files of other
  components and the live file of another handler sharing the folder (including
  `NewFileLogger`, which always takes this path). Pruning now counts and removes only the
  handler's own `prefix.<8 hex>.log` files. `FileByFormatHandler` keeps whole-folder
  pruning by contract; its godoc now says so.
- `Logger.Unhook` panicked (`makeslice: cap out of range`) on a logger with no hooks, e.g.
  when the same hook was removed twice.
- `Logger.Write` read the minimum level without the lock, a data race with a concurrent
  `SetMinLevel`.
- `TestRotatingFileHandlerForRaceCondition` joined errors from several goroutines without a
  lock, and `TestFileByFormatHandlerForRaceCondition` called `require` off the test
  goroutine; both are corrected. `TestLogger_Write` now restores the std `log` output.

## [1.0.9] - 2026-07-22

### Security

- `TelegramHandler` no longer leaks the bot token into surfaced error strings. When the
  request build or `client.Do` failed, the returned `*url.Error` embedded the full
  `.../bot<TOKEN>/sendDocument` URL — token included — which then landed in logs and log
  files. Both URL-bearing return sites now redact the token to `***` (the literal `bot`
  prefix is kept). A `len(botToken) < 8` guard skips redaction for tokens too short to be a
  valid Telegram token, so unrelated substrings are never over-matched.

### Added

- `WithFileMode(os.FileMode)` for `RotatingFileHandler` overrides the permission bits used
  when the handler CREATES a log file — the live `prefix.<8 hex>.log` (or `prefix.log` under
  `WithStableCurrentName`) and the gzip `.log.gz` temp under `WithCompress`. The default
  stays `0640` and is byte-identical when the option is unused. It applies only to files the
  handler creates (the process umask still masks the bits) and never re-`chmod`s a
  pre-existing `O_APPEND`-opened live file. `FileByFormatHandler` is intentionally left on
  the hardcoded `0640`.
- `WithMaxAgeDays(int)` is a units-safe wrapper over `WithMaxAge` that prunes rotated backups
  older than the given number of days, sidestepping the `WithMaxAge(14)`-is-14-nanoseconds
  footgun.
- `StackRedacted(e StackTraceError) string` — a package-level function returning `e.Stack()`
  with each frame's absolute build-host path shortened to `<parent>/<base>` — the identical
  reduction `LineTrace` applies — so the trace can be surfaced where `Stack()` (which carries
  absolute paths) cannot. Line numbers, the goroutine header, and frame ordering are
  preserved. It is a function, not a method on the `StackTraceError` interface, so the
  exported interface stays sealed and external implementers are not broken.
- `RuntimeDetailsFrozen() bool` is an advisory probe reporting whether the process-wide
  runtime-details string has already been computed and cached; once true, a later
  `SetRuntimeDetailsProvider` call is a no-op. It is a diagnostic (freeze-state only), not a
  synchronization primitive.
- `NewTraceErrorSkip(skip int)` exposes the frame-skip primitive that `NewTraceError` now
  delegates to (`NewTraceError` calls `NewTraceErrorSkip(1)` to absorb the extra delegation
  hop), for wrapping the constructor in a helper while still resolving the caller's frame.

### Changed

- The `SetRuntimeDetailsProvider` result is now sanitized to keep `Runtime()` single-line:
  CR/LF/TAB are collapsed to spaces and other control characters are stripped, so a
  multi-line provider block can no longer break the single-line log contract other sinks
  assume. Printable Unicode is preserved and a clean single-line result is byte-identical to
  before.

## [1.0.8] - 2026-07-20

### Added

- `RotatingFileHandler` options for lumberjack parity — all opt-in and OFF by default, so a
  zero-option handler stays byte-identical to the previous one:
  - `WithStableCurrentName()` keeps the live file at a fixed `prefix.log` path (rotated
    backups stay indexed `prefix.<8 hex>.log`) so external tooling can follow it with
    `tail -F`. Resume seeds size from `prefix.log` and adopts legacy indexed files as
    backups.
  - `WithMaxAge(d)` prunes rotated backups older than `d` (by mtime) on rotation, on top of
    the `WithMaxFiles` count bound; `d <= 0` disables it. The unit is a `time.Duration`, so
    pass the full span — `WithMaxAge(14 * 24 * time.Hour)`, not `WithMaxAge(14)`.
  - `WithCompress()` gzips each rotated backup to `prefix.<8 hex>.log.gz` and removes the
    plaintext, synchronously under the handler mutex and crash-safely (temp file + fsync +
    rename, then remove), preserving the source mtime so age pruning stays honest. A
    crash-interrupted compression self-heals on the next construction (the `.gz` wins).
- `NewFileLogger` gains `WithMaxFileAge(d)` and `WithFileCompression()`, forwarding the two
  options above to the underlying rotating handler. `WithStableCurrentName` is intentionally
  not forwarded — `NewFileLogger` timestamps every file line, which is at odds with a stable
  tail-able access-log format.

### Fixed

- `FileByFormatHandler` now surfaces its file-close error on the `Write` return instead of
  silently discarding it: the per-write closure had unnamed returns, so a deferred close that
  assigned into `err` was dropped after the return value was copied.

## [1.0.7] - 2026-07-18

### Changed

- `internal.StackTrace()` now calls `runtime.Stack` directly with an 8 KiB initial
  buffer (doubling only if a deeper stack does not fit, capped at 4 MiB) instead of
  `debug.Stack`, which starts at 1 KiB and doubles. This is roughly 3× faster on deep
  (production-depth) stacks, with byte-identical output and no public API change.

## [1.0.6] - 2026-07-18

### **BREAKING CHANGES**

The HTTP middleware has been extracted from the root `loginjector` package into the new
opt-in sub-package `github.com/prorochestvo/loginjector/httptap`. The middleware is no
longer mixed with the fan-out logger, all `Http`-prefixed identifiers have shed their
stutter (`httptap.NewPayloadHandler` reads; `httptap.NewHttpPayloadHandler` did not),
and the freed namespace positions future transport-tap siblings (`grpctap/`, `sqltap/`,
…).

This release also folds in the pre-tag API finalization: dead error returns are dropped,
lying names are corrected, the two cyclic file handlers collapse into one option-based
`RotatingFileHandler`, and unused public surface is pruned. All consumers are in-house
and pinned; there is no shim layer and no deprecation window.

**Full rename table:**

| Old                                             | New                                                                    |
| ----------------------------------------------- | ---------------------------------------------------------------------- |
| `loginjector.NewHttpPayloadHandler`             | `httptap.NewPayloadHandler`                                            |
| `loginjector.NewHttpPayloadHandlerWithOptions`  | `httptap.NewPayloadHandlerWithOptions`                                |
| `loginjector.NewHttpAccessHandler`              | `httptap.NewAccessHandler`                                            |
| `loginjector.HttpPayloadOption`                 | `httptap.PayloadOption`                                               |
| `loginjector.WithMaxRequestBody`                | `httptap.WithMaxRequestBody`                                          |
| `loginjector.WithMaxResponseBody`               | `httptap.WithMaxResponseBody`                                         |
| `loginjector.WithoutBodies`                     | `httptap.WithoutBodies`                                              |
| `loginjector.WithRedactHeaders`                 | `httptap.WithRedactHeaders`                                          |
| `loginjector.WithSummaryWriter`                 | `httptap.WithSummaryWriter`                                          |
| `Logger.Fatalf`                                 | `Logger.Panicf`                                                     |
| `Logger.Fatal`                                  | `Logger.Panic`                                                      |
| `CyclicOverwritingFilesHandler`                 | `RotatingFileHandler(folder, prefix, WithMaxFileSize(…), WithMaxFiles(…))` |
| `CyclicOverwritingFilesHandlerWithReset`        | `RotatingFileHandler(folder, prefix, …, WithFreshStart())`         |
| `Logger.JoinAs(level, log.SetOutput)`           | `log.SetOutput(Logger.WriterAs(level))`                             |
| `SilenceHandler`                                | removed — use `io.Discard`                                          |
| `SafeBuffer` / `NewBuffer` / `NewBufferString`  | removed from the public API (test-only, relocated internally)      |
| `CloseOrPrintLn`                                | removed                                                            |
| `CloseOrIgnore`                                 | removed — use `_ = c.Close()` or `CloseOrLog`                      |
| `JsonEncode` / `JsonDecode` / `Json*`           | removed                                                            |

**Signature and behaviour changes:**

- `NewLogger(min, handlers...)` returns `*Logger` (no `error`). It never had a failure
  path. Callers drop the `, err` and the `if err != nil` / `require.NoError` that
  followed.
- The four `httptap` constructors — `NewPayloadHandler`, `NewPayloadHandlerWithOptions`,
  `NewAccessHandler`, `NewRecoverHandler` — return `http.HandlerFunc` only and **panic**
  on a nil required argument (logger/out/next) instead of returning an error. A nil
  argument is a programmer error, not a runtime condition.
- `Logger.Fatalf`/`Fatal`, which `panic` and never call `os.Exit`, are renamed to
  `Panicf`/`Panic` to match the semantics of the standard library's `log` package. The
  bodies are unchanged.
- `CyclicOverwritingFilesHandler` and `CyclicOverwritingFilesHandlerWithReset` are
  replaced by a single `RotatingFileHandler(folder, prefix, opts...)`. The reset variant
  is now the `WithFreshStart()` option. The handler also resumes at the highest existing
  file index across a process restart instead of restarting at index 1 (previously it
  could delete the newest data on restart).
- `httptap` request/response body capture now defaults to a 64 KiB cap per body instead
  of unlimited. Pass `WithMaxRequestBody(-1)` / `WithMaxResponseBody(-1)` to restore
  unlimited capture. The downstream handler/client always receives the full body; only
  the logged copy is capped.
- `NewFileLogger` on-disk line format changes from `"<message>"` to
  `"<timestamp> <message>"` (layout `2006/01/02 15:04:05`). Anything that parses those
  files by column position must account for the leading timestamp.
- The default console sink for a handler-less `NewLogger` changes from the old
  builtin-`println`-to-**stderr** `PrintHandler` to `TimestampedPrintHandler`
  (timestamped, **stdout**). `PrintHandler` is kept but repurposed to a plain
  **stdout** writer (was stderr) and is no longer the default.
- `HookID` values are now formatted as `"hook-<n>"` from a per-logger counter instead of
  a UUID. IDs are unique within a single logger. This is breaking only for code that
  persisted or pattern-matched the old UUID shape.

**Migration:** replace HTTP-handler call sites with the `httptap` sub-package per the
table above; the root import is still required for `*loginjector.Logger` and
`loginjector.LogLevel`. Drop the `error` handling from `NewLogger` and the four
`httptap` constructors. Rename `Fatalf`/`Fatal` to `Panicf`/`Panic`. Replace
`CyclicOverwritingFilesHandler*` with `RotatingFileHandler`. There is no shim layer and
no two-version deprecation window.

### Added

- `levels/` sub-package with `Debug`, `Info`, `Warning`, `Error`, `Severe`, `Critical`
  constants (values 1..6) and a `Parse(string) LogLevel` helper. Replaces the per-consumer
  `type LogLevel int` enums that every downstream project was reinventing.
- `NewFileLogger(folder, name, minLevel, options...)` — one-call bootstrap that wires
  `RotatingFileHandler` rotation and, by default, a timestamped console printer. File
  lines are timestamped. An empty folder is rejected unless `WithTempDirFallback` is
  passed, in which case logs land under `os.TempDir()/logs`; the standard `log` package
  is redirected only when `WithStdLogRedirect` is passed (off by default).
- `TimestampedPrintHandler(opts...)` — console handler with `2006/01/02 15:04:05`
  timestamps and a computed continuation indent for multi-line messages.
- `RotatingFileHandler(folder, prefix, opts...)` — size-based rotation with
  `WithMaxFileSize` (default 5 MiB), `WithMaxFiles` (default 7), and `WithFreshStart`
  (truncate the index-1 file at construction). Resumes at the highest existing index
  across a restart and prunes the oldest files past the bound.
- `TimestampedHandler(inner, opts...)` — composable decorator that stamps each message
  with a timestamp and forwards it to any `io.Writer`; `TimestampedPrintHandler` is its
  `os.Stdout` instantiation.
- `Logger.StdLog(level, prefix)` — returns a standard-library `*log.Logger` wired with
  `log.Lmsgprefix` (no emitter-side date/time), so paired with a timestamped sink a line
  carries exactly one timestamp.
- `FileByFormatHandler(folder, max, generator)` — rotation driven by a user-supplied
  filename generator (e.g. date-stamp), pruning on filename change.
- `LeveledHandler` interface + `WithMinLevel(level, handler) io.Writer` helper —
  handlers can declare a per-handler minimum log level, decoupling them from the
  `Logger`'s global minimum. `WithPrinterMinLevel` option on `NewFileLogger` composes
  this so consumers can ship the common "file at Warning, console at Info" shape
  without rolling their own logger factory.
- `StackTraceError` interface and `NewStackTraceError`, `NewStackTraceErrorf`,
  `WrapStackTraceError` constructors. Captures the full goroutine stack at construction
  time plus a process-wide cached runtime descriptor (`os=… arch=… go=…`).
- `SetRuntimeDetailsProvider(func() string)` — process-wide hook that appends
  consumer-supplied detail (CPU model, RAM, PID, etc.) to `StackTraceError.Runtime()`
  without forcing a dependency such as `gopsutil` into this library. A panicking
  provider is recovered and the base descriptor is used as the fallback.
- `PublicError` interface + `NewPublicError(public, cause)` — separates client-safe
  text from internal cause; `Unwrap`, `errors.Is`, and `errors.As` traverse the cause
  chain.
- `PublicDetailsError` interface + `NewPublicErrorDetails(parts...)` — variadic
  convenience constructor matching the common consumer shape (space-joined parts, no
  cause, `Details()` accessor that aliases `PublicMessage()`).
- `HttpError` interface + `NewHttpError(code)` and `WrapHttpError(code, cause)`.
  `WrapHttpError` falls back to `"HTTP <code>"` for unknown status codes so the error
  string is never empty.
- `TraceError` Wrap variants — `NewTraceErrorf(format, args...)` and
  `WrapTraceError(cause, format, args...)` — joinable with `errors.As` and `errors.Is`
  via `Unwrap`.
- Closer helpers in `closers.go`: `CloseOrPanic`, `CloseOrLog`, `CloseOrLogError`,
  `CloseOrPrintLn`, `CloseOrIgnore` for `defer`-ing `io.Closer` cleanup with predictable
  failure semantics.
- `httptap.NewRecoverHandler(logger, level, next, opts...)` — buffered panic-recover
  middleware. Panics before AND after `WriteHeader` are converted to a clean HTTP 500
  with a configurable JSON body. `http.ErrAbortHandler` is re-panicked silently.
  Options: `WithFallbackResponse`, `WithFallbackContentType`, `WithPublicMessage`.
  **Note:** the wrapper buffers the response, so streaming routes (SSE / WebSocket /
  HTTP-2 push) lose streaming semantics — wrap only non-streaming routes.
- `httptap.NewAccessHandler(out, next)` — single-line-per-request access log
  (RFC3339Nano timestamp, status, method, path, duration) that writes to any
  `io.Writer` without requiring a `*Logger`.
- `httptap.NewPayloadHandlerWithOptions(logger, level, next, opts...)` — strict
  superset of `NewPayloadHandler` with the same byte-identical output when no options
  are passed. Options: `WithMaxRequestBody(n)`, `WithMaxResponseBody(n)`,
  `WithoutBodies()`, `WithRedactHeaders(...)`, `WithSummaryWriter(w)`.
- Eight composite `*RW` types in `httptap` that automatically expose the underlying
  `http.ResponseWriter`'s optional interfaces (`http.Flusher`, `http.Hijacker`,
  `http.Pusher`) to downstream handlers, so SSE / WebSocket / HTTP-2 push still work
  through `NewPayloadHandler` and `NewAccessHandler`.

### Changed

- `internal.LineTrace` rewritten on top of `runtime.Caller` + `runtime.FuncForPC`
  instead of parsing `debug.Stack()` text. Roughly 7× faster and 6.5× leaner per call,
  with no public API change. Now also handles top-level generic function names
  correctly (previous parser produced `]` for instantiated generic functions).
- HTTP payload summary output is configurable per-handler via `WithSummaryWriter(w)`;
  default remains `os.Stdout`.
- File handlers default to permissions `0640` (was `0644`) so log files are not
  world-readable by default.

### Removed

- `loginjector.NewHttpPayloadHandler` — moved to `httptap.NewPayloadHandler`.
- `loginjector.NewHttpPayloadHandlerWithOptions` — moved to `httptap.NewPayloadHandlerWithOptions`.
- `loginjector.NewHttpAccessHandler` — moved to `httptap.NewAccessHandler`.
- `loginjector.HttpPayloadOption` — moved to `httptap.PayloadOption`.
- `loginjector.WithMaxRequestBody`, `WithMaxResponseBody`, `WithoutBodies`,
  `WithRedactHeaders`, `WithSummaryWriter` — moved to `httptap.*` unchanged.
- `loginjector.SetHttpSummaryWriter` — **removed entirely** (not relocated). The
  per-handler `WithSummaryWriter` option is a strict superset; there are no known
  callers of the removed global.

### Fixed

- Data races in `Logger` fan-out: every handler and hook write is now serialized
  through a per-sink mutex wrapper installed at registration time, so a non-thread-safe
  handler (e.g. `bytes.Buffer`) is safe under concurrent `WriteLog` calls.
- Errors are no longer silently swallowed in `WriteLog`, file rotation, or HTTP
  middleware paths; concurrent sink errors are collected under a mutex and joined via
  `errors.Join` after the wait-group resolves.
- File rotation no longer leaves stale files past the configured cap: pruning is
  deterministic and respects the configured `maxFilesInFolder` regardless of process
  restart timing.

[Unreleased]: https://github.com/prorochestvo/loginjector/compare/v1.0.7...HEAD
[1.0.7]: https://github.com/prorochestvo/loginjector/compare/v1.0.6...v1.0.7
[1.0.6]: https://github.com/prorochestvo/loginjector/compare/v1.0.5...v1.0.6
