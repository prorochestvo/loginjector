# loginjector

`github.com/prorochestvo/loginjector` is a Go logging library, not a service: a `Logger`
fans each message out to `io.Writer` sinks (handlers and level hooks). Consumers embed it;
there is no binary, database, or server.

## Commands

- `make test` — race suite (`CGO_ENABLED=1`; on Linux `-race` needs cgo and a C compiler).
- `make verify` — every check; the list is the `verify` prerequisites in `Makefile`. The
  golangci-lint pin is `GOLANGCI_VERSION` in `Makefile`; `make lint-install` installs it.
- `make format` / `make tidy` rewrite files; no other target does.
- CI: `.github/workflows/ci.yml` runs `make verify`, and `make test` and `make build` on the
  `go.mod` floor.

## Map

| Path | Role |
|---|---|
| `logger.go` | `Logger`: fan-out, hooks, `LeveledHandler`, `WriterAs`, `StdLog`, `Panicf`/`Panic` |
| `handler.go` | handler factories: console, rotating file, file-by-format, Telegram, `WithMinLevel` |
| `filelogger.go` | `NewFileLogger` bootstrap and its options |
| `error.go`, `closers.go` | error types (`TraceError`, `StackTraceError`, `PublicError`, `HttpError`); `CloseOr*` |
| `levels/` | optional `Debug`..`Critical` ladder, `Parse`/`Name` |
| `httptap/` | opt-in HTTP middleware: payload, access, recover; the root package never imports it |
| `internal/` | stack-trace parsing for `error.go`; `internal/synctest` is test-only scaffolding |
| `gorules/`, `scripts/` | custom lint rules (ruleguard DSL) and text-level checks |

## Tripwires

- Go conventions come from the `stack-go` skills and the owner's R# rules; declaration
  order is R16; `funcorder` enforces only constructor and method placement.
- Local `make verify` runs on the current toolchain and does not catch standard-library
  APIs newer than the `go.mod` floor (1.22.2); CI's floor job does. It cannot be
  reproduced locally: Go 1.22.2 test binaries abort on macOS 27.
- `LogLevel` is a bare `int`; the root package defines no level constants. Higher is more
  severe.
- Hooks fire on an exact level match even below the minimum level. A handler fires at
  or above its own `MinLevel()` when it implements `LeveledHandler` (`WithMinLevel`),
  otherwise at or above the logger minimum.
- Within one logger, writes to a pointer sink are serialised per sink identity (a sink
  shared by two loggers is not); the logger releases its lock before sinks run and
  returns a sink panic as an error. `Write`/`WriterAs` report `len(p)` for a message
  filtered below the threshold.
- Deadlocks (see the `Logger` godoc): a sink re-entering itself, including through a
  second registration of the same sink; std `log` re-entry from a sink; a sink writing
  to the `WriterAs` adapter the outer call came through.
- `Panicf`/`Panic` panic; the library never calls `os.Exit`.
- `TelegramHandler` is a synchronous HTTP POST (20 s timeout): a matching message blocks
  its caller.
- `httptap` redaction: the default header and query-parameter sets in
  `httptap/redact.go` are always on and case-insensitive; options only add names. Do not
  weaken them.
- Runtime dependencies: standard library only (`testify` is test-only; `gorules` needs
  `go-ruleguard/dsl` and is never compiled); the module builds with `CGO_ENABLED=0`.
- Keep the `…ForRaceCondition` tests when touching the fan-out or the file handlers.
- `TestFileByFormatHandlerV2` predates the one-`Test*`-per-symbol rule; do not rename
  legacy tests in unrelated changes.

## Working agreement

Plan-first pipeline; the canonical procedure is the `pipeline:working-agreement` skill — load it
before starting non-trivial work. Project delta:

- **Gate:** `make verify`.
- **Review focus:** standard staged set — see `pipeline:working-agreement`.
- **Branching:** `dev` is the integration branch: `type/<issue>-<slug>` → PR into `dev`,
  squash-merged, CI green. `main` holds released code only and is never committed to
  directly.

## Release and CHANGELOG

- A branch lands in `dev` as one squash commit and adds its entries under `[Unreleased]`.
- A release is a PR into `dev` that promotes `[Unreleased]` to `[X.Y.Z] - YYYY-MM-DD`, adds
  its compare link, and moves `[Unreleased]`'s link base to the new tag. After it merges,
  `main` is fast-forwarded to `dev` (`git push origin origin/dev:main`; never a squash or
  merge commit, which would make the branches diverge) and the tag goes on that commit.
  Moving `main` and tagging are the owner's steps.
- The 1.0.x line is pre-stable: next version = latest tag
  (`git tag --sort=-version:refname | head -1`) + PATCH, regardless of `Added` or
  breaking entries; breaking changes are documented in the version's
  `BREAKING CHANGES` section.
- Read `CHANGELOG.md` before editing it: under `fish`, `ls CHANGELOG*` aborts on the first
  unmatched glob and can hide the file; an `[Unreleased]` section may already exist.
