package loginjector

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"
)

// FileLoggerOption configures NewFileLogger.
type FileLoggerOption func(*fileLoggerConfig)

// WithMaxFileCapacity overrides the maximum size of a single log file in bytes before
// the handler rotates to the next file. The default is 5 MiB (5 * 1024 * 1024). The
// underlying RotatingFileHandler takes a uint32, so values larger than
// math.MaxUint32 are not valid; pass a reasonable capacity.
func WithMaxFileCapacity(n uint32) FileLoggerOption {
	return func(c *fileLoggerConfig) { c.maxFileCapacity = n }
}

// WithMaxFilesInFolder overrides the maximum number of rotated log files retained in
// the log folder. Older files beyond the limit are pruned on rotation. The default is 7.
func WithMaxFilesInFolder(n int) FileLoggerOption {
	return func(c *fileLoggerConfig) { c.maxFilesInFolder = n }
}

// WithMaxFileAge forwards RotatingFileHandler's WithMaxAge to the underlying rotating
// handler: rotated files whose mtime is older than d are pruned on rotation, on top of the
// WithMaxFilesInFolder count bound. A d of zero or negative disables age pruning, which is
// the default. The unit is a time.Duration, so pass the full span —
// WithMaxFileAge(14 * 24 * time.Hour) for two weeks, not WithMaxFileAge(14) (14 ns).
func WithMaxFileAge(d time.Duration) FileLoggerOption {
	return func(c *fileLoggerConfig) { c.maxAge = d }
}

// WithFileCompression forwards RotatingFileHandler's WithCompress to the underlying
// rotating handler: each rotated file is gzipped to prefix.<8 hex>.log.gz and the plaintext
// removed, synchronously on the triggering write. It is OFF by default.
//
// RotatingFileHandler's WithStableCurrentName is deliberately NOT exposed here: NewFileLogger
// wraps the handler in TimestampedHandler, prefixing every line with a timestamp, which is at
// odds with a stable tail-able access-log format. Consumers wanting the fixed prefix.log live
// path build a RotatingFileHandler directly instead.
func WithFileCompression() FileLoggerOption {
	return func(c *fileLoggerConfig) { c.compress = true }
}

// WithTempDirFallback enables the temp-directory fallback: when the folder argument to
// NewFileLogger is empty, logs are written under filepath.Join(os.TempDir(), "logs")
// instead of returning an error. This option is OFF by default — an empty folder
// without this option is a hard error, so logs never silently land in an unexpected
// location. Note that the shared temp path is a residual risk: on multi-user systems
// another process could create the directory before NewFileLogger and own it with
// permissive permissions; NewFileLogger calls os.Chmod after MkdirAll to enforce 0750,
// but there is a brief window between directory creation and the chmod call.
func WithTempDirFallback() FileLoggerOption {
	return func(c *fileLoggerConfig) { c.tempFallback = true }
}

// WithStdLogRedirect redirects the standard library log package output to the new
// logger at the given level and sets log.Flags to 0. This option is OFF by default
// because log.SetOutput mutates global process state shared by every package in the
// binary — opting in is a conscious, explicit decision. If WithStdLogRedirect is
// passed more than once, the last call wins. If two NewFileLogger calls in the same
// process both use this option, the second call silently wins: log.SetOutput is
// global and the first logger will no longer receive standard library log output.
func WithStdLogRedirect(level LogLevel) FileLoggerOption {
	return func(c *fileLoggerConfig) {
		c.stdLogRedirect = true
		c.redirectLevel = level
	}
}

// WithoutPrinter disables the timestamped stdout printer that NewFileLogger attaches
// by default. Use this when you want only the rotated file output and no console echo.
func WithoutPrinter() FileLoggerOption {
	return func(c *fileLoggerConfig) { c.printer = false }
}

// WithPrinterOptions forwards PrintOption values to the TimestampedPrintHandler
// created by NewFileLogger. Has no effect when combined with WithoutPrinter.
func WithPrinterOptions(opts ...PrintOption) FileLoggerOption {
	return func(c *fileLoggerConfig) { c.printerOpts = append(c.printerOpts, opts...) }
}

// WithPrinterMinLevel overrides the threshold at which the console printer
// emits messages, decoupling it from the file-handler threshold (minLevel).
// When not set, the printer fires at minLevel (current behaviour). When set,
// the console printer fires at level >= this value while the file handler
// continues to fire at level >= minLevel.
//
// Has no effect when combined with WithoutPrinter — WithoutPrinter wins.
//
// Implemented by wrapping the printer handler in WithMinLevel before adding
// it to the Logger handler list; the per-handler gate in Logger.WriteLog
// does the rest.
func WithPrinterMinLevel(level LogLevel) FileLoggerOption {
	return func(c *fileLoggerConfig) {
		c.printerMinLevel = level
		c.printerMinLevelSet = true
	}
}

// NewFileLogger builds a Logger that writes to size-rotated files under folder and,
// by default, mirrors every message at or above minLevel to a timestamped stdout
// printer (TimestampedPrintHandler). The printer fires as a handler (level >= minLevel),
// not a hook, so its gating matches the file handler exactly.
//
// File lines are timestamped: the rotated file handler is wrapped in
// TimestampedHandler, so each on-disk line is "<timestamp> <message>" (layout
// "2006/01/02 15:04:05"), not the bare message. The console printer and the file
// handler are two separate sinks, each stamped exactly once.
//
// The standard library log package is NOT redirected unless WithStdLogRedirect is
// passed — that redirect mutates global process state. An empty folder is rejected
// unless WithTempDirFallback is passed, in which case logs land under
// filepath.Join(os.TempDir(), "logs"). An empty prefix is always rejected because a
// hidden dotfile log (prefix ".") is a footgun.
//
// Defaults: maxFileCapacity = 5 MiB, maxFilesInFolder = 7, printer ON, std-log
// redirect OFF, temp fallback OFF.
func NewFileLogger(folder, prefix string, minLevel LogLevel, opts ...FileLoggerOption) (*Logger, error) {
	cfg := fileLoggerConfig{
		maxFileCapacity:  5 << 20,
		maxFilesInFolder: 7,
		printer:          true,
	}
	for _, o := range opts {
		o(&cfg)
	}

	if prefix == "" {
		return nil, errors.New("loginjector: file logger prefix must not be empty")
	}

	// reject the dot entries — they produce a confusing in-folder dotfile rather than
	// a named log file.
	if prefix == "." || prefix == ".." {
		return nil, fmt.Errorf("loginjector: file logger prefix %q must be a plain file name", prefix)
	}

	// reject a prefix that contains path separators — it would escape the log folder.
	if filepath.Base(prefix) != prefix {
		return nil, fmt.Errorf("loginjector: file logger prefix %q must not contain path separators", prefix)
	}

	if folder == "" {
		if !cfg.tempFallback {
			return nil, errors.New("loginjector: empty folder and temp fallback not enabled")
		}
		folder = filepath.Join(os.TempDir(), "logs")
	}

	if err := os.MkdirAll(folder, 0o750); err != nil {
		return nil, fmt.Errorf("loginjector: create log folder %q: %w", folder, err)
	}
	// MkdirAll does not chmod a pre-existing directory; enforce 0o750 explicitly.
	if err := os.Chmod(folder, 0o750); err != nil {
		return nil, fmt.Errorf("loginjector: set permissions on log folder %q: %w", folder, err)
	}

	rotatingOpts := []RotatingFileOption{
		WithMaxFileSize(cfg.maxFileCapacity),
		WithMaxFiles(cfg.maxFilesInFolder),
	}
	// only append the additive options when active, so the zero-new-option call is
	// byte-identical to the size/count-only handler it replaced.
	if cfg.maxAge > 0 {
		rotatingOpts = append(rotatingOpts, WithMaxAge(cfg.maxAge))
	}
	if cfg.compress {
		rotatingOpts = append(rotatingOpts, WithCompress())
	}

	handlers := []io.Writer{
		// stamp file lines sink-side (blessed emitters use Lmsgprefix only, so nothing
		// else stamps the file); the RotatingFileHandler stays the underlying sink.
		TimestampedHandler(RotatingFileHandler(folder, prefix, rotatingOpts...)),
	}
	if cfg.printer {
		p := TimestampedPrintHandler(cfg.printerOpts...)
		if cfg.printerMinLevelSet {
			p = WithMinLevel(cfg.printerMinLevel, p)
		}
		handlers = append(handlers, p)
	}

	l := NewLogger(minLevel, handlers...)

	if cfg.stdLogRedirect {
		log.SetOutput(l.WriterAs(cfg.redirectLevel))
		log.SetFlags(0)
	}

	return l, nil
}

// fileLoggerConfig holds the resolved configuration for NewFileLogger.
type fileLoggerConfig struct {
	maxFileCapacity    uint32
	maxFilesInFolder   int
	maxAge             time.Duration // WithMaxFileAge; forwarded to RotatingFileHandler.
	compress           bool          // WithFileCompression; forwarded to RotatingFileHandler.
	tempFallback       bool
	printer            bool
	printerOpts        []PrintOption
	printerMinLevel    LogLevel
	printerMinLevelSet bool // disambiguates explicit-zero from unset.
	stdLogRedirect     bool
	redirectLevel      LogLevel
}
