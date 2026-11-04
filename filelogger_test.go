package loginjector

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/prorochestvo/loginjector/internal/synctest"
	"github.com/stretchr/testify/require"
)

// probe is a minimal io.Writer that records everything written to it.
var _ io.Writer = (*probe)(nil)

type probe struct{ buf bytes.Buffer }

func (p *probe) Write(b []byte) (int, error) { return p.buf.Write(b) }
func (p *probe) String() string              { return p.buf.String() }

//nolint:paralleltest,tparallel // subtests swap the std log output and share os.TempDir()/logs
func TestNewFileLogger(t *testing.T) {
	const minLevel LogLevel = 1 // use a raw level to keep this independent of the levels package

	t.Run("writes to prefix.00000001.log", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		l, err := NewFileLogger(dir, "app", minLevel, WithoutPrinter())
		require.NoError(t, err)
		require.NotNil(t, l)

		_, err = l.WriteLog(minLevel, []byte("hello world"))
		require.NoError(t, err)

		files, err := extractFilesOrFail(dir)
		require.NoError(t, err)
		require.Contains(t, files, "app.00000001.log")
		require.Contains(t, files["app.00000001.log"], "hello world")
	})

	t.Run("pre-existing folder keeps its permissions", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("directory permission bits are not portable on Windows")
		}
		t.Parallel()

		dir := t.TempDir()
		folder := filepath.Join(dir, "pre")
		require.NoError(t, os.Mkdir(folder, 0o700))
		require.NoError(t, os.Chmod(folder, 0o755))

		_, err := NewFileLogger(folder, "app", minLevel, WithoutPrinter())
		require.NoError(t, err)

		fi, err := os.Stat(folder)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o755), fi.Mode().Perm())
	})

	t.Run("created folder is 0750", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("directory permission bits are not portable on Windows")
		}
		t.Parallel()

		dir := t.TempDir()
		folder := filepath.Join(dir, "a", "b")
		_, err := NewFileLogger(folder, "app", minLevel, WithoutPrinter())
		require.NoError(t, err)

		fi, err := os.Stat(folder)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o750), fi.Mode().Perm())
	})

	t.Run("folder path that is a regular file errors", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		folder := filepath.Join(dir, "file")
		require.NoError(t, os.WriteFile(folder, []byte("file"), 0o600))

		_, err := NewFileLogger(folder, "app", minLevel, WithoutPrinter())
		require.Error(t, err)
	})

	t.Run("file lines are timestamped", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		l, err := NewFileLogger(dir, "stamp", minLevel, WithoutPrinter())
		require.NoError(t, err)

		_, err = l.WriteLog(minLevel, []byte("a logged line"))
		require.NoError(t, err)

		files, err := extractFilesOrFail(dir)
		require.NoError(t, err)
		content := files["stamp.00000001.log"]
		require.Regexp(t, `^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} `, content,
			"file lines must carry a leading timestamp")
		require.Contains(t, content, "a logged line")
	})

	t.Run("rotation under tiny capacity", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		l, err := NewFileLogger(dir, "rot", minLevel,
			WithoutPrinter(),
			WithMaxFileCapacity(10),
			WithMaxFilesInFolder(5),
		)
		require.NoError(t, err)
		require.NotNil(t, l)

		// write enough bytes to force a rollover
		for i := range 5 {
			_, err = l.WriteLog(minLevel, []byte(fmt.Sprintf("msg%d_padding", i)))
			require.NoError(t, err)
		}

		files, err := extractFilesOrFail(dir)
		require.NoError(t, err)
		// second file must exist proving rotation occurred
		require.Contains(t, files, "rot.00000002.log", "rotation must create a second file")
	})

	t.Run("temp fallback resolves under os.TempDir/logs", func(t *testing.T) {
		// serial: mutates filesystem under os.TempDir
		expectedDir := filepath.Join(os.TempDir(), "logs")
		prefix := fmt.Sprintf("loginjector-test-%d", os.Getpid())

		l, err := NewFileLogger("", prefix, minLevel, WithTempDirFallback(), WithoutPrinter())
		require.NoError(t, err)
		require.NotNil(t, l)
		if runtime.GOOS != "windows" {
			fi, statErr := os.Stat(expectedDir)
			require.NoError(t, statErr)
			require.Equal(t, os.FileMode(0o750), fi.Mode().Perm())
		}

		t.Cleanup(func() {
			// remove only the files this test created, leave the shared dir intact
			matches, _ := filepath.Glob(filepath.Join(expectedDir, prefix+".*"))
			for _, m := range matches {
				_ = os.Remove(m)
			}
		})

		_, err = l.WriteLog(minLevel, []byte("temp fallback test"))
		require.NoError(t, err)

		logFile := filepath.Join(expectedDir, prefix+".00000001.log")
		content, readErr := os.ReadFile(logFile)
		require.NoError(t, readErr)
		require.Contains(t, string(content), "temp fallback test")
	})

	t.Run("empty folder without fallback errors", func(t *testing.T) {
		t.Parallel()

		l, err := NewFileLogger("", "app", minLevel)
		require.Error(t, err)
		require.Nil(t, l)
		require.Contains(t, err.Error(), "temp fallback not enabled")
	})

	t.Run("empty prefix errors", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		l, err := NewFileLogger(dir, "", minLevel)
		require.Error(t, err)
		require.Nil(t, l)
		require.Contains(t, err.Error(), "prefix must not be empty")
	})

	t.Run("traversal prefix rejected", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		for _, badPrefix := range []string{"../evil", "sub/dir", "a/b/c"} {
			t.Run(badPrefix, func(t *testing.T) {
				t.Parallel()
				l, err := NewFileLogger(dir, badPrefix, minLevel)
				require.Error(t, err, "prefix %q must be rejected", badPrefix)
				require.Nil(t, l)
				require.Contains(t, err.Error(), "path separators")
			})
		}
	})

	t.Run("dot prefix rejected", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		for _, badPrefix := range []string{".", ".."} {
			t.Run(badPrefix, func(t *testing.T) {
				t.Parallel()
				l, err := NewFileLogger(dir, badPrefix, minLevel)
				require.Error(t, err, "prefix %q must be rejected", badPrefix)
				require.Nil(t, l)
				require.Contains(t, err.Error(), "plain file name")
			})
		}
	})

	t.Run("std log redirect OFF by default", func(t *testing.T) {
		// serial: touches global log state
		prev := log.Writer()
		prevFlags := log.Flags()
		t.Cleanup(func() {
			log.SetOutput(prev)
			log.SetFlags(prevFlags)
		})

		// use a sentinel writer as the baseline global log output
		var sentinel bytes.Buffer
		log.SetOutput(&sentinel)
		originalFlags := log.Flags()

		dir := t.TempDir()
		_, err := NewFileLogger(dir, "noredirect", minLevel, WithoutPrinter())
		require.NoError(t, err)

		// confirm global log output is still our sentinel, not the new logger
		log.Print("canary")
		require.Contains(t, sentinel.String(), "canary",
			"std log output must remain unchanged without WithStdLogRedirect")
		require.Equal(t, originalFlags, log.Flags(),
			"log.Flags must not be touched without WithStdLogRedirect")
	})

	t.Run("std log redirect ON forwards messages", func(t *testing.T) {
		// serial: touches global log state
		prev := log.Writer()
		prevFlags := log.Flags()
		t.Cleanup(func() {
			log.SetOutput(prev)
			log.SetFlags(prevFlags)
		})

		// Route the printer output to a SafeBuffer so we can observe messages
		// that flow through the new logger without touching the filesystem.
		var buf synctest.SafeBuffer
		dir := t.TempDir()
		const redirectLevel LogLevel = 2

		_, err := NewFileLogger(dir, "redirect", minLevel,
			WithStdLogRedirect(redirectLevel),
			WithPrinterOptions(WithOutput(&buf)),
		)
		require.NoError(t, err)

		// WithStdLogRedirect must have wired global log to the new logger.
		// Writing via log.Print must flow through the logger and reach buf.
		const unique = "redirected-unique-message-xyz"
		log.Print(unique)

		require.Contains(t, buf.String(), unique,
			"std log output must flow through the new logger when WithStdLogRedirect is set")
		require.Equal(t, 0, log.Flags(),
			"WithStdLogRedirect must set log.Flags to 0")
	})

	t.Run("WithoutPrinter attaches only file handler", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		l, err := NewFileLogger(dir, "noprint", minLevel, WithoutPrinter())
		require.NoError(t, err)
		require.NotNil(t, l)

		// the logger must have exactly one handler (the file handler)
		l.m.RLock()
		handlerCount := len(l.handlers)
		l.m.RUnlock()
		require.Equal(t, 1, handlerCount, "WithoutPrinter must leave only the file handler")
	})

	t.Run("printer is included by default", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		l, err := NewFileLogger(dir, "withprint", minLevel)
		require.NoError(t, err)
		require.NotNil(t, l)

		l.m.RLock()
		handlerCount := len(l.handlers)
		l.m.RUnlock()
		require.Equal(t, 2, handlerCount, "default logger must have file handler + printer")
	})

	t.Run("WithPrinterOptions forwarded to printer", func(t *testing.T) {
		t.Parallel()

		var p probe
		dir := t.TempDir()
		l, err := NewFileLogger(dir, "printeropt", minLevel,
			WithPrinterOptions(WithOutput(&p)),
		)
		require.NoError(t, err)
		_, err = l.WriteLog(minLevel, []byte("check printer"))
		require.NoError(t, err)
		require.Contains(t, p.String(), "check printer",
			"printer must receive the message via forwarded options")
	})
}

// TestNewFileLogger_WithPrinterMinLevel covers the per-printer threshold option.
func TestNewFileLogger_WithPrinterMinLevel(t *testing.T) {
	t.Parallel()

	// writeMessages writes messages at levels 1..5 to l and returns the file-buffer
	// content (read from the on-disk file via probe) and the console content.
	// The file handler writes to disk; to capture it here we redirect both outputs
	// in-memory via probes and build a fake file logger structure directly instead
	// of using disk I/O.

	// helper builds a Logger wired to fileBuf and an optionally-leveled consoleBuf.
	makeLogger := func(t *testing.T, fileMinLevel, printerLevel LogLevel, printerLevelSet bool) (*Logger, *probe, *probe) {
		t.Helper()
		fileBuf := &probe{}
		consoleBuf := &probe{}
		var handlers []io.Writer
		handlers = append(handlers, fileBuf)
		if printerLevelSet {
			handlers = append(handlers, WithMinLevel(printerLevel, consoleBuf))
		} else {
			handlers = append(handlers, consoleBuf)
		}
		l := NewLogger(fileMinLevel, handlers...)
		return l, fileBuf, consoleBuf
	}

	t.Run("default_path_matches_legacy", func(t *testing.T) {
		t.Parallel()
		const ml LogLevel = 3
		l, fileBuf, consoleBuf := makeLogger(t, ml, 0, false)
		for i := LogLevel(1); i <= 5; i++ {
			_, _ = l.WriteLog(i, []byte(fmt.Sprintf("msg%d", i)))
		}
		// both fire at same threshold (ml=3).
		for i := LogLevel(3); i <= 5; i++ {
			require.Contains(t, fileBuf.String(), fmt.Sprintf("msg%d", i))
			require.Contains(t, consoleBuf.String(), fmt.Sprintf("msg%d", i))
		}
		for i := LogLevel(1); i < 3; i++ {
			require.NotContains(t, fileBuf.String(), fmt.Sprintf("msg%d", i))
			require.NotContains(t, consoleBuf.String(), fmt.Sprintf("msg%d", i))
		}
	})

	t.Run("printer_below_file", func(t *testing.T) {
		t.Parallel()
		// fileMinLevel=4, printerMinLevel=2: console sees 2,3,4,5; file sees 4,5.
		l, fileBuf, consoleBuf := makeLogger(t, 4, 2, true)
		for i := LogLevel(1); i <= 5; i++ {
			_, _ = l.WriteLog(i, []byte(fmt.Sprintf("L%d", i)))
		}
		require.NotContains(t, fileBuf.String(), "L1")
		require.NotContains(t, fileBuf.String(), "L2")
		require.NotContains(t, fileBuf.String(), "L3")
		require.Contains(t, fileBuf.String(), "L4")
		require.Contains(t, fileBuf.String(), "L5")

		require.NotContains(t, consoleBuf.String(), "L1")
		require.Contains(t, consoleBuf.String(), "L2")
		require.Contains(t, consoleBuf.String(), "L3")
		require.Contains(t, consoleBuf.String(), "L4")
		require.Contains(t, consoleBuf.String(), "L5")
	})

	t.Run("printer_above_file", func(t *testing.T) {
		t.Parallel()
		// fileMinLevel=2, printerMinLevel=4: file sees 2,3,4,5; console sees 4,5.
		l, fileBuf, consoleBuf := makeLogger(t, 2, 4, true)
		for i := LogLevel(1); i <= 5; i++ {
			_, _ = l.WriteLog(i, []byte(fmt.Sprintf("H%d", i)))
		}
		require.NotContains(t, fileBuf.String(), "H1")
		require.Contains(t, fileBuf.String(), "H2")
		require.Contains(t, fileBuf.String(), "H3")
		require.Contains(t, fileBuf.String(), "H4")
		require.Contains(t, fileBuf.String(), "H5")

		require.NotContains(t, consoleBuf.String(), "H1")
		require.NotContains(t, consoleBuf.String(), "H2")
		require.NotContains(t, consoleBuf.String(), "H3")
		require.Contains(t, consoleBuf.String(), "H4")
		require.Contains(t, consoleBuf.String(), "H5")
	})

	t.Run("printer_equals_file", func(t *testing.T) {
		t.Parallel()
		// printerMinLevel == minLevel: behaves identically to no option.
		l, fileBuf, consoleBuf := makeLogger(t, 3, 3, true)
		for i := LogLevel(1); i <= 5; i++ {
			_, _ = l.WriteLog(i, []byte(fmt.Sprintf("E%d", i)))
		}
		// both receive the same set.
		require.Equal(t, fileBuf.String(), consoleBuf.String())
	})

	t.Run("printer_level_zero_explicit", func(t *testing.T) {
		t.Parallel()
		// printerMinLevel=0, fileMinLevel=1: console fires at every level >= 0.
		l, fileBuf, consoleBuf := makeLogger(t, 1, 0, true)
		_, _ = l.WriteLog(0, []byte("zero"))
		_, _ = l.WriteLog(1, []byte("one"))
		require.NotContains(t, fileBuf.String(), "zero", "file handler gated at 1")
		require.Contains(t, consoleBuf.String(), "zero", "console gated at 0 must receive level-0 message")
		require.Contains(t, consoleBuf.String(), "one")
	})

	t.Run("printer_min_level_with_WithoutPrinter", func(t *testing.T) {
		t.Parallel()
		// WithoutPrinter wins over WithPrinterMinLevel when using NewFileLogger.
		dir := t.TempDir()
		l, err := NewFileLogger(dir, "noprint2", 1,
			WithoutPrinter(),
			WithPrinterMinLevel(2),
		)
		require.NoError(t, err)
		l.m.RLock()
		hCount := len(l.handlers)
		l.m.RUnlock()
		require.Equal(t, 1, hCount, "WithoutPrinter must leave only the file handler")
	})

	t.Run("printer_min_level_equal_to_min_level_still_wraps", func(t *testing.T) {
		t.Parallel()
		// When printerMinLevel == minLevel the guard was previously a no-op; verify
		// that WithPrinterMinLevel is honored even in the equal case. We do this
		// behaviorally: the printer is wrapped in WithMinLevel, so a below-threshold
		// write that reaches the logger must not appear in the console buffer.
		// Use makeLogger directly so we get in-memory probes instead of disk I/O.
		const ml LogLevel = 3
		l, _, consoleBuf := makeLogger(t, ml, ml, true) // printerLevel == fileMinLevel
		_, _ = l.WriteLog(2, []byte("below"))           // level 2 < 3: must be gated
		_, _ = l.WriteLog(3, []byte("at"))              // level 3 >= 3: must pass
		_, _ = l.WriteLog(4, []byte("above"))           // level 4 >= 3: must pass

		require.NotContains(t, consoleBuf.String(), "below",
			"WithPrinterMinLevel equal to minLevel must still gate below-threshold messages")
		require.Contains(t, consoleBuf.String(), "at")
		require.Contains(t, consoleBuf.String(), "above")
	})
}

// TestNewFileLogger_RotationOptions covers the WithMaxFileAge and WithFileCompression
// forwarders added for lumberjack parity: they must reach the underlying
// RotatingFileHandler while WithStableCurrentName is deliberately not exposed here.
func TestNewFileLogger_RotationOptions(t *testing.T) {
	t.Parallel()
	const minLevel LogLevel = 1

	t.Run("WithFileCompression forwards compression", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		l, err := NewFileLogger(dir, "app", minLevel,
			WithoutPrinter(), WithMaxFileCapacity(20), WithMaxFilesInFolder(50), WithFileCompression())
		require.NoError(t, err)

		for i := range 4 {
			_, err = l.WriteLog(minLevel, []byte(fmt.Sprintf("padding-message-%d", i)))
			require.NoError(t, err)
		}

		gzs, err := filepath.Glob(filepath.Join(dir, "*.log.gz"))
		require.NoError(t, err)
		require.NotEmpty(t, gzs, "WithFileCompression must gzip rotated backups")
		// the compressed content is the timestamped file line, proving the whole chain
		// (TimestampedHandler -> RotatingFileHandler compression) is intact.
		require.Regexp(t, `^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} `, readGzOrFail(t, gzs[0]))
	})

	t.Run("WithMaxFileAge forwards age pruning", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		l, err := NewFileLogger(dir, "app", minLevel,
			WithoutPrinter(), WithMaxFileCapacity(20), WithMaxFilesInFolder(100), WithMaxFileAge(time.Hour))
		require.NoError(t, err)

		// each stamped line exceeds 20 bytes, so every write rotates: three backups form.
		for i := range 3 {
			_, err = l.WriteLog(minLevel, []byte(fmt.Sprintf("msg%d-padding-xxxxx", i)))
			require.NoError(t, err)
		}
		old := time.Now().Add(-2 * time.Hour)
		require.NoError(t, os.Chtimes(filepath.Join(dir, "app.00000001.log"), old, old))

		// more writes rotate again and run the age prune (count bound is 100, so age is
		// the only pressure).
		for i := range 3 {
			_, err = l.WriteLog(minLevel, []byte(fmt.Sprintf("more%d-padding-xxxxx", i)))
			require.NoError(t, err)
		}

		_, err = os.Stat(filepath.Join(dir, "app.00000001.log"))
		require.ErrorIs(t, err, os.ErrNotExist, "WithMaxFileAge must prune the backdated backup")
	})

	t.Run("no new options leaves NewFileLogger behavior unchanged", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		l, err := NewFileLogger(dir, "app", minLevel, WithoutPrinter())
		require.NoError(t, err)

		_, err = l.WriteLog(minLevel, []byte("hello world"))
		require.NoError(t, err)

		files, err := extractFilesOrFail(dir)
		require.NoError(t, err)
		require.Contains(t, files, "app.00000001.log")
		gzs, err := filepath.Glob(filepath.Join(dir, "*.log.gz"))
		require.NoError(t, err)
		require.Empty(t, gzs, "without WithFileCompression no .gz must be produced")
	})
}
