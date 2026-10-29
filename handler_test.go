package loginjector

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTelegramHandler(t *testing.T) {
	t.Parallel()
	t.Run("delivers to the live API", func(t *testing.T) {
		t.Parallel()
		botToken := os.Getenv("TELEGRAM_BOT_TOKEN")
		chatID := os.Getenv("TELEGRAM_BOT_CHAT_ID")

		if botToken == "" || chatID == "" {
			t.Skipf("TELEGRAM_BOT_TOKEN or TELEGRAM_BOT_CHAT_ID not set")
		}

		m := time.Now().UTC().Format(time.RFC3339) + ": 14C225CB-9BE0-40D8-8FB3-6218FE17AE53"

		h := TelegramHandler(botToken, chatID, "test.log", "LogInjector", "<b>demo</b> of telegram handler")
		_, err := h.Write([]byte(m))
		require.NoError(t, err)
	})

	t.Run("redacts the bot token from a surfaced request error", func(t *testing.T) {
		t.Parallel()

		// "%zz" is an invalid URL escape, so http.NewRequest fails deterministically and
		// fast without any network — and net/url echoes the offending URL (token included)
		// verbatim into the error string, exercising the redactToken closure on the
		// request-build return site. The token is >= 8 chars so the guard does not skip it.
		botToken := "1234567890ABCDEF%zz"

		h := TelegramHandler(botToken, "chat", "test.log", "label")
		_, err := h.Write([]byte("payload"))

		require.Error(t, err)
		require.NotContains(t, err.Error(), botToken,
			"the bot token must never appear in the surfaced error")
		require.Contains(t, err.Error(), "***",
			"the redaction placeholder must replace the token")
		require.Contains(t, err.Error(), "bot***/sendDocument",
			"the literal bot prefix stays; only the secret is masked")
	})

	t.Run("short token is left unredacted by the guard", func(t *testing.T) {
		t.Parallel()

		// a token below the 8-char guard is not redacted (it is not a valid Telegram token
		// and would over-match unrelated substrings). "%zz" still forces the build error.
		botToken := "%zz"

		h := TelegramHandler(botToken, "chat", "test.log", "label")
		_, err := h.Write([]byte("payload"))

		require.Error(t, err)
		require.NotContains(t, err.Error(), "***",
			"a sub-8-char token must not be redacted")
	})
}

func TestRotatingFileHandler(t *testing.T) {
	t.Parallel()
	assertRetains := func(t *testing.T, maxFiles int) {
		t.Helper()
		dir := t.TempDir()
		h := RotatingFileHandler(dir, "app", WithMaxFileSize(3), WithMaxFiles(maxFiles))

		_, err := h.Write([]byte("abcd"))
		require.NoError(t, err)
		content, err := os.ReadFile(filepath.Join(dir, "app.00000001.log"))
		require.NoError(t, err)
		require.Equal(t, []byte("abcd\n"), content)
	}
	t.Run("max files zero retains rotated write", func(t *testing.T) {
		t.Parallel()
		assertRetains(t, 0)
	})
	t.Run("negative max files retains rotated write", func(t *testing.T) {
		t.Parallel()
		assertRetains(t, -5)
	})
	t.Run("folder with glob metacharacters is literal", func(t *testing.T) {
		t.Parallel()
		base := t.TempDir()
		dir := filepath.Join(base, "app[1]")
		decoyDir := filepath.Join(base, "app1")
		require.NoError(t, os.MkdirAll(dir, 0o750))
		require.NoError(t, os.MkdirAll(decoyDir, 0o750))
		decoys := make(map[string][]byte)
		for i := 1; i <= 3; i++ {
			name := fmt.Sprintf("app.%08d.log", i)
			content := []byte(fmt.Sprintf("decoy-%d\n", i))
			decoys[name] = content
			require.NoError(t, os.WriteFile(filepath.Join(decoyDir, name), content, 0o640))
		}

		h := RotatingFileHandler(dir, "app", WithMaxFileSize(4), WithMaxFiles(2))
		for range 6 {
			_, err := h.Write([]byte("0123456789"))
			require.NoError(t, err)
		}

		for name, content := range decoys {
			actual, err := os.ReadFile(filepath.Join(decoyDir, name))
			require.NoError(t, err)
			require.Equal(t, content, actual)
		}
		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		require.Len(t, entries, 2)
		require.FileExists(t, filepath.Join(dir, "app.00000005.log"))
		require.FileExists(t, filepath.Join(dir, "app.00000006.log"))
	})
	t.Run("fresh start leaves glob-decoy folder untouched", func(t *testing.T) {
		t.Parallel()
		base := t.TempDir()
		dir := filepath.Join(base, "app[1]")
		decoyDir := filepath.Join(base, "app1")
		require.NoError(t, os.MkdirAll(dir, 0o750))
		require.NoError(t, os.MkdirAll(decoyDir, 0o750))
		decoy := filepath.Join(decoyDir, "app.00000001.log")
		content := []byte("decoy survives\n")
		require.NoError(t, os.WriteFile(decoy, content, 0o640))

		h := RotatingFileHandler(dir, "app", WithFreshStart())
		actual, err := os.ReadFile(decoy)
		require.NoError(t, err, "construction must not remove a file in the glob-decoy folder")
		require.Equal(t, content, actual)
		_, err = h.Write([]byte("first"))
		require.NoError(t, err)
		actual, err = os.ReadFile(decoy)
		require.NoError(t, err, "the first write must not prune the glob-decoy folder")
		require.Equal(t, content, actual)
	})
	t.Run("compressed rotation leaves glob-decoy files untouched", func(t *testing.T) {
		t.Parallel()
		base := t.TempDir()
		dir := filepath.Join(base, "app[1]")
		decoyDir := filepath.Join(base, "app1")
		require.NoError(t, os.MkdirAll(dir, 0o750))
		require.NoError(t, os.MkdirAll(decoyDir, 0o750))
		decoys := map[string][]byte{
			"app.00000001.log":        []byte("plain decoy\n"),
			"app.00000002.log.gz":     []byte("compressed decoy"),
			"app.00000003.log.gz.tmp": []byte("temporary decoy"),
		}
		for name, content := range decoys {
			require.NoError(t, os.WriteFile(filepath.Join(decoyDir, name), content, 0o640))
		}

		h := RotatingFileHandler(dir, "app", WithMaxFileSize(4), WithMaxFiles(2), WithCompress())
		for range 3 {
			_, err := h.Write([]byte("0123456789"))
			require.NoError(t, err)
		}
		for name, content := range decoys {
			actual, err := os.ReadFile(filepath.Join(decoyDir, name))
			require.NoError(t, err)
			require.Equal(t, content, actual)
		}
	})
	t.Run("malformed glob folder", func(t *testing.T) {
		t.Parallel()
		dir := filepath.Join(t.TempDir(), "app[")
		require.NoError(t, os.MkdirAll(dir, 0o750))
		h := RotatingFileHandler(dir, "app", WithMaxFileSize(4), WithMaxFiles(2))
		for range 3 {
			_, err := h.Write([]byte("0123456789"))
			require.NoError(t, err)
		}
	})
	t.Run("folder entry that is a directory is ignored", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		matchingDir := filepath.Join(dir, "app.00000009.log")
		require.NoError(t, os.Mkdir(matchingDir, 0o750))
		h := RotatingFileHandler(dir, "app", WithMaxFileSize(4), WithMaxFiles(2))
		for range 3 {
			_, err := h.Write([]byte("0123456789"))
			require.NoError(t, err)
		}
		info, err := os.Stat(matchingDir)
		require.NoError(t, err)
		require.True(t, info.IsDir())
		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		regularFiles := 0
		for _, entry := range entries {
			if !entry.IsDir() {
				regularFiles++
			}
		}
		require.Equal(t, 2, regularFiles)
	})
	t.Run("prunes only its own prefix files", func(t *testing.T) {
		t.Parallel()
		tmpFolder := t.TempDir()
		foreign := filepath.Join(tmpFolder, "aaa-foreign.log")
		require.NoError(t, os.WriteFile(foreign, []byte("foreign"), 0o640))

		other := RotatingFileHandler(tmpFolder, "aaa", WithMaxFileSize(1024), WithMaxFiles(2))
		_, err := other.Write([]byte("other"))
		require.NoError(t, err)

		h := RotatingFileHandler(tmpFolder, "zzz", WithMaxFileSize(4), WithMaxFiles(2))
		for range 6 {
			_, err = h.Write([]byte("0123456789"))
			require.NoError(t, err)
		}

		require.FileExists(t, foreign, "foreign .log file must survive rotation")
		require.FileExists(t, filepath.Join(tmpFolder, rotatingFileName("aaa", 1)), "another prefix's live file must survive rotation")
		own, err := filepath.Glob(filepath.Join(tmpFolder, "zzz.*.log"))
		require.NoError(t, err)
		require.Len(t, own, 2, "own files must be pruned to the max-files bound")
	})
	t.Run("appends and rotates", func(t *testing.T) {
		t.Parallel()
		tmpFolder := t.TempDir()

		h := RotatingFileHandler(tmpFolder, "err", WithMaxFileSize(7), WithMaxFiles(3))

		_, err := h.Write(bytes.Repeat([]byte("1"), 2))
		require.NoError(t, err)
		files, err := extractFilesOrFail(tmpFolder)
		require.NoError(t, err)
		require.Len(t, files, 1, "incorrect files count")
		f, ok := files["err.00000001."+defaultFileExtension]
		require.True(t, ok, "file context not found")
		require.Equal(t, "11\n", f, "incorrect file context")

		_, err = h.Write(bytes.Repeat([]byte("2"), 2))
		require.NoError(t, err)
		files, err = extractFilesOrFail(tmpFolder)
		require.NoError(t, err)
		require.Len(t, files, 1, "incorrect files count")
		f, ok = files["err.00000001."+defaultFileExtension]
		require.True(t, ok, "file context not found")
		require.Equal(t, "11\n22\n", f, "incorrect file context")

		_, err = h.Write(bytes.Repeat([]byte("3"), 2))
		require.NoError(t, err)
		files, err = extractFilesOrFail(tmpFolder)
		require.NoError(t, err)
		require.Len(t, files, 1, "incorrect files count")
		f, ok = files["err.00000001."+defaultFileExtension]
		require.True(t, ok, "file context not found")
		require.Equal(t, "11\n22\n33\n", f, "incorrect file context")

		_, err = h.Write([]byte("4"))
		require.NoError(t, err)
		files, err = extractFilesOrFail(tmpFolder)
		require.NoError(t, err)
		require.Len(t, files, 2, "incorrect files count")
		f, ok = files["err.00000001."+defaultFileExtension]
		require.True(t, ok, "file context not found")
		require.Equal(t, "11\n22\n33\n", f, "incorrect file context")
		f, ok = files["err.00000002."+defaultFileExtension]
		require.True(t, ok, "file context not found")
		require.Equal(t, "4\n", f, "incorrect file context")

		_, err = h.Write(bytes.Repeat([]byte("5"), 5))
		require.NoError(t, err)
		files, err = extractFilesOrFail(tmpFolder)
		require.NoError(t, err)
		require.Len(t, files, 2, "incorrect files count")
		f, ok = files["err.00000001."+defaultFileExtension]
		require.True(t, ok, "file context not found")
		require.Equal(t, "11\n22\n33\n", f, "incorrect file context")
		f, ok = files["err.00000002."+defaultFileExtension]
		require.True(t, ok, "file context not found")
		require.Equal(t, "4\n55555\n", f, "incorrect file context")

		_, err = h.Write(bytes.Repeat([]byte("6"), 10))
		require.NoError(t, err)
		_, err = h.Write(bytes.Repeat([]byte("7"), 10))
		require.NoError(t, err)
		files, err = extractFilesOrFail(tmpFolder)
		require.NoError(t, err)
		require.Len(t, files, 3, "incorrect files count")
		f, ok = files["err.00000002."+defaultFileExtension]
		require.True(t, ok, "file context not found")
		require.Equal(t, "4\n55555\n", f, "incorrect file context")
		f, ok = files["err.00000003."+defaultFileExtension]
		require.True(t, ok, "file context not found")
		require.Equal(t, "6666666666\n", f, "incorrect file context")
		f, ok = files["err.00000004."+defaultFileExtension]
		require.True(t, ok, "file context not found")
		require.Equal(t, "7777777777\n", f, "incorrect file context")
	})

	t.Run("traversal prefix rejected", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		for _, badPrefix := range []string{"../evil", "sub/dir", "a/b"} {
			t.Run(badPrefix, func(t *testing.T) {
				t.Parallel()

				h := RotatingFileHandler(dir, badPrefix, WithMaxFileSize(1024), WithMaxFiles(5))
				_, err := h.Write([]byte("should fail"))
				require.Error(t, err, "prefix %q must be rejected", badPrefix)
				require.Contains(t, err.Error(), "path separators")
			})
		}
	})

	t.Run("rotate and prune single instance", func(t *testing.T) {
		t.Parallel()
		tmpFolder := t.TempDir()

		h := RotatingFileHandler(tmpFolder, "err", WithMaxFileSize(3), WithMaxFiles(3))
		_, err := h.Write(bytes.Repeat([]byte("1"), 3))
		require.NoError(t, err)
		_, err = h.Write(bytes.Repeat([]byte("2"), 3))
		require.NoError(t, err)
		_, err = h.Write(bytes.Repeat([]byte("3"), 3))
		require.NoError(t, err)

		files, err := extractFilesOrFail(tmpFolder)
		require.NoError(t, err)
		require.Len(t, files, 3, "incorrect files count")
		f, ok := files["err.00000001."+defaultFileExtension]
		require.True(t, ok, "file context not found")
		require.Equal(t, "111\n", f, "incorrect file context")
		f, ok = files["err.00000002."+defaultFileExtension]
		require.True(t, ok, "file context not found")
		require.Equal(t, "222\n", f, "incorrect file context")
		f, ok = files["err.00000003."+defaultFileExtension]
		require.True(t, ok, "file context not found")
		require.Equal(t, "333\n", f, "incorrect file context")

		_, err = h.Write(bytes.Repeat([]byte("4"), 3))
		require.NoError(t, err)

		files, err = extractFilesOrFail(tmpFolder)
		require.NoError(t, err)
		require.Len(t, files, 3, "incorrect files count")
		_, ok = files["err.00000001."+defaultFileExtension]
		require.False(t, ok, "oldest file must be pruned")
		f, ok = files["err.00000002."+defaultFileExtension]
		require.True(t, ok, "file context not found")
		require.Equal(t, "222\n", f, "incorrect file context")
		f, ok = files["err.00000003."+defaultFileExtension]
		require.True(t, ok, "file context not found")
		require.Equal(t, "333\n", f, "incorrect file context")
		f, ok = files["err.00000004."+defaultFileExtension]
		require.True(t, ok, "file context not found")
		require.Equal(t, "444\n", f, "incorrect file context")
	})

	t.Run("seeds from disk", testRotatingFileHandlerSeedFromDisk)

	t.Run("fresh start", testRotatingFileHandlerFreshStart)

	t.Run("resumes at highest existing index across restart", testRotatingFileHandlerResumeHighestIndex)

	t.Run("does not delete newest data on restart", testRotatingFileHandlerNoDeleteNewest)

	t.Run("rotating into a pre-existing file seeds its size", testRotatingFileHandlerRotateIntoPreexisting)

	t.Run("resume tolerates foreign filenames", testRotatingFileHandlerResumeToleratesForeign)
	t.Run("resume handles glob metacharacters in prefix", testRotatingFileHandlerResumeGlobMeta)
	t.Run("Write reports the input length and writes a trimmed line", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		h := RotatingFileHandler(dir, "app")

		n, err := h.Write([]byte("  hi  \n"))
		require.NoError(t, err)
		require.Equal(t, 7, n)
		content, err := os.ReadFile(filepath.Join(dir, "app.00000001.log"))
		require.NoError(t, err)
		require.Equal(t, []byte("hi\n"), content)
	})
	t.Run("Write of an empty message reports zero and writes a newline", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		h := RotatingFileHandler(dir, "app")

		n, err := h.Write([]byte{})
		require.NoError(t, err)
		require.Zero(t, n)
		content, err := os.ReadFile(filepath.Join(dir, "app.00000001.log"))
		require.NoError(t, err)
		require.Equal(t, []byte("\n"), content)
	})
	t.Run("Write to a regular file as folder reports open failure", func(t *testing.T) {
		t.Parallel()
		folder := filepath.Join(t.TempDir(), "notadir")
		require.NoError(t, os.WriteFile(folder, []byte("file"), 0o640))
		h := RotatingFileHandler(folder, "app")

		n, err := h.Write([]byte("line"))
		require.Error(t, err)
		require.Zero(t, n)
	})
}

func TestRotatingFileHandlerForRaceCondition(t *testing.T) {
	t.Parallel()
	tmpFolder := t.TempDir()

	h := RotatingFileHandler(tmpFolder, "err", WithMaxFileSize(70), WithMaxFiles(10))
	messages := make([]string, 0, 16)
	for i := range 16 {
		messages = append(messages, fmt.Sprintf("%0.3d->>%s", i, uniqueToken()))
	}
	var mu sync.Mutex
	var writeErr error
	wg := sync.WaitGroup{}
	for _, m := range messages {
		wg.Add(1)
		go func(wg *sync.WaitGroup, w io.Writer, txt string) {
			defer wg.Done()
			if _, e := w.Write([]byte(txt)); e != nil {
				mu.Lock()
				writeErr = errors.Join(writeErr, e)
				mu.Unlock()
			}
		}(&wg, h, m)
	}
	wg.Wait()
	require.NoError(t, writeErr)

	files, err := extractFilesOrFail(tmpFolder)
	require.NoError(t, err)

	var sb strings.Builder
	for f, ctx := range files {
		fmt.Fprintf(&sb, "\n%s:\n%s\n", f, ctx)
	}
	allContext := strings.TrimSpace(sb.String())

	for _, m := range messages {
		if !strings.Contains(allContext, m) {
			assert.Containsf(t, allContext, m, "not found %s in %s", m, allContext)
		}
	}
}

func TestFileByFormatHandler(t *testing.T) {
	t.Parallel()
	t.Run("case-insensitive filesystem keeps the file just written", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		probe := filepath.Join(dir, "probe.log")
		require.NoError(t, os.WriteFile(probe, []byte("probe"), 0o600))
		_, err := os.Stat(filepath.Join(dir, "PROBE.LOG"))
		if err != nil {
			t.Skip("filesystem is case-sensitive")
		}
		require.NoError(t, os.Remove(probe))

		names := []string{"app", "App"}
		nameIndex := 0
		h := FileByFormatHandler(dir, 1, func() string {
			name := names[nameIndex]
			nameIndex++
			return name
		})
		_, err = h.Write([]byte("first"))
		require.NoError(t, err)
		_, err = h.Write([]byte("second"))
		require.NoError(t, err)

		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		var logFiles []os.DirEntry
		for _, entry := range entries {
			if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".log") {
				logFiles = append(logFiles, entry)
			}
		}
		require.Len(t, logFiles, 1)
		content, err := os.ReadFile(filepath.Join(dir, logFiles[0].Name()))
		require.NoError(t, err)
		require.Contains(t, string(content), "second\n")
	})
	t.Run("name with separator does not write into an existing subfolder", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		require.NoError(t, os.Mkdir(filepath.Join(dir, "a"), 0o750))
		h := FileByFormatHandler(dir, 10, func() string { return "a/b" })

		n, err := h.Write([]byte("line"))
		require.Error(t, err)
		require.Zero(t, n)
		require.NoFileExists(t, filepath.Join(dir, "a", "b.log"))
	})
	t.Run("zero max files retains the written file", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		h := FileByFormatHandler(dir, 0, func() string { return "first" })

		_, err := h.Write([]byte("line"))
		require.NoError(t, err)
		content, err := os.ReadFile(filepath.Join(dir, "first.log"))
		require.NoError(t, err)
		require.Equal(t, []byte("line\n"), content)
	})
	t.Run("keeps the current file when names are not time sorted", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		names := []string{"b", "a"}
		nameIndex := 0
		h := FileByFormatHandler(dir, 1, func() string {
			name := names[nameIndex]
			nameIndex++
			return name
		})

		_, err := h.Write([]byte("first line"))
		require.NoError(t, err)
		_, err = h.Write([]byte("second line"))
		require.NoError(t, err)
		content, err := os.ReadFile(filepath.Join(dir, "a.log"))
		require.NoError(t, err)
		require.Equal(t, []byte("second line\n"), content)
		require.NoFileExists(t, filepath.Join(dir, "b.log"))
	})
	t.Run("rejects generated names that are not plain file names", func(t *testing.T) {
		t.Parallel()
		assertRejects := func(t *testing.T, name string) {
			t.Helper()
			parent := t.TempDir()
			dir := filepath.Join(parent, "logs")
			require.NoError(t, os.Mkdir(dir, 0o750))
			h := FileByFormatHandler(dir, 10, func() string { return name })

			n, err := h.Write([]byte("line"))
			require.Error(t, err)
			require.Zero(t, n)
			entries, readErr := os.ReadDir(dir)
			require.NoError(t, readErr)
			require.Empty(t, entries)
			require.NoFileExists(t, filepath.Join(filepath.Dir(dir), "escape.log"))
		}
		t.Run("parent traversal", func(t *testing.T) {
			t.Parallel()
			assertRejects(t, "../escape")
		})
		t.Run("nested path", func(t *testing.T) {
			t.Parallel()
			assertRejects(t, "a/b")
		})
		t.Run("empty name", func(t *testing.T) {
			t.Parallel()
			assertRejects(t, "")
		})
		t.Run("dot", func(t *testing.T) {
			t.Parallel()
			assertRejects(t, ".")
		})
		t.Run("double dot", func(t *testing.T) {
			t.Parallel()
			assertRejects(t, "..")
		})
	})
	t.Run("valid generated name writes the expected bytes", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		h := FileByFormatHandler(dir, 10, func() string { return "2026-10-22" })

		_, err := h.Write([]byte("line"))
		require.NoError(t, err)
		content, err := os.ReadFile(filepath.Join(dir, "2026-10-22.log"))
		require.NoError(t, err)
		require.Equal(t, []byte("line\n"), content)
	})
	t.Run("Write reports the input length and writes a trimmed line", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		h := FileByFormatHandler(dir, 10, func() string { return "app" })

		n, err := h.Write([]byte("  hi  \n"))
		require.NoError(t, err)
		require.Equal(t, 7, n)
		content, err := os.ReadFile(filepath.Join(dir, "app.log"))
		require.NoError(t, err)
		require.Equal(t, []byte("hi\n"), content)
	})
	t.Run("Write of an empty message reports zero and writes a newline", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		h := FileByFormatHandler(dir, 10, func() string { return "app" })

		n, err := h.Write([]byte{})
		require.NoError(t, err)
		require.Zero(t, n)
		content, err := os.ReadFile(filepath.Join(dir, "app.log"))
		require.NoError(t, err)
		require.Equal(t, []byte("\n"), content)
	})
	t.Run("folder with glob metacharacters is literal", func(t *testing.T) {
		t.Parallel()
		base := t.TempDir()
		dir := filepath.Join(base, "f[1]")
		decoyDir := filepath.Join(base, "f1")
		require.NoError(t, os.MkdirAll(dir, 0o750))
		require.NoError(t, os.MkdirAll(decoyDir, 0o750))
		decoys := map[string][]byte{
			"old1.log": []byte("old one\n"),
			"old2.log": []byte("old two\n"),
		}
		for name, content := range decoys {
			require.NoError(t, os.WriteFile(filepath.Join(decoyDir, name), content, 0o640))
		}
		names := []string{"first", "second"}
		nameIndex := 0
		h := FileByFormatHandler(dir, 1, func() string {
			name := names[nameIndex]
			nameIndex++
			return name
		})
		for range 2 {
			_, err := h.Write([]byte("payload"))
			require.NoError(t, err)
		}
		for name, content := range decoys {
			actual, err := os.ReadFile(filepath.Join(decoyDir, name))
			require.NoError(t, err)
			require.Equal(t, content, actual)
		}
		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		require.Len(t, entries, 1)
	})
	tmpFolder := t.TempDir()

	m := sync.Mutex{}
	fileNumber := -1
	fileNameGenerator := func() string {
		startingDay := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
		m.Lock()
		defer m.Unlock()
		fileNumber++
		return startingDay.AddDate(0, 0, fileNumber).Format("2006-01-02")
	}
	handler := FileByFormatHandler(tmpFolder, 4, fileNameGenerator)
	expectedFileContexts := []string{
		"f1:i0001", "f2:i0001",
		"f3:i0001", "f4:i0001",
		"f5:i0001", "f6:i0001",
		"f7:i0001", "f8:i0001",
		"f9:i0001", "f0:i0001",
	}

	for _, fileContext := range expectedFileContexts {
		_, err := handler.Write([]byte(fileContext))
		require.NoError(t, err)
	}

	expectedDataset := map[string]string{
		"2000-01-10.log": "f0:i0001\n",
		"2000-01-09.log": "f9:i0001\n",
		"2000-01-08.log": "f8:i0001\n",
		"2000-01-07.log": "f7:i0001\n",
	}

	files, err := extractFilesOrFail(tmpFolder)
	require.NoError(t, err)
	require.Len(t, files, 4, "incorrect files count")

	for expectedFileName, expectedFileContext := range expectedDataset {
		actualContext, exists := files[expectedFileName]
		require.True(t, exists)
		require.Equal(t, expectedFileContext, actualContext, expectedFileName)
	}
}

func TestFileByFormatHandlerV2(t *testing.T) {
	t.Parallel()
	startedAt := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	tmpFolder := t.TempDir()

	dataset := []string{
		"f1:i0001", "f1:i0002",
		"f2:i0001", "f2:i0002",
		"f3:i0001", "f3:i0002",
		"f4:i0001", "f4:i0002",
		"f5:i0001", "f5:i0002",
	}
	expectedDataset := map[string]string{
		"2000-01-03.log": "f3:i0001\nf3:i0002\n",
		"2000-01-04.log": "f4:i0001\nf4:i0002\n",
		"2000-01-05.log": "f5:i0001\nf5:i0002\n",
	}

	fileIndexMutex := sync.Mutex{}
	fileIndex := 0
	handler := FileByFormatHandler(tmpFolder, 3, func() string {
		fileIndexMutex.Lock()
		defer fileIndexMutex.Unlock()
		d := startedAt.Add(time.Hour * 24 * time.Duration(fileIndex>>1))
		fileIndex++
		return d.Format(time.DateOnly)
	})

	for _, d := range dataset {
		_, err := handler.Write([]byte(d))
		require.NoError(t, err)
	}

	files, err := extractFilesOrFail(tmpFolder)
	require.NoError(t, err)
	require.Len(t, files, 3, "incorrect files count")
	for fileName, expectedFileContext := range expectedDataset {
		if strings.HasPrefix(fileName, "ignored") {
			continue
		}
		actualData, fExists := files[fileName]
		require.True(t, fExists, fileName)
		require.Equal(t, expectedFileContext, actualData, fileName)
	}
}

func TestFileByFormatHandlerForRaceCondition(t *testing.T) {
	t.Parallel()
	tmpFolder := t.TempDir()

	handlerFileName := "2000-01-10"
	handler := FileByFormatHandler(tmpFolder, 1, func() string { return handlerFileName })

	expectedFileContexts := make([]string, 100)
	for i := range expectedFileContexts {
		expectedFileContexts[i] = strconv.Itoa(i) + ":" + uniqueToken()
	}

	var wg sync.WaitGroup
	for _, fileContext := range expectedFileContexts {
		wg.Add(1)
		go func(w io.Writer) {
			defer wg.Done()
			_, err := w.Write([]byte(fileContext))
			assert.NoError(t, err)
		}(handler)
	}
	wg.Wait() // waiting when all jobs will be done

	files, err := extractFilesOrFail(tmpFolder)
	require.NoError(t, err)
	require.Len(t, files, 1, "incorrect files count")

	fileContext, ok := files[handlerFileName+".log"]
	require.True(t, ok)
	require.NotEmpty(t, fileContext)

	for _, expectedContext := range expectedFileContexts {
		require.Contains(t, fileContext, expectedContext)
	}
}

func TestVerifyFiles(t *testing.T) {
	t.Parallel()
	tmpFolder := t.TempDir()

	err := verifyFiles(tmpFolder, 3, filepath.Join(tmpFolder, "0-current.log"))
	require.NoError(t, err)

	files, err := extractFilesOrFail(tmpFolder)
	require.NoError(t, err)
	require.Empty(t, files, "incorrect files count")

	current := filepath.Join(tmpFolder, "0-current.log")
	err = os.WriteFile(current, []byte("current"), os.ModePerm)
	require.NoError(t, err)
	for range 3 {
		err = os.WriteFile(path.Join(tmpFolder, fmt.Sprintf("%d.%s", rand.Int31(), defaultFileExtension)), []byte("-"), os.ModePerm)
		require.NoError(t, err)
	}

	err = verifyFiles(tmpFolder, 3, current)
	require.NoError(t, err)

	files, err = extractFilesOrFail(tmpFolder)
	require.NoError(t, err)
	require.Len(t, files, 3, "incorrect files count")
	require.Contains(t, files, "0-current.log")
}

// testRotatingFileHandlerSeedFromDisk verifies that a handler constructed when a
// log file already exists on disk does not over-count. With the size properly
// seeded, the first write appends to file 1, crosses the threshold, and advances
// the internal index to 2. The second write then lands in file 2 — proving the
// rotation was triggered on time. Without the seed, both writes would land in
// file 1 (the counter would incorrectly start at 0).
func testRotatingFileHandlerSeedFromDisk(t *testing.T) {
	t.Parallel()

	tmpFolder := t.TempDir()
	prefix := "seed"
	const maxCap uint32 = 20

	// pre-create the target file (index 1) with content right at the capacity limit.
	existingContent := bytes.Repeat([]byte("x"), int(maxCap))
	firstFile := path.Join(tmpFolder, fmt.Sprintf("%s.%08X.%s", prefix, 1, defaultFileExtension))
	err := os.WriteFile(firstFile, existingContent, defaultFilePermissions)
	require.NoError(t, err)

	// constructing the handler should stat the file and seed fileSize from its real size.
	h := RotatingFileHandler(tmpFolder, prefix, WithMaxFileSize(maxCap), WithMaxFiles(5))

	// first write: appends to file 1, then detects fileSize > maxCap and advances index to 2.
	_, err = h.Write([]byte("first"))
	require.NoError(t, err)

	// second write: must go to file 2 (index advanced by the first write's overflow).
	_, err = h.Write([]byte("second"))
	require.NoError(t, err)

	files, err := extractFilesOrFail(tmpFolder)
	require.NoError(t, err)

	// file 2 must exist, proving the handler correctly detected overflow after seeding.
	secondFileName := fmt.Sprintf("%s.%08X.%s", prefix, 2, defaultFileExtension)
	content, secondExists := files[secondFileName]
	require.True(t, secondExists, "second file must exist after seeded overflow triggered rotation")
	require.Contains(t, content, "second", "second write must land in file 2")
}

// testRotatingFileHandlerResumeHighestIndex proves that a handler constructed
// against a folder holding several indices resumes at the highest one instead of
// restarting at index 1 (the oldest file).
func testRotatingFileHandlerResumeHighestIndex(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	const prefix = "resume"

	// pre-create indices 1..3, each below the cap so the first write appends to 3.
	for _, i := range []int{1, 2, 3} {
		p := filepath.Join(dir, fmt.Sprintf("%s.%08X.%s", prefix, i, defaultFileExtension))
		require.NoError(t, os.WriteFile(p, []byte(fmt.Sprintf("old-%d\n", i)), defaultFilePermissions))
	}

	h := RotatingFileHandler(dir, prefix, WithMaxFileSize(1024), WithMaxFiles(10))

	marker := "resumed-write"
	_, err := h.Write([]byte(marker))
	require.NoError(t, err)

	files, err := extractFilesOrFail(dir)
	require.NoError(t, err)

	// the write must land in index 3 (highest existing), not index 1.
	third := fmt.Sprintf("%s.%08X.%s", prefix, 3, defaultFileExtension)
	require.Contains(t, files[third], marker, "write must append to the highest existing index")
	first := fmt.Sprintf("%s.%08X.%s", prefix, 1, defaultFileExtension)
	require.NotContains(t, files[first], marker, "write must NOT land in the oldest file")
}

// testRotatingFileHandlerNoDeleteNewest proves that, after a simulated restart at
// the max file count, a few writes do not prune the file holding the most recent
// write — the defect the resume fix closes.
func testRotatingFileHandlerNoDeleteNewest(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	const prefix = "newest"
	const maxFiles = 3

	// pre-create indices 1..3 (at the maxFiles limit); index 3 is near the cap so
	// the first write rotates into index 4.
	require.NoError(t, os.WriteFile(filepath.Join(dir, fmt.Sprintf("%s.%08X.%s", prefix, 1, defaultFileExtension)), []byte("aaa\n"), defaultFilePermissions))
	require.NoError(t, os.WriteFile(filepath.Join(dir, fmt.Sprintf("%s.%08X.%s", prefix, 2, defaultFileExtension)), []byte("bbb\n"), defaultFilePermissions))
	require.NoError(t, os.WriteFile(filepath.Join(dir, fmt.Sprintf("%s.%08X.%s", prefix, 3, defaultFileExtension)), bytes.Repeat([]byte("c"), 10), defaultFilePermissions))

	h := RotatingFileHandler(dir, prefix, WithMaxFileSize(8), WithMaxFiles(maxFiles))

	// first write appends to index 3 (already >8 after seeding) → rotates to index 4.
	_, err := h.Write([]byte("d"))
	require.NoError(t, err)
	// second write lands in index 4, the newest data.
	newest := "the-newest-line"
	_, err = h.Write([]byte(newest))
	require.NoError(t, err)

	files, err := extractFilesOrFail(dir)
	require.NoError(t, err)
	require.LessOrEqual(t, len(files), maxFiles, "pruning must keep at most maxFiles files")

	fourth := fmt.Sprintf("%s.%08X.%s", prefix, 4, defaultFileExtension)
	require.Contains(t, files, fourth, "the newest file must NOT be pruned")
	require.Contains(t, files[fourth], newest, "the newest write must survive pruning")
}

// testRotatingFileHandlerRotateIntoPreexisting proves that when the handler
// rotates into a pre-existing higher-index file, it seeds that file's size from
// disk so the file does not grow to ~2× the cap before rotating again.
func testRotatingFileHandlerRotateIntoPreexisting(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	const prefix = "preexist"
	const maxCap uint32 = 20

	// index 1 is at the cap so the first write rotates into index 2.
	require.NoError(t, os.WriteFile(filepath.Join(dir, fmt.Sprintf("%s.%08X.%s", prefix, 1, defaultFileExtension)), bytes.Repeat([]byte("a"), int(maxCap)), defaultFilePermissions))
	// index 2 already exists near the cap from a prior run.
	require.NoError(t, os.WriteFile(filepath.Join(dir, fmt.Sprintf("%s.%08X.%s", prefix, 2, defaultFileExtension)), bytes.Repeat([]byte("b"), int(maxCap)), defaultFilePermissions))

	h := RotatingFileHandler(dir, prefix, WithMaxFileSize(maxCap), WithMaxFiles(10))

	// resume lands on index 2 (highest). It is already at the cap, so the first
	// write overflows and rotates to index 3.
	_, err := h.Write([]byte("first"))
	require.NoError(t, err)
	// the next write must land in index 3, proving index 2 was not allowed to grow
	// to ~2× the cap (which would happen if its size were seeded as 0 on rotation).
	_, err = h.Write([]byte("third-file-write"))
	require.NoError(t, err)

	files, err := extractFilesOrFail(dir)
	require.NoError(t, err)
	third := fmt.Sprintf("%s.%08X.%s", prefix, 3, defaultFileExtension)
	require.Contains(t, files, third, "rotation must reach index 3")
	require.Contains(t, files[third], "third-file-write", "the write after overflow must land in index 3")
}

// testRotatingFileHandlerResumeToleratesForeign proves that foreign or malformed
// filenames in the folder are ignored by the resume scan without panicking.
func testRotatingFileHandlerResumeToleratesForeign(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	const prefix = "mixed"

	// a valid index-2 file plus assorted files the scan must skip.
	require.NoError(t, os.WriteFile(filepath.Join(dir, fmt.Sprintf("%s.%08X.%s", prefix, 2, defaultFileExtension)), []byte("valid\n"), defaultFilePermissions))
	require.NoError(t, os.WriteFile(filepath.Join(dir, prefix+".nothex."+defaultFileExtension), []byte("x\n"), defaultFilePermissions))
	require.NoError(t, os.WriteFile(filepath.Join(dir, prefix+".0000000000000005."+defaultFileExtension), []byte("too long\n"), defaultFilePermissions))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "foreign."+defaultFileExtension), []byte("other\n"), defaultFilePermissions))

	h := RotatingFileHandler(dir, prefix, WithMaxFileSize(1024), WithMaxFiles(10))

	marker := "after-mixed"
	_, err := h.Write([]byte(marker))
	require.NoError(t, err)

	files, err := extractFilesOrFail(dir)
	require.NoError(t, err)
	// resume must have picked index 2 (the only well-formed match), appending there.
	second := fmt.Sprintf("%s.%08X.%s", prefix, 2, defaultFileExtension)
	require.Contains(t, files[second], marker, "write must append to the only valid index")
}

// testRotatingFileHandlerResumeGlobMeta proves the resume scan is not corrupted by glob
// metacharacters in the prefix: filepath.Glob is fed the literal "*.log" and the prefix
// is matched with a plain HasPrefix, so a prefix like "app[v1]" still resumes at the
// highest existing index instead of silently misfiring back to index 1 (which would leave
// the newest file as the prune-first orphan — the exact bug the resume logic prevents).
func testRotatingFileHandlerResumeGlobMeta(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	const prefix = "app[v1]" // '[' and ']' are glob metacharacters

	third := fmt.Sprintf("%s.%08X.%s", prefix, 3, defaultFileExtension)
	require.NoError(t, os.WriteFile(filepath.Join(dir, third), []byte("old\n"), defaultFilePermissions))

	h := RotatingFileHandler(dir, prefix, WithMaxFileSize(1024), WithMaxFiles(10))

	marker := "after-restart"
	_, err := h.Write([]byte(marker))
	require.NoError(t, err)

	files, err := extractFilesOrFail(dir)
	require.NoError(t, err)
	require.Contains(t, files[third], marker, "write must append to the resumed highest index despite metacharacters")
	_, hasIndex1 := files[fmt.Sprintf("%s.%08X.%s", prefix, 1, defaultFileExtension)]
	require.False(t, hasIndex1, "resume must not have created a fresh index-1 file")
}

// errWriter is an io.Writer that always returns an error.
var _ io.Writer = (*errWriter)(nil)

type errWriter struct{ err error }

func (e *errWriter) Write(_ []byte) (int, error) { return 0, e.err }

func TestTimestampedPrintHandler(t *testing.T) {
	t.Parallel()

	fixedTime := time.Date(2024, 3, 15, 10, 30, 45, 0, time.UTC)
	fixedClock := withClock(func() time.Time { return fixedTime })
	tsPrefix := "2024/03/15 10:30:45"
	// indent is the width of the timestamp prefix plus trailing space
	indent := strings.Repeat(" ", len(tsPrefix)+1)

	t.Run("single line", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		h := TimestampedPrintHandler(fixedClock, WithOutput(&buf))
		_, err := h.Write([]byte("hello"))
		require.NoError(t, err)
		require.Equal(t, tsPrefix+" hello\n", buf.String())
	})

	t.Run("multiline indent", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		h := TimestampedPrintHandler(fixedClock, WithOutput(&buf))
		_, err := h.Write([]byte("line1\nline2\nline3"))
		require.NoError(t, err)
		want := tsPrefix + " line1\n" + indent + "line2\n" + indent + "line3\n"
		require.Equal(t, want, buf.String())
	})

	t.Run("trailing whitespace trimmed equivalence", func(t *testing.T) {
		t.Parallel()

		var buf1, buf2 bytes.Buffer
		h1 := TimestampedPrintHandler(fixedClock, WithOutput(&buf1))
		h2 := TimestampedPrintHandler(fixedClock, WithOutput(&buf2))
		_, err := h1.Write([]byte("hi\n"))
		require.NoError(t, err)
		_, err = h2.Write([]byte("hi"))
		require.NoError(t, err)
		require.Equal(t, buf1.String(), buf2.String())
	})

	t.Run("internal blank line preserved", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		h := TimestampedPrintHandler(fixedClock, WithOutput(&buf))
		_, err := h.Write([]byte("a\n\nb"))
		require.NoError(t, err)
		want := tsPrefix + " a\n" + indent + "\n" + indent + "b\n"
		require.Equal(t, want, buf.String())
	})

	t.Run("custom WithTimeLayout", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		h := TimestampedPrintHandler(fixedClock, WithOutput(&buf), WithTimeLayout("15:04:05"))
		_, err := h.Write([]byte("msg"))
		require.NoError(t, err)
		require.True(t, strings.HasPrefix(buf.String(), "10:30:45 msg"))
	})

	t.Run("sink error surfaced", func(t *testing.T) {
		t.Parallel()

		want := errors.New("disk full")
		h := TimestampedPrintHandler(fixedClock, WithOutput(&errWriter{err: want}))
		_, err := h.Write([]byte("boom"))
		require.ErrorIs(t, err, want)
	})

	t.Run("empty input", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		h := TimestampedPrintHandler(fixedClock, WithOutput(&buf))
		_, err := h.Write([]byte(""))
		require.NoError(t, err)
		require.Equal(t, tsPrefix+"\n", buf.String())
	})

	t.Run("whitespace only input", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		h := TimestampedPrintHandler(fixedClock, WithOutput(&buf))
		_, err := h.Write([]byte("   \n  "))
		require.NoError(t, err)
		require.Equal(t, tsPrefix+"\n", buf.String())
	})
}

func TestTimestampedHandler(t *testing.T) {
	t.Parallel()

	fixedTime := time.Date(2024, 3, 15, 10, 30, 45, 0, time.UTC)
	fixedClock := withClock(func() time.Time { return fixedTime })
	tsPrefix := "2024/03/15 10:30:45"
	indent := strings.Repeat(" ", len(tsPrefix)+1)

	t.Run("single line forwarded to inner", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		h := TimestampedHandler(&buf, fixedClock)
		_, err := h.Write([]byte("hello"))
		require.NoError(t, err)
		require.Equal(t, tsPrefix+" hello\n", buf.String())
	})

	t.Run("multiline indents continuation lines", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		h := TimestampedHandler(&buf, fixedClock)
		_, err := h.Write([]byte("line1\nline2\nline3"))
		require.NoError(t, err)
		want := tsPrefix + " line1\n" + indent + "line2\n" + indent + "line3\n"
		require.Equal(t, want, buf.String())
	})

	t.Run("byte-for-byte match with TimestampedPrintHandler", func(t *testing.T) {
		t.Parallel()

		// the decorator over a buffer must render identically to the stdout printer
		// pointed at the same buffer via WithOutput — only the sink differs.
		for _, in := range []string{"hello", "a\nb\nc", "a\n\nb", "", "   \n  ", "trailing\n"} {
			var decorated, printer bytes.Buffer
			hDec := TimestampedHandler(&decorated, fixedClock)
			hPrint := TimestampedPrintHandler(fixedClock, WithOutput(&printer))
			_, err := hDec.Write([]byte(in))
			require.NoError(t, err)
			_, err = hPrint.Write([]byte(in))
			require.NoError(t, err)
			require.Equal(t, printer.String(), decorated.String(), "input %q must render identically", in)
		}
	})

	t.Run("WithOutput is ignored; inner is the sink", func(t *testing.T) {
		t.Parallel()

		var inner, redirect bytes.Buffer
		h := TimestampedHandler(&inner, fixedClock, WithOutput(&redirect))
		_, err := h.Write([]byte("payload"))
		require.NoError(t, err)
		require.Equal(t, tsPrefix+" payload\n", inner.String(), "output must go to inner, not WithOutput")
		require.Empty(t, redirect.String(), "WithOutput must be ignored by TimestampedHandler")
	})

	t.Run("sink error surfaced", func(t *testing.T) {
		t.Parallel()

		want := errors.New("disk full")
		h := TimestampedHandler(&errWriter{err: want}, fixedClock)
		_, err := h.Write([]byte("boom"))
		require.ErrorIs(t, err, want)
	})
}

//nolint:paralleltest // captureStdout redirects the process-global os.Stdout
func TestPrintHandler(t *testing.T) {
	// serial: captureStdout redirects the process-global os.Stdout.
	m := uniqueToken()
	out := captureStdout(t, func() {
		h := PrintHandler()
		n, err := h.Write([]byte("  " + m + "  "))
		require.NoError(t, err)
		require.Equal(t, len("  "+m+"  "), n, "Write must report the original message length")
	})
	require.Equal(t, m+"\n", out, "PrintHandler must write a trimmed line to stdout")
}

func testRotatingFileHandlerFreshStart(t *testing.T) {
	t.Parallel()

	const prefix = "reset"

	t.Run("removes existing ring files at construction", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		// pre-populate several ring files — a prior run that rotated past index 1.
		firstFile := filepath.Join(dir, fmt.Sprintf("%s.%08X.%s", prefix, 1, defaultFileExtension))
		secondFile := filepath.Join(dir, fmt.Sprintf("%s.%08X.%s", prefix, 2, defaultFileExtension))
		require.NoError(t, os.WriteFile(firstFile, bytes.Repeat([]byte("x"), 50), defaultFilePermissions))
		require.NoError(t, os.WriteFile(secondFile, bytes.Repeat([]byte("y"), 50), defaultFilePermissions))

		// construct the handler — removal must happen here, before any Write. The whole
		// ring is cleared, not just index-1 truncated, so no stale file survives to be
		// appended onto or pruned ahead of the fresh writes.
		h := RotatingFileHandler(dir, prefix, WithMaxFileSize(1024), WithMaxFiles(5), WithFreshStart())

		_, err := os.Stat(firstFile)
		require.ErrorIs(t, err, os.ErrNotExist, "index-1 file must be removed at construction")
		_, err = os.Stat(secondFile)
		require.ErrorIs(t, err, os.ErrNotExist, "stale higher-index file must be removed at construction")

		// now write something and assert only the new content is present, at index 1.
		_, err = h.Write([]byte("first"))
		require.NoError(t, err)

		files, err := extractFilesOrFail(dir)
		require.NoError(t, err)
		require.Len(t, files, 1)
		content, ok := files[fmt.Sprintf("%s.%08X.%s", prefix, 1, defaultFileExtension)]
		require.True(t, ok)
		require.Equal(t, "first\n", content, "only the fresh write must be present; old content must be gone")
	})

	t.Run("no existing file is a no-op", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		// no pre-existing files.
		h := RotatingFileHandler(dir, prefix, WithMaxFileSize(1024), WithMaxFiles(5), WithFreshStart())

		// resetRotation only removes matching files; it never creates the index-1
		// file, so construction must not create it when none existed — the base
		// handler creates it on the first write, and this handler must match that.
		firstFile := filepath.Join(dir, fmt.Sprintf("%s.%08X.%s", prefix, 1, defaultFileExtension))
		_, err := os.Stat(firstFile)
		require.ErrorIs(t, err, os.ErrNotExist, "construction must not create the index-1 file when none existed")

		_, err = h.Write([]byte("hello"))
		require.NoError(t, err)

		files, err := extractFilesOrFail(dir)
		require.NoError(t, err)
		require.Len(t, files, 1)
		content, ok := files[fmt.Sprintf("%s.%08X.%s", prefix, 1, defaultFileExtension)]
		require.True(t, ok)
		require.Equal(t, "hello\n", content)
	})

	t.Run("rotation and prune still apply", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		// maxFileSize=7 triggers rotation after ~7 bytes; maxFiles=2.
		h := RotatingFileHandler(dir, prefix, WithMaxFileSize(7), WithMaxFiles(2), WithFreshStart())

		// write 1: "111\n" = 4 bytes, file 1 size = 4 (no rotation).
		_, err := h.Write(bytes.Repeat([]byte("1"), 3))
		require.NoError(t, err)
		files, err := extractFilesOrFail(dir)
		require.NoError(t, err)
		require.Len(t, files, 1)

		// write 2: "22\n" = 3 bytes, file 1 total = 7 (no rotation yet — threshold is >7, not >=7).
		_, err = h.Write(bytes.Repeat([]byte("2"), 2))
		require.NoError(t, err)
		files, err = extractFilesOrFail(dir)
		require.NoError(t, err)
		require.Len(t, files, 1)
		content, ok := files[fmt.Sprintf("%s.%08X.%s", prefix, 1, defaultFileExtension)]
		require.True(t, ok)
		require.Equal(t, "111\n22\n", content)

		// write 3: "3\n" = 2 bytes, file 1 total = 9 > 7 → index advances to 2.
		// the write itself still goes to file 1; file 2 appears only on the next write.
		_, err = h.Write([]byte("3"))
		require.NoError(t, err)
		files, err = extractFilesOrFail(dir)
		require.NoError(t, err)
		require.Len(t, files, 1, "rotation advances the index but file 2 is only created on the next write")
		content, ok = files[fmt.Sprintf("%s.%08X.%s", prefix, 1, defaultFileExtension)]
		require.True(t, ok)
		require.Equal(t, "111\n22\n3\n", content)

		// write 4 goes to file 2 (index advanced after write 3).
		_, err = h.Write([]byte("4"))
		require.NoError(t, err)
		files, err = extractFilesOrFail(dir)
		require.NoError(t, err)
		require.Len(t, files, 2, "file 2 must exist after the first write following rotation")
		_, hasFile2 := files[fmt.Sprintf("%s.%08X.%s", prefix, 2, defaultFileExtension)]
		require.True(t, hasFile2, "file 2 must have been created")

		// write several more to force prune: maxFiles=2 so file 1 is pruned once file 3 exists.
		_, err = h.Write(bytes.Repeat([]byte("5"), 10))
		require.NoError(t, err)
		_, err = h.Write(bytes.Repeat([]byte("6"), 10))
		require.NoError(t, err)
		files, err = extractFilesOrFail(dir)
		require.NoError(t, err)
		require.Len(t, files, 2, "pruning must keep at most maxFiles files")
		_, hasFile1 := files[fmt.Sprintf("%s.%08X.%s", prefix, 1, defaultFileExtension)]
		require.False(t, hasFile1, "file 1 must have been pruned")
	})

	t.Run("seeded size resets to zero", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		const maxCap uint32 = 20

		// pre-create index-1 file at exactly maxCap bytes — without fresh start, this
		// seeds the counter at maxCap and the first write would trigger rotation.
		firstFile := filepath.Join(dir, fmt.Sprintf("%s.%08X.%s", prefix, 1, defaultFileExtension))
		err := os.WriteFile(firstFile, bytes.Repeat([]byte("x"), int(maxCap)), defaultFilePermissions)
		require.NoError(t, err)

		// with fresh start the ring is cleared, so the seeded counter must be 0.
		h := RotatingFileHandler(dir, prefix, WithMaxFileSize(maxCap), WithMaxFiles(5), WithFreshStart())

		// a small write must NOT trigger rotation (seeded size was 0, not maxCap).
		_, err = h.Write([]byte("small"))
		require.NoError(t, err)

		files, err := extractFilesOrFail(dir)
		require.NoError(t, err)

		// index must stay at 1 — no rotation occurred.
		_, hasFile2 := files[fmt.Sprintf("%s.%08X.%s", prefix, 2, defaultFileExtension)]
		require.False(t, hasFile2, "no rotation must have occurred because seeded size was 0 after fresh start")

		content, ok := files[fmt.Sprintf("%s.%08X.%s", prefix, 1, defaultFileExtension)]
		require.True(t, ok)
		require.Equal(t, "small\n", content, "file 1 must contain only the fresh write after fresh start")
	})
}

func TestRotatingFileHandler_FreshStartForRaceCondition(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// construct once — single truncation before any goroutine starts.
	h := RotatingFileHandler(dir, "race", WithMaxFileSize(70), WithMaxFiles(10), WithFreshStart())

	messages := make([]string, 0, 16)
	for i := range 16 {
		messages = append(messages, fmt.Sprintf("%0.3d->>%s", i, uniqueToken()))
	}

	var mu sync.Mutex
	var writeErr error
	wg := sync.WaitGroup{}
	for _, m := range messages {
		wg.Add(1)
		go func(wg *sync.WaitGroup, w io.Writer, txt string) {
			defer wg.Done()
			if _, e := w.Write([]byte(txt)); e != nil {
				mu.Lock()
				writeErr = errors.Join(writeErr, e)
				mu.Unlock()
			}
		}(&wg, h, m)
	}
	wg.Wait()
	require.NoError(t, writeErr)

	files, err := extractFilesOrFail(dir)
	require.NoError(t, err)

	var sb strings.Builder
	for f, ctx := range files {
		fmt.Fprintf(&sb, "\n%s:\n%s\n", f, ctx)
	}
	allContent := strings.TrimSpace(sb.String())

	for _, m := range messages {
		if !strings.Contains(allContent, m) {
			assert.Containsf(t, allContent, m, "message not found in logs: %s", m)
		}
	}
}

func extractFilesOrFail(folder string) (map[string]string, error) {
	files, err := filepath.Glob(path.Join(folder, "*."+defaultFileExtension))
	if err != nil || len(files) == 0 {
		return nil, err
	}
	r := make(map[string]string, 0)
	for _, filePath := range files {
		b, e := os.ReadFile(filePath)
		if runtime.GOOS == "windows" && strings.Contains(filePath, "\\") {
			// in some cases the filepath library is not able to handle the backslashes correctly
			// so we need to replace them with forward slashes
			filePath = strings.ReplaceAll(filePath, "\\", "/")
		}
		filePath = path.Base(filePath)
		if e != nil {
			r[filePath] = e.Error()
		} else {
			r[filePath] = string(b)
		}
	}
	return r, nil
}
