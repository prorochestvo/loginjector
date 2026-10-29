package extensions

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prorochestvo/loginjector"
	"github.com/prorochestvo/loginjector/internal/synctest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithMaxAgeDays(t *testing.T) {
	t.Parallel()
	t.Run("resolves to the same maxAge as the equivalent WithMaxAge", func(t *testing.T) {
		t.Parallel()

		var got, want rotatingFileConfig
		WithMaxAgeDays(14)(&got)
		WithMaxAge(14 * 24 * time.Hour)(&want)
		require.Equal(t, want.maxAge, got.maxAge,
			"WithMaxAgeDays(14) must equal WithMaxAge(14 days)")
	})

	t.Run("zero disables age pruning", func(t *testing.T) {
		t.Parallel()

		var cfg rotatingFileConfig
		WithMaxAgeDays(0)(&cfg)
		require.LessOrEqual(t, cfg.maxAge, time.Duration(0),
			"WithMaxAgeDays(0) must delegate to the zero-disables path")
	})

	t.Run("day bound prunes a backup older than it, keeps a fresh one", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		touchOld(t, filepath.Join(dir, idxName("app", 1)), "o1\n", 48*time.Hour) // older than 1 day
		touchOld(t, filepath.Join(dir, idxName("app", 2)), "f2\n", 0)            // fresh

		var cfg rotatingFileConfig
		cfg.maxFiles = 100
		WithMaxAgeDays(1)(&cfg)
		require.NoError(t, pruneRotation(dir, "app", cfg, idxName("app", 3), 0, time.Now()))

		require.NoFileExists(t, filepath.Join(dir, idxName("app", 1)),
			"the 48h-old backup must be pruned under a 1-day age bound")
		require.FileExists(t, filepath.Join(dir, idxName("app", 2)),
			"a fresh backup must survive the 1-day age bound")
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

func TestRotatingFileHandler_StableCurrentName(t *testing.T) {
	t.Parallel()
	t.Run("live path fixed while indexed backups accumulate", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		h := RotatingFileHandler(dir, "access", WithMaxFileSize(10), WithMaxFiles(20), WithStableCurrentName())

		writeRotating(t, h, "aaaaa") // access.log now holds "aaaaa\n" (6 bytes)
		writeRotating(t, h, "bbbbb") // 12 bytes exceeds 10-byte limit, rotates to access.00000001.log
		writeRotating(t, h, "ccccc") // access.log now holds "ccccc\n"
		writeRotating(t, h, "ddddd") // 12 > 10 -> rotate to access.00000002.log
		writeRotating(t, h, "eeeee") // access.log = "eeeee\n" (still fits)

		files := extractFilesWithGzOrFail(t, dir)
		require.Equal(t, "eeeee\n", files["access.log"], "the live file must stay at the fixed prefix.log path")
		require.Equal(t, "aaaaa\nbbbbb\n", files[idxName("access", 1)])
		require.Equal(t, "ccccc\nddddd\n", files[idxName("access", 2)])
		require.NotContains(t, files, idxName("access", 3), "no backup is created for the current live slot")
	})

	t.Run("maxFiles one preserves the triggering stable backup", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		h := RotatingFileHandler(dir, "access", WithMaxFiles(1), WithMaxFileSize(3), WithStableCurrentName())

		writeRotating(t, h, "abcd")

		backup := filepath.Join(dir, idxName("access", 1))
		require.FileExists(t, backup)
		contents, err := os.ReadFile(backup)
		require.NoError(t, err)
		require.Equal(t, "abcd\n", string(contents))
	})
	t.Run("rotation past a planted backup protects the renamed slot", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		h := RotatingFileHandler(dir, "access", WithStableCurrentName(), WithMaxFiles(1), WithMaxFileSize(3))
		require.NoError(t, os.WriteFile(filepath.Join(dir, idxName("access", 1)), []byte("planted\n"), defaultFilePermissions))

		writeRotating(t, h, "abcd")

		contents, err := os.ReadFile(filepath.Join(dir, idxName("access", 2)))
		require.NoError(t, err)
		require.Equal(t, "abcd\n", string(contents))
	})

	t.Run("resume seeds size from prefix.log; next index is max+1", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		// a prior run left a backup at index 5 and a live file near the cap.
		require.NoError(t, os.WriteFile(filepath.Join(dir, idxName("access", 5)), []byte("bck"), defaultFilePermissions))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "access.log"), bytes.Repeat([]byte("x"), 95), defaultFilePermissions))

		h := RotatingFileHandler(dir, "access", WithMaxFileSize(100), WithMaxFiles(50), WithStableCurrentName())

		// 95 + "0123456789\n" (11) = 106 > 100 -> rotate. Seeded from the 3-byte backup it
		// would be 3+11=14 and never rotate: reaching index 6 proves the live seed.
		writeRotating(t, h, "0123456789")
		writeRotating(t, h, "next")

		files := extractFilesWithGzOrFail(t, dir)
		require.Contains(t, files, idxName("access", 6), "next backup index must be highest(5)+1")
		require.Equal(t, "bck", files[idxName("access", 5)], "the pre-existing backup must be untouched")
		require.Equal(t, "next\n", files["access.log"], "the fresh live file holds the post-rotation write")
	})

	t.Run("rotation past a pre-existing index does not clobber", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, idxName("access", 1)), []byte("b1\n"), defaultFilePermissions))

		h := RotatingFileHandler(dir, "access", WithMaxFileSize(5), WithMaxFiles(50), WithStableCurrentName())
		// resume set index=2; plant a file at that exact slot before the first rotation.
		require.NoError(t, os.WriteFile(filepath.Join(dir, idxName("access", 2)), []byte("PLANTED\n"), defaultFilePermissions))

		writeRotating(t, h, "aaaaa") // 6 > 5 -> rotate; collision guard must skip index 2

		files := extractFilesWithGzOrFail(t, dir)
		require.Equal(t, "PLANTED\n", files[idxName("access", 2)], "the pre-existing backup must not be clobbered")
		require.Equal(t, "aaaaa\n", files[idxName("access", 3)], "rotation must advance to the next free slot")
	})

	t.Run("small maxFiles over many rotations keeps prefix.log", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		h := RotatingFileHandler(dir, "access", WithMaxFileSize(5), WithMaxFiles(2), WithStableCurrentName())

		for range 10 {
			writeRotating(t, h, "abcde") // 6 > 5 -> rotate on every write
		}
		writeRotating(t, h, "z") // fits (2 bytes) -> leaves a live prefix.log present

		files := extractFilesWithGzOrFail(t, dir)
		require.Contains(t, files, "access.log", "the live file must survive aggressive pruning")
		require.Equal(t, "z\n", files["access.log"])
		require.LessOrEqual(t, len(files), 2, "total files must stay within maxFiles (live + maxFiles-1 backups)")
	})

	t.Run("off then on adopts legacy backups as backups", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, idxName("access", 1)), []byte("legacy1\n"), defaultFilePermissions))
		require.NoError(t, os.WriteFile(filepath.Join(dir, idxName("access", 2)), []byte("legacy2\n"), defaultFilePermissions))

		h := RotatingFileHandler(dir, "access", WithMaxFileSize(1000), WithMaxFiles(50), WithStableCurrentName())
		writeRotating(t, h, "fresh")

		files := extractFilesWithGzOrFail(t, dir)
		require.Equal(t, "fresh\n", files["access.log"], "the live target must be prefix.log, never a legacy backup")
		require.Equal(t, "legacy1\n", files[idxName("access", 1)], "legacy backups are adopted untouched")
		require.Equal(t, "legacy2\n", files[idxName("access", 2)])
	})

	t.Run("on then off leaves a stale prefix.log in place", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		// a prior stable run left prefix.log plus an indexed backup.
		require.NoError(t, os.WriteFile(filepath.Join(dir, "access.log"), []byte("stale-live\n"), defaultFilePermissions))
		require.NoError(t, os.WriteFile(filepath.Join(dir, idxName("access", 3)), []byte("b3\n"), defaultFilePermissions))

		// indexed mode (no stable option): resume at the highest index and append there.
		h := RotatingFileHandler(dir, "access", WithMaxFileSize(1000), WithMaxFiles(50))
		writeRotating(t, h, "indexed-write")

		files := extractFilesWithGzOrFail(t, dir)
		require.Contains(t, files[idxName("access", 3)], "indexed-write", "indexed mode resumes on the indexed backup")
		require.Equal(t, "stale-live\n", files["access.log"], "a stale prefix.log survives and must be drained manually")
	})
}

func TestRotatingFileHandler_MaxAge(t *testing.T) {
	t.Parallel()
	t.Run("backdated backups pruned on rotation, fresh kept", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		h := RotatingFileHandler(dir, "app", WithMaxFileSize(5), WithMaxFiles(100), WithMaxAge(time.Hour))

		writeRotating(t, h, "aaaaa") // -> app.00000001.log
		writeRotating(t, h, "bbbbb") // -> app.00000002.log
		writeRotating(t, h, "ccccc") // -> app.00000003.log (index now 4)

		// backdate the two oldest past the cutoff; leave index 3 fresh.
		old := time.Now().Add(-2 * time.Hour)
		require.NoError(t, os.Chtimes(filepath.Join(dir, idxName("app", 1)), old, old))
		require.NoError(t, os.Chtimes(filepath.Join(dir, idxName("app", 2)), old, old))

		writeRotating(t, h, "ddddd") // fills app.00000004.log, overflows, runs the age prune

		files := extractFilesWithGzOrFail(t, dir)
		require.NotContains(t, files, idxName("app", 1), "over-age backup must be pruned")
		require.NotContains(t, files, idxName("app", 2), "over-age backup must be pruned")
		require.Contains(t, files, idxName("app", 3), "under-age backup must be kept")
		require.Contains(t, files, idxName("app", 4), "the just-rotated fresh backup must be kept")
	})

	t.Run("negative maxAge is a no-op", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		h := RotatingFileHandler(dir, "app", WithMaxFileSize(5), WithMaxFiles(100), WithMaxAge(-1))

		writeRotating(t, h, "aaaaa") // -> app.00000001.log
		writeRotating(t, h, "bbbbb") // -> app.00000002.log

		// backdate everything far past any plausible cutoff.
		old := time.Now().Add(-9000 * time.Hour)
		require.NoError(t, os.Chtimes(filepath.Join(dir, idxName("app", 1)), old, old))
		require.NoError(t, os.Chtimes(filepath.Join(dir, idxName("app", 2)), old, old))

		writeRotating(t, h, "ccccc") // triggers another rotation + prune

		files := extractFilesWithGzOrFail(t, dir)
		require.Contains(t, files, idxName("app", 1), "WithMaxAge(-1) must disable age pruning")
		require.Contains(t, files, idxName("app", 2), "WithMaxAge(-1) must disable age pruning")
	})

	t.Run("maxFiles one preserves the triggering aged backup", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		h := RotatingFileHandler(dir, "app", WithMaxFiles(1), WithMaxFileSize(3), WithMaxAge(time.Hour))

		writeRotating(t, h, "abcd")

		backup := filepath.Join(dir, idxName("app", 1))
		require.FileExists(t, backup)
		contents, err := os.ReadFile(backup)
		require.NoError(t, err)
		require.Equal(t, "abcd\n", string(contents))
	})

	t.Run("nanosecond maxAge keeps the newest backup until the next rotation", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		h := RotatingFileHandler(dir, "app", WithMaxFiles(3), WithMaxFileSize(3), WithMaxAge(time.Nanosecond))

		writeRotating(t, h, "abcd")
		require.FileExists(t, filepath.Join(dir, idxName("app", 1)))

		writeRotating(t, h, "efgh")
		require.NoFileExists(t, filepath.Join(dir, idxName("app", 1)))
		backup := filepath.Join(dir, idxName("app", 2))
		require.FileExists(t, backup)
		contents, err := os.ReadFile(backup)
		require.NoError(t, err)
		require.Equal(t, "efgh\n", string(contents))
	})
}

func TestRotatingFileHandler_Compress(t *testing.T) {
	t.Parallel()
	t.Run("rotated backup is a valid gz of the original bytes", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		h := RotatingFileHandler(dir, "app", WithMaxFileSize(7), WithMaxFiles(50), WithCompress())

		writeRotating(t, h, "hello") // app.00000001.log = "hello\n" (6)
		writeRotating(t, h, "world") // 12 > 7 -> rotate; compress index 1

		gzPath := filepath.Join(dir, idxName("app", 1)+"."+gzipExtension)
		require.FileExists(t, gzPath)
		require.NoFileExists(t, filepath.Join(dir, idxName("app", 1)), "plaintext must be removed after compression")
		require.Equal(t, "hello\nworld\n", readGzOrFail(t, gzPath), "the .gz must decompress to the pre-rotation plaintext")
	})

	t.Run("maxFiles one preserves the triggering compressed backup", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		h := RotatingFileHandler(dir, "app", WithMaxFiles(1), WithMaxFileSize(3), WithCompress())

		writeRotating(t, h, "abcd")

		backup := filepath.Join(dir, idxName("app", 1)+"."+gzipExtension)
		require.FileExists(t, backup)
		require.Equal(t, "abcd\n", readGzOrFail(t, backup))
	})

	t.Run("compression rotation error reports persisted input length", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		h := RotatingFileHandler(dir, "app", WithMaxFileSize(3), WithCompress())
		tmpPath := filepath.Join(dir, idxName("app", 1)+"."+gzipExtension+"."+tmpExtension)
		require.NoError(t, os.WriteFile(tmpPath, []byte("planted"), defaultFilePermissions))

		n, err := h.Write([]byte("abcd"))
		require.Error(t, err)
		require.Equal(t, 4, n)
	})

	t.Run("maxFiles one keeps only the newest compressed backup", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		h := RotatingFileHandler(dir, "app", WithMaxFiles(1), WithMaxFileSize(3), WithCompress())

		for _, message := range []string{"abcd", "efgh", "ijkl"} {
			writeRotating(t, h, message)
		}

		files := extractFilesWithGzOrFail(t, dir)
		require.Len(t, files, 1)
		require.Equal(t, "ijkl\n", files[idxName("app", 3)+"."+gzipExtension])
	})

	t.Run("zero maxFiles preserves the triggering compressed backup", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		h := RotatingFileHandler(dir, "app", WithMaxFiles(0), WithMaxFileSize(3), WithCompress())

		writeRotating(t, h, "abcd")

		backup := filepath.Join(dir, idxName("app", 1)+"."+gzipExtension)
		require.FileExists(t, backup)
		require.Equal(t, "abcd\n", readGzOrFail(t, backup))
	})

	t.Run("nanosecond maxAge and compression preserve the newest backup", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		h := RotatingFileHandler(dir, "app", WithMaxFiles(1), WithMaxFileSize(3), WithMaxAge(time.Nanosecond), WithCompress())

		writeRotating(t, h, "abcd")

		backup := filepath.Join(dir, idxName("app", 1)+"."+gzipExtension)
		require.FileExists(t, backup)
		require.Equal(t, "abcd\n", readGzOrFail(t, backup))
	})

	t.Run("higher pre-existing index survives compressed rotation", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		h := RotatingFileHandler(dir, "app", WithMaxFiles(1), WithMaxFileSize(3), WithCompress())
		higher := filepath.Join(dir, idxName("app", 2))
		require.NoError(t, os.WriteFile(higher, []byte("planted\n"), defaultFilePermissions))

		writeRotating(t, h, "abcd")

		backup := filepath.Join(dir, idxName("app", 1)+"."+gzipExtension)
		require.FileExists(t, backup)
		require.Equal(t, "abcd\n", readGzOrFail(t, backup))
		contents, err := os.ReadFile(higher)
		require.NoError(t, err)
		require.True(t, strings.HasPrefix(string(contents), "planted\n"))
	})

	t.Run("gz backup permissions are 0640", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		h := RotatingFileHandler(dir, "app", WithMaxFileSize(7), WithMaxFiles(50), WithCompress())
		writeRotating(t, h, "hello")
		writeRotating(t, h, "world")

		fi, err := os.Stat(filepath.Join(dir, idxName("app", 1)+"."+gzipExtension))
		require.NoError(t, err)
		require.Equal(t, os.FileMode(defaultFilePermissions), fi.Mode().Perm(), "compressed backups must not be world-readable")
	})

	t.Run("compression preserves the source mtime so age pruning stays honest", func(t *testing.T) {
		t.Parallel()

		// D4: a genuinely old log compressed today must still age out. If compressFile
		// dropped its os.Chtimes, the .gz would carry a fresh compression-time mtime and
		// WithMaxAge would keep it forever; every other test stays green, so this guards
		// the invariant directly.
		dir := t.TempDir()
		src := idxName("app", 1)
		touchOld(t, filepath.Join(dir, src), "old\n", 72*time.Hour)
		want, err := os.Stat(filepath.Join(dir, src))
		require.NoError(t, err)

		require.NoError(t, compressFile(dir, src, defaultFilePermissions))

		require.NoFileExists(t, filepath.Join(dir, src), "plaintext removed after compression")
		fi, err := os.Stat(filepath.Join(dir, src+"."+gzipExtension))
		require.NoError(t, err)
		require.WithinDuration(t, want.ModTime(), fi.ModTime(), time.Second,
			"the .gz must inherit the source mtime, not the compression time")
	})

	t.Run("prune counts .log and .log.gz, removing oldest regardless of extension", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		h := RotatingFileHandler(dir, "app", WithMaxFileSize(5), WithMaxFiles(3), WithCompress())

		for range 6 {
			writeRotating(t, h, "abcde") // 6 > 5 -> rotate + compress every write
		}
		writeRotating(t, h, "z") // fits, leaves a live file

		// live (app.log-indexed) + at most maxFiles-1 compressed backups.
		gzs, err := filepath.Glob(filepath.Join(dir, "*."+defaultFileExtension+"."+gzipExtension))
		require.NoError(t, err)
		require.LessOrEqual(t, len(gzs), 2, "compressed backups must be pruned to maxFiles-1")

		// the surviving backups must be the highest indices (oldest removed).
		files := extractFilesWithGzOrFail(t, dir)
		require.NotContains(t, files, idxName("app", 1)+"."+gzipExtension, "the oldest compressed backup must be pruned")
	})

	t.Run("crash leftover reconciles on next construction", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		// simulate a crash between rename-to-.gz and remove-plaintext: both exist for idx 3.
		require.NoError(t, os.WriteFile(filepath.Join(dir, idxName("app", 3)), []byte("stale-plain\n"), defaultFilePermissions))
		var gzBuf bytes.Buffer
		zw := gzip.NewWriter(&gzBuf)
		_, _ = zw.Write([]byte("authoritative\n"))
		require.NoError(t, zw.Close())
		require.NoError(t, os.WriteFile(filepath.Join(dir, idxName("app", 3)+"."+gzipExtension), gzBuf.Bytes(), defaultFilePermissions))
		// a stray temp from the interrupted compression must also be cleaned.
		require.NoError(t, os.WriteFile(filepath.Join(dir, idxName("app", 3)+"."+gzipExtension+"."+tmpExtension), []byte("junk"), defaultFilePermissions))

		h := RotatingFileHandler(dir, "app", WithMaxFileSize(1000), WithMaxFiles(50), WithCompress())
		writeRotating(t, h, "post-recovery")

		require.NoFileExists(t, filepath.Join(dir, idxName("app", 3)), "the stale plaintext must be discarded (the .gz wins)")
		require.NoFileExists(t, filepath.Join(dir, idxName("app", 3)+"."+gzipExtension+"."+tmpExtension), "stale temp must be cleaned")
		require.FileExists(t, filepath.Join(dir, idxName("app", 3)+"."+gzipExtension), "the durable .gz must remain")
		files := extractFilesWithGzOrFail(t, dir)
		require.Equal(t, "authoritative\n", files[idxName("app", 3)+"."+gzipExtension])
		require.Contains(t, files, idxName("app", 4), "resume must continue past the .gz-topped index")
	})

	t.Run("resume from an all-gz backup set starts at highest+1", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		for _, i := range []int{1, 2} {
			var b bytes.Buffer
			zw := gzip.NewWriter(&b)
			_, _ = fmt.Fprintf(zw, "old-%d\n", i)
			require.NoError(t, zw.Close())
			require.NoError(t, os.WriteFile(filepath.Join(dir, idxName("app", i)+"."+gzipExtension), b.Bytes(), defaultFilePermissions))
		}

		h := RotatingFileHandler(dir, "app", WithMaxFileSize(1000), WithMaxFiles(50), WithCompress())
		writeRotating(t, h, "resumed")

		files := extractFilesWithGzOrFail(t, dir)
		require.Contains(t, files[idxName("app", 3)], "resumed", "must resume at highest .gz index + 1, never appending into a .gz")
	})

	t.Run("fresh start with compress begins genuinely empty", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, idxName("app", 1)+"."+gzipExtension), []byte("junk"), defaultFilePermissions))
		require.NoError(t, os.WriteFile(filepath.Join(dir, idxName("app", 2)), []byte("junk\n"), defaultFilePermissions))
		require.NoError(t, os.WriteFile(filepath.Join(dir, idxName("app", 2)+"."+gzipExtension+"."+tmpExtension), []byte("junk"), defaultFilePermissions))

		h := RotatingFileHandler(dir, "app", WithMaxFileSize(1000), WithMaxFiles(50), WithCompress(), WithFreshStart())
		writeRotating(t, h, "fresh")

		files := extractFilesWithGzOrFail(t, dir)
		require.Len(t, files, 1, "fresh start must wipe .log, .gz and temp remnants")
		require.Equal(t, "fresh\n", files[idxName("app", 1)])
	})

	t.Run("zero-option handler ignores a foreign .log.gz", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "foreign.log.gz"), []byte("not ours"), defaultFilePermissions))
		require.NoError(t, os.WriteFile(filepath.Join(dir, idxName("app", 1)), []byte("existing\n"), defaultFilePermissions))

		// no compression option: the .gz must not enter globbing/parsing at all.
		h := RotatingFileHandler(dir, "app", WithMaxFileSize(1000), WithMaxFiles(3))
		writeRotating(t, h, "more")

		require.FileExists(t, filepath.Join(dir, "foreign.log.gz"), "a foreign .gz must be untouched by a zero-option handler")
		// use the plaintext-only helper: the foreign .gz holds non-gzip bytes on purpose,
		// and *.log globbing (like the handler's) never touches it.
		files, err := extractFilesOrFail(dir)
		require.NoError(t, err)
		require.Equal(t, "existing\nmore\n", files[idxName("app", 1)], "resume must ignore the .gz and append to the plaintext")
	})

	t.Run("resume tolerates glob metacharacters in prefix with compress", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		const prefix = "app[v1]"
		var b bytes.Buffer
		zw := gzip.NewWriter(&b)
		_, _ = zw.Write([]byte("old\n"))
		require.NoError(t, zw.Close())
		require.NoError(t, os.WriteFile(filepath.Join(dir, idxName(prefix, 3)+"."+gzipExtension), b.Bytes(), defaultFilePermissions))

		h := RotatingFileHandler(dir, prefix, WithMaxFileSize(1000), WithMaxFiles(50), WithCompress())
		writeRotating(t, h, "after-restart")

		files := extractFilesWithGzOrFail(t, dir)
		require.Contains(t, files[idxName(prefix, 4)], "after-restart", "metacharacter prefix must resume past the .gz index")
	})
}

// TestRotatingFileHandler_OptionsForRaceCondition drives concurrent writes with the full
// option set on (stable name + age + compress); the handler's mutex-guarded writer must
// serialize rotation, rename and synchronous compression with no race and no lost message.
func TestRotatingFileHandler_OptionsForRaceCondition(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	h := RotatingFileHandler(dir, "race",
		WithMaxFileSize(70), WithMaxFiles(10),
		WithStableCurrentName(), WithMaxAge(time.Hour), WithCompress(),
	)

	messages := make([]string, 0, 16)
	for i := range 16 {
		messages = append(messages, fmt.Sprintf("%0.3d->>%s", i, uniqueToken()))
	}

	var mu sync.Mutex
	var writeErr error
	var wg sync.WaitGroup
	for _, m := range messages {
		wg.Add(1)
		go func(txt string) {
			defer wg.Done()
			if _, e := h.Write([]byte(txt)); e != nil {
				mu.Lock()
				writeErr = errors.Join(writeErr, e)
				mu.Unlock()
			}
		}(m)
	}
	wg.Wait()
	require.NoError(t, writeErr)

	var sb strings.Builder
	for _, ctx := range extractFilesWithGzOrFail(t, dir) {
		fmt.Fprintf(&sb, "%s\n", ctx)
	}
	all := sb.String()
	for _, m := range messages {
		assert.Contains(t, all, m, "every message must survive across rotation, rename and compression")
	}

	compressDir := t.TempDir()
	compressHandler := RotatingFileHandler(compressDir, "race", WithMaxFiles(1), WithMaxFileSize(20), WithCompress())
	var compressWriteErr error
	var compressWG sync.WaitGroup
	for range 16 {
		compressWG.Add(1)
		go func() {
			defer compressWG.Done()
			if _, e := compressHandler.Write([]byte("concurrent compressed message")); e != nil {
				mu.Lock()
				compressWriteErr = errors.Join(compressWriteErr, e)
				mu.Unlock()
			}
		}()
	}
	compressWG.Wait()
	require.NoError(t, compressWriteErr)
}

// BenchmarkRotatingFileHandler_Write measures the non-rotating hot path with all options
// off. ReportAllocs guards the byte-identical contract: the options-off write path must
// not add heap allocations over the plain v1.0.7 handler.
func BenchmarkRotatingFileHandler_Write(b *testing.B) {
	dir := b.TempDir()
	// a huge cap so no write ever rotates: this isolates the append hot path.
	h := RotatingFileHandler(dir, "bench", WithMaxFileSize(1<<30), WithMaxFiles(10))
	msg := []byte("benchmark log line payload with a realistic width")

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := h.Write(msg); err != nil {
			b.Fatal(err)
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

//nolint:paralleltest,tparallel // subtests swap the std log output and share os.TempDir()/logs
func TestNewFileLogger(t *testing.T) {
	const minLevel loginjector.LogLevel = 1 // use a raw level to keep this independent of the levels package

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
		const redirectLevel loginjector.LogLevel = 2

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
		var buf bytes.Buffer
		l, err := NewFileLogger(dir, "noprint", minLevel, WithoutPrinter(), WithPrinterOptions(WithOutput(&buf)))
		require.NoError(t, err)
		require.NotNil(t, l)

		// the logger must have exactly one handler (the file handler)
		_, err = l.WriteLog(minLevel, []byte("without printer"))
		require.NoError(t, err)
		require.Empty(t, buf.String())
	})

	t.Run("printer is included by default", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		var buf bytes.Buffer
		l, err := NewFileLogger(dir, "withprint", minLevel, WithPrinterOptions(WithOutput(&buf)))
		require.NoError(t, err)
		require.NotNil(t, l)

		_, err = l.WriteLog(minLevel, []byte("printer included"))
		require.NoError(t, err)
		require.Contains(t, buf.String(), "printer included")
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

	// helper builds a loginjector.Logger wired to fileBuf and an optionally-leveled consoleBuf.
	makeLogger := func(t *testing.T, fileMinLevel, printerLevel loginjector.LogLevel, printerLevelSet bool) (*loginjector.Logger, *probe, *probe) {
		t.Helper()
		fileBuf := &probe{}
		consoleBuf := &probe{}
		var handlers []io.Writer
		handlers = append(handlers, fileBuf)
		if printerLevelSet {
			handlers = append(handlers, loginjector.WithMinLevel(printerLevel, consoleBuf))
		} else {
			handlers = append(handlers, consoleBuf)
		}
		l := loginjector.NewLogger(fileMinLevel, handlers...)
		return l, fileBuf, consoleBuf
	}

	t.Run("default_path_matches_legacy", func(t *testing.T) {
		t.Parallel()
		const ml loginjector.LogLevel = 3
		l, fileBuf, consoleBuf := makeLogger(t, ml, 0, false)
		for i := loginjector.LogLevel(1); i <= 5; i++ {
			_, _ = l.WriteLog(i, []byte(fmt.Sprintf("msg%d", i)))
		}
		// both fire at same threshold (ml=3).
		for i := loginjector.LogLevel(3); i <= 5; i++ {
			require.Contains(t, fileBuf.String(), fmt.Sprintf("msg%d", i))
			require.Contains(t, consoleBuf.String(), fmt.Sprintf("msg%d", i))
		}
		for i := loginjector.LogLevel(1); i < 3; i++ {
			require.NotContains(t, fileBuf.String(), fmt.Sprintf("msg%d", i))
			require.NotContains(t, consoleBuf.String(), fmt.Sprintf("msg%d", i))
		}
	})

	t.Run("printer_below_file", func(t *testing.T) {
		t.Parallel()
		// fileMinLevel=4, printerMinLevel=2: console sees 2,3,4,5; file sees 4,5.
		l, fileBuf, consoleBuf := makeLogger(t, 4, 2, true)
		for i := loginjector.LogLevel(1); i <= 5; i++ {
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
		for i := loginjector.LogLevel(1); i <= 5; i++ {
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
		for i := loginjector.LogLevel(1); i <= 5; i++ {
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
		var buf bytes.Buffer
		l, err := NewFileLogger(dir, "noprint2", 1,
			WithoutPrinter(),
			WithPrinterMinLevel(2),
			WithPrinterOptions(WithOutput(&buf)),
		)
		require.NoError(t, err)
		_, err = l.WriteLog(2, []byte("printer min level"))
		require.NoError(t, err)
		require.Empty(t, buf.String())
	})

	t.Run("printer_min_level_equal_to_min_level_still_wraps", func(t *testing.T) {
		t.Parallel()
		// When printerMinLevel == minLevel the guard was previously a no-op; verify
		// that WithPrinterMinLevel is honored even in the equal case. We do this
		// behaviorally: the printer is wrapped in loginjector.WithMinLevel, so a below-threshold
		// write that reaches the logger must not appear in the console buffer.
		// Use makeLogger directly so we get in-memory probes instead of disk I/O.
		const ml loginjector.LogLevel = 3
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
	const minLevel loginjector.LogLevel = 1

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

// TestPruneRotation exercises the retention core directly: it is deterministic (mtimes are
// set with os.Chtimes) and isolates the age/count/pair/live-exclusion rules from the write
// path.
func TestPruneRotation(t *testing.T) {
	t.Parallel()
	t.Run("age removes old, keeps fresh", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		touchOld(t, filepath.Join(dir, idxName("app", 1)), "o1\n", 2*time.Hour)
		touchOld(t, filepath.Join(dir, idxName("app", 2)), "f2\n", 0)
		touchOld(t, filepath.Join(dir, idxName("app", 3)), "f3\n", 0)

		cfg := rotatingFileConfig{maxFiles: 100, maxAge: time.Hour}
		require.NoError(t, pruneRotation(dir, "app", cfg, idxName("app", 4), 0, time.Now()))

		files := extractFilesWithGzOrFail(t, dir)
		require.NotContains(t, files, idxName("app", 1))
		require.Contains(t, files, idxName("app", 2))
		require.Contains(t, files, idxName("app", 3))
	})

	t.Run("live file excluded by name even when old", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		touchOld(t, filepath.Join(dir, idxName("app", 1)), "o1\n", 2*time.Hour)
		touchOld(t, filepath.Join(dir, idxName("app", 2)), "o2\n", 2*time.Hour)
		touchOld(t, filepath.Join(dir, idxName("app", 3)), "live\n", 2*time.Hour) // the current live, old

		cfg := rotatingFileConfig{maxFiles: 100, maxAge: time.Hour}
		require.NoError(t, pruneRotation(dir, "app", cfg, idxName("app", 3), 0, time.Now()))

		files := extractFilesWithGzOrFail(t, dir)
		require.NotContains(t, files, idxName("app", 1), "over-age backups go")
		require.NotContains(t, files, idxName("app", 2), "over-age backups go")
		require.Contains(t, files, idxName("app", 3), "the live file is excluded by name and never pruned")
	})

	t.Run("count keeps newest maxFiles-1 backups", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		for i := 1; i <= 5; i++ {
			touchOld(t, filepath.Join(dir, idxName("app", i)), fmt.Sprintf("f%d\n", i), 0)
		}
		cfg := rotatingFileConfig{maxFiles: 3, maxAge: 0}
		require.NoError(t, pruneRotation(dir, "app", cfg, idxName("app", 9), 0, time.Now()))

		files := extractFilesWithGzOrFail(t, dir)
		require.NotContains(t, files, idxName("app", 1))
		require.NotContains(t, files, idxName("app", 2))
		require.NotContains(t, files, idxName("app", 3))
		require.Contains(t, files, idxName("app", 4), "the two newest survive (live is the +1)")
		require.Contains(t, files, idxName("app", 5))
	})

	t.Run("age and count union", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		touchOld(t, filepath.Join(dir, idxName("app", 1)), "o1\n", 2*time.Hour)
		touchOld(t, filepath.Join(dir, idxName("app", 2)), "o2\n", 2*time.Hour)
		touchOld(t, filepath.Join(dir, idxName("app", 3)), "f3\n", 0)
		touchOld(t, filepath.Join(dir, idxName("app", 4)), "f4\n", 0)
		touchOld(t, filepath.Join(dir, idxName("app", 5)), "f5\n", 0)

		cfg := rotatingFileConfig{maxFiles: 3, maxAge: time.Hour}
		require.NoError(t, pruneRotation(dir, "app", cfg, idxName("app", 9), 0, time.Now()))

		files := extractFilesWithGzOrFail(t, dir)
		// age drops 1,2; count then keeps the newest 2 of {3,4,5}, dropping 3.
		require.NotContains(t, files, idxName("app", 1))
		require.NotContains(t, files, idxName("app", 2))
		require.NotContains(t, files, idxName("app", 3))
		require.Contains(t, files, idxName("app", 4))
		require.Contains(t, files, idxName("app", 5))
	})

	t.Run("gz pair counts as one file", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		// index 1 exists as BOTH plaintext and .gz (a crash remnant not yet reconciled).
		touchOld(t, filepath.Join(dir, idxName("app", 1)), "one\n", 0)
		touchOld(t, filepath.Join(dir, idxName("app", 1)+"."+gzipExtension), "one-gz", 0)
		touchOld(t, filepath.Join(dir, idxName("app", 2)+"."+gzipExtension), "two-gz", 0)
		touchOld(t, filepath.Join(dir, idxName("app", 3)), "three\n", 0)

		cfg := rotatingFileConfig{maxFiles: 3, maxAge: 0, compress: true}
		require.NoError(t, pruneRotation(dir, "app", cfg, idxName("app", 9), 0, time.Now()))

		// three backup UNITS {1,2,3}; keep newest two -> unit 1 (both files) removed.
		require.NoFileExists(t, filepath.Join(dir, idxName("app", 1)))
		require.NoFileExists(t, filepath.Join(dir, idxName("app", 1)+"."+gzipExtension))
		require.FileExists(t, filepath.Join(dir, idxName("app", 2)+"."+gzipExtension))
		require.FileExists(t, filepath.Join(dir, idxName("app", 3)))
	})

	t.Run("negative maxAge disables age pruning", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		touchOld(t, filepath.Join(dir, idxName("app", 1)), "o1\n", 9000*time.Hour)
		touchOld(t, filepath.Join(dir, idxName("app", 2)), "f2\n", 0)

		cfg := rotatingFileConfig{maxFiles: 100, maxAge: -1}
		require.NoError(t, pruneRotation(dir, "app", cfg, idxName("app", 9), 0, time.Now()))

		files := extractFilesWithGzOrFail(t, dir)
		require.Contains(t, files, idxName("app", 1), "negative maxAge must not prune by age")
		require.Contains(t, files, idxName("app", 2))
	})

	t.Run("maxFiles one preserves the protected backup only", func(t *testing.T) {
		t.Parallel()
		setup := func(t *testing.T, protectIdx int) map[string]string {
			t.Helper()
			dir := t.TempDir()
			for i := 1; i <= 3; i++ {
				touchOld(t, filepath.Join(dir, idxName("app", i)), fmt.Sprintf("f%d\n", i), 0)
			}
			cfg := rotatingFileConfig{maxFiles: 1}
			require.NoError(t, pruneRotation(dir, "app", cfg, idxName("app", 9), protectIdx, time.Now()))
			return extractFilesWithGzOrFail(t, dir)
		}

		t.Run("protect index 2 keeps only it", func(t *testing.T) {
			t.Parallel()
			require.Equal(t, map[string]string{idxName("app", 2): "f2\n"}, setup(t, 2))
		})
		t.Run("protect index 0 keeps none", func(t *testing.T) {
			t.Parallel()
			require.Empty(t, setup(t, 0))
		})
	})

	t.Run("protected old backup occupies one count slot", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		for i := 1; i <= 5; i++ {
			touchOld(t, filepath.Join(dir, idxName("app", i)), fmt.Sprintf("f%d\n", i), 0)
		}
		cfg := rotatingFileConfig{maxFiles: 3}
		require.NoError(t, pruneRotation(dir, "app", cfg, idxName("app", 9), 1, time.Now()))
		files := extractFilesWithGzOrFail(t, dir)
		require.Len(t, files, 2)
		require.Contains(t, files, idxName("app", 1))
		require.Contains(t, files, idxName("app", 5))
	})

	t.Run("protected over-age backup survives age pruning", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		for i := 1; i <= 3; i++ {
			touchOld(t, filepath.Join(dir, idxName("app", i)), fmt.Sprintf("f%d\n", i), 2*time.Hour)
		}
		cfg := rotatingFileConfig{maxFiles: 100, maxAge: time.Hour}
		require.NoError(t, pruneRotation(dir, "app", cfg, idxName("app", 9), 3, time.Now()))
		files := extractFilesWithGzOrFail(t, dir)
		require.Len(t, files, 1)
		require.Contains(t, files, idxName("app", 3))
	})
}

var (
	_ io.Writer = (*probe)(nil)
)

// tokenSeq backs uniqueToken.
var tokenSeq atomic.Uint64

type probe struct{ buf bytes.Buffer }

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

func (p *probe) Write(b []byte) (int, error) { return p.buf.Write(b) }
func (p *probe) String() string              { return p.buf.String() }

// readGzOrFail decompresses a gzip file and returns its plaintext, failing the test on
// any error. Compression tests decompress and compare bytes because an existence-only
// check would pass a rename-that-forgot-to-gzip bug.
func readGzOrFail(t *testing.T, filePath string) string {
	t.Helper()
	f, err := os.Open(filePath)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	zr, err := gzip.NewReader(f)
	require.NoError(t, err)
	defer func() { _ = zr.Close() }()
	b, err := io.ReadAll(zr)
	require.NoError(t, err)
	return string(b)
}

// extractFilesWithGzOrFail is the .gz-aware sibling of extractFilesOrFail: it maps every
// *.log to its raw bytes and every *.log.gz to its DECOMPRESSED plaintext, keyed by base
// name. The plain helper globs only *.log and is blind to compressed backups.
func extractFilesWithGzOrFail(t *testing.T, folder string) map[string]string {
	t.Helper()
	r := make(map[string]string)

	logs, err := filepath.Glob(filepath.Join(folder, "*."+defaultFileExtension))
	require.NoError(t, err)
	for _, p := range logs {
		b, e := os.ReadFile(p)
		require.NoError(t, e)
		r[filepath.Base(p)] = string(b)
	}

	gzs, err := filepath.Glob(filepath.Join(folder, "*."+defaultFileExtension+"."+gzipExtension))
	require.NoError(t, err)
	for _, p := range gzs {
		r[filepath.Base(p)] = readGzOrFail(t, p)
	}
	return r
}

// writeRotating writes s through h and asserts no error, keeping the option tests terse.
func writeRotating(t *testing.T, h io.Writer, s string) {
	t.Helper()
	_, err := h.Write([]byte(s))
	require.NoError(t, err)
}

// touchOld writes content to path and backdates its mtime by age, for deterministic
// age-prune tests (mtime is real filesystem data, so no clock seam is needed).
func touchOld(t *testing.T, filePath, content string, age time.Duration) {
	t.Helper()
	require.NoError(t, os.WriteFile(filePath, []byte(content), defaultFilePermissions))
	when := time.Now().Add(-age)
	require.NoError(t, os.Chtimes(filePath, when, when))
}

func idxName(prefix string, index int) string {
	return fmt.Sprintf("%s.%08X.%s", prefix, index, defaultFileExtension)
}

// uniqueToken returns a short, process-unique string for test data.
func uniqueToken() string {
	return "tok-" + strconv.FormatUint(tokenSeq.Add(1), 10)
}
