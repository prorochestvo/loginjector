package loginjector

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// readGzOrFail decompresses a gzip file and returns its plaintext, failing the test on
// any error. Compression tests decompress and compare bytes because an existence-only
// check would pass a rename-that-forgot-to-gzip bug.
func readGzOrFail(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Open(path)
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
func touchOld(t *testing.T, path, content string, age time.Duration) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(content), defaultFilePermissions))
	when := time.Now().Add(-age)
	require.NoError(t, os.Chtimes(path, when, when))
}

func idxName(prefix string, index int) string {
	return fmt.Sprintf("%s.%08X.%s", prefix, index, defaultFileExtension)
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
