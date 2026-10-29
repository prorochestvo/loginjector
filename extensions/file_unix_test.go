//go:build unix

package extensions

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type ownerFileInfo struct {
	sys any
}

func (ownerFileInfo) Name() string       { return "" }
func (ownerFileInfo) Size() int64        { return 0 }
func (ownerFileInfo) Mode() os.FileMode  { return 0 }
func (ownerFileInfo) ModTime() time.Time { return time.Time{} }
func (ownerFileInfo) IsDir() bool        { return false }
func (fi ownerFileInfo) Sys() any        { return fi.sys }

func TestCheckFolderOwner(t *testing.T) {
	t.Parallel()
	t.Run("different owner returns an error", func(t *testing.T) {
		t.Parallel()
		fi := ownerFileInfo{sys: &syscall.Stat_t{Uid: uint32(os.Geteuid()) + 1}}
		require.Error(t, checkFolderOwner(fi))
	})
	t.Run("current owner returns nil", func(t *testing.T) {
		t.Parallel()
		fi := ownerFileInfo{sys: &syscall.Stat_t{Uid: uint32(os.Geteuid())}}
		require.NoError(t, checkFolderOwner(fi))
	})
	t.Run("nil Sys returns nil", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, checkFolderOwner(ownerFileInfo{}))
	})
}

//nolint:paralleltest // umask is process-global and t.Setenv mutates process env
func TestNewFileLogger_Unix(t *testing.T) {
	t.Run("created folder is 0750 under a restrictive umask", func(t *testing.T) {
		// not t.Parallel() — umask is process-global.
		old := syscall.Umask(0o077)
		t.Cleanup(func() { syscall.Umask(old) })

		folder := filepath.Join(t.TempDir(), "created")
		_, err := NewFileLogger(folder, "app", 1, WithoutPrinter())
		require.NoError(t, err)

		fi, err := os.Stat(folder)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o750), fi.Mode().Perm())

		trailingFolder := filepath.Join(t.TempDir(), "trail") + string(os.PathSeparator)
		_, err = NewFileLogger(trailingFolder, "app", 1, WithoutPrinter())
		require.NoError(t, err)
		fi, err = os.Stat(trailingFolder)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o750), fi.Mode().Perm())
	})
	t.Run("temp fallback rejects a symlink", func(t *testing.T) {
		// not t.Parallel() — t.Setenv mutates process env.
		base := t.TempDir()
		t.Setenv("TMPDIR", base)
		victim := filepath.Join(base, "victim")
		require.NoError(t, os.Mkdir(victim, 0o700))
		require.NoError(t, os.Symlink(victim, filepath.Join(base, "logs")))

		_, err := NewFileLogger("", "app", 1, WithTempDirFallback(), WithoutPrinter())
		require.Error(t, err)

		info, err := os.Stat(victim)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o700), info.Mode().Perm())
		entries, err := os.ReadDir(victim)
		require.NoError(t, err)
		require.Empty(t, entries)
	})
	t.Run("temp fallback rejects a non-directory", func(t *testing.T) {
		// not t.Parallel() — t.Setenv mutates process env.
		base := t.TempDir()
		t.Setenv("TMPDIR", base)
		require.NoError(t, os.WriteFile(filepath.Join(base, "logs"), []byte("file"), 0o600))

		_, err := NewFileLogger("", "app", 1, WithTempDirFallback(), WithoutPrinter())
		require.Error(t, err)
	})
}

//nolint:paralleltest // sets the process umask
func TestWithFileMode(t *testing.T) {
	// pinUmaskZero sets the process umask to 0 for the duration of a subtest so an exact
	// mode assertion is deterministic, restoring the previous umask on cleanup.
	pinUmaskZero := func(t *testing.T) {
		t.Helper()
		old := syscall.Umask(0)
		t.Cleanup(func() { syscall.Umask(old) })
	}

	t.Run("created live file has the requested mode", func(t *testing.T) {
		pinUmaskZero(t)

		dir := t.TempDir()
		h := RotatingFileHandler(dir, "app", WithFileMode(0o600))
		writeRotating(t, h, "hello")

		fi, err := os.Stat(filepath.Join(dir, idxName("app", 1)))
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o600), fi.Mode().Perm(),
			"the created live file must carry the WithFileMode bits")
	})

	t.Run("rotated .gz backup has the requested mode", func(t *testing.T) {
		pinUmaskZero(t)

		dir := t.TempDir()
		h := RotatingFileHandler(dir, "app",
			WithMaxFileSize(5), WithMaxFiles(20), WithCompress(), WithFileMode(0o600))

		writeRotating(t, h, "aaaaa") // 6 > 5 -> rotate index 1, gzipped
		writeRotating(t, h, "bbbbb") // 6 > 5 -> rotate index 2, gzipped

		fi, err := os.Stat(filepath.Join(dir, idxName("app", 1)+"."+gzipExtension))
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o600), fi.Mode().Perm(),
			"the gzip backup must be created with the WithFileMode bits, not the default")
	})

	t.Run("default without the option stays 0640", func(t *testing.T) {
		pinUmaskZero(t)

		dir := t.TempDir()
		h := RotatingFileHandler(dir, "app")
		writeRotating(t, h, "hello")

		fi, err := os.Stat(filepath.Join(dir, idxName("app", 1)))
		require.NoError(t, err)
		require.Equal(t, os.FileMode(defaultFilePermissions), fi.Mode().Perm(),
			"with no WithFileMode the default 0640 must be byte-identical to before")
	})
}
