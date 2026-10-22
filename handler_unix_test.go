//go:build unix

package loginjector

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestWithFileMode covers WithFileMode threading the configured permission bits through
// the live-file open and the gzip temp create, while the default stays 0640. The subtests
// pin the process umask to 0 so the requested mode is observed unmasked; umask is
// process-global, so these subtests are not parallel and restore it on cleanup.
//
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
