//go:build unix

package extensions

import (
	"errors"
	"os"
	"syscall"
)

// checkFolderOwner reports an error when fi, an Lstat result for the temp-fallback log
// folder, is not owned by the effective user, so a directory another user pre-created
// in the shared temp directory is not adopted. A FileInfo without a *syscall.Stat_t is
// accepted.
func checkFolderOwner(fi os.FileInfo) error {
	stat, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return nil
	}
	if stat.Uid != uint32(os.Geteuid()) {
		return errors.New("directory is not owned by the current user")
	}
	return nil
}
