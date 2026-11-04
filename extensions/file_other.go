//go:build !unix

package extensions

import "os"

// checkFolderOwner is a no-op on platforms without Unix ownership semantics.
func checkFolderOwner(os.FileInfo) error { return nil }
