// Package fileutil provides file utility functions, complementing the
// lower level abstractions found in the standard library.
package fileutil

import (
	"fmt"
	"io"
	"os"
)

// CopyFileUpTo copies content of the file up to maxBytes
// it returns an error if file is larger than allowed maximum
func CopyFileUpTo(dst io.Writer, fname string, maxBytes int64) error {
	fd, err := os.Open(fname)
	if err != nil {
		return fmt.Errorf("open %s: %w", fname, err)
	}
	defer func() { _ = fd.Close() }()

	fi, err := fd.Stat()
	if err != nil {
		return fmt.Errorf("stat %s: %w", fname, err)
	}
	if fi.Size() > maxBytes {
		return fmt.Errorf("file %s size %d > max %d", fname, fi.Size(), maxBytes)
	}

	if _, err := io.Copy(dst, fd); err != nil {
		return fmt.Errorf("read %s: %w", fname, err)
	}

	return nil
}
