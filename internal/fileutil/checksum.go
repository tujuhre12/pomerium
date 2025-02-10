package fileutil

import (
	"errors"
	"io/fs"
	"os"
	"syscall"

	"github.com/pomerium/pomerium/internal/hashutil"
)

// StatCheckSum returns a checksum of the file info. It is valid to run this
// function against a file path that doesn't exist and an error will not be returned.
// The file path, size, modification time, device id and inode id are used to compute
// the hash. Any change to this data, even if the underlying file contents are the
// same, will result in a new checksum, and vice-versa, if the underlying contents
// change, but none of the other data does, the checksum will be the same.
func StatCheckSum(filePath string) (uint64, error) {
	d := hashutil.NewDigestWithSeed(7968108)
	d.WriteStringWithLen(filePath)

	for _, fn := range []func(string) (os.FileInfo, error){os.Stat, os.Lstat} {
		fi, err := fn(filePath)
		if errors.Is(err, fs.ErrNotExist) {
			_, _ = d.Write([]byte{0})
		} else if err != nil {
			return 0, err
		} else {
			d.WriteInt64(fi.Size())
			d.WriteInt64(fi.ModTime().Unix())
			if s, ok := fi.Sys().(*syscall.Stat_t); ok {
				d.WriteUint64(s.Dev)
				d.WriteUint64(s.Ino)
			}
		}
	}

	return d.Sum64(), nil
}
