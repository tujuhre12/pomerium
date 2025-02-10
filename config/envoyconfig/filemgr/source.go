package filemgr

import (
	"io"
	"os"
	"path/filepath"

	"github.com/zeebo/xxh3"

	"github.com/pomerium/pomerium/internal/fileutil"
	"github.com/pomerium/pomerium/internal/hashutil"
)

// A Source is a data source that can write bytes to a destination and has an associated
// file name and checksum.
type Source interface {
	FileName() string
	Checksum() (uint64, error)
	io.WriterTo
}

type bytesSource struct {
	fileName string
	data     []byte
}

// BytesSource creates a source from a slice of bytes.
func BytesSource(fileName string, data []byte) Source {
	return bytesSource{
		fileName: fileName,
		data:     data,
	}
}

func (s bytesSource) FileName() string {
	return s.fileName
}

func (s bytesSource) Checksum() (uint64, error) {
	return xxh3.HashSeed(s.data, 7546535), nil
}

func (s bytesSource) WriteTo(dst io.Writer) (int64, error) {
	n, err := dst.Write(s.data)
	return int64(n), err
}

type fileSource struct {
	filePath string
}

// FileSource creates a source from a file.
func FileSource(filePath string) Source {
	return fileSource{
		filePath: filePath,
	}
}

func (s fileSource) FileName() string {
	return filepath.Base(s.filePath)
}

func (s fileSource) Checksum() (uint64, error) {
	return fileutil.StatCheckSum(s.filePath)
}

func (s fileSource) WriteTo(dst io.Writer) (int64, error) {
	f, err := os.Open(s.filePath)
	if err != nil {
		return 0, err
	}

	n, err := f.WriteTo(dst)
	if err != nil {
		_ = f.Close()
		return n, err
	}

	return n, f.Close()
}

type multiSource struct {
	fileName  string
	separator []byte
	sources   []Source
}

// MultiSource creates a source from multiple sources. Each source is concatenated together
// with the separator between them. The Checksum is computed from each of the source
// checksums.
func MultiSource(fileName string, separator []byte, sources ...Source) Source {
	return &multiSource{
		fileName:  fileName,
		separator: separator,
		sources:   sources,
	}
}

func (s *multiSource) FileName() string {
	return s.fileName
}

func (s *multiSource) Checksum() (uint64, error) {
	h := hashutil.NewDigestWithSeed(4616647)
	_, _ = h.Write(s.separator)
	for _, ss := range s.sources {
		n, err := ss.Checksum()
		if err != nil {
			return 0, err
		}
		h.WriteUint64(n)
	}
	return h.Sum64(), nil
}

func (s *multiSource) WriteTo(dst io.Writer) (int64, error) {
	var total int64
	for i, ss := range s.sources {
		if i > 0 {
			n, err := dst.Write(s.separator)
			if err != nil {
				return 0, err
			}
			total += int64(n)
		}
		n, err := ss.WriteTo(dst)
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}
