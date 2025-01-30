package fileutil

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/go-set/v3"
	"github.com/zeebo/xxh3"

	"github.com/pomerium/pomerium/internal/log"
	"github.com/pomerium/pomerium/internal/signal"
)

const (
	pollInterval    = 200 * time.Millisecond
	recheckInterval = time.Second
)

type watchedFile struct {
	path     string
	linkSize int64
	linkTime int64
	fileSize int64
	fileTime int64
	hash     uint64
}

func (f *watchedFile) check(force bool) (changed bool) {
	// first check the file not following symlinks
	li, err := os.Lstat(f.path)
	if err != nil {
		maybeLogFileError(f.path, err)
		changed = swap(&f.linkSize, 0) || changed
		changed = swap(&f.linkTime, 0) || changed
	} else {
		changed = swap(&f.linkSize, li.Size()) || changed
		changed = swap(&f.linkTime, getFileModTime(li)) || changed
	}

	// next check the file following symlinks
	fi, err := os.Stat(f.path)
	if err != nil {
		maybeLogFileError(f.path, err)
		changed = swap(&f.fileSize, 0) || changed
		changed = swap(&f.fileTime, 0) || changed
	} else {
		changed = swap(&f.fileSize, fi.Size()) || changed
		changed = swap(&f.fileTime, getFileModTime(fi)) || changed
	}

	// if something changed, or we're forcing a check, do a full check on the file contents
	if changed || force {
		hash, err := hashFile(f.path)
		if err != nil {
			maybeLogFileError(f.path, err)
			changed = swap(&f.hash, 0)
		} else {
			changed = swap(&f.hash, hash)
		}
	}

	return changed
}

// A Watcher watches files for changes. It periodically polls the file size and file modification time.
// If the size or time has changed the file contents are hashed and if a change is detected a signal is
// broadcast. If a file cannot be read, the size, time and hash are all set to 0, so repeated errors
// should only signal once. If a file's time is changed but not the contents, no signal will be broadcast.
//
// After a change is detected via size or time, the watcher will recheck the file after a short time.
// This is to handle any possible lack of precision in filesystem modification times.
type Watcher struct {
	*signal.Signal

	cancelCtx context.Context
	cancel    context.CancelFunc

	mu       sync.Mutex
	watching map[string]*watchedFile
}

// NewWatcher creates a new Watcher.
func NewWatcher() *Watcher {
	w := &Watcher{
		Signal:   signal.New(),
		watching: make(map[string]*watchedFile),
	}
	w.cancelCtx, w.cancel = context.WithCancel(context.Background())
	go w.run()
	return w
}

func (w *Watcher) Stop() {
	w.cancel()
}

// Watch updates the watched file paths.
func (w *Watcher) Watch(filePaths []string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	seen := set.From(filePaths)
	for path := range w.watching {
		if !seen.Contains(path) {
			// remove file that's no longer tracked
			delete(w.watching, path)
		}
	}

	for _, path := range filePaths {
		_, ok := w.watching[path]
		if !ok {
			// add new file
			f := &watchedFile{path: path}
			// call check once so we don't signal immediately on an added file
			f.check(true)
			w.watching[path] = f
		}
	}
}

func (w *Watcher) run() {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.cancelCtx.Done():
			return
		case <-ticker.C:
			w.check()
		}
	}
}

func (w *Watcher) check() {
	w.mu.Lock()
	defer w.mu.Unlock()

	changed := false
	for _, f := range w.watching {
		if f.check(false) {
			changed = true
			go w.waitAndRecheck(f.path)
		}
	}
	if changed {
		w.Broadcast(w.cancelCtx)
	}
}

func (w *Watcher) waitAndRecheck(path string) {
	// wait for the recheck interval
	select {
	case <-w.cancelCtx.Done():
		return
	case <-time.After(recheckInterval):
	}

	// check the file again, this time force-computing the hash
	// in case the file modification time lacks the precision
	// necessary for a change to be detected
	w.mu.Lock()
	defer w.mu.Unlock()

	f := w.watching[path]
	if f == nil {
		return
	}
	if f.check(true) {
		w.Broadcast(w.cancelCtx)
	}
}

func getFileModTime(fi fs.FileInfo) int64 {
	tm := fi.ModTime()
	// UnixNano on a zero time is undefined, so just always return 0 for that
	if tm.IsZero() {
		return 0
	}
	return tm.UnixNano()
}

func hashFile(path string) (uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}

	h := xxh3.New()
	_, err = io.Copy(h, f)
	if err != nil {
		_ = f.Close()
		return 0, err
	}

	err = f.Close()
	if err != nil {
		return 0, err
	}

	return h.Sum64(), nil
}

func maybeLogFileError(path string, err error) {
	if !errors.Is(err, fs.ErrNotExist) {
		log.Error().Err(err).Str("path", path).Msg("file-watcher: error hashing file")
	}
}

func swap[T comparable](current *T, value T) (changed bool) {
	if *current == value {
		return false
	}
	*current = value
	return true
}
