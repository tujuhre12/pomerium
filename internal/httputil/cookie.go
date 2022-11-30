package httputil

import (
	"fmt"
	"net/http"
	"strings"
)

const (
	// ChunkedCanaryByte is the byte value used as a canary prefix to distinguish if
	// the cookie is multi-part or not. This constant *should not* be valid
	// base64. It's important this byte is ASCII to avoid UTF-8 variable sized runes.
	// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Set-Cookie#Directives
	ChunkedCanaryByte byte = '%'
	// MaxChunkSize sets the upper bound on a cookie chunks payload value.
	// Note, this should be lower than the actual cookie's max size (4096 bytes)
	// which includes metadata.
	MaxChunkSize = 3800
	// MaxNumChunks limits the number of chunks to iterate through. Conservatively
	// set to prevent any abuse.
	MaxNumChunks = 5
)

// SetChunkedCookie sets a cookie that supports chunking when too large.
func SetChunkedCookie(w http.ResponseWriter, cookie *http.Cookie) {
	if len(cookie.String()) <= MaxChunkSize {
		http.SetCookie(w, cookie)
		return
	}
	for i, c := range chunk(cookie.Value, MaxChunkSize) {
		// start with a copy of our original cookie
		nc := *cookie
		if i == 0 {
			// if this is the first cookie, add our canary byte
			nc.Value = fmt.Sprintf("%s%s", string(ChunkedCanaryByte), c)
		} else {
			// subsequent parts will be postfixed with their part number
			nc.Name = fmt.Sprintf("%s_%d", cookie.Name, i)
			nc.Value = c
		}
		http.SetCookie(w, &nc)
	}
}

// LoadChunkedCookie loads a cookie that supports chunking when too large.
func LoadChunkedCookie(r *http.Request, name string) (*http.Cookie, error) {
	cookie, err := r.Cookie(name)
	if err != nil {
		return nil, err
	}

	if len(cookie.Value) == 0 || cookie.Value[0] != ChunkedCanaryByte {
		return cookie, nil
	}

	data := cookie.Value
	var b strings.Builder
	fmt.Fprintf(&b, "%s", data[1:])
	for i := 1; i <= MaxNumChunks; i++ {
		next, err := r.Cookie(fmt.Sprintf("%s_%d", cookie.Name, i))
		if err != nil {
			break // break if we can't find the next cookie
		}
		fmt.Fprintf(&b, "%s", next.Value)
	}
	cookie.Value = b.String()
	return cookie, nil
}

func chunk(s string, size int) []string {
	ss := make([]string, 0, len(s)/size+1)
	for len(s) > 0 {
		if len(s) < size {
			size = len(s)
		}
		ss, s = append(ss, s[:size]), s[size:]
	}
	return ss
}
