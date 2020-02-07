package ninep

import (
	"io"
	"net"
	"strings"
)

// Returns the parent path of the given path, or leave unchanged if cannot go up any more directories
func Dirname(path string) string {
	i := strings.LastIndex(path, "/")
	if i == -1 {
		return path
	}
	return path[:i+1]
}

// Returns the file of the given path
func Basename(path string) string {
	i := strings.LastIndex(path, "/")
	if i == -1 {
		return path
	}
	return path[i+1:]
}

func IsClosedSocket(err error) bool {
	return err != nil && strings.Index(err.Error(), "use of closed network connection") != -1
}

func IsTimeoutErr(err error) bool {
	if err, ok := err.(net.Error); ok && err.Timeout() {
		return true
	}
	return false
}

func IsTemporaryErr(err error) bool {
	type t interface {
		Temporary() bool
	}

	if err, ok := err.(t); ok {
		return err.Temporary()
	} else {
		return false
	}
}

func readUpTo(r io.Reader, p []byte) (int, error) {
	var err error
	n := 0
	for n < len(p) && err == nil {
		m, e := r.Read(p[n:])
		n += m
		if IsTimeoutErr(e) {
			return 0, e
		} else if IsTemporaryErr(e) {
			continue
		}
		err = e
	}
	return n, err
}
