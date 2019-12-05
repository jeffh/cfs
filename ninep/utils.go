package ninep

import (
	"io"
	"net"
	"strings"
)

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

func IsAanRecoverableErr(err error) bool {
	return err == io.EOF || IsTimeoutErr(err) || IsTemporaryErr(err) || err == io.ErrUnexpectedEOF
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
