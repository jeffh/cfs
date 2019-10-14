package ninep

import "net"

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
