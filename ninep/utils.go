package ninep

import (
	"errors"
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
	return err != nil && (strings.Index(err.Error(), "use of closed network connection") != -1 || errors.Is(err, io.EOF))
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

func acceptRversion(c Loggable, rwc net.Conn, txn *cltTransaction, maxMsgSize, minMsgSize uint32) (uint32, error) {
	c.Tracef("Tversion(%d, %s)", maxMsgSize, VERSION_9P2000)
	txn.req.Tversion(maxMsgSize, VERSION_9P2000)
	if err := txn.req.writeRequest(rwc); err != nil {
		c.Errorf("failed to write version: %s", err)
		return 0, err
	}

	if err := txn.res.readReply(rwc); err != nil {
		c.Errorf("failed to read version: %s", err)
		return 0, err
	}

	request, ok := txn.res.Reply().(Rversion)
	if !ok {
		c.Errorf("failed to negotiate version: unexpected message type: %d", txn.req.requestType())
		return 0, ErrBadFormat
	}

	if !strings.HasPrefix(request.Version(), VERSION_9P) {
		c.Tracef("unsupported server version: %s", request.Version())
		return 0, ErrBadFormat
	}

	size := request.MsgSize()
	if size > maxMsgSize {
		c.Errorf("server returned size higher than client gave: (server: %d > client: %d)", size, maxMsgSize)
		return 0, ErrBadFormat
	}
	maxMsgSize = request.MsgSize()
	if minMsgSize > maxMsgSize {
		c.Errorf("server returned size lower than client supports: (server: %d < client: [gave: %d; min: %d])", size, maxMsgSize, minMsgSize)
		return 0, ErrBadFormat
	}

	c.Tracef("Accepted Rversion (msgSize=%d)", maxMsgSize)

	return maxMsgSize, nil
}
