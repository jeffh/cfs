package ninep

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"
	"syscall"
)

var (
	ErrBadFormat = errors.New("unrecognized 9P message")

	ErrUnrecognizedFid = errors.New("unknown fid")
	ErrFidExists       = errors.New("attempted to create a new fid where one already exists")

	ErrChangeUidNotAllowed = errors.New("changing uid is not allowed by protocol")
	ErrChangeGidNotAllowed = errors.New("not allowed to change gid")

	ErrWriteNotAllowed = fmt.Errorf("%w: not allowed to write", fs.ErrPermission)
	ErrReadNotAllowed  = fmt.Errorf("%w: not allowed to read", fs.ErrPermission)
	ErrSeekNotAllowed  = errors.New("seeking is not allowed")
	ErrUnsupported     = errors.New("unsupported")
	ErrNotImplemented  = errors.New("not implemented")
	ErrInvalidAccess   = fs.ErrPermission
)

var ErrServerClosed = errors.New("server closed")

func isClosedErr(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "use of closed network connection") ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, io.EOF)

}

func isRetryable(err error) bool {
	return errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.EAGAIN) ||
		errors.Is(err, syscall.EALREADY) ||
		errors.Is(err, syscall.EBUSY) ||
		errors.Is(err, syscall.EINTR) ||
		errors.Is(err, syscall.EINPROGRESS) ||
		errors.Is(err, syscall.ETIMEDOUT) ||
		errors.Is(err, syscall.ETIME)
}

// this is a list of errors that we attempt to preserve equality of over the wire.
// basically if Rerror.Ename() == err.Error() where err is in this list, then
// Rerror.Error() should return err.
var mappedErrors []error = []error{
	fs.ErrInvalid,
	fs.ErrPermission,
	fs.ErrExist,
	fs.ErrNotExist,
	fs.ErrClosed,
	os.ErrNoDeadline,
	io.EOF,
	io.ErrClosedPipe,
	io.ErrNoProgress,
	io.ErrShortBuffer,
	io.ErrShortWrite,
	io.ErrUnexpectedEOF,

	ErrBadFormat,
	ErrWriteNotAllowed,
	ErrReadNotAllowed,
	ErrSeekNotAllowed,
	ErrUnsupported,
	ErrNotImplemented,
	// ErrInvalidAccess,
	ErrChangeUidNotAllowed,
}
