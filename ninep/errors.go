package ninep

import (
	"errors"
	"strings"
	"syscall"
)

var (
	ErrBadFormat = errors.New("Unrecognized 9P protocol")

	ErrUnrecognizedFid = errors.New("unknown fid")
	ErrFidExists       = errors.New("attempted to create a new fid where one already exists")

	ErrWriteNotAllowed = errors.New("not allowed to write")
	ErrReadNotAllowed  = errors.New("not allowed to read")
	ErrSeekNotAllowed  = errors.New("seeking is not allowed")
	ErrUnsupported     = errors.New("unsupported")
	ErrNotImplemented  = errors.New("not implemented")
	ErrInvalidAccess   = errors.New("invalid access permissions")

	ErrChangeUidNotAllowed = errors.New("changing uid is not allowed by protocol")

	// error from functions that require an iterator given to it (and not nil)
	ErrMissingIterator = errors.New("Internal error, no iterator returned, but got no error")
)

var ErrServerClosed = errors.New("server closed")

func isClosedErr(err error) bool {
	return err != nil && strings.Index(err.Error(), "use of closed network connection") != -1
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
