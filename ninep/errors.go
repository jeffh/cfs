package ninep

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
)

var (
	ErrInvalidMessage = errors.New("invalid 9P message")

	ErrUnrecognizedFid = errors.New("referred to unknown fid")
	ErrFidExists       = errors.New("attempted to create a new fid where one already exists")

	ErrChangeUidNotAllowed = errors.New("changing uid is not allowed by protocol")
	ErrChangeGidNotAllowed = errors.New("not allowed to change gid")

	ErrWriteNotAllowed = fmt.Errorf("%w: not allowed to write", fs.ErrPermission)
	ErrReadNotAllowed  = fmt.Errorf("%w: not allowed to read", fs.ErrPermission)
	ErrUnsupported     = errors.New("unsupported")
	ErrSeekNotAllowed  = fmt.Errorf("%w: seeking is not allowed", ErrUnsupported)
	ErrNotImplemented  = errors.New("not implemented")
	ErrInvalidAccess   = fs.ErrPermission
)

var ErrServerClosed = errors.New("server closed")

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

	ErrInvalidMessage,
	ErrWriteNotAllowed,
	ErrReadNotAllowed,
	ErrSeekNotAllowed,
	ErrUnsupported,
	ErrNotImplemented,
	// ErrInvalidAccess,
	ErrChangeUidNotAllowed,
}
