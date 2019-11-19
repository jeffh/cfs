package ninep

import "errors"

var (
	ErrBadFormat = errors.New("Unrecognized 9P protocol")

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
