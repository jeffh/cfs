package s3fs

import "errors"

var (
	ErrUseMkDirToCreateBucket = errors.New("Create a directory to create a bucket")
)
