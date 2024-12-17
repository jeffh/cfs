package s3fs

import (
	"errors"
	"io/fs"

	"github.com/aws/smithy-go"
	"github.com/jeffh/cfs/ninep"
)

var (
	ErrUseMkDirToCreateBucket = errors.New("create a directory to create a bucket")
	ErrMustOpenForReading     = errors.New("must open for reading results")
)

func isFatalAwsErr(err error) bool {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		switch ae.ErrorCode() {
		case "AccessDenied", "AllAccessDisabled":
			return true
		}
	}
	return false
}

func mapAwsErrToNinep(err error) error {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		switch ae.ErrorCode() {
		case "AccessDenied", "AllAccessDisabled":
			return ninep.ErrInvalidAccess
		case "BucketAlreadyExists":
			return fs.ErrExist
		case "NoSuchKey":
			return fs.ErrNotExist
		case "NoSuchBucket":
			return fs.ErrNotExist
		}
	}
	return err
}
