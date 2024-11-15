package s3fs

import (
	"errors"
	"io/fs"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jeffh/cfs/ninep"
)

var (
	ErrUseMkDirToCreateBucket = errors.New("create a directory to create a bucket")
	ErrMustOpenForReading     = errors.New("must open for reading results")
)

func mapAwsErrToNinep(err error) error {
	switch e := err.(type) {
	case awserr.Error:
		switch e.Code() {
		case "AccessDenied", "AllAccessDisabled":
			return ninep.ErrInvalidAccess
		case s3.ErrCodeBucketAlreadyExists:
			return ninep.ErrInvalidAccess
		case s3.ErrCodeNoSuchKey:
			return fs.ErrNotExist
		case s3.ErrCodeNoSuchBucket:
			return fs.ErrNotExist
		default:
			break
		}
		return e
	default:
		return err
	}
}
