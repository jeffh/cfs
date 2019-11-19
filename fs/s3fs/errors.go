package s3fs

import (
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/jeffh/cfs/ninep"
)

var (
	ErrUseMkDirToCreateBucket = errors.New("Create a directory to create a bucket")
)

func mapAwsToNinep(err error) error {
	switch e := err.(type) {
	case awserr.Error:
		fmt.Printf("CODE: %#v\n", e.Code())
		switch e.Code() {
		case "AccessDenied":
			return ninep.ErrInvalidAccess
		default:
			break
		}
		return e
	default:
		return err
	}
}
