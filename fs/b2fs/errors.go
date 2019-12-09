package b2fs

import (
	"errors"
	"os"

	"github.com/kurin/blazer/b2"
)

var (
	ErrUseMkDirToCreateBucket = errors.New("Create a directory to create a bucket")
	ErrMustOpenForReading     = errors.New("Must open for reading results")
)

func mapB2ErrToNinep(err error) error {
	if b2.IsNotExist(err) {
		return os.ErrNotExist
	}
	return err
}
