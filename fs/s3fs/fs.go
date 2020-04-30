// Implements a 9p file system that talks to Amazon's S3 Object Storage
// Service.
//
// Also supports any S3-compatible service as well.
package s3fs

import (
	"io"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jeffh/cfs/ninep"
)

func stringPtrOrNil(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

type S3Ctx struct {
	Session *session.Session
	Client  *s3.S3
}

// Reasonable default configuration of NewFs(), an empty string of endpoint
// defaults to AWS' S3 service
func NewBasicFs(endpoint string) ninep.FileSystem {
	cfg := &aws.Config{
		Endpoint: stringPtrOrNil(endpoint),
	}
	sess := session.Must(session.NewSession())
	svc := s3.New(sess, cfg)
	ctx := S3Ctx{
		Session: sess,
		Client:  svc,
	}

	return NewFs(&ctx)
}

func NewFs(s3c *S3Ctx) ninep.FileSystem {
	return &ninep.SimpleWalkableFileSystem{
		ninep.SimpleFileSystem{
			Root: ninep.StaticRootDir(
				&buckets{
					ninep.SimpleFileInfo{
						FIName:    "buckets",
						FIMode:    os.ModeDir | 0755,
						FIModTime: time.Now(),
					},
					s3c,
				},
			),
		},
	}
}

////////////////////////////////////////////////////////////////////////////////

func staticDir(name string, children ...ninep.Node) *ninep.StaticReadOnlyDir {
	return &ninep.StaticReadOnlyDir{
		SimpleFileInfo: ninep.SimpleFileInfo{
			FIName: name,
			FIMode: os.ModeDir | 0555,
		},
		Children: children,
	}
}

func staticDirWithTime(name string, modTime time.Time, children ...ninep.Node) *ninep.StaticReadOnlyDir {
	return &ninep.StaticReadOnlyDir{
		SimpleFileInfo: ninep.SimpleFileInfo{
			FIName:    name,
			FIMode:    os.ModeDir | 0555,
			FIModTime: modTime,
		},
		Children: children,
	}
}

func dynamicDir(name string, resolve func() ([]ninep.Node, error)) *ninep.DynamicReadOnlyDir {
	return &ninep.DynamicReadOnlyDir{
		SimpleFileInfo: ninep.SimpleFileInfo{
			FIName: name,
			FIMode: os.ModeDir | 0555,
		},
		GetChildren: resolve,
	}
}

func dynamicDirItr(name string, resolve func() (ninep.NodeIterator, error)) *ninep.DynamicReadOnlyDirItr {
	return &ninep.DynamicReadOnlyDirItr{
		SimpleFileInfo: ninep.SimpleFileInfo{
			FIName: name,
			FIMode: os.ModeDir | 0777,
		},
		GetChildren: resolve,
	}
}

func dynamicCtlFile(name string, thread func(m ninep.OpenMode, r io.Reader, w io.Writer)) *ninep.SimpleFile {
	return ninep.CtlFile(name, 0777, time.Time{}, thread)
}

func staticStringFile(name string, modTime time.Time, contents string) *ninep.SimpleFile {
	return ninep.StaticReadOnlyFile(name, 0444, modTime, []byte(contents))
}

func dynamicStringFile(name string, modTime time.Time, content func() ([]byte, error)) *ninep.SimpleFile {
	return ninep.DynamicReadOnlyFile(name, 0444, modTime, content)
}
