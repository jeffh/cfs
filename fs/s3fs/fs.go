// Implements a 9p file system that talks to Amazon's S3 Object Storage
// Service.
//
// Also supports any S3-compatible service as well.
package s3fs

import (
	"context"
	"io"
	"io/fs"
	"iter"
	"log/slog"
	"net/http"
	"os"
	"strings"
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

func dynamicDirItr(name string, resolve func() iter.Seq2[ninep.Node, error]) *ninep.DynamicReadOnlyDirItr {
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

type fsys struct {
	s3c    *S3Ctx
	logger slog.Logger
}

func (f *fsys) parsePath(path string) (op, bucket, bop, key string) {
	mx := http.NewServeMux()
	parts := strings.SplitN(path, "/", 3)
	switch len(parts) {
	case 0:
		return "", "", "", ""
	case 1:
		return parts[1], "", "", ""
	case 2:
		return parts[1], parts[2], "", ""
	case 3:
		return parts[1], parts[2], parts[3], ""
	default:
		return parts[1], parts[2], parts[3], parts[4]
	}
}

func (f *fsys) MakeDir(ctx context.Context, path string, mode ninep.Mode) error {
	op, bucket, bops, key := f.parsePath(path)
	switch op {
	case "buckets":
		if bucket == "" {
			return fs.ErrExist
		} else {
			if bops
			if key != "" {
				// TODO: create object?
			}
			// CreateBucket
			_, err := f.s3c.Client.CreateBucket(&s3.CreateBucketInput{
				Bucket: aws.String(bucket),
			})
			slog.InfoContext(ctx, "[S3] CreateBucket", slog.String("name", bucket), slog.Any("err", err))
			return mapAwsErrToNinep(err)
		}
	default:
		return ninep.ErrWriteNotAllowed
	}
}
