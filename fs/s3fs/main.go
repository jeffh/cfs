package s3fs

import (
	"io"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jeffh/cfs/ninep"
)

type S3Ctx struct {
	Session *session.Session
	Client  *s3.S3
}

func NewFs(s3c *S3Ctx) ninep.FileSystem {
	return &ninep.SimpleWalkableFileSystem{
		ninep.SimpleFileSystem{
			Root: ninep.StaticRootDir(
				dynamicCtlFile("ctl", func(r io.Reader, w io.Writer) {
					rp := r.(*io.PipeReader)
					wp := w.(*io.PipeWriter)
					rp.CloseWithError(ninep.ErrUnsupported)
					wp.CloseWithError(ninep.ErrUnsupported)
				}),
				staticDir("presigned"),
				&buckets{
					ninep.SimpleFileInfo{
						FIName:    "buckets",
						FIMode:    os.ModeDir | 0777,
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

func dynamicCtlFile(name string, thread func(r io.Reader, w io.Writer)) *ninep.SimpleFile {
	return ninep.CtlFile(name, 0777, time.Time{}, thread)
}

func staticStringFile(name string, modTime time.Time, contents string) *ninep.SimpleFile {
	return ninep.StaticReadOnlyFile(name, 0444, modTime, []byte(contents))
}

func dynamicStringFile(name string, modTime time.Time, content func() ([]byte, error)) *ninep.SimpleFile {
	return ninep.DynamicReadOnlyFile(name, 0777, modTime, content)
}
