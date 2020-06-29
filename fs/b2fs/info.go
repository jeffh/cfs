package b2fs

import (
	"os"
	"strings"
	"time"

	"github.com/jeffh/b2client/b2"
)

type fileInfo struct {
	F            b2.File
	NameOffset   int
	NameOverride string
}

var _ os.FileInfo = (*fileInfo)(nil)

func (fi *fileInfo) Name() string {
	if fi.NameOverride != "" {
		return fi.NameOverride
	}
	return fi.F.FileName[fi.NameOffset:]
}
func (fi *fileInfo) Size() int64        { return fi.F.ContentLength }
func (fi *fileInfo) Mode() os.FileMode  { return 0644 }
func (fi *fileInfo) ModTime() time.Time { return time.Unix(0, fi.F.UploadTimestampMillis) }
func (fi *fileInfo) IsDir() bool        { return false }
func (fi *fileInfo) Sys() interface{}   { return fi.F }

type bucketInfo struct {
	B            b2.Bucket
	lastModified time.Time
}

var _ os.FileInfo = (*bucketInfo)(nil)

func (bi *bucketInfo) Name() string       { return bi.B.BucketName }
func (bi *bucketInfo) Size() int64        { return 0 }
func (bi *bucketInfo) Mode() os.FileMode  { return 0755 | os.ModeDir }
func (bi *bucketInfo) ModTime() time.Time { return bi.lastModified }
func (bi *bucketInfo) IsDir() bool        { return true }
func (bi *bucketInfo) Sys() interface{}   { return bi.B }

type staticDirInfo struct {
	name         string
	lastModified time.Time
}

var _ os.FileInfo = (*staticDirInfo)(nil)

func (di *staticDirInfo) Name() string       { return di.name }
func (di *staticDirInfo) Size() int64        { return 0 }
func (di *staticDirInfo) Mode() os.FileMode  { return 0755 | os.ModeDir }
func (di *staticDirInfo) ModTime() time.Time { return di.lastModified }
func (di *staticDirInfo) IsDir() bool        { return true }
func (di *staticDirInfo) Sys() interface{}   { return nil }

type staticFileInfo struct {
	name         string
	lastModified time.Time
}

var _ os.FileInfo = (*staticFileInfo)(nil)

func (fi *staticFileInfo) Name() string       { return fi.name }
func (fi *staticFileInfo) Size() int64        { return 0 }
func (fi *staticFileInfo) Mode() os.FileMode  { return 0644 }
func (fi *staticFileInfo) ModTime() time.Time { return fi.lastModified }
func (fi *staticFileInfo) IsDir() bool        { return false }
func (fi *staticFileInfo) Sys() interface{}   { return nil }

type bucketOp string

const (
	bucketOpNone           bucketOp = ""
	bucketOpObjectData              = "objects"
	bucketOpObjectMetadata          = "objects-metadata"
)

type intent struct {
	bucketType b2.BucketType
	bucketName string
	op         bucketOp
	key        string
}

// parse /<bucketType/<bucketName>/<bucketOp>/<key...>
func parseIntent(forWriting bool, path string) (i intent, err error) {
	parts := strings.Split(path, "/")
	L := len(parts)
	for L > 0 && parts[0] == "" {
		parts = parts[1:]
		L--
	}

	if L == 0 {
		return
	}

	if L >= 1 {
		switch parts[0] {
		case "all":
			i.bucketType = b2.BucketTypeAll
		case "private":
			i.bucketType = b2.BucketTypePrivate
		case "public":
			i.bucketType = b2.BucketTypePublic
		case "snapshot":
			i.bucketType = b2.BucketTypeSnapshot
		default:
			err = os.ErrNotExist
			return
		}
	}
	if L >= 2 {
		i.bucketName = parts[1]
	}
	if L >= 3 {
		switch parts[2] {
		case "objects":
			i.op = bucketOpObjectData
			i.op = bucketOp(parts[2])
		default:
			err = os.ErrNotExist
			return
		}
	}
	if L >= 4 {
		i.key = strings.Join(parts[3:], "/")
	}
	return
}
