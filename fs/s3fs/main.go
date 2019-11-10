package s3fs

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jeffh/cfs/ninep"
)

type objectOperation int

const (
	opData objectOperation = iota
)

func objectsForBucket(s3c *s3.S3, bucketName string) func() ([]ninep.Node, error) {
	return func() ([]ninep.Node, error) {
		return nil, nil
	}
}

////////////////////////////////

type objectsItr struct {
	s3c        *s3.S3
	bucketName string
	input      s3.ListObjectsV2Input
	output     *s3.ListObjectsV2Output
	index      int
	op         objectOperation
}

func (itr *objectsItr) NextNode() (ninep.Node, error) {
	if itr.output == nil {
		var err error
		itr.output, err = itr.s3c.ListObjectsV2(&itr.input)
		fmt.Printf("[S3] ListObjects(%#v): %v %v\n", itr.output, err)
		if err != nil {
			return nil, err
		}
		if len(itr.output.Contents) == 0 {
			return nil, io.EOF
		}
		itr.input.ContinuationToken = itr.output.NextContinuationToken
	} else if itr.index == len(itr.output.Contents) && itr.output.NextContinuationToken == nil {
		return nil, io.EOF
	}

	object := itr.output.Contents[itr.index]
	itr.index++
	if itr.index == len(itr.output.Contents) {
		if itr.output.NextContinuationToken != nil {
			itr.index = 0
			itr.output = nil
		}
	}
	bucketName := itr.bucketName
	s3c := itr.s3c
	f := ninep.StreamingReadOnlyFile(*object.Key, 0777, time.Time{}, func(w io.Writer) {
		var err error
		wp := w.(*io.PipeWriter)
		switch itr.op {
		case opData:
			var resp *s3.GetObjectOutput
			resp, err = s3c.GetObject(&s3.GetObjectInput{
				Bucket: aws.String(bucketName),
				Key:    object.Key,
			})
			if err == nil {
				_, err = io.Copy(w, resp.Body)
				resp.Body.Close()
			}
		}
		wp.CloseWithError(err)
	})
	return f, nil
}

func (itr *objectsItr) Reset() error {
	itr.input.ContinuationToken = nil
	itr.output = nil
	itr.index = 0
	return nil
}

func (itr *objectsItr) Close() error {
	itr.output = nil
	return nil
}

////////////////////////////////

// Represents an s3 bucket. For speed, this attempts to minimize the number of s3 api calls.
//
// Directory tree:
//  /objects/data/<key>
//  /objects/etag/<key>
//  /objects/storage-class/<key>
//  /objects/version/<key>
//  /objects/.../<key>
//  /create-object
//  /acl
//  /cors
//  /encryption
//  /logging
//  /location

type buckets struct {
	ninep.SimpleFileInfo
	s3c *s3.S3
}

func (b *buckets) Info() (os.FileInfo, error)     { return &b.SimpleFileInfo, nil }
func (b *buckets) WriteInfo(in os.FileInfo) error { return ninep.ErrUnsupported }
func (b *buckets) Delete(name string) error       { return ninep.ErrUnsupported }
func (b *buckets) List() (ninep.NodeIterator, error) {
	resp, err := b.s3c.ListBuckets(&s3.ListBucketsInput{})
	fmt.Printf("[S3] ListBuckets(): %v %v\n", resp, err)
	if err != nil {
		return nil, err
	}
	var uid string
	if owner := resp.Owner; owner != nil {
		if displayName := owner.DisplayName; displayName != nil {
			uid = *displayName
		}
	}
	now := time.Now()
	nodes := make([]ninep.Node, 0, len(resp.Buckets))
	for _, bucket := range resp.Buckets {
		if bucket != nil && bucket.Name != nil {
			var creationDate time.Time
			if bucket.CreationDate != nil {
				creationDate = *bucket.CreationDate
			} else {
				creationDate = now
			}
			nodes = append(nodes, bucketDir(b.s3c, *bucket.Name, creationDate, now, uid))
		}
	}
	return ninep.MakeNodeSliceIterator(nodes), nil
}
func (b *buckets) Walk(subpath []string) (ninep.Node, error) {
	size := len(subpath)
	if size < 1 {
		panic("Unexpected empty path to walk")
	}

	bucket := subpath[0]
	if size > 1 {
		if subpath[1] == "objects" {
			if size > 2 {
			} else {
				// list all objects
				dir := dynamicDirItr("objects", func() (ninep.NodeIterator, error) {
					itr := &objectsItr{
						s3c:        b.s3c,
						bucketName: bucket,
						input: s3.ListObjectsV2Input{
							Bucket: aws.String(bucket),
						},
					}
					return itr, nil
				})
				return dir, nil

			}
		} else {
			// try another path?
		}
	} else {
		// TODO: return bucketDir()
		return bucketDir(b.s3c, bucket, time.Time{}, time.Now(), ""), nil
	}

	return nil, os.ErrNotExist
}
func (b *buckets) CreateFile(name string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
	return nil, ninep.ErrUnsupported
}
func (b *buckets) CreateDir(name string, mode ninep.Mode) error {
	return ninep.ErrUnsupported
}

func bucketDir(svc *s3.S3, bucketName string, creationDate time.Time, now time.Time, uid string) ninep.Node {
	// TODO: use uid

	dir := &ninep.StaticReadOnlyDir{
		SimpleFileInfo: ninep.SimpleFileInfo{
			FIName:    bucketName,
			FIMode:    os.ModeDir | 0777,
			FIModTime: creationDate,
		},
		// FileInfo: ninep.FileInfoWithUsers(
		// 	&ninep.SimpleFileInfo{
		// 		FIName:    bucketName,
		// 		FIMode:    os.ModeDir | 0777,
		// 		FIModTime: creationDate,
		// 	},
		// 	uid,
		// 	"",
		// 	"",
		// ),
		Children: []ninep.Node{
			dynamicDir("objects", objectsForBucket(svc, bucketName)),
			dynamicStringFile("location", now, func() ([]byte, error) {
				out, err := svc.GetBucketLocation(&s3.GetBucketLocationInput{Bucket: aws.String(bucketName)})
				if err != nil {
					return nil, err
				}
				var loc string
				if location := out.LocationConstraint; location != nil {
					loc = *location
				}
				return []byte(loc), nil
			}),
		},
	}
	return dir
}

func bucketsDir(svc *s3.S3) func() ([]ninep.Node, error) {
	return func() ([]ninep.Node, error) {
		resp, err := svc.ListBuckets(&s3.ListBucketsInput{})
		if err != nil {
			return nil, err
		}
		var uid string
		if owner := resp.Owner; owner != nil {
			if displayName := owner.DisplayName; displayName != nil {
				uid = *displayName
			}
		}
		now := time.Now()
		nodes := make([]ninep.Node, 0, len(resp.Buckets))
		for _, bucket := range resp.Buckets {
			if bucket != nil && bucket.Name != nil {
				var creationDate time.Time
				if bucket.CreationDate != nil {
					creationDate = *bucket.CreationDate
				} else {
					creationDate = now
				}
				nodes = append(nodes, bucketDir(svc, *bucket.Name, creationDate, now, uid))
			}
		}
		return nodes, nil
	}
}

func NewFs(svc *s3.S3) ninep.FileSystem {
	return &ninep.SimpleFileSystem{
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
				svc,
			},
		),
	}
}

////////////////////////////////////////////////////////////////////////////////

func staticDir(name string, children ...ninep.Node) *ninep.StaticReadOnlyDir {
	return &ninep.StaticReadOnlyDir{
		SimpleFileInfo: ninep.SimpleFileInfo{
			FIName: name,
			FIMode: os.ModeDir | 0777,
		},
		Children: children,
	}
}

func staticDirWithTime(name string, modTime time.Time, children ...ninep.Node) *ninep.StaticReadOnlyDir {
	return &ninep.StaticReadOnlyDir{
		SimpleFileInfo: ninep.SimpleFileInfo{
			FIName:    name,
			FIMode:    os.ModeDir | 0777,
			FIModTime: modTime,
		},
		Children: children,
	}
}

func dynamicDir(name string, resolve func() ([]ninep.Node, error)) *ninep.DynamicReadOnlyDir {
	return &ninep.DynamicReadOnlyDir{
		SimpleFileInfo: ninep.SimpleFileInfo{
			FIName: name,
			FIMode: os.ModeDir | 0777,
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
