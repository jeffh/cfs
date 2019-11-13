package s3fs

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/jeffh/cfs/ninep"
)

type S3Ctx struct {
	Session *session.Session
	Client  *s3.S3
}

type objectOperation int

const (
	opData objectOperation = iota
)

func keysMatch(objKey *string, wantedKey string) bool {
	fmt.Printf("%#v == %#v\n", *objKey, wantedKey)
	return objKey != nil && (*objKey == wantedKey || *objKey == wantedKey+"/")
}

func objectInfo(prefix string, object *s3.Object) os.FileInfo {
	var uid string
	if owner := object.Owner; owner != nil {
		if displayName := owner.DisplayName; displayName != nil {
			uid = *displayName
		} else if id := owner.ID; id != nil {
			uid = *id
		}
	}
	name := (*object.Key)[len(prefix):]
	if strings.HasPrefix(name, "/") {
		name = name[1:]
	}
	if name == "" {
		name = "/"
	}

	// From Docs: https://docs.aws.amazon.com/AmazonS3/latest/user-guide/using-folders.html
	//
	// "The Amazon S3 console treats all objects that have a
	// forward slash ("/") character as the last (trailing)
	// character in the key name as a folder, for example
	// examplekeyname/."
	isDir := strings.HasSuffix(*object.Key, "/")

	var dirMode os.FileMode
	if isDir {
		dirMode = os.ModeDir
	}
	return ninep.FileInfoWithUsers(
		&ninep.SimpleFileInfo{
			FIName:    name,
			FIMode:    0777 | dirMode,
			FIModTime: *object.LastModified,
			FISize:    *object.Size,
		},
		uid,
		"",  // gid
		uid, // muid
	)
}

func objectFileHandle(s3c *S3Ctx, bucketName, objectKey string, op objectOperation, m ninep.OpenMode) (ninep.FileHandle, error) {
	h := &ninep.RWFileHandle{}
	if m.IsReadable() {
		r, w := io.Pipe()
		go func() {
			var err error
			switch op {
			case opData:
				var resp *s3.GetObjectOutput
				input := &s3.GetObjectInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String(objectKey),
				}
				resp, err = s3c.Client.GetObject(input)
				if err == nil {
					_, err = io.Copy(w, resp.Body)
					resp.Body.Close()
				}
			}
			w.CloseWithError(err)
			r.Close()
		}()
		h.R = r
	}
	if m.IsWriteable() {
		r, w := io.Pipe()
		go func() {
			var err error
			switch op {
			case opData:
				uploader := s3manager.NewUploader(s3c.Session)
				_, err = uploader.Upload(&s3manager.UploadInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String(objectKey),
					Body:   r,
				})
			}
			w.CloseWithError(err)
			r.Close()
		}()
		h.W = w
	}
	return h, nil
}

// represents either a known object or an object in potential
type objectNode struct {
	// required
	s3c        *S3Ctx
	bucketName string
	op         objectOperation

	// optional
	prefix string

	// either this
	key string

	// or this
	obj *s3.Object

	// fill this if you want to override the Info value
	info os.FileInfo
}

func (o *objectNode) getPath(subpath []string) string {
	prefix := filepath.Clean(filepath.Join(o.prefix, filepath.Join(subpath...)))
	if prefix == "." {
		prefix = ""
	}
	return prefix
}

func (o *objectNode) getKey() string {
	if o.key != "" {
		return o.key
	}
	return *o.obj.Key
}

func (o *objectNode) List() (ninep.NodeIterator, error) {
	input := s3.ListObjectsV2Input{
		Bucket: aws.String(o.bucketName),
		Prefix: aws.String(o.key),
	}
	res, err := o.s3c.Client.ListObjectsV2(&input)
	if err != nil {
		return nil, err
	}
	itr := &objectsItr{
		s3c:        o.s3c,
		bucketName: o.bucketName,
		prefix:     o.prefix,
		input:      input,
		output:     res,
	}
	return itr, nil
}

func (o *objectNode) Walk(subpath []string) ([]ninep.Node, error) {
	if len(subpath) == 0 {
		return nil, nil
	}
	key := filepath.Join(o.getKey(), filepath.Join(subpath...))
	input := s3.ListObjectsV2Input{
		Bucket: aws.String(o.bucketName),
		Prefix: aws.String(key),
	}
	res, err := o.s3c.Client.ListObjectsV2(&input)
	fmt.Printf("[S3.objectNode.Walk] ListObjectsV2(%#v)\n", input)
	if err != nil {
		return nil, err
	}
	size := len(res.Contents)
	if size == 1 && keysMatch(res.Contents[0].Key, key) {
		object := res.Contents[0]
		objInfo := objectInfo(o.getPath(subpath), object)
		nodes := make([]ninep.Node, len(subpath))
		for i, part := range subpath[:size-1] {
			prefix := o.prefix
			if i > 1 {
				prefix = o.getPath(subpath[:i-1])
			} else {
				prefix = filepath.Clean(prefix)
				if prefix == "." {
					prefix = ""
				}
			}
			key := part
			if key == "." && i > 0 {
				key = subpath[i-1]
			}
			nodes[i] = &objectNode{
				s3c:        o.s3c,
				bucketName: o.bucketName,
				prefix:     prefix,
				key:        filepath.Join(o.getKey(), filepath.Join(subpath[:i]...)),
				op:         o.op,
				info: &ninep.SimpleFileInfo{
					FIName:    key,
					FIMode:    0777 | os.ModeDir,
					FIModTime: *object.LastModified,
					FISize:    *object.Size,
				},
			}
		}
		prefix := o.getPath(subpath[:len(subpath)-1])
		nodes[size-1] = &objectNode{
			s3c:        o.s3c,
			bucketName: o.bucketName,
			prefix:     prefix,
			key:        filepath.Join(o.getKey(), filepath.Join(subpath...)),
			obj:        object,
			info:       objInfo,
			op:         o.op,
		}
		return nodes, nil
	} else {
		fmt.Printf("====> IS **NOT** EXACT\n")
		nodes := make([]ninep.Node, len(subpath))
		for i, part := range subpath {
			prefix := o.prefix
			if i > 1 {
				prefix = o.getPath(subpath[:i])
			} else {
				prefix = filepath.Clean(prefix)
				if prefix == "." {
					prefix = ""
				}
			}
			var dirMode os.FileMode
			if strings.HasSuffix(part, "/") {
				dirMode = os.ModeDir
			}
			key := part
			if key == "." && i > 0 {
				key = subpath[i-1]
			}
			nodes[i] = &objectNode{
				s3c:        o.s3c,
				bucketName: o.bucketName,
				prefix:     prefix,
				key:        key,
				op:         o.op,
				info: &ninep.SimpleFileInfo{
					FIName: key,
					FIMode: 0777 | dirMode,
				},
			}
		}
		return nodes, nil
	}
}

func (o *objectNode) CreateFile(name string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
	return objectFileHandle(o.s3c, o.bucketName, filepath.Join(o.getKey(), name), o.op, flag)
}
func (o *objectNode) CreateDir(name string, mode ninep.Mode) error {
	return ninep.ErrUnsupported
}
func (o *objectNode) Delete(name string) error { return ninep.ErrUnsupported }

func (o *objectNode) Info() (os.FileInfo, error) {
	if o.info != nil {
		return o.info, nil
	}

	object := o.obj
	// TODO: should we cache the object fetch?
	if o.obj == nil {
		input := &s3.ListObjectsV2Input{
			Bucket:  aws.String(o.bucketName),
			Prefix:  aws.String(o.key),
			MaxKeys: aws.Int64(1),
		}
		res, err := o.s3c.Client.ListObjectsV2(input)
		fmt.Printf("[S3.objectNode.Info] ListObjectsV2(%#v) %#v %v\n", input, res, err)
		if err != nil {
			return nil, err
		}

		if len(res.Contents) == 1 {
			obj := res.Contents[0]
			if keysMatch(obj.Key, o.key) {
				object = obj
			}
		}
		if object == nil {
			return nil, os.ErrNotExist
		}
	}
	return objectInfo(o.prefix, object), nil
}
func (o *objectNode) WriteInfo(in os.FileInfo) error { return ninep.ErrUnsupported }
func (o *objectNode) Open(m ninep.OpenMode) (ninep.FileHandle, error) {
	return objectFileHandle(o.s3c, o.bucketName, o.getKey(), o.op, m)
}

func objectsOrObjectForBucket(s3c *S3Ctx, bucketName string, keyPrefix []string, op objectOperation) ([]ninep.Node, error) {
	keyPrefixPath := filepath.Clean(filepath.Join(keyPrefix...))
	if keyPrefixPath == "." {
		keyPrefixPath = ""
	}

	object := &objectNode{
		s3c:        s3c,
		bucketName: bucketName,
		op:         op,
		key:        keyPrefix[0],
	}
	rest, err := object.Walk(keyPrefix[1:])
	if err != nil {
		return nil, err
	}
	traversal := make([]ninep.Node, len(keyPrefix))
	traversal[0] = object
	copy(traversal[1:], rest)
	return traversal, nil
}

func objectsForBucket(s3c *S3Ctx, bucketName, keyPrefix string) (ninep.Node, error) {
	var prefix *string
	if keyPrefix != "" {
		prefix = aws.String(keyPrefix)
		// make first fetch to see if it's one file or not
	}
	input := s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: prefix,
	}
	dir := dynamicDirItr("objects", func() (ninep.NodeIterator, error) {
		itr := &objectsItr{
			s3c:        s3c,
			bucketName: bucketName,
			input:      input,
		}
		return itr, nil
	})
	return dir, nil
}

////////////////////////////////

type objectsItr struct {
	s3c        *S3Ctx
	bucketName string
	prefix     string
	input      s3.ListObjectsV2Input
	output     *s3.ListObjectsV2Output
	index      int
	op         objectOperation
}

func (itr *objectsItr) NextNode() (ninep.Node, error) {
	if itr.output == nil {
		var err error
		itr.output, err = itr.s3c.Client.ListObjectsV2(&itr.input)
		if err != nil {
			return nil, err
		}
		if len(itr.output.Contents) == 0 {
			return nil, io.EOF
		}
		itr.input.ContinuationToken = itr.output.NextContinuationToken
	} else if itr.index == len(itr.output.Contents) && (itr.output.IsTruncated == nil || !*itr.output.IsTruncated) {
		return nil, io.EOF
	}

	obj := itr.output.Contents[itr.index]
	itr.index++
	if itr.index == len(itr.output.Contents) && itr.output.IsTruncated != nil {
		if *itr.output.IsTruncated {
			itr.index = 0
			itr.output = nil
		}
	}
	file := &objectNode{
		s3c:        itr.s3c,
		bucketName: itr.bucketName,
		prefix:     itr.prefix,
		op:         itr.op,
		obj:        obj,
	}
	return file, nil
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
	s3c *S3Ctx
}

func (b *buckets) Info() (os.FileInfo, error)     { return &b.SimpleFileInfo, nil }
func (b *buckets) WriteInfo(in os.FileInfo) error { return ninep.ErrUnsupported }
func (b *buckets) Delete(name string) error       { return ninep.ErrUnsupported }
func (b *buckets) List() (ninep.NodeIterator, error) {
	resp, err := b.s3c.Client.ListBuckets(&s3.ListBucketsInput{})
	fmt.Printf("[S3] ListBuckets()\n")
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
func (b *buckets) Walk(subpath []string) ([]ninep.Node, error) {
	fmt.Printf("BUCKET_WALK(%#v)\n", subpath)
	size := len(subpath)
	if size < 1 {
		return nil, nil
	}

	var (
		nodes []ninep.Node
		err   error
	)

	bucket := subpath[0]
	nodes = append(nodes, bucketDir(b.s3c, bucket, time.Time{}, time.Now(), ""))
	if size > 1 {
		operation := subpath[1]
		switch operation {
		case "objects":
			if size > 2 && subpath[2] != "." {
				nodes = append(
					nodes, dynamicDirItr("objects", func() (ninep.NodeIterator, error) {
						itr := &objectsItr{
							s3c:        b.s3c,
							bucketName: bucket,
							input: s3.ListObjectsV2Input{
								Bucket: aws.String(bucket),
							},
							op: opData,
						}
						return itr, nil
					}),
				)
				var objNodes []ninep.Node
				objNodes, err = objectsOrObjectForBucket(b.s3c, bucket, subpath[2:], opData)
				if err == nil {
					nodes = append(nodes, objNodes...)
				}
			} else {
				// list all objects
				var node ninep.Node
				node, err = objectsForBucket(b.s3c, bucket, "")
				if err == nil {
					nodes = append(nodes, node)
					// repeat if "." is at the end...
					if size == 2 {
						nodes = append(nodes, node)
					}
				}
			}
		default:
			err = os.ErrNotExist
		}
	}

	if err != nil {
		return nil, err
	} else {
		return nodes, err
	}

}
func (b *buckets) CreateFile(name string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
	return nil, ninep.ErrUnsupported
}
func (b *buckets) CreateDir(name string, mode ninep.Mode) error {
	return ninep.ErrUnsupported
}

func bucketDir(s3c *S3Ctx, bucketName string, creationDate time.Time, now time.Time, uid string) ninep.Node {
	dir := &ninep.DynamicReadOnlyDir{
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
		GetChildren: func() ([]ninep.Node, error) {
			// TODO: use uid
			objects, err := objectsForBucket(s3c, bucketName, "")
			if err != nil {
				return nil, err
			}
			return []ninep.Node{objects}, nil
		},
	}
	return dir
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
