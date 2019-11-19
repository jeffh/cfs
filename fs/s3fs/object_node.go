package s3fs

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/jeffh/cfs/ninep"
)

type objectOperation int

const (
	opData objectOperation = iota
)

func keysMatch(objKey *string, wantedKey string) bool {
	return objKey != nil && (*objKey == wantedKey || *objKey == wantedKey+"/")
}

func objectInfo(nameOffset int, object *s3.Object, fallbackKey string) os.FileInfo {
	var (
		uid     string
		name    string
		key     string
		modTime time.Time
		size    int64
	)
	if object != nil {
		if owner := object.Owner; owner != nil {
			if displayName := owner.DisplayName; displayName != nil {
				uid = *displayName
			} else if id := owner.ID; id != nil {
				uid = *id
			}
		}
		key = *object.Key
		modTime = *object.LastModified
		size = *object.Size
	} else {
		key = fallbackKey
	}
	name = key[nameOffset:]
	if strings.HasPrefix(name, "/") {
		name = name[1:]
	}
	if strings.HasSuffix(name, "/") {
		name = name[:len(name)-1]
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
	isDir := strings.HasSuffix(key, "/")

	var dirMode os.FileMode
	if isDir {
		dirMode = os.ModeDir
	}
	return ninep.FileInfoWithUsers(
		&ninep.SimpleFileInfo{
			FIName:    name,
			FIMode:    0777 | dirMode,
			FIModTime: modTime,
			FISize:    size,
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
	nameOffset int // index into getKey()

	// either this
	key string

	// or this
	obj *s3.Object

	// fill this if you want to override the Info value
	info os.FileInfo
}

func (o *objectNode) getName() string {
	return o.getKey()[o.nameOffset:]
}

func (o *objectNode) getKey() string {
	if o.obj != nil && o.obj.Key != nil {
		return *o.obj.Key
	}
	return o.key
}

func (o *objectNode) List() (ninep.NodeIterator, error) {
	input := s3.ListObjectsV2Input{
		Bucket: aws.String(o.bucketName),
		Prefix: aws.String(o.key),
	}
	res, err := o.s3c.Client.ListObjectsV2(&input)
	fmt.Printf("[S3] ListObjectsV2(%#v) -> %#v, %v\n", input, res, err)
	if err != nil {
		return nil, err
	}
	itr := &objectsItr{
		s3c:        o.s3c,
		bucketName: o.bucketName,
		nameOffset: o.nameOffset + len(o.getName()),
		prefix:     o.key,
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
		parentKey := filepath.Join(o.getKey(), filepath.Join(subpath[:len(subpath)-1]...))
		if !strings.HasSuffix(key, "/") {
			key += "/"
		}
		objInfo := objectInfo(len(parentKey), object, key)
		nodes := make([]ninep.Node, 0, len(subpath))
		for i, part := range subpath[:len(subpath)-1] {
			name := part
			if name == "." && i > 0 {
				name = subpath[i-1]
			}
			joinedSubpath := filepath.Join(subpath[:i]...)
			key := filepath.Join(o.getKey(), joinedSubpath)
			nodes = append(nodes, &objectNode{
				s3c:        o.s3c,
				bucketName: o.bucketName,
				nameOffset: o.nameOffset + len(joinedSubpath) - len(name),
				key:        key,
				op:         o.op,
				info: &ninep.SimpleFileInfo{
					FIName:    name,
					FIMode:    0777 | os.ModeDir,
					FIModTime: *object.LastModified,
					FISize:    *object.Size,
				},
			})
		}
		// prefix := o.getPath(subpath[:len(subpath)-1])
		nodes = append(nodes, &objectNode{
			s3c:        o.s3c,
			bucketName: o.bucketName,
			nameOffset: o.nameOffset + len(filepath.Join(subpath[:len(subpath)-1]...)),
			key:        filepath.Join(o.getKey(), filepath.Join(subpath...)),
			obj:        object,
			info:       objInfo,
			op:         o.op,
		})
		return nodes, nil
	} else {
		nodes := make([]ninep.Node, 0, len(subpath))
		for i, part := range subpath {
			var dirMode os.FileMode
			if strings.HasSuffix(part, "/") || (size > 0 && strings.HasPrefix(*res.Contents[0].Key, key)) {
				dirMode = os.ModeDir
			}
			name := part
			if name == "." && i > 0 {
				name = subpath[i-1]
			}
			nodes = append(nodes, &objectNode{
				s3c:        o.s3c,
				bucketName: o.bucketName,
				nameOffset: o.nameOffset + len(filepath.Join(subpath[:i]...)),
				key:        filepath.Join(o.getKey(), filepath.Join(subpath...)),
				op:         o.op,
				info: &ninep.SimpleFileInfo{
					FIName: name,
					FIMode: 0777 | dirMode,
				},
			})
		}
		return nodes, nil
	}
}

func (o *objectNode) CreateFile(name string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
	return objectFileHandle(o.s3c, o.bucketName, filepath.Join(o.getKey(), name), o.op, flag)
}
func (o *objectNode) CreateDir(name string, mode ninep.Mode) error {
	dirName := name
	if !strings.HasSuffix(name, "/") {
		dirName += "/"
	}
	key := filepath.Join(o.getKey(), name)
	_, err := o.s3c.Client.PutObject(&s3.PutObjectInput{
		Bucket:        aws.String(o.bucketName),
		Key:           aws.String(key),
		ContentLength: aws.Int64(0),
	})
	return err
}
func (o *objectNode) DeleteWithMode(name string, m ninep.Mode) error {
	key := filepath.Join(o.getKey(), name)
	if m.IsDir() {
		key += "/"
		input := &s3.ListObjectsV2Input{
			Bucket: aws.String(o.bucketName),
			Prefix: aws.String(key),
		}
		ctx := context.Background()
		var deleteErr error
		err := o.s3c.Client.ListObjectsV2PagesWithContext(ctx, input, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			ids := make([]*s3.ObjectIdentifier, 0, len(page.Contents))
			for _, obj := range page.Contents {
				ids = append(ids, &s3.ObjectIdentifier{Key: obj.Key})
			}
			_, deleteErr = o.s3c.Client.DeleteObjects(&s3.DeleteObjectsInput{
				Bucket: aws.String(o.bucketName),
				Delete: &s3.Delete{Objects: ids},
			})
			if deleteErr != nil {
				fmt.Printf("DeleteWithMode(%#v, %s) -> %v\n", name, m, deleteErr)
				return false
			}
			return true
		})
		fmt.Printf("DeleteWithMode(%#v, %s) | %#v -> %v\n", name, m, key, err)
		if err != nil {
			return err
		}
		return deleteErr
	} else {
		out, err := o.s3c.Client.DeleteObject(&s3.DeleteObjectInput{
			Bucket: aws.String(o.bucketName),
			Key:    aws.String(key),
		})
		fmt.Printf("DeleteWithMode(%#v, %s) | %#v -> %#v, %v\n", name, m, key, out, err)
		return err
	}
}
func (o *objectNode) Delete(name string) error {
	key := filepath.Join(o.getKey(), name)
	_, err := o.s3c.Client.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(o.bucketName),
		Key:    aws.String(key),
	})
	fmt.Printf("Delete(%#v) | %#v -> %v\n", name, key, err)
	return err
}

func (o *objectNode) Info() (os.FileInfo, error) {
	if o.info != nil {
		return o.info, nil
	}

	key := o.key
	object := o.obj
	// TODO: should we cache the object fetch?
	if o.obj == nil {
		input := &s3.ListObjectsV2Input{
			Bucket:  aws.String(o.bucketName),
			Prefix:  aws.String(key),
			MaxKeys: aws.Int64(1),
		}
		res, err := o.s3c.Client.ListObjectsV2(input)
		fmt.Printf("[S3.objectNode.Info] ListObjectsV2(%#v) %#v %v\n", input, res, err)
		if err != nil {
			return nil, err
		}

		if len(res.Contents) == 1 {
			obj := res.Contents[0]
			if keysMatch(obj.Key, key) {
				object = obj
			}
		}
		if object == nil && len(res.Contents) == 0 {
			return nil, os.ErrNotExist
		}
	}
	if !strings.HasSuffix(key, "/") {
		key += "/"
	}
	return objectInfo(o.nameOffset, object, key), nil
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
	var rest []ninep.Node
	if len(keyPrefix) > 1 {
		var err error
		rest, err = object.Walk(keyPrefix[1:])
		if err != nil {
			return nil, err
		}
	}
	traversal := make([]ninep.Node, 0, len(keyPrefix))
	traversal = append(traversal, object)
	traversal = append(traversal, rest...)
	return traversal, nil
}

func objectsForBucket(s3c *S3Ctx, name, bucketName string, op objectOperation) ninep.Node {
	return &objectNode{
		s3c:        s3c,
		bucketName: bucketName,
		op:         op,
		nameOffset: 0,
		key:        "",
		info: &ninep.SimpleFileInfo{
			FIName: name,
			FIMode: 0555 | os.ModeDir,
		},
	}
}

////////////////////////////////

type objectsItr struct {
	s3c        *S3Ctx
	bucketName string
	prefix     string
	input      s3.ListObjectsV2Input
	output     *s3.ListObjectsV2Output
	nameOffset int
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
		nameOffset: itr.nameOffset,
		key:        itr.prefix,
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

///////////////////////////////////////////////////////////////

func objectsRoot(name string, s3c *S3Ctx, bucketName string) ninep.Node {
	return staticDir(
		name,
		objectsForBucket(s3c, "data", bucketName, opData),
	)
}
