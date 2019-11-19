package s3fs

import (
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jeffh/cfs/ninep"
)

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
func (b *buckets) Delete(name string) error {
	_, err := b.s3c.Client.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(name),
	})
	return err
}
func (b *buckets) List() (ninep.NodeIterator, error) {
	resp, err := b.s3c.Client.ListBuckets(&s3.ListBucketsInput{})
	fmt.Printf("[S3.buckets.List] ListBuckets()\n")
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
			nodes = append(nodes, &bucketNode{
				s3c:        b.s3c,
				bucketName: *bucket.Name,
				bucket:     bucket,
				now:        now,
				uid:        uid,
			})
		}
	}
	return ninep.MakeNodeSliceIterator(nodes), nil
}

func (b *buckets) Walk(subpath []string) ([]ninep.Node, error) {
	size := len(subpath)
	if size < 1 {
		return nil, nil
	}

	var (
		nodes []ninep.Node
		err   error
	)

	bucket := subpath[0]
	subpath = subpath[1:]

	bNode := &bucketNode{
		s3c:        b.s3c,
		bucketName: bucket,
	}

	nodes = append(nodes, bNode)
	if size > 1 {
		operation := subpath[0]
		subpath = subpath[1:]
		switch operation {
		case "objects":
			sNode := objectsRoot("objects", b.s3c, bucket)
			nodes = append(nodes, sNode)

			if size > 2 {
				staticDir := subpath[0]
				subpath = subpath[1:]

				switch staticDir {
				case "data":
					// list all objects
					var node ninep.Node = objectsForBucket(b.s3c, "data", bucket, opData)
					nodes = append(nodes, node)
					if size > 3 && (subpath[0] != "." && subpath[0] != "") {
						var objNodes []ninep.Node
						objNodes, err = objectsOrObjectForBucket(b.s3c, bucket, subpath, opData)
						if err == nil {
							nodes = append(nodes, objNodes...)
						}
					} else {
						// repeat if "." is at the end...
						for _, p := range subpath {
							if p == "." {
								nodes = append(nodes, node)
							} else {
								break
							}
						}
					}
				case ".", "":
					nodes = append(nodes, sNode)
				default:
					err = os.ErrNotExist
				}
			}
		case ".", "":
			nodes = append(nodes, bNode)
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
	return nil, ErrUseMkDirToCreateBucket
}
func (b *buckets) CreateDir(name string, mode ninep.Mode) error {
	_, err := b.s3c.Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(name),
	})
	err = mapAwsToNinep(err)
	fmt.Printf("[S3] CreateBucket(%#v) -> %v\n", name, err)
	return err
}

////////////////////////////////////////////////////////////////////////////////

type bucketNode struct {
	// required
	s3c        *S3Ctx
	bucketName string

	// optional
	bucket *s3.Bucket
	uid    string
	now    time.Time
}

func (b *bucketNode) Info() (os.FileInfo, error) {
	if b.bucket == nil {
		// we only have Listing all buckets to find the one we need...
		resp, err := b.s3c.Client.ListBuckets(&s3.ListBucketsInput{})
		fmt.Printf("[S3.bucket.Info] ListBuckets() | %v %p\n", b.bucketName, b)
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
		for _, bucket := range resp.Buckets {
			if bucket != nil && bucket.Name != nil {
				b.uid = uid
				b.now = now
				b.bucket = bucket
				break
			}
		}
		if b.bucket == nil {
			return nil, os.ErrNotExist
		}
	}
	var date time.Time
	if b.bucket.CreationDate != nil {
		date = *b.bucket.CreationDate
	} else {
		date = b.now
	}
	var info os.FileInfo = &ninep.SimpleFileInfo{
		FIName:    b.bucketName,
		FIMode:    os.ModeDir | 0777,
		FIModTime: date,
	}
	if b.uid != "" {
		info = ninep.FileInfoWithUsers(info, b.uid, "", "")
	}
	return info, nil
}

func (b *bucketNode) List() (ninep.NodeIterator, error) {
	nodes := []ninep.Node{
		objectsForBucket(b.s3c, "objects", b.bucketName, opData),
	}
	return ninep.MakeNodeSliceIterator(nodes), nil
}

func (b *bucketNode) CreateFile(name string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
	return nil, ninep.ErrInvalidAccess
}

func (b *bucketNode) WriteInfo(in os.FileInfo) error               { return ninep.ErrInvalidAccess }
func (b *bucketNode) CreateDir(name string, mode ninep.Mode) error { return ninep.ErrInvalidAccess }
func (b *bucketNode) Delete(name string) error                     { return ninep.ErrInvalidAccess }
